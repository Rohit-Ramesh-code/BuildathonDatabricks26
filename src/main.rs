//! Shopping Sessions pipeline. rules.md is the spec.
//!
//! Stages: parse CSV -> dedup -> validate -> link people -> sessionize ->
//! attribute clicks -> write the two daily Parquet tables.

mod time;

use std::cmp::Ordering;
use std::env;
use std::fs::{self, File};
use std::hash::{BuildHasher, Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::Instant;

use anyhow::{Context, Result};
use arrow_array::{Date32Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use memchr::memchr;
use memmap2::Mmap;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use rayon::prelude::*;
use rustc_hash::FxBuildHasher;
use std::collections::HashMap;
use std::sync::Arc;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const DAY_MS: i64 = 86_400_000;
const SESSION_GAP_MS: i64 = 1_800_000;
const CLICK_REACH_MS: i64 = 7 * DAY_MS;
const LATE_MS: i64 = DAY_MS;
const PRICE_BAD: u32 = u32::MAX;
const ORGANIC: u32 = u32::MAX;
const TS_BAD: i64 = i64::MIN;

type FxHashMap<K, V> = HashMap<K, V, FxBuildHasher>;

fn fxmap<K, V>() -> FxHashMap<K, V> {
    HashMap::with_hasher(FxBuildHasher)
}

fn fxmap_cap<K, V>(cap: usize) -> FxHashMap<K, V> {
    HashMap::with_capacity_and_hasher(cap, FxBuildHasher)
}

#[derive(Clone, Copy)]
struct ByteKey {
    ptr: *const u8,
    len: u32,
}

unsafe impl Send for ByteKey {}
unsafe impl Sync for ByteKey {}

impl ByteKey {
    fn new(s: &[u8]) -> Self {
        Self {
            ptr: s.as_ptr(),
            len: s.len() as u32,
        }
    }

    fn bytes(&self) -> &[u8] {
        if self.len == 0 {
            &[]
        } else {
            unsafe { std::slice::from_raw_parts(self.ptr, self.len as usize) }
        }
    }
}

impl PartialEq for ByteKey {
    fn eq(&self, other: &Self) -> bool {
        self.bytes() == other.bytes()
    }
}
impl Eq for ByteKey {}

impl Hash for ByteKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.bytes().hash(state);
    }
}

#[derive(Clone, Copy)]
struct RawEvent {
    user: ByteKey,
    ts: i64,
    ingest: i64,
    product: ByteKey,
    price: u32,
    kind: u8,
}

#[derive(Clone, Copy)]
struct RawClick {
    user: ByteKey,
    ts: i64,
    campaign: u32,
}

struct Sharded<V> {
    shards: Vec<Mutex<FxHashMap<ByteKey, V>>>,
}

impl<V> Sharded<V> {
    fn new(n: usize, cap_each: usize) -> Self {
        let shards = (0..n)
            .map(|_| Mutex::new(fxmap_cap(cap_each)))
            .collect();
        Self { shards }
    }

    fn shard(&self, key: &ByteKey) -> usize {
        let mut hasher = FxBuildHasher.build_hasher();
        key.hash(&mut hasher);
        (hasher.finish() as usize) % self.shards.len()
    }
}

impl Sharded<RawEvent> {
    fn insert_earliest(&self, key: ByteKey, rec: RawEvent) {
        let mut map = self.shards[self.shard(&key)].lock().unwrap();
        match map.entry(key) {
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(rec);
            }
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                if rec.ingest < entry.get().ingest {
                    entry.insert(rec);
                }
            }
        }
    }
}

impl Sharded<RawClick> {
    fn insert_first(&self, key: ByteKey, rec: RawClick) {
        let mut map = self.shards[self.shard(&key)].lock().unwrap();
        map.entry(key).or_insert(rec);
    }
}

struct Users {
    arena: Vec<u8>,
    spans: Vec<(u32, u32)>,
    index: FxHashMap<u64, Vec<u32>>,
    parent: Vec<u32>,
    rank: Vec<u8>,
    min_id: Vec<u32>,
}

impl Users {
    fn new() -> Self {
        Self {
            arena: Vec::with_capacity(1 << 20),
            spans: Vec::with_capacity(1 << 16),
            index: fxmap(),
            parent: Vec::with_capacity(1 << 16),
            rank: Vec::with_capacity(1 << 16),
            min_id: Vec::with_capacity(1 << 16),
        }
    }

    fn bytes(&self, id: u32) -> &[u8] {
        let (start, len) = self.spans[id as usize];
        &self.arena[start as usize..start as usize + len as usize]
    }

    fn text(&self, id: u32) -> String {
        let bytes = self.bytes(id);
        String::from_utf8(bytes.to_vec())
            .unwrap_or_else(|err| String::from_utf8_lossy(err.as_bytes()).into_owned())
    }

    fn id_less(&self, a: u32, b: u32) -> bool {
        let (sa, la) = self.spans[a as usize];
        let (sb, lb) = self.spans[b as usize];
        let left = sa as usize..sa as usize + la as usize;
        let right = sb as usize..sb as usize + lb as usize;
        self.arena[left].cmp(&self.arena[right]) == Ordering::Less
    }

    fn intern(&mut self, value: &[u8]) -> u32 {
        let hash = hash_bytes(value);
        if let Some(ids) = self.index.get(&hash) {
            for &id in ids {
                if self.bytes(id) == value {
                    return id;
                }
            }
        }
        let id = self.spans.len() as u32;
        let start = self.arena.len() as u32;
        self.arena.extend_from_slice(value);
        self.spans.push((start, value.len() as u32));
        self.index.entry(hash).or_default().push(id);
        id
    }

    fn ensure(&mut self, value: &[u8]) -> u32 {
        let id = self.intern(value);
        if self.parent.len() == id as usize {
            self.parent.push(id);
            self.rank.push(0);
            self.min_id.push(id);
        }
        id
    }

    fn find(&mut self, mut x: u32) -> u32 {
        let mut root = x;
        while self.parent[root as usize] != root {
            root = self.parent[root as usize];
        }
        while self.parent[x as usize] != root {
            let parent = self.parent[x as usize];
            self.parent[x as usize] = root;
            x = parent;
        }
        root
    }

    fn union(&mut self, a: &[u8], b: &[u8]) {
        let ia = self.ensure(a);
        let ib = self.ensure(b);
        let mut ra = self.find(ia);
        let mut rb = self.find(ib);
        if ra == rb {
            return;
        }
        if self.rank[ra as usize] < self.rank[rb as usize] {
            std::mem::swap(&mut ra, &mut rb);
        }
        self.parent[rb as usize] = ra;
        if self.rank[ra as usize] == self.rank[rb as usize] {
            self.rank[ra as usize] = self.rank[ra as usize].saturating_add(1);
        }
        let mb = self.min_id[rb as usize];
        let ma = self.min_id[ra as usize];
        if self.id_less(mb, ma) {
            self.min_id[ra as usize] = mb;
        }
    }

    fn person(&mut self, value: &[u8]) -> u32 {
        let id = self.ensure(value);
        let root = self.find(id);
        self.min_id[root as usize]
    }
}

fn hash_bytes(data: &[u8]) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    for &byte in data {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

struct Event {
    person: u32,
    ts: i64,
    cat: u16,
    price: u32,
}

struct Click {
    person: u32,
    ts: i64,
    campaign: u32,
    id: ByteKey,
}

struct Session {
    person: u32,
    day: i32,
    start: i64,
    revenue: i64,
    purchases: u32,
}

#[derive(Default)]
struct UserAgg {
    sessions: i64,
    events: i64,
    revenue: i64,
}

#[derive(Default)]
struct CampAgg {
    sessions: i64,
    purchasing: i64,
    revenue: i64,
    assisted: i64,
}

fn ncpu() -> usize {
    env::var("NCPU")
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|&n| n > 0)
        .unwrap_or_else(|| std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4))
}

fn list_csv(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    if !dir.is_dir() {
        return Ok(out);
    }
    for entry in fs::read_dir(dir).with_context(|| format!("read {}", dir.display()))? {
        let path = entry?.path();
        if path.extension().and_then(|ext| ext.to_str()) == Some("csv") && path.is_file() {
            out.push(path);
        }
    }
    Ok(out)
}

fn map_file(path: &Path) -> Result<Mmap> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let mmap = unsafe { Mmap::map(&file) }.with_context(|| format!("mmap {}", path.display()))?;
    #[cfg(unix)]
    {
        let _ = mmap.advise(memmap2::Advice::Sequential);
    }
    Ok(mmap)
}

fn for_each_line(data: &[u8], mut visit: impl FnMut(&[u8])) {
    let mut rest = data;
    while let Some(index) = memchr(b'\n', rest) {
        visit(&rest[..index]);
        rest = &rest[index + 1..];
    }
    if !rest.is_empty() {
        visit(rest);
    }
}

/// Body of a CSV after its header, split on newline boundaries.
fn line_chunks(data: &[u8], threads: usize) -> Vec<(usize, usize)> {
    let Some(newline) = memchr(b'\n', data) else {
        return Vec::new();
    };
    let mut start = newline + 1;
    if start >= data.len() {
        return Vec::new();
    }
    let body = data.len() - start;
    let min_chunk = 256 * 1024;
    let n = (body / min_chunk).max(1).min(threads.max(1));
    let step = (body / n).max(1);
    let mut spans = Vec::with_capacity(n);
    for i in 0..n {
        if start >= data.len() {
            break;
        }
        let mut end = if i + 1 == n {
            data.len()
        } else {
            (start + step).min(data.len())
        };
        if end < data.len() {
            match memchr(b'\n', &data[end..]) {
                Some(rel) => end += rel + 1,
                None => end = data.len(),
            }
        }
        if end > start {
            spans.push((start, end));
        }
        start = end;
    }
    spans
}

fn split_n<'a>(line: &'a [u8], n: usize, out: &mut [&'a [u8]; 8]) -> bool {
    let mut start = 0usize;
    let mut count = 0usize;
    for (index, &byte) in line.iter().enumerate() {
        if byte == b',' {
            if count >= out.len() || count == n {
                return false;
            }
            out[count] = &line[start..index];
            count += 1;
            start = index + 1;
        }
    }
    if count + 1 != n || count >= out.len() {
        return false;
    }
    out[count] = &line[start..];
    true
}

fn unquote(field: &[u8]) -> Option<&[u8]> {
    if field.first() == Some(&b'"') {
        if field.len() >= 2 && field[field.len() - 1] == b'"' && memchr(b'"', &field[1..field.len() - 1]).is_none()
        {
            Some(&field[1..field.len() - 1])
        } else {
            None
        }
    } else if memchr(b'"', field).is_some() {
        None
    } else {
        Some(field)
    }
}

fn parse_price(field: &[u8]) -> Option<u32> {
    if field.is_empty() {
        return None;
    }
    let mut value: u64 = 0;
    for &byte in field {
        if !byte.is_ascii_digit() {
            return None;
        }
        value = value.saturating_mul(10).saturating_add((byte - b'0') as u64);
        if value > 10_000_000 {
            return None;
        }
    }
    if value == 0 {
        None
    } else {
        Some(value as u32)
    }
}

fn kind_of(field: &[u8]) -> u8 {
    match field {
        b"view" => 1,
        b"cart" => 2,
        b"purchase" => 3,
        _ => 0,
    }
}

fn json_str<'a>(text: &'a str, key: &str) -> Result<&'a str> {
    let pattern = format!("\"{key}\"");
    let index = text.find(&pattern).with_context(|| format!("missing {key}"))?;
    let rest = &text[index + pattern.len()..];
    let open = rest.find('"').context("missing json string")?;
    let rest = &rest[open + 1..];
    let close = rest.find('"').context("unterminated json string")?;
    Ok(&rest[..close])
}

fn load_window(path: &Path) -> Result<(i64, i64)> {
    let text = fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    let start = time::parse_ts(json_str(&text, "window_start")?.as_bytes()).context("window_start")?;
    let end = time::parse_ts(json_str(&text, "window_end")?.as_bytes()).context("window_end")?;
    Ok((start, end))
}

struct Catalog {
    products: FxHashMap<Vec<u8>, u16>,
    categories: Vec<String>,
    campaigns: FxHashMap<Vec<u8>, u32>,
    campaign_ids: Vec<String>,
    channels: Vec<String>,
    cat_index: FxHashMap<Vec<u8>, u16>,
}

fn intern_cat(catalog: &mut Catalog, raw: &[u8]) -> u16 {
    if let Some(&id) = catalog.products_cat_index(raw) {
        return id;
    }
    let id = catalog.categories.len() as u16;
    let name = String::from_utf8(raw.to_vec())
        .unwrap_or_else(|err| String::from_utf8_lossy(err.as_bytes()).into_owned());
    catalog.categories.push(name);
    catalog.cat_index.insert(raw.to_vec(), id);
    id
}

impl Catalog {
    fn products_cat_index(&self, raw: &[u8]) -> Option<&u16> {
        self.cat_index.get(raw)
    }
}

fn load_catalog(data_dir: &Path) -> Result<Catalog> {
    let mut cat_index = fxmap();
    cat_index.insert(b"unknown".to_vec(), 0u16);
    let mut catalog = Catalog {
        products: fxmap(),
        categories: vec!["unknown".to_string()],
        campaigns: fxmap(),
        campaign_ids: Vec::new(),
        channels: Vec::new(),
        cat_index,
    };

    let product_bytes = fs::read(data_dir.join("products.csv")).context("products.csv")?;
    let mut header = true;
    for_each_line(&product_bytes, |line| {
        if header {
            header = false;
            return;
        }
        let mut fields = [&[][..]; 8];
        if !split_n(line, 2, &mut fields) {
            return;
        }
        let (Some(product), Some(category)) = (unquote(fields[0]), unquote(fields[1])) else {
            return;
        };
        if product.is_empty() {
            return;
        }
        let cat = intern_cat(&mut catalog, category);
        catalog.products.entry(product.to_vec()).or_insert(cat);
    });

    let campaign_bytes = fs::read(data_dir.join("campaigns.csv")).context("campaigns.csv")?;
    header = true;
    for_each_line(&campaign_bytes, |line| {
        if header {
            header = false;
            return;
        }
        let mut fields = [&[][..]; 8];
        if !split_n(line, 2, &mut fields) {
            return;
        }
        let (Some(campaign), Some(channel)) = (unquote(fields[0]), unquote(fields[1])) else {
            return;
        };
        if campaign.is_empty() || catalog.campaigns.contains_key(campaign) {
            return;
        }
        let id = catalog.campaign_ids.len() as u32;
        catalog.campaigns.insert(campaign.to_vec(), id);
        catalog.campaign_ids.push(
            String::from_utf8(campaign.to_vec())
                .unwrap_or_else(|err| String::from_utf8_lossy(err.as_bytes()).into_owned()),
        );
        catalog.channels.push(
            String::from_utf8(channel.to_vec())
                .unwrap_or_else(|err| String::from_utf8_lossy(err.as_bytes()).into_owned()),
        );
    });
    Ok(catalog)
}

fn load_links(files: &[Mmap], users: &mut Users) {
    for file in files {
        let mut header = true;
        for_each_line(file, |line| {
            if header {
                header = false;
                return;
            }
            let mut fields = [&[][..]; 8];
            if !split_n(line, 2, &mut fields) {
                return;
            }
            let (Some(left), Some(right)) = (unquote(fields[0]), unquote(fields[1])) else {
                return;
            };
            if left.is_empty() || right.is_empty() {
                return;
            }
            users.union(left, right);
        });
    }
}

fn parse_events(files: &[Mmap], threads: usize) -> Sharded<RawEvent> {
    let bytes: usize = files.iter().map(|file| file.len()).sum();
    let estimate = (bytes / 48).max(1);
    let shards = (threads * 8).max(1);
    let sink = Sharded::new(shards, estimate / shards + 1);
    let mut tasks = Vec::new();
    for file in files {
        for (start, end) in line_chunks(file, threads) {
            tasks.push((file, start, end));
        }
    }
    tasks.par_iter().for_each(|(file, start, end)| {
        for_each_line(&file[*start..*end], |line| {
            if line.is_empty() {
                return;
            }
            let mut fields = [&[][..]; 8];
            if !split_n(line, 7, &mut fields) {
                return;
            }
            let Some(event_id) = unquote(fields[0]) else { return };
            if event_id.is_empty() {
                return;
            }
            let (Some(user), Some(ts_raw), Some(ingest_raw), Some(kind_raw), Some(product), Some(price_raw)) = (
                unquote(fields[1]),
                unquote(fields[2]),
                unquote(fields[3]),
                unquote(fields[4]),
                unquote(fields[5]),
                unquote(fields[6]),
            ) else {
                return;
            };
            let Some(ingest) = time::parse_ts(ingest_raw) else {
                return;
            };
            let kind = kind_of(kind_raw);
            let price = if kind == 3 {
                parse_price(price_raw).unwrap_or(PRICE_BAD)
            } else {
                0
            };
            sink.insert_earliest(
                ByteKey::new(event_id),
                RawEvent {
                    user: ByteKey::new(user),
                    ts: time::parse_ts(ts_raw).unwrap_or(TS_BAD),
                    ingest,
                    product: ByteKey::new(product),
                    price,
                    kind,
                },
            );
        });
    });
    sink
}

fn parse_clicks(files: &[Mmap], campaigns: &FxHashMap<Vec<u8>, u32>, threads: usize) -> Sharded<RawClick> {
    let bytes: usize = files.iter().map(|file| file.len()).sum();
    let estimate = (bytes / 32).max(1);
    let shards = (threads * 8).max(1);
    let sink = Sharded::new(shards, estimate / shards + 1);
    let mut tasks = Vec::new();
    for file in files {
        for (start, end) in line_chunks(file, threads) {
            tasks.push((file, start, end));
        }
    }
    tasks.par_iter().for_each(|(file, start, end)| {
        for_each_line(&file[*start..*end], |line| {
            if line.is_empty() {
                return;
            }
            let mut fields = [&[][..]; 8];
            if !split_n(line, 4, &mut fields) {
                return;
            }
            let Some(click_id) = unquote(fields[0]) else { return };
            if click_id.is_empty() {
                return;
            }
            let (Some(user), Some(campaign), Some(ts_raw)) = (
                unquote(fields[1]),
                unquote(fields[2]),
                unquote(fields[3]),
            ) else {
                return;
            };
            sink.insert_first(
                ByteKey::new(click_id),
                RawClick {
                    user: ByteKey::new(user),
                    ts: time::parse_ts(ts_raw).unwrap_or(TS_BAD),
                    campaign: campaigns.get(campaign).copied().unwrap_or(ORGANIC),
                },
            );
        });
    });
    sink
}

fn take_events(sharded: Sharded<RawEvent>) -> Vec<(ByteKey, RawEvent)> {
    let mut out = Vec::new();
    for shard in sharded.shards {
        out.extend(shard.into_inner().unwrap());
    }
    out
}

fn take_clicks(sharded: Sharded<RawClick>) -> Vec<(ByteKey, RawClick)> {
    let mut out = Vec::new();
    for shard in sharded.shards {
        out.extend(shard.into_inner().unwrap());
    }
    out
}

fn day_of(ts: i64) -> i32 {
    ts.div_euclid(DAY_MS) as i32
}

fn write_user_daily(dir: &Path, rows: &[(String, i32, i64, i64, i64, Option<String>)]) -> Result<()> {
    fs::create_dir_all(dir)?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Utf8, false),
        Field::new("day", DataType::Date32, false),
        Field::new("sessions", DataType::Int64, false),
        Field::new("events", DataType::Int64, false),
        Field::new("revenue_cents", DataType::Int64, false),
        Field::new("top_category", DataType::Utf8, true),
    ]));
    let user_id = StringArray::from(rows.iter().map(|row| Some(row.0.as_str())).collect::<Vec<_>>());
    let day = Date32Array::from(rows.iter().map(|row| row.1).collect::<Vec<_>>());
    let sessions = Int64Array::from(rows.iter().map(|row| row.2).collect::<Vec<_>>());
    let events = Int64Array::from(rows.iter().map(|row| row.3).collect::<Vec<_>>());
    let revenue = Int64Array::from(rows.iter().map(|row| row.4).collect::<Vec<_>>());
    let top = StringArray::from(
        rows.iter()
            .map(|row| row.5.as_deref())
            .collect::<Vec<_>>(),
    );
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(user_id),
            Arc::new(day),
            Arc::new(sessions),
            Arc::new(events),
            Arc::new(revenue),
            Arc::new(top),
        ],
    )?;
    write_batch(&dir.join("part-0.parquet"), schema, batch)
}

fn write_campaign_daily(
    dir: &Path,
    rows: &[(String, String, i32, i64, i64, i64, i64)],
) -> Result<()> {
    fs::create_dir_all(dir)?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("campaign_id", DataType::Utf8, false),
        Field::new("channel", DataType::Utf8, false),
        Field::new("day", DataType::Date32, false),
        Field::new("sessions", DataType::Int64, false),
        Field::new("purchasing_sessions", DataType::Int64, false),
        Field::new("revenue_cents", DataType::Int64, false),
        Field::new("assisted_sessions", DataType::Int64, false),
    ]));
    let campaign = StringArray::from(rows.iter().map(|row| Some(row.0.as_str())).collect::<Vec<_>>());
    let channel = StringArray::from(rows.iter().map(|row| Some(row.1.as_str())).collect::<Vec<_>>());
    let day = Date32Array::from(rows.iter().map(|row| row.2).collect::<Vec<_>>());
    let sessions = Int64Array::from(rows.iter().map(|row| row.3).collect::<Vec<_>>());
    let purchasing = Int64Array::from(rows.iter().map(|row| row.4).collect::<Vec<_>>());
    let revenue = Int64Array::from(rows.iter().map(|row| row.5).collect::<Vec<_>>());
    let assisted = Int64Array::from(rows.iter().map(|row| row.6).collect::<Vec<_>>());
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(campaign),
            Arc::new(channel),
            Arc::new(day),
            Arc::new(sessions),
            Arc::new(purchasing),
            Arc::new(revenue),
            Arc::new(assisted),
        ],
    )?;
    write_batch(&dir.join("part-0.parquet"), schema, batch)
}

fn write_batch(path: &Path, schema: Arc<Schema>, batch: RecordBatch) -> Result<()> {
    let file = File::create(path).with_context(|| format!("create {}", path.display()))?;
    let props = WriterProperties::builder()
        .set_compression(Compression::UNCOMPRESSED)
        .build();
    let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

fn run() -> Result<()> {
    let started = Instant::now();
    let threads = ncpu();
    let _ = rayon::ThreadPoolBuilder::new()
        .num_threads(threads)
        .build_global();

    let mut args = env::args().skip(1);
    let data_dir = env::var("DATA_DIR")
        .ok()
        .or_else(|| args.next())
        .unwrap_or_else(|| "/data".to_string());
    let out_dir = env::var("OUTPUT_DIR")
        .ok()
        .or_else(|| args.next())
        .unwrap_or_else(|| "/output".to_string());
    let data_dir = PathBuf::from(data_dir);
    let out_dir = PathBuf::from(out_dir);

    let (window_start, window_end) = load_window(&data_dir.join("manifest.json"))?;
    let click_start = window_start - CLICK_REACH_MS;
    let catalog = load_catalog(&data_dir)?;
    eprintln!(
        "window {window_start}..{window_end} products {} campaigns {} in {}ms",
        catalog.products.len(),
        catalog.campaign_ids.len(),
        started.elapsed().as_millis()
    );

    let mark = Instant::now();
    let link_paths = list_csv(&data_dir.join("links"))?;
    let link_files: Vec<Mmap> = link_paths.iter().map(|path| map_file(path)).collect::<Result<_>>()?;
    let mut users = Users::new();
    load_links(&link_files, &mut users);
    eprintln!("links {} ids in {}ms", users.spans.len(), mark.elapsed().as_millis());

    let mark = Instant::now();
    let event_paths = list_csv(&data_dir.join("events"))?;
    let click_paths = list_csv(&data_dir.join("clicks"))?;
    let event_files: Vec<Mmap> = event_paths.iter().map(|path| map_file(path)).collect::<Result<_>>()?;
    let click_files: Vec<Mmap> = click_paths.iter().map(|path| map_file(path)).collect::<Result<_>>()?;
    let (raw_events, raw_clicks) = rayon::join(
        || parse_events(&event_files, threads),
        || parse_clicks(&click_files, &catalog.campaigns, threads),
    );
    let raw_events = take_events(raw_events);
    let raw_clicks = take_clicks(raw_clicks);
    eprintln!(
        "parsed events {} clicks {} in {}ms",
        raw_events.len(),
        raw_clicks.len(),
        mark.elapsed().as_millis()
    );

    let mark = Instant::now();
    let mut events = Vec::with_capacity(raw_events.len());
    for (_, raw) in &raw_events {
        if raw.user.len == 0
            || raw.kind == 0
            || raw.product.len == 0
            || raw.ts == TS_BAD
            || raw.ts < window_start
            || raw.ts >= window_end
            || (raw.kind == 3 && raw.price == PRICE_BAD)
            || raw.ingest.saturating_sub(raw.ts) > LATE_MS
        {
            continue;
        }
        let person = users.person(raw.user.bytes());
        let cat = catalog
            .products
            .get(raw.product.bytes())
            .copied()
            .unwrap_or(0);
        events.push(Event {
            person,
            ts: raw.ts,
            cat,
            price: if raw.kind == 3 { raw.price } else { 0 },
        });
    }
    drop(raw_events);

    let mut clicks = Vec::with_capacity(raw_clicks.len());
    for (click_id, raw) in &raw_clicks {
        if raw.user.len == 0
            || raw.campaign == ORGANIC
            || raw.ts == TS_BAD
            || raw.ts < click_start
            || raw.ts >= window_end
        {
            continue;
        }
        clicks.push(Click {
            person: users.person(raw.user.bytes()),
            ts: raw.ts,
            campaign: raw.campaign,
            id: *click_id,
        });
    }
    drop(raw_clicks);
    eprintln!(
        "valid events {} clicks {} in {}ms",
        events.len(),
        clicks.len(),
        mark.elapsed().as_millis()
    );

    let mark = Instant::now();
    events.par_sort_unstable_by(|left, right| {
        left.person.cmp(&right.person).then(left.ts.cmp(&right.ts))
    });
    clicks.par_sort_unstable_by(|left, right| {
        left.person
            .cmp(&right.person)
            .then(left.ts.cmp(&right.ts))
            .then_with(|| right.id.bytes().cmp(left.id.bytes()))
    });

    let mut sessions = Vec::new();
    let mut user_daily: FxHashMap<(u32, i32), UserAgg> = fxmap();
    let mut category_revenue: FxHashMap<(u32, i32, u16), i64> = fxmap();
    let mut index = 0usize;
    while index < events.len() {
        let person = events[index].person;
        let start_at = index;
        index += 1;
        while index < events.len() && events[index].person == person {
            index += 1;
        }
        let group = &events[start_at..index];
        let mut cursor = 0usize;
        while cursor < group.len() {
            let session_start = group[cursor].ts;
            let day = day_of(session_start);
            let mut last = session_start;
            let mut next = cursor + 1;
            while next < group.len() {
                let ts = group[next].ts;
                if ts != last && ts - last > SESSION_GAP_MS {
                    break;
                }
                if ts != last {
                    last = ts;
                }
                next += 1;
            }
            let agg = user_daily.entry((person, day)).or_default();
            agg.sessions += 1;
            agg.events += (next - cursor) as i64;
            let mut revenue = 0i64;
            let mut purchases = 0u32;
            for event in &group[cursor..next] {
                if event.price > 0 {
                    revenue += event.price as i64;
                    purchases += 1;
                    *category_revenue
                        .entry((person, day, event.cat))
                        .or_default() += event.price as i64;
                }
            }
            agg.revenue += revenue;
            sessions.push(Session {
                person,
                day,
                start: session_start,
                revenue,
                purchases,
            });
            cursor = next;
        }
    }
    eprintln!(
        "sessions {} user-days {} in {}ms",
        sessions.len(),
        user_daily.len(),
        mark.elapsed().as_millis()
    );

    let mark = Instant::now();
    let mut best_category: FxHashMap<(u32, i32), (i64, u16)> = fxmap();
    for ((person, day, cat), revenue) in category_revenue {
        match best_category.get_mut(&(person, day)) {
            None => {
                best_category.insert((person, day), (revenue, cat));
            }
            Some(current) => {
                let name = catalog.categories[cat as usize].as_str();
                let current_name = catalog.categories[current.1 as usize].as_str();
                if revenue > current.0 || (revenue == current.0 && name < current_name) {
                    *current = (revenue, cat);
                }
            }
        }
    }

    let campaign_count = catalog.campaign_ids.len();
    let mut stamp = vec![0u32; campaign_count];
    let mut generation = 0u32;
    let mut touched = Vec::new();
    let mut campaign_daily: FxHashMap<(u32, i32), CampAgg> = fxmap();
    let mut click_at = 0usize;
    let mut session_at = 0usize;
    while session_at < sessions.len() {
        let person = sessions[session_at].person;
        while click_at < clicks.len() && clicks[click_at].person < person {
            click_at += 1;
        }
        let mut click_end = click_at;
        while click_end < clicks.len() && clicks[click_end].person == person {
            click_end += 1;
        }
        let slice = &clicks[click_at..click_end];
        while session_at < sessions.len() && sessions[session_at].person == person {
            let session = &sessions[session_at];
            let earliest = session.start.saturating_sub(CLICK_REACH_MS);
            let lower = slice.partition_point(|click| click.ts < earliest);
            let upper = slice.partition_point(|click| click.ts <= session.start);
            let credited = if upper > lower {
                Some(slice[upper - 1].campaign)
            } else {
                None
            };
            generation = generation.wrapping_add(1);
            if generation == 0 {
                stamp.fill(0);
                generation = 1;
            }
            touched.clear();
            for click in &slice[lower..upper] {
                let id = click.campaign as usize;
                if stamp[id] != generation {
                    stamp[id] = generation;
                    touched.push(click.campaign);
                }
            }
            let bucket = credited.unwrap_or(ORGANIC);
            let agg = campaign_daily.entry((bucket, session.day)).or_default();
            agg.sessions += 1;
            agg.revenue += session.revenue;
            if session.purchases > 0 {
                agg.purchasing += 1;
            }
            for campaign in &touched {
                campaign_daily
                    .entry((*campaign, session.day))
                    .or_default()
                    .assisted += 1;
            }
            session_at += 1;
        }
        click_at = click_end;
    }
    eprintln!(
        "attributed campaign-rows {} in {}ms",
        campaign_daily.len(),
        mark.elapsed().as_millis()
    );

    let user_rows: Vec<_> = user_daily
        .into_iter()
        .map(|((person, day), agg)| {
            let top = best_category
                .get(&(person, day))
                .map(|&(_, cat)| catalog.categories[cat as usize].clone());
            (
                users.text(person),
                day,
                agg.sessions,
                agg.events,
                agg.revenue,
                top,
            )
        })
        .collect();
    let campaign_rows: Vec<_> = campaign_daily
        .into_iter()
        .filter(|(_, agg)| agg.sessions > 0 || agg.assisted > 0)
        .map(|((campaign, day), agg)| {
            let (id, channel) = if campaign == ORGANIC {
                ("organic".to_string(), "organic".to_string())
            } else {
                (
                    catalog.campaign_ids[campaign as usize].clone(),
                    catalog.channels[campaign as usize].clone(),
                )
            };
            (
                id,
                channel,
                day,
                agg.sessions,
                agg.purchasing,
                agg.revenue,
                agg.assisted,
            )
        })
        .collect();

    let user_sessions: i64 = user_rows.iter().map(|row| row.2).sum();
    let user_revenue: i64 = user_rows.iter().map(|row| row.4).sum();
    let camp_sessions: i64 = campaign_rows.iter().map(|row| row.3).sum();
    let camp_revenue: i64 = campaign_rows.iter().map(|row| row.5).sum();
    eprintln!(
        "self-check sessions {user_sessions} vs {camp_sessions}, revenue {user_revenue} vs {camp_revenue}"
    );

    write_user_daily(&out_dir.join("user_daily"), &user_rows)?;
    write_campaign_daily(&out_dir.join("campaign_daily"), &campaign_rows)?;
    eprintln!("done in {}ms", started.elapsed().as_millis());
    Ok(())
}

fn main() {
    if let Err(error) = run() {
        eprintln!("error: {error:#}");
        std::process::exit(1);
    }
}
