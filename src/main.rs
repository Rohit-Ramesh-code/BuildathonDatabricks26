mod blocks;
mod compact;
mod cpuguard;
mod csv;
mod hugealloc;
mod key;
mod output;
mod people;
mod pqw;
mod scatter;
mod ts;

use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use memmap2::Mmap;
use rustc_hash::{FxHashMap, FxHashSet};

use crate::csv::{ascii_digits, data_lines, for_each_record, parse_line};
use crate::compact::EventCopy32;
use crate::key::{Key, LongIds, key_of, key_of_deferred, mix};
use crate::people::People;
use crate::blocks::BlockScatter;
use crate::scatter::Scatter;
use crate::ts::DAY_MS;

#[global_allocator]
static ALLOCATOR: hugealloc::HugeAlloc = hugealloc::HugeAlloc;

const SESSION_GAP_MS: i64 = 1_800_000;
const CLICK_REACH_MS: i64 = 7 * DAY_MS;
const LATE_MS: i128 = DAY_MS as i128;
const PRICE_MAX: u64 = 10_000_000;
const NEVER: i64 = i64::MAX;
pub const ORGANIC: u32 = u32::MAX;

static DEDUP_PARTITIONS: AtomicUsize = AtomicUsize::new(1024);
static PERSON_PARTITIONS: AtomicUsize = AtomicUsize::new(1024);

fn set_partitions(estimated_events: usize) {
    let forced = std::env::var("PIPELINE_PARTITIONS").ok().and_then(|v| v.parse::<usize>().ok());
    let count = match forced {
        Some(n) if n.is_power_of_two() && n <= 8192 => n,
        _ if estimated_events > 30_000_000 => 4096,
        _ => 2048,
    };
    DEDUP_PARTITIONS.store(count, Ordering::Relaxed);
    PERSON_PARTITIONS.store(count, Ordering::Relaxed);
}

#[inline(always)]
fn dedup_partitions() -> usize {
    DEDUP_PARTITIONS.load(Ordering::Relaxed)
}

#[inline(always)]
fn person_partitions() -> usize {
    PERSON_PARTITIONS.load(Ordering::Relaxed)
}
const CHUNK_BYTES: usize = 16 << 20;
const CLICK_CHUNK_BYTES: usize = 4 << 20;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

struct Timer {
    on: bool,
    start: Instant,
    last: Instant,
}

impl Timer {
    fn new() -> Self {
        let now = Instant::now();
        Self { on: std::env::var_os("TIMING").is_some(), start: now, last: now }
    }
    fn mark(&mut self, stage: &str) {
        if self.on {
            let now = Instant::now();
            eprintln!(
                "[timing] {stage:<34} {:8.3}s  (total {:8.3}s)",
                (now - self.last).as_secs_f64(),
                (now - self.start).as_secs_f64()
            );
            self.last = now;
        }
    }
}

fn run_tasks<S: Send>(
    threads: usize,
    count: usize,
    make: impl Fn() -> S + Sync,
    work: impl Fn(&mut S, usize) + Sync,
) -> Vec<S> {
    if threads <= 1 {

        let mut state = make();
        for task in 0..count {
            work(&mut state, task);
        }
        return vec![state];
    }
    let next = AtomicUsize::new(0);
    std::thread::scope(|scope| {
        let handles: Vec<_> = (0..threads)
            .map(|_| {
                scope.spawn(|| {
                    let mut state = make();
                    loop {
                        let task = next.fetch_add(1, Ordering::Relaxed);
                        if task >= count {
                            break;
                        }
                        work(&mut state, task);
                    }
                    state
                })
            })
            .collect();
        handles.into_iter().map(|handle| handle.join().expect("worker panicked")).collect()
    })
}

fn csv_files(folder: &Path) -> Vec<PathBuf> {
    let Ok(entries) = std::fs::read_dir(folder) else { return Vec::new() };
    let mut files: Vec<PathBuf> = entries
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| {
            path.extension().is_some_and(|ext| ext == "csv")
                && std::fs::metadata(path).is_ok_and(|meta| meta.is_file())
        })
        .collect();
    files.sort();
    files
}

fn map_folder(folder: &Path) -> Result<Vec<Option<Mmap>>> {
    let mut maps = Vec::new();
    for path in csv_files(folder) {
        let file = File::open(&path)?;
        if file.metadata()?.len() == 0 {
            maps.push(None);
            continue;
        }

        maps.push(Some(unsafe { Mmap::map(&file)? }));
    }
    Ok(maps)
}

fn mostly_packable(maps: &[Option<Mmap>]) -> bool {
    let files: Vec<&[u8]> = maps.iter().map(bytes_of).filter(|b| !b.is_empty()).collect();
    let step = files.len().div_ceil(16).max(1);
    let (mut rows, mut unpackable) = (0usize, 0usize);
    for data in files.iter().step_by(step) {
        let Some(first) = memchr::memchr(b'\n', data) else { continue };
        let end = data.len().min(first + 1 + (256 << 10));
        let mut seen = 0;
        for_each_record::<7>(data, first + 1, end, |fields, _| {
            if seen < 2048 {
                seen += 1;
                rows += 1;
                let packs = |field: Option<&[u8]>| field.is_none_or(|v| compact::pack62(v).is_some());
                if !packs(fields[0]) || !packs(fields[1]) {
                    unpackable += 1;
                }
            }
        });
    }
    unpackable * 50 <= rows
}

fn many_long_users(maps: &[Option<Mmap>]) -> bool {
    let files: Vec<&[u8]> = maps.iter().map(bytes_of).filter(|b| !b.is_empty()).collect();
    let step = files.len().div_ceil(16).max(1);
    let (mut rows, mut long) = (0usize, 0usize);
    for data in files.iter().step_by(step) {
        let Some(first) = memchr::memchr(b'\n', data) else { continue };
        let end = data.len().min(first + 1 + (256 << 10));
        let mut seen = 0;
        for_each_record::<7>(data, first + 1, end, |fields, _| {
            if seen < 2048 {
                seen += 1;
                rows += 1;
                if fields[1].is_some_and(|user| user.len() > 15) {
                    long += 1;
                }
            }
        });
    }
    long * 50 > rows
}

fn bytes_of(map: &Option<Mmap>) -> &[u8] {
    map.as_deref().unwrap_or(&[])
}

#[derive(Clone, Copy, PartialEq)]
enum Kind {
    Event,
    Click,
}

#[derive(Clone, Copy)]
struct Chunk {
    kind: Kind,
    file: usize,
    start: usize,
    end: usize,
}

fn chunk_bytes() -> usize {
    std::env::var("PIPELINE_CHUNK_BYTES").ok().and_then(|v| v.parse().ok()).filter(|&n| n > 0).unwrap_or(CHUNK_BYTES)
}

fn chunks_of(kind: Kind, maps: &[Option<Mmap>], out: &mut Vec<Chunk>) {

    let chunk_bytes = match kind {
        Kind::Click if std::env::var_os("PIPELINE_CHUNK_BYTES").is_none() => CLICK_CHUNK_BYTES,
        _ => chunk_bytes(),
    };
    for (file, map) in maps.iter().enumerate() {
        let len = bytes_of(map).len();
        let mut start = 0;
        while start < len {
            let end = (start + chunk_bytes).min(len);
            out.push(Chunk { kind, file, start, end });
            start = end;
        }
    }
}

fn load_lookup(path: &Path) -> Vec<(Vec<u8>, Vec<u8>)> {
    let Ok(bytes) = std::fs::read(path) else { return Vec::new() };
    let mut seen = FxHashSet::default();
    let mut rows = Vec::new();
    let mut fields = [None; 2];
    for line in data_lines(&bytes) {
        if parse_line(line, &mut fields) {
            if let Some(key) = fields[0] {
                if seen.insert(key.to_vec()) {
                    rows.push((key.to_vec(), fields[1].unwrap_or_default().to_vec()));
                }
            }
        }
    }
    rows
}

fn load_products(path: &Path) -> (FxHashMap<Key, u32>, Vec<Vec<u8>>) {
    let Ok(bytes) = std::fs::read(path) else { return (FxHashMap::default(), Vec::new()) };
    let mut products: FxHashMap<Key, u32> = FxHashMap::default();
    products.reserve(bytes.len() / 16);
    let mut category_names: Vec<Vec<u8>> = Vec::new();
    let mut category_index: FxHashMap<&[u8], u32> = FxHashMap::default();
    let mut fields = [None; 2];
    for line in data_lines(&bytes) {
        if !parse_line(line, &mut fields) {
            continue;
        }
        let Some(product) = fields[0] else { continue };
        let std::collections::hash_map::Entry::Vacant(slot) = products.entry(key_of(product, None)) else { continue };
        let category = fields[1].unwrap_or_default();
        let next = category_names.len() as u32;
        let index = *category_index.entry(category).or_insert(next);
        if index == next {
            category_names.push(category.to_vec());
        }
        slot.insert(index);
    }
    (products, category_names)
}

fn load_window(data: &Path) -> Result<(i64, i64)> {
    let manifest: serde_json::Value = serde_json::from_slice(&std::fs::read(data.join("manifest.json"))?)?;
    let get = |key: &str| -> Result<i64> {
        let text = manifest[key].as_str().ok_or_else(|| format!("manifest has no {key}"))?;
        ts::parse(text.as_bytes()).ok_or_else(|| format!("manifest {key} is not a timestamp: {text}").into())
    };
    Ok((get("window_start")?, get("window_end")?))
}

fn resolve_people(maps: &[Option<Mmap>], threads: usize, longs: &LongIds) -> People {
    use std::sync::atomic::AtomicU32;
    const BUCKET_BITS: u32 = 8;
    const BUCKETS: usize = 1 << BUCKET_BITS;
    const SPAN: usize = 1 << 16;

    let chunk = chunk_bytes().min(1 << 20);
    let mut ranges = Vec::new();
    for (file, map) in maps.iter().enumerate() {
        let len = bytes_of(map).len();
        let mut start = 0;
        while start < len {
            let end = (start + chunk).min(len);
            ranges.push((file, start, end));
            start = end;
        }
    }
    let mut pieces: Vec<Vec<(Key, Key)>> = vec![Vec::new(); ranges.len()];
    for (at, pairs) in run_tasks(threads, ranges.len(), Vec::new, |out: &mut Vec<(usize, Vec<(Key, Key)>)>, at| {
        let (file, start, end) = ranges[at];
        let mut pairs = Vec::new();
        for_each_record::<2>(bytes_of(&maps[file]), start, end, |fields, _| {

            if let (Some(a), Some(b)) = (fields[0], fields[1]) {
                pairs.push((key_of_deferred(a, longs), key_of_deferred(b, longs)));
            }
        });
        longs.close_unit();
        out.push((at, pairs));
    })
    .into_iter()
    .flatten()
    {
        pieces[at] = pairs;
    }

    longs.build(threads);
    let mut first_pair = Vec::with_capacity(pieces.len() + 1);
    let mut pair_count = 0usize;
    for piece in &pieces {
        first_pair.push(pair_count);
        pair_count += piece.len();
    }
    if pair_count == 0 {
        return People::new(std::iter::empty());
    }
    assert!(pair_count < (1 << 31), "over 2^31 links");

    let bucket_of = |key: Key| (mix(key) >> (64 - BUCKET_BITS)) as usize;

    type Piece = (Vec<(Key, u32)>, Vec<u32>);
    let mut by_piece: Vec<Piece> = vec![(Vec::new(), Vec::new()); pieces.len()];
    for (at, piece) in run_tasks(threads, pieces.len(), Vec::new, |out: &mut Vec<(usize, Piece)>, at| {
        let pairs = &pieces[at];
        let mut ids: Vec<u8> = Vec::with_capacity(pairs.len() * 2);
        let mut starts = vec![0u32; BUCKETS + 1];
        for &(a, b) in pairs.iter() {
            let (ba, bb) = (bucket_of(a), bucket_of(b));
            ids.push(ba as u8);
            ids.push(bb as u8);
            starts[ba + 1] += 1;
            starts[bb + 1] += 1;
        }
        for b in 0..BUCKETS {
            starts[b + 1] += starts[b];
        }
        let mut cursor = starts.clone();
        let mut flat: Vec<(Key, u32)> = vec![(0, 0); pairs.len() * 2];
        for (i, &(a, b)) in pairs.iter().enumerate() {
            let end = ((first_pair[at] + i) * 2) as u32;
            for (side, key) in [(0u32, a), (1, b)] {
                let bucket = ids[2 * i + side as usize] as usize;
                flat[cursor[bucket] as usize] = (key, end + side);
                cursor[bucket] += 1;
            }
        }
        out.push((at, (flat, starts)));
    })
    .into_iter()
    .flatten()
    {
        by_piece[at] = piece;
    }
    drop(pieces);
    let mut bucket_keys: Vec<Vec<Key>> = vec![Vec::new(); BUCKETS];
    let mut bucket_ends: Vec<Vec<(u32, u32)>> = vec![Vec::new(); BUCKETS];
    for (b, keys, ends) in run_tasks(threads, BUCKETS, Vec::new, |out: &mut Vec<(usize, Vec<Key>, Vec<(u32, u32)>)>, b| {
        let bucket = |piece: &Piece| -> std::ops::Range<usize> { piece.1.get(b).map_or(0..0, |&s| s as usize..piece.1[b + 1] as usize) };
        let entries: usize = by_piece.iter().map(|piece| bucket(piece).len()).sum();
        let mask = (entries * 2).next_power_of_two().max(16) - 1;
        let mut table = vec![u32::MAX; mask + 1];
        let mut keys: Vec<Key> = Vec::new();
        let mut ends: Vec<(u32, u32)> = Vec::with_capacity(entries);
        for piece in &by_piece {
            for &(key, end) in &piece.0[bucket(piece)] {
                let mut slot = mix(key) as usize & mask;
                let local = loop {
                    match table[slot] {
                        u32::MAX => {
                            table[slot] = keys.len() as u32;
                            keys.push(key);
                            break keys.len() as u32 - 1;
                        }
                        local if keys[local as usize] == key => break local,
                        _ => slot = (slot + 1) & mask,
                    }
                };
                ends.push((end, local));
            }
        }
        out.push((b, keys, ends));
    })
    .into_iter()
    .flatten()
    {
        bucket_keys[b] = keys;
        bucket_ends[b] = ends;
    }
    drop(by_piece);
    let mut bucket_base = Vec::with_capacity(BUCKETS);
    let mut nodes = 0usize;
    for keys in &bucket_keys {
        bucket_base.push(nodes as u32);
        nodes += keys.len();
    }
    let node_keys: Vec<Key> = bucket_keys.into_iter().flatten().collect();
    let ends: Vec<AtomicU32> = (0..pair_count * 2).map(|_| AtomicU32::new(0)).collect();
    run_tasks(threads, BUCKETS, || (), |_, b| {
        for &(end, local) in &bucket_ends[b] {
            ends[end as usize].store(bucket_base[b] + local, Ordering::Relaxed);
        }
    });
    drop(bucket_ends);

    let parent: Vec<AtomicU32> = (0..nodes as u32).map(AtomicU32::new).collect();
    fn find(parent: &[AtomicU32], mut node: u32) -> u32 {
        loop {
            let up = parent[node as usize].load(Ordering::Acquire);
            if up == node {
                return node;
            }
            let grand = parent[up as usize].load(Ordering::Acquire);
            if grand != up {
                let _ = parent[node as usize].compare_exchange_weak(up, grand, Ordering::AcqRel, Ordering::Relaxed);
            }
            node = grand;
        }
    }
    run_tasks(threads, pair_count.div_ceil(SPAN), || (), |_, at| {
        for pair in at * SPAN..((at + 1) * SPAN).min(pair_count) {
            let (mut a, mut b) = (ends[pair * 2].load(Ordering::Relaxed), ends[pair * 2 + 1].load(Ordering::Relaxed));
            loop {
                a = find(&parent, a);
                b = find(&parent, b);
                if a == b {
                    break;
                }
                let (low, high) = if a < b { (a, b) } else { (b, a) };
                if parent[high as usize].compare_exchange(high, low, Ordering::AcqRel, Ordering::Acquire).is_ok() {
                    break;
                }
            }
        }
    });
    drop(ends);

    let best: Vec<AtomicU32> = (0..nodes).map(|_| AtomicU32::new(u32::MAX)).collect();
    let roots: Vec<AtomicU32> = (0..nodes).map(|_| AtomicU32::new(0)).collect();
    run_tasks(threads, nodes.div_ceil(SPAN), || (), |_, at| {
        for node in at * SPAN..((at + 1) * SPAN).min(nodes) {
            let root = find(&parent, node as u32);
            roots[node].store(root, Ordering::Relaxed);
            let slot = &best[root as usize];
            let mut held = slot.load(Ordering::Acquire);
            loop {
                if held != u32::MAX && key::cmp(node_keys[held as usize], node_keys[node], longs).is_le() {
                    break;
                }
                match slot.compare_exchange_weak(held, node as u32, Ordering::AcqRel, Ordering::Acquire) {
                    Ok(_) => break,
                    Err(now) => held = now,
                }
            }
        }
    });

    let pieces: Vec<Vec<(Key, Key)>> = run_tasks(threads, nodes.div_ceil(SPAN), Vec::new, |out: &mut Vec<(usize, Vec<(Key, Key)>)>, at| {
        let range = at * SPAN..((at + 1) * SPAN).min(nodes);
        let named = range
            .map(|node| {
                let root = roots[node].load(Ordering::Relaxed) as usize;
                (node_keys[node], node_keys[best[root].load(Ordering::Relaxed) as usize])
            })
            .collect();
        out.push((at, named));
    })
    .into_iter()
    .flatten()
    .map(|(_, named)| named)
    .collect();
    People::build(&pieces, threads)
}

#[derive(Clone, Copy)]
struct EventCopy {
    id: Key,
    user: Key,
    seq: u64,
    ingest: i64,
    ts: i64,
    price: u32,
    category: u32,
}

const INVALID: u32 = u32::MAX;

#[derive(Clone, Copy)]
pub struct Event {
    pub person: Key,
    pub ts: i64,
    pub price: u32,
    pub category: u32,
}

#[derive(Clone, Copy)]
pub struct Click {
    pub person: Key,
    pub click_id: Key,
    pub ts: i64,
    pub campaign: u32,
}

struct Context {
    window: (i64, i64),
    products: FxHashMap<Key, u32>,
    unknown_category: u32,
    campaigns: FxHashMap<Key, u32>,
    longs: LongIds,

    compact: bool,

    name_from_lines: bool,
}

#[inline(always)]
fn judge<'a>(context: &Context, fields: &[Option<&'a [u8]>; 7], ingest: i64, dates: &mut ts::DateCache) -> Option<(&'a [u8], i64, u32, u32)> {
    let [_, user, ts, _, kind, product, price] = *fields;
    let (user, product, kind) = (user?, product?, kind?);
    let purchase = match kind {
        b"purchase" => true,
        b"view" | b"cart" => false,
        _ => return None,
    };
    let ts = ts::parse_cached(ts?, dates)?;
    if ts < context.window.0 || ts >= context.window.1 {
        return None;
    }
    let price = if purchase {
        let price = price.filter(|p| ascii_digits(p))?;
        let value = price.iter().try_fold(0u64, |acc, &b| {
            let next = acc * 10 + u64::from(b - b'0');
            (next <= PRICE_MAX).then_some(next)
        })?;
        if value < 1 {
            return None;
        }
        value as u32
    } else {
        0
    };
    if ingest == NEVER || i128::from(ingest) - i128::from(ts) > LATE_MS {
        return None;
    }

    let category = if purchase {
        *context.products.get(&key_of(product, None)).unwrap_or(&context.unknown_category)
    } else {
        0
    };
    Some((user, ts, price, category))
}

#[inline]
fn dedup_partition(id: Key) -> usize {
    (mix(id) as usize) & (dedup_partitions() - 1)
}

#[inline]
fn person_partition(person: Key) -> usize {
    (mix(person) as usize) & (person_partitions() - 1)
}

struct ParseState {
    events: Scatter<EventCopy>,
    events32: BlockScatter<EventCopy32>,
    slow_users: Vec<Key>,
    thread: u64,
    clicks: BlockScatter<Click>,
    early_clicks: Vec<Click>,
    dates: ts::DateCache,
}

fn populate_chunk(data: &[u8], start: usize, end: usize) {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    if !*ON.get_or_init(|| std::env::var("POPULATE").as_deref() == Ok("1")) || data.is_empty() {
        return;
    }
    const MADV_POPULATE_READ: libc::c_int = 22;
    let base = data.as_ptr() as usize;
    let from = (base + start) & !4095;

    let to = base + (end + 4096).min(data.len());
    if to > from {

        unsafe { libc::madvise(from as *mut libc::c_void, to - from, MADV_POPULATE_READ) };
    }
}

fn kernel_at_least(major: u32, minor: u32) -> bool {
    let release = std::fs::read_to_string("/proc/sys/kernel/osrelease").unwrap_or_default();
    let mut parts = release.trim().split(|c: char| !c.is_ascii_digit()).filter(|s| !s.is_empty()).map(|s| s.parse::<u32>().ok());
    match (parts.next().flatten(), parts.next().flatten()) {
        (Some(a), Some(b)) => (a, b) >= (major, minor),
        _ => false,
    }
}

fn parse_chunk(
    context: &Context,
    people: Option<&People>,
    event_maps: &[Option<Mmap>],
    click_maps: &[Option<Mmap>],
    chunk: Chunk,
    state: &mut ParseState,
) {
    let (start_ms, end_ms) = (context.window.0.saturating_sub(CLICK_REACH_MS), context.window.1);
    populate_chunk(bytes_of(if chunk.kind == Kind::Event { &event_maps[chunk.file] } else { &click_maps[chunk.file] }), chunk.start, chunk.end);
    match chunk.kind {
        Kind::Event => {
            let file_seq = (chunk.file as u64) << 40;

            let window_start = context.window.0;
            for_each_record::<7>(bytes_of(&event_maps[chunk.file]), chunk.start, chunk.end, #[inline(always)] |fields, offset| {
                let Some(event_id) = fields[0] else { return };
                let ingest = fields[3].and_then(|v| ts::parse_ingest(v, &mut state.dates)).unwrap_or(NEVER);
                if context.compact {
                    if let Some(id) = compact::pack62(event_id) {
                        let copy = match judge(context, fields, ingest, &mut state.dates) {
                            Some((user, ts, price, category)) => {
                                let user = compact::pack62(user).unwrap_or_else(|| {
                                    state.slow_users.push(key_of_deferred(user, &context.longs));
                                    compact::SLOW_USER | state.thread << 40 | (state.slow_users.len() as u64 - 1)
                                });
                                EventCopy32 { id, user, ingest, ts: (ts - window_start) as u32, price_cat: price << 8 | category }
                            }
                            None => EventCopy32 { id, user: 0, ingest, ts: 0, price_cat: compact::INVALID32 },
                        };
                        state.events32.push((compact::mix64(id) as usize) & (dedup_partitions() - 1), copy);
                        return;
                    }
                }
                let id = key_of(event_id, None);
                let seq = file_seq | offset as u64;
                let copy = match judge(context, fields, ingest, &mut state.dates) {
                    Some((user, ts, price, category)) => {

                        let user = if context.name_from_lines { key_of(user, None) } else { key_of_deferred(user, &context.longs) };
                        EventCopy { id, user, seq, ingest, ts, price, category }
                    }
                    None => EventCopy { id, user: 0, seq, ingest, ts: 0, price: INVALID, category: 0 },
                };
                state.events.push(dedup_partition(id), copy);
            });
            state.events32.flush();
            crate::scatter::fence();
        }
        Kind::Click if people.is_none() => {

            for_each_record::<4>(bytes_of(&click_maps[chunk.file]), chunk.start, chunk.end, |fields, _| {
                let [Some(click_id), Some(user), Some(campaign), Some(ts)] = *fields else { return };
                let Some(&campaign) = context.campaigns.get(&key_of(campaign, None)) else { return };
                let Some(ts) = ts::parse_cached(ts, &mut state.dates) else { return };
                if ts < start_ms || ts >= end_ms {
                    return;
                }
                state.early_clicks.push(Click { person: key_of(user, None), click_id: key::click_key(click_id), ts, campaign });
            });
        }
        Kind::Click => {
            let people = people.expect("clicks are parsed after links");
            let mut pending: Vec<(Key, Click)> = Vec::with_capacity(CLICK_BATCH);
            for_each_record::<4>(bytes_of(&click_maps[chunk.file]), chunk.start, chunk.end, |fields, _| {

                let [Some(click_id), Some(user), Some(campaign), Some(ts)] = *fields else { return };
                let Some(&campaign) = context.campaigns.get(&key_of(campaign, None)) else { return };
                let Some(ts) = ts::parse_cached(ts, &mut state.dates) else { return };
                if ts < start_ms || ts >= end_ms {
                    return;
                }

                let user = key_of(user, None);
                people.prefetch(user);
                pending.push((user, Click { person: 0, click_id: key::click_key(click_id), ts, campaign }));
                if pending.len() == CLICK_BATCH {
                    flush_clicks(people, &mut pending, &mut state.clicks);
                }
            });
            flush_clicks(people, &mut pending, &mut state.clicks);
        }
    }

    context.longs.flush();
    context.longs.close_unit();
}

fn resolve_clicks(people: &People, clicks: &[Click], out: &mut BlockScatter<Click>) {
    const AHEAD: usize = 16;
    for click in clicks.iter().take(AHEAD) {
        people.prefetch(click.person);
    }
    for (i, click) in clicks.iter().enumerate() {
        if let Some(next) = clicks.get(i + AHEAD) {
            people.prefetch(next.person);
        }
        let mut click = *click;
        click.person = people.get(click.person);
        out.push(person_partition(click.person), click);
    }
}

const CLICK_BATCH: usize = 64;

#[inline]
fn flush_clicks(people: &People, pending: &mut Vec<(Key, Click)>, out: &mut BlockScatter<Click>) {
    for (user, mut click) in pending.drain(..) {
        click.person = people.get(user);
        out.push(person_partition(click.person), click);
    }
}

type NameRef = ([u64; 2], u64);

fn dedup(pieces: Vec<&[EventCopy]>, people: &People, scratch: &mut DedupScratch, out: &mut BlockScatter<Event>, mut seqs: Option<&mut BlockScatter<NameRef>>) {
    let total: usize = pieces.iter().map(|piece| piece.len()).sum();
    assert!(total < u32::MAX as usize, "dedup partition over 4G copies");
    let starts: Vec<usize> = pieces.iter().scan(0, |at, piece| { let start = *at; *at += piece.len(); Some(start) }).collect();
    let copy_at = |index: u32| {
        let index = index as usize;
        let piece = starts.partition_point(|&start| start <= index) - 1;
        &pieces[piece][index - starts[piece]]
    };
    const EMPTY: u64 = u64::MAX;
    let slots = (total * 2).next_power_of_two().max(16);
    let mask = slots - 1;
    let DedupScratch { table, winner, survivors, .. } = scratch;
    table.clear();
    table.resize(slots, EMPTY);
    winner.clear();
    winner.resize(total, false);
    let mut index: u32 = 0;
    for copy in pieces.iter().copied().flatten() {
        let h = mix(copy.id);
        let tag = h >> 32;

        let mut slot = (h >> 20) as usize & mask;
        loop {
            let held = table[slot];
            if held == EMPTY {
                table[slot] = (tag << 32) | u64::from(index);
                winner[index as usize] = true;
                break;
            }
            if held >> 32 == tag {
                let other_index = held as u32;
                let other = copy_at(other_index);
                if other.id == copy.id {
                    if (copy.ingest, copy.seq) < (other.ingest, other.seq) {
                        table[slot] = (tag << 32) | u64::from(index);
                        winner[other_index as usize] = false;
                        winner[index as usize] = true;
                    }
                    break;
                }
            }
            slot = (slot + 1) & mask;
        }
        index += 1;
    }

    survivors.clear();
    for (copy, &won) in pieces.iter().copied().flatten().zip(winner.iter()) {
        if won && copy.price != INVALID {
            survivors.push(*copy);
        }
    }
    const AHEAD: usize = 16;
    const NAME_FILTER: usize = 256;
    let mut recent = [0 as Key; NAME_FILTER];
    for copy in survivors.iter().take(AHEAD) {
        people.prefetch(copy.user);
    }
    for at in 0..survivors.len() {
        if let Some(ahead) = survivors.get(at + AHEAD) {
            people.prefetch(ahead.user);
        }
        let copy = &survivors[at];
        let person = people.get(copy.user);
        out.push(person_partition(person), Event { person, ts: copy.ts, price: copy.price, category: copy.category });
        if let Some(seqs) = seqs.as_deref_mut() {

            if person == copy.user && crate::key::is_long(person) {
                let slot = ((person >> 8) as usize) & (NAME_FILTER - 1);
                if recent[slot] != person {
                    recent[slot] = person;
                    seqs.push(person_partition(person), ([person as u64, (person >> 64) as u64], copy.seq));
                }
            }
        }
    }
}

#[derive(Default)]
struct DedupScratch {
    table: Vec<u64>,
    winner: Vec<bool>,
    survivors: Vec<EventCopy>,
    survivors32: Vec<(Key, u32, u32)>,
}

static NAME_MISSING: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

static TIE_CONFLICT: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

fn dedup32(pieces: Vec<&[EventCopy32]>, people: &People, slow_users: &[Vec<Key>], window_start: i64, scratch: &mut DedupScratch, out: &mut BlockScatter<Event>) {
    let total: usize = pieces.iter().map(|piece| piece.len()).sum();
    assert!(total < u32::MAX as usize, "dedup partition over 4G copies");
    let starts: Vec<usize> = pieces.iter().scan(0, |at, piece| { let start = *at; *at += piece.len(); Some(start) }).collect();
    let copy_at = |index: u32| {
        let index = index as usize;
        let piece = starts.partition_point(|&start| start <= index) - 1;
        &pieces[piece][index - starts[piece]]
    };
    const EMPTY: u64 = u64::MAX;
    let slots = (total * 2).next_power_of_two().max(16);
    let mask = slots - 1;
    let DedupScratch { table, winner, survivors32, .. } = scratch;
    table.clear();
    table.resize(slots, EMPTY);
    winner.clear();
    winner.resize(total, false);

    let resolve = |user: u64| {
        if user & compact::SLOW_USER != 0 {
            slow_users[((user >> 40) & 0x7F_FFFF) as usize][(user & 0xFF_FFFF_FFFF) as usize]
        } else {
            compact::unpack62_key(user)
        }
    };
    let same_user = |a: u64, b: u64| a == b || ((a | b) & compact::SLOW_USER != 0 && resolve(a) == resolve(b));
    let mut index: u32 = 0;
    for copy in pieces.iter().copied().flatten() {
        let h = compact::mix64(copy.id);
        let tag = h >> 32;

        let mut slot = (h >> 20) as usize & mask;
        loop {
            let held = table[slot];
            if held == EMPTY {
                table[slot] = (tag << 32) | u64::from(index);
                winner[index as usize] = true;
                break;
            }
            if held >> 32 == tag {
                let other_index = held as u32;
                let other = copy_at(other_index);
                if other.id == copy.id {
                    if copy.ingest < other.ingest {
                        table[slot] = (tag << 32) | u64::from(index);
                        winner[other_index as usize] = false;
                        winner[index as usize] = true;
                    } else if copy.ingest == other.ingest
                        && (copy.ts != other.ts || copy.price_cat != other.price_cat || !same_user(copy.user, other.user))
                    {

                        TIE_CONFLICT.store(true, Ordering::Relaxed);
                    }
                    break;
                }
            }
            slot = (slot + 1) & mask;
        }
        index += 1;
    }
    survivors32.clear();
    for (copy, &won) in pieces.iter().copied().flatten().zip(winner.iter()) {
        if won && copy.price_cat != compact::INVALID32 {
            let user = resolve(copy.user);
            survivors32.push((user, copy.ts, copy.price_cat));
        }
    }
    const AHEAD: usize = 16;
    for &(user, _, _) in survivors32.iter().take(AHEAD) {
        people.prefetch(user);
    }
    for at in 0..survivors32.len() {
        if let Some(&(ahead, _, _)) = survivors32.get(at + AHEAD) {
            people.prefetch(ahead);
        }
        let (user, ts, price_cat) = survivors32[at];
        let person = people.get(user);
        out.push(
            person_partition(person),
            Event { person, ts: window_start + i64::from(ts), price: price_cat >> 8, category: price_cat & 0xFF },
        );
    }
}

trait TsOffset: Copy + Ord + Default {
    fn from_rel(rel: i64) -> Self;
    fn rel(self) -> i64;

    fn scratch(state: &mut PeopleState) -> &mut ThreadScratch<Self>;
}

impl TsOffset for u32 {
    #[inline(always)]
    fn from_rel(rel: i64) -> Self {
        rel as u32
    }
    #[inline(always)]
    fn rel(self) -> i64 {
        i64::from(self)
    }
    fn scratch(state: &mut PeopleState) -> &mut ThreadScratch<Self> {
        &mut state.scratch
    }
}

impl TsOffset for i64 {
    #[inline(always)]
    fn from_rel(rel: i64) -> Self {
        rel
    }
    #[inline(always)]
    fn rel(self) -> i64 {
        self
    }
    fn scratch(state: &mut PeopleState) -> &mut ThreadScratch<Self> {
        &mut state.scratch_wide
    }
}

#[derive(Clone, Copy, Default)]
struct Purchase<T> {
    pub ts: T,
    pub price: u32,
    pub category: u32,
}

#[derive(Clone, Copy)]
pub struct SessionClick {
    pub click_id: Key,
    pub ts: i64,
    pub campaign: u32,
}

pub struct UserRow {
    pub person: Key,
    pub day: i32,
    pub sessions: u64,
    pub events: u64,
    pub revenue: u64,
    pub top_category: Option<u32>,
}

#[derive(Default, Clone, Copy)]
pub struct CampaignCounts {
    pub sessions: u64,
    pub purchasing: u64,
    pub revenue: u64,
    pub assisted: u64,
}

struct CampaignCounters {
    dense: Vec<CampaignCounts>,

    assisted: Vec<u64>,
    first_day: i64,
    days: usize,
    organic: u32,
    sparse: FxHashMap<(u32, i32), CampaignCounts>,
}

impl CampaignCounters {
    fn new(campaigns: usize, window: (i64, i64)) -> Self {
        let first_day = window.0.div_euclid(DAY_MS);
        let days = ((window.1 - 1).div_euclid(DAY_MS) - first_day + 1).max(1) as usize;
        let cells = (campaigns + 1).saturating_mul(days);
        let dense = if cells <= 8 << 20 { vec![CampaignCounts::default(); cells] } else { Vec::new() };
        let assisted = if dense.is_empty() { Vec::new() } else { vec![0; cells] };
        Self { dense, assisted, first_day, days, organic: campaigns as u32, sparse: FxHashMap::default() }
    }

    #[inline(always)]
    fn assist_row(&mut self, day: i32) -> Option<&mut [u64]> {
        let offset = i64::from(day) - self.first_day;
        if self.assisted.is_empty() || !(0..self.days as i64).contains(&offset) {
            return None;
        }
        let width = self.organic as usize + 1;
        Some(&mut self.assisted[offset as usize * width..(offset as usize + 1) * width])
    }

    #[inline(always)]
    fn at(&mut self, campaign: u32, day: i32) -> &mut CampaignCounts {
        let index = if campaign == ORGANIC { self.organic } else { campaign };
        let offset = i64::from(day) - self.first_day;
        if !self.dense.is_empty() && (0..self.days as i64).contains(&offset) {
            &mut self.dense[index as usize * self.days + offset as usize]
        } else {
            self.sparse.entry((campaign, day)).or_default()
        }
    }

    fn merge_into(self, total: &mut FxHashMap<(u32, i32), CampaignCounts>) {
        let add = |total: &mut FxHashMap<(u32, i32), CampaignCounts>, key, c: &CampaignCounts| {
            let row = total.entry(key).or_default();
            row.sessions += c.sessions;
            row.purchasing += c.purchasing;
            row.revenue += c.revenue;
            row.assisted += c.assisted;
        };
        let width = self.organic as usize + 1;
        for (cell, counts) in self.dense.iter().enumerate() {
            let (index, offset) = (cell / self.days, cell % self.days);
            let mut counts = *counts;
            counts.assisted += self.assisted[offset * width + index];
            if counts.sessions == 0 && counts.assisted == 0 {
                continue;
            }
            let index = index as u32;
            let campaign = if index == self.organic { ORGANIC } else { index };
            add(total, (campaign, (self.first_day + offset as i64) as i32), &counts);
        }
        for (key, counts) in &self.sparse {
            add(total, *key, counts);
        }
    }
}

struct PeopleState {
    win_count: Vec<u32>,
    win_pos: Vec<u32>,
    active: Vec<u32>,
    scratch: ThreadScratch<u32>,
    scratch_wide: ThreadScratch<i64>,
    user_rows: Vec<UserRow>,
    spare_categories: Vec<(u32, u64)>,
    campaign_daily: CampaignCounters,
    session_number: u64,
}

struct DailyAcc {
    day: i32,
    sessions: u64,
    events: u64,
    revenue: u64,
    by_category: Vec<(u32, u64)>,
}

fn flush(person: Key, acc: &mut Option<DailyAcc>, rows: &mut Vec<UserRow>, category_names: &[Vec<u8>], spare: &mut Vec<(u32, u64)>) {
    if let Some(mut day) = acc.take() {

        let top_category = day
            .by_category
            .iter()
            .max_by(|a, b| a.1.cmp(&b.1).then_with(|| category_names[b.0 as usize].cmp(&category_names[a.0 as usize])))
            .map(|&(category, _)| category);
        rows.push(UserRow { person, day: day.day, sessions: day.sessions, events: day.events, revenue: day.revenue, top_category });

        day.by_category.clear();
        *spare = std::mem::take(&mut day.by_category);
    }
}

#[allow(clippy::too_many_arguments)]
fn person_sessions<T: TsOffset>(person: Key, ts_rel: &[T], purchases: &[Purchase<T>], window_start: i64, clicks: &[SessionClick], state: &mut PeopleState, category_names: &[Vec<u8>], longs: &LongIds) {
    let mut day_acc: Option<DailyAcc> = None;
    let mut first = 0;
    let (mut low, mut high) = (0usize, 0usize);
    let mut next_purchase = 0usize;
    while first < ts_rel.len() {

        let mut last = first + 1;
        while last < ts_rel.len() && ts_rel[last].rel() - ts_rel[last - 1].rel() <= SESSION_GAP_MS {
            last += 1;
        }
        let start = window_start + ts_rel[first].rel();
        let session_len = last - first;
        let end_ts = ts_rel[last - 1];
        first = last;
        state.session_number += 1;

        let day = start.div_euclid(DAY_MS) as i32;

        let purchase_first = next_purchase;
        while next_purchase < purchases.len() && purchases[next_purchase].ts <= end_ts {
            next_purchase += 1;
        }
        let session_purchases = &purchases[purchase_first..next_purchase];
        let revenue: u64 = session_purchases.iter().map(|purchase| u64::from(purchase.price)).sum();
        let purchased = !session_purchases.is_empty();

        if day_acc.as_ref().is_some_and(|acc| acc.day != day) {
            flush(person, &mut day_acc, &mut state.user_rows, category_names, &mut state.spare_categories);
        }
        let acc = day_acc.get_or_insert_with(|| DailyAcc { day, sessions: 0, events: 0, revenue: 0, by_category: std::mem::take(&mut state.spare_categories) });
        acc.sessions += 1;
        acc.events += session_len as u64;
        acc.revenue += revenue;
        for event in session_purchases {
            match acc.by_category.iter_mut().find(|(c, _)| *c == event.category) {
                Some(entry) => entry.1 += u64::from(event.price),
                None => acc.by_category.push((event.category, u64::from(event.price))),
            }
        }

        while high < clicks.len() && clicks[high].ts <= start {
            let c = clicks[high].campaign as usize;
            if state.win_count[c] == 0 {
                state.win_pos[c] = state.active.len() as u32;
                state.active.push(clicks[high].campaign);
            }
            state.win_count[c] += 1;
            high += 1;
        }
        while low < high && clicks[low].ts < start - CLICK_REACH_MS {
            let c = clicks[low].campaign as usize;
            state.win_count[c] -= 1;
            if state.win_count[c] == 0 {
                let at = state.win_pos[c] as usize;
                let last = state.active.pop().expect("active campaign");
                if at < state.active.len() {
                    state.active[at] = last;
                    state.win_pos[last as usize] = at as u32;
                }
            }
            low += 1;
        }
        let credited = if high > low {

            let latest = clicks[high - 1].ts;
            let mut first_latest = high - 1;
            while first_latest > low && clicks[first_latest - 1].ts == latest {
                first_latest -= 1;
            }

            match state.campaign_daily.assist_row(day) {
                Some(row) => {
                    for &campaign in &state.active {
                        row[campaign as usize] += 1;
                    }
                }
                None => {
                    for &campaign in &state.active {
                        state.campaign_daily.at(campaign, day).assisted += 1;
                    }
                }
            }
            let mut best = first_latest;
            for at in first_latest + 1..high {
                if key::cmp(clicks[at].click_id, clicks[best].click_id, longs) == std::cmp::Ordering::Less {
                    best = at;
                }
            }
            clicks[best].campaign
        } else {
            ORGANIC
        };
        let row = state.campaign_daily.at(credited, day);
        row.sessions += 1;
        row.purchasing += u64::from(purchased);
        row.revenue += revenue;
    }
    flush(person, &mut day_acc, &mut state.user_rows, category_names, &mut state.spare_categories);

    for &campaign in &state.active {
        state.win_count[campaign as usize] = 0;
    }
    state.active.clear();
}

struct Names<'a> {
    seq_pieces: Vec<&'a [NameRef]>,
    event_maps: &'a [Option<Mmap>],
}

impl Names<'_> {

    fn user_at(&self, seq: u64) -> Option<&[u8]> {
        let data = bytes_of(self.event_maps.get((seq >> 40) as usize)?);
        let start = (seq & ((1 << 40) - 1)) as usize;
        let end = memchr::memchr(b'\n', data.get(start..)?).map_or(data.len(), |i| start + i);
        let mut fields: [Option<&[u8]>; 7] = [None; 7];
        if crate::csv::parse_line(&data[start..end], &mut fields) { fields[1] } else { None }
    }
}

fn radix_sort<K: Copy>(v: &mut Vec<K>, tmp: &mut Vec<K>, counts: &mut Vec<u32>, key: impl Fn(&K) -> u64) {
    let n = v.len();
    if n < 96 {
        v.sort_unstable_by_key(|k| key(k));
        return;
    }
    let max = v.iter().map(&key).max().unwrap_or(0);
    let bits = 64 - max.leading_zeros();
    const RADIX: u32 = 11;
    let passes = bits.div_ceil(RADIX);
    tmp.clear();
    tmp.resize(n, v[0]);
    counts.resize(1 << RADIX, 0);
    for pass in 0..passes {
        let shift = pass * RADIX;
        counts.fill(0);
        for k in v.iter() {
            counts[((key(k) >> shift) as usize) & ((1 << RADIX) - 1)] += 1;
        }
        let mut sum = 0u32;
        for c in counts.iter_mut() {
            let here = *c;
            *c = sum;
            sum += here;
        }
        for k in v.iter() {
            let b = ((key(k) >> shift) as usize) & ((1 << RADIX) - 1);
            tmp[counts[b] as usize] = *k;
            counts[b] += 1;
        }
        std::mem::swap(v, tmp);
    }
}

fn people_partition<T: TsOffset>(
    event_pieces: Vec<&[Event]>,
    click_pieces: Vec<&[Click]>,
    longs: &LongIds,
    state: &mut PeopleState,
    category_names: &[Vec<u8>],
    window_start: i64,
    names: &Names,
) {
    let total: usize = event_pieces.iter().map(|piece| piece.len()).sum();
    if total == 0 {
        return;
    }
    let mut scratch = std::mem::take(T::scratch(state));
    let ThreadScratch {
        table, group_keys, event_start, cursor, click_group: _, click_start, grouped_clicks, grouped_ts, purchase_start, grouped_purchases, keyed, keyed_purchases,
        keyed_clicks, tmp_keyed, tmp_purchases, tmp_clicks, counts,
    } = &mut scratch;

    let slots = (total * 2).next_power_of_two().max(16);
    let mask = slots - 1;
    table.clear();
    table.resize(slots, u32::MAX);
    group_keys.clear();
    let find = |table: &[u32], group_keys: &[Key], person: Key| -> (usize, Option<u32>) {
        let mut slot = (mix(person) >> 24) as usize & mask;
        loop {
            match table[slot] {
                u32::MAX => return (slot, None),
                group if group_keys[group as usize] == person => return (slot, Some(group)),
                _ => slot = (slot + 1) & mask,
            }
        }
    };

    keyed.clear();
    keyed_purchases.clear();

    let mut lines: Option<FxHashMap<Key, u64>> = None;

    const PIECES_AHEAD: usize = 2;
    let prefetch_piece = |piece: &[Event]| {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            use std::arch::x86_64::{_MM_HINT_T0, _mm_prefetch};
            let base = piece.as_ptr().cast::<i8>();
            let bytes = std::mem::size_of_val(piece);
            let mut at = 0;
            while at < bytes.min(1024) {
                _mm_prefetch(base.wrapping_add(at), _MM_HINT_T0);
                at += 64;
            }
        }
    };
    for piece in event_pieces.iter().take(PIECES_AHEAD) {
        prefetch_piece(piece);
    }
    for (piece_at, piece) in event_pieces.iter().enumerate() {
    if let Some(ahead) = event_pieces.get(piece_at + PIECES_AHEAD) {
        prefetch_piece(ahead);
    }
    for event in piece.iter() {
        let group = match find(table, group_keys, event.person) {
            (_, Some(group)) => group,
            (slot, None) => {
                table[slot] = group_keys.len() as u32;
                group_keys.push(event.person);

                if !names.seq_pieces.is_empty() && crate::key::is_long(event.person) && longs.slice(event.person).is_empty() {
                    let lines = lines.get_or_insert_with(|| {
                        names.seq_pieces.iter().copied().flatten().map(|&(k, seq)| ((k[0] as Key) | ((k[1] as Key) << 64), seq)).collect()
                    });
                    match lines.get(&event.person).and_then(|&seq| names.user_at(seq)) {
                        Some(id) if key_of(id, None) == event.person => longs.defer_name(event.person, id),
                        _ => NAME_MISSING.store(true, Ordering::Relaxed),
                    }
                }
                (group_keys.len() - 1) as u32
            }
        };
        let ts_rel = T::from_rel(event.ts - window_start);
        keyed.push((group, ts_rel));
        if event.price > 0 {
            keyed_purchases.push((group, Purchase { ts: ts_rel, price: event.price, category: event.category }));
        }
    }
    }
    let groups = group_keys.len();

    radix_sort(keyed, tmp_keyed, counts, |&(_, ts)| ts.rel() as u64);
    radix_sort(keyed_purchases, tmp_purchases, counts, |(_, purchase)| purchase.ts.rel() as u64);

    event_start.clear();
    event_start.resize(groups + 1, 0);
    for &(group, _) in keyed.iter() {
        event_start[group as usize + 1] += 1;
    }
    for g in 0..groups {
        event_start[g + 1] += event_start[g];
    }

    cursor.clear();
    cursor.extend_from_slice(event_start);
    purchase_start.clear();
    purchase_start.resize(groups + 1, 0);
    grouped_ts.clear();
    grouped_ts.reserve(total);
    let spare = grouped_ts.spare_capacity_mut();
    for &(group, ts_rel) in keyed.iter() {
        let at = &mut cursor[group as usize];
        spare[*at as usize].write(ts_rel);
        *at += 1;
    }
    for &(group, _) in keyed_purchases.iter() {
        purchase_start[group as usize + 1] += 1;
    }

    unsafe { grouped_ts.set_len(total) };
    for g in 0..groups {
        purchase_start[g + 1] += purchase_start[g];
    }
    let purchase_total = purchase_start[groups] as usize;
    cursor.clear();
    cursor.extend_from_slice(purchase_start);
    grouped_purchases.clear();
    grouped_purchases.reserve(purchase_total);
    let spare = grouped_purchases.spare_capacity_mut();
    for &(group, purchase) in keyed_purchases.iter() {
        let at = &mut cursor[group as usize];
        spare[*at as usize].write(purchase);
        *at += 1;
    }

    unsafe { grouped_purchases.set_len(purchase_total) };

    keyed_clicks.clear();
    click_start.clear();
    click_start.resize(groups + 1, 0);
    let prefetch_clicks = |piece: &[Click]| {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            use std::arch::x86_64::{_MM_HINT_T0, _mm_prefetch};
            let base = piece.as_ptr().cast::<i8>();
            let bytes = std::mem::size_of_val(piece);
            let mut at = 0;
            while at < bytes.min(1536) {
                _mm_prefetch(base.wrapping_add(at), _MM_HINT_T0);
                at += 64;
            }
        }
    };
    for piece in click_pieces.iter().take(PIECES_AHEAD) {
        prefetch_clicks(piece);
    }
    for (piece_at, piece) in click_pieces.iter().enumerate() {
    if let Some(ahead) = click_pieces.get(piece_at + PIECES_AHEAD) {
        prefetch_clicks(ahead);
    }
    for click in piece.iter() {
        if let Some(group) = find(table, group_keys, click.person).1 {
            keyed_clicks.push((group, SessionClick { click_id: click.click_id, ts: click.ts, campaign: click.campaign }));
            click_start[group as usize + 1] += 1;
        }
    }
    }
    let click_base = window_start - CLICK_REACH_MS;
    radix_sort(keyed_clicks, tmp_clicks, counts, |(_, click)| (click.ts - click_base) as u64);
    for g in 0..groups {
        click_start[g + 1] += click_start[g];
    }
    let kept_clicks = click_start[groups] as usize;
    cursor.clear();
    cursor.extend_from_slice(click_start);
    grouped_clicks.clear();
    grouped_clicks.reserve(kept_clicks);
    let spare = grouped_clicks.spare_capacity_mut();
    for &(group, click) in keyed_clicks.iter() {
        let at = &mut cursor[group as usize];
        spare[*at as usize].write(click);
        *at += 1;
    }

    unsafe { grouped_clicks.set_len(kept_clicks) };
    for g in 0..groups {
        let person_ts = &grouped_ts[event_start[g] as usize..event_start[g + 1] as usize];
        let person_purchases = &grouped_purchases[purchase_start[g] as usize..purchase_start[g + 1] as usize];
        let person_clicks = &grouped_clicks[click_start[g] as usize..click_start[g + 1] as usize];
        person_sessions(group_keys[g], person_ts, person_purchases, window_start, person_clicks, state, category_names, longs);
    }
    *T::scratch(state) = scratch;
}

#[derive(Default)]
struct ThreadScratch<T> {
    table: Vec<u32>,
    group_keys: Vec<Key>,
    event_start: Vec<u32>,
    cursor: Vec<u32>,
    grouped_ts: Vec<T>,
    purchase_start: Vec<u32>,
    grouped_purchases: Vec<Purchase<T>>,
    keyed: Vec<(u32, T)>,
    keyed_purchases: Vec<(u32, Purchase<T>)>,
    click_group: Vec<u32>,
    click_start: Vec<u32>,
    grouped_clicks: Vec<SessionClick>,
    keyed_clicks: Vec<(u32, SessionClick)>,
    tmp_keyed: Vec<(u32, T)>,
    tmp_purchases: Vec<(u32, Purchase<T>)>,
    tmp_clicks: Vec<(u32, SessionClick)>,
    counts: Vec<u32>,
}

fn run() -> Result<()> {
    let mut timer = Timer::new();
    let args: Vec<String> = std::env::args().collect();
    let data = PathBuf::from(std::env::var("DATA_DIR").ok().or_else(|| args.get(1).cloned()).unwrap_or("/data".into()));
    let out = PathBuf::from(std::env::var("OUTPUT_DIR").ok().or_else(|| args.get(2).cloned()).unwrap_or("/output".into()));
    let threads = std::env::var("NCPU")
        .ok()
        .and_then(|n| n.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or_else(|| std::thread::available_parallelism().map_or(8, usize::from));

    let data_ref = &data;
    let (window, (products, category_names), (campaign_rows, campaigns), event_maps, click_maps, link_maps) =
        std::thread::scope(|scope| -> Result<_> {
            let products = scope.spawn(move || load_products(&data_ref.join("products.csv")));
            let campaigns = scope.spawn(move || {
                let campaign_rows = load_lookup(&data_ref.join("campaigns.csv"));
                let campaigns: FxHashMap<Key, u32> =
                    campaign_rows.iter().enumerate().map(|(index, (id, _))| (key_of(id, None), index as u32)).collect();
                (campaign_rows, campaigns)
            });
            let map = |folder: &'static str| scope.spawn(move || map_folder(&data_ref.join(folder)).map_err(|e| e.to_string()));
            let (events, clicks, links) = (map("events"), map("clicks"), map("links"));
            let window = load_window(data_ref)?;
            let join_maps = |h: std::thread::ScopedJoinHandle<'_, std::result::Result<Vec<Option<Mmap>>, String>>| -> Result<Vec<Option<Mmap>>> {
                Ok(h.join().expect("mmap thread panicked")?)
            };
            Ok((
                window,
                products.join().expect("products thread panicked"),
                campaigns.join().expect("campaigns thread panicked"),
                join_maps(events)?,
                join_maps(clicks)?,
                join_maps(links)?,
            ))
        })?;
    let mut category_names = category_names;

    let unknown_category = category_names.len() as u32;
    category_names.push(b"unknown".to_vec());
    timer.mark("lookups + mmap");

    let compact = category_names.len() <= 256
        && window.1.saturating_sub(window.0) < (1i64 << 32)
        && std::env::var_os("PIPE_LEGACY").is_none()
        && mostly_packable(&event_maps);
    let name_from_lines = !compact
        && match std::env::var("NAME_FROM_LINES").as_deref() {
            Ok("0") => false,
            Ok("1") => true,
            _ => many_long_users(&event_maps),
        };
    let context = Context { window, products, unknown_category, campaigns, longs: LongIds::default(), compact, name_from_lines };

    let mut event_chunks = Vec::new();
    chunks_of(Kind::Event, &event_maps, &mut event_chunks);
    event_chunks.sort_by_key(|chunk| std::cmp::Reverse(chunk.end - chunk.start));
    let people_slot: Mutex<Option<People>> = Mutex::new(None);

    let total_bytes = |maps: &[Option<Mmap>]| maps.iter().map(|m| bytes_of(m).len()).sum::<usize>();
    let per_buffer = |bytes: usize, line: usize, parts: usize| bytes / line * 115 / 100 / threads / parts;
    set_partitions(total_bytes(&event_maps) / 77);
    let (dedup_parts, person_parts) = (dedup_partitions(), person_partitions());
    let event_cap = per_buffer(total_bytes(&event_maps), 77, dedup_parts);

    blocks::reserve(2 * (total_bytes(&event_maps) + total_bytes(&click_maps)) + (256 << 20));
    let next_thread = AtomicUsize::new(0);
    let early_reserve = std::env::var("EARLY_CLICKS").as_deref() != Ok("0");
    let new_events = || ParseState {
        events: Scatter::new(dedup_parts, if compact { 0 } else { event_cap }),
        events32: BlockScatter::new_paired(dedup_parts),
        slow_users: Vec::new(),
        thread: next_thread.fetch_add(1, Ordering::Relaxed) as u64,
        clicks: BlockScatter::new(0),

        early_clicks: Vec::with_capacity(if early_reserve { total_bytes(&click_maps) / 47 * 2 / threads } else { 0 }),
        dates: ts::DateCache::default(),
    };
    let new_clicks = || ParseState {
        events: Scatter::new(dedup_parts, 0),
        events32: BlockScatter::new_paired(0),
        slow_users: Vec::new(),
        thread: u64::MAX,
        clicks: BlockScatter::new(person_parts),
        early_clicks: Vec::new(),
        dates: ts::DateCache::default(),
    };

    let early = std::env::var("EARLY_CLICKS").as_deref() != Ok("0");
    let mut click_chunks = Vec::new();
    chunks_of(Kind::Click, &click_maps, &mut click_chunks);
    click_chunks.sort_by_key(|chunk| std::cmp::Reverse(chunk.end - chunk.start));
    let early_count = if early { click_chunks.len() } else { 0 };
    let parsed = run_tasks(threads, event_chunks.len() + 1 + early_count, new_events, |state, at| {
        if at == 0 {

            let link_threads = std::env::var("LINK_THREADS").ok().and_then(|v| v.parse::<usize>().ok()).filter(|&n| n > 0).unwrap_or(3);
            *people_slot.lock().unwrap() = Some(resolve_people(&link_maps, link_threads, &context.longs));
        } else if at <= event_chunks.len() {
            parse_chunk(&context, None, &event_maps, &click_maps, event_chunks[at - 1], state);
        } else {
            parse_chunk(&context, None, &event_maps, &click_maps, click_chunks[at - 1 - event_chunks.len()], state);
        }
    });
    let people = people_slot.into_inner().unwrap().expect("links resolved");
    timer.mark("links || parse events");
    let clicked = run_tasks(threads, if early { 0 } else { click_chunks.len() }, new_clicks, |state, at| {
        parse_chunk(&context, Some(&people), &event_maps, &click_maps, click_chunks[at], state)
    });
    let mut slow_users: Vec<Vec<Key>> = (0..threads).map(|_| Vec::new()).collect();
    let mut event_parts: Vec<Scatter<EventCopy>> = Vec::new();
    let mut event32_parts: Vec<BlockScatter<EventCopy32>> = Vec::new();
    let mut early_clicks: Vec<Vec<Click>> = Vec::new();
    for state in parsed {
        early_clicks.push(state.early_clicks);
        slow_users[state.thread as usize] = state.slow_users;
        event_parts.push(state.events);
        event32_parts.push(state.events32);
    }
    let mut click_parts: Vec<BlockScatter<Click>> = clicked.into_iter().map(|s| s.clicks).collect();
    const RESOLVE_TASK: usize = 1 << 15;
    let mut click_tasks: Vec<(usize, usize, usize)> = Vec::new();
    for (t, clicks) in early_clicks.iter().enumerate() {
        let mut start = 0;
        while start < clicks.len() {
            click_tasks.push((t, start, (start + RESOLVE_TASK).min(clicks.len())));
            start += RESOLVE_TASK;
        }
    }

    context.longs.build(threads);
    timer.mark("parse clicks");

    let unmap_inputs = match std::env::var("UNMAP_INPUTS").as_deref() {
        Ok("1") => true,
        Ok("0") => false,
        _ => kernel_at_least(6, 4),
    };

    let mut event_maps = Some(event_maps);
    let mut click_maps = Some(click_maps);
    if unmap_inputs {
        let events = if context.name_from_lines { None } else { event_maps.take() };
        let maps = (events, link_maps);
        std::thread::spawn(move || drop(maps));
    }
    let event_maps_ref: &[Option<Mmap>] = event_maps.as_deref().unwrap_or(&[]);

    let recycle_events = std::env::var("RECYCLE_EVENTS").as_deref() != Ok("0");

    let deduped = run_tasks(
        threads,
        dedup_parts + click_tasks.len(),
        || (DedupScratch::default(), BlockScatter::new(person_parts), BlockScatter::<NameRef>::new(if context.name_from_lines { person_parts } else { 0 }), BlockScatter::<Click>::new(if early { person_parts } else { 0 })),
        |(scratch, out, seqs, clicks_out), p| {
            if p >= dedup_parts {
                let (t, start, end) = click_tasks[p - dedup_parts];
                resolve_clicks(&people, &early_clicks[t][start..end], clicks_out);
                return;
            }
            let pieces32: Vec<&[EventCopy32]> = event32_parts.iter().flat_map(|scatter| scatter.pieces(p)).filter(|s| !s.is_empty()).collect();
            if !pieces32.is_empty() {
                dedup32(pieces32, &people, &slow_users, context.window.0, scratch, out);

                for scatter in &event32_parts {

                    unsafe { scatter.recycle_partition(p, out) };
                }
            }
            let pieces: Vec<&[EventCopy]> = event_parts.iter().flat_map(|scatter| scatter.pieces(p)).filter(|s| !s.is_empty()).collect();
            if !pieces.is_empty() {
                dedup(pieces, &people, scratch, out, if context.name_from_lines { Some(seqs) } else { None });

                if recycle_events {
                    for scatter in &event_parts {
                        let (start, end) = scatter.region(p);

                        unsafe { out.donate(start, end) };
                    }
                }
            }
        },
    );
    let (person_events, person_seqs): (Vec<BlockScatter<Event>>, Vec<BlockScatter<NameRef>>) = deduped.into_iter().map(|(_, out, seqs, clicks_out)| { if early { click_parts.push(clicks_out); } (out, seqs) }).unzip();
    timer.mark("dedup");
    if TIE_CONFLICT.load(Ordering::Relaxed) {

        use std::os::unix::process::CommandExt;
        eprintln!("pipeline: equal-ingest copies differ; rerunning on the 64-byte path");
        let error = std::process::Command::new(std::env::current_exe()?).args(std::env::args_os().skip(1)).env("PIPE_LEGACY", "1").exec();
        return Err(error.into());
    }

    let mut order: Vec<usize> = (0..person_parts).collect();
    let weight = |q: usize| person_events.iter().map(|scatter| scatter.len(q)).sum::<usize>();
    let weights: Vec<usize> = (0..person_parts).map(weight).collect();
    order.sort_by_key(|&q| std::cmp::Reverse(weights[q]));
    let campaign_count = campaign_rows.len();
    let states = run_tasks(
        threads,
        person_parts,
        || PeopleState {
            scratch: ThreadScratch::default(),
            scratch_wide: ThreadScratch::default(),
            user_rows: Vec::new(),
            spare_categories: Vec::new(),
            campaign_daily: CampaignCounters::new(campaign_count, context.window),
            session_number: 0,
            win_count: vec![0; campaign_count],
            win_pos: vec![0; campaign_count],
            active: Vec::new(),
        },
        |state, at| {
            let q = order[at];
            let event_pieces: Vec<&[Event]> = person_events.iter().flat_map(|scatter| scatter.pieces(q)).filter(|s| !s.is_empty()).collect();

            let seq_pieces: Vec<&[NameRef]> = if !context.name_from_lines { Vec::new() } else { person_seqs.iter().flat_map(|scatter| scatter.pieces(q)).filter(|s| !s.is_empty()).collect() };
            let names = Names { seq_pieces, event_maps: event_maps_ref };
            let click_pieces: Vec<&[Click]> = click_parts.iter().flat_map(|scatter| scatter.pieces(q)).filter(|s| !s.is_empty()).collect();

            if context.window.1 - context.window.0 <= i64::from(u32::MAX) {
                people_partition::<u32>(event_pieces, click_pieces, &context.longs, state, &category_names, context.window.0, &names);
            } else {
                people_partition::<i64>(event_pieces, click_pieces, &context.longs, state, &category_names, context.window.0, &names);
            }

            context.longs.close_unit();
        },
    );

    context.longs.build(threads);
    if NAME_MISSING.load(Ordering::Relaxed) {

        use std::os::unix::process::CommandExt;
        eprintln!("pipeline: a person's name could not be read from its event line; rerunning with per-row names");
        let error = std::process::Command::new(std::env::current_exe()?).args(std::env::args_os().skip(1)).env("NAME_FROM_LINES", "0").exec();
        return Err(error.into());
    }
    if unmap_inputs {
        let maps = (event_maps.take(), click_maps.take());
        std::thread::spawn(move || drop(maps));
    }
    timer.mark("sessions + attribution");

    output::prepare(&out)?;

    let mut counters = Vec::with_capacity(states.len());
    let mut user_parts = Vec::new();
    for state in states {
        counters.push(state.campaign_daily);
        if !state.user_rows.is_empty() {
            user_parts.push(state.user_rows);
        }
    }
    let name_into = |person: Key, out: &mut Vec<u8>| context.longs.append_bytes(person, out);
    let failures: Mutex<Vec<String>> = Mutex::new(Vec::new());

    let campaign_job = Mutex::new(Some(counters));
    let write_campaign = |counters: Vec<CampaignCounters>| {
        let mut campaign_daily: FxHashMap<(u32, i32), CampaignCounts> = FxHashMap::default();
        for counter in counters {
            counter.merge_into(&mut campaign_daily);
        }
        let path = out.join("campaign_daily/part-00000.parquet");
        if let Err(error) = output::write_campaign_daily_fast(&path, &campaign_daily, &campaign_rows) {
            failures.lock().unwrap().push(error.to_string());
        }
    };
    std::thread::scope(|scope| {
        let last = user_parts.len().saturating_sub(1);
        for (n, rows) in user_parts.iter().enumerate() {
            let (out, name_into, category_names, failures, campaign_job, write_campaign) =
                (&out, &name_into, &category_names, &failures, &campaign_job, &write_campaign);
            scope.spawn(move || {
                let path = out.join(format!("user_daily/part-{n:05}.parquet"));
                if let Err(error) = output::write_user_daily_fast(&path, rows, name_into, category_names) {
                    failures.lock().unwrap().push(error.to_string());
                }
                if n == last {
                    if let Some(counters) = campaign_job.lock().unwrap().take() {
                        write_campaign(counters);
                    }
                }
            });
        }
    });

    if let Some(counters) = campaign_job.into_inner().unwrap() {
        write_campaign(counters);
    }
    if let Some(error) = failures.into_inner().unwrap().into_iter().next() {
        return Err(error.into());
    }
    if user_parts.is_empty() {
        output::write_user_daily_fast(&out.join("user_daily/part-00000.parquet"), &[], &name_into, &category_names)?;
    }
    timer.mark("parquet");

    std::process::exit(0);
}

fn main() {

    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    unsafe {

        libc::mallopt(libc::M_ARENA_MAX, 1);
        libc::mallopt(libc::M_TOP_PAD, 64 << 20);

        libc::mallopt(libc::M_MMAP_THRESHOLD, 64 << 20);
        libc::mallopt(libc::M_TRIM_THRESHOLD, 256 << 20);
    }
    match run() {

        Ok(()) => std::process::exit(0),
        Err(error) => {
            eprintln!("pipeline: {error}");
            std::process::exit(1);
        }
    }
}
