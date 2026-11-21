//! Shopping Sessions pipeline.
//!
//! Short ids pack into integers. Events land in small partitions by event id, the earliest
//! ingest wins inside the partition, then sessions run per person. Rules live in rules.md.

mod time;

use std::cmp::Ordering;
use std::collections::HashMap;
use std::env;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use anyhow::{Context, Result};
use arrow_array::{Date32Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use memchr::memchr;
use memmap2::Mmap;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use rayon::prelude::*;
use rustc_hash::FxBuildHasher;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const DAY_MS: i64 = 86_400_000;
const SESSION_GAP_MS: i64 = 1_800_000;
const CLICK_REACH_MS: i64 = 7 * DAY_MS;
const LATE_MS: i128 = DAY_MS as i128;
const PARTS: usize = 128;
const PART_MASK: usize = PARTS - 1;
const PART_BITS: u32 = 7;
const SLOW_USER: u64 = 1 << 63;
const ORGANIC: u32 = u32::MAX;
const LONG_MARK: u8 = 0xFF;

type FxHashMap<K, V> = HashMap<K, V, FxBuildHasher>;

fn fxmap<K, V>() -> FxHashMap<K, V> {
    HashMap::with_hasher(FxBuildHasher)
}

fn ncpu() -> usize {
    env::var("NCPU")
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|&n| n > 0)
        .unwrap_or_else(|| std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4))
}

#[inline(always)]
fn mix64(id: u64) -> u64 {
    let mut hash = id.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    hash ^= hash >> 32;
    hash = hash.wrapping_mul(0xD6E8_FEB8_6659_FD93);
    hash ^ (hash >> 32)
}

fn mix_key(key: u128) -> u64 {
    mix64((key as u64) ^ ((key >> 64) as u64).rotate_left(17))
}

/// 1..=10 bytes of [0-9A-Za-z], 6 bits each, left-aligned in 60 bits.
#[inline(always)]
fn pack62(id: &[u8]) -> Option<u64> {
    let n = id.len();
    if n == 0 || n > 10 {
        return None;
    }
    let mut value = 0u64;
    for &byte in id {
        let code = match byte {
            b'0'..=b'9' => byte - b'0' + 1,
            b'A'..=b'Z' => byte - b'A' + 11,
            b'a'..=b'z' => byte - b'a' + 37,
            _ => return None,
        };
        value = (value << 6) | u64::from(code);
    }
    Some(value << (6 * (10 - n)))
}

fn unpack_key(packed: u64) -> u128 {
    let mut bytes = [0u8; 16];
    let mut n = 0usize;
    for i in 0..10 {
        let code = ((packed >> (54 - 6 * i)) & 63) as u8;
        if code == 0 {
            break;
        }
        bytes[n] = match code {
            1..=10 => b'0' + code - 1,
            11..=36 => b'A' + code - 11,
            _ => b'a' + code - 37,
        };
        n += 1;
    }
    bytes[15] = n as u8;
    u128::from_be_bytes(bytes)
}

fn key_of_short(id: &[u8]) -> u128 {
    let mut bytes = [0u8; 16];
    bytes[..id.len()].copy_from_slice(id);
    bytes[15] = id.len() as u8;
    u128::from_be_bytes(bytes)
}

struct Longs {
    map: Mutex<FxHashMap<u128, Vec<u8>>>,
}

impl Longs {
    fn new() -> Self {
        Self { map: Mutex::new(fxmap()) }
    }

    fn intern(&self, id: &[u8]) -> u128 {
        let mut map = self.map.lock().expect("long ids");
        if let Some((&key, _)) = map.iter().find(|(_, bytes)| bytes.as_slice() == id) {
            return key;
        }
        let key = ((map.len() as u128) << 8) | u128::from(LONG_MARK);
        map.insert(key, id.to_vec());
        key
    }

    fn bytes(&self, key: u128) -> Vec<u8> {
        if key as u8 != LONG_MARK {
            let raw = key.to_be_bytes();
            return raw[..raw[15] as usize].to_vec();
        }
        self.map.lock().expect("long ids").get(&key).cloned().unwrap_or_default()
    }
}

fn key_of(id: &[u8], longs: &Longs) -> u128 {
    if id.len() <= 15 {
        key_of_short(id)
    } else {
        longs.intern(id)
    }
}

fn cmp_key(left: u128, right: u128, longs: &Longs) -> Ordering {
    if left as u8 != LONG_MARK && right as u8 != LONG_MARK {
        left.cmp(&right)
    } else {
        longs.bytes(left).cmp(&longs.bytes(right))
    }
}

fn parse_fields<'a, const N: usize>(line: &'a [u8], out: &mut [Option<&'a [u8]>; N]) -> bool {
    if line.is_empty() {
        return false;
    }
    let mut count = 0usize;
    let mut start = 0usize;
    let mut quoted = false;
    for (at, &byte) in line.iter().enumerate() {
        if byte == b',' {
            if count == N {
                return false;
            }
            if !take_field(line, start, at, quoted, &mut out[count]) {
                return false;
            }
            count += 1;
            start = at + 1;
            quoted = false;
        } else if byte == b'"' {
            quoted = true;
        }
    }
    if count + 1 != N || !take_field(line, start, line.len(), quoted, &mut out[count]) {
        return false;
    }
    true
}

fn split_line<'a, const N: usize>(line: &'a [u8]) -> Option<[&'a [u8]; N]> {
    if line.is_empty() {
        return None;
    }
    let mut out = [b"".as_slice(); N];
    let mut start = 0usize;
    let mut count = 0usize;
    for (at, &byte) in line.iter().enumerate() {
        if byte == b'"' {
            return split_quoted::<N>(line);
        }
        if byte == b',' {
            if count >= N {
                return None;
            }
            out[count] = &line[start..at];
            count += 1;
            start = at + 1;
        }
    }
    if count + 1 != N {
        return None;
    }
    out[count] = &line[start..];
    Some(out)
}

fn split_quoted<'a, const N: usize>(line: &'a [u8]) -> Option<[&'a [u8]; N]> {
    let mut fields = [None; N];
    if !parse_fields::<N>(line, &mut fields) {
        return None;
    }
    let mut out = [b"".as_slice(); N];
    for (slot, field) in out.iter_mut().zip(fields) {
        if let Some(field) = field {
            *slot = field;
        }
    }
    Some(out)
}

fn take_field<'a>(line: &'a [u8], start: usize, end: usize, quoted: bool, slot: &mut Option<&'a [u8]>) -> bool {
    let mut field = &line[start..end];
    if quoted {
        let n = field.len();
        if n < 2 || field[0] != b'"' || field[n - 1] != b'"' || field[1..n - 1].contains(&b'"') {
            return false;
        }
        field = &field[1..n - 1];
    }
    *slot = if field.is_empty() { None } else { Some(field) };
    true
}

fn for_lines(data: &[u8], mut visit: impl FnMut(&[u8])) {
    let mut rest = data;
    while let Some(index) = memchr(b'\n', rest) {
        visit(&rest[..index]);
        rest = &rest[index + 1..];
    }
    if !rest.is_empty() {
        visit(rest);
    }
}

fn line_chunks(data: &[u8], threads: usize) -> Vec<(usize, usize)> {
    let Some(newline) = memchr(b'\n', data) else {
        return Vec::new();
    };
    let mut start = newline + 1;
    if start >= data.len() {
        return Vec::new();
    }
    let body = data.len() - start;
    let n = (body / (1 << 20)).max(1).min(threads.max(1));
    let step = (body / n).max(1);
    let mut spans = Vec::with_capacity(n);
    for i in 0..n {
        if start >= data.len() {
            break;
        }
        let mut end = if i + 1 == n { data.len() } else { (start + step).min(data.len()) };
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


struct Mapped {
    map: Mmap,
}

fn map_csvs(dir: &Path) -> Result<Vec<Mapped>> {
    let mut paths = Vec::new();
    if dir.is_dir() {
        for entry in fs::read_dir(dir).with_context(|| format!("read {}", dir.display()))? {
            let path = entry?.path();
            if path.extension().and_then(|ext| ext.to_str()) == Some("csv") && path.is_file() {
                paths.push(path);
            }
        }
    }
    paths.sort();
    let mut out = Vec::new();
    for path in paths {
        let file = File::open(&path).with_context(|| format!("open {}", path.display()))?;
        if file.metadata()?.len() == 0 {
            continue;
        }
        let map = unsafe { Mmap::map(&file) }.with_context(|| format!("mmap {}", path.display()))?;
        out.push(Mapped { map });
    }
    Ok(out)
}

fn chunks(files: &[Mapped], threads: usize) -> Vec<(&[u8], usize, usize)> {
    let mut pieces = Vec::new();
    for file in files {
        for (start, end) in line_chunks(&file.map, threads) {
            pieces.push((file.map.as_ref(), start, end, end - start));
        }
    }
    pieces.sort_by_key(|piece| std::cmp::Reverse(piece.3));
    let threads = threads.max(1);
    let mut bins: Vec<Vec<(&[u8], usize, usize)>> = vec![Vec::new(); threads];
    let mut load = vec![0usize; threads];
    for (data, start, end, size) in pieces {
        let bin = load.iter().enumerate().min_by_key(|(_, used)| **used).unwrap().0;
        load[bin] += size;
        bins[bin].push((data, start, end));
    }
    bins.into_iter().flatten().collect()
}

fn canonical_num(bytes: &[u8]) -> Option<u32> {
    if bytes.is_empty() || bytes.len() > 7 || !bytes[0].is_ascii_digit() {
        return None;
    }
    if bytes.len() > 1 && bytes[0] == b'0' {
        return None;
    }
    let mut value = 0u32;
    for &byte in bytes {
        if !byte.is_ascii_digit() {
            return None;
        }
        value = value * 10 + u32::from(byte - b'0');
    }
    Some(value)
}

struct Catalog {
    product_num: Vec<u16>,
    product_extra: FxHashMap<u128, u16>,
    campaign_num: Vec<u32>,
    campaign_extra: FxHashMap<u128, u32>,
    categories: Vec<Vec<u8>>,
    campaign_ids: Vec<Vec<u8>>,
    channels: Vec<Vec<u8>>,
}

fn lookup_product(catalog: &Catalog, field: &[u8], longs: &Longs) -> u16 {
    if let Some(rest) = field.strip_prefix(b"p") {
        if let Some(number) = canonical_num(rest) {
            let index = number as usize;
            if index < catalog.product_num.len() {
                let cat = catalog.product_num[index];
                if cat != u16::MAX {
                    return cat;
                }
            }
            return 0;
        }
    }
    catalog.product_extra.get(&key_of(field, longs)).copied().unwrap_or(0)
}

fn lookup_campaign(catalog: &Catalog, field: &[u8], longs: &Longs) -> u32 {
    if field.len() >= 3 && field[0] == b'c' && field[1] == b'_' {
        if let Some(number) = canonical_num(&field[2..]) {
            let index = number as usize;
            if index < catalog.campaign_num.len() {
                let id = catalog.campaign_num[index];
                if id != ORGANIC {
                    return id;
                }
            }
            return ORGANIC;
        }
    }
    catalog
        .campaign_extra
        .get(&key_of(field, longs))
        .copied()
        .unwrap_or(ORGANIC)
}

fn load_catalog(data_dir: &Path, longs: &Longs) -> Result<Catalog> {
    let mut cat_index: FxHashMap<Vec<u8>, u16> = fxmap();
    cat_index.insert(b"unknown".to_vec(), 0);
    let mut catalog = Catalog {
        product_num: Vec::new(),
        product_extra: fxmap(),
        campaign_num: Vec::new(),
        campaign_extra: fxmap(),
        categories: vec![b"unknown".to_vec()],
        campaign_ids: Vec::new(),
        channels: Vec::new(),
    };
    let products = fs::read(data_dir.join("products.csv")).context("products.csv")?;
    let mut header = true;
    for_lines(&products, |line| {
        if header {
            header = false;
            return;
        }
        let mut fields = [None; 2];
        if !parse_fields::<2>(line, &mut fields) {
            return;
        }
        let Some(product) = fields[0] else { return };
        let category = fields[1].unwrap_or_default();
        let cat = if let Some(&id) = cat_index.get(category) {
            id
        } else {
            let id = catalog.categories.len() as u16;
            catalog.categories.push(category.to_vec());
            cat_index.insert(category.to_vec(), id);
            id
        };
        if let Some(rest) = product.strip_prefix(b"p") {
            if let Some(number) = canonical_num(rest) {
                let index = number as usize;
                if index < catalog.product_num.len() && catalog.product_num[index] != u16::MAX {
                    return;
                }
                if catalog.product_num.len() <= index {
                    catalog.product_num.resize(index + 1, u16::MAX);
                }
                catalog.product_num[index] = cat;
                return;
            }
        }
        catalog.product_extra.entry(key_of(product, longs)).or_insert(cat);
    });
    let campaigns = fs::read(data_dir.join("campaigns.csv")).context("campaigns.csv")?;
    header = true;
    for_lines(&campaigns, |line| {
        if header {
            header = false;
            return;
        }
        let mut fields = [None; 2];
        if !parse_fields::<2>(line, &mut fields) {
            return;
        }
        let Some(campaign) = fields[0] else { return };
        if campaign.is_empty() {
            return;
        }
        let channel = fields[1].unwrap_or_default();
        let known = if campaign.len() >= 3 && campaign[0] == b'c' && campaign[1] == b'_' {
            canonical_num(&campaign[2..]).is_some_and(|number| {
                let index = number as usize;
                index < catalog.campaign_num.len() && catalog.campaign_num[index] != ORGANIC
            })
        } else {
            catalog.campaign_extra.contains_key(&key_of(campaign, longs))
        };
        if known {
            return;
        }
        let id = catalog.campaign_ids.len() as u32;
        catalog.campaign_ids.push(campaign.to_vec());
        catalog.channels.push(channel.to_vec());
        if campaign.len() >= 3 && campaign[0] == b'c' && campaign[1] == b'_' {
            if let Some(number) = canonical_num(&campaign[2..]) {
                let index = number as usize;
                if catalog.campaign_num.len() <= index {
                    catalog.campaign_num.resize(index + 1, ORGANIC);
                }
                catalog.campaign_num[index] = id;
                return;
            }
        }
        catalog.campaign_extra.insert(key_of(campaign, longs), id);
    });
    Ok(catalog)
}

struct People {
    fast_keys: Vec<u64>,
    fast_vals: Vec<u128>,
    fast_mask: usize,
    slow: FxHashMap<u128, u128>,
}

impl People {
    fn get_fast(&self, packed: u64) -> u128 {
        if self.fast_mask == 0 {
            return unpack_key(packed);
        }
        let mut slot = mix64(packed) as usize & self.fast_mask;
        loop {
            let key = self.fast_keys[slot];
            if key == 0 {
                return unpack_key(packed);
            }
            if key == packed {
                return self.fast_vals[slot];
            }
            slot = (slot + 1) & self.fast_mask;
        }
    }

    fn get_slow(&self, key: u128) -> u128 {
        self.slow.get(&key).copied().unwrap_or(key)
    }
}

fn pack_of_key(key: u128) -> Option<u64> {
    let mark = key as u8;
    if mark == 0 || mark == LONG_MARK {
        return None;
    }
    let raw = key.to_be_bytes();
    pack62(&raw[..mark as usize])
}

struct UnionFind {
    index: FxHashMap<u128, u32>,
    parent: Vec<u32>,
    keys: Vec<u128>,
    min_at: Vec<u32>,
}

impl UnionFind {
    fn new() -> Self {
        Self { index: fxmap(), parent: Vec::new(), keys: Vec::new(), min_at: Vec::new() }
    }

    fn ensure(&mut self, key: u128) -> u32 {
        if let Some(&id) = self.index.get(&key) {
            return id;
        }
        let id = self.keys.len() as u32;
        self.index.insert(key, id);
        self.parent.push(id);
        self.keys.push(key);
        self.min_at.push(id);
        id
    }

    fn find(&mut self, mut node: u32) -> u32 {
        let mut root = node;
        while self.parent[root as usize] != root {
            root = self.parent[root as usize];
        }
        while self.parent[node as usize] != root {
            let parent = self.parent[node as usize];
            self.parent[node as usize] = root;
            node = parent;
        }
        root
    }

    fn union(&mut self, left: u128, right: u128, longs: &Longs) {
        let a = self.ensure(left);
        let b = self.ensure(right);
        let mut ra = self.find(a);
        let mut rb = self.find(b);
        if ra == rb {
            return;
        }
        if cmp_key(self.keys[self.min_at[rb as usize] as usize], self.keys[self.min_at[ra as usize] as usize], longs) == Ordering::Less
        {
            std::mem::swap(&mut ra, &mut rb);
        }
        self.parent[rb as usize] = ra;
        let mb = self.min_at[rb as usize];
        if cmp_key(self.keys[mb as usize], self.keys[self.min_at[ra as usize] as usize], longs) == Ordering::Less {
            self.min_at[ra as usize] = mb;
        }
    }

    fn finish(&mut self) -> People {
        for index in 0..self.parent.len() {
            let root = self.find(index as u32);
            self.parent[index] = root;
        }
        let mut pairs: Vec<(u64, u128)> = Vec::new();
        let mut slow = fxmap();
        for index in 0..self.keys.len() {
            let person = self.keys[self.min_at[self.parent[index] as usize] as usize];
            let member = self.keys[index];
            if let Some(packed) = pack_of_key(member) {
                pairs.push((packed, person));
            } else {
                slow.insert(member, person);
            }
        }
        let slots = (pairs.len() * 2).next_power_of_two().max(16);
        let mask = slots - 1;
        let mut fast_keys = vec![0u64; slots];
        let mut fast_vals = vec![0u128; slots];
        for (packed, person) in pairs {
            let mut slot = mix64(packed) as usize & mask;
            while fast_keys[slot] != 0 && fast_keys[slot] != packed {
                slot = (slot + 1) & mask;
            }
            fast_keys[slot] = packed;
            fast_vals[slot] = person;
        }
        People { fast_keys, fast_vals, fast_mask: mask, slow }
    }
}

fn load_links(files: &[Mapped], threads: usize, longs: &Longs) -> People {
    let pieces = chunks(files, threads);
    let pairs: Vec<Vec<(u128, u128)>> = pieces
        .par_iter()
        .map(|(data, start, end)| {
            let mut out = Vec::new();
            for_lines(&data[*start..*end], |line| {
                let Some(fields) = split_line::<2>(line) else { return };
                if !fields[0].is_empty() && !fields[1].is_empty() {
                    out.push((key_of(fields[0], longs), key_of(fields[1], longs)));
                }
            });
            out
        })
        .collect();
    let mut uf = UnionFind::new();
    for (left, right) in pairs.into_iter().flatten() {
        uf.union(left, right, longs);
    }
    uf.finish()
}

#[derive(Clone, Copy)]
struct Ev32 {
    id: u64,
    user: u64,
    ingest: i64,
    ts: i64,
    price: u32,
    cat: u16,
    valid: u8,
}

#[derive(Clone, Copy)]
struct EvSlow {
    id: u128,
    user: u128,
    ingest: i64,
    ts: i64,
    price: u32,
    cat: u16,
    valid: u8,
}

#[derive(Clone, Copy)]
struct OutEv {
    person: u128,
    ts: i64,
    price: u32,
    cat: u16,
}

#[derive(Clone, Copy)]
struct ClickRec {
    person: u128,
    id: u128,
    ts: i64,
    campaign: u32,
}

struct ThreadParse {
    fast: Vec<Vec<Ev32>>,
    slow: Vec<Vec<EvSlow>>,
    slow_users: Vec<u128>,
    clicks: Vec<ClickRec>,
}

fn empty_parts<T>() -> Vec<Vec<T>> {
    (0..PARTS).map(|_| Vec::new()).collect()
}

fn slow_user_ref(thread: u64, index: usize) -> u64 {
    SLOW_USER | (thread << 40) | index as u64
}

fn price_of(field: &[u8]) -> Option<u32> {
    if field.is_empty() {
        return None;
    }
    let mut value = 0u64;
    for &byte in field {
        if !byte.is_ascii_digit() {
            return None;
        }
        value = value.saturating_mul(10).saturating_add(u64::from(byte - b'0'));
        if value > 10_000_000 {
            return None;
        }
    }
    if value == 0 { None } else { Some(value as u32) }
}

fn judge(kind: &[u8], ts: i64, price: Option<u32>, start: i64, end: i64, ingest: i64) -> Option<(u32, bool)> {
    let purchase = match kind {
        b"view" | b"cart" => false,
        b"purchase" => true,
        _ => return None,
    };
    if ts < start || ts >= end {
        return None;
    }
    if i128::from(ingest) - i128::from(ts) > LATE_MS {
        return None;
    }
    if purchase {
        Some((price?, true))
    } else {
        Some((0, false))
    }
}

fn parse_all(
    event_files: &[Mapped],
    click_files: &[Mapped],
    catalog: &Catalog,
    people: &People,
    longs: &Longs,
    window: (i64, i64),
    threads: usize,
) -> Vec<ThreadParse> {
    let event_chunks = chunks(event_files, threads);
    let click_chunks = chunks(click_files, threads);
    let click_start = window.0 - CLICK_REACH_MS;
    let bins = threads.max(1);
    (0..bins)
        .into_par_iter()
        .map(|thread| {
            let bytes: usize = event_chunks.iter().skip(thread).step_by(bins).map(|&(_, start, end)| end - start).sum();
            let mut events = Vec::with_capacity(bytes / 48);
            let mut parsed = ThreadParse {
                fast: Vec::new(),
                slow: empty_parts(),
                slow_users: Vec::new(),
                clicks: Vec::new(),
            };
            for &(data, start, end) in event_chunks.iter().skip(thread).step_by(bins) {
                for_lines(&data[start..end], |line| {
                    let Some(fields) = split_line::<7>(line) else { return };
                    if fields[0].is_empty() {
                        return;
                    }
                    let Some(ingest) = time::parse_ts(fields[3]) else { return };
                    let user_bytes = fields[1];
                    let kind = fields[4];
                    let product = fields[5];
                    let ts = time::parse_ts(fields[2]);
                    let valid = ts.and_then(|ts| {
                        if user_bytes.is_empty() || product.is_empty() {
                            return None;
                        }
                        let (price, purchase) = judge(kind, ts, price_of(fields[6]), window.0, window.1, ingest)?;
                        let cat = if purchase { lookup_product(catalog, product, longs) } else { 0 };
                        Some((ts, price, cat))
                    });
                    if let Some(id) = pack62(fields[0]) {
                        let (ts, price, cat, valid_flag, user) = if let Some((ts, price, cat)) = valid {
                            let user = match pack62(user_bytes) {
                                Some(packed) => packed,
                                None => {
                                    let index = parsed.slow_users.len();
                                    parsed.slow_users.push(key_of(user_bytes, longs));
                                    slow_user_ref(thread as u64, index)
                                }
                            };
                            (ts, price, cat, 1u8, user)
                        } else {
                            (0, 0, 0, 0u8, 0u64)
                        };
                        events.push(Ev32 { id, user, ingest, ts, price, cat, valid: valid_flag });
                    } else {
                        let id = key_of(fields[0], longs);
                        let (ts, price, cat, valid_flag, user) = if let Some((ts, price, cat)) = valid {
                            (ts, price, cat, 1u8, key_of(user_bytes, longs))
                        } else {
                            (0, 0, 0, 0u8, 0u128)
                        };
                        parsed.slow[mix_key(id) as usize & PART_MASK].push(EvSlow {
                            id,
                            user,
                            ingest,
                            ts,
                            price,
                            cat,
                            valid: valid_flag,
                        });
                    }
                });
            }
            parsed.fast = bucket_events(events);
            for &(data, start, end) in click_chunks.iter().skip(thread).step_by(bins) {
                for_lines(&data[start..end], |line| {
                    let Some(fields) = split_line::<4>(line) else { return };
                    if fields[0].is_empty() || fields[1].is_empty() || fields[2].is_empty() || fields[3].is_empty() {
                        return;
                    }
                    let campaign = lookup_campaign(catalog, fields[2], longs);
                    if campaign == ORGANIC {
                        return;
                    }
                    let Some(ts) = time::parse_ts(fields[3]) else { return };
                    if ts < click_start || ts >= window.1 {
                        return;
                    }
                    let person = if let Some(packed) = pack62(fields[1]) {
                        people.get_fast(packed)
                    } else {
                        people.get_slow(key_of(fields[1], longs))
                    };
                    parsed.clicks.push(ClickRec {
                        person,
                        id: key_of(fields[0], longs),
                        ts,
                        campaign,
                    });
                });
            }
            parsed
        })
        .collect()
}

fn person_of(user: u64, thread: &[Vec<u128>], people: &People) -> u128 {
    if user & SLOW_USER != 0 {
        let part = ((user >> 40) & 0x7F_FFFF) as usize;
        let index = (user & 0xFF_FFFF_FFFF) as usize;
        let key = thread[part][index];
        people.get_slow(key)
    } else {
        people.get_fast(user)
    }
}


fn bucket_events(events: Vec<Ev32>) -> Vec<Vec<Ev32>> {
    let mut counts = vec![0usize; PARTS];
    for event in &events {
        counts[mix64(event.id) as usize & PART_MASK] += 1;
    }
    let mut buckets: Vec<Vec<Ev32>> = counts.into_iter().map(Vec::with_capacity).collect();
    for event in events {
        buckets[mix64(event.id) as usize & PART_MASK].push(event);
    }
    buckets
}

fn dedup_fast(pieces: Vec<Vec<Ev32>>, slow_users: &[Vec<u128>], people: &People) -> Vec<OutEv> {
    let mut rows = Vec::new();
    for piece in pieces {
        rows.extend(piece);
    }
    let n = rows.len();
    if n == 0 {
        return Vec::new();
    }
    let slots = (n * 2).next_power_of_two().max(16);
    let mask = slots - 1;
    let mut table = vec![u64::MAX; slots];
    for (index, row) in rows.iter().enumerate() {
        let hash = mix64(row.id);
        let tag = hash >> 32;
        let mut slot = (hash >> PART_BITS) as usize & mask;
        loop {
            let held = table[slot];
            if held == u64::MAX {
                table[slot] = (tag << 32) | index as u64;
                break;
            }
            if held >> 32 == tag {
                let other = held as u32 as usize;
                if rows[other].id == row.id {
                    if row.ingest < rows[other].ingest {
                        table[slot] = (tag << 32) | index as u64;
                    }
                    break;
                }
            }
            slot = (slot + 1) & mask;
        }
    }
    let mut out = Vec::with_capacity(n);
    for held in table {
        if held == u64::MAX {
            continue;
        }
        let row = rows[held as u32 as usize];
        if row.valid == 1 {
            out.push(OutEv {
                person: person_of(row.user, slow_users, people),
                ts: row.ts,
                price: row.price,
                cat: row.cat,
            });
        }
    }
    out
}

fn dedup_slow(pieces: Vec<Vec<EvSlow>>, people: &People) -> Vec<OutEv> {
    let mut rows = Vec::new();
    for piece in pieces {
        rows.extend(piece);
    }
    if rows.is_empty() {
        return Vec::new();
    }
    rows.sort_unstable_by(|left, right| left.id.cmp(&right.id).then(left.ingest.cmp(&right.ingest)));
    let mut out = Vec::new();
    let mut index = 0usize;
    while index < rows.len() {
        let row = rows[index];
        if row.valid == 1 {
            out.push(OutEv {
                person: people.get_slow(row.user),
                ts: row.ts,
                price: row.price,
                cat: row.cat,
            });
        }
        index += 1;
        while index < rows.len() && rows[index].id == row.id {
            index += 1;
        }
    }
    out
}

struct UserRow {
    person: u128,
    day: i32,
    sessions: i64,
    events: i64,
    revenue: i64,
    top: Option<u16>,
}

#[derive(Clone, Copy, Default)]
struct Camp {
    sessions: i64,
    purchasing: i64,
    revenue: i64,
    assisted: i64,
}

fn sessionize(
    events: &[OutEv],
    clicks: &[ClickRec],
    categories: &[Vec<u8>],
    campaigns: usize,
) -> (Vec<UserRow>, FxHashMap<(u32, i32), Camp>) {
    let mut users = Vec::new();
    let mut camps: FxHashMap<(u32, i32), Camp> = fxmap();
    let mut stamp = vec![0u32; campaigns];
    let mut generation = 1u32;
    let mut touched = Vec::new();
    let mut event_at = 0usize;
    let mut click_at = 0usize;
    while event_at < events.len() {
        let person = events[event_at].person;
        let event_lo = event_at;
        event_at += 1;
        while event_at < events.len() && events[event_at].person == person {
            event_at += 1;
        }
        while click_at < clicks.len() && clicks[click_at].person < person {
            click_at += 1;
        }
        let click_lo = click_at;
        while click_at < clicks.len() && clicks[click_at].person == person {
            click_at += 1;
        }
        let group = &events[event_lo..event_at];
        let slice = &clicks[click_lo..click_at];
        let mut cursor = 0usize;
        let mut day = i32::MIN;
        let mut sessions = 0i64;
        let mut event_count = 0i64;
        let mut revenue = 0i64;
        let mut by_cat: Vec<(u16, i64)> = Vec::new();
        let flush = |day: &mut i32,
                     sessions: &mut i64,
                     event_count: &mut i64,
                     revenue: &mut i64,
                     by_cat: &mut Vec<(u16, i64)>,
                     users: &mut Vec<UserRow>| {
            if *sessions == 0 {
                return;
            }
            let mut top = None;
            let mut best = 0i64;
            for &(cat, rev) in by_cat.iter() {
                let replace = match top {
                    None => true,
                    Some(prev) => rev > best || (rev == best && categories[cat as usize] < categories[prev as usize]),
                };
                if replace {
                    top = Some(cat);
                    best = rev;
                }
            }
            users.push(UserRow {
                person,
                day: *day,
                sessions: *sessions,
                events: *event_count,
                revenue: *revenue,
                top,
            });
            *sessions = 0;
            *event_count = 0;
            *revenue = 0;
            by_cat.clear();
        };
        while cursor < group.len() {
            let session_start = group[cursor].ts;
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
            let session_day = (session_start.div_euclid(DAY_MS)) as i32;
            if sessions > 0 && session_day != day {
                flush(&mut day, &mut sessions, &mut event_count, &mut revenue, &mut by_cat, &mut users);
            }
            day = session_day;
            sessions += 1;
            event_count += (next - cursor) as i64;
            let mut session_revenue = 0i64;
            let mut purchases = 0u32;
            for event in &group[cursor..next] {
                if event.price > 0 {
                    session_revenue += i64::from(event.price);
                    purchases += 1;
                    match by_cat.iter_mut().find(|(cat, _)| *cat == event.cat) {
                        Some(entry) => entry.1 += i64::from(event.price),
                        None => by_cat.push((event.cat, i64::from(event.price))),
                    }
                }
            }
            revenue += session_revenue;
            let earliest = session_start.saturating_sub(CLICK_REACH_MS);
            let lower = slice.partition_point(|click| click.ts < earliest);
            let upper = slice.partition_point(|click| click.ts <= session_start);
            generation = generation.wrapping_add(1);
            if generation == 0 {
                stamp.fill(0);
                generation = 1;
            }
            touched.clear();
            let credited = if upper > lower {
                let latest = slice[upper - 1].ts;
                let mut first = upper - 1;
                while first > lower && slice[first - 1].ts == latest {
                    first -= 1;
                }
                for click in &slice[lower..upper] {
                    let id = click.campaign as usize;
                    if stamp[id] != generation {
                        stamp[id] = generation;
                        touched.push(click.campaign);
                    }
                }
                slice[first].campaign
            } else {
                ORGANIC
            };
            let bucket = camps.entry((credited, session_day)).or_default();
            bucket.sessions += 1;
            bucket.revenue += session_revenue;
            if purchases > 0 {
                bucket.purchasing += 1;
            }
            for campaign in &touched {
                camps.entry((*campaign, session_day)).or_default().assisted += 1;
            }
            cursor = next;
        }
        flush(&mut day, &mut sessions, &mut event_count, &mut revenue, &mut by_cat, &mut users);
    }
    (users, camps)
}

fn json_string<'a>(text: &'a str, key: &str) -> Option<&'a str> {
    let needle = format!("\"{key}\"");
    let at = text.find(&needle)?;
    let after = &text[at + needle.len()..];
    let open = after.find('"')?;
    let value = &after[open + 1..];
    let close = value.find('"')?;
    Some(&value[..close])
}

fn text_of(bytes: &[u8]) -> String {
    String::from_utf8(bytes.to_vec()).unwrap_or_else(|err| String::from_utf8_lossy(err.as_bytes()).into_owned())
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
    let top = StringArray::from(rows.iter().map(|row| row.5.as_deref()).collect::<Vec<_>>());
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

fn write_campaign_daily(dir: &Path, rows: &[(String, String, i32, i64, i64, i64, i64)]) -> Result<()> {
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
        .set_dictionary_enabled(false)
        .set_statistics_enabled(EnabledStatistics::None)
        .build();
    let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

fn sessionize_all(
    events: &[OutEv],
    clicks: &[ClickRec],
    categories: &[Vec<u8>],
    campaigns: usize,
    threads: usize,
) -> (Vec<UserRow>, FxHashMap<(u32, i32), Camp>) {
    if events.is_empty() {
        return (Vec::new(), fxmap());
    }
    let threads = threads.max(1).min(events.len());
    let mut ranges = Vec::with_capacity(threads);
    let mut start = 0usize;
    for t in 0..threads {
        let mut end = if t + 1 == threads { events.len() } else { events.len() * (t + 1) / threads };
        if end <= start {
            continue;
        }
        if end < events.len() {
            let person = events[end].person;
            while end < events.len() && events[end].person == person {
                end += 1;
            }
        }
        ranges.push((start, end));
        start = end;
    }
    let parts: Vec<_> = ranges
        .into_par_iter()
        .map(|(lo, hi)| {
            let c0 = clicks.partition_point(|click| click.person < events[lo].person);
            sessionize(&events[lo..hi], &clicks[c0..], categories, campaigns)
        })
        .collect();
    let mut users = Vec::new();
    let mut camps: FxHashMap<(u32, i32), Camp> = fxmap();
    for (rows, part) in parts {
        users.extend(rows);
        for (key, agg) in part {
            let row = camps.entry(key).or_default();
            row.sessions += agg.sessions;
            row.purchasing += agg.purchasing;
            row.revenue += agg.revenue;
            row.assisted += agg.assisted;
        }
    }
    (users, camps)
}

fn run() -> Result<()> {
    let started = Instant::now();
    let threads = ncpu();
    let _ = rayon::ThreadPoolBuilder::new().num_threads(threads).build_global();
    let mut args = env::args().skip(1);
    let data_dir = PathBuf::from(
        env::var("DATA_DIR").ok().or_else(|| args.next()).unwrap_or_else(|| "/data".to_string()),
    );
    let out_dir = PathBuf::from(
        env::var("OUTPUT_DIR").ok().or_else(|| args.next()).unwrap_or_else(|| "/output".to_string()),
    );
    let longs = Longs::new();
    let manifest = fs::read_to_string(data_dir.join("manifest.json")).context("manifest.json")?;
    let window_start = time::parse_ts(json_string(&manifest, "window_start").context("window_start")?.as_bytes())
        .context("window_start")?;
    let window_end = time::parse_ts(json_string(&manifest, "window_end").context("window_end")?.as_bytes())
        .context("window_end")?;
    let catalog = load_catalog(&data_dir, &longs)?;
    eprintln!("catalog {}ms", started.elapsed().as_millis());

    let mark = Instant::now();
    let link_files = map_csvs(&data_dir.join("links"))?;
    let people = load_links(&link_files, threads, &longs);
    eprintln!("links {}ms", mark.elapsed().as_millis());

    let mark = Instant::now();
    let event_files = map_csvs(&data_dir.join("events"))?;
    let click_files = map_csvs(&data_dir.join("clicks"))?;
    let parsed = parse_all(&event_files, &click_files, &catalog, &people, &longs, (window_start, window_end), threads);
    eprintln!("parse {}ms", mark.elapsed().as_millis());

    let mark = Instant::now();
    let slow_users: Vec<Vec<u128>> = parsed.iter().map(|thread| thread.slow_users.clone()).collect();
    let mut fast_buckets: Vec<Vec<Vec<Ev32>>> = empty_parts();
    let mut slow_buckets: Vec<Vec<Vec<EvSlow>>> = empty_parts();
    let mut clicks = Vec::new();
    for thread in parsed {
        for (index, part) in thread.fast.into_iter().enumerate() {
            if !part.is_empty() {
                fast_buckets[index].push(part);
            }
        }
        for (index, part) in thread.slow.into_iter().enumerate() {
            if !part.is_empty() {
                slow_buckets[index].push(part);
            }
        }
        clicks.extend(thread.clicks);
    }
    let (fast_out, slow_out): (Vec<Vec<OutEv>>, Vec<Vec<OutEv>>) = rayon::join(
        || {
            fast_buckets
                .into_par_iter()
                .map(|pieces| dedup_fast(pieces, &slow_users, &people))
                .collect()
        },
        || slow_buckets.into_par_iter().map(|pieces| dedup_slow(pieces, &people)).collect(),
    );
    let mut events = Vec::new();
    for part in fast_out {
        events.extend(part);
    }
    for part in slow_out {
        events.extend(part);
    }
    let slow_order = longs.map.lock().expect("long ids").len() > 0;
    events.par_sort_unstable_by(|left, right| left.person.cmp(&right.person).then(left.ts.cmp(&right.ts)));
    if slow_order {
        clicks.par_sort_unstable_by(|left, right| {
            left.person.cmp(&right.person).then(left.ts.cmp(&right.ts)).then_with(|| cmp_key(left.id, right.id, &longs))
        });
    } else {
        clicks.par_sort_unstable_by(|left, right| {
            left.person.cmp(&right.person).then(left.ts.cmp(&right.ts)).then(left.id.cmp(&right.id))
        });
    }
    eprintln!("dedup events {} clicks {} in {}ms", events.len(), clicks.len(), mark.elapsed().as_millis());

    let mark = Instant::now();
    let campaigns = catalog.campaign_ids.len();
    let (user_rows, mut camp_map) = sessionize_all(&events, &clicks, &catalog.categories, campaigns, threads);
    eprintln!("sessions {} user-days {} camps {} in {}ms", events.len(), user_rows.len(), camp_map.len(), mark.elapsed().as_millis());

    let user_out: Vec<_> = user_rows
        .iter()
        .map(|row| {
            let top = row.top.map(|cat| text_of(&catalog.categories[cat as usize]));
            (text_of(&longs.bytes(row.person)), row.day, row.sessions, row.events, row.revenue, top)
        })
        .collect();
    let camp_out: Vec<_> = camp_map
        .drain()
        .filter(|(_, agg)| agg.sessions > 0 || agg.assisted > 0)
        .map(|((campaign, day), agg)| {
            let (id, channel) = if campaign == ORGANIC {
                ("organic".to_string(), "organic".to_string())
            } else {
                (
                    text_of(&catalog.campaign_ids[campaign as usize]),
                    text_of(&catalog.channels[campaign as usize]),
                )
            };
            (id, channel, day, agg.sessions, agg.purchasing, agg.revenue, agg.assisted)
        })
        .collect();
    let user_sessions: i64 = user_out.iter().map(|row| row.2).sum();
    let user_revenue: i64 = user_out.iter().map(|row| row.4).sum();
    let camp_sessions: i64 = camp_out.iter().map(|row| row.3).sum();
    let camp_revenue: i64 = camp_out.iter().map(|row| row.5).sum();
    eprintln!("self-check sessions {user_sessions} vs {camp_sessions}, revenue {user_revenue} vs {camp_revenue}");

    let mark = Instant::now();
    write_user_daily(&out_dir.join("user_daily"), &user_out)?;
    write_campaign_daily(&out_dir.join("campaign_daily"), &camp_out)?;
    eprintln!("write {}ms, total {}ms", mark.elapsed().as_millis(), started.elapsed().as_millis());
    Ok(())
}

fn main() {
    if let Err(error) = run() {
        eprintln!("error: {error:#}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pack_roundtrip() {
        for sample in [b"u1".as_slice(), b"eabc", b"A", b"p123", b"0123456789", b"zzzzzzzzzz", b"U"] {
            let packed = pack62(sample).expect("packs");
            assert_eq!(longs_free(unpack_key(packed)), sample);
            assert_eq!(key_of_short(sample), unpack_key(packed));
            assert_eq!(packed >> 60, 0);
        }
        assert!(pack62(b"A\tB").is_none());
        assert!(pack62(b"").is_none());
        assert!(pack62(b"too-long-id").is_none());
    }

    fn longs_free(key: u128) -> Vec<u8> {
        let raw = key.to_be_bytes();
        raw[..raw[15] as usize].to_vec()
    }
}
