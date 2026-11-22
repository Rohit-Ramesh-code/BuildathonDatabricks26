use std::io::Write;

pub enum Col<'a> {

    Str { bytes: &'a [u8], ends: &'a [u32] },

    OptStr { bytes: &'a [u8], ends: &'a [u32], present: &'a [bool] },
    Date(&'a [i32]),
    I64(&'a [i64]),

    StrDict { bytes: &'a [u8], ends: &'a [u32] },
    OptStrDict { bytes: &'a [u8], ends: &'a [u32], present: &'a [bool] },
    DateDict(&'a [i32]),
    I64Dict(&'a [i64]),
}

struct Dict {
    plain: Vec<u8>,
    count: usize,
    indices: Vec<u32>,
}

fn dict_runs(bytes: &[u8], ends: &[u32]) -> Dict {
    let (mut plain, mut indices) = (Vec::new(), Vec::with_capacity(ends.len()));
    let (mut count, mut s, mut prev): (usize, usize, Option<&[u8]>) = (0, 0, None);
    for &e in ends {
        let v = &bytes[s..e as usize];
        s = e as usize;
        if prev != Some(v) {
            plain.extend_from_slice(&(v.len() as u32).to_le_bytes());
            plain.extend_from_slice(v);
            count += 1;
            prev = Some(v);
        }
        indices.push(count as u32 - 1);
    }
    Dict { plain, count, indices }
}

fn dict_small<T: Copy + Into<i64>>(values: &[T], le: impl Fn(T, &mut Vec<u8>)) -> Option<Dict> {
    let (lo, hi) = values.iter().fold((i64::MAX, i64::MIN), |(lo, hi), &v| (lo.min(v.into()), hi.max(v.into())));
    if values.is_empty() || hi - lo >= 1 << 16 {
        return None;
    }
    let mut slot = vec![u32::MAX; (hi - lo + 1) as usize];
    let (mut plain, mut indices, mut count) = (Vec::new(), Vec::with_capacity(values.len()), 0u32);
    for &v in values {
        let at = &mut slot[(v.into() - lo) as usize];
        if *at == u32::MAX {
            *at = count;
            count += 1;
            le(v, &mut plain);
        }
        indices.push(*at);
    }
    Some(Dict { plain, count: count as usize, indices })
}

fn dict_strings(bytes: &[u8], ends: &[u32]) -> Dict {
    let mut map: rustc_hash::FxHashMap<&[u8], u32> = rustc_hash::FxHashMap::default();
    let (mut plain, mut indices) = (Vec::new(), Vec::with_capacity(ends.len()));
    let mut s = 0usize;
    for &e in ends {
        let v = &bytes[s..e as usize];
        s = e as usize;
        let next = map.len() as u32;
        let i = *map.entry(v).or_insert_with(|| {
            plain.extend_from_slice(&(v.len() as u32).to_le_bytes());
            plain.extend_from_slice(v);
            next
        });
        indices.push(i);
    }
    Dict { plain, count: map.len(), indices }
}

fn dict_ints<T: Copy + Eq + std::hash::Hash>(values: &[T], le: impl Fn(T, &mut Vec<u8>)) -> Dict {
    let mut map: rustc_hash::FxHashMap<T, u32> = rustc_hash::FxHashMap::default();
    let (mut plain, mut indices) = (Vec::new(), Vec::with_capacity(values.len()));
    for &v in values {
        let next = map.len() as u32;
        let i = *map.entry(v).or_insert_with(|| {
            le(v, &mut plain);
            next
        });
        indices.push(i);
    }
    Dict { plain, count: map.len(), indices }
}

fn bit_width(count: usize) -> u8 {
    (usize::BITS - count.saturating_sub(1).leading_zeros()).max(1) as u8
}

fn packed_indices(indices: &[u32], width: u8, out: &mut Vec<u8>) {
    out.push(width);
    let groups = indices.len().div_ceil(8);
    let mut h = ((groups as u64) << 1) | 1;
    while h >= 0x80 {
        out.push((h as u8) | 0x80);
        h >>= 7;
    }
    out.push(h as u8);
    let w = u32::from(width);
    let (mut acc, mut bits) = (0u64, 0u32);
    for g in 0..groups {
        for k in 0..8 {
            let v = u64::from(indices.get(g * 8 + k).copied().unwrap_or(0));
            acc |= v << bits;
            bits += w;
            while bits >= 8 {
                out.push(acc as u8);
                acc >>= 8;
                bits -= 8;
            }
        }
    }
    debug_assert_eq!(bits, 0);
}

fn dict_page_header(size: usize, count: usize) -> Vec<u8> {
    let mut t = T::new();
    t.i32f(1, 2);
    t.i32f(2, size as i32);
    t.i32f(3, size as i32);
    t.begin(7);
    t.i32f(1, count as i32);
    t.i32f(2, 0);
    t.end();
    t.b.push(0);
    t.b
}

struct T {
    b: Vec<u8>,
    last: Vec<i16>,
}

impl T {
    fn new() -> Self {
        T { b: Vec::new(), last: vec![0] }
    }
    fn varint(&mut self, mut v: u64) {
        while v >= 0x80 {
            self.b.push((v as u8) | 0x80);
            v >>= 7;
        }
        self.b.push(v as u8);
    }
    fn zz(&mut self, v: i64) {
        self.varint(((v << 1) ^ (v >> 63)) as u64);
    }
    fn field(&mut self, id: i16, ty: u8) {
        let last = *self.last.last().unwrap();
        let d = id - last;
        if d > 0 && d <= 15 {
            self.b.push(((d as u8) << 4) | ty);
        } else {
            self.b.push(ty);
            self.zz(i64::from(id));
        }
        *self.last.last_mut().unwrap() = id;
    }
    fn i32f(&mut self, id: i16, v: i32) {
        self.field(id, 5);
        self.zz(i64::from(v));
    }
    fn i64f(&mut self, id: i16, v: i64) {
        self.field(id, 6);
        self.zz(v);
    }
    fn strf(&mut self, id: i16, s: &[u8]) {
        self.field(id, 8);
        self.varint(s.len() as u64);
        self.b.extend_from_slice(s);
    }
    fn begin(&mut self, id: i16) {
        self.field(id, 12);
        self.last.push(0);
    }
    fn begin_elem(&mut self) {
        self.last.push(0);
    }
    fn end(&mut self) {
        self.b.push(0);
        self.last.pop();
    }
    fn list(&mut self, id: i16, elem: u8, n: usize) {
        self.field(id, 9);
        if n < 15 {
            self.b.push(((n as u8) << 4) | elem);
        } else {
            self.b.push(0xF0 | elem);
            self.varint(n as u64);
        }
    }
}

const BYTE_ARRAY: i32 = 6;
const INT32: i32 = 1;
const INT64: i32 = 2;

fn page_header(uncompressed: usize, num_values: usize, encoding: i32) -> Vec<u8> {
    let mut t = T::new();
    t.i32f(1, 0);
    t.i32f(2, uncompressed as i32);
    t.i32f(3, uncompressed as i32);
    t.begin(5);
    t.i32f(1, num_values as i32);
    t.i32f(2, encoding);
    t.i32f(3, 3);
    t.i32f(4, 3);
    t.end();
    t.b.push(0);
    t.b
}

fn def_levels(present: &[bool], out: &mut Vec<u8>) {
    let groups = present.len().div_ceil(8);
    let mut body = Vec::with_capacity(groups + 10);
    let mut h = ((groups as u64) << 1) | 1;
    while h >= 0x80 {
        body.push((h as u8) | 0x80);
        h >>= 7;
    }
    body.push(h as u8);
    for g in 0..groups {
        let mut byte = 0u8;
        for k in 0..8 {
            if present.get(g * 8 + k).copied().unwrap_or(false) {
                byte |= 1 << k;
            }
        }
        body.push(byte);
    }
    out.extend_from_slice(&(body.len() as u32).to_le_bytes());
    out.extend_from_slice(&body);
}

fn plain_strings(bytes: &[u8], ends: &[u32], out: &mut Vec<u8>) {
    let mut s = 0usize;
    for &e in ends {
        let e = e as usize;
        out.extend_from_slice(&((e - s) as u32).to_le_bytes());
        out.extend_from_slice(&bytes[s..e]);
        s = e;
    }
}

pub const MAX_PAGE: usize = 1 << 30;

pub fn write(path: &std::path::Path, schema_name: &str, names: &[&str], cols: &[Col], rows: usize) -> std::io::Result<()> {
    let mut file = Vec::with_capacity(rows * 48 + 4096);
    file.extend_from_slice(b"PAR1");
    let mut chunks = Vec::new();
    let mut page = Vec::new();
    for (name, col) in names.iter().zip(cols) {
        page.clear();
        let dict = match col {
            Col::StrDict { bytes, ends } => Some((BYTE_ARRAY, dict_runs(bytes, ends))),
            Col::OptStrDict { bytes, ends, .. } => Some((BYTE_ARRAY, dict_strings(bytes, ends))),
            Col::DateDict(v) => Some((INT32, dict_small(v, |x, out| out.extend_from_slice(&x.to_le_bytes())).unwrap_or_else(|| dict_ints(v, |x, out| out.extend_from_slice(&x.to_le_bytes()))))),
            Col::I64Dict(v) => Some((INT64, dict_small(v, |x, out| out.extend_from_slice(&x.to_le_bytes())).unwrap_or_else(|| dict_ints(v, |x, out| out.extend_from_slice(&x.to_le_bytes()))))),
            _ => None,
        };
        if let Some((ty, d)) = dict {
            let dict_offset = file.len();
            let dh = dict_page_header(d.plain.len(), d.count);
            file.extend_from_slice(&dh);
            file.extend_from_slice(&d.plain);
            if let Col::OptStrDict { present, .. } = col {
                def_levels(present, &mut page);
            }
            packed_indices(&d.indices, bit_width(d.count), &mut page);
            let header = page_header(page.len(), rows, 8);
            let offset = file.len();
            file.extend_from_slice(&header);
            file.extend_from_slice(&page);
            chunks.push((ty, *name, offset, file.len() - dict_offset, Some(dict_offset)));
            continue;
        }
        let ty = match col {
            Col::Str { bytes, ends } => {
                plain_strings(bytes, ends, &mut page);
                BYTE_ARRAY
            }
            Col::OptStr { bytes, ends, present } => {
                def_levels(present, &mut page);
                plain_strings(bytes, ends, &mut page);
                BYTE_ARRAY
            }
            Col::Date(v) => {
                for x in v.iter() {
                    page.extend_from_slice(&x.to_le_bytes());
                }
                INT32
            }
            Col::I64(v) => {
                for x in v.iter() {
                    page.extend_from_slice(&x.to_le_bytes());
                }
                INT64
            }
            Col::StrDict { .. } | Col::OptStrDict { .. } | Col::DateDict(_) | Col::I64Dict(_) => unreachable!(),
        };
        let header = page_header(page.len(), rows, 0);
        let offset = file.len();
        file.extend_from_slice(&header);
        file.extend_from_slice(&page);
        chunks.push((ty, *name, offset, header.len() + page.len(), None));
    }

    let mut t = T::new();
    t.i32f(1, 1);
    t.list(2, 12, cols.len() + 1);
    t.begin_elem();
    t.strf(4, schema_name.as_bytes());
    t.i32f(5, cols.len() as i32);
    t.end();
    for (name, col) in names.iter().zip(cols) {
        t.begin_elem();
        let (ty, rep, conv, logical) = match col {
            Col::Str { .. } | Col::StrDict { .. } => (BYTE_ARRAY, 0, Some(0), Some(1)),
            Col::OptStr { .. } | Col::OptStrDict { .. } => (BYTE_ARRAY, 1, Some(0), Some(1)),
            Col::Date(_) | Col::DateDict(_) => (INT32, 0, Some(6), Some(6)),
            Col::I64(_) | Col::I64Dict(_) => (INT64, 0, None, None),
        };
        t.i32f(1, ty);
        t.i32f(3, rep);
        t.strf(4, name.as_bytes());
        if let Some(c) = conv {
            t.i32f(6, c);
        }
        if let Some(l) = logical {
            t.begin(10);
            t.begin(l);
            t.end();
            t.end();
        }
        t.end();
    }
    t.i64f(3, rows as i64);
    t.list(4, 12, 1);
    t.begin_elem();
    t.list(1, 12, chunks.len());
    let total: usize = chunks.iter().map(|c| c.3).sum();
    for &(ty, name, offset, size, dict_offset) in &chunks {
        t.begin_elem();
        t.i64f(2, dict_offset.unwrap_or(offset) as i64);
        t.begin(3);
        t.i32f(1, ty);
        if dict_offset.is_some() {
            t.list(2, 5, 3);
            t.zz(0);
            t.zz(3);
            t.zz(8);
        } else {
            t.list(2, 5, 2);
            t.zz(0);
            t.zz(3);
        }
        t.list(3, 8, 1);
        t.varint(name.len() as u64);
        t.b.extend_from_slice(name.as_bytes());
        t.i32f(4, 0);
        t.i64f(5, rows as i64);
        t.i64f(6, size as i64);
        t.i64f(7, size as i64);
        t.i64f(9, offset as i64);
        if let Some(d) = dict_offset {
            t.i64f(11, d as i64);
        }
        t.end();
        t.end();
    }
    t.i64f(2, total as i64);
    t.i64f(3, rows as i64);
    t.end();
    t.b.push(0);
    file.extend_from_slice(&t.b);
    file.extend_from_slice(&(t.b.len() as u32).to_le_bytes());
    file.extend_from_slice(b"PAR1");
    std::fs::File::create(path)?.write_all(&file)
}
