use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicUsize, Ordering};

pub const BLOCK: usize = 128;
const HUGE: usize = 2 << 20;
const SLAB: usize = 2 << 20;
const FALLBACK_CHUNK: usize = 64 << 20;

static RES_NEXT: AtomicUsize = AtomicUsize::new(0);
static RES_END: AtomicUsize = AtomicUsize::new(0);

fn map(bytes: usize) -> usize {
    let len = bytes.div_ceil(HUGE) * HUGE;

    unsafe {
        let p = libc::mmap(std::ptr::null_mut(), len + HUGE, libc::PROT_READ | libc::PROT_WRITE,
                           libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_NORESERVE, -1, 0);
        assert!(p != libc::MAP_FAILED, "mmap failed");
        let aligned = (p as usize + HUGE - 1) & !(HUGE - 1);
        libc::madvise(aligned as *mut libc::c_void, len, libc::MADV_HUGEPAGE);
        aligned
    }
}

pub fn reserve(bytes: usize) {
    let base = map(bytes);
    RES_NEXT.store(base, Ordering::Relaxed);
    RES_END.store(base + bytes, Ordering::Relaxed);
}

pub fn slab(bytes: usize) -> (usize, usize) {
    let size = bytes.div_ceil(SLAB) * SLAB;
    if RES_END.load(Ordering::Relaxed) != 0 {
        let p = RES_NEXT.fetch_add(size, Ordering::Relaxed);
        if p + size <= RES_END.load(Ordering::Relaxed) {
            return (p, p + size);
        }
    }
    let size = size.max(FALLBACK_CHUNK);
    let p = map(size);
    (p, p + size)
}

pub struct Arena {
    cur: usize,
    end: usize,
    free: Vec<(usize, usize)>,
}

impl Arena {
    fn new() -> Self {
        Arena { cur: 0, end: 0, free: Vec::new() }
    }

    fn alloc(&mut self, bytes: usize, align: usize) -> usize {
        let mut p = (self.cur + align - 1) & !(align - 1);
        if p + bytes > self.end {
            loop {
                match self.free.pop() {
                    Some((s, e)) => {
                        let q = (s + align - 1) & !(align - 1);
                        if q + bytes <= e {
                            self.cur = q + bytes;
                            self.end = e;
                            return q;
                        }
                    }
                    None => break,
                }
            }
            let (s, e) = slab(bytes + align);
            self.cur = s;
            self.end = e;
            p = (s + align - 1) & !(align - 1);
        }
        self.cur = p + bytes;
        p
    }
}

pub struct BlockScatter<T> {
    arena: Arena,

    cur: Vec<(*mut T, u32)>,

    lists: Vec<Vec<*mut T>>,

    pending: Vec<MaybeUninit<T>>,
    has_pending: Vec<bool>,
    paired: bool,
}

unsafe impl<T: Send> Send for BlockScatter<T> {}
unsafe impl<T: Sync> Sync for BlockScatter<T> {}

impl<T: Copy> BlockScatter<T> {
    pub fn new(parts: usize) -> Self {
        let mut pending = Vec::with_capacity(parts);

        unsafe { pending.set_len(parts) };
        BlockScatter {
            arena: Arena::new(),
            cur: vec![(std::ptr::null_mut(), BLOCK as u32); parts],
            lists: (0..parts).map(|_| Vec::new()).collect(),
            pending,
            has_pending: vec![false; parts],
            paired: false,
        }
    }

    pub fn new_paired(parts: usize) -> Self {
        BlockScatter { paired: true, ..Self::new(parts) }
    }

    #[cold]
    #[inline(never)]
    fn new_block(&mut self, part: usize) {
        let p = self.arena.alloc(BLOCK * std::mem::size_of::<T>(), 64) as *mut T;
        self.lists[part].push(p);
        self.cur[part] = (p, 0);
    }

    #[inline(always)]
    pub fn push(&mut self, part: usize, item: T) {
        if self.cur[part].1 as usize == BLOCK {
            self.new_block(part);
        }
        let (block, fill) = self.cur[part];
        #[cfg(all(target_arch = "x86_64", target_feature = "avx512f"))]
        if self.paired && std::mem::size_of::<T>() == 32 {
            if fill % 2 == 0 {
                self.pending[part].write(item);
                self.has_pending[part] = true;
                self.cur[part].1 = fill + 1;
                return;
            }

            unsafe {
                use std::arch::x86_64::{__m256i, __m512i, _mm512_castsi256_si512, _mm512_inserti64x4, _mm512_stream_si512};
                let a = std::mem::transmute_copy::<T, __m256i>(self.pending[part].assume_init_ref());
                let b = std::mem::transmute_copy::<T, __m256i>(&item);
                let line: __m512i = _mm512_inserti64x4::<1>(_mm512_castsi256_si512(a), b);
                _mm512_stream_si512(block.add(fill as usize - 1).cast(), line);
            }
            self.has_pending[part] = false;
            self.cur[part].1 = fill + 1;
            return;
        }

        unsafe { block.add(fill as usize).write(item) };
        self.cur[part].1 = fill + 1;
    }

    pub fn flush(&mut self) {
        for part in 0..self.cur.len() {
            if self.has_pending[part] {
                self.has_pending[part] = false;
                let (block, fill) = self.cur[part];

                unsafe { block.add(fill as usize - 1).write(self.pending[part].assume_init()) };
            }
        }
    }

    pub fn len(&self, part: usize) -> usize {
        match self.lists[part].len() {
            0 => 0,
            n => (n - 1) * BLOCK + self.cur[part].1 as usize,
        }
    }

    pub fn pieces(&self, part: usize) -> impl Iterator<Item = &[T]> + '_ {
        let n = self.lists[part].len();
        let last = self.cur[part].1 as usize;
        self.lists[part].iter().enumerate().map(move |(i, &p)| {
            let len = if i + 1 == n { last } else { BLOCK };

            unsafe { std::slice::from_raw_parts(p as *const T, len) }
        })
    }

    pub unsafe fn donate(&mut self, start: usize, end: usize) {
        self.arena.free.push((start, end));
    }

    pub unsafe fn recycle_partition<U: Copy>(&self, part: usize, to: &mut BlockScatter<U>) {
        let bytes = BLOCK * std::mem::size_of::<T>();
        for &p in &self.lists[part] {
            to.arena.free.push((p as usize, p as usize + bytes));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Copy, PartialEq, Debug)]
    #[repr(C, align(32))]
    struct Item([u64; 4]);

    #[test]
    fn round_trip_plain_and_paired() {
        for paired in [false, true] {
            let mut s = if paired { BlockScatter::new_paired(3) } else { BlockScatter::new(3) };
            let item = |i: u64| Item([i, i + 1, i + 2, i + 3]);
            for i in 0..1000u64 {
                s.push((i % 3) as usize, item(i));
            }
            s.flush();
            for part in 0..3 {
                let got: Vec<Item> = s.pieces(part).flat_map(|b| b.iter().copied()).collect();
                let want: Vec<Item> = (0..1000).filter(|i| i % 3 == part as u64).map(item).collect();
                assert_eq!(got, want, "paired {paired} part {part}");
                assert_eq!(s.len(part), want.len());
            }
        }
    }

    #[test]
    fn recycled_blocks_are_reused() {
        let mut a: BlockScatter<Item> = BlockScatter::new(2);
        for i in 0..500u64 {
            a.push((i % 2) as usize, Item([i; 4]));
        }
        let mut b: BlockScatter<Item> = BlockScatter::new(1);
        let first = a.lists[0][0] as usize;
        unsafe { a.recycle_partition(0, &mut b) };
        for i in 0..10u64 {
            b.push(0, Item([i; 4]));
        }
        let reused = b.lists[0][0] as usize;
        assert!(a.lists[0].iter().any(|&p| p as usize == reused), "b took a recycled block");
        assert_eq!(b.pieces(0).next().unwrap()[3], Item([3; 4]));
        let _ = first;

        assert_eq!(a.len(1), 250);
    }
}
