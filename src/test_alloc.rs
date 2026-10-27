use std::{
    alloc::{GlobalAlloc, Layout},
    cell::Cell,
};

thread_local! {
    static COUNT: Cell<Option<usize>> = const { Cell::new(None) };
}

struct CountingAllocator;

#[cfg(all(not(target_os = "windows"), feature = "jemalloc"))]
static INNER: jemallocator::Jemalloc = jemallocator::Jemalloc;
#[cfg(not(all(not(target_os = "windows"), feature = "jemalloc")))]
static INNER: std::alloc::System = std::alloc::System;

fn record_allocation() {
    COUNT.with(|count| {
        if let Some(value) = count.get() {
            count.set(Some(value + 1));
        }
    });
}

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record_allocation();
        INNER.alloc(layout)
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        record_allocation();
        INNER.alloc_zeroed(layout)
    }
    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        record_allocation();
        INNER.realloc(pointer, layout, size)
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        INNER.dealloc(pointer, layout);
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

pub(crate) fn allocations(action: impl FnOnce()) -> usize {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            COUNT.with(|count| count.set(None));
        }
    }
    COUNT.with(|count| count.set(Some(0)));
    let _reset = Reset;
    action();
    COUNT.with(|count| count.get().unwrap())
}
