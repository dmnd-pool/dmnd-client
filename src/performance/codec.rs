use binary_sv2::GetSize;
use roles_logic_sv2::parsers::Mining;
use std::{
    alloc::{GlobalAlloc, Layout, System},
    hint::black_box,
    sync::atomic::{AtomicUsize, Ordering},
    time::Instant,
};

#[cfg(not(feature = "native"))]
use roles_logic_sv2::mining_sv2::SubmitSharesExtended as Share;
#[cfg(feature = "native")]
use roles_logic_sv2::{
    mining_sv2::SubmitSharesExtendedOwned as Share, parsers::MiningOwned as Message,
};

#[cfg(feature = "native")]
type Frame = demand_sv2_connection::StandardSv2Frame<Message>;
#[cfg(not(feature = "native"))]
type Message = Mining<'static>;
#[cfg(not(feature = "native"))]
type Frame = framing_sv2::framing::Sv2Frame<Message, Vec<u8>>;

struct Count;
static ALLOCS: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for Count {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        System.alloc(layout)
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        System.dealloc(pointer, layout);
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        System.realloc(pointer, layout, size)
    }
}

#[global_allocator]
static ALLOCATOR: Count = Count;

fn message() -> Message {
    Message::SubmitSharesExtended(Share {
        channel_id: 1,
        sequence_number: 2,
        job_id: 3,
        nonce: 4,
        ntime: 5,
        version: 6,
        extranonce: vec![7; 32].try_into().unwrap(),
    })
}

fn measure(name: &str, iterations: usize, mut action: impl FnMut()) {
    action();
    let before = ALLOCS.load(Ordering::Relaxed);
    let start = Instant::now();
    for _ in 0..iterations {
        action();
    }
    let time = start.elapsed();
    let allocations = ALLOCS.load(Ordering::Relaxed) - before;
    println!(
        "{name}: {:.2} ns/op, {:.4} allocations/op",
        time.as_nanos() as f64 / iterations as f64,
        allocations as f64 / iterations as f64,
    );
}

fn main() {
    let message = message();
    let frame = Frame::from_message(message.clone(), 0x1b, 0, true).unwrap();
    for _ in 0..5 {
        measure("message_size", 5_000_000, || {
            black_box(black_box(&message).get_size());
        });
        measure("frame_size", 5_000_000, || {
            black_box(black_box(&frame).encoded_length());
        });
        measure("share_encode", 100_000, || {
            let message = self::message();
            let mut bytes = [0u8; 57];
            binary_sv2::to_writer(message, &mut bytes).unwrap();
            black_box(bytes);
        });
        measure("share_decode", 100_000, || {
            let mut bytes = [7u8; 57];
            bytes[24] = 32;
            let message = Mining::try_from((0x1b, bytes.as_mut_slice())).unwrap();
            black_box(message);
        });
    }
}
