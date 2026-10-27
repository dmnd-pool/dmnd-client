#[cfg(feature = "native")]
use binary_sv2 as _;
#[cfg(not(feature = "native"))]
use binary_sv2::Sv2DataType;
#[cfg(feature = "native")]
use bitcoin::hashes::HashEngine;
use bitcoin::{
    block::{Header, Version},
    hashes::{sha256d, Hash as BHash},
    hex::DisplayHex,
    BlockHash, CompactTarget,
};
use primitive_types::U256;
use roles_logic_sv2::utils::from_u128_to_u256;
use std::{
    alloc::{GlobalAlloc, Layout, System},
    hint::black_box,
    ops::{Div, Mul},
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    time::Instant,
};
use sv1_api::{
    client_to_server,
    server_to_client::Notify,
    utils::{HexU32Be, MerkleNode, PrevHash},
};
use tracing::{debug, error, info};

struct Allocator;
static COUNT: AtomicUsize = AtomicUsize::new(0);
static RECORD: AtomicBool = AtomicBool::new(false);

unsafe impl GlobalAlloc for Allocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if RECORD.load(Ordering::Relaxed) {
            COUNT.fetch_add(1, Ordering::Relaxed);
        }
        System.alloc(layout)
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        if RECORD.load(Ordering::Relaxed) {
            COUNT.fetch_add(1, Ordering::Relaxed);
        }
        System.realloc(pointer, layout, size)
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        System.dealloc(pointer, layout);
    }
}

#[global_allocator]
static ALLOCATOR: Allocator = Allocator;

fn share_log_enabled() -> bool {
    false
}

struct Downstream;

fn main() {
    let job = Notify {
        job_id: "17".to_string(),
        prev_hash: PrevHash([3; 32].into()),
        coin_base1: vec![0x51; 180].into(),
        coin_base2: vec![0x21; 48].into(),
        merkle_branch: (0..12)
            .map(|value| MerkleNode([value; 32].into()))
            .collect(),
        version: HexU32Be(0x20000000),
        bits: HexU32Be(0x1d00ffff),
        time: HexU32Be(1700000000),
        clean_jobs: true,
    };
    let mut request = client_to_server::Submit {
        user_name: "worker.1".to_string(),
        job_id: "17".to_string(),
        extra_nonce2: vec![5; 8].try_into().unwrap(),
        time: HexU32Be(1700000000),
        nonce: HexU32Be(0),
        version_bits: None,
        id: 11,
    };
    let extranonce = [7; 8];
    for nonce in 0..10000 {
        request.nonce.0 = nonce;
        black_box(validate_share(
            black_box(&request),
            black_box(&job),
            black_box(1.0),
            black_box(&extranonce),
            None,
        ));
    }
    COUNT.store(0, Ordering::Relaxed);
    RECORD.store(true, Ordering::Relaxed);
    for nonce in 0..1000 {
        request.nonce.0 = nonce;
        black_box(validate_share(
            black_box(&request),
            black_box(&job),
            black_box(1.0),
            black_box(&extranonce),
            None,
        ));
    }
    RECORD.store(false, Ordering::Relaxed);
    println!(
        "allocations/call {:.1}",
        COUNT.load(Ordering::Relaxed) as f64 / 1000.0
    );
    for round in 0..9 {
        let start = Instant::now();
        for nonce in 0..150000 {
            request.nonce.0 = nonce;
            black_box(validate_share(
                black_box(&request),
                black_box(&job),
                black_box(1.0),
                black_box(&extranonce),
                None,
            ));
        }
        println!(
            "round {} ns/call {:.2}",
            round,
            start.elapsed().as_nanos() as f64 / 150000.0
        );
    }
}
