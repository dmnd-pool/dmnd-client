use binary_sv2::GetSize;
#[cfg(feature = "native")]
use demand_sv2_connection as _;
#[cfg(not(feature = "native"))]
use framing_sv2 as _;
use std::{
    alloc::{GlobalAlloc, Layout, System},
    hint::black_box,
    sync::atomic::{AtomicUsize, Ordering},
    time::Instant,
};

#[cfg(not(feature = "native"))]
type Template = roles_logic_sv2::template_distribution_sv2::NewTemplate<'static>;
#[cfg(feature = "native")]
type Template = roles_logic_sv2::template_distribution_sv2::NewTemplateOwned;
#[cfg(not(feature = "native"))]
type Declaration = roles_logic_sv2::job_declaration_sv2::DeclareMiningJob<'static>;
#[cfg(feature = "native")]
type Declaration = roles_logic_sv2::job_declaration_sv2::DeclareMiningJobOwned;

struct Count;
static ALLOCS: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for Count {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        System.alloc(layout)
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        System.dealloc(pointer, layout)
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        System.realloc(pointer, layout, size)
    }
}

#[global_allocator]
static ALLOCATOR: Count = Count;

fn template() -> Template {
    let mut outputs = Vec::new();
    for seed in 0..3 {
        outputs.extend_from_slice(&[0; 8]);
        outputs.push(46);
        outputs.extend_from_slice(&[seed; 46]);
    }
    Template {
        template_id: 1,
        future_template: true,
        version: 0x20000000,
        coinbase_tx_version: 2,
        coinbase_prefix: vec![3, 0x40, 0x0d, 0x0c].try_into().unwrap(),
        coinbase_tx_input_sequence: u32::MAX,
        coinbase_tx_value_remaining: 3_125_000_000,
        coinbase_tx_outputs_count: 3,
        coinbase_tx_outputs: outputs.try_into().unwrap(),
        coinbase_tx_locktime: 0,
        merkle_path: (0..12)
            .map(|i| [i; 32].into())
            .collect::<Vec<_>>()
            .try_into()
            .unwrap(),
    }
}

fn declaration(count: usize) -> Declaration {
    let ids = (0..count)
        .map(|i| [(i % 256) as u8; 32].into())
        .collect::<Vec<_>>()
        .try_into()
        .unwrap();
    Declaration {
        request_id: 1,
        mining_job_token: vec![7; 32].try_into().unwrap(),
        version: 0x20000000,
        #[cfg(not(feature = "native"))]
        coinbase_prefix: vec![8; 80].try_into().unwrap(),
        #[cfg(feature = "native")]
        coinbase_tx_prefix: vec![8; 80].try_into().unwrap(),
        #[cfg(not(feature = "native"))]
        coinbase_suffix: vec![9; 160].try_into().unwrap(),
        #[cfg(feature = "native")]
        coinbase_tx_suffix: vec![9; 160].try_into().unwrap(),
        #[cfg(not(feature = "native"))]
        tx_list: ids,
        #[cfg(feature = "native")]
        wtxid_list: ids,
        excess_data: vec![10; 32].try_into().unwrap(),
    }
}

fn measure<M: binary_sv2::Serialize + GetSize + Clone>(name: &str, message: M, iterations: usize) {
    let mut bytes = vec![0; message.get_size()];
    binary_sv2::to_writer(message.clone(), &mut bytes).unwrap();
    let expected = bytes.clone();
    let fingerprint = bytes.iter().fold(14695981039346656037u64, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(1099511628211)
    });
    println!(
        "{name} wire_size={} wire_fnv={fingerprint:016x}",
        bytes.len()
    );
    for round in 0..7 {
        let messages = (0..iterations).map(|_| message.clone()).collect::<Vec<_>>();
        let allocations = ALLOCS.load(Ordering::Relaxed);
        let start = Instant::now();
        for message in messages {
            binary_sv2::to_writer(black_box(message), black_box(&mut bytes)).unwrap();
            black_box(&bytes);
        }
        let time = start.elapsed();
        let allocations = ALLOCS.load(Ordering::Relaxed) - allocations;
        assert_eq!(bytes, expected);
        println!(
            "{name} round={round} encode_ns={:.2} allocations={:.2}",
            time.as_nanos() as f64 / iterations as f64,
            allocations as f64 / iterations as f64
        );
    }
}

fn main() {
    measure("new_template", template(), 10_000);
    measure("declare_500", declaration(500), 1_000);
    measure("declare_2000", declaration(2_000), 250);
}
