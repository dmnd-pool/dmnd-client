use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};
struct Counting;
static ENABLE: AtomicBool = AtomicBool::new(false);
static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if ENABLE.load(Ordering::Relaxed) {
            ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        }
        System.alloc(layout)
    }
    unsafe fn dealloc(&self, p: *mut u8, layout: Layout) {
        System.dealloc(p, layout);
    }
    unsafe fn realloc(&self, p: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        if ENABLE.load(Ordering::Relaxed) {
            ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        }
        System.realloc(p, layout, size)
    }
}
#[global_allocator]
static ALLOCATOR: Counting = Counting;

use tokio::{
    net::{TcpListener, TcpStream},
    sync::mpsc::{Receiver, Sender},
    task::AbortHandle,
};

const PUBLIC: [u8; 32] = [
    0x79, 0xbe, 0x66, 0x7e, 0xf9, 0xdc, 0xbb, 0xac, 0x55, 0xa0, 0x62, 0x95, 0xce, 0x87, 0x0b, 0x07,
    0x02, 0x9b, 0xfc, 0xdb, 0x2d, 0xce, 0x28, 0xd9, 0x59, 0xf2, 0x81, 0x5b, 0x16, 0xf8, 0x17, 0x98,
];
const SECRET: [u8; 32] = [
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1,
];

async fn sockets() -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (client, server) = tokio::join!(
        TcpStream::connect(listener.local_addr().unwrap()),
        listener.accept()
    );
    let client = client.unwrap();
    let server = server.unwrap().0;
    client.set_nodelay(true).unwrap();
    server.set_nodelay(true).unwrap();
    (client, server)
}

type Channels<F> = (Receiver<F>, Sender<F>, AbortHandle, AbortHandle);

async fn measure<F: Send + 'static>(
    name: &str,
    (
        (mut incoming, outgoing, client_read, client_write),
        (mut server_rx, server_tx, server_read, server_write),
    ): (Channels<F>, Channels<F>),
    make: impl Fn(u32) -> F + Send + Copy + 'static,
) {
    let echo = tokio::spawn(async move {
        while let Some(frame) = server_rx.recv().await {
            if server_tx.send(frame).await.is_err() {
                break;
            }
        }
    });
    for sequence in 0..1000 {
        outgoing.send(make(sequence)).await.unwrap();
        drop(incoming.recv().await.unwrap());
    }
    for round in 0..7 {
        ALLOCATIONS.store(0, Ordering::Relaxed);
        ENABLE.store(true, Ordering::Relaxed);
        let started = Instant::now();
        for sequence in 0..10_000 {
            outgoing.send(make(sequence)).await.unwrap();
            drop(incoming.recv().await.unwrap());
        }
        let elapsed = started.elapsed();
        ENABLE.store(false, Ordering::Relaxed);
        println!(
            "{name} round={round} roundtrip_ns={:.1} allocations_per_roundtrip={:.4}",
            elapsed.as_nanos() as f64 / 10_000.0,
            ALLOCATIONS.load(Ordering::Relaxed) as f64 / 10_000.0
        );
    }
    for round in 0..7 {
        let sender = outgoing.clone();
        let sending = tokio::spawn(async move {
            for _ in 0..100_000 {
                sender.send(make(7)).await.unwrap();
            }
        });
        let started = Instant::now();
        for _ in 0..100_000 {
            drop(incoming.recv().await.unwrap());
        }
        sending.await.unwrap();
        println!(
            "{name} pipeline_round={round} frames_per_second={:.1}",
            100_000.0 / started.elapsed().as_secs_f64()
        );
    }
    drop(outgoing);
    drop(incoming);
    echo.abort();
    client_read.abort();
    client_write.abort();
    server_read.abort();
    server_write.abort();
    tokio::time::sleep(Duration::from_millis(100)).await;
}

async fn old() {
    let (client, server) = sockets().await;
    let initiator =
        old_codec::HandshakeRole::Initiator(old_codec::Initiator::from_raw_k(PUBLIC).unwrap());
    let responder = old_codec::HandshakeRole::Responder(
        old_codec::Responder::from_authority_kp(&PUBLIC, &SECRET, Duration::from_secs(60)).unwrap(),
    );
    let (client, server) = tokio::join!(
        old_transport::noise_connection_tokio::Connection::new::<u32>(client, initiator),
        old_transport::noise_connection_tokio::Connection::new::<u32>(server, responder),
    );
    measure("old", (client.unwrap(), server.unwrap()), |value| {
        old_codec::StandardSv2Frame::from_message(value, 0x42, 0, false)
            .unwrap()
            .into()
    })
    .await;
}

async fn new() {
    let (client, server) = sockets().await;
    let initiator = new_transport::HandshakeRole::Initiator(
        new_transport::Initiator::from_raw_k(PUBLIC).unwrap(),
    );
    let responder = new_transport::HandshakeRole::Responder(
        new_transport::Responder::from_authority_kp(&PUBLIC, &SECRET, Duration::from_secs(60))
            .unwrap(),
    );
    let (client, server) = tokio::join!(
        new_transport::noise_connection_tokio::Connection::new::<u32>(client, initiator),
        new_transport::noise_connection_tokio::Connection::new::<u32>(server, responder),
    );
    measure("new", (client.unwrap(), server.unwrap()), |value| {
        new_transport::StandardSv2Frame::from_message(value, 0x42, 0, false)
            .unwrap()
            .into()
    })
    .await;
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    if std::env::args().any(|arg| arg == "reverse") {
        new().await;
        old().await;
    } else {
        old().await;
        new().await;
    }
}
