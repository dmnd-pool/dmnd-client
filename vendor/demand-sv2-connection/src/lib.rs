mod frames;
pub mod noise_connection_tokio;

pub use frames::{StandardEitherFrame, StandardSv2Frame};
pub use noise_sv2;
pub use noise_sv2::{Initiator, Responder};

#[derive(Debug)]
pub enum HandshakeRole {
    Initiator(Box<Initiator>),
    Responder(Box<Responder>),
}

#[derive(Debug)]
pub enum Error {
    HandshakeRemoteInvalidMessage,
    CodecError(codec_sv2::Error),
    RecvError,
    SendError,
    SocketClosed,
    Io(std::io::Error),
}

impl From<codec_sv2::Error> for Error {
    fn from(error: codec_sv2::Error) -> Self {
        Self::CodecError(error)
    }
}

impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

impl<T> From<tokio::sync::mpsc::error::SendError<T>> for Error {
    fn from(_: tokio::sync::mpsc::error::SendError<T>) -> Self {
        Self::SendError
    }
}

#[cfg(test)]
mod test_support {
    use std::{
        alloc::{GlobalAlloc, Layout, System},
        cell::Cell,
    };

    thread_local! {
        static COUNT: Cell<Option<usize>> = const { Cell::new(None) };
    }

    pub struct CountingAllocator;

    unsafe impl GlobalAlloc for CountingAllocator {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            COUNT.with(|count| {
                if let Some(value) = count.get() {
                    count.set(Some(value + 1));
                }
            });
            System.alloc(layout)
        }

        unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
            System.dealloc(pointer, layout);
        }

        unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
            COUNT.with(|count| {
                if let Some(value) = count.get() {
                    count.set(Some(value + 1));
                }
            });
            System.realloc(pointer, layout, size)
        }
    }

    pub fn allocations(action: impl FnOnce()) -> usize {
        COUNT.with(|count| count.set(Some(0)));
        action();
        COUNT.with(|count| count.replace(None).unwrap())
    }
}

#[cfg(test)]
#[global_allocator]
static ALLOCATOR: test_support::CountingAllocator = test_support::CountingAllocator;
