use memmap2::MmapMut;
use std::fs::OpenOptions;
use std::path::Path;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use crate::orderbook::order::ShmOrder;


//#[repr(C)]
//#[derive(Clone, Copy, Debug)]

//pub struct Order {
//    // All u64s first (8-byte aligned)
//    pub order_id: u64,
//    pub price: u64,
//    pub timestamp: u64,
//    // Then u32s (4-byte aligned)
//    pub client_id: u32,
//    pub quantity: u32,
//    // Then u8s (1-byte aligned)
//    pub side: u8,   // 0=buy, 1=sell
//    pub status: u8, // 0=pending, 1=filled, 2=rejected
//    // Array of bytes last
//    pub symbol: [u8; 8],
//}
//


// QueueHeader with cache-line padding matching Go
#[repr(C)]
pub struct QueueHeader {
    producer_head: AtomicU64, // offset 0
    _pad1: [u8; 56],          // pad to 64B
    consumer_tail: AtomicU64, // offset 64
    _pad2: [u8; 56],          // pad to 128B
    magic: AtomicU32,         // offset 128
    capacity: AtomicU32,      // offset 132
}

const QUEUE_MAGIC: u32 = 0xDEADBEEF;
const QUEUE_CAPACITY: usize = 65536;
const ORDER_SIZE: usize = std::mem::size_of::<ShmOrder>();
const HEADER_SIZE: usize = std::mem::size_of::<QueueHeader>();
const TOTAL_SIZE: usize = HEADER_SIZE + (QUEUE_CAPACITY * ORDER_SIZE);

// Compile-time layout assertions (fail build if wrong)
const _: () = assert!(ORDER_SIZE == 48, "Order must be 43 bytes");
const _: () = assert!(HEADER_SIZE == 136, "QueueHeader must be 136 bytes");
const _: () = {
    // Verify ConsumerTail is at offset 64
    assert!(
        std::mem::offset_of!(QueueHeader, consumer_tail) == 64,
        "ConsumerTail must be at offset 64"
    );
};

#[derive(Debug)]
pub struct Queue {
    mmap: MmapMut,
    header_ptr: *mut QueueHeader, // Cached pointer
    orders_ptr: *mut ShmOrder,       // Cached orders pointer
}

impl Queue {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, QueueError> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .map_err(|e| QueueError::FileOpen(e.to_string()))?;

        let metadata = file
            .metadata()
            .map_err(|e| QueueError::FileStat(e.to_string()))?;
        if metadata.len() != TOTAL_SIZE as u64 {
            return Err(QueueError::InvalidSize {
                got: metadata.len(),
                expected: TOTAL_SIZE as u64,
            });
        }

        let mut mmap =
            unsafe { MmapMut::map_mut(&file) }.map_err(|e| QueueError::Mmap(e.to_string()))?;

        if let Err(e) = mmap.lock() {
            eprintln!("Warning: failed to mlock: {}", e);
        }

        // Cache both pointers
        let header_ptr = { mmap.as_mut_ptr() as *mut QueueHeader };
        let orders_ptr = unsafe { mmap.as_mut_ptr().add(HEADER_SIZE) as *mut ShmOrder };

        // Validate
        let header = unsafe { &*header_ptr };
        let magic = header.magic.load(Ordering::Relaxed);
        if magic != QUEUE_MAGIC {
            return Err(QueueError::InvalidMagic { got: magic });
        }

        let capacity = header.capacity.load(Ordering::Relaxed);
        if capacity != QUEUE_CAPACITY as u32 {
            return Err(QueueError::CapacityMismatch {
                got: capacity,
                expected: QUEUE_CAPACITY as u32,
            });
        }

        Ok(Queue {
            mmap,
            header_ptr,
            orders_ptr,
        })
    }

    /// Get mutable header reference - ZERO COST
    #[inline(always)]
    fn header_mut(&self) -> &mut QueueHeader {
        unsafe { &mut *self.header_ptr }
    }

    /// Get immutable header reference - ZERO COST
    #[inline(always)]
    fn header(&self) -> &QueueHeader {
        unsafe { &*self.header_ptr }
    }

    /// Get order at position - ZERO COST pointer arithmetic
    #[inline(always)]
    fn get_order(&self, pos: usize) -> ShmOrder {
        unsafe { *self.orders_ptr.add(pos) }
    }

    /// Set order at position - ZERO COST pointer arithmetic
    #[inline(always)]
    fn set_order(&self, pos: usize, order: ShmOrder) {
        unsafe {
            *self.orders_ptr.add(pos) = order;
        }
    }

    /// ULTRA-FAST dequeue - all pointers cached, no borrows
    #[inline]
    pub fn dequeue(&mut self) -> Result<Option<ShmOrder>, QueueError> {
        let header = self.header_mut();

        let producer_head = header.producer_head.load(Ordering::Acquire);
        let consumer_tail = header.consumer_tail.load(Ordering::Relaxed);

        if consumer_tail == producer_head {
            return Ok(None);
        }

        let pos = (consumer_tail % QUEUE_CAPACITY as u64) as usize;
        std::sync::atomic::fence(Ordering::Acquire);
        let order = self.get_order(pos);

        header
            .consumer_tail
            .store(consumer_tail + 1, Ordering::Release);

        Ok(Some(order))
    }

    pub fn enqueue(&mut self, order: ShmOrder) -> Result<(), QueueError> {
        let header = self.header_mut();

        let consumer_tail = header.consumer_tail.load(Ordering::Acquire);
        let producer_head = header.producer_head.load(Ordering::Relaxed);

        let next_head = producer_head + 1;

        if next_head - consumer_tail > QUEUE_CAPACITY as u64 {
            return Err(QueueError::QueueFull {
                depth: next_head - consumer_tail,
            });
        }

        let pos = (producer_head % QUEUE_CAPACITY as u64) as usize;
        self.set_order(pos, order);

        header.producer_head.store(next_head, Ordering::Release);

        Ok(())
    }

    pub fn depth(&self) -> u64 {
        let header = self.header();
        let producer_head = header.producer_head.load(Ordering::Relaxed);
        let consumer_tail = header.consumer_tail.load(Ordering::Relaxed);
        producer_head.saturating_sub(consumer_tail)
    }

    pub fn capacity(&self) -> u64 {
        QUEUE_CAPACITY as u64
    }

    pub fn flush(&self) -> Result<(), QueueError> {
        self.mmap
            .flush()
            .map_err(|e| QueueError::Flush(e.to_string()))
    }

    pub fn dequeue_spin(&mut self, max_spins: usize) -> Result<Option<ShmOrder>, QueueError> {
        for _ in 0..max_spins {
            match self.dequeue()? {
                Some(order) => return Ok(Some(order)),
                None => std::hint::spin_loop(),
            }
        }
        Ok(None)
    }
}

impl Drop for Queue {
    fn drop(&mut self) {
        // Flush before closing
        let _ = self.mmap.flush();
        // Unlock pages (memmap2 handles this automatically)
        let _ = self.mmap.unlock();
    }
}

// Error types
#[derive(Debug , Clone)]
pub enum QueueError {
    FileOpen(String),
    FileStat(String),
    InvalidSize { got: u64, expected: u64 },
    Mmap(String),
    InvalidMagic { got: u32 },
    CapacityMismatch { got: u32, expected: u32 },
    CorruptedOrder,
    QueueFull { depth: u64 },
    Flush(String),
}

impl std::fmt::Display for QueueError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QueueError::FileOpen(e) => write!(f, "Failed to open file: {}", e),
            QueueError::FileStat(e) => write!(f, "Failed to stat file: {}", e),
            QueueError::InvalidSize { got, expected } => {
                write!(f, "Invalid file size: got {}, expected {}", got, expected)
            }
            QueueError::Mmap(e) => write!(f, "Failed to mmap: {}", e),
            QueueError::InvalidMagic { got } => {
                write!(f, "Invalid queue magic: got 0x{:X}", got)
            }
            QueueError::CapacityMismatch { got, expected } => {
                write!(f, "Capacity mismatch: got {}, expected {}", got, expected)
            }
            QueueError::CorruptedOrder => write!(f, "Corrupted order detected"),
            QueueError::QueueFull { depth } => {
                write!(f, "Queue full - backpressure at depth {}", depth)
            }
            QueueError::Flush(e) => write!(f, "Failed to flush: {}", e),
        }
    }
}

impl std::error::Error for QueueError {}

// Thread-safe: Queue can be sent between threads
unsafe impl Send for Queue {}
// Not Sync: only one thread should access at a time (SPSC model)

#[cfg(test)]
mod tests {
    use super::*;
    // use std::time::Instant;

    #[test]
    fn test_layout() {
        assert_eq!(ORDER_SIZE, 56, "Order must be 48 bytes");
        assert_eq!(HEADER_SIZE, 136, "QueueHeader must be 136 bytes");
        assert_eq!(
            std::mem::offset_of!(QueueHeader, consumer_tail),
            64,
            "ConsumerTail must be at offset 64"
        );
        assert_eq!(
            std::mem::offset_of!(QueueHeader, producer_head),
            0,
            "ProducerHead must be at offset 0"
        );
    }

    #[test]
    fn test_order_default() {
        let order = ShmOrder::default();
        assert_eq!(order.order_id, 0);
        assert_eq!(order.shares_qty, 0);
        assert_eq!(order.price, 0);
    }

    #[test]
    fn test_queue_create_and_open() {
        let path = "/tmp/test_hft_queue_create";
        let _ = std::fs::remove_file(path);

        // Create queue
        let _queue = Queue::open(path).expect_err("Should fail before creation");

        // This test expects Go to create the queue first
        // So we skip in normal test runs
    }

    #[test]
    fn test_error_display() {
        let err = QueueError::InvalidSize {
            got: 100,
            expected: 3276800,
        };
        let msg = err.to_string();
        assert!(msg.contains("100"));
        assert!(msg.contains("3276800"));
    }
}
