// query response will be a uder balance response or a holdings response or a success on adding a new user 
use crate::balance_manager::my_balance_manager2::{UserBalance};
use crate::orderbook::order;
use memmap2::MmapMut;
use std::fs::{self, OpenOptions };
use std::path::Path;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::os::unix::fs::OpenOptionsExt;
use crate::logger::types::OrderLogWrapper;


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
const QUEUE_MAGIC: u32 = 0xDEA;
// reduce size 
const QUEUE_CAPACITY: usize = 65536;
const LOG_SIZE: usize = std::mem::size_of::<OrderLogWrapper>();
const HEADER_SIZE: usize = std::mem::size_of::<QueueHeader>();
const TOTAL_SIZE: usize = HEADER_SIZE + (QUEUE_CAPACITY * LOG_SIZE);

// Compile-time layout assertions (fail build if wrong)
const _: () = assert!(LOG_SIZE == 64, "Order must be 64 bytes");
const _: () = assert!(HEADER_SIZE == 136, "QueueHeader must be 136 bytes");
const _: () = {
    // Verify ConsumerTail is at offset 64
    assert!(
        std::mem::offset_of!(QueueHeader, consumer_tail) == 64,
        "ConsumerTail must be at offset 64"
    );
};

#[derive(Debug)]
pub struct OrderLogQueue {
    mmap: MmapMut,
    header_ptr: *mut QueueHeader,           // Cached pointer
    log_ptr: *mut OrderLogWrapper,       // Cached logs pointer
}

impl OrderLogQueue {
    pub fn create<P: AsRef<Path>>(path: P) -> Result<Self, QueueError> {
        let _ = fs::remove_file(&path);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .create_new(true) // O_EXCL
            .mode(0o666)
            .open(&path)
            .map_err(|e| QueueError::FileOpen(e.to_string()))?;
    
        file.set_len(TOTAL_SIZE as u64)
            .map_err(|e| QueueError::FileStat(e.to_string()))?;
    
        file.sync_all()
            .map_err(|e| QueueError::FileStat(e.to_string()))?;
    
        let mut mmap =
            unsafe { MmapMut::map_mut(&file) }.map_err(|e| QueueError::Mmap(e.to_string()))?;
    
        if let Err(e) = mmap.lock() {
            eprintln!("Warning: failed to mlock: {}", e);
        }
    
        let header_ptr = mmap.as_mut_ptr() as *mut QueueHeader;
    
        unsafe {
            (*header_ptr)
                .producer_head
                .store(0, Ordering::SeqCst);
            (*header_ptr)
                .consumer_tail
                .store(0, Ordering::SeqCst);
            (*header_ptr)
                .magic
                .store(QUEUE_MAGIC, Ordering::SeqCst);
            (*header_ptr)
                .capacity
                .store(QUEUE_CAPACITY as u32, Ordering::SeqCst);
        }
    
        mmap.flush()
            .map_err(|e| QueueError::Flush(e.to_string()))?;
    
        let log_ptr = unsafe {
            mmap.as_mut_ptr().add(HEADER_SIZE) as *mut OrderLogWrapper
        };
    
        Ok(OrderLogQueue {
            mmap,
            header_ptr,
            log_ptr,
        })
    }
    
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
        let log_ptr = unsafe { mmap.as_mut_ptr().add(HEADER_SIZE) as *mut OrderLogWrapper };

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

        Ok(OrderLogQueue {
            mmap,
            header_ptr,
            log_ptr,
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
    fn get_log_response(&self, pos: usize) -> OrderLogWrapper {
        unsafe { *self.log_ptr.add(pos) }
    }

    /// Set order at position - ZERO COST pointer arithmetic
    #[inline(always)]
    fn set_log_response(&self, pos: usize, response: OrderLogWrapper) {
        unsafe {
            *self.log_ptr.add(pos) = response;
        }
    }

    /// ULTRA-FAST dequeue - all pointers cached, no borrows
    #[inline]
    pub fn dequeue(&mut self) -> Result<Option<OrderLogWrapper>, QueueError> {
        let header = self.header_mut();

        let producer_head = header.producer_head.load(Ordering::Acquire);
        let consumer_tail = header.consumer_tail.load(Ordering::Relaxed);

        if consumer_tail == producer_head {
            return Ok(None);
        }

        let pos = (consumer_tail % QUEUE_CAPACITY as u64) as usize;
        std::sync::atomic::fence(Ordering::Acquire);
        let log = self.get_log_response(pos);

        header
            .consumer_tail
            .store(consumer_tail + 1, Ordering::Release);

        Ok(Some(log))
    }

    pub fn enqueue(&mut self, log: OrderLogWrapper) -> Result<(), QueueError> {
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
        self.set_log_response(pos, log);

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

    pub fn dequeue_spin(&mut self, max_spins: usize) -> Result<Option<OrderLogWrapper>, QueueError> {
        for _ in 0..max_spins {
            match self.dequeue()? {
                Some(order) => return Ok(Some(order)),
                None => std::hint::spin_loop(),
            }
        }
        Ok(None)
    }
}

impl Drop for OrderLogQueue {
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
unsafe impl Send for OrderLogQueue {}
// Not Sync: only one thread should access at a time (SPSC model)

