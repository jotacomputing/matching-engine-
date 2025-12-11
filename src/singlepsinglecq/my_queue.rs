use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicU64, Ordering};
use crate::orderbook::types::QueueError;

/// SPSC ring buffer (producer / consumer must be pinned to separate threads).
/// - producer_index and consumer_index are padded to avoid false sharing.
/// - buffer stores MaybeUninit<T> slots.
/// - only two unsafe sites: writing into slot and reading from slot.
pub struct SpscQueue<T> {
   
    producer_index: AtomicU64,
    _pad1: [u8; 56],

   
    consumer_index: AtomicU64,
    _pad2: [u8; 56],

    buffer: Box<[MaybeUninit<T>]>,
    capacity: usize,
    mask: usize,
}

impl<T> SpscQueue<T> {
    pub fn new(capacity: usize) -> Self {
        assert!(capacity.is_power_of_two(), "capacity must be a power of 2");

        let mut v = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            v.push(MaybeUninit::uninit());
        }
        let buffer = v.into_boxed_slice();

        Self {
            producer_index: AtomicU64::new(0),
            _pad1: [0; 56],
            consumer_index: AtomicU64::new(0),
            _pad2: [0; 56],
            buffer,
            capacity,
            mask: capacity - 1,
        }
    }

    pub fn len(&self) -> usize {
        let p = self.producer_index.load(Ordering::Acquire);
        let c = self.consumer_index.load(Ordering::Relaxed);
        (p.wrapping_sub(c)) as usize
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

   
    pub fn push(&self, data: T) -> Result<(), QueueError> {
       
        //  Read consumer_index with Acquire so we see consumer progress.
        //  Producer index read can be Relaxed bcs we own it.
        let c = self.consumer_index.load(Ordering::Acquire);
        let p = self.producer_index.load(Ordering::Relaxed);

        // full if producer is capacity ahead of consumer
        if p.wrapping_sub(c) == self.capacity as u64 {
            return Err(QueueError::QueueFullError);
        }

        let idx = (p as usize) & self.mask;

       
        unsafe {
            let slot_ptr = self.buffer.as_ptr().add(idx) as *mut MaybeUninit<T>;
            // initialize the slot
            slot_ptr.write(MaybeUninit::new(data));
        }

       
        self.producer_index.store(p.wrapping_add(1), Ordering::Release);
        Ok(())
    }

  
    pub fn pop(&self) -> Option<T> {
       
        //  Read producer_index with Acquire so we see published writes.
        //  Consumer index read can be Relaxed (we own it).
        let p = self.producer_index.load(Ordering::Acquire);
        let c = self.consumer_index.load(Ordering::Relaxed);

        // empty if equal
        if c == p {
            return None;
        }

        let idx = (c as usize) & self.mask;

       
        let value = unsafe {
            let slot_ptr = self.buffer.as_ptr().add(idx) as *const MaybeUninit<T>;
            // moves the T out and leaves slot uninitialized
            (*slot_ptr).assume_init_read()
        };

       
        self.consumer_index.store(c.wrapping_add(1), Ordering::Release);
        Some(value)
    }
}

// Queue is safe to move between threads if T is Send.
// It must NOT be Sync: we don't allow &SpscQueue to be used concurrently by multiple producers/consumers.
unsafe impl<T: Send> Send for SpscQueue<T> {}
// Do NOT impl Sync.

