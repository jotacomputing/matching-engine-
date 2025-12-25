

use bounded_spsc_queue::Producer;

// SHM reader , passed ordrs to the balance manager 
use crate::{orderbook::order::ShmOrder, shm::queue::IncomingOrderQueue};
use crate::orderbook::order::Side;
use crate::orderbook::order::{Order };
pub struct ShmReader {
    pub queue: IncomingOrderQueue,  
    pub order_batch : Vec<ShmOrder>,
    pub shm_bm_order_queue_try : Producer<Order>,
}
impl ShmReader {
    /// Returns None if queue can't be opened
    pub fn new( shm_bm_order_queue_try : Producer<Order>) -> Option<Self> {
        match IncomingOrderQueue::open("/tmp/IncomingOrders") {
            Ok(queue) => Some(Self { queue   , order_batch : Vec::with_capacity(1000) , shm_bm_order_queue_try}),
            Err(e) => {
                eprintln!("[SHM Reader] Failed to open queue: {:?}", e);
                None
            }
        }
    }
    pub fn run_reader(&mut self) {
        eprintln!("[SHM Reader] Starting on Core 2");
        
        self.order_batch.clear();
        let mut count = 0u64;
        let mut last_log = std::time::Instant::now();
        
        loop {
            for _ in 0..1000{
                match self.queue.dequeue(){
                    Ok(Some(shm_order))=>{
                        self.order_batch.push(shm_order);
                    }
                    Ok(None)=>{
                        break;
                    }
                    Err(e) => {
                        eprintln!("[SHM Reader] Dequeue error: {:?}", e);
                        break;
                    }
                }
            }
            for shm_order in self.order_batch.drain(..){
                //println!("got the order from shm");
                let order_side = match  shm_order.side {
                    0 => {
                        Side::Bid
                    },
                    1=>{
                        Side::Ask
                    }
                    _ => {
                        continue;
                    }
                };
                let order = Order::new(
                    shm_order.user_id,
                    shm_order.order_id,
                    order_side,
                    shm_order.order_type,
                    shm_order.shares_qty,
                    shm_order.price,
                    shm_order.timestamp,
                    shm_order.symbol,
                );
                // push -> blocking method , cant drop orders 
                self.shm_bm_order_queue_try.push(order);
                count += 1;

            }// Metrics (every 2 seconds)
            if last_log.elapsed().as_secs() >= 2 {
                let rate = count as f64 / last_log.elapsed().as_secs_f64();
                eprintln!("[SHM Reader] {:.2}M orders/sec", rate / 1_000_000.0);
                count = 0;
                last_log = std::time::Instant::now();
            }
        }
    }
}

pub struct StShmReader{
    pub queue: IncomingOrderQueue,
}

impl StShmReader{
    pub fn new() -> Option<Self> {
        match IncomingOrderQueue::open("/tmp/IncomingOrders") {
            Ok(queue) => Some(Self { queue }),
            Err(e) => {
                eprintln!("[SHM Reader] Failed to open queue: {:?}", e);
                None
            }
        }
    }
    #[inline(always)]
    pub fn receive_order(&mut self)->Option<Order>{
        match self.queue.dequeue() {
            Ok(Some(shm_order)) => {
                let order_side = match  shm_order.side {
                    0 => {
                        Side::Bid
                    },
                    1=>{
                        Side::Ask
                    }
                    _=>{
                        return None;
                    }
                };
                let order = Order::new(
                    shm_order.user_id,
                    shm_order.order_id,
                    order_side,
                    shm_order.order_type,
                    shm_order.shares_qty,
                    shm_order.price,
                    shm_order.timestamp,
                    shm_order.symbol,
                );
                // send to balance manager 
                return Some(order);                
            }
            Ok(None)=>{
                
            }
            Err(_) => {
                return  None;
            }
        }

        None
    }
}