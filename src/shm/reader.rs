

use crate::shm::queue::QueueError;
// SHM reader , passed ordrs to the balance manager 
use crate::{orderbook::order::ShmOrder, shm::queue::Queue};
use crate::orderbook::order::Side;
use crossbeam::{channel::Sender};
use crate::orderbook::order::{Order };
use crate::orderbook::types::{ShmReaderError};

pub struct ShmReader {
    pub queue: Queue,  // Not Option
    pub order_sender_to_balance_manager: Sender<Order>,
}

impl ShmReader {
    /// Returns None if queue can't be opened
    pub fn new(order_sender_to_balance_manager: Sender<Order>) -> Option<Self> {
        match Queue::open("/tmp/sex") {
            Ok(queue) => Some(Self { queue, order_sender_to_balance_manager }),
            Err(e) => {
                eprintln!("[SHM Reader] Failed to open queue: {:?}", e);
                None
            }
        }
    }
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub fn run_reader(&mut self) {
        eprintln!("[SHM Reader] Starting on Core 2");
        
        
        let mut count = 0u64;
        let mut last_log = std::time::Instant::now();
        
        loop {
            
            match self.queue.dequeue() {
            
                Ok(Some(shm_order)) => {
                    //println!("=== DEQUEUED order_id={} timestamp={} ===", 
                     //   shm_order.order_id, shm_order.timestamp);
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
                        shm_order.shares_qty,
                        shm_order.price,
                        shm_order.timestamp,
                        shm_order.symbol,
                    );
                    
                    //println!("order from shm recv ");
                    //println!("Sending to balance manager ");
                    // send to balance manager 

                    match self.order_sender_to_balance_manager.send(order) {
                        Ok(_) => {}
                        Err(e) => {
                            eprintln!("[SHM Reader] Channel full, dropping order: {:?}", e);
                        }
                    }
                    
                    count += 1;
                }
                Ok(None) => {
                    // Queue empty, spin briefly
                    std::hint::spin_loop();
                }
                Err(e) => {
                    eprintln!("[SHM Reader] Dequeue error: {:?}", e);
                }
            }
            
            // Metrics (every 2 seconds)
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
    pub queue: Queue,
}

impl StShmReader{
    pub fn new() -> Option<Self> {
        match Queue::open("/tmp/sex") {
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
                    shm_order.shares_qty,
                    shm_order.price,
                    shm_order.timestamp,
                    shm_order.symbol,
                );
                return Some(order);
                //println!("order from shm recv ");
                //println!("Sending to balance manager ");
                // send to balance manager 

                
            }
            Ok(None)=>{
                
            }
            Err(e) => {
                return  None;
            }
        }

        None
    }
}