use crate::{shm::event_queue::{OrderEventQueue, OrderEvents}, singlepsinglecq::my_queue::SpscQueue};
use std::sync::Arc;
// writer for the order events becuase balance manager , and publisher can send diffeent events 
// publisher more priority so that we get the order responses for post req 
// insufficient funds would have low probobaility 
pub struct ShmWriter{
    pub recv_from_bm : Arc<SpscQueue<OrderEvents>>, // insufficient funds 
    pub recv_from_publisher :Arc<SpscQueue<OrderEvents>>, // rest all order events 
    pub order_event_queue : OrderEventQueue
}

impl ShmWriter{
    pub fn new(recv_from_bm : Arc<SpscQueue<OrderEvents>>, recv_from_publisher : Arc<SpscQueue<OrderEvents>>)->Option<Self>{
        let order_event_queue = OrderEventQueue::open("/trading/OrderEvents");
        match order_event_queue {
            Ok(queue)=>{
                Some(Self{
                    recv_from_bm,
                    recv_from_publisher,
                    order_event_queue : queue 
                })
            }
            Err(_)=>{
                eprint!("Failed to open write queue");
                None
            }
        }
    }

    pub fn start_shm_writter(&mut self){
        loop {
            let mut did_work = false;
            if let Some(event) = self.recv_from_bm.pop(){
                let _ = self.order_event_queue.enqueue(event);
                did_work = true;
            }
            if let Some(event) = self.recv_from_publisher.pop(){
                let _ = self.order_event_queue.enqueue(event);
                did_work = true;
            }

            if !did_work{
                std::hint::spin_loop();
            }


        }
    }

}