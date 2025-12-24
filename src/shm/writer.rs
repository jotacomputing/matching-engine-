use bounded_spsc_queue::Consumer;

use crate::{shm::event_queue::{OrderEventQueue, OrderEvents}, singlepsinglecq::my_queue::SpscQueue};
use std::sync::Arc;
// writer for the order events becuase balance manager , and publisher can send diffeent events 
// publisher more priority so that we get the order responses for post req 
// insufficient funds would have low probobaility 
pub struct ShmWriter{
    pub recv_from_bm : Arc<SpscQueue<OrderEvents>>, // insufficient funds 
    pub recv_from_publisher :Arc<SpscQueue<OrderEvents>>, // rest all order events 
    pub order_event_queue : OrderEventQueue ,
    pub rec_from_bm_try : Consumer<OrderEvents>,
    pub rec_from_publisher_try : Consumer<OrderEvents>
}

impl ShmWriter{
    pub fn new(recv_from_bm : Arc<SpscQueue<OrderEvents>>, recv_from_publisher : Arc<SpscQueue<OrderEvents>> , rec_from_bm_try : Consumer<OrderEvents>,rec_from_publisher_try : Consumer<OrderEvents>)->Option<Self>{
        let order_event_queue = OrderEventQueue::open("/tmp/OrderEvents");
        match order_event_queue {
            Ok(queue)=>{
                Some(Self{
                    recv_from_bm,
                    recv_from_publisher,
                    order_event_queue : queue ,
                    rec_from_bm_try , 
                    rec_from_publisher_try
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
            // old code 
            //if let Some(event) = self.recv_from_bm.pop(){
            //    let _ = self.order_event_queue.enqueue(event);
            //    did_work = true;
            //}
            //if let Some(event) = self.recv_from_publisher.pop(){
            //    let _ = self.order_event_queue.enqueue(event);
            //    did_work = true;
            //}

            // new code 
            if let Some(event) = self.rec_from_bm_try.try_pop(){
                let _ = self.order_event_queue.enqueue(event);
                did_work = true;
            }

            if let Some(event) = self.rec_from_publisher_try.try_pop(){
                let _ = self.order_event_queue.enqueue(event);
                did_work = true;
            }

            if !did_work{
                std::hint::spin_loop();
            }


        }
    }

}