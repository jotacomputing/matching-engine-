use crossbeam::{channel::Receiver, queue::ArrayQueue};
use crate::{orderbook::types::{Event, TickerData}, pubsub::pubsub_manager::RedisPubSubManager};
use std::sync::Arc;
use crate::singlepsinglecq::my_queue::SpscQueue;

pub struct EventPublisher {
    pub receiver: Receiver<Event>,   
    pub event_queue : Arc<SpscQueue<Event>> , 
    pub mypubsub : RedisPubSubManager
}
impl EventPublisher {
    pub fn new(rx: Receiver<Event> , event_queue : Arc<SpscQueue<Event>> , mypubsub : RedisPubSubManager) -> Self {
        Self { receiver: rx , event_queue  , mypubsub}
    }
    pub fn start_publisher(&mut self) {
        println!("[PUBLISHER] Started (crossbeam batched mode) on core 5");
        loop {
            
            match self.event_queue.pop() {
                Some(rec_event) => {
                    let ticker_message = TickerData::new(
                        String::from("ticker"), 
                        rec_event.market_update.symbol, 
                        rec_event.market_update.event_time, 
                        rec_event.market_update.last_traded_price
                    );
                    let stream = format!("trade.{}", rec_event.market_update.symbol);

                    if let Ok(payload) = serde_json::to_vec(&ticker_message){
                       let _ = self.mypubsub.publish(&stream, payload);
                    }
                    
                }
                None =>{
                    break;
                }
            }
        }
    }

    
}


