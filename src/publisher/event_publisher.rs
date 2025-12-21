use crossbeam::{channel::Receiver, queue::ArrayQueue};
use crate::{orderbook::{order::Side, types::{DepthData, Event, Fills, TickerData, TradeData}}, pubsub::pubsub_manager::RedisPubSubManager, shm::event_queue::{EventType, OrderEvents}};
use std::{fmt::format, sync::Arc};
use crate::singlepsinglecq::my_queue::SpscQueue;

pub struct EventPublisher {
    pub receiver: Receiver<Event>,   
    pub event_queue : Arc<SpscQueue<Event>> , 
    pub mypubsub : RedisPubSubManager ,
    pub pub_writter_order_event_queue : Arc<SpscQueue<OrderEvents>>
}
impl EventPublisher {
    pub fn new(rx: Receiver<Event> , event_queue : Arc<SpscQueue<Event>> , mypubsub : RedisPubSubManager , pub_writter_order_event_queue : Arc<SpscQueue<OrderEvents>>) -> Self {
        Self { receiver: rx , event_queue  , mypubsub , pub_writter_order_event_queue}
    }
    pub fn start_publisher(&mut self) {
        println!("[PUBLISHER] Started (crossbeam batched mode) on core 5");
        loop {
            match self.event_queue.pop() {
                Some(rec_event) => {
                    //println!("recived the events ");

                    {
                        let ticker_message = TickerData::new(
                            String::from("ticker"), 
                            rec_event.market_update.symbol, 
                            rec_event.market_update.event_time, 
                            rec_event.market_update.last_traded_price
                        );
                        //println!("formed the ticker message {:?}", ticker_message);
                        let ticker_stream = format!("ticker.{}", rec_event.market_update.symbol);
                        if let Ok(payload) = serde_json::to_vec(&ticker_message){
                            //println!("publishing");
                           let _ = self.mypubsub.publish(&ticker_stream, payload);
                        }
                    }
                    {
                        let depth_message = DepthData::new(
                            String::from("depth"), 
                            rec_event.market_update.symbol, 
                            rec_event.market_update.event_time, 
                            rec_event.market_update.trade_time, 
                            rec_event.market_update.depth.0, 
                            rec_event.market_update.depth.1
                        );
                        let depth_stream = format!("depth.{}" , rec_event.market_update.symbol);
                        if let Ok(payload) = serde_json::to_vec(&depth_message){
                            let _ = self.mypubsub.publish(&depth_stream, payload);
                        }
                    }
                    {
                        // check if we even need to return fills to the user and then avod clone here 
                        for fill in rec_event.market_update.match_result.fills.fills.clone(){
                            let trade_stream = format!("trade.{}" , rec_event.market_update.symbol);
                            let trade_message = TradeData::new(
                                String::from("trade"),  
                                rec_event.market_update.symbol, 
                                rec_event.market_update.event_time, 
                                rec_event.market_update.trade_time, 
                                fill.price, 
                                fill.quantity, 
                                fill.maker_order_id, 
                                fill.taker_order_id, 
                                match fill.taker_side {
                                    Side::Ask => true,
                                    Side::Bid => false
                                }
                            );
    
                            if let Ok(payload)=serde_json::to_vec(&trade_message){
                                let _ = self.mypubsub.publish(&trade_stream, payload);
                            }
                        }
                    }
                    // we need to send trade messages for all fills 
                    // sedning order events to the shm writter for the partuclar order updates 
                    // check if fills are really needed or not for the user 
                    let orignal_qty =  rec_event.market_update.match_result.orignal_qty;
                    let remaining_qty = rec_event.market_update.match_result.remaining_qty;

                    if remaining_qty == 0  {
                        let _ = self.pub_writter_order_event_queue.push(OrderEvents { 
                            user_id: rec_event.market_update.match_result.user_id, 
                            order_id: rec_event.market_update.match_result.order_id, 
                            symbol: rec_event.market_update.symbol, 
                            event_type: EventType::CompletelyFilled(rec_event.market_update.match_result) 
                        });
                    }
                    else if remaining_qty == orignal_qty {
                        let _ = self.pub_writter_order_event_queue.push(OrderEvents { 
                            user_id: rec_event.market_update.match_result.user_id, 
                            order_id: rec_event.market_update.match_result.order_id, 
                            symbol: rec_event.market_update.symbol, 
                            event_type: EventType::Accepted(rec_event.market_update.match_result)
                        });
                    }
                    else if orignal_qty - remaining_qty > 0 {
                        let _ = self.pub_writter_order_event_queue.push(OrderEvents { 
                            user_id: rec_event.market_update.match_result.user_id, 
                            order_id: rec_event.market_update.match_result.order_id, 
                            symbol: rec_event.market_update.symbol, 
                            event_type: EventType::PartiallyFilled(rec_event.market_update.match_result) 
                        });
                    }
                }
                
                None =>{
                    std::hint::spin_loop();
                }
            }
        }
    }

    
}


