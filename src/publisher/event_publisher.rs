use bounded_spsc_queue::{Consumer, Producer};
use crate::{orderbook::{order::Side, types::{DepthData, Event, TickerData, TradeData}}, pubsub::pubsub_manager::RedisPubSubManager, shm::event_queue::{ OrderEvents}};
use std::{ sync::Arc};
use crate::singlepsinglecq::my_queue::SpscQueue;

pub struct EventPublisher { 
    pub mypubsub : RedisPubSubManager ,
    pub event_queue_from_engine_try : Consumer<Event>,
    pub event_queue_sender_to_writter_try : Producer<OrderEvents>
}
impl EventPublisher {
    pub fn new(  mypubsub : RedisPubSubManager  ,  event_queue_from_engine_try : Consumer<Event>,event_queue_sender_to_writter_try : Producer<OrderEvents>) -> Self {
        Self {  
            mypubsub , 
            event_queue_from_engine_try , 
            event_queue_sender_to_writter_try , 
        }
    }
    pub fn start_publisher(&mut self) {
        println!("[PUBLISHER] Started (crossbeam batched mode) on core 5");
        loop {
            
            match self.event_queue_from_engine_try.try_pop(){

                Some(rec_event) => {
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
                        
                            let depth = rec_event.market_update.depth;
                            let depth_message = DepthData::new(
                                String::from("depth"), 
                                rec_event.market_update.symbol, 
                                rec_event.market_update.event_time, 
                                rec_event.market_update.trade_time, 
                                depth.0,
                                depth.1
                            );
                            let depth_stream = format!("depth.{}" , rec_event.market_update.symbol);
                            if let Ok(payload) = serde_json::to_vec(&depth_message){
                                let _ = self.mypubsub.publish(&depth_stream, payload);
                            }
                        
                    }
                    {
                        // check if we even need to return fills to the user and then avod clone here 
                        for fill in rec_event.market_update.match_result.fills.fills{
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
                        let _ = self.event_queue_sender_to_writter_try.try_push(OrderEvents {
                             user_id: rec_event.market_update.match_result.user_id, 
                             order_id: rec_event.market_update.match_result.order_id, 
                             symbol: rec_event.market_update.symbol, 
                             event_kind: 2, 
                             filled_qty: orignal_qty, 
                             remaining_qty, 
                             original_qty: orignal_qty,  
                             error_code: 0
                             });
                    }
                    else if remaining_qty == orignal_qty {
                        let _ = self.event_queue_sender_to_writter_try.try_push(OrderEvents {
                            user_id: rec_event.market_update.match_result.user_id, 
                            order_id: rec_event.market_update.match_result.order_id, 
                            symbol: rec_event.market_update.symbol, 
                            event_kind: 0, 
                            filled_qty: 0, 
                            remaining_qty, 
                            original_qty: orignal_qty,  
                            error_code: 0
                            });
                    }
                    else if orignal_qty - remaining_qty > 0 {
                        let _ = self.event_queue_sender_to_writter_try.try_push(OrderEvents {
                            user_id: rec_event.market_update.match_result.user_id, 
                            order_id: rec_event.market_update.match_result.order_id, 
                            symbol: rec_event.market_update.symbol, 
                            event_kind: 1, 
                            filled_qty: orignal_qty - remaining_qty, 
                            remaining_qty, 
                            original_qty: orignal_qty,  
                            error_code: 0
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


