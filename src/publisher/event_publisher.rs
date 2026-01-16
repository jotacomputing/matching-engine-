use bounded_spsc_queue::{Consumer, Producer};
use std::time::{SystemTime, UNIX_EPOCH};
use crate::{logger::types::TradeLogs, orderbook::{order::Side, types::{DepthData, Event, TickerData, TradeData}}, pubsub::pubsub_manager::RedisPubSubManager, shm::{event_queue::OrderEvents, fill_queue_mm::MarketMakerFill}};

pub struct EventPublisher { 
    pub mypubsub : RedisPubSubManager ,
    pub event_queue_from_engine_try : Consumer<Event>,
    pub event_queue_sender_to_writter_try : Producer<OrderEvents>,
    pub trade_log_sender_to_logger : Producer<TradeLogs>,
    pub mm_fill_sender : Producer<MarketMakerFill> ,
}
impl EventPublisher {
    pub fn new( 
        mypubsub : RedisPubSubManager  ,
        event_queue_from_engine_try : Consumer<Event>,
        event_queue_sender_to_writter_try : Producer<OrderEvents> ,
        trade_log_sender_to_logger : Producer<TradeLogs>,
        mm_fill_sender : Producer<MarketMakerFill> ,
    ) -> Self {
        Self {  
            mypubsub , 
            event_queue_from_engine_try , 
            event_queue_sender_to_writter_try , 
            trade_log_sender_to_logger , 
            mm_fill_sender
        }
    }
    pub fn start_publisher(&mut self) {
        println!("[PUBLISHER] Started (crossbeam batched mode) on core 5");
        loop {
            
            match self.event_queue_from_engine_try.try_pop(){

                Some(rec_event) => {
                    //println!("event recived byy publisher");
                    //println!("sending ticker depth and trade messages for the FE ");
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

                        for fill in rec_event.market_update.match_result.fills.fills{
                            if fill.maker_user_id == 0 {
                                // this is the user ID of the market maker 
                                // market maker was the maker user , this order was on the book 
                                match fill.taker_side{
                                    Side::Ask=>{
                                        // taker was seeling so the market maker was buying 
                                        // the fill is of a buy order , side 0 
                                        self.mm_fill_sender.push(MarketMakerFill { 
                                            timestamp : SystemTime::now()
                                            .duration_since(UNIX_EPOCH)
                                            .unwrap()
                                            .as_nanos() as u64,
                                            fill_price: fill.price, 
                                            fill_quantity: fill.quantity , 
                                            symbol : fill.symbol , 
                                            side_of_mm_order : 0
                                        });
                                    }
                                    Side::Bid=>{
                                        // incoming order was buy order so mm was selling 
                                        self.mm_fill_sender.push(MarketMakerFill { 
                                            timestamp : SystemTime::now()
                                            .duration_since(UNIX_EPOCH)
                                            .unwrap()
                                            .as_nanos() as u64,
                                            fill_price: fill.price, 
                                            fill_quantity: fill.quantity , 
                                            symbol : fill.symbol , 
                                            side_of_mm_order : 1
                                        });
                                    }
                                }
                                
                            }

                            if fill.taker_user_id == 0{
                                // this was the incoming order 
                                match fill.taker_side{
                                    Side::Ask=>{
                                        self.mm_fill_sender.push(MarketMakerFill { 
                                            timestamp : SystemTime::now()
                                            .duration_since(UNIX_EPOCH)
                                            .unwrap()
                                            .as_nanos() as u64,
                                            fill_price: fill.price, 
                                            fill_quantity: fill.quantity , 
                                            symbol : fill.symbol , 
                                            side_of_mm_order : 1
                                        });
                                    }
                                    Side::Bid=>{
                                        self.mm_fill_sender.push(MarketMakerFill { 
                                            timestamp : SystemTime::now()
                                            .duration_since(UNIX_EPOCH)
                                            .unwrap()
                                            .as_nanos() as u64,
                                            fill_price: fill.price, 
                                            fill_quantity: fill.quantity , 
                                            symbol : fill.symbol , 
                                            side_of_mm_order : 0
                                        });
                                    }
                                }
                            }
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

                            let trade_log = TradeLogs{
                                buyer_order_id : fill.maker_order_id ,
                                seller_order_id : fill.taker_order_id ,
                                price : fill.price ,
                                quantity : fill.quantity,
                                symbol : fill.symbol ,
                                is_buyer_maker : match fill.taker_side {
                                    Side::Ask => true,
                                    Side::Bid => false
                                } ,
                                timestamp : SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap()
                                .as_nanos() as i64 , 
                            };

                            let _ = self.trade_log_sender_to_logger.try_push(trade_log);
                        }
                    }
                    // we need to send trade messages for all fills 
                    // sedning order events to the shm writter for the partuclar order updates 
                    // check if fills are really needed or not for the user 
                    //println!("sending order event to the API for the order we got ");

                    let orignal_qty =  rec_event.market_update.match_result.orignal_qty;
                    let remaining_qty = rec_event.market_update.match_result.remaining_qty;
                   // println!("orignal quantiyi {:?}" , orignal_qty);
                   // println!("remaining quantiyi {:?}" , remaining_qty);
                    if remaining_qty == 0  {
                        let _ = self.event_queue_sender_to_writter_try.push(OrderEvents {
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
                        let _ = self.event_queue_sender_to_writter_try.push(OrderEvents {
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
                        let _ = self.event_queue_sender_to_writter_try.push(OrderEvents {
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


