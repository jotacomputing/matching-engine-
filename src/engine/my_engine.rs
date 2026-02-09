use bounded_spsc_queue::{Consumer, Producer};
use chrono::prelude::*;
use crate::logger::types::OrderBookSnapShot;
use crate::orderbook::order::{Order, Side};
use crate::orderbook::types::{Event, Fills, MarketUpdateAfterTrade, MatchResult, OrderBookError} ;
use crate::orderbook::order_book::{ OrderBook};
use crate::shm::cancel_orders_queue::{ CancelOrderQueue};
use crate::shm::event_queue::OrderEvents;
use crate::shm::market_maker_feed::MarketMakerFeed;
use std::time::{SystemTime, UNIX_EPOCH};

// 100 max symbols for now 

const MAX_SYMBOLS : usize = 100 ; 
pub trait Engine{
    fn add_book(&mut self , symbol : u32);
    fn get_book(&self , symbol : u32)->Option<&OrderBook>; // can only get a refrence , orderbooks are owned by the engine
    fn get_book_mut(&mut self, symbol: u32) -> Option<&mut OrderBook>;
    fn remove_book(&mut self , symbol : u32);
    fn get_book_count(&self)->usize;
    fn has_book(&self , symbol : u32)->bool;
}

//pub struct MyEngine{
//    // the engine will own all the orderbooks
//    pub engine_id :usize ,
//    pub book_count : usize, 
//    pub books : HashMap<u32 , OrderBook>,
//    pub test_orderbook : OrderBook,
//
//    pub cancel_order_queue : CancelOrderQueue,
//
//
//    pub bm_order_reciver_try : Consumer<Order>,
//    pub sending_fills_to_bm_try : Producer<Fills>,
//    pub sending_event_to_publisher_try : Producer<Event>,
//    pub sending_order_events_to_writter_try : Producer<OrderEvents>,
//}
//
//impl MyEngine{
//    pub fn new( engine_id : usize ,
//        bm_order_reciver_try : Consumer<Order>,
//        sending_fills_to_bm_try : Producer<Fills>,
//        sending_event_to_publisher_try : Producer<Event>,
//        sending_order_events_to_writter_try : Producer<OrderEvents>,
//        )->Option<Self> {
//            let cancel_orders_queue = CancelOrderQueue::open("/tmp/CancelOrders");
//            match cancel_orders_queue {
//                Ok(queue)=>{
//                    Some(Self{
//                        engine_id,
//                        book_count : 0 ,
//                        books : HashMap::new(),
//                        test_orderbook : OrderBook::new(100),
//                        cancel_order_queue : queue , 
//                        bm_order_reciver_try , 
//                        sending_fills_to_bm_try , 
//                        sending_event_to_publisher_try ,
//                        sending_order_events_to_writter_try
//                    } )
//                }
//                Err(_)=>{
//                    eprint!("error in init engine because of cancel order queue");
//                    None
//                }
//            }
//            
//    }
//    pub fn run_engine<F>(&mut self , mut feedCallback : F)where F : FnMut(MarketMakerFeed){
//        eprintln!("[ENGINE] Started (crossbeam batched mode) on core 1");
//
//        let mut count = 0u64;
//        let mut last_log = std::time::Instant::now();
//
//        loop {
//            // new queue use 
//            if let Some(mut recieved_order) = self.bm_order_reciver_try.try_pop(){
//                count += 1;
//                if let Some(order_book) = self.get_book_mut(recieved_order.symbol){
//                    let events = match recieved_order.order_type {
//                        0 => order_book.match_market_order(&mut recieved_order , feedCallback),
//                        1 =>{
//                            // limit order 
//                            match recieved_order.side{
//                                Side::Ask => order_book.match_ask(&mut recieved_order , feedCallback),
//                                Side::Bid=> order_book.match_bid(&mut recieved_order , feedCallback)
//                                
//                            }
//                        }
//                        _=>{
//                            eprint!("Invalid order struct");
//                            return; 
//                        }
//                    };
//                   
//                    if let Ok(match_result)=events{
//                        // direct push cant drop
//                        let order_book_symbol = order_book.symbol;
//                        let order_book_last_price = order_book.last_trade_price;
//                        let order_book_depth = order_book.get_depth();
//                        let _ = self.sending_fills_to_bm_try.push(match_result.fills.clone());
//                        let now_utc = Utc::now();
//                    
//                        let market_update = MarketUpdateAfterTrade::new(
//                            order_book_symbol, 
//                            order_book_last_price,
//                            order_book_depth,
//                            now_utc.timestamp(), 
//                            now_utc.timestamp(), 
//                            match_result
//                        );
//                        //println!("sending events to publisher");
//                        // engine is the bottle neck , push try psuh dosent matter 
//                         self.sending_event_to_publisher_try.try_push(Event::new(market_update));
//                            
//                       // println!("sedning events to publisher ");
//                    }
//                    
//                }
//            }    
//            match self.cancel_order_queue.dequeue(){
//                Ok(Some(order_to_be_canceled))=>{
//                    if let Some(order_book) = self.books.get_mut(&order_to_be_canceled.symbol){
//                        order_book.cancel_order(order_to_be_canceled.order_id , feedCallback);
//                        // integrate order_event_queue with engine also 
//                    }
//                }
//                Ok(None)=>{
//                    // no order to be canceled do smth
//                }
//                Err(_)=>{
//                    eprint!("cancel queue erorr 3")
//                }
//            }
//             //spin loop 
//            ////else{
//            ////    std::hint::spin_loop();
//            //}
//
//            if last_log.elapsed().as_secs() >= 2 {
//                let rate = count as f64 / last_log.elapsed().as_secs_f64();
//                eprintln!("[ENGINE] {:.2}M orders/sec", rate / 1_000_000.0);
//                count = 0;
//                last_log = std::time::Instant::now();
//            }
//        }
//        
//    }
//
//}
//impl Engine for MyEngine{
//    fn add_book(&mut self , symbol : u32) {
//        let new_book = OrderBook::new(symbol);
//        self.books.insert(symbol, new_book);
//        self.book_count = self.book_count.saturating_add(1);
//    }
//    fn get_book(&self , symbol : u32)->Option<&OrderBook> {
//       self.books.get(&symbol).map(|orderbook| orderbook)
//    }
//    fn get_book_mut(&mut self, symbol: u32) -> Option<&mut OrderBook> {
//        self.books.get_mut(&symbol)
//    }
//    fn get_book_count(&self)->usize {
//        self.book_count
//    }
//    fn has_book(&self, symbol: u32) -> bool {
//        self.books.contains_key(&symbol)
//    }
//
//    // cleaning up logic reqd 
//    fn remove_book(&mut self , symbol : u32) {
//        if self.books.contains_key(&symbol){
//            self.books.remove(&symbol);
//            self.book_count = self.book_count.saturating_sub(1);
//        }
//    }
//}



pub const DEPTH_N: usize = 20;
pub struct STEngine{
    pub engine_id :usize ,
    pub book_count : usize, 
    pub books : Vec<Option<OrderBook>>,
    pub cancel_order_queue : CancelOrderQueue,
    pub sending_event_to_publisher_try : Producer<Event>,
    pub sending_order_events_to_writter_try : Producer<OrderEvents>,
}
impl STEngine{
    pub fn new( engine_id : usize , 
        event_sender_to_publisher : Producer<Event> , 
        sending_order_events_to_writter_try : Producer<OrderEvents>, 
    )->Self {
        // initialise the publisher channel here 
            let cancel_order_queue = CancelOrderQueue::open("/tmp/CancelOrders");
            if cancel_order_queue.is_err(){
                eprintln!("Error initialising the cancel order queue in the Stengine ");   
            }
            Self{
                engine_id,
                book_count : 0 ,
                books : (0..MAX_SYMBOLS).map(|_| None).collect(),
                cancel_order_queue : cancel_order_queue.unwrap(),
                sending_event_to_publisher_try : event_sender_to_publisher,
                sending_order_events_to_writter_try
            } 
    }
    
    pub fn process_order<F>(&mut self , mut recieved_order : Order , mut feedCallBack : F)->(Option<MatchResult> , Option<MarketUpdateAfterTrade>) where F : FnMut(MarketMakerFeed){
      
        if let Some(order_book) = self.get_book_mut(recieved_order.symbol){
            
            let events = match recieved_order.order_type {
                0 => order_book.match_market_order(&mut recieved_order , feedCallBack),
                1 => match recieved_order.side {
                    Side::Ask => order_book.match_ask(&mut recieved_order , feedCallBack),
                    Side::Bid => order_book.match_bid(&mut recieved_order , feedCallBack)
                }
                _ => {
                    eprint!("Invalid order struct");
                    Err(OrderBookError::InvalidOrder)
                }
            };
        
            if let Ok(match_result) = events {
                let now_utc = Utc::now();
                let order_book_symbol = order_book.symbol;
                let order_book_last_price = order_book.last_trade_price;
                let order_book_depth = order_book.get_depth();
                let market_update = MarketUpdateAfterTrade::new(
                    order_book_symbol, 
                    order_book_last_price,
                    order_book_depth,
                    now_utc.timestamp(), 
                    now_utc.timestamp(), 
                    match_result.clone()
                );

                return (Some(match_result), Some(market_update));
            }
            
        }
        (None , None)
    }

    pub fn snapshot_for_all_book<F , G>(&mut self , mut emit : F  , mut next_event_id : G )where F : FnMut(OrderBookSnapShot) , G : FnMut()->u64{
       // println!("inside the snapthost function");
        
        for orderbook in self.books.iter(){
            match orderbook {
                Some(book)=>{
                    let (bids , asks) = book.get_depth_upto_n::<DEPTH_N>();
                    emit(OrderBookSnapShot { 
                        timestamp : SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_nanos() as i64 , 
                        event_id: next_event_id(), 
                        symbol: book.symbol, 
                        bids ,
                        asks
                    });
                }
                None => {}
            }
          
        }
    }
}



impl Engine for STEngine{
    fn add_book(&mut self , symbol : u32) {
        let new_book = OrderBook::new(symbol);
        self.books[symbol as usize] = Some(new_book);
        self.book_count = self.book_count.saturating_add(1);
    }
    fn get_book(&self , symbol : u32)->Option<&OrderBook> {
        self.books[symbol as usize].as_ref()
    }
    fn get_book_mut(&mut self, symbol: u32) -> Option<&mut OrderBook> {
        self.books[symbol as usize].as_mut()
    }
    fn get_book_count(&self)->usize {
        self.book_count
    }
    fn has_book(&self, symbol: u32) -> bool {
       self.books[symbol as usize].is_some()
    }

    // cleaning up logic reqd 
    fn remove_book(&mut self , symbol : u32) {
        if self.has_book(symbol){
            self.books[symbol as usize] = None;
            self.book_count = self.book_count.saturating_sub(1);
        }
    }
}