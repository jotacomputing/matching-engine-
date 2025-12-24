use bounded_spsc_queue::{Consumer, Producer};
use chrono::prelude::*;
use std::collections::HashMap;
use crate::orderbook::order::{Order, Side};
use crate::orderbook::types::{Event, Fills, MarketUpdateAfterTrade, MatchResult} ;
use crate::orderbook::order_book::{ OrderBook};
use crate::shm::cancel_orders_queue::{ CancelOrderQueue};
//use crossbeam::queue::ArrayQueue;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use crate::singlepsinglecq::my_queue::SpscQueue;


pub trait Engine{
    fn add_book(&mut self , symbol : u32);
    fn get_book(&self , symbol : u32)->Option<&OrderBook>; // can only get a refrence , orderbooks are owned by the engine
    fn get_book_mut(&mut self, symbol: u32) -> Option<&mut OrderBook>;
    fn remove_book(&mut self , symbol : u32);
    fn get_book_count(&self)->usize;
    fn has_book(&self , symbol : u32)->bool;
}

pub struct MyEngine{
    // the engine will own all the orderbooks
    pub engine_id :usize ,
    pub book_count : usize, 
    pub books : HashMap<u32 , OrderBook>,
    pub test_orderbook : OrderBook,

    pub bm_engine_order_queue : Arc<SpscQueue<Order>>,
    pub fill_queue : Arc<SpscQueue<Fills>>,
    pub event_queue : Arc<SpscQueue<Event>>,


    pub cancel_order_queue : CancelOrderQueue,


    pub bm_order_reciver_try : Consumer<Order>,
    pub sending_fills_to_bm_try : Producer<Fills>,
    pub sending_event_to_publisher_try : Producer<Event>,
}

impl MyEngine{
    pub fn new( engine_id : usize , bm_engine_order_queue : Arc<SpscQueue<Order>>, 
        fill_queue : Arc<SpscQueue<Fills>>,event_queue : Arc<SpscQueue<Event>>,
        bm_order_reciver_try : Consumer<Order>,
        sending_fills_to_bm_try : Producer<Fills>,
        sending_event_to_publisher_try : Producer<Event>,
        )->Option<Self> {

            let cancel_orders_queue = CancelOrderQueue::open("/tmp/trading/CancelOrders");
            match cancel_orders_queue {
                Ok(queue)=>{
                    Some(Self{
                        engine_id,
                        book_count : 0 ,
                        books : HashMap::new(),
                        test_orderbook : OrderBook::new(100),
                        bm_engine_order_queue,
                        fill_queue,
                        event_queue , 
                        cancel_order_queue : queue , 
                        bm_order_reciver_try , 
                        sending_fills_to_bm_try , 
                        sending_event_to_publisher_try 
                    } )
                }

                Err(_)=>{
                    eprint!("error in init engine because of cancel order queue");
                    None
                }
            }
            
    }
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub fn run_engine(&mut self ){
        eprintln!("[ENGINE] Started (crossbeam batched mode) on core 1");

        let mut count = 0u64;
        let mut last_log = std::time::Instant::now();
        loop {
            // new queue use 
           
            if let Some(mut recieved_order) = self.bm_order_reciver_try.try_pop(){
                //println!("got the order from bm");
                count += 1;
                if let Some(order_book) = self.get_book_mut(recieved_order.symbol){
                    let order_book_symbol = order_book.symbol;
                    let order_book_last_price = order_book.last_trade_price.load(Ordering::Relaxed);
                    let order_book_depth = order_book.get_depth();
                    let events = match recieved_order.order_type {
                        
                        0 => order_book.match_market_order(&mut recieved_order),
                        1 =>{
                            // limit order 
                            match recieved_order.side{
                                Side::Ask => order_book.match_ask(&mut recieved_order),
                                Side::Bid=> order_book.match_bid(&mut recieved_order)
                                
                            }
                        }
                        _=>{
                            eprint!("Invalid order struct");
                            return; 
                        }
                    };
                   
                    if let Ok(match_result)=events{
                        //println!("sending fills to bm");
                        //println!("{:?}" , match_result);
                        let _ = self.sending_fills_to_bm_try.push(match_result.fills.clone());
                        let now_utc = Utc::now();
                    
                        let market_update = MarketUpdateAfterTrade::new(
                            order_book_symbol, 
                            order_book_last_price,
                            order_book_depth,
                            now_utc.timestamp(), 
                            now_utc.timestamp(), 
                            match_result
                        );
                        //println!("sending events to publisher");
                        let _ = self.sending_event_to_publisher_try.push(Event::new(market_update));
                       // println!("sedning events to publisher ");
                    }
                    
                }
            }    
            match self.cancel_order_queue.dequeue(){
                Ok(Some(order_to_be_canceled))=>{
                    if let Some(order_book) = self.books.get_mut(&order_to_be_canceled.symbol){
                        order_book.cancel_order(order_to_be_canceled.order_id);
                        // integrate order_event_queue with engine also 
                    }
                }
                Ok(None)=>{
                    // no order to be canceled do smth
                }
                Err(_)=>{
                    eprint!("cancel queue erorr 3")
                }
            }
             //spin loop 
            ////else{
            ////    std::hint::spin_loop();
            //}

            if last_log.elapsed().as_secs() >= 2 {
                let rate = count as f64 / last_log.elapsed().as_secs_f64();
                eprintln!("[ENGINE] {:.2}M orders/sec", rate / 1_000_000.0);
                count = 0;
                last_log = std::time::Instant::now();
            }
        }
        
    }

}
impl Engine for MyEngine{
    fn add_book(&mut self , symbol : u32) {
        let new_book = OrderBook::new(symbol);
        self.books.insert(symbol, new_book);
        self.book_count = self.book_count.saturating_add(1);
    }
    fn get_book(&self , symbol : u32)->Option<&OrderBook> {
       self.books.get(&symbol).map(|orderbook| orderbook)
    }
    fn get_book_mut(&mut self, symbol: u32) -> Option<&mut OrderBook> {
        self.books.get_mut(&symbol)
    }
    fn get_book_count(&self)->usize {
        self.book_count
    }
    fn has_book(&self, symbol: u32) -> bool {
        self.books.contains_key(&symbol)
    }

    // cleaning up logic reqd 
    fn remove_book(&mut self , symbol : u32) {
        if self.books.contains_key(&symbol){
            self.books.remove(&symbol);
            self.book_count = self.book_count.saturating_sub(1);
        }
    }
    

}


pub struct STEngine{
    pub engine_id :usize ,
    pub book_count : usize, 
    pub books : HashMap<u32 , OrderBook>,
}

impl STEngine{
    pub fn new( engine_id : usize )->Self {
        // initialise the publisher channel here 
        
            Self{
                engine_id,
                book_count : 0 ,
                books : HashMap::new(),
                
            } 
            
    }
    #[inline(always)]
    pub fn process_order(&mut self , mut recieved_order : Order)->Option<MatchResult>{
      
        if let Some(order_book) = self.get_book_mut(recieved_order.symbol){
            let events = match recieved_order.side {
                Side::Bid => order_book.match_bid(&mut recieved_order),
                Side::Ask => order_book.match_ask(&mut recieved_order)
            };
           
            return events.ok();
           // println!("order matched events created ")
        }
        None
    }
}

impl Engine for STEngine{
    fn add_book(&mut self , symbol : u32) {
        let new_book = OrderBook::new(symbol);
        self.books.insert(symbol, new_book);
        self.book_count = self.book_count.saturating_add(1);
    }
    fn get_book(&self , symbol : u32)->Option<&OrderBook> {
       self.books.get(&symbol).map(|orderbook| orderbook)
    }
    fn get_book_mut(&mut self, symbol: u32) -> Option<&mut OrderBook> {
        self.books.get_mut(&symbol)
    }
    fn get_book_count(&self)->usize {
        self.book_count
    }
    fn has_book(&self, symbol: u32) -> bool {
        self.books.contains_key(&symbol)
    }

    // cleaning up logic reqd 
    fn remove_book(&mut self , symbol : u32) {
        if self.books.contains_key(&symbol){
            self.books.remove(&symbol);
            self.book_count = self.book_count.saturating_sub(1);
        }
    }
    

}