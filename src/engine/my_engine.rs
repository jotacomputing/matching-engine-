use std::collections::HashMap;
use crate::orderbook::order::{Order, Side};
use crate::orderbook::types::Event ;
use crate::orderbook::order_book::OrderBook;
use crate::shm::queue::Queue;

pub trait Engine{
    fn add_book(&mut self , symbol : u32);
    fn get_book(&self , symbol : u32)->Option<&OrderBook>;
    fn get_book_mut(&mut self, symbol: u32) -> Option<&mut OrderBook>;
    fn remove_book(&mut self , symbol : u32);
    fn get_book_count(&self)->usize;
    fn has_book(&self , symbol : u32)->bool;
}

pub struct MyEngine{
    // the engine will own all the orderbooks
    pub engine_id :usize ,
    pub book_count : usize, 
    pub books : HashMap< u32 , OrderBook>,
    pub event_publisher : crossbeam::channel::Sender<Event>
}

impl MyEngine{
    pub fn new(event_publisher :  crossbeam::channel::Sender<Event>, engine_id : usize)->Self {
        // initialise the publisher channel here 
        
            Self{
                engine_id,
                book_count : 0 ,
                books : HashMap::new(),
                event_publisher  ,
            } 
            
    }
    pub fn run_engine(&mut self ){
        // the queue struct (shared memory file will be initialised by the producer )
        // we need to initlaise a queue struct here and then start listening to it in an infinite loop
        // on reciveing the order we shud call the match function after serialising the order 




        let mut queue = match Queue::open("/tmp/sex") {
            Ok(q)=>q,
            Err(e)=>{
                eprint!("error occoured {}"  , e);
                return;
            }
        };
        let mut count = 0u64;
        let mut last_log = std::time::Instant::now();
        
        loop {
            match queue.dequeue() {
                Ok(Some(shm_order))=>{
                    
                    //println!("got the shm order");
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
                    let mut my_order = Order::new(shm_order.order_id, order_side, shm_order.shares_qty, shm_order.price, shm_order.timestamp, shm_order.symbol);
                    
                    if let Some(order_book) = self.get_book_mut(my_order.symbol){
                        let events = match order_side {
                            Side::Bid => order_book.match_bid(&mut my_order),
                            Side::Ask => order_book.match_ask(&mut my_order)
                        };
                        //println!("{:?}" , events);
                        if let Ok(match_result)=events{
                            let _ = self.event_publisher.send(Event::MatchResult(match_result));
                        }
                       
                    }
                    count+=1;
                    if last_log.elapsed().as_secs() >= 2 {
                        let rate = count as f64 / last_log.elapsed().as_secs_f64();
                        eprintln!("[MATCH ONLY] {:.2}M orders/sec", rate / 1_000_000.0);
                        count = 0;
                        last_log = std::time::Instant::now();
                    }
                
                }
                Ok(None)=>{
                    //println!("order not reiceved");
                }
                Err(_)=>{
                    println!("Some errorr");
                }
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