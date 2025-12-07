use std::collections::HashMap;
use crate::orderbook::order::{Order, Side};
use crate::orderbook::types::{Event, Fills} ;
use crate::orderbook::order_book::OrderBook;
use crate::shm::queue::Queue;
use crossbeam::channel::{Sender , Receiver};

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
    pub event_publisher : Sender<Event>,
    pub test_orderbook : OrderBook,
    pub sender_to_balance_manager : Sender<Fills>,
    pub order_receiver :Receiver<Order>
}

impl MyEngine{
    pub fn new(event_publisher : Sender<Event>, engine_id : usize , sender_to_balance_manager: Sender<Fills> , order_receiver :Receiver<Order>)->Self {
        // initialise the publisher channel here 
        
            Self{
                engine_id,
                book_count : 0 ,
                books : HashMap::new(),
                event_publisher  ,
                test_orderbook : OrderBook::new(100),
                sender_to_balance_manager , 
                order_receiver
            } 
            
    }
    pub fn run_engine(&mut self ){
        eprintln!("[ENGINE] Started (crossbeam batched mode) on core 1");

        let mut count = 0u64;
        let mut last_log = std::time::Instant::now();
        loop {
            match self.order_receiver.recv() {
                Ok(mut recieved_order)=>{
                    println!("recived order {:?}" , recieved_order);
                    if let Some(order_book) = self.get_book_mut(recieved_order.symbol){
                        let events = match recieved_order.side {
                            Side::Bid => order_book.match_bid(&mut recieved_order),
                            Side::Ask => order_book.match_ask(&mut recieved_order)
                        };
                        println!("{:?}" , events);
                        if let Ok(match_result)=events{
                            let _ = self.sender_to_balance_manager.send(match_result.fills.clone());
                            println!("sending fills to balance manager ");
                            let _ = self.event_publisher.send(Event::MatchResult(match_result));
                        }
                    }
                }
                Err(_)=>{

                }
            }

            if last_log.elapsed().as_secs() >= 2 {
                let rate = count as f64 / last_log.elapsed().as_secs_f64();
                eprintln!("[Balance Manager] {:.2}M orders/sec", rate / 1_000_000.0);
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


