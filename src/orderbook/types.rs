use std::sync::Arc;
use crate::orderbook::order::Side;
pub type OrderId = u64;
#[derive(Debug)]
pub struct Fill{
    pub price : u64 , 
    pub quantity : u32 ,
    // taker orderid -> incoming order tht caused the match 
    pub taker_order_id : OrderId,
    // the order that was on the book that caused the match 
    pub maker_order_id : OrderId
}

impl Fill{
    pub fn new(price:u64 , quantity:u32 , taker_order_id : OrderId , maker_order_id : OrderId) -> Self{
        Self{
             price  ,
             quantity , 
             taker_order_id ,
             maker_order_id

        }
    }

    pub fn total_volume(&self)->u64{
        self.price * self.quantity as u64
    }
}
#[derive(Debug)]
pub struct Fills{
    pub fills : Vec<Fill>
}

impl Fills{
    pub fn new()->Self{
        Self{
            fills : Vec::with_capacity(1000)
        }
    }

    pub fn add(&mut self , fill : Fill){
        self.fills.push(fill);
    }

}
// this can be given back to the Api using the pubsub
#[derive(Debug)]
pub struct MatchResult{
    /// The ID of the incoming order that initiated the match
    pub order_id : OrderId , 
    pub fills : Fills,
    pub remaining_qty : u32,
}

impl MatchResult{
    pub fn new(order_id: OrderId, initial_quantity: u32)->Self{
        Self { order_id , fills: Fills::new(), remaining_qty: initial_quantity }
    }
    pub fn add_transaction(&mut self , fill : Fill){
       self.remaining_qty =  self.remaining_qty.saturating_sub(fill.quantity);
       self.fills.add(fill);
    }
}
#[derive(Debug)]
pub struct TradeResult {
    pub symbol : String ,
    pub match_result : MatchResult
}

impl TradeResult{
    pub fn new(symbol : String , match_result : MatchResult)->Self{
        Self { symbol, match_result }
    }
}

//pub type TradeListener = Arc<dyn Fn(&TradeResult) + Send + Sync>;
pub type TradeListener = Arc<dyn Fn(TradeResult) + Send + Sync>;


#[derive(Debug)]
pub struct PriceLevelChangedEvent{
    pub side : Side  ,
    pub quantity : u64 , 
    pub price : u64,
}

pub type PriceLevelChangedEventListener = Arc<dyn Fn(PriceLevelChangedEvent) + Send+Sync>;

// dyn Fn() means any type which taken in a PricelevelChangedEvent and returns nothing 
// if we dint want it to be thread safe we cud have used just Box 

#[derive(Debug)]
pub enum OrderBookError{
    // aff errors that can occour 
}
#[derive(Debug)]
pub enum Event {
    PriceLevelChangedEvent(PriceLevelChangedEvent) ,
    MatchResult(MatchResult)
}
pub struct PubLishError{

}

pub struct PublishSuccess{
    
}