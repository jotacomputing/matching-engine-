use std::sync::Arc;
use crate::{orderbook::order::Side, publisher::event_publisher::EventPublisher};
pub type OrderId = u64;
use thiserror::Error;
use serde::{Serialize, Deserialize };


#[derive(Debug , Clone , Copy)]
pub struct Fill{
    pub price : u64 , 
    pub quantity : u32 ,
    // taker orderid -> incoming order tht caused the match 
    pub taker_order_id : OrderId,
    // maker -> the order that was on the book that caused the match 
    pub maker_order_id : OrderId , 
    pub taker_side : Side ,
    pub maker_user_id : u64 ,
    pub taker_user_id : u64 , 
    pub symbol : u32
}

impl Fill{
    pub fn new(price:u64 , quantity:u32 , taker_order_id : OrderId , maker_order_id : OrderId , maker_user_id : u64 ,  taker_user_id : u64 ,  symbol : u32 , taker_side : Side) -> Self{
        Self{
             price  ,
             quantity , 
             taker_order_id ,
             maker_order_id , 
             maker_user_id , 
             taker_user_id , 
             symbol ,
             taker_side
        }
    }

    pub fn total_volume(&self)->u64{
        self.price * self.quantity as u64
    }
}
#[derive(Debug  , Clone)]
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
#[derive(Debug , Clone)]
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

#[derive(Debug , Error)]
pub enum OrderBookError{
    // aff errors that can occour 
}


#[derive(Debug , Error)]
pub enum PubLishError{

}

pub enum PublishSuccess{
    
}
#[derive(Debug )]
pub enum BalanceManagerError{
    InsufficientFunds ,
    BalanceLockingFailed , 
    UserNotFound ,
    BalanceNotFound,
    InvalidSymbol 
}

pub struct BalanceInfo{
    
}
#[derive(Debug )]
pub enum ShmReaderError{
    QueueError
}
#[derive(Debug )]
pub enum QueueError{
    QueueFullError , 
    QueueEmptyError
}


#[derive(Debug)]
pub struct Event {
    pub market_update :  MarketUpdateAfterTrade
}

impl Event{
    pub fn new(market_update : MarketUpdateAfterTrade)->Self{
        Self { market_update }
    }
}
#[derive(Debug)]
pub struct MarketUpdateAfterTrade{
    pub symbol : u32 ,
    pub last_traded_price : u64 , 
    pub depth : (Vec<[String ; 3]> , Vec<[String ; 3]>),
    pub event_time : i64 ,
    pub trade_time : i64 , 
    pub match_result : MatchResult
}
impl MarketUpdateAfterTrade {
    pub fn new(symbol : u32 , last_traded_price : u64 ,  depth : (Vec<[String ; 3]> , Vec<[String ; 3]>), event_time : i64 ,trade_time : i64 , match_result : MatchResult)->Self{
        Self { symbol , last_traded_price, depth, event_time, trade_time ,  match_result }
    }
}




#[derive(Debug, Serialize, Deserialize)]
pub struct TickerData {
    #[serde(rename = "e")]
    pub event: String, // "ticker"

    #[serde(rename = "s")]
    pub symbol: u32,

    #[serde(rename = "E")]
    pub event_time: i64,

    #[serde(rename = "p")]
    pub price: u64,
}

impl TickerData{
    pub fn new(event: String , symbol: u32,event_time: i64,price: u64,)->Self{
        Self { event, symbol , event_time , price }
    }
}


#[derive(Serialize, Deserialize, Debug)]
pub struct DepthData{
    #[serde(rename = "e")]
    pub event: String,         

    #[serde(rename = "s")]
    pub symbol: u32,

    #[serde(rename = "E")]
    pub event_time: i64,

    #[serde(rename = "T")]
    pub trade_time: i64,

    #[serde(rename = "U")]
    pub first_id: i64,

    #[serde(rename = "u")]
    pub last_id: i64,

    #[serde(rename = "b")]
    pub bids: Vec<[String; 3]>,  

    #[serde(rename = "a")]
    pub asks: Vec<[String; 3]>,
}

impl DepthData{
    pub fn new(event: String,symbol: u32, event_time: i64,trade_time: i64 , bids: Vec<[String; 3]>, asks: Vec<[String; 3]>,)->Self{
        Self { event , symbol , event_time , trade_time , first_id : 0 , last_id : 0 , bids , asks }
    }
}


#[derive(Debug, Serialize, Deserialize)]
pub struct TradeData {
    #[serde(rename = "e")]
    pub event: String,

    #[serde(rename = "s")]
    pub symbol: u32,

    #[serde(rename = "E")]
    pub event_time: i64,

    #[serde(rename = "T")]
    pub trade_time: i64,

    #[serde(rename = "t")]
    pub trade_id: i64,

    #[serde(rename = "p")]
    pub price: u64,

    #[serde(rename = "q")]
    pub quantity: u32,

    #[serde(rename = "a")]
    pub buyer_order_id: String,

    #[serde(rename = "b")]
    pub seller_order_id: String,

    #[serde(rename = "m")]
    pub is_buyer_maker: bool,
}

impl TradeData{
    pub fn new(event: String, symbol: u32, event_time: i64, trade_time: i64, price: u64,quantity: u32, buyer_order_id: String,seller_order_id: String,
        is_buyer_maker: bool)->Self{
        Self { event, symbol , event_time , trade_time , trade_id: 0 , price , quantity , buyer_order_id , seller_order_id , is_buyer_maker }
    }
}
// the trade data will be extracted by each fills by the manager 