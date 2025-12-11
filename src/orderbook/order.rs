use crate::orderbook::types::{OrderId };


#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub  enum Side{
    Bid ,
    Ask 
}
#[derive(Debug , Copy , Clone)]
pub struct Order{
   // pub order_type : Type,
    pub user_id : u64,
    pub order_id : OrderId , 
    pub side : Side , 
    pub order_type : u8,
    pub shares_qty : u32 , 
    pub price : u64 ,
    pub timestamp : u64 , 
    pub next : Option<usize>,
    pub prev : Option<usize>,
    pub symbol: u32,
}

impl Order{
    pub fn new(user_id : u64 ,order_id : OrderId , side : Side, order_type : u8 , shares_qty : u32 , price : u64 , timestamp :u64 , symbol : u32)->Self{
        Self{
            user_id,
            order_id ,
            side ,
            order_type , 
            shares_qty,
            price ,
            timestamp,
            next : None,
            prev : None , 
            symbol
        }
    }
}

#[derive(Debug , Copy , Clone)]
pub enum Type {
    Market ,
    Limit 
}
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct ShmOrder{
    pub order_id: u64,
    pub price: u64,
    pub timestamp: u64,
    pub user_id : u64 ,
    // Then u32s (4-byte aligned)

    pub shares_qty: u32,
    // Then u8s (1-byte aligned)
    pub symbol: u32,
    pub side: u8,   // 0=buy, 1=sell
    pub order_type : u8,   // 0 -> market order  , 1 -> limit order 
    pub status: u8, // 0=pending, 1=filled, 2=rejected
}

impl Default for ShmOrder {
    fn default() -> Self {
        ShmOrder {
            user_id : 0 ,
            order_id: 0,
            symbol: 0,
            shares_qty: 0,
            price: 0,
            side: 0,
            order_type : 0 , 
            timestamp: 0,
            status: 0,
        }
    }
}