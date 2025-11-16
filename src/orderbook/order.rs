use crate::orderbook::types::{OrderId };


#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub  enum Side{
    Bid ,
    Ask 
}
#[derive(Debug , Copy , Clone)]
pub struct Order{
   // pub order_type : Type,
    pub order_id : OrderId , 
    pub side : Side , 
    pub shares_qty : u32 , 
    pub price : u64 ,
    pub timestamp : u64 , 
    pub next : Option<usize>,
    pub prev : Option<usize>,
    pub symbol: u32,
}

impl Order{
    pub fn new(order_id : u64 , side : Side , shares_qty : u32 , price : u64 , timestamp :u64 , symbol : u32)->Self{
        Self{
            order_id ,
            side ,
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
    // Then u32s (4-byte aligned)
    pub client_id: u32,
    pub shares_qty: u32,
    // Then u8s (1-byte aligned)
    pub symbol: u32,
    pub side: u8,   // 0=buy, 1=sell
    pub status: u8, // 0=pending, 1=filled, 2=rejected
    // Array of bytes last
    _padding: [u8; 10], // padding to make it 48 bytes     
}

impl Default for ShmOrder {
    fn default() -> Self {
        ShmOrder {
            order_id: 0,
            client_id: 0,
            symbol: 0,
            shares_qty: 0,
            price: 0,
            side: 0,
            timestamp: 0,
            status: 0,
            _padding: [0; 10],
        }
    }
}