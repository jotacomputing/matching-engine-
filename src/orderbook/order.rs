use crate::orderbook::types::{OrderId };
use crate::orderbook::order_manager::OrderKey;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub  enum Side{
    Bid ,
    Ask 
}
#[derive(Debug , Copy , Clone)]
pub struct Order{
    pub order_type : Type,
    pub order_id : OrderId , 
    pub side : Side , 
    pub shares_qty : u64 , 
    pub price : u64 ,
    pub timestamp : u64 , 
    pub next : Option<OrderKey>,
    pub prev : Option<OrderKey>
}


#[derive(Debug , Copy , Clone)]
pub enum Type {
    Market ,
    Limit 
}