use std::collections::BTreeMap ; 
use crate::orderbook::{order::Side, price_level::PriceLevel , order::Order};
use crate::orderbook::order_manager::OrderManager;

#[derive(Debug)]
pub struct BookSide{
    side : Side ,
    pub levels : BTreeMap<u64 , PriceLevel>,
}

impl BookSide{
    pub fn new(side : Side)->Self{
        Self{
            side , 
            levels : BTreeMap::new()
        }
    }

    pub fn get_best_price(&mut self)->Option<u64>{
        match self.side{
            Side::Bid => self.levels.keys().rev().next().cloned(),
            Side::Ask => self.levels.keys().next().cloned(),
        }
    }

    pub fn insert(&mut self , order:Order , manager : &mut OrderManager){
        let price = {
            order.price
        };
        let level =self.levels.entry(price)
        .or_insert_with(||PriceLevel::new(price)
        );
        level.add_order(manager, order);
    }    

    pub fn remove_level_if_empty(&mut self, price: u64) {
        if let Some(level) = self.levels.get(&price) {
            if level.head.is_none() && level.tail.is_none(){
                self.levels.remove(&price);
            }
        }
    }

    pub fn delete_order(&mut self , price : u64 , manager : &mut OrderManager , order_id : u64){
        if let Some(level) =  self.levels.get_mut(&price).as_deref_mut(){
            level.delete_order(order_id, manager);
        }
    }

}