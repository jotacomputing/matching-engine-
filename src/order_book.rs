use crate::book::BookSide;
use crate::order::{self, Order, Side};
use std::collections::VecDeque;
use crate::order_manager::OrderManager;

#[derive(Debug)]
pub struct PriceLevel{
    pub total_volume : u64,
    pub price : u64, 
    pub orders : VecDeque<Order>,
}

pub struct Fill{
    pub price : u64 , 
    pub quantity : u64 ,
    pub orderId : u64
}
#[derive(Debug)]
pub struct OrderBook{
    pub askside : BookSide,
    pub bidside : BookSide,
}

impl OrderBook{
    pub fn new()->Self{
        Self { askside: BookSide::new(Side::Ask), bidside: BookSide::new(Side::Bid) }
    }


    pub fn insert_order(&mut self , order : Order , manager : &mut OrderManager){
        match order.side {
            Side::Ask => self.askside.insert(order , manager) ,
            Side::Bid => self.bidside.insert(order , manager),
        }
    }

    pub fn match_market_order(&mut self , order:&mut Order , manager : &mut OrderManager)->Vec<Fill>{
        // wejust need to fill the shares 
        //if a very large maket order comes and there are not enough shares for it to eat , its canceled

        let opposite_side = match order.side{
            Side::Ask => &mut self.bidside , 
            Side::Bid => &mut self.askside,
        };  // took a mutable refrence of the side owned by the orderbook 

        let mut fills : Vec<Fill> = vec![];

        while order.shares_qty > 0 {
            let best_price = match opposite_side.get_best_price() {
                Some(price) => price,
                None => break 
            };
            // now we start fillig orders at this level 
            let empty = {
                let level = opposite_side.levels.get_mut(&best_price).unwrap();
                // we got the price Level we start matchng 
                while order.shares_qty > 0 && level.check_if_empty() == false{
                    let mut oldest_order_key = level.remove_oldest_order(manager).unwrap();
                    let (mut shares , order_id , mut next_order_key) = {
                        let oldest_order = manager.all_orders.get_mut(oldest_order_key).unwrap();
                        (oldest_order.shares_qty , oldest_order.order_id , oldest_order.next)
                    };

                    if order.shares_qty >= shares{
                        // then this order will be complete consumed 
                        // TODO -> expose a function in the ordermanager that removes the order from the maps
                        // we get a copy of shares , but we dont need to update the actual because we are deleting
                        let consumed = shares;
                        order.shares_qty = order.shares_qty.saturating_sub(shares);
                        fills.push(Fill{
                            price : best_price ,
                            quantity : consumed , 
                            orderId : order_id,
                        });
                        manager.all_orders.remove(oldest_order_key);
                        manager.id_to_key.remove(&order_id);
                    }
                    else {
                        // if shares is more then then the market order is finished and then oldest order will
                        // be chnaged and inserted at the head of the level not the tail 
                        let consumed = order.shares_qty;
                        manager.all_orders.get_mut(oldest_order_key).unwrap().shares_qty -= consumed;
                        // changed in the orignal map too 
                        fills.push(Fill{
                            price : best_price ,
                            quantity : consumed , 
                            orderId : order_id,
                        });
                        order.shares_qty = 0 ; 
                        // take a mutable refrence and update the shares and then insert 
                        level.insert_at_head(oldest_order_key, manager);
                    }

                }

                level.check_if_empty()
            };

            if empty{
                opposite_side.remove_level_if_empty(best_price);
            }

        }

        fills 
    }


    pub fn match_bid(&mut self , order: &mut Order , manager : &mut OrderManager)->Vec<Fill>{
        let mut fills : Vec<Fill> = vec![]; 
        let opposite_side = &mut  self.askside ;
        // we have a bid to match , the best price shud be the loweest ask 
        
        while order.shares_qty > 0 {
            let best_price = match opposite_side.get_best_price(){
                Some(price) => price,
                None => break
            };
            if best_price > order.price{
                break;
            }

            let empty = {
                let level = opposite_side.levels.get_mut(&best_price).unwrap();
                // we got the price Level we start matchng 
                while order.shares_qty > 0 && level.check_if_empty() == false{
                    let mut oldest_order_key = level.remove_oldest_order(manager).unwrap();
                    let (mut shares , order_id , mut next_order_key) = {
                        let oldest_order = manager.all_orders.get_mut(oldest_order_key).unwrap();
                        (oldest_order.shares_qty , oldest_order.order_id , oldest_order.next)
                    };

                    if order.shares_qty >= shares{
                        // then this order will be complete consumed 
                        // TODO -> expose a function in the ordermanager that removes the order from the maps
                        // we get a copy of shares , but we dont need to update the actual because we are deleting
                        let consumed = shares;
                        order.shares_qty = order.shares_qty.saturating_sub(shares);
                        fills.push(Fill{
                            price : best_price ,
                            quantity : consumed , 
                            orderId : order_id,
                        });
                        manager.all_orders.remove(oldest_order_key);
                        manager.id_to_key.remove(&order_id);
                    }
                    else {
                        // if shares is more then then the market order is finished and then oldest order will
                        // be chnaged and inserted at the head of the level not the tail 
                        let consumed = order.shares_qty;
                        manager.all_orders.get_mut(oldest_order_key).unwrap().shares_qty -= consumed;
                        // changed in the orignal map too 
                        fills.push(Fill{
                            price : best_price ,
                            quantity : consumed , 
                            orderId : order_id,
                        });
                        order.shares_qty = 0 ; 
                        // take a mutable refrence and update the shares and then insert 
                        level.insert_at_head(oldest_order_key, manager);
                    }

                }

                level.check_if_empty()
            };

            if empty{
                opposite_side.remove_level_if_empty(best_price);
            }
        }
        if order.shares_qty > 0 {
            // this will go into the order book 
            self.bidside.insert(order.clone() , manager);
        }
        fills
    }

    pub fn match_ask(&mut self , order: &mut Order , manager : &mut OrderManager)->Vec<Fill>{
        let mut fills : Vec<Fill> = vec![]; 
        let opposite_side = &mut  self.bidside ;
        // we have a bid to match , the best price shud be the loweest ask 
        
        while order.shares_qty > 0 {
            let best_price = match opposite_side.get_best_price(){
                Some(price) => price,
                None => break
            };
            if best_price < order.price{
                break;
            }

            let empty = {
                let level = opposite_side.levels.get_mut(&best_price).unwrap();
                // we got the price Level we start matchng 
                while order.shares_qty > 0 && level.check_if_empty() == false{
                    let mut oldest_order_key = level.remove_oldest_order(manager).unwrap();
                    let (mut shares , order_id , mut next_order_key) = {
                        let oldest_order = manager.all_orders.get_mut(oldest_order_key).unwrap();
                        (oldest_order.shares_qty , oldest_order.order_id , oldest_order.next)
                    };

                    if order.shares_qty >= shares{
                        // we get a copy of shares , but we dont need to update the actual because we are deleting
                        let consumed = shares;
                        order.shares_qty = order.shares_qty.saturating_sub(shares);
                        fills.push(Fill{
                            price : best_price ,
                            quantity : consumed , 
                            orderId : order_id,
                        });
                        manager.all_orders.remove(oldest_order_key);
                        manager.id_to_key.remove(&order_id);
                    }
                    else {
                        // if shares is more then then the market order is finished and then oldest order will
                        // be chnaged and inserted at the head of the level not the tail 
                        let consumed = order.shares_qty;
                        manager.all_orders.get_mut(oldest_order_key).unwrap().shares_qty -= consumed;
                        // changed in the orignal map too 
                        fills.push(Fill{
                            price : best_price ,
                            quantity : consumed , 
                            orderId : order_id,
                        });
                        order.shares_qty = 0 ; 
                        // take a mutable refrence and update the shares and then insert 
                        level.insert_at_head(oldest_order_key, manager);
                    }

                }

                level.check_if_empty()
            };

            if empty{
                opposite_side.remove_level_if_empty(best_price);
            }
        }
        if order.shares_qty > 0 {
            // this will go into the order book 
            self.askside.insert(order.clone() , manager);
        }
        fills
    }
}


