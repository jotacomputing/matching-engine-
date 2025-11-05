use crate::orderbook::book::BookSide;
use crate::orderbook::order::{self, Order, Side , Type};
use core::error;
use std::collections::VecDeque;
use crate::orderbook::order_manager::OrderManager;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use crate::orderbook::types::{Fill , Fills , MatchResult  , OrderBookError};
use crate::orderbook::iterator::{LevelInfo , LevelsWithCumalativeDepth};
use tokio::sync::mpsc;
use crate::orderbook::types::Event;
#[derive(Debug)]
pub struct PriceLevel{
    pub total_volume : u64,
    pub price : u64, 
    pub orders : VecDeque<Order>,
}


#[derive(Debug)]
pub struct OrderBook{
    pub order_reciver : mpsc::Receiver<Order>,
    pub match_res_sender : mpsc::Sender<Event>,
    pub symbol : String , 
    pub askside : BookSide,
    pub bidside : BookSide,
    pub last_trade_price : AtomicU64,
    pub manager : OrderManager
}

impl OrderBook{
    pub fn new(symbol : &str , rx : mpsc::Receiver<Order> , sx : mpsc::Sender<Event>)->Self{
        Self {
            order_reciver : rx , 
            match_res_sender : sx ,
            symbol : String::from(symbol),
            askside: BookSide::new(Side::Ask),
            bidside: BookSide::new(Side::Bid) ,
            last_trade_price: AtomicU64::new(0),
            manager : OrderManager::new()
        }
    }


    pub fn insert_order(&mut self , order : Order ){
        match order.side {
            Side::Ask => self.askside.insert(order ,&mut self.manager) ,
            Side::Bid => self.bidside.insert(order ,&mut self.manager),
        }
    }

    pub fn match_market_order(&mut self , order:&mut Order )->Result<MatchResult , OrderBookError>{
        // wejust need to fill the shares 
        //if a very large maket order comes and there are not enough shares for it to eat , its canceled

        let opposite_side = match order.side{
            Side::Ask => &mut self.bidside , 
            Side::Bid => &mut self.askside,
        };  // took a mutable refrence of the side owned by the orderbook 

        let mut fills = Fills::new();

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
                    let mut oldest_order_key = level.remove_oldest_order(&mut self.manager).unwrap();
                    let (mut shares , order_id , mut next_order_key) = {
                        let oldest_order =  self.manager.all_orders.get_mut(oldest_order_key).unwrap();
                        (oldest_order.shares_qty , oldest_order.order_id , oldest_order.next)
                    };

                    if order.shares_qty >= shares{
                        // then this order will be complete consumed 
                        // TODO -> expose a function in the ordermanager that removes the order from the maps
                        // we get a copy of shares , but we dont need to update the actual because we are deleting
                        let consumed = shares;
                        order.shares_qty = order.shares_qty.saturating_sub(shares);
                        fills.add(Fill{
                            price : best_price ,
                            quantity : consumed , 
                            taker_order_id : order.order_id,
                            maker_order_id : order_id
                        });
                         self.manager.all_orders.remove(oldest_order_key);
                         self.manager.id_to_key.remove(&order_id);
                    }
                    else {
                        // if shares is more then then the market order is finished and then oldest order will
                        // be chnaged and inserted at the head of the level not the tail 
                        let consumed = order.shares_qty;
                        self.manager.all_orders.get_mut(oldest_order_key).unwrap().shares_qty =  self.manager.all_orders.get_mut(oldest_order_key).unwrap().shares_qty.saturating_sub(consumed);
                        // changed in the orignal map too 
                        fills.add(Fill{
                            price : best_price ,
                            quantity : consumed , 
                            taker_order_id : order.order_id,
                            maker_order_id : order_id
                        });
                        order.shares_qty = 0 ; 
                        // take a mutable refrence and update the shares and then insert 
                        level.insert_at_head(oldest_order_key, &mut self.manager);
                    }

                }

                level.check_if_empty()
            };

            if empty{
                opposite_side.remove_level_if_empty(best_price);
            }

        }

        Ok(MatchResult{
            order_id : order.order_id , fills , remaining_qty:0
        }) 
    }


    pub fn match_bid(&mut self , order: &mut Order)->Result<MatchResult , OrderBookError>{
        let mut fills =  Fills::new();
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
                    let mut oldest_order_key = level.remove_oldest_order(&mut self.manager).unwrap();
                    let (mut shares , order_id , mut next_order_key) = {
                        let oldest_order = self.manager.all_orders.get_mut(oldest_order_key).unwrap();
                        (oldest_order.shares_qty , oldest_order.order_id , oldest_order.next)
                    };

                    if order.shares_qty >= shares{
                        // then this order will be complete consumed 
                        // TODO -> expose a function in the ordermanager that removes the order from the maps
                        // we get a copy of shares , but we dont need to update the actual because we are deleting
                        let consumed = shares;
                        order.shares_qty = order.shares_qty.saturating_sub(shares);
                        fills.add(Fill{
                            price : best_price ,
                            quantity : consumed , 
                            taker_order_id : order.order_id,
                            maker_order_id : order_id
                        });
                        self.manager.all_orders.remove(oldest_order_key);
                        self.manager.id_to_key.remove(&order_id);
                    }
                    else {
                        // if shares is more then then the market order is finished and then oldest order will
                        // be chnaged and inserted at the head of the level not the tail 
                        let consumed = order.shares_qty;
                        self.manager.all_orders.get_mut(oldest_order_key).unwrap().shares_qty -= consumed;
                        // changed in the orignal map too 
                        fills.add(Fill{
                            price : best_price ,
                            quantity : consumed , 
                            taker_order_id : order.order_id,
                            maker_order_id : order_id
                        });
                        order.shares_qty = 0 ; 
                        // take a mutable refrence and update the shares and then insert 
                        level.insert_at_head(oldest_order_key, &mut self.manager);
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
            self.bidside.insert(order.clone() , &mut self.manager);
        }
        Ok(MatchResult{
            order_id : order.order_id , fills , remaining_qty : order.shares_qty
        })
    }

    pub fn match_ask(&mut self , order: &mut Order)->Result<MatchResult , OrderBookError>{
        let mut fills = Fills::new();
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
                    let mut oldest_order_key = level.remove_oldest_order(&mut self.manager).unwrap();
                    let (mut shares , order_id , mut next_order_key) = {
                        let oldest_order = self.manager.all_orders.get_mut(oldest_order_key).unwrap();
                        (oldest_order.shares_qty , oldest_order.order_id , oldest_order.next)
                    };

                    if order.shares_qty >= shares{
                        // we get a copy of shares , but we dont need to update the actual because we are deleting
                        let consumed = shares;
                        order.shares_qty = order.shares_qty.saturating_sub(shares);
                        fills.add(Fill{
                            price : best_price ,
                            quantity : consumed , 
                            taker_order_id : order.order_id,
                            maker_order_id : order_id
                        });
                        self.manager.all_orders.remove(oldest_order_key);
                        self.manager.id_to_key.remove(&order_id);
                    }
                    else {
                        // if shares is more then then the market order is finished and then oldest order will
                        // be chnaged and inserted at the head of the level not the tail 
                        let consumed = order.shares_qty;
                        self.manager.all_orders.get_mut(oldest_order_key).unwrap().shares_qty -= consumed;
                        // changed in the orignal map too 
                        fills.add(Fill{
                            price : best_price ,
                            quantity : consumed , 
                            taker_order_id : order.order_id,
                            maker_order_id : order_id
                        });
                        order.shares_qty = 0 ; 
                        // take a mutable refrence and update the shares and then insert 
                        level.insert_at_head(oldest_order_key, &mut self.manager);
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
            self.askside.insert(order.clone() , &mut self.manager);
        }
        Ok(MatchResult{
            order_id : order.order_id , fills , remaining_qty : order.shares_qty
        })
    }

    pub fn get_best_bid(&mut self)->Option<u64>{
        self.bidside.get_best_price()
    }

    pub fn get_best_ask(&mut self)->Option<u64>{
        self.askside.get_best_price()
    }

    pub fn get_last_trade_price(&self)->Option<u64>{
        Some(self.last_trade_price.load(Ordering::Relaxed))
    }

    pub fn get_depth(&self , limit:u64)->(Vec<[String ; 3]> , Vec<[String ; 3]>){
        // tuple of 2 vectros , each with type string and 3 elements 
        let mut bids = Vec::new();
        let mut asks = Vec::new();

        for level in LevelsWithCumalativeDepth::new(&self.askside.levels, Side::Ask){
            asks.push(
               [
                level.price.to_string() , level.qty.to_string() , level.cumalative_depth.to_string()
               ] 
            );
        }

        for level in LevelsWithCumalativeDepth::new(&self.bidside.levels, Side::Bid){
            bids.push(
               [
                level.price.to_string() , level.qty.to_string() , level.cumalative_depth.to_string()
               ] 
            );
        }

        (asks , bids)


    }

    pub async fn run_orderbook(&mut self  ){
        while let Some(mut order) = self.order_reciver.recv().await{
            match order.order_type{
                Type::Limit => {
                    match order.side{
                        Side::Ask =>{
                            if let Some(events) = self.match_ask(&mut order).ok(){
                                let match_res_event = Event::MatchResult(events);
                                let res =  self.match_res_sender.send(match_res_event).await;
                                if res.is_err(){
                                    eprint!("error while publlishing ")
                                }
                            }
                        }

                        Side::Bid =>{
                            if let Some(events) = self.match_bid(&mut order).ok(){
                                let match_res_event = Event::MatchResult(events);
                                let res =  self.match_res_sender.send(match_res_event).await;
                                if res.is_err(){
                                    eprint!("error while publlishing ")
                                }
                               
                            }
                        }
                    }
                }
                Type::Market=>{
                     if let Some(events) = self.match_market_order(&mut order).ok(){
                        let match_res_event = Event::MatchResult(events);
                        let res  = self.match_res_sender.send(match_res_event).await;
                        // sedn depth and price level updates too
                        // call the get depth function of the order book 
                        // Add yielding
                        if res.is_err(){
                            eprint!("error while publlishing ")
                        }
                     }
                }
            }
        }
    }

}

