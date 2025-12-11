use crate::orderbook::book::BookSide;
use crate::orderbook::order::{ Order, Side };
use crate::orderbook::order_manager::OrderManager;
use std::sync::atomic::{ AtomicU64, Ordering};
use crate::orderbook::types::{Fill , Fills , MatchResult  , OrderBookError};
use crate::orderbook::iterator:: LevelsWithCumalativeDepth;


#[derive(Debug)]
pub struct OrderBook{
    pub symbol : u32 , 
    pub askside : BookSide,
    pub bidside : BookSide,
    pub last_trade_price : AtomicU64,
    pub manager : OrderManager,
    pub fill_buffer : Vec<Fill>
}

impl OrderBook{
    pub fn new(symbol : u32 )->Self{
        Self {
            symbol ,
            askside: BookSide::new(Side::Ask),
            bidside: BookSide::new(Side::Bid) ,
            last_trade_price: AtomicU64::new(0),
            manager : OrderManager::new(),
            fill_buffer : Vec::with_capacity(50),
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
        self.fill_buffer.clear();
        let opposite_side = match order.side{
            Side::Ask => &mut self.bidside , 
            Side::Bid => &mut self.askside,
        };  // took a mutable refrence of the side owned by the orderbook 

        //let mut fills = Fills::new();

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
                    let  oldest_order_key = level.remove_oldest_order(&mut self.manager).unwrap();
                    let ( shares , order_id , user_id) = {
                        let oldest_order =  self.manager.get_mut(oldest_order_key).unwrap();
                        (oldest_order.shares_qty , oldest_order.order_id , oldest_order.user_id )
                    };

                    if order.shares_qty >= shares{
                        // then this order will be complete consumed 
                        // TODO -> expose a function in the ordermanager that removes the order from the maps
                        // we get a copy of shares , but we dont need to update the actual because we are deleting
                        let consumed = shares;
                        order.shares_qty = order.shares_qty.saturating_sub(shares);
                        self.fill_buffer.push(Fill{
                            price : best_price ,
                            quantity : consumed , 
                            taker_order_id : order.order_id,
                            maker_order_id : order_id , 
                            maker_user_id : user_id , 
                            taker_user_id : order.user_id , 
                            symbol : self.symbol,
                            taker_side : order.side
                        });
                         self.manager.remove_order(order_id);
                         
                    }
                    else {
                        // if shares is more then then the market order is finished and then oldest order will
                        // be chnaged and inserted at the head of the level not the tail 
                        let consumed = order.shares_qty;
                        self.manager.get_mut(oldest_order_key).unwrap().shares_qty =  self.manager.get_mut(oldest_order_key).unwrap().shares_qty.saturating_sub(consumed);
                        // changed in the orignal map too 
                        self.fill_buffer.push(Fill{
                            price : best_price ,
                            quantity : consumed , 
                            taker_order_id : order.order_id,
                            maker_order_id : order_id,
                            maker_user_id : user_id , 
                            taker_user_id : order.user_id , 
                            symbol : self.symbol,
                            taker_side : order.side
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
            order_id : order.order_id , fills : {Fills { fills: self.fill_buffer.clone() }} , remaining_qty:0
        }) 
    }

    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub fn match_bid(&mut self , order: &mut Order)->Result<MatchResult , OrderBookError>{
       // println!("recived the order , matching now");
       self.fill_buffer.clear();
        //let mut fills =  Fills::new();
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


                    let  oldest_order_key = level.remove_oldest_order(&mut self.manager).unwrap();
                    let ( shares , order_id , user_id) = {
                        let oldest_order = self.manager.get_mut(oldest_order_key).unwrap();
                        (oldest_order.shares_qty , oldest_order.order_id , oldest_order.user_id )
                    };

                    if order.shares_qty >= shares{
                        // then this order will be complete consumed 
                        // TODO -> expose a function in the ordermanager that removes the order from the maps
                        // we get a copy of shares , but we dont need to update the actual because we are deleting
                        let consumed = shares;
                        order.shares_qty = order.shares_qty.saturating_sub(shares);
                        self.fill_buffer.push(Fill{
                            price : best_price ,
                            quantity : consumed , 
                            taker_order_id : order.order_id,
                            maker_order_id : order_id,
                            maker_user_id : user_id , 
                            taker_user_id : order.user_id , 
                            symbol : self.symbol,
                            taker_side : order.side
                        });
                        self.manager.remove_order(order_id);
                    }
                    else {
                        // if shares is more then then the market order is finished and then oldest order will
                        // be chnaged and inserted at the head of the level not the tail 
                        let consumed = order.shares_qty;
                        self.manager.get_mut(oldest_order_key).unwrap().shares_qty -= consumed;
                        // changed in the orignal map too 
                        self.fill_buffer.push(Fill{
                            price : best_price ,
                            quantity : consumed , 
                            taker_order_id : order.order_id,
                            maker_order_id : order_id,
                            maker_user_id : user_id , 
                            taker_user_id : order.user_id , 
                            symbol : self.symbol,
                            taker_side : order.side
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
            // this will go into the order book , owner ship transfered 
            let remaining_order = Order{
                user_id : order.user_id,
                order_id : order.order_id , 
                side : order.side , 
                order_type : 1,
                shares_qty : order.shares_qty ,
                timestamp : order.timestamp , 
                price : order.price ,
                next : None , 
                prev : None,
                symbol : order.symbol
            };
            self.bidside.insert(remaining_order , &mut self.manager);
        }
        if !self.fill_buffer.is_empty(){
            self.last_trade_price.store(
                self.fill_buffer.last().unwrap().price,
                Ordering::Relaxed,
            );
        }

        Ok(MatchResult{
            order_id : order.order_id , fills : {Fills { fills: self.fill_buffer.clone() }} , remaining_qty : order.shares_qty
        })
    }
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub fn match_ask(&mut self , order: &mut Order)->Result<MatchResult , OrderBookError>{
       // println!("recived the order , matching now");
       // let mut fills = Fills::new();
       self.fill_buffer.clear();
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
                    let  oldest_order_key = level.remove_oldest_order(&mut self.manager).unwrap();
                    let ( shares , order_id , user_id) = {
                        let oldest_order = self.manager.get_mut(oldest_order_key).unwrap();
                        (oldest_order.shares_qty , oldest_order.order_id  , oldest_order.user_id)
                    };

                    if order.shares_qty >= shares{
                        // we get a copy of shares , but we dont need to update the actual because we are deleting
                        let consumed = shares;
                        order.shares_qty = order.shares_qty.saturating_sub(shares);
                        self.fill_buffer.push(Fill{
                            price : best_price ,
                            quantity : consumed , 
                            taker_order_id : order.order_id,
                            maker_order_id : order_id,
                            maker_user_id : user_id , 
                            taker_user_id : order.user_id , 
                            symbol : self.symbol,
                            taker_side : order.side
                        });
                        self.manager.remove_order(order_id);
                    }
                    else {
                        // if shares is more then then the market order is finished and then oldest order will
                        // be chnaged and inserted at the head of the level not the tail 
                        let consumed = order.shares_qty;
                        self.manager.get_mut(oldest_order_key).unwrap().shares_qty -= consumed;
                        // changed in the orignal map too 
                        self.fill_buffer.push(Fill{
                            price : best_price ,
                            quantity : consumed , 
                            taker_order_id : order.order_id,
                            maker_order_id : order_id,
                            maker_user_id : user_id , 
                            taker_user_id : order.user_id , 
                            symbol : self.symbol,
                            taker_side : order.side
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
            let remaining_order = Order{
                user_id : order.user_id,
                order_id : order.order_id , 
                side : order.side , 
                order_type : 1,
                shares_qty : order.shares_qty ,
                timestamp : order.timestamp , 
                price : order.price ,
                next : None , 
                prev : None,
                symbol : order.symbol
            };
            self.askside.insert(remaining_order, &mut self.manager);
        }
        if !self.fill_buffer.is_empty(){
            self.last_trade_price.store(
                self.fill_buffer.last().unwrap().price,
                Ordering::Relaxed,
            );
        }
        Ok(MatchResult{
            order_id : order.order_id , fills : {Fills { fills: self.fill_buffer.clone() }} , remaining_qty : order.shares_qty
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

    pub fn get_depth(&self)->(Vec<[String ; 3]> , Vec<[String ; 3]>){
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

    pub fn cancel_order(&mut self ,order_id : u64){
       if let Some(&order_index) = self.manager.id_to_index.get(&order_id){
            // we got the orderIndex 
            let (side , price) = {
                let order = self.manager.get(order_index).unwrap();
                (order.side , order.price)
            };
            match side{
                Side::Ask => {
                    self.askside.delete_order(price, &mut self.manager, order_id);

                },
                Side::Bid => {
                    self.bidside.delete_order(price, &mut self.manager, order_id);
                }
            }
       }
    }
}

