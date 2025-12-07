// jobs 
// will mantain userbalances acc to nuser ID 
// will expose functions such as check and lock funds for an order 
// expose functions to update balances after a trade or a fill
// it will constanlty recv data from thw queue as well as reply to the grpc requests 
// on reciving it performs checks and locks the funds for the order if valid 
// it passes the order to the engine to be processed 
// balance manager has 3 responsibilities  update balances from fills , reading from a channel 
// read from the SHM queue for the new order 
// The balanaces and holdings will be in a shared state for the grpc server and the balance manager 
// avalable means free balance or holdings that can be reserved 

use std::sync::Arc;
use std::sync::atomic::{AtomicU64 , AtomicU32  , Ordering};
use dashmap::DashMap;
use crossbeam::channel::{Receiver, SendError, Sender};
//use dashmap::DashMap;
use crate::orderbook::types::{BalanceManagerError, Fill, Fills, MatchResult };
use crate::orderbook::order::{self, Order, Side};
const MAX_USERS: usize = 100; // pre allocating for a max of 10 million users 
const MAX_SYMBOLS : usize = 100 ; 
const DEFAULT_BALANCE : u64 = 10000;
#[repr(C)]
#[repr(align(64))]  
pub struct UserBalance {
    pub user_id: AtomicU64,   // 8        
    pub available_balance: AtomicU64,      
    pub reserved_balance: AtomicU64,         
    pub total_traded_today: AtomicU64,  
    pub order_count_today: AtomicU32,   
    // 36 bytes , pad to 64 
    _padding: [u8; 28],  
}
impl Default for UserBalance{
    fn default()->Self{
        UserBalance { user_id: AtomicU64::new(0),
            available_balance: AtomicU64::new(DEFAULT_BALANCE), 
            reserved_balance: AtomicU64::new(0),
            total_traded_today: AtomicU64::new(0), 
            order_count_today: AtomicU32::new(0),
             _padding: [0 ; 28] 
            }
    }
}
pub struct UserHoldings{
    pub user_id: u64,     // 8 byte 
    pub available_holdings : [AtomicU32 ; MAX_SYMBOLS],
    pub reserved_holdings : [AtomicU32 ; MAX_SYMBOLS]
}
impl Default for UserHoldings{
    fn default() -> Self {
        UserHoldings {
            user_id: 0,
            available_holdings: unsafe { std::mem::zeroed() },  // Faster than from_fn
            reserved_holdings : unsafe {
                std::mem::zeroed()
            }
        }
    }
}
pub struct SharedBalanceState{
    pub balances : Arc<Box<[UserBalance ; MAX_USERS]>>,
    pub holdings : Arc<Box<[UserHoldings ; MAX_USERS]>>,
    pub user_id_to_index : Arc<DashMap<u64 , u32>>, // user_id to balance index 
    pub next_free_slot: AtomicU32,
    pub total_users: AtomicU32,

}
impl SharedBalanceState {
    pub fn new() -> Self {
        Self {
            balances: Arc::new(Box::new(std::array::from_fn(|_| UserBalance::default()))),
            holdings: Arc::new(Box::new(std::array::from_fn(|_| UserHoldings::default()))),
            user_id_to_index: Arc::new(DashMap::with_capacity(MAX_USERS)),
            next_free_slot: AtomicU32::new(0),
            total_users: AtomicU32::new(0),
        }
    }
}

impl Default for SharedBalanceState {
    fn default() -> Self {
        Self::new()
    }
}
pub struct MyBalanceManager{
    pub order_sender : crossbeam::channel::Sender<Order>,
    pub fill_recv : crossbeam::channel::Receiver<Fills>,
    pub order_receiver : crossbeam::channel::Receiver<Order>,
    pub state : Arc<SharedBalanceState>,
}
//pub trait BalanceManagerTrait{
//    fn check_and_lock_funds(order : Order)->Result<() , BalanceManagerError>;
//    fn update_funds_after_trade(order : Order)->Result<() , BalanceManagerError>;
//}

impl MyBalanceManager{
    pub fn new(order_sender : Sender<Order> , fill_recv :Receiver<Fills> , order_receiver : Receiver<Order>)->(Self , Arc<SharedBalanceState>){
        let shared_state = Arc::new(SharedBalanceState::new());
        (Self { 
            order_sender, 
            fill_recv,
            order_receiver, 
            state: Arc::clone(&shared_state)
         }
         , shared_state)
    }
    pub fn get_user_index(&self , user_id : u64 )->Result<u32 , BalanceManagerError>{
        self.state.user_id_to_index
        .get(&user_id).map(|index| *index)
        .ok_or(BalanceManagerError::UserNotFound)
    }

    pub fn get_user_balance(&self , user_index : u32 )->&UserBalance{
        &self.state.balances[user_index as usize]
    }
    pub fn get_user_holdings(&self , user_index : u32)->&UserHoldings{
        &self.state.holdings[user_index as usize]
    }
    
    //// returned the state so that it can be passed to the grpc server 
    pub fn check_and_lock_funds(&self , order : Order)->Result<() , BalanceManagerError>{
        // currently for limit orders , we get an order 
        // we have user id , symbol , side , holfings 
        let user_index = self.get_user_index(order.user_id)?;   // fatal error , return immidieately to the function who is calling
        let balance = self.get_user_balance(user_index);
        let holdings = self.get_user_holdings(user_index);
        match order.side {
            Side::Ask =>{
                // wants to sell 
                let avalable_holdings_for_symbol = holdings.available_holdings[order.symbol as usize].load(Ordering::Acquire);
                let reserved_holdings_for_symbol = holdings.reserved_holdings[order.symbol as usize].load(Ordering::Acquire);

                if order.shares_qty > avalable_holdings_for_symbol{
                    return Err(BalanceManagerError::InsufficientFunds);
                }
                holdings.available_holdings[order.symbol as usize].store(avalable_holdings_for_symbol - order.shares_qty , Ordering::Release);
                holdings.reserved_holdings[order.symbol as usize].store(reserved_holdings_for_symbol + order.shares_qty , Ordering::Release);
            }
            Side::Bid =>{
                // wants to buy  , if balacne > price * qty , we can rserve 
                // avalable is the free balance right now and reserved is what is alr reserved 
                let required_balance = order.price*order.shares_qty as u64;
                let avalaible_balance = balance.available_balance.load(Ordering::Acquire);
                let reserved_balance = balance.reserved_balance.load(Ordering::Acquire);

                if required_balance > avalaible_balance {
                    return Err(BalanceManagerError::InsufficientFunds);
                }
                // we can reserv and and pass on the order to the matching egnine 
                balance.available_balance.store(avalaible_balance - required_balance , Ordering::Release);
                balance.reserved_balance.store(reserved_balance+required_balance , Ordering::Release);   
            }
        }
        Ok(())
    }

    pub fn update_balances_after_trade(&self, order_fills: Fills) -> Result<(), BalanceManagerError> {
        for fill in order_fills.fills {
            
            let maker_index = self.get_user_index(fill.maker_user_id)?;
            let maker_balance = self.get_user_balance(maker_index);
            let maker_holdings = self.get_user_holdings(maker_index);
            
            let taker_index = self.get_user_index(fill.taker_user_id)?;
            let taker_balance = self.get_user_balance(taker_index);
            let taker_holdings = self.get_user_holdings(taker_index);
            
            let fill_value = fill.price * fill.quantity as u64;
            
            match fill.taker_side {
                Side::Ask => {
                    // Taker is selling , jo order aya was sell order , order book pe(maker) buy order 
                    
                    // add money , he sold 
                    let taker_avail_bal = taker_balance.available_balance.load(Ordering::Relaxed);
                    taker_balance.available_balance
                        .store(taker_avail_bal + fill_value, Ordering::Relaxed);
                    
                    // remove holdings from resevred
                    let taker_reserved_holdings = taker_holdings.reserved_holdings[fill.symbol as usize]
                        .load(Ordering::Relaxed);
                    taker_holdings.reserved_holdings[fill.symbol as usize]
                        .store(taker_reserved_holdings - fill.quantity, Ordering::Relaxed);
                    

                    let maker_reserved_bal = maker_balance.reserved_balance.load(Ordering::Relaxed);
                    maker_balance.reserved_balance
                        .store(maker_reserved_bal - fill_value, Ordering::Relaxed);
                    
                    // add shares , he bough 
                    let maker_avail_holdings = maker_holdings.available_holdings[fill.symbol as usize]
                        .load(Ordering::Relaxed);
                    maker_holdings.available_holdings[fill.symbol as usize]
                        .store(maker_avail_holdings + fill.quantity, Ordering::Relaxed);
                }
                
                Side::Bid => {
                    // Taker is buying , incoming is a buying order 
                    
                    
                    let taker_reserved_bal = taker_balance.reserved_balance.load(Ordering::Relaxed);
                    taker_balance.reserved_balance
                        .store(taker_reserved_bal - fill_value, Ordering::Relaxed);
                    
                   
                    let taker_avail_holdings = taker_holdings.available_holdings[fill.symbol as usize]
                        .load(Ordering::Relaxed);
                    taker_holdings.available_holdings[fill.symbol as usize]
                        .store(taker_avail_holdings + fill.quantity, Ordering::Relaxed);
                    
                    
                    let maker_avail_bal = maker_balance.available_balance.load(Ordering::Relaxed);
                    maker_balance.available_balance
                        .store(maker_avail_bal + fill_value, Ordering::Relaxed);
                    
                    
                    let maker_reserved_holdings = maker_holdings.reserved_holdings[fill.symbol as usize]
                        .load(Ordering::Relaxed);
                    maker_holdings.reserved_holdings[fill.symbol as usize]
                        .store(maker_reserved_holdings - fill.quantity, Ordering::Relaxed);
                }
            }
        }
        
        Ok(())
    }


    pub fn run_balance_manager(&mut self){
        loop {
            match  self.order_receiver.recv() {
                Ok(recieved_order)=>{
                    match self.check_and_lock_funds(recieved_order) {
                        Ok(_)=>{
                            match self.order_sender.send(recieved_order)  {
                                Ok(_)=>{} , 
                                Err(_)=>{}
                            }
                        }
                        Err(_)=>{}
                    }
                }
                Err(_)=>{
                    eprintln!("channel error");
                }
            } 
        }
    }
    
}