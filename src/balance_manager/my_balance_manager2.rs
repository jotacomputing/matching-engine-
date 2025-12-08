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
use dashmap::DashMap;
use crossbeam::channel::{Receiver, Sender};
//use dashmap::DashMap;
use crate::orderbook::types::{BalanceManagerError, Fills, };
use crate::orderbook::order::{ Order, Side};
const MAX_USERS: usize = 100; // pre allocating for a max of 10 million users 
const MAX_SYMBOLS : usize = 100 ; 
const DEFAULT_BALANCE : u64 = 10000;
#[repr(C)]
#[repr(align(64))]  
#[derive(Debug)]
pub struct UserBalance {
    pub user_id: u64,   // 8        
    pub available_balance: u64,      
    pub reserved_balance: u64,         
    pub total_traded_today: u64,  
    pub order_count_today: u64,   
    // 36 bytes , pad to 64 
    _padding: [u8; 28],  
}
impl Default for UserBalance{
    fn default()->Self{
        UserBalance { user_id: 0,
            available_balance: DEFAULT_BALANCE, 
            reserved_balance: 0,
            total_traded_today: 0, 
            order_count_today: 0,
             _padding: [0 ; 28] 
            }
    }
}

#[derive(Debug)]
pub struct UserHoldings{
    pub user_id: u64,     // 8 byte 
    pub available_holdings : [u32 ; MAX_SYMBOLS],
    pub reserved_holdings : [u32 ; MAX_SYMBOLS]
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
pub struct BalanceState{
    pub balances : Box<[UserBalance ; MAX_USERS]>,
    pub holdings : Box<[UserHoldings ; MAX_USERS]>,
    pub user_id_to_index : DashMap<u64 , u32>, // user_id to balance index 
    pub next_free_slot: u32,
    pub total_users: u32,

}
impl BalanceState {
    pub fn new() -> Self {
        Self {
            balances: Box::new(std::array::from_fn(|_| UserBalance::default())),
            holdings: Box::new(std::array::from_fn(|_| UserHoldings::default())),
            user_id_to_index: DashMap::with_capacity(MAX_USERS),
            next_free_slot: 0,
            total_users: 0,
        }
    }
}

impl Default for BalanceState {
    fn default() -> Self {
        Self::new()
    }
}
pub struct MyBalanceManager{
    pub order_sender : crossbeam::channel::Sender<Order>,
    pub fill_recv : crossbeam::channel::Receiver<Fills>,
    pub order_receiver : crossbeam::channel::Receiver<Order>,
    pub state : BalanceState,
}

impl MyBalanceManager{
    pub fn new(order_sender : Sender<Order> , fill_recv :Receiver<Fills> , order_receiver : Receiver<Order>)->Self{
        let balance_state = BalanceState::new();
        Self { order_sender, fill_recv, order_receiver, state: balance_state }
    }
    pub fn get_user_index(&self , user_id : u64 )->Result<u32 , BalanceManagerError>{
        self.state.user_id_to_index
        .get(&user_id).map(|index| *index)
        .ok_or(BalanceManagerError::UserNotFound)
    }
    // taking mutable refrences to the balance manager 
    pub fn get_user_balance(&mut self , user_index : u32 )->&mut UserBalance{
        &mut self.state.balances[user_index as usize]
    }
    pub fn get_user_holdings(&mut self , user_index : u32)->&mut UserHoldings{
        &mut self.state.holdings[user_index as usize]
    }
    
    //// returned the state so that it can be passed to the grpc server 
    pub fn check_and_lock_funds(&mut self , order : Order)->Result<() , BalanceManagerError>{
        // currently for limit orders , we get an order 
        // we have user id , symbol , side , holfings 
        let user_index = self.get_user_index(order.user_id)?;   // fatal error , return immidieately to the function who is calling
        //println!("user exists");
        //println!("user balance ");
        //println!("user holdings");
        
        match order.side {
            Side::Ask =>{
                let holdings = self.get_user_holdings(user_index);
                // wants to sell 
                let avalable_holdings_for_symbol = holdings.available_holdings[order.symbol as usize];
                let reserved_holdings_for_symbol = holdings.reserved_holdings[order.symbol as usize];

                if order.shares_qty > avalable_holdings_for_symbol{
                    return Err(BalanceManagerError::InsufficientFunds);
                }
                
                holdings.available_holdings[order.symbol as usize] = avalable_holdings_for_symbol - order.shares_qty;
                holdings.reserved_holdings[order.symbol as usize] = reserved_holdings_for_symbol + order.shares_qty;
            }
            Side::Bid =>{
                // wants to buy  , if balacne > price * qty , we can rserve 
                // avalable is the free balance right now and reserved is what is alr reserved 
                let balance = self.get_user_balance(user_index);
                let required_balance = order.price*order.shares_qty as u64;
                let avalaible_balance = balance.available_balance;
                let reserved_balance = balance.reserved_balance;

                if required_balance > avalaible_balance {
                    return Err(BalanceManagerError::InsufficientFunds);
                }
                // we can reserv and and pass on the order to the matching egnine 
                balance.available_balance = avalaible_balance - required_balance;
                balance.reserved_balance = reserved_balance+required_balance;   
            }
        }
        Ok(())
    }

    pub fn update_balances_after_trade(&mut self, order_fills: Fills) -> Result<(), BalanceManagerError> {
        //println!("fills , recved , need to update");
        for fill in order_fills.fills {
            
            let maker_index = self.get_user_index(fill.maker_user_id)?;
            let taker_index = self.get_user_index(fill.taker_user_id)?;
            let fill_value = fill.price * fill.quantity as u64;
            
            match fill.taker_side {
                Side::Ask => {
                    // Taker is selling , jo order aya was sell order , order book pe(maker) buy order 
                    // add money , he sold 

                    {let  taker_balance = self.get_user_balance(taker_index);
                    let taker_avail_bal = taker_balance.available_balance;
                    taker_balance.available_balance = taker_avail_bal + fill_value;}
                    
                    // remove holdings from resevred
                    {let  taker_holdings = self.get_user_holdings(taker_index);
                    let taker_reserved_holdings = taker_holdings.reserved_holdings[fill.symbol as usize];
                    taker_holdings.reserved_holdings[fill.symbol as usize] = taker_reserved_holdings - fill.quantity;}
                    
                    {let  maker_balance = self.get_user_balance(maker_index);
                    let maker_reserved_bal = maker_balance.reserved_balance;
                    maker_balance.reserved_balance= maker_reserved_bal - fill_value;}
                    
                    // add shares , he bough 
                    {let  maker_holdings = self.get_user_holdings(maker_index);
                    let maker_avail_holdings = maker_holdings.available_holdings[fill.symbol as usize];
                    maker_holdings.available_holdings[fill.symbol as usize]= maker_avail_holdings + fill.quantity}
                        
                }
                
                Side::Bid => {
                    // Taker is buying , incoming is a buying order 

                   { let  taker_balance = self.get_user_balance(taker_index);
                    let taker_reserved_bal = taker_balance.reserved_balance;
                    taker_balance.reserved_balance= taker_reserved_bal - fill_value;}
                        
                    
                    {let  taker_holdings = self.get_user_holdings(taker_index);
                    let taker_avail_holdings = taker_holdings.available_holdings[fill.symbol as usize];
                    taker_holdings.available_holdings[fill.symbol as usize]= taker_avail_holdings + fill.quantity;}
                    
                    {let  maker_balance = self.get_user_balance(maker_index);
                    let maker_avail_bal = maker_balance.available_balance;
                    maker_balance.available_balance= maker_avail_bal + fill_value;}
    
                    
                    {let  maker_holdings = self.get_user_holdings(maker_index);
                    let maker_reserved_holdings = maker_holdings.reserved_holdings[fill.symbol as usize];
                    maker_holdings.reserved_holdings[fill.symbol as usize]= maker_reserved_holdings - fill.quantity;}
                }
            }
        }
        
        Ok(())
    }


    pub fn run_balance_manager(&mut self){
        eprintln!("[PUBLISHER] Started (crossbeam batched mode) on core 6");
        let mut count = 0u64;
        let mut last_log = std::time::Instant::now();
        loop {
            match self.fill_recv.try_recv() {
                
                Ok(recieved_fill)=>{
                    //println!("fills recdived , updating balances ");
                    let _ = self.update_balances_after_trade(recieved_fill);
                },
                Err(_)=>{

                }
            }

            match  self.order_receiver.try_recv() {
                Ok(recieved_order)=>{
                    //println!("received order ");
                    //println!("starting to reserve funds");
//
                    //println!("\n>>> BM received: order_id={} user_id={} symbol={} qty={} price={}", 
                    //    recieved_order.order_id,
                    //    recieved_order.user_id, 
                    //    recieved_order.symbol,
                    //    recieved_order.shares_qty,
                    //    recieved_order.price);
                    
                    match self.check_and_lock_funds(recieved_order) {
                        Ok(_)=>{
                            //println!("sendint to engine");
                            match self.order_sender.send(recieved_order)  {
                                Ok(_)=>{} , 
                                Err(_)=>{}
                            }
                            count+=1;
                        }
                        Err(BalanceManagerError)=>{
                           eprint!("{:?}" , BalanceManagerError);
                        }
                    }
                }
                Err(_)=>{
                    //eprintln!("channel error");
                }
            } 
            if last_log.elapsed().as_secs() >= 2 {
                let rate = count as f64 / last_log.elapsed().as_secs_f64();
                eprintln!("[Balance Manager] {:.2}M orders/sec", rate / 1_000_000.0);
                count = 0;
                last_log = std::time::Instant::now();
            }
        }
    }

    pub fn add_test_users(&mut self ){
        self.state.user_id_to_index.insert(10, 1);
        self.state.user_id_to_index.insert(20, 2);
        // user id 20  , index = 2 , symbol 0 
        self.state.holdings[2].available_holdings[0] =10;
    }

    pub fn add_throughput_test_users(&mut self) {
        const HIGH_BALANCE: u64 = 100_000_000_000;
        const HIGH_HOLDINGS: u32 = 1_000_000_000;
        
        eprintln!("[BM]  INITIALIZING THROUGHPUT USERS...");
        
        // Add user 10 (buyer)
        self.state.user_id_to_index.insert(10, 1);
        self.state.balances[1].user_id =10;
        self.state.balances[1].available_balance= HIGH_BALANCE;
        
        // Add user 20 (seller)
        self.state.user_id_to_index.insert(20, 2);
        self.state.balances[2].user_id=20;
        self.state.balances[2].available_balance = HIGH_BALANCE;
        
        // Give seller holdings for symbol 0
        for symbol in 0..MAX_SYMBOLS {
            self.state.holdings[2].available_holdings[symbol] = HIGH_HOLDINGS;
        }
        
        // ✅ VERIFY INITIALIZATION
        let user10_bal = self.state.balances[1].available_balance;
        let user20_bal = self.state.balances[2].available_balance;
        let user20_holdings = self.state.holdings[2].available_holdings[0];
        
        eprintln!("[BM] User 10 balance: {}", user10_bal);
        eprintln!("[BM] User 20 balance: {}", user20_bal);
        eprintln!("[BM] User 20 holdings[0]: {}", user20_holdings);
        eprintln!("[BM] User map contains 10: {}", self.state.user_id_to_index.contains_key(&10));
        eprintln!("[BM] User map contains 20: {}", self.state.user_id_to_index.contains_key(&20));
    }
    
    
}