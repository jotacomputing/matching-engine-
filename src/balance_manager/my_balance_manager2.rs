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
use bounded_spsc_queue::{Consumer, Producer};
use crossbeam_utils::Backoff;
use crate::logger::types::{BalanceDelta, BaseLogs, HoldingDelta};
use crate::shm::holdings_response_queue::{HoldingResQueue, HoldingResponse};
use dashmap::DashMap;
use crate::orderbook::types::{BalanceManagerError, Fills, };
use crate::orderbook::order::{ Order, OrderToBeCanceled, Side};
use crate::shm::event_queue::OrderEvents;
use crate::shm::query_queue::{QueryQueue};
use crate::shm::balance_response_queue::{ BalanceResQueue, BalanceResponse};
const MAX_USERS: usize = 1000; 
const MAX_SYMBOLS : usize = 100 ; 
const DEFAULT_BALANCE : u64 = 10000;
const DEFAULT_HOLDING_QTY: u32 = 100;

const MARKET_MAKER_BALANCE : u64 = 100000000;


#[repr(C)]
#[repr(align(64))]
#[derive(Debug , Clone, Copy)]
pub struct UserBalance {
    pub user_id: u64,   // 8        
    pub available_balance: u64,      
    pub reserved_balance: u64,         
    pub total_traded_today: u64,  
    pub order_count_today: u64,   
    _pad: [u8; 24],
}// 40 bytes alligned to 64 
impl Default for UserBalance{
    fn default()->Self{
        UserBalance { user_id: 0,
            available_balance: DEFAULT_BALANCE, 
            reserved_balance: 0,
            total_traded_today: 0, 
            order_count_today: 0,
            _pad: [0; 24],
            }
    }
}

impl UserBalance{
    pub fn new(user_id : u64)->Self{
        Self {
             user_id, 
             available_balance: DEFAULT_BALANCE, 
             reserved_balance: 0, 
             total_traded_today: 0, 
             order_count_today: 0, 
             _pad: [0;24]
             }
    }
    pub fn market_maker(user_id : u64)->Self{
        Self {
            user_id , 
            available_balance: MARKET_MAKER_BALANCE, 
            reserved_balance: 0, 
            total_traded_today: 0, 
            order_count_today: 0, 
            _pad: [0;24]
            }
    }
}

#[repr(C)]
// dont allign to 64 , huge data wont fit into cache line anyhow 
#[derive(Debug , Clone, Copy)]
pub struct UserHoldings{
    pub user_id: u64,     // 8 byte 
    pub available_holdings : [u32 ; MAX_SYMBOLS], // symbol is a uique index in the array 
    pub reserved_holdings : [u32 ; MAX_SYMBOLS]
}
impl Default for UserHoldings{
    fn default() -> Self {
        UserHoldings {
            user_id: 0,
            available_holdings: [DEFAULT_HOLDING_QTY ; MAX_SYMBOLS],  // Faster than from_fn
            reserved_holdings : [0u32 ; MAX_SYMBOLS]
        }
    }

}

impl UserHoldings{
    pub fn new(user_id : u64)->Self{
        Self { 
            user_id, 
            available_holdings: [DEFAULT_HOLDING_QTY ; MAX_SYMBOLS],
            reserved_holdings: [0u32 ; MAX_SYMBOLS]
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
pub struct MyBalanceManager2{
    pub state : BalanceState,


    pub query_queue : QueryQueue,

    pub fill_recv_from_engine_try : Consumer<Fills>,
    pub order_recv_from_shm_try : Consumer<Order>,
    pub order_send_to_engine_try : Producer<Order>,
    pub events_to_wrriter_try : Producer<OrderEvents>,


    pub balance_updates_sender : Producer<BalanceResponse>,
    pub holding_update_sender : Producer<HoldingResponse>

}

impl MyBalanceManager2{
    pub fn new(fill_recv_from_engine_try : Consumer<Fills>,
        order_recv_from_shm_try : Consumer<Order>,
        order_send_to_engine_try : Producer<Order>,
        events_to_wrriter_try : Producer<OrderEvents>,
        balance_updates_sender : Producer<BalanceResponse>,
        holding_update_sender : Producer<HoldingResponse>
    )->Self{
        let query_queue = QueryQueue::open("/tmp/Queries");
        
        if query_queue.is_err(){
            eprintln!("query queue init error in balance manager");
            eprintln!("{:?}" , query_queue)
        }
        let balance_state = BalanceState::new();
        Self { state: balance_state,
         query_queue: query_queue.unwrap() , 
         fill_recv_from_engine_try ,
         order_recv_from_shm_try,
         order_send_to_engine_try,
         events_to_wrriter_try,
         balance_updates_sender,
         holding_update_sender
        }
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
    pub fn get_user_balance_copy_for_query(&mut self , user_index : u32)->UserBalance{
        self.state.balances[user_index as usize]
    }
    pub fn get_user_holdings_copy_for_query(&mut self , user_index : u32)->UserHoldings{
        self.state.holdings[user_index as usize]
    }
    //// returned the state so that it can be passed to the grpc server 
     #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub fn check_and_lock_funds(&mut self , order : Order)->Result<() , BalanceManagerError>{
        // currently for limit orders , we get an order 
        // we have user id , symbol , side , holfings 
        let user_index = self.get_user_index(order.user_id)?;   // fatal error , return immidieately to the function who is calling
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
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
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

    pub fn update_balance_after_order_cancel(&mut self , canceled_order : OrderToBeCanceled , side : Side , qty : u32 , price : u64 )->Result<() , BalanceManagerError>{
        let user_index = self.get_user_index(canceled_order.user_id)?;
        // understand what happens with thw ? mark here 
        match side {
            Side::Ask => {
                
                // side was ask , he was selling so this was only chnaged , order wsent fullfilled , no use of price 
                let old_reserved_holdings = self.state.holdings[user_index as usize].reserved_holdings[canceled_order.symbol as usize];
                let old_available_holdings = self.state.holdings[user_index as usize].available_holdings[canceled_order.symbol as usize];

                self.state.holdings[user_index as usize].reserved_holdings[canceled_order.symbol as usize] = old_reserved_holdings - qty;
                self.state.holdings[user_index as usize].available_holdings[canceled_order.symbol as usize] = old_available_holdings + qty;

            }
            Side::Bid =>{

                // he was buying , his ballcne wud have been reserved 
                let old_reserved_balance = self.get_user_balance(user_index).reserved_balance;
                let old_available_balance = self.get_user_balance(user_index).available_balance;
                self.state.balances[user_index as usize].available_balance = old_available_balance + price*qty as u64;
                self.state.balances[user_index as usize].reserved_balance = old_reserved_balance - price*qty as u64;
                
            }
        };

        Ok(())
        
    }

    pub fn add_user(&mut self , user_id : u64)->Result<u32 , BalanceManagerError>{
        if self.state.user_id_to_index.contains_key(&user_id){
            return Err(BalanceManagerError::UserAlreadyExists);
        }
        if self.state.next_free_slot as usize >= MAX_USERS {
            return Err(BalanceManagerError::MaxUsersReached);
        }
        let idx = self.state.next_free_slot;
        self.state.next_free_slot += 1;
        self.state.total_users += 1;

        self.state.balances[idx as usize] = UserBalance::new(user_id);
        self.state.holdings[idx as usize] = UserHoldings::new(user_id);
        
        self.state.user_id_to_index.insert(user_id, idx);

        Ok(idx)
    }

pub fn run_balance_manager(&mut self) {
    const BATCH_ORDERS: usize = 1000;

    eprintln!("[balance manager] Started on core (batched, prioritized fills)"); 

    let mut count: u64 = 0;
    let mut last_log = std::time::Instant::now();
    let mut _channel_recv_time = std::time::Duration::ZERO;
    let mut lock_funds_time = std::time::Duration::ZERO;
    let mut update_balance_time = std::time::Duration::ZERO;

    let  backoff = Backoff::new();

    loop {
        // 1) Drain all fills first (highest priority) — drain loop (fast)
        while let Some(recieved_fill) = self.fill_recv_from_engine_try.try_pop() {
            //println!("recvied fills");
            let update_start = std::time::Instant::now();
            let _ = self.update_balances_after_trade(recieved_fill);
            update_balance_time += update_start.elapsed();
            backoff.reset();
        }

        // 2) Process up to BATCH_ORDERS new orders (so fills remain highest priority, but orders are not starved)
        let mut processed = 0usize;
        while processed < BATCH_ORDERS {
            match self.order_recv_from_shm_try.try_pop() {
                Some(recieved_order) => {
                    
                    let lock_start = std::time::Instant::now();
                    match self.check_and_lock_funds(recieved_order) {
                        
                        Ok(()) => {
                            //println!("funds locked");
                            lock_funds_time += lock_start.elapsed();
                            // push not try push 
                            self.order_send_to_engine_try.push(recieved_order);
                            count += 1;
                        }
                        Err(_) => {
                            // insufficient funds — drop or log minimal
                            //eprintln!("[BM] Insufficient funds: {:?}", e);
                            let _ = self.events_to_wrriter_try.try_push(
                                OrderEvents { 
                                    user_id: recieved_order.user_id,
                                    order_id: recieved_order.order_id,
                                    symbol: recieved_order.symbol,
                                    event_kind: 3,
                                    filled_qty: 0,
                                    remaining_qty: recieved_order.shares_qty,
                                    original_qty: recieved_order.shares_qty,
                                    error_code: 1
                                 }
                            );
                        }
                    }
                    processed += 1;
                    backoff.reset();
                }
                None => break,
            }
        }
        match self.query_queue.dequeue(){
            Ok(Some(rec_query))=>{
                match rec_query.query_type{
                    2 =>{
                        // we get the query to add a user 
                    }
                    _ =>{

                    }
                }
            }
            Ok(None)=>{

            }
            Err(_)=>{

            }
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
        
       
        self.state.user_id_to_index.insert(20, 2);
        self.state.balances[2].user_id=20;
        self.state.balances[2].available_balance = HIGH_BALANCE;
        
       
        for symbol in 0..MAX_SYMBOLS {
            self.state.holdings[2].available_holdings[symbol] = HIGH_HOLDINGS;
        }
        
       
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
pub enum BalanceManagerResForLocking{
    BalanceManagerResUpdateDeltaBalance(BalanceManagerResUpdateDeltaBalance),
    BalanceManagerResUpdateDeltaHolding(BalanceManagerResUpdateDeltaHolding)
}
pub struct BalanceManagerResUpdateDeltaBalance{
    pub delta_available_balance : i64 ,
    pub delta_reserved_balance  : i64 ,
}

pub struct BalanceManagerResUpdateDeltaHolding{

    pub delta_available_holding  : i32 ,
    pub delta_reserved_holding   : i32
}


pub struct STbalanceManager{
    pub state : BalanceState,
    pub events_to_wrriter_try : Producer<OrderEvents> , 

    pub balance_updates_sender : Producer<BalanceResponse>,
    pub holding_update_sender : Producer<HoldingResponse>
}

impl STbalanceManager{
    pub fn new(
        events_to_wrriter_try : Producer<OrderEvents>,
        balance_updates_sender : Producer<BalanceResponse>,
        holding_update_sender : Producer<HoldingResponse>
    )->Self{
        let holding_response_queue = HoldingResQueue::open("/tmp/HoldingsResponse");
        let balance_response_queue = BalanceResQueue::open("/tmp/BalanceResponse");
        if balance_response_queue.is_err(){
            eprintln!("response queue init error in balance manager");
            eprintln!("{:?}" , balance_response_queue)
        }
        if holding_response_queue.is_err(){
            eprintln!("response queue init error in balance manager");
            eprintln!("{:?}" , holding_response_queue)
        }
        let balance_state = BalanceState::new();
        Self {  
            state: balance_state ,  
            events_to_wrriter_try,
            balance_updates_sender,
            holding_update_sender
        }
    }
    #[inline(always)]
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
    pub fn get_i64(& self ,ip : u64)->Result<i64, std::num::TryFromIntError>{
        i64::try_from(ip)
    }
    pub fn get_i32(& self ,ip : u32)->Result<i32, std::num::TryFromIntError>{
        i32::try_from(ip)
    }

    pub fn change_user_balance(&mut self , user_id : u64  , reserved_balance : u64 , available_balance : u64)->Result<() , BalanceManagerError>{
        let user_index = self.get_user_index(user_id)?;
        self.state.balances[user_index as usize].available_balance = available_balance;
        self.state.balances[user_index as usize].reserved_balance = reserved_balance;
        Ok(())
    }

    pub fn change_user_holdings(&mut self , user_id : u64 , symbol : u32 , reserved_shares_qty : u32 , available_qty : u32)->Result<() , BalanceManagerError>{
        let user_index = self.get_user_index(user_id)?;
        self.state.holdings[user_index as usize].available_holdings[symbol as usize] = available_qty;
        self.state.holdings[user_index as usize].reserved_holdings[symbol as usize] = reserved_shares_qty;
        Ok(())
    }


    #[inline(always)]
    pub fn check_and_lock_funds(&mut self , order : Order)->Result<BalanceManagerResForLocking , BalanceManagerError>{
        // currently for limit orders , we get an order 
        // we have user id , symbol , side , holfings 
        let user_index = self.get_user_index(order.user_id)?; 
        // this functio would return the UserNotFound error 
          // fatal error , return immidieately to the function who is calling
        
        match order.side {

            Side::Ask =>{
                //println!("sell order");
                let holdings = self.get_user_holdings(user_index);
                // wants to sell 
                let avalable_holdings_for_symbol = holdings.available_holdings[order.symbol as usize];
                let reserved_holdings_for_symbol = holdings.reserved_holdings[order.symbol as usize];

                //println!("available holdings are {:?}" , avalable_holdings_for_symbol);
                //println!("reserved holdings are {:?}" , reserved_holdings_for_symbol);
                

                if order.shares_qty > avalable_holdings_for_symbol{
                    return Err(BalanceManagerError::InsufficientFunds);
                }
                
                holdings.available_holdings[order.symbol as usize] = avalable_holdings_for_symbol - order.shares_qty;
                holdings.reserved_holdings[order.symbol as usize] = reserved_holdings_for_symbol + order.shares_qty;

                self.holding_update_sender.push(HoldingResponse { 
                    user_id: order.user_id, 
                    symbol: order.symbol, 
                    delta_available_holding: -self.get_i32(order.shares_qty).unwrap(), 
                    delta_reserved_holding: self.get_i32(order.shares_qty).unwrap()
                });
                

                return Ok(BalanceManagerResForLocking::BalanceManagerResUpdateDeltaHolding(BalanceManagerResUpdateDeltaHolding{
                    delta_available_holding : -self.get_i32(order.shares_qty).unwrap(),
                    delta_reserved_holding : self.get_i32(order.shares_qty).unwrap()
                }));
            }
            Side::Bid =>{
                //println!("buy order");
                // wants to buy  , if balacne > price * qty , we can rserve 
                // avalable is the free balance right now and reserved is what is alr reserved 
                let balance = self.get_user_balance(user_index);
                let required_balance = order.price*order.shares_qty as u64;
                let avalaible_balance = balance.available_balance;
                let reserved_balance = balance.reserved_balance;

                //println!("available balance is {:?}" ,avalaible_balance);
                //println!("reserved balance is {:?}" , reserved_balance);

                if required_balance > avalaible_balance {
                    return Err(BalanceManagerError::InsufficientFunds);
                }
                // we can reserv and and pass on the order to the matching egnine 
                balance.available_balance = avalaible_balance - required_balance;
                balance.reserved_balance = reserved_balance + required_balance;  


                self.balance_updates_sender.push(BalanceResponse { 
                    user_id: order.user_id, 
                    delta_available_balance: -self.get_i64(required_balance).unwrap(), 
                    delta_reserved_balance: self.get_i64(required_balance).unwrap()
                });

                return Ok(BalanceManagerResForLocking::BalanceManagerResUpdateDeltaBalance(BalanceManagerResUpdateDeltaBalance { 
                    delta_available_balance: -self.get_i64(required_balance).unwrap(), 
                    delta_reserved_balance: self.get_i64(required_balance).unwrap()
                }));
            }
            
        }
        
    }
    #[inline(always)]
    pub fn update_balances_after_trade<F , G>(&mut self, order_fills: Fills , mut emit : F , mut next_id : G)-> Result<(), BalanceManagerError>  where F : FnMut(BaseLogs) , G : FnMut()->u64 {
        //println!("updating the post trade funds for each fill , sending updates for go cache");
        for fill in order_fills.fills {
            
            let maker_index = self.get_user_index(fill.maker_user_id)?;
            let taker_index = self.get_user_index(fill.taker_user_id)?;
            let fill_value = fill.price * fill.quantity as u64;

            let (taker_balance_update ,  
                taker_holding_update  , 
                maker_balance_update ,
                maker_holding_update
            )  = match fill.taker_side {
                Side::Ask => {
                    // Taker is selling , jo order aya was sell order , order book pe(maker) buy order 
                    // add money , he sold 
                    // sell order tha , execute hogya 
                        
                        let  taker_balance = self.get_user_balance(taker_index);
                        let taker_avail_bal = taker_balance.available_balance;
                        taker_balance.available_balance = taker_avail_bal + fill_value;

                        // balacne updates for the go cache 
                        let taker_balance_update = BalanceResponse{
                            user_id : fill.taker_user_id,
                            delta_available_balance : self.get_i64(fill_value).unwrap(),
                            delta_reserved_balance : 0 
                        };

                        // logging 
                        emit(BaseLogs::BalanceDelta(BalanceDelta { 
                            event_id: next_id(), 
                            user_id: fill.taker_user_id, 
                            delta_available: self.get_i64(fill_value).unwrap(), 
                            delta_reserved: 0, 
                            order_id: fill.taker_order_id, 
                            reason: 1 
                        }));




                        // remove holdings from resevred
                       
                        let  taker_holdings = self.get_user_holdings(taker_index);
                        let taker_reserved_holdings = taker_holdings.reserved_holdings[fill.symbol as usize];
                        taker_holdings.reserved_holdings[fill.symbol as usize] = taker_reserved_holdings - fill.quantity;

                        // holding update for the go cache 
                        let taker_holding_update = HoldingResponse{
                            user_id : fill.taker_user_id ,
                            symbol : fill.symbol ,
                            delta_available_holding : 0 , 
                            delta_reserved_holding : -self.get_i32(fill.quantity).unwrap()
                        };

                        emit(BaseLogs::HoldingDelta(HoldingDelta { 
                            order_id: fill.taker_order_id, 
                            event_id: next_id(), 
                            user_id: fill.taker_user_id, 
                            symbol: fill.symbol, 
                            delta_available: 0, 
                            delta_reserved: -self.get_i32(fill.quantity).unwrap(), 
                            reason: 1 
                        }));
                        
                        let  maker_balance = self.get_user_balance(maker_index);
                        let maker_reserved_bal = maker_balance.reserved_balance;
                        maker_balance.reserved_balance= maker_reserved_bal - fill_value;

                        // balance update for the go cache 
                        let maker_balance_update = BalanceResponse{
                            user_id : fill.maker_user_id ,
                            delta_available_balance : 0 , 
                            delta_reserved_balance : -self.get_i64(fill_value).unwrap()
                        };
                        emit(BaseLogs::BalanceDelta(BalanceDelta { 
                            event_id: next_id(), 
                            user_id: fill.maker_user_id, 
                            delta_available: 0, 
                            delta_reserved: -self.get_i64(fill_value).unwrap(), 
                            order_id: fill.maker_order_id, 
                            reason: 1 
                        }));

                    

                        // add shares , he bough 
                        let  maker_holdings = self.get_user_holdings(maker_index);
                        let maker_avail_holdings = maker_holdings.available_holdings[fill.symbol as usize];
                        maker_holdings.available_holdings[fill.symbol as usize]= maker_avail_holdings + fill.quantity;

                        // holding update for the go cache 
                        let maker_holding_update = HoldingResponse{
                            user_id : fill.maker_user_id , 
                            symbol : fill.symbol ,
                            delta_available_holding : self.get_i32(fill.quantity).unwrap(),
                            delta_reserved_holding : 0 
                        };

                        emit(BaseLogs::HoldingDelta(HoldingDelta { 
                            order_id: fill.maker_order_id, 
                            event_id: next_id(), 
                            user_id: fill.maker_user_id, 
                            symbol: fill.symbol, 
                            delta_available: self.get_i32(fill.quantity).unwrap(), 
                            delta_reserved: 0, 
                            reason: 1 
                        }));
                    
                    (taker_balance_update , taker_holding_update , maker_balance_update , maker_holding_update)
                         
                }   
                
                Side::Bid => {
                    // Taker is buying , incoming is a buying order 

                       let  taker_balance = self.get_user_balance(taker_index);
                       let taker_reserved_bal = taker_balance.reserved_balance;
                       taker_balance.reserved_balance= taker_reserved_bal - fill_value;

                       let taker_balance_update = BalanceResponse{
                        user_id : fill.taker_user_id , 
                        delta_available_balance : 0 ,
                        delta_reserved_balance : -self.get_i64(fill_value).unwrap()
                       };
                       emit(BaseLogs::BalanceDelta(BalanceDelta { 
                        event_id: next_id(), 
                        user_id: fill.taker_user_id, 
                        delta_available: 0, 
                        delta_reserved: -self.get_i64(fill_value).unwrap(), 
                        order_id: fill.taker_order_id, 
                        reason: 1 
                    }));




                       
                       let  taker_holdings = self.get_user_holdings(taker_index);
                       let taker_avail_holdings = taker_holdings.available_holdings[fill.symbol as usize];
                       taker_holdings.available_holdings[fill.symbol as usize]= taker_avail_holdings + fill.quantity;

                       let taker_holding_update = HoldingResponse{
                            user_id : fill.taker_user_id , 
                            symbol : fill.symbol ,
                            delta_available_holding : self.get_i32(fill.quantity).unwrap(),
                            delta_reserved_holding : 0
                       };
                       emit(BaseLogs::HoldingDelta(HoldingDelta { 
                        order_id: fill.taker_order_id, 
                        event_id: next_id(), 
                        user_id: fill.taker_user_id, 
                        symbol: fill.symbol, 
                        delta_available: self.get_i32(fill.quantity).unwrap(), 
                        delta_reserved: 0, 
                        reason: 1 
                    }));



                       let  maker_balance = self.get_user_balance(maker_index);
                       let maker_avail_bal = maker_balance.available_balance;
                       maker_balance.available_balance= maker_avail_bal + fill_value;

                      let maker_balance_update = BalanceResponse{
                        user_id : fill.maker_user_id , 
                        delta_available_balance : self.get_i64(fill_value).unwrap(),
                        delta_reserved_balance : 0 
                      };

                      emit(BaseLogs::BalanceDelta(BalanceDelta { 
                        event_id: next_id(), 
                        user_id: fill.maker_user_id, 
                        delta_available: self.get_i64(fill_value).unwrap(), 
                        delta_reserved: 0, 
                        order_id: fill.maker_order_id, 
                        reason: 1 
                    }));
                    
            
                    
                       let  maker_holdings = self.get_user_holdings(maker_index);
                       let maker_reserved_holdings = maker_holdings.reserved_holdings[fill.symbol as usize];
                       maker_holdings.reserved_holdings[fill.symbol as usize]= maker_reserved_holdings - fill.quantity;


                       let maker_holding_update = HoldingResponse{
                        user_id : fill.maker_user_id , 
                        symbol : fill.symbol , 
                        delta_available_holding : 0 , 
                        delta_reserved_holding : -self.get_i32(fill.quantity).unwrap()
                       };
                       emit(BaseLogs::HoldingDelta(HoldingDelta { 
                        order_id: fill.maker_order_id, 
                        event_id: next_id(), 
                        user_id: fill.maker_user_id, 
                        symbol: fill.symbol, 
                        delta_available: 0, 
                        delta_reserved: -self.get_i32(fill.quantity).unwrap(), 
                        reason: 1 
                    }));
                       
                       (taker_balance_update , taker_holding_update , maker_balance_update , maker_holding_update)
                        
                }
            };

            // send udates to the writter for the maker and the taker indexes 
            let _ = self.balance_updates_sender.push(maker_balance_update);
            let _ = self.balance_updates_sender.push(taker_balance_update);
            let _ = self.holding_update_sender.push(maker_holding_update);
            let _ = self.holding_update_sender.push(taker_holding_update);
            
        }
        Ok(())
        
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
        
        let user10_bal = self.state.balances[1].available_balance;
        let user20_bal = self.state.balances[2].available_balance;
        let user20_holdings = self.state.holdings[2].available_holdings[0];
        
        eprintln!("[BM] User 10 balance: {}", user10_bal);
        eprintln!("[BM] User 20 balance: {}", user20_bal);
        eprintln!("[BM] User 20 holdings[0]: {}", user20_holdings);
        eprintln!("[BM] User map contains 10: {}", self.state.user_id_to_index.contains_key(&10));
        eprintln!("[BM] User map contains 20: {}", self.state.user_id_to_index.contains_key(&20));
    }
    pub fn update_balance_after_order_cancel(&mut self , canceled_order : OrderToBeCanceled , side : Side , qty : u32 , price : u64 )->Result<() , BalanceManagerError>{
        let user_index = self.get_user_index(canceled_order.user_id)?;
        // understand what happens with thw ? mark here 
        match side {
            Side::Ask => {
                
                // side was ask , he was selling so this was only chnaged , order wsent fullfilled , no use of price 
                let old_reserved_holdings = self.state.holdings[user_index as usize].reserved_holdings[canceled_order.symbol as usize];
                let old_available_holdings = self.state.holdings[user_index as usize].available_holdings[canceled_order.symbol as usize];

                self.state.holdings[user_index as usize].reserved_holdings[canceled_order.symbol as usize] = old_reserved_holdings - qty;
                self.state.holdings[user_index as usize].available_holdings[canceled_order.symbol as usize] = old_available_holdings + qty;

            }
            Side::Bid =>{

                // he was buying , his ballcne wud have been reserved 
                let old_reserved_balance = self.get_user_balance(user_index).reserved_balance;
                let old_available_balance = self.get_user_balance(user_index).available_balance;

                self.state.balances[user_index as usize].available_balance = old_available_balance + price*qty as u64;
                self.state.balances[user_index as usize].reserved_balance = old_reserved_balance - price*qty as u64;
                
            }
        };

        Ok(())
        
    }

    pub fn add_user(&mut self , user_id : u64)->Result<u32 , BalanceManagerError>{
        if self.state.user_id_to_index.contains_key(&user_id){
            return Err(BalanceManagerError::UserAlreadyExists);
        }
        if self.state.next_free_slot as usize >= MAX_USERS {
            return Err(BalanceManagerError::MaxUsersReached);
        }
        let idx = self.state.next_free_slot;
        self.state.next_free_slot += 1;
        self.state.total_users += 1;

        self.state.balances[idx as usize] = UserBalance::new(user_id);
        self.state.holdings[idx as usize] = UserHoldings::new(user_id);
        
        self.state.user_id_to_index.insert(user_id, idx);
        Ok(idx)
    }

    pub fn add_market_maker(&mut self)->Result<u32 , BalanceManagerError>{
        if self.state.user_id_to_index.contains_key(&0){
            return Err(BalanceManagerError::UserAlreadyExists);
        }
        if self.state.next_free_slot as usize >= MAX_USERS {
            return Err(BalanceManagerError::MaxUsersReached);
        }
        let idx = self.state.next_free_slot;
        self.state.next_free_slot += 1;
        self.state.total_users += 1;

        self.state.balances[idx as usize] = UserBalance::market_maker(0);
        self.state.holdings[idx as usize] = UserHoldings::new(0);
        
        self.state.user_id_to_index.insert(0, idx);
        Ok(idx)
    }

}