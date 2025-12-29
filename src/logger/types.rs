

// order recived 
// order rejcted 
// order matched 
//  balance locked 
// balance updated 
// source 0->shm reader , 1 -> balance mangaer  , 2 -> matching engine
// severity 0-> info (from components) 1-> error , 2 -> Debug 
#[repr(C)]
#[derive( Debug, Clone, Copy )]
pub struct OrderLogWrapper{
    pub order_delta            : OrderDelta,
    pub timestamp              : i64 , 
    pub severity               : u8 , 
    pub source                 : u8 , 
} // 52 bytes  


#[repr(C)]
#[derive( Debug, Clone, Copy)]
pub struct BalanceLogWrapper{
    pub balance_delta          : BalanceDelta,
    pub timestamp              : i64 , 
    pub severity               : u8 , 
    pub source                 : u8 , 
}
// 67 bytes 

#[repr(C)]
#[derive( Debug, Clone, Copy )]
pub struct HoldingsLogs{
    pub holding_delta           : HoldingDelta,
    pub timestamp               : i64 ,
    pub severity                : u8 , 
    pub source                  : u8 , 
}
// 55 bytes 

#[derive( Debug, Clone , serde::Serialize)]
pub struct OrderBookSnapShot{
    pub event_id               : u64 ,
    pub symbol                 : u32 , 
    pub bids                   : Vec<[String ; 3]>,
    pub asks                   : Vec<[String ; 3]> ,
    pub timestamp              : i64 ,
    pub severity               : u8 , 
    pub source                 : u8 , 
}




#[derive( Debug, Clone, Copy)]
pub struct BalanceDelta{
    pub event_id: u64,
    pub user_id: u64,
    pub delta_available: i64,
    pub delta_reserved: i64,
    pub reason: u8,      // reso for the balance update , either balances locked = 0 , or funds updated =1
    pub order_id: u64,     // the taker order id which caused the balance updations 
}

#[derive(Debug ,Copy, Clone)]
pub struct HoldingDelta {
    pub event_id: u64,
    pub user_id: u64,
    pub symbol: u32,
    pub delta_available: i32,
    pub delta_reserved: i32,
    pub reason: u8,
    pub order_id: u64,
}



#[derive( Debug, Clone, Copy )]
pub struct OrderDelta{
    pub event_id               : u64 ,
    pub order_id               : u64 ,
    pub user_id                : u64 ,
    pub price                  : u64 , 
    pub symbol                 : u32 ,
    pub shares_qty             : u32 ,
    pub side                   : u8 ,
    pub order_event_type       : u8 ,   // 0 order recived at SHM reader , 1->order matched , 2 -> order canceled log 
}


pub enum BaseLogs{
    BalanceDelta(BalanceDelta) ,
    HoldingDelta(HoldingDelta) ,
    OrderDelta(OrderDelta)
}