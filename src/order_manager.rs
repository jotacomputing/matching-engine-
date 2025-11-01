use std::collections::HashMap;
use slotmap::{SlotMap, new_key_type};
use crate::order::Order;

new_key_type! { pub  struct  OrderKey; }

pub struct OrderManager{
    pub all_orders : SlotMap<OrderKey , Order>,
    pub id_to_key : HashMap<u64  , OrderKey>
}

impl OrderManager{
    pub fn new()->Self{
        Self{
            all_orders : SlotMap::with_key(),
            id_to_key : HashMap::new()
        }
    }

    
}

