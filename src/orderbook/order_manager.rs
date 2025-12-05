use crate::orderbook::order::Order;
use rustc_hash::FxHashMap;

#[derive(Debug)]
pub struct OrderManager{
    // all orders for an orderBook are owned by the orderManager it self , we will copy the indexes for storing in the price level 
    pub all_orders : Vec<Option<Order>>,
    pub id_to_index : FxHashMap<u64  , usize>,
    pub free_list : Vec<usize>
}

impl OrderManager{
    pub fn new()->Self{
        Self{
            all_orders : Vec::with_capacity(1_000_000),
            id_to_index: FxHashMap::with_capacity_and_hasher(
                1_000_000,
                Default::default()
            ),
            free_list : Vec::new(),
        }
    }
    pub fn insert_order(&mut self , order : Order)->usize{
        // it will insert into these structures , first itl will check the free list 
        let order_id = order.order_id;
        let index = if let Some(free_slot) = self.free_list.pop(){
            // we got a free slot , use it 
            self.all_orders[free_slot] = Some(order);
            free_slot
        }else{
            // we dont have a free slot , direcly push 
            self.all_orders.push(Some(order));
            self.all_orders.len() - 1
        };
        self.id_to_index.insert(order_id, index);
        index
    }

    pub fn remove_order(&mut self, order_id: u64){
        if let Some(&order_index) = self.id_to_index.get(&order_id) {
            // defrences too 
            self.all_orders[order_index] = None;   
            self.free_list.push(order_index);      
            self.id_to_index.remove(&order_id);     
        }
    }

    pub fn get(&self, index: usize) -> Option<&Order> {
        self.all_orders.get(index)?.as_ref()
    }

    // Get mutable reference
    pub fn get_mut(&mut self, index: usize) -> Option<&mut Order> {
        self.all_orders.get_mut(index)?.as_mut()
    }

}
 
