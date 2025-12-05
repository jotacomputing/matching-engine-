use crate::orderbook::{order::Order};
use crate::orderbook::order_manager::OrderManager;
#[derive(Debug)]

// a particular price level has a linkedList of orderindexes cuurenlty storing the head and tail 
pub struct PriceLevel{
    pub price : u64 , 
    pub total_vol : u32 , 
    pub head : Option<usize>,
    pub tail : Option<usize>
}

// let head is front and tail is back , oldest at head highest priority , we will insert at tail
impl PriceLevel{
    pub fn new(price : u64)->Self{
        Self{
            price ,
            total_vol: 0 , 
            head : None , 
            tail : None , 
        }
    }
    // when a limitn order cannot be fuldilled it will be required to be inserted in the order book

    pub fn add_order(&mut self ,  manager :&mut OrderManager ,   order:Order){
        let order_index = manager.insert_order(order);
        // if this is the first order , this wud be the head and this wud be the tail
        match self.tail{
            None => {
                self.head = Some(order_index);
                self.tail = Some(order_index);
            }

            Some(tail_index) => { 
               if let Some(tail_order) = manager.get_mut(tail_index) {
                    tail_order.next = Some(order_index) ; 
               }

               if let Some(curr_order) = manager.get_mut(order_index){
                    curr_order.prev = Some(tail_index);
               }
               
              //let curr_order = manager.all_orders.get_mut(order_key).unwrap();
              // better way of doing this using if let some 
                 self.tail = Some(order_index);
               
            }
        }
        if let Some(curr_order) = manager.get(order_index) {
            self.total_vol += curr_order.shares_qty;
        }

    }

    pub fn delete_order(&mut self , order_id : u64 , manager : &mut OrderManager){
        // we get an order to remove 
        // that order will arl be there in the map 
        // it wud be there on the book 
        if let Some(&order_index) = manager.id_to_index.get(&order_id){
            if self.head == self.tail {
            // took a mutable refeence in a scope and extracted the value we needed 
                let shares = {
                    let order_to_del = manager.get_mut(order_index).unwrap();
                    order_to_del.shares_qty
                };
                self.head = None;
                self.tail = None;
                self.total_vol = self.total_vol.saturating_sub(shares);
            }
            // if the order to be deleted is the head of the list 
            else if self.head == Some(order_index){
                // extracted what all we needed 
                let (shares , next_order_key ) = {
                    let order_to_del = manager.get_mut(order_index).unwrap();
                    (order_to_del.shares_qty , order_to_del.next )
                };
                self.head = next_order_key;
                if let Some(new_head_key) = self.head{
                    let new_head = manager.get_mut(new_head_key).unwrap();
                    new_head.prev = None;
                }
                self.total_vol = self.total_vol.saturating_sub(shares);
            }
            // if the order to be deleted is the tail of the list 
            else if self.tail == Some(order_index){
                let (shares  , prev_order_key) = {
                    let order_to_del = manager.get_mut(order_index).unwrap();
                    (order_to_del.shares_qty  , order_to_del.prev)
                };
                self.tail = prev_order_key; 
                if let Some(new_tail_key) = self.tail{
                    let new_tail = manager.get_mut(new_tail_key).unwrap();
                    new_tail.next = None;
                }
                self.total_vol = self.total_vol.saturating_sub(shares);
            }
            else{
                // in the middle 
                //{let prev_order_key = order_to_delete.prev.unwrap();} // we are still using order to del here , a mutable refrence to the map 
                //let next_order_key = order_to_delete.next.unwrap();
                let (prev_order_key , next_order_key , shares)={
                    let order_to_del = manager.get_mut(order_index).unwrap();
                    (order_to_del.prev , order_to_del.next , order_to_del.shares_qty)
                };

                if prev_order_key.is_some() && next_order_key.is_some(){
                    {
                        let prev_order = manager.get_mut(prev_order_key.unwrap()).unwrap();  // we cant call this here 
                        prev_order.next = next_order_key;
                    }

                    {
                        let next_order = manager.get_mut(next_order_key.unwrap()).unwrap();
                        next_order.prev = prev_order_key ; 
                    }
                }
            // remoe the shares queantity 
                // this line wont work because we have alr a ref to order.del
                //self.total_vol = self.total_vol.saturating_sub(order_to_delete.shares_qty);
                self.total_vol = self.total_vol.saturating_sub(shares);
        }
        
            manager.remove_order(order_id);

        }
            // there is only one order in the list 
        
       // if self.head.is_none() && self.tail.is_none(){
       //     self.empty = true;
       // }

    }

    pub fn get_total_volume(&self )->u32{
        self.total_vol
    }
    pub fn remove_oldest_order(&mut self , manager : &mut OrderManager)->Option<usize>{
        // we need to return the order index of the order present at head 
        // we didnot delete it yet from the maps 
        match self.head{
            None => None  ,
            Some(head_index )=>{
                let (shares , next_order_key , ) = {
                    let head_order = manager.get_mut(head_index).unwrap();
                    (head_order.shares_qty , head_order.next)
                };
                self.head = next_order_key;
                if let Some(new_head_index) = next_order_key{
                    let new_head_order= manager.get_mut(new_head_index).unwrap();
                    new_head_order.prev = None;
                }
                if self.tail == Some(head_index) {
                    self.tail = None;
                }

                self.total_vol = self.total_vol.saturating_sub(shares);
                // Clean up the popped order's links
                if let Some(order) = manager.get_mut(head_index) {
                    order.prev = None;
                    order.next = None;
                }
                Some(head_index)
            }
        }
        
    }

    pub fn check_if_empty(&self)->bool{
        if self.head.is_none() && self.tail.is_none(){
            return true;
        }
        false
    }
    // This function reinserts at the head if an order was removed and only some shares of that order weere matched
    pub fn insert_at_head(&mut self , order_index : usize , manager : &mut OrderManager){
        // it maybe the only order which was popped and needs to be reiserted 
        // the order wasent removed from the maps so we cant the orderkey from the Id ;
        let shares = {
            let order = manager.get_mut(order_index).unwrap();
            order.shares_qty
        };
        match self.head{
            // if self .head is empty that means itwas the last order at that price 
            None => {
                self.head = Some(order_index) ;
                self.tail = Some(order_index) ; 
            }

            Some(old_head_key)=>{
                // if there is a head order 
                if let Some(order) = manager.get_mut(order_index) {
                    order.prev = None;
                    order.next = Some(old_head_key);
                }
                if let Some(old_head) = manager.get_mut(old_head_key) {
                    old_head.prev = Some(order_index);
                }
                self.head = Some(order_index);
            }
        }

        self.total_vol += shares;
    }


    
}



