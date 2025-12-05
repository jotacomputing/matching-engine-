// jobs 
// will mantain userbalances acc to nuser ID 
// will expose functions such as check and lock funds for an order 
// expose functions to update balances after a trade or a fill
use crate::orderbook::types::BalanceManagerError;
use crate::orderbook::order::Order;
pub trait BalanceManager{
    fn check_and_lock_funds(user_id : usize , order : Order)->Result<() , BalanceManagerError>;
    fn update_funds()->Result<() , BalanceManagerError>;
}