use rust_orderbook_2::orderbook::order_manager::OrderManager;
use tokio::macros;

#[tokio::main]
async fn main(){
    let manager = OrderManager::new();
}


pub fn start_engine(){
    // this shud be done in the main.rs file 
    // to start the engine 
    // we need to spawn a reciver thread with the function , it will take an immutable refrence to the senders map
    // spawn a publisher thread and pass the complete owsner ship of the reciver of the event 
    // iterate thrpugh all the maps and call the add book function 

   // let  publisher = EventPublisher::new(rx);
    
}