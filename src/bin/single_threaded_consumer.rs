use rust_orderbook_2::{
    balance_manager::my_balance_manager2::STbalanceManager, engine::my_engine::{Engine, STEngine}, orderbook::{order::Order, types::Event}, shm::reader::StShmReader
};
use std::time::Instant;
use rust_orderbook_2::shm::queue::IncomingOrderQueue;
use bounded_spsc_queue::Producer;
use rust_orderbook_2::shm::event_queue::OrderEvents;
pub struct TradingCore {
    pub balance_manager: STbalanceManager,
    pub shm_reader: StShmReader,
    pub engine: STEngine,
    processed_count: u64,
    last_log: Instant,
    order_batch : Vec<Order>
}

impl TradingCore {
    pub fn new(event_sender_to_writter : Producer<OrderEvents> , event_sender_to_publisher_by_engine : Producer<Event>) -> Self {
        Self {
            balance_manager: STbalanceManager::new(event_sender_to_writter),
            shm_reader: StShmReader::new().unwrap(),
            engine: STEngine::new(0 , event_sender_to_publisher_by_engine),
            processed_count: 0,
            last_log: Instant::now(),
            order_batch : Vec::with_capacity(1000)
        }
    }

    pub fn run(&mut self) {
        eprintln!("[Trading Core] Starting single-threaded mode");
        loop {
            self.order_batch.clear();
            for _ in 0 ..1000{
                if let Some(order) = self.shm_reader.receive_order() {
                    self.order_batch.push(order);       
                }
                else {
                    break;
                }
            }
            for order in self.order_batch.drain(..){
                match self.balance_manager.check_and_lock_funds(order) {
                    Ok(_) => {
                        // Process order in engine
                        let engine_res = self.engine.process_order(order) ; 
                        
                        match engine_res.0 {
                            Some(match_result) => {
                                // Update balances from fills
                                
                                if let Err(e) = self.balance_manager
                                    .update_balances_after_trade(match_result.fills)

                                {
                                    
                                    eprintln!("[Trading Core] Balance update error: {:?}", e);
                                }
                                
                            }
                            None => {
                                eprintln!("[Trading Core] Failed to process order");
                            }
                        }

                        match engine_res.1 {
                            Some(market_update)=>{
                               let _ = self.engine.sending_event_to_publisher_try.try_push(Event::new(market_update));
                            }
                            None=>{
                                // no amrket update , dosent matter 
                            }
                        }
                        
                        self.processed_count += 1;
                    }
                    Err(_) => {
                        // Order rejected (insufficient funds, etc)
                        let _ = self.balance_manager.events_to_wrriter_try.try_push(
                            OrderEvents { 
                                user_id: order.user_id,
                                order_id: order.order_id,
                                symbol: order.symbol,
                                event_kind: 3,
                                filled_qty: 0,
                                remaining_qty: order.shares_qty,
                                original_qty: order.shares_qty,
                                error_code: 1
                            }
                        );
                    }
                }
            }
          
            if self.last_log.elapsed().as_secs() >= 2 {
                let elapsed = self.last_log.elapsed();
                let rate = self.processed_count as f64 / elapsed.as_secs_f64();
                
                eprintln!(
                    "[Trading Core] {:.2}M orders/sec ({} orders in {:.2}s)",
                    rate / 1_000_000.0,
                    self.processed_count,
                    elapsed.as_secs_f64()
                );
                
                self.processed_count = 0;
                self.last_log = Instant::now();
            }

        
        }
    }
}


#[hotpath::main]
fn main() {
    let (order_event_producer_bm , _) = bounded_spsc_queue::make::<OrderEvents>(32678);
    let (event_producer_engine , _) = bounded_spsc_queue::make::<Event>(32678);
    let _ = IncomingOrderQueue::create("/tmp/IncomingOrders");
    let mut trading_system = TradingCore::new(order_event_producer_bm , event_producer_engine);
    trading_system.balance_manager.add_throughput_test_users();
    trading_system.engine.add_book(0);
    
    eprintln!("[Main] Initialization complete, starting trading loop");
    trading_system.run();
}



