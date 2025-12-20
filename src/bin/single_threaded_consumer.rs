// src/bin/single_threaded.rs
use rust_orderbook_2::{
    balance_manager::my_balance_manager2::STbalanceManager, engine::my_engine::{Engine, STEngine}, orderbook::{order::Order}, shm::reader::StShmReader
};
use std::time::Instant;
use rust_orderbook_2::shm::queue::Queue;
pub struct TradingCore {
    pub balance_manager: STbalanceManager,
    pub shm_reader: StShmReader,
    pub engine: STEngine,
    processed_count: u64,
    last_log: Instant,
    order_batch : Vec<Order>
}

impl TradingCore {
    pub fn new() -> Self {
        Self {
            balance_manager: STbalanceManager::new(),
            shm_reader: StShmReader::new().unwrap(),
            engine: STEngine::new(0),
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
                        match self.engine.process_order(order) {
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
                        
                        self.processed_count += 1;
                    }
                    Err(e) => {
                        // Order rejected (insufficient funds, etc)
                        // This is normal, don't spam logs
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
    let _ = Queue::create("/tmp/sex");
    let mut trading_system = TradingCore::new();
    trading_system.balance_manager.add_throughput_test_users();
    trading_system.engine.add_book(0);
    
    eprintln!("[Main] Initialization complete, starting trading loop");
    trading_system.run();
}



