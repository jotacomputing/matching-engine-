use crossbeam::{channel::Receiver, queue::ArrayQueue};
use crate::orderbook::types::Event;
use std::sync::Arc;
pub struct EventPublisher {
    pub receiver: Receiver<Event>,   
    pub event_queue : Arc<ArrayQueue<Event>>
}
impl EventPublisher {
    pub fn new(rx: Receiver<Event> , event_queue : Arc<ArrayQueue<Event>>) -> Self {
        Self { receiver: rx , event_queue }
    }
    pub fn start_publisher(&mut self) {
        let mut batch = Vec::with_capacity(10_000);
        let mut count = 0u64;
        let mut total_batches = 0u64;
        let mut last_log = std::time::Instant::now();
        
        println!("[PUBLISHER] Started (crossbeam batched mode) on core 5");
        loop {
            batch.clear();
            
            // Blocking recv (wait for first event)
            match self.event_queue.pop() {

                Some(event) => {
                    //println!("publisher receieved fills");
                    batch.push(event);
                    count += 1;
                }
                None =>{
                    break;
                }
            }
            
            //  Non-blocking drain (get up to 9,999 more)
            for event in self.receiver.try_iter().take(9_999) {
                batch.push(event);
                count += 1;
            }
            
            total_batches += 1;
            
            // Step 3: Process batch (currently just drops)
            // TODO: When publishing to Kafka:
            // publish_batch_to_kafka(&batch);
            
            // Step 4: Stats every 5 seconds
            if last_log.elapsed().as_secs() >= 5 {
                let rate = count as f64 / last_log.elapsed().as_secs_f64();
                let avg_batch_size = if total_batches > 0 {
                    count as f64 / total_batches as f64
                } else {
                    0.0
                };
                
                eprintln!("[PUBLISHER] {:.2}M events/sec, avg batch: {:.0}",
                    rate / 1_000_000.0, avg_batch_size);
                
                count = 0;
                total_batches = 0;
                last_log = std::time::Instant::now();
            }
        }
    }
}


