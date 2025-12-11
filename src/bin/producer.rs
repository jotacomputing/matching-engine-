use std::{
    sync::atomic::{AtomicI64, Ordering},
    thread,
    time::{Duration, Instant},
};

use rust_orderbook_2::shm::queue::Queue;
use rust_orderbook_2::orderbook::order::ShmOrder;

fn main() {
    // ==== Open queue ====
    let mut q = Queue::open("/tmp/sex").expect("Failed to open queue");

    println!("[OMS] Rust Producer - THROUGHPUT TEST MODE");
    println!("[OMS] Using concentrated price levels with alternating users");

    static ATOMIC_COUNT: AtomicI64 = AtomicI64::new(0);

    // ==== Metrics Thread (2 sec interval) ====
    {
        let q_count_ref = &ATOMIC_COUNT;

        thread::spawn(move || {
            let mut last_count = 0_i64;
            let mut last_time = Instant::now();

            loop {
                thread::sleep(Duration::from_secs(2));

                let now = Instant::now();
                let elapsed = now.duration_since(last_time).as_secs_f64();
                let current = q_count_ref.load(Ordering::Relaxed);

                let ops = (current - last_count) as f64;
                let throughput = ops / elapsed;

                println!(
                    "[OMS] {:.0} orders/sec ({:.2} million/sec)",
                    throughput,
                    throughput / 1_000_000.0
                );

                last_count = current;
                last_time = now;
            }
        });
    }

    // ==== Pre-allocated Order (Pool-like reuse) ====
    let mut order = ShmOrder {
        user_id: 10,  // Will alternate between 10 and 20
        order_id: 0,
        shares_qty: 1,
        symbol: 0,
        status: 0,
        side: 0,   
        order_type : 1,   // 0 = buy, 1 = sell
        price: 10,
        timestamp: current_time_ns(),
       
    };

    // Price levels around 50000
    let prices = [9_u64, 10, 11];

    // ==== Producer Loop ====
    let mut count = 0u64;
    let base_timestamp = current_time_ns();

    loop {
        count += 1;

        order.order_id = count;
        
        // Alternate between users and sides
        // Even count: User 10 buys (side=0)
        // Odd count:  User 20 sells (side=1)
        if count % 2 == 0 {
            order.user_id = 10;
            order.side = 0; // Buy
        } else {
            order.user_id = 20;
            order.side = 1; // Sell
        }
        
        // Cycle through 3 price levels
        order.price = prices[((count / 2) % 3) as usize];
        order.timestamp = base_timestamp;

        let mut attempts = 0;

        // Retry logic for full queue
        loop {
            match q.enqueue(order) {
                Ok(_) => {
                    ATOMIC_COUNT.fetch_add(1, Ordering::Relaxed);
                    break;
                }
                Err(_) => {
                    attempts += 1;
                    if attempts > 3 {
                        thread::yield_now();
                        attempts = 0;
                    }
                }
            }
        }
    }
}

// === Helper: faster timestamp ===
fn current_time_ns() -> u64 {
    use std::time::SystemTime;
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}
