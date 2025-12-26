use std::{
    env,
    sync::atomic::{AtomicI64, Ordering},
    thread,
    time::{Duration, Instant},
};

use rust_orderbook_2::orderbook::order::ShmOrder;
use rust_orderbook_2::shm::queue::IncomingOrderQueue;

// === Config ===
// Default target rate if env var not set.
const DEFAULT_MAX_RATE_PER_SEC: u64 = 10000;

fn main() {
    // ==== Read max rate from env or use default ====
    let max_rate_per_sec: u64 = env::var("MAX_RATE_PER_SEC")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_MAX_RATE_PER_SEC);

    println!(
        "[OMS] Rust Producer - THROUGHPUT TEST MODE (target {:.2}M orders/sec)",
        max_rate_per_sec as f64 / 1_000_000.0
    );
    println!("[OMS] Using concentrated price levels with alternating users");

    // ==== Open queue ====
    let mut q =
        IncomingOrderQueue::open("/tmp/IncomingOrders").expect("Failed to open queue");

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

    // ==== Pre-allocated Order ====
    let mut order = ShmOrder {
        user_id: 10, // Will alternate between 10 and 20
        order_id: 0,
        shares_qty: 1,
        symbol: 0,
        status: 0,
        side: 0,
        order_type: 1, // 0 = buy, 1 = sell
        price: 10,
        timestamp: current_time_ns(),
    };

    let prices = [9_u64, 10, 11];

    // ==== Producer Loop with rate limiting ====
    let mut count: u64 = 0;
    let base_timestamp = current_time_ns();

    let mut window_start = Instant::now();
    let mut sent_in_window: u64 = 0;

    loop {
        // Simple 1-second sliding window rate limiter
        let elapsed = window_start.elapsed().as_secs_f64();
        if elapsed >= 1.0 {
            // Reset window
            window_start = Instant::now();
            sent_in_window = 0;
        } else if sent_in_window >= max_rate_per_sec {
            // Already sent target for this second; small sleep to avoid busy spin
            let sleep_us = 50;
            thread::sleep(Duration::from_micros(sleep_us));
            continue;
        }

        count += 1;
        order.order_id = count;

        // Alternate users and sides
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
                    sent_in_window += 1;
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
