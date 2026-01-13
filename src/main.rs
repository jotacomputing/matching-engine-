use std::thread::JoinHandle;
use rust_orderbook_2::balance_manager::my_balance_manager2::{ MyBalanceManager2};
use rust_orderbook_2::logger::types::TradeLogs;
use rust_orderbook_2::orderbook::order::Order;
use rust_orderbook_2::orderbook::types::Fills;
use rust_orderbook_2::orderbook::{types::Event};
use rust_orderbook_2::engine::my_engine::{ Engine};
use rust_orderbook_2::publisher::event_publisher::EventPublisher;
use core_affinity;
use rust_orderbook_2::pubsub::pubsub_manager::RedisPubSubManager;
use rust_orderbook_2::shm::fill_queue_mm::MarketMakerFill;
use rust_orderbook_2::shm::holdings_response_queue::{HoldingResQueue, HoldingResponse};
use rust_orderbook_2::shm::market_maker_feed::MarketMakerFeed;
use rust_orderbook_2::shm::queue::IncomingOrderQueue;
use rust_orderbook_2::shm::cancel_orders_queue::CancelOrderQueue;
use rust_orderbook_2::shm::event_queue::{OrderEventQueue, OrderEvents};
use rust_orderbook_2::shm::query_queue::QueryQueue;
use rust_orderbook_2::shm::balance_response_queue::{BalanceResQueue, BalanceResponse};
use rust_orderbook_2::shm::reader::ShmReader;
use rust_orderbook_2::shm::writer::ShmWriter;
use bounded_spsc_queue;

#[hotpath::main]
fn main(){
    // initilaise all queues once , mmap with the virtual adddress space of this process 
    // threads can indivisually open the queues (SPSC)
    let _ = IncomingOrderQueue::create("/tmp/IncomingOrders").expect("failed to create queue");
    let _ = CancelOrderQueue::create("/tmp/CancelOrders").expect("failed to create queue");
    let _ = OrderEventQueue::create("/tmp/OrderEvents").expect("failed to create queue");
    let _ = QueryQueue::create("/tmp/Queries").expect("failed to create queue");
    let _ = HoldingResQueue::create("/tmp/HoldingsResponse").expect("failed to create queue");
    let _ = BalanceResQueue::create("/tmp/BalanceResponse").expect("failed to open queue");

    
    let (fill_producer_engine , fill_consumer_bm ) = bounded_spsc_queue::make::<Fills>(32768);
    let (event_producer_engine , event_consumer_publisher) = bounded_spsc_queue::make::<Event>(32768);
    let (order_producer_bm , order_consumer_engine) = bounded_spsc_queue::make::<Order>(32768);
    let (order_producer_shm_reader , order_consumer_bm) = bounded_spsc_queue::make::<Order>(32768);
    let (order_event_producer_bm , order_event_consumer_writter_from_bm) = bounded_spsc_queue::make::<OrderEvents>(32768);
    let (order_event_producer_publisher , order_event_consumer_writter_from_publisher) = bounded_spsc_queue::make::<OrderEvents>(32768);
    let (order_event_producer_engine , order_event_consumer_writter_from_engine) = bounded_spsc_queue::make::<OrderEvents>(32768);
    let (balance_event_producer_bm , balance_event_consumer_writter) = bounded_spsc_queue::make::<BalanceResponse>(32768);
    let (holding_event_producer_bm , holding_event_consumer_writter) = bounded_spsc_queue::make::<HoldingResponse>(32768);
    let (trade_log_producer_publisher , _)= bounded_spsc_queue::make::<TradeLogs>(32768);
    let (mm_fill_sender , mm_fill_reciever)= bounded_spsc_queue::make::<MarketMakerFill>(32768);
    let (_ , mm_feed_receiver) = bounded_spsc_queue::make::<MarketMakerFeed>(32768);


    let shm_reader_handle = std::thread::spawn(move || {
        core_affinity::set_for_current(core_affinity::CoreId { id: 2 });

        let mut my_shm_reader = ShmReader::new(      
            order_producer_shm_reader
        ).unwrap();

        my_shm_reader.run_reader();
    });

    // BALANCE MANAGER HANDLE 

    let balance_manager_handle = std::thread::spawn(move || {
        core_affinity::set_for_current(core_affinity::CoreId { id: 6 });

        let mut my_balance_manager = MyBalanceManager2::new(
            fill_consumer_bm , 
            order_consumer_bm,
            order_producer_bm,
            order_event_producer_bm,
            balance_event_producer_bm,
            holding_event_producer_bm
        );

        my_balance_manager.add_throughput_test_users();
        my_balance_manager.run_balance_manager();
    });

    // ENGINE HANDLES 
    let mut running_engines: Vec<JoinHandle<()>> = Vec::new();

    //let first_join_handle = std::thread::spawn(move || {
    //    core_affinity::set_for_current(core_affinity::CoreId { id: 1 });
//
    //    let mut engine = MyEngine::new(
    //        0,
    //        order_consumer_engine,
    //        fill_producer_engine,
    //        event_producer_engine,
    //        order_event_producer_engine
    //    ).unwrap();
    //    engine.add_book(0);
    //    engine.run_engine(|feed|{
    //        println!("demo");
    //    });
    //});
    //running_engines.push(first_join_handle);



    let pubsub_connection = RedisPubSubManager::new("redis://localhost:6379");
    if pubsub_connection.is_err(){
        panic!("pubsub error , not initialising publisher");
    }
    //PUBLISHER REQUIRES AN EVENT RECV ONLY 
    let publisher_handle = std::thread::spawn(move || {
        core_affinity::set_for_current(core_affinity::CoreId { id: 5 });

        let mut my_publisher = EventPublisher::new(
            
            pubsub_connection.unwrap() , 
            
            event_consumer_publisher,
            order_event_producer_publisher , trade_log_producer_publisher,mm_fill_sender
        );

        my_publisher.start_publisher();
    });
    // SHM WRITTER TO WRITE TO QUEUES 
    let writter_handle = std::thread::spawn(move|| {
        core_affinity::set_for_current(core_affinity::CoreId { id: 7 });
        let  shm_writter = ShmWriter::new(
            order_event_consumer_writter_from_bm,
            order_event_consumer_writter_from_publisher,
            order_event_consumer_writter_from_engine,
            balance_event_consumer_writter,
            holding_event_consumer_writter,
            mm_fill_reciever,
            mm_feed_receiver
        );
        if shm_writter.is_some(){
            shm_writter.unwrap().start_shm_writter();
        }
        else{
            eprintln!("error initialising shm writter")
        }
    });
    // AWAITING THE MAIN THREAD FOR INFINITE TIME UNTILL ALL THESE THREADS JOIN 
    for handle in running_engines {
        handle.join().expect("Engine thread panicked");
    }
    publisher_handle.join().expect("Publisher thread panicked");
    balance_manager_handle.join().expect("Balance Manager Paniked");
    shm_reader_handle.join().expect("SHM reader panicked");
    writter_handle.join().expect("Shm writter panicked");
    println!("System shutdown");

    
}
