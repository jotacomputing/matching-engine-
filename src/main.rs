use std::sync::Arc;
use std::thread::JoinHandle;
use rust_orderbook_2::balance_manager::my_balance_manager2::{ MyBalanceManager2};
use rust_orderbook_2::balance_manager::types::{BalanceQuery , HoldingsQuery};
use rust_orderbook_2::orderbook::order::Order;
use rust_orderbook_2::orderbook::types::Fills;
use rust_orderbook_2::orderbook::{types::Event};
use rust_orderbook_2::engine::my_engine::{ Engine, MyEngine};
use rust_orderbook_2::publisher::event_publisher::EventPublisher;
use core_affinity;
use rust_orderbook_2::pubsub::pubsub_manager::RedisPubSubManager;
use rust_orderbook_2::shm::reader::ShmReader;
use rust_orderbook_2::singlepsinglecq::my_queue::SpscQueue;
use crossbeam::queue::ArrayQueue;

#[hotpath::main]
fn main(){
    let fill_queue = Arc::new(SpscQueue::<Fills>::new(32768));
    let event_queue = Arc::new(SpscQueue::<Event>::new(32768));
    let bm_engine_order_queue = Arc::new(SpscQueue::<Order>::new(32768));
    let shm_bm_order_queue = Arc::new(SpscQueue::<Order>::new(32768));




    let fill_queue_clone_for_engine = Arc::clone(&fill_queue);
    let fill_queue_clone_for_bm = Arc::clone(&fill_queue);

    let event_queue_clone_for_engine = Arc::clone(&event_queue);
    let event_queue_clone_for_publisher = Arc::clone(&event_queue);

    let bm_engine_order_queue_clone_for_engine = Arc::clone(&bm_engine_order_queue);
    let bm_engine_order_queue_clone_for_bm = Arc::clone(&bm_engine_order_queue);

    let shm_bm_order_queue_clone_for_reader = Arc::clone(&shm_bm_order_queue);
    let shm_bm_order_queue_clone_for_bm = Arc::clone(&shm_bm_order_queue);

    

    
    
    let (event_sender , event_rec) = crossbeam::channel::bounded::<Event>(1024);
    let (fill_sender , fill_receiver) = crossbeam::channel::bounded::<Fills>(1024);
    let (bm_to_engine_sender , bm_to_engine_reciver) = crossbeam::channel::bounded::<Order>(1024);
    let (shm_to_bm_sender , shm_to_bm_receiver) = crossbeam::channel::bounded::<Order>(1024);
    let (_grpc_balance_query_sender , grpc_balance_query_recv)= crossbeam::channel::bounded::<BalanceQuery>(1024);
    let (_grpc_holding_query_sender , grpc_holding_query_recv)= crossbeam::channel::bounded::<HoldingsQuery>(1024);
    // the querysenders will be sent to the grpc serivce that will be created , for each querry a oneshott channel will be initiaised 
    
    // clones for passing to different parts 

    let event_sender_clone = event_sender.clone();
    let event_receiver_clone = event_rec.clone();

    let fill_sender_clone = fill_sender.clone();
    let fill_reciver_clone = fill_receiver.clone();


    let bm_to_engine_sender_clone = bm_to_engine_sender.clone();
    let bm_to_engine_reciver_clone = bm_to_engine_reciver.clone();


    let shm_to_bm_sender_clone = shm_to_bm_sender.clone();
    let shm_to_bm_receiver_clone = shm_to_bm_receiver.clone();


    let grpc_balance_query_recv_clone = grpc_balance_query_recv.clone();
    let grpc_holding_query_recv_clone =  grpc_holding_query_recv.clone();



    // SHM READER ONLY REQUIRES AN ORDER SENDER 
     let shm_reader_handle = std::thread::spawn(move || {
        core_affinity::set_for_current(core_affinity::CoreId { id: 2 });

       
        let mut my_shm_reader = ShmReader::new(
            shm_to_bm_sender_clone,
            shm_bm_order_queue_clone_for_reader
        ).unwrap();

        my_shm_reader.run_reader();
    });

    // BALANCE MANAGER REQUIRES ORDER RECIVER , ORDER SENDER AND FILL RECIVER 
    //let balance_manager_handle = std::thread::spawn(move ||{
    //    core_affinity::set_for_current(core_affinity::CoreId { id: 6 });
    //    let mut my_balance_manager = MyBalanceManager::new(bm_to_engine_sender_clone, fill_reciver_clone, shm_to_bm_receiver_clone);
    //    my_balance_manager.0.add_throughput_test_users();
    //    my_balance_manager.0.run_balance_manager();
    //    // my_balance_manager.1 is the shared state which needs to be passed to the GRPC server for normal queries 
    //});

    let balance_manager_handle = std::thread::spawn(move || {
        core_affinity::set_for_current(core_affinity::CoreId { id: 6 });

        let mut my_balance_manager = MyBalanceManager2::new(
            bm_to_engine_sender_clone,
            fill_reciver_clone,
            shm_to_bm_receiver_clone,
            grpc_balance_query_recv_clone,
            grpc_holding_query_recv_clone,
            fill_queue_clone_for_bm,
            shm_bm_order_queue_clone_for_bm,
            bm_engine_order_queue_clone_for_bm
        );

        my_balance_manager.add_throughput_test_users();
        my_balance_manager.run_balance_manager();
    });

    let mut running_engines: Vec<JoinHandle<()>> = Vec::new();

    let first_join_handle = std::thread::spawn(move || {
        core_affinity::set_for_current(core_affinity::CoreId { id: 1 });

        let mut engine = MyEngine::new(
            event_sender_clone,
            0,
            fill_sender_clone,
            bm_to_engine_reciver_clone,
            bm_engine_order_queue_clone_for_engine,
            fill_queue_clone_for_engine,
            event_queue_clone_for_engine
        );

        engine.add_book(0);
        engine.run_engine();
    });

    running_engines.push(first_join_handle);
    let pubsub_connection = RedisPubSubManager::new("redis://localhost:6379");
    if pubsub_connection.is_err(){
        panic!("pubsub error , not initialising publisher");
    }
    //PUBLISHER REQUIRES AN EVENT RECV ONLY 
    let publisher_handle = std::thread::spawn(move || {
        core_affinity::set_for_current(core_affinity::CoreId { id: 5 });

        let mut my_publisher = EventPublisher::new(
            event_receiver_clone,
            event_queue_clone_for_publisher,
            pubsub_connection.unwrap()
        );

        my_publisher.start_publisher();
    });

   // DROPING ORIGNAL CHANNELS
    drop(event_sender);
    drop(fill_sender);
    drop(fill_receiver);
    drop(event_rec);
    drop(bm_to_engine_reciver);
    drop(bm_to_engine_sender);
    drop(shm_to_bm_receiver);
    drop(shm_to_bm_sender);
    drop(grpc_balance_query_recv);
    drop(grpc_holding_query_recv);

    // AWAITING THE MAIN THREAD FOR INFINITE TIME UNTILL ALL THESE THREADS JOIN 
    for handle in running_engines {
        handle.join().expect("Engine thread panicked");
    }
    publisher_handle.join().expect("Publisher thread panicked");
    balance_manager_handle.join().expect("Balance Manager Paniked");
    shm_reader_handle.join().expect("SHM reader panicked");
    println!("System shutdown");
    

}
