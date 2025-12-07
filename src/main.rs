use std::thread::JoinHandle;
use rust_orderbook_2::balance_manager::my_balance_manager::{ MyBalanceManager};
use rust_orderbook_2::orderbook::order::Order;
use rust_orderbook_2::orderbook::types::Fills;
use rust_orderbook_2::orderbook::{types::Event};
use rust_orderbook_2::engine::my_engine::{ Engine, MyEngine};
use rust_orderbook_2::publisher::event_publisher::EventPublisher;
use core_affinity;
use rust_orderbook_2::shm::reader::ShmReader;

fn main(){

    let (event_sender , event_rec) = crossbeam::channel::bounded::<Event>(10000000);
    let (fill_sender , fill_receiver) = crossbeam::channel::bounded::<Fills>(10000000);
    let (order_sender , order_reciver) = crossbeam::channel::bounded::<Order>(10000000);


    // clones for passing to different parts 
    let event_sender_clone = event_sender.clone();
    let event_receiver_clone = event_rec.clone();
    let fill_sender_clone = fill_sender.clone();
    let fill_reciver_clone = fill_receiver.clone();
    let order_sender_clone = order_sender.clone();
    let order_reciver_clone = order_reciver.clone();
    let order_sender_clone2 = order_sender.clone();
    let order_reciever_clone2 = order_reciver.clone();


    // SHM READER ONLY REQUIRES AN ORDER SENDER 
    let shm_reader_handle = std::thread::spawn(move||{
        let my_shm_reader = ShmReader::new(order_sender_clone2);
        my_shm_reader.unwrap().run_reader();
    });


    // BALANCE MANAGER REQUIRES ORDER RECIVER , ORDER SENDER AND FILL RECIVER 
    let balance_manager_handle = std::thread::spawn(move ||{
        core_affinity::set_for_current(core_affinity::CoreId { id: 6 });
        let mut my_balance_manager = MyBalanceManager::new(order_sender_clone, fill_reciver_clone, order_reciever_clone2);
        my_balance_manager.0.run_balance_manager();
    });


    let mut  running_engines : Vec<JoinHandle<()>> = Vec::new();

    // ENGINE RQUIRES ORDER RECV , FILL SENDER , EVENT SENDER 
    // do the above task in a for loop when we initlaise mutliple engines 
    let first_join_handle = std::thread::spawn(move ||{
        let _cores = core_affinity::get_core_ids().expect("Failed to get core IDs");
        core_affinity::set_for_current(core_affinity::CoreId { id:  1 });
        let mut engine = MyEngine::new(event_sender_clone , 0 , fill_sender_clone , order_reciver_clone );
        engine.add_book(0);
        engine.run_engine();
    });
    running_engines.push(first_join_handle);

    //PUBLISHER REQUIRES AN EVENT RECV ONLY 
    let publisher_handle  = std::thread::spawn(move||{
        core_affinity::set_for_current(core_affinity::CoreId { id: 5 });
        let mut my_publisher = EventPublisher::new(event_receiver_clone);
        my_publisher.start_publisher();
    });

   // DROPING ORIGNAL CHANNELS
    drop(event_sender);
    drop(fill_sender);
    drop(fill_receiver);
    drop(event_rec);
    drop(order_reciver);
    drop(order_sender);


    // AWAITING THE MAIN THREAD FOR INFINITE TIME UNTILL ALL THESE THREADS JOIN 
    for handle in running_engines {
        handle.join().expect("Engine thread panicked");
    }
    publisher_handle.join().expect("Publisher thread panicked");
    balance_manager_handle.join().expect("Balance Manager Paniked");
    shm_reader_handle.join().expect("SHM reader panicked");
    println!("System shutdown");
    

}
