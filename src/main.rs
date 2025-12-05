use std::thread::JoinHandle;
use rust_orderbook_2::orderbook::{types::Event};
use rust_orderbook_2::engine::my_engine::{ Engine, MyEngine};
use rust_orderbook_2::publisher::event_publisher::EventPublisher;
use core_affinity;

fn main(){
    let (event_sender , event_rec) = crossbeam::channel::bounded::<Event>(10000000);
    let sender_clone = event_sender.clone();
    let mut  running_engines : Vec<JoinHandle<()>> = Vec::new();
    let first_join_handle = std::thread::spawn(move ||{
        let _cores = core_affinity::get_core_ids().expect("Failed to get core IDs");
        core_affinity::set_for_current(core_affinity::CoreId { id:  1 });
        let mut engine = MyEngine::new(sender_clone , 0);
        engine.add_book(0);
        engine.run_engine();
    });
    running_engines.push(first_join_handle);
    drop(event_sender);

    // do the above task in a for loop when we initlaise mutliple engines 

    let publisher_handle  = std::thread::spawn(move||{
        core_affinity::set_for_current(core_affinity::CoreId { id: 5 });
        let mut my_publisher = EventPublisher::new(event_rec);
        my_publisher.start_publisher();
    });

    for handle in running_engines {
        handle.join().expect("Engine thread panicked");
    }
    
    publisher_handle.join().expect("Publisher thread panicked");
    

    println!("System shutdown");
    

}
