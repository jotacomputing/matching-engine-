
use rust_orderbook_2::{
    balance_manager::my_balance_manager2::{BalanceManagerResForLocking, STbalanceManager}, engine::my_engine::{Engine, STEngine}, logger::{log_reciever::LogReciever, types::{BalanceDelta, BaseLogs, HoldingDelta, OrderBookSnapShot, OrderDelta, TradeLogs}}, orderbook::{order::Order, types::Event}, shm::{balance_log_queue::BalanceLogQueue, balance_response_queue::BalanceResponse, holdings_log_queue::HoldingLogQueue, holdings_response_queue::HoldingResponse, reader::StShmReader, snapshot_queue::OrderBookSnapShotQueue, trade_log_queue::TradeLogQueue}
};
use std::time::{Instant , Duration};
use rust_orderbook_2::shm::queue::{IncomingOrderQueue};
use rust_orderbook_2::shm::balance_response_queue::BalanceResQueue;
use rust_orderbook_2::shm::cancel_orders_queue::CancelOrderQueue;
use rust_orderbook_2::shm::event_queue::OrderEventQueue;
use rust_orderbook_2::shm::holdings_response_queue::HoldingResQueue;
use rust_orderbook_2::shm::query_queue::QueryQueue;
use bounded_spsc_queue::Producer;
use rust_orderbook_2::shm::event_queue::OrderEvents;
use rust_orderbook_2::pubsub::pubsub_manager::RedisPubSubManager;
use rust_orderbook_2::shm::writer::ShmWriter;
use rust_orderbook_2::shm::order_log_queue::OrderLogQueue;
use rust_orderbook_2::publisher::event_publisher::EventPublisher;
use rust_orderbook_2::orderbook::order::Side;
use std::sync::atomic::{AtomicU64, Ordering};

static EVENT_ID: AtomicU64 = AtomicU64::new(1);
#[inline(always)]
fn next_event_id() -> u64 {
    EVENT_ID.fetch_add(1, Ordering::Relaxed)
}


const SNAPSHOT_INTERVAL: Duration = Duration::from_secs(30);
pub struct TradingCore {
    pub balance_manager: STbalanceManager,
    pub shm_reader: StShmReader,
    pub engine: STEngine,
    pub query_queue : QueryQueue,
    processed_count: u64,
    order_batch : Vec<Order> , 
    pub log_sender_to_logger : Producer<BaseLogs>,
    pub snapshot_sender_to_logger : Producer<OrderBookSnapShot> ,
    pub last_snap_shot : Instant,
}
impl TradingCore {
    pub fn new(
        event_sender_to_writter : Producer<OrderEvents> ,
        event_sender_to_publisher_by_engine : Producer<Event> , 
        order_event_producer_engine : Producer<OrderEvents>,
        balance_event_producer_bm : Producer<BalanceResponse>,
        holding_event_producer_bm : Producer<HoldingResponse>,
        log_sender_to_logger : Producer<BaseLogs>,
        snapshot_sender_to_logger : Producer<OrderBookSnapShot>
    ) -> Self {
        let query_queue = QueryQueue::open("/tmp/Queries");
        if query_queue.is_err(){
            eprintln!("query queue init error in balance manager");
            eprintln!("{:?}" , query_queue)
        }
        Self {
            query_queue : query_queue.unwrap(),
            balance_manager: STbalanceManager::new(event_sender_to_writter , balance_event_producer_bm , holding_event_producer_bm),
            shm_reader: StShmReader::new().unwrap(),
            engine: STEngine::new(0 , event_sender_to_publisher_by_engine , order_event_producer_engine),
            processed_count: 0,
            order_batch : Vec::with_capacity(1000),
            log_sender_to_logger , 
            snapshot_sender_to_logger,
            last_snap_shot : Instant::now()
        }
    }
    pub fn run(&mut self) {
        eprintln!("[Trading Core] Starting single-threaded mode");

        loop {

            self.order_batch.clear();
            for _ in 0 ..1000{
                if let Some(order) = self.shm_reader.receive_order() {
                    // some order recved 
                    //println!("order recived {:?}" , order);
                    self.log_sender_to_logger.try_push(BaseLogs::OrderDelta(OrderDelta{
                        event_id : next_event_id(),
                        order_id : order.order_id ,
                        user_id : order.user_id ,
                        price : order.price ,
                        symbol : order.symbol ,
                        shares_qty : order.shares_qty ,
                        side : match order.side {
                            Side::Ask => 1,
                            Side::Bid => 0 
                        }, 
                        order_event_type : 0 
                    }));
                    self.order_batch.push(order);       
                    
                }
                else {
                    break;
                }
            }
            for order in self.order_batch.drain(..){
                match self.balance_manager.check_and_lock_funds(order) {
                    Ok(balance_response_for_logger) => {
                        //println!("balances have been locked and holdings reserved it ws valid order");
                        // balances have been locked or holding shave been reserved 
                        match balance_response_for_logger {
                            BalanceManagerResForLocking::BalanceManagerResUpdateDeltaBalance(balance_delta)=>{
                                self.log_sender_to_logger.try_push(BaseLogs::BalanceDelta(BalanceDelta{
                                    event_id : next_event_id() ,
                                    user_id : order.user_id ,
                                    delta_available : balance_delta.delta_available_balance , 
                                    delta_reserved : balance_delta.delta_reserved_balance ,
                                    reason : 0 ,
                                    order_id : order.order_id
                                }));
                            }

                            BalanceManagerResForLocking::BalanceManagerResUpdateDeltaHolding(holding_delta)=>{
                                self.log_sender_to_logger.try_push(BaseLogs::HoldingDelta(HoldingDelta{
                                    event_id : next_event_id() , 
                                    user_id : order.user_id ,
                                    symbol : order.symbol ,
                                    delta_available : holding_delta.delta_available_holding ,
                                    delta_reserved : holding_delta.delta_reserved_holding,
                                    reason : 0 ,
                                    order_id : order.order_id
                                }));
                            }
                            
                        }
                        // Process order in engine
                        let engine_res = self.engine.process_order(order) ; 
                        match engine_res.0 {
                            Some(match_result) => {
                                // log that order has been matched 
                                //println!("{:?}" , match_result);
                                //println!("logging that order has been matched ");
                                self.log_sender_to_logger.try_push(BaseLogs::OrderDelta(OrderDelta { 
                                    event_id: next_event_id(), 
                                    order_id: order.order_id, 
                                    user_id: order.user_id, 
                                    price: order.price, 
                                    symbol: order.symbol, 
                                    shares_qty: order.shares_qty, 
                                    side : match order.side {
                                        Side::Ask => 1,
                                        Side::Bid => 0 
                                    }, 
                                    order_event_type: 1
                                }));
                                // Update balances from fills
                                if let Err(e) = self.balance_manager
                                    .update_balances_after_trade(match_result.fills ,
                                         |log|{
                                            let _ = self.log_sender_to_logger.try_push(log);
                                         } , 
                                            ||->u64{
                                                next_event_id()
                                            }
                                        )
                                // to do add logging here also when balance changes for consistanc 
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
                               // println!("sending data to publisher");
                               let _ = self.engine.sending_event_to_publisher_try.try_push(Event::new(market_update));
                            }
                            None=>{
                                // no amrket update , dosent matter 
                            }
                        }
                        
                        self.processed_count += 1;
                    }
                    Err(_) => {
                        println!("insufficient funds");
                        // log that order has been rejected 
                        self.log_sender_to_logger.try_push(BaseLogs::OrderDelta(OrderDelta { 
                            event_id: next_event_id(), 
                            order_id: order.order_id, 
                            user_id: order.user_id, 
                            price: order.price, 
                            symbol: order.symbol, 
                            shares_qty: order.shares_qty, 
                            side : match order.side {
                                Side::Ask => 1,
                                Side::Bid => 0 
                            }, 
                            order_event_type: 2, 
                        }));
                        // Order rejected (insufficient funds, etc)
                        let _ = self.balance_manager.events_to_wrriter_try.push(
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

            match self.engine.cancel_order_queue.dequeue(){
                Ok(Some(order_to_be_canceled))=>{
                    if let Some(order_book) = self.engine.get_book_mut(order_to_be_canceled.symbol){
                        // we canceled the order update the balance , pass the orderEvent to the Writter too
                        // this queue would be only for canceling the limit orderrs 
                        // we need to get the detials of this order from the order manager 
                        // side , qty nd price 
                        let order_index = order_book.manager.id_to_index[&order_to_be_canceled.order_id];
                        let order_detials = order_book.manager.get(order_index).unwrap();
                        match self.balance_manager.update_balance_after_order_cancel(order_to_be_canceled, order_detials.side, order_detials.shares_qty, order_detials.price) {
                            Ok(_)=>{
                                order_book.cancel_order(order_to_be_canceled.order_id);
                                // order can be aprtialyl filled also when cancl order comes 
                                // need to chnage the order struct to include '
                                self.engine.sending_order_events_to_writter_try.try_push(OrderEvents { 
                                    user_id: order_to_be_canceled.user_id, 
                                    order_id: order_to_be_canceled.order_id, 
                                    symbol: order_to_be_canceled.symbol, 
                                    event_kind: 4, // cancel order 
                                    filled_qty: 0, 
                                    remaining_qty: 0, 
                                    original_qty: 0, 
                                    error_code: 0
                                 });
                            }
                            Err(_)=>{

                            }
                        }
                    }else{
                        eprint!("invalid order")
                    }
                }
                Ok(None)=>{
                    
                }
                Err(_) => {

                }
            }
            match self.query_queue.dequeue(){
                Ok(Some(query))=>{
                    // we wud only get the add user query from here , qid , user_id , query_Type 2 
                    match query.query_type{
                        2 => {
                            let _= self.balance_manager.add_user(query.user_id);

                        }
                        _=>{

                        }
                    }
                }
                Ok(None)=>{}
                Err(_)=>{}
            }     

            if self.last_snap_shot.elapsed() >= SNAPSHOT_INTERVAL{
                //println!("need to send snapshot now");
                self.engine.snapshot_for_all_book(|snapshot|{
                    //println!("sending snapshot to logg recv");
                    //println!("{:?}" , snapshot);
                    let _ = self.snapshot_sender_to_logger.try_push(snapshot);
                }, next_event_id);
                self.last_snap_shot = Instant::now();
            }   


        }
    }
}


#[hotpath::main]
fn main() {

    let _ = IncomingOrderQueue::create("/tmp/IncomingOrders").expect("failed to create queue");
    let _ = CancelOrderQueue::create("/tmp/CancelOrders").expect("failed to create queue");
    let _ = OrderEventQueue::create("/tmp/OrderEvents").expect("failed to create queue");
    let _ = QueryQueue::create("/tmp/Queries").expect("failed to create queue");
    let _ = HoldingResQueue::create("/tmp/HoldingsResponse").expect("failed to create queue");
    let _ = BalanceResQueue::create("/tmp/BalanceResponse").expect("failed to open queue");
    let _ = OrderLogQueue::create("/tmp/OrderLogs").expect("failed to create the Log queue");
    let _ = BalanceLogQueue::create("/tmp/BalanceLogs").expect("failed to open balance log queue");
    let _ = HoldingLogQueue::create("/tmp/HoldingLogs").expect("failed to open holding queues");
    let _ = TradeLogQueue::create("/tmp/TradeLogs").expect("failed to open trade logs queue");
    let _ = OrderBookSnapShotQueue::create("/tmp/SnapShot").expect("failed to open snap shot queue");


    let (order_event_producer_bm , order_event_consumer_writter_from_bm) = bounded_spsc_queue::make::<OrderEvents>(32678);
    let (event_producer_engine , event_consumer_publisher) = bounded_spsc_queue::make::<Event>(32678);
    let (order_event_producer_publisher , order_event_consumer_writter_from_publisher) = bounded_spsc_queue::make::<OrderEvents>(32768);
    let (order_event_producer_engine , order_event_consumer_writter_from_engine) = bounded_spsc_queue::make::<OrderEvents>(32768);
    let (balance_event_producer_bm , balance_event_consumer_writter) = bounded_spsc_queue::make::<BalanceResponse>(32768);
    let (holding_event_producer_bm , holding_event_consumer_writter) = bounded_spsc_queue::make::<HoldingResponse>(32768);
    let (log_producer_core , log_consumer_logger)=bounded_spsc_queue::make::<BaseLogs>(32786);
    let (trade_log_producer_publisher , trade_log_consumer_logger)= bounded_spsc_queue::make::<TradeLogs>(32768);
    let (orderbook_snapshot_sender , order_book_snapshot_reciver) = bounded_spsc_queue::make::<OrderBookSnapShot>(32768);


    let trading_core_handle = std::thread::spawn(move ||{
        core_affinity::set_for_current(core_affinity::CoreId { id: 2 });
        let mut trading_system = TradingCore::new(
            order_event_producer_bm , 
            event_producer_engine , 
            order_event_producer_engine,
            balance_event_producer_bm,
            holding_event_producer_bm,
            log_producer_core ,
            orderbook_snapshot_sender
        );
        trading_system.balance_manager.add_throughput_test_users();
        trading_system.engine.add_book(0);
        trading_system.run();
    });


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
            order_event_producer_publisher,
            trade_log_producer_publisher
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
            holding_event_consumer_writter
        );
        if shm_writter.is_some(){
            shm_writter.unwrap().start_shm_writter();
        }
        else{
            eprintln!("error initialising shm writter")
        }
    });

    let log_reciver_handle = std::thread::spawn(move||{
        core_affinity::set_for_current(core_affinity::CoreId { id: 3 });
        let mut log_reciver = LogReciever::new(
log_consumer_logger , 
trade_log_consumer_logger , 
order_book_snapshot_reciver) ;
        log_reciver.run();
    });
    

    eprintln!("[Main] Initialization complete, starting trading loop");
    
    publisher_handle.join().expect("publisher pankicked");
    writter_handle.join().expect("writter panicked");
    trading_core_handle.join().expect("trading core oanicked ");
    log_reciver_handle.join().expect("log eciver panicked");
    println!("System shutdown");
}



