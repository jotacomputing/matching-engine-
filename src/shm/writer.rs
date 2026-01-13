use bounded_spsc_queue::Consumer;

use crate::{shm::{balance_response_queue::BalanceResponse, event_queue::{OrderEventQueue, OrderEvents}, fill_queue_mm::{MarketMakerFill, MarketMakerFillQueue}, holdings_response_queue::HoldingResponse, market_maker_feed::{self, MarketMakerFeed, MarketMakerFeedQueue}}, singlepsinglecq::my_queue::SpscQueue};
// writer for the order events becuase balance manager , and publisher can send diffeent events 
// publisher more priority so that we get the order responses for post req 
// insufficient funds would have low probobaility 
use crate::shm::holdings_response_queue::HoldingResQueue;
use crate::shm::balance_response_queue::BalanceResQueue;

pub struct ShmWriter{
    pub order_event_queue       : OrderEventQueue ,
    pub balance_response_queue  : BalanceResQueue,
    pub holding_response_queue  : HoldingResQueue,
    pub market_maker_fill_queue : MarketMakerFillQueue,
    pub market_maker_feed_queue : MarketMakerFeedQueue,


    pub rec_from_bm_try : Consumer<OrderEvents>,
    pub rec_from_publisher_try : Consumer<OrderEvents> , 
    pub rec_from_engine_try : Consumer<OrderEvents>,
    pub rec_balance_update : Consumer<BalanceResponse>,
    pub rec_holdings_updates : Consumer<HoldingResponse>,
    pub mm_fill_recive : Consumer<MarketMakerFill>,
    pub mm_feed_recive : Consumer<MarketMakerFeed>
}


impl ShmWriter{
    pub fn new( rec_from_bm_try : Consumer<OrderEvents>,
        rec_from_publisher_try : Consumer<OrderEvents> , 
        rec_from_engine_try : Consumer<OrderEvents>,
        rec_balance_update : Consumer<BalanceResponse>,
        rec_holdings_updates : Consumer<HoldingResponse>,
        mm_fill_recive : Consumer<MarketMakerFill>,
        mm_feed_recive : Consumer<MarketMakerFeed>
        
    )->Option<Self>{
        let order_event_queue = OrderEventQueue::open("/tmp/OrderEvents");
        let holding_response_queue = HoldingResQueue::open("/tmp/HoldingsResponse");
        let balance_response_queue = BalanceResQueue::open("/tmp/BalanceResponse");
        let market_maker_fill_queue = MarketMakerFillQueue::open("/tmp/MarketMakerFills");
        let market_maker_feed_queue = MarketMakerFeedQueue::open("/tmp/MarketMakerFeed");
        if balance_response_queue.is_err(){
            eprintln!("response queue init error in balance manager");
            eprintln!("{:?}" , balance_response_queue)
        }
        if holding_response_queue.is_err(){
            eprintln!("response queue init error in balance manager");
            eprintln!("{:?}" , holding_response_queue)
        }
        if market_maker_fill_queue.is_err(){
            eprintln!("market maker queue init error")
        }
        match order_event_queue {
            Ok(queue)=>{
                Some(Self{
                    mm_feed_recive,
                    order_event_queue : queue ,
                    rec_from_bm_try , 
                    rec_from_publisher_try , 
                    rec_from_engine_try,
                    holding_response_queue : holding_response_queue.unwrap(),
                    balance_response_queue : balance_response_queue.unwrap(),
                    market_maker_fill_queue : market_maker_fill_queue.unwrap(),
                    market_maker_feed_queue : market_maker_feed_queue.unwrap(),
                    rec_balance_update,
                    rec_holdings_updates,
                    mm_fill_recive
                    
                })
            }
            Err(_)=>{
                eprint!("Failed to open write queue");
                None
            }
        }
    }

    pub fn start_shm_writter(&mut self){
        loop {
            let mut did_work = false;
            // THE BALANCE AND THE HOLDINGS EVENTS FOR THE UPDATED BALANCE , HOLDINGS , AFTER EACH TRADE 
            if let Some(balance_updates) = self.rec_balance_update.try_pop(){
                let _ = self.balance_response_queue.enqueue(balance_updates);
                did_work = true;
            }

            if let Some(holding_updates) = self.rec_holdings_updates.try_pop(){
                let _= self.holding_response_queue.enqueue(holding_updates);
                did_work = true;
            }
            // THE ORDER EVENT FOR USER TOO SEE , THE RESULT OF HIS PLACED ORDER 
            if let Some(event) = self.rec_from_publisher_try.try_pop(){
                let _ = self.order_event_queue.enqueue(event);
                did_work = true;
            }
            if let Some(feed )= self.mm_feed_recive.try_pop(){
                let _ = self.market_maker_feed_queue.enqueue(feed);
                did_work = true;
            }
            if let Some(fill) = self.mm_fill_recive.try_pop(){
                let _ = self.market_maker_fill_queue.enqueue(fill);
                did_work = true;
            }
            // THE INSUFFICIENT FUND EVENT 
            if let Some(event) = self.rec_from_bm_try.try_pop(){
                let _ = self.order_event_queue.enqueue(event);
                did_work = true;
            }
            // THE SUCCESSFULL CANCELLATION OF ORDER EVENT 
            if let Some(event) = self.rec_from_engine_try.try_pop(){
                let _= self.order_event_queue.enqueue(event);
                did_work = true ;
            }
            
            if !did_work{
                std::hint::spin_loop();
            }
        }
    }

}