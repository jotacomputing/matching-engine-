use tokio::sync::mpsc;
use crate::orderbook::types::Event;
pub struct EventPublisher{
    reciever : mpsc::Receiver<Event>
}

impl EventPublisher {
    pub fn new(rx : mpsc::Receiver<Event>)->Self{
        Self{
            reciever : rx
        }
        
    }
    pub async  fn start_publisher(&mut self){
        while let Some(event) = self.reciever.recv().await {
            // publish to kafka or our message broker 
            println!("event recied {:?}" ,event);
        }
    }
}