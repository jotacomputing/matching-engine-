use redis::{Client, Connection, Commands, RedisError};

pub struct RedisPubSubManager {
    connection: Connection,
}

impl RedisPubSubManager {
    pub fn new(redis_url: &str) -> Result<Self, RedisError> {
       
        let client = Client::open(redis_url)?;
        
        
        let connection = client.get_connection()?;
        
        Ok(Self { connection })
    }
    
    pub fn publish(&mut self, stream: &str, message: Vec<u8>) -> Result<usize, RedisError> {
        self.connection.publish(stream , message)
    }
}

