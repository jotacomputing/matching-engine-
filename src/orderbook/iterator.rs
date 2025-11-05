use crate::orderbook::price_level::PriceLevel;
use crate::orderbook::order::Side;
use std::collections::BTreeMap;
use std::sync::Arc;

#[derive(Debug)]
pub struct LevelInfo{
    pub price : u64 , 
    pub qty : u64 , 
    pub cumalative_depth : u64
}

pub struct LevelsWithCumalativeDepth<'a>{
    iter : Box<dyn Iterator<Item = (&'a u64, &'a PriceLevel)> + 'a>,
    cumalative_depth : u64
}

impl<'a> LevelsWithCumalativeDepth<'a>{
    // we are impemneting an iterator that just takes an immuatable refrence 
    pub fn new(price_level : &'a BTreeMap<u64 , PriceLevel>,side : Side)->Self{
        let iter : Box<dyn Iterator<Item = (&'a u64, &'a PriceLevel)> +'a> = match side {
            Side::Ask => Box::new(price_level.iter().rev()),
            Side::Bid => Box::new(price_level.iter())
        };
        Self{
            iter , 
            cumalative_depth : 0 
        }
    }
}

impl<'a> Iterator for LevelsWithCumalativeDepth<'a>{
    type Item = LevelInfo ;
    fn next(&mut self)->Option<Self::Item>{
        self.iter.next().map(|entry|{
            let price = *entry.0;
            let quantity = entry.1.get_total_volume();
            self.cumalative_depth = self.cumalative_depth.saturating_add(quantity);

            LevelInfo{
                price , qty : quantity , cumalative_depth : self.cumalative_depth
            }
        })
    }
}