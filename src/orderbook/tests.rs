use crate::orderbook::order_book::OrderBook;
use crate::orderbook::order::{Order,Side};
#[cfg(test)]
mod tests {
    use super::*;

    fn new_order(user_id : u64 , order_id: u64, side: Side, qty: u32, price: u64, timestamp: u64, symbol: u32) -> Order {
        Order::new(user_id , order_id, side,  1, qty, price, timestamp, symbol)
    }

    #[test]
    fn test_basic_limit_bid_and_ask_match() {
        let mut book = OrderBook::new(1);

        // Insert limit ask, should be unmatched
        let ask = new_order(20  ,1, Side::Ask, 100, 105, 1, 1);
        book.insert_order(ask);
        assert_eq!(book.askside.levels.get(&105).unwrap().get_total_volume(), 100);
        assert_eq!(book.get_best_ask(), Some(105));
        assert_eq!(book.get_best_bid(), None);

        // Insert aggressive bid that matches
        let mut bid = new_order(20 , 2, Side::Bid, 100, 105, 2, 1);
        let result = book.match_bid(&mut bid , |_|{}).unwrap();
        assert_eq!(result.fills.fills.len(), 1);
        assert_eq!(result.remaining_qty, 0);
        assert_eq!(book.get_best_ask(), None); // Book should be empty now
        assert_eq!(book.get_best_bid(), None);
    }

    #[test]
    fn test_partial_fill_then_resting_order() {
        let mut book = OrderBook::new(1);

        // One resting ask at 105
        let ask = new_order(20 , 10, Side::Ask, 100, 105, 10, 1);
        book.insert_order(ask);

        // Bid for 50 at 105 (partial fill)
        let mut bid = new_order(20 , 11, Side::Bid, 50, 105, 11, 1);
        let result = book.match_bid(&mut bid , |_|{}).unwrap();
        assert_eq!(result.fills.fills.len(), 1);
        assert_eq!(result.remaining_qty, 0);

        // Remaining 50 ask should still be in book
        let level = book.askside.levels.get(&105).unwrap();
        assert_eq!(level.get_total_volume(), 50);

        // Second bid for 100 at 105 (should fill the 50 and rest the rest)
        let mut bid2 = new_order(20 , 12, Side::Bid, 100, 105, 12, 1);
        let result2 = book.match_bid(&mut bid2 , |_|{}).unwrap();
        assert_eq!(result2.fills.fills.len(), 1);
        assert_eq!(result2.remaining_qty, 50); // 50 rested
        // Bid book should now have the leftover bid
        assert_eq!(book.bidside.levels.get(&105).unwrap().get_total_volume(), 50);
    }

    #[test]
    fn test_market_order_drains_multiple_levels() {
        let mut book = OrderBook::new(1);
        // Resting asks at 105 (60) and 106 (50)
        book.insert_order(new_order(30 , 21, Side::Ask, 60, 105, 21, 1));
        book.insert_order(new_order(30 , 22, Side::Ask, 50, 106, 22, 1));

        let mut market_bid = new_order(30 , 23, Side::Bid, 90, 110, 23, 1);
        let result = book.match_market_order(&mut market_bid , |_|{}).unwrap();
        assert_eq!(result.fills.fills.len(), 2);
        assert_eq!(result.fills.fills[0].quantity, 60);
        assert_eq!(result.fills.fills[1].quantity, 30);
        assert_eq!(result.remaining_qty, 0);

        // 106 ask should still have 20 left
        let level = book.askside.levels.get(&106).unwrap();
        assert_eq!(level.get_total_volume(), 20);
        assert_eq!(book.askside.levels.len(), 1); // 105 level removed
    }

    #[test]
    fn test_resting_orders_and_cancellation() { 
        let mut book = OrderBook::new(1);
        // Rest 2 bids at 101 (order_id:10, order_id:11)
        book.insert_order(new_order(40 , 10, Side::Bid, 100, 101, 10, 1));
        book.insert_order(new_order(40 , 11, Side::Bid, 50, 101, 11, 1));
        // Rest one bid at 100 , 
        book.insert_order(new_order(40 , 12, Side::Bid, 120, 100, 12, 1));
        // Assert orderbook depth
        assert_eq!(book.bidside.levels.get(&101).unwrap().get_total_volume(), 150);
        assert_eq!(book.bidside.levels.get(&100).unwrap().get_total_volume(), 120);
        // Now, cancel order 11
        book.cancel_order(11 , |_|{});
        assert_eq!(book.bidside.levels.get(&101).unwrap().get_total_volume(), 100);
        // Cancel order 10
        book.cancel_order(10 , |_|{});
        // Depth at 101 should be gone now
        assert!(book.bidside.levels.get(&101).is_none() || book.bidside.levels.get(&101).unwrap().get_total_volume() <= 0);
    }

    #[test]
    fn test_edge_case_fill_remaining_and_price_levels() {
        let mut book = OrderBook::new(2);
        // Resting asks at two levels
        book.insert_order(new_order(50 , 100, Side::Ask, 80, 200, 1, 2));
        book.insert_order(new_order(50 , 101, Side::Ask, 60, 201, 2, 2));
        // Bid fills all of 200
        let mut bid = new_order(50 , 102, Side::Bid, 100, 200, 3, 2);
        let result = book.match_bid(&mut bid , |_|{}).unwrap();
        assert_eq!(result.fills.fills.len(), 1);
        assert_eq!(result.fills.fills[0].quantity, 80); // Filled all of 200
        assert_eq!(result.remaining_qty, 20); // Rested at 200
        // Next bid fills remaining
        let mut bid2 = new_order(50 , 103, Side::Bid, 20, 200, 4, 2);
        let result2 = book.match_bid(&mut bid2 , |_|{}).unwrap();
        assert_eq!(result2.fills.fills.len(), 0);
        assert_eq!(result2.remaining_qty, 20);
    }

    #[test]
    fn test_cancel_nonexistent_and_reuse_slots() {
        let mut book = OrderBook::new(3);
        book.insert_order(new_order(60 , 201, Side::Bid, 100, 500, 1, 3));
        // Cancel order that doesn't exist (should not panic)
        book.manager.remove_order(9999);
        assert_eq!(book.manager.all_orders.len(), 1); // Only one order existed
        // Insert new order and make sure slot was reused after actual cancellation
        book.manager.remove_order(201);
        let idx_reuse = book.manager.insert_order(new_order(60 , 202, Side::Bid, 200, 500, 2, 3));
        assert_eq!(idx_reuse, 0); // Slot reused if capacity=1
    }
    #[test]
    fn test_market_order_consumes_multiple_price_levels() {
        let mut book = OrderBook::new(5);
        book.insert_order(new_order(100, 1, Side::Ask, 80, 100, 1, 5));
        book.insert_order(new_order(100, 2, Side::Ask, 120, 101, 2, 5));
        book.insert_order(new_order(100, 3, Side::Ask, 150, 102, 3, 5));
        // Market order for 300 shares
        let mut mkt_bid = new_order(100 ,100, Side::Bid, 300, 999_999, 10, 5);
        let result = book.match_market_order(&mut mkt_bid , |_|{}).unwrap();
        let fill_sizes: Vec<u32> = result.fills.fills.iter().map(|f| f.quantity).collect();
        assert_eq!(fill_sizes, vec![80, 120, 100]); // Last fill is 100 because only 300 taken
        assert_eq!(result.remaining_qty, 0);
        // Book should only have 50 shares left at 102
        assert_eq!(book.askside.levels.get(&102).unwrap().get_total_volume(), 50);
    }

    // 2. Canceling Head, Tail, Middle Orders
    #[test]
    fn test_cancel_head_tail_middle() {
        let mut book = OrderBook::new(6);
        // Insert three orders at the same price (order: head/mid/tail)
        let o1 = new_order(200,1, Side::Bid, 50, 100, 1, 6);
        let o2 = new_order(200,2, Side::Bid, 60, 100, 2, 6);
        let o3 = new_order(200,3, Side::Bid, 70, 100, 3, 6);
        book.insert_order(o1); // head
        book.insert_order(o2); // middle
        book.insert_order(o3); // tail
        let pl = book.bidside.levels.get(&100).unwrap();
        assert_eq!(pl.get_total_volume(), 180);
        // Cancel head
        book.cancel_order(1 , |_|{});
        assert_eq!(book.bidside.levels.get(&100).unwrap().get_total_volume(), 130);
        // Cancel tail
        book.cancel_order(3 , |_|{});
        assert_eq!(book.bidside.levels.get(&100).unwrap().get_total_volume(), 60);
        // Insert a new one for middle cancel
        book.insert_order(new_order(200 , 4, Side::Bid, 20, 100, 4, 6)); // At new tail
        book.cancel_order(2 , |_|{}); // Middle
        assert_eq!(book.bidside.levels.get(&100).unwrap().get_total_volume(), 20);
    }

    // 3. Submit bid when ask side is empty, and vice versa
    #[test]
    fn test_bid_or_ask_on_empty_opposite_side() {
        let mut book = OrderBook::new(7);
        // No asks, submit bid
        let mut bid = new_order(500 , 10, Side::Bid, 100, 100, 1, 7);
        let result = book.match_bid(&mut bid , |_|{}).unwrap();
        assert!(result.fills.fills.is_empty());
        // Check it's resting in book
        assert_eq!(book.bidside.levels.get(&100).unwrap().get_total_volume(), 100);
        // No bids but submit ask
        let mut ask = new_order(500 , 11, Side::Ask, 120, 101, 1, 7);
        let result2 = book.match_ask(&mut ask , |_|{}).unwrap();
        assert!(result2.fills.fills.is_empty());
        assert_eq!(book.askside.levels.get(&101).unwrap().get_total_volume(), 120);
    }

    // 4. Partial fills leading to leftovers (multiple partials)
    #[test]
    fn test_multi_partial_fills_leading_to_leftovers() {
        let mut book = OrderBook::new(8);
        book.insert_order(new_order(10, 21, Side::Ask, 30, 105, 1, 8));
        book.insert_order(new_order(10,22, Side::Ask, 50, 105, 2, 8));
        book.insert_order(new_order(10,23, Side::Ask, 40, 105, 3, 8));
        // First bid for 20 (partial fill head)
        let mut bid1 = new_order(10,31, Side::Bid, 20, 105, 4, 8);
        let result1 = book.match_bid(&mut bid1 , |_|{}).unwrap();
        assert_eq!(result1.fills.fills.len(), 1);
        assert_eq!(book.askside.levels.get(&105).unwrap().get_total_volume(), 100);
        // Second bid for 20 (fills remainder of head, starts on next)
        let mut bid2 = new_order(10,32, Side::Bid, 20, 105, 5, 8);
        let result2 = book.match_bid(&mut bid2 , |_|{}).unwrap();
        assert_eq!(result2.fills.fills.len(), 2);
        // Remaining orders: 50 and 40
        assert_eq!(book.askside.levels.get(&105).unwrap().get_total_volume(), 80);
    }

    // 5. Order cancellation before being matched
    #[test]
    fn test_cancel_order_before_matched() {
        let mut book = OrderBook::new(9);
        // Insert bid, then cancel before any match
        book.insert_order(new_order(20,50, Side::Bid, 200, 150, 1, 9));
        book.cancel_order(50 , |_|{});
        // Confirm removed from book
        assert!(book.bidside.levels.get(&150).is_none() ||
                book.bidside.levels.get(&150).unwrap().get_total_volume() == 0);
        // Now insert an ask that would have matched, but nothing to cross
        let mut ask = new_order(20 ,51, Side::Ask, 200, 150, 2, 9);
        let result = book.match_ask(&mut ask , |_|{}).unwrap();
        // Should rest, no fills
        assert!(result.fills.fills.is_empty());
        assert_eq!(book.askside.levels.get(&150).unwrap().get_total_volume(), 200);
    }

    // 6. Reusing free slots/indices
    #[test]
    fn test_free_list_slot_reuse() {
        let mut book = OrderBook::new(10);
        // Insert & remove 3 orders
        let _idx1 = book.manager.insert_order(new_order(35 , 61, Side::Bid, 10, 80, 1, 10));
        let _idx2 = book.manager.insert_order(new_order(35 , 62, Side::Bid, 10, 80, 2, 10));
        let _idx3 = book.manager.insert_order(new_order(35 , 63, Side::Bid, 10, 80, 3, 10));
        println!("After inserts: free_list: {:?}", book.manager.free_list);
        println!("After inserts: all_orders:");
        for (idx, slot) in book.manager.all_orders.iter().enumerate() { println!("  {}: {:?}", idx, slot); }
        book.cancel_order(61, |_|{});
        book.cancel_order(62, |_|{});
        book.cancel_order(63, |_|{});
        println!("After cancels: free_list: {:?}", book.manager.free_list);
        for (idx, slot) in book.manager.all_orders.iter().enumerate() { println!("  {}: {:?}", idx, slot); }
// After re-inserts
        // Insert 3 new orders, they should reuse indices
        let idx4 = book.manager.insert_order(new_order(35  ,64, Side::Bid, 10, 80, 4, 10));
        let idx5 = book.manager.insert_order(new_order(35  ,65, Side::Bid, 10, 80, 5, 10));
        let idx6 = book.manager.insert_order(new_order(35  ,66, Side::Bid, 10, 80, 6, 10));
        println!("After re-inserts: free_list: {:?}", book.manager.free_list);
        for (idx, slot) in book.manager.all_orders.iter().enumerate() { println!("  {}: {:?}", idx, slot); }
        let slots: Vec<Option<&Order>> = vec![
            book.manager.get(idx4),
            book.manager.get(idx5),
            book.manager.get(idx6),
        ];
        assert!(slots.iter().all(|o| o.is_some()));
    }

    // 7. Book State Consistency Checks
   // #[test]
    //fn test_book_state_consistency() {
    //    let mut book = OrderBook::new(11);
    //    // Fill up some orders
    //    for i in 0..10 {
    //        book.insert_order(new_order(1000+i, Side::Bid, (i+1)*5 , 90+i, i as u64, 11));
    //    }
    //    // Cancel a few
    //    book.manager.remove_order(1002);
    //    book.manager.remove_order(1004);
    //    // After removal: check links and totals
    //    for (&price, level) in &book.bidside.levels {
    //        // All orders at price must have prev/next forming a valid chain
    //        let mut curr_idx = level.head;
    //        let mut total = 0u32;
    //        let mut seen = 0;
    //        let mut prev = None;
    //        while let Some(idx) = curr_idx {
    //            let order = book.manager.get(idx).unwrap();
    //            if prev.is_some() {
    //                assert_eq!(order.prev, prev);
    //            }
    //            total += order.shares_qty;
    //            prev = Some(idx);
    //            seen += 1;
    //            curr_idx = order.next;
    //        }
    //        assert_eq!(total, level.get_total_volume());
    //        if seen > 0 {
    //            // tail really is at end
    //            assert_eq!(prev, level.tail);
    //        }
    //    }
    //    // Book should have no 0-qty orders
    //    for o in &book.manager.all_orders {
    //        if let Some(order) = o {
    //            assert!(order.shares_qty > 0);
    //        }
    //    }
    //}

}
