#[cfg(test)]
mod tests_balance_manager2 {
    use crate::balance_manager::my_balance_manager2::MyBalanceManager2;
    use crate::orderbook::types::BalanceManagerError;
    use crossbeam::channel::{Sender, Receiver};
    use crate::orderbook::order::{Order, Side};
    use crate::orderbook::types::{Fills, Fill , OrderId};
    use crate::balance_manager::types::{BalanceQuery, HoldingsQuery};

    // Helper function to create a test balance manager
    fn setup_balance_manager() -> (MyBalanceManager2, Receiver<Order>, Sender<Fills>, Sender<Order>) {
        let (order_to_engine_tx, order_to_engine_rx) = crossbeam::channel::unbounded::<Order>();
        let (fill_tx, fill_rx) = crossbeam::channel::unbounded::<Fills>();
        let (order_from_shm_tx, order_from_shm_rx) = crossbeam::channel::unbounded::<Order>();
        let (balance_query_tx, balance_query_rx) = crossbeam::channel::unbounded::<BalanceQuery>();
        let (holdings_query_tx, holdings_query_rx) = crossbeam::channel::unbounded::<HoldingsQuery>();
        
        let mut bm = MyBalanceManager2::new(
            order_to_engine_tx.clone(),
            fill_rx,
            order_from_shm_rx,
            balance_query_rx,
            holdings_query_rx
        );
        
        // Add test users
        bm.state.user_id_to_index.insert(10, 1);
        bm.state.user_id_to_index.insert(20, 2);
        bm.state.holdings[2].available_holdings[0] = 100;
        
        (bm, order_to_engine_rx, fill_tx, order_from_shm_tx)
    }

    // Helper to create test order
    fn create_test_order(user_id: u64, order_id: OrderId, side: Side, qty: u32, price: u64, symbol: u32) -> Order {
        Order {
            user_id,
            order_id,
            side,
            shares_qty: qty,
            price,
            timestamp: 0,
            next: None,
            prev: None,
            symbol,
        }
    }

    // Helper to create test fill
    fn create_test_fill(
        maker_user_id: u64,
        taker_user_id: u64,
        maker_order_id: OrderId,
        taker_order_id: OrderId,
        taker_side: Side,
        price: u64,
        quantity: u32,
        symbol: u32
    ) -> Fill {
        Fill {
            price,
            quantity,
            taker_order_id,
            maker_order_id,
            taker_side,
            maker_user_id,
            taker_user_id,
            symbol,
        }
    }

    #[test]
    fn test_user_lookup_success() {
        let (bm, _, _, _) = setup_balance_manager();
        
        let result = bm.get_user_index(10);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 1);
    }

    #[test]
    fn test_user_lookup_not_found() {
        let (bm, _, _, _) = setup_balance_manager();
        
        let result = bm.get_user_index(999);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), BalanceManagerError::UserNotFound));
    }

    #[test]
    fn test_check_and_lock_funds_bid_success() {
        let (mut bm, _, _, _) = setup_balance_manager();
        
        let order = create_test_order(10, 1, Side::Bid, 10, 100, 0);
        
        // Initial balance check
        let initial_available = bm.state.balances[1].available_balance;
        let initial_reserved = bm.state.balances[1].reserved_balance;
        
        // Lock funds
        let result = bm.check_and_lock_funds(order);
        assert!(result.is_ok());
        
        // Verify balance changes (direct access, no atomic loads)
        let new_available = bm.state.balances[1].available_balance;
        let new_reserved = bm.state.balances[1].reserved_balance;
        
        assert_eq!(new_available, initial_available - 1000); // 10 * 100
        assert_eq!(new_reserved, initial_reserved + 1000);
    }

    #[test]
    fn test_check_and_lock_funds_bid_insufficient() {
        let (mut bm, _, _, _) = setup_balance_manager();
        
        // Try to buy more than available balance (default is 10000)
        let order = create_test_order(10, 1, Side::Bid, 1000, 100, 0);
        
        let result = bm.check_and_lock_funds(order);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), BalanceManagerError::InsufficientFunds));
    }

    #[test]
    fn test_check_and_lock_funds_ask_success() {
        let (mut bm, _, _, _) = setup_balance_manager();
        
        // User 20 (index 2) has 100 holdings for symbol 0
        let order = create_test_order(20, 1, Side::Ask, 50, 200, 0);
        
        let initial_available = bm.state.holdings[2].available_holdings[0];
        let initial_reserved = bm.state.holdings[2].reserved_holdings[0];
        
        let result = bm.check_and_lock_funds(order);
        assert!(result.is_ok());
        
        let new_available = bm.state.holdings[2].available_holdings[0];
        let new_reserved = bm.state.holdings[2].reserved_holdings[0];
        
        assert_eq!(new_available, initial_available - 50);
        assert_eq!(new_reserved, initial_reserved + 50);
    }

    #[test]
    fn test_check_and_lock_funds_ask_insufficient() {
        let (mut bm, _, _, _) = setup_balance_manager();
        
        // Try to sell more than available holdings
        let order = create_test_order(20, 1, Side::Ask, 200, 100, 0);
        
        let result = bm.check_and_lock_funds(order);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), BalanceManagerError::InsufficientFunds));
    }

    #[test]
    fn test_multiple_orders_reserve_correctly() {
        let (mut bm, _, _, _) = setup_balance_manager();
        
        let order1 = create_test_order(10, 1, Side::Bid, 10, 100, 0);
        let order2 = create_test_order(10, 2, Side::Bid, 20, 50, 0);
        
        assert!(bm.check_and_lock_funds(order1).is_ok());
        assert!(bm.check_and_lock_funds(order2).is_ok());
        
        let available = bm.state.balances[1].available_balance;
        let reserved = bm.state.balances[1].reserved_balance;
        
        // 10000 - (10*100 + 20*50) = 10000 - 2000 = 8000
        assert_eq!(available, 8000);
        assert_eq!(reserved, 2000);
    }

    #[test]
    fn test_update_balances_taker_bid() {
        let (mut bm, _, _, _) = setup_balance_manager();
        
        // Setup: User 10 buys, User 20 sells
        // First reserve funds for buyer (user 10)
        let buy_order = create_test_order(10, 1, Side::Bid, 10, 100, 0);
        bm.check_and_lock_funds(buy_order).unwrap();
        
        // Reserve holdings for seller (user 20)
        let sell_order = create_test_order(20, 2, Side::Ask, 10, 100, 0);
        bm.check_and_lock_funds(sell_order).unwrap();
        
        // Verify reservations before fill
        assert_eq!(bm.state.holdings[2].reserved_holdings[0], 10);
        
        // Create fill - taker is buyer (user 10), maker is seller (user 20)
        let fill = create_test_fill(
            20,         // maker_user_id (seller)
            10,         // taker_user_id (buyer)
            2,          // maker_order_id
            1,          // taker_order_id
            Side::Bid,  // taker_side (buying)
            100,        // price
            10,         // quantity
            0           // symbol
        );
        
        let fills = Fills {
            fills: vec![fill],
        };
        
        // Update balances
        let result = bm.update_balances_after_trade(fills);
        assert!(result.is_ok());
        
        // Verify buyer (user 10, index 1)
        assert_eq!(bm.state.balances[1].reserved_balance, 0);
        assert_eq!(bm.state.holdings[1].available_holdings[0], 10);
        
        // Verify seller (user 20, index 2)
        assert_eq!(bm.state.balances[2].available_balance, 11000);
        assert_eq!(bm.state.holdings[2].reserved_holdings[0], 0);
        assert_eq!(bm.state.holdings[2].available_holdings[0], 90);
    }

    #[test]
    fn test_update_balances_taker_ask() {
        let (mut bm, _, _, _) = setup_balance_manager();
        
        // Setup: User 20 sells (taker), User 10 buys (maker)
        let sell_order = create_test_order(20, 1, Side::Ask, 10, 100, 0);
        bm.check_and_lock_funds(sell_order).unwrap();
        
        let buy_order = create_test_order(10, 2, Side::Bid, 10, 100, 0);
        bm.check_and_lock_funds(buy_order).unwrap();
        
        // Create fill - taker is seller (user 20), maker is buyer (user 10)
        let fill = create_test_fill(
            10,         // maker_user_id (buyer)
            20,         // taker_user_id (seller)
            2,          // maker_order_id
            1,          // taker_order_id
            Side::Ask,  // taker_side (selling)
            100,        // price
            10,         // quantity
            0           // symbol
        );
        
        let fills = Fills {
            fills: vec![fill],
        };
        
        let result = bm.update_balances_after_trade(fills);
        assert!(result.is_ok());
        
        // Verify taker/seller (user 20, index 2)
        assert_eq!(bm.state.balances[2].available_balance, 11000);
        
        // Verify maker/buyer (user 10, index 1)
        assert_eq!(bm.state.holdings[1].available_holdings[0], 10);
    }

    #[test]
    fn test_partial_fills() {
        let (mut bm, _, _, _) = setup_balance_manager();
        
        // Reserve for order of 100 shares (buyer)
        let order = create_test_order(10, 1, Side::Bid, 100, 50, 0);
        bm.check_and_lock_funds(order).unwrap();
        
        // Setup seller with holdings and reserve them
        bm.state.holdings[2].available_holdings[0] = 100;
        let seller_order = create_test_order(20, 2, Side::Ask, 30, 50, 0);
        bm.check_and_lock_funds(seller_order).unwrap();
        
        // Fill partially (30 shares)
        let fill1 = create_test_fill(
            20, 10, 2, 1, Side::Bid, 50, 30, 0
        );
        
        let fills1 = Fills {
            fills: vec![fill1],
        };
        
        bm.update_balances_after_trade(fills1).unwrap();
        
        // Reserved should be reduced by 30*50 = 1500
        // Original reserved was 100*50 = 5000, now should be 3500
        assert_eq!(bm.state.balances[1].reserved_balance, 3500);
        assert_eq!(bm.state.holdings[1].available_holdings[0], 30);
    }

    #[test]
    fn test_multiple_fills_same_order() {
        let (mut bm, _, _, _) = setup_balance_manager();
        
        // Reserve for order of 100 shares
        let order = create_test_order(10, 1, Side::Bid, 100, 50, 0);
        bm.check_and_lock_funds(order).unwrap();
        
        // Setup seller with enough holdings
        bm.state.holdings[2].available_holdings[0] = 100;
        
        // First seller order for 30 shares
        let sell_order1 = create_test_order(20, 2, Side::Ask, 30, 50, 0);
        bm.check_and_lock_funds(sell_order1).unwrap();
        
        // First fill - 30 shares
        let fill1 = create_test_fill(20, 10, 2, 1, Side::Bid, 50, 30, 0);
        bm.update_balances_after_trade(Fills { fills: vec![fill1] }).unwrap();
        
        // Second seller order for 40 shares
        let sell_order2 = create_test_order(20, 3, Side::Ask, 40, 50, 0);
        bm.check_and_lock_funds(sell_order2).unwrap();
        
        // Second fill - 40 shares
        let fill2 = create_test_fill(20, 10, 3, 1, Side::Bid, 50, 40, 0);
        bm.update_balances_after_trade(Fills { fills: vec![fill2] }).unwrap();
        
        // Total filled: 70 shares, reserved should be 30*50 = 1500
        assert_eq!(bm.state.balances[1].reserved_balance, 1500);
        assert_eq!(bm.state.holdings[1].available_holdings[0], 70);
    }

    #[test]
    fn test_batch_fills() {
        let (mut bm, _, _, _) = setup_balance_manager();
        
        // Reserve for large order
        let order = create_test_order(10, 1, Side::Bid, 100, 50, 0);
        bm.check_and_lock_funds(order).unwrap();
        
        // Setup seller with 100 holdings
        bm.state.holdings[2].available_holdings[0] = 100;
        
        // Reserve for seller orders
        let sell1 = create_test_order(20, 2, Side::Ask, 30, 50, 0);
        bm.check_and_lock_funds(sell1).unwrap();
        
        let sell2 = create_test_order(20, 3, Side::Ask, 40, 50, 0);
        bm.check_and_lock_funds(sell2).unwrap();
        
        let sell3 = create_test_order(20, 4, Side::Ask, 30, 50, 0);
        bm.check_and_lock_funds(sell3).unwrap();
        
        // Multiple fills in one batch
        let fill1 = create_test_fill(20, 10, 2, 1, Side::Bid, 50, 30, 0);
        let fill2 = create_test_fill(20, 10, 3, 1, Side::Bid, 50, 40, 0);
        let fill3 = create_test_fill(20, 10, 4, 1, Side::Bid, 50, 30, 0);
        
        let fills = Fills {
            fills: vec![fill1, fill2, fill3],
        };
        
        bm.update_balances_after_trade(fills).unwrap();
        
        // All 100 shares filled
        assert_eq!(bm.state.balances[1].reserved_balance, 0);
        assert_eq!(bm.state.holdings[1].available_holdings[0], 100);
    }

    #[test]
    //fn test_add_test_users() {
    //    let (balance_query_tx, balance_query_rx) = crossbeam::channel::unbounded::<BalanceQuery>();
    //    let (holdings_query_tx, holdings_query_rx) = crossbeam::channel::unbounded::<HoldingsQuery>();
    //    let (order_tx, order_rx) = crossbeam::channel::unbounded::<Order>();
    //    let (_, fill_rx) = crossbeam::channel::unbounded::<Fills>();
    //    
    //    let mut bm = MyBalanceManager2::new(order_tx, fill_rx, order_rx, balance_query_rx, holdings_query_rx);
    //    
    //    bm.add_test_users();
    //    
    //    // Verify users were added
    //    assert!(bm.state.user_id_to_index.contains_key(&10));
    //    assert!(bm.state.user_id_to_index.contains_key(&20));
    //    
    //    // Verify user 20 has holdings
    //    assert_eq!(bm.state.holdings[2].available_holdings[0], 10);
    //}

    #[test]
    fn test_zero_quantity_order() {
        let (mut bm, _, _, _) = setup_balance_manager();
        
        let order = create_test_order(10, 1, Side::Bid, 0, 100, 0);
        
        let result = bm.check_and_lock_funds(order);
        assert!(result.is_ok());
        
        // Balance should not change
        assert_eq!(bm.state.balances[1].available_balance, 10000);
        assert_eq!(bm.state.balances[1].reserved_balance, 0);
    }

    #[test]
    fn test_different_symbols() {
        let (mut bm, _, _, _) = setup_balance_manager();
        
        // Add holdings for multiple symbols
        bm.state.holdings[1].available_holdings[0] = 100;
        bm.state.holdings[1].available_holdings[1] = 50;
        
        let order1 = create_test_order(10, 1, Side::Ask, 30, 100, 0);
        let order2 = create_test_order(10, 2, Side::Ask, 20, 100, 1);
        
        assert!(bm.check_and_lock_funds(order1).is_ok());
        assert!(bm.check_and_lock_funds(order2).is_ok());
        
        assert_eq!(bm.state.holdings[1].available_holdings[0], 70);
        assert_eq!(bm.state.holdings[1].available_holdings[1], 30);
    }

    // ========== END-TO-END TESTS ==========

    #[test]
    fn test_end_to_end_single_order_flow() {
        let (mut bm, engine_rx, fill_tx, shm_tx) = setup_balance_manager();
        
        // 1. Buyer order arrives from SHM
        let order = create_test_order(10, 1, Side::Bid, 10, 100, 0);
        shm_tx.send(order).unwrap();
        
        // 2. Balance manager receives and processes
        let received_order = bm.order_receiver.try_recv().unwrap();
        assert_eq!(received_order.order_id, 1);
        
        // 3. Check and lock funds
        let lock_result = bm.check_and_lock_funds(received_order);
        assert!(lock_result.is_ok());
        
        // 4. Send to engine
        bm.order_sender.send(received_order).unwrap();
        let engine_order = engine_rx.try_recv().unwrap();
        assert_eq!(engine_order.order_id, 1);
        
        // 5. Setup and reserve seller's holdings
        let sell_order = create_test_order(20, 2, Side::Ask, 10, 100, 0);
        bm.check_and_lock_funds(sell_order).unwrap();
        
        // 6. Engine sends back fill
        let fill = create_test_fill(20, 10, 2, 1, Side::Bid, 100, 10, 0);
        fill_tx.send(Fills { fills: vec![fill] }).unwrap();
        
        // 7. Balance manager updates from fill
        let received_fill = bm.fill_recv.try_recv().unwrap();
        bm.update_balances_after_trade(received_fill).unwrap();
        
        // 8. Verify final state
        assert_eq!(bm.state.balances[1].reserved_balance, 0);
        assert_eq!(bm.state.balances[1].available_balance, 9000);
        assert_eq!(bm.state.holdings[1].available_holdings[0], 10);
    }

    #[test]
    fn test_end_to_end_partial_fill_flow() {
        let (mut bm, engine_rx, fill_tx, shm_tx) = setup_balance_manager();
        
        // Order for 100 shares
        let order = create_test_order(10, 1, Side::Bid, 100, 50, 0);
        shm_tx.send(order).unwrap();
        
        let received_order = bm.order_receiver.try_recv().unwrap();
        bm.check_and_lock_funds(received_order).unwrap();
        bm.order_sender.send(received_order).unwrap();
        
        // Setup seller with 100 holdings
        bm.state.holdings[2].available_holdings[0] = 100;
        
        // First partial sell order - 40 shares
        let sell1 = create_test_order(20, 2, Side::Ask, 40, 50, 0);
        bm.check_and_lock_funds(sell1).unwrap();
        
        // First partial fill - 40 shares
        let fill1 = create_test_fill(20, 10, 2, 1, Side::Bid, 50, 40, 0);
        fill_tx.send(Fills { fills: vec![fill1] }).unwrap();
        let received_fill1 = bm.fill_recv.try_recv().unwrap();
        bm.update_balances_after_trade(received_fill1).unwrap();
        
        // Second partial sell order - 60 shares
        let sell2 = create_test_order(20, 3, Side::Ask, 60, 50, 0);
        bm.check_and_lock_funds(sell2).unwrap();
        
        // Second partial fill - 60 shares (complete)
        let fill2 = create_test_fill(20, 10, 3, 1, Side::Bid, 50, 60, 0);
        fill_tx.send(Fills { fills: vec![fill2] }).unwrap();
        let received_fill2 = bm.fill_recv.try_recv().unwrap();
        bm.update_balances_after_trade(received_fill2).unwrap();
        
        // Verify complete fill
        assert_eq!(bm.state.balances[1].reserved_balance, 0);
        assert_eq!(bm.state.balances[1].available_balance, 5000);
        assert_eq!(bm.state.holdings[1].available_holdings[0], 100);
    }

    #[test]
    fn test_end_to_end_multiple_users_trading() {
        let (mut bm, engine_rx, fill_tx, shm_tx) = setup_balance_manager();
        
        // User 10 places buy order
        let buy_order = create_test_order(10, 1, Side::Bid, 50, 100, 0);
        shm_tx.send(buy_order).unwrap();
        let recv_buy = bm.order_receiver.try_recv().unwrap();
        bm.check_and_lock_funds(recv_buy).unwrap();
        bm.order_sender.send(recv_buy).unwrap();
        
        // User 20 places sell order
        let sell_order = create_test_order(20, 2, Side::Ask, 50, 100, 0);
        shm_tx.send(sell_order).unwrap();
        let recv_sell = bm.order_receiver.try_recv().unwrap();
        bm.check_and_lock_funds(recv_sell).unwrap();
        bm.order_sender.send(recv_sell).unwrap();
        
        // They match
        let fill = create_test_fill(20, 10, 2, 1, Side::Bid, 100, 50, 0);
        fill_tx.send(Fills { fills: vec![fill] }).unwrap();
        let received_fill = bm.fill_recv.try_recv().unwrap();
        bm.update_balances_after_trade(received_fill).unwrap();
        
        // Verify buyer
        assert_eq!(bm.state.balances[1].available_balance, 5000);
        assert_eq!(bm.state.holdings[1].available_holdings[0], 50);
        
        // Verify seller
        assert_eq!(bm.state.balances[2].available_balance, 15000);
        assert_eq!(bm.state.holdings[2].available_holdings[0], 50);
    }

    #[test]
    fn test_end_to_end_insufficient_funds_rejection() {
        let (mut bm, engine_rx, _, shm_tx) = setup_balance_manager();
        
        // Try to buy more than available balance
        let order = create_test_order(10, 1, Side::Bid, 200, 100, 0);
        shm_tx.send(order).unwrap();
        
        let received_order = bm.order_receiver.try_recv().unwrap();
        let lock_result = bm.check_and_lock_funds(received_order);
        
        // Should be rejected
        assert!(lock_result.is_err());
        assert!(matches!(lock_result.unwrap_err(), BalanceManagerError::InsufficientFunds));
        
        // Order should NOT be sent to engine
        assert!(engine_rx.try_recv().is_err());
        
        // Balance should remain unchanged
        assert_eq!(bm.state.balances[1].available_balance, 10000);
        assert_eq!(bm.state.balances[1].reserved_balance, 0);
    }

    #[test]
    fn test_end_to_end_sell_without_holdings() {
        let (mut bm, engine_rx, _, shm_tx) = setup_balance_manager();
        
        // User 10 has no holdings, tries to sell
        let order = create_test_order(10, 1, Side::Ask, 10, 100, 0);
        shm_tx.send(order).unwrap();
        
        let received_order = bm.order_receiver.try_recv().unwrap();
        let lock_result = bm.check_and_lock_funds(received_order);
        
        // Should be rejected
        assert!(lock_result.is_err());
        assert!(matches!(lock_result.unwrap_err(), BalanceManagerError::InsufficientFunds));
        
        // Order should NOT reach engine
        assert!(engine_rx.try_recv().is_err());
    }

    #[test]
    fn test_throughput_initialization() {
        let (balance_query_tx, balance_query_rx) = crossbeam::channel::unbounded::<BalanceQuery>();
        let (holdings_query_tx, holdings_query_rx) = crossbeam::channel::unbounded::<HoldingsQuery>();
        let (order_tx, order_rx) = crossbeam::channel::unbounded::<Order>();
        let (_, fill_rx) = crossbeam::channel::unbounded::<Fills>();
        
        let mut bm = MyBalanceManager2::new(order_tx, fill_rx, order_rx, balance_query_rx, holdings_query_rx);
        
        bm.add_throughput_test_users();
        
        // Verify high balance initialization
        assert_eq!(bm.state.balances[1].available_balance, 100_000_000_000);
        assert_eq!(bm.state.balances[2].available_balance, 100_000_000_000);
        assert_eq!(bm.state.holdings[2].available_holdings[0], 1_000_000_000);
    }
}
