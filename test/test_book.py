import unittest

from grpc_stream_client.book import OrderId, Order, LimitOrderBook


def to_order(addr: str, subacc: int, cid: int, is_bid: bool, quantums: int, subticks: int) -> Order:
    return Order(OrderId(addr, subacc, cid, 0), is_bid, quantums, quantums, subticks)


class TestLimitOrderBook(unittest.TestCase):
    def setUp(self):
        # Initialize a LimitOrderBook before each test
        self.lob = LimitOrderBook()

    def test_order_insertion(self):
        # Create and add orders across multiple price levels
        orders = [
            to_order("address1", 1, 101, True, 100, 50),
            to_order("address2", 1, 102, True, 110, 50),
            to_order("address3", 1, 103, True, 120, 51),
            to_order("address4", 1, 104, False, 130, 52),
            to_order("address5", 1, 105, False, 140, 52),
            to_order("address6", 1, 106, False, 150, 53),
        ]
        for order in orders:
            self.lob.add_order(order)

        # Check if orders are inserted correctly maintaining price-time priority
        self.assertEqual(len(self.lob._bids), 2)  # Two price levels for bids
        self.assertEqual(len(self.lob._asks), 2)  # Two price levels for asks
        self.assertIn(50, self.lob._bids)
        self.assertIn(51, self.lob._bids)
        self.assertIn(52, self.lob._asks)
        self.assertIn(53, self.lob._asks)

        # Check order of orders at a single level
        bid_level_50 = list(self.lob._bids[50])
        self.assertEqual(bid_level_50[0].order_id.client_id, 101)
        self.assertEqual(bid_level_50[1].order_id.client_id, 102)

        ask_level_52 = list(self.lob._asks[52])
        self.assertEqual(ask_level_52[0].order_id.client_id, 104)
        self.assertEqual(ask_level_52[1].order_id.client_id, 105)

    def test_order_removal(self):
        # Add multiple orders and then remove them
        orders = [
            to_order("address1", 1, 101, True, 100, 50),
            to_order("address2", 1, 102, True, 110, 50),
            to_order("address3", 1, 103, False, 120, 51),
            to_order("address4", 1, 104, False, 130, 51),
        ]
        for order in orders:
            self.lob.add_order(order)

        # Remove an order and check the state
        self.lob.remove_order(OrderId("address1", 1, 101, 0))
        self.assertTrue(OrderId("address1", 1, 101, 0) not in self.lob._bids[50])

        self.lob.remove_order(OrderId("address3", 1, 103, 0))
        self.assertTrue(OrderId("address3", 1, 103, 0) not in self.lob._asks[51])

        # Verify remaining orders maintain the correct order
        remaining_bid = self.lob._bids[50].head.data
        self.assertEqual(remaining_bid.order_id.client_id, 102)

        remaining_ask = self.lob._asks[51].head.data
        self.assertEqual(remaining_ask.order_id.client_id, 104)

    def test_order_update(self):
        # Add an order and update it
        order = to_order("address4", 2, 104, False, 250, 53)
        self.lob.add_order(order)

        retrieved = self.lob.get_order(order.order_id)
        assert retrieved is not None
        retrieved.quantums = 300

        # Check if the order's quantums are updated
        updated_order = next(self.lob.asks())
        self.assertEqual(updated_order.quantums, 300)

    def test_order_levels(self):
        # Add multiple orders to the same level and different levels
        order1 = to_order("address5", 3, 105, True, 300, 54)
        order2 = to_order("address6", 3, 106, True, 350, 54)
        order3 = to_order("address7", 3, 107, True, 400, 55)

        self.lob.add_order(order1)
        self.lob.add_order(order2)
        self.lob.add_order(order3)

        # Check the correct organization in levels
        self.assertEqual(len(self.lob._bids[54]), 2)
        self.assertEqual(len(self.lob._bids[55]), 1)


if __name__ == "__main__":
    unittest.main()
