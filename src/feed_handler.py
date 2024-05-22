"""
Handle messages on the gRPC book feed and build local order books.
"""
from typing import Dict

from v4_proto.dydxprotocol.clob.query_pb2 import StreamOrderbookUpdatesResponse
from v4_proto.dydxprotocol.indexer.off_chain_updates.off_chain_updates_pb2 import OrderPlaceV1, OrderUpdateV1, \
    OrderRemoveV1
from v4_proto.dydxprotocol.indexer.protocol.v1.clob_pb2 import IndexerOrder, IndexerOrderId

import src.book as lob


class FeedHandler:

    def __init__(self):
        # Store order books by clob pair ID
        self.books: Dict[int, lob.LimitOrderBook] = {}

        # Discard messages until the first snapshot is received
        self.has_seen_first_snapshot = False

    def handle(self, message: StreamOrderbookUpdatesResponse):
        """
        Handle a message from the gRPC feed, updating the local order book
        state.
        """
        # Skip messages until the first snapshot is received
        if not self.has_seen_first_snapshot and not message.snapshot:
            return

        # Clear books if this is a new snapshot of the book state
        if message.snapshot:
            self.books = {}
            self.has_seen_first_snapshot = True

        # Process each order update in the batch
        for update in message.updates:
            update_type = update.WhichOneof('update_message')
            if update_type == 'order_place':
                self._handle_order_place(update.order_place)
            elif update_type == 'order_update':
                self._handle_order_update(update.order_update)
            elif update_type == 'order_remove':
                self._handle_order_remove(update.order_remove)
            else:
                raise ValueError(f"Unknown update type '{update_type}' in: {update}")

    def _get_book(self, clob_pair_id: int) -> lob.LimitOrderBook:
        """
        Get the order book for a given CLOB pair ID, creating one if none
        exists.
        """
        if clob_pair_id not in self.books:
            self.books[clob_pair_id] = lob.LimitOrderBook()
        return self.books[clob_pair_id]

    def _handle_order_place(self, order_place: OrderPlaceV1):
        """
        Handle an order placement message.
        """
        # Parse the order fields
        oid = parse_id(order_place.order.order_id)
        order = lob.Order(
            order_id=oid,
            is_bid=order_place.order.side == IndexerOrder.SIDE_BUY,
            original_quantums=order_place.order.quantums,
            quantums=order_place.order.quantums,
            subticks=order_place.order.subticks,
        )

        # Insert the order into the relevant book
        clob_pair_id = order_place.order.order_id.clob_pair_id
        self._get_book(clob_pair_id).add_order(order)

    def _handle_order_update(self, order_update: OrderUpdateV1):
        """
        Handle an order update message.
        """
        # Find the order
        clob_pair_id = order_update.order_id.clob_pair_id
        oid = parse_id(order_update.order_id)
        order = self._get_book(clob_pair_id).get_order(oid)

        # Subtract the total filled quantums from the original quantums to get
        # the remaining amount
        order.quantums = order.original_quantums - order_update.total_filled_quantums

    def _handle_order_remove(self, order_remove: OrderRemoveV1):
        """
        Handle an order removal message.
        """
        # Remove the order from the relevant book
        clob_pair_id = order_remove.removed_order_id.clob_pair_id
        self._get_book(clob_pair_id).remove_order(parse_id(order_remove.removed_order_id))


def parse_id(oid_fields: IndexerOrderId) -> lob.OrderId:
    """
    Parse an order ID from the fields in an IndexerOrderId message.
    """
    return lob.OrderId(
        owner_address=oid_fields.subaccount_id.owner,
        subaccount_number=oid_fields.subaccount_id.number,
        client_id=oid_fields.client_id,
    )
