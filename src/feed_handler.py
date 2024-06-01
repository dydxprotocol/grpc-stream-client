"""
Handle messages on the gRPC book feed and build local order books.
"""
from typing import Dict

from google.protobuf.json_format import MessageToJson
from v4_proto.dydxprotocol.clob.query_pb2 import StreamOrderbookUpdatesResponse, StreamOrderbookUpdate, \
    StreamOrderbookFill
from v4_proto.dydxprotocol.indexer.off_chain_updates.off_chain_updates_pb2 import OrderPlaceV1, OrderUpdateV1, \
    OrderRemoveV1, OffChainUpdateV1
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
        state. See the protobuf definition[1] of `StreamOrderbookUpdatesResponse`
        for the message format.

        [1]  https://github.com/dydxprotocol/v4-chain/blob/efa59b4bf40ee72077cc3c62013c1ae0da340163/proto/dydxprotocol/clob/query.proto#L172-L223
        """
        # Each update is either an 'orderbook_update' or an 'order_fill'
        for update in message.updates:
            update_type = update.WhichOneof('update_message')
            if update_type == 'orderbook_update':
                self._handle_orderbook_update(update.orderbook_update)
            elif update_type == 'order_fill':
                self._handle_order_fill(update.order_fill)
            else:
                raise ValueError(f"Unknown update type '{update_type}' in: {update}")

    def _handle_orderbook_update(self, update: StreamOrderbookUpdate):
        """
        Handle the StreamOrderBookUpdate message[1], which is a series of
        OffChainUpdateV1[2] messages + a flag indicating whether this is a
        snapshot.

        [1] https://github.com/dydxprotocol/v4-chain/blob/efa59b4bf40ee72077cc3c62013c1ae0da340163/proto/dydxprotocol/clob/query.proto#L197-L208
        [2] https://github.com/dydxprotocol/v4-chain/blob/efa59b4bf40ee72077cc3c62013c1ae0da340163/proto/dydxprotocol/indexer/off_chain_updates/off_chain_updates.proto#L83-L90
        """
        # Skip messages until the first snapshot is received
        if not self.has_seen_first_snapshot and not update.snapshot:
            return

        # Clear books if this is a new snapshot of the book state
        if update.snapshot:
            self.books = {}
            self.has_seen_first_snapshot = True

        # Process each update in the batch
        for u in update.updates:
            u: OffChainUpdateV1
            update_type = u.WhichOneof('update_message')
            if update_type == 'order_place':
                self._handle_order_place(u.order_place)
            elif update_type == 'order_update':
                self._handle_order_update(u.order_update)
            elif update_type == 'order_remove':
                self._handle_order_remove(u.order_remove)
            else:
                raise ValueError(f"Unknown update type '{update_type}' in: {u}")

    def _handle_order_fill(self, order_fill: StreamOrderbookFill):
        """
        Handle the StreamOrderbookFill message[1] by printing information about
        the fill.

        Does not update the book state, because order quantity remaining changes
        form partial/complete fills are handled by order update/remove messages.

        [1] https://github.com/dydxprotocol/v4-chain/blob/efa59b4bf40ee72077cc3c62013c1ae0da340163/proto/dydxprotocol/clob/query.proto#L212-L223
        """
        print(f"Fill: {MessageToJson(order_fill, indent=None)}")

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
        Handle an order update message which contains the total filled amount
        for the order (so the remaining amount is original - total filled).
        """
        if order_update.total_filled_quantums == 0:
            return

        # Find the order
        clob_pair_id = order_update.order_id.clob_pair_id
        oid = parse_id(order_update.order_id)
        order = self._get_book(clob_pair_id).get_order(oid)

        # Ignore updates for orders that don't exist in the book. Each
        # placement is guaranteed to be followed by an update, so ignoring
        # updates for orders that don't yet exist in the book is safe.
        if order is None:
            return

        # Subtract the total filled quantums from the original quantums to get
        # the remaining amount
        order.quantums = order.original_quantums - order_update.total_filled_quantums

    def _handle_order_remove(self, order_remove: OrderRemoveV1):
        """
        Handle an order removal message.

        This may be a best-effort cancel (i.e. not guaranteed to be final; the
        order may later fill) or an order expiry due to its good-til-block or
        good-til-time (which is final).

        If the cancel is reverted, another order placement message will be sent
        first.
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
