"""
Handle messages on the gRPC book feed and build local order books.
"""
from typing import Dict, List

from v4_proto.dydxprotocol.clob.query_pb2 import StreamOrderbookUpdatesResponse, StreamOrderbookUpdate, \
    StreamOrderbookFill, StreamTakerOrder
from v4_proto.dydxprotocol.indexer.off_chain_updates.off_chain_updates_pb2 import OrderPlaceV1, OrderUpdateV1, \
    OrderRemoveV1, OffChainUpdateV1

import src.book as lob
import src.helpers as helpers
from src import fills
from abc import ABC, abstractmethod
import logging
import src.config as config
import src.taker_order_metrics as taker_order_metrics

conf = config.Config().get_config()

# Abstract class for the feed handler.
class FeedHandler(ABC):
    @abstractmethod
    def handle(self, message: StreamOrderbookUpdatesResponse) -> List[fills.Fill]:
        pass

    @abstractmethod
    def get_books(self) -> Dict[int, lob.LimitOrderBook]:
        pass

class StandardFeedHandler(FeedHandler):

    def __init__(self):
        # Store order books by clob pair ID
        self.books: Dict[int, lob.LimitOrderBook] = {}

        # Block heights by clob pair ID
        self.heights: Dict[int, int] = {}

        # Discard messages until the first snapshot is received
        self.has_seen_first_snapshot = False

        self.taker_order_metrics = taker_order_metrics.TakerOrderMetrics()

    def handle(self, message: StreamOrderbookUpdatesResponse) -> List[fills.Fill]:
        """
        Handle a message from the gRPC feed, updating the local order book
        state. See the protobuf definition[1] of `StreamOrderbookUpdatesResponse`
        for the message format.

        Returns a list of fills that occurred in the message.

        [1] https://github.com/dydxprotocol/v4-chain/blob/432e711decf01b855cf5ca90b699c9b187399826/proto/dydxprotocol/clob/query.proto#L172-L175
        """
        collected_fills = []
        for update in message.updates:
            # Each update is either an 'orderbook_update' or an 'order_fill'
            update_type = update.WhichOneof('update_message')
            height = update.block_height
            if update_type == 'orderbook_update':
                self._handle_orderbook_update(update.orderbook_update, height)
            elif update_type == 'order_fill':
                fs = self._handle_fills(update.order_fill, update.exec_mode)
                if fs:  # No fills parsed before snapshot
                    self._update_height(fs[0].clob_pair_id, height)
                collected_fills += fs
            elif update_type == 'taker_order':
                self._handle_taker_order(update.taker_order, height)
            else:
                raise ValueError(f"Unknown update type '{update_type}' in: {update}")

        return collected_fills

    def _update_height(self, clob_pair_id: int, new_block_height: int):
        if new_block_height <= 0:
            raise ValueError(f"Invalid block height: {new_block_height}")

        if (clob_pair_id not in self.heights or
                new_block_height >= self.heights[clob_pair_id]):
            self.heights[clob_pair_id] = new_block_height
        else:
            raise ValueError(
                f"Block height decreased from "
                f"{self.heights[clob_pair_id]} to {new_block_height}"
            )
    
    def _handle_taker_order(
            self,
            stream_taker_order: StreamTakerOrder,
            block_height: int
    ):
        """
        Handle a StreamTakerOrder message.
        """
        order = helpers.parse_protocol_order(stream_taker_order.order)
        self.taker_order_metrics.process_order(order, block_height)


    def _handle_fills(
            self,
            order_fill: StreamOrderbookFill,
            exec_mode: int,
    ) -> List[fills.Fill]:
        """
        Handle a StreamOrderbookFill message[1].

        [1] https://github.com/dydxprotocol/v4-chain/blob/432e711decf01b855cf5ca90b699c9b187399826/proto/dydxprotocol/clob/query.proto#L211-L222
        """
        # Skip messages until the first snapshot is received
        if not self.has_seen_first_snapshot:
            return []

        fs = fills.parse_fill(order_fill, exec_mode)
        for fill in fs:
            # Find the order that filled
            clob_pair_id = fill.clob_pair_id
            maker_oid = fill.maker
            order = self._get_book(clob_pair_id).get_order(maker_oid)

            # If the order isn't in the book (was already removed or something)
            # then there's no fill state to update. This can happen because we
            # remove orders when they are "best effort" cancelled, but orders
            # can still technically be matched until they expire.
            if order is not None:
                # Subtract the total filled quantums from the original quantums to get
                # the remaining amount
                order.quantums = order.original_quantums - fill.maker_total_filled_quantums
        return fs

    def _handle_orderbook_update(
            self,
            update: StreamOrderbookUpdate,
            block_height: int
    ):
        """
        Handle the StreamOrderBookUpdate message[1], which is a series of
        OffChainUpdateV1[2] messages + a flag indicating whether this is a
        snapshot.

        [1] https://github.com/dydxprotocol/v4-chain/blob/432e711decf01b855cf5ca90b699c9b187399826/proto/dydxprotocol/clob/query.proto#L196-L207
        [2] https://github.com/dydxprotocol/v4-chain/blob/432e711decf01b855cf5ca90b699c9b187399826/proto/dydxprotocol/indexer/off_chain_updates/off_chain_updates.proto#L105-L114
        """
        # Skip messages until the first snapshot is received
        if not self.has_seen_first_snapshot and not update.snapshot:
            return

        if update.snapshot:
            # This is a new snapshot of the book state; start processing updates
            if not self.has_seen_first_snapshot:
                self.has_seen_first_snapshot = True
            else:
                raise AssertionError("Saw multiple snapshots, expected exactly one.")

        # Process each update in the batch
        for u in update.updates:
            u: OffChainUpdateV1
            update_type = u.WhichOneof('update_message')

            cpid = None
            if update_type == 'order_place':
                cpid = self._handle_order_place(u.order_place)
            elif update_type == 'order_update':
                cpid = self._handle_order_update(u.order_update)
            elif update_type == 'order_remove':
                cpid = self._handle_order_remove(u.order_remove)
            else:
                raise ValueError(f"Unknown update type '{update_type}' in: {u}")

            self._update_height(cpid, block_height)

        self._validate_books()

    def _validate_books(self):
        for cpid, book in self.books.items():
            ask = next(book.asks(), None)
            bid = next(book.bids(), None)

            if ask and bid:
                p_ask = ask.subticks
                p_bid = bid.subticks
                if p_ask <= p_bid:
                    raise AssertionError(f"Ask price {p_ask} <= bid price "
                                         f"{p_bid} for clob pair {cpid}")

    def _get_book(self, clob_pair_id: int) -> lob.LimitOrderBook:
        """
        Get the order book for a given CLOB pair ID, creating one if none
        exists.
        """
        if clob_pair_id not in self.books:
            self.books[clob_pair_id] = lob.LimitOrderBook()
        return self.books[clob_pair_id]

    def _handle_order_place(self, order_place: OrderPlaceV1) -> int:
        """
        Handle an order placement message and return the clob pair id.
        """
        # Parse the order fields
        order = helpers.parse_indexer_order(order_place.order)
        oid = order.order_id

        # Insert the order into the relevant book
        clob_pair_id = order_place.order.order_id.clob_pair_id
        book = self._get_book(clob_pair_id)

        # Should see a remove before a place for the same order id
        if book.get_order(oid) is not None:
            raise AssertionError(f"Order {oid} already exists in the book")

        book.add_order(order)
        return clob_pair_id

    def _handle_order_update(self, order_update: OrderUpdateV1) -> int:
        """
        Handle an order update message which contains the total filled amount
        for the order (so the remaining amount is original - total filled).

        Return the clob pair id.
        """
        # Find the order
        clob_pair_id = order_update.order_id.clob_pair_id
        oid = helpers.parse_indexer_oid(order_update.order_id)
        order = self._get_book(clob_pair_id).get_order(oid)

        # Ignore updates for orders that don't exist in the book. Each
        # placement is guaranteed to be followed by an update, so ignoring
        # updates for orders that don't yet exist in the book is safe.
        if order is None:
            return clob_pair_id

        # Subtract the total filled quantums from the original quantums to get
        # the remaining amount
        order.quantums = order.original_quantums - order_update.total_filled_quantums
        return clob_pair_id

    def _handle_order_remove(self, order_remove: OrderRemoveV1) -> int:
        """
        Handle an order removal message.

        This may be a best-effort cancel (i.e. not guaranteed to be final; the
        order may later fill) or an order expiry due to its good-til-block or
        good-til-time (which is final).

        If the cancel is reverted, another order placement message will be sent
        first.

        Return the clob pair id.
        """
        # Remove the order from the relevant book
        clob_pair_id = order_remove.removed_order_id.clob_pair_id
        oid = helpers.parse_indexer_oid(order_remove.removed_order_id)
        book = self._get_book(clob_pair_id)
        if book.get_order(oid) is not None:
            book.remove_order(oid)
        else:
            raise AssertionError(f"Order {oid} not in the book")
        return clob_pair_id
    
    def get_books(self) -> Dict[int, lob.LimitOrderBook]:
        """
        Returns the books stored in this feed handler.
        """
        return self.books
