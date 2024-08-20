"""
Handle messages on the gRPC book feed and build local order books.
"""
from __future__ import annotations
from typing import Dict, List

from v4_proto.dydxprotocol.clob.query_pb2 import StreamOrderbookUpdatesResponse, StreamOrderbookUpdate, \
    StreamOrderbookFill, StreamTakerOrder
from v4_proto.dydxprotocol.indexer.off_chain_updates.off_chain_updates_pb2 import OrderPlaceV1, OrderUpdateV1, \
    OrderRemoveV1, OffChainUpdateV1

import src.book as lob
import src.helpers as helpers
from src import fills, subaccounts
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

    @abstractmethod
    def get_subaccounts(self) -> Dict[subaccounts.SubaccountId, subaccounts.StreamSubaccount]:
        pass

    @abstractmethod
    def get_recent_subaccount_updates(self) -> Dict[subaccounts.SubaccountId, subaccounts.StreamSubaccount]:
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

        # Store subaccounts by subaccount ID
        self.subaccounts: Dict[subaccounts.SubaccountId, subaccounts.StreamSubaccount] = {}

        # List of most recently updated subaccount ids
        self.updated_subaccounts = []

    def handle(self, message: StreamOrderbookUpdatesResponse) -> List[fills.Fill]:
        """
        Handle a message from the gRPC feed, updating the local order book
        state. See the protobuf definition[1] of `StreamOrderbookUpdatesResponse`
        for the message format.

        Returns a list of fills that occurred in the message.

        [1] https://github.com/dydxprotocol/v4-chain/blob/432e711decf01b855cf5ca90b699c9b187399826/proto/dydxprotocol/clob/query.proto#L172-L175
        """
        collected_fills = []
        self.updated_subaccounts = []
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
            elif update_type == 'subaccount_update':
                self._handle_subaccounts(update.subaccount_update)
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

    def _handle_subaccounts(self, update: subaccounts.StreamSubaccountUpdate):
        """
        Handle the StreamSubaccountUpdate message, updating the local subaccount state.
        """
        parsed_subaccount = subaccounts.parse_subaccounts(update)
        subaccount_id = parsed_subaccount.subaccount_id
        self.updated_subaccounts.append(subaccount_id)

        if update.snapshot:
            # Skip subsequent snapshots. This will only happen if
            # snapshot interval is turned on on the full node.
            if subaccount_id in self.subaccounts:
                logging.warning(f"Saw multiple snapshots for subaccount id {subaccount_id}")
                return
            self.subaccounts[subaccount_id] = parsed_subaccount
        else:
            # Skip messages until the first snapshot is received
            if subaccount_id not in self.subaccounts:
                return
            # Update the existing subaccount
            existing_subaccount = self.subaccounts[subaccount_id]
            # Update perpetual positions
            existing_subaccount.perpetual_positions.update(parsed_subaccount.perpetual_positions)
            # Update asset positions
            existing_subaccount.asset_positions.update(parsed_subaccount.asset_positions)

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
            logging.warning(f"Skipping update before first snapshot: {update}")
            return

        # Skip subsequent snapshots. This will only happen if
        # snapshot interval is turned on on the full node.
        if update.snapshot and self.has_seen_first_snapshot:
            logging.warning(f"Skipping subsequent snapshot: {update}")
            return

        if update.snapshot:
            # This is a new snapshot of the book state; start processing updates
            if not self.has_seen_first_snapshot:
                self.has_seen_first_snapshot = True

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

    def get_subaccounts(self) -> Dict[subaccounts.SubaccountId, subaccounts.StreamSubaccount]:
        """
        Returns the subaccounts stored in this feed handler.
        """
        return self.subaccounts

    def get_recent_subaccount_updates(self) -> Dict[subaccounts.SubaccountId, subaccounts.StreamSubaccount]:
        """
        Returns the subaccounts that were updated in the most recent message.
        """
        return {subaccount_id: self.subaccounts[subaccount_id] for subaccount_id in self.updated_subaccounts}

    def compare_subaccounts(self, other) -> bool:
        """
        Compares the subaccounts between this feed handler and another.
        Log mismatches.
        """
        self_subaccounts = self.get_subaccounts()
        other_subaccounts = other.get_subaccounts()

        if len(self_subaccounts) != len(other_subaccounts):
            logging.error(
                f"Subaccount length mismatch: self has {len(self_subaccounts)} subaccounts, "
                f"other has {len(other_subaccounts)} subaccounts."
            )

        mismatched_subaccounts = {}
        for subaccount_id, self_subaccount in self_subaccounts.items():
            other_subaccount = other_subaccounts.get(subaccount_id)
            if other_subaccount is None:
                logging.error(f"Subaccount {subaccount_id} is present in self but missing in other.")
                mismatched_subaccounts[subaccount_id] = (self_subaccount, None)
            elif self_subaccount != other_subaccount:
                mismatched_subaccounts[subaccount_id] = (self_subaccount, other_subaccount)

        for subaccount_id, other_subaccount in other_subaccounts.items():
            if subaccount_id not in self_subaccounts:
                logging.error(f"Subaccount {subaccount_id} is present in other but missing in self.")
                mismatched_subaccounts[subaccount_id] = (None, other_subaccount)

        if mismatched_subaccounts:
            for subaccount_id, (self_subaccount, other_subaccount) in mismatched_subaccounts.items():
                if self_subaccount is None:
                    logging.error(
                        f"Subaccount {subaccount_id} is missing in self but present in other: {other_subaccount}")
                elif other_subaccount is None:
                    logging.error(
                        f"Subaccount {subaccount_id} is missing in other but present in self: {self_subaccount}")
                else:
                    logging.error(
                        f"Subaccount {subaccount_id} differs:\n"
                        f"self: {self_subaccount}\n"
                        f"other: {other_subaccount}"
                    )
            return False

        return True

    def compare(self, other: StandardFeedHandler) -> bool:
        """
        Compares the two standard feed handlers. Returns a
        boolean True if the feed handlers are equivalent, otherwise
        error lgos out inconsistencies.
        """
        failed = False
        self_books = self.get_books()
        other_books = other.get_books()
        if len(self_books) != len(other_books):
            logging.error(f"Mismatched book clob pair ids: \n{self_books.keys()}\n{other_books.keys()}")
            return False

        for cpid in self_books.keys():
            self_book = self_books[cpid]
            other_book = other_books[cpid]
            comparison_failed = self_book.compare_books(other_book)
            if comparison_failed:
                failed = True

        subaccounts_match = self.compare_subaccounts(other)
        if not subaccounts_match:
            failed = True

        return not failed
