"""
Handle messages on the gRPC book feed and build local order books.
"""

from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Optional
import logging

from grpc_stream_client.fills import Fill, parse_fill
from grpc_stream_client.subaccounts import SubaccountId, StreamSubaccount, StreamSubaccountUpdate, parse_subaccounts
from grpc_stream_client.book import LimitOrderBook
import grpc_stream_client.config as config
import grpc_stream_client.helpers as helpers
import grpc_stream_client.taker_order_metrics as taker_order_metrics
import v4_proto.dydxprotocol.clob.query_pb2 as query_pb2
import v4_proto.dydxprotocol.indexer.off_chain_updates.off_chain_updates_pb2 as off_chain_updates_pb2

conf = config.Config().get_config()


class FeedHandler(ABC):
    @abstractmethod
    def handle(self, message: query_pb2.StreamOrderbookUpdatesResponse) -> list[Fill]:
        pass

    @abstractmethod
    def get_books(self) -> dict[int, LimitOrderBook]:
        pass

    @abstractmethod
    def get_subaccounts(self) -> dict[SubaccountId, StreamSubaccount]:
        pass

    @abstractmethod
    def get_recent_subaccount_updates(self) -> dict[SubaccountId, StreamSubaccount]:
        pass


BlockHeight = int
ClobPairId = int


class StandardFeedHandler(FeedHandler):
    books: dict[ClobPairId, LimitOrderBook]
    block_heights: dict[ClobPairId, BlockHeight]
    has_seen_first_snapshot: bool  # Discard messages until the first snapshot is received
    subaccounts: dict[SubaccountId, StreamSubaccount]
    recently_updated_subaccounts: set[SubaccountId]

    def __init__(self):
        self.books = {}
        self.block_heights = {}
        self.has_seen_first_snapshot = False
        self.taker_order_metrics = taker_order_metrics.TakerOrderMetrics()
        self.subaccounts = {}
        self.recently_updated_subaccounts = set()

    def handle(self, message: query_pb2.StreamOrderbookUpdatesResponse) -> list[Fill]:
        """
        Handle a message from the gRPC feed, updating the local order book
        state. See the protobuf definition[1] of `StreamOrderbookUpdatesResponse`
        for the message format.

        Returns a list of fills that occurred in the message.

        [1] https://github.com/dydxprotocol/v4-chain/blob/432e711decf01b855cf5ca90b699c9b187399826/proto/dydxprotocol/clob/query.proto#L172-L175
        """
        collected_fills = []
        self.recently_updated_subaccounts = set()
        for update in message.updates:
            # Each update is either an 'orderbook_update' or an 'order_fill'
            update_type = update.WhichOneof("update_message")
            height = update.block_height
            if update_type == "orderbook_update":
                self._handle_orderbook_update(update.orderbook_update, height)
            elif update_type == "order_fill":
                fs = self._handle_fills(update.order_fill, update.exec_mode)
                if fs:  # No fills parsed before snapshot
                    self._update_height(fs[0].clob_pair_id, height)
                collected_fills += fs
            elif update_type == "taker_order":
                self._handle_taker_order(update.taker_order, height)
            elif update_type == "subaccount_update":
                self._handle_subaccounts(update.subaccount_update)
            else:
                raise ValueError(f"Unknown update type '{update_type}' in: {update}")

        return collected_fills

    def _update_height(self, clob_pair_id: int, new_block_height: int):
        if new_block_height <= 0:
            raise ValueError(f"Invalid block height: {new_block_height}")

        if clob_pair_id not in self.block_heights or new_block_height >= self.block_heights[clob_pair_id]:
            self.block_heights[clob_pair_id] = new_block_height
        else:
            raise ValueError(f"Block height decreased from {self.block_heights[clob_pair_id]} to {new_block_height}")

    def _handle_subaccounts(self, update: StreamSubaccountUpdate):
        """Handle the StreamSubaccountUpdate message, updating the local subaccount state"""
        parsed_subaccount = parse_subaccounts(update)
        subaccount_id = parsed_subaccount.subaccount_id
        self.recently_updated_subaccounts.add(subaccount_id)

        if update.snapshot:
            # Skip subsequent snapshots. This will only happen if
            # snapshot interval is turned on on the full node.
            if subaccount_id in self.subaccounts:
                logging.warning(f"Saw multiple snapshots for subaccount id {subaccount_id}")
                self.recently_updated_subaccounts.remove(subaccount_id)
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
            existing_subaccount.perpetual_positions = {
                k: v for k, v in existing_subaccount.perpetual_positions.items() if v.quantums != 0
            }
            # Update asset positions
            existing_subaccount.asset_positions.update(parsed_subaccount.asset_positions)
            existing_subaccount.asset_positions = {
                k: v for k, v in existing_subaccount.asset_positions.items() if v.quantums != 0
            }

    def _handle_taker_order(self, stream_taker_order: query_pb2.StreamTakerOrder, block_height: int):
        order = helpers.parse_protocol_order(stream_taker_order.order)
        self.taker_order_metrics.handle_order(order, block_height)

    def _handle_fills(self, order_fill: query_pb2.StreamOrderbookFill, exec_mode: int) -> list[Fill]:
        # Skip messages until the first snapshot is received
        if not self.has_seen_first_snapshot:
            return []

        fs = parse_fill(order_fill, exec_mode)
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

    def _handle_orderbook_update(self, update: query_pb2.StreamOrderbookUpdate, block_height: int):
        # Skip messages until the first snapshot is received
        if not self.has_seen_first_snapshot and not update.snapshot:
            return

        # Skip subsequent snapshots. This will only happen if
        # snapshot interval is turned on on the full node.
        if update.snapshot and self.has_seen_first_snapshot:
            logging.warning("Skipping subsequent snapshot")
            return

        if update.snapshot:
            # This is a new snapshot of the book state; start processing updates
            if not self.has_seen_first_snapshot:
                self.has_seen_first_snapshot = True

        # Process each update in the batch
        u: off_chain_updates_pb2.OffChainUpdateV1
        for u in update.updates:
            update_type = u.WhichOneof("update_message")

            cpid = None
            if update_type == "order_place":
                cpid = self._handle_order_place(u.order_place)
            elif update_type == "order_update":
                cpid = self._handle_order_update(u.order_update)
            elif update_type == "order_remove":
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
                    raise AssertionError(f"Ask price {p_ask} <= bid price {p_bid} for clob pair {cpid}")

    def _get_book(self, clob_pair_id: ClobPairId) -> LimitOrderBook:
        """Get the order book for a given CLOB pair ID, creating one if none exists"""
        if clob_pair_id not in self.books:
            self.books[clob_pair_id] = LimitOrderBook()
        return self.books[clob_pair_id]

    def _handle_order_place(self, order_place: off_chain_updates_pb2.OrderPlaceV1) -> int:
        """Handle an order placement message and return the clob pair id"""
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

    def _handle_order_update(self, order_update: off_chain_updates_pb2.OrderUpdateV1) -> int:
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

    def _handle_order_remove(self, order_remove: off_chain_updates_pb2.OrderRemoveV1) -> int:
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

    def get_books(self) -> dict[int, LimitOrderBook]:
        """Returns the books stored in this feed handler"""
        return self.books

    def get_subaccounts(self) -> dict[SubaccountId, StreamSubaccount]:
        """Returns the subaccounts stored in this feed handler"""
        return self.subaccounts

    def get_recent_subaccount_updates(self) -> dict[SubaccountId, StreamSubaccount]:
        """Returns the subaccounts that were updated in the most recent message"""
        return {
            subaccount_id: self.subaccounts[subaccount_id]
            for subaccount_id in self.recently_updated_subaccounts
            if subaccount_id in self.subaccounts
        }

    def compare_subaccounts(self, other: "StandardFeedHandler") -> bool:
        """Compares the subaccounts between this feed handler and another and log mismatches"""
        self_subaccounts = self.get_subaccounts()
        other_subaccounts = other.get_subaccounts()

        if len(self_subaccounts) != len(other_subaccounts):
            logging.error(
                f"Subaccount length mismatch: self has {len(self_subaccounts)} subaccounts, "
                f"other has {len(other_subaccounts)} subaccounts."
            )

        mismatched_subaccounts: dict[SubaccountId, tuple[Optional[StreamSubaccount], Optional[StreamSubaccount]]] = {}
        for subaccount_id, self_subaccount in self_subaccounts.items():
            other_subaccount = other_subaccounts.get(subaccount_id)
            if other_subaccount is None:
                logging.error(f"Subaccount {subaccount_id} is present in self but missing in other.")
                mismatched_subaccounts[subaccount_id] = (self_subaccount, None)
            elif self_subaccount != other_subaccount:
                mismatched_subaccounts[subaccount_id] = (
                    self_subaccount,
                    other_subaccount,
                )

        for subaccount_id, other_subaccount in other_subaccounts.items():
            if subaccount_id not in self_subaccounts:
                logging.error(f"Subaccount {subaccount_id} is present in other but missing in self.")
                mismatched_subaccounts[subaccount_id] = (None, other_subaccount)

        if mismatched_subaccounts:
            for subaccount_id, (
                self_subaccount_,
                other_subaccount_,
            ) in mismatched_subaccounts.items():
                if self_subaccount_ is None:
                    logging.error(
                        f"Subaccount {subaccount_id} is missing in self but present in other: {other_subaccount_}"
                    )
                elif other_subaccount_ is None:
                    logging.error(
                        f"Subaccount {subaccount_id} is missing in other but present in self: {self_subaccount_}"
                    )
                else:
                    logging.error(
                        f"Subaccount {subaccount_id} differs:\nself: {self_subaccount_}\nother: {other_subaccount_}"
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
            book_match = self_book.compare_books(other_book)
            if not book_match:
                failed = True

        subaccounts_match = self.compare_subaccounts(other)
        if not subaccounts_match:
            failed = True

        return not failed
