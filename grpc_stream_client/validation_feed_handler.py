"""
Thin wrapper around the StandardFeedHandler to perform orderbook checks.
The full node emitting full node streaming metrics must have the `fns-snapshot-interval`
flag set to a non negative number.
"""

from typing import Dict, List
import logging

from grpc_stream_client import fills, subaccounts
from grpc_stream_client.feed_handler import FeedHandler, StandardFeedHandler
from v4_proto.dydxprotocol.clob.query_pb2 import StreamOrderbookUpdatesResponse
import grpc_stream_client.book as book
import grpc_stream_client.config as config
import grpc_stream_client.helpers as helpers

logger = logging.getLogger(__name__)
conf = config.Config().get_config()


class ValidationFeedHandler(FeedHandler):
    """
    For each message, update local order book.
    Once a snapshot message is handled, for each subsequent snapshot,
     log comparison of the local order book and the snapshot order book
    """

    def __init__(self):
        self.has_seen_first_snapshot = False
        self.standard_feed_handler = StandardFeedHandler()
        self.block_height = 0

    def handle(self, message: StreamOrderbookUpdatesResponse) -> List[fills.Fill]:
        """
        Handle a message from the gRPC feed, updating the local order book state.
        If message is a snapshot, compare the local order book to an order book constructed from the snapshot alone.
        Log an error if the snapshot check fails.
        """
        self.standard_feed_handler.handle(message)
        is_snapshot = len(message.updates) > 0 and helpers.is_snapshot_update(message.updates[0])
        self.block_height = max(update.block_height for update in message.updates)

        if self.has_seen_first_snapshot and is_snapshot:
            logging.info(f"Recieved a validation snapshot at block height {message.updates[0].block_height}")
            reconstructed_feed_handler = StandardFeedHandler()
            reconstructed_feed_handler.handle(message)
            success = self.standard_feed_handler.compare(reconstructed_feed_handler)
            if success:
                logging.info("🟢 Validation Snapshot check succeeded")
            else:
                logging.error("🔴 Validation Snapshot check failed")
        if is_snapshot and not self.has_seen_first_snapshot:
            self.has_seen_first_snapshot = True
        return []

    def get_books(self) -> Dict[int, book.LimitOrderBook]:
        return self.standard_feed_handler.get_books()

    def get_subaccounts(self) -> Dict[subaccounts.SubaccountId, subaccounts.StreamSubaccount]:
        return self.standard_feed_handler.get_subaccounts()

    def get_recent_subaccount_updates(self) -> Dict[subaccounts.SubaccountId, subaccounts.StreamSubaccount]:
        """Returns the subaccounts that were updated in the most recent message"""
        return self.standard_feed_handler.get_recent_subaccount_updates()
