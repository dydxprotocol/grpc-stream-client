"""
Thin wrapper around the StandardFeedHandler to perform orderbook checks.
The full node emitting full node streaming metrics must have the `fns-snapshot-interval`
flag set to a non negative number.
"""
from typing import Dict, List

from v4_proto.dydxprotocol.clob.query_pb2 import StreamOrderbookUpdatesResponse

import src.book as lob
import src.helpers as helpers
from src import fills, subaccounts
import logging
import src.config as config
import src.taker_order_metrics as taker_order_metrics
from src.feed_handler import FeedHandler, StandardFeedHandler

conf = config.Config().get_config()

class ValidationFeedHandler(FeedHandler):
    def __init__(self):
        # Discard messages until the first snapshot is received
        self.has_seen_first_snapshot = False

        # Standard feed handler to maintain standard orderbook.
        self.standard_feed_handler = StandardFeedHandler()
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
        self.standard_feed_handler.handle(message)
        is_snapshot = len(message.updates) > 0 and helpers.is_snapshot_update(message.updates[0])
        
        # Assume if the first message is a snapshot, rest of message is a snapshot and that
        # the whole snapshot is in this singular `StreamOrderbookUpdatesResponse` object.
        # if we've recieved a snapshot message, assemble a new orderbook and run validation checks.
        if self.has_seen_first_snapshot and is_snapshot:
            logging.info(f"Recieved a validation snapshot at block height {message.updates[0].block_height}")
            reconstructed_feed_handler = StandardFeedHandler()
            reconstructed_feed_handler.handle(message)
            success = self.standard_feed_handler.compare(reconstructed_feed_handler)
            if success:
                logging.info("🟢 Validation Snapshot check succeeded")
            else:
                logging.error("🔴 Validation Snapshot check failed")
        if is_snapshot and self.has_seen_first_snapshot == False:
            self.has_seen_first_snapshot = True
        return []

    def get_books(self) -> Dict[int, lob.LimitOrderBook]:
        """
        Returns the books stored in this feed handler.
        """
        return self.standard_feed_handler.get_books()

    def get_subaccounts(self) -> Dict[subaccounts.SubaccountId, subaccounts.StreamSubaccount]:
        """
        Returns the subaccounts stored in this feed handler.
        """
        return self.standard_feed_handler.get_subaccounts()

    def get_recent_subaccount_updates(self) -> Dict[subaccounts.SubaccountId, subaccounts.StreamSubaccount]:
        """
        Returns the subaccounts that were updated in the most recent message.
        """
        return self.standard_feed_handler.get_recent_subaccount_updates()