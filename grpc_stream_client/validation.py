"""
Connect to a full node gRPC feed and print the top 5 asks and bids periodically,
along with trades whenever they occur.

Logs all messages in JSON format to the path specified in config.yaml.
"""

from typing import Dict, List, Optional
import logging

import grpc  # type: ignore

from grpc_stream_client.book import LimitOrderBook
from grpc_stream_client.helpers import has_snapshot_update
from grpc_stream_client.validation_feed_handler import ValidationFeedHandler
from v4_proto.dydxprotocol.clob.query_pb2 import StreamOrderbookUpdatesRequest, StreamOrderbookUpdatesResponse
from v4_proto.dydxprotocol.clob.query_pb2_grpc import QueryStub
import grpc_stream_client.config as config

logger = logging.getLogger(__name__)

BlockHeight = int


class MissingSnapshotError(RuntimeError):
    pass


async def fetch_orderbook_snapshot(
    clob_pair_ids: List[int],
    validation_feed_handler: ValidationFeedHandler,
) -> Optional[tuple[Dict[int, LimitOrderBook], BlockHeight]]:
    """
    Subscribe to the gRPC stream of order book updates.
    Use the initial snapshot to return order book states and block height.
    to make a local orderbook and return it.
    Raises exception if the initial message does not have a snapshot.
    """
    # Connect to the gRPC feed and start listening
    # (adjust to use secure channel if needed)
    address, _ = config.get_addr_and_cpids()
    async with grpc.aio.insecure_channel(address, config.GRPC_OPTIONS) as channel:
        try:
            
            stub = QueryStub(channel)
            request = StreamOrderbookUpdatesRequest(clob_pair_id=clob_pair_ids)
            response: StreamOrderbookUpdatesResponse
            async for response in stub.StreamOrderbookUpdates(request):
                if has_snapshot_update(response):
                    handler = ValidationFeedHandler()
                    handler.handle(response)
                    books = handler.get_books()
                    return books, handler.block_height
            logger.error("Stream ended")
        except grpc.aio.AioRpcError as e:
            logger.error(f"gRPC error occurred: {e.code()} - {e.details()}")
            raise e
        except Exception as e:
            logger.error(f"Unexpected error in stream: {e}")
            raise e
    raise MissingSnapshotError
