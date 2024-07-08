"""
Connect to a full node gRPC feed and print the top 5 asks and bids periodically,
along with trades whenever they occur.

Logs all messages in JSON format to the path specified in config.yaml.
"""
import logging
from typing import Dict, List

import grpc
# Classes generated from the proto files
from v4_proto.dydxprotocol.clob.query_pb2 import StreamOrderbookUpdatesRequest, StreamOrderbookUpdatesResponse, StreamOrderbookUpdate
from v4_proto.dydxprotocol.clob.query_pb2_grpc import QueryStub

import src.book as lob
import src.validation_feed_handler as validation_feed_handler
import src.config as config



async def fetch_orderbook_snapshot(
    clob_pair_ids: List[int],
) -> tuple[Dict[int, lob.LimitOrderBook], int]:
    """
    Subscribe to the gRPC stream of order book updates use the initial snapshot
    to make a local orderbook and return it.
    """
    # Connect to the gRPC feed and start listening
    # (adjust to use secure channel if needed)
    address, _  = config.get_addr_and_cpids()
    async with grpc.aio.insecure_channel(address, config.GRPC_OPTIONS) as channel:
        try:
            stub = QueryStub(channel)
            request = StreamOrderbookUpdatesRequest(clob_pair_id=clob_pair_ids)
            async for response in stub.StreamOrderbookUpdates(request):
                response: StreamOrderbookUpdatesResponse = response
                for update in response.updates:
                    update_type = update.WhichOneof('update_message')
                    if update_type == 'orderbook_update' and update.orderbook_update.snapshot:
                        return validation_feed_handler.generate_books_from_snapshot(
                            clob_pair_ids,
                            update.orderbook_update
                        ), update.block_height
            logging.error("Stream ended")
        except grpc.aio.AioRpcError as e:
            logging.error(f"gRPC error occurred: {e.code()} - {e.details()}")
            raise e
        except Exception as e:
            logging.error(f"Unexpected error in stream: {e}")
            raise e