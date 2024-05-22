"""
Connect to a full node gRPC feed and print the top 5 asks and bids every 1000ms.
"""

import asyncio
import itertools

import grpc
from google.protobuf.json_format import MessageToJson
# Classes generated from the proto files
from v4_proto.dydxprotocol.clob.query_pb2 import StreamOrderbookUpdatesRequest
from v4_proto.dydxprotocol.clob.query_pb2_grpc import QueryStub

import src.book as lob
from src.config import GRPC_OPTIONS, CONFIG
from src.feed_handler import FeedHandler


async def listen_to_stream(channel: grpc.Channel, feed_handler: FeedHandler):
    """
    Listen to the gRPC stream of order book updates and use the `feed_handler`
    to keep track of the order book state.
    """
    try:
        # As soon as the channel is open, request the stream
        stub = QueryStub(channel)
        request = StreamOrderbookUpdatesRequest(
            clob_pair_id=CONFIG['stream_options']['clob_pair_ids']
        )

        # Pass each of the stream's messages to the feed handler
        async for response in stub.StreamOrderbookUpdates(request):
            print(f"> {MessageToJson(response, indent=None)}")
            feed_handler.handle(response)

        print("Stream ended")
    except grpc.aio.AioRpcError as e:
        print(f"gRPC error occurred: {e.code()} - {e.details()}")
    except Exception as e:
        print(f"Unexpected error in stream: {e}")


async def print_books_every_n_ms(feed_handler: FeedHandler, ms: int):
    """
    Every `ms` milliseconds, print the top asks and bids in each book.
    """
    while True:
        await asyncio.sleep(ms / 1000)
        for clob_pair_id, book in feed_handler.books.items():
            print(f"Book for CLOB pair {clob_pair_id}:")
            pretty_print_book(book)


def pretty_print_book(book: lob.LimitOrderBook):
    """
    Pretty print the top 5 ask and bid orders in the order book.
    """
    # take the top 5 asks and bids
    top_asks = list(itertools.islice(book.asks(), 5))
    top_bids = list(itertools.islice(book.bids(), 5))

    # print the top 5 asks in reverse order then the top 5 bids
    print(f"{'Price':>12} {'Qty':>12} {'Client Id':>12} {'Address':>43} Acc")
    for o in top_asks[::-1]:
        print(f"{o.subticks:>12} "
              f"{o.quantums:>12} "
              f"{o.order_id.client_id:>12} "
              f"{o.order_id.owner_address:>43} "
              f"{o.order_id.subaccount_number}")

    print(f"{'--':>12} {'--':>12}")

    for o in top_bids:
        print(f"{o.subticks:>12} "
              f"{o.quantums:>12} "
              f"{o.order_id.client_id:>12} "
              f"{o.order_id.owner_address:>43} "
              f"{o.order_id.subaccount_number}")

    print()


async def main():
    """
    Main entry point for the script. Load the configuration, connect to the
    gRPC feed, and kick off processing.
    """
    host = CONFIG['dydx_full_node']['grpc_host']
    port = CONFIG['dydx_full_node']['grpc_port']
    addr = f"{host}:{port}"

    # This manages order book state
    feed_handler = FeedHandler()

    # Connect to the gRPC feed and start listening
    # (adjust to use secure channel if needed)
    async with grpc.aio.insecure_channel(addr, GRPC_OPTIONS) as channel:
        interval = CONFIG['interval_ms']
        await asyncio.gather(
            listen_to_stream(channel, feed_handler),
            asyncio.create_task(print_books_every_n_ms(feed_handler, interval)),
        )


if __name__ == "__main__":
    print("Starting with conf:", CONFIG)
    asyncio.run(main())
