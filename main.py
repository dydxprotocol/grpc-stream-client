import asyncio
import itertools
from typing import List

import grpc
from google.protobuf.json_format import MessageToJson
# Classes generated from the proto files
from v4_proto.dydxprotocol.clob.query_pb2 import (StreamOrderbookUpdatesRequest,
                                                  StreamOrderbookUpdatesResponse)
from v4_proto.dydxprotocol.clob.query_pb2_grpc import QueryStub

import src.book as lob
from src.config import GRPC_OPTIONS, CONFIG
from src.feed_handler import FeedHandler


async def listen_to_stream(
        channel: grpc.Channel,
        buffer: List[StreamOrderbookUpdatesResponse],
):
    """
    Listen to the gRPC stream of order book updates and append them to the
    buffer until the stream ends.
    """
    try:
        stub = QueryStub(channel)
        request = StreamOrderbookUpdatesRequest(
            clob_pair_id=CONFIG['stream_options']['clob_pair_ids']
        )
        async for response in stub.StreamOrderbookUpdates(request):
            response: StreamOrderbookUpdatesResponse
            buffer.append(response)
        print("Stream ended")
    except grpc.aio.AioRpcError as e:
        print(f"gRPC error occurred: {e.code()} - {e.details()}")
    except Exception as e:
        print(f"Unexpected error in stream: {e}")


async def process_buffer_every_n_ms(
        buffer: List[StreamOrderbookUpdatesResponse],
        ms: int,
):
    """
    Every `ms` milliseconds, update the state of the order books and print the
    top asks and bids.
    """
    handler = FeedHandler()
    while True:
        await asyncio.sleep(ms / 1000)

        # Debug buffer contents
        print(f"Buffer has {len(buffer)} messages")
        for response in buffer:
            print(f"> {MessageToJson(response, indent=None)}")

        # Update state for each message
        for msg in buffer:
            handler.handle(msg)

        # Print book state
        for clob_pair_id, book in handler.books.items():
            print(f"Book for CLOB pair {clob_pair_id}:")
            pretty_print_book(book)

        buffer.clear()


def pretty_print_book(book: lob.LimitOrderBook):
    """
    Pretty print the top 5 ask and bid orders in the order book.
    """
    # take the top 5 asks and bids
    top_asks = list(itertools.islice(book.asks(), 5))
    top_bids = list(itertools.islice(book.bids(), 5))

    # print the top 5 asks in reverse order then the top 5 bids
    print(f"{'Price':>12} {'Qty':>12} {'Client Id':>12} {'Address':>43} Acc")
    for side in [top_asks[::-1], top_bids]:
        for o in side:
            print(f"{o.subticks:>12} "
                  f"{o.quantums:>12} "
                  f"{o.order_id.client_id:>12} "
                  f"{o.order_id.owner_address:>43} "
                  f"{o.order_id.subaccount_number}")
        print(f"{'--':>12} {'--':>12}")
    print()


async def main():
    """
    Main entry point for the script. Load the configuration, connect to the
    gRPC feed, and kick off processing.
    """
    host = CONFIG['dydx_full_node']['grpc_host']
    port = CONFIG['dydx_full_node']['grpc_port']
    addr = f"{host}:{port}"

    # Adjust to use secure channel if needed
    buffer: List[StreamOrderbookUpdatesResponse] = []
    async with grpc.aio.insecure_channel(addr, GRPC_OPTIONS) as channel:
        interval = CONFIG['interval_ms']
        await asyncio.gather(
            listen_to_stream(channel, buffer),
            asyncio.create_task(process_buffer_every_n_ms(buffer, interval)),
        )


if __name__ == "__main__":
    print("Starting with conf:", CONFIG)
    asyncio.run(main())
