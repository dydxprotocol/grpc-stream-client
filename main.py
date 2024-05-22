"""
Connect to a full node gRPC feed and print the top 5 asks and bids every 1000ms.
"""
import asyncio
import itertools
from typing import List

import grpc
from google.protobuf.json_format import MessageToJson
# Classes generated from the proto files
from v4_proto.dydxprotocol.clob.query_pb2 import StreamOrderbookUpdatesRequest
from v4_proto.dydxprotocol.clob.query_pb2_grpc import QueryStub

import src.book as lob
import src.config as config
from src.feed_handler import FeedHandler
from src.market_info import query_market_info, quantums_to_size, subticks_to_price


async def listen_to_stream(
        channel: grpc.Channel,
        clob_pair_ids: List[int],
        feed_handler: FeedHandler,
):
    """
    Subscribe to the gRPC stream of order book updates and use the
    `feed_handler` to keep track of the order book state.
    """
    try:
        stub = QueryStub(channel)
        request = StreamOrderbookUpdatesRequest(clob_pair_id=clob_pair_ids)
        async for response in stub.StreamOrderbookUpdates(request):
            print(f"> {MessageToJson(response, indent=None)}")
            feed_handler.handle(response)
        print("Stream ended")
    except grpc.aio.AioRpcError as e:
        print(f"gRPC error occurred: {e.code()} - {e.details()}")
        raise e
    except Exception as e:
        print(f"Unexpected error in stream: {e}")
        raise e


async def print_books_every_n_ms(
        feed_handler: FeedHandler,
        cpid_to_market_info: dict[int, dict],
        ms: int,
):
    """
    Every `ms` milliseconds, print the top asks and bids in each book.
    """
    while True:
        await asyncio.sleep(ms / 1000)
        for clob_pair_id, book in feed_handler.books.items():
            info = cpid_to_market_info[clob_pair_id]
            print(f"Book for CLOB pair {clob_pair_id} ({info['ticker']}):")
            pretty_print_book(
                book,
                info['atomicResolution'],
                info['quantumConversionExponent'],
            )


def pretty_print_book(
        book: lob.LimitOrderBook,
        atomic_resolution: int,
        quantum_conversion_exponent: int,
):
    """
    Pretty print the top 5 ask and bid orders in the order book, converting
    integer fields to human-readable decimals.
    """
    # take the top 5 asks and bids
    top_asks = list(itertools.islice(book.asks(), 5))
    top_bids = list(itertools.islice(book.bids(), 5))

    # print the top 5 asks in reverse order then the top 5 bids
    print(f"{'Price':>12} {'Qty':>12} {'Client Id':>12} {'Address':>43} Acc")
    for o in top_asks[::-1]:
        price = subticks_to_price(o.subticks, atomic_resolution, quantum_conversion_exponent)
        size = quantums_to_size(o.quantums, atomic_resolution)
        print(f"{price:>12f} "
              f"{size:>12f} "
              f"{o.order_id.client_id:>12} "
              f"{o.order_id.owner_address:>43} "
              f"{o.order_id.subaccount_number}")

    print(f"{'--':>12} {'--':>12}")

    for o in top_bids:
        price = subticks_to_price(o.subticks, atomic_resolution, quantum_conversion_exponent)
        size = quantums_to_size(o.quantums, atomic_resolution)
        print(f"{price:>12f} "
              f"{size:>12f} "
              f"{o.order_id.client_id:>12} "
              f"{o.order_id.owner_address:>43} "
              f"{o.order_id.subaccount_number}")

    print()


async def main(conf: dict, cpid_to_market_info: dict[int, dict]):
    host = conf['dydx_full_node']['grpc_host']
    port = conf['dydx_full_node']['grpc_port']
    cpids = conf['stream_options']['clob_pair_ids']
    addr = f"{host}:{port}"

    # This manages order book state
    feed_handler = FeedHandler()

    # Connect to the gRPC feed and start listening
    # (adjust to use secure channel if needed)
    async with grpc.aio.insecure_channel(addr, config.GRPC_OPTIONS) as channel:
        interval = conf['interval_ms']
        print_books_task = asyncio.create_task(
            print_books_every_n_ms(
                feed_handler,
                cpid_to_market_info,
                interval,
            ),
        )
        await asyncio.gather(
            listen_to_stream(channel, cpids, feed_handler),
            print_books_task,
        )


if __name__ == "__main__":
    c = config.load_yaml_config("config.yaml")
    print("Starting with conf:", c)
    id_to_info = query_market_info(c['indexer_api'])
    print("Got market info: ", id_to_info)
    asyncio.run(main(c, id_to_info))
