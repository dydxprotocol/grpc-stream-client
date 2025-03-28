"""
Subscribe to the gRPC stream, await a book snapshot, then query an Indexer
book snapshot and output a comparison.
"""
import asyncio
import logging
from typing import Dict, List

import aiohttp
import grpc
from google.protobuf import json_format
from v4_proto.dydxprotocol.clob.query_pb2 import StreamOrderbookUpdatesRequest
from v4_proto.dydxprotocol.clob.query_pb2_grpc import QueryStub

import src.book as lob
import src.config as config
from src.feed_handler import StandardFeedHandler
from src.market_info import query_market_info, quantums_to_size, subticks_to_price

conf = config.Config().get_config()


async def get_book_snapshots_from_grpc(
        channel: grpc.Channel,
        clob_pair_ids: List[int]
) -> Dict[int, lob.LimitOrderBook]:
    feed_handler = StandardFeedHandler()

    try:
        stub = QueryStub(channel)
        request = StreamOrderbookUpdatesRequest(clob_pair_id=clob_pair_ids, subaccount_ids=[])
        async for response in stub.StreamOrderbookUpdates(request):
            # Update the feed state and check for snapshots
            try:
                _ = feed_handler.handle(response)
                if feed_handler.has_seen_first_snapshot:
                    return feed_handler.get_books()
            except Exception as e:
                logging.error(f"error handling message: {json_format.MessageToJson(e, indent=None)}")
                raise e
        logging.error("stream ended")
    except grpc.aio.AioRpcError as e:
        logging.error(f"gRPC error occurred: {e.code()} - {e.details()}")
        raise e
    except Exception as e:
        logging.error(f"unexpected error in stream: {e}")
        raise e

    return {}

def grpc_book_to_data(
        book: lob.LimitOrderBook,
        atomic_resolution: int,
        quantum_conversion_exponent: int,
):
    def order_to_json(order: lob.Order):
        price = subticks_to_price(order.subticks, atomic_resolution, quantum_conversion_exponent)
        size = quantums_to_size(order.quantums, atomic_resolution)
        return {
            "price": price,
            "size": size,
        }

    return {
        "asks": [order_to_json(o) for o in book.asks()],
        "bids": [order_to_json(o) for o in book.bids()],
    }

async def get_indexer_book_snapshot(session: aiohttp.ClientSession, ticker: str):
    url = f"{conf['indexer_api']}/v4/orderbooks/perpetualMarket/{ticker}"
    async with session.get(url) as response:
        response.raise_for_status()
        rjs = await response.json()
        return ticker, rjs

async def main(cpid_to_market_info: dict[int, dict]):
    cpids = conf['stream_options']['clob_pair_ids']
    grpc_addr = f"{conf['dydx_full_node']['host']}:{conf['dydx_full_node']['grpc_port']}"

    # Connect to the gRPC feed and get books
    books = None
    async with grpc.aio.insecure_channel(grpc_addr, config.GRPC_OPTIONS) as channel:
        logging.info("opened grpc channel, awaiting book snapshots...")
        books = await get_book_snapshots_from_grpc(channel, cpids)
        logging.info("got book snapshots")

    logging.info("getting indexer snapshots...")
    indexer_books = None
    async with aiohttp.ClientSession() as session:
        indexer_resps = await asyncio.gather(
            *[get_indexer_book_snapshot(session, cpid_to_market_info[cpid]['ticker']) for cpid in cpids]
        )
        indexer_books = {ticker: book for ticker, book in indexer_resps}

    logging.info("got indexer snapshots")

    for cpid, book in books.items():
        atomic_resolution = cpid_to_market_info[cpid]['atomicResolution']
        qce = cpid_to_market_info[cpid]['quantumConversionExponent']

        ticker = cpid_to_market_info[cpid]['ticker']
        print(f"gRPC book for {ticker}:")
        print(grpc_book_to_data(book, atomic_resolution, qce))

        print(f"Indexer book for {ticker}:")
        print(indexer_books[ticker])

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
    )

    id_to_info = query_market_info(conf['indexer_api'])
    logging.info(f"got market info from indexer, opening grpc channel...")

    asyncio.run(main(id_to_info))
