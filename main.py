"""
Connect to a full node gRPC feed and print the top 5 asks and bids periodically,
along with trades whenever they occur.

Logs all messages in JSON format to the path specified in config.yaml.
"""
import argparse
import asyncio
import itertools
import logging
import websockets
from typing import Dict, List

import grpc
from google.protobuf import json_format

# Classes generated from the proto files
from v4_proto.dydxprotocol.clob.query_pb2 import StreamOrderbookUpdatesRequest, \
    StreamOrderbookUpdatesResponse
from v4_proto.dydxprotocol.clob.query_pb2_grpc import QueryStub
from v4_proto.dydxprotocol.subaccounts.subaccount_pb2 import SubaccountId

import src.book as lob
import src.config as config
import src.fills as fills
import src.subaccounts as subaccounts
from src.feed_handler import FeedHandler, StandardFeedHandler
from src.validation_feed_handler import ValidationFeedHandler
from src.market_info import query_market_info, quantums_to_size, subticks_to_price

conf = config.Config().get_config()


async def listen_to_grpc_stream(
        channel: grpc.Channel,
        clob_pair_ids: List[int],
        subaccount_ids: List[str],
        cpid_to_market_info: dict[int, dict],
        feed_handler: FeedHandler,
        log_path: str,
):
    """
    Subscribe to the gRPC stream of order book updates and use the
    `feed_handler` to keep track of the order book state. Print any
    fills that occur.
    """
    logging.info("Starting to listen to the grpc stream")

    try:
        stub = QueryStub(channel)
        # parse subaccount ids, each subaccount id is string with format owner/number
        subaccount_protos = [SubaccountId(owner=sa.split('/')[0], number=int(sa.split('/')[1])) for sa in subaccount_ids]
        request = StreamOrderbookUpdatesRequest(clob_pair_id=clob_pair_ids, subaccount_ids=subaccount_protos)
        with open(log_path, 'w') as log:
            async for response in stub.StreamOrderbookUpdates(request):
                # Log the message
                log.write(json_format.MessageToJson(response, indent=None) + '\n')
                # Update the order book state and print any fills
                try:
                    fill_events = feed_handler.handle(response)
                    if conf['print_fills']:
                        print_fills(fill_events, cpid_to_market_info)
                    if conf['print_subaccounts']:
                        print_subaccounts(feed_handler.get_recent_subaccount_updates())
                except Exception as e:
                    logging.error(f"Error handling message: {json_format.MessageToJson(e, indent=None)}")
                    raise e
        logging.error("Stream ended")
    except grpc.aio.AioRpcError as e:
        logging.error(f"gRPC error occurred: {e.code()} - {e.details()}")
        raise e
    except Exception as e:
        logging.error(f"Unexpected error in stream: {e}")
        raise e

async def listen_to_websocket(
        websocket: websockets.WebSocketClientProtocol,
        cpid_to_market_info: dict[int, dict],
        feed_handler: FeedHandler,
        log_path: str,
):
    """
    Subscribe to the websocket of order book updates and use the
    `feed_handler` to keep track of the order book state. Print any
    fills that occur.
    """
    logging.info("Starting to listen to the websocket")

    try:
        with open(log_path, 'w') as log:
            async for message in websocket:
                # Parse the incoming data into a protobuf object
                response = StreamOrderbookUpdatesResponse()
                json_format.Parse(message, response)
                # Log the message
                log.write(str(message) + '\n')
                # Update the order book state and print any fills
                try:
                    fill_events = feed_handler.handle(response)
                    if conf['print_fills']:
                        print_fills(fill_events, cpid_to_market_info)
                    if conf['print_subaccounts']:
                        print_subaccounts(feed_handler.get_recent_subaccount_updates())
                except Exception as e:
                    logging.error(f"Error handling message: {str(response)}")
                    raise e
        logging.error("websocket stream ended")
    except websockets.exceptions.WebSocketException as e:
        logging.error(f"websocket stream error occurred: {e}")
        raise e
    except Exception as e:
        logging.error(f"Unexpected error in websocket stream: {e}")
        raise e


def print_fills(
        fill_events: List[fills.Fill],
        cpid_to_market_info: dict[int, dict],
):
    """
    Print the fills that occurred in the last message.
    """
    for fill in fill_events:
        info = cpid_to_market_info[fill.clob_pair_id]
        ar = info['atomicResolution']
        qce = info['quantumConversionExponent']

        logging.info(" ".join([
            # Exec mode 7 is for fills finalized by consensus
            '(optimistic)' if fill.exec_mode != 7 else '(finalized)',
            str(fill.fill_type),
            'buy' if fill.taker_is_buy else 'sell',
            str(quantums_to_size(fill.quantums, ar)),
            '@',
            str(subticks_to_price(fill.subticks, ar, qce)),
            f'taker={fill.taker}',
            f'maker={fill.maker}',
        ]))


def print_subaccounts(subaccounts_dict: Dict[subaccounts.SubaccountId, subaccounts.StreamSubaccount]):
    """
    Print the subaccounts in a human-readable way.
    """
    for subaccount_id, subaccount in subaccounts_dict.items():
        perpetual_positions_str = ", ".join(
            [f"Perpetual ID: {perp_id}, Quantums: {perp_position.quantums}" for perp_id, perp_position in
             subaccount.perpetual_positions.items()]
        )
        asset_positions_str = ", ".join(
            [f"Asset ID: {asset_id}, Quantums: {asset_position.quantums}" for asset_id, asset_position in
             subaccount.asset_positions.items()]
        )

        logging.info(" | ".join([
            f"Subaccount ID: {subaccount_id.owner_address}/{subaccount_id.subaccount_number}",
            f"Perpetual Positions: {perpetual_positions_str}",
            f"Asset Positions: {asset_positions_str}"
        ]))


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
        for clob_pair_id, book in feed_handler.get_books().items():
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


async def main(args: dict, cpid_to_market_info: dict[int, dict]):
    host = conf['dydx_full_node']['host']
    cpids = conf['stream_options']['clob_pair_ids']
    subaccount_ids = conf['stream_options']['subaccount_ids']

    # This manages order book state
    feed_handler: FeedHandler = StandardFeedHandler()
    if args['validation_mode']:
        logging.info("Starting Full Node Streaming Client in Validation Mode")
        feed_handler = ValidationFeedHandler()

    if conf['use_grpc']:
        grpc_port = conf['dydx_full_node']['grpc_port']
        grpc_addr = f"{host}:{grpc_port}"
        # Connect to the gRPC feed and start listening
        # (adjust to use secure channel if needed)
        async with grpc.aio.insecure_channel(grpc_addr, config.GRPC_OPTIONS) as channel:
            interval = conf['interval_ms']
            tasks = [
                listen_to_grpc_stream(
                    channel,
                    cpids,
                    subaccount_ids,
                    cpid_to_market_info,
                    feed_handler,
                    conf['log_stream_messages'],
                ),
            ]
            if conf['print_books']:
                print_books_task = asyncio.create_task(
                    print_books_every_n_ms(
                        feed_handler,
                        cpid_to_market_info,
                        interval,
                    ),
                )
                tasks.append(print_books_task)

            await asyncio.gather(
                *tasks
            )
    elif conf['use_websocket']:
        params = []
        if cpids:
            joined_cpids = ",".join([str(x) for x in cpids])
            params.append(f"clobPairIds={joined_cpids}")

        if subaccount_ids:
            joined_subaccount_ids = ",".join([str(x) for x in subaccount_ids])
            params.append(f"subaccountIds={joined_subaccount_ids}")
        websocket_port = conf['dydx_full_node']['websocket_port']
        params_str = "&".join(params)
        websocket_addr = f"ws://{host}:{websocket_port}/ws?{params_str}"
        # Connect to the websocket and start listening
        async with websockets.connect(websocket_addr, ping_interval=None, max_size=8_388_608) as websocket:
            interval = conf['interval_ms']
            tasks = [
                listen_to_websocket(
                    websocket,
                    cpid_to_market_info,
                    feed_handler,
                    conf['log_stream_messages'],
                ),
            ]
            if conf['print_books']:
                print_books_task = asyncio.create_task(
                    print_books_every_n_ms(
                        feed_handler,
                        cpid_to_market_info,
                        interval,
                    ),
                )
                tasks.append(print_books_task)

            await asyncio.gather(
                *tasks
            )

    else:
        logging.error("Must specify use_grpc or use_websocket in config.yaml")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
    )

    parser = argparse.ArgumentParser(description='Sample GRPC Client for Full Node Streaming')
    parser.add_argument(
        '--validation-mode',
        action='store_true',
        help='when supplied, client will be started in validation mode'
    )
    args = parser.parse_args()

    logging.info(f"Starting with conf: {conf}")

    id_to_info = query_market_info(conf['indexer_api'])
    logging.info(f"Got market info: {id_to_info}")

    asyncio.run(main(vars(args), id_to_info))
