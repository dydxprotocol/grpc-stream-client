#!/usr/bin/env -S UV_PROJECT_ENVIRONMENT=.venv uv run
# /// script
# requires-python = ">=3.9"
# dependencies = [
#    "grpcio>=1.67.0",
#    "grpcio-tools==1.64.1",
#    "protobuf==5.28.1",
#    "pyyaml==6.0.1",
#    "requests~=2.32.2",
#    "sortedcontainers==2.4.0",
#    "v4-proto==6.0.8",
#    "websockets==12.0",
# ]
# ///
"""
This script reads a JSON or protobuf log file with feed messages and writes out
an unnested CSV with concise message lines that can be more easily read in
Excel.

Usage:
    python feed_to_csv.py [json|proto] <in_file>
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Optional
import argparse
import csv
import datetime
import sys

from google.protobuf import json_format

from grpc_stream_client.serde import read_message_from_log
from v4_proto.dydxprotocol.clob.query_pb2 import (
    StreamOrderbookUpdatesResponse,
    StreamOrderbookUpdate,
    StreamOrderbookFill,
)
from v4_proto.dydxprotocol.indexer.off_chain_updates.off_chain_updates_pb2 import (
    OrderPlaceV1,
    OrderUpdateV1,
    OrderRemoveV1,
    OffChainUpdateV1,
)
from v4_proto.dydxprotocol.indexer.protocol.v1.clob_pb2 import IndexerOrder
import grpc_stream_client.fills as fills

TIFS = {
    IndexerOrder.TIME_IN_FORCE_UNSPECIFIED: "GTT",
    IndexerOrder.TIME_IN_FORCE_IOC: "IOC",
    IndexerOrder.TIME_IN_FORCE_POST_ONLY: "PO",
    IndexerOrder.TIME_IN_FORCE_FILL_OR_KILL: "FOK",
}


FILL_TYPES = {
    fills.FillType.NORMAL: "MatchOrders",
    fills.FillType.LIQUIDATION: "MatchPerpetualLiquidation",
    fills.FillType.DELEVERAGING: "MatchPerpetualDeleveraging",
}


@dataclass
class Event:
    """
    A line in the CSV output format. Doesn't include all fields, but has enough
    to be useful for most purposes + still be readable in Excel.
    """

    ts: str  # Timestamp of message receipt
    height: int  # Block height of the message
    mode: int  # Execution mode of the message
    batch: str  # "TRUE" if the message is part of a batch with the previous one
    snap: str  # "TRUE" if the message is a snapshot

    msg: str  # The type of message
    clob_pair_id: int  # The clob pair ID of the message
    px: Optional[int]  # The price of the message
    sz: Optional[int]  # The size of the message

    cid: str  # The client ID of the message
    uid: str  # The address/subaccount that sent the message

    t_cid: str  # The taker client ID
    t_uid: str  # The taker address/subaccount

    sd: str  # The side of the message
    gtb: str  # The good-till-block of the message
    tif: str  # The time-in-force of the message


def json_log_to_csv(in_path: Path):
    """
    Read in a JSON log file, generate protobuf messages, and write out a CSV
    file where each line is an `Event`.
    """
    with open(in_path, "r") as in_file:
        writer = csv.writer(sys.stdout)
        writer.writerow(Event.__dataclass_fields__.keys())
        for line in in_file:
            message = StreamOrderbookUpdatesResponse()
            json_format.Parse(line, message)
            for event in feed_msg_to_events(message, None):
                writer.writerow([getattr(event, field) for field in Event.__dataclass_fields__.keys()])


def proto_log_to_csv(in_path: Path):
    """
    Read in a protobuf log file and write out a CSV file where each line is an
    `Event`.
    """
    with open(in_path, "rb") as log:
        writer = csv.writer(sys.stdout)
        writer.writerow(Event.__dataclass_fields__.keys())
        while (message := read_message_from_log(log)) is not None:
            for event in feed_msg_to_events(message[1], message[0]):
                writer.writerow([getattr(event, field) for field in Event.__dataclass_fields__.keys()])


def feed_msg_to_events(message: StreamOrderbookUpdatesResponse, ts: Optional[datetime.datetime | str]) -> list[Event]:
    """Parse a message from the gRPC feed into a list of event line items"""
    if ts is None:
        ts_str = datetime.datetime.now().isoformat()
    elif isinstance(ts, datetime.datetime):
        ts_str = ts.isoformat()
    es = []
    for update in message.updates:
        height = update.block_height
        mode = update.exec_mode

        update_type = update.WhichOneof("update_message")
        if update_type == "orderbook_update":
            for x in parse_orderbook_updates(ts_str, height, mode, update.orderbook_update):
                es.append(x)
        elif update_type == "order_fill":
            for x in parse_order_fills(ts_str, height, mode, update.order_fill):
                es.append(x)
        else:
            raise ValueError(f"Unknown update type '{update_type}' in: {update}")
    return es


def parse_orderbook_updates(ts: str, height: int, mode: int, update: StreamOrderbookUpdate):
    u: OffChainUpdateV1
    for i, u in enumerate(update.updates):
        update_type = u.WhichOneof("update_message")
        if update_type == "order_place":
            place: OrderPlaceV1 = u.order_place
            yield Event(
                ts=ts,
                height=height,
                mode=mode,
                batch="" if i == 0 else "TRUE",
                snap="TRUE" if update.snapshot else "",
                msg=str(u.order_place.__class__.__name__),
                clob_pair_id=place.order.order_id.clob_pair_id,
                px=place.order.subticks,
                sz=place.order.quantums,
                cid=str(place.order.order_id.client_id),
                uid=f"{place.order.order_id.subaccount_id.owner}/{place.order.order_id.subaccount_id.number}",
                t_cid="",
                t_uid="",
                sd="BUY" if place.order.side == IndexerOrder.SIDE_BUY else "SELL",
                gtb=str(place.order.good_til_block),
                tif=TIFS.get(place.order.time_in_force, ""),
            )
        elif update_type == "order_update":
            ud: OrderUpdateV1 = u.order_update
            yield Event(
                ts=ts,
                height=height,
                mode=mode,
                batch="" if i == 0 else "TRUE",
                snap="TRUE" if update.snapshot else "",
                msg=str(u.order_update.__class__.__name__),
                clob_pair_id=ud.order_id.clob_pair_id,
                px=None,
                sz=ud.total_filled_quantums if ud.total_filled_quantums > 0 else None,
                cid=str(ud.order_id.client_id),
                uid=f"{ud.order_id.subaccount_id.owner}/{ud.order_id.subaccount_id.number}",
                t_cid="",
                t_uid="",
                sd="",
                gtb="",
                tif="",
            )
        elif update_type == "order_remove":
            ur: OrderRemoveV1 = u.order_remove
            yield Event(
                ts=ts,
                height=height,
                mode=mode,
                batch="" if i == 0 else "TRUE",
                snap="TRUE" if update.snapshot else "",
                msg=str(u.order_remove.__class__.__name__),
                clob_pair_id=ur.removed_order_id.clob_pair_id,
                px=None,
                sz=None,
                cid=str(ur.removed_order_id.client_id),
                uid=f"{ur.removed_order_id.subaccount_id.owner}/{ur.removed_order_id.subaccount_id.number}",
                t_cid="",
                t_uid="",
                sd="",
                gtb="",
                tif="",
            )
        else:
            raise ValueError(f"Unknown update type '{update_type}' in: {u}")


def parse_order_fills(ts, height, mode, order_fill: StreamOrderbookFill):
    events = []
    fill_events = fills.parse_fill(order_fill, mode)
    for i, fe in enumerate(fill_events):
        events.append(
            Event(
                ts=ts,
                height=height,
                mode=mode,
                batch="" if i == 0 else "TRUE",
                snap="",
                msg=FILL_TYPES[fe.fill_type],
                clob_pair_id=fe.clob_pair_id,
                px=fe.subticks,
                sz=fe.quantums,
                cid=str(fe.maker.client_id) if fe.maker.client_id > 0 else "",
                uid=f"{fe.maker.owner_address}/{fe.maker.subaccount_number}",
                t_cid=str(fe.taker.client_id) if fe.taker.client_id > 0 else "",
                t_uid=f"{fe.taker.owner_address}/{fe.taker.subaccount_number}",
                sd="BUY" if fe.taker_is_buy else "SELL",
                gtb="",
                tif="",
            )
        )

    return events


class Args(argparse.Namespace):
    input_filename: Path
    format: Literal["json", "proto"]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Converts a JSON or protobuf log file to an un-nested CSV on stdout",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "input_filename",
        type=Path,
        help="input filename",
    )
    parser.add_argument(
        "--format",
        choices=["json", "proto"],
        default="json",
        help="output to JSON or protobuf log",
    )
    args = parser.parse_args(namespace=Args)

    if args.format == "json":
        json_log_to_csv(args.input_filename)
    elif args.format == "proto":
        proto_log_to_csv(args.input_filename)
