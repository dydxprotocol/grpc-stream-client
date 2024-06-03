"""
Test the feed handler by replaying real messages from the gRPC feed.

There are two sets of recorded messages:
- `feed_from_t0.log` is the whole feed spanning t0 - t2
- `feed_from_t1.log` is the feed from t1 - t2

The test loads the snapshot from the latter file, then replays the former file
up to the snapshot, and checks that the order book state matches the snapshot.

To record new messages, run this script with the flag `--record-to path.log`.
"""
import asyncio
import datetime
import os
import sys
import unittest
from typing import BinaryIO, Optional, List, Tuple

import grpc
# Classes generated from the proto files
from v4_proto.dydxprotocol.clob.query_pb2 import StreamOrderbookUpdatesRequest
from v4_proto.dydxprotocol.clob.query_pb2 import StreamOrderbookUpdatesResponse
from v4_proto.dydxprotocol.clob.query_pb2_grpc import QueryStub

import src.config as config
from src.feed_handler import FeedHandler
import src.book as lob


class TestFeedHandler(unittest.TestCase):
    def setUp(self):
        # Load the snapshot at t1
        snap, prev_msg = load_snapshot(asset_path('feed_from_t1.log'))
        self.snapshot_state = snap

        # Load the full feed from t0 through the message preceding the snapshot
        feed_path = asset_path('feed_from_t0.log')
        feed_state = FeedHandler()
        start_time = datetime.datetime.now()
        n_messages = 0
        with open(feed_path, 'rb') as log:
            while (message := read_message_from_log(log)) is not None:
                feed_state.handle(message)
                n_messages += 1
                if message == prev_msg:
                    break
        end_time = datetime.datetime.now()

        # Compute the time taken to process the messages
        time_taken = end_time - start_time
        print(f"Read, deserialized, and handled {n_messages} msgs "
              f"in {time_taken.total_seconds()}s "
              f"({n_messages / time_taken.total_seconds() // 1000}k msgs/s)")

        self.feed_state = feed_state

    def test_replay_state(self):
        # Check that the order book state after replaying the feed up to the
        # snapshot matches the snapshot
        for clob_pair_id in self.snapshot_state.books:
            feed_asks = list(self.feed_state.books[clob_pair_id].asks())
            feed_bids = list(self.feed_state.books[clob_pair_id].bids())

            snap_asks = list(self.snapshot_state.books[clob_pair_id].asks())
            snap_bids = list(self.snapshot_state.books[clob_pair_id].bids())

            self.assertEqual(snap_asks, feed_asks, f"asks for book {clob_pair_id}")
            self.assertEqual(snap_bids, feed_bids, f"bids for book {clob_pair_id}")

        # Check best bid and ask prices and sizes
        best_bid: lob.Order = next(self.snapshot_state.books[0].bids())
        self.assertEqual(3032356319, best_bid.order_id.client_id)
        self.assertEqual(1146000000, best_bid.quantums)
        self.assertEqual(6759500000, best_bid.subticks)

        best_ask: lob.Order = next(self.snapshot_state.books[0].asks())
        self.assertEqual(1002750162, best_ask.order_id.client_id)
        self.assertEqual(30000000, best_ask.quantums)
        self.assertEqual(6759600000, best_ask.subticks)


def asset_path(filename: str) -> str:
    """ Full path to the test asset file. """
    return os.path.join(
        os.path.dirname(__file__),
        'assets',
        filename,
    )


def load_snapshot(path: str) -> Tuple[FeedHandler, StreamOrderbookUpdatesResponse]:
    """
    Load the snapshot from the given log file and return the feed handler
    state after processing the snapshot + the message that directly preceded
    the snapshot.
    """
    snapshot = read_all_from_log(path)

    # Use the feed handler to get the book state after the snapshot, and
    # also save the message just before the snapshot
    prev_msg = None
    snapshot_state = FeedHandler()
    for msg in snapshot:
        # Stop processing once the snapshot is seen
        is_snapshot = msg.updates[0].orderbook_update.snapshot
        if snapshot_state.has_seen_first_snapshot and not is_snapshot:
            break

        # Save the message just before the snapshot
        if not is_snapshot:
            prev_msg = msg

        snapshot_state.handle(msg)

    return snapshot_state, prev_msg


def append_message_to_log(log: BinaryIO, message: StreamOrderbookUpdatesResponse):
    """
    Binary serialize the message and append it to the log file, prefixed by the
    message length so that it can be read back in.
    """
    serialized_message = message.SerializeToString()
    message_length = len(serialized_message)
    log.write(message_length.to_bytes(4, byteorder='big'))
    log.write(serialized_message)


def read_message_from_log(log: BinaryIO) -> Optional[StreamOrderbookUpdatesResponse]:
    """
    Read a message from the log file, deserializing it from the binary format,
    returning None if the end of the file is reached.
    """
    length_bytes = log.read(4)
    if not length_bytes:
        return None

    message_length = int.from_bytes(length_bytes, byteorder='big')
    serialized_message = log.read(message_length)
    message = StreamOrderbookUpdatesResponse()
    message.ParseFromString(serialized_message)
    return message


def read_all_from_log(path: str) -> List[StreamOrderbookUpdatesResponse]:
    """
    Read all messages from the log file and return them in a list.
    """
    msgs = []
    with open(path, 'rb') as log:
        while (message := read_message_from_log(log)) is not None:
            msgs.append(message)
    return msgs


async def record_messages(conf: dict, path: str):
    host = conf['dydx_full_node']['grpc_host']
    port = conf['dydx_full_node']['grpc_port']
    clob_pair_ids = conf['stream_options']['clob_pair_ids']
    addr = f"{host}:{port}"

    # Connect to the gRPC feed and start listening
    with open(path, 'wb') as log:
        n = 0
        async with grpc.aio.insecure_channel(addr, config.GRPC_OPTIONS) as channel:
            try:
                stub = QueryStub(channel)
                request = StreamOrderbookUpdatesRequest(clob_pair_id=clob_pair_ids)
                async for response in stub.StreamOrderbookUpdates(request):
                    append_message_to_log(log, response)
                    n += 1
                    if n % 1000 == 0:
                        print(f"Recorded {n / 1000}k messages")
                print("Stream ended")
            except grpc.aio.AioRpcError as e:
                print(f"gRPC error occurred: {e.code()} - {e.details()}")
            except Exception as e:
                print(f"Unexpected error in stream: {e}")


if __name__ == '__main__':
    # Parse command line args --record-to path
    # If --record-to is given, record messages to the given file
    # Otherwise, run the tests
    if len(sys.argv) > 1 and sys.argv[1] == '--record-to':
        c = config.load_yaml_config('config.yaml')
        print(f"Recording messages to '{sys.argv[2]}' with conf {c}")
        asyncio.run(record_messages(c, sys.argv[2]))
    else:
        unittest.main()
