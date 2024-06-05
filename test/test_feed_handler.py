"""
Test the feed handler by replaying real messages from the gRPC feed.

There are two sets of recorded messages:
- `feed_from_t0.log` is the whole feed spanning t0 - t2
- `feed_from_t1.log` is the feed from t1 - t2

The test loads the snapshot from the latter file, then replays the former file
up to the snapshot, and checks that the order book state matches the snapshot.

## Recording messages

To record new sample messages, run this script with the flag `--record-to`, e.g.

    python test/test_feed_handler.py --record-to test/assets/

## Live data test

To start a long-running test that runs this test with live data, use the flag
`--long-running-test` and specify a number of messages, e.g.

    python test/test_feed_handler.py --long-running-test 100000

It will use the config in `config.yaml` to connect to the gRPC feed and record
messages to a temporary directory until seeing number specified.

It will then open a second connection, save a snapshot, and check for equivalence.
If the feed states do not match, the logs will be moved to the working directory.

Then it repeats.
"""
import asyncio
import datetime
import os
import sys
import tempfile
import unittest
from typing import BinaryIO, Optional, List, Tuple

import grpc
# Classes generated from the proto files
from v4_proto.dydxprotocol.clob.query_pb2 import StreamOrderbookUpdatesRequest
from v4_proto.dydxprotocol.clob.query_pb2 import StreamOrderbookUpdatesResponse
from v4_proto.dydxprotocol.clob.query_pb2_grpc import QueryStub

import src.book as lob
import src.config as config
from src.feed_handler import FeedHandler


class TestFeedHandler(unittest.TestCase):
    def setUp(self):
        # Load the snapshot at t1
        snap, prev_msg = load_snapshot(asset_path('feed_from_t1.log'))
        self.snapshot_state = snap

        # Load the full feed from t0 through the message preceding the snapshot
        feed_path = asset_path('feed_from_t0.log')
        start_time = datetime.datetime.now()
        feed_state, n_messages = load_feed_through_snapshot(feed_path, prev_msg)
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
        assert_books_equal(self.feed_state, self.snapshot_state)

        # Check best bid and ask prices and sizes
        best_bid: lob.Order = next(self.snapshot_state.books[0].bids())
        self.assertEqual(3032356319, best_bid.order_id.client_id)
        self.assertEqual(1146000000, best_bid.quantums)
        self.assertEqual(6759500000, best_bid.subticks)

        best_ask: lob.Order = next(self.snapshot_state.books[0].asks())
        self.assertEqual(1002750162, best_ask.order_id.client_id)
        self.assertEqual(30000000, best_ask.quantums)
        self.assertEqual(6759600000, best_ask.subticks)


def assert_books_equal(feed_state_1: FeedHandler, feed_state_2: FeedHandler):
    """
    Raise an AssertionError if the order book states of the two feed handlers
    do not match.
    """
    clob_pair_ids = set(feed_state_1.books.keys())
    clob_pair_ids.update(feed_state_2.books.keys())

    for clob_pair_id in clob_pair_ids:
        feed_asks = list(feed_state_1.books[clob_pair_id].asks())
        feed_bids = list(feed_state_1.books[clob_pair_id].bids())

        snap_asks = list(feed_state_2.books[clob_pair_id].asks())
        snap_bids = list(feed_state_2.books[clob_pair_id].bids())

        if snap_asks != feed_asks:
            raise AssertionError(f"asks for book {clob_pair_id} do not match")
        if snap_bids != feed_bids:
            raise AssertionError(f"bids for book {clob_pair_id} do not match")


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


def load_feed_through_snapshot(
        path: str,
        stop_at_msg: StreamOrderbookUpdatesResponse
) -> Tuple[FeedHandler, int]:
    """
    Load the feed from the given log file through the `stop_at_msg` (inclusive)
    and return the feed handler state + number of messages processed.
    """
    feed_state = FeedHandler()
    n_messages = 0
    with open(path, 'rb') as log:
        while (message := read_message_from_log(log)) is not None:
            feed_state.handle(message)
            n_messages += 1
            if message == stop_at_msg:
                break

    return feed_state, n_messages


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


async def record_messages(
        conf: dict,
        path: str,
        threshold_event: asyncio.Event,
        message_threshold: int,
):
    """
    Record messages from the gRPC feed to a binary log file.

    :param conf: Connection configuration
    :param path: Path to the log file
    :param threshold_event: Event to signal when message threshold is reached
    :param message_threshold: Call stop_event.set() after this many messages
    """
    host = conf['dydx_full_node']['grpc_host']
    port = conf['dydx_full_node']['grpc_port']
    clob_pair_ids = conf['stream_options']['clob_pair_ids']
    addr = f"{host}:{port}"

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
                        print(f"Recorded {n / 1000}k messages to {path}")
                    if n >= message_threshold:
                        threshold_event.set()
                print("Stream ended")
            except grpc.aio.AioRpcError as e:
                print(f"gRPC error occurred: {e.code()} - {e.details()}")
            except Exception as e:
                print(f"Unexpected error in stream: {e}")


async def connect_and_collect_overlapping(conf: dict, parent: str, n_messages: int):
    """
    1. Connect to the gRPC feed and collect `n_messages` messages.
    2. Connect a second feed and collect 1000 messages concurrently.
    3. Close both feeds.
    4. Return paths to the files log1, log2 inside the provided `dir`.
    """
    log1 = unique_path(parent, 'feed_from_t0.log')
    log2 = unique_path(parent, 'feed_from_t1.log')

    # Event to signal when the first task reaches n messages
    e = asyncio.Event()

    # Start recording the first set of messages
    print(f"Recording {n_messages//1000}k messages to {log1}")
    task1 = asyncio.create_task(record_messages(conf, log1, e, n_messages))

    await e.wait()

    # Start recording the second set of messages concurrently
    n = 1000
    print(f"Recording {n//1000}k messages to {log2}")
    e2 = asyncio.Event()
    task2 = asyncio.create_task(record_messages(conf, log2, e2, n))

    # Wait for the second task to record 1000 messages
    await e2.wait()

    # Cancel the tasks
    print("Seen enough messages, cancelling tasks...")
    task1.cancel()
    task2.cancel()
    for i, task in enumerate([task1, task2]):
        try:
            await task1
        except asyncio.CancelledError:
            print(f"task {i} cancelled")

    # Print the size of each log file
    size_log1 = os.path.getsize(log1)
    size_log2 = os.path.getsize(log2)
    print(f"Size of log1: {size_log1} bytes")
    print(f"Size of log2: {size_log2} bytes")
    return log1, log2


def unique_path(parent: str, base: str) -> str:
    """
    Return a unique path inside the given directory by appending a number to
    the base name.
    """
    n = 0
    while os.path.exists(os.path.join(parent, f"{base}.{n}")):
        n += 1
    return os.path.join(parent, f"{base}.{n}")


async def long_running_test(conf: dict, n_messages: int):
    while True:
        # Create a temporary directory
        with tempfile.TemporaryDirectory() as temp_dir:
            l1, l2 = await connect_and_collect_overlapping(conf, temp_dir, n_messages)

            # Check if the feeds line up
            try:
                snap, prev_msg = load_snapshot(l2)
                feed_state, n_messages = load_feed_through_snapshot(l1, prev_msg)
                assert_books_equal(feed_state, snap)
            except Exception as e:
                to_dir = os.getcwd()
                print(f"Identified inconsistent feed states: {e}")
                print(f"Moving logs to {to_dir}")

                to_l1 = unique_path(to_dir, os.path.basename(l1))
                to_l2 = unique_path(to_dir, os.path.basename(l2))

                os.rename(l1, to_l1)
                os.rename(l2, to_l2)

                print(f"Moved {l1} to {to_l1}")
                print(f"Moved {l2} to {to_l2}")

                raise e

            print("Feed states match")


if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == '--record-to':
        c = config.load_yaml_config('config.yaml')
        print(f"Recording messages to '{sys.argv[2]}' with conf {c}")
        os.makedirs(sys.argv[2], exist_ok=True)
        asyncio.run(connect_and_collect_overlapping(c, sys.argv[2], 100000))
    elif len(sys.argv) > 1 and sys.argv[1] == '--long-running-test':
        c = config.load_yaml_config('config.yaml')
        asyncio.run(long_running_test(c, int(sys.argv[2])))
    else:
        unittest.main()
