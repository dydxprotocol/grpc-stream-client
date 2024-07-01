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
`--long-running-test` and specify a number of seconds after which to query a
snapshot, e.g.

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
from typing import Optional, List, Tuple

import grpc
from v4_proto.dydxprotocol.clob.query_pb2 import StreamOrderbookUpdatesRequest
from v4_proto.dydxprotocol.clob.query_pb2 import StreamOrderbookUpdatesResponse
from v4_proto.dydxprotocol.clob.query_pb2_grpc import QueryStub

import src.book as lob
import src.config as config
import src.serde as serde
from src.feed_handler import FeedHandler


class TestFeedHandler(unittest.TestCase):
    def setUp(self):
        # Load the snapshot at t1
        snap, prev_msg, idx = load_snapshot(asset_path('feed_from_t1.log'))
        self.assertIsNotNone(idx, "snapshot not found in feed")
        self.assertGreater(
            idx,
            0,
            "this test only makes sense if the snapshot isn't the first message"
        )
        self.snapshot_state = snap

        # Load the full feed from t0 through the message preceding the snapshot
        feed_path = asset_path('feed_from_t0.log')
        feed_state, n_messages = load_feed_through_snapshot(feed_path, prev_msg)

        self.feed_state = feed_state

    def test_replay_state(self):
        # Check that the order book state after replaying the feed up to the
        # snapshot matches the snapshot
        assert_books_equal(self.feed_state, self.snapshot_state)

        # Check best bid and ask prices and sizes
        best_bid: lob.Order = next(self.snapshot_state.books[0].bids())
        self.assertEqual(130932877, best_bid.order_id.client_id)
        self.assertEqual(500000000, best_bid.quantums)
        self.assertEqual(6037200000, best_bid.subticks)

        best_ask: lob.Order = next(self.snapshot_state.books[0].asks())
        self.assertEqual(130932874, best_ask.order_id.client_id)
        self.assertEqual(30000000, best_ask.quantums)
        self.assertEqual(6037300000, best_ask.subticks)


def asks_bids_from_feed(
        f: FeedHandler,
        clob_pair_id: int
) -> Tuple[List[lob.Order], List[lob.Order]]:
    book = f.books.get(clob_pair_id, None)
    if not book:
        return [], []
    return list(book.asks()), list(book.bids())


def assert_books_equal(feed_state_1: FeedHandler, feed_state_2: FeedHandler):
    """
    Raise an AssertionError if the order book states of the two feed handlers
    do not match.
    """
    clob_pair_ids = set(feed_state_1.books.keys())
    clob_pair_ids.update(feed_state_2.books.keys())

    for clob_pair_id in clob_pair_ids:
        feed_asks, feed_bids = asks_bids_from_feed(feed_state_1, clob_pair_id)
        snap_asks, snap_bids = asks_bids_from_feed(feed_state_2, clob_pair_id)

        if snap_asks != feed_asks:
            debug_book_side(feed_asks, snap_asks)
            raise AssertionError(f"asks for book {clob_pair_id} do not match")
        if snap_bids != feed_bids:
            debug_book_side(feed_bids, snap_bids)
            raise AssertionError(f"bids for book {clob_pair_id} do not match")


def debug_book_side(have_side: List[lob.Order], expect_side: List[lob.Order]):
    """
    Print each order book side by side for debugging.
    """
    print(f"   {'have':>38} | {'expect':>38}")
    print(f"🟠 {'px':>12} {'sz':>12} {'cid':>12} | {'px':>12} {'sz':>12} {'cid':>12}")
    i = 0
    while i < len(have_side) or i < len(expect_side):
        have = have_side[i] if i < len(have_side) else None
        expect = expect_side[i] if i < len(expect_side) else None
        status = "🟢" if have == expect else "🔴"
        print(f"{status} "
              f"{have.subticks if have else '':>12} "
              f"{have.quantums if have else '':>12} "
              f"{have.order_id.client_id if have else '':>12} | "
              f"{expect.subticks if expect else '':>12} "
              f"{expect.quantums if expect else '':>12} "
              f"{expect.order_id.client_id if expect else '':>12}")
        i += 1


def asset_path(filename: str) -> str:
    """ Full path to the test asset file. """
    return os.path.join(
        os.path.dirname(__file__),
        'assets',
        filename,
    )


def load_snapshot(path: str) -> Tuple[FeedHandler, StreamOrderbookUpdatesResponse, Optional[int]]:
    """
    Load the snapshot from the given log file and return the feed handler
    state after processing the snapshot, the message that directly preceded
    the snapshot, and the index of the first snapshot message.

    If the snapshot is never seen or never transmitted completely, returns
    None for the snapshot index.
    """
    snapshot = serde.read_all_from_log(path)
    # Use the feed handler to get the book state after the snapshot, and
    # also save the message just before the snapshot
    prev_msg = None
    snapshot_idx = None
    snapshot_state = FeedHandler()
    for idx, msg in enumerate(snapshot):
        is_snapshot = msg.updates[0].orderbook_update.snapshot

        # Store the idx of the first snapshot message
        if is_snapshot and snapshot_idx is None:
            snapshot_idx = idx

        # Stop processing once the snapshot is done processing
        if snapshot_state.has_seen_first_snapshot and not is_snapshot:
            return snapshot_state, prev_msg, snapshot_idx

        # Save the message just before the snapshot
        if not is_snapshot:
            prev_msg = msg

        snapshot_state.handle(msg)

    # If we get here without passing through the snapshot, return None for
    # snapshot idx, because the snapshot either wasn't seen or wasn't
    # complete
    return snapshot_state, prev_msg, None


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
        while (x := serde.read_message_from_log(log)) is not None:
            ts, msg = x
            feed_state.handle(msg)
            n_messages += 1
            if msg == stop_at_msg:
                break

    return feed_state, n_messages

async def record_messages(conf: dict, path: str):
    """
    Record messages from the gRPC feed to a binary log file.

    :param conf: Connection configuration
    :param path: Path to the log file
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
                    response: StreamOrderbookUpdatesResponse
                    ts = datetime.datetime.now()
                    serde.append_message_to_log(log, response, ts)
                    n += 1
                    if n % 100 == 0:
                        print(f"Recorded {n} messages to {path}")
                print("Stream ended")
            except grpc.aio.AioRpcError as e:
                print(f"gRPC error occurred: {e.code()} - {e.details()}")
            except Exception as e:
                print(f"Unexpected error in stream: {e}")


async def connect_and_collect_overlapping(conf: dict, parent: str, n_seconds: int):
    """
    1. Connect to the gRPC feed and collect messages for `n_seconds`.
    2. Connect a second feed and collect messages for 3 seconds concurrently.
    3. Close both feeds.
    4. Return paths to the files log1, log2 inside the provided `dir`.
    """
    log1 = unique_path(parent, 'feed_from_t0.log')
    log2 = unique_path(parent, 'feed_from_t1.log')

    # Start recording the first set of messages
    print(f"Recording messages to {log1} for {n_seconds} seconds")
    task1 = asyncio.create_task(record_messages(conf, log1))
    await asyncio.sleep(n_seconds)

    # Start recording the second set of messages concurrently
    print(f"Recording snapshot on a different feed")
    task2 = asyncio.create_task(record_messages(conf, log2))
    await asyncio.sleep(3)

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
    # Special case: if the base name doesn't exist, just go with that
    if not os.path.exists(os.path.join(parent, base)):
        return os.path.join(parent, base)

    n = 1
    while os.path.exists(os.path.join(parent, f"{base}.{n}")):
        n += 1
    return os.path.join(parent, f"{base}.{n}")


def replay_test(feed_from_t0_path: str, feed_from_t1_path: str):
    """
    Replay the feed from t0 up to the snapshot in t1, load the snapshot from
    t1, and check that the order book state from the t0 feed matches the
    snapshot.

    Throws an AssertionError if the order book states do not match.

    :param feed_from_t0_path: Path to feed starting at t0 ending at t2
    :param feed_from_t1_path: Path to feed starting at t1 ending < t2
    """
    snap, prev_msg, idx = load_snapshot(feed_from_t1_path)
    if idx is None:
        print("No snapshot in feed, skipping test...")
    elif idx == 0:
        print("Snapshot was first message, skipping test...")
    else:
        feed_state, n_messages = load_feed_through_snapshot(feed_from_t0_path, prev_msg)
        assert_books_equal(feed_state, snap)


async def long_running_test(conf: dict, n_seconds: int):
    while True:
        # Create a temporary directory
        with tempfile.TemporaryDirectory() as temp_dir:
            l1, l2 = await connect_and_collect_overlapping(conf, temp_dir, n_seconds)

            # Check if the feeds line up
            try:
                replay_test(l1, l2)
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
        asyncio.run(connect_and_collect_overlapping(c, sys.argv[2], 30))
    elif len(sys.argv) > 1 and sys.argv[1] == '--long-running-test':
        c = config.load_yaml_config('config.yaml')
        asyncio.run(long_running_test(c, int(sys.argv[2])))
    elif len(sys.argv) > 1 and sys.argv[1] == '--replay-from':
        replay_test(sys.argv[2], sys.argv[3])
    else:
        unittest.main()