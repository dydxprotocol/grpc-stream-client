"""Limit order book data structure (l3) for orders with dYdX protocol order IDs."""

from __future__ import annotations
from dataclasses import dataclass
from typing import Generic, Iterator, Optional, TypeVar
import logging

# TODO deprecate sortedcontainers as maintainer refuses to collaborate on typing
# https://github.com/grantjenks/python-sortedcontainers/pull/107
from sortedcontainers import SortedDict  # type: ignore


@dataclass(frozen=True)
class OrderId:
    owner_address: str  # order owner address
    subaccount_number: int  # owner subaccount index
    client_id: int  # client identifier for the order (unique at any given time, but can be reused)
    order_flags: int  # conditional, short term, or long term order


@dataclass
class Order:
    order_id: OrderId
    is_bid: bool
    original_quantums: int  # size originally placed (needs conversion to decimal)
    quantums: int  # size remaining in the book (needs conversion to decimal)
    subticks: int  # price (needs conversion to decimal)


class LimitOrderBook:
    def __init__(self):
        self.oid_to_order_node: dict[OrderId, DoublyLinkedListNode] = {}
        self._asks: SortedDict[int, DoublyLinkedList] = SortedDict()
        # reverse order (highest first)
        self._bids: SortedDict[int, DoublyLinkedList] = SortedDict(lambda x: -x)

    @staticmethod
    def _get_or_create_level(subticks: int, book_side: SortedDict) -> "DoublyLinkedList":
        """Get or create a level in the book side"""
        if subticks not in book_side:
            book_side[subticks] = DoublyLinkedList()
        return book_side[subticks]

    def add_order(self, order: Order) -> Order:
        """Add an order to the book, to the end of the queue on its level"""
        # find the side / level the order belongs to
        book_side = self._bids if order.is_bid else self._asks
        level = self._get_or_create_level(order.subticks, book_side)

        # add the order to the level and index the node
        order_node = level.append(order)
        self.oid_to_order_node[order.order_id] = order_node
        return order

    def remove_order(self, oid: OrderId) -> Order:
        """
        Remove an order from the book by its order ID and return it.
        """
        order_node = self.oid_to_order_node.pop(oid)
        order = order_node.data

        book_side = self._bids if order.is_bid else self._asks
        level: DoublyLinkedList = book_side[order.subticks]
        level.remove(order_node)
        if level.head is None:
            del book_side[order.subticks]

        return order

    def get_order(self, oid: OrderId) -> Optional[Order]:
        """
        Get an order from the book by its order ID.
        """
        node = self.oid_to_order_node.get(oid)
        if node is None:
            return None
        return node.data

    def asks(self) -> Iterator[Order]:
        """
        Iterate over the asks in ascending order.
        """
        for price, level in self._asks.items():
            for order in level:
                yield order

    def bids(self) -> Iterator[Order]:
        """
        Iterate over the bids in descending order.
        """
        for price, level in self._bids.items():
            for order in level:
                yield order

    def get_midpoint_price(self) -> int:
        """
        Get the midpoint price
        """
        highest_bid = self._bids.peekitem(0)
        lowest_ask = self._asks.peekitem(0)
        return (highest_bid[0] + lowest_ask[0]) / 2.0

    def compare_books(self, other: LimitOrderBook) -> bool:
        """
        Compares the two orderbooks. Prints out any inconsistencies.
        Returns a boolean True if the books are the same.
        """
        failed = False
        num_orders = len(self.oid_to_order_node)
        num_asks = len(self._asks)
        num_bids = len(self._bids)

        other_num_orders = len(other.oid_to_order_node)
        other_num_asks = len(other._asks)
        other_num_bids = len(other._bids)

        # If order book is empty, return true vacuously.
        if num_orders == other_num_orders == 0:
            return True

        if num_orders != other_num_orders:
            logging.error(f"FAIL: num orders: {num_orders} {other_num_orders}")
            failed = True
        for oid in self.oid_to_order_node:
            if oid not in other.oid_to_order_node:
                logging.error(f"FAIL: order: {oid} does not exist in other book")
                failed = True
        if num_asks != other_num_asks:
            logging.error(f"FAIL: num asks: {num_asks} {other_num_asks}")
            failed = True
        if num_bids != other_num_bids:
            logging.error(f"FAIL: num bids: {num_bids} {other_num_bids}")
            failed = True
        for level, dll in self._asks.items():
            other_dll = other._asks[level]
            if not dll.is_equal(other_dll):
                logging.error(f"FAIL: Asks Price level {level} mismatching orders")
                failed = True
        for level, dll in self._bids.items():
            other_dll = other._bids[level]
            if not dll.is_equal(other_dll):
                logging.error(f"FAIL: Bids Price level {level} mismatching orders")
                failed = True

        # Only compare mid price if both sides have orders.
        if num_asks > 0 and num_bids > 0:
            midpoint_price = self.get_midpoint_price()
            other_midpoint_price = other.get_midpoint_price()
            if midpoint_price != other_midpoint_price:
                logging.error(f"FAIL: midpoint price: {midpoint_price} {other_midpoint_price}")
                failed = True

        return not failed


def asks_bids_from_book(
    book: LimitOrderBook,
) -> tuple[list[Order], list[Order]]:
    return list(book.asks()), list(book.bids())


def assert_books_equal(book_1: LimitOrderBook, book_2: LimitOrderBook):
    """
    Raise an AssertionError if the order book states of the two feed handlers
    do not match.
    """

    feed_asks, feed_bids = asks_bids_from_book(book_1)
    snap_asks, snap_bids = asks_bids_from_book(book_2)

    if snap_asks != feed_asks:
        print_book_side(feed_asks, snap_asks)
        raise AssertionError("asks for book do not match")
    if snap_bids != feed_bids:
        print_book_side(feed_bids, snap_bids)
        raise AssertionError("bids for book do not match")


def print_book_side(have_side: list[Order], expect_side: list[Order]):
    print(f"   {'have':>38} | {'expect':>38}")
    print(f"🟠 {'px':>12} {'sz':>12} {'cid':>12} | {'px':>12} {'sz':>12} {'cid':>12}")
    i = 0
    while i < len(have_side) or i < len(expect_side):
        have = have_side[i] if i < len(have_side) else None
        expect = expect_side[i] if i < len(expect_side) else None
        status = "🟢" if have == expect else "🔴"
        print(
            f"{status} "
            f"{have.subticks if have else '':>12} "
            f"{have.quantums if have else '':>12} "
            f"{have.order_id.client_id if have else '':>12} | "
            f"{expect.subticks if expect else '':>12} "
            f"{expect.quantums if expect else '':>12} "
            f"{expect.order_id.client_id if expect else '':>12}"
        )
        i += 1


NodeData = TypeVar("NodeData")


class DoublyLinkedListNode(Generic[NodeData]):
    data: NodeData
    prev: Optional["DoublyLinkedListNode"]
    next: Optional["DoublyLinkedListNode"]

    def __init__(self, data: NodeData):
        self.data = data
        self.prev = None
        self.next = None


class DoublyLinkedList(Generic[NodeData]):
    head: Optional[DoublyLinkedListNode]
    tail: Optional[DoublyLinkedListNode]

    def __init__(self):
        self.head = None
        self.tail = None

    def append(self, data: NodeData) -> DoublyLinkedListNode[NodeData]:
        new_node = DoublyLinkedListNode(data)
        if self.head is None:
            self.head = self.tail = new_node
        elif self.tail is not None:
            self.tail.next = new_node
            new_node.prev = self.tail
            self.tail = new_node
        else:
            raise ValueError("head node is not None but tail is None")
        return new_node

    def remove(self, node_to_remove: DoublyLinkedListNode):
        """
        Remove the specified node from the list. Handles updating the head,
        tail, and length of the list.
        """
        if node_to_remove.prev:
            node_to_remove.prev.next = node_to_remove.next
        else:
            self.head = node_to_remove.next
        if node_to_remove.next:
            node_to_remove.next.prev = node_to_remove.prev
        else:
            self.tail = node_to_remove.prev

        node_to_remove.prev = node_to_remove.next = None

    def is_equal(self, other: DoublyLinkedList) -> bool:
        """
        Compares elements in two Doubly Linked Lists.
        """
        if len(self) != len(other):
            return False
        my_list = list(self)
        other_list = list(other)
        for my_elem, other_elem in zip(my_list, other_list):
            if my_elem != other_elem:
                return False
        return True

    def __len__(self):
        return len([x for x in self])

    def __iter__(self):
        current = self.head
        while current:
            yield current.data
            current = current.next
