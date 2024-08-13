"""
Limit order book data structure (l3) for orders with dYdX protocol order IDs.
"""
from __future__ import annotations
import logging

from dataclasses import dataclass
from typing import Dict, Iterator, List, Optional, Tuple

from sortedcontainers import SortedDict


@dataclass(frozen=True)  # frozen=True allows use as dict keys
class OrderId:
    """
    These fields uniquely identify an order for some CLOB pair.
    """
    owner_address: str  # address of the account which placed the order
    subaccount_number: int  # index of the subaccount belonging to owner
    client_id: int  # client identifier for the order (unique at any given time, but can be reused)
    order_flags: int  # Order flags indicating if this is a conditional, short term, or long term order.


@dataclass
class Order:
    """
    An order with integer fields and a known owner.
    """
    order_id: OrderId
    is_bid: bool
    original_quantums: int  # integer size originally placed (needs conversion to decimal)
    quantums: int  # integer size remaining in the book (needs conversion to decimal)
    subticks: int  # integer price (needs conversion to decimal)


class LimitOrderBook:
    """
    A limit order book for a single CLOB pair.

    Insertion is O(log(N)), removal and update are O(1).
    """

    def __init__(self):
        self.oid_to_order_node: Dict[OrderId, ListNode] = {}
        self._asks: SortedDict[int, DoublyLinkedList] = SortedDict()
        # reverse order (highest first)
        self._bids: SortedDict[int, DoublyLinkedList] = SortedDict(lambda x: -x)

    @staticmethod
    def _get_or_create_level(subticks: int, book_side: SortedDict) -> 'DoublyLinkedList':
        """
        Get or create a level in the book side. Each level is a doubly linked
        list of orders.
        """
        if subticks not in book_side:
            book_side[subticks] = DoublyLinkedList()
        return book_side[subticks]

    def add_order(self, order: Order) -> Order:
        """
        Add an order to the book, to the end of the queue on its level.
        """
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

    def compare_books(self, other: LimitOrderBook):
        """
        Compares the two orderbooks. Prints out any inconsistencies
        """
        num_orders = len(self.oid_to_order_node)
        num_asks = len(self._asks)
        num_bids = len(self._bids)
        midpoint_price = self.get_midpoint_price()

        other_num_orders = len(other.oid_to_order_node)
        other_num_asks = len(other._asks)
        other_num_bids = len(other._bids)
        other_midpoint_price = other.get_midpoint_price()

        if num_orders != other_num_orders:
            logging.error(f"FAIL: num orders: {num_orders} {other_num_orders}")
        if num_asks != other_num_asks:
            logging.error(f"FAIL: num asks: {num_asks} {other_num_asks}")
        if num_bids != other_num_bids:
            logging.error(f"FAIL: num bids: {num_bids} {other_num_bids}")
        if midpoint_price != other_midpoint_price:
            logging.error(f"FAIL: midpoint price: {midpoint_price} {other_midpoint_price}")
        for level, dll in self._asks.items():
            other_dll = other._asks[level]
            if not dll.is_equal(other_dll):
                logging.error(f"FAIL: Asks Price level {level} mismatching orders")
        for level, dll in self._bids.items():
            other_dll = other._bids[level]
            if not dll.is_equal(other_dll):
                logging.error(f"FAIL: Bids Price level {level} mismatching orders")


def asks_bids_from_book(book: LimitOrderBook,) -> Tuple[List[Order], List[Order]]:
    return list(book.asks()), list(book.bids())


def assert_books_equal(book_1: LimitOrderBook, book_2: LimitOrderBook):
    """
    Raise an AssertionError if the order book states of the two feed handlers
    do not match.
    """

    feed_asks, feed_bids = asks_bids_from_book(book_1)
    snap_asks, snap_bids = asks_bids_from_book(book_2)

    if snap_asks != feed_asks:
        debug_book_side(feed_asks, snap_asks)
        raise AssertionError(f"asks for book do not match")
    if snap_bids != feed_bids:
        debug_book_side(feed_bids, snap_bids)
        raise AssertionError(f"bids for book do not match")


def debug_book_side(have_side: List[Order], expect_side: List[Order]):
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


class ListNode:
    """
    A node in a doubly linked list.
    """

    def __init__(self, data):
        self.data = data
        self.prev = None
        self.next = None


class DoublyLinkedList:
    """
    A doubly linked list with O(1) append and remove operations.
    """

    def __init__(self):
        self.head = None
        self.tail = None

    def append(self, data) -> ListNode:
        """
        Append a new node with the provided data to the end of the list.
        """
        new_node = ListNode(data)
        if self.head is None:
            self.head = self.tail = new_node
        else:
            self.tail.next = new_node
            new_node.prev = self.tail
            self.tail = new_node
        return new_node

    def remove(self, node_to_remove: ListNode):
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
