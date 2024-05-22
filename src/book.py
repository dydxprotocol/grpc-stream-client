"""
Limit order book data structure (l3) for orders with dYdX protocol order IDs.
"""
from dataclasses import dataclass
from typing import Dict, Iterator

from sortedcontainers import SortedDict


@dataclass(frozen=True)  # frozen=True allows use as dict keys
class OrderId:
    """
    These fields uniquely identify an order for some CLOB pair.
    """
    owner_address: str  # address of the account which placed the order
    subaccount_number: int  # index of the subaccount belonging to owner
    client_id: int  # client identifier for the order (unique at any given time, but can be reused)


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

    def get_order(self, oid: OrderId) -> Order:
        """
        Get an order from the book by its order ID.
        """
        return self.oid_to_order_node[oid].data

    def update_order(self, oid: OrderId, new_quantums: int) -> Order:
        """
        Update the quantums of an order in the book by its order ID without
        changing its position in the queue.
        """
        order_node = self.oid_to_order_node[oid]
        order_node.data.quantums = new_quantums
        return order_node.data

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

    def __len__(self):
        return len([x for x in self])

    def __iter__(self):
        current = self.head
        while current:
            yield current.data
            current = current.next
