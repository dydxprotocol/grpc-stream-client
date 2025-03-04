"""
Aggregates information about taker orders.
"""

import logging

import grpc_stream_client.book as book
import grpc_stream_client.config as config

conf = config.Config().get_config()


class TakerOrderMetrics:
    asks: int
    bids: int
    block_height: int
    do_print_taker_orders: bool
    num_orders: int
    order_flags: dict[int, int]

    def __init__(self, print_taker_orders: bool = False):
        self.do_print_taker_orders = print_taker_orders
        self.reset()

    def reset(self):
        self.block_height = 0
        self.num_orders = 0
        self.bids = 0
        self.asks = 0
        self.order_flags = {}

    def handle_order(self, order: book.Order, block_height: int):
        if block_height != self.block_height and block_height != 0:
            self.print()
            self.reset()
        self.block_height = block_height

        if order.is_bid:
            self.bids += 1
        else:
            self.asks += 1
        order_flag = order.order_id.order_flags
        self.order_flags[order_flag] = self.order_flags.get(order_flag, 0) + 1

    def print(self):
        if not conf["print_taker_orders"]:
            return
        num_short = self.order_flags.get(0, 0)
        num_conditional = self.order_flags.get(32, 0)
        num_long_term = self.order_flags.get(64, 0)
        logging.info(
            f"Block {self.block_height}: {self.num_orders} Taker orders, {self.bids} bids, {self.asks} asks, "
            + f"{num_short} short term, {num_long_term} long term, {num_conditional} conditional orders."
        )
