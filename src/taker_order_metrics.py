"""
Aggregates information about taker orders.
"""
import src.book as lob
import logging
import src.config as config

conf = config.Config().get_config()


class TakerOrderMetrics():
    def __init__(self):
        self.block_height = 0
        self.num_orders = 0
        self.bids = 0
        self.asks = 0
        self.order_flags = {}

    def process_order(self, order: lob.Order, block_height: int):
        # print out metrics if block height increases
        if block_height != self.block_height and block_height != 0:
            self.print()
            self.flush()
        self.block_height = block_height

        if order.is_bid:
            self.bids += 1
        else:
            self.asks += 1
        order_flag = order.order_id.order_flags
        self.order_flags[order_flag] = self.order_flags.get(order_flag, 0) + 1


    def flush(self):
        self.__init__()

    def print(self):
        if not conf['print_taker_orders']:
            return
        num_short = self.order_flags.get(0, 0)
        num_conditional = self.order_flags.get(32, 0)
        num_long_term = self.order_flags.get(64, 0)
        logging.info(f"Block {self.block_height}: {self.num_orders} Taker orders, {self.bids} bids, {self.asks} asks, " +
            f"{num_short} short term, {num_long_term} long term, {num_conditional} conditional orders.")
        
    