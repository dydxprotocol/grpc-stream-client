from v4_proto.dydxprotocol.indexer.off_chain_updates.off_chain_updates_pb2 import OffChainUpdateV1
from v4_proto.dydxprotocol.indexer.protocol.v1.clob_pb2 import IndexerOrder, IndexerOrderId
from v4_proto.dydxprotocol.clob.order_pb2 import OrderId, Order
from v4_proto.dydxprotocol.clob.query_pb2 import StreamOrderbookUpdatesResponse, StreamUpdate
import grpc_stream_client.book as book


def get_clob_pair_id_from_offchain_update(update: OffChainUpdateV1) -> int:
    clob_pair_id = None
    update_type = update.WhichOneof("update_message")

    if update_type == "order_place":
        clob_pair_id = update.order_place.order.order_id.clob_pair_id
    elif update_type == "order_update":
        clob_pair_id = update.order_update.order_id.clob_pair_id
    elif update_type == "order_remove":
        clob_pair_id = update.order_remove.removed_order_id.clob_pair_id
    else:
        raise ValueError(f"Unknown update type '{update_type}' in: {update}")
    return clob_pair_id


def parse_indexer_oid(oid_fields: IndexerOrderId) -> book.OrderId:
    """
    Parse an order ID from the fields in an IndexerOrderId message.
    """
    return book.OrderId(
        owner_address=oid_fields.subaccount_id.owner,
        subaccount_number=oid_fields.subaccount_id.number,
        client_id=oid_fields.client_id,
        order_flags=oid_fields.order_flags,
    )


def parse_protocol_oid(oid_fields: OrderId) -> book.OrderId:
    """
    Parse an order ID from the fields in a Protocol OrderId message.
    """
    return book.OrderId(
        owner_address=oid_fields.subaccount_id.owner,
        subaccount_number=oid_fields.subaccount_id.number,
        client_id=oid_fields.client_id,
        order_flags=oid_fields.order_flags,
    )


def parse_indexer_order(order: IndexerOrder) -> book.Order:
    """
    Parse an order ID from the fields in a Indexer OrderId message.
    """
    lob_oid = parse_indexer_oid(order.order_id)
    return book.Order(
        order_id=lob_oid,
        is_bid=order.side == IndexerOrder.SIDE_BUY,
        original_quantums=order.quantums,
        quantums=order.quantums,
        subticks=order.subticks,
    )


def parse_protocol_order(order: Order) -> book.Order:
    """
    Parse an order ID from the fields in a Protocol OrderId message.
    """
    lob_oid = parse_protocol_oid(order.order_id)
    return book.Order(
        order_id=lob_oid,
        is_bid=order.side == Order.SIDE_BUY,
        original_quantums=order.quantums,
        quantums=order.quantums,
        subticks=order.subticks,
    )


def is_snapshot_update(update: StreamUpdate) -> bool:
    """
    Checks if the given update is a snapshot update.
    """
    update_type = update.WhichOneof("update_message")
    if update_type == "orderbook_update":
        return update.orderbook_update.snapshot
    elif update_type == "order_fill" or update_type == "taker_order":
        return False
    elif update_type == "subaccount_update":
        return update.subaccount_update.snapshot
    else:
        raise ValueError(f"Unknown update type '{update_type}' in: {update}")
    

def has_snapshot_update(response: StreamOrderbookUpdatesResponse) -> bool:
    for update in response.updates:
        if is_snapshot_update(update):
            return True
    return False
