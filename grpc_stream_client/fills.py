from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Tuple

from grpc_stream_client.book import OrderId
from v4_proto.dydxprotocol.clob.matches_pb2 import ClobMatch
from v4_proto.dydxprotocol.clob.query_pb2 import StreamOrderbookFill
from v4_proto.dydxprotocol.indexer.protocol.v1.clob_pb2 import IndexerOrder
from v4_proto.dydxprotocol.subaccounts.subaccount_pb2 import SubaccountId
import v4_proto.dydxprotocol.clob.order_pb2 as order_pb2

# TODO: Deleveraging fills are not yet emitted by the full node
FillType = Enum("FillType", ["NORMAL", "LIQUIDATION", "DELEVERAGING"])


@dataclass
class Fill:
    clob_pair_id: int  # The clob pair ID of the fill
    maker: OrderId  # No client id if deleveraging
    taker: OrderId  # No client id if liquidation or deleveraging
    quantums: int  # amount filled (needs conversion to decimal)
    subticks: int  # fill price (needs conversion to decimal)
    taker_is_buy: bool  # True if the taker is buying, False if selling
    exec_mode: int  # 7 if the fill is finalized by consensus (otherwise node is just guessing)
    fill_type: FillType
    maker_total_filled_quantums: int = 0  # *Total* amount the maker has filled


def parse_fill(order_fill: StreamOrderbookFill, exec_mode: int) -> List[Fill]:
    """
    Parse the StreamOrderbookFill message[1] into a series of `Fill` events.

    See also the `ClobMatch` message[2] for the structure of the fill.

    [1] https://github.com/dydxprotocol/v4-chain/blob/432e711decf01b855cf5ca90b699c9b187399826/proto/dydxprotocol/clob/query.proto#L211-L222
    [2] https://github.com/dydxprotocol/v4-chain/blob/432e711decf01b855cf5ca90b699c9b187399826/proto/dydxprotocol/clob/matches.proto#L13-L20
    """
    clob_match = order_fill.clob_match
    match_type = clob_match.WhichOneof("match")

    # The fill message includes maker order states at the time of fill
    # to eliminate ambiguity around
    #   (1) replaces (same cid could change price) and
    #   (2) order of events (when the proposer generates the final
    #       fills, it doesn't necessarily see the same book state as
    #       the node).
    # This is shaped id -> (order state, total fill amount)
    order_states_at_fill_time = {
        parse_pb_id(o.order_id): (o, tfa)
        for (o, tfa) in zip(order_fill.orders, order_fill.fill_amounts)
    }

    # Normal fill
    if match_type == "match_orders":
        return parse_fills(exec_mode, clob_match, order_states_at_fill_time)
    elif match_type == "match_perpetual_liquidation":
        return parse_liquidations(exec_mode, clob_match, order_states_at_fill_time)
    elif match_type == "match_perpetual_deleveraging":
        return parse_deleveraging_fills(exec_mode, clob_match)
    
    raise ValueError("match_type was not in {match_orders, match_perpetual_liquidation, match_perpetual_deleveraging}")


def parse_pb_id(oid: order_pb2.OrderId) -> OrderId:
    """
    Parse an order ID from the fields in an OrderId protobuf message.
    """
    return OrderId(
        owner_address=oid.subaccount_id.owner,
        subaccount_number=oid.subaccount_id.number,
        client_id=oid.client_id,
        order_flags=oid.order_flags,
    )


def get_order_id_for_liquidation(acc_id: SubaccountId) -> OrderId:
    """
    Parse an order ID from a subaccount id, filling in the client ID with 0.
    Fills the order flags with 9999.

    This is used for liquidations and deleveraging, where the orders are
    protocol-generated and don't have client IDs.
    """
    return OrderId(
        owner_address=acc_id.owner,
        subaccount_number=acc_id.number,
        client_id=0,
        order_flags=9999,
    )


def parse_fills(
    exec_mode: int,
    clob_match: ClobMatch,
    order_states_at_fill_time: Dict[OrderId, Tuple[order_pb2.Order, int]],
) -> List[Fill]:
    fills = []
    taker_id = parse_pb_id(clob_match.match_orders.taker_order_id)

    for fill in clob_match.match_orders.fills:
        maker_id = parse_pb_id(fill.maker_order_id)
        maker, total_filled_quantums = order_states_at_fill_time[maker_id]
        fills.append(
            Fill(
                clob_pair_id=maker.order_id.clob_pair_id,
                maker=maker_id,
                taker=taker_id,
                maker_total_filled_quantums=total_filled_quantums,
                quantums=fill.fill_amount,
                subticks=maker.subticks,
                taker_is_buy=maker.side == IndexerOrder.SIDE_SELL,
                exec_mode=exec_mode,
                fill_type=FillType.NORMAL,
            )
        )

    return fills


def parse_liquidations(
    exec_mode: int,
    clob_match: ClobMatch,
    order_states_at_fill_time: Dict[OrderId, Tuple[order_pb2.Order, int]],
) -> List[Fill]:
    """Parse liquidation fills from a MatchPerpetualLiquidation message"""
    fills = []

    liqd_acc_id = clob_match.match_perpetual_liquidation.liquidated
    liquidated_id = get_order_id_for_liquidation(liqd_acc_id)

    for fill in clob_match.match_perpetual_liquidation.fills:
        maker_id = parse_pb_id(fill.maker_order_id)
        maker, total_filled_quantums = order_states_at_fill_time[maker_id]
        fills.append(
            Fill(
                clob_pair_id=maker.order_id.clob_pair_id,
                maker=maker_id,
                taker=liquidated_id,
                maker_total_filled_quantums=total_filled_quantums,
                quantums=fill.fill_amount,
                subticks=maker.subticks,
                taker_is_buy=clob_match.match_perpetual_liquidation.is_buy,
                exec_mode=exec_mode,
                fill_type=FillType.LIQUIDATION,
            )
        )
    return fills


# TODO: Deleveraging fills are not yet emitted by the full node
def parse_deleveraging_fills(exec_mode: int, clob_match: ClobMatch) -> List[Fill]:
    """Parse fills from a MatchPerpetualDeleveraging message"""
    fills = []

    liqd_acc = clob_match.match_perpetual_deleveraging.liquidated
    liquidated_id = get_order_id_for_liquidation(liqd_acc)

    for fill in clob_match.match_perpetual_deleveraging.fills:
        fills.append(
            Fill(
                clob_pair_id=clob_match.match_perpetual_deleveraging.perpetual_id,
                maker=get_order_id_for_liquidation(fill.offsetting_subaccount_id),
                taker=liquidated_id,
                quantums=fill.fill_amount,
                subticks=0,  # We don't know subticks for deleveraging
                taker_is_buy=False,  # We don't know side for deleveraging
                exec_mode=exec_mode,
                fill_type=FillType.DELEVERAGING,
            )
        )

    return fills
