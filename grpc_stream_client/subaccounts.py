from dataclasses import dataclass
from typing import Dict

from v4_proto.dydxprotocol.subaccounts.streaming_pb2 import StreamSubaccountUpdate


@dataclass(frozen=True)
class SubaccountId:
    owner_address: str
    subaccount_number: int


@dataclass
class SubaccountPerpetualPosition:
    perpetual_id: int
    quantums: int


@dataclass
class SubaccountAssetPosition:
    asset_id: int
    quantums: int


@dataclass
class StreamSubaccount:
    subaccount_id: SubaccountId
    # Map from perpetual_id to SubaccountPerpetualPosition
    perpetual_positions: Dict[int, SubaccountPerpetualPosition]
    # Map from asset_id to SubaccountAssetPosition
    asset_positions: Dict[int, SubaccountAssetPosition]


def parse_subaccounts(
    stream_subaccount_update: StreamSubaccountUpdate,
) -> StreamSubaccount:
    subaccount_id = SubaccountId(
        owner_address=stream_subaccount_update.subaccount_id.owner,
        subaccount_number=stream_subaccount_update.subaccount_id.number,
    )

    perpetual_positions = {
        pos.perpetual_id: SubaccountPerpetualPosition(
            perpetual_id=pos.perpetual_id, quantums=pos.quantums
        )
        for pos in stream_subaccount_update.updated_perpetual_positions
    }

    asset_positions = {
        pos.asset_id: SubaccountAssetPosition(asset_id=pos.asset_id, quantums=pos.quantums)
        for pos in stream_subaccount_update.updated_asset_positions
    }

    return StreamSubaccount(
        subaccount_id=subaccount_id,
        perpetual_positions=perpetual_positions,
        asset_positions=asset_positions,
    )
