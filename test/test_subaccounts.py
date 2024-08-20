import unittest
from src.subaccounts import (
    SubaccountId, SubaccountPerpetualPosition,
    SubaccountAssetPosition, StreamSubaccount, parse_subaccounts
)
from v4_proto.dydxprotocol.subaccounts.streaming_pb2 import StreamSubaccountUpdate


class TestSubaccounts(unittest.TestCase):

    def test_parse_subaccounts(self):
        # Mock the StreamSubaccountUpdate protobuf message
        mock_update = StreamSubaccountUpdate()
        mock_update.subaccount_id.owner = "0xAddress1"
        mock_update.subaccount_id.number = 1

        perp_position = mock_update.updated_perpetual_positions.add()
        perp_position.perpetual_id = 101
        perp_position.quantums = 1000

        asset_position = mock_update.updated_asset_positions.add()
        asset_position.asset_id = 202
        asset_position.quantums = 5000

        expected_subaccount = StreamSubaccount(
            subaccount_id=SubaccountId(owner_address="0xAddress1", subaccount_number=1),
            perpetual_positions={101: SubaccountPerpetualPosition(perpetual_id=101, quantums=1000)},
            asset_positions={202: SubaccountAssetPosition(asset_id=202, quantums=5000)}
        )

        parsed_subaccount = parse_subaccounts(mock_update)

        self.assertEqual(parsed_subaccount, expected_subaccount)

    def test_subaccount_comparison(self):
        subaccount1 = StreamSubaccount(
            subaccount_id=SubaccountId(owner_address="0xAddress1", subaccount_number=1),
            perpetual_positions={101: SubaccountPerpetualPosition(perpetual_id=101, quantums=1000)},
            asset_positions={202: SubaccountAssetPosition(asset_id=202, quantums=5000)}
        )

        subaccount2 = StreamSubaccount(
            subaccount_id=SubaccountId(owner_address="0xAddress1", subaccount_number=1),
            perpetual_positions={101: SubaccountPerpetualPosition(perpetual_id=101, quantums=1000)},
            asset_positions={202: SubaccountAssetPosition(asset_id=202, quantums=5000)}
        )

        # Test equality
        self.assertEqual(subaccount1, subaccount2)

        # Test inequality with different quantums
        subaccount2.perpetual_positions[101].quantums = 2000
        self.assertNotEqual(subaccount1, subaccount2)

        # Test inequality with different asset_positions
        subaccount2 = StreamSubaccount(
            subaccount_id=SubaccountId(owner_address="0xAddress1", subaccount_number=1),
            perpetual_positions={101: SubaccountPerpetualPosition(perpetual_id=101, quantums=1000)},
            asset_positions={203: SubaccountAssetPosition(asset_id=203, quantums=5000)}
        )
        self.assertNotEqual(subaccount1, subaccount2)

    def test_subaccount_dict_equality(self):
        subaccount1 = StreamSubaccount(
            subaccount_id=SubaccountId(owner_address="0xAddress1", subaccount_number=1),
            perpetual_positions={101: SubaccountPerpetualPosition(perpetual_id=101, quantums=1000)},
            asset_positions={202: SubaccountAssetPosition(asset_id=202, quantums=5000)}
        )

        subaccount2 = StreamSubaccount(
            subaccount_id=SubaccountId(owner_address="0xAddress2", subaccount_number=2),
            perpetual_positions={102: SubaccountPerpetualPosition(perpetual_id=102, quantums=2000)},
            asset_positions={203: SubaccountAssetPosition(asset_id=203, quantums=6000)}
        )

        subaccount3 = StreamSubaccount(
            subaccount_id=SubaccountId(owner_address="0xAddress3", subaccount_number=2),
            perpetual_positions={102: SubaccountPerpetualPosition(perpetual_id=102, quantums=2000)},
            asset_positions={203: SubaccountAssetPosition(asset_id=203, quantums=6000)}
        )

        dict1 = {
            subaccount1.subaccount_id: subaccount1,
            subaccount2.subaccount_id: subaccount2
        }

        dict2 = {
            subaccount1.subaccount_id: subaccount1,
            subaccount2.subaccount_id: subaccount2
        }

        # Test equality
        self.assertEqual(dict1, dict2)

        dict2[subaccount3.subaccount_id] = subaccount3
        self.assertNotEqual(dict1, dict2)


if __name__ == '__main__':
    unittest.main()
