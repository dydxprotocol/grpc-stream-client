import unittest

from google.protobuf import json_format
from v4_proto.dydxprotocol.clob.query_pb2 import StreamOrderbookUpdatesResponse

from src.fills import parse_fill, FillType

# Example fill taken from the stream, encoded as JSON
FILL_JSON = """
{
  "updates": [
    {
      "orderFill": {
        "clobMatch": {
          "matchOrders": {
            "takerOrderId": {
              "subaccountId": {
                "owner": "dydx1dj2nd8lgadugrl98gd8cu9zltd2uz2jncpwny9"
              },
              "clientId": 1716570794
            },
            "fills": [
              {
                "fillAmount": "13000000",
                "makerOrderId": {
                  "subaccountId": {
                    "owner": "dydx1expvgcc4j9cqnag2yp4n0tzf4ejtxtgnpj928r"
                  },
                  "clientId": 394714802
                }
              }
            ]
          }
        },
        "orders": [
          {
            "orderId": {
              "subaccountId": {
                "owner": "dydx1expvgcc4j9cqnag2yp4n0tzf4ejtxtgnpj928r"
              },
              "clientId": 394714802
            },
            "side": "SIDE_BUY",
            "quantums": "764000000",
            "subticks": "6847800000",
            "goodTilBlock": 16451679,
            "timeInForce": "TIME_IN_FORCE_POST_ONLY",
            "clientMetadata": 2323185667
          },
          {
            "orderId": {
              "subaccountId": {
                "owner": "dydx1dj2nd8lgadugrl98gd8cu9zltd2uz2jncpwny9"
              },
              "clientId": 1716570794
            },
            "side": "SIDE_SELL",
            "quantums": "13000000",
            "subticks": "6846900000",
            "goodTilBlock": 16451672,
            "timeInForce": "TIME_IN_FORCE_IOC"
          }
        ],
        "fillAmounts": [
          "13000000",
          "13000000"
        ]
      }
    }
  ],
  "blockHeight": 16451664,
  "execMode": 102
}
"""


class TestFills(unittest.TestCase):
    def test_parse_fill(self):
        # Construct a StreamOrderbookUpdatesResponse message from JSON
        msg = StreamOrderbookUpdatesResponse()
        json_format.Parse(FILL_JSON, msg)

        # Parse the fill
        fills = parse_fill(msg.updates[0].order_fill, msg.exec_mode)

        # Define expected output
        expected_fills = [
            {
                'clob_pair_id': 0,  # As the clob_pair_id is not explicitly given in the example
                'maker': {
                    'owner_address': 'dydx1expvgcc4j9cqnag2yp4n0tzf4ejtxtgnpj928r',
                    'subaccount_number': 0,
                    'client_id': 394714802,
                },
                'taker': {
                    'owner_address': 'dydx1dj2nd8lgadugrl98gd8cu9zltd2uz2jncpwny9',
                    'subaccount_number': 0,
                    'client_id': 1716570794,
                },
                'quantums': 13000000,
                'subticks': 6847800000,
                'taker_is_buy': True,
                'exec_mode': 102,
                'fill_type': FillType.NORMAL,
            }
        ]

        # Verify the parsed fills
        self.assertEqual(len(fills), len(expected_fills))

        for fill, expected_fill in zip(fills, expected_fills):
            self.assertEqual(fill.clob_pair_id, expected_fill['clob_pair_id'])
            self.assertEqual(fill.maker.owner_address, expected_fill['maker']['owner_address'])
            self.assertEqual(fill.maker.subaccount_number, expected_fill['maker']['subaccount_number'])
            self.assertEqual(fill.maker.client_id, expected_fill['maker']['client_id'])
            self.assertEqual(fill.taker.owner_address, expected_fill['taker']['owner_address'])
            self.assertEqual(fill.taker.subaccount_number, expected_fill['taker']['subaccount_number'])
            self.assertEqual(fill.taker.client_id, expected_fill['taker']['client_id'])
            self.assertEqual(fill.quantums, expected_fill['quantums'])
            self.assertEqual(fill.subticks, expected_fill['subticks'])
            self.assertEqual(fill.taker_is_buy, expected_fill['taker_is_buy'])
            self.assertEqual(fill.exec_mode, expected_fill['exec_mode'])
            self.assertEqual(fill.fill_type, expected_fill['fill_type'])


if __name__ == '__main__':
    unittest.main()
