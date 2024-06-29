import unittest
from decimal import Decimal

from google.protobuf import json_format
from v4_proto.dydxprotocol.clob.query_pb2 import StreamOrderbookFill

from src.fills import parse_fill, FillType

# Example fill taken from the stream, encoded as JSON
FILL_JSON = """
{
  "clobMatch": {
    "matchOrders": {
      "takerOrderId": {
        "subaccountId": {
          "owner": "dydx15m3lvgfwe4xad7wqyskvn6qz5w5ahue60hhemn"
        },
        "clientId": 1368487720
      },
      "fills": [
        {
          "fillAmount": "1527000000",
          "makerOrderId": {
            "subaccountId": {
              "owner": "dydx100l9m6g70j28g2tk3jj4plmge8vsmj6jdrlzhk",
              "number": 1
            },
            "clientId": 533425689
          }
        },
        {
          "fillAmount": "1109000000",
          "makerOrderId": {
            "subaccountId": {
              "owner": "dydx1q869gyjwanxhw5xdgfg67pg3y8gjeuzth6u6zl"
            },
            "clientId": 1188620611
          }
        },
        {
          "fillAmount": "3055000000",
          "makerOrderId": {
            "subaccountId": {
              "owner": "dydx17z3prca48l3c93wtlfp69p25gze45uey57z667"
            },
            "clientId": 1796902745
          }
        }
      ]
    }
  },
  "orders": [
    {
      "orderId": {
        "subaccountId": {
          "owner": "dydx100l9m6g70j28g2tk3jj4plmge8vsmj6jdrlzhk",
          "number": 1
        },
        "clientId": 533425689
      },
      "side": "SIDE_SELL",
      "quantums": "1527000000",
      "subticks": "6545800000",
      "goodTilBlock": 18455236,
      "timeInForce": "TIME_IN_FORCE_POST_ONLY"
    },
    {
      "orderId": {
        "subaccountId": {
          "owner": "dydx1q869gyjwanxhw5xdgfg67pg3y8gjeuzth6u6zl"
        },
        "clientId": 1188620611
      },
      "side": "SIDE_SELL",
      "quantums": "1109000000",
      "subticks": "6545900000",
      "goodTilBlock": 18455229,
      "timeInForce": "TIME_IN_FORCE_POST_ONLY"
    },
    {
      "orderId": {
        "subaccountId": {
          "owner": "dydx17z3prca48l3c93wtlfp69p25gze45uey57z667"
        },
        "clientId": 1796902745
      },
      "side": "SIDE_SELL",
      "quantums": "3055000000",
      "subticks": "6545900000",
      "goodTilBlock": 18455227,
      "timeInForce": "TIME_IN_FORCE_POST_ONLY"
    },
    {
      "orderId": {
        "subaccountId": {
          "owner": "dydx15m3lvgfwe4xad7wqyskvn6qz5w5ahue60hhemn"
        },
        "clientId": 1368487720
      },
      "side": "SIDE_BUY",
      "quantums": "22368000000",
      "subticks": "6546200000",
      "goodTilBlock": 18455229,
      "timeInForce": "TIME_IN_FORCE_IOC"
    }
  ],
  "fillAmounts": [
    "1527000000",
    "1109000000",
    "3055000000",
    "5691000000"
  ]
}
"""
FILL_EXEC_MODE = 7
CLOB_PAIR_ID = 0  # Assuming CLOB pair ID is 0 for this test


class TestFills(unittest.TestCase):
    def test_parse_fill(self):
        # Construct a StreamOrderbookUpdatesResponse message from JSON
        msg = StreamOrderbookFill()
        json_format.Parse(FILL_JSON, msg)

        # Parse the fill
        fills = parse_fill(msg, FILL_EXEC_MODE)

        # Define expected output
        expected_fills = [
            {
                'clob_pair_id': CLOB_PAIR_ID,
                'maker': {
                    'owner_address': 'dydx100l9m6g70j28g2tk3jj4plmge8vsmj6jdrlzhk',
                    'subaccount_number': 1,
                    'client_id': 533425689
                },
                'taker': {
                    'owner_address': 'dydx15m3lvgfwe4xad7wqyskvn6qz5w5ahue60hhemn',
                    'subaccount_number': 0,
                    'client_id': 1368487720
                },
                'maker_total_filled_quantums': Decimal('1527000000'),
                'quantums': Decimal('1527000000'),
                'subticks': Decimal('6545800000'),
                'taker_is_buy': True,
                'exec_mode': FILL_EXEC_MODE,
                'fill_type': FillType.NORMAL,
            },
            {
                'clob_pair_id': CLOB_PAIR_ID,
                'maker': {
                    'owner_address': 'dydx1q869gyjwanxhw5xdgfg67pg3y8gjeuzth6u6zl',
                    'subaccount_number': 0,
                    'client_id': 1188620611
                },
                'taker': {
                    'owner_address': 'dydx15m3lvgfwe4xad7wqyskvn6qz5w5ahue60hhemn',
                    'subaccount_number': 0,
                    'client_id': 1368487720
                },
                'maker_total_filled_quantums': Decimal('1109000000'),
                'quantums': Decimal('1109000000'),
                'subticks': Decimal('6545900000'),
                'taker_is_buy': True,
                'exec_mode': FILL_EXEC_MODE,
                'fill_type': FillType.NORMAL,
            },
            {
                'clob_pair_id': CLOB_PAIR_ID,
                'maker': {
                    'owner_address': 'dydx17z3prca48l3c93wtlfp69p25gze45uey57z667',
                    'subaccount_number': 0,
                    'client_id': 1796902745
                },
                'taker': {
                    'owner_address': 'dydx15m3lvgfwe4xad7wqyskvn6qz5w5ahue60hhemn',
                    'subaccount_number': 0,
                    'client_id': 1368487720
                },
                'maker_total_filled_quantums': Decimal('3055000000'),
                'quantums': Decimal('3055000000'),
                'subticks': Decimal('6545900000'),
                'taker_is_buy': True,
                'exec_mode': FILL_EXEC_MODE,
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
            self.assertEqual(fill.maker_total_filled_quantums, expected_fill['maker_total_filled_quantums'])
            self.assertEqual(fill.quantums, expected_fill['quantums'])
            self.assertEqual(fill.subticks, expected_fill['subticks'])
            self.assertEqual(fill.taker_is_buy, expected_fill['taker_is_buy'])
            self.assertEqual(fill.exec_mode, expected_fill['exec_mode'])
            self.assertEqual(fill.fill_type, expected_fill['fill_type'])


if __name__ == '__main__':
    unittest.main()
