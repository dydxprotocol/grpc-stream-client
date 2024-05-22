import unittest

from src.market_info import subticks_to_price, quantums_to_size


class TestLimitOrderBook(unittest.TestCase):
    def setUp(self):
        self.mock_market_info = {
            0: {'clobPairId': '0',
                'ticker': 'BTC-USD',
                'status': 'ACTIVE',
                'oraclePrice': '69427.47074',
                'priceChange24H': '-242.74838',
                'volume24H': '525170936.0884',
                'trades24H': 243376,
                'nextFundingRate': '0.00006573928571428571',
                'initialMarginFraction': '0.05',
                'maintenanceMarginFraction': '0.03',
                'openInterest': '753.6966',
                'atomicResolution': -10,
                'quantumConversionExponent': -9,
                'tickSize': '1',
                'stepSize': '0.0001',
                'stepBaseQuantums': 1000000,
                'subticksPerTick': 100000}
        }

    def test_subticks_to_price(self):
        # Convert subticks to price
        price = subticks_to_price(
            6988800000,
            self.mock_market_info[0]['atomicResolution'],
            self.mock_market_info[0]['quantumConversionExponent'],
        )
        self.assertEqual(price, 69888)

    def test_quantums_to_size(self):
        # Convert quantums to size
        size = quantums_to_size(
            9000000,
            self.mock_market_info[0]['atomicResolution'],
        )
        self.assertEqual(size, 0.0009)


if __name__ == '__main__':
    unittest.main()
