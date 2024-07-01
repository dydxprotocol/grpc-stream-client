"""
Data to (1) convert integer price and quantity fields to human-readable decimals
and (2) map numeric market ids to human-readable instrument names.
"""
import requests

# N.b. can also query this from
#   https://github.com/dydxprotocol/v4-chain/blob/ab1ab540ea6cdf9e3b7348f7132cba0b358dc447/proto/dydxprotocol/assets/query.proto#L19
# E.g.
#   https://dydx-ops-rest.kingnodes.com/dydxprotocol/assets/asset
USDC_ATOMIC_RESOLUTION = -6


def quantums_to_size(quantums: int, atomic_resolution: int) -> float:
    """
    Convert quantums to a human-readable size.
    """
    return quantums / 10 ** -atomic_resolution


def subticks_to_price(subticks: int, atomic_resolution: int, quantum_conversion_exponent: int) -> float:
    """
    Convert subticks to a human-readable price.
    """
    exponent = atomic_resolution - quantum_conversion_exponent - USDC_ATOMIC_RESOLUTION
    return subticks / 10 ** exponent


def query_market_info(indexer_api) -> dict[int, dict]:
    """
    Query market information from the indexer and return a mapping from integer
    market id to market info.

    Each market info dict has keys `atomicResolution` and
    `quantumConversionExponent` for unit conversion.
    """
    uri = f"{indexer_api}/v4/perpetualMarkets"
    resp = requests.get(uri)
    if resp.status_code != 200:
        raise ValueError(f"Failed to query markets from {uri}: {resp.text}")
    return {int(x['clobPairId']): x for _, x in resp.json()["markets"].items()}
