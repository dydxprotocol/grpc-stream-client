import logging
from typing import List, Dict

from dataclasses import dataclass
import grpc
from v4_proto.dydxprotocol.subaccounts.query_pb2_grpc import QueryStub as SubaccountQueryStub
from v4_proto.dydxprotocol.subaccounts.query_pb2 import QueryGetSubaccountRequest
import src.config as config
from src.helpers import bytes_to_int


@dataclass
class PerpetualPosition:
    perpetual_id: int
    quantums: int

@dataclass
class Subaccount:
    owner: str
    number: int
    perpetual_positions: List[PerpetualPosition]


async def fetch_subaccounts(
        address_account_pairs: List[str]
) -> Dict[str, Subaccount]:
    """
    Connect to the gRPC feed and fetch subaccounts for the provided list of address/account_number pairs.

    Args:
        address_account_pairs (List[str]): A list of strings in the format "address/account_number".

    Returns:
        Dict[str, Subaccount]: A dictionary mapping each "address/account_number" to its corresponding Subaccount.
    """
    address, _ = config.get_addr_and_cpids()
    async with grpc.aio.insecure_channel(address, config.GRPC_OPTIONS) as channel:
        try:
            stub = SubaccountQueryStub(channel)
            subaccounts = {}
            for pair in address_account_pairs:
                owner, account_number = pair.split('/')
                request = QueryGetSubaccountRequest(owner=owner, number=int(account_number))
                response = await stub.Subaccount(request)

                # Convert proto PerpetualPosition to the data class
                perpetual_positions = [
                    PerpetualPosition(
                        perpetual_id=pp.perpetual_id,
                        quantums=bytes_to_int(pp.quantums)
                    )
                    for pp in response.subaccount.perpetual_positions
                ]

                subaccount = Subaccount(
                    owner=owner,
                    number=int(account_number),
                    perpetual_positions=perpetual_positions
                )

                subaccounts[pair] = subaccount
            return subaccounts
        except grpc.aio.AioRpcError as e:
            logging.error(f"gRPC error occurred: {e.code()} - {e.details()}")
            raise e
        except Exception as e:
            logging.error(f"Unexpected error in fetching subaccounts: {e}")
            raise e
