"""
Connect to a full node gRPC feed and print the messages as JSON.
"""
import asyncio

import grpc
from google.protobuf.json_format import MessageToJson
# Classes generated from the proto files
from v4_proto.dydxprotocol.clob.query_pb2 import StreamOrderbookUpdatesRequest
from v4_proto.dydxprotocol.clob.query_pb2_grpc import QueryStub

import src.config as config


async def main(conf: dict):
    host = conf['dydx_full_node']['grpc_host']
    port = conf['dydx_full_node']['grpc_port']
    clob_pair_ids = conf['stream_options']['clob_pair_ids']
    addr = f"{host}:{port}"

    # Connect to the gRPC feed and start listening
    # (adjust to use secure channel if needed)
    async with grpc.aio.insecure_channel(addr, config.GRPC_OPTIONS) as channel:
        try:
            stub = QueryStub(channel)
            request = StreamOrderbookUpdatesRequest(clob_pair_id=clob_pair_ids)
            async for response in stub.StreamOrderbookUpdates(request):
                print(MessageToJson(response, indent=None))
                # Alternatively, print like this for gRPC string format
                # print(response)
            print("Stream ended")
        except grpc.aio.AioRpcError as e:
            print(f"gRPC error occurred: {e.code()} - {e.details()}")
        except Exception as e:
            print(f"Unexpected error in stream: {e}")


if __name__ == "__main__":
    c = config.load_yaml_config('config.yaml')
    print("Starting with conf:", c)
    asyncio.run(main(c))
