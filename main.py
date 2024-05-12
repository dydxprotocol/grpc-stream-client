import asyncio

import grpc
from google.protobuf.json_format import MessageToJson
from src.config import GRPC_OPTIONS, CONFIG

# Classes generated from the proto files
from v4_proto.dydxprotocol.clob.query_pb2 import StreamOrderbookUpdatesRequest
from v4_proto.dydxprotocol.clob.query_pb2_grpc import QueryStub


async def listen_to_stream(channel):
    try:
        stub = QueryStub(channel)
        request = StreamOrderbookUpdatesRequest(
            clob_pair_id=CONFIG['stream_options']['clob_pair_ids']
        )
        async for response in stub.StreamOrderbookUpdates(request):
            print(f"> {MessageToJson(response, indent=None)}")
        print("Stream ended")
    except grpc.aio.AioRpcError as e:
        print(f"gRPC error occurred: {e.code()} - {e.details()}")
    except Exception as e:
        print(f"Unexpected error in stream: {e}")


async def main():
    host = CONFIG['dydx_full_node']['grpc_host']
    port = CONFIG['dydx_full_node']['grpc_port']
    addr = f"{host}:{port}"

    # Adjust to use secure channel if needed
    async with grpc.aio.insecure_channel(addr, GRPC_OPTIONS) as channel:
        await listen_to_stream(channel)


if __name__ == "__main__":
    print("Starting with conf:", CONFIG)
    asyncio.run(main())
