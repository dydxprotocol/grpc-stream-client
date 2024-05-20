import asyncio
from typing import List

import grpc
from google.protobuf.json_format import MessageToJson
from src.config import GRPC_OPTIONS, CONFIG

# Classes generated from the proto files
from v4_proto.dydxprotocol.clob.query_pb2 import StreamOrderbookUpdatesRequest, StreamOrderbookUpdatesResponse
from v4_proto.dydxprotocol.clob.query_pb2_grpc import QueryStub


async def listen_to_stream(channel, buffer: List[StreamOrderbookUpdatesResponse]):
    try:
        stub = QueryStub(channel)
        request = StreamOrderbookUpdatesRequest(
            clob_pair_id=CONFIG['stream_options']['clob_pair_ids']
        )
        async for response in stub.StreamOrderbookUpdates(request):
            response: StreamOrderbookUpdatesResponse
            buffer.append(response)
        print("Stream ended")
    except grpc.aio.AioRpcError as e:
        print(f"gRPC error occurred: {e.code()} - {e.details()}")
    except Exception as e:
        print(f"Unexpected error in stream: {e}")


def process_buffer(buffer: List[StreamOrderbookUpdatesResponse]):
    print(f"Buffer has {len(buffer)} messages")
    for response in buffer:
        print(f"> {MessageToJson(response, indent=None)}")
    buffer.clear()


async def process_buffer_every_n_ms(buffer, ms):
    while True:
        await asyncio.sleep(ms / 1000)
        process_buffer(buffer)


async def main():
    host = CONFIG['dydx_full_node']['grpc_host']
    port = CONFIG['dydx_full_node']['grpc_port']
    addr = f"{host}:{port}"

    # Adjust to use secure channel if needed
    buffer: List[StreamOrderbookUpdatesResponse] = []
    async with grpc.aio.insecure_channel(addr, GRPC_OPTIONS) as channel:
        await asyncio.gather(
            listen_to_stream(channel, buffer),
            asyncio.create_task(process_buffer_every_n_ms(buffer, CONFIG['interval_ms'])),
        )


if __name__ == "__main__":
    print("Starting with conf:", CONFIG)
    asyncio.run(main())
