#!/usr/bin/env -S UV_PROJECT_ENVIRONMENT=.venv uv run
# /// script
# requires-python = ">=3.9"
# dependencies = [
#    "grpcio>=1.67.0",
#    "grpcio-tools==1.64.1",
#    "protobuf==5.28.1",
#    "pyyaml==6.0.1",
#    "requests~=2.32.2",
#    "sortedcontainers==2.4.0",
#    "v4-proto==6.0.8",
#    "websockets==12.0",
# ]
# ///
import asyncio
import logging

from google.protobuf.json_format import MessageToJson
import grpc  # type: ignore

from grpc_stream_client.config import Config
from v4_proto.dydxprotocol.clob.query_pb2 import StreamOrderbookUpdatesRequest
from v4_proto.dydxprotocol.clob.query_pb2_grpc import QueryStub

logger = logging.getLogger(__name__)


async def main(host: str, port: int, clob_pair_ids: list[int]):
    addr = f"{host}:{port}"
    async with grpc.aio.insecure_channel(addr, config.GRPC_OPTIONS) as channel:
        try:
            stub = QueryStub(channel)
            request = StreamOrderbookUpdatesRequest(clob_pair_id=clob_pair_ids)
            async for response in stub.StreamOrderbookUpdates(request):
                logger.info(MessageToJson(response, indent=None))
                # Alternatively, print like this for gRPC string format
                # print(response)
            logger.info("Stream ended")
        except grpc.aio.AioRpcError as e:
            logger.info(f"gRPC error occurred: {e.code()} - {e.details()}")
        except Exception as e:
            logger.info(f"Unexpected error in stream: {e}")


if __name__ == "__main__":
    config = Config().get_config()
    logger.info("Starting with conf:", config)
    asyncio.run(main(config["host"], config["port"], config["clob_pair_ids"]))
