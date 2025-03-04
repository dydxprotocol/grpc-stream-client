from pathlib import Path
from typing import BinaryIO, Optional
import datetime

from v4_proto.dydxprotocol.clob.query_pb2 import StreamOrderbookUpdatesResponse


def append_message_to_log(
    log: BinaryIO, message: StreamOrderbookUpdatesResponse, timestamp: datetime.datetime
):
    """
    Binary serialize the message and append it to the log file, prefixed by the
    timestamp (6 bytes for milliseconds) and message length (4 bytes).
    """
    timestamp_ms = int(timestamp.timestamp() * 1000)
    serialized_message = message.SerializeToString()
    message_length = len(serialized_message)

    log.write(timestamp_ms.to_bytes(6, byteorder="big"))
    log.write(message_length.to_bytes(4, byteorder="big"))
    log.write(serialized_message)


def read_message_from_log(
    log: BinaryIO,
) -> Optional[tuple[datetime.datetime, StreamOrderbookUpdatesResponse]]:
    """
    Read a message from the log file, deserializing it from the binary format,
    returning None if the end of the file is reached.
    """
    timestamp_bytes = log.read(6)
    if not timestamp_bytes:
        return None

    timestamp_ms = int.from_bytes(timestamp_bytes, byteorder="big")
    timestamp = datetime.datetime.fromtimestamp(timestamp_ms / 1000)

    length_bytes = log.read(4)
    if not length_bytes:
        return None

    message_length = int.from_bytes(length_bytes, byteorder="big")
    serialized_message = log.read(message_length)
    message = StreamOrderbookUpdatesResponse()
    message.ParseFromString(serialized_message)
    return timestamp, message


def read_all_from_log(path: Path) -> list[StreamOrderbookUpdatesResponse]:
    messages = []
    with open(path, "rb") as log:
        while (message := read_message_from_log(log)) is not None:
            messages.append(message[1])
    return messages
