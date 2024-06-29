"""
Helpers to read and write protobuf messages to/from disk.
"""
import datetime
from typing import BinaryIO, Optional, List, Tuple
from v4_proto.dydxprotocol.clob.query_pb2 import StreamOrderbookUpdatesResponse


def append_message_to_log(
        log: BinaryIO,
        message: StreamOrderbookUpdatesResponse,
        timestamp: datetime.datetime
):
    """
    Binary serialize the message and append it to the log file, prefixed by the
    timestamp (6 bytes for milliseconds) and message length (4 bytes).
    """
    # Convert timestamp to milliseconds since epoch
    timestamp_ms = int(timestamp.timestamp() * 1000)

    # Serialize the message
    serialized_message = message.SerializeToString()
    message_length = len(serialized_message)

    log.write(timestamp_ms.to_bytes(6, byteorder='big'))
    log.write(message_length.to_bytes(4, byteorder='big'))
    log.write(serialized_message)


def read_message_from_log(log: BinaryIO) -> Optional[
    Tuple[datetime.datetime, StreamOrderbookUpdatesResponse]
]:
    """
    Read a message from the log file, deserializing it from the binary format,
    returning None if the end of the file is reached.
    """
    timestamp_bytes = log.read(6)
    if not timestamp_bytes:
        return None

    timestamp_ms = int.from_bytes(timestamp_bytes, byteorder='big')
    timestamp = datetime.datetime.fromtimestamp(timestamp_ms / 1000)

    length_bytes = log.read(4)
    if not length_bytes:
        return None

    message_length = int.from_bytes(length_bytes, byteorder='big')
    serialized_message = log.read(message_length)
    message = StreamOrderbookUpdatesResponse()
    message.ParseFromString(serialized_message)
    return timestamp, message


def read_all_from_log(path: str) -> List[StreamOrderbookUpdatesResponse]:
    """
    Read all messages from the log file and return them in a list.
    """
    msgs = []
    with open(path, 'rb') as log:
        while (message := read_message_from_log(log)) is not None:
            msgs.append(message[1])
    return msgs
