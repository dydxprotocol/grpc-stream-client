from v4_proto.dydxprotocol.indexer.off_chain_updates.off_chain_updates_pb2 import OffChainUpdateV1

def get_clob_pair_id_from_offchain_update(update: OffChainUpdateV1) -> int:
    clob_pair_id = None
    update_type = update.WhichOneof('update_message')

    if update_type == 'order_place':
        clob_pair_id = update.order_place.order.order_id.clob_pair_id
    elif update_type == 'order_update':
        clob_pair_id = update.order_update.order_id.clob_pair_id
    elif update_type == 'order_remove':
        clob_pair_id = update.order_remove.removed_order_id.clob_pair_id
    else:
        raise ValueError(f"Unknown update type '{update_type}' in: {update}")
    return clob_pair_id


def bytes_to_int(byte_array: bytes) -> int:
    """
    Converts a byte array (representing an arbitrary-size signed integer) into an int.

    Args:
        byte_array (bytes): Array of bytes.

    Returns:
        int: The converted integer.
    """
    if len(byte_array) <= 1:
        return 0

    negated = (byte_array[0] & 1) == 1
    hex_string = byte_array[1:].hex()
    abs_value = int(hex_string, 16)

    return -abs_value if negated else abs_value
