import yaml

# gRPC channel options
GRPC_OPTIONS = [
    ("grpc.keepalive_time_ms", 30000),  # Send keepalive ping every 30 seconds
    ("grpc.keepalive_timeout_ms", 10000,),  # Wait 10 seconds for ping ack before considering the connection dead
    ("grpc.keepalive_permit_without_calls", True,),  # Allow keepalive pings even when there are no calls
    ("grpc.http2.min_time_between_pings_ms", 30000,),  # Minimum allowed time between pings
    ("grpc.http2.min_ping_interval_without_data_ms", 30000,),  # Minimum allowed time between pings with no data
]


def load_yaml_config(path):
    with open(path, 'r') as file:
        config = yaml.safe_load(file)
    return config
