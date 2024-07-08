import yaml

# gRPC channel options
GRPC_OPTIONS = [
    ("grpc.keepalive_time_ms", 3000),  # Send keepalive ping every 30 seconds
    ("grpc.keepalive_timeout_ms", 1000,),  # Wait 10 seconds for ping ack before considering the connection dead
    ("grpc.keepalive_permit_without_calls", True,),  # Allow keepalive pings even when there are no calls
    ("grpc.http2.min_time_between_pings_ms", 3000,),  # Minimum allowed time between pings
    ("grpc.http2.min_ping_interval_without_data_ms", 3000,),  # Minimum allowed time between pings with no data
]

# Singleton class to load configs
class Config(object):
    def __new__(cls,):
        if not hasattr(cls, 'instance'):
            cls.instance = super(Config, cls).__new__(cls)
        return cls.instance
  
    def __init__(self):
        self.config = {}
        with open("config.yaml", 'r') as file:
            config = yaml.safe_load(file)
            self.config = config

    def get_config(self):
        return self.config

def get_addr_and_cpids():
    config = Config().get_config()
    host = config['dydx_full_node']['grpc_host']
    port = config['dydx_full_node']['grpc_port']
    cpids = config['stream_options']['clob_pair_ids']
    addr = f"{host}:{port}"
    return addr, cpids