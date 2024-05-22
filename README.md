Example Client for dYdX Full Node gRPC Streams

Compatible with dYdX full nodes running [v4.1.2](https://github.com/dydxprotocol/v4-chain/releases/tag/protocol%2Fv4.1.2).

### Setup
    
    python3 -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt

You first need a full node with [gRPC streaming enabled](https://docs.dydx.exchange/validators/full_node_streaming#enabling-grpc-streaming). 
Add the full node address to the `config.yaml` file.

### Book Streaming Example

    python main.py

Example output:

    ...
    > {"updates": [{"orderPlace": {"order": {"orderId": ...
    Book for CLOB pair 0 (BTC-USD):
           Price          Qty    Client Id                                     Address Acc
    70061.000000     0.001400   2147483648 dydx1c0m5x87llaunl5sgv3q5vd7j5uha26d2q2r2q0 0
    70011.000000     0.028600            1 dydx1qzq0dx4xjj06d4nxfx7ls53d8wru625s445mhs 0
    69602.000000     0.000900    263179296 dydx1tv34m5kg659jm2t5tuazyfr48z7tw2ggpc0nd2 3
    69589.000000     0.000900    263179297 dydx1v6wnra2yyd3t56x7f4md7qhhvx43tds27qryrh 9
    69430.000000     0.000200    500030665 dydx14ktekhvect3gsrz0w94axjer5gqg4qfxj2kjqj 0
              --           --
    69393.000000     0.000900   3132017306 dydx1tv34m5kg659jm2t5tuazyfr48z7tw2ggpc0nd2 3
    69324.000000     0.000900   3670642909 dydx1tv34m5kg659jm2t5tuazyfr48z7tw2ggpc0nd2 3
    69174.000000     0.000900   3670642910 dydx1v6wnra2yyd3t56x7f4md7qhhvx43tds27qryrh 9
    68679.000000     0.001400            0 dydx1c0m5x87llaunl5sgv3q5vd7j5uha26d2q2r2q0 0
    68624.000000     0.029100            0 dydx1qzq0dx4xjj06d4nxfx7ls53d8wru625s445mhs 0

    Book for CLOB pair 1 (ETH-USD):
           Price          Qty    Client Id                                     Address Acc
     3755.900000     0.017000   2750692130 dydx102k7x0l06xgry0kytx3lvylv5hlhtcy7ccv2kl 3
     3754.000000     0.017000   2750692128 dydx1xlhw6sakd036arhl4qnnavn4lm0xyqx8gmh48d 7
     3750.300000     0.017000   2750692127 dydx1xlhw6sakd036arhl4qnnavn4lm0xyqx8gmh48d 7
     3748.400000     0.017000   2750692128 dydx102k7x0l06xgry0kytx3lvylv5hlhtcy7ccv2kl 3
     3740.900000     0.017000   3132941788 dydx102k7x0l06xgry0kytx3lvylv5hlhtcy7ccv2kl 3
              --           --
     3739.100000     0.017000   1863188447 dydx1xlhw6sakd036arhl4qnnavn4lm0xyqx8gmh48d 7
     3737.200000     0.017000   1863188446 dydx102k7x0l06xgry0kytx3lvylv5hlhtcy7ccv2kl 3
     3733.400000     0.017000   1863188447 dydx102k7x0l06xgry0kytx3lvylv5hlhtcy7ccv2kl 3
     3731.600000     0.017000   1863188449 dydx1xlhw6sakd036arhl4qnnavn4lm0xyqx8gmh48d 7
     3727.400000     0.061000            0 dydx14rplxdyycc6wxmgl8fggppgq4774l70zt6phkw 0
    ...

### Print gRPC Messages Example
    
    python examples/print_feed_as_json.py


### Unit Tests

    python -m unittest discover -s test
