Example Client for dYdX Full Node gRPC Streams

Compatible with dYdX full nodes running [v5.0.5+](https://github.com/dydxprotocol/v4-chain/releases/tag/protocol%2Fv4.1.3) (includes fills).

See the [v4.1.2-compatible tag](https://github.com/dydxprotocol/grpc-stream-client/tree/v4.1.2-compatible) for the version compatible with v4.1.2 (doesn't include fills, just book updates).

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
    (optimistic) FillType.NORMAL buy 1.98 @ 3775.4 taker=OrderId(owner_address='dydx14dltc2w6y3dhf0naz8luglsvjt0vhvswm2j6d0', subaccount_number=0, client_id=172418967) maker=OrderId(owner_address='dydx1q869gyjwanxhw5xdgfg67pg3y8gjeuzth6u6zl', subaccount_number=0, client_id=3032340154)
    (optimistic) FillType.NORMAL buy 1.0 @ 3775.4 taker=OrderId(owner_address='dydx14dltc2w6y3dhf0naz8luglsvjt0vhvswm2j6d0', subaccount_number=0, client_id=172418968) maker=OrderId(owner_address='dydx1expvgcc4j9cqnag2yp4n0tzf4ejtxtgnpj928r', subaccount_number=0, client_id=4187582710)
    Book for CLOB pair 0 (BTC-USD):
    Price          Qty    Client Id                                     Address Acc
    67667.000000     0.176000   3032340153 dydx1q869gyjwanxhw5xdgfg67pg3y8gjeuzth6u6zl 0
    67667.000000     0.186100   3029283519 dydx1dax7t2529z8996579zuqdatv62wpsw89lv3apc 0
    67667.000000     0.443300   1745942212 dydx17z3prca48l3c93wtlfp69p25gze45uey57z667 0
    67667.000000     0.050000   1002744015 dydx1expvgcc4j9cqnag2yp4n0tzf4ejtxtgnpj928r 0
    67667.000000     0.060800   1108947245 dydx100l9m6g70j28g2tk3jj4plmge8vsmj6jdrlzhk 1
    --           --
    67666.000000     0.100000   1739498652 dydx1ensqm4lyl5vg6mrj7uwpvrggjmw2wd8rnwu40c 1
    67666.000000     0.003000   1002744011 dydx1expvgcc4j9cqnag2yp4n0tzf4ejtxtgnpj928r 0
    67666.000000     0.060000   1673775367 dydx1s9q4vcyel46z2c3lx7tsk8dh6ma06zqwwt2yh9 0
    67666.000000     0.277700   1933984618 dydx17vhycd0r2x79d9n84jytpjkvllv72za6da40ry 0
    67665.000000     0.003000   1002744014 dydx1expvgcc4j9cqnag2yp4n0tzf4ejtxtgnpj928r 0

    Book for CLOB pair 1 (ETH-USD):
    Price          Qty    Client Id                                     Address Acc
    3775.500000     5.297000    168192650 dydx14dltc2w6y3dhf0naz8luglsvjt0vhvswm2j6d0 0
    3775.500000     5.297000   1746901703 dydx17z3prca48l3c93wtlfp69p25gze45uey57z667 0
    3775.500000     1.500000   1747609852 dydx1ensqm4lyl5vg6mrj7uwpvrggjmw2wd8rnwu40c 1
    3775.500000     1.959000    898243471 dydx15u3dtsf4twdxttvy7850dkex7tcf3ps2y8wcuf 0
    3775.500000     0.400000    958722475 dydx1s9q4vcyel46z2c3lx7tsk8dh6ma06zqwwt2yh9 0
    --           --
    3775.300000     1.921000    898243470 dydx15u3dtsf4twdxttvy7850dkex7tcf3ps2y8wcuf 0
    3775.300000     0.982000   4187582713 dydx1expvgcc4j9cqnag2yp4n0tzf4ejtxtgnpj928r 0
    3775.200000     1.968000   3029283520 dydx1dax7t2529z8996579zuqdatv62wpsw89lv3apc 0
    3775.200000     0.860000   4187582712 dydx1expvgcc4j9cqnag2yp4n0tzf4ejtxtgnpj928r 0
    3775.000000    15.892000   2565059354 dydx1g6vlujs2fw7c4886uc8g092474d54uhcf2swrg 0

    (finalized) FillType.NORMAL buy 1.98 @ 3775.4 taker=OrderId(owner_address='dydx14dltc2w6y3dhf0naz8luglsvjt0vhvswm2j6d0', subaccount_number=0, client_id=172418967) maker=OrderId(owner_address='dydx1q869gyjwanxhw5xdgfg67pg3y8gjeuzth6u6zl', subaccount_number=0, client_id=3032340154)
    (finalized) FillType.NORMAL buy 1.0 @ 3775.4 taker=OrderId(owner_address='dydx14dltc2w6y3dhf0naz8luglsvjt0vhvswm2j6d0', subaccount_number=0, client_id=172418968) maker=OrderId(owner_address='dydx1expvgcc4j9cqnag2yp4n0tzf4ejtxtgnpj928r', subaccount_number=0, client_id=4187582710)
    ...

### Print gRPC Messages Example
    
    python examples/print_feed_as_json.py


### Unit Tests

    python -m unittest discover -s test
