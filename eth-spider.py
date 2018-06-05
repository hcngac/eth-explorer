from elasticsearch import Elasticsearch
from web3 import Web3
import sys
from pprint import PrettyPrinter
pp = PrettyPrinter(indent=4)


def w3():
    return Web3(Web3.IPCProvider("/var/eth/geth.ipc"))
#    return Web3(Web3.HTTPProvider("http://pub-node1.etherscan.io:8545"))


def account_is_contract(eth, address):
    if type(address) is not str:
        return False
    code = eth.getCode(address)
    if Web3.toHex(code) == '0x':
        return False
    else:
        return True


def tx_isContract(eth, tx):
    tx["toContract"] = account_is_contract(eth, tx["to"])
    tx["fromContract"] = account_is_contract(eth, tx["from"])
    return tx


def tx_reformat(eth, tx):
    tx = tx_isContract(eth, dict(tx))
    for idx in ['blockHash', 'hash', 'r', 's']:
        tx[idx] = Web3.toHex(tx[idx])
    for idx in ['value', 'gasPrice']:
        tx[idx] = float(tx[idx]) / 1e+18
    return tx


def scrap_tx(eth, fromBlock, toBlock):
    logs = eth.getLogs({"fromBlock": fromBlock, "toBlock": toBlock})
    txs = list(map(lambda log: tx_reformat(eth, eth.getTransaction(
        log["transactionHash"])), logs))
    return txs


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Ethereum Transaction Scraper.")
    parser.add_argument("-f", action="store", type=int, default=-1,
                        help="Starting block number. (Default: latest block)")
    parser.add_argument("-t", action="store", type=int, default=-1,
                        help="Last block number. (Default: latest block)")
    parser.add_argument("-b", action="store", type=int, default=1000,
                        help="Batch size. (Default 1000)")
    parser.add_argument("-m", action="store", type=int, default=10000,
                        help="Maximum file size. (lines)(Default 10000)")
    parser.add_argument("-p", action="store_true",
                        help="Pretty print results.")
    parser.add_argument("-e", action="store_true",
                        help="Use localhost Elasticsearch.")
    parser.add_argument("-s", action="store", type=str,
                        default="nostore", help="Store transactions to files s.<i>.log. Saving to file is never pretty-printed.")
    args = parser.parse_args()

    web3 = w3()
    f = web3.eth.blockNumber if args.f is -1 else args.f
    t = web3.eth.blockNumber if args.t is -1 else args.t

    print("From block: " + str(f), file=sys.stderr)
    print("To block: " + str(t), file=sys.stderr)

    if args.e:
        id = 1
        elasticSearch = Elasticsearch(
            hosts=[{"host": "localhost", "port": 9200}])
        for fromBlock in range(f, t + 1, args.b):
            toBlock = t if (fromBlock + args.b -
                            1) > t else (fromBlock + args.b - 1)
            print("Batch from block: " + str(fromBlock), file=sys.stderr)
            print("Batch to block: " + str(toBlock), file=sys.stderr)
            txs = scrap_tx(web3.eth, fromBlock, toBlock)
            print("Transaction count: " + str(len(txs)))
            for tx in txs:
                if elasticSearch.exists(index="eth-scraper",
                                        doc_type="tx-log", id=id):
                    elasticSearch.delete(index="eth-scraper",
                                         doc_type="tx-log", id=id)
                elasticSearch.create(index="eth-scraper",
                                     doc_type="tx-log", id=id, body=tx)
                id = id + 1

    elif args.s == "nostore" and args.p:
        for fromBlock in range(f, t + 1, args.b):
            toBlock = t if (fromBlock + args.b -
                            1) > t else (fromBlock + args.b - 1)
            print("Batch from block: " + str(fromBlock), file=sys.stderr)
            print("Batch to block: " + str(toBlock), file=sys.stderr)
            txs = scrap_tx(web3.eth, fromBlock, toBlock)
            print("Transaction count: " + str(len(txs)))
            for tx in txs:
                pp.pprint(scrap_tx(web3.eth, fromBlock, toBlock))

    elif args.s == "nostore" and not args.p:
        for fromBlock in range(f, t + 1, args.b):
            toBlock = t if (fromBlock + args.b -
                            1) > t else (fromBlock + args.b - 1)
            print("Batch from block: " + str(fromBlock), file=sys.stderr)
            print("Batch to block: " + str(toBlock), file=sys.stderr)
            txs = scrap_tx(web3.eth, fromBlock, toBlock)
            print("Transaction count: " + str(len(txs)))
            for tx in txs:
                print(tx)

    else:
        file_serial = 1
        file_tx_count = 0
        max_tx_count = args.m
        file_prefix = args.s
        current_file_name = file_prefix + str(file_serial) + '.log'

        print("Save to " + args.s)
        for fromBlock in range(f, t + 1, args.b):
            toBlock = t if (fromBlock + args.b -
                            1) > t else (fromBlock + args.b - 1)
            print("Batch from block: " + str(fromBlock), file=sys.stderr)
            print("Batch to block: " + str(toBlock), file=sys.stderr)
            txs = scrap_tx(web3.eth, fromBlock, toBlock)
            print("Transaction count: " + str(len(txs)))
            file_tx_count = file_tx_count + len(txs)
            with open(current_file_name, "a") as tx_log_file:
                for tx in txs:
                    print(tx, file=tx_log_file)
            if file_tx_count > max_tx_count:
                file_serial = file_serial + 1
                current_file_name = file_prefix + str(file_serial) + '.log'
                file_tx_count = 0


if __name__ == "__main__":
    main()
