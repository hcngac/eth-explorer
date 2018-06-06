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


def get_txs_of_block(eth, block):
    txs = []
    tx_idx = 0
    tx = eth.getTransactionFromBlock(block, tx_idx)
    while tx is not None:
        txs.append(tx_reformat(eth, tx))
        tx = eth.getTransactionFromBlock(block, tx_idx)
        tx_idx = tx_idx + 1
    return txs


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Ethereum Transaction Scraper.")
    parser.add_argument("-f", action="store", type=int, default=-1,
                        help="Starting block number. (Default: latest block)")
    parser.add_argument("-t", action="store", type=int, default=-1,
                        help="Last block number. (Default: latest block)")
    args = parser.parse_args()

    web3 = w3()
    f = web3.eth.blockNumber if args.f is -1 else args.f
    t = -1 if args.t is -1 else args.t

    print("From block: " + str(f), file=sys.stderr)
    print("To block: " + str(t), file=sys.stderr)

    elasticSearch = Elasticsearch(
        hosts=[{"host": "localhost", "port": 9200}])

    if t != -1:
        for block in range(f, t + 1):
            txs = get_txs_of_block(web3.eth, block)
            print("Block: " + str(block) +
                  " Transaction count: " + str(len(txs)))
            for tx in txs:
                elasticSearch.index(index="eth-scraping",
                                    doc_type="tx-log", id="block-"+str(block)+"-tx-"+str(tx["transactionIndex"]), body=tx)

    else:
        block = f
        while True:
            while web3.eth.blockNumber <= block:
                pass
            txs = get_txs_of_block(web3.eth, block)
            print("Block: " + str(block) +
                  " Transaction count: " + str(len(txs)))
            for tx in txs:
                elasticSearch.index(index="eth-scraping",
                                    doc_type="tx-log", id="block-"+str(block)+"-tx-"+str(tx["transactionIndex"]), body=tx)
            block = block + 1


if __name__ == "__main__":
    main()
