'use strict';

const createTree = require('functional-red-black-tree');

class MemoryStorage {
    /**
     * Initialize connector
     */
    constructor() {
        this.blocks = [];
        this.balances = {};
        this.transactions = {};

        this.ranks = createTree((a, b) => {
            return b - a;
        });
    }

    init() {
        return Promise.resolve();
    }

    /**
     * Get latest block hash.
     */
    getLatestBlock() {
        return new Promise((resolve, reject) => {
            let hash = (this.blocks.length <= 0) ? undefined : this.blocks[this.blocks.length - 1].hash;
            resolve(hash);
        });
    }

    /**
     * Get top n addresses ordered by highest balances.
     */
    getMostRichest(n) {
        return new Promise((resolve, reject) => {
            // Gather all riched addresses up to specified number.
            let res = [];

            this.ranks.forEach((balance, addrs) => {
                Object.keys(addrs).forEach(addr => {
                    if (res.length > n) {
                        return true;
                    }

                    res.push({
                        address: addr,
                        balance: balance
                    });
                });

                if (res.length > n) {
                    return true;
                }

                return false;
            });

            res = res.sort((a, b) => {
                if (a.balance === b.balance) {
                    return b.address > a.address ? -1 : 1;
                } else {
                    return b.balance - a.balance;
                }
            });

            res = res.map(item => {
                return item;
            });

            resolve(res);
        });
    }

    /**
     * Check if the specified transactions is already exists.
     *
     * @param {string[]} ids list of transaction identifiers to check.
     *
     * @returns {Promise<Set<string>>}
     */
    hasTransactions(ids) {
        if (!ids.length) {
            throw new Error('no transaction is specified');
        }

        return new Promise((resolve, reject) => {
            let found = new Set();

            for (let id of ids) {
                if (this.transactions.hasOwnProperty(id)) {
                    found.add(id);
                }
            }

            resolve(found);
        });
    }

    /**
     * Remove latest block and update balances.
     */
    invalidateLatestBlock() {
        return new Promise((resolve, reject) => {
            if (this.blocks.length <= 0) {
                reject(new Error('no block available'));
            } else {
                let lastBlock = this.blocks[this.blocks.length - 1];
                this._calculate(lastBlock, true);
                this.blocks.pop();

                resolve(null);
            }
        });
    }

    /**
     * Add a new block and update balances.
     */
    addBlock(block, txs) {
        return new Promise((resolve, reject) => {
            block.txs = txs;

            this._calculate(block);
            this.blocks.push(block);

            block.txs.forEach(tx => {
                this.transactions[tx.hash] = tx;
            });

            resolve(true);
        });
    }

    _calculate(block, reverse = false) {
        block.txs.forEach(tx => {
            // Update balance using outputs.
            tx.outputs.forEach(vout => {
                let addr = vout.address;
                if (!addr) {
                    return;
                }

                let change = reverse ? -vout.satoshis : vout.satoshis;
                let current = addr in this.balances ? this.balances[addr] : 0;

                this._change_balance(addr, current + change);
            });

            // Update balance using inputs.
            tx.inputs.forEach(vin => {
                // Gather required data to calculate balance.
                let txid = vin.prevTxId;
                if (!txid) {
                    return;
                }

                let tx = this.transactions[txid];
                if (!tx) {
                    return;
                }

                let vout = tx.outputs[vin.outputIndex];
                let addr = vout.address;
                if (!addr) {
                    return;
                }

                // Calculate new balance.
                let change = reverse ? vout.satoshis : -vout.satoshis;
                let current = addr in this.balances ? this.balances[addr] : 0;

                this._change_balance(addr, current + change);
            });
        });
    }

    /**
     * Update balances from red black tree and json arr=>balance
     */
    _change_balance(addr, balance) {
        // remove from old balances set
        let current = this.balances[addr];
        if (current) {
            let addresses = this.ranks.get(current);
            if (addresses && addresses[addr]) {
                delete addresses[addr];
            }
        }

        // set new balances to set
        this.balances[addr] = balance;

        if (balance <= 0) {
            return;
        }

        // insert to new balances set
        let addresses = this.ranks.get(balance);
        if (!addresses) {
            addresses = {};
            this.ranks = this.ranks.insert(balance, addresses);
        }

        addresses[addr] = true;
    }
}

module.exports = MemoryStorage;
