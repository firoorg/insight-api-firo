'use strict';

const async = require('async');
const bitcore = require('zcore-lib');
const _ = bitcore.deps._;
const pools = require('../../pools.json');
const BN = bitcore.crypto.BN;
const LRU = require('lru-cache');
const Common = require('../common');
const Transactions = require('../transactions');
const MemConnector = require('./memconnector');

const createTree = require('functional-red-black-tree');
const EventEmitter = require('events');
const pLimit = require('p-limit');

const limie = pLimit(10);

const ErrNotImplement = new Error('not implemented');

class RichListController {
    /**
     * @param {json} options require node and conn is optional
     */
    constructor(options) {
        this.node = options.node;
        this.scanning = false;

        if (options.conn) {
            this.conn = options.conn;
        } else {
            this.conn = new MemConnector();
        }

        let blockEmitter = new EventEmitter();

        blockEmitter.on('bitcoind/hashblock', h => {
            if (!this.scanning) {
                this._consider(h);
            }
        });

        this.node.services.bitcoind.subscribe('hashblock', blockEmitter);

        this.node.getBestBlockHash((err, hash) => {
            this._consider(hash);
        });
    }

    list(req, res) {
        this.node.getBestBlockHash((err, currentHash) => {
            this.conn
                .bestBlock()
                .then(localHash => {
                    if (currentHash === localHash) {
                        return this.conn.get_top(100);
                    } else {
                        res.status(503);
                    }
                })
                .then(top_list => {
                    if (top_list) {
                        res.jsonp(top_list);
                    }
                });
        });
    }

    /**
     * Check heighest local block with global and consider to update local data
     */
    _consider(latest) {
        this.scanning = true;

        this.conn.bestBlock().then(local => {
            if (local === latest) {
                this.scanning = false;
                return;
            }

            // Check if local block still on the chain.
            this.node.getBlockHeader(local, (err, header) => {
                let next = err => {
                    this.node.getBestBlockHash((err, hash) => {
                        this._consider(hash);
                    });
                };

                // if not found invalidate best block
                if (err) {
                    this.conn
                        .invalidate()
                        .then(v => {
                            next();
                        })
                        .catch(err => {
                            console.error(err);
                            next();
                        });
                } else {
                    if (local === 0) {
                        header = {
                            height: 0
                        };
                    }

                    // Get next block.
                    console.log(header.height);
                    this.node.getBlockHeader(
                        header.height + 1,
                        (err, header) => {
                            this._new(header.hash)
                                .then(() => {
                                    next();
                                })
                                .catch(err => {
                                    console.error(err);
                                    next();
                                });
                        }
                    );
                }
            });
        });
    }

    /**
     * Append new local block
     */
    _new(hash) {
        let getDetailedTransaction = txid => {
            return new Promise((resolve, reject) => {
                this.node.getDetailedTransaction(txid, (err, transaction) => {
                    if (err) {
                        reject(err);
                    } else {
                        resolve(transaction);
                    }
                });
            });
        };

        return new Promise((resolve, reject) => {
            this.node.getBlockOverview(hash, (err, block) => {
                if (err) {
                    reject(err);
                } else {
                    resolve(block);
                }
            });
        }).then(block => {
            let promises = [];

            block.txids.forEach(txid => {
                promises.push(limie(() => getDetailedTransaction(txid)));
            });

            return Promise.all(promises)
                .then(txs => {
                    block.txs = txs;
                    return this.conn.insert_block(block);
                })
                .catch(err => {
                    console.error(err);
                    throw err;
                });
        });
    }
}

module.exports = RichListController;
