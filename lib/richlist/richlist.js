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
        this.node.getBestBlockHash((err, h) => {
            this.conn.bestBlock((err, local_h) => {
                if (h === local_h) {
                    this.conn.get_top(100, (err, top_list) => {
                        res.jsonp(top_list);
                    });
                } else {
                    res.status(503);
                }
            });
        });
    }

    /**
     * Check heighest local block with global and consider to update local data
     */
    _consider(latest) {
        this.scanning = true;

        this.conn.bestBlock((err, local) => {
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
                    this.conn.invalidate(next);
                } else {
                    if (local === 0) {
                        header = {
                            height: 0
                        };
                    }

                    // Get next block.
                    this.node.getBlockHeader(
                        header.height + 1,
                        (err, header) => {
                            this._new(header.hash, next);
                        }
                    );
                }
            });
        });
    }

    /**
     * Append new local block
     */
    _new(hash, callback) {
        this.node.getBlockOverview(hash, (err, block) => {
            async.mapSeries(
                block.txids,
                (txid, next) => {
                    this.node.getDetailedTransaction(
                        txid,
                        (err, transaction) => {
                            if (err) {
                                return next(err);
                            }
                            next(null, transaction);
                        }
                    );
                },
                (err, txs) => {
                    if (err) {
                        throw new Error('get transaction error');
                    }

                    block.txs = txs;

                    this.conn.insert_block(block, err => {
                        callback();
                    });
                }
            );
        });
    }
}

module.exports = RichListController;
