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

const limie = pLimit(5);

const ErrNotImplement = new Error('not implemented');

class RichListController {
    /**
     * @param {json} options require node and conn is optional
     */
    constructor(options) {
        this.node = options.node;
        this.scanning = false;
		this.waitToStop = false;

        if (options.conn) {
            this.conn = options.conn;
        } else {
            this.conn = new MemConnector();
        }

        let blockEmitter = new EventEmitter();

        blockEmitter.on('bitcoind/hashblock', h => {
            if (!this.scanning && !this.waitToStop) {
                this._consider(h);
            }
        });

        this.node.services.bitcoind.subscribe('hashblock', blockEmitter);

        this.common = options.common || new Common({ log: this.node.log });
    }

    init() {
        return new Promise((resolve, reject) => {
            this.node.getBestBlockHash((err, hash) => {
                if (err) {
                    reject(err);
                } else {
                    resolve(hash);
                }
            });
        }).then(hash => {
            if (!this.scanning) {
                return this._consider(hash);
            }
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
                        return this.common.notReady(null, res);
                    }
                })
                .then(top_list => {
                    if (top_list) {
                        res.jsonp(top_list);
                    }
                });
        });
    }

	stop(done) {

		let stopInterval = setInterval(()=>{
			if(!this.waitToStop){
				this.waitToStop = true;
			}else if(!this.scanning){
				clearInterval(stopInterval);
				done();
			}
		},500);
	}

    /**
     * Check heighest local block with global and consider to update local data
     */
    _consider(latest) {
		if(this.waitToStop){
			this.scanning = false;
			return;
		}

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
                            this.node.log.error(err);
                            next();
                        });
                } else {
                    if (local === 0) {
                        header = {
                            height: 0
                        };
                    }

                    // Get next block.
                    if (header.height % 100 == 0) {
                        this.node.getBlockHeader(
                            latest,
                            (err, lastBlockHeader) => {
                                this.node.log.info(
                                    'Scan balances height:',
                                    header.height,
                                    'Percentage:',
                                    (
                                        (header.height /
                                            lastBlockHeader.height) *
                                        100
                                    ).toFixed(2)
                                );
                            }
                        );
                    }
                    this.node.getBlockHeader(
                        header.height + 1,
                        (err, header) => {
                            this._new(header.hash)
                                .then(() => {
                                    next();
                                })
                                .catch(err => {
                                    this.node.log.error(err);
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
                    let correctedTxs = [];
                    txs.forEach(tx => {
                        if (tx.height === block.height) {
                            correctedTxs.push(tx);
                        }
                    });
                    block.txs = correctedTxs;
                    return this.conn.insert_block(block);
                })
                .catch(err => {
                    this.node.log.error(err);
                    throw err;
                });
        });
    }
}

module.exports = RichListController;
