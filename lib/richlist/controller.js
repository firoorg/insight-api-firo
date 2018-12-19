'use strict';

const EventEmitter = require('events');
const MemoryStorage = require('./storage/memory');

const Stopped = 0;
const Running = 1;
const Stopping = 2;

const InvalidBlockError = new Error('block not valid');
const AlreadyLatestBlockError = new Error('already latest block');

class RichListController extends EventEmitter {
    /**
     * @param {object} options require node and storage is optional
     */
    constructor(options) {
        super();

        this.node = options.node;
        this.state = Stopped;
        this.emitter = new EventEmitter();

        if (options.storage) {
            this.storage = options.storage;
        } else {
            this.storage = new MemoryStorage();
        }

        this.startScanBlocks = this.startScanBlocks.bind(this); // we need this to be able to pass startScanBlocks as a callback directly
        this.node.services.bitcoind.subscribe('hashblock', this.emitter);
    }

    init() {
        this.state = Running;
        this.startScanBlocks();

        return Promise.resolve();
    }

    list(req, res) {
        this.storage.getMostRichest(100).then(list => {
            if (list) {
                res.jsonp(list);
            }
        });
    }

	stop() {
        return new Promise((resolve, reject) => {
            // initiat stopping
            this.state = Stopping;
            this.node.services.bitcoind.unsubscribe('hashblock', this.emitter);

            // wait until stopped
            this.emitter.once('stopped', resolve);
            this.emitter.emit('bitcoind/hashblock');
        });
    }

    startScanBlocks() {
        // check if stopping request
        if (this.state === Stopping) {
            this.state = Stopped;
            this.emitter.removeListener('bitcoind/hashblock', this.startScanBlocks);
            this.emitter.emit('stopped');
            return;
        }

        // start blocks scanning loop
        this.storage.getLatestBlock().then(local => {
            // check if latest local block still on the chain
            return new Promise((resolve, reject) => {
                this.node.getBlockHeader(local || 0, (err, block) => {
                    if (err) {
                        // assume the error is block is not valid
                        resolve();
                    } else {
                        resolve(block);
                    }
                });
            });
        }).then(block => {
            if (!block) {
                // the latest local block does not on the chain, invalidate it
                return this.storage.invalidateLatestBlock().then(() => {
                    throw InvalidBlockError;
                });
            }

            if (!block.nextHash) {
                throw AlreadyLatestBlockError;
            }

            // get next block
            return new Promise((resolve, reject) => {
                this.node.getBlockOverview(block.nextHash, (err, block) => {
                    if (err) {
                        // assume the error is block is not valid
                        resolve();
                    } else {
                        resolve(block);
                    }
                });
            });
        }).then(block => {
            if (!block) {
                // the next block for latest local block does not on the chain, invalidate the latest local block
                return this.storage.invalidateLatestBlock().then(() => {
                    throw InvalidBlockError;
                });
            }

            // get all transactions for the block
            let promise = undefined;
            let txs = [];

            block.txids.forEach(txid => {
                if (promise) {
                    promise = promise.then(tx => {
                        txs.push(tx);
                        return this.getTransaction(txid);
                    });
                } else {
                    promise = this.getTransaction(txid);
                }
            });

            return promise.then(tx => {
                txs.push(tx);
                return { block, txs };
            });
        }).then(({ block, txs }) => {
            // filter transactions
            let added = new Set();

            block.txs = [];

            for (let tx of txs) {
                // if one of txs is undefined that mean a chain has been switched while we requesting transaction details
                if (tx === undefined) {
                    return this.storage.invalidateLatestBlock().then(() => {
                        throw InvalidBlockError;
                    });
                }

                // previously there is a bug that allow one transaction to be duplicated
                // so we need this to handle that case
                if (tx.height !== block.height || added.has(tx.hash)) {
                    continue;
                }

                block.txs.push(tx);
                added.add(tx.hash);
            }

            return this.storage.addBlock(block).then(() => block);
        }).then(block => {
            if (block.height % 100 === 0) {
                this.node.log.info('Blocks scanned', block.height);
            }

            // start next loop
            this.startScanBlocks();
        }).catch(err => {
            if (err == InvalidBlockError) {
                // start next loop
                this.startScanBlocks();
            } else if (err == AlreadyLatestBlockError) {
                // local block is already up to date, wait until there is a new block to scan again
                this.emitter.once('bitcoind/hashblock', this.startScanBlocks);
                this.emit('latestBlockScanned');
            } else {
                throw err;
            }
        });
    }

    getTransaction(txid) {
        return new Promise((resolve, reject) => {
            this.node.getDetailedTransaction(txid, (err, transaction) => {
                if (err) {
                    resolve();
                } else {
                    resolve(transaction);
                }
            });
        });
    };
}

module.exports = RichListController;
