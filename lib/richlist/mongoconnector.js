var async = require('async');
var bitcore = require('zcore-lib');
var _ = bitcore.deps._;
var pools = require('../../pools.json');
var BN = bitcore.crypto.BN;
var LRU = require('lru-cache');
var Common = require('../common');
var Transactions = require('../transactions');

// Connector
class MongoConnector {
    /**
     *
     * @param {string} options.mongo is mongodb connection
     */
    constructor(options) {
        this.mongo = options.mongo;
    }

    init() {
        return this.mongo
            .connect()
            .then(() => {
                // cache collections
                this.utxos = this.mongo.db().collection('utxos');
                this.blocks = this.mongo.db().collection('blocks');
                this.tops = this.mongo.db().collection('tops');
            })
            .then(() => {
                // create uniqued index of tx id and vouts index
                return this.utxos.createIndex(
                    { tx: 1, index: 1 },
                    { unique: 1 }
                );
            })
            .then(() => {
                // create tx id index
                return this.utxos.createIndex({ tx: 1 });
            })
            .then(() => {
                // create index of block height
                return this.blocks.createIndex({ height: 1 });
            })
            .then(() => {
                // create spent index
                return this.utxos.createIndex({ spent: 1 });
            });
    }

    /**
     * Cleanup db
     */
    cleandb() {
        // delete all blocks
        return this.blocks
            .deleteMany()
            .then(() => {
                // delete all utxos
                return this.utxos.deleteMany();
            })
            .then(() => {
                // delete all tops
                return this.tops.deleteMany();
            });
    }

    /**
     * Get local best block
     */
    bestBlock() {
        // get heighest block
        return this.blocks
            .find()
            .sort({ height: -1 })
            .limit(1)
            .toArray()
            .then(blocks => {
                // return heighest block hash or 0 if no block available
                if (blocks && blocks.length > 0) {
                    return blocks[0].hash;
                } else {
                    return 0;
                }
            });
    }

    /**
     * Get top n address order by balances
     */
    get_top(n) {
        return this.tops
            .find()
            .limit(n)
            .toArray()
            .then(tops => {
                return tops.map(top => ({
                    address: top.address,
                    balance: (top.balance / 1e8).toFixed(8)
                }));
            });
    }

    /**
     * Remove lastest block from blocks array and update balance
     */
    invalidate() {
        // start transaction
        let session = this.mongo.startSession();
        session.startTransaction();

        // get heighest block hash
        let promise = this.bestBlock()
            .then(hash => {
                // delete heighest block
                return this.blocks.deleteMany({ hash: hash }).then(() => hash);
            })
            .then(hash => {
                // delete all utxos in heighest block
                return this.utxos.deleteMany({ block: hash }).then(() => hash);
            })
            .then(hash => {
                // update all spent utxos in heighest block to unspent
                return this.utxos.updateMany(
                    { spent: hash },
                    { $set: { spent: false } }
                );
            });

        return this.setupTransactionFinisher(promise, session);
    }

    /**
     * Insert new block to local
     */
    insert_block(block) {
        // start transaction
        let session = this.mongo.startSession();
        session.startTransaction();

        // get blocks collection
        let promise = this.blocks
            .insertOne({
                hash: block.hash,
                height: block.height
            })
            .then(() => {
                // get new utxos and new spent utxos
                let outputs = [];
                let inputs = [];

                block.txs.forEach(tx => {
                    // read all txs in blocks
                    for (let i = 0; i < tx.outputs.length; i++) {
                        // read all vouts
                        if (!tx.outputs[i].address) {
                            // ignore mint
                            continue;
                        }
                        outputs.push({
                            tx: tx.hash,
                            block: block.hash,
                            index: i,
                            address: tx.outputs[i].address,
                            satoshis: tx.outputs[i].satoshis,
                            spent: false
                        });
                    }

                    tx.inputs.forEach(vin => {
                        // read all vins
                        if (!vin.prevTxId) {
                            return;
                        }

                        inputs.push({
                            tx: vin.prevTxId,
                            block: block.hash,
                            index: vin.outputIndex
                        });
                    });
                });

                // add new utxos
                return this.utxos.insertMany(outputs).then(() => {
                    // mark previous utxos as spent
                    let promises = inputs.map(spent => {
                        return this.utxos.updateMany(
                            {
                                tx: spent.tx,
                                index: spent.index
                            },
                            {
                                $set: { spent: spent.block }
                            }
                        );
                    });

                    return Promise.all(promises);
                });
            });

        return this.setupTransactionFinisher(promise, session);
    }

    /**
     * Update top n and record to mongo db
     */
    update() {
        // start transaction
        let session = this.mongo.startSession();
        session.startTransaction();

        let promise = this.utxos
            .aggregate([
                { $match: { spent: false } },
                {
                    $group: {
                        _id: '$address',
                        balance: { $sum: '$satoshis' }
                    }
                },
                { $sort: { balance: -1, _id: 1 } }
            ])
            .toArray()
            .then(balances => {
                // prepare tops
                let tops = balances.map(balance => ({
                    address: balance._id,
                    balance: balance.balance
                }));

                // delete all tops
                return this.tops.deleteMany().then(() => {
                    // insert all tops
                    return this.tops.insertMany(tops);
                });
            });

        return this.setupTransactionFinisher(promise, session);
    }

    setupTransactionFinisher(promise, session) {
        return promise
            .catch(err => {
                // abort transaction if above operations fail
                return session.abortTransaction().then(() => {
                    throw err;
                });
            })
            .then(() => {
                return session.commitTransaction();
            })
            .then(() => {
                session.endSession();
            })
            .catch(err => {
                session.endSession();
                throw err;
            });
    }
}

module.exports = MongoConnector;
