var async = require('async');
var bitcore = require('zcore-lib');
var _ = bitcore.deps._;
var pools = require('../../pools.json');
var BN = bitcore.crypto.BN;
var LRU = require('lru-cache');
var Common = require('../common');
var Transactions = require('../transactions');

var ErrNotImplement = new Error('not implemented');

const MongoClient = require('mongodb').MongoClient;
const test = require('assert');

// Collection
const _COL_UTXOS = 'utxos';
const _COL_BLOCK = 'blocks';

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
                this.utxos = this.mongo.db().collection(_COL_UTXOS);
                this.blocks = this.mongo.db().collection(_COL_BLOCK);
            })
            .then(() => {
                // create uniqued index of tx id and vouts index
                this.utxos.createIndex({ tx: 1, index: 1 }, { unique: 1 });
            })
            .then(() => {
                // create tx id index
                this.utxos.createIndex({ tx: 1 });
            })
            .then(() => {
                // create index of block height
                this.blocks.createIndex({ height: 1 });
            });
    }

    /**
     * Cleanup db
     */
    cleandb() {
        // delete all blocks
        return this.blocks.deleteMany().then(() => {
            // delete all utxos
            return this.utxos.deleteMany();
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
        // get sum of balance of each address that never have been spent
        return this.utxos
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
            .limit(n)
            .toArray()
            .then(balances => {
                // satoshis to xzc
                balances = balances.map(balance => {
                    return {
                        address: balance._id,
                        balance: (balance.balance / 1e8).toFixed(8)
                    };
                });

                return balances;
            });
    }

    /**
     * Remove lastest block from blocks array and update balance
     */
    invalidate() {
        // get heighest block hash
        return this.bestBlock()
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
    }

    /**
     * Insert new block to local
     */
    insert_block(block) {
        // get blocks collection
        return this.blocks
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

                // create promises array to update spent tx
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

                // add new utxos
                return this.utxos.insertMany(outputs).then(() => {
                    // update all spent utxos
                    return Promise.all(promises);
                });
            });
    }
}

module.exports = MongoConnector;
