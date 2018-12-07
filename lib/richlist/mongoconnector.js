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
// Connection url
const url = 'mongodb://localhost:27017';
// Database Name
const dbName = 'test';

// Collection
const _COL_UTXOS = 'utxos';
const _COL_BLOCK = 'blocks';

// Connector
class MongoConnector {
    /**
     * Initialize connector
     */
    constructor() {
        this._conn_db((err, client, db) => {
            db.collection(_COL_UTXOS).createIndex(
                { tx: 1, index: 1 },
                { unique: true },
                err => {
                    db.collection(_COL_UTXOS).createIndex({ tx: 1 }, err => {});
                }
            );
        });
    }

    /**
     * Cleanup db
     */
    cleandb() {
        if (this._inited) return;
        this._conn_db((err, client, db) => {
            db.collection(_COL_UTXOS).deleteMany();
            db.collection(_COL_BLOCK).deleteMany();
            client.close();
        });
    }

    /**
     * Get local best block
     */
    bestBlock(callback) {
        this._conn_db((err, client, db) => {
            var col = db.collection(_COL_BLOCK);
            col.find()
                .sort({ height: -1 })
                .limit(1)
                .toArray((err, arr) => {
                    client.close();
                    callback(null, !arr || arr.length <= 0 ? 0 : arr[0].hash);
                });
        });
    }

    /**
     * Get top n address order by balances
     */
    get_top(n, callback) {
        this._conn_db((err, client, db) => {
            var col = db.collection(_COL_UTXOS);
            col.aggregate([
                { $match: { spent: false } },
                { $group: { _id: '$address', balance: { $sum: '$satoshis' } } },
                { $sort: { balance: -1, _id: 1 } }
            ])
                .limit(n)
                .toArray((err, result) => {
                    client.close();
                    callback(
                        err,
                        result.map(r => {
                            return {
                                address: r._id,
                                balance: (r.balance / 1e8).toFixed(8)
                            };
                        })
                    );
                    // console.log(err,result);
                });
        });
        // callback(null,res);
    }

    /**
     * Remove lastest block from blocks array and update balance
     */
    invalidate(callback) {
        this._conn_db((err, client, db) => {
            var col = db.collection(_COL_UTXOS);
            var block_col = db.collection(_COL_BLOCK);
            block_col
                .find()
                .sort({ height: -1 })
                .toArray((err, block) => {
                    var hash = block[0].hash;
                    block_col.deleteMany({ hash: hash }, (err, r) => {
                        col.deleteMany({ block: hash }, (err, r1) => {
                            col.updateMany(
                                { spent: hash },
                                { $set: { spent: false } },
                                (err, r2) => {
                                    callback(err);
                                }
                            );
                        });
                    });
                });
        });
    }

    /**
     * Insert new block to local
     */
    insert_block(block, callback) {
        var self = this;

        self._conn_db((err, client, db) => {
            var col = db.collection(_COL_UTXOS);
            var block_col = db.collection(_COL_BLOCK);
            var new_utxos = [];
            var new_spent = [];
            for (var tx_i = 0; tx_i < block.txs.length; tx_i++) {
                var tx = block.txs[tx_i];
                for (var out_i = 0; out_i < tx.outputs.length; out_i++)
                    new_utxos.push({
                        tx: tx.hash,
                        block: block.hash,
                        index: out_i,
                        address: tx.outputs[out_i].address,
                        satoshis: tx.outputs[out_i].satoshis,
                        spent: false
                    });
                for (var in_i = 0; in_i < tx.inputs.length; in_i++) {
                    var inp = tx.inputs[in_i];
                    if (!inp.prevTxId) continue;
                    new_spent.push({
                        tx: inp.prevTxId,
                        block: block.hash,
                        index: inp.outputIndex
                    });
                }
            }

            block_col.insertOne(
                {
                    hash: block.hash,
                    height: block.height
                },
                (err, result) => {
                    if (!err) {
                        col.insertMany(new_utxos, (err, result) => {
                            async.mapSeries(
                                new_spent,
                                (spent, next) => {
                                    col.updateMany(
                                        {
                                            tx: spent.tx,
                                            index: spent.index
                                        },
                                        {
                                            $set: { spent: spent.block }
                                        }
                                    );
                                    next();
                                },
                                (err, results) => {
                                    client.close();
                                    callback(err);
                                }
                            );
                        });
                    } else {
                        client.close();
                        callback(err);
                    }
                }
            ); // insert block
        }); // conn db
    }

    _conn_db(callback) {
        // Use connect method to connect to the server
        MongoClient.connect(
            url + '/' + dbName,
            { useNewUrlParser: true },
            function(err, client) {
                if (err) throw err;

                const db = client.db(dbName);
                callback(err, client, db);
            }
        );
    }
} // class

module.exports = MongoConnector;
