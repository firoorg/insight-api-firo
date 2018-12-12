var async = require('async');
var bitcore = require('zcore-lib');
var _ = bitcore.deps._;
var pools = require('../../pools.json');
var BN = bitcore.crypto.BN;
var LRU = require('lru-cache');
var Common = require('../common');
var Transactions = require('../transactions');

const generationTX =
    '0000000000000000000000000000000000000000000000000000000000000000';

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
                this.balances = this.mongo.db().collection('balances');
            })
            .then(() => {
                // create uniqued index of tx id and vouts index
                return this.utxos.createIndex([['tx', 1], ['index', 1]], {
                    unique: 1
                });
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
            })
            .then(() => {
                // create balances index
                return this.balances.createIndex([
                    ['balance', 1],
                    ['address', 1]
                ]);
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
                // delete all balances
                return this.balances.deleteMany();
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
        return this.balances
            .find()
            .sort({ balance: -1, address: 1 })
            .limit(n)
            .toArray()
            .then(balances => {
                return balances.map(balance => ({
                    address: balance.address,
                    balance: (balance.balance / 1e8).toFixed(8)
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
        let changes = {};
        let promise = this.bestBlock()
            .then(hash => {
                // delete heighest block
                return this.blocks.deleteMany({ hash: hash }).then(() => hash);
            })
            .then(hash => {
                // get change for spent
                return this.utxos
                    .find({ spent: hash })
                    .toArray()
                    .then(spents => {
                        spents.forEach(spent => {
                            let address = spent.address;
                            let satoshis = spent.satoshis;

                            if (!changes[address]) {
                                changes[address] = 0;
                            }

                            changes[address] += satoshis;
                        });
                    })
                    .then(() => hash);
            })
            .then(hash => {
                // update all spent utxos in heighest block to unspent
                return this.utxos
                    .updateMany({ spent: hash }, { $set: { spent: false } })
                    .then(() => hash);
            })
            .then(hash => {
                // get change for utxos in this block
                return this.utxos
                    .find({ block: hash })
                    .toArray()
                    .then(spents => {
                        spents.forEach(spent => {
                            let address = spent.address;
                            let satoshis = spent.satoshis;

                            if (!changes[address]) {
                                changes[address] = 0;
                            }

                            changes[address] -= satoshis;
                        });
                        return hash;
                    });
            })
            .then(hash => {
                // delete all utxos in heighest block
                return this.utxos.deleteMany({ block: hash }).then(() => hash);
            })
            .then(() => {
                return this.update(changes, session);
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
                let changes = {};

                block.txs.forEach(tx => {
                    // read all txs in blocks
                    for (let i = 0; i < tx.outputs.length; i++) {
                        // read all vouts

                        let address = tx.outputs[i].address;
                        if (!tx.outputs[i].address) {
                            // ignore mint
                            continue;
                        }

                        let balance = tx.outputs[i].satoshis;

                        outputs.push({
                            tx: tx.hash,
                            block: block.hash,
                            index: i,
                            address: address,
                            satoshis: balance,
                            spent: false
                        });

                        // add balance change
                        if (!changes[address]) {
                            changes[address] = 0;
                        }
                        changes[address] += balance;
                    }

                    tx.inputs.forEach(vin => {
                        // read all vins
                        if (!vin.prevTxId || vin.prevTxId === generationTX) {
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
                return this.utxos
                    .insertMany(outputs)
                    .then(() => {
                        // mark previous utxos as spent
                        let promises = inputs.map(spent => {
                            if (!spent.tx) {
                                console.log('this is null');
                            }
                            let r = this.utxos
                                .findOneAndUpdate(
                                    {
                                        tx: spent.tx,
                                        index: spent.index
                                    },
                                    {
                                        $set: { spent: spent.block }
                                    }
                                )
                                .then(r => {
                                    if (!r.lastErrorObject.updatedExisting) {
                                        console.log(r);
                                        console.log(spent);
                                    }
                                    return r;
                                });

                            return r;
                        });

                        return Promise.all(promises).then(spents => {
                            spents.forEach(spent => {
                                if (!spent.lastErrorObject.updatedExisting) {
                                    // console.log(spent);
                                    return;
                                }
                                let address = spent.value.address;
                                let balance = spent.value.satoshis;

                                // add balance changes
                                if (!changes[address]) {
                                    changes[address] = 0;
                                }
                                changes[address] -= balance;
                            });

                            return changes;
                        });
                    })
                    .then(changes => {
                        // update balance changes
                        return this.update(changes, session);
                    })
                    .catch(err => {
                        throw err;
                    });
            });

        return this.setupTransactionFinisher(promise, session);
    }

    /**
     * Update top n and record to mongo db
     */
    update(changes, session = null) {
        // start transaction
        if (!session) {
            session = this.mongo.startSession();
            session.startTransaction();
        }

        let promises = Object.keys(changes)
            .map(address => ({
                address: address,
                change: changes[address]
            }))
            .map(change => {
                return this.balances.updateOne(
                    { address: change.address },
                    { $inc: { balance: change.change } },
                    {
                        upsert: true
                    }
                );
            });

        let promise = Promise.all(promises).then(r => {
            return this.balances.deleteMany({ balance: 0 });
        });

        if (!session) {
            return this.setupTransactionFinisher(promise, session);
        } else {
            return promise;
        }
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
