var async = require('async');
var bitcore = require('zcore-lib');
var _ = bitcore.deps._;
var pools = require('../../pools.json');
var BN = bitcore.crypto.BN;
var LRU = require('lru-cache');
var Common = require('../common');
var Transactions = require('../transactions');

const mongodb = require('mongodb');

const Decimal = require('decimal.js');

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
                return this.blocks.createIndex(
                    [['utxos.tx', 1], ['utxos.index', 1]],
                    {
                        unique: 1
                    }
                );
            })
            .then(() => {
                // create index of block height
                return this.blocks.createIndex({ height: 1 });
            })
            .then(() => {
                // create spent index
                return this.blocks.createIndex({ 'utxos.spent': 1 });
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
     * Remove latest block from blocks array and update balance
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
                return this.blocks.find({ hash: hash }).toArray();
            })
            .then(blocks => {
                // get change for spent
                if (!blocks || blocks.length <= 0) {
                    throw new Error('no block available');
                }

                let block = blocks[0];

                // add back balance from spents
                block.spents.forEach(spent => {
                    let address = spent.address;
                    let satoshis = spent.satoshis;

                    if (!changes[address]) {
                        changes[address] = Decimal(0);
                    }

                    changes[address] = changes[address].plus(satoshis);
                });

                // subtract back balance  from utxos
                block.utxos.forEach(utxo => {
                    let address = utxo.address;
                    let satoshis = utxo.satoshis;

                    if (!changes[address]) {
                        changes[address] = Decimal(0);
                    }

                    changes[address] = changes[address].minus(satoshis);
                });

                return block.hash;
            })
            .then(hash => {
                // delete latest block
                return this.blocks.deleteOne({ hash: hash });
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

        // get new utxos and new spent utxos
        let outputs = [];
        let inputs = [];
        let changes = {};

        block.txs.forEach(tx => {
            // read all txs in blocks
            for (let i = 0; i < tx.outputs.length; i++) {
                // read all vouts

                let address = tx.outputs[i].address;
                if (!address) {
                    // ignore mint
                    continue;
                }

                // satoshi is smallest unit then make it to integer
                let balance = Math.round(tx.outputs[i].satoshis);
                if (!balance) {
                    continue;
                }

                outputs.push({
                    tx: tx.hash,
                    block: block.hash,
                    index: i,
                    address: address,
                    satoshis: balance
                });

                // add to balance
                if (!changes[address]) {
                    changes[address] = new Decimal(0);
                }
                changes[address] = changes[address].plus(balance);
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

        // get all spents transaction
        let promises = inputs.map(spent => {
            return this.blocks
                .find({
                    // get spent utxo
                    utxos: {
                        $elemMatch: {
                            tx: spent.tx,
                            index: spent.index
                        }
                    }
                })
                .project({ 'utxos.$.': 1 })
                .toArray()
                .then(blocks => {
                    let utxos = [];

                    // get spent utxo
                    // from currect block
                    outputs.forEach(output => {
                        if (
                            output.tx === spent.tx &&
                            output.index === spent.index
                        ) {
                            utxos.push(output);
                        }
                    });

                    // from previous block
                    blocks.forEach(block => {
                        block.utxos.forEach(utxo => {
                            if (
                                utxo.tx === spent.tx &&
                                utxo.index === spent.index
                            ) {
                                utxos.push(utxo);
                            }
                        });
                    });

                    return utxos;
                });
        });

        let promise = Promise.all(promises)
            .then(utxosArray => {
                // flatten
                let spent_utxos = [];
                utxosArray.forEach(utxos => {
                    utxos.forEach(utxo => {
                        spent_utxos.push(utxo);
                    });
                });
                return spent_utxos;
            })
            .then(utxos => {
                // update change for each spent utxo
                utxos.forEach(utxo => {
                    if (!utxo) {
                        return;
                    }
                    let address = utxo.address;
                    if (!address) {
                        return;
                    }

                    let balance = Math.round(utxo.satoshis);
                    if (!balance) {
                        return;
                    }

                    // subtract from balance
                    if (!changes[address]) {
                        changes[address] = new Decimal(0);
                    }
                    changes[address] = changes[address].minus(balance);
                });
                return utxos;
            })
            .then(utxos => {
                // store to blocks collection
                return this.blocks.insertOne({
                    hash: block.hash,
                    heigh: block.height,
                    utxos: outputs,
                    spents: utxos
                });
            })
            .then(() => {
                return this.update(changes, session);
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
            .map(address => {
                return {
                    address: address,
                    change: mongodb.Decimal128.fromString(
                        changes[address].round().toString()
                    )
                };
            })
            .map(change => {
                // update balance change each address
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
