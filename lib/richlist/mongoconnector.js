const mongodb = require('mongodb');
const Decimal = require('decimal.js');

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
                this.blocks = this.mongo.db().collection('blocks');
                this.balances = this.mongo.db().collection('balances');
                this.outputs = this.mongo.db().collection('outputs');
            })
            .then(() => {
                // create index of block height
                return this.blocks.createIndex({ height: 1 });
            })
            .then(() => {
                // create balances index
                return this.balances.createIndex(
                    [['balance', 1], ['address', 1]],
                    { unique: 1 }
                );
            })
            .then(() => {
                return this.balances.createIndex([['address', 1]], {
                    unique: 1
                });
            })
            .then(() => {
                // create balances index
                return this.outputs.createIndex([['tx', 1], ['index', 1]], {
                    unique: 1
                });
            })
            .then(() => {
                return this.outputs.createIndex([['block', 1]], {
                    sparse: true
                });
            })
            .then(() => {
                return this.outputs.createIndex([['spent', 1]], {
                    sparse: true
                });
            });
    }

    /**
     * Cleanup db
     */
    cleandb() {
        return this.blocks
            .deleteMany()
            .then(() => {
                return this.balances.deleteMany();
            })
            .then(() => {
                return this.outputs.deleteMany();
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
            .find({
                balance: { $ne: 0 }
            })
            .sort({ balance: -1, address: 1 })
            .limit(n)
            .toArray()
            .then(balances => {
                return balances.map(balance => ({
                    address: balance.address,
                    balance: balance.balance
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

        // get latest block
        let promise = this.blocks
            .find()
            .sort({ heigh: -1 })
            .limit(1)
            .toArray()
            .then(blocks => {
                if (!blocks || blocks.length <= 0) {
                    throw new Error('no block available');
                }

                return blocks[0];
            })
            .then(block => {
                // get all unspent outputs belong to the latest block
                return this.outputs
                    .find({ block: block.hash })
                    .toArray()
                    .then(outputs => ({ block, outputs }));
            })
            .then(({ block, outputs }) => {
                // get all spent outputs belong to the latest block
                return this.outputs
                    .find({ spent: block.hash })
                    .toArray()
                    .then(spents => ({ block, outputs, spents }));
            })
            .then(({ block, outputs, spents }) => {
                // invert all balance addition and subtraction for the latest block
                return this.updateBalances(
                    this.calculateFinalBalance(outputs, spents, true)
                ).then(() => block);
            })
            .then(block => {
                // delete latest block
                return this.blocks
                    .deleteOne({ heigh: block.heigh })
                    .then(() => block);
            })
            .then(block => {
                // update all outputs used as input on latest block to unspent
                return this.outputs
                    .updateMany(
                        { spent: block.hash },
                        { $unset: { spent: '' } }
                    )
                    .then(() => block);
            })
            .then(block => {
                // update all outputs to be uncreated
                return this.outputs
                    .updateMany(
                        { block: block.hash },
                        { $unset: { block: '' } }
                    )
                    .then(() => block);
            });

        return this.setupTransactionFinisher(promise, session);
    }

    /**
     * Insert new block to local
     */
    insert_block(block) {
        // parse all txs in a block
        let outputs = [];
        let inputs = [];

        block.txs.forEach(tx => {
            // parse all vouts
            for (let i = 0; i < tx.outputs.length; i++) {
                let address = tx.outputs[i].address;
                if (!address) {
                    // ignore mint
                    continue;
                }

                // satoshi is smallest unit then make it to integer
                // FIXME: investigate why sometime it is floating point
                let balance = Math.round(tx.outputs[i].satoshis);
                if (!balance) {
                    continue;
                }

                outputs.push({
                    tx: tx.hash,
                    index: i,
                    block: block.hash,
                    address: address,
                    satoshis: balance
                });
            }

            // read all vins
            tx.inputs.forEach(vin => {
                // skip if it come from coinbase
                if (!vin.prevTxId) {
                    return;
                }

                // skip if it come from zerocoin spent
                if (
                    vin.prevTxId ===
                    '0000000000000000000000000000000000000000000000000000000000000000'
                ) {
                    return;
                }

                let balance = Math.round(vin.satoshis);
                if (!balance) {
                    return;
                }
                inputs.push({
                    tx: vin.prevTxId,
                    index: vin.outputIndex,
                    address: vin.address,
                    satoshis: balance,
                    spent: block.hash
                });
            });
        });

        // start transaction
        let session = this.mongo.startSession();
        session.startTransaction();

        // insert block
        let promise = this.blocks
            .insertOne({
                hash: block.hash,
                heigh: block.height
            })
            .then(() => {
                // update all new outputs create by this block
                return Promise.all(
                    outputs.map(output =>
                        this.outputs.updateOne(
                            {
                                tx: output.tx,
                                index: output.index
                            },
                            { $set: output },
                            {
                                upsert: 1
                            }
                        )
                    )
                );
            })
            .then(() => {
                // mark outputs that spent by this block
                return Promise.all(
                    inputs.map(input => {
                        this.outputs.updateOne(
                            {
                                tx: input.tx,
                                index: input.index
                            },
                            { $set: input },
                            { upsert: 1 }
                        );
                    })
                );
            })
            .then(() => {
                return this.updateBalances(
                    this.calculateFinalBalance(outputs, inputs)
                );
            });

        return this.setupTransactionFinisher(promise, session);
    }

    updateBalances(changes) {
        let addresses = Object.keys(changes);

        let promises = addresses.map(addr =>
            this.balances.updateOne(
                { address: addr },
                {
                    $inc: {
                        balance: mongodb.Long.fromString(
                            changes[addr].toString()
                        )
                    }
                },
                { upsert: true }
            )
        );

        return Promise.all(promises);
    }

    setupTransactionFinisher(promise, session) {
        return promise
            .catch(err => {
                // abort transaction if previous operations fail
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

    getOutputs(criteria) {
        let promises = criteria.map(criteria => {
            return this.outputs.findOne({
                tx: criteria.tx,
                index: criteria.index
            });
        });

        return Promise.all(promises);
    }

    calculateFinalBalance(outputs, spents, inverse = false) {
        let balances = {};

        let update = (output, subtract) => {
            let address = output.address;
            let balance = output.satoshis;

            if (!balances[address]) {
                balances[address] = new Decimal(0);
            }

            balances[address] =
                subtract ^ inverse
                    ? balances[address].minus(balance)
                    : balances[address].add(balance);
        };

        outputs.forEach(output => {
            update(output, false);
        });

        spents.forEach(output => {
            update(output, true);
        });

        return balances;
    }
}

module.exports = MongoConnector;
