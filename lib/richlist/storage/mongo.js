'use strict';

const mongodb = require('mongodb');
const Decimal = require('decimal.js');

class MongoStorage {
    /**
     *
     * @param options.mongo is mongodb connection.
     */
    constructor(options) {
        this.mongo = options.mongo;
    }

    init() {
        // cache collections
        this.blocks = this.mongo.db().collection('blocks');
        this.transactions = this.mongo.db().collection('transactions');
        this.outputs = this.mongo.db().collection('outputs');
        this.balances = this.mongo.db().collection('balances');

        // create indices
        return this.blocks.createIndex(
            { height: -1 },
            { unique: 1 }
        ).then(() => this.transactions.createIndex(
            { hash: 1 },
            { unique: 1 }
        )).then(() => this.outputs.createIndex(
            [['tx', 1], ['index', 1]],
            { unique: 1 }
        )).then(() => this.outputs.createIndex(
            { spent: 1 },
            { sparse: true }
        )).then(() => this.balances.createIndex(
            [['balance', -1], ['address', 1]],
            { unique: 1 }
        )).then(() => this.balances.createIndex(
            { address: 1 },
            { unique: 1 }
        ));
    }

    /**
     * Cleanup db
     */
    cleandb() {
        return this.blocks.deleteMany().then(() => {
            return this.transactions.deleteMany();
        }).then(() => {
            return this.outputs.deleteMany();
        }).then(() => {
            return this.balances.deleteMany();
        });
    }

    /**
     * Get latest block hash.
     */
    getLatestBlock() {
        return this.blocks.find().sort({ height: -1 }).limit(1).toArray().then(blocks => {
            // return heighest block hash or undefined if no block available
            if (blocks && blocks.length > 0) {
                return blocks[0].hash;
            } else {
                return undefined;
            }
        });
    }

    /**
     * Get top n addresses ordered by highest balances.
     */
    getMostRichest(n) {
        return this.balances.find(
            { balance: { $ne: 0 } },
            { projection: { address: 1, balance: 1, _id: 0 } }
        ).sort({ balance: -1, address: 1 }).limit(n).toArray();
    }

    /**
     * Check if the specified transactions is already exists.
     *
     * @param {string[]} ids list of transaction identifiers to check.
     *
     * @returns {Promise<Set<string>>}
     */
    hasTransactions(ids) {
        let found = new Set();

        return this.transactions.find({ hash: { $in: ids }}).forEach(tx => found.add(tx.hash)).then(() => found);
    }

    /**
     * Remove latest block and update balances.
     */
    invalidateLatestBlock() {
        // get latest block
        return this.blocks.find().sort({ height: -1 }).limit(1).toArray().then(blocks => {
            if (!blocks || blocks.length <= 0) {
                throw new Error('no block available');
            }

            return blocks[0];
        }).then(block => {
            // get all unspent outputs belong to the latest block
            return this.outputs.find({ tx: { $in: block.transactions } }).toArray().then(outputs => ({ block, outputs }));
        }).then(({ block, outputs }) => {
            // get all spent outputs belong to the latest block
            return this.outputs.find({ spent: { $in: block.transactions } }).toArray().then(spents => ({ block, outputs, spents }));
        }).then(({ block, outputs, spents }) => {
            outputs = outputs || [];
            spents = spents || [];

            // invert all balance addition and subtraction for the latest block
            return this.updateBalances(this.calculateFinalBalance(outputs, spents, true)).then(() => block);
        }).then(block => {
            // update all outputs used as input on latest block to unspent
            return this.outputs.updateMany(
                { spent: { $in: block.transactions } },
                { $unset: { spent: '' } }
            ).then(() => block);
        }).then(block => {
            // delete all outputs belonged to the latest block
            return this.outputs.deleteMany({ tx: { $in: block.transactions } }).then(() => block);
        }).then(block => {
            // delete all transactions belonged to the latest block
            return this.transactions.deleteMany({ hash: { $in: block.transactions } }).then(() => block);
        }).then(block => {
            // delete latest block
            return this.blocks.deleteOne({ height: block.height });
        });
    }

    /**
     * Add a new block and update balances.
     */
    addBlock(block, txs) {
        // get vins and vouts from all transactions
        let outputs = [];
        let inputs = [];

        for (let tx of txs) {
            // gather all vouts to add balances
            for (let i = 0; i < tx.outputs.length; i++) {
                // we don't know which address that need to add balances if vout does not have it
                let address = tx.outputs[i].address;
                if (!address) {
                    continue;
                }

                // FIXME: investigate why sometime it is floating point
                let balance = Math.round(tx.outputs[i].satoshis);
                if (!balance) {
                    continue;
                }

                outputs.push({
                    tx: tx.hash,
                    index: i,
                    address: address,
                    satoshis: balance
                });
            }

            // gather all vins to subtract balances
            for (let vin of tx.inputs) {
                // coinbase does not need to subtract from balances
                if (!vin.prevTxId) {
                    continue;
                }

                // for zeroicoin spend we don't know what address that need to subtract balance
                if (vin.prevTxId === '0000000000000000000000000000000000000000000000000000000000000000') {
                    continue;
                }

                // FIXME: investigate why sometime it is floating point
                let balance = Math.round(vin.satoshis);
                if (!balance) {
                    continue;
                }

                inputs.push({
                    tx: tx.hash,
                    outputTx: vin.prevTxId,
                    outputIndex: vin.outputIndex,
                    address: vin.address,
                    satoshis: balance
                });
            }
        }

        const insertManyCollection = (collection, data) => {
            if(data.length === 0) {
                return Promise.resolve()
            }
            // insertMany fails when data is empty array 
            // (node:1) UnhandledPromiseRejectionWarning: MongoError: Invalid Operation, no operations specified
            return collection.insertMany(data)
        }

        // insert documents
        return insertManyCollection(this.transactions, txs.map(tx => ({
            hash: tx.hash,
            locktime: tx.locktime,
            version: tx.version
        }))).then(() => {
            return insertManyCollection(this.outputs, outputs);
        }).then(() => this.blocks.insertOne({
            hash: block.hash,
            height: block.height,
            time: block.time,
            medianTime: block.medianTime,
            nonce: block.nonce,
            difficulty: block.difficulty,
            chainWork: block.chainWork,
            bits: parseInt(block.bits, 16),
            transactions: txs.map(tx => tx.hash),
            version: block.version
        })).then(() => {
            // mark all outputs that spend by this block
            let promises = inputs.map(input => this.outputs.updateOne(
                { tx: input.outputTx, index: input.outputIndex },
                { $set: { spent: input.tx } }
            ));

            return Promise.all(promises);
        }).then(() => this.updateBalances(this.calculateFinalBalance(outputs, inputs)));
    }

    updateBalances(changes) {
        let addresses = Object.keys(changes);

        let promises = addresses.map(addr => this.balances.updateOne(
            { address: addr },
            { $inc: { balance: mongodb.Long.fromString(changes[addr].toString()) } },
            { upsert: true }
        ));

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

            balances[address] = (subtract ^ inverse) ? balances[address].minus(balance) : balances[address].add(balance);
        };

        outputs.forEach(output => update(output, false));
        spents.forEach(spent => update(spent, true));

        return balances;
    }
}

module.exports = MongoStorage;
