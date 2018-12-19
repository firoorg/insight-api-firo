'use strict';

const should = require('should');
const RichListController = require('../../lib/richlist/controller');
const RichListStorage = require('../../lib/richlist/storage/mongo');
const MongoClient = require('mongodb').MongoClient;

describe('RichListController', function() {
    let mongo = new MongoClient('mongodb://localhost:27017/insight_zcoin_test', { useNewUrlParser: true });
    let listeners = undefined;
    let storage = undefined;
    let controller = undefined;
    let blocks = undefined;
    let transactions = undefined;

    before(function() {
        return mongo.connect();
    });

    beforeEach(function() {
        listeners = {};
        blocks = {
            0: {
                height: 0,
                hash: '0000000000000000000000000000000000000000000000000000000000000000',
                nextHash: '0000000000000000000000000000000000000000000000000000000000000001'
            },
            '0000000000000000000000000000000000000000000000000000000000000001': {
                height: 1,
                hash: '0000000000000000000000000000000000000000000000000000000000000001',
                txids: [
                    '1000000000000000000000000000000000000000000000000000000000000000'
                ],
                nextHash: '0000000000000000000000000000000000000000000000000000000000000002'
            }
        };
        transactions = {
            '1000000000000000000000000000000000000000000000000000000000000000': {
                height: 1,
                hash: '1000000000000000000000000000000000000000000000000000000000000000',
                inputs: [
                    {
                        prevTxId: undefined
                    }
                ],
                outputs: [
                    {
                        address: 'aNUjTa4XLrCpRL5hqJf8Y4T6Cn3pZLLRUH',
                        satoshis: 50 * 1e8
                    }
                ]
            }
        };

        storage = new RichListStorage({ mongo: mongo });
        controller = new RichListController({
            storage,
            node: {
                services: {
                    bitcoind: {
                        subscribe: function(name, listener) {
                            let list = listeners[name] || [];
                            list.push(listener);
                            listeners[name] = list;
                        },
                        unsubscribe: function(name, listener) {
                            let list = listeners[name];
                            let index = list ? list.indexOf(listener) : undefined;
                            if (index !== undefined) {
                                list.splice(index, 1);
                            }
                        }
                    }
                },
                getBlockHeader: function(id, cb) {
                    let block = blocks[id];
                    if (block) {
                        cb(undefined, block);
                    } else {
                        cb(new Error('block not found'));
                    }
                },
                getBlockOverview: function(id, cb) {
                    let block = blocks[id];
                    if (block) {
                        cb(undefined, block);
                    } else {
                        cb(new Error('block not found'));
                    }
                },
                getDetailedTransaction: function(id, cb) {
                    let tx = transactions[id];
                    if (tx) {
                        cb(undefined, tx);
                    } else {
                        cb(new Error('transaction not found'));
                    }
                }
            }
        });

        return storage.init().then(() => storage.cleandb());
    });

    afterEach(function() {
        return controller.stop();
    });

    describe('#list()', function() {
        it('should handle softfork correctly', function(done) {
            // chain to be discarded
            blocks['0000000000000000000000000000000000000000000000000000000000000002'] = {
                height: 2,
                hash: '0000000000000000000000000000000000000000000000000000000000000002',
                txids: [
                    '1000000000000000000000000000000000000000000000000000000000000001'
                ]
            };

            transactions['1000000000000000000000000000000000000000000000000000000000000001'] = {
                height: 2,
                hash: '1000000000000000000000000000000000000000000000000000000000000001',
                inputs: [
                    {
                        prevTxId: '1000000000000000000000000000000000000000000000000000000000000000',
                        outputIndex: 0,
                        address: 'aNUjTa4XLrCpRL5hqJf8Y4T6Cn3pZLLRUH',
                        satoshis: 50 * 1e8
                    }
                ],
                outputs: [
                    {
                        address: 'aNUjTa4XLrCpRL5hqJf8Y4T6Cn3pZLLRUH',
                        satoshis: 44 * 1e8
                    },
                    {
                        address: 'a1kCCGddf5pMXSipLVD9hBG2MGGVNaJ15U',
                        satoshis: 5 * 1e8
                    }
                ]
            };

            controller.once('latestBlockScanned', function() {
                // new chain
                delete blocks['0000000000000000000000000000000000000000000000000000000000000002'];
                delete transactions['1000000000000000000000000000000000000000000000000000000000000001'];

                blocks['0000000000000000000000000000000000000000000000000000000000000001'].nextHash = '0000000000000000000000000000000000000000000000000000000000000003';
                blocks['0000000000000000000000000000000000000000000000000000000000000003'] = {
                    height: 2,
                    hash: '0000000000000000000000000000000000000000000000000000000000000003',
                    txids: [
                        '1000000000000000000000000000000000000000000000000000000000000002'
                    ]
                };

                transactions['1000000000000000000000000000000000000000000000000000000000000002'] = {
                    height: 2,
                    hash: '1000000000000000000000000000000000000000000000000000000000000002',
                    inputs: [
                        {
                            prevTxId: '1000000000000000000000000000000000000000000000000000000000000000',
                            outputIndex: 0,
                            address: 'aNUjTa4XLrCpRL5hqJf8Y4T6Cn3pZLLRUH',
                            satoshis: 50 * 1e8
                        }
                    ],
                    outputs: [
                        {
                            address: 'aNUjTa4XLrCpRL5hqJf8Y4T6Cn3pZLLRUH',
                            satoshis: 40 * 1e8
                        },
                        {
                            address: 'a9hZRxDCTomprkk4ajNUbGCGJbTTnXNcR5',
                            satoshis: 9 * 1e8
                        }
                    ]
                };

                controller.once('latestBlockScanned', function() {
                    controller.list(undefined, {
                        jsonp: function(list) {
                            list.length.should.equal(2);
                            list[0].address.should.equal('aNUjTa4XLrCpRL5hqJf8Y4T6Cn3pZLLRUH');
                            list[0].balance.should.equal(40 * 1e8);
                            list[1].address.should.equal('a9hZRxDCTomprkk4ajNUbGCGJbTTnXNcR5');
                            list[1].balance.should.equal(9 * 1e8);

                            done();
                        }
                    });
                });

                // parse updated blocks
                for (let listener of listeners['hashblock']) {
                    listener.emit('bitcoind/hashblock');
                }
            });

            controller.init();
        });
    });
});
