'use strict';

const EventEmitter = require('events');
const should = require('should');
const RichListController = require('../../lib/richlist/controller');
const RichListStorage = require('../../lib/richlist/storage/mongo');
const MongoClient = require('mongodb').MongoClient;

class FakeBitcoinService extends EventEmitter {
}

describe('RichListController', function() {
    let mongo = new MongoClient('mongodb://localhost:27017/insight_zcoin_test', { useNewUrlParser: true });
    let bitcoind = undefined;
    let storage = undefined;
    let controller = undefined;
    let blocks = undefined;
    let transactions = undefined;

    before(function() {
        return mongo.connect();
    });

    beforeEach(function() {
        blocks = {
            0: {
                hash: '4381deb85b1b2c9843c222944b616d997516dcbd6a964e1eaf0def0830695233',
                height: 0,
                time: 1414776286,
                medianTime: 1414776286,
                nonce: 142392,
                difficulty: 0.000244140625,
                chainWork: '0000000000000000000000000000000000000000000000000000000000100010',
                bits: '1e0ffff0',
                version: 2,
                nextHash: 'c0c53331e3d96dbe4a20976196c0a214124bef9a7829df574f00f4e5a1b7ae52'
            },
            'c0c53331e3d96dbe4a20976196c0a214124bef9a7829df574f00f4e5a1b7ae52': {
                hash: 'c0c53331e3d96dbe4a20976196c0a214124bef9a7829df574f00f4e5a1b7ae52',
                height: 1,
                time: 1475020813,
                medianTime: 1475020813,
                nonce: 3260,
                difficulty: 0.000244140625,
                chainWork: '0000000000000000000000000000000000000000000000000000000000200020',
                bits: '1e0ffff0',
                version: 65538,
                txids: [
                    '98f7ecc5b17fa795ceb45809918e726d50a42fdb9207f40d8a0fe0dcf0f57b70'
                ]
            }
        };
        transactions = {
            '98f7ecc5b17fa795ceb45809918e726d50a42fdb9207f40d8a0fe0dcf0f57b70': {
                hash: '98f7ecc5b17fa795ceb45809918e726d50a42fdb9207f40d8a0fe0dcf0f57b70',
                locktime: 0,
                version: 1,
                inputs: [
                    {
                        prevTxId: undefined
                    }
                ],
                outputs: [
                    {
                        address: 'aEF2p3jepoWF2yRYZjb6EACCP4CaP41doV',
                        satoshis: 40 * 1e8
                    },
                    {
                        address: 'aCAgTPgtYcA4EysU4UKC86EQd5cTtHtCcr',
                        satoshis: 2 * 1e8
                    },
                    {
                        address: 'aLrg41sXbXZc5MyEj7dts8upZKSAtJmRDR',
                        satoshis: 2 * 1e8
                    },
                    {
                        address: 'aQ18FBVFtnueucZKeVg4srhmzbpAeb1KoN',
                        satoshis: 2 * 1e8
                    },
                    {
                        address: 'a1HwTdCmQV3NspP2QqCGpehoFpi8NY4Zg3',
                        satoshis: 2 * 1e8
                    },
                    {
                        address: 'a1kCCGddf5pMXSipLVD9hBG2MGGVNaJ15U',
                        satoshis: 2 * 1e8
                    }
                ]
            }
        };

        bitcoind = new FakeBitcoinService();
        storage = new RichListStorage({ mongo: mongo });
        controller = new RichListController({
            storage,
            node: {
                log: {
                    info: function() {
                    }
                },
                services: { bitcoind },
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
            blocks['c0c53331e3d96dbe4a20976196c0a214124bef9a7829df574f00f4e5a1b7ae52'].nextHash = 'bdf3fe560c2a65f563111afa39247fc2584fc9315118f86a9c9e2f93f974bace';

            blocks['bdf3fe560c2a65f563111afa39247fc2584fc9315118f86a9c9e2f93f974bace'] = {
                hash: 'bdf3fe560c2a65f563111afa39247fc2584fc9315118f86a9c9e2f93f974bace',
                height: 2,
                time: 1475020836,
                medianTime: 1475020813,
                nonce: 5003,
                difficulty: 0.000244140625,
                chainWork: '0000000000000000000000000000000000000000000000000000000000300030',
                bits: '1e0ffff0',
                version: 65538,
                txids: [
                    '3b1cc7daa8e866c5dd6e2d9a79470379f1fc9470156e6ec3d76c67fced99d230'
                ]
            };

            transactions['3b1cc7daa8e866c5dd6e2d9a79470379f1fc9470156e6ec3d76c67fced99d230'] = {
                hash: '3b1cc7daa8e866c5dd6e2d9a79470379f1fc9470156e6ec3d76c67fced99d230',
                locktime: 0,
                version: 1,
                inputs: [
                    {
                        prevTxId: '98f7ecc5b17fa795ceb45809918e726d50a42fdb9207f40d8a0fe0dcf0f57b70',
                        outputIndex: 0,
                        address: 'aEF2p3jepoWF2yRYZjb6EACCP4CaP41doV',
                        satoshis: 40 * 1e8
                    }
                ],
                outputs: [
                    {
                        address: 'aEF2p3jepoWF2yRYZjb6EACCP4CaP41doV',
                        satoshis: 30 * 1e8
                    },
                    {
                        address: 'aCAgTPgtYcA4EysU4UKC86EQd5cTtHtCcr',
                        satoshis: 10 * 1e8
                    }
                ]
            };

            controller.once('latestBlockScanned', function() {
                // new chain
                delete blocks['bdf3fe560c2a65f563111afa39247fc2584fc9315118f86a9c9e2f93f974bace'];
                delete transactions['3b1cc7daa8e866c5dd6e2d9a79470379f1fc9470156e6ec3d76c67fced99d230'];

                blocks['c0c53331e3d96dbe4a20976196c0a214124bef9a7829df574f00f4e5a1b7ae52'].nextHash = '2663970914b4e4617e68955147651758b0626c8cd27070d1a15a2b952bf88ae4';
                blocks['2663970914b4e4617e68955147651758b0626c8cd27070d1a15a2b952bf88ae4'] = {
                    hash: '2663970914b4e4617e68955147651758b0626c8cd27070d1a15a2b952bf88ae4',
                    height: 2,
                    time: 1475020836,
                    medianTime: 1475020813,
                    nonce: 5003,
                    difficulty: 0.000244140625,
                    chainWork: '0000000000000000000000000000000000000000000000000000000000300030',
                    bits: '1e0ffff0',
                    version: 65538,
                    txids: [
                        '3519bbba67a5abad88a041aff470699bb55b8b7bee32b8856055078720da54f6'
                    ]
                };

                transactions['3519bbba67a5abad88a041aff470699bb55b8b7bee32b8856055078720da54f6'] = {
                    hash: '3519bbba67a5abad88a041aff470699bb55b8b7bee32b8856055078720da54f6',
                    locktime: 0,
                    version: 1,
                    inputs: [
                        {
                            prevTxId: '98f7ecc5b17fa795ceb45809918e726d50a42fdb9207f40d8a0fe0dcf0f57b70',
                            outputIndex: 0,
                            address: 'aEF2p3jepoWF2yRYZjb6EACCP4CaP41doV',
                            satoshis: 40 * 1e8
                        }
                    ],
                    outputs: [
                        {
                            address: 'aEF2p3jepoWF2yRYZjb6EACCP4CaP41doV',
                            satoshis: 20 * 1e8
                        },
                        {
                            address: 'aCAgTPgtYcA4EysU4UKC86EQd5cTtHtCcr',
                            satoshis: 20 * 1e8
                        }
                    ]
                };

                controller.once('latestBlockScanned', function() {
                    controller.list(undefined, {
                        jsonp: function(list) {
                            list.length.should.equal(6);
                            list[0].address.should.equal('aCAgTPgtYcA4EysU4UKC86EQd5cTtHtCcr');
                            list[0].balance.should.equal(22 * 1e8);
                            list[1].address.should.equal('aEF2p3jepoWF2yRYZjb6EACCP4CaP41doV');
                            list[1].balance.should.equal(20 * 1e8);
                            list[2].address.should.equal('a1HwTdCmQV3NspP2QqCGpehoFpi8NY4Zg3');
                            list[2].balance.should.equal(2 * 1e8);
                            list[3].address.should.equal('a1kCCGddf5pMXSipLVD9hBG2MGGVNaJ15U');
                            list[3].balance.should.equal(2 * 1e8);
                            list[4].address.should.equal('aLrg41sXbXZc5MyEj7dts8upZKSAtJmRDR');
                            list[4].balance.should.equal(2 * 1e8);
                            list[5].address.should.equal('aQ18FBVFtnueucZKeVg4srhmzbpAeb1KoN');
                            list[5].balance.should.equal(2 * 1e8);
                            done();
                        }
                    });
                });

                // parse updated blocks
                bitcoind.emit('block');
            });

            controller.init();
            bitcoind.emit('ready');
        });
    });
});
