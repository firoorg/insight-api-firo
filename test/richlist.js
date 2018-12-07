'use strict';

var should = require('should');
var sinon = require('sinon');
var RichListController = require('../lib/richlist/richlist');
var MongoConnector = require('../lib/richlist/mongoconnector');
var bitcore = require('zcore-lib');
var _ = require('lodash');

var chain1 = require('./data/richlist/chain1.json');
var chain2 = require('./data/richlist/chain2.json');
var transactions = require('./data/richlist/transactions.json');

var copy_chain = (obj)=>{
    return JSON.parse(JSON.stringify(obj));
}

var blocks = copy_chain(chain1);

describe('Rich List',function(){
    describe('softfolk',function(){

        var getblock_f = (idx,callback)=>{
            for(var i=0;i<blocks.length;i++){
                if(idx === blocks[i].hash || idx === blocks[i].height ){
                    callback(null, blocks[i]);
                    return;
                }
            }
            callback(new Error("out of index"),null);
        }
        
        var node = {
            
            getBestBlockHash: function(callback){
                callback(null,blocks[blocks.length-1].hash);
            },
            getBlockHeader: getblock_f,
            getBlockOverview: getblock_f,
            getBlock: getblock_f,
            getDetailedTransaction: function(id,callback){
                for(var i=0;i<transactions.length;i++)
                    if(id === transactions[i].hash)
                        callback(null, transactions[i]);
            },
            services:{
                bitcoind:{
                    subscribe:(ch,e)=>{
                        node.zmq_block_emitter = e;
                        node.zmq_block_ch = ch;
                    }
                }
            },
            send_zmq_blockhash:(h)=>{
                node.zmq_block_emitter.emit("bitcoind/hashblock",h);
            }
        }

        // var mongoConn = new MongoConnector();
        // mongoConn.cleandb();

        it('test', function(done){
            // var controller = new RichListController({node:node,conn:mongoConn});
            var controller = new RichListController({node:node});
            var test_step = 0;
            var res = {
                status:(s)=>{
                    setTimeout(res.call,100);
                },
                call:()=>{
                    controller.list(null,res);
                },
                jsonp:(r)=>{
                    if(test_step===0){
                        r.length.should.equal(2);
                        r[0].address.should.equal("aGm5jzFHLCt4pQLGwoHD2WijDmqbXfeWCz");
                        r[0].balance.should.equal((80).toFixed(8) );

                        r[1].address.should.equal("a1kCCGddf5pMXSipLVD9hBG2MGGVNaJ15U");
                        r[1].balance.should.equal((4).toFixed(8));

                        // invalidate a block
                        blocks.pop();

                        test_step++;

                        node.getBestBlockHash((err,h)=>{
                            node.send_zmq_blockhash(h);
                        });

                        res.call();
            
                    }else if(test_step===1){
                        r[0].address.should.equal("a9hZRxDCTomprkk4ajNUbGCGJbTTnXNcR5");
                        r[0].balance.should.equal((40).toFixed(8) );

                        r[1].address.should.equal("aNUjTa4XLrCpRL5hqJf8Y4T6Cn3pZLLRUH");
                        r[1].balance.should.equal((40).toFixed(8));

                        r[2].address.should.equal("a1kCCGddf5pMXSipLVD9hBG2MGGVNaJ15U");
                        r[2].balance.should.equal((4).toFixed(8));

                        // confirm old chain
                        blocks = copy_chain(chain1);

                        test_step++;

                        node.getBestBlockHash((err,h)=>{
                            node.send_zmq_blockhash(h);
                        });

                        res.call();
                    }else if(test_step===2){
                        r.length.should.equal(2);
                        r[0].address.should.equal("aGm5jzFHLCt4pQLGwoHD2WijDmqbXfeWCz");
                        r[0].balance.should.equal((80).toFixed(8) );

                        r[1].address.should.equal("a1kCCGddf5pMXSipLVD9hBG2MGGVNaJ15U");
                        r[1].balance.should.equal((4).toFixed(8));

                        // soft folk to chain2
                        blocks = copy_chain(chain2);

                        test_step++;

                        node.getBestBlockHash((err,h)=>{
                            node.send_zmq_blockhash(h);
                        });

                        res.call();
                    }else if(test_step===3){
                        r[0].address.should.equal("a9hZRxDCTomprkk4ajNUbGCGJbTTnXNcR5");
                        r[0].balance.should.equal((40).toFixed(8) );

                        r[1].address.should.equal("aGm5jzFHLCt4pQLGwoHD2WijDmqbXfeWCz");
                        r[1].balance.should.equal((40).toFixed(8));

                        r[2].address.should.equal("a1kCCGddf5pMXSipLVD9hBG2MGGVNaJ15U");
                        r[2].balance.should.equal((4).toFixed(8));

                        done();
                    }
                },
            };
            node.getBestBlockHash((err,h)=>{
                node.send_zmq_blockhash(h);
            });

            res.call();
        });

    });
});
