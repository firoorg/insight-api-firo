'use strict';

var async = require('async');
var bitcore = require('zcore-lib');
var _ = bitcore.deps._;
var pools = require('../../pools.json');
var BN = bitcore.crypto.BN;
var LRU = require('lru-cache');
var Common = require('../common');
var Transactions = require('../transactions');
var MemConnector = require('./memconnector');

var createTree = require("functional-red-black-tree");
const EventEmitter = require('events');

var ErrNotImplement = new Error("not implemented");

/**
 * 
 * @param {json} options require node and conn is optional
 */
function RichListController(options) {
    var self = this;

    this.node = options.node;
    this._scanning = false;

    if(options.conn){
        this.conn = options.conn;
    }else{
        this.conn = new MemConnector();
    }

    this.conn.init();

    var blockEmitter = new EventEmitter();
    blockEmitter.on('bitcoind/hashblock',(h)=>{
        console.log('emit',h);
        if(!this._scanning)
            self._consider(h);
    });

    this.node.services.bitcoind.subscribe('hashblock',blockEmitter);

    // init
    // self.node.getBestBlockHash(function(err,h){
    //     self._consider(h);
    // });
}

/**
 * Get top 100 user order by balances
 */
RichListController.prototype.list = function (req,res) {
    var self = this;
    self.node.getBestBlockHash(function(err,h){
        if(h===self.conn.bestBlock()){
            res.jsonp(self.conn.get_top(100));
        }else{
            res.status(503);
        }
    });
};

/**
 * Check heighest local block with global and consider to update local data
 */
RichListController.prototype._consider = function(bestHash){
    var self = this;
    this._scanning = true;

    var local_best = self.conn.bestBlock();
    if(local_best === bestHash){
        this._scanning = false;
        return;
    }

    self.node.getBlockHeader(local_best,function(err,h){

        var next = function(){
            self._consider(bestHash);
        }

        // if not found invalidate best block
        if(err){
            self.conn.invalidate(next);
        }else{
            if(local_best===0){
                h = {
                    height:0,
                }
            }

            self.node.getBlockHeader(h.height+1,function(err,next_h){
                    self._new(next_h.hash,next);
            });
        }
        
    });
}

/**
 * Append new local block
 */
RichListController.prototype._new = function (blockid,callback){
    var self = this;

    self.node.getBlockOverview(blockid,function(err,block){
        var ids = block.txids;
        async.mapSeries(ids, function (txid, next) {
            self.node.getDetailedTransaction(txid, function (err, transaction) {
                if (err) {
                    return next(err);
                }
                next(null,transaction);
            });
        }, function (err, txs) {
            if(err){
                throw new Error("get transaction error");
            }

            block.txs = txs;
            self.conn.insert_block(block);
            callback();
        });
    });
}


module.exports = RichListController;
