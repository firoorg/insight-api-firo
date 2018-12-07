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

    self.node = options.node;
    self._scanning = false;

    if(options.conn){
        self.conn = options.conn;
    }else{
        self.conn = new MemConnector();
    }

    self.conn.init();

    var blockEmitter = new EventEmitter();
    blockEmitter.on('bitcoind/hashblock',(h)=>{
        if(!self._scanning)
            self._consider(h);
    });

    self.node.getBestBlockHash((err,h)=>{
        self._consider(h);
    });

    self.node.services.bitcoind.subscribe('hashblock',blockEmitter);
};

/**
 * Get top 100 user order by balances
 */
RichListController.prototype.list = function(req,res){
    var self = this;
    self.node.getBestBlockHash((err,h)=>{
        self.conn.bestBlock((err,local_h)=>{
            if(h===local_h){
                self.conn.get_top(100,(err,top_list)=>{
                    res.jsonp(top_list);
                });
            }else{
                res.status(503);
            }
        });
    });
};

/**
 * Check heighest local block with global and consider to update local data
 */
RichListController.prototype._consider = function(bestHash){
    var self = this;
    self._scanning = true;

    self.conn.bestBlock( (err,local_h) =>{

        if(local_h === bestHash){
            self._scanning = false;
            return;
        }

        self.node.getBlockHeader(local_h,(err,h)=>{

            var next = function(err){
                self._consider(bestHash);
            }

            // if not found invalidate best block
            if(err){
                self.conn.invalidate(next);
            }else{
                if(local_h===0){
                    h = {
                        height:0,
                    }
                }
                console.log('h',h.height)
                self.node.getBlockHeader(h.height+1,(err,next_h)=>{
                    self._new(next_h.hash,next);
                });
            }
            
        });
    });
};

/**
 * Append new local block
 */
RichListController.prototype._new = function(blockid,callback){
    var self = this;

    self.node.getBlockOverview(blockid,(err,block)=>{
        var ids = block.txids;
        async.mapSeries(ids, function (txid, next) {
            self.node.getDetailedTransaction(txid,  (err, transaction) => {
                if (err) {
                    return next(err);
                }
                next(null,transaction);
            });
        }, (err, txs) => {
            if(err){
                throw new Error("get transaction error");
            }

            block.txs = txs;
            self.conn.insert_block(block,err=>{
                callback();
            });
        });
    });
};


module.exports = RichListController;
