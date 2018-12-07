var async = require('async');
var bitcore = require('zcore-lib');
var _ = bitcore.deps._;
var pools = require('../../pools.json');
var BN = bitcore.crypto.BN;
var LRU = require('lru-cache');
var Common = require('../common');
var Transactions = require('../transactions');

var ErrNotImplement = new Error("not implemented");

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
function MongoConnector(){
}



/**
 * Initialize connector
 */
MongoConnector.prototype.init = function(){
    if(this._inited)return;
    this._conn_db((err,client,db)=>{
        db.collection(_COL_UTXOS).createIndex(
            {tx:1,index:1},
            {unique: true},(err)=>{
                db.collection(_COL_UTXOS).createIndex(
                    {tx:1},
                    (err)=>{

                    }
                )
            });
    });
}

/**
 * Cleanup db
 */
MongoConnector.prototype.cleandb = function(){
    if(this._inited)return;
    this._conn_db((err,client,db)=>{
        db.collection(_COL_UTXOS).deleteMany();
        db.collection(_COL_BLOCK).deleteMany();
        client.close();
    });
}

/**
 * Get local best block
 */
MongoConnector.prototype.bestBlock = function(callback){
    this._conn_db( (err,client,db)=>{
        var col = db.collection(_COL_BLOCK);
        col.find().sort({height:-1}).limit(1).toArray((err,arr)=>{
            client.close();
            callback(null,(!arr || arr.length <= 0 )?0:arr[0].hash);
        });
    });
}

/**
 * Get top n address order by balances
 */
MongoConnector.prototype.get_top = function(n,callback){
    this._conn_db((err,client,db)=>{
        var col = db.collection(_COL_UTXOS);
        col.aggregate([
            {$match:{spent:false}},
            {$group:{_id:"$address",balance:{$sum:"$satoshis"}}},
            {$sort:{balance:-1,_id:1}},
        ]).limit(n).toArray((err,result)=>{
            callback(err,result.map(r=>{
                return {
                    address:r._id,
                    balance:(r.balance/1e8).toFixed(8),
                }
            }));
            // console.log(err,result);
        });
    });
    // callback(null,res);
}

/**
 * Remove lastest block from blocks array and update balance
 */
MongoConnector.prototype.invalidate = function(callback){
    this._conn_db( (err,client,db)=>{
        var col = db.collection(_COL_UTXOS);
        var block_col = db.collection(_COL_BLOCK);
        block_col.find().sort({height:-1}).toArray((err,block)=>{
            var hash = block[0].hash;
            block_col.deleteMany({hash:hash},(err,r)=>{
                col.deleteMany({block:hash},(err,r1)=>{
                    col.updateMany(
                        {spent:hash},
                        {$set:{spent:false}},
                    (err,r2)=>{
                        callback(err);
                    })
                })
            });
        });
    });
}

/**
 * Insert new block to local
 */
MongoConnector.prototype.insert_block = function(block,callback){
    var self = this;

    self._conn_db((err,client,db)=>{
        var col = db.collection(_COL_UTXOS);
        var block_col = db.collection(_COL_BLOCK);
        var new_utxos = [];
        var new_spent = [];
        for(var tx_i=0;tx_i<block.txs.length;tx_i++){
            var tx = block.txs[tx_i];
            for(var out_i=0;out_i<tx.outputs.length;out_i++)
                new_utxos.push({
                    tx: tx.hash,
                    block: block.hash,
                    index: out_i,
                    address: tx.outputs[out_i].address,
                    satoshis: tx.outputs[out_i].satoshis,
                    spent: false,
                });
            for(var in_i=0;in_i<tx.inputs.length;in_i++){
                var inp = tx.inputs[in_i];
                if(!inp.prevTxId)
                    continue;
                new_spent.push({
                    tx: inp.prevTxId,
                    block: block.hash,
                    index: inp.outputIndex,
                });
            }
        }

        block_col.insertOne({
            hash: block.hash,
            height: block.height,
        },(err,result)=>{
            if(!err){
                col.insertMany(new_utxos,(err,result)=>{
                    callback(err);
                    async.mapSeries(new_spent,(spent,next)=>{
                        col.updateMany({
                            tx: spent.tx,index: spent.index
                        },{
                            $set:{spent:spent.block}
                        });
                        next();
                    },(err,results)=>{
                        callback(err);
                    });
                    client.close();
                });
            }else{
                callback(err);
            }
        });
    });
}

MongoConnector.prototype._calculate = function(block,reverse=false){
    for(var t_idx = 0;t_idx < block.txs.length;t_idx++){

        let tx = block.txs[t_idx];

        // outputs
        for(var o_idx = 0;o_idx<tx.outputs.length;o_idx++){
            var out = tx.outputs[o_idx];

            var addr = out.address;
            var change_bal = reverse?-out.satoshis:out.satoshis;
            var new_bal = addr in this._data_balances?
                this._data_balances[addr]+change_bal:change_bal;

            if(addr)
                this._change_balance(addr,new_bal);
        }

        // inputs
        for(var i_idx=0;i_idx<tx.inputs.length;i_idx++){
            var inp = tx.inputs[i_idx];
            var tx_id = inp.prevTxId;
            var tx_idx = inp.outputIndex;
            if(!tx_id)
                continue;
            var qtx = this._transactions[tx_id];
            if(qtx){
                var out = qtx.outputs[tx_idx];
                var addr = out.address;
                var change_bal = reverse?out.satoshis:-out.satoshis;
                var new_bal = addr in this._data_balances?
                    this._data_balances[addr]+change_bal:change_bal;
                if(addr)
                    this._change_balance(addr,new_bal);
            }
        }
    }
}

/**
 * Update balances from red black tree and json arr=>balance
 */
MongoConnector.prototype._change_balance = function(addr,new_bal){

    // remove from old balances set
    var old_val = this._data_balances[addr];
    if(old_val){
        var _set = this._data_balances_tree.get(old_val);
        if(_set && _set[addr]){
            delete _set[addr];
        	this._data_balances_tree = this._data_balances_tree.remove(old_val);
        	this._data_balances_tree = this._data_balances_tree.insert(old_val,_set);
        }
    }

    // set new balances to set
    this._data_balances[addr] = new_bal;

    // insert to new balances set
    var new_set = this._data_balances_tree.get(new_bal);

    if(!new_set){
        new_set = {};
    }else{
        this._data_balances_tree = this._data_balances_tree.remove(new_bal);
    }
    new_set[addr] = true;
    this._data_balances_tree = this._data_balances_tree.insert(new_bal,new_set);
}

MongoConnector.prototype._conn_db = function(callback){
    // Use connect method to connect to the server
    MongoClient.connect(url+'/'+dbName,{ useNewUrlParser: true }, function(err, client) {
        if(err)
            throw err;
        // assert.equal(null, err);
        // console.log("Connected successfully to server");
    
        const db = client.db(dbName);
        callback(err,client,db);
    });
};

module.exports = MongoConnector;