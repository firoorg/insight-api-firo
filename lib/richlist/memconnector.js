var async = require('async');
var bitcore = require('zcore-lib');
var _ = bitcore.deps._;
var pools = require('../../pools.json');
var BN = bitcore.crypto.BN;
var LRU = require('lru-cache');
var Common = require('../common');
var Transactions = require('../transactions');

var createTree = require("functional-red-black-tree");

var ErrNotImplement = new Error("not implemented");

// Connector
function MemConnector(){
}

/**
 * Initialize connector
 */
MemConnector.prototype.init = function(){
    if(this._inited)return;
    this._blocks = [];
    this._inited = true;

    // init
    if(!this._data_idx)
        this._data_idx = -1;
    
    this._data_balances = {}; 
    this._data_balances_tree = createTree();

    this._transactions = {};
}

/**
 * Get local best block
 */
MemConnector.prototype.bestBlock = function(callback){
    callback(null,this._blocks.length <= 0?0:this._blocks[this._blocks.length-1].hash);
}

/**
 * Get top n address order by balances
 */
MemConnector.prototype.get_top = function(n,callback){
    var bals = this._data_balances_tree.keys;
    var res = [];
    for(var k=bals.length-1;k>=0;k--){
        var bal = bals[k];
        if(bal == 0){
            break;
        }
        var addrs = this._data_balances_tree.get(bal);
        Object.keys(addrs).forEach( (addr)=>{
            if(res.length > n)
                return;
            res.push({
                address:addr,
                balance:(bal/1e8).toFixed(8),
            });
        });
    }

	res= res.sort((x,y)=>{
		if(x.balance==y.balance){
			return y.address>x.address?-1:1;
		}
		else return x.balace-y.balance;
    });

    callback(null,res);
}

/**
 * Remove lastest block from blocks array and update balance
 */
MemConnector.prototype.invalidate = function(callback){
    var last_block = this._blocks[this._blocks.length-1];
    this._calculate(last_block,true);
    this._blocks.pop();
    callback(null);
}

/**
 * Insert new block to local
 */
MemConnector.prototype.insert_block = function(block,callback){
    this._calculate(block);
    this._blocks.push(block);

    for(var t_idx=0;t_idx<block.txs.length;t_idx++)
        this._transactions[block.txs[t_idx].hash] = block.txs[t_idx];

    callback(null);
}

MemConnector.prototype._calculate = function(block,reverse=false){
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
MemConnector.prototype._change_balance = function(addr,new_bal){

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

module.exports = MemConnector;
