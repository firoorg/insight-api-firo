'use strict';

var bitcore = require('zcore-lib');
var _ = bitcore.deps._;
var $ = bitcore.util.preconditions;
var Common = require('./common');
var async = require('async');

var MAXINT = 0xffffffff; // Math.pow(2, 32) - 1;

function TxController(node) {
    this.node = node;
    this.common = new Common({log: this.node.log});
}

TxController.prototype.show = function (req, res) {
    if (req.transaction) {
        res.jsonp(req.transaction);
    }
};

/**
 * Find transaction by hash ...
 */
TxController.prototype.transaction = function (req, res, next) {
    var self = this;
    var txid = req.params.txid;

    this.node.getDetailedTransaction(txid, function (err, transaction) {
        if (err && err.code === -5) {
            return self.common.handleErrors(null, res);
        } else if (err) {
            return self.common.handleErrors(err, res);
        }

        self.transformTransaction(transaction, function (err, transformedTransaction) {
            if (err) {
                return self.common.handleErrors(err, res);
            }
            req.transaction = transformedTransaction;
            next();
        });

    });
};

TxController.prototype.transformTransaction = async function (transaction, options, callback) {
    if (_.isFunction(options)) {
        callback = options;
        options = {};
    }
    $.checkArgument(_.isFunction(callback));

    var confirmations = 0;
    if (transaction.height >= 0) {
        confirmations = this.node.services.bitcoind.height - transaction.height + 1;
    }

    var transformed = {
        txid: transaction.hash,
        version: transaction.version,
        locktime: transaction.locktime
    };

    if (transaction.coinbase) {
        transformed.vin = [
            {
                coinbase: transaction.inputs[0].script,
                sequence: transaction.inputs[0].sequence,
                n: 0
            }
        ];
    } else {
        transformed.vin = transaction.inputs.map(this.transformInput.bind(this, options));
        if (transformed.vin && transformed.vin.length) {
            for (var i = 0; i < transformed.vin.length; i++) {
                var vin = transformed.vin[i];
                if (vin && vin.scriptSig && vin.scriptSig.asm && vin.scriptSig.asm.startsWith('OP_ZEROCOINSPEND ')) {
                    vin.addr = 'Zerospend';
                } else if (vin && vin.scriptSig && vin.scriptSig.asm && vin.scriptSig.asm.startsWith('OP_SIGMASPEND ')) {
                    vin.addr = 'Sigmaspend';
                } else if (vin && vin.scriptSig && vin.scriptSig.asm && vin.scriptSig.asm.startsWith('OP_LELANTUSJOINSPLIT ')) {
                  vin.addr = 'Lelantusjsplit';
                } else if (vin && vin.scriptSig && vin.scriptSig.asm && vin.scriptSig.asm.startsWith('OP_LELANTUSJOINSPLITPAYLOAD')) {
                  vin.addr = 'Lelantusjsplit';
                } else if (vin && vin.scriptSig && vin.scriptSig.asm && vin.scriptSig.asm.startsWith('OP_SPARKSPEND')) {
                    vin.addr = 'Sparkspend';
                } else if (vin && vin.scriptSig && vin.scriptSig.asm && vin.scriptSig.asm.startsWith('OP_ZEROCOINTOSIGMAREMINT ')) {
                    vin.addr = 'Remint';
                    vin.value = transaction.outputSatoshis / 1e8;
                    vin.valueSat = transaction.outputSatoshis;
                }
            }
        }
    }

    transformed.vout = transaction.outputs.map(this.transformOutput.bind(this, options));
    if (transformed.vout && transformed.vout.length) {
        for (var o = 0; o < transformed.vout.length; o++) {
            var vout = transformed.vout[o];
            if (vout && vout.scriptPubKey && vout.scriptPubKey.asm && vout.scriptPubKey.asm.startsWith('OP_ZEROCOINMINT ')) {
                if (!vout.scriptPubKey.addresses) {
                    vout.scriptPubKey.addresses = [];
                }
                vout.scriptPubKey.type = 'zerocoinmint';
                vout.scriptPubKey.addresses.push('Zeromint');
            } else if (vout && vout.scriptPubKey && vout.scriptPubKey.asm && vout.scriptPubKey.asm.startsWith('OP_SIGMAMINT ')) {
                if (!vout.scriptPubKey.addresses) {
                    vout.scriptPubKey.addresses = [];
                }
                vout.scriptPubKey.type = 'sigmamint';
                vout.scriptPubKey.addresses.push('Sigmamint');
            } else if (vout && vout.scriptPubKey && vout.scriptPubKey.asm && vout.scriptPubKey.asm.startsWith('OP_LELANTUSMINT ')) {
                if (!vout.scriptPubKey.addresses) {
                  vout.scriptPubKey.addresses = [];
                }
                vout.scriptPubKey.type = 'lelantusmint';
                vout.scriptPubKey.addresses.push('Lelantusmint');
            } else if (vout && vout.scriptPubKey && vout.scriptPubKey.asm && vout.scriptPubKey.asm.startsWith('OP_LELANTUSJMINT')) {
              if (!vout.scriptPubKey.addresses) {
                vout.scriptPubKey.addresses = [];
              }
              vout.scriptPubKey.type = 'lelantusjmint';
              vout.scriptPubKey.addresses.push('Lelantusjmint');
            } else if (vout && vout.scriptPubKey && vout.scriptPubKey.asm && vout.scriptPubKey.asm.startsWith('OP_SPARKMINT')) {
                if (!vout.scriptPubKey.addresses) {
                    vout.scriptPubKey.addresses = [];
                }
                vout.scriptPubKey.type = 'sparkmint';
                vout.scriptPubKey.addresses.push('Sparkmint');
            } else if (vout && vout.scriptPubKey && vout.scriptPubKey.asm && vout.scriptPubKey.asm.startsWith('OP_SPARKSMINT')) {
                if (!vout.scriptPubKey.addresses) {
                    vout.scriptPubKey.addresses = [];
                }
                vout.scriptPubKey.type = 'sparksmint';
                vout.scriptPubKey.addresses.push('Sparksmint');
            }
        }

        const txid = String(transformed.txid);
        try {
            const result = await new Promise((resolve) => {
                this.node.services.bitcoind.getsparknametxdetails(txid, (err, result) => {
                    if (err) {
                        resolve(null);
                    } else {
                        resolve(result);
                    }
                });
            });
            if (result && result.name && result.address) {
                transformed.sparkData = {
                    name: result.name,
                    address: result.address,
                    validUntil: result.validUntil
                };

                if (result.additionalInfo !== undefined) {
                    transformed.sparkData.additionalInfo = result.additionalInfo;
                }

                const sparknameOutput = {
                    n: transformed.vout.length,
                    value: 0,
                    scriptPubKey: {
                        type: 'sparkname',
                        addresses: ['Sparkname'],
                        asm: '',
                        hex: ''
                    }
                };

                transformed.vout.unshift(sparknameOutput);

                transformed.vout.forEach((vout, index) => {
                    vout.n = index;
                });
            }
        } catch (error) {
        }
    }

    transformed.blockhash = transaction.blockHash;
    transformed.blockheight = transaction.height;
    transformed.confirmations = confirmations;
    // TODO consider mempool txs with receivedTime?
    var time = transaction.blockTimestamp ? transaction.blockTimestamp : Math.round(Date.now() / 1000);
    transformed.time = time;
    if (transformed.confirmations) {
        transformed.blocktime = transformed.time;
    }

    if (transaction.coinbase) {
        transformed.isCoinBase = true;
    }

    transformed.valueOut = transaction.outputSatoshis / 1e8;
    transformed.size = transaction.hex.length / 2; // in bytes
    if (!transaction.coinbase) {
        transformed.valueIn = transaction.inputSatoshis / 1e8;
        transformed.fees = transaction.feeSatoshis / 1e8;
    }

    transformed.txlock = transaction.txlock;

    transformed.extraPayload = transaction.extraPayload;

    if (transformed.extraPayload) {
        if(transaction.version >= 3 && transaction.type == 1) { //TRANSACTION_PROVIDER_REGISTER
            transformed.proReg = transaction.proReg;
        }
        if(transaction.version >= 3 && transaction.type == 2) { //TRANSACTION_PROVIDER_UPDATE_SERVICE
            transformed.proUpServ = transaction.proUpServ;
        }
        if(transaction.version >= 3 && transaction.type == 3) { //TRANSACTION_PROVIDER_UPDATE_REGISTRAR
            transformed.proUpReg = transaction.proUpReg;
        }
        if(transaction.version >= 3 && transaction.type == 4) { //TRANSACTION_PROVIDER_UPDATE_REVOKE
            transformed.proUpRev = transaction.proUpRev;
        }
        if(transaction.version >= 3 && transaction.type == 5) { //TRANSACTION_COINBASE
            transformed.cbTx = transaction.cbTx;
        }
        if(transaction.version >= 3 && transaction.type == 6) { //TRANSACTION_QUORUM_COMMITMENT
            transformed.finalCommitment = transaction.finalCommitment;
        }
        if(transaction.version >= 3 && transaction.type == 7) { //Spork control TX
          transformed.sporkTx = transaction.sporkTx;
        }
    }
  callback(null, transformed);
};

TxController.prototype.transformInput = function (options, input, index) {
    // Input scripts are validated and can be assumed to be valid
    var transformed = {
        txid: input.prevTxId,
        vout: input.outputIndex,
        sequence: input.sequence,
        n: index
    };

    if (!options.noScriptSig) {
        transformed.scriptSig = {
            hex: input.script
        };
        if (!options.noAsm) {
            transformed.scriptSig.asm = input.scriptAsm;
        }
    }

    transformed.addr = input.address;
    transformed.valueSat = input.satoshis;
    transformed.value = input.satoshis / 1e8;
    transformed.doubleSpentTxID = null; // TODO
    //transformed.isConfirmed = null; // TODO
    //transformed.confirmations = null; // TODO
    //transformed.unconfirmedInput = null; // TODO

    return transformed;
};

TxController.prototype.transformOutput = function (options, output, index) {
    var transformed = {
        value: (output.satoshis / 1e8).toFixed(8),
        n: index,
        scriptPubKey: {
            hex: output.script
        }
    };

    if (!options.noAsm) {
        transformed.scriptPubKey.asm = output.scriptAsm;
    }

    if (!options.noSpent) {
        transformed.spentTxId = output.spentTxId || null;
        transformed.spentIndex = _.isUndefined(output.spentIndex) ? null : output.spentIndex;
        transformed.spentHeight = output.spentHeight || null;
    }

    if (output.address) {
        transformed.scriptPubKey.addresses = [output.address];
        var address = bitcore.Address(output.address); //TODO return type from bitcore-node
        transformed.scriptPubKey.type = address.type;
    }
    return transformed;
};

TxController.prototype.transformInvTransaction = function (transaction) {
    var self = this;

    var valueOut = 0;
    var vout = [];
    for (var i = 0; i < transaction.outputs.length; i++) {
        var output = transaction.outputs[i];
        valueOut += output.satoshis;
        if (output.script) {
            var address = output.script.toAddress(self.node.network);
            if (address) {
                var obj = {};
                obj[address.toString()] = output.satoshis;
                vout.push(obj);
            }
        }
    }

    var isRBF = _.some(_.map(transaction.inputs, 'sequenceNumber'), function (seq) {
        return seq < MAXINT - 1;
    });

    var transformed = {
        txid: transaction.hash,
        valueOut: valueOut / 1e8,
        vout: vout,
        isRBF: isRBF,
        txlock: false
    };

    return transformed;
};

TxController.prototype.rawTransaction = function (req, res, next) {
    var self = this;
    var txid = req.params.txid;

    this.node.getTransaction(txid, function (err, transaction) {
        if (err && err.code === -5) {
            return self.common.handleErrors(null, res);
        } else if (err) {
            return self.common.handleErrors(err, res);
        }

        req.rawTransaction = {
            'rawtx': transaction.toBuffer().toString('hex')
        };

        next();
    });
};

TxController.prototype.showRaw = function (req, res) {
    if (req.rawTransaction) {
        res.jsonp(req.rawTransaction);
    }
};

TxController.prototype.list = function (req, res) {
    var self = this;

    var blockHash = req.query.block;
    var address = req.query.address;
    var page = parseInt(req.query.pageNum) || 0;
    var pageLength = 10;
    var pagesTotal = 1;

    if (blockHash) {
        self.node.getBlockOverview(blockHash, function (err, block) {
            if (err && err.code === -5) {
                return self.common.handleErrors(null, res);
            } else if (err) {
                return self.common.handleErrors(err, res);
            }

            var totalTxs = block.txids.length;
            var txids;

            if (!_.isUndefined(page)) {
                var start = page * pageLength;
                txids = block.txids.slice(start, start + pageLength);
                pagesTotal = Math.ceil(totalTxs / pageLength);
            } else {
                txids = block.txids;
            }

            async.mapSeries(txids, function (txid, next) {
                self.node.getDetailedTransaction(txid, function (err, transaction) {
                    if (err) {
                        return next(err);
                    }
                    self.transformTransaction(transaction, next);
                });
            }, function (err, transformed) {
                if (err) {
                    return self.common.handleErrors(err, res);
                }

                res.jsonp({
                    pagesTotal: pagesTotal,
                    txs: transformed
                });
            });

        });
    } else if (address) {
        var options = {
            from: page * pageLength,
            to: (page + 1) * pageLength
        };

        self.node.getAddressHistory(address, options, function (err, result) {
            if (err) {
                return self.common.handleErrors(err, res);
            }

            var txs = result.items.map(function (info) {
                return info.tx;
            }).filter(function (value, index, self) {
                return self.indexOf(value) === index;
            });

            async.map(
                txs,
                function (tx, next) {
                    self.transformTransaction(tx, next);
                },
                function (err, transformed) {
                    if (err) {
                        return self.common.handleErrors(err, res);
                    }
                    res.jsonp({
                        pagesTotal: Math.ceil(result.totalCount / pageLength),
                        txs: transformed
                    });
                }
            );
        });
    } else {
        return self.common.handleErrors(new Error('Block hash or address expected'), res);
    }
};

TxController.prototype.send = function (req, res) {
    var self = this;
    this.node.sendTransaction(req.body.rawtx, function (err, txid) {
        if (err) {
            // TODO handle specific errors
            return self.common.handleErrors(err, res);
        }

        res.json({'txid': txid});
    });
};

module.exports = TxController;
