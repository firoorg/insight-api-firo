'use strict';

var Common = require('./common');
var Async = require('async');

function FaucetController(node) {
    this.node = node;
    this.common = new Common({log: this.node.log});
}

FaucetController.prototype.sendto = function (req, res) {
  this.node.sendToAddress(req.query.sendToAddr, 1, function(err, data) {
    if (err) {
      if(err.hasOwnProperty('message')) {
        res.jsonp({
          result: 'error: ' + err.message
        });
      }
    } else {
      res.jsonp({
        result: 'success: ' + data.result
      });
    }
  });
};

FaucetController.prototype.info = function (req, res) {
  var node = this.node;
  Async.parallel([
    function(callback) {
      node.getWalletInfo(function (err, data) {
        callback(err, data);
      })
    },
    function(callback) {
      node.getAccountAddress('', function (err, data) {
        callback(err, data);
      })
    }
  ],
  function(err, results) {
    if(err) {
      res.status(500).send("Invalid request");
      return;
    }
    res.jsonp({
      balance: results[0].balance,
      txcount: results[0].txcount,
      address: results[1],
    });
  });
};


module.exports = FaucetController;
