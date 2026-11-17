'use strict';

var request = require('request');

function ExtendedController(node) {
    this.node = node;
}

ExtendedController.prototype.getmoneysupply = function (req, res) {
  var self = this;
  this.node.getTotalSupply(function (err, data) {
    if (err) {
      self.node.log.error(err);
    } else {
      res.jsonp({
        total: data.balance / 1e+8,
      });
    }
  });
};

module.exports = ExtendedController;
