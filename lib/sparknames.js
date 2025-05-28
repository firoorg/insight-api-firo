'use strict';

function SparkNameController(node) {
  this.node = node;
}

SparkNameController.prototype.getsparknames = function (req, res) {
  this.node.services.bitcoind.getsparknames(function (err, response) {
    if (err) {
      return res.jsonp(err);
    }

    const result = Array.isArray(response.result) ? response.result : [];
    res.jsonp(result);
  });
};

module.exports = SparkNameController;
