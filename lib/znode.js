'use strict';

var Common = require('./common');

function ZNodeController(node) {
  this.node = node;
  this.common = new Common({ log: this.node.log });
}

// looks like i need to have it in separate call as was causing issues
ZNodeController.prototype.listZNodes = function (req, res) {
  this.node.services.bitcoind.evoznodelist(function (err, response) {
    if (err) {
      res.jsonp(err);
    }
    res.jsonp(response);
  });
};

ZNodeController.prototype.ZNodeCount = function (req, res) {
  this.node.services.bitcoind.evoznodecount("count", function (err, response) {
    if (err) {
      res.jsonp(err);
    }
    res.jsonp(response);
  });
};

ZNodeController.prototype.listZNodesFilter = function (req, res) {
  var filter = req.body.filter || req.params.filter || req.query.filter;
  this.node.services.bitcoind.evoznodelist(function (err, response) {
    if (err) {
      res.jsonp(err);
    }
    try {
      const keys = Object.keys(response.result);
      const znodes = [];
      keys.forEach((key) => {
        const selectedNode = response.result[key];
        const znode = {
          collateral: key,
          txhash: key.split(",")[0].substr(10),
          outidx: key.split(", ")[1].slice(0, -1),
          ip: selectedNode.address.split(":").slice(0, -1).join(''),
          proTxHash: selectedNode.proTxHash,
          address: selectedNode.address,
          payee: selectedNode.payee,
          status: selectedNode.status,
          lastpaidtime: selectedNode.lastpaidtime,
          lastpaidblock: selectedNode.lastpaidblock,
          owneraddress: selectedNode.owneraddress,
          votingaddress: selectedNode.votingaddress,
          collateraladdress: selectedNode.collateraladdress,
          pubkeyoperator: selectedNode.pubkeyoperator,
        }
        znodes.push(znode);
      });
      if (!filter) {
        res.jsonp({
          result: znodes,
          error: response.error,
          id: response.id
        });
      } else if (filter.includes(',')) {
        const znodelist = znodes;
        // comma separated list of collateralHash-collateralIndex, collateralHash-collateralIndex or more filters
        const collateralsInInterest = filter.split(',');
        let goodZNodes = [];
        collateralsInInterest.forEach((interestedFilter) => {
          if (interestedFilter.includes('-')) {
            const txhash = interestedFilter.split('-')[0];
            const outidx = interestedFilter.split('-')[1];
            const goodZNodeList = znodelist.filter((znode) => (znode.txhash === txhash && znode.outidx === outidx))
            if (goodZNodeList.length > 0) {
              goodZNodes = goodZNodes.concat(goodZNodeList);
            }
          } else {
            const goodZNodeList = znodelist.filter((znode) => JSON.stringify(znode).includes(interestedFilter))
            if (goodZNodeList.length > 0) {
              goodZNodes = goodZNodes.concat(goodZNodeList);
            }
          }
        })
        const znodelistToRespond = [...new Set(goodZNodes)];
        res.jsonp({
          result: znodelistToRespond,
          error: response.error,
          id: response.id
        })
      } else if (filter.includes('-')) {
        const znodelist = znodes;
        // scneario for just one znode
        let goodZNodes = [];
        const txhash = filter.split('-')[0];
        const outidx = filter.split('-')[1];
        const goodZNodeList = znodelist.filter((znode) => (znode.txhash === txhash && znode.outidx === outidx))
        if (goodZNodeList.length > 0) {
          goodZNodes = goodZNodes.concat(goodZNodeList);
        }
        const znodelistToRespond = [...new Set(goodZNodes)];
        res.jsonp({
          result: znodelistToRespond,
          error: response.error,
          id: response.id
        })
      } else {
        const filteredList = znodes.filter(function (znode) {
          return JSON.stringify(znode).includes(filter);
        })
        res.jsonp({
          result: filteredList,
          error: response.error,
          id: response.id
        })
      }
    } catch (error) {
      res.jsonp(error)
    }
  });
};

module.exports = ZNodeController;
