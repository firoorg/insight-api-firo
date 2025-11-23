'use strict';

function Common(options) {
    this.log = options.log;
}

Common.prototype.escapeHtml = function(str) {
  if (typeof str !== 'string') return str;
  return str
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#039;');
};

Common.prototype.notReady = function (err, res, p) {
    res.status(503).send('Server not yet ready. Sync Percentage:' + p);
};

Common.prototype.handleErrors = function (err, res) {
    if (err) {
        if (err.code) {
            res.status(400).send(this.escapeHtml(err.message) + '. Code:' + err.code);
        } else {
            this.log.error(err.stack);
            res.status(503).send(this.escapeHtml(err.message));
        }
    } else {
        res.status(404).send('Not found');
    }
};

module.exports = Common;
