'use strict';

var Writable = require('stream').Writable;
var bodyParser = require('body-parser');
var compression = require('compression');
var BaseService = require('./service');
var inherits = require('util').inherits;
var BlockController = require('./blocks');
var TxController = require('./transactions');
var AddressController = require('./addresses');
var StatusController = require('./status');
var MessagesController = require('./messages');
var UtilsController = require('./utils');
var CurrencyController = require('./currency');
var ZerocoinController = require('./zerocoin');
var ExtendedController = require('./extended');
var RichListController = require('./richlist/controller');
var RateLimiter = require('./ratelimiter');
var morgan = require('morgan');
var bitcore = require('zcore-lib');
var _ = bitcore.deps._;
var $ = bitcore.util.preconditions;
var Transaction = bitcore.Transaction;
var EventEmitter = require('events').EventEmitter;
var RichListStorage = require('./richlist/storage/mongo');
var MongoClient = require('mongodb').MongoClient;
var SigmaStatusController = require('./sigmastatus');
var ZNodeController = require('./znode');
var LelantusStatusController = require('./lelantusstatus');

/**
 * A service for Bitcore to enable HTTP routes to query information about the blockchain.
 *
 * @param {Object} options
 * @param {Boolean} options.enableCache - This will enable cache-control headers
 * @param {Number} options.cacheShortSeconds - The time to cache short lived cache responses.
 * @param {Number} options.cacheLongSeconds - The time to cache long lived cache responses.
 * @param {String} options.routePrefix - The URL route prefix
 * @param {String} options.db.url - The DB URL
 */
var InsightAPI = function (options) {
    BaseService.call(this, options);

    // initialize sub services
    this.services = [];

    // in minutes
    this.currencyRefresh = options.currencyRefresh || CurrencyController.DEFAULT_CURRENCY_DELAY;

    this.subscriptions = {
        inv: []
    };

    if (!_.isUndefined(options.enableCache)) {
        $.checkArgument(_.isBoolean(options.enableCache));
        this.enableCache = options.enableCache;
    }
    this.cacheShortSeconds = options.cacheShortSeconds;
    this.cacheLongSeconds = options.cacheLongSeconds;

    this.rateLimiterOptions = options.rateLimiterOptions;
    this.disableRateLimiter = options.disableRateLimiter;

    this.blockSummaryCacheSize = options.blockSummaryCacheSize || BlockController.DEFAULT_BLOCKSUMMARY_CACHE_SIZE;
    this.blockCacheSize = options.blockCacheSize || BlockController.DEFAULT_BLOCK_CACHE_SIZE;

    if (!_.isUndefined(options.routePrefix)) {
        this.routePrefix = options.routePrefix;
    } else {
        this.routePrefix = this.name;
    }

    this.mongo = new MongoClient(options.db.url, { useNewUrlParser: true });
    this.txController = new TxController(this.node);

    // init rich list
    this.richListStorage = new RichListStorage({ mongo: this.mongo });
    this.richListController = new RichListController({ node: this.node, storage: this.richListStorage });
    this.services.push(this.richListController);

    this.sigmaStatusController = new SigmaStatusController({ node: this.node, connection_params: options.postgres_db });
    this.lelantusStatusController = new LelantusStatusController({ node: this.node, connection_params: options.postgres_db });
};

InsightAPI.dependencies = ['bitcoind', 'web'];

inherits(InsightAPI, BaseService);

InsightAPI.prototype.cache = function (maxAge) {
    var self = this;
    return function (req, res, next) {
        if (self.enableCache) {
            res.header('Cache-Control', 'public, max-age=' + maxAge);
        }
        next();
    };
};

InsightAPI.prototype.cacheShort = function () {
    var seconds = this.cacheShortSeconds || 30; // thirty seconds
    return this.cache(seconds);
};

InsightAPI.prototype.cacheLong = function () {
    var seconds = this.cacheLongSeconds || 86400; // one day
    return this.cache(seconds);
};

InsightAPI.prototype.getRoutePrefix = function () {
    return this.routePrefix;
};

InsightAPI.prototype.start = function (callback) {
    // check if mtp is already enabled
    new Promise((resolve, reject) => {
        this.node.services.bitcoind.getInfo(function (err, info) {
            if (err) {
                reject(new Error('Error occured when called getInfo'));
            } else {
                resolve(info);
            }
        });
    }).then(info => {
        global.mtp = {
            isMtpEnabled: info.protocolVersion >= 90026,
            network: bitcore.Networks.get(info.network)
        };
    }).then(() => {
        return this.mongo.connect();
    }).then(() => {
        return this.richListStorage.init();
    }).then(() => {
        return this.richListController.init();
    }).then(() => {
        this.node.services.bitcoind.on('tx', this.transactionEventHandler.bind(this));
        this.node.services.bitcoind.on('block', this.blockEventHandler.bind(this));
        callback();
    });
};

InsightAPI.prototype.createLogInfoStream = function () {
    var self = this;

    function Log(options) {
        Writable.call(this, options);
    }

    inherits(Log, Writable);

    Log.prototype._write = function (chunk, enc, callback) {
        self.node.log.info(chunk.slice(0, chunk.length - 1)); // remove new line and pass to logger
        callback();
    };
    var stream = new Log();

    return stream;
};

InsightAPI.prototype.getRemoteAddress = function (req) {
    if (req.headers['cf-connecting-ip']) {
        return req.headers['cf-connecting-ip'];
    }
    return req.socket.remoteAddress;
};

InsightAPI.prototype._getRateLimiter = function () {
    var rateLimiterOptions = _.isUndefined(this.rateLimiterOptions) ? {} : _.clone(this.rateLimiterOptions);
    rateLimiterOptions.node = this.node;
    var limiter = new RateLimiter(rateLimiterOptions);
    return limiter;
};

InsightAPI.prototype.setupRoutes = function (app) {

    var self = this;

    //Enable rate limiter
    if (!this.disableRateLimiter) {
        var limiter = this._getRateLimiter();
        app.use(limiter.middleware());
    }

    //Setup logging
    morgan.token('remote-forward-addr', function (req) {
        return self.getRemoteAddress(req);
    });
    var logFormat = ':remote-forward-addr ":method :url" :status :res[content-length] :response-time ":user-agent" ';
    var logStream = this.createLogInfoStream();
    app.use(morgan(logFormat, { stream: logStream }));

    //Enable compression
    app.use(compression());

    //Enable urlencoded data
    app.use(bodyParser.urlencoded({ extended: true }));

    //Enable CORS
    app.use(function (req, res, next) {

        res.header('Access-Control-Allow-Origin', '*');
        res.header('Access-Control-Allow-Methods', 'GET, HEAD, PUT, POST, OPTIONS');
        res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept, Content-Length, Cache-Control, cf-connecting-ip');

        var method = req.method && req.method.toUpperCase && req.method.toUpperCase();

        if (method === 'OPTIONS') {
            res.statusCode = 204;
            res.end();
        } else {
            next();
        }
    });

    //Block routes
    var blockOptions = {
        node: this.node,
        blockSummaryCacheSize: this.blockSummaryCacheSize,
        blockCacheSize: this.blockCacheSize
    };
    var blocks = new BlockController(blockOptions);
    app.get('/blocks', this.cacheShort(), blocks.list.bind(blocks));

    app.get('/block/:blockHash', this.cacheShort(), blocks.checkBlockHash.bind(blocks), blocks.show.bind(blocks));
    app.param('blockHash', blocks.block.bind(blocks));

    app.get('/rawblock/:blockHash', this.cacheLong(), blocks.checkBlockHash.bind(blocks), blocks.showRaw.bind(blocks));
    app.param('blockHash', blocks.rawBlock.bind(blocks));

    app.get('/block-index/:height', this.cacheShort(), blocks.blockIndex.bind(blocks));
    app.param('height', blocks.blockIndex.bind(blocks));

    // Rich list routes
    app.get('/richlist', this.cacheShort(), this.richListController.list.bind(this.richListController));

    // Transaction routes
    var transactions = new TxController(this.node);
    app.get('/tx/:txid', this.cacheShort(), transactions.show.bind(transactions));
    app.param('txid', transactions.transaction.bind(transactions));
    app.get('/txs', this.cacheShort(), transactions.list.bind(transactions));
    app.post('/tx/send', transactions.send.bind(transactions));

    // Raw Routes
    app.get('/rawtx/:txid', this.cacheLong(), transactions.showRaw.bind(transactions));
    app.param('txid', transactions.rawTransaction.bind(transactions));

    // Address routes
    var addresses = new AddressController(this.node);
    app.get('/addr/:addr', this.cacheShort(), addresses.checkAddr.bind(addresses), addresses.show.bind(addresses));
    app.get('/addr/:addr/utxo', this.cacheShort(), addresses.checkAddr.bind(addresses), addresses.utxo.bind(addresses));
    app.get('/addrs/:addrs/utxo', this.cacheShort(), addresses.checkAddrs.bind(addresses), addresses.multiutxo.bind(addresses));
    app.post('/addrs/utxo', this.cacheShort(), addresses.checkAddrs.bind(addresses), addresses.multiutxo.bind(addresses));
    app.get('/addrs/:addrs/txs', this.cacheShort(), addresses.checkAddrs.bind(addresses), addresses.multitxs.bind(addresses));
    app.post('/addrs/txs', this.cacheShort(), addresses.checkAddrs.bind(addresses), addresses.multitxs.bind(addresses));

    // Address property routes
    app.get('/addr/:addr/balance', this.cacheShort(), addresses.checkAddr.bind(addresses), addresses.balance.bind(addresses));
    app.get('/addr/:addr/totalReceived', this.cacheShort(), addresses.checkAddr.bind(addresses), addresses.totalReceived.bind(addresses));
    app.get('/addr/:addr/totalSent', this.cacheShort(), addresses.checkAddr.bind(addresses), addresses.totalSent.bind(addresses));
    app.get('/addr/:addr/unconfirmedBalance', this.cacheShort(), addresses.checkAddr.bind(addresses), addresses.unconfirmedBalance.bind(addresses));

    // Status route
    var status = new StatusController(this.node);
    app.get('/status', this.cacheShort(), status.show.bind(status));
    app.get('/sync', this.cacheShort(), status.sync.bind(status));
    app.get('/peer', this.cacheShort(), status.peer.bind(status));
    app.get('/version', this.cacheShort(), status.version.bind(status));

    // Address routes
    var messages = new MessagesController(this.node);
    app.get('/messages/verify', messages.verify.bind(messages));
    app.post('/messages/verify', messages.verify.bind(messages));

    // Utils route
    var utils = new UtilsController(this.node);
    app.get('/utils/estimatefee', utils.estimateFee.bind(utils));

    // ZelNode route
    var masternode = new ZNodeController(this.node);
    app.get('/masternode/listmasternodes', masternode.listZNodes.bind(utils));
    app.get('/masternode/listmasternodesarray/:filter?', masternode.listZNodesFilter.bind(utils));
    app.post('/masternode/listmasternodesarray', masternode.listZNodesFilter.bind(utils));
    app.get('/masternode/masternodecount', masternode.ZNodeCount.bind(utils));

    // Currency
    var currency = new CurrencyController({
        node: this.node,
        currencyRefresh: this.currencyRefresh
    });
    app.get('/currency', currency.index.bind(currency));

    // Zerocoin
    var zerocoin = new ZerocoinController(this.node);
    app.get('/zerocoin/gettotalzeromint', zerocoin.gettotalzeromint.bind(zerocoin));
    app.get('/zerocoin/gettotalzerospend', zerocoin.gettotalzerospend.bind(zerocoin));
    app.get('/zerocoin/getrealsupply', zerocoin.getrealsupply.bind(zerocoin));
    app.get('/zerocoin/getzerominttxs', zerocoin.getzerominttxs.bind(zerocoin));
    app.get('/zerocoin/getzerospendtxs', zerocoin.getzerospendtxs.bind(zerocoin));
    app.get('/sigma/gettotalsigmamint', zerocoin.gettotalsigmamint.bind(zerocoin));
    app.get('/sigma/gettotalsigmaspend', zerocoin.gettotalsigmaspend.bind(zerocoin));
    app.get('/sigma/getrealsupply', zerocoin.getsigmarealsupply.bind(zerocoin));

    // Extended
    var extended = new ExtendedController(this.node);
    app.get('/ext/getmoneysupply', extended.getmoneysupply.bind(extended));
    app.get('/ext/getsparknames', extended.getsparknames.bind(extended));
    
    //Sigma status
    app.get('/sigmastatus', this.sigmaStatusController.getSigmaStatus.bind(this.sigmaStatusController));

    app.get('/lelantusstatus', this.lelantusStatusController.getStatus.bind(this.lelantusStatusController));

    // Not Found
    app.use(function (req, res) {
        res.status(404).jsonp({
            status: 404,
            url: req.originalUrl,
            error: 'Not found'
        });
    });

};

InsightAPI.prototype.getPublishEvents = function () {
    return [
        {
            name: 'inv',
            scope: this,
            subscribe: this.subscribe.bind(this),
            unsubscribe: this.unsubscribe.bind(this),
            extraEvents: ['tx', 'block']
        }
    ];
};

InsightAPI.prototype.blockEventHandler = function (hashBuffer) {
    // Notify inv subscribers
    for (var i = 0; i < this.subscriptions.inv.length; i++) {
        this.subscriptions.inv[i].emit('block', hashBuffer.toString('hex'));
    }
};
InsightAPI.prototype.transactionEventHandler = function (txBuffer) {
    var tx = new Transaction().fromBuffer(txBuffer);
    var result = this.txController.transformInvTransaction(tx);

    for (var i = 0; i < this.subscriptions.inv.length; i++) {
        this.subscriptions.inv[i].emit('tx', result);
    }
};

InsightAPI.prototype.subscribe = function (emitter) {
    $.checkArgument(emitter instanceof EventEmitter, 'First argument is expected to be an EventEmitter');

    var emitters = this.subscriptions.inv;
    var index = emitters.indexOf(emitter);
    if (index === -1) {
        emitters.push(emitter);
    }
};

InsightAPI.prototype.unsubscribe = function (emitter) {
    $.checkArgument(emitter instanceof EventEmitter, 'First argument is expected to be an EventEmitter');

    var emitters = this.subscriptions.inv;
    var index = emitters.indexOf(emitter);
    if (index > -1) {
        emitters.splice(index, 1);
    }
};

InsightAPI.prototype.stop = function (done) {
    var promises = this.services.map(service => service.stop());
    Promise.all(promises).then(() => done());
}

module.exports = InsightAPI;
