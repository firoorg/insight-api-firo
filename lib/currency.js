'use strict';

var request = require('request');

function CurrencyController(options) {
    this.node = options.node;
    var refresh = options.currencyRefresh || CurrencyController.DEFAULT_CURRENCY_DELAY;
    this.currencyDelay = refresh * 60000;

    this.wci_zcoin_rate = 0.00;
    this.exchange_rates = {
        zcoin_usd: 0.00,
        btc_usd: 0.00,
        btc_zcoin: 0.00
    }
    this.timestamp = Date.now();
}

CurrencyController.DEFAULT_CURRENCY_DELAY = 10;

CurrencyController.prototype.index = function (req, res) {
    var self = this;
    var currentTime = Date.now();
    if (self.exchange_rates.zcoin_usd < 0.01 || currentTime >= (self.timestamp + self.currencyDelay)) {
        self.timestamp = currentTime;
        // https://www.worldcoinindex.com/apiservice/ticker?key={KEY}&label=xzcbtc&fiat=usd
        // output = {"Markets":[{"Label":"XZC/BTC","Name":"Zcoin","Price":30.98490677,"Volume_24h":689074.6535595949,"Timestamp":1522313700}]}

        // https://api.coinmarketcap.com/v1/ticker/zcoin/
        // output = [{"id":"zcoin","name":"ZCoin","symbol":"XZC","rank":"78","price_usd":"31.1187","price_btc":"0.00411886","24h_volume_usd":"813540.0","market_cap_usd":"137302092.0","available_supply":"4412205.0","total_supply":"4412205.0","max_supply":"21400000.0","percent_change_1h":"0.47","percent_change_24h":"-6.64","percent_change_7d":"-24.63","last_updated":"1522313948"}]
        request('https://api.coingecko.com/api/v3/simple/price?ids=zcoin&vs_currencies=usd', function (err, response, body) {
            if (err) {
                self.node.log.error(err);
            } else {
                try {
                    var obj = JSON.parse(body);
                    if (obj) {
                        self.wci_zcoin_rate = parseFloat(obj["zcoin"].usd);
                    }
                    // self.exchange_rates = response.Markets[-4]; // Zerocoin, start from last of the list, get zcoin array data put in vars.
                    // self.bitstampRate = parseFloat(JSON.parse(body).last);
                    /* var response = JSON.parse(body);
                     self.exchange_rates = response.exchange_rates;
                     self.exchange_rates.bitstamp = response.exchange_rates.zcoin_usd; //backward comp.
                    */
                    self.exchange_rates.zcoin_usd = self.wci_zcoin_rate;
                    self.exchange_rates.bitstamp = self.wci_zcoin_rate; //backward compatible
                } catch (e) {
                    self.node.log.error(e);
                }
            }
            res.jsonp({
                status: 200,
                data: self.exchange_rates
            });
        });
    } else {
        res.jsonp({
            status: 200,
            data: self.exchange_rates
        });
    }

};

module.exports = CurrencyController;
