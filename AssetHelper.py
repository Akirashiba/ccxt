# -*- coding: utf-8 -*-
import ccxt

class AssetHelper(object):
    """docstring for AssetConverter"""
    def __init__(self, exchange):
        super(AssetConverter, self).__init__()
        self.exchange = exchange

    def symbol_trade_speed(self, symbol):
        """ 获得货币对交易速度(24hr) """
        ticker = self.exchange.fetch_ticker(symbol)

        pass


    def asset_evaluate(self, base, target):
        """
            获得 从 baseAsset 到 TargetAsset 价值量的映射(不考虑手续费)
            输入参数为: baseAsset, TargetAsset
            返回格式为:
            base_side: baseAsset 转换成 middleAsset 的 交易方式(buy or sell)
            middleAsset: 中间货币
            middle_side: middleAsset 转换成 TargetAsset 的 交易方式(buy or sell)
            price: 每个 baseAsset 可以换成 TargetAsset 的价格 
            {
                "{base_side}_{middleAsset}_{middle_side}": {price}
                ...
            }
        """
        symbols = self.exchange.symbols
 
        symbol = base + "/" + target
        rev_symbol = self.reverse_symbol(symbol)

        if symbol in symbols:
            return self.get_best_price(symbol, "asks")

        elif rev_symbol in symbols:
            return self.get_best_price(symbol, "bids")

        else:
            base_relate_map = self.get_relate_map(base)
            target_relate_map = self.get_relate_map(target)

            lv2pathways = self.find_lv2pathways(
                            base_relate_map, target_relate_map)

            unified_price = {} 
            for path, middles in lv2pathways.items():
                base_side, middle_side = path.split("_n_")
                for middle in middles:

                    b_middle = base + "/" + middle
                    if base_side is "buy":
                        b_middle = self.reverse_symbol(b_middle)

                    b_to_m = self.get_best_price(b_middle, base_side)

                    middle_t = middle + "/" + target
                    if middle_side is "buy":
                        middle_t = self.reverse_symbol(middle_t)

                    m_to_t = self.get_best_price(middle_t, middle_side)

                    b_to_t = float(b_to_m) * float(m_to_t)

                    key = "{0}_{1}_{2}".format(base_side,middle,middle_side)

                    unified_price[key] = b_to_t

            return unified_price

    def get_best_price(self, symbol, side):
        """ 取得当前市场交易深度里 The best asks or bids """
        sides = {
            "sell": "asks",
            "buy": "bids"
        }
        side = sides[side.lower()] if side.lower() in sides else side

        depth = self.exchange.fetch_order_book(symbol, 5)

        return depth[side][0][0]


    def get_relate_map(self, asset):
        """ 
            获得 Asset 买卖关系图
            输入参数为: asset
            返回格式为:
            {
                "buy_relate": [通过 buy 方式可以获取的所有货币],
                "sell_relate": [通过 buy 方式可以获取的所有货币]
            }
        """ 
        _quote_map = self.quote_currency_map(self.exchange.symbols)

        sell_relate = [quote for quote, quote_area
                  in _quote_map.items() if asset in quote_area]

        buy_relate = _quote_map[asset] if asset in _quote_map else []

        relate_map = {
            "buy_relate": buy_relate,  
            "sell_relate": sell_relate
        }

        return relate_map

    @staticmethod
    def find_lv2pathways(base_map, target_map):
        """ 
            获得从baseAsset 到 TargetAsset 经过两次转换的中间货币 以及 转换方式
            输入参数为: base_map(baseAsset 买卖关系图)
                       target_map(targetAsset 买卖关系图)
            返回格式为:
            {
                "buy_n_sell": [buy_n_sell 中间货币],
                "sell_n_sell": [sell_n_sell 中间货币],
                "buy_n_buy": [buy_n_buy 中间货币],
                "sell_n_buy": [sell_n_buy 中间货币]
            }

        """
        buy_n_sell = [_b for _b in base_map["buy_relate"] 
                    for _t in target_map["buy_relate"] if _b == _t]

        sell_n_sell = [_b for _b in base_map["sell_relate"] 
                    for _t in target_map["buy_relate"] if _b == _t]

        buy_n_buy = [_b for _b in base_map["buy_relate"] 
                    for _t in target_map["sell_relate"] if _b == _t]

        sell_n_buy = [_b for _b in base_map["sell_relate"] 
                    for _t in target_map["sell_relate"] if _b == _t]

        lv2pathways = {
            "buy_n_sell": buy_n_sell,
            "sell_n_sell": sell_n_sell,
            "buy_n_buy": buy_n_buy,
            "sell_n_buy": sell_n_buy
        }

        return lv2pathways

    @staticmethod
    def reverse_symbol(symbol):
       base, quote = symbol.split("/")
       return quote + "/" + base
    
    @staticmethod
    def quote_currency_map(symbols):
        """ 
            获取交易所 所有交易区 以及 交易区内所有货币对
            输入参数为: 交易所所有货币对列表 self.exchange.symbols
            返回格式为:
            {
                quoteAsset:[baseAsset1,baseAsset2....],
                ...
            }
        """
        currency_map = defaultdict(dict)
        for symbol in symbols:
            base, quote = symbol.split("/")
            if quote not in currency_map:
                currency_map[quote] = [base]
            else:
                currency_map[quote].append(base)

        return currency_map