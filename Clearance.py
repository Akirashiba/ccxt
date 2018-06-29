# coding=utf-8
"""
功能:将账户配置里的所有交易所货币对进行阶梯式抛售

运行方式:python Clearance.py xxx.json

Json文件格式:
{
    "slippage": 0.05,
    "percent": 0.5,
    "coinpairs": [
        {
            "exchange": "bigone",
            "apiKey": "xxx",
            "secret": "xxx",
            "symbols": [
                {
                    "symbol": "CVC/BTC",
                    "price": 0.0002
                }
            ]
        }
    ]
}
    
"""
import ccxt
import ccxt.async as ccxt_async
from ccxt.base.errors import ExchangeError
from ccxt.base.errors import RequestTimeout
from ccxt.base.errors import OrderNotFound
from ccxt.base.errors import AuthenticationError
from ccxt.base.errors import NetworkError
from multiprocessing import Pool
import time
import asyncio
import sys
import json


# 重试次数
CREATE_SELL_MAX_RETRY = 2 
CANCEL_MAX_RETRY = 2
GET_REMAIN_MAX_RETRY = 2

# 取消订单重试时间间隔
CANCEL_RETRY_INTERVAL = 0.5 # sec

# 多进程进程数
PROCESS_NUM = 5

# 阶梯试探时间间隔 和 阶梯数
STEP_INTERVAL = 10 # sec
STEPS = 4


class CancelOrderError(Exception):
    """Base class for all exceptions"""
    pass


class CreateOrderError(Exception):
    """Base class for all exceptions"""
    pass


class FetchOrderError(Exception):
    """Base class for all exceptions"""
    pass


class Clearance(object):
    def __init__(self, account_config):
        self.exchange_name = account_config["exchange"]
        self.slippage = account_config["slippage"]
        self.percent = account_config["percent"]
        self.symbols = account_config["symbols"]

        self.account_params = {
            "apiKey": account_config["apiKey"],
            "secret": account_config["secret"],
        }

        self.exchange = eval("ccxt_async.{0}({1})"
                        "".format(self.exchange_name, self.account_params))

    def run(self):
        """ 清仓 """
        self.symbols = self.symbols_override()

        asyncio.get_event_loop().run_until_complete(self.exchange.loadMarkets())

        tasks = [
            asyncio.ensure_future(self.worker()),
            asyncio.ensure_future(self.worker()),
            asyncio.ensure_future(self.worker()),
        ]
    
        asyncio.get_event_loop().run_until_complete(asyncio.wait(tasks))
        results = [task.result() for task in tasks]
        

    def symbols_override(self):
        """ 计算 可出售底价 和 试探价格阶梯 """
        balance_info = asyncio.get_event_loop().run_until_complete(
                                                self.exchange.fetchBalance())
        _symbols = []
        for _symbol in self.symbols:
            symbol = _symbol['symbol']
            base, quote = symbol.split('/')
            amount = balance_info[base]['free'] * self.percent

            price = _symbol['price']
            bottom_price = price * (1 - self.slippage)
            price_ladder = self.ladder_price(bottom_price,price,STEPS)
            
            _symbols.append({
                'symbol': symbol,
                'price_ladder': price_ladder,
                'amount': amount 
            })

        return _symbols

    async def worker(self):
        """ 单个协程woker """
        result = []
        while self.symbols:
            symbol = self.symbols.pop()

            try:
                amount_remain = await self.laddering_sell(symbol)
                # output something
                result.append((symbol['symbol'], amount_remain))

            except CancelOrderError as cancel_error:
                print("cancel_error ", cancel_error)
                pass

            except CreateOrderError as create_error:
                print("create_error ", cancel_error)
                pass

            except FetchOrderError as fetch_error:
                print("fetch_error ", cancel_error)
                pass

            except Exception as e:
                print("exception ", e)
                pass

        return result

    async def laddering_sell(self,trade_info):
        """ 价格从大到小分阶梯向市场试探 """
        price_ladder = trade_info['price_ladder']
        amount = trade_info['amount']
        symbol = trade_info['symbol']

        step = 1

        while price_ladder and amount > 0:
            step_price = price_ladder.pop()
            print("step", step, " start")

            order_id = await self.sell(symbol, amount, step_price)

            await asyncio.sleep(STEP_INTERVAL)

            await self.cancel_order(order_id)
                
            remain = await self.get_amount_remain(order_id)

            amount = float(remain)

            step += 1

        return amount

    async def get_amount_remain(self, order_id, retry=0):
        """ 查看订单内未完成交易的数量 """
        if retry is GET_REMAIN_MAX_RETRY:
            error_msg = "fail to get amount_remain from order " + str(order_id)
            raise FetchOrderError(error_msg)
        try:
            order_info = await self.exchange.fetch_order(order_id)
            print("sellorder ", order_id, "remain ", order_info['remaining'])

            return order_info['remaining']

        except RequestTimeout:
            return await self.get_amount_remain(order_id, retry+1)

        except Exception:
            return await self.get_amount_remain(order_id, retry+1)

    async def sell(self, symbol, amount, price, retry=0):
        """ 创建卖出交易订单 """
        if retry is CREATE_SELL_MAX_RETRY:
            error_msg = "fail to create the limit sell order of {0} "\
                    "at the price of {1} after {2} retries"\
                    "".format(symbol, price, CREATE_SELL_MAX_RETRY)
            raise CreateOrderError(error_msg)
        try:
            since = int(time.time() * 1000)

            print("create sellorder[ symbol: {0}, amount: {1}, price: {2}]"
                                        "".format(symbol, amount, price))
            response = await self.exchange.createOrder(
                                symbol, "limit", "sell", amount, price)

            if isinstance(response, dict) and "id" in response:
                return response["id"]
            else:
                raise ExchangeError(response)

        except RequestTimeout:    
            orders = await self.exchange.fetchOrders(symbol, since)
            if orders:
                order = orders.pop()
                return order['id']
            else:
                return await self.sell(symbol, amount, price, retry+1)

        except (NetworkError,ExchangeError) as e:
            raise CreateOrderError(e)
            
    async def cancel_order(self, order_id, retry=0):
        """ 取消订单 """
        if retry is CANCEL_MAX_RETRY:
            error_msg = "fail to cancel order {1} after {2} retries"\
                                "".format(order_id, CANCEL_MAX_RETRY)
            raise CancelOrderError("fail to cancel_order ", order_id)
        try:
            print("cancel sellorder[ order_id:", order_id, " ]")
            await self.exchange.cancelOrder(order_id)
            return True

        except RequestTimeout as time_out:
            await asyncio.sleep(CANCEL_RETRY_INTERVAL)
            return await self.cancel_order(order_id, retry+1)

        except OrderNotFound as not_found:
            return True

        except (NetworkError,ExchangeError) as e:
            raise CancelOrderError(e)

    @staticmethod
    def ladder_price(low, high, step_num):
        step = (high - low)/(step_num - 1)
        return [low + step*i for i in range(0,step_num)]


def single_clearance(account_config):

    exchange_cl = Clearance(account_config)

    results = exchange_cl.run()


def multi_exchange_clearance(config_file):
    """ 多进程 """
    with open(config_file,"r") as f:
        user_config = json.load(f)

    genneral_config = {
        "slippage": user_config["slippage"],
        "percent": user_config["percent"],
    }

    symbols = user_config["coinpairs"]
    for i in range(0, len(symbols)):
        symbols[i].update(genneral_config)

    pool = Pool(PROCESS_NUM)

    pool.map(single_clearance,symbols)
    
    pool.close()
    pool.join()


if __name__ == "__main__":
    config_file = sys.argv[1]
    multi_exchange_clearance(config_file)