import asyncio
from decimal import Decimal
from typing import Dict, List, Optional, Tuple, Union, Callable
import dataclasses
from xtquant import xttrader, xtdata, xtconstant
from xtquant.xttype import StockAccount
from xtquant.xttrader import XtQuantTrader, XtQuantTraderCallback

import datetime
import logging

from . import qmt_helpers, qmt_manager
from basana.core import bar, dispatcher, enums, token_bucket, logs
from basana.core.pair import Pair, PairInfo
from order import Order
from enums import *

BarEventHandler = bar.BarEventHandler
logger = logging.getLogger(__name__)


# Error = client.Error
# OrderBookEvent = order_book.OrderBookEvent
# OrderBookEventHandler = order_book.OrderBookEventHandler
# OrderOperation = enums.OrderOperation
# TradeEvent = trades.TradeEvent
# TradeEventHandler = trades.TradeEventHandler


class Exchange:
    def __init__(
            self, dispatcher: dispatcher.EventDispatcher, qmt_path: str, account_id: str, session: int = 123456,
            config: dict = None
    ):
        self.dispatcher = dispatcher
        self.xt_trader = XtQuantTrader(qmt_path, session)
        self.account = StockAccount(account_id)
        self.config = config or {}
        self.asset_info: Dict[str, dict] = {}
        self._qmt_mgr = qmt_manager.QmtClientManager(dispatcher, session)
        # 行情订阅管理
        self.subscribed_pairs = set()

    def subscribe_to_multi_bar_events(self, tickers: [str], interval: str, event_handler: callable):
        self._qmt_mgr.subscribe_to_multi_bar_events(tickers, interval, event_handler)

    def get_asset_info(self, ticker: str) -> Dict:
        if ticker not in self.asset_info:
            detail = xtdata.get_instrument_detail(ticker)
            self.asset_info[ticker] = {
                'stock_name': detail.get('InstrumentName', ''),  # 股票最小变动单位0.01元
                'prev_close': detail['PreClose']
            }
        return self.asset_info[ticker]

    def create_order_limit(self, side: OrderSide, ticker: str, quantity: int, limit: Decimal,
                           on_order_error: Callable = None) -> Order:
        if Callable is None:
            order_id = self.xt_trader.order_stock(self.account, ticker, side, quantity, OrderPriceType.FIX_PRICE, limit)
        else:
            order_id = self.xt_trader.order_stock_async(self.account, ticker, side, quantity, OrderPriceType.FIX_PRICE,
                                                        limit)

        if order_id == -1:
            logger.error(f"[ORDER] Creating order FAILED: {side} {quantity} {ticker} @{limit}")
        return Order(order_id, self.account, ticker, side, quantity, OrderPriceType.FIX_PRICE, limit)

    async def cancel_order(self, symbol: str, order_id: str) -> bool:
        """取消订单"""
        return self.xt_trader.cancel_order_stock(self.account, order_id) == 0

    async def get_order_info(self, symbol: str, order_id: str) -> dict:
        """查询订单详情"""
        order = self.xt_trader.query_stock_order(self.account, order_id)
        return self._format_order(order)

    # 账户信息查询
    async def get_balance(self) -> dict:
        """获取账户资金"""
        asset = self.xt_trader.query_stock_asset(self.account)
        return {
            'cash': Decimal(str(asset.cash)),
            'frozen': Decimal(str(asset.frozen_cash)),
            'total': Decimal(str(asset.total_asset))
        }

    async def get_positions(self) -> Dict[str, dict]:
        """获取持仓信息"""
        positions = self.xt_trader.query_stock_positions(self.account)
        return {pos.stock_code: self._format_position(pos) for pos in positions}

    # 数据处理方法
    # def _format_order(self, order: StockOrder) -> dict:
    #     """格式化订单信息"""
    #     return {
    #         'id': str(order.order_id),
    #         'symbol': order.stock_code,
    #         'amount': order.order_volume,
    #         'filled': order.traded_volume,
    #         'price': Decimal(str(order.price)),
    #         'status': self._convert_order_status(order.order_status),
    #         'side': 'BUY' if order.order_type == xtconstant.STOCK_BUY else 'SELL',
    #         'timestamp': datetime.datetime.fromtimestamp(order.order_time / 1000)
    #     }

    def _format_position(self, position) -> dict:
        """格式化持仓信息"""
        return {
            'symbol': position.stock_code,
            'amount': position.volume,
            'frozen': position.frozen_volume,
            'avg_price': Decimal(str(position.avg_price))
        }

    def _convert_period(self, interval: str) -> str:
        """转换K线周期到QMT格式"""
        period_map = {
            '1m': '1m',
            '5m': '5m',
            '15m': '15m',
            '30m': '30m',
            '1h': '1h',
            '1d': '1d'
        }
        return period_map.get(interval, '1d')

    def _convert_order_status(self, status: int) -> str:
        """转换订单状态"""
        status_map = {
            48: 'SUBMITTED',  # 未报
            49: 'PENDING',  # 待报
            50: 'PARTIAL',  # 已报
            56: 'FILLED',  # 已成
            54: 'CANCELED'  # 已撤
        }
        return status_map.get(status, 'UNKNOWN')

    # 事件回调处理
    def _on_market_data(self, data: dict):
        """处理实时行情推送"""
        for symbol, tick in data.items():
            event = self._create_tick_event(symbol, tick)
            self.dispatcher.dispatch(event)

    def _create_tick_event(self, symbol: str, tick: dict) -> 'event.Event':
        """创建Tick事件对象"""
        return {
            'symbol': symbol,
            'time': datetime.datetime.fromtimestamp(tick['time'] / 1000),
            'price': Decimal(str(tick['lastPrice'])),
            'volume': tick['volume'],
            'bid1': Decimal(str(tick['bidPrice'][0])),
            'ask1': Decimal(str(tick['askPrice'][0]))
        }

    from queue import Queue


class QMTExchange:
    """
    QMTExchange 类实现了A股交易所的接口，基于QMT API (xtquant)，
    提供实时行情订阅、历史数据获取、订单创建和管理功能。
    """

    def __init__(self, path: str, session_id: int, account_id: str, options: Dict = None):
        """
        初始化QMT交易所实例。

        参数:
            path (str): MiniQMT客户端的userdata_mini路径
            session_id (int): 与MiniQMT通信的会话ID
            account_id (str): 资金账号ID
            options (Dict, optional): 其他配置选项
        """
        self.path = path
        self.session_id = session_id
        self.account_id = account_id
        self.options = options or {}
        self.account = StockAccount(account_id)
        self.trader = None
        self.callback = None
        self.data_callbacks = {}  # 存储行情数据回调
        self.order_callbacks = Queue()  # 存储订单回调信息
        self.trade_callbacks = Queue()  # 存储成交回调信息
        self.is_running = False
        self.assets_info = {}  # 存储合约信息

        # 初始化QMT交易客户端
        self._init_trader()
        logger.info("[SETUP] Using QMT Exchange for A-Share trading")

    def _init_trader(self):
        """初始化QMT交易客户端和回调"""
        self.trader = XtQuantTrader(self.path, self.session_id)
        self.callback = QMTCallback(self.order_callbacks, self.trade_callbacks)
        self.trader.register_callback(self.callback)
        self.trader.start()
        connect_result = self.trader.connect()
        if connect_result != 0:
            raise ConnectionError(f"Failed to connect to MiniQMT: {connect_result}")
        subscribe_result = self.trader.subscribe(self.account)
        if subscribe_result != 0:
            raise RuntimeError(f"Failed to subscribe account {self.account_id}: {subscribe_result}")
        self.is_running = True

    def last_quote(self, pair: str) -> float:
        """
        获取指定交易对的最新报价。

        参数:
            pair (str): 交易对，例如 '600000.SH'

        返回:
            float: 最新价格
        """
        candles = self.candles_by_limit(pair, '1m', 1)
        if not candles:
            raise ValueError(f"No quote data for {pair}")
        return candles[0]['close']

    def assets_info(self, pair: str) -> Dict:
        """
        获取指定交易对的资产信息。

        参数:
            pair (str): 交易对，例如 '600000.SH'

        返回:
            Dict: 资产信息
        """
        if pair not in self.assets_info:
            detail = xtdata.get_instrument_detail(pair, iscomplete=False)
            if detail is None:
                raise ValueError(f"Invalid asset: {pair}")
            self.assets_info[pair] = {
                'min_quantity': 100,  # A股最小交易单位为100股
                'max_quantity': detail.get('FloatVolume', 0),
                'tick_size': detail.get('PriceTick', 0.01),
            }
        return self.assets_info[pair]

    def _validate(self, pair: str, quantity: float) -> bool:
        """
        验证订单数量是否符合限制。

        参数:
            pair (str): 交易对
            quantity (float): 交易数量

        返回:
            bool: 是否有效
        """
        info = self.assets_info(pair)
        if quantity < info['min_quantity'] or quantity > info['max_quantity']:
            logger.error(
                f"Invalid quantity for {pair}: {quantity}, min: {info['min_quantity']}, max: {info['max_quantity']}")
            return False
        if quantity % info['min_quantity'] != 0:
            logger.error(f"Quantity must be a multiple of {info['min_quantity']} for {pair}")
            return False
        return True

    def create_order_limit(self, side: str, pair: str, quantity: float, limit: float) -> Dict:
        """
        创建限价订单。

        参数:
            side (str): 买卖方向，'BUY' 或 'SELL'
            pair (str): 交易对
            quantity (float): 交易数量
            limit (float): 限价

        返回:
            Dict: 订单信息
        """
        if not self._validate(pair, quantity):
            raise ValueError(f"Invalid order parameters for {pair}")
        order_type = xtconstant.STOCK_BUY if side.upper() == 'BUY' else xtconstant.STOCK_SELL
        order_id = self.trader.order_stock(
            self.account, pair, order_type, int(quantity), xtconstant.FIX_PRICE, limit, 'strategy',
            f'{side}_{pair}'
        )
        if order_id == -1:
            raise RuntimeError(f"Failed to create order for {pair}")
        return {
            'exchange_id': order_id,
            'pair': pair,
            'side': side,
            'type': 'LIMIT',
            'status': 'OPEN',
            'price': limit,
            'quantity': quantity,
            'created_at': datetime.datetime.now(),
            'updated_at': datetime.datetime.now()
        }

    def create_order_market(self, side: str, pair: str, quantity: float) -> Dict:
        """
        创建市价订单（注意：QMT模拟环境不支持市价单）。

        参数:
            side (str): 买卖方向，'BUY' 或 'SELL'
            pair (str): 交易对
            quantity (float): 交易数量

        返回:
            Dict: 订单信息
        """
        if not self._validate(pair, quantity):
            raise ValueError(f"Invalid order parameters for {pair}")
        order_type = xtconstant.STOCK_BUY if side.upper() == 'BUY' else xtconstant.STOCK_SELL
        latest_price = self.last_quote(pair)
        order_id = self.trader.order_stock(
            self.account, pair, order_type, int(quantity), xtconstant.FIX_PRICE, latest_price, 'strategy',
            f'{side}_{pair}'
        )
        if order_id == -1:
            raise RuntimeError(f"Failed to create market order for {pair}")
        return {
            'exchange_id': order_id,
            'pair': pair,
            'side': side,
            'type': 'MARKET',
            'status': 'OPEN',
            'price': latest_price,
            'quantity': quantity,
            'created_at': datetime.datetime.now(),
            'updated_at': datetime.datetime.now()
        }

    def cancel_order(self, order: Dict) -> bool:
        """
        取消订单。

        参数:
            order (Dict): 订单信息

        返回:
            bool: 是否成功取消
        """
        result = self.trader.cancel_order_stock(self.account, order['exchange_id'])
        return result == 0

    def orders(self, pair: str, limit: int = 100) -> List[Dict]:
        """
        查询订单列表。

        参数:
            pair (str): 交易对
            limit (int): 查询数量限制

        返回:
            List[Dict]: 订单列表
        """
        orders = self.trader.query_stock_orders(self.account)
        filtered_orders = [o for o in orders if o.stock_code == pair][:limit]
        return [{
            'exchange_id': o.order_id,
            'pair': o.stock_code,
            'side': 'BUY' if o.order_type == xtconstant.STOCK_BUY else 'SELL',
            'type': 'LIMIT' if o.price_type == xtconstant.FIX_PRICE else 'MARKET',
            'status': self._map_order_status(o.order_status),
            'price': o.price,
            'quantity': o.order_volume,
            'created_at': datetime.datetime.fromtimestamp(o.order_time / 1000),
            'updated_at': datetime.datetime.fromtimestamp(o.order_time / 1000)
        } for o in filtered_orders]

    def order(self, pair: str, order_id: int) -> Dict:
        """
        查询单个订单。

        参数:
            pair (str): 交易对
            order_id (int): 订单ID

        返回:
            Dict: 订单信息
        """
        order = self.trader.query_stock_order(self.account, order_id)
        if not order or order.stock_code != pair:
            raise ValueError(f"Order {order_id} not found for {pair}")
        return {
            'exchange_id': order.order_id,
            'pair': order.stock_code,
            'side': 'BUY' if order.order_type == xtconstant.STOCK_BUY else 'SELL',
            'type': 'LIMIT' if order.price_type == xtconstant.FIX_PRICE else 'MARKET',
            'status': self._map_order_status(order.order_status),
            'price': order.price,
            'quantity': order.order_volume,
            'created_at': datetime.datetime.fromtimestamp(order.order_time / 1000),
            'updated_at': datetime.datetime.fromtimestamp(order.order_time / 1000)
        }

    def account(self) -> Dict:
        """
        查询账户信息。

        返回:
            Dict: 账户余额和资产信息
        """
        asset = self.trader.query_stock_asset(self.account)
        if not asset:
            raise RuntimeError("Failed to query account asset")
        return {
            'balances': {
                'cash': asset.cash,
                'frozen_cash': asset.frozen_cash,
                'market_value': asset.market_value,
                'total_asset': asset.total_asset
            }
        }

    def position(self, pair: str) -> Tuple[float, float]:
        """
        查询指定交易对的持仓。

        参数:
            pair (str): 交易对

        返回:
            Tuple[float, float]: 持仓数量和市值
        """
        position = self.trader.query_stock_position(self.account, pair)
        if not position:
            return 0.0, 0.0
        return position.volume, position.market_value

    def candles_subscription(self, pair: str, period: str) -> Tuple[Queue, Queue]:
        """
        订阅实时K线数据。

        参数:
            pair (str): 交易对
            period (str): K线周期，如 '1m', '5m', '1d'

        返回:
            Tuple[Queue, Queue]: K线数据队列和错误信息队列
        """
        candle_queue = Queue()
        error_queue = Queue()

        def on_data(datas):
            for stock_code, data_list in datas.items():
                if stock_code == pair:
                    for data in data_list:
                        candle = self._format_candle(stock_code, period, data)
                        candle_queue.put(candle)

        self.data_callbacks[pair] = on_data
        seq = xtdata.subscribe_quote(pair, period=period, count=0, callback=on_data)
        if seq <= 0:
            error_queue.put(ValueError(f"Failed to subscribe to {pair} with period {period}"))
            return candle_queue, error_queue

        # 启动一个线程维持订阅
        def keep_running():
            while self.is_running:
                asyncio.sleep(1)
            xtdata.unsubscribe_quote(seq)

        Thread(target=keep_running, daemon=True).start()
        return candle_queue, error_queue

    def candles_by_limit(self, pair: str, period: str, limit: int) -> List[Dict]:
        """
        按数量限制获取历史K线数据。

        参数:
            pair (str): 交易对
            period (str): K线周期
            limit (int): 数量限制

        返回:
            List[Dict]: K线数据列表
        """
        data = xtdata.get_market_data(
            field_list=['time', 'open', 'high', 'low', 'close', 'volume'],
            stock_list=[pair],
            period=period,
            count=limit
        )
        candles = []
        for field, df in data.items():
            if not df.empty and pair in df.index:
                times = data['time'].loc[pair].values
                for i in range(len(times)):
                    candle = {
                        'time': datetime.datetime.fromtimestamp(times[i] / 1000),
                        'open': data['open'].loc[pair].iloc[i],
                        'high': data['high'].loc[pair].iloc[i],
                        'low': data['low'].loc[pair].iloc[i],
                        'close': data['close'].loc[pair].iloc[i],
                        'volume': data['volume'].loc[pair].iloc[i],
                        'complete': True,
                        'metadata': {}
                    }
                    candles.append(candle)
        return candles[::-1][:limit]  # 按时间升序返回

    def candles_by_period(self, pair: str, period: str, start: datetime.datetime, end: datetime.datetime) -> \
            List[
                Dict]:
        """
        按时间范围获取历史K线数据。

        参数:
            pair (str): 交易对
            period (str): K线周期
            start (datetime): 开始时间
            end (datetime): 结束时间

        返回:
            List[Dict]: K线数据列表
        """
        start_str = start.strftime('%Y%m%d')
        end_str = end.strftime('%Y%m%d')
        xtdata.download_history_data(pair, period, start_time=start_str, end_time=end_str)
        data = xtdata.get_market_data(
            field_list=['time', 'open', 'high', 'low', 'close', 'volume'],
            stock_list=[pair],
            period=period,
            start_time=start_str,
            end_time=end_str
        )
        candles = []
        for field, df in data.items():
            if not df.empty and pair in df.index:
                times = data['time'].loc[pair].values
                for i in range(len(times)):
                    t = datetime.datetime.fromtimestamp(times[i] / 1000)
                    if start <= t <= end:
                        candle = {
                            'time': t,
                            'open': data['open'].loc[pair].iloc[i],
                            'high': data['high'].loc[pair].iloc[i],
                            'low': data['low'].loc[pair].iloc[i],
                            'close': data['close'].loc[pair].iloc[i],
                            'volume': data['volume'].loc[pair].iloc[i],
                            'complete': True,
                            'metadata': {}
                        }
                        candles.append(candle)
        return candles

    def _format_candle(self, pair: str, period: str, data: Dict) -> Dict:
        """
        格式化K线数据。
        """
        t = datetime.datetime.fromtimestamp(data['time'] / 1000)
        return {
            'pair': pair,
            'time': t,
            'updated_at': t,
            'open': data.get('open', 0.0),
            'high': data.get('high', 0.0),
            'low': data.get('low', 0.0),
            'close': data.get('close', 0.0),
            'volume': data.get('volume', 0.0),
            'complete': True,
            'metadata': {}
        }

    def _map_order_status(self, status: int) -> str:
        """
        映射QMT订单状态到通用状态。
        """
        if status in [xtconstant.ORDER_SUCCEEDED, xtconstant.ORDER_PART_SUCC]:
            return 'FILLED'
        elif status == xtconstant.ORDER_CANCELED:
            return 'CANCELED'
        elif status == xtconstant.ORDER_JUNK:
            return 'REJECTED'
        else:
            return 'OPEN'

    def stop(self):
        """
        停止交易所实例，关闭连接。
        """
        self.is_running = False
        self.trader.stop()
        logger.info("[SHUTDOWN] QMT Exchange stopped")


class QMTCallback(XtQuantTraderCallback):
    """
    QMT交易回调类，用于处理订单和成交的推送信息。
    """

    def __init__(self, order_queue: Queue, trade_queue: Queue):
        super().__init__()
        self.order_queue = order_queue
        self.trade_queue = trade_queue

    def on_disconnected(self):
        logger.error("Connection to MiniQMT lost")

    def on_stock_order(self, order):
        order_info = {
            'exchange_id': order.order_id,
            'pair': order.stock_code,
            'side': 'BUY' if order.order_type == xtconstant.STOCK_BUY else 'SELL',
            'status': self._map_order_status(order.order_status),
            'price': order.price,
            'quantity': order.order_volume,
            'updated_at': datetime.datetime.fromtimestamp(order.order_time / 1000)
        }
        self.order_queue.put(order_info)
        logger.info(f"Order update: {order_info}")

    def on_stock_trade(self, trade):
        trade_info = {
            'exchange_id': trade.order_id,
            'pair': trade.stock_code,
            'side': 'BUY' if trade.order_type == xtconstant.STOCK_BUY else 'SELL',
            'price': trade.traded_price,
            'quantity': trade.traded_volume,
            'updated_at': datetime.datetime.fromtimestamp(trade.traded_time / 1000)
        }
        self.trade_queue.put(trade_info)
        logger.info(f"Trade executed: {trade_info}")

    def on_order_error(self, order_error):
        logger.error(f"Order error: ID={order_error.order_id}, Error={order_error.error_msg}")

    def on_cancel_error(self, cancel_error):
        logger.error(f"Cancel error: ID={cancel_error.order_id}, Error={cancel_error.error_msg}")

    def _map_order_status(self, status: int) -> str:
        if status in [xtconstant.ORDER_SUCCEEDED, xtconstant.ORDER_PART_SUCC]:
            return 'FILLED'
        elif status == xtconstant.ORDER_CANCELED:
            return 'CANCELED'
        elif status == xtconstant.ORDER_JUNK:
            return 'REJECTED'
        else:
            return 'OPEN'
