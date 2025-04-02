# Basana
#
# Copyright 2022 Gabriel Martin Becedillas Ruiz
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from decimal import Decimal
from typing import Dict, List, Optional, Tuple, Union
import dataclasses
from xtquant import xttrader, xtdata, xtconstant
from xtquant.xttype import StockAccount
from xtquant.xttrader import XtQuantTrader, XtQuantTraderCallback

import aiohttp
import datetime

from . import client, helpers, order_book, trades, spot, cross_margin, isolated_margin, websocket_mgr
from basana.core import bar, dispatcher, enums, token_bucket
from basana.core.pair import Pair, PairInfo

BarEventHandler = bar.BarEventHandler
Error = client.Error
OrderBookEvent = order_book.OrderBookEvent
OrderBookEventHandler = order_book.OrderBookEventHandler
OrderOperation = enums.OrderOperation
TradeEvent = trades.TradeEvent
TradeEventHandler = trades.TradeEventHandler


@dataclasses.dataclass(frozen=True)
class PairInfoEx(PairInfo):
    """Information about a trading pair.

    :param base_precision: The precision for the base symbol.
    :param quote_precision: The precision for the quote symbol.
    :param permissions: The account and pair permissions.

    Check **Account and Symbol Permissions** in https://binance-docs.github.io/apidocs/spot/en/#public-api-definitions.
    """

    #: The account and pair permissions.
    permissions: List[str]


class QMTExchange:
    """QMT A股交易所接口封装"""

    def __init__(
            self, xt_trader: XtQuantTrader, account: StockAccount, dispatcher: dispatcher.EventDispatcher,
            config: dict = None
    ):
        self.xt_trader = xt_trader
        self.account = account
        self.dispatcher = dispatcher
        self.config = config or {}
        self._pair_info_cache: Dict[str, dict] = {}

        # 行情订阅管理
        self.subscribed_pairs = set()
        self._setup_event_handlers()

    def _setup_event_handlers(self):
        """初始化事件处理器"""
        xtdata.subscribe_whole_quote([], self._on_market_data)

    # 行情订阅相关方法
    def subscribe_to_bar_events(self, symbol: str, interval: str, event_handler: callable, flush_delay: float = 1):
        """订阅K线数据"""
        qmt_period = self._convert_period(interval)
        xtdata.subscribe_quote(symbol, qmt_period, callback=event_handler)
        self.subscribed_pairs.add(symbol)

    def subscribe_to_order_book_events(self, symbol: str, event_handler: callable, depth: int = 10):
        """订阅订单簿更新"""
        # QMT Level2数据需要特殊处理
        xtdata.subscribe_quote(symbol, 'order_book', callback=event_handler)

    def subscribe_to_trade_events(self, symbol: str, event_handler: callable):
        """订阅逐笔成交"""
        xtdata.subscribe_quote(symbol, 'tick', callback=event_handler)

    # 行情数据获取
    async def get_bid_ask(self, symbol: str) -> Tuple[Decimal, Decimal]:
        """获取当前最优买卖价"""
        quote = xtdata.get_full_tick([symbol]).get(symbol, {})
        return (
            Decimal(str(quote.get('bidPrice', [0])[0])),
            Decimal(str(quote.get('askPrice', [0])[0]))
        )

    async def get_pair_info(self, symbol: str) -> dict:
        """获取证券基本信息"""
        if symbol not in self._pair_info_cache:
            detail = xtdata.get_instrument_detail(symbol)
            self._pair_info_cache[symbol] = {
                'base_precision': 2,  # 股票最小变动单位0.01元
                'quote_precision': 2,
                'min_order_volume': 100,  # 主板最小100股
                'price_tick': 0.01
            }
        return self._pair_info_cache[symbol]

    # 订单相关方法
    async def create_order(
            self, symbol: str, operation: int, amount: int, price_type: int = xtconstant.FIX_PRICE, price: float = None
    ) -> str:
        """创建股票订单"""
        order = StockOrder()
        order.stock_code = symbol
        order.order_type = operation
        order.order_volume = amount
        order.price_type = price_type
        order.price = price or 0.0

        # 调用QMT交易接口
        order_id = self.xt_trader.order_stock(
            self.account, symbol, operation, amount,
            price_type, price, "Strategy", "AutoOrder"
        )
        return str(order_id)

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
    def _format_order(self, order: StockOrder) -> dict:
        """格式化订单信息"""
        return {
            'id': str(order.order_id),
            'symbol': order.stock_code,
            'amount': order.order_volume,
            'filled': order.traded_volume,
            'price': Decimal(str(order.price)),
            'status': self._convert_order_status(order.order_status),
            'side': 'BUY' if order.order_type == xtconstant.STOCK_BUY else 'SELL',
            'timestamp': datetime.datetime.fromtimestamp(order.order_time / 1000)
        }

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


def get_filter_from_symbol_info(symbol_info: dict, filter_type: str) -> Optional[dict]:
    filters = symbol_info["filters"]
    price_filters = [filter for filter in filters if filter["filterType"] == filter_type]
    return None if not price_filters else price_filters[0]


def get_precision_from_step_size(step_size: str) -> int:
    return int(-Decimal(step_size).log10() / Decimal(10).log10())
