from basana.core import dispatcher, enums, logs
import basana as bs
from decimal import Decimal
import asyncio
import logging
from typing import Dict, List, Optional, Tuple, Union, Callable

logger = logging.getLogger(__name__)


class OrderController:
    def __init__(self):
        self.exchange: bs.Broker
        self.lock = asyncio.Lock()

    def _validate_order(self, ticker: str, quantity: int):
        return True

    async def create_order_market(self, ticker: str, side: enums.OrderOperation, amount: int):
        pass

    async def create_order_limit(self, ticker: str, side: enums.OrderOperation, quantity: int, limit: Decimal):
        logger.info(f"[ORDER] Creating LIMIT order: {side} {quantity} {ticker} @{limit} ")
        if not self._validate_order(ticker, quantity):
            logger.error(f"[ORDER] Invalid order: {side} {quantity} {ticker} @{limit}")
            return

        async with self.lock:
            pass

    def on_order_error(self, order_error):
        pass

    def position(self, ticker: str) -> (Decimal, Decimal, Decimal, Decimal):
        # TODO: 缓存并延迟更新，避免频繁请求
        pass
