from basana.core import dispatcher, enums, logs
import basana as bs
from decimal import Decimal
import asyncio
import logging

logger = logging.getLogger(__name__)


class OrderController:
    def __init__(self):
        self.exchange: bs.Broker
        self.lock = asyncio.Lock()

    def create_order_market(self, ticker: str, side: enums.OrderOperation, amount: int):
        pass

    def create_order_limit(self, ticker: str, side: enums.OrderOperation, amount: int, limit: Decimal):
        async with self.lock:
            logger.info(f"[ORDER] Creating LIMIT order: {side} {amount} {ticker} @{limit} ")

    def position(self, ticker: str) -> (Decimal, Decimal, Decimal, Decimal):
        # TODO: 缓存并延迟更新，避免频繁请求
        pass
