from basana.core import dispatcher, enums
import basana as bs
from decimal import Decimal


class OrderController:
    def __init__(self):
        self.broker: bs.Broker

    def create_order_market(self, ticker: str, side: enums.OrderOperation, amount: int):
        pass

    def create_order_limit(self):
        pass

    def position_detail(self, ticker: str) -> (Decimal, Decimal, Decimal, Decimal):
        # TODO: 缓存并延迟更新，避免频繁请求
        pass
