from abc import ABC, abstractmethod
import basana as bs
from basana.core import enums
from decimal import Decimal

class Broker(ABC):
    @abstractmethod
    def position(self, ticker: str):
        pass

    @abstractmethod
    def create_order_limit(self, ticker: str, side: enums.OrderOperation, quantity: int, limit: Decimal):
        pass
