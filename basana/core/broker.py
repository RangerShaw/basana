from abc import ABC, abstractmethod
import basana as bs


class Broker(ABC):
    @abstractmethod
    def position(self, ticker: str):
        pass
