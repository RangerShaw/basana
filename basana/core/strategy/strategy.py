from abc import ABC, abstractmethod
import basana as bs


class Strategy(ABC):
    @abstractmethod
    async def on_bar_event(self, bar_event: bs.BarEvent, broker: bs.OrderController):
        pass
