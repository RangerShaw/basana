from abc import ABC, abstractmethod
import basana as bs
import asyncio
import logging

logger = logging.getLogger(__name__)


class Strategy(ABC):
    def __init__(self, queue_size: int = 100):
        self.data_queue = asyncio.Queue(maxsize=queue_size)

    async def enqueue_data(self, data):
        try:
            self.data_queue.put_nowait(data)
        except asyncio.QueueFull:
            logger.error(f"[STRATEGY] Data queue full {data}")

    @abstractmethod
    async def on_bar_event(self, bar_event: bs.BarEvent, broker: bs.OrderController):
        pass
