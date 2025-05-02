from abc import ABC, abstractmethod
from typing import Optional

import basana as bs
import asyncio
import logging

logger = logging.getLogger(__name__)


class Strategy(ABC):
    def __init__(self, name: str = "", queue_size: int = 100):
        self.name: str = name
        self.order_controller: Optional[bs.OrderController] = None
        self.data_queue = asyncio.Queue(maxsize=queue_size)
        self.task = None  # 本策略的运行协程

    def start(self, order_controller: bs.OrderController):
        self.order_controller = order_controller
        if self.task is None or self.task.done():
            logger.info(f"[STRATEGY] Starting strategy {self.name}")
            self.task = asyncio.create_task(self.process_queue())

    async def enqueue_data(self, data):
        try:
            self.data_queue.put_nowait(data)
        except asyncio.QueueFull:
            logger.error(f"[STRATEGY] Data queue full {data}")

    async def process_queue(self):
        """持续运行，从队列中按顺序处理数据"""
        logger.info(f"[STRATEGY] {self.name} process queue started")
        try:
            while True:
                data = await self.data_queue.get()  # 阻塞等待数据
                try:
                    await self.on_bar_event(data)
                except Exception as e:
                    logger.error(f"[STRATEGY] Error processing data {data} for {self.name}: {e}")
                finally:
                    self.data_queue.task_done()
                    # 定期记录队列大小，用于监控积压情况
                    if self.data_queue.qsize() > 0 and self.data_queue.qsize() % 10 == 0:
                        logger.warning(f"[STRATEGY] Queue size high: {self.data_queue.qsize()} in {self.name}")
        except asyncio.CancelledError:
            logger.warning(f"[{self.name}] Process queue cancelled for {self.name}")
            raise
        except Exception as e:
            logger.error(f"[{self.name}] Process queue crashed for {self.name}: {e}")
            raise

    @abstractmethod
    async def on_bar_event(self, bar_event: bs.BarEvent):
        raise NotImplementedError
