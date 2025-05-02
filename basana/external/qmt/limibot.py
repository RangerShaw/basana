import basana as bs
from typing import List


class Limibot:
    def __init__(self, exchange, ):
        self.exchange = exchange
        self.order_controller = bs.OrderController(self.exchange)
        self.strategy_controller = bs.StrategyController(self.order_controller)

    def subscribe_strategy_to_bars(self, strategy: bs.Strategy, ticker: str):
        self.strategy_controller.subscribe_strategy_to_bars(strategy, ticker)

    def start(self):
        self.strategy_controller.start()


import asyncio
import random
from collections import defaultdict
from typing import Dict, List
import logging

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# 模拟QMT实时数据回调接口
class QMTDataFeed:
    def __init__(self):
        self.callbacks = defaultdict(list)  # 股票代码 -> 回调函数列表

    def register_callback(self, stock_code: str, callback):
        """注册回调函数，监听特定股票的数据"""
        self.callbacks[stock_code].append(callback)
        logger.info(f"Registered callback for stock {stock_code}")

    async def simulate_data_stream(self):
        """模拟实时数据流，实际中应替换为QMT的实时数据接口"""
        data_id = 0
        while True:
            for stock_code in self.callbacks.keys():
                price = random.uniform(10, 100)
                volume = random.randint(100, 1000)
                data = {
                    "stock_code": stock_code,
                    "price": price,
                    "volume": volume,
                    "data_id": data_id,
                    "timestamp": asyncio.get_event_loop().time()
                }
                logger.info(f"Generated data for {stock_code}, ID={data_id}, Price={price:.2f}")
                # 为每个回调创建独立任务，允许不同策略并行处理
                for callback in self.callbacks[stock_code]:
                    asyncio.create_task(callback(data))
            data_id += 1
            await asyncio.sleep(1)  # 每秒生成一次数据


# 策略基类
class Strategy:
    def __init__(self, name: str, stock_code: str, queue_size: int = 100):
        self.name = name
        self.stock_code = stock_code
        self.data_queue = asyncio.Queue(maxsize=queue_size)  # 数据队列，确保顺序处理
        self.queue_task = None  # 保存持续运行的任务
        self.start_processing()  # 在初始化时启动处理任务

    def start_processing(self):
        """启动持续运行的处理任务"""
        if self.queue_task is None or self.queue_task.done():
            self.queue_task = asyncio.create_task(self.process_queue())
            logger.info(f"[{self.name}] Started processing task for {self.stock_code}")

    async def process_data(self, data: Dict):
        """将数据放入队列，确保顺序处理"""
        try:
            self.data_queue.put_nowait(data)  # 非阻塞放入队列
            logger.info(
                f"[{self.name}] Queued data ID={data['data_id']} for {self.stock_code}, Queue size={self.data_queue.qsize()}")
        except asyncio.QueueFull:
            logger.warning(f"[{self.name}] Queue full for {self.stock_code}, dropping data ID={data['data_id']}")

    async def process_queue(self):
        """持续运行，从队列中按顺序处理数据"""
        logger.info(f"[{self.name}] Process queue started for {self.stock_code}")
        try:
            while True:  # 无限循环，持续检查队列
                data = await self.data_queue.get()  # 阻塞等待数据
                try:
                    await self.handle_data(data)
                except Exception as e:
                    logger.error(
                        f"[{self.name}] Error processing data ID={data.get('data_id')} for {self.stock_code}: {e}")
                finally:
                    self.data_queue.task_done()
                    # 定期记录队列大小，用于监控积压情况
                    if self.data_queue.qsize() > 0 and self.data_queue.qsize() % 10 == 0:
                        logger.warning(
                            f"[{self.name}] Queue size high: {self.data_queue.qsize()} for {self.stock_code}")
        except asyncio.CancelledError:
            logger.info(f"[{self.name}] Process queue cancelled for {self.stock_code}")
            raise
        except Exception as e:
            logger.error(f"[{self.name}] Process queue crashed for {self.stock_code}: {e}")
            raise

    async def handle_data(self, data: Dict):
        """处理单个数据，子类实现具体逻辑"""
        raise NotImplementedError

    async def shutdown(self):
        """清理处理任务"""
        if self.queue_task and not self.queue_task.done():
            self.queue_task.cancel()
            try:
                await self.queue_task
            except asyncio.CancelledError:
                pass
            logger.info(f"[{self.name}] Shutdown processing task for {self.stock_code}")


# 具体策略1：均线策略（模拟耗时操作）
class MovingAverageStrategy(Strategy):
    def __init__(self, stock_code: str, window: int = 5):
        super().__init__(name="MovingAverage", stock_code=stock_code)
        self.prices = []
        self.window = window

    async def handle_data(self, data: Dict):
        # 模拟耗时操作
        await asyncio.sleep(2)  # 假设处理需要2秒
        self.prices.append(data["price"])
        if len(self.prices) > self.window:
            self.prices.pop(0)
        if len(self.prices) == self.window:
            avg_price = sum(self.prices) / self.window
            logger.info(
                f"[{self.name}] Stock {self.stock_code}: ID={data['data_id']}, "
                f"Price={data['price']:.2f}, {self.window}-MA={avg_price:.2f}, Timestamp={data['timestamp']:.2f}"
            )


# 具体策略2：动量策略
class MomentumStrategy(Strategy):
    def __init__(self, stock_code: str):
        super().__init__(name="Momentum", stock_code=stock_code)
        self.last_price = None

    async def handle_data(self, data: Dict):
        current_price = data["price"]
        if self.last_price is not None:
            momentum = current_price - self.last_price
            logger.info(
                f"[{self.name}] Stock {self.stock_code}: ID={data['data_id']}, "
                f"Price={current_price:.2f}, Momentum={momentum:.2f}, Timestamp={data['timestamp']:.2f}"
            )
        self.last_price = current_price


# 策略管理器
class StrategyManager:
    def __init__(self):
        self.data_feed = QMTDataFeed()
        self.strategies: List[Strategy] = []

    def add_strategy(self, strategy: Strategy):
        """添加策略，并注册数据回调"""
        self.strategies.append(strategy)

        # 包装 process_data，添加超时控制
        async def wrapped_process_data(data):
            try:
                await asyncio.wait_for(strategy.process_data(data), timeout=5.0)
            except asyncio.TimeoutError:
                logger.error(
                    f"Timeout in {strategy.name} for {strategy.stock_code} processing data ID={data['data_id']}")

        self.data_feed.register_callback(strategy.stock_code, wrapped_process_data)

    async def run(self):
        """运行所有策略，监听数据流"""
        try:
            data_task = asyncio.create_task(self.data_feed.simulate_data_stream())
            await asyncio.gather(data_task)
        except asyncio.CancelledError:
            logger.info("Data feed cancelled")
            await self.shutdown()
            raise
        except Exception as e:
            logger.error(f"Error in data feed: {e}")
            await self.shutdown()
            raise

    async def shutdown(self):
        """清理所有策略的任务"""
        logger.info("Shutting down strategies...")
        shutdown_tasks = [strategy.shutdown() for strategy in self.strategies]
        await asyncio.gather(*shutdown_tasks, return_exceptions=True)
        logger.info("All strategies shut down")


# 主程序
async def main():
    # 创建策略管理器
    manager = StrategyManager()
    # 为不同股票添加不同的策略
    manager.add_strategy(MovingAverageStrategy(stock_code="AAPL"))
    manager.add_strategy(MomentumStrategy(stock_code="AAPL"))
    manager.add_strategy(MovingAverageStrategy(stock_code="GOOGL"))
    manager.add_strategy(MomentumStrategy(stock_code="GOOGL"))
    try:
        # 运行策略管理器
        await manager.run()
    except KeyboardInterrupt:
        logger.info("Received shutdown signal, stopping...")
        await manager.shutdown()
    except Exception as e:
        logger.error(f"Main program error: {e}")
        await manager.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
