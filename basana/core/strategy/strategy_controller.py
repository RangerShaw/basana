import logging
from basana.external.qmt.exchange import Exchange
from typing import Any, Dict, List, Optional
import basana as bs

logger = logging.getLogger(__name__)


class StrategyController:
    def __init__(self, order_controller: bs.OrderController):
        self.order_controller: bs.OrderController = order_controller
        self.subscriptions: Dict[str, bs.Strategy] = {}
        self.strategies: List[bs.Strategy] = []

    async def on_bar_event(self, bar_event: bs.BarEvent):
        strat = self.subscriptions[bar_event.bar.pair.base_symbol]
        await strat.enqueue_data(bar_event)

    def subscribe_strategy_to_bars(self, strategy: bs.Strategy, ticker: str):
        if ticker in self.subscriptions:
            logger.error(f"[STRATEGY] ticker subscription already exists")
        else:
            self.subscriptions[ticker] = strategy
            self.strategies.append(strategy)

    def start(self):
        for strategy in self.subscriptions.values():
            strategy.start(self.order_controller)


