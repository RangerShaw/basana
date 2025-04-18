from basana.external.qmt.exchange import Exchange
from typing import Any, Dict, List, Optional
import basana as bs


class StrategyController:
    def __init__(self, order_controller: bs.OrderController):
        self.order_controller: bs.OrderController = order_controller
        self.strategies: Dict[str, bs.Strategy] = {}

    async def on_bar_event(self, bar_event: bs.BarEvent):
        strat = self.strategies[bar_event.bar.pair.base_symbol]
        await strat.on_bar_event(bar_event, self.order_controller)
