from basana.external.qmt.exchange import Exchange
from typing import Any, Dict, List, Optional
import basana as bs


class StrategyController:
    def __init__(self):
        self.broker: bs.Broker
        self.strategies: Dict[str, bs.Strategy]
