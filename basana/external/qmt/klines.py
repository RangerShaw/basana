from datetime import datetime
from decimal import Decimal
import logging

from . import helpers, qmt_manager as qm
from basana.core import bar, event
from basana.core.pair import Pair

logger = logging.getLogger(__name__)


class Bar(bar.Bar):
    def __init__(self, time: datetime, pair: Pair, json: dict):
        super().__init__(
            time, pair, Decimal(json["open"]), Decimal(json["high"]),
            Decimal(json["low"]), Decimal(json["lastPrice"]), Decimal(json["volume"])
        )
        self.pair: Pair = pair
        self.json: dict = json


# Generate BarEvents events from websocket messages.
class WebSocketEventSource(qm.ChannelEventSource):
    def __init__(self, pair: Pair, producer: event.Producer):
        super().__init__(producer=producer)

    async def push_from_message(self, message: dict):
        for ticker, data in message.items():
            print(ticker, data)
            t = helpers.timestamp_to_datetime(data['time'])
            this_bar = Bar(t, ticker, data)
            self.push(bar.BarEvent(t, this_bar))


def get_channel(pair: Pair, interval: str) -> str:
    return f"{pair.base_symbol}@kline_{interval}_QMT"


datas = {
    '002859.SZ': {
        'amount': 61182700.0, 'askPrice': [19.71, 19.72, 19.73, 19.740000000000002, 19.76],
        'askVol': [79, 7, 9, 5, 4], 'bidPrice': [19.7, 19.69, 19.68, 19.67, 19.66],
        'bidVol': [220, 44, 96, 65, 135], 'high': 20.38, 'lastClose': 20.28, 'lastPrice': 19.71,
        'lastSettlementPrice': 20.28, 'low': 19.7, 'open': 20.09, 'openInt': 13, 'pe': 0.0, 'pvolume': 3063327,
        'settlementPrice': 0.0, 'speed1Min': 0.0, 'speed5Min': 0.0, 'stockStatus': 3, 'time': 1743650742000,
        'transactionNum': 0, 'volRatio': 0.0, 'volume': 30633
    },
    '159819.SZ': {
        'amount': 284167800.0, 'askPrice': [0.935, 0.936, 0.937, 0.9380000000000001, 0.9390000000000001],
        'askVol': [28077, 41989, 32195, 13724, 4871], 'bidPrice': [0.934, 0.933, 0.932, 0.931, 0.93],
        'bidVol': [48727, 118324, 36022, 34268, 55653], 'high': 0.9520000000000001, 'lastClose': 0.952,
        'lastPrice': 0.934, 'lastSettlementPrice': 0.952, 'low': 0.934, 'open': 0.9400000000000001,
        'openInt': 13, 'pe': 0.9343000000000001, 'pvolume': 301466000, 'settlementPrice': 0.0, 'speed1Min': 0.0,
        'speed5Min': 0.0, 'stockStatus': 3, 'time': 1743650742000, 'transactionNum': 0, 'volRatio': 0.0,
        'volume': 3014660
    }
}
