import basana as bs
from decimal import Decimal
class Strategy(bs.TradingSignalSource):
    def __init__(self, dispatcher: bs.EventDispatcher, buy_point: Decimal):
        super().__init__(dispatcher)
        self._values = (None, None)
        self.buy_point: Decimal = buy_point

    def update_high_after_buy(self):
        pass

    async def on_bar_event(self, bar_event: bs.BarEvent):
        pct_change = bar_event.bar.pct_change
        self.update_high_after_buy()

        if pct_change >=0.8:
            self.push(bs.TradingSignal(bar_event.when, bs.Position.LONG, bar_event.bar.pair))
        elif:
            pass

        # Feed the technical indicator.
        value = float(bar_event.bar.close)
        self.bb.add(value)

        # Keep the last two values to check if there is a crossover.
        self._values = (self._values[-1], value)

        # Is the indicator ready ?
        if len(self.bb) < 2 or self.bb[-2] is None:
            return

        # Go long when price moves below lower band.
        if self._values[-2] >= self.bb[-2].lb and self._values[-1] < self.bb[-1].lb:
            self.push(bs.TradingSignal(bar_event.when, bs.Position.LONG, bar_event.bar.pair))
        # Go short when price moves above upper band.
        elif self._values[-2] <= self.bb[-2].ub and self._values[-1] > self.bb[-1].ub:
            self.push(bs.TradingSignal(bar_event.when, bs.Position.SHORT, bar_event.bar.pair))
        # Go neutral when the price touches the middle band.
        elif self._values[-2] < self.bb[-2].cb and self._values[-1] >= self.bb[-1].cb \
                or self._values[-2] > self.bb[-2].cb and self._values[-1] <= self.bb[-1].cb:
            self.push(bs.TradingSignal(bar_event.when, bs.Position.NEUTRAL, bar_event.bar.pair))
