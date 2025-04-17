import basana as bs
from decimal import Decimal


class Strategy(bs.TradingSignalSource, bs.Strategy):
    def __init__(self, dispatcher: bs.EventDispatcher, o_controller: bs.OrderController, buy_point: Decimal,
                 sell_point: Decimal):
        super().__init__(dispatcher)
        self.o_controller: bs.OrderController = o_controller
        self.buy_point: Decimal = buy_point  # 当日涨幅
        self.sell_point: Decimal = sell_point  # 相对买入后最高价的比例
        self.high_after_buy: Decimal = Decimal('NaN')
        self._values = (None, None)

    def update_high_after_buy(self):
        pass

    async def on_bar_event(self, bar_event: bs.BarEvent, broker: bs.OrderController):
        pct_change = bar_event.bar.pct_change
        ticker = bar_event.bar.pair.base_symbol
        free_asset, lock_asset, free_cash, lock_cash = broker.position_detail(ticker)
        self.update_high_after_buy()

        if pct_change >= 0.8:
            self.o_controller.create_order_market(bar_event.bar.pair.base_symbol, bs.OrderOperation.BUY, )
        elif bar_event.bar.close / self.high_after_buy <= self.sell_point:
            self.o_controller.create_order_market(bar_event.bar.pair.base_symbol, bs.OrderOperation.SELL, )

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
