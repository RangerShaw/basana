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
        curr_price, pct_change = bar_event.bar.close, bar_event.bar.pct_change
        ticker = bar_event.bar.pair.base_symbol
        free_asset, lock_asset, free_cash, lock_cash = broker.position(ticker)
        self.update_high_after_buy()

        if pct_change >= 0.8:
            amount = int(free_cash / curr_price / 100) * 100
            await self.o_controller.create_order_market(bar_event.bar.pair.base_symbol, bs.OrderOperation.BUY, amount)
        elif bar_event.bar.close / self.high_after_buy <= self.sell_point:
            await self.o_controller.create_order_market(bar_event.bar.pair.base_symbol, bs.OrderOperation.SELL, free_asset)

