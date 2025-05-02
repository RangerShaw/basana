import basana as bs
from decimal import Decimal


class Strategy(bs.Strategy):
    def __init__(self, buy_point: Decimal, sell_point: Decimal, name: str = ""):
        super().__init__(name)
        self.buy_point: Decimal = buy_point  # 当日涨幅
        self.sell_point: Decimal = sell_point  # 相对买入后最高价的比例
        self.high_after_buy: Decimal = Decimal('NaN')

    def update_high_after_buy(self):
        pass

    async def on_bar_event(self, bar_event: bs.BarEvent):
        curr_price, pct_change = bar_event.bar.close, bar_event.bar.pct_change
        ticker = bar_event.bar.pair.base_symbol
        free_asset, lock_asset, free_cash, lock_cash = self.order_controller.position(ticker)
        self.update_high_after_buy()

        if pct_change >= self.buy_point:
            amount = int(free_cash / curr_price / 100) * 100
            order = await self.order_controller.create_order_limit(
                bar_event.bar.pair.base_symbol, bs.OrderOperation.BUY, amount, curr_price
            )
        elif bar_event.bar.close / self.high_after_buy <= self.sell_point:
            order = await self.order_controller.create_order_limit(
                bar_event.bar.pair.base_symbol, bs.OrderOperation.SELL, free_asset, curr_price
            )
