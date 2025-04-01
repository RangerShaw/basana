from decimal import Decimal
from typing import Dict, Optional
import asyncio
import dataclasses
import datetime
import json
import logging
from xtquant import xttrader

from basana.core.logs import StructuredMessage
from basana.external.binance import exchange, spot
import basana as bs
from exchange import QmtExchange

@dataclasses.dataclass
class PositionInfo:
    ticker: str
    initial: Decimal
    initial_avg_price: Decimal
    target: Decimal
    order_id: Optional[str] = None

    @property
    def current(self) -> Decimal:
        # 需要根据实际成交更新
        return self.initial  # 示例值


class SpotAccountPositionManager:
    # Responsible for managing orders and tracking positions in response to trading signals.
    def __init__(
            self, exchange: QmtExchange, position_amount: Decimal, quote_symbol: str,
            stop_loss_pct: Decimal, checkpoint_fname: str
    ):
        assert position_amount > 0
        assert stop_loss_pct > 0

        self._exchange = exchange
        self._position_amount = position_amount
        self._quote_symbol = quote_symbol
        self._positions: Dict[bs.Pair, PositionInfo] = {}
        self._stop_loss_pct = stop_loss_pct
        self._checkpoint_fname = checkpoint_fname
        self._last_check_loss: Optional[datetime.datetime] = None

    def save(self):
        with open(self._checkpoint_fname, "w") as f:
            json_dict = {
                str(pair): dataclasses.asdict(pos_info) for pair, pos_info in self._positions.items()
            }
            for pos_info in json_dict.values():
                pos_info["order"] = pos_info["order"].json

            json.dump(json_dict, f, default=str)

    def load(self):
        with open(self._checkpoint_fname) as f:
            json_dict = json.load(f)
            json_dict = {
                bs.Pair(*pair.split("/")): PositionInfo(
                    pair=bs.Pair(*pair.split("/")),
                    initial=Decimal(pos_info["initial"]),
                    initial_avg_price=Decimal(pos_info["initial_avg_price"]),
                    target=Decimal(pos_info["target"]),
                    order=spot.OrderInfo(pos_info["order"], []),
                ) for pair, pos_info in json_dict.items()
            }
            self._positions = json_dict

    async def cancel_open_orders(self, pair: bs.Pair):
        open_orders = await self._exchange.spot_account.get_open_orders(pair)
        await asyncio.gather(*[
            self._exchange.spot_account.cancel_order(pair, order_id=open_order.id)
            for open_order in open_orders
        ])

    async def get_position_info(self, pair: bs.Pair) -> Optional[PositionInfo]:
        pos_info = self._positions.get(pair)
        if pos_info and pos_info.order_open:
            pos_info.order = await self._exchange.spot_account.get_order_info(pair, order_id=pos_info.order.id)
            self.save()
        return pos_info

    async def check_loss(self):
        pairs = [pos_info.pair for pos_info in self._positions.values() if pos_info.current != 0]
        # For every pair get position information along with bid and ask prices.
        coros = [self.get_position_info(pair) for pair in pairs]
        coros.extend(self._exchange.get_bid_ask(pair) for pair in pairs)
        res = await asyncio.gather(*coros)
        midpoint = int(len(res) / 2)
        all_pos_info = res[0:midpoint]
        all_bid_ask = res[midpoint:]

        # Log each position an check PnL.
        for pos_info, (bid, ask) in zip(all_pos_info, all_bid_ask):
            pnl_pct = pos_info.calculate_unrealized_pnl_pct(bid, ask)
            logging.info(StructuredMessage(
                f"Position for {pos_info.pair}", current=pos_info.current, target=pos_info.target,
                avg_price=pos_info.avg_price, pnl_pct=pnl_pct, order_open=pos_info.order_open
            ))
            if pnl_pct <= self._stop_loss_pct * -1:
                logging.info(f"Stop loss for {pos_info.pair}")
                await self.switch_position(pos_info.pair, bs.Position.NEUTRAL, force=True)

    async def switch_position(self, pair: bs.Pair, target_position: bs.Position, force: bool = False):
        pass

    async def adjust_position(self, symbol: str, current: Decimal, target: Decimal, bid: Decimal, ask: Decimal):
        """执行实际调仓操作"""
        # 计算需要调整的数量
        delta = target - current

        if delta == 0:
            logging.debug("%s 无需调仓", symbol)
            return

        # 确定买卖方向和价格
        if delta > 0:
            order_type = STOCK_BUY
            price = float(ask)  # 以卖一价买入
        else:
            order_type = STOCK_SELL
            price = float(bid)  # 以买一价卖出

        # 发送限价单
        try:
            order_id = self.xt.order_stock(
                self.account,
                symbol,
                order_type,
                abs(int(delta)),  # QMT要求整数股数
                FIX_PRICE,
                price,
                strategy_name="PositionManager",
                order_remark="系统调仓"
            )

            # 更新仓位信息
            self.positions[symbol] = PositionInfo(
                symbol=symbol,
                initial=current,
                initial_avg_price=self._calc_new_avg_price(current, abs(delta), price, order_type),
                target=target,
                order_id=order_id
            )

            logging.info("已下单 %s %s %d股 @%.2f",
                         symbol, "买入" if delta > 0 else "卖出", abs(delta), price)

        except Exception as e:
            logging.error("下单失败: %s", str(e))
            raise

    async def on_trading_signal(self, trading_signal: bs.TradingSignal):
        pairs = list(trading_signal.get_pairs())
        logging.info(StructuredMessage("Trading signal", pairs=pairs))

        try:
            coros = []
            for pair, target_position in pairs:
                # No borrowing with spot account.
                if target_position == bs.Position.SHORT:
                    target_position = bs.Position.NEUTRAL
                coros.append(self.switch_position(pair, target_position))
            await asyncio.gather(*coros)
        except Exception as e:
            logging.exception(e)

    async def on_bar_event(self, bar_event: bs.BarEvent):
        bar = bar_event.bar
        logging.info(StructuredMessage(bar.pair, close=bar.close))
        if self._last_check_loss is None or self._last_check_loss < bar_event.when:
            self._last_check_loss = bar_event.when
            await self.check_loss()


def signed_to_position(signed):
    if signed > 0:
        return bs.Position.LONG
    elif signed < 0:
        return bs.Position.SHORT
    else:
        return bs.Position.NEUTRAL
