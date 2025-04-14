from decimal import Decimal
import asyncio
import logging

from basana.external.qmt import exchange as qmt_exchange
from basana.external.qmt import position_manager
import basana as bs

from samples.strategies import bbands


async def main():
    logging.basicConfig(level=logging.DEBUG, format="[%(asctime)s %(levelname)s] %(message)s")

    event_dispatcher = bs.realtime_dispatcher()
    pair = bs.Pair("600157.SH", "CNY")
    position_amount = Decimal(100)
    stop_loss_pct = Decimal(5)
    checkpoint_fname = "qmt_bbands_positions.json"
    qmt_path = 'D:\\ProgramFiles\\迅投极速策略交易系统交易终端 招商证券QMT测试37233版本\\userdata_mini'
    account_id = '07021349'

    exchange = qmt_exchange.Exchange(event_dispatcher, qmt_path, account_id)

    # Connect the strategy to the bar events from the exchange.
    strategy = bbands.Strategy(event_dispatcher, period=20, std_dev=1.5)
    # exchange.subscribe_to_bar_events(pair.base_symbol, "1s", strategy.on_bar_event)
    exchange.subscribe_to_multi_bar_events(["600157.SH", "002859.SZ", "159819.SZ"],"3s",  strategy.on_bar_event)

    # We'll be using the spot account, so there will be no short positions opened.
    position_mgr = position_manager.SpotAccountPositionManager(
        exchange, position_amount, pair.quote_symbol, stop_loss_pct, checkpoint_fname
    )
    # Connect the position manager to the strategy signals and to bar events just for logging.
    strategy.subscribe_to_trading_signals(position_mgr.on_trading_signal)
    exchange.subscribe_to_multi_bar_events(["600157.SH", "002859.SZ", "159819.SZ"], "3s", position_mgr.on_bar_event)

    await event_dispatcher.run()


if __name__ == "__main__":
    asyncio.run(main())
