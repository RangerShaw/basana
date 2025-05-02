from urllib.parse import urljoin
import abc
import asyncio
import datetime
import logging
import time

import aiohttp
from typing import Dict, List, Optional, Any, Set, cast, Callable
from functools import partial, partialmethod

from . import config, klines
from basana.core import dispatcher, bar, logs, event, helpers
from basana.core.config import get_config_value
from basana.core.pair import Pair
from . import qmt_helpers

from xtquant import xttrader, xtdata, xtconstant
from xtquant.xttype import StockAccount
from xtquant.xttrader import XtQuantTrader, XtQuantTraderCallback

logger = logging.getLogger(__name__)


class ChannelEventSource(event.FifoQueueEventSource):
    def __init__(self, producer: event.Producer):
        super().__init__(producer=producer)

    @abc.abstractmethod
    async def push_from_message(self, message: dict):
        raise NotImplementedError()


class Channel(metaclass=abc.ABCMeta):
    @property
    @abc.abstractmethod
    def alias(self) -> str:
        raise NotImplementedError()

    @property
    @abc.abstractmethod
    def stream(self) -> str:
        raise NotImplementedError()

    def keep_alive_period(self, config_overrides: dict = {}) -> Optional[datetime.timedelta]:
        return None

    # async def resolve_stream_name(self, api_client: client.APIClient):
    #     pass
    #
    # async def keep_alive(self, api_client: client.APIClient):  # pragma: no cover
    #     pass


class PublicChannel(Channel):
    def __init__(self, name: str):
        self._name = name

    @property
    def alias(self) -> str:
        return self._name

    @property
    def stream(self) -> str:
        return self._name


# Generate BarEvents events from websocket messages.
class WebSocketEventSource(ChannelEventSource):
    def __init__(self, producer: event.Producer):
        super().__init__(producer=producer)

    def push_from_message(self, message: dict):
        for ticker, data in message.items():
            print(ticker, data)
            t = qmt_helpers.timestamp_to_datetime(data['time'])
            this_bar = klines.Bar(t, ticker, data)
            self.push(bar.BarEvent(t, this_bar))


class QmtClient(event.Producer, metaclass=abc.ABCMeta):
    def __init__(
            self, dispatcher: dispatcher.EventDispatcher, session: int = None,
            config_overrides: dict = {}
    ):
        url = urljoin(
            get_config_value(config.DEFAULTS, "api.websockets.base_url", overrides=config_overrides),
            "/stream"
        )
        self._url = url
        self._session = session
        self._config_overrides = config_overrides
        self._event_sources: Dict[str, ChannelEventSource] = {}
        self._reconnect_request = asyncio.Event()
        self._subscribe_request = asyncio.Event()
        self._finished = asyncio.Event()
        self._pending_subscriptions: Dict[str, Set[str]] = {}  # channels and tickers
        self.backoff_secs = 1
        self._run_called = False
        self._heartbeat = get_config_value(config.DEFAULTS, "api.websockets.heartbeat", overrides=config_overrides)

        self._dispatcher = dispatcher
        self._alias_to_channel: Dict[str, Channel] = {}
        self._next_keep_alive: Dict[str, datetime.datetime] = {}
        self._next_msg_id = int(time.time() * 1000)

    def set_channel_event_source_ex(self, channel: Channel, tickers: List[str], event_source: ChannelEventSource):
        assert channel.alias not in self._alias_to_channel, "channel already registered"
        assert channel not in self._event_sources, "channel already registered"
        self._event_sources[channel.alias] = event_source
        self._pending_subscriptions[channel.alias] = set(tickers)
        self._subscribe_request.set()
        self._alias_to_channel[channel.alias] = channel

    def get_channel_event_source_ex(self, channel: Channel) -> Optional[ChannelEventSource]:
        return self._event_sources.get(channel.alias)

    async def subscribe_to_stocks(self, channel_alias: str, tickers: List[str]):
        logger.debug(logs.StructuredMessage("Subscribing", src=self, channels=tickers))
        xtdata.subscribe_whole_quote(tickers, callback=partial(self._on_quote_datas, channel_alias))
        # self._schedule_keep_alive(tickers)

    def schedule_resubscription(self, channel_alias: str, tickers: Set[str]):
        self._pending_subscriptions[channel_alias].update(tickers)

    async def on_error(self, error: Any):
        logger.error(logs.StructuredMessage("Error", src=self, error=error))

    async def on_unknown_message(self, message: aiohttp.WSMessage):
        logger.warning(logs.StructuredMessage("Unknown message", src=self, type=message.type, data=message.data))

    async def main(self):
        assert not self._run_called, "run already called"

        self._run_called = True
        last_connect_ts = 0
        while True:
            # Backoff, if necessary, before connecting.
            prev_attempt_age = time.time() - last_connect_ts
            if prev_attempt_age < self.backoff_secs:
                await asyncio.sleep(self.backoff_secs - prev_attempt_age)

            try:
                logger.debug(logs.StructuredMessage("Connecting QMT", src=self))
                last_connect_ts = time.time()

                async with helpers.TaskGroup() as tg:
                    # Turn this off since we just reconnected.
                    self._reconnect_request.clear()

                    # Connect to all channels.
                    # self._pending_subscriptions.update(self._event_sources.keys())
                    self._subscribe_request.set()

                    # Run tasks.
                    tg.create_task(self._subscribe_loop())
                    # tg.create_task(self._msg_loop(ws_cli))
                    # tg.create_task(self._reconnect(ws_cli))
            except Exception as e:
                await self.on_error(e)

    async def _subscribe_loop(self):
        while True:
            await self._subscribe_request.wait()
            self._subscribe_request.clear()
            if xtdata.get_client().is_connected():
                logger.debug(logs.StructuredMessage("Subscribing", _pending_subscriptions=self._pending_subscriptions))
                for channel_alias, tickers in self._pending_subscriptions.items():
                    await self.subscribe_to_stocks(channel_alias, list(tickers))
                self._pending_subscriptions = {}

    def _on_quote_datas(self, channel_alias: str, datas: dict):
        if source := self._event_sources.get(channel_alias):
            source.push_from_message(datas)

    async def _on_response(self, message: dict):
        if message["result"] is not None:
            await self.on_error(message)

    def _get_next_msg_id(self) -> int:
        ret = self._next_msg_id
        self._next_msg_id += 1
        return ret


class QmtTraderCallback(XtQuantTraderCallback):
    def on_disconnected(self):
        print("connection lost")

    def on_stock_order(self, order):
        print("on order callback:")
        print(order.stock_code, order.order_status, order.order_sysid)

    def on_stock_trade(self, trade):
        print("on trade callback")
        print(trade.account_id, trade.stock_code, trade.order_id)

    def on_order_error(self, order_error):
        print("on order_error callback")
        print(order_error.order_id, order_error.error_id, order_error.error_msg)

    def on_cancel_error(self, cancel_error):
        print("on cancel_error callback")
        print(cancel_error.order_id, cancel_error.error_id, cancel_error.error_msg)

    def on_order_stock_async_response(self, response):
        print("on_order_stock_async_response")
        print(response.account_id, response.order_id, response.seq)

    def on_account_status(self, status):
        print("on_account_status")
        print(status.account_id, status.account_type, status.status)


class QmtClientManager:
    def __init__(
            self, dispatch: dispatcher.EventDispatcher, session: int = None,
            config_overrides: dict = {}
    ):
        self._dispatcher = dispatch
        self._session = session
        self._config_overrides = config_overrides
        self._websocket: Optional[QmtClient] = None

    def subscribe_to_bar_events(self, pair: Pair, interval: str, event_handler: bar.BarEventHandler):
        self._subscribe_to_ws_channel_events(
            PublicChannel(klines.get_channel(interval)),
            [pair.base_symbol],
            lambda ws_cli: WebSocketEventSource(ws_cli),
            cast(dispatcher.EventHandler, event_handler)
        )

    def subscribe_to_multi_bar_events(self, tickers: [str], interval: str, event_handler: callable):
        self._subscribe_to_ws_channel_events(
            PublicChannel(klines.get_channel(interval)),
            tickers,
            lambda ws_cli: WebSocketEventSource(ws_cli),
            cast(dispatcher.EventHandler, event_handler)
        )

    def _subscribe_to_ws_channel_events(
            self, channel: Channel, tickers: List[str], event_src_factory: Callable[[QmtClient], ChannelEventSource],
            event_handler: dispatcher.EventHandler
    ):
        # Get/create the event source for the channel.
        qmt_cli = self._get_qmt_client()
        event_source = qmt_cli.get_channel_event_source_ex(channel)
        if not event_source:
            event_source = event_src_factory(qmt_cli)
            qmt_cli.set_channel_event_source_ex(channel, tickers, event_source)

        # Subscribe the event handler to the event source.
        self._dispatcher.subscribe(event_source, event_handler)

    def _get_qmt_client(self) -> QmtClient:
        if self._websocket is None:
            self._websocket = QmtClient(
                self._dispatcher, session=self._session, config_overrides=self._config_overrides
            )
        return self._websocket
