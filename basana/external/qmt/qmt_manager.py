from urllib.parse import urljoin
import abc
import asyncio
import datetime
import json
import logging
import time

import aiohttp
from typing import Dict, List, Optional, Any, Set, cast, Callable

from . import config,  klines
from basana.core import dispatcher, bar, logs, event, helpers, websockets as core_ws
from basana.core.config import get_config_value
from basana.core.pair import Pair

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

    async def resolve_stream_name(self, api_client: client.APIClient):
        pass

    def keep_alive_period(self, config_overrides: dict = {}) -> Optional[datetime.timedelta]:
        return None

    async def keep_alive(self, api_client: client.APIClient):  # pragma: no cover
        pass


class PublicChannel(Channel):
    def __init__(self, name: str):
        self._name = name

    @property
    def alias(self) -> str:
        return self._name

    @property
    def stream(self) -> str:
        return self._name


class QmtClient(event.Producer, metaclass=abc.ABCMeta):
    def __init__(
            self, dispatcher: dispatcher.EventDispatcher, api_client: client.APIClient,
            session: Optional[aiohttp.ClientSession] = None, config_overrides: dict = {}
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
        self._pending_subscriptions: Set[str] = set()
        self.backoff_secs = 1
        self._run_called = False
        self._heartbeat = get_config_value(config.DEFAULTS, "api.websockets.heartbeat", overrides=config_overrides)

        self._dispatcher = dispatcher
        self._cli = api_client
        self._alias_to_channel: Dict[str, Channel] = {}
        self._stream_to_channel: Dict[str, Channel] = {}
        self._next_keep_alive: Dict[str, datetime.datetime] = {}
        self._next_msg_id = int(time.time() * 1000)

    def set_channel_event_source_ex(self, channel: Channel, event_source: ChannelEventSource):
        assert channel.alias not in self._alias_to_channel, "channel already registered"
        assert channel not in self._event_sources, "channel already registered"
        self._event_sources[channel.alias] = event_source
        self._pending_subscriptions.add(channel.alias)
        self._subscribe_request.set()
        self._alias_to_channel[channel.alias] = channel

    def get_channel_event_source_ex(self, channel: Channel) -> Optional[ChannelEventSource]:
        return self._event_sources.get(channel.alias)

    async def subscribe_to_channels(self, channel_aliases: List[str], ws_cli: aiohttp.ClientWebSocketResponse):
        logger.debug(logs.StructuredMessage("Subscribing", src=self, channels=channel_aliases))

        # Give a chance for dynamic channels to resolve the stream name.
        channels: List[Channel] = [self._alias_to_channel[alias] for alias in channel_aliases]
        await asyncio.gather(*[
            channel.resolve_stream_name(self._cli) for channel in channels
        ])
        self._stream_to_channel.update({
            channel.stream: channel for channel in channels
        })

        msg_id = self._get_next_msg_id()
        await ws_cli.send_str(json.dumps({
            "id": msg_id,
            "method": "SUBSCRIBE",
            "params": [channel.stream for channel in channels]
        }))

        # Schedule keep alives.
        for channel in channels:
            self._schedule_keep_alive(channel)

    async def subscribe_to_stocks(self, tickers: List[str], ws_cli: aiohttp.ClientWebSocketResponse):
        logger.debug(logs.StructuredMessage("Subscribing", src=self, channels=tickers))
        xtdata.subscribe_whole_quote(tickers, callback=self._on_quote_datas)
        # self._schedule_keep_alive(tickers)

    def schedule_resubscription(self, channels: List[str]):
        self._pending_subscriptions.update(channels)

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
            prev_attemp_age = time.time() - last_connect_ts
            if prev_attemp_age < self.backoff_secs:
                await asyncio.sleep(self.backoff_secs - prev_attemp_age)

            try:
                logger.debug(logs.StructuredMessage("Connecting QMT", src=self))
                last_connect_ts = time.time()

                async with helpers.TaskGroup() as tg:
                    # Turn this off since we just reconnected.
                    self._reconnect_request.clear()

                    # Connect to all channels.
                    self._pending_subscriptions.update(self._event_sources.keys())
                    self._subscribe_request.set()

                    # Run tasks.
                    tg.create_task(self._msg_loop(ws_cli))
                    tg.create_task(self._subscribe_loop(ws_cli))
                    tg.create_task(self._reconnect(ws_cli))
            except Exception as e:
                await self.on_error(e)

    async def _msg_loop(self, ws_cli: aiohttp.ClientWebSocketResponse):
        logger.debug(logs.StructuredMessage("Running message loop", src=self))

        # The iterator exits normally when the connection is closed with close code 1000 (OK) or 1001 (going away).
        # It raises a ConnectionClosedError when the connection is closed with any other code.
        async for message in ws_cli:
            handled = False
            if message.type == aiohttp.WSMsgType.TEXT:
                json_msg = json.loads(message.data)
                handled = await self.handle_message(json_msg)
            if not handled:
                await self.on_unknown_message(message)

        # If the message loop finished we need to notify the other tasks so they can finish as well.
        self._subscribe_request.set()
        self._reconnect_request.set()

    async def _subscribe_loop(self, ws_cli: aiohttp.ClientWebSocketResponse):
        while not ws_cli.closed:
            await self._subscribe_request.wait()
            self._subscribe_request.clear()
            if not ws_cli.closed and self._pending_subscriptions:
                channels = list(self._pending_subscriptions)
                self._pending_subscriptions = set()
                await self.subscribe_to_channels(channels, ws_cli)

    async def _reconnect(self, ws_cli: aiohttp.ClientWebSocketResponse):
        # Will exit when reconnection is requested or when its canceled.
        await self._reconnect_request.wait()
        self._reconnect_request.clear()
        # If the client is already closed then there is nothing left to do.
        if not ws_cli.closed:
            await ws_cli.close()

    async def handle_message(self, message: dict) -> bool:
        coro = None

        # A response to a message we sent.
        if {"result", "id"} <= set(message.keys()):
            coro = self._on_response(message)
        # A message associated to a channel.
        elif stream := message.get("stream"):
            channel = self._stream_to_channel.get(stream)
            assert channel, f"{stream} could not be mapped to a channel instance"
            # Resubscribe to the channel if the listen key expired.
            if message.get("data", {}).get("e") == "listenKeyExpired":
                logger.debug(logs.StructuredMessage(
                    "License key expired. Scheduling re-subscription", alias=channel.alias
                ))
                self.schedule_resubscription([channel.alias])
            # Get the event source for the channel alias.
            if event_source := self.get_channel_event_source(channel.alias):
                coro = event_source.push_from_message(message)

        ret = False
        if coro:
            await coro
            ret = True
        return ret

    async def _on_quote_datas(self, datas: dict):
        for ticker, data in datas.items():
            if source := self._event_sources.get(ticker):
                source.push_data(data)

    async def _on_response(self, message: dict):
        if message["result"] is not None:
            await self.on_error(message)

    def _get_next_msg_id(self) -> int:
        ret = self._next_msg_id
        self._next_msg_id += 1
        return ret

    def _keep_alive_channel(self, channel: Channel) -> dispatcher.SchedulerJob:
        async def scheduler_job():
            if self._next_keep_alive[channel.alias] <= self._dispatcher.now():
                logger.debug(logs.StructuredMessage("Channel keep alive", alias=channel.alias))
                try:
                    await channel.keep_alive(self._cli)
                finally:
                    self._schedule_keep_alive(channel)

        return scheduler_job

    def _schedule_keep_alive(self, channel: Channel):
        period = channel.keep_alive_period(self._config_overrides)
        if period:
            schedule_dt = self._dispatcher.now() + period
            logger.debug(logs.StructuredMessage("Scheduling keep alive", when=schedule_dt, alias=channel.alias))
            self._next_keep_alive[channel.alias] = schedule_dt
            self._dispatcher.schedule(schedule_dt, self._keep_alive_channel(channel))


class QmtClientManager:
    def __init__(
            self, dispatch: dispatcher.EventDispatcher, api_client: client.APIClient,
            session: Optional[aiohttp.ClientSession] = None, config_overrides: dict = {}
    ):
        self._dispatcher = dispatch
        self._cli = api_client
        self._session = session
        self._config_overrides = config_overrides
        self._websocket: Optional[QmtClient] = None

    def subscribe_to_bar_events(self, pair: Pair, interval: str, event_handler: bar.BarEventHandler):
        self._subscribe_to_ws_channel_events(
            PublicChannel(klines.get_channel(pair, interval)),
            lambda ws_cli: klines.WebSocketEventSource(pair, ws_cli),
            cast(dispatcher.EventHandler, event_handler)
        )

    def subscribe_to_multi_bar_events(self, tickers: [str], event_handler: callable):
        """订阅多只股票的K线数据"""
        xtdata.subscribe_whole_quote(tickers, callback=event_handler)
        self.subscribed_pairs.add(tickers)

    def _subscribe_to_ws_channel_events(
            self, channel: Channel,
            event_src_factory: Callable[[QmtClient], ChannelEventSource],
            event_handler: dispatcher.EventHandler
    ):
        # Get/create the event source for the channel.
        ws_cli = self._get_ws_client()
        event_source = ws_cli.get_channel_event_source_ex(channel)
        if not event_source:
            event_source = event_src_factory(ws_cli)
            ws_cli.set_channel_event_source_ex(channel, event_source)

        # Subscribe the event handler to the event source.
        self._dispatcher.subscribe(event_source, event_handler)

    def _get_ws_client(self) -> QmtClient:
        if self._websocket is None:
            self._websocket = QmtClient(
                self._dispatcher, self._cli, session=self._session, config_overrides=self._config_overrides
            )
        return self._websocket
