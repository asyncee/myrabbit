import logging
import time
from typing import Type

from myrabbit.core.consumer.consumer import Consumer

logger = logging.getLogger(__name__)


class ReconnectingConsumer:
    """
    Consumer that automatically reconnects with increasing delay.
    """

    def __init__(self, consumer_cls: Type[Consumer], consumer_kwargs: dict) -> None:
        self._reconnect_delay = 0
        self._consumer_cls = consumer_cls
        self._consumer_kwargs = consumer_kwargs

        self._consumer = self.make_consumer()
        self._should_run = True

    def make_consumer(self) -> Consumer:
        return self._consumer_cls(**self._consumer_kwargs)

    def run(self) -> None:
        while self._should_run:
            try:
                self._consumer.run()
            except KeyboardInterrupt:
                self._consumer.stop()
                break
            self._maybe_reconnect()

    def stop(self) -> None:
        self._consumer.stop()
        self._should_run = False

    def _maybe_reconnect(self) -> None:
        if self._consumer.should_reconnect:
            self._consumer.stop()
            reconnect_delay = self._get_reconnect_delay()
            logger.info("Reconnecting after %d seconds", reconnect_delay)
            time.sleep(reconnect_delay)
            self._consumer = self.make_consumer()

    def _get_reconnect_delay(self) -> int:
        if self._consumer.was_consuming:
            self._reconnect_delay = 0
        else:
            self._reconnect_delay += 1
        if self._reconnect_delay > 30:
            self._reconnect_delay = 30
        return self._reconnect_delay
