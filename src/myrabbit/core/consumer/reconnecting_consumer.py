import logging
import time

from myrabbit.core.consumer.consumer import Consumer

LOGGER = logging.getLogger(__name__)


class ReconnectingConsumer:
    """This is an example consumer that will reconnect if the nested
    ExampleConsumer indicates that a reconnect is necessary.
    """

    def __init__(self, amqp_url, listeners):
        self._reconnect_delay = 0
        self._amqp_url = amqp_url
        self.listeners = listeners

        self._consumer = Consumer(self._amqp_url, self.listeners)
        self._should_run = True

    def run(self):
        while self._should_run:
            try:
                self._consumer.run()
            except KeyboardInterrupt:
                self._consumer.stop()
                break
            self._maybe_reconnect()

    def stop(self):
        self._consumer.stop()
        self._should_run = False

    def _maybe_reconnect(self):
        if self._consumer.should_reconnect:
            self._consumer.stop()
            reconnect_delay = self._get_reconnect_delay()
            LOGGER.info("Reconnecting after %d seconds", reconnect_delay)
            time.sleep(reconnect_delay)
            self._consumer = Consumer(self._amqp_url, self.listeners)

    def _get_reconnect_delay(self):
        if self._consumer.was_consuming:
            self._reconnect_delay = 0
        else:
            self._reconnect_delay += 1
        if self._reconnect_delay > 30:
            self._reconnect_delay = 30
        return self._reconnect_delay
