from typing import Optional

import pika

from myrabbit.core.publisher import Publisher


class ReconnectingPublisherFactory:
    def __init__(self, amqp_url: str):
        self._amqp_url = amqp_url
        # self._publisher_connection: Optional[pika.BlockingConnection] = None

    # FIXME: use existing connection.
    def get_connection(self) -> pika.BlockingConnection:
        # TODO: implement reconnect on expected exceptions
        #   and reconnection strategy (block, retry times, delays).
        parameters = pika.URLParameters(self._amqp_url)

        # if self._publisher_connection is None or self._publisher_connection.is_closed:
        #     self._publisher_connection = pika.BlockingConnection(parameters)

        # return self._publisher_connection
        return pika.BlockingConnection(parameters)

    def publisher(self) -> Publisher:
        return Publisher(self.get_connection())
