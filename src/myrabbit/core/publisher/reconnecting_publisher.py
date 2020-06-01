import abc

import pika

from myrabbit.core.publisher import Publisher


class PublisherFactory(abc.ABC):
    @abc.abstractmethod
    def get_connection(self) -> pika.BlockingConnection:
        pass

    @abc.abstractmethod
    def publisher(self) -> Publisher:
        pass


class ReconnectingPublisherFactory(PublisherFactory):
    def __init__(self, amqp_url: str):
        self._amqp_url = amqp_url

    def get_connection(self) -> pika.BlockingConnection:
        # TODO: implement reconnection on expected exceptions
        #   and reconnection strategy (block, retry times, delays).
        parameters = pika.URLParameters(self._amqp_url)
        return pika.BlockingConnection(parameters)

    def publisher(self) -> Publisher:
        return Publisher(self.get_connection())
