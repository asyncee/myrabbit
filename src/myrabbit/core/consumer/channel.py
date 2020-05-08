from dataclasses import dataclass

from pika.channel import Channel

from myrabbit.core.consumer.listener import Listener


@dataclass
class ConsumedChannel:
    listener: Listener
    pika_channel: Channel
    consumer_tag: str = ""

    @property
    def exchange(self):
        return self.listener.exchange

    @property
    def queue(self):
        return self.listener.queue
