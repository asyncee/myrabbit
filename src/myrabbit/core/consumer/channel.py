from dataclasses import dataclass

from pika.channel import Channel

from myrabbit.core.consumer.listener import Exchange, Listener, Queue


@dataclass
class ConsumedChannel:
    listener: Listener
    pika_channel: Channel
    consumer_tag: str = ""

    @property
    def exchange(self) -> Exchange:
        return self.listener.exchange

    @property
    def queue(self) -> Queue:
        return self.listener.queue
