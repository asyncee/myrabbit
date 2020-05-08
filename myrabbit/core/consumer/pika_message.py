import logging
from dataclasses import dataclass

import pika
from pika.channel import Channel
from pika.spec import Basic

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PikaMessage:
    channel: Channel
    basic_deliver: Basic.Deliver
    properties: pika.BasicProperties
    body: bytes

    def requeue(self):
        logger.info("Requeue message %s", self)
        self.channel.connection.ioloop.add_callback_threadsafe(
            lambda: self.channel.basic_reject(
                self.basic_deliver.delivery_tag, requeue=True
            )
        )

    def acknowledge(self):
        logger.info("Acknowledging message %s", self)
        self.channel.connection.ioloop.add_callback_threadsafe(
            lambda: self.channel.basic_ack(self.basic_deliver.delivery_tag)
        )

    def reply(self, body: bytes):
        if not self.properties.reply_to:
            raise ValueError(
                f"Can not reply to message {self}: "
                f"invalid 'reply_to' value: {self.properties.reply_to!r}"
            )

        logger.info("Replying to %s", self.properties.reply_to)
        self.channel.connection.ioloop.add_callback_threadsafe(
            lambda: self.channel.basic_publish(
                exchange="", routing_key=self.properties.reply_to, body=body
            )
        )
