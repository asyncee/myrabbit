import logging
from dataclasses import dataclass

import pika
from pika.channel import Channel
from pika.spec import Basic

from myrabbit.core.consumer.reply import Reply

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PikaMessage:
    channel: Channel
    basic_deliver: Basic.Deliver
    properties: pika.BasicProperties
    body: bytes

    def requeue(self) -> None:
        logger.info("Requeue message %s", self)
        self.channel.connection.ioloop.add_callback_threadsafe(
            lambda: self.channel.basic_reject(
                self.basic_deliver.delivery_tag, requeue=True
            )
        )

    def acknowledge(self) -> None:
        logger.info("Acknowledging message %s", self)
        self.channel.connection.ioloop.add_callback_threadsafe(
            lambda: self.channel.basic_ack(self.basic_deliver.delivery_tag)
        )

    def reply(self, reply: Reply) -> None:
        if not self.properties.reply_to:
            raise ValueError(
                f"Can not reply to message {self}: "
                f"invalid 'reply_to' value: {self.properties.reply_to!r}"
            )

        reply_rk = self.properties.reply_to
        if reply_rk.startswith("amq."):
            # Direct reply to, exchange must be default.
            reply_exchange = ""
        else:
            # Reply to the exchange message come from.
            reply_exchange = self.basic_deliver.exchange

        logger.info(
            "Replying to exchange %s, routing key %s [%s] with %s",
            reply_exchange,
            reply_rk,
            self.properties.correlation_id,
            reply.body,
        )

        properties = reply.properties or pika.BasicProperties()
        if properties.correlation_id is None:
            properties.correlation_id = self.properties.correlation_id

        self.channel.connection.ioloop.add_callback_threadsafe(
            lambda: self.channel.basic_publish(
                exchange=reply_exchange,
                routing_key=reply_rk,
                body=reply.body,
                properties=properties,
            )
        )
