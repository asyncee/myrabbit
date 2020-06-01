import functools
import logging
from concurrent.futures.thread import ThreadPoolExecutor
from functools import partial
from typing import Any, Callable, Dict, List, Optional

import pika
from pika import SelectConnection
from pika.adapters.asyncio_connection import AsyncioConnection
from pika.channel import Channel
from pika.connection import Connection
from pika.spec import Basic, Exchange, Queue

from myrabbit.core.consumer.channel import ConsumedChannel
from myrabbit.core.consumer.listener import Listener
from myrabbit.core.consumer.pika_message import PikaMessage

logger = logging.getLogger(__name__)


class Consumer(object):
    """This is an example consumer that will handle unexpected interactions
    with RabbitMQ such as channel and connection closures.
    If RabbitMQ closes the connection, this class will stop and indicate
    that reconnection is necessary. You should look at the output, as
    there are limited reasons why the connection may be closed, which
    usually are tied to permission related issues or socket timeouts.
    If the channel is closed, it will indicate a problem with one of the
    commands that were issued and that should surface in the output as well.
    """

    def __init__(
        self, amqp_url: str, listeners: List[Listener], prefetch_count: int = 1
    ):
        """Create a new instance of the consumer class, passing in the AMQP
        URL used to connect to RabbitMQ.
        :param str amqp_url: The AMQP url to connect with
        """
        self.should_reconnect = False
        self.was_consuming = False

        self._listeners: List[Listener] = listeners
        self._channels: Dict[int, ConsumedChannel] = {}

        self._connection = None
        self._closing = False
        self._url = amqp_url
        self._consuming = False
        # In production, experiment with higher prefetch values
        # for higher consumer throughput
        self._prefetch_count = prefetch_count

    def connect(self) -> SelectConnection:
        """This method connects to RabbitMQ, returning the connection handle.
        When the connection is established, the on_connection_open method
        will be invoked by pika.
        :rtype: pika.adapters.asyncio_connection.AsyncioConnection
        """
        logger.info("Connecting to %s", self._url)
        return SelectConnection(
            parameters=pika.URLParameters(self._url),
            on_open_callback=self.on_connection_open,
            on_open_error_callback=self.on_connection_open_error,
            on_close_callback=self.on_connection_closed,
        )

    def maybe_close_connection(self) -> None:
        if not self._channels:
            self.close_connection()

    def close_connection(self) -> None:
        assert self._connection

        self._consuming = False
        if self._connection.is_closing or self._connection.is_closed:
            logger.info("Connection is closing or already closed")
        else:
            logger.info("Closing connection")
            self._connection.close()

    def on_connection_open(self, _unused_connection: SelectConnection) -> None:
        """This method is called by pika once the connection to RabbitMQ has
        been established. It passes the handle to the connection object in
        case we need it, but in this case, we'll just mark it unused.
        """
        logger.info("Connection opened")

        for listener in self._listeners:
            self.open_channel(listener)

    def on_connection_open_error(
        self, _unused_connection: AsyncioConnection, err: Exception
    ) -> None:
        """This method is called by pika if the connection to RabbitMQ
        can't be established.
        """
        logger.error("Connection open failed: %s", err)
        self.reconnect()

    def on_connection_closed(
        self, _unused_connection: Connection, reason: Exception
    ) -> None:
        """
        This method is invoked by pika when the connection to RabbitMQ is
        closed unexpectedly. Since it is unexpected, we will reconnect to
        RabbitMQ if it disconnects.
        """
        assert self._connection

        if self._closing:
            self._connection.ioloop.stop()
        else:
            logger.warning("Connection closed, reconnect necessary: %s", reason)
            self.reconnect()

    def reconnect(self) -> None:
        """
        Will be invoked if the connection can't be opened or is
        closed. Indicates that a reconnect is necessary then stops the
        ioloop.
        """
        self.should_reconnect = True
        self.stop()

    def open_channel(self, listener: Listener) -> None:
        """Open a new channel with RabbitMQ by issuing the Channel.Open RPC
        command. When RabbitMQ responds that the channel is open, the
        on_channel_open callback will be invoked by pika.
        """
        assert self._connection
        logger.info("Creating a new channel")
        self._connection.channel(
            on_open_callback=partial(self.on_channel_open, listener=listener)
        )

    def on_channel_open(self, channel: Channel, listener: Listener) -> None:
        """This method is invoked by pika when the channel has been opened.
        The channel object is passed in so we can make use of it.
        Since the channel is now open, we'll declare the exchange to use.
        :param pika.channel.Channel channel: The channel object
        """
        logger.info("Channel opened")
        consumed_channel = ConsumedChannel(listener=listener, pika_channel=channel)
        self.remember_channel(consumed_channel)

        self.add_on_channel_close_callback(consumed_channel)
        self.setup_exchange(consumed_channel)

    def remember_channel(self, channel: ConsumedChannel) -> None:
        self._channels[int(channel.pika_channel)] = channel

    def forget_channel(self, channel: ConsumedChannel) -> None:
        self._channels.pop(int(channel.pika_channel), None)

    def add_on_channel_close_callback(self, consumed_channel: ConsumedChannel) -> None:
        """This method tells pika to call the on_channel_closed method if
        RabbitMQ unexpectedly closes the channel.
        """
        consumed_channel.pika_channel.add_on_close_callback(
            partial(self.on_channel_closed, consumed_channel=consumed_channel)
        )

    def on_channel_closed(
        self, channel: Channel, reason: Exception, consumed_channel: ConsumedChannel
    ) -> None:
        """Invoked by pika when RabbitMQ unexpectedly closes the channel.
        Channels are usually closed if you attempt to do something that
        violates the protocol, such as re-declare an exchange or queue with
        different parameters. In this case, we'll close the connection
        to shutdown the object.
        """
        logger.warning("Channel %i was closed: %s", channel, reason)
        self.forget_channel(consumed_channel)
        self.maybe_close_connection()

    def setup_exchange(self, consumed_channel: ConsumedChannel) -> None:
        """
        Setup the exchange on RabbitMQ by invoking the Exchange.Declare RPC
        command. When it is complete, the on_exchange_declareok method will
        be invoked by pika.
        """
        logger.info("Declaring exchange: %s", consumed_channel.exchange)
        exchange = consumed_channel.exchange

        if exchange.name == "":
            self.setup_queue(consumed_channel)
            return

        consumed_channel.pika_channel.exchange_declare(
            exchange=exchange.name,
            exchange_type=exchange.type,
            callback=functools.partial(
                self.on_exchange_declareok, channel=consumed_channel
            ),
            durable=exchange.durable,
            auto_delete=exchange.auto_delete,
        )

    def on_exchange_declareok(
        self, _unused_frame: Exchange.DeclareOk, channel: ConsumedChannel
    ) -> None:
        """
        Invoked by pika when RabbitMQ has finished the
        Exchange.Declare RPC command.
        """
        logger.info("Exchange declared: %s", channel.exchange)
        self.setup_queue(channel)

    def setup_queue(self, channel: ConsumedChannel) -> None:
        """
        Setup the queue on RabbitMQ by invoking the Queue.Declare RPC
        command. When it is complete, the on_queue_declareok method will
        be invoked by pika.
        """
        logger.info("Declaring queue %s", channel.queue)
        cb = functools.partial(self.on_queue_declareok, channel=channel)
        queue = channel.queue
        channel.pika_channel.queue_declare(
            queue=queue.name,
            durable=queue.durable,
            exclusive=queue.exclusive,
            auto_delete=queue.auto_delete,
            callback=cb,
        )

    def on_queue_declareok(
        self, _unused_frame: Queue.DeclareOk, channel: ConsumedChannel
    ) -> None:
        """
        Method invoked by pika when the Queue.Declare RPC call made in
        setup_queue has completed. In this method we will bind the queue
        and exchange together with the routing key by issuing the Queue.Bind
        RPC command. When this command is complete, the on_bindok method will
        be invoked by pika.
        """
        if channel.exchange.name == "":
            self.set_qos(channel)
            return

        logger.info(
            "Binding %s to %s with routing key %s",
            channel.exchange,
            channel.queue,
            channel.listener.routing_key,
        )
        cb = functools.partial(self.on_bindok, channel=channel)
        channel.pika_channel.queue_bind(
            channel.queue.name,
            channel.exchange.name,
            routing_key=channel.listener.routing_key,
            callback=cb,
        )

    def on_bindok(self, _unused_frame: Queue.BindOk, channel: ConsumedChannel) -> None:
        """
        Invoked by pika when the Queue.Bind method has completed. At this
        point we will set the prefetch count for the channel.
        """
        logger.info("Queue bound: %s", channel.queue)
        self.set_qos(channel)

    def set_qos(self, channel: ConsumedChannel) -> None:
        """
        This method sets up the consumer prefetch to only be delivered
        one message at a time. The consumer must acknowledge this message
        before RabbitMQ will deliver another one. You should experiment
        with different prefetch values to achieve desired performance.
        """
        channel.pika_channel.basic_qos(
            prefetch_count=self._prefetch_count,
            callback=partial(self.on_basic_qos_ok, channel=channel),
        )

    def on_basic_qos_ok(
        self, _unused_frame: Basic.QosOk, channel: ConsumedChannel
    ) -> None:
        """
        Invoked by pika when the Basic.QoS method has completed. At this
        point we will start consuming messages by calling start_consuming
        which will invoke the needed RPC commands to start the process.
        :param pika.frame.Method _unused_frame: The Basic.QosOk response frame
        """
        logger.info("QOS set to: %d", self._prefetch_count)
        self.start_consuming(channel)

    def start_consuming(self, channel: ConsumedChannel) -> None:
        """
        This method sets up the consumer by first calling
        add_on_cancel_callback so that the object is notified if RabbitMQ
        cancels the consumer. It then issues the Basic.Consume RPC command
        which returns the consumer tag that is used to uniquely identify the
        consumer with RabbitMQ. We keep the value to use it when we want to
        cancel consuming. The on_message method is passed in as a callback pika
        will invoke when a message is fully received.
        """
        logger.info("Issuing consumer related RPC commands")
        self.add_on_cancel_callback(channel)

        consumer_tag = channel.pika_channel.basic_consume(
            queue=channel.queue.name,
            on_message_callback=partial(self.on_message, channel=channel),
            auto_ack=channel.listener.auto_ack,
        )
        channel.consumer_tag = consumer_tag

        self.was_consuming = True
        self._consuming = True

    def add_on_cancel_callback(self, channel: ConsumedChannel) -> None:
        """Add a callback that will be invoked if RabbitMQ cancels the consumer
        for some reason. If RabbitMQ does cancel the consumer,
        on_consumer_cancelled will be invoked by pika.
        """
        channel.pika_channel.add_on_cancel_callback(
            partial(self.on_consumer_cancelled, channel=channel)
        )

    def on_consumer_cancelled(
        self, method_frame: Basic.Cancel, channel: ConsumedChannel
    ) -> None:
        """
        Invoked by pika when RabbitMQ sends a Basic.Cancel for a consumer
        receiving messages.
        """
        logger.info("Consumer was cancelled remotely, shutting down: %r", method_frame)
        if channel.pika_channel:
            channel.pika_channel.close()
            self.forget_channel(channel)

    def on_message(
        self,
        unused_channel: Channel,
        basic_deliver: Basic.Deliver,
        properties: pika.BasicProperties,
        body: bytes,
        channel: ConsumedChannel,
    ) -> None:
        """
        Invoked by pika when a message is delivered from RabbitMQ. The
        channel is passed for your convenience. The basic_deliver object that
        is passed in carries the exchange, routing key, delivery tag and
        a redelivered flag for the message. The properties passed in is an
        instance of BasicProperties with the message properties and the body
        is the message that was sent.
        """
        logger.info(
            "Received message #%s from %s: %s (consumer %s, corr_id: %s)",
            basic_deliver.delivery_tag,
            properties.app_id,
            body,
            channel.consumer_tag,
            properties.correlation_id,
        )
        self._handle_message(unused_channel, basic_deliver, properties, body, channel)

    def _handle_message(
        self,
        unused_channel: Channel,
        basic_deliver: Basic.Deliver,
        properties: pika.BasicProperties,
        body: bytes,
        channel: ConsumedChannel,
    ) -> None:
        channel.listener.handle(
            PikaMessage(unused_channel, basic_deliver, properties, body),
        )

    def stop_consuming(self) -> None:
        """Tell RabbitMQ that you would like to stop consuming by sending the
        Basic.Cancel RPC command.
        """
        for channel in self._channels.values():
            logger.info("Sending a Basic.Cancel RPC command to RabbitMQ")
            cb = functools.partial(self.on_cancelok, channel=channel)
            channel.pika_channel.basic_cancel(channel.consumer_tag, cb)

    def on_cancelok(
        self, _unused_frame: Basic.CancelOk, channel: ConsumedChannel
    ) -> None:
        """
        This method is invoked by pika when RabbitMQ acknowledges the
        cancellation of a consumer. At this point we will close the channel.
        This will invoke the on_channel_closed method once the channel has been
        closed, which will in-turn close the connection.
        """
        if not self._channels:
            self._consuming = False

        logger.info(
            "RabbitMQ acknowledged the cancellation of the consumer: %s",
            channel.consumer_tag,
        )
        self.close_channel(channel)

    def close_channel(self, channel: ConsumedChannel) -> None:
        """
        Call to close the channel with RabbitMQ cleanly by issuing the
        Channel.Close RPC command.
        """
        logger.info("Closing the channel")
        channel.pika_channel.close()

    def run(self) -> None:
        """Run the example consumer by connecting to RabbitMQ and then
        starting the IOLoop to block and allow the AsyncioConnection to operate.
        """
        self._connection = self.connect()
        assert self._connection
        self._connection.ioloop.start()

    def stop(self) -> None:
        """Cleanly shutdown the connection to RabbitMQ by stopping the consumer
        with RabbitMQ. When RabbitMQ confirms the cancellation, on_cancelok
        will be invoked by pika, which will then closing the channel and
        connection. The IOLoop is started again because this method is invoked
        when CTRL-C is pressed raising a KeyboardInterrupt exception. This
        exception stops the IOLoop which needs to be running for pika to
        communicate with RabbitMQ. All of the commands issued prior to starting
        the IOLoop will be buffered but not processed.
        """
        assert self._connection
        if not self._closing:
            self._closing = True
            logger.info("Stopping")
            if self._consuming:
                self.stop_consuming()
                try:
                    self._connection.ioloop.start()
                except RuntimeError:
                    pass
            else:
                self._connection.ioloop.stop()
            logger.info("Stopped")


class ThreadedConsumer(Consumer):
    def __init__(  # type: ignore
        self, *args, executor: Optional[ThreadPoolExecutor] = None, **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self._executor = executor or ThreadPoolExecutor()

    def _handle_message(
        self,
        unused_channel: Channel,
        basic_deliver: Basic.Deliver,
        properties: pika.BasicProperties,
        body: bytes,
        channel: ConsumedChannel,
    ) -> None:
        def log_exceptions(fn: Callable, *args: Any, **kwargs: Any) -> None:
            try:
                fn(*args, **kwargs)
            except Exception:
                logger.exception(
                    "Exception happened while handling a message. Listener: %s, properties: %s",
                    channel.listener,
                    properties,
                )

        self._executor.submit(
            log_exceptions(
                channel.listener.handle,
                PikaMessage(unused_channel, basic_deliver, properties, body),
            )
        )

    def stop(self) -> None:
        super().stop()
        self._executor.shutdown()
