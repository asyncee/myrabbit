import threading
import typing
from ipaddress import IPv4Address
from typing import Optional
from urllib.parse import urlparse

import pika
from opentelemetry import context, propagators, trace
from opentelemetry.instrumentation.instrumentor import BaseInstrumentor
from opentelemetry.trace import SpanKind, Status
from opentelemetry.trace.status import StatusCanonicalCode
from pika.channel import Channel
from pika.spec import Basic, BasicProperties

from myrabbit.core import consumer, publisher
from myrabbit.core.consumer.channel import ConsumedChannel


def get_thread_id() -> str:
    return str(threading.current_thread().ident)


def get_thread_name() -> str:
    return threading.current_thread().name


def get_header_from_properties(
    properties: pika.BasicProperties, header_name: str
) -> typing.List[str]:
    headers = properties.headers or {}
    return [value for (key, value) in headers.items() if key == header_name]


class MyrabbitInstrumentor(BaseInstrumentor):
    service_name: str

    def instrument_myrabbit(self, service_name: str, **kwargs):
        self.service_name = service_name
        self.instrument(**kwargs)

    def _instrument(self, **kwargs):
        service_name = self.service_name

        self._original_publish = original_publish = publisher.Publisher.publish
        self._original_handle_message = consumer.Consumer._handle_message
        self._original_handle_message_threaded = (
            consumer.ThreadedConsumer._handle_message
        )

        def instrumented_publish(
            self,
            exchange: str,
            routing_key: str,
            message: bytes,
            properties: Optional[BasicProperties] = None,
        ) -> None:
            original_implementation = original_publish.__get__(self, self.__class__)

            if context.get_value("suppress_instrumentation"):
                original_implementation(exchange, routing_key, message, properties)
                return None

            # Following semantic conventions is used:
            # https://github.com/open-telemetry/opentelemetry-specification/blob/master/specification/trace/semantic_conventions/messaging.md

            exception = None
            span_name = f"{exchange}.{routing_key} send".lower()

            properties = properties or BasicProperties()

            with trace.get_tracer(__name__).start_as_current_span(
                span_name, kind=SpanKind.CLIENT
            ) as span:
                host = self._connection._impl.params.host
                port = self._connection._impl.params.port
                vhost = self._connection._impl.params.virtual_host

                span.set_attribute("messaging.system", "rabbitmq")
                span.set_attribute("messaging.destination", exchange)
                span.set_attribute("messaging.destination_kind", "queue")
                span.set_attribute("messaging.protocol", "amqp")
                span.set_attribute("messaging.protocol_version", "0.9.1")
                span.set_attribute("messaging.url", f"amqp://{host}:{port}/{vhost}")

                if routing_key:
                    span.set_attribute("messaging.rabbitmq.routing_key", routing_key)

                if properties.correlation_id:
                    span.set_attribute(
                        "messaging.conversation_id", properties.correlation_id
                    )

                if properties.message_id:
                    span.set_attribute("messaging.message_id", properties.message_id)

                try:
                    IPv4Address(host)
                except ValueError:
                    span.set_attribute("net.peer.name", host)
                else:
                    span.set_attribute("net.peer.ip", host)

                span.set_attribute("net.peer.port", port)
                span.set_attribute("net.transport", "IP.TCP")

                span.set_attribute("thread.id", get_thread_id())
                span.set_attribute("thread.name", get_thread_name())

                span.set_attribute("service.name", service_name)

                span.set_attribute("myrabbit.exchange", exchange)
                span.set_attribute("myrabbit.routing_key", routing_key)

                try:
                    span.set_attribute("myrabbit.message", message.decode("utf-8"))
                except UnicodeDecodeError:
                    pass

                headers = properties.headers or {}
                propagators.inject(type(headers).__setitem__, headers)
                properties.headers = headers

                try:
                    original_implementation(exchange, routing_key, message, properties)
                except Exception as exc:
                    exception = exc
                    span.set_status(Status(StatusCanonicalCode.UNKNOWN))
                else:
                    span.set_status(Status(StatusCanonicalCode.OK))

            if exception is not None:
                raise exception.with_traceback(exception.__traceback__)

        def instrumented_handle_message(
            self,
            unused_channel: Channel,
            basic_deliver: Basic.Deliver,
            properties: pika.BasicProperties,
            body: bytes,
            channel: ConsumedChannel,
            original_handle_message,
        ) -> None:
            original_implementation = original_handle_message.__get__(
                self, self.__class__
            )

            if context.get_value("suppress_instrumentation"):
                original_implementation(
                    unused_channel, basic_deliver, properties, body, channel
                )
                return None

            token = context.attach(
                propagators.extract(get_header_from_properties, properties)
            )
            exchange = basic_deliver.exchange
            routing_key = basic_deliver.routing_key
            queue = channel.queue.name
            span_name = f"{exchange}.{routing_key}.{queue} process".lower()

            exception = None

            try:
                with trace.get_tracer(__name__).start_as_current_span(
                    span_name, kind=trace.SpanKind.SERVER
                ) as span:
                    host = channel.pika_channel.connection.params.host
                    port = channel.pika_channel.connection.params.port
                    url = f"amqp://{host}:{port}"

                    span.set_attribute("messaging.system", "rabbitmq")
                    span.set_attribute("messaging.destination", exchange)
                    span.set_attribute("messaging.destination_kind", "queue")
                    span.set_attribute("messaging.operation", "process")
                    span.set_attribute("messaging.protocol", "amqp")
                    span.set_attribute("messaging.protocol_version", "0.9.1")
                    span.set_attribute("messaging.url", url)

                    if properties.message_id:
                        span.set_attribute(
                            "messaging.message_id", properties.message_id
                        )

                    if routing_key:
                        span.set_attribute(
                            "messaging.rabbitmq.routing_key", routing_key
                        )

                    if properties.correlation_id:
                        span.set_attribute(
                            "messaging.conversation_id", properties.correlation_id
                        )

                    try:
                        IPv4Address(host)
                    except ValueError:
                        span.set_attribute("net.peer.name", host)
                    else:
                        span.set_attribute("net.peer.ip", host)

                    span.set_attribute("net.peer.port", port)
                    span.set_attribute("net.transport", "IP.TCP")

                    span.set_attribute("thread.id", get_thread_id())
                    span.set_attribute("thread.name", get_thread_name())

                    span.set_attribute("service.name", service_name)

                    span.set_attribute("myrabbit.exchange", exchange)
                    span.set_attribute("myrabbit.routing_key", routing_key)

                    try:
                        span.set_attribute("myrabbit.message", body.decode("utf-8"))
                    except UnicodeDecodeError:
                        pass

                    try:
                        original_implementation(
                            unused_channel, basic_deliver, properties, body, channel
                        )
                    except Exception as exc:
                        exception = exc
                        span.set_status(Status(StatusCanonicalCode.UNKNOWN))
                    else:
                        span.set_status(Status(StatusCanonicalCode.OK))
            finally:
                context.detach(token)

            if exception is not None:
                raise exception.with_traceback(exception.__traceback__)

        def _wrap(impl):
            def inner(*args, **kwargs):
                return instrumented_handle_message(
                    *args, **kwargs, original_handle_message=impl
                )

            return inner

        publisher.Publisher.publish = instrumented_publish
        consumer.Consumer._handle_message = _wrap(consumer.Consumer._handle_message)
        consumer.ThreadedConsumer._handle_message = _wrap(
            consumer.ThreadedConsumer._handle_message
        )

    def _uninstrument(self, **kwargs):
        publisher.Publisher.publish = self._original_publish
        consumer.Consumer._handle_message = self._original_handle_message
        consumer.ThreadedConsumer._handle_message = (
            self._original_handle_message_threaded
        )
