import queue
import random
from dataclasses import dataclass
from time import sleep
from typing import Callable

import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchExportSpanProcessor, ConsoleSpanExporter

from myrabbit import CommandWithMessage, Listener, PikaMessage, Service
from myrabbit.contrib.opentelemetry.instrumentor import MyrabbitInstrumentor
from myrabbit.core.consumer import Consumer
from myrabbit.core.consumer.listener import Exchange
from myrabbit.core.consumer.listener import Queue as Q
from myrabbit.core.publisher import make_publisher


@dataclass
class Command:
    name: str


@pytest.mark.instrumentation
def test_instrumentor():
    from myrabbit.core import consumer, publisher

    original_consume = consumer.Consumer._handle_message
    original_publish = publisher.Publisher.publish

    MyrabbitInstrumentor().instrument_myrabbit(service_name="instrumented_service")

    assert consumer.Consumer._handle_message is not original_consume
    assert publisher.Publisher.publish is not original_publish


@pytest.mark.instrumentation
def test_instrumentation(rmq_url: str, run_consumer: Callable):
    trace.set_tracer_provider(TracerProvider())
    MyrabbitInstrumentor().instrument_myrabbit(service_name="instrumented_service")
    span_processor = BatchExportSpanProcessor(ConsoleSpanExporter())
    trace.get_tracer_provider().add_span_processor(span_processor)

    def some_callback(msg: PikaMessage) -> None:
        pass

    exchange = "exchange_" + str(random.randint(100000, 999999))
    queue_name = "queue_" + str(random.randint(100000, 999999))

    listeners = [
        Listener(
            exchange=Exchange(type="topic", name=exchange, auto_delete=True),
            queue=Q(queue_name, auto_delete=True),
            routing_key="test",
            handle_message=some_callback,
        )
    ]

    consumer = Consumer(rmq_url, listeners)

    with run_consumer(consumer), make_publisher(rmq_url) as publisher:
        publisher.publish(exchange, "test", b"test message")
        sleep(1)


@pytest.mark.instrumentation
def test_service_instrumentation(
    make_service: Callable, run_consumer: Callable, rmq_url: str
) -> None:
    trace.set_tracer_provider(TracerProvider())
    MyrabbitInstrumentor().instrument_myrabbit(service_name="instrumented_service")
    span_processor = BatchExportSpanProcessor(ConsoleSpanExporter())
    trace.get_tracer_provider().add_span_processor(span_processor)

    x: Service = make_service("X")
    y: Service = make_service("Y")

    @x.on_command(Command)
    def x_handle_command(command: CommandWithMessage[Command]) -> None:
        pass

    consumer = Consumer(rmq_url, x.listeners)

    with run_consumer(consumer):
        y.send("X", Command(name="command-for-x"))

    sleep(1)
