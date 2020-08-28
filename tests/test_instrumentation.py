import random
from queue import Queue
from time import sleep
from typing import Callable

import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchExportSpanProcessor, ConsoleSpanExporter

from myrabbit import Listener, PikaMessage
from myrabbit.contrib.opentelemetry.instrumentor import MyrabbitInstrumentor
from myrabbit.core.consumer.listener import Exchange
from myrabbit.core.consumer.listener import Queue as Q
from myrabbit.core.publisher import make_publisher


@pytest.mark.instrumentation
def test_instrumentor():
    from myrabbit.core import consumer, publisher
    from myrabbit.core.consumer import consumer as consumer_mod
    from myrabbit.core.publisher import publisher as publisher_mod

    assert consumer.Consumer == consumer_mod.Consumer
    assert publisher.Publisher == publisher_mod.Publisher

    original_consumer = consumer.Consumer
    original_publisher = publisher.Publisher

    MyrabbitInstrumentor().instrument_myrabbit(service_name="instrumented_service")

    assert consumer.Consumer == consumer_mod.Consumer
    assert consumer.Consumer != original_consumer

    assert publisher.Publisher == publisher_mod.Publisher
    assert publisher.Publisher != original_publisher


@pytest.mark.instrumentation
def test_instrumentation(rmq_url: str, run_consumer: Callable):
    trace.set_tracer_provider(TracerProvider())
    MyrabbitInstrumentor().instrument_myrabbit(service_name="instrumented_service")
    span_processor = BatchExportSpanProcessor(ConsoleSpanExporter())
    trace.get_tracer_provider().add_span_processor(span_processor)

    queue: Queue = Queue()

    def some_callback(msg: PikaMessage) -> None:
        queue.put(msg)

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

    # It is important to import Consumer right there
    # because that way we import its instrumented version.
    from myrabbit.core.consumer import Consumer

    consumer = Consumer(rmq_url, listeners)

    with run_consumer(consumer), make_publisher(rmq_url) as publisher:
        publisher.publish(exchange, "test", b"test message")
        sleep(1)
