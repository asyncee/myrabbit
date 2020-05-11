import queue
from dataclasses import dataclass

from myrabbit import EventBus
from myrabbit import EventWithMessage
from myrabbit.core.consumer.basic_consumer import BasicConsumer
from myrabbit.service import Service


@dataclass
class YEvent:
    name: str


def test_service(event_bus: EventBus, run_consumer, rmq_url):
    q = queue.Queue()

    x = Service("X", event_bus)
    y = Service("Y", event_bus)

    @x.on("Y", YEvent)
    def handle_y_event(event: EventWithMessage[YEvent]):
        q.put(event)

    consumer = BasicConsumer(rmq_url, x.listeners)
    with run_consumer(consumer):
        y.publish(YEvent(name="y-service-event"))

    message: EventWithMessage = q.get(block=True, timeout=1)
    assert message.event == YEvent(name="y-service-event")
