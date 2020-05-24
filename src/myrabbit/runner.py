from typing import List

from myrabbit.core.consumer.consumer import Consumer
from myrabbit.core.consumer.consumer import ThreadedConsumer
from myrabbit.core.consumer.listener import Listener
from myrabbit.core.consumer.reconnecting_consumer import ReconnectingConsumer
from myrabbit.service import Service


def run_services(amqp_url: str, *services: Service) -> None:
    listeners: List[Listener] = sum([s.listeners for s in services], [])
    consumer = ReconnectingConsumer(
        Consumer, consumer_kwargs=dict(amqp_url=amqp_url, listeners=listeners)
    )
    consumer.run()


def run_services_threaded(amqp_url: str, *services: Service) -> None:
    listeners: List[Listener] = sum([s.listeners for s in services], [])
    consumer = ReconnectingConsumer(
        ThreadedConsumer, consumer_kwargs=dict(amqp_url=amqp_url, listeners=listeners)
    )
    consumer.run()
