from myrabbit.core.consumer.basic_consumer import BasicConsumer
from myrabbit.service import Service


def run_services(amqp_url: str, *services: Service):
    listeners = sum([s.listeners for s in services], [])
    consumer = BasicConsumer(amqp_url, listeners)
    consumer.run()
