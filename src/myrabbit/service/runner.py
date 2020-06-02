from typing import List, Type, Union

from myrabbit import CommandBus, EventBus
from myrabbit.core.consumer.consumer import Consumer, ThreadedConsumer
from myrabbit.core.consumer.listener import Listener
from myrabbit.core.consumer.reconnecting_consumer import ReconnectingConsumer
from myrabbit.core.publisher.reconnecting_publisher import ReconnectingPublisherFactory

from .service import Service
from .service_builder import ServiceBuilder


def _print_motd(services: List[Service]):
    from colorama import init
    from colorama import Fore, Style

    init()

    print(Fore.GREEN + Style.BRIGHT + "Services:" + Style.NORMAL)
    for service in services:
        print("> " + service.service_name)
    print()

    for service in services:
        print(Style.BRIGHT + f"[{service.service_name}]" + Style.NORMAL)

        if (commands := service.doc.get_commands()) :
            print("> Commands:")
            for command in commands:
                print("  - " + command)

        if (events := service.doc.get_events()) :
            print("> Listens to events:")
            for event in events:
                print("  - " + event)

        if (replies := service.doc.get_command_replies()) :
            print("> Handles replies to commands:")
            for reply in replies:
                print("  - " + reply)
        print()

    print(Style.RESET_ALL)


def run_services(
    amqp_url: str,
    *services: Union[Service, ServiceBuilder],
    consumer_cls: Type[Consumer] = ThreadedConsumer,
) -> None:
    factory = ReconnectingPublisherFactory(amqp_url)
    event_bus = EventBus(factory)
    command_bus = CommandBus(factory)

    to_run = []
    for inst in services:
        if isinstance(inst, Service):
            to_run.append(inst)
        elif isinstance(inst, ServiceBuilder):
            to_run.append(inst.build(event_bus, command_bus))
        else:
            raise ValueError(f"Invalid service or builder: {inst!r}")

    _print_motd(to_run)
    listeners: List[Listener] = sum([s.listeners for s in to_run], [])
    consumer = ReconnectingConsumer(
        consumer_cls, consumer_kwargs=dict(amqp_url=amqp_url, listeners=listeners)
    )
    consumer.run()


run_services_threaded = run_services
