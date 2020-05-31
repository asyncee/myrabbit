import logging
from dataclasses import dataclass
from typing import Optional

from myrabbit.core.consumer.message_handler import MessageHandler

from . import handle_message_strategy as strategy
from .callbacks import Callbacks
from .pika_message import PikaMessage

logger = logging.getLogger(__name__)


@dataclass
class Exchange:
    name: str
    type: str
    durable: bool = True
    auto_delete: bool = True


@dataclass
class Queue:
    name: str
    durable: bool = True
    auto_delete: bool = False
    exclusive: bool = False


@dataclass
class Listener:
    exchange: Exchange
    queue: Queue
    routing_key: str
    handle_message: MessageHandler
    auto_ack: bool = False
    handle_message_strategy: Optional[strategy.HandleMessageStrategy] = None
    callbacks: Optional[Callbacks] = None

    def handle(self, message: PikaMessage) -> None:
        self._callbacks().before_request(message)
        execute_strategy = self._get_strategy()
        logger.info("Handling message with %s strategy", execute_strategy)
        execute_strategy(self.handle_message, message)  # type: ignore
        self._callbacks().after_request(message)

    def _callbacks(self) -> Callbacks:
        return self.callbacks or Callbacks()

    def _get_strategy(self) -> strategy.HandleMessageStrategy:
        if self.handle_message_strategy:
            return self.handle_message_strategy

        return strategy.BaseStrategy(auto_ack=self.auto_ack)
