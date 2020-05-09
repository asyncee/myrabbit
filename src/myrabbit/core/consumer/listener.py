import logging
from dataclasses import dataclass
from typing import Optional

from myrabbit.core.consumer.message_handler import MessageHandler
from . import handle_message_strategy as strategy
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

    def handle(self, message: PikaMessage):
        strategy = self._get_strategy()
        logger.info("Handling message with %s strategy", strategy)
        strategy(self.handle_message, message)

    def _get_strategy(self) -> strategy.HandleMessageStrategy:
        if self.handle_message_strategy:
            return self.handle_message_strategy

        return strategy.BaseStrategy(auto_ack=self.auto_ack)
