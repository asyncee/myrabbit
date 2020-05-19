import abc
import logging
from typing import Optional

from myrabbit.core.consumer.message_handler import MessageHandler
from myrabbit.core.consumer.pika_message import PikaMessage
from myrabbit.core.consumer.reply import Reply

logger = logging.getLogger(__name__)


class HandleMessageStrategy(abc.ABC):
    @abc.abstractmethod
    def __call__(self, handle_message: MessageHandler, message: PikaMessage) -> None:
        pass


class BaseStrategy(HandleMessageStrategy):
    def __init__(self, auto_ack: bool):
        self._auto_ack = auto_ack

    def __call__(self, handle_message: MessageHandler, message: PikaMessage) -> None:
        # todo: add retry support on expected exceptions
        try:
            result: Optional[Reply] = handle_message(message)
        except Exception:
            logger.exception("Exception happened during handling message %s", message)
            message.requeue()
            return

        reply_to = message.properties.reply_to

        if not self._auto_ack:
            message.acknowledge()

        if result is not None and reply_to:
            message.reply(result)


class ManualHandle(HandleMessageStrategy):
    def __call__(self, handle_message: MessageHandler, message: PikaMessage) -> None:
        handle_message(message)
