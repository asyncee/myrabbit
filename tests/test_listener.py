from collections import defaultdict
from unittest.mock import Mock

from myrabbit.core.consumer.callbacks import Callbacks
from myrabbit.core.consumer.handle_message_strategy import HandleMessageStrategy
from myrabbit.core.consumer.listener import Exchange
from myrabbit.core.consumer.listener import Listener
from myrabbit.core.consumer.listener import Queue
from myrabbit.core.consumer.message_handler import MessageHandler
from myrabbit.core.consumer.pika_message import PikaMessage


class FakeStrategy(HandleMessageStrategy):
    def __call__(self, handle_message: MessageHandler, message: PikaMessage) -> None:
        pass


def test_callbacks() -> None:
    before_request: Mock = Mock()
    after_request: Mock = Mock()
    callbacks = Callbacks(
        defaultdict(
            list,
            {"before_request": [before_request], "after_request": [after_request],},
        )
    )
    listener = Listener(
        Exchange("exchange", "fanout"),
        Queue("queue"),
        "routing-key",
        lambda message: None,
        callbacks=callbacks,
        handle_message_strategy=FakeStrategy(),
    )

    msg = Mock()
    listener.handle(msg)

    assert before_request.called_once_with(listener, msg)
    assert after_request.called_once_with(listener, msg)
