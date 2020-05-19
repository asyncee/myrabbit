from typing import Callable
from typing import Optional

from myrabbit.core.consumer.reply import Reply
from myrabbit.core.consumer.pika_message import PikaMessage

MessageHandler = Callable[[PikaMessage], Optional[Reply]]
