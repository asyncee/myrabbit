from typing import Callable, Optional

from myrabbit.core.consumer.pika_message import PikaMessage
from myrabbit.core.consumer.reply import Reply

MessageHandler = Callable[[PikaMessage], Optional[Reply]]
