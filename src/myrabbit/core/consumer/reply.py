from dataclasses import dataclass
from typing import Optional

import pika


@dataclass
class Reply:
    body: bytes
    properties: Optional[pika.BasicProperties] = None
