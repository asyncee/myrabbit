class CommandReplyHeaders:
    COMMAND_REPLY_PREFIX = "X-Command-Reply"
    REPLY_OUTCOME: str = f"{COMMAND_REPLY_PREFIX}-Outcome"
    REPLY_HEADERS_KEY: str = f"{COMMAND_REPLY_PREFIX}-Headers"
    REPLY_TYPE: str = f"{COMMAND_REPLY_PREFIX}-Type"
