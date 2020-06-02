from typing import List, Type


class Doc:
    def __init__(self) -> None:
        self._commands: List[str] = []
        self._command_replies: List[str] = []
        self._events: List[str] = []

    def add_event(self, event_source: str, event_type: Type) -> None:
        self._events.append(f"{event_source}: {event_type.__name__}")

    def add_command(self, command_type: Type) -> None:
        self._commands.append(command_type.__name__)

    def add_command_reply(self, command_destination: str, command_type: Type) -> None:
        self._command_replies.append(
            f"{command_type.__name__} from {command_destination}"
        )

    def get_commands(self) -> List[str]:
        return self._commands

    def get_events(self) -> List[str]:
        return self._events

    def get_command_replies(self) -> List[str]:
        return self._command_replies
