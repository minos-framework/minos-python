from enum import (
    Enum,
    auto,
)


class EnrouteDecoratorKind(Enum):
    """Enroute Kind enumerate."""

    Command = auto()
    Query = auto()
    Event = auto()

    @property
    def pre_fn_name(self) -> str:
        """Get the pre execution function name.

        :return: A string value containing the function name.
        """
        mapping = {
            self.Command: "_pre_command_handle",
            self.Query: "_pre_query_handle",
            self.Event: "_pre_event_handle",
        }
        return mapping[self]

    @property
    def post_fn_name(self) -> str:
        """Get the post execution function name.

        :return: A string value containing the function name.
        """
        mapping = {
            self.Command: "_post_command_handle",
            self.Query: "_post_query_handle",
            self.Event: "_post_event_handle",
        }
        return mapping[self]
