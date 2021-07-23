"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
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
    def pref_fn_name(self) -> str:
        """TODO

        :return:TODO
        """
        mapping = {
            self.Command: "_pre_command_handle",
            self.Query: "_pre_query_handle",
            self.Event: "_pre_event_handle",
        }
        return mapping[self]
