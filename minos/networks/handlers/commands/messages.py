"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from typing import (
    Any,
)

from minos.common import (
    Command,
    Request,
    Response,
)


class CommandRequest(Request):
    """TODO"""

    __slots__ = "command"

    def __init__(self, command: Command):
        self.command = command

    async def content(self) -> list[Any]:
        """TODO"""
        return self.command.items


class CommandResponse(Response):
    """TODO"""

    __slots__ = "_items"

    def __init__(self, items: Any):
        if not isinstance(items, list):
            items = [items]
        self._items = items

    async def content(self) -> list[Any]:
        """TODO"""
        return self._items
