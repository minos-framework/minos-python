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
    """Command Request class."""

    __slots__ = "command"

    def __init__(self, command: Command):
        self.command = command

    async def content(self) -> list[Any]:
        """Request content.

        :return: A list of items.
        """
        return self.command.items


class CommandResponse(Response):
    """Command Response class."""
