"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from __future__ import (
    annotations,
)

from typing import (
    Any,
)

from minos.common import (
    Command,
    Request,
    Response,
    ResponseException,
)


class CommandRequest(Request):
    """Command Request class."""

    __slots__ = "command"

    def __init__(self, command: Command):
        self.command = command

    def __eq__(self, other: CommandRequest) -> bool:
        return type(self) == type(other) and self.command == other.command

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.command!r})"

    async def content(self, **kwargs) -> Any:
        """Request content.

        :param kwargs: Additional named arguments.
        :return: The command content.
        """
        data = self.command.data
        return data


class CommandResponse(Response):
    """Command Response class."""


class CommandResponseException(ResponseException):
    """Command Response Exception class."""
