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
    Optional,
)
from uuid import (
    UUID,
)

from ..messages import (
    Request,
    Response,
    ResponseException,
)


class HandlerRequest(Request):
    """Handler Request class."""

    __slots__ = "raw"

    def __init__(self, raw: Any):
        self.raw = raw

    def __eq__(self, other: HandlerRequest) -> bool:
        return type(self) == type(other) and self.raw == other.raw

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.raw!r})"

    @property
    def user(self) -> Optional[UUID]:
        """
        Returns the UUID of the user making the Request.
        """
        return getattr(self.raw, "user", None)

    async def content(self, **kwargs) -> Any:
        """Request content.

        :param kwargs: Additional named arguments.
        :return: The content.
        """
        data = self.raw.data
        return data


class HandlerResponse(Response):
    """Handler Response class."""


class HandlerResponseException(ResponseException):
    """Handler Response Exception class."""
