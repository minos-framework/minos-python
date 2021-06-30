"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from __future__ import (
    annotations,
)

from collections import (
    defaultdict,
)
from json import (
    JSONDecodeError,
)
from typing import (
    Any,
)

from aiohttp import (
    web,
)

from minos.common import (
    Request,
    Response,
)


class HttpRequest(Request):
    """Http Request class."""

    def __init__(self, request: web.Request):
        self.raw_request = request

    def __eq__(self, other: HttpRequest) -> bool:
        return type(self) == type(other) and self.raw_request == other.raw_request

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.raw_request!r})"

    async def content(self) -> list[Any]:
        """Get the request content.

        :return: A list of items.
        """
        try:
            data = await self.raw_request.json()
        except JSONDecodeError:
            data = self.url_args
            if len(data) == 1:
                _, data = data.popitem()
        if not isinstance(data, list):
            data = [data]
        return data

    @property
    def url_args(self) -> dict[str, list[Any]]:
        """Get the url arguments as a dictionary in which the keys are the names and teh values are a list the lists of
            values.

        :return: A dictionary instance.
        """
        args = defaultdict(list)
        for k, v in self._raw_url_args.items():
            args[k].append(v)
        return args

    @property
    def _raw_url_args(self):
        return self.raw_request.rel_url.query  # pragma: no cover


class HttpResponse(Response):
    """Http Response class."""
