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

from cached_property import (
    cached_property,
)

from minos.common import (
    Request,
    Response,
)


class HttpRequest(Request):
    """Http Request class."""

    def __init__(self, request):
        self.raw_request = request

    def __eq__(self, other: HttpRequest) -> bool:
        return type(self) == type(other) and self.raw_request == other.raw_request

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.raw_request!r})"

    async def content(self) -> list[Any]:
        """Get the request content.

        :return: A list of items.
        """
        data = await self._raw_json()
        data = [(entry | self.url_args | self.path_args) for entry in data]
        return data

    async def _raw_json(self) -> list[dict[str, Any]]:
        try:
            data = await self.raw_request.json()
        except JSONDecodeError:
            data = dict()

        if not isinstance(data, list):
            data = [data]
        return data

    @cached_property
    def url_args(self) -> dict[str, Any]:
        """Get the url arguments as a dictionary in which the keys are the names and teh values are a list the lists of
            values.

        :return: A dictionary instance.
        """
        args = defaultdict(list)
        for k, v in self._raw_url_args:
            args[k].append(v)
        args = {k: v if len(v) > 1 else v[0] for k, v in args.items()}
        return args

    @property
    def _raw_url_args(self):
        return self.raw_request.rel_url.query.items()  # pragma: no cover

    @cached_property
    def path_args(self) -> dict[str, Any]:
        """TODO

        :return: TODO
        """
        args = defaultdict(list)
        for k, v in self._raw_path_args:
            args[k].append(v)
        args = {k: v if len(v) > 1 else v[0] for k, v in args.items()}
        return args

    @property
    def _raw_path_args(self):
        return self.raw_request.match_info.items()  # pragma: no cover


class HttpResponse(Response):
    """Http Response class."""
