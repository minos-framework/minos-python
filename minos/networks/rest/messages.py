"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
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
    MinosModel,
    Request,
    Response,
)


class HttpRequest(Request):
    """TODO"""

    def __init__(self, request: web.Request):
        self.raw_request = request

    async def content(self) -> list[Any]:
        """TODO"""
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
    def url_args(self) -> dict[str, Any]:
        """TODO

        :return: TODO
        """
        args = defaultdict(list)
        for k, v in self._raw_url_args.items():
            args[k].append(v)
        return args

    @property
    def _raw_url_args(self):
        return self.raw_request.rel_url.query  # pragma: no cover


class HttpResponse(Response):
    """TODO"""

    def __init__(self, items: Any):
        if not isinstance(items, list):
            items = [items]
        self._items = items

    async def content(self) -> list[Any]:
        """TODO"""
        return [item if not isinstance(item, MinosModel) else item.avro_data for item in self._items]
