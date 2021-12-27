from __future__ import (
    annotations,
)

import warnings
from collections import (
    defaultdict,
)
from collections.abc import (
    Callable,
    Iterable,
)
from itertools import (
    chain,
)
from typing import (
    Any,
    Optional,
    Union,
)
from urllib.parse import (
    parse_qsl,
)
from uuid import (
    UUID,
)

from aiohttp.web import Request as AioHttpRequest
from cached_property import (
    cached_property,
)

from minos.common import (
    AvroDataDecoder,
    AvroSchemaDecoder,
    MinosAvroProtocol,
    import_module,
)

from ..exceptions import (
    NotHasParamsException,
)
from ..requests import (
    Request,
    Response,
    ResponseException,
)


class RestRequest(Request):
    """Rest Request class."""

    __slots__ = "raw"

    def __init__(self, raw: AioHttpRequest, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.raw = raw

    @property
    def raw_request(self) -> AioHttpRequest:
        """Get the raw request within the instance.

        :return: An ``aiohttp.web.Request`` instance.
        """
        warnings.warn(
            f"'{RestRequest.__name__}s.raw_request' is deprecated in favor of '{RestRequest.__name__}.raw'.",
            DeprecationWarning,
        )
        return self.raw

    def __eq__(self, other: RestRequest) -> bool:
        return type(self) == type(other) and self.raw == other.raw

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.raw!r})"

    @cached_property
    def user(self) -> Optional[UUID]:
        """
        Returns the UUID of the user making the Request.
        """
        if "User" not in self.headers:
            return None
        return UUID(self.headers["User"])

    @property
    def headers(self) -> dict[str, str]:
        """Get the headers of the request.

        :return: A dictionary in which keys are ``str`` instances and values are ``str`` instances.
        """
        # noinspection PyTypeChecker
        return self.raw.headers

    @property
    def has_content(self) -> bool:
        """Check if the request has content.

        :return: ``True`` if it has content or ``False`` otherwise.
        """
        return self.raw.body_exists

    async def _content(self, type_: Optional[Union[type, str]] = None, **kwargs) -> Any:
        if "model_type" in kwargs:
            warnings.warn("The 'model_type' argument is deprecated. Use 'type_' instead", DeprecationWarning)
            if type_ is None:
                type_ = kwargs["model_type"]

        data = await self._content_parser()
        return self._build(data, type_)

    @cached_property
    def _content_parser(self) -> Callable:
        mapper = {
            "application/json": self._raw_json,
            "application/x-www-form-encoded": self._raw_form,
            "avro/binary": self._raw_avro,
            "text/plain": self._raw_text,
            "application/octet-stream": self._raw_bytes,
        }

        if self.content_type not in mapper:
            raise ValueError(
                f"The given Content-Type ({self.content_type!r}) is not supported for automatic content parsing yet."
            )

        return mapper[self.content_type]

    async def _raw_json(self) -> Any:
        return await self.raw.json()

    async def _raw_form(self) -> dict[str, Any]:
        form = await self.raw.json(loads=parse_qsl)
        return self._parse_multi_dict(form)

    async def _raw_avro(self) -> Any:
        data = MinosAvroProtocol.decode(await self._raw_bytes())
        schema = MinosAvroProtocol.decode_schema(await self._raw_bytes())

        type_ = AvroSchemaDecoder(schema).build()
        return AvroDataDecoder(type_).build(data)

    async def _raw_text(self) -> str:
        return await self.raw.text()

    async def _raw_bytes(self) -> bytes:
        return await self.raw.read()

    @property
    def content_type(self) -> str:
        """Get the content type.

        :return: A ``str`` value.
        """
        return self.raw.content_type

    @property
    def has_params(self) -> bool:
        """Check if the request has params.

        :return: ``True`` if it has params or ``False`` otherwise.
        """
        sentinel = object()
        return next(chain(self._raw_url_params, self._raw_query_params), sentinel) is not sentinel

    async def _params(self, type_: Optional[Union[type, str]] = None, **kwargs) -> dict[str, Any]:
        data = self._parse_multi_dict(chain(self._raw_url_params, self._raw_query_params))
        return self._build(data, type_)

    async def url_params(self, type_: Optional[Union[type, str]] = None, **kwargs) -> Any:
        """Get the url params.

        :param type_: Optional ``type`` or ``str`` (classname) that defines the request content type.
        :param kwargs: Additional named arguments.
        :return: A dictionary instance.
        """
        if not self.has_url_params:
            raise NotHasParamsException(f"{self!r} has not url params.")

        data = self._parse_multi_dict(self._raw_url_params)
        return self._build(data, type_)

    @property
    def has_url_params(self) -> bool:
        """Check if the request has url params.

        :return: ``True`` if it has url params or ``False`` otherwise.
        """
        sentinel = object()
        return next(iter(self._raw_url_params), sentinel) is not sentinel

    @property
    def _raw_url_params(self):
        return self.raw.rel_url.query.items()  # pragma: no cover

    async def query_params(self, type_: Optional[Union[type, str]] = None, **kwargs) -> Any:
        """Get the query params.


        :param type_: Optional ``type`` or ``str`` (classname) that defines the request content type.
        :param kwargs: Additional named arguments.
        :return: A dictionary instance.
        """
        if not self.has_query_params:
            raise NotHasParamsException(f"{self!r} has not query params.")

        data = self._parse_multi_dict(self._raw_query_params)
        return self._build(data, type_)

    @property
    def has_query_params(self) -> bool:
        # noinspection GrazieInspection
        """Check if the request has query params.

                :return: ``True`` if it has query params or ``False`` otherwise.
                """
        sentinel = object()
        return next(iter(self._raw_query_params), sentinel) is not sentinel

    @property
    def _raw_query_params(self):
        return self.raw.match_info.items()  # pragma: no cover

    @staticmethod
    def _build(data: Any, type_: Union[type, str]) -> Any:
        if type_ is None:
            return data

        if isinstance(type_, str):
            type_ = import_module(type_)

        return AvroDataDecoder(type_).build(data)

    @staticmethod
    def _parse_multi_dict(raw: Iterable[str, Any]) -> dict[str, Any]:
        args = defaultdict(list)
        for k, v in raw:
            args[k].append(v)
        return {k: v if len(v) > 1 else v[0] for k, v in args.items()}


class RestResponse(Response):
    """Rest Response class."""


class RestResponseException(ResponseException):
    """Rest Response Exception class."""
