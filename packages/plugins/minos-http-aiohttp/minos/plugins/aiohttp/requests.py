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
    Awaitable,
    Optional,
    Union,
)
from urllib.parse import (
    parse_qsl,
    urlencode,
)
from uuid import (
    UUID,
)

from aiohttp import (
    web,
)
from cached_property import (
    cached_property,
)
from orjson import (
    orjson,
)

from minos.common import (
    AvroDataDecoder,
    AvroDataEncoder,
    AvroSchemaDecoder,
    AvroSchemaEncoder,
    MinosAvroProtocol,
    TypeHintBuilder,
    import_module,
)
from minos.networks import (
    HttpRequest,
    HttpResponse,
    HttpResponseException,
    NotHasParamsException,
)


class AioHttpRequest(HttpRequest):
    """Aiohttp Request class."""

    __slots__ = "raw"

    def __init__(self, raw: web.Request, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.raw = raw

    @property
    def raw_request(self) -> web.Request:
        """Get the raw request within the instance.

        :return: An ``aiohttp.web.Request`` instance.
        """
        warnings.warn(
            f"'{type(self).__name__}s.raw_request' is deprecated in favor of '{type(self).__name__}.raw'.",
            DeprecationWarning,
        )
        return self.raw

    def __eq__(self, other: AioHttpRequest) -> bool:
        return type(self) == type(other) and self.raw == other.raw

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.raw!r})"

    @property
    def user(self) -> Optional[UUID]:
        """
        Returns the UUID of the user making the Request.
        """
        if "user" not in self.headers:
            return None
        return UUID(self.headers["user"])

    @cached_property
    def headers(self) -> dict[str, str]:
        """Get the headers of the request.

        :return: A dictionary in which keys are ``str`` instances and values are ``str`` instances.
        """
        # noinspection PyTypeChecker
        return self.raw.headers.copy()

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
            "application/x-www-form-urlencoded": self._raw_form,
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


class AioHttpResponse(HttpResponse):
    """Aiohttp Response class."""

    # noinspection PyUnusedLocal
    async def content(self, **kwargs) -> Optional[bytes]:
        """Raw response content.

        :param kwargs: Additional named arguments.
        :return: The raw content as a ``bytes`` instance.
        """
        if not self.has_content:
            return None
        return await self._content_parser()

    @cached_property
    def _content_parser(self) -> Callable[[], Awaitable[bytes]]:
        mapper = {
            "application/json": self._raw_json,
            "application/x-www-form-urlencoded": self._raw_form,
            "avro/binary": self._raw_avro,
            "text/plain": self._raw_text,
            "application/octet-stream": self._raw_bytes,
        }

        if self.content_type not in mapper:
            return self._raw_bytes

        return mapper[self.content_type]

    async def _raw_json(self) -> bytes:
        return orjson.dumps(self._raw_data)

    async def _raw_form(self) -> bytes:
        return urlencode(self._raw_data).encode()

    async def _raw_avro(self) -> bytes:
        type_ = TypeHintBuilder(self._data).build()
        schema = AvroSchemaEncoder(type_).build()
        data = AvroDataEncoder(self._data).build()

        return MinosAvroProtocol.encode(data, schema)

    async def _raw_text(self) -> bytes:
        if not isinstance(self._raw_data, str):
            raise ValueError(
                f"Given 'Content-Type' ({self.content_type!r}) is not supported for the given data: {self._raw_data!r}."
            )
        return self._raw_data.encode()

    async def _raw_bytes(self) -> bytes:
        if not isinstance(self._raw_data, bytes):
            raise ValueError(
                f"Given 'Content-Type' ({self.content_type!r}) is not supported for the given data: {self._raw_data!r}."
            )
        return self._raw_data

    # noinspection PyUnusedLocal
    @cached_property
    def _raw_data(self) -> Any:
        """Raw response content.

        :return: A list of raw items.
        """
        return AvroDataEncoder(self._data).build()


class AioHttpResponseException(HttpResponseException):
    """Aiohttp Response Exception class."""
