from __future__ import (
    annotations,
)

from collections.abc import (
    Callable,
)
from typing import (
    Any,
    Awaitable,
    Optional,
)
from urllib.parse import (
    urlencode,
)

from cached_property import (
    cached_property,
)
from orjson import (
    orjson,
)

from minos.common import (
    AvroDataEncoder,
    AvroSchemaEncoder,
    MinosAvroProtocol,
    TypeHintBuilder,
)
from minos.networks import (
    Response,
)


class RestResponse(Response):
    """Rest Response class."""

    def __init__(self, *args, content_type: str = "application/json", **kwargs):
        super().__init__(*args, **kwargs)
        self.content_type = content_type

    @classmethod
    def from_response(cls, response: Optional[Response]) -> RestResponse:
        """Build a new ``RestRequest`` from another response.

        :param response: The base response.
        :return: A ``RestResponse`` instance.
        """
        if isinstance(response, RestResponse):
            return response

        if response is None:
            return RestResponse()

        return RestResponse(response._data)

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
