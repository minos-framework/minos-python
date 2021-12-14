import json
from typing import (
    Any,
    Optional,
    Union,
)
from unittest.mock import (
    AsyncMock,
)
from urllib.parse import (
    urlencode,
)
from uuid import (
    UUID,
)

from aiohttp import (
    test_utils,
    web,
)
from multidict import (
    MultiDict,
)

from minos.common import (
    MinosAvroProtocol,
)


def json_mocked_request(data: Any, **kwargs) -> web.Request:
    """For testng purposes. """
    return mocked_request(json.dumps(data).encode(), content_type="application/json", **kwargs)


def form_mocked_request(data: dict[str, Any], **kwargs) -> web.Request:
    """For testng purposes. """
    return mocked_request(urlencode(data, doseq=True).encode(), content_type="application/x-www-form-encoded", **kwargs)


def avro_mocked_request(data: Any, schema: Any, **kwargs) -> web.Request:
    """For testng purposes. """
    return mocked_request(MinosAvroProtocol.encode(data, schema), content_type="avro/binary", **kwargs)


def text_mocked_request(data: str, **kwargs) -> web.Request:
    """For testng purposes. """
    return mocked_request(data.encode(), content_type="text/plain", **kwargs)


def bytes_mocked_request(data: bytes, **kwargs) -> web.Request:
    """For testng purposes. """
    return mocked_request(data, content_type="application/octet-stream", **kwargs)


def mocked_request(
    data: Optional[bytes] = None,
    headers: Optional[dict[str, str]] = None,
    user: Optional[Union[str, UUID]] = None,
    content_type: Optional[str] = None,
    method: str = "POST",
    path: str = "localhost",
    url_params: Optional[list[tuple[str, str]]] = None,
    query_params: Optional[list[tuple[str, str]]] = None,
) -> web.Request:
    """For testing purposes"""
    if headers is None:
        headers = dict()
    else:
        headers = headers.copy()

    if user is not None:
        headers["User"] = str(user)

    if content_type is not None:
        headers["Content-Type"] = content_type

    kwargs = {
        "method": method,
        "path": path,
        "headers": headers,
    }

    request = test_utils.make_mocked_request(**kwargs)
    request.read = AsyncMock(return_value=data)

    if url_params is not None:
        # noinspection PyProtectedMember
        request._rel_url = request._rel_url.with_query(url_params)

    if query_params is not None:
        request._match_info = MultiDict(map(lambda kv: (str(kv[0]), str(kv[1])), query_params))

    return request
