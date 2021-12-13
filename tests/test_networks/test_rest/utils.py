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
# noinspection PyProtectedMember
from aiohttp.streams import (
    EmptyStreamReader,
)


def json_mocked_request(data: Any, **kwargs) -> web.Request:
    """For testng purposes. """
    return mocked_request(json.dumps(data).encode(), content_type="application/json", **kwargs)


def form_mocked_request(data: dict[str, Any], **kwargs) -> web.Request:
    """For testng purposes. """
    return mocked_request(urlencode(data, doseq=True).encode(), content_type="application/x-www-form-encoded", **kwargs)


def mocked_request(
    data: Optional[bytes] = None,
    headers: Optional[dict[str, str]] = None,
    user: Optional[Union[str, UUID]] = None,
    content_type: Optional[str] = None,
    method: str = "POST",
    path: str = "localhost",
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

    if data is None:
        kwargs["payload"] = EmptyStreamReader()

    response = test_utils.make_mocked_request(**kwargs)
    response.read = AsyncMock(return_value=data)

    return response
