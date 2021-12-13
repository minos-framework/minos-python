import json
from typing import (
    Any,
    Optional,
    Union,
)
from unittest.mock import (
    AsyncMock,
)
from uuid import (
    UUID,
)

from aiohttp import (
    test_utils,
    web,
)


def json_mocked_request(data: Any, **kwargs) -> web.Request:
    """For testng purposes. """
    data = json.dumps(data).encode()
    return mocked_request(data, content_type="application/json", **kwargs)


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

    response = test_utils.make_mocked_request(method, path, headers=headers)

    if data is not None:
        response.read = AsyncMock(return_value=data)

    return response
