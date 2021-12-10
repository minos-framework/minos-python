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
    uuid4,
)

from aiohttp import (
    test_utils,
    web,
)


def mocked_request(data: Any = None, user: Optional[Union[str, UUID]] = None) -> web.Request:
    """For testing purposes"""
    if user is None:
        user = uuid4()

    response = test_utils.make_mocked_request("POST", "localhost", headers={"User": str(user), "something": "123"})

    if data is not None:
        mock = AsyncMock(return_value=data)
        response.json = mock

    return response
