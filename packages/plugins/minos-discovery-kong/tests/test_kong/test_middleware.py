import unittest
from collections import (
    namedtuple,
)
from uuid import (
    uuid4,
)

from minos.networks import (
    Request,
    Response,
)
from minos.plugins.kong.middleware import (
    middleware,
)
from tests.utils import (
    InMemoryHttpRequest,
)

PROTOCOL = "http"


async def _fn(request: Request) -> Response:
    return Response(f"{await request.content()}bar")


Raw = namedtuple("Raw", ["headers", "content"])


class TestKongDiscoveryClient(unittest.IsolatedAsyncioTestCase):
    async def test_middleware_no_user_headers(self):
        user_uuid = uuid4()
        request_headers = {"X-Consumer-Custom-ID": str(user_uuid)}
        request = InMemoryHttpRequest(content="foo", headers=request_headers)
        response = await middleware(request, _fn)

        self.assertEqual("foobar", await response.content())

        self.assertEqual({"X-Consumer-Custom-ID": str(user_uuid), "user": str(user_uuid)}, request_headers)


if __name__ == "__main__":
    unittest.main()
