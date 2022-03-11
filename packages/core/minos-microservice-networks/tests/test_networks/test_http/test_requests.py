import unittest
from abc import (
    ABC,
)
from typing import (
    Any,
    Optional,
    Union,
)
from uuid import (
    uuid4,
)

from minos.networks import (
    HttpRequest,
    HttpResponse,
    Request,
    Response,
)


class _HttpRequest(HttpRequest):
    def __init__(self, headers):
        super().__init__()
        self._headers = headers

    @property
    def headers(self) -> dict[str, str]:
        """For testing purposes"""
        return self._headers

    # noinspection PyPropertyDefinition
    @property
    def content_type(self) -> str:
        """For testing purposes"""

    async def url_params(self, type_: Optional[Union[type, str]] = None, **kwargs) -> Any:
        """For testing purposes"""

    # noinspection PyPropertyDefinition
    @property
    def has_url_params(self) -> bool:
        """For testing purposes"""

    async def query_params(self, type_: Optional[Union[type, str]] = None, **kwargs) -> Any:
        """For testing purposes"""

    # noinspection PyPropertyDefinition
    @property
    def has_query_params(self) -> bool:
        """For testing purposes"""

    # noinspection PyPropertyDefinition
    @property
    def has_content(self) -> bool:
        """For testing purposes"""

    # noinspection PyPropertyDefinition
    @property
    def has_params(self) -> bool:
        """For testing purposes"""

    def __eq__(self, other: Request) -> bool:
        """For testing purposes"""

    def __repr__(self) -> str:
        """For testing purposes"""


class _HttpResponse(HttpResponse):
    """For testing purposes"""


class TestHttpRequest(unittest.IsolatedAsyncioTestCase):
    def test_is_subclass(self):
        self.assertTrue(issubclass(HttpRequest, (ABC, Request)))

    def test_abstract(self):
        expected = {"headers", "content_type", "url_params", "has_url_params", "query_params", "has_query_params"}

        for e in expected:
            # noinspection PyUnresolvedReferences
            self.assertIn(e, HttpRequest.__abstractmethods__)

    def test_user(self):
        user = uuid4()
        request = _HttpRequest({"user": str(user)})
        self.assertEqual(user, request.user)

    def test_user_not_present(self):
        request = _HttpRequest(dict())
        self.assertEqual(None, request.user)


class TestHttpResponse(unittest.IsolatedAsyncioTestCase):
    def test_is_subclass(self):
        self.assertTrue(issubclass(HttpResponse, (ABC, Response)))

    def test_constructor(self):
        response = _HttpResponse()
        self.assertEqual("application/json", response.content_type)

    def test_constructor_extended(self):
        response = _HttpResponse(content_type="text/plain")
        self.assertEqual("text/plain", response.content_type)

    async def test_from_response(self):
        response = Response({"foo": "bar"})
        observed = _HttpResponse.from_response(response)
        self.assertEqual("application/json", observed.content_type)
        self.assertEqual({"foo": "bar"}, await observed.content())

    def test_from_response_already(self):
        response = _HttpResponse()
        observed = _HttpResponse.from_response(response)
        self.assertEqual(observed, response)

    async def test_from_response_none(self):
        response = None
        observed = _HttpResponse.from_response(response)
        self.assertEqual("application/json", observed.content_type)
        self.assertEqual(None, await observed.content())


if __name__ == "__main__":
    unittest.main()
