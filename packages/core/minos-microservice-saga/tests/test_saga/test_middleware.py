import unittest
from collections import (
    namedtuple,
)
from uuid import (
    uuid4,
)

from minos.aggregate import (
    TransactionStatus,
)
from minos.networks import (
    REQUEST_HEADERS_CONTEXT_VAR,
    BrokerRequest,
    Request,
    Response,
)
from minos.saga import (
    transactional_command,
)
from tests.utils import (
    MinosTestCase,
)


async def _fn(request: Request) -> Response:
    return Response(f"{await request.content()}bar")


Raw = namedtuple("Raw", ["headers", "content"])


class TestMiddleware(MinosTestCase):
    async def test_transactional_command_unused(self):
        # noinspection PyTypeChecker
        request = BrokerRequest(Raw({"transactions": None}, "foo"))
        response = await transactional_command(request, _fn)

        self.assertEqual("foobar", await response.content())

    async def test_transactional_command_unused_with_headers(self):
        request_headers = {"foo": "bar"}
        REQUEST_HEADERS_CONTEXT_VAR.set(request_headers)
        # noinspection PyTypeChecker
        request = BrokerRequest(Raw(request_headers, "foo"))
        response = await transactional_command(request, _fn)

        self.assertEqual("foobar", await response.content())

        self.assertEqual({"foo": "bar", "related_services": "order"}, request_headers)

    async def test_transactional_command_unused_with_headers_related_services(self):
        request_headers = {"foo": "bar", "related_services": "ticket"}
        REQUEST_HEADERS_CONTEXT_VAR.set(request_headers)
        # noinspection PyTypeChecker
        request = BrokerRequest(Raw(request_headers, "foo"))
        response = await transactional_command(request, _fn)

        self.assertEqual("foobar", await response.content())

        self.assertEqual({"foo", "related_services"}, request_headers.keys())
        self.assertEqual("bar", request_headers["foo"])
        self.assertEqual({"order", "ticket"}, set(request_headers["related_services"].split(",")))

    async def test_transactional_command_used(self):
        uuid = uuid4()
        # noinspection PyTypeChecker
        request = BrokerRequest(Raw({"transactions": str(uuid)}, "foo"))
        response = await transactional_command(request, _fn)

        self.assertEqual("foobar", await response.content())

        transaction = await self.transaction_repository.get(uuid)
        self.assertEqual(TransactionStatus.PENDING, transaction.status)

    async def test_transactional_command_used_with_headers(self):
        uuid = uuid4()
        request_headers = {"foo": "bar", "transactions": str(uuid)}
        REQUEST_HEADERS_CONTEXT_VAR.set(request_headers)
        # noinspection PyTypeChecker
        request = BrokerRequest(Raw(request_headers, "foo"))
        response = await transactional_command(request, _fn)

        self.assertEqual("foobar", await response.content())
        self.assertEqual({"foo": "bar", "related_services": "order"}, request_headers)

    async def test_transactional_command_used_nested(self):
        uuid1 = uuid4()
        uuid2 = uuid4()
        request_headers = {"transactions": f"{uuid1!s},{uuid2!s}"}
        # noinspection PyTypeChecker
        request = BrokerRequest(Raw(request_headers, "foo"))
        response = await transactional_command(request, _fn)

        self.assertEqual("foobar", await response.content())

        self.assertEqual(TransactionStatus.PENDING, (await self.transaction_repository.get(uuid1)).status)
        self.assertEqual(TransactionStatus.PENDING, (await self.transaction_repository.get(uuid2)).status)

    async def test_transactional_command_used_nested_with_headers(self):
        uuid1 = uuid4()
        uuid2 = uuid4()
        request_headers = {"foo": "bar", "transactions": f"{uuid1!s},{uuid2!s}"}
        REQUEST_HEADERS_CONTEXT_VAR.set(request_headers)
        # noinspection PyTypeChecker
        request = BrokerRequest(Raw(request_headers, "foo"))
        response = await transactional_command(request, _fn)

        self.assertEqual("foobar", await response.content())

        self.assertEqual({"foo": "bar", "related_services": "order", "transactions": str(uuid1)}, request_headers)


if __name__ == "__main__":
    unittest.main()
