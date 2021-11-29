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


Raw = namedtuple("Raw", ["headers", "data"])


class TestMiddleware(MinosTestCase):
    async def test_transactional_command_unused(self):
        # noinspection PyTypeChecker
        request = BrokerRequest(Raw({"transaction": None}, "foo"))
        response = await transactional_command(request, _fn)

        self.assertEqual("foobar", await response.content())

    async def test_transactional_command_used(self):
        uuid = uuid4()
        # noinspection PyTypeChecker
        request = BrokerRequest(Raw({"transaction": str(uuid)}, "foo"))
        response = await transactional_command(request, _fn)

        self.assertEqual("foobar", await response.content())

        transaction = await self.transaction_repository.get(uuid)
        self.assertEqual(TransactionStatus.PENDING, transaction.status)


if __name__ == "__main__":
    unittest.main()
