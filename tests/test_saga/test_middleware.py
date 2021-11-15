import unittest
from collections import (
    namedtuple,
)
from uuid import (
    uuid4,
)

from minos.aggregate import (
    TransactionNotFoundException,
    TransactionStatus,
)
from minos.common import (
    NULL_UUID,
)
from minos.networks import (
    HandlerRequest,
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


Raw = namedtuple("Raw", ["saga", "data"])


class TestMiddleware(MinosTestCase):
    async def test_transactional_command_unused(self):
        uuid = NULL_UUID
        request = HandlerRequest(Raw(uuid, "foo"))
        response = await transactional_command(request, _fn)

        self.assertEqual("foobar", await response.content())
        with self.assertRaises(TransactionNotFoundException):
            await self.transaction_repository.get(uuid)

    async def test_transactional_command_used(self):
        uuid = uuid4()
        request = HandlerRequest(Raw(uuid, "foo"))
        response = await transactional_command(request, _fn)

        self.assertEqual("foobar", await response.content())

        transaction = await self.transaction_repository.get(uuid)
        self.assertEqual(TransactionStatus.PENDING, transaction.status)


if __name__ == "__main__":
    unittest.main()
