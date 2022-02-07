import unittest
from unittest.mock import (
    AsyncMock,
    patch,
)
from uuid import (
    UUID,
    uuid4,
)

from minos.aggregate import (
    RootEntity,
    SnapshotService,
)
from minos.common import (
    ModelType,
)
from minos.common.testing import (
    PostgresAsyncTestCase,
)
from minos.networks import (
    BrokerCommandEnrouteDecorator,
    InMemoryRequest,
    PeriodicEventEnrouteDecorator,
    ResponseException,
)
from tests.utils import (
    BASE_PATH,
    MinosTestCase,
    Order,
)

Agg = ModelType.build("Agg", {"uuid": UUID})


class TestSnapshotService(MinosTestCase, PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def setUp(self) -> None:
        super().setUp()

        self.service = SnapshotService(config=self.config)

    def test_get_enroute(self):
        expected = {
            "__get_one__": {BrokerCommandEnrouteDecorator("GetOrderSnapshot")},
            "__get_many__": {BrokerCommandEnrouteDecorator("GetOrderSnapshots")},
            "__synchronize__": {PeriodicEventEnrouteDecorator("* * * * *")},
        }
        observed = SnapshotService.__get_enroute__(self.config)
        self.assertEqual(expected, observed)

    async def test_get(self):
        uuid = uuid4()
        expected = Agg(uuid)
        with patch.object(RootEntity, "get", return_value=expected):
            response = await self.service.__get_one__(InMemoryRequest({"uuid": uuid}))
        self.assertEqual(expected, await response.content())

    async def test_get_raises(self):
        with self.assertRaises(ResponseException):
            await self.service.__get_one__(InMemoryRequest())
        with patch.object(RootEntity, "get", side_effect=ValueError):
            with self.assertRaises(ResponseException):
                await self.service.__get_one__(InMemoryRequest({"uuid": uuid4()}))

    async def test_get_many(self):
        uuids = [uuid4(), uuid4()]

        expected = [Agg(u) for u in uuids]
        with patch.object(RootEntity, "get", side_effect=expected):
            response = await self.service.__get_many__(InMemoryRequest({"uuids": uuids}))
        self.assertEqual(expected, await response.content())

    async def test_get_many_raises(self):
        with self.assertRaises(ResponseException):
            await self.service.__get_many__(InMemoryRequest())
        with patch.object(RootEntity, "get", side_effect=ValueError):
            with self.assertRaises(ResponseException):
                await self.service.__get_many__(InMemoryRequest({"uuids": [uuid4()]}))

    def test_type(self):
        self.assertEqual(Order, self.service.type_)

    async def test_synchronize(self):
        mock = AsyncMock()
        self.snapshot_repository.synchronize = mock
        response = await self.service.__synchronize__(InMemoryRequest(None))
        self.assertEqual(1, mock.call_count)
        self.assertEqual(None, response)


if __name__ == "__main__":
    unittest.main()
