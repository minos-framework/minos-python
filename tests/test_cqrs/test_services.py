"""tests.test_cqrs.test_services module."""

import unittest
from unittest.mock import (
    patch,
)
from uuid import (
    UUID,
    uuid4,
)

from minos.common import (
    ModelType,
)
from minos.common.testing import (
    PostgresAsyncTestCase,
)
from minos.cqrs import (
    MinosIllegalHandlingException,
    Service,
)
from minos.networks import (
    BrokerCommandEnrouteDecorator,
    BrokerQueryEnrouteDecorator,
    ResponseException,
    WrappedRequest,
)
from tests.utils import (
    BASE_PATH,
    AsyncIter,
    FakeCommandService,
    FakeQueryService,
    FakeRequest,
    FakeSagaManager,
    FakeService,
    Foo,
)


class TestServices(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def setUp(self) -> None:
        super().setUp()
        self.saga_manager = FakeSagaManager()
        self.service = FakeService(config=self.config, saga_manager=self.saga_manager)

    async def test_constructor(self):
        self.assertEqual(self.config, self.service.config)
        self.assertEqual(self.saga_manager, self.service.saga_manager)


class TestQueryService(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def setUp(self) -> None:
        super().setUp()
        self.saga_manager = FakeSagaManager()
        self.service = FakeQueryService(config=self.config, saga_manager=self.saga_manager)

    def test_base(self):
        self.assertIsInstance(self.service, Service)

    def test_pre_command(self):
        with self.assertRaises(MinosIllegalHandlingException):
            self.service._pre_command_handle(FakeRequest("foo"))

    def test_pre_query(self):
        request = FakeRequest("foo")
        self.assertEqual(request, self.service._pre_query_handle(request))

    async def test_pre_event(self):
        with patch("minos.cqrs.PreEventHandler.handle") as mock:
            mock.return_value = "bar"
            observed = self.service._pre_event_handle(FakeRequest("foo"))
            self.assertIsInstance(observed, WrappedRequest)
            self.assertEqual(FakeRequest("foo"), observed.base)
            self.assertEqual(0, mock.call_count)
            self.assertEqual("bar", await observed.content())
            self.assertEqual(1, mock.call_count)

    def test_get_enroute(self):
        expected = {
            "find_foo": {BrokerQueryEnrouteDecorator("FindFoo")},
        }
        observed = FakeQueryService.__get_enroute__(self.config)
        self.assertEqual(expected, observed)


class TestCommandService(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def setUp(self) -> None:
        super().setUp()
        self.saga_manager = FakeSagaManager()
        self.service = FakeCommandService(config=self.config, saga_manager=self.saga_manager)

    def test_base(self):
        self.assertIsInstance(self.service, Service)

    def test_pre_command(self):
        request = FakeRequest("foo")
        self.assertEqual(request, self.service._pre_command_handle(request))

    def test_pre_query(self):
        with self.assertRaises(MinosIllegalHandlingException):
            self.service._pre_query_handle(FakeRequest("foo"))

    def test_pre_event(self):
        with self.assertRaises(MinosIllegalHandlingException):
            self.service._pre_event_handle(FakeRequest("foo"))

    def test_get_enroute(self):
        expected = {
            "__get_aggregate__": {BrokerCommandEnrouteDecorator("GetFoo")},
            "__get_aggregates__": {BrokerCommandEnrouteDecorator("GetFoos")},
            "create_foo": {BrokerCommandEnrouteDecorator("CreateFoo")},
        }
        observed = FakeCommandService.__get_enroute__(self.config)
        self.assertEqual(expected, observed)

    async def test_get_aggregate(self):
        uuid = uuid4()
        Agg = ModelType.build("Agg", {"uuid": UUID})
        expected = Agg(uuid)
        with patch("minos.common.Aggregate.get_one", return_value=expected):
            response = await self.service.__get_aggregate__(FakeRequest({"uuid": uuid}))
        self.assertEqual(expected, await response.content())

    async def test_get_aggregate_raises(self):
        with patch("tests.utils.FakeRequest.content", side_effect=ValueError):
            with self.assertRaises(ResponseException):
                await self.service.__get_aggregate__(FakeRequest(None))
        with patch("minos.common.Aggregate.get_one", side_effect=ValueError):
            with self.assertRaises(ResponseException):
                await self.service.__get_aggregate__(FakeRequest({"uuid": uuid4()}))

    async def test_get_aggregates(self):
        uuids = [uuid4(), uuid4()]
        Agg = ModelType.build("Agg", {"uuid": UUID})

        expected = [Agg(u) for u in uuids]
        with patch("minos.common.Aggregate.get", return_value=AsyncIter(expected)):
            response = await self.service.__get_aggregates__(FakeRequest({"uuids": uuids}))
        self.assertEqual(expected, await response.content())

    async def test_get_aggregates_raises(self):
        with patch("tests.utils.FakeRequest.content", side_effect=ValueError):
            with self.assertRaises(ResponseException):
                await self.service.__get_aggregates__(FakeRequest(None))
        with patch("minos.common.Aggregate.get", side_effect=ValueError):
            with self.assertRaises(ResponseException):
                await self.service.__get_aggregates__(FakeRequest({"uuids": [uuid4()]}))

    def test_aggregate_cls(self):
        self.assertEqual(Foo, self.service.__aggregate_cls__)


if __name__ == "__main__":
    unittest.main()
