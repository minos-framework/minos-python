"""tests.test_cqrs.test_services module."""
import sys
import unittest
from unittest.mock import (
    AsyncMock,
    patch,
)

from minos.common import (
    DatabaseLockPool,
    DependencyInjector,
    PoolFactory,
)
from minos.common.testing import (
    DatabaseMinosTestCase,
)
from minos.cqrs import (
    MinosIllegalHandlingException,
    Service,
)
from minos.networks import (
    BrokerCommandEnrouteDecorator,
    BrokerQueryEnrouteDecorator,
    InMemoryRequest,
    WrappedRequest,
)
from tests.utils import (
    BASE_PATH,
    FakeCommandService,
    FakeQueryService,
    FakeService,
)


class TestService(DatabaseMinosTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def setUp(self) -> None:
        super().setUp()

        self.lock_pool = DatabaseLockPool.from_config(self.config)

        self.injector = DependencyInjector(self.config, [PoolFactory])
        self.injector.wire_injections(modules=[sys.modules[__name__]])

        self.service = FakeService(config=self.config, lock_pool=self.lock_pool)

    def tearDown(self) -> None:
        self.injector.unwire_injections()

    async def test_constructor(self):
        self.assertEqual(self.config, self.service.config)
        self.assertEqual(self.lock_pool, self.service.lock_pool)
        self.assertEqual(self.injector.pool_factory, self.service.pool_factory)

        with self.assertRaises(AttributeError):
            self.service.event_repository

    async def test_pre_event(self):
        with patch("minos.cqrs.PreEventHandler.handle") as mock:
            mock.return_value = "bar"
            observed = self.service._pre_event_handle(InMemoryRequest("foo"))
            self.assertIsInstance(observed, WrappedRequest)
            self.assertEqual(InMemoryRequest("foo"), observed.base)
            self.assertEqual(0, mock.call_count)
            self.assertEqual("bar", await observed.content())
            self.assertEqual(1, mock.call_count)


class TestQueryService(DatabaseMinosTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def setUp(self) -> None:
        super().setUp()
        self.saga_manager = AsyncMock()
        self.service = FakeQueryService(config=self.config, saga_manager=self.saga_manager)

    def test_base(self):
        self.assertIsInstance(self.service, Service)

    def test_pre_command(self):
        with self.assertRaises(MinosIllegalHandlingException):
            self.service._pre_command_handle(InMemoryRequest("foo"))

    def test_pre_query(self):
        request = InMemoryRequest("foo")
        self.assertEqual(request, self.service._pre_query_handle(request))

    def test_get_enroute(self):
        expected = {
            "find_foo": {BrokerQueryEnrouteDecorator("FindFoo")},
        }
        observed = FakeQueryService.__get_enroute__(self.config)
        self.assertEqual(expected, observed)


class TestCommandService(DatabaseMinosTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def setUp(self) -> None:
        super().setUp()
        self.saga_manager = AsyncMock()
        self.service = FakeCommandService(config=self.config, saga_manager=self.saga_manager)

    def test_base(self):
        self.assertIsInstance(self.service, Service)

    def test_pre_command(self):
        request = InMemoryRequest("foo")
        self.assertEqual(request, self.service._pre_command_handle(request))

    def test_pre_query(self):
        with self.assertRaises(MinosIllegalHandlingException):
            self.service._pre_query_handle(InMemoryRequest("foo"))

    def test_get_enroute(self):
        expected = {
            "create_foo": {BrokerCommandEnrouteDecorator("CreateFoo")},
        }
        observed = FakeCommandService.__get_enroute__(self.config)
        self.assertEqual(expected, observed)


if __name__ == "__main__":
    unittest.main()
