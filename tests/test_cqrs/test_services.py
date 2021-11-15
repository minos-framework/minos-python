"""tests.test_cqrs.test_services module."""

import unittest
from unittest.mock import (
    AsyncMock,
    patch,
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
    WrappedRequest,
)
from tests.utils import (
    BASE_PATH,
    FakeCommandService,
    FakeQueryService,
    FakeRequest,
    FakeService,
)


class TestServices(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def setUp(self) -> None:
        super().setUp()
        self.saga_manager = AsyncMock()
        self.service = FakeService(config=self.config, saga_manager=self.saga_manager)

    async def test_constructor(self):
        self.assertEqual(self.config, self.service.config)
        self.assertEqual(self.saga_manager, self.service.saga_manager)


class TestQueryService(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def setUp(self) -> None:
        super().setUp()
        self.saga_manager = AsyncMock()
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
        self.saga_manager = AsyncMock()
        self.service = FakeCommandService(config=self.config, saga_manager=self.saga_manager)

    def test_base(self):
        self.assertIsInstance(self.service, Service)

    def test_pre_command(self):
        request = FakeRequest("foo")
        self.assertEqual(request, self.service._pre_command_handle(request))

    def test_pre_query(self):
        with self.assertRaises(MinosIllegalHandlingException):
            self.service._pre_query_handle(FakeRequest("foo"))

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
            "create_foo": {BrokerCommandEnrouteDecorator("CreateFoo")},
        }
        observed = FakeCommandService.__get_enroute__(self.config)
        self.assertEqual(expected, observed)


if __name__ == "__main__":
    unittest.main()
