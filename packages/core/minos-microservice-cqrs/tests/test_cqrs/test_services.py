"""tests.test_cqrs.test_services module."""
import unittest
from unittest.mock import (
    AsyncMock,
    patch,
)

from minos.common.testing import (
    MinosTestCase,
)
from minos.cqrs import (
    MinosIllegalHandlingException,
    PreEventHandler,
    Service,
)
from minos.networks import (
    InMemoryRequest,
    WrappedRequest,
)
from tests.utils import (
    BASE_PATH,
    FakeCommandService,
    FakeQueryService,
    FakeService,
)


class TestService(MinosTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def setUp(self) -> None:
        super().setUp()
        self.service = FakeService(config=self.config)

    def get_injections(self):
        return self.config.get_injections()

    async def test_constructor(self):
        self.assertEqual(self.aggregate, self.service.aggregate)

        with self.assertRaises(AttributeError):
            self.service.delta_repository

    async def test_pre_event(self):
        with patch.object(PreEventHandler, "handle") as mock:
            mock.return_value = "bar"
            observed = self.service._pre_event_handle(InMemoryRequest("foo"))
            self.assertIsInstance(observed, WrappedRequest)
            self.assertEqual(InMemoryRequest("foo"), observed.base)
            self.assertEqual(0, mock.call_count)
            self.assertEqual("bar", await observed.content())
            self.assertEqual(1, mock.call_count)


class TestQueryService(MinosTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def setUp(self) -> None:
        super().setUp()
        self.saga_manager = AsyncMock()
        self.service = FakeQueryService()

    def get_injections(self):
        return self.config.get_injections()

    def test_base(self):
        self.assertIsInstance(self.service, Service)

    def test_pre_command(self):
        with self.assertRaises(MinosIllegalHandlingException):
            self.service._pre_command_handle(InMemoryRequest("foo"))

    def test_pre_query(self):
        request = InMemoryRequest("foo")
        self.assertEqual(request, self.service._pre_query_handle(request))


class TestCommandService(MinosTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def setUp(self) -> None:
        super().setUp()
        self.saga_manager = AsyncMock()
        self.service = FakeCommandService()

    def get_injections(self):
        return self.config.get_injections()

    def test_base(self):
        self.assertIsInstance(self.service, Service)

    def test_pre_command(self):
        request = InMemoryRequest("foo")
        self.assertEqual(request, self.service._pre_command_handle(request))

    def test_pre_query(self):
        with self.assertRaises(MinosIllegalHandlingException):
            self.service._pre_query_handle(InMemoryRequest("foo"))


if __name__ == "__main__":
    unittest.main()
