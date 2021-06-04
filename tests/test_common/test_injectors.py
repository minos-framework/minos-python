"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import sys
import unittest
from unittest.mock import (
    MagicMock,
    call,
)

from dependency_injector.containers import (
    Container,
)

from minos.common import (
    DependencyInjector,
    MinosConfig,
)
from tests.utils import (
    BASE_PATH,
    FakeBroker,
    FakeRepository,
    FakeSagaManager,
)


class TestMinosDependencyInjector(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.config_file_path = BASE_PATH / "test_config.yml"
        self.config = MinosConfig(path=str(self.config_file_path))

    def test_repository(self):
        injector = DependencyInjector(self.config, repository_cls=FakeRepository)
        self.assertIsInstance(injector.repository, FakeRepository)

    def test_event_broker(self):
        injector = DependencyInjector(self.config, event_broker_cls=FakeBroker)
        self.assertIsInstance(injector.event_broker, FakeBroker)

    def test_command_broker(self):
        injector = DependencyInjector(self.config, command_broker_cls=FakeBroker)
        self.assertIsInstance(injector.command_broker, FakeBroker)

    def test_command_reply_broker(self):
        injector = DependencyInjector(self.config, command_reply_broker_cls=FakeBroker)
        self.assertIsInstance(injector.container.command_reply_broker(), FakeBroker)

    def test_saga_manager(self):
        injector = DependencyInjector(self.config, saga_manager_cls=FakeSagaManager)
        self.assertIsInstance(injector.saga_manager, FakeSagaManager)

    def test_container(self):
        injector = DependencyInjector(self.config)
        self.assertIsInstance(injector.container, Container)
        self.assertEqual(self.config, injector.container.config())

    def test_container_repository(self):
        injector = DependencyInjector(self.config, repository_cls=FakeRepository)
        self.assertEqual(injector.repository, injector.container.repository())

    def test_container_event_broker(self):
        injector = DependencyInjector(self.config, event_broker_cls=FakeBroker)
        self.assertEqual(injector.event_broker, injector.container.event_broker())

    def test_container_command_broker(self):
        injector = DependencyInjector(self.config, command_broker_cls=FakeBroker)
        self.assertEqual(injector.command_broker, injector.container.command_broker())

    def test_container_command_reply_broker(self):
        injector = DependencyInjector(self.config, command_reply_broker_cls=FakeBroker)
        self.assertEqual(injector.command_reply_broker, injector.container.command_reply_broker())

    def test_container_saga_manager(self):
        injector = DependencyInjector(self.config, saga_manager_cls=FakeSagaManager)
        self.assertEqual(injector.saga_manager, injector.container.saga_manager())

    async def test_wire_unwire(self):
        injector = DependencyInjector(
            self.config,
            repository_cls=FakeRepository,
            event_broker_cls=FakeBroker,
            command_broker_cls=FakeBroker,
            command_reply_broker_cls=FakeBroker,
            saga_manager_cls=FakeSagaManager,
        )

        mock = MagicMock()
        injector.container.wire = mock
        await injector.wire(modules=[sys.modules[__name__]])
        self.assertEqual(1, mock.call_count)
        self.assertEqual(call(modules=[sys.modules[__name__]]), mock.call_args)

        mock = MagicMock()
        injector.container.unwire = mock
        await injector.unwire()
        self.assertEqual(1, mock.call_count)


if __name__ == "__main__":
    unittest.main()
