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

from minos.common import (
    MinosConfig,
    MinosDependencyInjector,
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

    def test_container(self):
        injector = MinosDependencyInjector(self.config)
        self.assertEqual(self.config, injector.container.config())

    def test_container_repository(self):
        injector = MinosDependencyInjector(self.config, repository_cls=FakeRepository)
        self.assertIsInstance(injector.container.repository(), FakeRepository)

    def test_container_event_broker(self):
        injector = MinosDependencyInjector(self.config, event_broker_cls=FakeBroker)
        self.assertIsInstance(injector.container.event_broker(), FakeBroker)

    def test_container_command_broker(self):
        injector = MinosDependencyInjector(self.config, command_broker_cls=FakeBroker)
        self.assertIsInstance(injector.container.command_broker(), FakeBroker)

    def test_container_command_reply_broker(self):
        injector = MinosDependencyInjector(self.config, command_reply_broker_cls=FakeBroker)
        self.assertIsInstance(injector.container.command_reply_broker(), FakeBroker)

    def test_container_saga_manager(self):
        injector = MinosDependencyInjector(self.config, saga_manager_cls=FakeSagaManager)
        self.assertIsInstance(injector.container.saga_manager(), FakeSagaManager)

    async def test_wire_unwire(self):
        injector = MinosDependencyInjector(
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
