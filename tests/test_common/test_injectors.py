"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import sys
import unittest
from typing import (
    AsyncIterator,
    NoReturn,
)
from unittest.mock import (
    MagicMock,
    call,
)

from minos.common import (
    Aggregate,
    CommandReply,
    MinosBroker,
    MinosConfig,
    MinosDependencyInjector,
    MinosRepository,
    MinosRepositoryEntry,
    MinosSagaManager,
)
from tests.utils import (
    BASE_PATH,
)


class _MinosRepository(MinosRepository):
    async def _submit(self, entry: MinosRepositoryEntry) -> MinosRepositoryEntry:
        pass

    async def _select(self, *args, **kwargs) -> AsyncIterator[MinosRepositoryEntry]:
        pass


class _MinosBroker(MinosBroker):
    @classmethod
    async def send(cls, items: list[Aggregate], **kwargs) -> NoReturn:
        pass


class _MinosSagaManager(MinosSagaManager):
    def _run_new(self, name: str, **kwargs) -> NoReturn:
        pass

    def _load_and_run(self, reply: CommandReply, **kwargs) -> NoReturn:
        pass


class TestMinosDependencyInjector(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.config_file_path = BASE_PATH / "test_config.yml"
        self.config = MinosConfig(path=str(self.config_file_path))

    def test_container(self):
        injector = MinosDependencyInjector(self.config)
        self.assertEqual(self.config, injector.container.config())

    def test_container_repository(self):
        injector = MinosDependencyInjector(self.config, repository_cls=_MinosRepository)
        self.assertIsInstance(injector.container.repository(), _MinosRepository)

    def test_container_event_broker(self):
        injector = MinosDependencyInjector(self.config, event_broker_cls=_MinosBroker)
        self.assertIsInstance(injector.container.event_broker(), _MinosBroker)

    def test_container_command_broker(self):
        injector = MinosDependencyInjector(self.config, command_broker_cls=_MinosBroker)
        self.assertIsInstance(injector.container.command_broker(), _MinosBroker)

    def test_container_command_reply_broker(self):
        injector = MinosDependencyInjector(self.config, command_reply_broker_cls=_MinosBroker)
        self.assertIsInstance(injector.container.command_reply_broker(), _MinosBroker)

    def test_container_saga_manager(self):
        injector = MinosDependencyInjector(self.config, saga_manager_cls=_MinosSagaManager)
        self.assertIsInstance(injector.container.saga_manager(), _MinosSagaManager)

    async def test_wire_unwire(self):
        injector = MinosDependencyInjector(
            self.config,
            repository_cls=_MinosRepository,
            event_broker_cls=_MinosBroker,
            command_broker_cls=_MinosBroker,
            command_reply_broker_cls=_MinosBroker,
            saga_manager_cls=_MinosSagaManager,
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
