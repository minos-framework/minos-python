"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

import unittest
from unittest.mock import (
    MagicMock,
    PropertyMock,
    call,
    patch,
)

from aiomisc.entrypoint import (
    Entrypoint,
)

from minos.common import (
    DependencyInjector,
    EntrypointLauncher,
)
from minos.common.testing import (
    PostgresAsyncTestCase,
)
from tests.utils import (
    BASE_PATH,
    FakeBroker,
    FakeEntrypoint,
    FakeRepository,
    FakeSagaManager,
)


class TestEntrypointLauncher(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def setUp(self):
        super().setUp()
        self.injector = DependencyInjector(
            self.config,
            repository_cls=FakeRepository,
            event_broker_cls=FakeBroker,
            command_broker_cls=FakeBroker,
            command_reply_broker_cls=FakeBroker,
            saga_manager_cls=FakeSagaManager,
        )
        self.services = ["a", "b", "c"]

        self.launcher = EntrypointLauncher(injector=self.injector, services=self.services)

    def test_injector(self):
        self.assertEqual(self.injector, self.launcher.injector)

    def test_services(self):
        self.assertEqual(self.services, self.launcher.services)

    async def test_entrypoint(self):
        async def _fn(*args, **kwargs):
            pass

        mock = MagicMock(side_effect=_fn)
        self.launcher.setup = mock
        self.launcher.destroy = mock
        self.assertIsInstance(self.launcher.entrypoint, Entrypoint)

    async def test_setup(self):
        async def _fn(*args, **kwargs):
            pass

        mock = MagicMock(side_effect=_fn)
        self.launcher.injector.wire = mock
        await self.launcher.setup()

        self.assertEqual(1, mock.call_count)
        from minos import (
            common,
        )

        self.assertEqual(call(modules=[common]), mock.call_args)

    async def test_destroy(self):
        async def _fn(*args, **kwargs):
            pass

        mock = MagicMock(side_effect=_fn)
        self.launcher.injector.unwire = mock
        await self.launcher.destroy()

        self.assertEqual(1, mock.call_count)
        self.assertEqual(call(), mock.call_args)

    def test_launch(self):
        entrypoint = FakeEntrypoint()
        with patch("minos.common.EntrypointLauncher.entrypoint", new_callable=PropertyMock) as mock:
            mock.return_value = entrypoint
            self.launcher.launch()
        self.assertEqual(1, entrypoint.call_count)


if __name__ == "__main__":
    unittest.main()
