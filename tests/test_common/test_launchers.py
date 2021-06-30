"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

import unittest
from unittest.mock import (
    AsyncMock,
    call,
    patch,
)

from aiomisc.entrypoint import (
    Entrypoint,
)

from minos.common import (
    EntrypointLauncher,
    classname,
)
from minos.common.testing import (
    PostgresAsyncTestCase,
)
from tests.utils import (
    BASE_PATH,
    FakeBroker,
    FakeEntrypoint,
    FakeLoop,
    FakeRepository,
    FakeSagaManager,
)


class Foo:
    def __init__(self, **kwargs):
        self.kwargs = kwargs


class TestEntrypointLauncher(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def setUp(self):
        super().setUp()
        self.injections = {
            "repository": FakeRepository,
            "event_broker": FakeBroker,
            "command_broker": FakeBroker,
            "command_reply_broker": FakeBroker,
            "saga_manager": FakeSagaManager,
        }
        self.services = [1, 2, Foo, classname(Foo)]
        self.launcher = EntrypointLauncher(config=self.config, injections=self.injections, services=self.services)

    def test_from_config(self):
        launcher = EntrypointLauncher.from_config(config=self.config)
        self.assertIsInstance(launcher, EntrypointLauncher)
        self.assertEqual(self.config, launcher.config)
        self.assertEqual(dict(), launcher.injector.injections)
        self.assertEqual(list(), launcher.services)

    def test_services(self):
        self.assertEqual([1, 2], self.launcher.services[:2])
        self.assertIsInstance(self.launcher.services[2], Foo)
        self.assertEqual({"config": self.config, "interval": 0.1}, self.launcher.services[2].kwargs)

    async def test_entrypoint(self):
        mock = AsyncMock()
        self.launcher.setup = mock
        self.launcher.destroy = mock
        self.assertIsInstance(self.launcher.entrypoint, Entrypoint)

    async def test_setup(self):
        mock = AsyncMock()
        self.launcher.injector.wire = mock
        await self.launcher.setup()

        self.assertEqual(1, mock.call_count)
        from minos import (
            common,
        )

        self.assertEqual(call(modules=[common]), mock.call_args)

    async def test_destroy(self):
        mock = AsyncMock()
        self.launcher.injector.unwire = mock
        await self.launcher.destroy()

        self.assertEqual(1, mock.call_count)
        self.assertEqual(call(), mock.call_args)

    def test_launch(self):
        mock_setup = AsyncMock()
        self.launcher.setup = mock_setup

        mock_destroy = AsyncMock()
        self.launcher.destroy = mock_destroy

        with patch("minos.common.launchers._create_entrypoint") as mock_launcher:
            mock_launcher.side_effect = FakeEntrypoint
            with patch("minos.common.launchers._create_loop") as mock_loop:
                mock_loop.side_effect = FakeLoop
                self.launcher.launch()

        self.assertEqual(1, mock_launcher.call_count)
        self.assertEqual(1, mock_loop.call_count)
        # FIXME: Improve this tests.


if __name__ == "__main__":
    unittest.main()
