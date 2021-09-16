import unittest
from unittest.mock import (
    AsyncMock,
    call,
    patch,
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
        import tests

        self.launcher = EntrypointLauncher(
            config=self.config, injections=self.injections, services=self.services, external_modules=[tests],
        )

    def test_from_config(self):
        launcher = EntrypointLauncher.from_config(config=self.config)
        self.assertIsInstance(launcher, EntrypointLauncher)
        self.assertEqual(self.config, launcher.config)
        self.assertEqual(dict(), launcher.injector.injections)
        self.assertEqual(list(), launcher.services)

    def test_services(self):
        self.assertEqual([1, 2], self.launcher.services[:2])
        self.assertIsInstance(self.launcher.services[2], Foo)
        self.assertEqual({"config": self.config}, self.launcher.services[2].kwargs)

    async def test_entrypoint(self):
        mock_setup = AsyncMock()
        self.launcher.setup = mock_setup

        mock_destroy = AsyncMock()
        self.launcher.destroy = mock_destroy

        with patch("minos.common.launchers._create_loop") as mock_loop:
            loop = FakeLoop()
            mock_loop.return_value = loop
            with patch("minos.common.launchers._create_entrypoint") as mock_entrypoint:
                entrypoint = FakeEntrypoint()
                mock_entrypoint.return_value = entrypoint
                self.assertEqual(entrypoint, self.launcher.entrypoint)
                self.assertEqual(dict(loop=loop, log_config=False), mock_entrypoint.call_args.kwargs)

    async def test_loop(self):
        with patch("minos.common.launchers._create_loop") as mock_loop:
            loop = FakeLoop()
            mock_loop.return_value = loop
            self.assertEqual(loop, self.launcher.loop)
            self.assertEqual(call(), mock_loop.call_args)

    async def test_setup(self):
        mock = AsyncMock()
        self.launcher.injector.wire = mock
        await self.launcher.setup()

        self.assertEqual(1, mock.call_count)
        import tests
        from minos import (
            common,
        )

        self.assertEqual(call(modules=[tests, common]), mock.call_args)

    async def test_destroy(self):
        self.launcher.injector.wire = AsyncMock()
        await self.launcher.setup()

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

        with patch("minos.common.launchers._create_loop") as mock_loop:
            loop = FakeLoop()
            mock_loop.return_value = loop
            with patch("minos.common.launchers._create_entrypoint") as mock_entrypoint:
                entrypoint = FakeEntrypoint()
                mock_entrypoint.return_value = entrypoint
                self.launcher.launch()

        self.assertEqual(1, mock_entrypoint.call_count)
        self.assertEqual(1, mock_loop.call_count)


if __name__ == "__main__":
    unittest.main()
