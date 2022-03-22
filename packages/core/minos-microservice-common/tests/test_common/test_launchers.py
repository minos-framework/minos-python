import unittest
import warnings
from unittest.mock import (
    AsyncMock,
    call,
    patch,
)

import uvloop

from minos.common import (
    EntrypointLauncher,
    InjectableMixin,
    Port,
    classname,
)
from minos.common.testing import (
    PostgresAsyncTestCase,
)
from tests.utils import (
    CONFIG_FILE_PATH,
    FakeEntrypoint,
    FakeLoop,
)


class FooPort(Port):
    """For testing purposes."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.kwargs = kwargs

    async def start(self) -> None:
        """For testing purposes."""

    async def stop(self, err: Exception = None) -> None:
        """For testing purposes."""


class TestEntrypointLauncher(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = CONFIG_FILE_PATH

    def setUp(self):
        super().setUp()
        self.injections = list()
        self.ports = [1, 2, FooPort, classname(FooPort)]
        import tests

        self.launcher = EntrypointLauncher(
            config=self.config,
            injections=self.injections,
            ports=self.ports,
            external_modules=[tests],
            external_packages=["tests"],
        )

    def test_from_config(self):
        launcher = EntrypointLauncher.from_config(self.config)
        self.assertIsInstance(launcher, EntrypointLauncher)
        self.assertEqual(self.config, launcher.config)
        self.assertEqual(12, len(launcher.injections))

        for injection in launcher.injections.values():
            self.assertIsInstance(injection, InjectableMixin)

        self.assertEqual(3, len(launcher.ports))
        for port in launcher.ports:
            self.assertIsInstance(port, Port)

    def test_injections(self):
        self.assertEqual(dict(), self.launcher.injections)

    def test_ports(self):
        self.assertEqual([1, 2], self.launcher.ports[:2])
        self.assertIsInstance(self.launcher.ports[2], FooPort)
        # noinspection PyUnresolvedReferences
        self.assertEqual({"config": self.config}, self.launcher.ports[2].kwargs)

    def test_services(self):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            self.assertEqual(self.launcher.ports, self.launcher.services)

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
        self.launcher.injector.wire_and_setup = mock
        await self.launcher.setup()

        self.assertEqual(1, mock.call_count)
        import tests
        from minos import (
            common,
        )

        self.assertEqual(0, len(mock.call_args.args))
        self.assertEqual(2, len(mock.call_args.kwargs))
        observed = mock.call_args.kwargs["modules"]

        self.assertIn(tests, observed)
        self.assertIn(common, observed)

        self.assertEqual(["tests"], mock.call_args.kwargs["packages"])

        await self.launcher.destroy()

    async def test_destroy(self):
        self.launcher.injector.wire_and_setup = AsyncMock()
        await self.launcher.setup()

        mock = AsyncMock()
        self.launcher.injector.unwire_and_destroy = mock
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

                with warnings.catch_warnings():
                    warnings.simplefilter("ignore")

                    self.launcher.launch()

        self.assertEqual(1, mock_entrypoint.call_count)
        self.assertEqual(1, mock_loop.call_count)


class TestEntryPointLauncherLoop(unittest.TestCase):
    def test_loop(self):
        launcher = EntrypointLauncher.from_config(CONFIG_FILE_PATH)
        self.assertIsInstance(launcher.loop, uvloop.Loop)


if __name__ == "__main__":
    unittest.main()
