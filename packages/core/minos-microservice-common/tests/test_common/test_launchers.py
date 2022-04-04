import unittest
import warnings
from unittest.mock import (
    AsyncMock,
    MagicMock,
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
    CommonTestCase,
    FakeEntrypoint,
    FakeLoop,
)


class FooPort(Port):
    """For testing purposes."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.kwargs = kwargs

    async def _start(self) -> None:
        """For testing purposes."""

    async def _stop(self, err: Exception = None) -> None:
        """For testing purposes."""


class TestEntrypointLauncher(CommonTestCase, PostgresAsyncTestCase):
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
        self.assertEqual(len(self.config.get_injections()), len(launcher.injections))

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
        wire_mock = MagicMock()
        setup_mock = AsyncMock()
        mock_entrypoint_aenter = AsyncMock()

        self.launcher.injector.wire_injections = wire_mock
        self.launcher.injector.setup_injections = setup_mock

        with patch("minos.common.launchers._create_loop") as mock_loop:
            loop = FakeLoop()
            mock_loop.return_value = loop
            with patch("minos.common.launchers._create_entrypoint") as mock_entrypoint:
                entrypoint = FakeEntrypoint()
                mock_entrypoint.return_value = entrypoint

                entrypoint.__aenter__ = mock_entrypoint_aenter

                await self.launcher.setup()

        self.assertEqual(1, wire_mock.call_count)
        import tests
        from minos import (
            common,
        )

        self.assertEqual(0, len(wire_mock.call_args.args))
        self.assertEqual(2, len(wire_mock.call_args.kwargs))
        observed = wire_mock.call_args.kwargs["modules"]

        self.assertIn(tests, observed)
        self.assertIn(common, observed)

        self.assertEqual(["tests"], wire_mock.call_args.kwargs["packages"])

        self.assertEqual(1, setup_mock.call_count)
        self.assertEqual(1, mock_entrypoint_aenter.call_count)

    async def test_destroy(self):
        self.launcher._setup = AsyncMock()
        await self.launcher.setup()

        destroy_mock = AsyncMock()
        unwire_mock = MagicMock()
        mock_entrypoint_aexit = AsyncMock()

        self.launcher.injector.destroy_injections = destroy_mock
        self.launcher.injector.unwire_injections = unwire_mock

        with patch("minos.common.launchers._create_loop") as mock_loop:
            loop = FakeLoop()
            mock_loop.return_value = loop
            with patch("minos.common.launchers._create_entrypoint") as mock_entrypoint:
                entrypoint = FakeEntrypoint()
                mock_entrypoint.return_value = entrypoint

                entrypoint.__aexit__ = mock_entrypoint_aexit

                await self.launcher.destroy()

        self.assertEqual(1, unwire_mock.call_count)
        self.assertEqual(1, destroy_mock.call_count)
        self.assertEqual(1, mock_entrypoint_aexit.call_count)

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

        self.assertEqual(1, mock_setup.call_count)
        self.assertEqual(1, mock_destroy.call_count)


class TestEntryPointLauncherLoop(unittest.TestCase):
    def test_loop(self):
        launcher = EntrypointLauncher.from_config(CONFIG_FILE_PATH)
        self.assertIsInstance(launcher.loop, uvloop.Loop)


if __name__ == "__main__":
    unittest.main()
