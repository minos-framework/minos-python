import unittest
import warnings
from typing import (
    Any,
)
from unittest.mock import (
    MagicMock,
    PropertyMock,
    call,
    patch,
)

from minos.common import (
    Config,
    ConfigV1,
    InjectableMixin,
    MinosConfig,
    MinosConfigException,
    Port,
)
from tests.utils import (
    CONFIG_FILE_PATH,
)


class _Config(Config):
    """For testing purposes."""

    # noinspection PyPropertyDefinition
    @property
    def _version(self) -> int:
        """For testing purposes."""

    def _get_name(self) -> str:
        """For testing purposes."""

    def _get_injections(self) -> list[type[InjectableMixin]]:
        """For testing purposes."""

    def _get_databases(self) -> dict[str, dict[str, Any]]:
        """For testing purposes."""

    def _get_interfaces(self) -> dict[str, dict[str, Any]]:
        """For testing purposes."""

    def _get_ports(self) -> list[type[Port]]:
        """For testing purposes."""

    def _get_routers(self) -> list[type]:
        """For testing purposes."""

    def _get_middleware(self) -> list[type]:
        """For testing purposes."""

    def _get_services(self) -> list[type]:
        """For testing purposes."""

    def _get_discovery(self) -> dict[str, Any]:
        """For testing purposes."""

    def _get_aggregate(self) -> dict[str, Any]:
        """For testing purposes."""

    def _get_saga(self) -> dict[str, Any]:
        """For testing purposes."""


class TestConfig(unittest.TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.config = _Config(CONFIG_FILE_PATH)

    def test_is_subclass(self):
        self.assertTrue(issubclass(Config, InjectableMixin))

    def test_get_injectable_name(self):
        self.assertTrue("config", _Config.get_injectable_name())

    def test_file_path(self):
        self.assertEqual(CONFIG_FILE_PATH, self.config.file_path)

    def test_get_by_key(self):
        self.assertEqual("Order", self.config.get_by_key("service.name"))

    def test_get_cls_by_key(self):
        self.assertEqual(int, self.config.get_type_by_key("service.aggregate"))

    def test_get_version(self):
        with patch.object(_Config, "_version", new_callable=PropertyMock, return_value=0) as mock:
            self.assertEqual(0, self.config.version)

        self.assertEqual([call()], mock.call_args_list)

    def test_get_name(self):
        mock = MagicMock(return_value="foo")
        self.config._get_name = mock

        self.assertEqual("foo", self.config.get_name())

        self.assertEqual([call()], mock.call_args_list)

    def test_get_injections(self):
        mock = MagicMock(return_value="foo")
        self.config._get_injections = mock

        self.assertEqual("foo", self.config.get_injections())

        self.assertEqual([call()], mock.call_args_list)

    def test_get_database_default(self):
        mock = MagicMock(return_value={"default": "foo"})
        self.config._get_databases = mock

        self.assertEqual("foo", self.config.get_database_by_name())

        self.assertEqual([call()], mock.call_args_list)

    def test_get_database_event(self):
        mock = MagicMock(return_value={"event": "foo"})
        self.config._get_databases = mock

        self.assertEqual("foo", self.config.get_database_by_name("event"))

        self.assertEqual([call()], mock.call_args_list)

    def test_get_interface_http(self):
        mock = MagicMock(return_value={"http": "foo"})
        self.config._get_interfaces = mock

        self.assertEqual("foo", self.config.get_interface_by_name("http"))

        self.assertEqual([call()], mock.call_args_list)

    def test_get_routers(self):
        mock = MagicMock(return_value="foo")
        self.config._get_routers = mock

        self.assertEqual("foo", self.config.get_routers())

        self.assertEqual([call()], mock.call_args_list)

    def test_get_middleware(self):
        mock = MagicMock(return_value="foo")
        self.config._get_middleware = mock

        self.assertEqual("foo", self.config.get_middleware())

        self.assertEqual([call()], mock.call_args_list)

    def test_get_services(self):
        mock = MagicMock(return_value="foo")
        self.config._get_services = mock

        self.assertEqual("foo", self.config.get_services())

        self.assertEqual([call()], mock.call_args_list)

    def test_get_discovery(self):
        mock = MagicMock(return_value="foo")
        self.config._get_discovery = mock

        self.assertEqual("foo", self.config.get_discovery())

        self.assertEqual([call()], mock.call_args_list)

    def test_get_aggregate(self):
        mock = MagicMock(return_value="foo")
        self.config._get_aggregate = mock

        self.assertEqual("foo", self.config.get_aggregate())

        self.assertEqual([call()], mock.call_args_list)

    def test_get_saga(self):
        mock = MagicMock(return_value="foo")
        self.config._get_saga = mock

        self.assertEqual("foo", self.config.get_saga())

        self.assertEqual([call()], mock.call_args_list)

    def test_new(self):
        config = Config(CONFIG_FILE_PATH)
        self.assertIsInstance(config, ConfigV1)

    def test_new_raises(self):
        with self.assertRaises(MinosConfigException):
            Config("path/to/config.yml")


class TestMinosConfig(unittest.TestCase):
    def test_is_subclass(self):
        self.assertTrue(issubclass(MinosConfig, Config))

    def test_warnings(self):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            config = MinosConfig(CONFIG_FILE_PATH)
            self.assertIsInstance(config, Config)


if __name__ == "__main__":
    unittest.main()
