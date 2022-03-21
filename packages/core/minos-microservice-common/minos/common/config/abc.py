from __future__ import (
    annotations,
)

import os
import warnings
from abc import (
    ABC,
    abstractmethod,
)
from collections import (
    defaultdict,
)
from collections.abc import (
    Callable,
)
from pathlib import (
    Path,
)
from typing import (
    TYPE_CHECKING,
    Any,
    Optional,
    Union,
)

import yaml

from ..exceptions import (
    MinosConfigException,
)
from ..importlib import (
    import_module,
)
from ..injections import (
    Injectable,
)

if TYPE_CHECKING:
    from ..injections import (
        InjectableMixin,
    )


@Injectable("config")
class Config(ABC):
    """Config base class."""

    _PARAMETERIZED_MAPPER: dict = dict()
    _ENVIRONMENT_MAPPER: dict = dict()

    __slots__ = ("_file_path", "_data", "_with_environment", "_parameterized")

    def __init__(self, path: Union[Path, str], with_environment: bool = True, **kwargs):
        super().__init__()
        if isinstance(path, str):
            path = Path(path)

        self._file_path = path
        self._data = self._load(path)

        self._with_environment = with_environment
        self._parameterized = kwargs

    def __new__(cls, *args, **kwargs) -> Config:
        if cls not in (Config, MinosConfig):
            return super().__new__(cls)

        from .v1 import (
            ConfigV1,
        )

        version_mapper = defaultdict(
            lambda: ConfigV1,
            {
                1: ConfigV1,
            },
        )

        version = _get_version(*args, **kwargs)

        return super().__new__(version_mapper[version])

    @property
    def file_path(self) -> Path:
        """Get the config's file path.

        :return: A ``Path`` instance.
        """
        return self._file_path

    @staticmethod
    def _load(path: Path) -> dict[str, Any]:
        if not path.exists():
            raise MinosConfigException(f"Check if this path: {path} is correct")

        with path.open() as file:
            return yaml.load(file, Loader=yaml.FullLoader)

    @property
    def version(self) -> int:
        """Get the version value.

        :return: A ``int`` instance.
        """
        return self._version

    @property
    @abstractmethod
    def _version(self) -> int:
        raise NotImplementedError

    def get_name(self) -> str:
        """Get the name value.

        :return: A ``str`` instance.
        """
        return self._get_name()

    @abstractmethod
    def _get_name(self) -> str:
        raise NotImplementedError

    def get_injections(self) -> list[type[InjectableMixin]]:
        """Get the injections value.

        :return: A ``list`` of ``InjectableMixin`` types.
        """
        return self._get_injections()

    @abstractmethod
    def _get_injections(self) -> list[type[InjectableMixin]]:
        raise NotImplementedError

    def get_database_by_name(self, name: Optional[str] = None) -> dict[str, Any]:
        """Get the database value by name.

        :param name: The name of the database. If ``None`` is provided then the default database will be used.
        :return: A ``dict`` containing the database's config values.
        """
        databases = self.get_databases()

        if name is None or name not in databases:
            name = "default"

        return databases[name]

    def get_databases(self) -> dict[str, dict[str, Any]]:
        """Get all databases' values.

        :return: A mapping from database name to database's config values.
        """
        return self._get_databases()

    def _get_databases(self) -> dict[str, dict[str, Any]]:
        raise NotImplementedError

    def get_interface_by_name(self, name: str) -> dict[str, Any]:
        """Get the interface value by name.

        :param name: The name of the interface.
        :return: A ``dict`` containing the interface's config values.
        """

        interfaces = self.get_interfaces()

        if name not in interfaces:
            raise MinosConfigException(f"There is not a {name!r} interface.")

        return interfaces[name]

    def get_interfaces(self) -> dict[str, dict[str, Any]]:
        """Get all interfaces' values.

        :return: A mapping from interface name to interface's config values.
        """
        return self._get_interfaces()

    def _get_interfaces(self) -> dict[str, dict[str, Any]]:
        raise NotImplementedError

    def get_routers(self) -> list[type]:
        """Get the routers value.

        :return: A ``list`` of ``type`` instances.
        """
        return self._get_routers()

    @abstractmethod
    def _get_routers(self) -> list[type]:
        raise NotImplementedError

    def get_middleware(self) -> list[Callable]:
        """Get the middleware value.

        :return: A ``list`` of ``Callable`` instances.
        """
        return self._get_middleware()

    @abstractmethod
    def _get_middleware(self) -> list[type]:
        raise NotImplementedError

    def get_services(self) -> list[type]:
        """Get the services value.

        :return: A ``list`` of ``type`` instances.
        """
        return self._get_services()

    @abstractmethod
    def _get_services(self) -> list[type]:
        raise NotImplementedError

    def get_discovery(self) -> dict[str, Any]:
        """Get the discovery value.

        :return: A ``dict`` instance containing the discovery's config values.
        """
        return self._get_discovery()

    @abstractmethod
    def _get_discovery(self) -> dict[str, Any]:
        raise NotImplementedError

    def get_aggregate(self) -> dict[str, Any]:
        """Get the aggregate value.

        :return: A ``dict`` instance containing the aggregate's config values.
        """
        return self._get_aggregate()

    @abstractmethod
    def _get_aggregate(self) -> dict[str, Any]:
        raise NotImplementedError

    def get_saga(self) -> dict[str, Any]:
        """Get the saga value.

        :return: A ``dict`` instance containing the saga's config values.
        """
        return self._get_saga()

    @abstractmethod
    def _get_saga(self) -> dict[str, Any]:
        raise NotImplementedError

    def get_type_by_key(self, key: str) -> type:
        """Get a type instance by key.

        :param key: The key that identifies the value.
        :return: A ``type`` instance.
        """
        classname = self.get_by_key(key)
        return import_module(classname)

    def get_by_key(self, key: str) -> Any:
        """Get a value by key.

        :param key: The key that identifies the value.
        :return: A value instance.
        """
        if key in self._PARAMETERIZED_MAPPER and self._PARAMETERIZED_MAPPER[key] in self._parameterized:
            return self._parameterized[self._PARAMETERIZED_MAPPER[key]]

        if self._with_environment and key in self._ENVIRONMENT_MAPPER and self._ENVIRONMENT_MAPPER[key] in os.environ:
            return os.environ[self._ENVIRONMENT_MAPPER[key]]

        def _fn(k: str, data: dict[str, Any]) -> Any:
            current, _, following = k.partition(".")

            part = data[current]
            if not following:
                return part

            return _fn(following, part)

        try:
            return _fn(key, self._data)
        except Exception:
            raise MinosConfigException(f"{key!r} field is not defined on the configuration!")


# noinspection PyUnusedLocal
def _get_version(path: Union[str, Path], *args, **kwargs) -> int:
    if isinstance(path, str):
        path = Path(path)
    if not path.exists():
        raise MinosConfigException(f"Check if this path: {path} is correct")

    with path.open() as file:
        data = yaml.load(file, Loader=yaml.FullLoader)

    return data.get("version", 1)


class MinosConfig(Config, ABC):
    """MinosConfig class."""

    def __new__(cls, *args, **kwargs):
        warnings.warn(f"{MinosConfig!r} has been deprecated. Use {Config} instead.", DeprecationWarning)
        return super().__new__(cls, *args, **kwargs)
