import warnings
from abc import (
    ABC,
    abstractmethod,
)
from collections import (
    defaultdict,
)
from pathlib import (
    Path,
)
from typing import (
    Any,
    Optional,
    Union,
)

import yaml

from ..exceptions import (
    MinosConfigException,
)
from ..injections import (
    Injectable,
)


@Injectable("config")
class Config(ABC):
    """Config base class."""

    def __new__(cls, *args, **kwargs):
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

    def get_database(self, name: Optional[str] = None) -> dict[str, Any]:
        """TODO

        :param name: TODO
        :return: TODO
        """
        if name is None:
            name = "default"

        return self._get_database(name)

    @abstractmethod
    def _get_database(self, name: str) -> dict[str, Any]:
        raise NotImplementedError

    def get_injections(self) -> list[str]:
        """TODO

        :return: TODO
        """
        return self._get_injections()

    @abstractmethod
    def _get_injections(self) -> list[str]:
        raise NotImplementedError

    def get_ports(self) -> list[str]:
        """TODO

        :return: TODO
        """
        return self._get_ports()

    @abstractmethod
    def _get_ports(self) -> list[str]:
        raise NotImplementedError

    def get_name(self) -> str:
        """TODO

        :return: TODO
        """
        return self._get_name()

    @abstractmethod
    def _get_name(self) -> str:
        raise NotImplementedError

    def get_interface(self, name: str) -> dict[str, Any]:
        """TODO

        :param name: TODO
        :return: TODO
        """
        return self._get_interface(name)

    @abstractmethod
    def _get_interface(self, name: str):
        raise NotImplementedError

    def get_routers(self) -> list[str]:
        """TODO

        :return: TODO
        """
        return self._get_routers()

    @abstractmethod
    def _get_routers(self) -> list[str]:
        raise NotImplementedError

    def get_middleware(self) -> list[str]:
        """TODO

        :return: TODO
        """
        return self._get_middleware()

    @abstractmethod
    def _get_middleware(self) -> list[str]:
        raise NotImplementedError

    def get_discovery(self) -> dict[str, Any]:
        """TODO

        :return: TODO
        """
        return self._get_discovery()

    @abstractmethod
    def _get_discovery(self) -> dict[str, Any]:
        raise NotImplementedError

    def get_services(self) -> list[str]:
        """TODO

        :return: TODO
        """
        return self._get_services()

    @abstractmethod
    def _get_services(self) -> list[str]:
        raise NotImplementedError

    def get_aggregate(self) -> dict[str, Any]:
        """TODO

        :return: TODO
        """
        return self._get_aggregate()

    @abstractmethod
    def _get_aggregate(self) -> dict[str, Any]:
        raise NotImplementedError


# noinspection PyUnusedLocal
def _get_version(path: Union[str, Path], *args, **kwargs) -> int:
    if isinstance(path, str):
        path = Path(path)
    if not path.exists():
        raise MinosConfigException(f"Check if this path: {path} is correct")

    with path.open() as file:
        data = yaml.load(file, Loader=yaml.FullLoader)

    return data.get("version", 1)


class MinosConfig(Config):
    """MinosConfig class."""

    def __new__(cls, *args, **kwargs):
        warnings.warn(f"{MinosConfig!r} has been deprecated. Use {Config} instead.", DeprecationWarning)
        return super().__new__(cls, *args, **kwargs)
