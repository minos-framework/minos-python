from __future__ import (
    annotations,
)

from contextlib import (
    suppress,
)
from copy import (
    deepcopy,
)
from typing import (
    TYPE_CHECKING,
    Any,
    Union,
)

from ..exceptions import (
    MinosConfigException,
)
from ..importlib import (
    import_module,
)
from .abc import (
    Config,
)

if TYPE_CHECKING:
    from ..injections import (
        InjectableMixin,
    )


class ConfigV2(Config):
    """Config V2 class."""

    @property
    def _version(self) -> int:
        return 2

    def _get_name(self) -> str:
        return self.get_by_key("name")

    def _get_injections(self) -> list[Union[InjectableMixin, type[InjectableMixin]]]:
        from ..builders import (
            BuildableMixin,
        )
        from ..injections import (
            InjectableMixin,
        )

        partial_ans = list()

        with suppress(MinosConfigException):
            partial_ans.append(self._get_pools().get("factory"))

        with suppress(MinosConfigException):
            partial_ans.append(self._get_interfaces().get("http").get("connector"))

        with suppress(MinosConfigException):
            partial_ans.append(self._get_interfaces().get("broker").get("publisher"))

        with suppress(MinosConfigException):
            partial_ans.append(self._get_interfaces().get("broker").get("subscriber"))

        with suppress(MinosConfigException):
            partial_ans.extend(self._get_aggregate().get("repositories", dict()).values())

        with suppress(MinosConfigException):
            partial_ans.append(self._get_discovery().get("connector"))

        with suppress(MinosConfigException):
            partial_ans.append(self._get_saga().get("manager"))

        with suppress(MinosConfigException):
            injections = self.get_by_key("injections")
            partial_ans.extend(import_module(injection) for injection in injections)

        ans = list()
        for type_ in partial_ans:
            if type_ is None:
                continue

            if isinstance(type_, dict):
                type_ = type_["client"]

            if (
                not issubclass(type_, InjectableMixin)
                and issubclass(type_, BuildableMixin)
                and isinstance((builder_type := type_.get_builder()), InjectableMixin)
            ):
                type_ = builder_type
            elif not issubclass(type_, InjectableMixin):
                raise MinosConfigException(f"{type_!r} must be subclass of {InjectableMixin!r}.")

            ans.append(type_)

        # noinspection PyTypeChecker
        return ans

    def _get_databases(self) -> dict[str, dict[str, Any]]:
        data = deepcopy(self.get_by_key("databases"))
        return data

    def _get_interfaces(self) -> dict[str, dict[str, Any]]:
        data = deepcopy(self.get_by_key("interfaces"))

        if "http" in data:
            data["http"] = self._parse_http_interface(data["http"])
        if "broker" in data:
            data["broker"] = self._parse_broker_interface(data["broker"])
        if "periodic" in data:
            data["periodic"] = self._parse_periodic_interface(data["periodic"])

        return data

    @staticmethod
    def _parse_http_interface(data: dict[str, Any]) -> dict[str, Any]:
        if "port" in data:
            data["port"] = import_module(data["port"])
        if "connector" in data:
            data["connector"]["client"] = import_module(data["connector"]["client"])
        return data

    @staticmethod
    def _parse_broker_interface(data: dict[str, Any]) -> dict[str, Any]:
        if "port" in data:
            data["port"] = import_module(data["port"])

        if "publisher" in data:
            data["publisher"]["client"] = import_module(data["publisher"]["client"])
            if "queue" in data["publisher"]:
                data["publisher"]["queue"] = import_module(data["publisher"]["queue"])

        if "subscriber" in data:
            data["subscriber"]["client"] = import_module(data["subscriber"]["client"])
            if "queue" in data["subscriber"]:
                data["subscriber"]["queue"] = import_module(data["subscriber"]["queue"])
            if "validator" in data["subscriber"]:
                data["subscriber"]["validator"] = import_module(data["subscriber"]["validator"])

        return data

    @staticmethod
    def _parse_periodic_interface(data: dict[str, Any]) -> dict[str, Any]:
        if "port" in data:
            data["port"] = import_module(data["port"])

        return data

    def _get_pools(self) -> dict[str, type]:
        from ..pools import (
            PoolFactory,
        )

        factory = PoolFactory

        try:
            types = self.get_by_key("pools")
        except MinosConfigException:
            types = dict()

        types = {name: import_module(classname) for name, classname in types.items()}

        return {
            "factory": factory,
            "types": types,
        }

    def _get_routers(self) -> list[type]:
        try:
            data = self.get_by_key("routers")
        except MinosConfigException:
            data = list()

        data = [import_module(classname) for classname in data]

        return data

    def _get_middleware(self) -> list[type]:
        try:
            data = self.get_by_key("middleware")
        except MinosConfigException:
            data = list()

        data = [import_module(classname) for classname in data]

        return data

    def _get_services(self) -> list[type]:
        try:
            services = self.get_by_key("services")
        except MinosConfigException:
            services = list()

        services = [import_module(classname) for classname in services]

        return services

    def _get_discovery(self) -> dict[str, Any]:
        data = deepcopy(self.get_by_key("discovery"))
        data["client"] = import_module(data["client"])
        data["connector"] = import_module(data["connector"])
        return data

    def _get_aggregate(self) -> dict[str, Any]:
        data = deepcopy(self.get_by_key("aggregate"))

        data["entities"] = [import_module(classname) for classname in data.get("entities", list())]
        data["repositories"] = {name: import_module(value) for name, value in data.get("repositories", dict()).items()}
        return data

    def _get_saga(self) -> dict[str, Any]:
        data = deepcopy(self.get_by_key("saga"))
        data["manager"] = import_module(data["manager"])
        return data

    def _to_parameterized_variable(self, key: str) -> str:
        return key.replace(".", "_").lower()

    def _to_environment_variable(self, key: str) -> str:
        return f"MINOS_{key.replace('.', '_').upper()}"
