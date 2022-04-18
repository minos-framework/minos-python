from __future__ import (
    annotations,
)

from contextlib import (
    suppress,
)
from typing import (
    TYPE_CHECKING,
    Any,
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


class ConfigV1(Config):
    """
    A Minos configuration provides information on the connection points available at that service.
    It consists of the following parts:

    - Service meta-information (such as name, or version).
    - REST Service endpoints available.
    - Repository database connection for event sourcing.
    - Snapshot database connection.
    - Events it publishes/consumes from de given Kafka service.
    - Commands it reacts to from other microservices.
    - Sagas it takes part on.
    """

    _ENVIRONMENT_MAPPER = {
        "service.name": "MINOS_SERVICE_NAME",
        "rest.host": "MINOS_REST_HOST",
        "rest.port": "MINOS_REST_PORT",
        "broker.host": "MINOS_BROKER_HOST",
        "broker.port": "MINOS_BROKER_PORT",
        "broker.queue.host": "MINOS_BROKER_QUEUE_HOST",
        "broker.queue.port": "MINOS_BROKER_QUEUE_PORT",
        "broker.queue.database": "MINOS_BROKER_QUEUE_DATABASE",
        "broker.queue.user": "MINOS_BROKER_QUEUE_USER",
        "broker.queue.password": "MINOS_BROKER_QUEUE_PASSWORD",
        "commands.service": "MINOS_COMMANDS_SERVICE",
        "queries.service": "MINOS_QUERIES_SERVICE",
        "repository.host": "MINOS_REPOSITORY_HOST",
        "repository.port": "MINOS_REPOSITORY_PORT",
        "repository.database": "MINOS_REPOSITORY_DATABASE",
        "repository.user": "MINOS_REPOSITORY_USER",
        "repository.password": "MINOS_REPOSITORY_PASSWORD",
        "snapshot.host": "MINOS_SNAPSHOT_HOST",
        "snapshot.port": "MINOS_SNAPSHOT_PORT",
        "snapshot.database": "MINOS_SNAPSHOT_DATABASE",
        "snapshot.user": "MINOS_SNAPSHOT_USER",
        "snapshot.password": "MINOS_SNAPSHOT_PASSWORD",
        "discovery.client": "MINOS_DISCOVERY_CLIENT",
        "discovery.host": "MINOS_DISCOVERY_HOST",
        "discovery.port": "MINOS_DISCOVERY_PORT",
    }

    _PARAMETERIZED_MAPPER = {
        "service.name": "service_name",
        "service.injections": "service_injections",
        "rest.host": "rest_host",
        "rest.port": "rest_port",
        "broker.host": "broker_host",
        "broker.port": "broker_port",
        "broker.queue.host": "broker_queue_host",
        "broker.queue.port": "broker_queue_port",
        "broker.queue.database": "broker_queue_database",
        "broker.queue.user": "broker_queue_user",
        "broker.queue.password": "broker_queue_password",
        "commands.service": "commands_service",
        "queries.service": "queries_service",
        "saga.broker": "saga_broker",
        "saga.port": "saga_port",
        "repository.host": "repository_host",
        "repository.port": "repository_port",
        "repository.database": "repository_database",
        "repository.user": "repository_user",
        "repository.password": "repository_password",
        "snapshot.host": "snapshot_host",
        "snapshot.port": "snapshot_port",
        "snapshot.database": "snapshot_database",
        "snapshot.user": "snapshot_user",
        "snapshot.password": "snapshot_password",
        "discovery.client": "minos_discovery_client",
        "discovery.host": "minos_discovery_host",
        "discovery.port": "minos_discovery_port",
    }

    @property
    def _version(self) -> int:
        return 1

    def _get_name(self) -> str:
        return self.get_by_key("service.name")

    def _get_aggregate(self) -> dict[str, Any]:
        return {
            "entities": [self.get_type_by_key("service.aggregate")],
            "repositories": dict(),
        }

    def _get_saga(self) -> dict[str, Any]:
        try:
            saga = self.get_by_key("saga")
        except MinosConfigException:
            saga = dict()
        saga.pop("storage", None)
        return saga

    def _get_injections(self) -> list[type[InjectableMixin]]:
        from ..pools import (
            Pool,
            PoolFactory,
        )

        injections = self._get_raw_injections()
        injections = [
            injection
            for injection in injections
            if not (issubclass(injection, Pool) or issubclass(injection, PoolFactory))
        ]
        with suppress(MinosConfigException):
            pool_factory = self._get_pools().get("factory")
            if pool_factory is not None:
                # noinspection PyTypeChecker
                injections.insert(0, pool_factory)

        # noinspection PyTypeChecker
        return injections

    def _get_raw_injections(self) -> list[type[InjectableMixin]]:
        try:
            injections = self.get_by_key("service.injections")
            if isinstance(injections, dict):
                injections = list(injections.values())
        except MinosConfigException:
            injections = list()

        injections = [import_module(classname) for classname in injections]

        # noinspection PyTypeChecker
        return injections

    def _get_interfaces(self) -> dict[str, dict[str, Any]]:
        interfaces = dict()

        with suppress(MinosConfigException):
            interfaces["http"] = self._get_interface_http()

        with suppress(MinosConfigException):
            interfaces["broker"] = self._get_interface_broker()

        with suppress(MinosConfigException):
            interfaces["periodic"] = self._get_interface_periodic()

        return interfaces

    def _get_interface_http(self) -> dict[str, Any]:
        try:
            port = next(
                port
                for port in self.get_by_key("service.services")
                if ("http" in port.lower() or "rest" in port.lower())
            )
        except Exception as exc:
            raise MinosConfigException(f"The 'http' interface is not available: {exc!r}")

        try:
            connector = self.get_by_key("rest")
        except MinosConfigException:
            connector = dict()

        return {
            "port": import_module(port),
            "connector": connector,
        }

    def _get_interface_broker(self) -> dict[str, Any]:
        try:
            port = next(port for port in self.get_by_key("service.services") if "broker" in port.lower())
        except Exception as exc:
            raise MinosConfigException(f"The 'broker' interface is not available: {exc!r}")

        try:
            common = self.get_by_key("broker")
        except MinosConfigException:
            common = dict()

        try:
            common["queue"] = self.get_by_key("broker.queue")
            common["queue"].pop("database", None)
            common["queue"].pop("port", None)
            common["queue"].pop("host", None)
            common["queue"].pop("port", None)
            common["queue"].pop("user", None)
            common["queue"].pop("password", None)
        except MinosConfigException:
            common["queue"] = dict()

        return {
            "port": import_module(port),
            "publisher": dict(),
            "subscriber": dict(),
            "common": common,
        }

    def _get_interface_periodic(self):
        try:
            port = next(port for port in self.get_by_key("service.services") if "periodic" in port.lower())
        except Exception as exc:
            raise MinosConfigException(f"The 'periodic' interface is not available: {exc!r}")

        return {
            "port": import_module(port),
        }

    def _get_services(self) -> list[type]:
        try:
            services = self.get_by_key("services")
        except MinosConfigException:
            services = list()

        services = [import_module(classname) for classname in services]

        return services

    def _get_pools(self) -> dict[str, type]:
        from ..pools import (
            Pool,
            PoolFactory,
        )

        factory = next(
            (injection for injection in self._get_raw_injections() if issubclass(injection, PoolFactory)), PoolFactory
        )
        injections = [injection for injection in self._get_raw_injections() if issubclass(injection, Pool)]
        if not len(injections):
            return dict()

        types = dict()
        for injection in injections:
            if "lock" in injection.__name__.lower():
                types["lock"] = injection
            elif "database" in injection.__name__.lower():
                types["database"] = injection
            elif "broker" in injection.__name__.lower():
                types["broker"] = injection

        return {
            "factory": factory,
            "types": types,
        }

    def _get_routers(self) -> list[type]:
        try:
            routers = self.get_by_key("routers")
        except MinosConfigException:
            routers = list()

        routers = [import_module(classname) for classname in routers]

        return routers

    def _get_middleware(self) -> list[type]:
        try:
            middleware = self.get_by_key("middleware")
        except MinosConfigException:
            middleware = list()

        middleware = [import_module(classname) for classname in middleware]

        return middleware

    def _get_databases(self) -> dict[str, dict[str, Any]]:
        databases = dict()

        with suppress(MinosConfigException):
            databases["broker"] = self._get_database_broker()

        with suppress(MinosConfigException):
            databases["event"] = self._get_database_event()

        with suppress(MinosConfigException):
            databases["snapshot"] = self._get_database_snapshot()

        with suppress(MinosConfigException):
            databases["saga"] = self._get_database_saga()

        with suppress(MinosConfigException):
            databases["query"] = self._get_database_query()

        with suppress(MinosConfigException):
            databases["default"] = self._get_database_event()

        return databases

    def _get_database_broker(self):
        return self._get_database_by_name("broker.queue")

    def _get_database_saga(self) -> dict[str, Any]:
        raw = self._get_database_by_name("saga.storage")
        return raw

    def _get_database_event(self) -> dict[str, Any]:
        return self._get_database_by_name("repository")

    def _get_database_query(self) -> dict[str, Any]:
        return self._get_database_by_name("query_repository")

    def _get_database_snapshot(self) -> dict[str, Any]:
        return self._get_database_by_name("snapshot")

    def _get_database_by_name(self, prefix: str):
        data = self.get_by_key(prefix)
        data.pop("records", None)
        data.pop("retry", None)
        if "client" in data:
            data["client"] = import_module(data["client"])
        return data

    def _get_discovery(self) -> dict[str, Any]:
        data = self.get_by_key("discovery")
        data["client"] = self.get_type_by_key("discovery.client")
        return data

    def _to_parameterized_variable(self, key: str) -> str:
        return self._PARAMETERIZED_MAPPER[key]

    def _to_environment_variable(self, key: str) -> str:
        return self._ENVIRONMENT_MAPPER[key]
