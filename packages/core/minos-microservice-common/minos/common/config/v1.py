from __future__ import (
    annotations,
)

from pathlib import (
    Path,
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
    from ..ports import (
        Port,
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

    def _get_name(self) -> str:
        return self.get_by_key("service.name")

    def _get_aggregate(self) -> dict[str, Any]:
        return {
            "entities": [self.get_cls_by_key("service.aggregate")],
        }

    def _get_saga(self) -> dict[str, Any]:
        try:
            saga = self.get_by_key("saga")
        except MinosConfigException:
            saga = dict()
        saga.pop("storage", None)
        return saga

    def _get_injections(self) -> list[type[InjectableMixin]]:
        try:
            injections = self.get_by_key("service.injections")
            if isinstance(injections, dict):
                injections = list(injections.values())
        except MinosConfigException:
            injections = list()

        injections = [import_module(classname) for classname in injections]

        # noinspection PyTypeChecker
        return injections

    def _get_ports(self) -> list[type[Port]]:
        try:
            ports = self.get_by_key("service.services")
        except MinosConfigException:
            ports = list()

        ports = [import_module(classname) for classname in ports]

        # noinspection PyTypeChecker
        return ports

    def _get_interface(self, name: str):
        if name == "http":
            return self._get_interface_http()

        if name == "broker":
            return self._get_interface_broker()

        raise ValueError(f"There is not a {name!r} interface.")

    def _get_interface_http(self) -> dict[str, Any]:
        return {
            "connector": {
                "host": self.get_by_key("rest.host"),
                "port": int(self.get_by_key("rest.port")),
            },
        }

    def _get_interface_broker(self) -> dict[str, Any]:
        return {
            "publisher": dict(),
            "subscriber": dict(),
            "common": {
                "host": self.get_by_key("broker.host"),
                "port": int(self.get_by_key("broker.port")),
                "queue": {
                    "records": int(self.get_by_key("broker.queue.records")),
                    "retry": int(self.get_by_key("broker.queue.retry")),
                },
            },
        }

    def _get_services(self) -> list[type]:
        try:
            services = self.get_by_key("services")
        except MinosConfigException:
            services = list()

        services = [import_module(classname) for classname in services]

        return services

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

    def _get_database(self, name: str) -> dict[str, Any]:
        if name == "broker":
            return self._get_database_broker()

        if name == "event":
            return self._get_database_event()

        if name == "snapshot":
            return self._get_database_snapshot()

        if name == "saga":
            return self._get_database_saga()

        if name == "query":
            return self._get_database_query()

        return self._get_database_event()

    def _get_database_broker(self):
        return self._get_database_by_name("broker.queue")

    def _get_database_saga(self) -> dict[str, Any]:
        raw = self.get_by_key("saga.storage.path")
        return {
            "path": Path(raw) if raw.startswith("/") else self.file_path.parent / raw,
        }

    def _get_database_event(self) -> dict[str, Any]:
        return self._get_database_by_name("repository")

    def _get_database_query(self) -> dict[str, Any]:
        return self._get_database_by_name("query_repository")

    def _get_database_snapshot(self) -> dict[str, Any]:
        return self._get_database_by_name("snapshot")

    def _get_database_by_name(self, prefix: str):
        return {
            "database": self.get_by_key(f"{prefix}.database"),
            "user": self.get_by_key(f"{prefix}.user"),
            "password": self.get_by_key(f"{prefix}.password"),
            "host": self.get_by_key(f"{prefix}.host"),
            "port": int(self.get_by_key(f"{prefix}.port")),
        }

    def _get_discovery(self) -> dict[str, Any]:
        return {
            "client": self.get_cls_by_key("discovery.client"),
            "host": self.get_by_key("discovery.host"),
            "port": self.get_by_key("discovery.port"),
        }
