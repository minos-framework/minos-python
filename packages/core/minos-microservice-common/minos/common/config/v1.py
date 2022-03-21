from __future__ import (
    annotations,
)

import os
from collections import (
    namedtuple,
)
from pathlib import (
    Path,
)
from typing import (
    Any,
)

import yaml

from ..exceptions import (
    MinosConfigException,
)
from .abc import (
    Config,
)

BROKER = namedtuple("Broker", "host port queue")
QUEUE = namedtuple("Queue", "database user password host port records retry")
SERVICE = namedtuple("Service", "name aggregate injections services")
STORAGE = namedtuple("Storage", "path")

SAGA = namedtuple("Saga", "storage")
REST = namedtuple("Rest", "host port")
REPOSITORY = namedtuple("Repository", "database user password host port")
SNAPSHOT = namedtuple("Snapshot", "database user password host port")
DISCOVERY = namedtuple("Discovery", "client host port")

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

    __slots__ = ("_path", "_data", "_with_environment", "_parameterized")

    def __init__(self, path: Path | str, with_environment: bool = True, **kwargs):
        super().__init__()
        if isinstance(path, str):
            path = Path(path)

        self._path = path
        self._load(path)

        self._with_environment = with_environment
        self._parameterized = kwargs

    def _get_database(self, name: str) -> dict[str, Any]:
        if name == "broker":
            # noinspection PyProtectedMember
            return self.broker.queue._asdict()

        if name == "saga":
            # noinspection PyProtectedMember
            return self.saga.storage._asdict()

        if name == "query":
            return self.query_repository._asdict()

        return self.repository._asdict()

    def _get_injections(self) -> list[str]:
        return self._service_injections

    def _get_ports(self) -> list[str]:
        return self._service_services

    def _get_name(self) -> str:
        return self.service.name

    def _get_interface(self, name: str):
        if name == "http":
            return {"connector": self.rest._asdict()}

        if name == "broker":
            return {"publisher": self.broker._asdict(), "subscriber": self.broker._asdict()}

        if name == "periodic":
            return dict()

        raise ValueError("TODO")

    def _get_routers(self) -> list[str]:
        return self.routers

    def _get_middleware(self) -> list[str]:
        return self.middleware

    def _get_discovery(self) -> dict[str, Any]:
        return self.discovery._asdict()

    def _get_services(self) -> list[str]:
        return self.services

    def _get_aggregate(self) -> dict[str, Any]:
        return {"root_entity": self.service.aggregate}

    def _load(self, path: Path) -> None:
        if not path.exists():
            raise MinosConfigException(f"Check if this path: {path} is correct")

        with path.open() as file:
            self._data = yaml.load(file, Loader=yaml.FullLoader)

    def _get(self, key: str, **kwargs: Any) -> Any:
        if key in _PARAMETERIZED_MAPPER and _PARAMETERIZED_MAPPER[key] in self._parameterized:
            return self._parameterized[_PARAMETERIZED_MAPPER[key]]

        if self._with_environment and key in _ENVIRONMENT_MAPPER and _ENVIRONMENT_MAPPER[key] in os.environ:
            return os.environ[_ENVIRONMENT_MAPPER[key]]

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

    @property
    def service(self) -> SERVICE:
        """Get the service config.

        :return: A ``SERVICE`` NamedTuple instance.
        """
        return SERVICE(
            name=self._get("service.name"),
            aggregate=self._get("service.aggregate"),
            injections=self._service_injections,
            services=self._service_services,
        )

    @property
    def _service_injections(self) -> list[str]:
        try:
            injections = self._get("service.injections")
            if isinstance(injections, dict):
                injections = list(injections.values())
            return injections
        except MinosConfigException:
            return list()

    @property
    def _service_services(self) -> list[str]:
        try:
            return self._get("service.services")
        except MinosConfigException:
            return list()

    @property
    def rest(self) -> REST:
        """Get the rest config.

        :return: A ``REST`` NamedTuple instance.
        """
        return REST(host=self._get("rest.host"), port=int(self._get("rest.port")))

    @property
    def broker(self) -> BROKER:
        """Get the events config.

        :return: A ``EVENTS`` NamedTuple instance.
        """
        queue = self._broker_queue
        return BROKER(host=self._get("broker.host"), port=self._get("broker.port"), queue=queue)

    @property
    def _broker_queue(self) -> QUEUE:
        return QUEUE(
            database=self._get("broker.queue.database"),
            user=self._get("broker.queue.user"),
            password=self._get("broker.queue.password"),
            host=self._get("broker.queue.host"),
            port=int(self._get("broker.queue.port")),
            records=int(self._get("broker.queue.records")),
            retry=int(self._get("broker.queue.retry")),
        )

    @property
    def services(self) -> list[str]:
        """Get the commands config.

        :return: A list containing the service class names as string values..
        """
        try:
            return self._get("services")
        except MinosConfigException:
            return list()

    @property
    def routers(self) -> list[str]:
        """Get the routers.

        :return: A list containing the router class names as string values.
        """
        try:
            return self._get("routers")
        except MinosConfigException:
            return list()

    @property
    def middleware(self) -> list[str]:
        """Get the commands config.

        :return: A list containing the service class names as string values..
        """
        try:
            return self._get("middleware")
        except MinosConfigException:
            return list()

    @property
    def saga(self) -> SAGA:
        """Get the sagas config.

        :return: A ``SAGAS`` NamedTuple instance.
        """

        storage = self._saga_storage
        return SAGA(storage=storage)

    @property
    def _saga_storage(self) -> STORAGE:
        raw = self._get("saga.storage.path")
        path = Path(raw) if raw.startswith("/") else self._path.parent / raw

        queue = STORAGE(path=path)
        return queue

    @property
    def repository(self) -> REPOSITORY:
        """Get the repository config.

        :return: A ``REPOSITORY`` NamedTuple instance.
        """
        return REPOSITORY(
            database=self._get("repository.database"),
            user=self._get("repository.user"),
            password=self._get("repository.password"),
            host=self._get("repository.host"),
            port=int(self._get("repository.port")),
        )

    @property
    def query_repository(self) -> REPOSITORY:
        """Get the query_repository config.

        :return: A ``QUERY_REPOSITORY`` NamedTuple instance.
        """
        return REPOSITORY(
            database=self._get("query_repository.database"),
            user=self._get("query_repository.user"),
            password=self._get("query_repository.password"),
            host=self._get("query_repository.host"),
            port=int(self._get("query_repository.port")),
        )

    @property
    def snapshot(self) -> SNAPSHOT:
        """Get the snapshot config.

        :return: A ``SNAPSHOT`` NamedTuple instance.
        """
        return SNAPSHOT(
            database=self._get("snapshot.database"),
            user=self._get("snapshot.user"),
            password=self._get("snapshot.password"),
            host=self._get("snapshot.host"),
            port=int(self._get("snapshot.port")),
        )

    @property
    def discovery(self) -> DISCOVERY:
        """Get the sagas config.

        :return: A ``DISCOVERY`` NamedTuple instance.
        """
        client = self._get("discovery.client")
        host = self._get("discovery.host")
        port = self._get("discovery.port")
        return DISCOVERY(client=client, host=host, port=port)
