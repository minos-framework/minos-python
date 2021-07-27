"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from __future__ import (
    annotations,
)

import abc
import collections
import os
from pathlib import (
    Path,
)
from typing import (
    Any,
    NoReturn,
    Union,
)

import yaml

from ..exceptions import (
    MinosConfigException,
)

BROKER = collections.namedtuple("Broker", "host port queue")
QUEUE = collections.namedtuple("Queue", "database user password host port records retry")
SERVICE = collections.namedtuple("Service", "name injections services")
CONTROLLER = collections.namedtuple("Controller", "name controller action")
STORAGE = collections.namedtuple("Storage", "path")

EVENTS = collections.namedtuple("Events", "service")
COMMANDS = collections.namedtuple("Commands", "service")
QUERIES = collections.namedtuple("Queries", "service")
SAGA = collections.namedtuple("Saga", "items storage")
REST = collections.namedtuple("Rest", "host port")
REPOSITORY = collections.namedtuple("Repository", "database user password host port")
SNAPSHOT = collections.namedtuple("Snapshot", "database user password host port")
DISCOVERY = collections.namedtuple("Discovery", "host port")

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
    "events.service": "MINOS_EVENTS_SERVICE",
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
    "discovery.host": "MINOS_DISCOVERY_HOST",
    "discovery.port": "MINOS_DISCOVERY_PORT",
}

_PARAMETERIZED_MAPPER = {
    "service.name": "service_name",
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
    "events.service": "events_service",
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
    "discovery.host": "minos_discovery_host",
    "discovery.port": "minos_discovery_port",
}


class MinosConfigAbstract(abc.ABC):
    """Minos abstract config class."""

    __slots__ = "_services", "_path"

    def __init__(self, path: Union[Path, str]):
        if isinstance(path, str):
            path = Path(path)
        self._services = {}
        self._path = path
        self._load(path)

    @abc.abstractmethod
    def _load(self, path: Path) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def _get(self, key: str, **kwargs: Any):
        raise NotImplementedError


class MinosConfig(MinosConfigAbstract):
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

    __slots__ = ("_data", "_with_environment", "_parameterized")

    def __init__(self, path: Path | str, with_environment: bool = True, **kwargs):
        super().__init__(path)
        self._with_environment = with_environment
        self._parameterized = kwargs

    def _load(self, path: Path) -> NoReturn:
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

        return _fn(key, self._data)

    @property
    def service(self) -> SERVICE:
        """Get the service config.

        :return: A ``SERVICE`` NamedTuple instance.
        """
        return SERVICE(
            name=self._get("service.name"), injections=self._service_injections, services=self._service_services
        )

    @property
    def _service_injections(self) -> dict[str, str]:
        try:
            return self._get("service.injections")
        except KeyError:
            return dict()

    @property
    def _service_services(self) -> list[str]:
        try:
            return self._get("service.services")
        except KeyError:
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
    def events(self) -> EVENTS:
        """Get the events config.

        :return: A ``EVENTS`` NamedTuple instance.
        """
        service = self._get("events.service")
        return EVENTS(service=service)

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
    def commands(self) -> COMMANDS:
        """Get the commands config.

        :return: A ``COMMAND`` NamedTuple instance.
        """
        service = self._get("commands.service")
        return COMMANDS(service=service)

    @property
    def queries(self) -> QUERIES:
        """Get the queries config.

        :return: A ``QUERIES`` NamedTuple instance.
        """
        return QUERIES(service=self._get("queries.service"))

    @property
    def saga(self) -> SAGA:
        """Get the sagas config.

        :return: A ``SAGAS`` NamedTuple instance.
        """

        sagas = self._saga_items
        storage = self._saga_storage
        return SAGA(items=sagas, storage=storage)

    @property
    def _saga_storage(self) -> STORAGE:
        raw = self._get("saga.storage.path")
        path = Path(raw) if raw.startswith("/") else self._path.parent / raw

        queue = STORAGE(path=path)
        return queue

    @property
    def _saga_items(self) -> list[CONTROLLER]:
        info = self._get("saga.items")
        sagas = [self._sagas_items_entry(saga) for saga in info]
        return sagas

    @staticmethod
    def _sagas_items_entry(saga: dict[str, Any]) -> CONTROLLER:
        return CONTROLLER(name=saga["name"], controller=saga["controller"], action=saga["action"])

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
        host = self._get("discovery.host")
        port = self._get("discovery.port")
        return DISCOVERY(host=host, port=port)
