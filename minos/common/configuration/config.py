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
import typing as t
from pathlib import (
    Path,
)

import yaml

from ..exceptions import (
    MinosConfigDefaultAlreadySetException,
    MinosConfigException,
)

BROKER = collections.namedtuple("Broker", "host port")
QUEUE = collections.namedtuple("Queue", "database user password host port records retry")
ENDPOINT = collections.namedtuple("Endpoint", "name route method controller action")
SERVICE = collections.namedtuple("Service", "name")
CONTROLLER = collections.namedtuple("Controller", "name controller action")
STORAGE = collections.namedtuple("Storage", "path")

EVENTS = collections.namedtuple("Events", "broker items queue")
COMMANDS = collections.namedtuple("Commands", "broker items queue")
SAGA = collections.namedtuple("Saga", "items queue storage broker")
REST = collections.namedtuple("Rest", "broker endpoints")
REPOSITORY = collections.namedtuple("Repository", "database user password host port")
SNAPSHOT = collections.namedtuple("Snapshot", "database user password host port")

_ENVIRONMENT_MAPPER = {
    "commands.queue.host": "MINOS_COMMANDS_QUEUE_HOST",
    "commands.queue.port": "MINOS_COMMANDS_QUEUE_PORT",
    "commands.queue.database": "MINOS_COMMANDS_QUEUE_DATABASE",
    "commands.queue.user": "MINOS_COMMANDS_QUEUE_USER",
    "commands.queue.password": "MINOS_COMMANDS_QUEUE_PASSWORD",
    "commands.broker": "MINOS_COMMANDS_BROKER",
    "commands.port": "MINOS_COMMANDS_PORT",
    "saga.broker": "MINOS_SAGA_BROKER",
    "saga.port": "MINOS_SAGA_PORT",
    "saga.queue.host": "MINOS_SAGA_QUEUE_HOST",
    "saga.queue.port": "MINOS_SAGA_QUEUE_PORT",
    "saga.queue.database": "MINOS_SAGA_QUEUE_DATABASE",
    "saga.queue.user": "MINOS_SAGA_QUEUE_USER",
    "saga.queue.password": "MINOS_SAGA_QUEUE_PASSWORD",
    "events.queue.host": "MINOS_EVENTS_QUEUE_HOST",
    "events.queue.port": "MINOS_EVENTS_QUEUE_PORT",
    "events.queue.database": "MINOS_EVENTS_QUEUE_DATABASE",
    "events.queue.user": "MINOS_EVENTS_QUEUE_USER",
    "events.queue.password": "MINOS_EVENTS_QUEUE_PASSWORD",
    "events.broker": "MINOS_EVENTS_BROKER",
    "events.port": "MINOS_EVENTS_PORT",
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
}

_PARAMETERIZED_MAPPER = {
    "commands.queue.host": "commands_queue_host",
    "commands.queue.port": "commands_queue_port",
    "commands.queue.database": "commands_queue_database",
    "commands.queue.user": "commands_queue_user",
    "commands.queue.password": "commands_queue_password",
    "commands.broker": "commands_broker",
    "commands.port": "commands_port",
    "saga.broker": "saga_broker",
    "saga.port": "saga_port",
    "saga.queue.host": "saga_queue_host",
    "saga.queue.port": "saga_queue_port",
    "saga.queue.database": "saga_queue_database",
    "saga.queue.user": "saga_queue_user",
    "saga.queue.password": "saga_queue_password",
    "events.queue.host": "events_queue_host",
    "events.queue.port": "events_queue_port",
    "events.queue.database": "events_queue_database",
    "events.queue.user": "events_queue_user",
    "events.queue.password": "events_queue_password",
    "events.broker": "events_broker",
    "events.port": "events_port",
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
}

_default: t.Optional[MinosConfigAbstract] = None


class MinosConfigAbstract(abc.ABC):
    """Minos abstract config class."""

    __slots__ = "_services", "_path"

    def __init__(self, path: t.Union[Path, str]):
        if isinstance(path, str):
            path = Path(path)
        self._services = {}
        self._path = path
        self._load(path)

    @abc.abstractmethod
    def _load(self, path: Path) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def _get(self, key: str, **kwargs: t.Any):
        raise NotImplementedError

    def __enter__(self) -> MinosConfigAbstract:
        self.set_default(self)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> t.NoReturn:
        self.unset_default()

    @staticmethod
    def set_default(value: MinosConfigAbstract) -> t.NoReturn:
        """Set default config.

        :param value: Default config.
        :return: This method does not return anything.
        """
        if MinosConfigAbstract.get_default() is not None:
            raise MinosConfigDefaultAlreadySetException("There is already another config set as default.")
        global _default
        _default = value

    @classmethod
    def get_default(cls) -> MinosConfigAbstract:
        """Get default config.

        :return: A ``MinosConfigAbstract`` instance.
        """
        global _default
        return _default

    @staticmethod
    def unset_default() -> t.NoReturn:
        """Unset the default config.

        :return: This method does not return anything.
        """
        global _default
        _default = None


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

    def _load(self, path: Path) -> t.NoReturn:
        if not path.exists():
            raise MinosConfigException(f"Check if this path: {path} is correct")

        with path.open() as file:
            self._data = yaml.load(file, Loader=yaml.FullLoader)

    def _get(self, key: str, **kwargs: t.Any) -> t.Any:
        if key in _PARAMETERIZED_MAPPER and _PARAMETERIZED_MAPPER[key] in self._parameterized:
            return self._parameterized[_PARAMETERIZED_MAPPER[key]]

        if self._with_environment and key in _ENVIRONMENT_MAPPER and _ENVIRONMENT_MAPPER[key] in os.environ:
            return os.environ[_ENVIRONMENT_MAPPER[key]]

        def _fn(k: str, data: dict[str, t.Any]) -> t.Any:
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
        return SERVICE(name=self._get("service.name"))

    @property
    def rest(self) -> REST:
        """Get the rest config.

        :return: A ``REST`` NamedTuple instance.
        """
        broker = self._rest_broker
        endpoints = self._rest_endpoints
        return REST(broker=broker, endpoints=endpoints)

    @property
    def _rest_broker(self):
        broker = BROKER(host=self._get("rest.host"), port=int(self._get("rest.port")))
        return broker

    @property
    def _rest_endpoints(self) -> list[ENDPOINT]:
        info = self._get("rest.endpoints")
        endpoints = [self._rest_endpoints_entry(endpoint) for endpoint in info]
        return endpoints

    @staticmethod
    def _rest_endpoints_entry(endpoint: dict[str, t.Any]) -> ENDPOINT:
        return ENDPOINT(
            name=endpoint["name"],
            route=endpoint["route"],
            method=endpoint["method"].upper(),
            controller=endpoint["controller"],
            action=endpoint["action"],
        )

    @property
    def events(self) -> EVENTS:
        """Get the events config.

        :return: A ``EVENTS`` NamedTuple instance.
        """
        broker = self._events_broker
        queue = self._events_queue
        events = self._events_items
        return EVENTS(broker=broker, items=events, queue=queue)

    @property
    def _events_broker(self) -> BROKER:
        return BROKER(host=self._get("events.broker"), port=int(self._get("events.port")))

    @property
    def _events_queue(self) -> QUEUE:
        return QUEUE(
            database=self._get("events.queue.database"),
            user=self._get("events.queue.user"),
            password=self._get("events.queue.password"),
            host=self._get("events.queue.host"),
            port=int(self._get("events.queue.port")),
            records=int(self._get("events.queue.records")),
            retry=int(self._get("events.queue.retry")),
        )

    @property
    def _events_items(self) -> list[CONTROLLER]:
        info = self._get("events.items")
        events = [self._events_items_entry(event) for event in info]
        return events

    @staticmethod
    def _events_items_entry(event: dict[str, t.Any]) -> CONTROLLER:
        return CONTROLLER(name=event["name"], controller=event["controller"], action=event["action"])

    @property
    def commands(self) -> COMMANDS:
        """Get the commands config.

        :return: A ``COMMAND`` NamedTuple instance.
        """
        broker = self._commands_broker
        queue = self._commands_queue
        commands = self._commands_items
        return COMMANDS(broker=broker, items=commands, queue=queue)

    @property
    def saga(self) -> SAGA:
        """Get the sagas config.

        :return: A ``SAGAS`` NamedTuple instance.
        """
        queue = self._sagas_queue
        sagas = self._saga_items
        storage = self._saga_storage
        broker = self._saga_broker
        return SAGA(items=sagas, queue=queue, storage=storage, broker=broker)

    @property
    def _commands_broker(self) -> BROKER:
        broker = BROKER(host=self._get("commands.broker"), port=int(self._get("commands.port")))
        return broker

    @property
    def _commands_queue(self) -> QUEUE:
        queue = QUEUE(
            database=self._get("commands.queue.database"),
            user=self._get("commands.queue.user"),
            password=self._get("commands.queue.password"),
            host=self._get("commands.queue.host"),
            port=int(self._get("commands.queue.port")),
            records=int(self._get("commands.queue.records")),
            retry=int(self._get("commands.queue.retry")),
        )
        return queue

    @property
    def _commands_items(self) -> list[CONTROLLER]:
        info = self._get("commands.items")
        commands = [self._commands_items_entry(command) for command in info]
        return commands

    @staticmethod
    def _commands_items_entry(command: dict[str, t.Any]) -> CONTROLLER:
        return CONTROLLER(name=command["name"], controller=command["controller"], action=command["action"])

    @property
    def _saga_storage(self) -> STORAGE:
        raw = self._get("saga.storage.path")
        path = Path(raw) if raw.startswith("/") else self._path.parent / raw

        queue = STORAGE(path=path)
        return queue

    @property
    def _saga_broker(self) -> BROKER:
        return BROKER(host=self._get("saga.broker"), port=int(self._get("saga.port")))

    @property
    def _sagas_queue(self) -> QUEUE:
        queue = QUEUE(
            database=self._get("saga.queue.database"),
            user=self._get("saga.queue.user"),
            password=self._get("saga.queue.password"),
            host=self._get("saga.queue.host"),
            port=int(self._get("saga.queue.port")),
            records=int(self._get("saga.queue.records")),
            retry=int(self._get("saga.queue.retry")),
        )
        return queue

    @property
    def _saga_items(self) -> list[CONTROLLER]:
        info = self._get("saga.items")
        sagas = [self._sagas_items_entry(saga) for saga in info]
        return sagas

    @staticmethod
    def _sagas_items_entry(saga: dict[str, t.Any]) -> CONTROLLER:
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
