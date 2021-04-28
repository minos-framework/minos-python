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
EVENT = collections.namedtuple("Event", "name controller action")
COMMAND = collections.namedtuple("Command", "name controller action")
SERVICE = collections.namedtuple("Service", "name")

EVENTS = collections.namedtuple("Events", "broker items queue")
COMMANDS = collections.namedtuple("Commands", "broker items queue")
REST = collections.namedtuple("Rest", "broker endpoints")

REPOSITORY = collections.namedtuple("Repository", "database user password host port")

_default: t.Optional[MinosConfigAbstract] = None


class MinosConfigAbstract(abc.ABC):
    """TODO"""

    __slots__ = "_services", "_path"

    def __init__(self, path: t.Union[Path, str]):
        if isinstance(path, Path):
            path = str(path)
        self._services = {}
        self._path = path
        self._load(path)

    @abc.abstractmethod
    def _load(self, path: str) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def _get(self, key: str, **kwargs: t.Any):
        raise NotImplementedError

    @staticmethod
    def _file_exit(path: str) -> bool:
        if os.path.isfile(path):
            return True
        return False

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


_ENVIRONMENT_MAPPER = {
    "commands.queue.host": "MINOS_COMMANDS_QUEUE_HOST",
    "commands.queue.port": "MINOS_COMMANDS_QUEUE_PORT",
    "commands.queue.database": "MINOS_COMMANDS_QUEUE_DATABASE",
    "commands.queue.user": "MINOS_COMMANDS_QUEUE_USER",
    "commands.queue.password": "MINOS_COMMANDS_QUEUE_PASSWORD",
    "commands.broker": "MINOS_COMMANDS_BROKER",
    "commands.port": "MINOS_COMMANDS_PORT",
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
}


class MinosConfig(MinosConfigAbstract):
    """TODO"""

    __slots__ = ("_data",)

    def _load(self, path):
        if self._file_exit(path):
            with open(path) as f:
                self._data = yaml.load(f, Loader=yaml.FullLoader)
        else:
            raise MinosConfigException(f"Check if this path: {path} is correct")

    def _get(self, key: str, **kwargs: t.Any) -> t.Any:
        if key in _ENVIRONMENT_MAPPER:
            env = os.getenv(_ENVIRONMENT_MAPPER[key])
            if env is not None:
                return env

        def _fn(k: str, data: dict[str, t.Any]) -> t.Any:
            current, _, following = k.partition(".")

            part = data[current]
            if not following:
                return part

            return _fn(following, part)

        return _fn(key, self._data)

    @property
    def service(self) -> SERVICE:
        """TODO

        :return: TODO
        """
        return SERVICE(name=self._get("service.name"))

    @property
    def rest(self):
        """TODO

        :return: TODO
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
        """TODO

        :return: TODO
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
    def _events_items(self) -> list[EVENT]:
        info = self._get("events.items")
        events = [self._events_items_entry(event) for event in info]
        return events

    @staticmethod
    def _events_items_entry(event: dict[str, t.Any]) -> EVENT:
        return EVENT(name=event["name"], controller=event["controller"], action=event["action"])

    @property
    def commands(self) -> COMMANDS:
        """TODO

        :return: TODO
        """
        broker = self._commands_broker
        queue = self._commands_queue
        commands = self._commands_items
        return COMMANDS(broker=broker, items=commands, queue=queue)

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
    def _commands_items(self) -> list[COMMAND]:
        info = self._get("commands.items")
        commands = [self._commands_items_entry(command) for command in info]
        return commands

    @staticmethod
    def _commands_items_entry(command: dict[str, t.Any]) -> COMMAND:
        return COMMAND(name=command["name"], controller=command["controller"], action=command["action"])

    @property
    def repository(self) -> REPOSITORY:
        """Get the repository config.

        :return: A ``Repository`` NamedTuple instance.
        """
        return REPOSITORY(
            database=self._get("repository.database"),
            user=self._get("repository.user"),
            password=self._get("repository.password"),
            host=self._get("repository.host"),
            port=int(self._get("repository.port")),
        )
