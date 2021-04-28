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
        if key in self._data:
            return self._data[key]
        return None

    @property
    def service(self) -> SERVICE:
        """TODO

        :return: TODO
        """
        service = self._get("service")
        return SERVICE(name=service["name"])

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
        rest_info = self._get("rest")
        broker = BROKER(host=rest_info["host"], port=rest_info["port"])
        return broker

    @property
    def _rest_endpoints(self) -> list[ENDPOINT]:
        info = self._get("rest")["endpoints"]
        endpoints = []
        for endpoint in info:
            endpoints.append(
                ENDPOINT(
                    name=endpoint["name"],
                    route=endpoint["route"],
                    method=endpoint["method"].upper(),
                    controller=endpoint["controller"],
                    action=endpoint["action"],
                )
            )
        return endpoints

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
        event_info = self._get("events")
        return BROKER(host=event_info["broker"], port=event_info["port"])

    @property
    def _events_queue(self) -> QUEUE:
        info = self._get("events")["queue"]
        return QUEUE(
            database=info["database"],
            user=info["user"],
            password=info["password"],
            host=info["host"],
            port=info["port"],
            records=info["records"],
            retry=info["retry"],
        )

    @property
    def _events_items(self) -> list[EVENT]:
        info = self._get("events")["items"]
        events = []
        for event in info:
            events.append(EVENT(name=event["name"], controller=event["controller"], action=event["action"]))
        return events

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
        info = self._get("commands")
        broker = BROKER(host=info["broker"], port=info["port"])
        return broker

    @property
    def _commands_queue(self) -> QUEUE:
        info = self._get("commands")["queue"]
        queue = QUEUE(
            database=info["database"],
            user=info["user"],
            password=info["password"],
            host=info["host"],
            port=info["port"],
            records=info["records"],
            retry=info["retry"],
        )
        return queue

    @property
    def _commands_items(self) -> list[COMMAND]:
        command_info = self._get("commands")
        commands = []
        for command in command_info["items"]:
            commands.append(COMMAND(name=command["name"], controller=command["controller"], action=command["action"]))
        return commands

    @property
    def repository(self) -> REPOSITORY:
        """Get the repository config.

        :return: A ``Repository`` NamedTuple instance.
        """
        info = self._get("repository")
        return REPOSITORY(
            database=os.getenv("POSTGRES_DATABASE", info["database"]),
            user=os.getenv("POSTGRES_USER", info["user"]),
            password=os.getenv("POSTGRES_PASSWORD", info["password"]),
            host=os.getenv("POSTGRES_HOST", info["host"]),
            port=os.getenv("POSTGRES_PORT", info["port"]),
        )
