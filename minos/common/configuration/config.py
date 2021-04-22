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
    MinosConfigException,
)

BROKER = collections.namedtuple("Broker", "host port")
DATABASE = collections.namedtuple("Database", "path name")
QUEUE = collections.namedtuple("Queue", "database user password host port records")
ENDPOINT = collections.namedtuple("Endpoint", "name route method controller action")
EVENT = collections.namedtuple("Event", "name controller action")
COMMAND = collections.namedtuple("Command", "name controller action")
SERVICE = collections.namedtuple("Service", "name")

EVENTS = collections.namedtuple("Events", "broker database items queue")
COMMANDS = collections.namedtuple("Commands", "broker database items queue")
REST = collections.namedtuple("Rest", "broker endpoints")

REPOSITORY = collections.namedtuple("Repository", "database user password host port")


class MinosConfigAbstract(abc.ABC):
    __slots__ = "_services", "_path"

    _default: t.Optional[MinosConfigAbstract] = None

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

    def _file_exit(self, path: str) -> bool:
        if os.path.isfile(path):
            return True
        return False

    def __enter__(self) -> MinosConfigAbstract:
        self.set_default(self)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> t.NoReturn:
        self.unset_default()

    @classmethod
    def set_default(cls, value: t.Optional[MinosConfigAbstract]) -> t.NoReturn:
        """TODO

        :param value: TODO
        :return: TODO
        """
        if cls.get_default() is not None:
            raise Exception("There is already another config set as default.")  # TODO: Convert into a minos exception
        MinosConfigAbstract._default = value

    @classmethod
    def get_default(cls) -> MinosConfigAbstract:
        """TODO

        :return: TODO
        """
        return cls._default

    def unset_default(self):
        MinosConfigAbstract._default = None


class MinosConfig(MinosConfigAbstract):
    __slots__ = "_data", "_instances"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._instances = dict()

    def _load(self, path):
        if self._file_exit(path):
            with open(path) as f:
                self._data = yaml.load(f, Loader=yaml.FullLoader)
        else:
            raise MinosConfigException(f"Check if this path: {path} is correct")

    def _get(self, key: str, **kwargs: t.Any) -> t.Union[str, int, t.Dict[str, t.Any], None]:
        if key in self._data:
            return self._data[key]
        return None

    @property
    def service(self):
        service = self._get("service")
        return SERVICE(name=service["name"])

    @property
    def rest(self):
        rest_info = self._get("rest")
        broker = BROKER(host=rest_info["host"], port=rest_info["port"])
        endpoints = []
        for endpoint in rest_info["endpoints"]:
            endpoints.append(
                ENDPOINT(
                    name=endpoint["name"],
                    route=endpoint["route"],
                    method=endpoint["method"].upper(),
                    controller=endpoint["controller"],
                    action=endpoint["action"],
                )
            )
        return REST(broker=broker, endpoints=endpoints)

    @property
    def events(self):
        event_info = self._get("events")
        broker = BROKER(host=event_info["broker"], port=event_info["port"])
        database = DATABASE(path=event_info["database"]["path"], name=event_info["database"]["name"])
        queue = QUEUE(
            database=event_info["queue"]["database"],
            user=event_info["queue"]["user"],
            password=event_info["queue"]["password"],
            host=event_info["queue"]["host"],
            port=event_info["queue"]["port"],
            records=event_info["queue"]["records"],
        )
        events = []
        for event in event_info["items"]:
            events.append(EVENT(name=event["name"], controller=event["controller"], action=event["action"],))
        return EVENTS(broker=broker, items=events, database=database, queue=queue)

    @property
    def commands(self):
        command_info = self._get("commands")
        broker = BROKER(host=command_info["broker"], port=command_info["port"])
        database = DATABASE(path=command_info["database"]["path"], name=command_info["database"]["name"])
        queue = QUEUE(
            database=command_info["queue"]["database"],
            user=command_info["queue"]["user"],
            password=command_info["queue"]["password"],
            host=command_info["queue"]["host"],
            port=command_info["queue"]["port"],
            records=command_info["queue"]["records"],
        )
        commands = []
        for command in command_info["items"]:
            commands.append(COMMAND(name=command["name"], controller=command["controller"], action=command["action"],))
        return COMMANDS(broker=broker, items=commands, database=database, queue=queue)

    @property
    def repository(self) -> t.NamedTuple:
        """TODO

        :return: TODO
        """
        command_info = self._get("repository")
        return REPOSITORY(
            database=command_info["database"],
            user=command_info["user"],
            password=command_info["password"],
            host=command_info["host"],
            port=command_info["port"],
        )

    @property
    def repository_instance(self) -> t.Any:
        """TODO

        :return: TODO
        """

        if "repository" not in self._instances:
            from ..repository import PostgreSqlMinosRepository
            self._instances["repository"] = PostgreSqlMinosRepository(**self.repository._asdict())

        return self._instances["repository"]
