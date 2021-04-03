import collections
import os
import typing as t

import yaml
import abc

from minos.common.exceptions import MinosConfigException

BROKER = collections.namedtuple("Broker", "host port")
DATABASE = collections.namedtuple("Database", "path name")
ENDPOINT = collections.namedtuple("Endpoint", "name route method controller action")
EVENT = collections.namedtuple("Event", "name controller action")
COMMAND = collections.namedtuple("Command", "name controller action")
SERVICE = collections.namedtuple("Service", "name")

EVENTS = collections.namedtuple("Events", "broker database items")
COMMANDS = collections.namedtuple("Commands", "broker database items")
REST = collections.namedtuple("Rest", "broker endpoints")


class MinosConfigAbstract(abc.ABC):
    __slots__ = "_services", "_path"

    def __init__(self, path: str):
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


class MinosConfig(MinosConfigAbstract):
    __slots__ = "_data"

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
        return SERVICE(name=service['name'])

    @property
    def rest(self):
        rest_info = self._get("rest")
        broker = BROKER(host=rest_info['host'], port=rest_info['port'])
        endpoints = []
        for endpoint in rest_info['endpoints']:
            endpoints.append(ENDPOINT(name=endpoint['name'], route=endpoint['route'],
                                      method=endpoint['method'].upper(), controller=endpoint['controller'],
                                      action=endpoint['action'])
                             )
        return REST(broker=broker, endpoints=endpoints)

    @property
    def events(self):
        event_info = self._get("events")
        broker = BROKER(host=event_info['broker'], port=event_info['port'])
        database = DATABASE(path=event_info['database']['path'], name=event_info['database']['name'])
        events = []
        for event in event_info['items']:
            events.append(EVENT(name=event['name'], controller=event['controller'],
                                action=event['action'])
                          )
        return EVENTS(broker=broker, items=events, database=database)

    @property
    def commands(self):
        command_info = self._get("commands")
        broker = BROKER(host=command_info['broker'], port=command_info['port'])
        database = DATABASE(path=command_info['database']['path'], name=command_info['database']['name'])
        commands = []
        for command in command_info['items']:
            commands.append(COMMAND(name=command['name'], controller=command['controller'],
                                    action=command['action'])
                            )
        return COMMANDS(broker=broker, items=commands, database=database)
