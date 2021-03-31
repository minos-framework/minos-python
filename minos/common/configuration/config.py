import collections
import configparser
import os
import typing as t
from minos.common.exceptions import MinosConfigException
from minos.common.logs import log

KAFKA = collections.namedtuple("Kafka", "host port")
REST = collections.namedtuple("Rest", "host port")
MICROSERVICE = collections.namedtuple("Microservice", "name events saga commands")


class MinosConfig(object):
    __slots__ = "_parser"

    def __init__(self, path: str):
        if os.path.isfile(path):
            self._parser = configparser.ConfigParser()
            self._parser.read(path)
        else:
            raise MinosConfigException(f"file {path} does not exit")

    @property
    def kafka(self):
        return KAFKA(
            host=self._get("host", "kafka"),
            port=self._get("port", "kafka")
        )

    @property
    def rest(self):
        return REST(
            host=self._get("host", "rest"),
            port=self._get("port", "rest")
        )

    @property
    def microservice(self):
        return MICROSERVICE(
            name=self._get("name", "microservice"),
            events=self._get("events", "microservice"),
            saga=self._get("saga", "microservice"),
            commands=self._get("commands", "microservice")
        )

    def _get(self, key: str, section: str, default: t.Any = None) -> t.Union[None, t.List, str]:

        if self._parser.has_section(section) and self._parser.has_option(section, key):
            key_val = self._parser[section][key]
            # check if key val is an array of values
            if "," in key_val:
                log.debug(f"{key_val} is an array of values")
                return_val = key_val.split(",")
            else:
                log.debug(f"{key_val} is a string")
                # check if is a numeric value
                if key_val.isdecimal():
                    return_val = int(key_val)
                else:
                    return_val = key_val
            return return_val
        else:
            return default
