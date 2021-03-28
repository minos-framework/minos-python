import configparser
import os
import typing as t

from minos.common.configuration.providers.abstract import MinosAbstractProvider
from minos.common.exceptions import MinosConfigException


class MinosConfigIniProvider(MinosAbstractProvider):
    __slots__ = "_path", "_parser"

    def __init__(self, path: str):
        if os.path.isfile(path):
            self._path = path
            self._parser = configparser.ConfigParser()
        else:
            raise MinosConfigException(f"file {path} does not exit")

    def read(self):
        self._parser.read(self._path)

    def get(self, key: str, section: str) -> t.Any:
        # get a key from a specific section
        if self._parser.has_section(section):
            # check if the key exist
            if self._parser.has_option(section, key):
                return self._parser[section][key]
            else:
                raise MinosConfigException(f"the key {key} is not in the config file")
        else:
            raise MinosConfigException(f"the section {section} does not exist")
