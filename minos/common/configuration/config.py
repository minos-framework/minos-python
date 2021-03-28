from enum import Enum

from minos.common.configuration.providers.ini import MinosConfigIniProvider


class MinosConfigProviders(Enum):
    INI = 1


class MinosConfig(object):

    @staticmethod
    def read(path: str, provider: MinosConfigProviders):
        """
        prepare the config class with the specified provider
        """
        if provider == MinosConfigProviders.INI:
            config = MinosConfigIniProvider(path=path)
            config.read()
        return config
