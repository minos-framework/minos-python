"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from __future__ import (
    annotations,
)

from typing import (
    Generic,
    NoReturn,
    TypeVar,
)

from dependency_injector.wiring import (
    Provide,
)

from .configuration import (
    MinosConfig,
)
from .exceptions import (
    MinosConfigException,
)

T = TypeVar("T")


class MinosSetup(Generic[T]):
    """Minos setup base class."""

    _config: MinosConfig = Provide["config"]

    def __init__(self, *args, already_setup: bool = False, **kwargs):
        self.already_setup = already_setup
        self.already_destroyed = False

    @classmethod
    def from_config(cls, *args, config: MinosConfig = None, **kwargs) -> T:
        """Build a new instance from config.

        :param args: Additional positional arguments.
        :param config: Config instance. If `None` is provided, default config is chosen.
        :param kwargs: Additional named arguments.
        :return: A instance of the called class.
        """
        if config is None:
            config = cls._config
            if isinstance(config, Provide):
                raise MinosConfigException("The config object must be setup.")
        return cls._from_config(*args, config=config, **kwargs)

    @classmethod
    def _from_config(cls, *args, config: MinosConfig, **kwargs) -> T:
        return cls(*args, **kwargs)

    async def __aenter__(self) -> T:
        await self.setup()
        return self

    async def setup(self) -> NoReturn:
        """Setup miscellaneous repository things.

        :return: This method does not return anything.
        """
        if not self.already_setup:
            await self._setup()
            self.already_setup = True
        self.already_destroyed = False

    async def _setup(self) -> NoReturn:
        return

    async def __aexit__(self, exc_type, exc_value, exc_traceback):
        await self.destroy()

    async def destroy(self) -> NoReturn:
        """Destroy miscellaneous repository things.

        :return: This method does not return anything.
        """
        if not self.already_destroyed:
            await self._destroy()
            self.already_destroyed = True
        self.already_setup = False

    async def _destroy(self) -> NoReturn:
        """Destroy miscellaneous repository things."""
