"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from abc import (
    abstractmethod,
)
from typing import (
    Generic,
    NoReturn,
    TypeVar,
)

T = TypeVar("T")


class MinosSetup(Generic[T]):
    """Minos setup base class."""

    def __init__(self, *args, already_setup: bool = False, **kwargs):
        self.already_setup = already_setup
        self.already_destroyed = False

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

    @abstractmethod
    async def _setup(self) -> NoReturn:
        """Setup miscellaneous repository things."""
        raise NotImplementedError

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
