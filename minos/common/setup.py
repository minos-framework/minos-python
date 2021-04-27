"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from abc import (
    abstractmethod,
)
from typing import (
    NoReturn,
)


class MinosSetup(object):
    """Minos setup base class."""

    def __init__(self, *args, already_setup: bool = False, **kwargs):
        self.already_setup = already_setup

    async def setup(self) -> NoReturn:
        """Setup miscellaneous repository thing.

        :return: This method does not return anything.
        """
        if not self.already_setup:
            await self._setup()
            self.already_setup = True

    @abstractmethod
    async def _setup(self) -> NoReturn:
        """Setup miscellaneous repository thing."""
        raise NotImplementedError
