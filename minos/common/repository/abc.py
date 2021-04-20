"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from abc import (
    ABC,
    abstractmethod,
)

from .entries import (
    MinosRepositoryEntry,
)


class MinosRepository(ABC):
    """TODO"""

    @abstractmethod
    def insert(self, value: MinosRepositoryEntry):
        """TODO

        :param value: TODO
        :return: TODO
        """
        pass

    @abstractmethod
    def update(self, value: MinosRepositoryEntry):
        """TODO

        :param value: TODO
        :return: TODO
        """
        pass

    @abstractmethod
    def delete(self, value: MinosRepositoryEntry):
        """TODO

        :param value: TODO
        :return: TODO
        """
        pass

    @abstractmethod
    def select(self, *args, **kwargs) -> list[MinosRepositoryEntry]:
        """TODO

        :param args: TODO
        :param kwargs: TODO
        :return: TODO
        """
        pass
