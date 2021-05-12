"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from __future__ import (
    annotations,
)

import abc
from typing import (
    Any,
    NoReturn,
    Optional,
)


class MinosStorage(abc.ABC):
    """Minos Storage interface."""

    @abc.abstractmethod
    def add(self, **kwargs) -> NoReturn:
        """Store a value.

        :param kwargs: Additional named arguments.
        :return: This method does not return anything.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def get(self, **kwargs) -> Optional[Any]:
        """Get the stored value..

        :param kwargs: Additional named arguments.
        :return: The stored value.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def delete(self, **kwargs) -> NoReturn:
        """Delete the stored value.

        :param kwargs:
        :return: This method does not return anything.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def update(self, **kwargs) -> NoReturn:
        """Update the stored value.

        :param kwargs: Additional named arguments.
        :return: This method does not return anything.
        """
        raise NotImplementedError

    @classmethod
    @abc.abstractmethod
    def build(cls, **kwargs) -> MinosStorage:
        """Build a new instance.

        :param kwargs: Additional named arguments.
        :return: A new ``MinosStorage`` instance.
        """
        raise NotImplementedError
