"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from __future__ import annotations

import abc
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .step import SagaStep


class MinosBaseSagaBuilder(abc.ABC):
    """TODO"""

    @abc.abstractmethod
    def step(self, **kwargs) -> SagaStep:  # pragma: no cover
        """TODO

        :param kwargs: TODO
        :return: TODO
        """
        raise NotImplementedError

    @abc.abstractmethod
    def execute(self, **kwargs):  # pragma: no cover
        """TODO

        :param kwargs: TODO
        :return: TODO
        """
        raise NotImplementedError
