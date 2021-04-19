"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import abc


class MinosBaseBroker(abc.ABC):
    @abc.abstractmethod
    def _database(self):  # pragma: no cover
        raise NotImplementedError

    @abc.abstractmethod
    def send(self):  # pragma: no cover
        raise NotImplementedError
