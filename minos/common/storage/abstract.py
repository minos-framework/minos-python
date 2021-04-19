"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

import abc


class MinosStorage(abc.ABC):
    @abc.abstractmethod
    def add(self, **kwargs):
        raise NotImplementedError

    @abc.abstractmethod
    def get(self, **kwargs):
        raise NotImplementedError

    @abc.abstractmethod
    def delete(self, **kwargs):
        raise NotImplementedError

    @abc.abstractmethod
    def update(self, **kwargs):
        raise NotImplementedError

    @abc.abstractmethod
    def build(self, **kwargs):
        raise NotImplementedError
