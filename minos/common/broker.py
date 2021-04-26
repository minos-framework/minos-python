"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import abc
from ..common import MinosModel, ModelRef, Aggregate


class MinosBaseBroker(abc.ABC):
    """Base Broker class."""

    @abc.abstractmethod
    def _database(self):  # pragma: no cover
        raise NotImplementedError

    @abc.abstractmethod
    def send(self):  # pragma: no cover
        raise NotImplementedError


class Event(MinosModel):
    """Base Event class."""

    topic: str
    model: str
    items: list[ModelRef[Aggregate]]


class Command(MinosModel):
    """Base Command class."""

    topic: str
    model: str
    items: list[ModelRef[Aggregate]]
    reply_on: str
