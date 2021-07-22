# Copyright (C) 2020 Clariteia SL
#
# This file is part of minos framework.
#
# Minos framework can not be copied and/or distributed without the express
# permission of Clariteia SL.
from __future__ import (
    annotations,
)

from abc import (
    abstractmethod,
)
from enum import (
    Enum,
    auto,
)
from functools import (
    cached_property,
)
from inspect import (
    getmembers,
    iscoroutinefunction,
    isfunction,
)
from typing import (
    Callable,
    Iterable,
    Type,
)


class BaseDecorator:
    """TODO"""

    KIND: EnrouteKind

    def __call__(self, fn: Callable):
        if iscoroutinefunction(fn):

            async def _wrapper(*args, **kwargs):
                return await fn(*args, **kwargs)

        else:

            def _wrapper(*args, **kwargs):
                return fn(*args, **kwargs)

        _wrapper.__decorators__ = {self} | getattr(fn, "__decorators__", set())
        kinds = set(decorator.KIND for decorator in _wrapper.__decorators__)
        if len(kinds) > 1:
            raise EnrouteKindError(f"There are multiple kinds but only one is allowed: {kinds}")

        _wrapper.__base_func__ = getattr(fn, "__base_func__", fn)

        return _wrapper

    def __repr__(self):
        args = ", ".join(map(repr, self))
        return f"{type(self).__name__}({args})"

    def __eq__(self, other: BaseDecorator) -> bool:
        return type(self) == type(other) and tuple(self) == tuple(other)

    def __hash__(self) -> int:
        return hash(tuple(self))

    @abstractmethod
    def __iter__(self) -> Iterable:
        raise NotImplementedError


class EnrouteKind(Enum):
    """TODO"""

    Command = auto()
    Event = auto()
    Query = auto()


class EnrouteKindError(Exception):
    """TODO"""


class BrokerCommandEnroute(BaseDecorator):
    """Broker Command Enroute class"""

    KIND = EnrouteKind.Command

    def __init__(self, topics: Iterable[str]):
        if isinstance(topics, str):
            topics = (topics,)
        self.topics = tuple(topics)

    def __iter__(self) -> Iterable:
        yield from (self.topics,)


class BrokerQueryEnroute(BaseDecorator):
    """Broker Query Enroute class"""

    KIND = EnrouteKind.Query

    def __init__(self, topics: Iterable[str]):
        if isinstance(topics, str):
            topics = (topics,)
        self.topics = tuple(topics)

    def __iter__(self) -> Iterable:
        yield from (self.topics,)


class BrokerEventEnroute(BaseDecorator):
    """Broker Event Enroute class"""

    KIND = EnrouteKind.Event

    def __init__(self, topics: Iterable[str]):
        if isinstance(topics, str):
            topics = (topics,)
        self.topics = tuple(topics)

    def __iter__(self) -> Iterable:
        yield from (self.topics,)


class BrokerEnroute:
    """Broker Enroute class"""

    command = BrokerCommandEnroute
    query = BrokerQueryEnroute
    event = BrokerEventEnroute


class RestCommandEnroute(BaseDecorator):
    """Rest Command Enroute class"""

    KIND = EnrouteKind.Command

    def __init__(self, url: str, method: str):
        self.url = url
        self.method = method

    def __iter__(self) -> Iterable:
        yield from (
            self.url,
            self.method,
        )


class RestQueryEnroute(BaseDecorator):
    """Rest Query Enroute class"""

    KIND = EnrouteKind.Query

    def __init__(self, url: str, method: str):
        self.url = url
        self.method = method

    def __iter__(self) -> Iterable:
        yield from (
            self.url,
            self.method,
        )


class RestEnroute:
    """Rest Enroute class"""

    command = RestCommandEnroute
    query = RestQueryEnroute


class Enroute:
    """Enroute decorator main class"""

    broker = BrokerEnroute
    rest = RestEnroute


enroute = Enroute


class EnrouteDecoratorAnalyzer:
    """Search decorators in specified class"""

    def __init__(self, classname: Type):
        self.classname = classname

    def rest(self) -> dict[Callable, set[BaseDecorator]]:
        """Returns rest values."""
        return self._get_items({RestCommandEnroute, RestQueryEnroute})

    def command(self) -> dict[Callable, set[BaseDecorator]]:
        """Returns command values."""
        return self._get_items({BrokerCommandEnroute, BrokerQueryEnroute})

    def event(self) -> dict[Callable, set[BaseDecorator]]:
        """Returns event values."""
        return self._get_items({BrokerEventEnroute})

    def _get_items(self, expected_types: set[Type[BaseDecorator]]) -> dict[Callable, set[BaseDecorator]]:
        items = dict()
        for fn, decorators in self._result.items():
            decorators = {decorator for decorator in decorators if type(decorator) in expected_types}
            if len(decorators):
                items[fn] = decorators
        return items

    def get_all(self) -> dict[Callable, set[BaseDecorator]]:
        """TODO

        :return:TODO
        """
        return self._result

    @cached_property
    def _result(self) -> dict[Callable, set[BaseDecorator]]:
        """TODO

        :return: TODO
        """
        result = dict()
        for _, fn in getmembers(self.classname, predicate=isfunction):
            if not hasattr(fn, "__decorators__"):
                continue
            result[fn] = fn.__decorators__
        return result
