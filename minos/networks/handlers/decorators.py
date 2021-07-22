# Copyright (C) 2020 Clariteia SL
#
# This file is part of minos framework.
#
# Minos framework can not be copied and/or distributed without the express
# permission of Clariteia SL.
from __future__ import (
    annotations,
)

import ast
from enum import (
    Enum,
    auto,
)
from functools import (
    cached_property,
)
from inspect import (
    getsource,
)
from typing import (
    Callable,
    Type,
)


class BaseDecorator:
    """TODO"""

    KIND: EnrouteKind

    def __call__(self, fn):
        def _wrapper(*args, analyze_mode: bool = False, **kwargs):
            if not analyze_mode:
                return fn(*args, **kwargs)

            result = [self]
            try:
                result += fn(*args, analyze_mode=analyze_mode, **kwargs)
            except TypeError:  # pragma: no cover
                pass

            kinds = set(decorator.KIND for decorator in result)
            if len(kinds) > 1:
                raise EnrouteKindError(f"There are multiple kinds but only one is allowed: {kinds}")

            return result

        _wrapper.__base_func__ = getattr(fn, "__base_func__", fn)
        return _wrapper


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

    def __init__(self, topics: list[str]):
        self.topics = topics


class BrokerQueryEnroute(BaseDecorator):
    """Broker Query Enroute class"""

    KIND = EnrouteKind.Query

    def __init__(self, topics: list[str]):
        self.topics = topics


class BrokerEventEnroute(BaseDecorator):
    """Broker Event Enroute class"""

    KIND = EnrouteKind.Event

    def __init__(self, topics: list[str]):
        self.topics = topics


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


class RestQueryEnroute(BaseDecorator):
    """Rest Query Enroute class"""

    KIND = EnrouteKind.Query

    def __init__(self, url: str, method: str):
        self.url = url
        self.method = method


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

    def get_all(self) -> dict[Callable, list[BaseDecorator]]:
        """TODO

        :return:TODO
        """
        return self._result

    @cached_property
    def _result(self) -> dict[Callable, list[BaseDecorator]]:
        """TODO

        :return: TODO
        """
        fns = self._find_decorators(self.classname)
        result = dict()
        for name, decorated in fns.items():
            if not decorated:
                continue
            fn = getattr(self.classname, name)
            result[fn] = fn(analyze_mode=True)

        return result

    @staticmethod
    def _find_decorators(target: Type) -> dict[str, bool]:
        """Search decorators in a given class
        Original source: https://stackoverflow.com/a/9580006/3921457
        """
        res = dict()

        def _fn(node):
            res[node.name] = bool(node.decorator_list)

        v = ast.NodeVisitor()
        v.visit_FunctionDef = _fn
        v.visit(compile(getsource(target), "?", "exec", ast.PyCF_ONLY_AST))
        return res
