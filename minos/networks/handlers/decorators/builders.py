"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from inspect import (
    isawaitable,
)
from typing import (
    Awaitable,
    Callable,
    Type,
    Union,
)

from minos.common import (
    Request,
    Response,
    import_module,
)

from .analyzers import (
    EnrouteAnalyzer,
)
from .definitions import (
    EnrouteDecorator,
)


class EnrouteBuilder:
    """TODO"""

    def __init__(self, decorated: Union[str, Type]):
        if isinstance(decorated, str):
            decorated = import_module(decorated)

        self.analyzer = EnrouteAnalyzer(decorated)
        self.instance = decorated()

    def get_rest_command_query(self) -> list[(Callable[[Request], Awaitable[Response]], EnrouteDecorator)]:
        """TODO

        :return: TODO
        """
        mapping = self.analyzer.get_rest_command_query()
        return self._build(mapping)

    def get_broker_command_query(self) -> list[(Callable[[Request], Awaitable[Response]], EnrouteDecorator)]:
        """TODO

        :return: TODO
        """
        mapping = self.analyzer.get_broker_command_query()
        return self._build(mapping)

    def get_broker_event(self) -> list[(Callable[[Request], Awaitable[Response]], EnrouteDecorator)]:
        """TODO

        :return: TODO
        """
        mapping = self.analyzer.get_broker_event()
        return self._build(mapping)

    def _build(
        self, mapping: dict[str, set[EnrouteDecorator]]
    ) -> list[(Callable[[Request], Awaitable[Response]], EnrouteDecorator)]:

        ans = list()
        for name, decorators in mapping.items():
            fn = getattr(self.instance, name)
            for decorator in decorators:
                pre_fn = getattr(self.instance, decorator.KIND.pref_fn_name, None)
                if pre_fn is not None:

                    async def _fn(request):
                        request = await pre_fn(request)
                        return await fn(request)

                else:
                    if isawaitable(fn):
                        _fn = fn
                    else:

                        async def _fn(request):
                            return fn(request)

                ans.append((_fn, decorator))
        return ans
