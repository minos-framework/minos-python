"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from inspect import (
    getmembers,
    isfunction,
    ismethod,
)
from typing import (
    Callable,
    Optional,
    Type,
    Union,
)

from minos.common import (
    MinosConfig,
    import_module,
)

from .definitions import (
    BrokerCommandEnrouteDecorator,
    BrokerEventEnrouteDecorator,
    BrokerQueryEnrouteDecorator,
    EnrouteDecorator,
    RestCommandEnrouteDecorator,
    RestQueryEnrouteDecorator,
)


class EnrouteAnalyzer:
    """Search decorators in specified class"""

    def __init__(self, decorated: Union[str, Type], config: Optional[MinosConfig] = None):
        if isinstance(decorated, str):
            decorated = import_module(decorated)

        self.decorated = decorated
        self.config = config

    def get_rest_command_query(self) -> dict[str, set[EnrouteDecorator]]:
        """Returns rest values.

        :return: A mapping with functions as keys and a sets of decorators as values.
        """
        return self._get_items({RestCommandEnrouteDecorator, RestQueryEnrouteDecorator})

    def get_broker_command_query(self) -> dict[str, set[EnrouteDecorator]]:
        """Returns command values.

        :return: A mapping with functions as keys and a sets of decorators as values.
        """
        return self._get_items({BrokerCommandEnrouteDecorator, BrokerQueryEnrouteDecorator})

    def get_broker_event(self) -> dict[str, set[EnrouteDecorator]]:
        """Returns event values.

        :return: A mapping with functions as keys and a sets of decorators as values.
        """
        return self._get_items({BrokerEventEnrouteDecorator})

    def _get_items(self, expected_types: set[Type[EnrouteDecorator]]) -> dict[str, set[EnrouteDecorator]]:
        items = dict()
        for fn, decorators in self.get_all().items():
            decorators = {decorator for decorator in decorators if type(decorator) in expected_types}
            if len(decorators):
                items[fn] = decorators
        return items

    def get_all(self) -> dict[str, set[EnrouteDecorator]]:
        """Get all functions decorated with enroute decorators.

        :return: A mapping with functions as keys and a sets of decorators as values.
        """
        fn: Callable = getattr(self.decorated, "__get_enroute__", self._get_all)
        return fn(config=self.config)

    # noinspection PyUnusedLocal
    def _get_all(self, *args, **kwargs) -> dict[str, set[EnrouteDecorator]]:
        result = dict()
        for name, fn in getmembers(self.decorated, predicate=lambda x: ismethod(x) or isfunction(x)):
            if not hasattr(fn, "__decorators__"):
                continue
            result[name] = fn.__decorators__
        return result
