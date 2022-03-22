from inspect import (
    getmembers,
    isfunction,
    ismethod,
)
from typing import (
    Any,
    Callable,
    Optional,
    Type,
)

from minos.common import (
    Config,
    import_module,
)

from .callables import (
    HandlerWrapper,
)
from .definitions import (
    BrokerCommandEnrouteDecorator,
    BrokerEnrouteDecorator,
    BrokerEventEnrouteDecorator,
    BrokerQueryEnrouteDecorator,
    EnrouteDecorator,
    PeriodicEventEnrouteDecorator,
    RestCommandEnrouteDecorator,
    RestEnrouteDecorator,
    RestQueryEnrouteDecorator,
)


class EnrouteCollector:
    """Search decorators in specified class"""

    # noinspection PyUnusedLocal
    def __init__(self, decorated: Any, config: Optional[Config] = None, **kwargs):
        if isinstance(decorated, str):
            decorated = import_module(decorated)

        self.decorated = decorated
        self.config = config

    def get_rest_command_query(self) -> dict[str, set[RestEnrouteDecorator]]:
        """Returns rest's command and query values.

        :return: A mapping with functions as keys and a sets of decorators as values.
        """
        # noinspection PyTypeChecker
        return self._get_items({RestCommandEnrouteDecorator, RestQueryEnrouteDecorator})

    def get_broker_command_query_event(self) -> dict[str, set[BrokerEnrouteDecorator]]:
        """Returns broker's command, query and event values.

        :return: A mapping with functions as keys and a sets of decorators as values.
        """
        # noinspection PyTypeChecker
        return self._get_items(
            {BrokerCommandEnrouteDecorator, BrokerQueryEnrouteDecorator, BrokerEventEnrouteDecorator}
        )

    def get_broker_command_query(self) -> dict[str, set[BrokerEnrouteDecorator]]:
        """Returns broker's command and query values.

        :return: A mapping with functions as keys and a sets of decorators as values.
        """
        # noinspection PyTypeChecker
        return self._get_items({BrokerCommandEnrouteDecorator, BrokerQueryEnrouteDecorator})

    def get_broker_event(self) -> dict[str, set[BrokerEnrouteDecorator]]:
        """Returns broker's event values.

        :return: A mapping with functions as keys and a sets of decorators as values.
        """
        # noinspection PyTypeChecker
        return self._get_items({BrokerEventEnrouteDecorator})

    def get_periodic_event(self) -> dict[str, set[PeriodicEventEnrouteDecorator]]:
        """Returns periodic event values.

        :return: A mapping with functions as keys and a sets of decorators as values.
        """
        # noinspection PyTypeChecker
        return self._get_items({PeriodicEventEnrouteDecorator})

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
            if isinstance(fn, HandlerWrapper):
                result[name] = fn.meta.decorators
        return result
