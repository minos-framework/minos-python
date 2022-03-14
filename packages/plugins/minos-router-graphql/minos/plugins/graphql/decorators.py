from abc import (
    ABC,
)
from collections.abc import (
    Iterable,
)
from typing import (
    Final,
)

from minos.networks import (
    EnrouteDecorator,
    EnrouteDecoratorKind,
    enroute,
)
from collections.abc import (
    Collection,
)
from collections import (
    defaultdict,
)


class GraphQlEnrouteDecorator(EnrouteDecorator, ABC):
    """GraphQl Enroute class"""

    def __init__(self, name: str, argument, output):
        self.name = name
        self.argument = argument
        self.output = output

    def __iter__(self) -> Iterable:
        yield from (self.name,)

    def _validate_not_redefined(self, decorators: Collection[EnrouteDecorator]) -> None:
        mapper = defaultdict(set)
        for decorator in decorators:
            mapper[tuple(decorator)].add(decorator)


class GraphQlCommandEnrouteDecorator(GraphQlEnrouteDecorator):
    """GraphQl Command Enroute class"""

    KIND: Final[EnrouteDecoratorKind] = EnrouteDecoratorKind.Command


class GraphQlQueryEnrouteDecorator(GraphQlEnrouteDecorator):
    """GraphQl Query Enroute class"""

    KIND: Final[EnrouteDecoratorKind] = EnrouteDecoratorKind.Query


class GraphQlEnroute:
    """GraphQl Enroute class"""

    command = GraphQlCommandEnrouteDecorator
    query = GraphQlQueryEnrouteDecorator

    @classmethod
    def register(cls):
        """TODO"""
        # noinspection PyProtectedMember
        enroute._register_sub_enroute("graphql", cls)
