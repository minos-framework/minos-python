from abc import (
    ABC,
)
from collections import (
    defaultdict,
)
from collections.abc import (
    Collection,
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


class GraphQlEnrouteDecorator(EnrouteDecorator, ABC):
    """GraphQl Enroute class"""

    def __init__(self, name: str, argument, output):
        self.name = name
        self.argument = argument
        self.output = output

    def __iter__(self) -> Iterable:
        yield from (self.name,)


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
