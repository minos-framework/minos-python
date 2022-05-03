from abc import (
    ABC,
)
from collections.abc import (
    Iterable,
)
from typing import (
    Final,
    Optional,
)

from minos.networks import (
    EnrouteDecorator,
    EnrouteDecoratorKind,
    enroute,
)


class GraphQlEnrouteDecorator(EnrouteDecorator, ABC):
    """GraphQl Enroute Decorator class"""

    def __init__(self, name: str, output, argument: Optional = None, **kwargs):
        super().__init__(**kwargs)
        self.name = name
        self.argument = argument
        self.output = output

    def __iter__(self) -> Iterable:
        yield from (
            type(self),
            self.name,
        )


class GraphQlCommandEnrouteDecorator(GraphQlEnrouteDecorator):
    """GraphQl Command Enroute Decorator class"""

    KIND: Final[EnrouteDecoratorKind] = EnrouteDecoratorKind.Command


class GraphQlQueryEnrouteDecorator(GraphQlEnrouteDecorator):
    """GraphQl Query Enroute Decorator class"""

    KIND: Final[EnrouteDecoratorKind] = EnrouteDecoratorKind.Query


class GraphQlEnroute:
    """GraphQl Enroute class"""

    command = GraphQlCommandEnrouteDecorator
    query = GraphQlQueryEnrouteDecorator

    @classmethod
    def register(cls):
        """Register the graphql sub-enroute."""
        # noinspection PyProtectedMember
        enroute._register_sub_enroute("graphql", cls)
