from uuid import (
    UUID,
)

from minos.aggregate import (
    Aggregate,
    RootEntity,
)


class Notifier(RootEntity):
    """Notifier RootEntity class."""


class NotifierAggregate(Aggregate[Notifier]):
    """NotifierAggregate class."""

    @staticmethod
    async def create() -> UUID:
        """Create a new instance."""
        root = await Notifier.create()
        return root.uuid
