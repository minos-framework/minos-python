from collections.abc import (
    Iterable,
)
from uuid import (
    UUID,
)

from .abc import (
    BrokerSubscriberDuplicateValidator,
)


class InMemoryBrokerSubscriberDuplicateValidator(BrokerSubscriberDuplicateValidator):
    """In Memory Broker Subscriber Duplicate Detector class."""

    def __init__(self, seen: Iterable[tuple[str, UUID]] = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if seen is None:
            seen = set()
        self._seen = set(seen)

    @property
    def seen(self) -> set[tuple[str, UUID]]:
        """Get the seen pairs.

        :return: A ``set`` of ``tuple`` instances in which the first value is a ``str`` and the second an ``UUID``.
        """
        return self._seen

    async def _is_unique(self, topic: str, uuid: UUID) -> bool:
        if (topic, uuid) not in self._seen:
            self._seen.add((topic, uuid))
            return True
        return False
