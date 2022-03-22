from collections.abc import (
    Iterable,
)
from uuid import (
    UUID,
)

from .abc import (
    BrokerSubscriberDuplicateDetector,
    BrokerSubscriberDuplicateDetectorBuilder,
)


class InMemoryBrokerSubscriberDuplicateDetector(BrokerSubscriberDuplicateDetector):
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

    async def _is_valid(self, topic: str, uuid: UUID) -> bool:
        if (topic, uuid) not in self._seen:
            self._seen.add((topic, uuid))
            return True
        return False


class InMemoryBrokerSubscriberDuplicateDetectorBuilder(BrokerSubscriberDuplicateDetectorBuilder):
    """In Memory Broker Subscriber Queue Builder class."""

    def build(self) -> InMemoryBrokerSubscriberDuplicateDetector:
        """Build the instance.

        :return: An ``InMemoryBrokerSubscriberQueue`` instance.
        """
        return InMemoryBrokerSubscriberDuplicateDetector(**self.kwargs)


InMemoryBrokerSubscriberDuplicateDetector.set_builder(InMemoryBrokerSubscriberDuplicateDetectorBuilder)
