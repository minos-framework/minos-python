from .abc import (
    DiscoveryClient,
)


class InMemoryDiscoveryClient(DiscoveryClient):
    """In Memory Discovery Client class."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._is_subscribed = False

    @property
    def is_subscribed(self) -> bool:
        """Check if the client is subscribed or not.

        :return:
        """
        return self._is_subscribed

    async def subscribe(self, *args, **kwargs) -> None:
        """Subscribe to the discovery.

        :param args: Additional positional arguments.
        :param kwargs: Additional named arguments.
        :return: This method does not return anything.
        """
        self._is_subscribed = True

    async def unsubscribe(self, *args, **kwargs) -> None:
        """Unsubscribe from the discovery.

        :param args: Additional positional arguments.
        :param kwargs: Additional named arguments.
        :return: This method does not return anything.
        """
        self._is_subscribed = False
