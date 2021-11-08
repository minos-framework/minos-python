from aiomisc.service.periodic import (
    PeriodicService,
)
from dependency_injector.wiring import (
    Provide,
    inject,
)

from minos.aggregate import (
    SnapshotRepository,
)


class SnapshotService(PeriodicService):
    """Minos Snapshot Service class."""

    @inject
    def __init__(
        self, snapshot_repository: SnapshotRepository = Provide["snapshot_repository"], interval: float = 60, **kwargs
    ):
        super().__init__(interval=interval, **kwargs)

        self.snapshot_repository = snapshot_repository

    async def start(self) -> None:
        """Start the service execution.

        :return: This method does not return anything.
        """
        await self.snapshot_repository.setup()
        await super().start()

    async def callback(self) -> None:
        """Callback implementation to be executed periodically.

        :return: This method does not return anything.
        """
        await self.snapshot_repository.synchronize()

    async def stop(self, err: Exception = None) -> None:
        """Stop the service execution.

        :param err: Optional exception that stopped the execution.
        :return: This method does not return anything.
        """
        await super().stop(err)
        await self.snapshot_repository.destroy()
