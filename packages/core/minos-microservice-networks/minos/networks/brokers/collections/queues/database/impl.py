from __future__ import (
    annotations,
)

import logging
from asyncio import (
    CancelledError,
    Event,
    PriorityQueue,
    QueueEmpty,
    TimeoutError,
    create_task,
    wait_for,
)
from contextlib import (
    suppress,
)
from typing import (
    Any,
    Generic,
    NoReturn,
    Optional,
    TypeVar,
)

from cached_property import (
    cached_property,
)

from minos.common import (
    Builder,
    Config,
    DatabaseClient,
    DatabaseMixin,
)

from ....messages import (
    BrokerMessage,
)
from ..abc import (
    BrokerQueue,
)
from .factories import (
    BrokerQueueDatabaseOperationFactory,
)

logger = logging.getLogger(__name__)

GenericBrokerQueueDatabaseOperationFactory = TypeVar(
    "GenericBrokerQueueDatabaseOperationFactory", bound=BrokerQueueDatabaseOperationFactory
)


class DatabaseBrokerQueue(
    BrokerQueue,
    DatabaseMixin[GenericBrokerQueueDatabaseOperationFactory],
    Generic[GenericBrokerQueueDatabaseOperationFactory],
):
    """Database Broker Queue class."""

    _queue: PriorityQueue[_Entry]

    def __init__(
        self,
        *args,
        retry: Optional[int] = None,
        records: Optional[int] = None,
        database_key: Optional[tuple[str]] = None,
        **kwargs,
    ):
        if database_key is None:
            database_key = ("broker",)
        super().__init__(*args, database_key=database_key, **kwargs)

        if retry is None:
            retry = 2
        if records is None:
            records = 1000

        self._retry = retry
        self._records = records

        self._queue = PriorityQueue(maxsize=records)

        self._run_task = None
        self._enqueued_event = Event()

    @property
    def retry(self) -> int:
        """Get the retry value.

        :return: A ``int`` value.
        """
        return self._retry

    @property
    def records(self) -> int:
        """Get the records value.

        :return: A ``int`` value.
        """
        return self._records

    @classmethod
    def _from_config(cls, config: Config, **kwargs) -> DatabaseBrokerQueue:
        broker_interface = config.get_interface_by_name("broker")
        queue_config = broker_interface.get("common", dict()).get("queue", dict())
        database_config = {"database_key": None}

        return cls(**(kwargs | database_config | queue_config))

    async def _setup(self) -> None:
        await super()._setup()
        await self._create_table()
        await self._start_run()

    async def _destroy(self) -> None:
        await self._stop_run()
        await self._flush_queue()
        await super()._destroy()

    async def _create_table(self) -> None:
        operation = self.database_operation_factory.build_create()
        await self.execute_on_database(operation)

    async def _start_run(self) -> None:
        if self._run_task is None:
            self._run_task = create_task(self._run())

    async def _stop_run(self) -> None:
        if self._run_task is not None:
            task = self._run_task
            self._run_task = None

            task.cancel()
            with suppress(TimeoutError, CancelledError):
                await wait_for(task, 0.5)

    async def _flush_queue(self):
        while True:
            try:
                entry = self._queue.get_nowait()
            except QueueEmpty:
                break
            operation = self.database_operation_factory.build_mark_processed(entry.id_)
            await self.execute_on_database(operation)
            self._queue.task_done()

    async def _enqueue(self, message: BrokerMessage) -> None:
        operation = self.database_operation_factory.build_submit(message.topic, message.avro_bytes)
        await self.execute_on_database(operation)
        await self._notify_enqueued(message)

    # noinspection PyUnusedLocal
    async def _notify_enqueued(self, message: BrokerMessage) -> None:
        self._enqueued_event.set()

    async def _dequeue(self) -> BrokerMessage:
        while True:
            entry = await self._queue.get()
            try:
                # noinspection PyBroadException
                try:
                    message = entry.data
                except Exception as exc:
                    logger.warning(
                        f"There was a problem while trying to deserialize the entry with {entry.id_!r} id: {exc}"
                    )
                    operation = self.database_operation_factory.build_mark_processed(entry.id_)
                    await self.execute_on_database(operation)
                    continue

                operation = self.database_operation_factory.build_delete(entry.id_)
                await self.execute_on_database(operation)
                return message
            finally:
                self._queue.task_done()

    async def _run(self, max_wait: Optional[float] = 60.0) -> NoReturn:
        while self._run_task is not None:
            await self._wait_for_entries(max_wait)
            await self._dequeue_batch()

    async def _wait_for_entries(self, max_wait: Optional[float]) -> None:
        while True:
            if await self._get_count():
                return

            with suppress(TimeoutError):
                return await wait_for(self._wait_enqueued(), max_wait)

    async def _wait_enqueued(self) -> None:
        await self._enqueued_event.wait()
        self._enqueued_event.clear()

    async def _get_count(self) -> int:
        # noinspection PyTypeChecker
        operation = self.database_operation_factory.build_count(self.retry)
        row = await self.execute_on_database_and_fetch_one(operation)
        count = row[0]
        return count

    async def _dequeue_batch(self) -> None:
        async with self.database_pool.acquire() as client:
            rows = await self._dequeue_rows(client)

            if not len(rows):
                return

            entries = [_Entry(*row) for row in rows]

            ids = tuple(entry.id_ for entry in entries)
            operation = self.database_operation_factory.build_mark_processing(ids)
            await client.execute(operation)

        for entry in entries:
            await self._queue.put(entry)

    async def _dequeue_rows(self, client: DatabaseClient) -> list[Any]:
        operation = self.database_operation_factory.build_query(self._retry, self._records)
        await client.execute(operation)
        return [row async for row in client.fetch_all()]


class _Entry:
    def __init__(self, id_: int, data_bytes: bytes):
        self.id_ = id_
        self.data_bytes = data_bytes

    @cached_property
    def data(self) -> BrokerMessage:
        """Get the data.

        :return: A ``Model`` inherited instance.
        """
        return BrokerMessage.from_avro_bytes(self.data_bytes)

    def __lt__(self, other: Any) -> bool:
        # noinspection PyBroadException
        try:
            return isinstance(other, type(self)) and self.data < other.data
        except Exception:
            return False


class DatabaseBrokerQueueBuilder(Builder):
    """Database Broker Queue Builder class."""

    def with_config(self, config: Config):
        """Set config.

        :param config: The config to be set.
        :return: This method return the builder instance.
        """
        self.kwargs |= {"database_key": None}
        self.kwargs |= config.get_interface_by_name("broker").get("common", dict()).get("queue", dict())
        return super().with_config(config)


DatabaseBrokerQueue.set_builder(DatabaseBrokerQueueBuilder)
