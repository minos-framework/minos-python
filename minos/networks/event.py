import asyncio
import typing as t
from collections import Counter

from aiokafka import AIOKafkaConsumer, ConsumerRebalanceListener
from aiomisc import Service
from kafka.errors import OffsetOutOfRangeError
from minos.common.logs import log
from minos.common.storage.abstract import MinosStorage
from minos.common.storage.lmdb import MinosStorageLmdb


class MinosLocalState:
    __slots__ = "_counts", "_offsets", "_storage"

    def __init__(self, storage: MinosStorage):
        self._counts = {}
        self._offsets = {}
        self._storage = storage

    def dump_local_state(self):
        db_name = "LocalState"
        for tp in self._counts:
            key = f"{tp.topic}:{tp.partition}"
            state = {
                "last_offset": self._offsets[tp],
                "counts": dict(self._counts[tp])
            }
            actual_state = self._storage.get(db_name, key)
            if actual_state is not None:
                self._storage.update(db_name, key, state)
            else:
                self._storage.add(db_name, key, state)

    def load_local_state(self, partitions):
        db_name = "LocalState"
        self._counts.clear()
        self._offsets.clear()
        for tp in partitions:
            # prepare the default state
            state = {
                "last_offset": -1,  # Non existing, will reset
                "counts": {}
            }
            key = f"{tp.topic}:{tp.partition}"
            returned_val = self._storage.get(db_name, key)
            if returned_val is not None:
                state = returned_val
            self._counts[tp] = Counter(state['counts'])
            self._offsets[tp] = state['last_offset']

    def discard_state(self, tps):
        """
        reset the entire memory database with the default values
        """
        for tp in tps:
            self._offsets[tp] = -1
            self._counts[tp] = Counter()

    def get_last_offset(self, tp):
        return self._offsets[tp]

    def add_counts(self, tp, counts, las_offset):
        self._counts[tp] += counts
        self._offsets[tp] = las_offset


class MinosRebalanceListener(ConsumerRebalanceListener):

    def __init__(self, consumer, database_state: MinosLocalState):
        self._consumer = consumer
        self._state = database_state

    async def on_partitions_revoked(self, revoked):
        log.debug("KAFKA Consumer: Revoked %s", revoked)
        self._state.dump_local_state()

    async def on_partitions_assigned(self, assigned):
        log.debug("KAFKA Consumer: Assigned %s", assigned)
        self._state.load_local_state(partitions=assigned)
        for tp in assigned:
            last_offset = self._state.get_last_offset(tp)
            if last_offset < 0:
                await self._consumer.seek_to_beginning(tp)
            else:
                self._consumer.seek(tp, last_offset + 1)


class MinosEventServer(Service):
    """
    Event Manager

    Consumer for the Broker ( at the moment only Kafka is supported )

    """
    __slots__ = "tasks", "_conf", "_local_state", "_storage"

    def __init__(self, *, conf: t.Dict, storage: MinosStorage = MinosStorageLmdb, **kwargs: t.Any):
        self.tasks = set()  # type: t.Set[asyncio.Task]
        self._conf = conf
        self._storage = storage.build(self._conf['db_events_path'])
        self._local_state = MinosLocalState(storage=self._storage)

        super().__init__(**kwargs)

    def create_task(self, coro: t.Awaitable[t.Any]):
        task = self.loop.create_task(coro)
        self.tasks.add(task)
        task.add_done_callback(self.tasks.remove)

    async def save_state_every_second(self):
        while True:
            try:
                await asyncio.sleep(1)
            except asyncio.CancelledError:
                break
            self._local_state.dump_local_state()

    async def handle_message(self, consumer: t.Any):
        while True:
            try:
                msg_set = await consumer.getmany(timeout_ms=1000)
            except OffsetOutOfRangeError as err:
                # this is the case that the database is not updated and must be reset
                tps = err.args[0].keys()
                self._local_state.discard_state(tps)
                await consumer.seek_to_beginning(*tps)
                continue
            for tp, msgs in msg_set.items():
                log.debug(f"EVENT Manager topic: {tp}")
                log.debug(f"EVENT Manager msg: {msgs}")
                counts = Counter()
                for msg in msgs:
                    counts[msg.key] += 1
                self._local_state.add_counts(tp, counts, msg.offset)

    async def start(self) -> t.Any:
        self.start_event.set()
        log.debug("Event Consumer Manager: Started")
        # start the Service Event Consumer for Kafka
        consumer = AIOKafkaConsumer(loop=self.loop,
                                    enable_auto_commit=False,
                                    auto_offset_reset="none",
                                    group_id=self._conf['group'],
                                    bootstrap_servers=f"{self._conf['kafka_host']}:{self._conf['kafka_port']}",
                                    key_deserializer=lambda key: key.decode("utf-8") if key else "",)

        await consumer.start()
        # prepare the database interface
        listener = MinosRebalanceListener(consumer, self._local_state)

        consumer.subscribe(topics=self.topics, listener=listener)

        self.create_task(self.save_state_every_second())


        self.create_task(self.handle_message(consumer))
