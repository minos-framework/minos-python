import asyncio
import functools
import typing as t
from collections import Counter

from aiokafka import AIOKafkaConsumer, ConsumerRebalanceListener
from aiomisc import Service
from kafka.errors import OffsetOutOfRangeError
from minos.common.configuration.config import MinosConfig
from minos.common.logs import log
from minos.common.storage.abstract import MinosStorage
from minos.common.storage.lmdb import MinosStorageLmdb


class MinosLocalState:
    __slots__ = "_counts", "_offsets", "_storage", "_dbname"

    def __init__(self, storage: MinosStorage, db_name="LocalState"):
        self._counts = {}
        self._offsets = {}
        self._dbname = db_name
        self._storage = storage

    def dump_local_state(self):

        for tp in self._counts:
            key = f"{tp.topic}:{tp.partition}"
            state = {
                "last_offset": self._offsets[tp],
                "counts": dict(self._counts[tp])
            }
            actual_state = self._storage.get(self._dbname, key)
            if actual_state is not None:
                self._storage.update(self._dbname, key, state)
            else:
                self._storage.add(self._dbname, key, state)

    def load_local_state(self, partitions):
        self._counts.clear()
        self._offsets.clear()
        for tp in partitions:
            # prepare the default state
            state = {
                "last_offset": -1,  # Non existing, will reset
                "counts": {}
            }
            key = f"{tp.topic}:{tp.partition}"
            returned_val = self._storage.get(self._dbname, key)
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
    __slots__ = "_tasks", "_local_state", "_storage", "_handlers", "_broker_host", "_broker_port", "_broker_group"

    def __init__(self, *, conf: MinosConfig, storage: MinosStorage = MinosStorageLmdb, **kwargs: t.Any):
        self._tasks = set()  # type: t.Set[asyncio.Task]
        self._broker_host = conf.events.broker.host
        self._broker_port = conf.events.broker.port
        self._broker_port = f"{conf.service.name}_group_id"
        self._storage = storage.build(conf.events.database.path)
        self._local_state = MinosLocalState(storage=self._storage, db_name=conf.events.database.name)

        super().__init__(**kwargs)

    def create_task(self, coro: t.Awaitable[t.Any]):
        task = self.loop.create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.remove)

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
                # check if the topic is managed by the handler
                if tp.topic in self._handlers:
                    func_handler = functools.partial(self._handlers[tp.topic])
                    counts = Counter()
                    for msg in msgs:
                        counts[msg.key] += 1

                        func_handler(msg.value)
                    self._local_state.add_counts(tp, counts, msg.offset)


    async def start(self) -> t.Any:
        self.start_event.set()
        log.debug("Event Consumer Manager: Started")
        # start the Service Event Consumer for Kafka
        consumer = AIOKafkaConsumer(loop=self.loop,
                                    enable_auto_commit=False,
                                    auto_offset_reset="none",
                                    group_id=self._broker_group,
                                    bootstrap_servers=f"{self._broker_host}:{self._broker_port}",
                                    key_deserializer=lambda key: key.decode("utf-8") if key else "",)

        await consumer.start()
        # prepare the database interface
        listener = MinosRebalanceListener(consumer, self._local_state)
        topics: t.List = list(self._handlers.keys())
        consumer.subscribe(topics=topics, listener=listener)

        self.create_task(self.save_state_every_second())
        self.create_task(self.handle_message(consumer))
