from abc import (
    ABC,
    abstractmethod,
)
from datetime import (
    datetime,
    timedelta,
    timezone,
)
from unittest.mock import (
    patch,
)
from uuid import (
    UUID,
    uuid4,
)

from minos.aggregate import (
    Action,
    EventEntry,
    EventRepository,
    EventRepositoryConflictException,
    EventRepositoryException,
    FieldDiffContainer,
    TransactionEntry,
)
from minos.common import (
    NULL_UUID,
    current_datetime,
)
from minos.common.testing import (
    MinosTestCase,
)


class EventRepositoryTestCase(MinosTestCase, ABC):
    __test__ = False

    def setUp(self) -> None:
        super().setUp()
        self.event_repository = self.build_event_repository()
        self.field_diff_container_patcher = patch(
            "minos.aggregate.FieldDiffContainer.from_avro_bytes", return_value=FieldDiffContainer.empty()
        )
        self.field_diff_container_patcher.start()

        self.uuid = uuid4()
        self.uuid_1 = uuid4()
        self.uuid_2 = uuid4()
        self.uuid_4 = uuid4()

        self.first_transaction = uuid4()
        self.second_transaction = uuid4()

        self.entries = [
            EventEntry(
                self.uuid_1,
                "example.Car",
                1,
                bytes("foo", "utf-8"),
                1,
                Action.CREATE,
                current_datetime(),
            ),
            EventEntry(
                self.uuid_1,
                "example.Car",
                2,
                bytes("bar", "utf-8"),
                2,
                Action.UPDATE,
                current_datetime(),
            ),
            EventEntry(
                self.uuid_2,
                "example.Car",
                1,
                bytes("hello", "utf-8"),
                3,
                Action.CREATE,
                current_datetime(),
            ),
            EventEntry(
                self.uuid_1,
                "example.Car",
                3,
                bytes("foobar", "utf-8"),
                4,
                Action.UPDATE,
                current_datetime(),
            ),
            EventEntry(
                self.uuid_1,
                "example.Car",
                4,
                bytes(),
                5,
                Action.DELETE,
                current_datetime(),
            ),
            EventEntry(
                self.uuid_2,
                "example.Car",
                2,
                bytes("bye", "utf-8"),
                6,
                Action.UPDATE,
                current_datetime(),
            ),
            EventEntry(
                self.uuid_4,
                "example.MotorCycle",
                1,
                bytes("one", "utf-8"),
                7,
                Action.CREATE,
                current_datetime(),
            ),
            EventEntry(
                self.uuid_2,
                "example.Car",
                3,
                bytes("hola", "utf-8"),
                8,
                Action.UPDATE,
                current_datetime(),
                transaction_uuid=self.first_transaction,
            ),
            EventEntry(
                self.uuid_2,
                "example.Car",
                3,
                bytes("salut", "utf-8"),
                9,
                Action.UPDATE,
                current_datetime(),
                transaction_uuid=self.second_transaction,
            ),
            EventEntry(
                self.uuid_2,
                "example.Car",
                4,
                bytes("adios", "utf-8"),
                10,
                Action.UPDATE,
                current_datetime(),
                transaction_uuid=self.first_transaction,
            ),
        ]

    async def asyncSetUp(self):
        await super().asyncSetUp()
        await self.event_repository.setup()

    async def asyncTearDown(self):
        await self.event_repository.destroy()
        await super().asyncTearDown()

    def tearDown(self):
        self.field_diff_container_patcher.stop()
        super().tearDown()

    async def populate(self) -> None:
        await self.transaction_repository.submit(TransactionEntry(self.first_transaction))
        await self.transaction_repository.submit(TransactionEntry(self.second_transaction))

        await self.event_repository.create(EventEntry(self.uuid_1, "example.Car", 1, bytes("foo", "utf-8")))
        await self.event_repository.update(EventEntry(self.uuid_1, "example.Car", 2, bytes("bar", "utf-8")))
        await self.event_repository.create(EventEntry(self.uuid_2, "example.Car", 1, bytes("hello", "utf-8")))
        await self.event_repository.update(EventEntry(self.uuid_1, "example.Car", 3, bytes("foobar", "utf-8")))
        await self.event_repository.delete(EventEntry(self.uuid_1, "example.Car", 4))
        await self.event_repository.update(EventEntry(self.uuid_2, "example.Car", 2, bytes("bye", "utf-8")))
        await self.event_repository.create(EventEntry(self.uuid_4, "example.MotorCycle", 1, bytes("one", "utf-8")))
        await self.event_repository.update(
            EventEntry(self.uuid_2, "example.Car", 3, bytes("hola", "utf-8"), transaction_uuid=self.first_transaction)
        )
        await self.event_repository.update(
            EventEntry(self.uuid_2, "example.Car", 3, bytes("salut", "utf-8"), transaction_uuid=self.second_transaction)
        )
        await self.event_repository.update(
            EventEntry(self.uuid_2, "example.Car", 4, bytes("adios", "utf-8"), transaction_uuid=self.first_transaction)
        )

    @abstractmethod
    def build_event_repository(self) -> EventRepository:
        """For testing purposes."""

    def assert_equal_repository_entries(self, expected: list[EventEntry], observed: list[EventEntry]) -> None:
        """For testing purposes."""

        self.assertEqual(len(expected), len(observed))

        for e, o in zip(expected, observed):
            self.assertEqual(type(e), type(o))
            self.assertEqual(e.uuid, o.uuid)
            self.assertEqual(e.name, o.name)
            self.assertEqual(e.version, o.version)
            self.assertEqual(e.data, o.data)
            self.assertEqual(e.id, o.id)
            self.assertEqual(e.action, o.action)
            self.assertAlmostEqual(e.created_at or current_datetime(), o.created_at, delta=timedelta(seconds=5))

    async def test_generate_uuid(self):
        await self.event_repository.create(EventEntry(NULL_UUID, "example.Car", 1, bytes("foo", "utf-8")))
        observed = [v async for v in self.event_repository.select()]
        self.assertEqual(1, len(observed))
        self.assertIsInstance(observed[0].uuid, UUID)
        self.assertNotEqual(NULL_UUID, observed[0].uuid)

    async def test_submit(self):
        await self.event_repository.submit(EventEntry(self.uuid, "example.Car", action=Action.CREATE))
        expected = [EventEntry(self.uuid, "example.Car", 1, bytes(), 1, Action.CREATE)]
        observed = [v async for v in self.event_repository.select()]
        self.assert_equal_repository_entries(expected, observed)

    async def test_submit_with_version(self):
        await self.event_repository.submit(EventEntry(self.uuid, "example.Car", version=3, action=Action.CREATE))
        expected = [EventEntry(self.uuid, "example.Car", 3, bytes(), 1, Action.CREATE)]
        observed = [v async for v in self.event_repository.select()]
        self.assert_equal_repository_entries(expected, observed)

    async def test_submit_with_created_at(self):
        created_at = datetime(2021, 10, 25, 8, 30, tzinfo=timezone.utc)
        await self.event_repository.submit(
            EventEntry(self.uuid, "example.Car", created_at=created_at, action=Action.CREATE)
        )
        expected = [EventEntry(self.uuid, "example.Car", 1, bytes(), 1, Action.CREATE, created_at=created_at)]
        observed = [v async for v in self.event_repository.select()]
        self.assert_equal_repository_entries(expected, observed)

    async def test_submit_raises_duplicate(self):
        await self.event_repository.submit(EventEntry(self.uuid, "example.Car", 1, action=Action.CREATE))
        with self.assertRaises(EventRepositoryConflictException):
            await self.event_repository.submit(EventEntry(self.uuid, "example.Car", 1, action=Action.CREATE))

    async def test_submit_raises_no_action(self):
        with self.assertRaises(EventRepositoryException):
            await self.event_repository.submit(EventEntry(self.uuid, "example.Car", 1, "foo".encode()))

    async def test_select_empty(self):
        self.assertEqual([], [v async for v in self.event_repository.select()])

    async def test_offset(self):
        self.assertEqual(0, await self.event_repository.offset)
        await self.event_repository.submit(EventEntry(self.uuid, "example.Car", version=3, action=Action.CREATE))
        self.assertEqual(1, await self.event_repository.offset)

    async def test_select(self):
        await self.populate()
        expected = self.entries
        observed = [v async for v in self.event_repository.select()]
        self.assert_equal_repository_entries(expected, observed)

    async def test_select_id(self):
        await self.populate()
        expected = [self.entries[1]]
        observed = [v async for v in self.event_repository.select(id=2)]
        self.assert_equal_repository_entries(expected, observed)

    async def test_select_id_lt(self):
        await self.populate()
        expected = self.entries[:4]
        observed = [v async for v in self.event_repository.select(id_lt=5)]
        self.assert_equal_repository_entries(expected, observed)

    async def test_select_id_gt(self):
        await self.populate()
        expected = self.entries[4:]
        observed = [v async for v in self.event_repository.select(id_gt=4)]
        self.assert_equal_repository_entries(expected, observed)

    async def test_select_id_le(self):
        await self.populate()
        expected = self.entries[:4]
        observed = [v async for v in self.event_repository.select(id_le=4)]
        self.assert_equal_repository_entries(expected, observed)

    async def test_select_id_ge(self):
        await self.populate()
        expected = self.entries[4:]
        observed = [v async for v in self.event_repository.select(id_ge=5)]
        self.assert_equal_repository_entries(expected, observed)

    async def test_select_uuid(self):
        await self.populate()
        expected = [self.entries[2], self.entries[5], self.entries[7], self.entries[8], self.entries[9]]
        observed = [v async for v in self.event_repository.select(uuid=self.uuid_2)]
        self.assert_equal_repository_entries(expected, observed)

    async def test_select_name(self):
        await self.populate()
        expected = [self.entries[6]]
        observed = [v async for v in self.event_repository.select(name="example.MotorCycle")]
        self.assert_equal_repository_entries(expected, observed)

    async def test_select_version(self):
        await self.populate()
        expected = [self.entries[4], self.entries[9]]
        observed = [v async for v in self.event_repository.select(version=4)]
        self.assert_equal_repository_entries(expected, observed)

    async def test_select_version_lt(self):
        await self.populate()
        expected = [self.entries[0], self.entries[2], self.entries[6]]
        observed = [v async for v in self.event_repository.select(version_lt=2)]
        self.assert_equal_repository_entries(expected, observed)

    async def test_select_version_gt(self):
        await self.populate()
        expected = [
            self.entries[1],
            self.entries[3],
            self.entries[4],
            self.entries[5],
            self.entries[7],
            self.entries[8],
            self.entries[9],
        ]
        observed = [v async for v in self.event_repository.select(version_gt=1)]
        self.assert_equal_repository_entries(expected, observed)

    async def test_select_version_le(self):
        await self.populate()
        expected = [self.entries[0], self.entries[2], self.entries[6]]
        observed = [v async for v in self.event_repository.select(version_le=1)]
        self.assert_equal_repository_entries(expected, observed)

    async def test_select_version_ge(self):
        await self.populate()
        expected = [
            self.entries[1],
            self.entries[3],
            self.entries[4],
            self.entries[5],
            self.entries[7],
            self.entries[8],
            self.entries[9],
        ]
        observed = [v async for v in self.event_repository.select(version_ge=2)]
        self.assert_equal_repository_entries(expected, observed)

    async def test_select_transaction_uuid_null(self):
        await self.populate()
        expected = self.entries[:7]
        observed = [v async for v in self.event_repository.select(transaction_uuid=NULL_UUID)]
        self.assert_equal_repository_entries(expected, observed)

    async def test_select_transaction_uuid(self):
        await self.populate()
        expected = [self.entries[7], self.entries[9]]
        observed = [v async for v in self.event_repository.select(transaction_uuid=self.first_transaction)]
        self.assert_equal_repository_entries(expected, observed)

    async def test_select_transaction_uuid_ne(self):
        await self.populate()
        expected = [self.entries[7], self.entries[8], self.entries[9]]
        observed = [v async for v in self.event_repository.select(transaction_uuid_ne=NULL_UUID)]
        self.assert_equal_repository_entries(expected, observed)

    async def test_select_transaction_uuid_in(self):
        await self.populate()
        expected = [self.entries[7], self.entries[8], self.entries[9]]
        observed = [
            v
            async for v in self.event_repository.select(
                transaction_uuid_in=(self.first_transaction, self.second_transaction)
            )
        ]
        self.assert_equal_repository_entries(expected, observed)

    async def test_select_combined(self):
        await self.populate()
        expected = [self.entries[2], self.entries[5], self.entries[7], self.entries[8], self.entries[9]]
        observed = [v async for v in self.event_repository.select(name="example.Car", uuid=self.uuid_2)]
        self.assert_equal_repository_entries(expected, observed)
