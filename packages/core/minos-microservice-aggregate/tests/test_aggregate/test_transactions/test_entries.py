import unittest
from datetime import (
    datetime,
)
from unittest.mock import (
    AsyncMock,
    MagicMock,
    call,
    patch,
)
from uuid import (
    UUID,
    uuid4,
)

from minos.aggregate import (
    TRANSACTION_CONTEXT_VAR,
    TransactionalMixin,
    TransactionEntry,
    TransactionRepositoryConflictException,
    TransactionStatus,
)
from minos.common import (
    NULL_UUID,
)
from tests.utils import (
    AggregateTestCase,
    FakeAsyncIterator,
)


class TestTransactionEntry(AggregateTestCase):
    def test_constructor(self):
        transaction = TransactionEntry()

        self.assertIsInstance(transaction.uuid, UUID)
        self.assertEqual(TransactionStatus.PENDING, transaction.status)
        self.assertEqual(True, transaction._autocommit)

        self.assertEqual(self.transaction_repository, transaction.repository)

    def test_constructor_extended(self):
        uuid = uuid4()
        status = TransactionStatus.PENDING
        transaction = TransactionEntry(uuid, status, autocommit=False)
        self.assertEqual(uuid, transaction.uuid)
        self.assertEqual(status, transaction.status)
        self.assertEqual(False, transaction._autocommit)

        self.assertEqual(self.transaction_repository, transaction.repository)

    def test_constructor_raw_status(self):
        transaction = TransactionEntry(status="pending")
        self.assertEqual(TransactionStatus.PENDING, transaction.status)

    async def test_async_context_manager_with_context_var(self):
        self.assertEqual(TRANSACTION_CONTEXT_VAR.get(), None)

        async with TransactionEntry() as transaction:
            self.assertEqual(TRANSACTION_CONTEXT_VAR.get(), transaction)

        self.assertEqual(TRANSACTION_CONTEXT_VAR.get(), None)

    async def test_async_context_manager(self):
        with patch.object(TransactionEntry, "save") as save_mock, patch.object(
            TransactionEntry, "commit"
        ) as commit_mock:
            async with TransactionEntry():
                self.assertEqual(1, save_mock.call_count)
                self.assertEqual(0, commit_mock.call_count)

            self.assertEqual(1, commit_mock.call_count)

    async def test_async_context_manager_without_autocommit(self):
        with patch.object(TransactionEntry, "commit") as commit_mock:
            async with TransactionEntry(autocommit=False) as transaction:
                self.assertEqual(0, commit_mock.call_count)
                transaction.status = TransactionStatus.PENDING

            self.assertEqual(0, commit_mock.call_count)

    async def test_async_context_manager_raises(self):
        with self.assertRaises(ValueError):
            async with TransactionEntry(status=TransactionStatus.COMMITTED):
                pass

        with self.assertRaises(ValueError):
            async with TransactionEntry(status=TransactionStatus.RESERVED):
                pass

        with self.assertRaises(ValueError):
            async with TransactionEntry(status=TransactionStatus.REJECTED):
                pass

        with self.assertRaises(ValueError):
            async with TransactionEntry(destination_uuid=uuid4()):
                pass

        transaction = TransactionEntry()
        TRANSACTION_CONTEXT_VAR.set(TransactionEntry())
        with self.assertRaises(ValueError):
            async with transaction:
                pass

    async def test_reserve_success(self) -> None:
        uuid = uuid4()

        transaction = TransactionEntry(uuid, TransactionStatus.PENDING)

        with patch.object(TransactionEntry, "save") as save_mock, patch.object(
            TransactionEntry, "validate", return_value=True
        ) as validate_mock:
            await transaction.reserve()

        self.assertEqual(1, validate_mock.call_count)
        self.assertEqual(call(), validate_mock.call_args)

        self.assertEqual(
            [call(status=TransactionStatus.RESERVING), call(status=TransactionStatus.RESERVED)],
            save_mock.call_args_list,
        )

    async def test_reserve_failure(self) -> None:
        uuid = uuid4()
        transaction = TransactionEntry(uuid, TransactionStatus.PENDING)

        with patch.object(TransactionEntry, "save") as save_mock, patch.object(
            TransactionEntry, "validate", return_value=False
        ) as validate_mock:
            with self.assertRaises(TransactionRepositoryConflictException):
                await transaction.reserve()

        self.assertEqual(1, validate_mock.call_count)
        self.assertEqual(call(), validate_mock.call_args)

        self.assertEqual(
            [call(status=TransactionStatus.RESERVING), call(status=TransactionStatus.REJECTED)],
            save_mock.call_args_list,
        )

    async def test_reserve_raises(self) -> None:
        with self.assertRaises(ValueError):
            await TransactionEntry(status=TransactionStatus.RESERVED).reserve()
        with self.assertRaises(ValueError):
            await TransactionEntry(status=TransactionStatus.COMMITTED).reserve()
        with self.assertRaises(ValueError):
            await TransactionEntry(status=TransactionStatus.REJECTED).reserve()

    async def test_validate_true(self):
        uuid = uuid4()
        another = uuid4()

        observer_mock = AsyncMock(return_value={another})
        select_transaction_mock = MagicMock(return_value=FakeAsyncIterator([]))

        self.event_repository.get_related_transactions = observer_mock
        self.transaction_repository.select = select_transaction_mock

        transaction = TransactionEntry(uuid)

        self.assertTrue(await transaction.validate())

        self.assertEqual([call(transaction_uuid=uuid)], observer_mock.call_args_list)
        self.assertEqual(
            [
                call(
                    uuid=NULL_UUID,
                    status_in=(
                        TransactionStatus.RESERVING,
                        TransactionStatus.RESERVED,
                        TransactionStatus.COMMITTING,
                        TransactionStatus.COMMITTED,
                        TransactionStatus.REJECTED,
                    ),
                ),
                call(
                    destination_uuid=NULL_UUID,
                    uuid_in=(another,),
                    status_in=(
                        TransactionStatus.RESERVING,
                        TransactionStatus.RESERVED,
                        TransactionStatus.COMMITTING,
                        TransactionStatus.COMMITTED,
                    ),
                ),
            ],
            select_transaction_mock.call_args_list,
        )

    async def test_validate_false_destination_already(self):
        uuid = uuid4()
        another = uuid4()

        transaction_event_1 = [TransactionEntry(another, TransactionStatus.RESERVED)]

        select_event_mock = AsyncMock(return_value={another})
        select_transaction_mock = MagicMock(return_value=FakeAsyncIterator(transaction_event_1))

        self.event_repository.get_related_transactions = select_event_mock
        self.transaction_repository.select = select_transaction_mock

        transaction = TransactionEntry(uuid, destination_uuid=another)

        self.assertFalse(await transaction.validate())

        self.assertEqual([], select_event_mock.call_args_list)
        self.assertEqual(
            [
                call(
                    uuid=another,
                    status_in=(
                        TransactionStatus.RESERVING,
                        TransactionStatus.RESERVED,
                        TransactionStatus.COMMITTING,
                        TransactionStatus.COMMITTED,
                        TransactionStatus.REJECTED,
                    ),
                )
            ],
            select_transaction_mock.call_args_list,
        )

    async def test_validate_false_already_committed(self):
        uuid = uuid4()

        select_transaction_1 = []

        select_event_mock = AsyncMock(return_value={NULL_UUID})
        select_transaction_mock = MagicMock(
            side_effect=[FakeAsyncIterator([]), FakeAsyncIterator(select_transaction_1)],
        )

        self.event_repository.get_related_transactions = select_event_mock
        self.transaction_repository.select = select_transaction_mock

        transaction = TransactionEntry(uuid)

        self.assertFalse(await transaction.validate())

        self.assertEqual([call(transaction_uuid=uuid)], select_event_mock.call_args_list)
        self.assertEqual(
            [
                call(
                    uuid=NULL_UUID,
                    status_in=(
                        TransactionStatus.RESERVING,
                        TransactionStatus.RESERVED,
                        TransactionStatus.COMMITTING,
                        TransactionStatus.COMMITTED,
                        TransactionStatus.REJECTED,
                    ),
                ),
            ],
            select_transaction_mock.call_args_list,
        )

    async def test_validate_false_already_reserved(self):
        uuid = uuid4()
        another = uuid4()

        select_transaction_1 = [TransactionEntry(another, TransactionStatus.RESERVED)]

        select_event_mock = AsyncMock(return_value={another})
        select_transaction_mock = MagicMock(
            side_effect=[FakeAsyncIterator([]), FakeAsyncIterator(select_transaction_1)],
        )

        self.event_repository.get_related_transactions = select_event_mock
        self.transaction_repository.select = select_transaction_mock

        transaction = TransactionEntry(uuid)

        self.assertFalse(await transaction.validate())

        self.assertEqual([call(transaction_uuid=uuid)], select_event_mock.call_args_list)
        self.assertEqual(
            [
                call(
                    uuid=NULL_UUID,
                    status_in=(
                        TransactionStatus.RESERVING,
                        TransactionStatus.RESERVED,
                        TransactionStatus.COMMITTING,
                        TransactionStatus.COMMITTED,
                        TransactionStatus.REJECTED,
                    ),
                ),
                call(
                    destination_uuid=NULL_UUID,
                    uuid_in=(another,),
                    status_in=(
                        TransactionStatus.RESERVING,
                        TransactionStatus.RESERVED,
                        TransactionStatus.COMMITTING,
                        TransactionStatus.COMMITTED,
                    ),
                ),
            ],
            select_transaction_mock.call_args_list,
        )

    async def test_reject(self) -> None:
        uuid = uuid4()

        transaction = TransactionEntry(uuid, TransactionStatus.RESERVED)

        with patch.object(TransactionalMixin, "reject_transaction") as observable_mock, patch.object(
            TransactionEntry, "save"
        ) as save_mock:
            await transaction.reject()

        self.assertEqual([call(transaction_uuid=transaction.uuid)], observable_mock.call_args_list)
        self.assertEqual([call(status=TransactionStatus.REJECTED)], save_mock.call_args_list)

    async def test_reject_raises(self) -> None:
        with self.assertRaises(ValueError):
            await TransactionEntry(status=TransactionStatus.COMMITTED).reject()
        with self.assertRaises(ValueError):
            await TransactionEntry(status=TransactionStatus.REJECTED).reject()

    async def test_commit(self) -> None:
        uuid = uuid4()
        another = uuid4()

        commit_transaction = AsyncMock()

        self.event_repository.commit_transaction = commit_transaction

        transaction = TransactionEntry(uuid, TransactionStatus.RESERVED, destination_uuid=another)

        with patch.object(TransactionEntry, "save") as save_mock:
            await transaction.commit()

        self.assertEqual(
            [call(transaction_uuid=uuid, destination_transaction_uuid=another)],
            commit_transaction.call_args_list,
        )

        self.assertEqual(
            [call(status=TransactionStatus.COMMITTING), call(status=TransactionStatus.COMMITTED)],
            save_mock.call_args_list,
        )

    async def test_commit_pending(self) -> None:
        uuid = uuid4()

        transaction = TransactionEntry(uuid, TransactionStatus.PENDING)

        async def _fn():
            transaction.status = TransactionStatus.RESERVED

        with patch.object(TransactionEntry, "reserve", side_effect=_fn) as reserve_mock, patch.object(
            TransactionEntry, "save"
        ) as save_mock, patch.object(TransactionEntry, "_commit", return_value=True) as commit_mock:
            await transaction.commit()

        self.assertEqual(1, reserve_mock.call_count)

        self.assertEqual(1, commit_mock.call_count)
        self.assertEqual(call(), commit_mock.call_args)

        self.assertEqual(
            [call(status=TransactionStatus.COMMITTING), call(status=TransactionStatus.COMMITTED)],
            save_mock.call_args_list,
        )

    async def test_commit_raises(self) -> None:
        with self.assertRaises(ValueError):
            await TransactionEntry(status=TransactionStatus.COMMITTED).commit()
        with self.assertRaises(ValueError):
            await TransactionEntry(status=TransactionStatus.REJECTED).commit()

    async def test_save(self) -> None:
        uuid = uuid4()
        submit_mock = AsyncMock()
        self.transaction_repository.submit = submit_mock

        await TransactionEntry(uuid, TransactionStatus.PENDING).save()
        self.assertEqual(1, submit_mock.call_count)
        self.assertEqual(call(TransactionEntry(uuid, TransactionStatus.PENDING)), submit_mock.call_args)

        submit_mock.reset_mock()
        await TransactionEntry(uuid, TransactionStatus.PENDING).save(status=TransactionStatus.COMMITTED)
        self.assertEqual(1, submit_mock.call_count)
        self.assertEqual(call(TransactionEntry(uuid, TransactionStatus.COMMITTED)), submit_mock.call_args)

    async def test_uuids(self):
        first = TransactionEntry()
        await first.save()

        second = TransactionEntry(destination_uuid=first.uuid)
        await second.save()

        self.assertEqual((NULL_UUID, first.uuid, second.uuid), await second.uuids)

    async def test_destination(self):
        first = TransactionEntry()
        await first.save()

        second = TransactionEntry(destination_uuid=first.uuid)
        self.assertEqual(first, await second.destination)

    def test_equals(self):
        uuid = uuid4()
        base = TransactionEntry(uuid, TransactionStatus.PENDING)
        self.assertEqual(TransactionEntry(uuid, TransactionStatus.PENDING), base)
        self.assertNotEqual(TransactionEntry(uuid4(), TransactionStatus.PENDING), base)
        self.assertNotEqual(TransactionEntry(uuid, TransactionStatus.COMMITTED), base)

    def test_iter(self):
        uuid = uuid4()
        destination_uuid = uuid4()
        transaction = TransactionEntry(uuid, TransactionStatus.PENDING, destination_uuid)
        self.assertEqual([uuid, TransactionStatus.PENDING, destination_uuid], list(transaction))

    def test_repr(self):
        uuid = uuid4()
        destination_uuid = uuid4()
        transaction = TransactionEntry(uuid, TransactionStatus.PENDING, destination_uuid)
        expected = (
            f"TransactionEntry(uuid={uuid!r}, status={TransactionStatus.PENDING!r}, "
            f"destination_uuid={destination_uuid!r}, updated_at={None!r})"
        )
        self.assertEqual(expected, repr(transaction))

    def test_as_raw(self):
        uuid = uuid4()
        status = TransactionStatus.PENDING
        updated_at = datetime(2020, 10, 13, 8, 45, 32)
        destination_uuid = uuid4()

        entry = TransactionEntry(uuid, status, destination_uuid, updated_at)
        expected = {
            "uuid": uuid,
            "status": status,
            "destination_uuid": destination_uuid,
            "updated_at": updated_at,
        }

        self.assertEqual(expected, entry.as_raw())


class TestTransactionStatus(unittest.TestCase):
    def test_value_of_created(self):
        self.assertEqual(TransactionStatus.PENDING, TransactionStatus.value_of("pending"))

    def test_value_of_reserved(self):
        self.assertEqual(TransactionStatus.RESERVED, TransactionStatus.value_of("reserved"))

    def test_value_of_committed(self):
        self.assertEqual(TransactionStatus.COMMITTED, TransactionStatus.value_of("committed"))

    def test_value_of_rejected(self):
        self.assertEqual(TransactionStatus.REJECTED, TransactionStatus.value_of("rejected"))

    def test_value_of_raises(self):
        with self.assertRaises(ValueError):
            TransactionStatus.value_of("foo")


if __name__ == "__main__":
    unittest.main()
