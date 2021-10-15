import unittest
import warnings
from shutil import (
    rmtree,
)
from unittest.mock import (
    MagicMock,
)

from minos.saga import (
    AlreadyCommittedException,
    AlreadyOnSagaException,
    LocalSagaStep,
    RemoteSagaStep,
    Saga,
    SagaException,
    SagaExecution,
    SagaOperation,
    SagaStep,
    identity_fn,
)
from tests.utils import (
    ADD_ORDER,
    BASE_PATH,
    commit_callback,
    create_payment,
    send_create_order,
    send_delete_order,
    send_delete_ticket,
)


class TestSaga(unittest.TestCase):
    DB_PATH = BASE_PATH / "test_db.lmdb"

    # noinspection PyMissingOrEmptyDocstring
    def tearDown(self) -> None:
        rmtree(self.DB_PATH, ignore_errors=True)

    def test_commit_constructor(self):
        saga = Saga(commit=identity_fn)
        self.assertEqual(SagaOperation(identity_fn), saga.commit_operation)

    def test_commit(self):
        saga = Saga()
        observed = saga.commit()
        self.assertEqual(saga, observed)
        self.assertEqual(SagaOperation(identity_fn), saga.commit_operation)

    def test_commit_define_callback(self):
        saga = Saga()
        observed = saga.commit(commit_callback)
        self.assertEqual(saga, observed)
        self.assertEqual(SagaOperation(commit_callback), saga.commit_operation)

    def test_commit_raises(self):
        saga = Saga().commit()
        with self.assertRaises(AlreadyCommittedException):
            saga.commit()

    def test_committed_true(self):
        saga = Saga()
        saga.commit_operation = SagaOperation(identity_fn)
        self.assertTrue(saga.committed)

    def test_committed_false(self):
        saga = Saga()
        self.assertFalse(saga.committed)

    def test_local(self):
        saga = Saga()
        initial = LocalSagaStep()
        step = saga.local(initial)
        self.assertEqual(step, initial)
        self.assertEqual(saga, step.saga)

    def test_local_operation(self):
        saga = Saga()
        step = saga.local(SagaOperation(create_payment))
        self.assertEqual(LocalSagaStep(on_execute=SagaOperation(create_payment)), step)
        self.assertEqual(saga, step.saga)

    def test_local_callback(self):
        saga = Saga()
        step = saga.local(create_payment)
        self.assertEqual(LocalSagaStep(on_execute=SagaOperation(create_payment)), step)
        self.assertEqual(saga, step.saga)

    def test_local_empty(self):
        saga = Saga()
        step = saga.local()
        self.assertIsInstance(step, SagaStep)
        self.assertEqual(saga, step.saga)

    def test_local_raises(self):
        saga = Saga().commit()
        with self.assertRaises(AlreadyCommittedException):
            saga.local()

        saga = Saga()
        with self.assertRaises(TypeError):
            # noinspection PyTypeChecker
            saga.local(RemoteSagaStep())

    def test_step(self):
        saga = Saga()
        mock = MagicMock(side_effect=saga.remote)
        saga.remote = mock
        with warnings.catch_warnings():
            step = saga.step()

        self.assertEqual(1, mock.call_count)
        self.assertIsInstance(step, RemoteSagaStep)

    def test_remote(self):
        saga = Saga()
        initial = RemoteSagaStep()
        step = saga.remote(initial)
        self.assertEqual(step, initial)
        self.assertEqual(saga, step.saga)

    def test_remote_operation(self):
        saga = Saga()
        step = saga.remote(SagaOperation(send_delete_ticket))
        self.assertEqual(RemoteSagaStep(on_execute=SagaOperation(send_delete_ticket)), step)
        self.assertEqual(saga, step.saga)

    def test_remote_callback(self):
        saga = Saga()
        step = saga.remote(send_delete_ticket)
        self.assertEqual(RemoteSagaStep(on_execute=SagaOperation(send_delete_ticket)), step)
        self.assertEqual(saga, step.saga)

    def test_remote_empty(self):
        saga = Saga()
        step = saga.remote()
        self.assertIsInstance(step, SagaStep)
        self.assertEqual(saga, step.saga)

    def test_remote_raises(self):
        saga = Saga().commit()
        with self.assertRaises(AlreadyCommittedException):
            saga.remote()

        saga = Saga()
        with self.assertRaises(TypeError):
            # noinspection PyTypeChecker
            saga.remote(LocalSagaStep())

    def test_empty_step_raises(self):
        with self.assertRaises(SagaException):
            Saga().remote(send_create_order).on_failure(send_delete_order).remote().commit()

    def test_duplicate_operation_raises(self):
        with self.assertRaises(SagaException):
            Saga().remote(send_create_order).on_failure(send_delete_order).on_failure(send_delete_ticket).commit()

    def test_missing_send_raises(self):
        with self.assertRaises(SagaException):
            Saga().remote().on_failure(send_delete_ticket).commit()

    def test_build_execution(self):
        saga = Saga().remote(send_create_order).on_failure(send_delete_order).commit()
        execution = SagaExecution.from_saga(saga)
        self.assertIsInstance(execution, SagaExecution)

    def test_add_step(self):
        step = RemoteSagaStep(send_create_order)
        saga = Saga().remote(step).commit()

        self.assertEqual([step], saga.steps)

    def test_add_step_raises(self):
        step = RemoteSagaStep(send_create_order, saga=Saga())
        with self.assertRaises(AlreadyOnSagaException):
            Saga().remote(step)

    def test_raw(self):
        saga = ADD_ORDER
        expected = {
            "commit": {"callback": "minos.saga.definitions.operations.identity_fn"},
            "steps": [
                {
                    "cls": "minos.saga.definitions.steps.remote.RemoteSagaStep",
                    "on_execute": {"callback": "tests.utils.send_create_order"},
                    "on_success": {"callback": "tests.utils.handle_order_success"},
                    "on_error": None,
                    "on_failure": {"callback": "tests.utils.send_delete_order"},
                },
                {
                    "cls": "minos.saga.definitions.steps.local.LocalSagaStep",
                    "on_execute": {"callback": "tests.utils.create_payment"},
                    "on_success": None,
                    "on_error": None,
                    "on_failure": {"callback": "tests.utils.delete_payment"},
                },
                {
                    "cls": "minos.saga.definitions.steps.remote.RemoteSagaStep",
                    "on_execute": {"callback": "tests.utils.send_create_ticket"},
                    "on_success": {"callback": "tests.utils.handle_ticket_success"},
                    "on_error": {"callback": "tests.utils.handle_ticket_error"},
                    "on_failure": {"callback": "tests.utils.send_delete_ticket"},
                },
            ],
        }
        self.assertEqual(expected, saga.raw)

    def test_from_raw(self):
        raw = {
            "commit": {"callback": "minos.saga.definitions.operations.identity_fn"},
            "steps": [
                {
                    "cls": "minos.saga.definitions.steps.remote.RemoteSagaStep",
                    "on_execute": {"callback": "tests.utils.send_create_order"},
                    "on_success": {"callback": "tests.utils.handle_order_success"},
                    "on_error": None,
                    "on_failure": {"callback": "tests.utils.send_delete_order"},
                },
                {
                    "cls": "minos.saga.definitions.steps.local.LocalSagaStep",
                    "on_execute": {"callback": "tests.utils.create_payment"},
                    "on_success": None,
                    "on_error": None,
                    "on_failure": {"callback": "tests.utils.delete_payment"},
                },
                {
                    "cls": "minos.saga.definitions.steps.remote.RemoteSagaStep",
                    "on_execute": {"callback": "tests.utils.send_create_ticket"},
                    "on_success": {"callback": "tests.utils.handle_ticket_success"},
                    "on_error": {"callback": "tests.utils.handle_ticket_error"},
                    "on_failure": {"callback": "tests.utils.send_delete_ticket"},
                },
            ],
        }
        self.assertEqual(ADD_ORDER, Saga.from_raw(raw))

    def test_from_raw_already(self):
        self.assertEqual(ADD_ORDER, Saga.from_raw(ADD_ORDER))


if __name__ == "__main__":
    unittest.main()
