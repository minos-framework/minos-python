import unittest
from shutil import (
    rmtree,
)

from minos.saga import (
    MinosAlreadyOnSagaException,
    MinosSagaAlreadyCommittedException,
    MinosSagaException,
    Saga,
    SagaExecution,
    SagaOperation,
    SagaStep,
    identity_fn,
)
from tests.utils import (
    ADD_ORDER,
    BASE_PATH,
    commit_callback,
    send_create_order,
    send_delete_order,
    send_delete_ticket,
)


class TestSaga(unittest.TestCase):
    DB_PATH = BASE_PATH / "test_db.lmdb"

    # noinspection PyMissingOrEmptyDocstring
    def tearDown(self) -> None:
        rmtree(self.DB_PATH, ignore_errors=True)

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
        with self.assertRaises(MinosSagaAlreadyCommittedException):
            saga.commit()

    def test_committed_true(self):
        saga = Saga()
        saga.commit_operation = SagaOperation(identity_fn)
        self.assertTrue(saga.committed)

    def test_committed_false(self):
        saga = Saga()
        self.assertFalse(saga.committed)

    def test_step_raises(self):
        saga = Saga().commit()
        with self.assertRaises(MinosSagaAlreadyCommittedException):
            saga.step()

    def test_empty_step_raises(self):
        with self.assertRaises(MinosSagaException):
            Saga().step(send_create_order).on_failure(send_delete_order).step().commit()

    def test_duplicate_operation_raises(self):
        with self.assertRaises(MinosSagaException):
            Saga().step(send_create_order).on_failure(send_delete_order).on_failure(send_delete_ticket).commit()

    def test_missing_send_raises(self):
        with self.assertRaises(MinosSagaException):
            Saga().step().on_failure(send_delete_ticket).commit()

    def test_build_execution(self):
        saga = Saga().step(send_create_order).on_failure(send_delete_order).commit()
        execution = SagaExecution.from_saga(saga)
        self.assertIsInstance(execution, SagaExecution)

    def test_add_step(self):
        step = SagaStep(send_create_order)
        saga = Saga().step(step).commit()

        self.assertEqual([step], saga.steps)

    def test_add_step_raises(self):
        step = SagaStep(send_create_order, saga=Saga())
        with self.assertRaises(MinosAlreadyOnSagaException):
            Saga().step(step)

    def test_raw(self):
        saga = ADD_ORDER
        expected = {
            "commit": {"callback": "minos.saga.definitions.operations.identity_fn"},
            "steps": [
                {
                    "on_execute": {"callback": "tests.utils.send_create_order"},
                    "on_success": {"callback": "tests.utils.handle_order_success"},
                    "on_failure": {"callback": "tests.utils.send_delete_order"},
                },
                {
                    "on_execute": {"callback": "tests.utils.send_create_ticket"},
                    "on_success": {"callback": "tests.utils.handle_ticket_success"},
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
                    "on_execute": {"callback": "tests.utils.send_create_order"},
                    "on_success": {"callback": "tests.utils.handle_order_success"},
                    "on_failure": {"callback": "tests.utils.send_delete_order"},
                },
                {
                    "on_execute": {"callback": "tests.utils.send_create_ticket"},
                    "on_success": {"callback": "tests.utils.handle_ticket_success"},
                    "on_failure": {"callback": "tests.utils.send_delete_ticket"},
                },
            ],
        }
        self.assertEqual(ADD_ORDER, Saga.from_raw(raw))

    def test_from_raw_already(self):
        self.assertEqual(ADD_ORDER, Saga.from_raw(ADD_ORDER))


if __name__ == "__main__":
    unittest.main()
