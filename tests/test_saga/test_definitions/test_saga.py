import unittest
import warnings
from unittest.mock import (
    MagicMock,
)

from minos.saga import (
    AlreadyCommittedException,
    AlreadyOnSagaException,
    EmptySagaException,
    EmptySagaStepException,
    LocalSagaStep,
    RemoteSagaStep,
    Saga,
    SagaException,
    SagaExecution,
    SagaNotCommittedException,
    SagaOperation,
    SagaStep,
    identity_fn,
)
from tests.utils import (
    ADD_ORDER,
    commit_callback,
    create_payment,
    send_create_order,
    send_delete_order,
    send_delete_ticket,
)


class TestSaga(unittest.TestCase):
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
        step = saga.local_step(initial)
        self.assertEqual(step, initial)
        self.assertEqual(saga, step.saga)

    def test_local_operation(self):
        saga = Saga()
        step = saga.local_step(SagaOperation(create_payment))
        self.assertEqual(LocalSagaStep(on_execute=SagaOperation(create_payment)), step)
        self.assertEqual(saga, step.saga)

    def test_local_callback(self):
        saga = Saga()
        step = saga.local_step(create_payment)
        self.assertEqual(LocalSagaStep(on_execute=SagaOperation(create_payment)), step)
        self.assertEqual(saga, step.saga)

    def test_local_empty(self):
        saga = Saga()
        step = saga.local_step()
        self.assertIsInstance(step, SagaStep)
        self.assertEqual(saga, step.saga)

    def test_local_raises(self):
        saga = Saga().commit()
        with self.assertRaises(AlreadyCommittedException):
            saga.local_step()

        saga = Saga()
        with self.assertRaises(TypeError):
            # noinspection PyTypeChecker
            saga.local_step(RemoteSagaStep())

    def test_step(self):
        saga = Saga()
        mock = MagicMock(side_effect=saga.remote_step)
        saga.remote_step = mock
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            step = saga.step()

        self.assertEqual(1, mock.call_count)
        self.assertIsInstance(step, RemoteSagaStep)

    def test_remote(self):
        saga = Saga()
        initial = RemoteSagaStep()
        step = saga.remote_step(initial)
        self.assertEqual(step, initial)
        self.assertEqual(saga, step.saga)

    def test_remote_operation(self):
        saga = Saga()
        step = saga.remote_step(SagaOperation(send_delete_ticket))
        self.assertEqual(RemoteSagaStep(on_execute=SagaOperation(send_delete_ticket)), step)
        self.assertEqual(saga, step.saga)

    def test_remote_callback(self):
        saga = Saga()
        step = saga.remote_step(send_delete_ticket)
        self.assertEqual(RemoteSagaStep(on_execute=SagaOperation(send_delete_ticket)), step)
        self.assertEqual(saga, step.saga)

    def test_remote_empty(self):
        saga = Saga()
        step = saga.remote_step()
        self.assertIsInstance(step, SagaStep)
        self.assertEqual(saga, step.saga)

    def test_remote_raises(self):
        saga = Saga().commit()
        with self.assertRaises(AlreadyCommittedException):
            saga.remote_step()

        saga = Saga()
        with self.assertRaises(TypeError):
            # noinspection PyTypeChecker
            saga.remote_step(LocalSagaStep())

    def test_empty_step_raises(self):
        with self.assertRaises(SagaException):
            Saga().remote_step(send_create_order).on_failure(send_delete_order).remote_step().commit()

    def test_duplicate_operation_raises(self):
        with self.assertRaises(SagaException):
            Saga().remote_step(send_create_order).on_failure(send_delete_order).on_failure(send_delete_ticket).commit()

    def test_missing_send_raises(self):
        with self.assertRaises(SagaException):
            Saga().remote_step().on_failure(send_delete_ticket).commit()

    def test_build_execution(self):
        saga = Saga().remote_step(send_create_order).on_failure(send_delete_order).commit()
        execution = SagaExecution.from_saga(saga)
        self.assertIsInstance(execution, SagaExecution)

    def test_add_step(self):
        step = RemoteSagaStep(send_create_order)
        saga = Saga().remote_step(step).commit()

        self.assertEqual([step], saga.steps)

    def test_add_step_raises(self):
        step = RemoteSagaStep(send_create_order, saga=Saga())
        with self.assertRaises(AlreadyOnSagaException):
            Saga().remote_step(step)

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

    def test_validate(self):
        saga = Saga.from_raw(ADD_ORDER)
        saga.validate()
        self.assertTrue(True)

    def test_validate_raises(self):
        with self.assertRaises(EmptySagaException):
            Saga().validate()

        with self.assertRaises(EmptySagaStepException):
            Saga(steps=[RemoteSagaStep()]).validate()

        with self.assertRaises(SagaNotCommittedException):
            Saga(steps=[LocalSagaStep(create_payment)]).validate()


if __name__ == "__main__":
    unittest.main()
