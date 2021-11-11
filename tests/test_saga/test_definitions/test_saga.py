import unittest
import warnings
from unittest.mock import (
    MagicMock,
)

from minos.saga import (
    AlreadyCommittedException,
    AlreadyOnSagaException,
    ConditionalSagaStep,
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
)
from tests.utils import (
    ADD_ORDER,
    create_payment,
    send_create_order,
    send_delete_order,
    send_delete_ticket,
)


class TestSaga(unittest.TestCase):
    def test_commit_constructor(self):
        saga = Saga()
        self.assertEqual(False, saga.committed)

    def test_commit_constructor_define_callback(self):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            saga = Saga(commit=create_payment)

        self.assertEqual([LocalSagaStep(SagaOperation(create_payment))], saga.steps)
        self.assertEqual(True, saga.committed)

    def test_commit(self):
        saga = Saga()
        observed = saga.commit()
        self.assertEqual(saga, observed)
        self.assertEqual(True, saga.committed)

    def test_commit_define_callback(self):
        saga = Saga()

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            # noinspection PyTypeChecker
            observed = saga.commit(create_payment)
        self.assertEqual(saga, observed)
        self.assertEqual([LocalSagaStep(SagaOperation(create_payment))], saga.steps)
        self.assertEqual(True, saga.committed)

    def test_commit_raises(self):
        saga = Saga().commit()
        with self.assertRaises(AlreadyCommittedException):
            saga.commit()

    def test_committed_true(self):
        saga = Saga(committed=True)
        self.assertTrue(saga.committed)

    def test_committed_false(self):
        saga = Saga()
        self.assertFalse(saga.committed)

    def test_conditional(self):
        saga = Saga()
        initial = ConditionalSagaStep()
        step = saga.conditional_step(initial)
        self.assertEqual(step, initial)
        self.assertEqual(saga, step.saga)

    def test_conditional_empty(self):
        saga = Saga()
        step = saga.conditional_step()
        self.assertIsInstance(step, SagaStep)
        self.assertEqual(saga, step.saga)

    def test_conditional_raises(self):
        saga = Saga().commit()
        with self.assertRaises(AlreadyCommittedException):
            saga.conditional_step()

        saga = Saga()
        with self.assertRaises(TypeError):
            # noinspection PyTypeChecker
            saga.conditional_step(RemoteSagaStep())

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
        # noinspection DuplicatedCode
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
        execution = SagaExecution.from_definition(saga)
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
            "committed": True,
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
            "committed": True,
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
