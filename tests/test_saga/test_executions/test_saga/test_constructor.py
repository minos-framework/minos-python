"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

import unittest
from uuid import (
    UUID,
)

from minos.saga import (
    MinosSagaNotCommittedException,
    Saga,
    SagaContext,
    SagaExecution,
    SagaStatus,
)
from tests.callbacks import (
    create_order_callback,
    create_ticket_callback,
    delete_order_callback,
)
from tests.utils import (
    Foo,
    foo_fn_raises,
)


class TestSagaExecutionConstructor(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.saga = (
            Saga("OrdersAdd")
            .step()
            .invoke_participant("CreateOrder", create_order_callback)
            .with_compensation("DeleteOrder", delete_order_callback)
            .on_reply("order1")
            .step()
            .invoke_participant("CreateTicket", create_ticket_callback)
            .with_compensation("DeleteOrder", delete_order_callback)
            .on_reply("order2", foo_fn_raises)
            .commit()
        )

    def test_from_saga(self):
        execution = SagaExecution.from_saga(self.saga)
        self.assertEqual(self.saga, execution.definition)
        self.assertIsInstance(execution.uuid, UUID)
        self.assertEqual(SagaContext(), execution.context)
        self.assertEqual(SagaStatus.Created, execution.status)
        self.assertFalse(execution.already_rollback)
        self.assertIsNone(execution.paused_step)

    def test_from_saga_raises(self):
        with self.assertRaises(MinosSagaNotCommittedException):
            SagaExecution.from_saga(Saga("AddOrder"))

    def test_from_saga_with_context(self):
        context = SagaContext(foo=Foo("foo"), one=1, a="a")
        execution = SagaExecution.from_saga(self.saga, context=context)
        self.assertEqual(self.saga, execution.definition)
        self.assertIsInstance(execution.uuid, UUID)
        self.assertEqual(context, execution.context)
        self.assertEqual(SagaStatus.Created, execution.status)
        self.assertFalse(execution.already_rollback)
        self.assertIsNone(execution.paused_step)


if __name__ == "__main__":
    unittest.main()
