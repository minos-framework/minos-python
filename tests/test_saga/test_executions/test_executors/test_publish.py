"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import unittest
from uuid import (
    uuid4,
)

from minos.saga import (
    LocalExecutor,
    PublishExecutor,
)
from tests.utils import (
    NaiveBroker,
)


class TestPublishExecutor(unittest.TestCase):
    def test_constructor(self):
        broker = NaiveBroker()
        uuid = uuid4()
        executor = PublishExecutor(definition_name="AddFoo", execution_uuid=uuid, broker=broker)

        self.assertIsInstance(executor, LocalExecutor)
        self.assertEqual("AddFoo", executor.definition_name)
        self.assertEqual(uuid, executor.execution_uuid)
        self.assertEqual(broker, executor.broker)


if __name__ == "__main__":
    unittest.main()
