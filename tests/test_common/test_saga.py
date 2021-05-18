"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

import unittest
from typing import (
    NoReturn,
)
from unittest.mock import (
    MagicMock,
    call,
)

from minos.common import (
    CommandReply,
    MinosSagaManager,
)
from tests.aggregate_classes import (
    Car,
)


class _MinosSagaManager(MinosSagaManager):
    def _run_new(self, name: str) -> NoReturn:
        pass

    def _load_and_run(self, reply: CommandReply) -> NoReturn:
        pass


class TestMinosSagaManager(unittest.TestCase):
    def test_run_new(self):
        manager = _MinosSagaManager()

        mock = MagicMock(side_effect=manager._run_new)
        manager._run_new = mock

        manager.run(name="hello")

        self.assertEqual(1, mock.call_count)
        self.assertEqual(call("hello"), mock.call_args)

    def test_reload_and_run(self):
        manager = _MinosSagaManager()

        mock = MagicMock(side_effect=manager._load_and_run)
        manager._load_and_run = mock

        reply = CommandReply("hello", [Car(1, 1, 3, "blue")], "saga_id8972348237", "task_id32423432")

        manager.run(reply=reply)

        self.assertEqual(1, mock.call_count)
        self.assertEqual(call(reply), mock.call_args)

    def test_run_raises(self):

        manager = _MinosSagaManager()
        with self.assertRaises(ValueError):
            manager.run()


if __name__ == "__main__":
    unittest.main()
