"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

import unittest
from typing import (
    NoReturn,
)

from minos.common import (
    MinosSetup,
)


class TestMinosSetup(unittest.IsolatedAsyncioTestCase):
    def test_already_setup_default(self):
        instance = _MinosSetupMock()
        self.assertEqual(False, instance.already_setup)
        self.assertEqual(0, instance.calls)

    def test_already_setup_true(self):
        instance = _MinosSetupMock(already_setup=True)
        self.assertEqual(True, instance.already_setup)
        self.assertEqual(0, instance.calls)

    async def test_setup(self):
        instance = _MinosSetupMock()
        await instance.setup()
        self.assertEqual(True, instance.already_setup)
        self.assertEqual(1, instance.calls)
        await instance.setup()
        self.assertEqual(True, instance.already_setup)
        self.assertEqual(1, instance.calls)

    async def test_setup_already_setup(self):
        instance = _MinosSetupMock(already_setup=True)
        await instance.setup()
        self.assertEqual(True, instance.already_setup)
        self.assertEqual(0, instance.calls)


class _MinosSetupMock(MinosSetup):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.calls = False

    async def _setup(self) -> NoReturn:
        self.calls += 1


if __name__ == "__main__":
    unittest.main()
