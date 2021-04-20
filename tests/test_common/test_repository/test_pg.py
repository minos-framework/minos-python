"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

import unittest

from minos.common import (
    MinosRepository,
    PostgreSqlMinosRepository,
)


class TestPostgreSqlMinosRepository(unittest.TestCase):
    def test_constructor(self):
        repository = PostgreSqlMinosRepository("localhost", 1234)
        self.assertIsInstance(repository, MinosRepository)
        self.assertEqual("localhost", repository.host)
        self.assertEqual(1234, repository.port)


if __name__ == "__main__":
    unittest.main()
