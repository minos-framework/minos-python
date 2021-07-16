"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import unittest
from minos.networks import (
    Enroute,
)


class TestDecorators(unittest.IsolatedAsyncioTestCase):
    enroute = Enroute

    async def test_rest_query(self):
        func = self.enroute.rest.query(url="tickets/", method="GET")
        result = func(func)
        self.assertEqual(func, result)

    async def test_rest_command(self):
        func = self.enroute.rest.command(topics=["CreateTicket"])
        result = func(func)
        self.assertEqual(func, result)

    async def test_broker_query(self):
        func = self.enroute.broker.query(topics=["CreateTicket"])
        result = func(func)
        self.assertEqual(func, result)

    async def test_broker_command(self):
        func = self.enroute.broker.command(topics=["CreateTicket"])
        result = func(func)
        self.assertEqual(func, result)

    async def test_broker_event(self):
        func = self.enroute.broker.event(topics=["CreateTicket"])
        result = func(func)
        self.assertEqual(func, result)


if __name__ == "__main__":
    unittest.main()
