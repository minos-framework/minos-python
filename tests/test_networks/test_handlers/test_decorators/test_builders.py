"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import unittest

from minos.common import (
    Response,
    classname,
)
from minos.networks import (
    EnrouteBuilder,
)
from tests.utils import (
    FakeRequest,
    FakeService,
)


class TestEnrouteDecorator(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.request = FakeRequest("test")
        self.builder = EnrouteBuilder(FakeService)

    def test_instance_str(self):
        builder = EnrouteBuilder(classname(FakeService))
        self.assertEqual(FakeService, builder.decorated)

    async def test_method_query_call_2(self):
        # FIXME
        fn = self.builder.get_broker_event()[0][0]
        response = await fn(self.request)
        self.assertEqual(Response("test"), response)


if __name__ == "__main__":
    unittest.main()
