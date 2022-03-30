import unittest
from minos.plugins.minos_kong import (
    MinosKongClient,
)


class TestMinosKongClient(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.client = MinosKongClient("localhost", 8001, circuit_breaker_time=0.1)

    def test_constructor(self):
        client = MinosKongClient()
        self.assertEqual("localhost", client.host)
        self.assertEqual(5567, client.port)

    def test_route(self):
        # noinspection HttpUrlsUsage
        self.assertEqual("http://localhost:8001", self.client.route)

    async def test_subscribe(self):
        response = await self.client.subscribe(
            "172.160.16.24", 5660, "test", [{"url": "/foo", "method": "POST"}, {"url": "/bar", "method": "GET"}]
        )
        self.assertTrue(201 == response.status_code)

    async def test_unsubscribe(self):
        response = await self.client.subscribe(
            "172.160.16.24", 5660, "test", [{"url": "/foo", "method": "POST"}, {"url": "/bar", "method": "GET"}]
        )

        response_delete = await self.client.unsubscribe("test")
        self.assertTrue(204 == response_delete.status_code)

if __name__ == "__main__":
    unittest.main()
