import unittest
import httpx
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

        self.assertTrue(201 == response.status_code)

        response_delete = await self.client.unsubscribe("test")
        self.assertTrue(204 == response_delete.status_code)

    async def test_route_params(self):
        response = await self.client.subscribe(
            "172.160.16.24",
            5660,
            "test",
            [{"url": "/foo/{:user}", "method": "POST"}, {"url": "/bar/{:domain}/{:username}", "method": "GET"}],
        )

        self.assertTrue(201 == response.status_code)

        async with httpx.AsyncClient() as client:
            url = f"http://{self.client.host}:{self.client.port}/services/test/routes"
            response = await client.get(url)
            response_data = response.json()
            self.assertGreater(len(response_data["data"]), 0)

        self.assertTrue(200 == response.status_code)


if __name__ == "__main__":
    unittest.main()
