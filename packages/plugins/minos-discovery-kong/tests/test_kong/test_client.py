import os
import unittest
from uuid import (
    uuid4,
)

import httpx

from minos.plugins.kong import (
    KongDiscoveryClient,
)

PROTOCOL = "http"


class TestKongDiscoveryClient(unittest.IsolatedAsyncioTestCase):
    KONG_HOST = os.getenv("KONG_HOST", "localhost")
    KONG_PORT = os.getenv("KONG_PORT", 8001)

    def setUp(self) -> None:
        self.client = KongDiscoveryClient(self.KONG_HOST, self.KONG_PORT, circuit_breaker_time=0.1)

    @staticmethod
    def generate_underscore_uuid():
        name = str(uuid4())
        return name.replace("-", "_")

    def test_constructor(self):
        client = KongDiscoveryClient()
        self.assertEqual("localhost", client.host)
        self.assertEqual(5567, client.port)

    def test_route(self):
        # noinspection HttpUrlsUsage
        self.assertEqual(f"{PROTOCOL}://{self.client.host}:{self.client.port}", self.client.route)

    async def test_subscribe(self):
        name = self.generate_underscore_uuid()
        response = await self.client.subscribe(
            "172.160.16.24", 5660, name, [{"url": "/foo", "method": "POST"}, {"url": "/bar", "method": "GET"}]
        )
        self.assertTrue(201 == response.status_code)

        async with httpx.AsyncClient() as client:
            url = f"{PROTOCOL}://{self.client.host}:{self.client.port}/services/{name}"
            response = await client.get(url)
            response_data = response.json()
            self.assertTrue(200 == response.status_code)
            self.assertEqual(5660, response_data["port"])
            self.assertEqual("172.160.16.24", response_data["host"])
            self.assertEqual(PROTOCOL, response_data["protocol"])

    async def test_unsubscribe(self):
        name = self.generate_underscore_uuid()
        response = await self.client.subscribe(
            "172.160.16.24", 5660, name, [{"url": "/foo", "method": "POST"}, {"url": "/bar", "method": "GET"}]
        )

        self.assertTrue(201 == response.status_code)

        response_delete = await self.client.unsubscribe(name)
        self.assertTrue(204 == response_delete.status_code)

        async with httpx.AsyncClient() as client:
            url = f"{PROTOCOL}://{self.client.host}:{self.client.port}/services/{name}"
            response = await client.get(url)
            self.assertTrue(404 == response.status_code)

    async def test_route_params(self):
        expected = ["/foo/.*", "/bar/.*/.*"]
        response = await self.client.subscribe(
            "172.160.16.24",
            5660,
            "test",
            [{"url": "/foo/{:user}", "method": "POST"}, {"url": "/bar/{:domain}/{:username}", "method": "GET"}],
        )

        self.assertTrue(201 == response.status_code)

        async with httpx.AsyncClient() as client:
            url = f"{PROTOCOL}://{self.client.host}:{self.client.port}/services/test/routes"
            response = await client.get(url)
            response_data = response.json()
            self.assertTrue(200 == response.status_code)
            self.assertGreater(len(response_data["data"]), 0)

            for route in response_data["data"]:
                self.assertTrue(bool(set(route["paths"]) & set(expected)))


if __name__ == "__main__":
    unittest.main()
