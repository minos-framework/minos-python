import os
import unittest
from datetime import (
    datetime,
    timedelta,
)
from uuid import (
    uuid4,
)

import httpx
from pytz import (
    utc,
)

from minos.common import (
    Config,
)
from minos.plugins.kong import (
    KongClient,
    KongDiscoveryClient,
)
from tests.utils import (
    CONFIG_FILE_PATH,
    TEST_HOST,
)

PROTOCOL = "http"


class TestKongDiscoveryClient(unittest.IsolatedAsyncioTestCase):
    KONG_HOST = os.getenv("KONG_HOST", "localhost")
    KONG_PORT = os.getenv("KONG_PORT", 8001)

    def setUp(self) -> None:
        self.client = KongDiscoveryClient(self.KONG_HOST, self.KONG_PORT, circuit_breaker_time=0.1)
        self.kong = KongClient()

    @staticmethod
    def generate_underscore_uuid():
        name = str(uuid4())
        return name.replace("-", "_")

    async def test_register_service(self):
        name = self.generate_underscore_uuid()
        response = await self.kong.register_service(
            discovery_route=self.client.route,
            service_name=name,
            microservice_host=TEST_HOST,
            microservice_port=5660,
        )

        self.assertTrue(201 == response.status_code)

        async with httpx.AsyncClient() as client:
            url = f"{self.client.route}/services/{name}"
            response = await client.get(url)
            response_data = response.json()
            self.assertTrue(200 == response.status_code)
            self.assertEqual(5660, response_data["port"])
            self.assertEqual(TEST_HOST, response_data["host"])
            self.assertEqual(PROTOCOL, response_data["protocol"])

    async def test_create_consumer(self):
        user_uuid = uuid4()
        user_name = self.generate_underscore_uuid()
        response = await self.kong.create_consumer(username=user_name, user=user_uuid, tags=[])

        self.assertTrue(201 == response.status_code)

    async def test_add_basic_auth_to_consumer(self):
        user_uuid = uuid4()
        user_name = self.generate_underscore_uuid()
        response = await self.kong.create_consumer(username=user_name, user=user_uuid, tags=[])

        self.assertTrue(201 == response.status_code)
        resp = response.json()

        response = await self.kong.add_basic_auth_to_consumer(user_name, "test", resp["id"])

        self.assertTrue(201 == response.status_code)

    async def test_add_jwt_to_consumer(self):
        user_uuid = uuid4()
        user_name = self.generate_underscore_uuid()
        response = await self.kong.create_consumer(username=user_name, user=user_uuid, tags=[])

        self.assertTrue(201 == response.status_code)
        resp = response.json()

        response = await self.kong.add_jwt_to_consumer(consumer=resp["id"])

        self.assertTrue(201 == response.status_code)

    async def test_add_acl_to_consumer(self):
        user_uuid = uuid4()
        user_name = self.generate_underscore_uuid()
        response = await self.kong.create_consumer(username=user_name, user=user_uuid, tags=[])

        self.assertTrue(201 == response.status_code)
        resp = response.json()

        response = await self.kong.add_acl_to_consumer(role="admin", consumer=resp["id"])

        self.assertTrue(201 == response.status_code)

    async def test_activate_acl_plugin_on_service(self):
        name = self.generate_underscore_uuid()
        response = await self.kong.register_service(
            discovery_route=self.client.route,
            service_name=name,
            microservice_host=TEST_HOST,
            microservice_port=5660,
        )

        self.assertTrue(201 == response.status_code)

        response = await self.kong.activate_acl_plugin_on_service(service_name=name, allow=["admin"])

        self.assertTrue(201 == response.status_code)

    async def test_activate_basic_auth_plugin_on_service(self):
        name = self.generate_underscore_uuid()
        response = await self.kong.register_service(
            discovery_route=self.client.route,
            service_name=name,
            microservice_host=TEST_HOST,
            microservice_port=5660,
        )

        self.assertTrue(201 == response.status_code)

        response = await self.kong.activate_basic_auth_plugin_on_service(service_name=name)

        self.assertTrue(201 == response.status_code)

    async def test_activate_basic_auth_plugin_on_route(self):
        name = self.generate_underscore_uuid()
        response = await self.kong.register_service(
            discovery_route=self.client.route,
            service_name=name,
            microservice_host=TEST_HOST,
            microservice_port=5660,
        )

        self.assertTrue(201 == response.status_code)
        res = response.json()
        response = await self.kong.create_route(
            endpoint=self.client.route,
            protocols=["http"],
            methods=["GET"],
            paths=["/foo"],
            service=res["id"],
            strip_path=False,
        )

        self.assertTrue(201 == response.status_code)

        res = response.json()
        response = await self.kong.activate_basic_auth_plugin_on_route(route_id=res["id"])

        self.assertTrue(201 == response.status_code)

    async def test_activate_jwt_plugin_on_route(self):
        name = self.generate_underscore_uuid()
        response = await self.kong.register_service(
            discovery_route=self.client.route,
            service_name=name,
            microservice_host=TEST_HOST,
            microservice_port=5660,
        )

        self.assertTrue(201 == response.status_code)
        res = response.json()
        response = await self.kong.create_route(
            endpoint=self.client.route,
            protocols=["http"],
            methods=["GET"],
            paths=["/foo"],
            service=res["id"],
            strip_path=False,
        )

        self.assertTrue(201 == response.status_code)

        res = response.json()
        response = await self.kong.activate_jwt_plugin_on_route(route_id=res["id"])

        self.assertTrue(201 == response.status_code)

    async def test_jwt_token_generation(self):
        user_uuid = uuid4()
        user_name = self.generate_underscore_uuid()
        response = await self.kong.create_consumer(username=user_name, user=user_uuid, tags=[])

        self.assertTrue(201 == response.status_code)
        resp = response.json()

        response = await self.kong.add_jwt_to_consumer(consumer=resp["id"])

        self.assertTrue(201 == response.status_code)
        resp = response.json()

        token = await self.kong.get_jwt_token(key=resp["key"], secret=resp["secret"])

        self.assertGreater(len(token), 50)

    async def test_jwt_token_generation_with_expiration(self):
        user_uuid = uuid4()
        user_name = self.generate_underscore_uuid()
        response = await self.kong.create_consumer(username=user_name, user=user_uuid, tags=[])

        self.assertTrue(201 == response.status_code)
        resp = response.json()

        response = await self.kong.add_jwt_to_consumer(consumer=resp["id"])

        self.assertTrue(201 == response.status_code)
        resp = response.json()

        current = datetime.now(tz=utc)
        token = await self.kong.get_jwt_token(
            key=resp["key"],
            secret=resp["secret"],
            exp=current + timedelta(minutes=10),
            nbf=current + timedelta(minutes=9),
        )

        self.assertGreater(len(token), 50)


class TestKongClientFromConfig(unittest.IsolatedAsyncioTestCase):
    KONG_HOST = os.getenv("KONG_HOST", "localhost")
    KONG_PORT = os.getenv("KONG_PORT", 8001)

    def setUp(self) -> None:
        config = Config(CONFIG_FILE_PATH)
        self.kong = KongClient.from_config(config=config)

    @staticmethod
    def generate_underscore_uuid():
        name = str(uuid4())
        return name.replace("-", "_")

    async def test_create_consumer(self):
        user_uuid = uuid4()
        user_name = self.generate_underscore_uuid()
        response = await self.kong.create_consumer(username=user_name, user=user_uuid, tags=[])

        self.assertTrue(201 == response.status_code)


if __name__ == "__main__":
    unittest.main()
