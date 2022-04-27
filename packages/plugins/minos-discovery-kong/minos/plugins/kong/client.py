from __future__ import (
    annotations,
)

from datetime import (
    datetime,
    timedelta,
)
from uuid import (
    UUID,
)

import httpx as httpx
import jwt

from minos.common import (
    Config,
    SetupMixin,
    current_datetime,
)


class KongClient(SetupMixin):
    """Kong Client class."""

    def __init__(
        self, protocol: str = None, host: str = None, port: int = None, token_expiration_sec: int = None, **kwargs
    ):
        super().__init__(**kwargs)
        if host is None:
            host = "localhost"
        if port is None:
            port = 8001
        if protocol is None:
            protocol = "http"
        if token_expiration_sec is None:
            token_expiration_sec = 60 * 5

        self.route = f"{protocol}://{host}:{port}"
        self.token_expiration_sec = token_expiration_sec

    @classmethod
    def _from_config(cls, config: Config, **kwargs) -> KongClient:
        discovery_config = config.get_discovery()

        token_expiration_sec = discovery_config.get("token-exp")
        protocol = discovery_config.get("protocol")
        host = discovery_config.get("host")
        port = discovery_config.get("port")

        return cls(protocol=protocol, host=host, port=port, token_expiration_sec=token_expiration_sec, **kwargs)

    @staticmethod
    async def register_service(
        discovery_route: str, service_name: str, microservice_host: str, microservice_port: int
    ) -> httpx.Response:
        url = f"{discovery_route}/services"  # kong url for service POST or add
        data = {"name": service_name, "protocol": "http", "host": microservice_host, "port": microservice_port}
        async with httpx.AsyncClient() as client:
            response = await client.post(url, json=data)
            return response

    @staticmethod
    async def delete_service(discovery_route: str, service_name) -> httpx.Response:
        """
        the delete of a service must be checking before if the service already have the routes
        if yes the DELETE routes must be called
        :param discovery_route:
        :param service_name:
        :return:
        """
        async with httpx.AsyncClient() as client:
            url_get_route = f"{discovery_route}/services/{service_name}/routes"  # url to get the routes
            response_routes = await client.get(url_get_route)
            json_routes_response = response_routes.json()
            if len(json_routes_response["data"]) > 0:  # service already have route, routes must be deleted
                for route in json_routes_response["data"]:
                    url_delete_route = f"{discovery_route}/routes/{route['id']}"  # url for routes delete
                    await client.delete(url_delete_route)
            url_delete_service = f"{discovery_route}/services/{service_name}"  # url for service delete
            response_delete_service = await client.delete(url_delete_service)
            return response_delete_service

    @staticmethod
    async def create_route(
        endpoint: str,
        protocols: list[str],
        methods: list[str],
        paths: list[str],
        service: str,
        regex_priority: int,
        strip_path: bool = False,
    ):
        url = f"{endpoint}/routes"
        payload = {
            "protocols": protocols,
            "methods": methods,
            "paths": paths,
            "service": {"id": service},
            "regex_priority": regex_priority,
            "strip_path": strip_path,
        }

        async with httpx.AsyncClient() as client:
            resp = await client.post(url, json=payload)

            return resp

    async def create_consumer(self, username: str, user: UUID, tags: list[str]):
        payload = {"username": username, "custom_id": str(user), "tags": tags}

        async with httpx.AsyncClient() as client:
            resp = await client.post(f"{self.route}/consumers", json=payload)
            return resp

    async def add_basic_auth_to_consumer(self, username: str, password: str, consumer: str):
        payload = {
            "username": username,
            "password": password,
        }

        async with httpx.AsyncClient() as client:
            resp = await client.post(f"{self.route}/consumers/{consumer}/basic-auth", json=payload)

            return resp

    async def add_jwt_to_consumer(self, consumer: str):
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                f"{self.route}/consumers/{consumer}/jwt",
                headers={"content-type": "application/x-www-form-urlencoded"},
            )

            return resp

    async def add_acl_to_consumer(self, role: str, consumer: str):
        payload = {
            "group": role,
        }

        async with httpx.AsyncClient() as client:
            resp = await client.post(f"{self.route}/consumers/{consumer}/acls", json=payload)
            return resp

    async def activate_acl_plugin_on_service(self, service_name: str, allow: list[str]):
        payload = {"name": "acl", "config": {"allow": allow}}

        async with httpx.AsyncClient() as client:
            resp = await client.post(f"{self.route}/services/{service_name}/plugins", json=payload)
            return resp

    async def activate_acl_plugin_on_route(self, route_id: str, allow: list[str]):
        payload = {"name": "acl", "config": {"allow": allow}}

        async with httpx.AsyncClient() as client:
            resp = await client.post(f"{self.route}/routes/{route_id}/plugins", json=payload)
            return resp

    async def activate_basic_auth_plugin_on_service(self, service_name: str):
        payload = {"name": "basic-auth", "config": {"hide_credentials": False}}

        async with httpx.AsyncClient() as client:
            resp = await client.post(f"{self.route}/services/{service_name}/plugins", json=payload)
            return resp

    async def activate_basic_auth_plugin_on_route(self, route_id: str):
        payload = {"name": "basic-auth", "config": {"hide_credentials": False}}

        async with httpx.AsyncClient() as client:
            resp = await client.post(f"{self.route}/routes/{route_id}/plugins", json=payload)
            return resp

    async def activate_jwt_plugin_on_route(self, route_id: str):
        payload = {
            "name": "jwt",
            "config": {"secret_is_base64": False, "run_on_preflight": True, "claims_to_verify": ["exp", "nbf"]},
        }

        async with httpx.AsyncClient() as client:
            resp = await client.post(f"{self.route}/routes/{route_id}/plugins", json=payload)
            return resp

    async def generate_jwt_token(
        self, key: str, secret: str, algorithm: str = "HS256", exp: datetime = None, nbf: datetime = None
    ) -> str:
        payload = {"iss": key, "exp": exp, "nbf": nbf}

        current = current_datetime()

        if not exp:
            payload["exp"] = current + timedelta(seconds=self.token_expiration_sec)

        if not nbf:
            payload["nbf"] = current

        return jwt.encode(payload, secret, algorithm=algorithm)

    @staticmethod
    async def decode_token(token: str, algorithm: str = "HS256"):
        return jwt.decode(token, options={"verify_signature": False}, algorithms=[algorithm])

    async def get_jwt_by_id(self, id: str):
        async with httpx.AsyncClient() as client:
            resp = await client.get(f"{self.route}/jwts/{id}")
            return resp

    async def get_consumer_jwts(self, consumer: str):
        async with httpx.AsyncClient() as client:
            resp = await client.get(f"{self.route}/consumers/{consumer}/jwt")
            return resp
