import aiohttp


class DiscoveryClient:
    def __init__(self):
        discovery_ip = "172.20.0.3"
        discovery_port = 8080
        self.route = f"http://{discovery_ip}:{discovery_port}"

    async def subscribe(self, ip: str, port: int, name: str):
        endpoint = self.route + "/subscribe"
        async with aiohttp.ClientSession() as session:
            service_metadata = {"ip": ip, "port": port, "name": name}

            await session.post(endpoint, json=service_metadata)

    async def unsubscribe(self, name: str):
        endpoint = self.route + "/unsubscribe"
        async with aiohttp.ClientSession() as session:
            query_param = f"?name={name}"

            await session.post(endpoint + query_param)
