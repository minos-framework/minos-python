<p align="center">
  <a href="https://minos.run" target="_blank"><img src="https://raw.githubusercontent.com/minos-framework/.github/main/images/logo.png" alt="Minos logo"></a>
</p>

## minos-kong

[![PyPI Latest Release](https://img.shields.io/pypi/v/minos-kong.svg)](https://pypi.org/project/minos-kong/)
[![GitHub Workflow Status](https://img.shields.io/github/workflow/status/minos-framework/minos-python/pages%20build%20and%20deployment?label=docs)](https://minos-framework.github.io/minos-python)
[![License](https://img.shields.io/github/license/minos-framework/minos-python.svg)](https://github.com/minos-framework/minos-python/blob/main/LICENSE)
[![Coverage](https://codecov.io/github/minos-framework/minos-python/coverage.svg?branch=main)](https://codecov.io/gh/minos-framework/minos-python)
[![Stack Overflow](https://img.shields.io/badge/Stack%20Overflow-Ask%20a%20question-green)](https://stackoverflow.com/questions/tagged/minos)

## Summary
Pre-Alpha release, use at your own risk
Minos Kong is a plugin that integrate minos micorservices with Kong API Gateway

## Installation

Install the dependency:

```shell
pip install minos-discovery-kong
```

Modify `config.yml` file:

```yaml
...
discovery:
  client: minos.plugins.kong.KongDiscoveryClient
  host: localhost
  port: 8001
...
```

## How to
The above configuration is sufficient for the microservice to subscribe on startup and unsubscribe on shutdown.
Therefore, all you would have to do would be to make your requests against:

`http://localhost:8000/your_endpoint`

## Kong official documentation
### Official docs
You can get read the official docs [here](https://docs.konghq.com/gateway/2.8.x/admin-api/).

### Postman

You can get the official postman collection for postman [here](https://documenter.getpostman.com/view/10587735/SzS7QS2c#intro).

## Konga - Administrative interface
For development purposes you can add open-source administrative section by using next docker service:
```yaml
services:
  ...
  konga:
      image: pantsel/konga
      ports:
          - 1337:1337
      links:
          - kong:kong
      container_name: konga
      environment:
          - NODE_ENV=production
```

## Decorators
Decorator `@enroute` can support next params:
 - `path` - route url path.
 - `method` - HTTP method.
 - `authenticated` (Optional) - True if route need authentication.
 - `authorized_groups` (Optional) - Groups which can access to specified route (they must exist in Kong).
 - `regex_priority` (Optional) - A number used to choose which route resolves a given request when several routes match it using regexes simultaneously. When two routes match the path and have the same regex_priority, the older one (lowest created_at) is used. Note that the priority for non-regex routes is different (longer non-regex routes are matched before shorter ones). Defaults to 0.

Example:
```python
    @enroute.rest.command(f"/users/{{uuid:{UUID_REGEX.pattern}}}/jwt", "POST", authenticated=True, authorized_groups=["admin"], regex_priority=2)
    @enroute.broker.command("GetUserJWT")
    async def foo(self, request: Request) -> Response:
       ...
```
## Route path
It is important to know that it is best to define routes with a regular expression when it is an id, uuid or similar. This is to avoid collisions with similar routes.
Instead of using:
```python
@enroute.rest.command("/users/{uuid}", "POST")
```
Use:
```python
import re
UUID_REGEX = re.compile(r"\w{8}-\w{4}-\w{4}-\w{4}-\w{12}")
@enroute.rest.command(f"/users/{{uuid:{UUID_REGEX.pattern}}}", "POST")
```

## Authentication

Modify `config.yml` file. Add new middleware and modify discovery section:
```yaml
...
middleware:
  ...
  - minos.plugins.kong.middleware

discovery:
  connector: minos.networks.DiscoveryConnector
  client: minos.plugins.kong.KongDiscoveryClient
  host: localhost
  auth-type: basic-auth
  port: 8001
...
```

Currently, there are 2 possible types of authentication:
- `basic-auth`
- `jwt`

For jwt auth type you can specify default token expiration. Example:
```yaml
...
middleware:
  ...
  - minos.plugins.kong.middleware

discovery:
  connector: minos.networks.DiscoveryConnector
  client: minos.plugins.kong.KongDiscoveryClient
  host: localhost
  auth-type: jwt
  token-exp: 60 # seconds
  port: 8001
...
```

### JWT Token creation & refresh
Example on how to create and refresh token. You need to store in database or similar the secret and key returned form kong in order to refresh existing token.
```python
from minos.common import (
    UUID_REGEX,
    NotProvidedException,
    Config,
    Inject,
)
from minos.cqrs import (
    CommandService,
)
from minos.networks import (
    Request,
    Response,
    enroute,
)

from ..aggregates import (
    User,
)
from minos.plugins.kong import KongClient

class UserCommandService(CommandService):
    """UserCommandService class."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.kong = self._get_kong_client()

    @staticmethod
    @Inject()
    def _get_kong_client(config: Config) -> KongClient:
        """Get the service name."""
        if config is None:
            raise NotProvidedException("The config object must be provided.")
        return KongClient.from_config(config)

    @enroute.rest.command(f"/users/{{uuid:{UUID_REGEX.pattern}}}/jwt", "POST", authenticated=True,
                          authorized_groups=["admin"], regex_priority=3)
    @enroute.broker.command("GetUserJWT")
    async def create_user_jwt(self, request: Request) -> Response:
        params = await request.params()
        uuid = params["uuid"]
        user = await User.get(uuid)

        if user.uuid == request.user:
            token = await self.add_jwt_to_consumer(request.headers.get("X-Consumer-ID"))
            return Response({"token": token})
        else:
            return Response(status=404)

    async def add_jwt_to_consumer(self, consumer: str):
        resp = await self.kong.add_jwt_to_consumer(consumer=consumer)
        res = resp.json()
        self.key = res['key']
        self.secret = res['secret']
        token = await self.kong.generate_jwt_token(key=self.key, secret=self.secret)
        return token

    @enroute.rest.command(f"/users/{{uuid:{UUID_REGEX.pattern}}}/refresh-jwt", "POST", authenticated=True,
                          authorized_groups=["admin"], regex_priority=3)
    @enroute.broker.command("RefreshJWT")
    async def refresh_jwt(self, request: Request) -> Response:
        token = await self.kong.generate_jwt_token(key=self.key, secret=self.secret)
        return Response({"token": token})
```

For the route to be authenticated with the method specified above, a parameter called `authenticated` must be passed:
```python
class CategoryCommandService(CommandService):
    @enroute.rest.command("/categories", "POST", authenticated=True, authorized_groups=["super_admin", "admin"])
    @enroute.broker.command("CreateCategory")
    async def create_category(self, request: Request) -> Response:
        try:
            content = await request.content()
            category = await Category.create(**content)
            return Response(category)
        except Exception:
            raise ResponseException("An error occurred during category creation.")
```

If `authorized_groups` is also specified, this means that ACL will be enabled for that path and only users in the specified group will be allowed access.

Example of how to create a user and add them to a group:

```python
from minos.common import (
    NotProvidedException,
    Config,
    Inject,
)
from minos.cqrs import (
    CommandService,
)
from minos.networks import (
    Request,
    Response,
    enroute,
)
from ..aggregates import (
    Role,
    User,
)
from minos.plugins.kong import KongClient


class UserCommandService(CommandService):
    """UserCommandService class."""


    @enroute.rest.command("/users", "POST")
    @enroute.broker.command("CreateUser")
    async def create_user(self, request: Request) -> Response:
        """Create a new ``User`` instance.

        :param request: The ``Request`` instance.
        :return: A ``Response`` instance.
        """
        content = await request.content()

        active = True
        if "active" in content:
            active = content["active"]

        user = User(
            name=content["name"],
            surname=content["surname"],
            email=content["email"],
            telephone=content["telephone"],
            role=content["role"],
            active=active,
        )
        await user.save()

        kong = KongClient(self._get_kong_url())

        consumer_raw = await kong.create_consumer(username=f"{user.name} {user.surname}", user=user.uuid, tags=[])
        consumer = consumer_raw.json()

        basic_auth = await kong.add_basic_auth_to_consumer(username=f"{user.name.lower()}_{user.surname.lower()}",
                                                      password=content["password"], consumer=consumer["id"])

        acl = await kong.add_acl_to_consumer(role=user.role.name.lower(), consumer=consumer["id"])

        return Response(user)

    @staticmethod
    @Inject()
    def _get_kong_url(config: Config) -> str:
        """Get the service name."""
        if config is None:
            raise NotProvidedException("The config object must be provided.")
        return f"http://{config.get_by_key('discovery.host')}:{config.get_by_key('discovery.port')}"
```

Generate JWT example:
```python
from minos.common import (
    UUID_REGEX,
    NotProvidedException,
    Config,
    Inject,
)
from minos.cqrs import (
    CommandService,
)
from minos.networks import (
    Request,
    Response,
    enroute,
)
from ..aggregates import (
    Role,
    User,
)
from minos.plugins.kong import KongClient

class UserCommandService(CommandService):
    """UserCommandService class."""

    @enroute.rest.command(f"/users/{{uuid:{UUID_REGEX.pattern}}}/jwt", "POST", authenticated=True, authorized_groups=["admin"])
    @enroute.broker.command("GetUserJWT")
    async def create_user_jwt(self, request: Request) -> Response:
        params = await request.params()
        uuid = params["uuid"]
        user = await User.get(uuid)

        if user.uuid == request.user:
            kong = KongClient(self._get_kong_url())
            jwt = await kong.add_jwt_to_consumer(request.headers.get("X-Consumer-ID"))
            return Response(jwt.json())
        else:
            return Response(status=404)

    @staticmethod
    @Inject()
    def _get_kong_url(config: Config) -> str:
        """Get the service name."""
        if config is None:
            raise NotProvidedException("The config object must be provided.")
        return f"http://{config.get_by_key('discovery.host')}:{config.get_by_key('discovery.port')}"
```

You can get read the official docs [here](https://pantsel.github.io/konga/).


## Documentation

The official API Reference is publicly available at the [GitHub Pages](https://minos-framework.github.io/minos-python).

## Source Code

The source code of this project is hosted at the [GitHub Repository](https://github.com/minos-framework/minos-python).

## Getting Help

For usage questions, the best place to go to is [StackOverflow](https://stackoverflow.com/questions/tagged/minos).

## Discussion and Development

Most development discussions take place over the [GitHub Issues](https://github.com/minos-framework/minos-python/issues). In addition, a [Gitter channel](https://gitter.im/minos-framework/community) is available for development-related questions.

## License

This project is distributed under the [MIT](https://raw.githubusercontent.com/minos-framework/minos-python/main/LICENSE) license.
