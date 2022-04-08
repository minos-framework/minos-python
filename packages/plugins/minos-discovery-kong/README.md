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
  connector: minos.networks.DiscoveryConnector
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
