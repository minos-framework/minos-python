<p align="center">
  <a href="https://minos.run" target="_blank"><img src="https://raw.githubusercontent.com/minos-framework/.github/main/images/logo.png" alt="Minos logo"></a>
</p>

## minos-router-graphql

[![PyPI Latest Release](https://img.shields.io/pypi/v/minos-broker-kafka.svg)](https://pypi.org/project/minos-broker-kafka/)
[![GitHub Workflow Status](https://img.shields.io/github/workflow/status/minos-framework/minos-python/pages%20build%20and%20deployment?label=docs)](https://minos-framework.github.io/minos-python)
[![License](https://img.shields.io/github/license/minos-framework/minos-python.svg)](https://github.com/minos-framework/minos-python/blob/main/LICENSE)
[![Coverage](https://codecov.io/github/minos-framework/minos-python/coverage.svg?branch=main)](https://codecov.io/gh/minos-framework/minos-python)
[![Stack Overflow](https://img.shields.io/badge/Stack%20Overflow-Ask%20a%20question-green)](https://stackoverflow.com/questions/tagged/minos)

## Summary

This is graphQL plugin for Minos framework. This plugin integrates the official [graphql-core](https://github.com/graphql-python/graphql-core) library. It is oriented to facilitate the development and better organize the graphql code.

## Installation

Install the dependency:

```shell
pip install minos-router-graphql
```

Modify `config.yml` file:

```yaml
...
routers:
  - minos.plugins.graphql.GraphQlRouter
...
```

## How to use

### Define your business operation

We will use simple query for this demonstration:

```python
from graphql import (
    GraphQLString,
)

from minos.networks import (
    Request,
    Response,
    enroute,
)


class QueryService:
    @enroute.graphql.query(name="SimpleQuery", output=GraphQLString)
    def simple_query(self, request: Request):
        ...

        return Response("ABCD")
```

### Execute query

Send `post` request to `http://your_ip_address:port/service_name/graphql` endpoint:

```json
{
  "query": "{ SimpleQuery }"
}
```

You will receive:

```json
{
  "data": {
    "SimpleQuery": "ABCD"
  },
  "errors": []
}
```

That's all you need to make it work !

For more information about graphql and how to define fields or structures, please see the official [graphql-core](https://github.com/graphql-python/graphql-core). library.

## Decorators

There are 2 types of decorators, one for `queries` and one for `mutations` (commands).

```python
@enroute.graphql.query(name="TestQuery", argument=GraphQLString, output=GraphQLString)
def test_query(self, request: Request):
    ...
    return Responnse(...)


@enroute.graphql.command(name="TestCommand", argument=GraphQLString, output=GraphQLString)
def test_command(self, request: Request):
    ...
    return Responnse(...)
```

Both decorators have the following arguments:

- `name`: The name of the query or command
- `argument` [Optional]: The arguments it receives, if any.
- `output`: The expected output.

### Resolvers

As you have seen above, the decorator does not specify the function that will resolve it.

This is because it automatically takes the decorated function.

In the following example:

```python
@enroute.graphql.query(name="TestQuery", argument=GraphQLString, output=GraphQLString)
def test_query(self, request: Request):
    ...
    return Responnse(...)
```

The function in charge of resolving the query is the decorated function `test_query`.

### Queries (Query Service)

Queries are used for a single purpose as their name indicates and that is to obtain information, that is, for queries.

Base structure example:

```python
class QueryService:
    @enroute.graphql.query(name="TestQuery", argument=GraphQLString, output=GraphQLString)
    def test_query(self, request: Request):
        ...
        return Responnse(...)
```

More complex example:

```python
from graphql import (
    GraphQLBoolean,
    GraphQLField,
    GraphQLID,
    GraphQLInt,
    GraphQLNonNull,
    GraphQLObjectType,
    GraphQLString,
)
from typing import (
    NamedTuple,
    Optional,
)
from minos.networks import (
    Request,
    Response,
    enroute,
)

user_type = GraphQLObjectType(
    "UserType",
    {
        "id": GraphQLField(GraphQLNonNull(GraphQLID)),
        "firstName": GraphQLField(GraphQLNonNull(GraphQLString)),
        "lastName": GraphQLField(GraphQLNonNull(GraphQLString)),
        "tweets": GraphQLField(GraphQLInt),
        "verified": GraphQLField(GraphQLNonNull(GraphQLBoolean)),
    },
)


class User(NamedTuple):
    """A simple user object class."""

    firstName: str
    lastName: str
    tweets: Optional[int]
    id: Optional[str] = None
    verified: bool = False


class QueryService:
    @enroute.graphql.query(name="GetUser", argument=GraphQLInt, output=user_type)
    def test_query(self, request: Request):
        id = await request.content()
        return Response(User(firstName="Jack", lastName="Johnson", tweets=563, id=str(id), verified=True))
```

If you POST `{service_name}/graphql` endpoint passing the query and variables:

```json
{
  "query": "query ($userId: Int!) { GetUser(request: $userId) {id firstName lastName tweets verified}}",
  "variables": {
    "userId": 3
  }
}
```

Yoy will receive:

```json
{
  "data": {
    "GetUser": {
      "id": "3",
      "firstName": "Jack",
      "lastName": "Johnson",
      "tweets": 563,
      "verified": true
    }
  },
  "errors": []
}
```

### Mutations (Command Service)

Mutations are used to create, update or delete information.

Base structure example:

```python
class CommandService:
    @enroute.graphql.command(name="TestQuery", argument=GraphQLString, output=GraphQLString)
    def test_command(self, request: Request):
        ...
        return Responnse(...)
```

More complex example:

```python
from graphql import (
    GraphQLBoolean,
    GraphQLField,
    GraphQLID,
    GraphQLInputField,
    GraphQLInputObjectType,
    GraphQLInt,
    GraphQLNonNull,
    GraphQLObjectType,
    GraphQLString,
)
from typing import (
    NamedTuple,
    Optional,
)
from minos.networks import (
    Request,
    Response,
    enroute,
)

user_type = GraphQLObjectType(
    "UserType",
    {
        "id": GraphQLField(GraphQLNonNull(GraphQLID)),
        "firstName": GraphQLField(GraphQLNonNull(GraphQLString)),
        "lastName": GraphQLField(GraphQLNonNull(GraphQLString)),
        "tweets": GraphQLField(GraphQLInt),
        "verified": GraphQLField(GraphQLNonNull(GraphQLBoolean)),
    },
)

user_input_type = GraphQLInputObjectType(
    "UserInputType",
    {
        "firstName": GraphQLInputField(GraphQLNonNull(GraphQLString)),
        "lastName": GraphQLInputField(GraphQLNonNull(GraphQLString)),
        "tweets": GraphQLInputField(GraphQLInt),
        "verified": GraphQLInputField(GraphQLBoolean),
    },
)


class User(NamedTuple):
    """A simple user object class."""

    firstName: str
    lastName: str
    tweets: Optional[int]
    id: Optional[str] = None
    verified: bool = False


class CommandService:
    @enroute.graphql.command(name="CreateUser", argument=GraphQLNonNull(user_input_type), output=user_type)
    def test_command(self, request: Request):
        params = await request.content()
        return Response(
            User(
                firstName=params["firstName"],
                lastName=params["lastName"],
                tweets=params["tweets"],
                id="4kjjj43-l23k4l3-325kgaa2",
                verified=params["verified"],
            )
        )
```

If you POST `{service_name}/graphql` endpoint passing the query and variables:

```json
{
  "query": "mutation ($userData: UserInputType!) { CreateUser(request: $userData) {id, firstName, lastName, tweets, verified}}",
  "variables": {
    "userData": {
      "firstName": "John",
      "lastName": "Doe",
      "tweets": 42,
      "verified": true
    }
  }
}
```

Yoy will receive:

```json
{
  "data": {
    "CreateUser": {
      "id": "4kjjj43-l23k4l3-325kgaa2",
      "firstName": "John",
      "lastName": "Doe",
      "tweets": 42,
      "verified": true
    }
  },
  "errors": []
}
```

### Get Schema

By calling `{service_name}/graphql/schema` with `GET` method, you will receive the schema:

```text
"type Query {\n  GetUser(request: Int): UserType\n}\n\ntype UserType {\n  id: ID!\n  firstName: String!\n  lastName: String!\n  tweets: Int\n  verified: Boolean!\n}\n\ntype Mutation {\n  CreateUser(request: UserInputType!): UserType\n}\n\ninput UserInputType {\n  firstName: String!\n  lastName: String!\n  tweets: Int\n  verified: Boolean\n}"
```

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
