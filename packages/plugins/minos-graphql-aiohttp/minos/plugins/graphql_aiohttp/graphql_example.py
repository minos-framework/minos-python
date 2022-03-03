import asyncio

from graphql import (
    GraphQLField,
    GraphQLObjectType,
    GraphQLSchema,
    GraphQLString,
    graphql,
)


def resolve_hello(obj, info):
    return "world"


schema = GraphQLSchema(
    query=GraphQLObjectType(name="RootQueryType", fields={"hello": GraphQLField(GraphQLString, resolve=resolve_hello)})
)
