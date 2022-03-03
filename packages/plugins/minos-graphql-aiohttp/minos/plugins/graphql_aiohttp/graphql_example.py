import asyncio
from graphql import graphql, GraphQLSchema, GraphQLObjectType, GraphQLField, GraphQLString


def resolve_hello(obj, info):
    return "world"


schema = GraphQLSchema(
    query=GraphQLObjectType(name="RootQueryType", fields={"hello": GraphQLField(GraphQLString, resolve=resolve_hello)})
)
