from __future__ import (
    annotations,
)

from graphql import GraphQLSchema, GraphQLObjectType, GraphQLField

from minos.plugins.graphql.decorators import (
    GraphQlQueryEnrouteDecorator,
    GraphQlCommandEnrouteDecorator,
)


class GraphQLSchemaBuilder:
    def __init__(self, *args, **kwargs):
        self.schema = GraphQLSchema(**kwargs)

    @classmethod
    def build(cls, queries: list, mutations: list) -> GraphQLSchema:
        params = dict()
        if len(queries) > 0:
            params["query"] = GraphQLSchemaBuilder._build_queries(queries)
        if len(mutations) > 0:
            params["mutation"] = GraphQLSchemaBuilder._build_mutations(mutations)
        return cls(**params).schema

    @classmethod
    def _build_queries(cls, queries):
        fields = {item.name: cls._build_query(item) for item in queries}

        return GraphQLObjectType("Query", fields=fields)

    @staticmethod
    def _build_query(item: GraphQlQueryEnrouteDecorator):
        return {item.name: GraphQLField(item.argument, resolve=item)}

    @classmethod
    def _build_mutations(cls, mutations):
        fields = {item.name: cls._build_mutation(item) for item in mutations}

        return GraphQLObjectType("Mutation", fields=fields)

    @staticmethod
    def _build_mutation(item: GraphQlCommandEnrouteDecorator):
        return {item.name: GraphQLField(item.argument, resolve=item)}
