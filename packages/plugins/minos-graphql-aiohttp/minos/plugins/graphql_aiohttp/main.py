from aiohttp import web
from minos.plugins.graphql_aiohttp.services import RestService


def main():
    web.run_app(RestService().create_application())


if __name__ == "__main__":
    main()
