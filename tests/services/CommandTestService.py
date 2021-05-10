from minos.common import Command


class CommandService(object):
    async def get_order(self, topic: str, command: Command):
        return "get_order"

    async def add_order(self, topic: str, command: Command):
        return "add_order"

    async def delete_order(self, topic: str, command: Command):
        return "delete_order"

    async def update_order(self, topic: str, command: Command):
        return "update_order"
