from minos.common import (
    Event,
)


class CqrsService(object):
    async def ticket_added(self, topic: str, event: Event):
        return "request_added"

    async def ticket_deleted(self, topic: str, event: Event):
        return "ticket_deleted"
