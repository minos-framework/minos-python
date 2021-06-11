"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from __future__ import annotations

import logging
from operator import attrgetter

from .dynamic import DynamicModel
from .aggregate import Aggregate

logger = logging.getLogger(__name__)


def _diff(a: dict, b: dict) -> dict:
    d = set(a.items()) - set(b.items())
    return dict(d)


class AggregateDiff(DynamicModel):
    """TODO"""

    @classmethod
    def from_update(cls, a: Aggregate, b: Aggregate):
        """TODO

        :param a: TODO
        :param b: TODO
        :return: TODO
        """
        logger.debug(f"Computing the {cls!r} between {a!r} and {b!r}...")

        if a.id != b.id:
            raise Exception()  # TODO

        new, old = sorted([a, b], key=attrgetter("version"), reverse=True)
        fields = _diff(new.fields, old.fields)

        fields.pop("version")

        return cls(fields)

    @classmethod
    def from_create(cls, aggregate: Aggregate):
        """TODO

        :param aggregate: TODO
        :return: TODO
        """
        fields = dict(aggregate.fields)
        fields.pop("id")
        fields.pop("version")

        return cls(fields)

    @classmethod
    def simplify(cls, *args: AggregateDiff):
        """TODO

        :param args: TODO
        :return: TODO
        """
        current = cls(args[0].fields)
        for another in args[1:]:
            current._fields |= another._fields
        return current
