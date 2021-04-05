# Copyright (C) 2020 Clariteia SL
#
# This file is part of minos framework.
#
# Minos framework can not be copied and/or distributed without the express
# permission of Clariteia SL.

import abc


class MinosBaseSagaBuilder(abc.ABC):
    @abc.abstractmethod
    def start(self, **kwargs):  # pragma: no cover
        raise NotImplementedError

    @abc.abstractmethod
    def step(self, **kwargs):   # pragma: no cover
        raise NotImplementedError

    @abc.abstractmethod
    def invokeParticipant(self, **kwargs):  # pragma: no cover
        raise NotImplementedError

    @abc.abstractmethod
    def withCompensation(self, **kwargs):   # pragma: no cover
        raise NotImplementedError

    @abc.abstractmethod
    def onReply(self, **kwargs):    # pragma: no cover
        raise NotImplementedError

    @abc.abstractmethod
    def execute(self, **kwargs):    # pragma: no cover
        raise NotImplementedError
