import abc


class MinosAbstractProvider(abc.ABC):

    @abc.abstractmethod
    def read(self):
        raise NotImplementedError

    @abc.abstractmethod
    def get(self, **kwargs):
        raise NotImplementedError
