import abc


class MinosStorage(abc.ABC):

    @abc.abstractmethod
    def add(self, **kwargs):
        raise NotImplementedError

    @abc.abstractmethod
    def get(self, **kwargs):
        raise NotImplementedError

    @abc.abstractmethod
    def delete(self, **kwargs):
        raise NotImplementedError

    @abc.abstractmethod
    def update(self, **kwargs):
        raise NotImplementedError

    @abc.abstractmethod
    def build(self, **kwargs):
        raise NotImplementedError
