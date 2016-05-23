from batch import BatchableOperation
from batch import Batcher
from batch import GenResult
from cache_get_operation import TestCacheGetBatcher


class TestCacheSetOperation(BatchableOperation):
    """An operation that simulates storing in a key-value data store.

    An operation that simulates storing values in a key-value data store
    that serves as a cache for some other data store.  The result of a
    TestCacheSetOperation is unspecified.  We may fetch the cached
    values using TestCacheGetOperation.
    """

    # Private attributes:
    # basestring _key - The key for which to store the value.
    # mixed _value - The non-None value to store.

    def __init__(self, key, value):
        self._key = key
        self._value = value

    def batcher(self):
        return TestCacheSetBatcher.instance()


class TestCacheSetBatcher(Batcher):
    """The Batcher for TestCacheSetOperation.

    Public attributes:

    dict<basestring, mixed> cache - A map from the keys to the values
        stored in the cache.
    """

    # The singleton instance of TestCacheSetBatcher, or None if we have not
    # created it yet.
    _instance = None

    def __init__(self):
        self.cache = {}

    @staticmethod
    def instance():
        """Return the singleton instance of TestCacheSetBatcher."""
        if TestCacheSetBatcher._instance is None:
            TestCacheSetBatcher._instance = TestCacheSetBatcher()
        return TestCacheSetBatcher._instance

    def gen_batch(self, operations):
        cache = TestCacheGetBatcher.instance().cache
        for operation in operations:
            cache[operation._key] = operation._value
        yield GenResult([None] * len(operations))
