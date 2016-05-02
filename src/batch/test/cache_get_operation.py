from batch import BatchableOperation
from batch import Batcher
from batch import GenResult


class TestCacheGetOperation(BatchableOperation):
    """An operation that simulates fetching from a key-value data store.
    
    An operation that simulates fetching from a key-value data store
    that serves as a cache for some other data store.  We may store
    cached values using TestCacheGetOperation.
    """
    
    # Private attributes:
    # basestring _key - The key to fetch.
    
    def __init__(self, key):
        self._key = key
    
    def batcher(self):
        return TestCacheGetBatcher.instance()


class TestCacheGetBatcher(Batcher):
    """The Batcher for TestCacheGetOperation.
    
    Public attributes:
    
    dict<basestring, mixed> cache - A map from the keys to the cached
        values.
    """
    
    # The singleton instance of TestCacheGetBatcher, or None if we have not
    # created it yet.
    _instance = None
    
    def __init__(self):
        self.cache = {}
    
    @staticmethod
    def instance():
        """Return the singleton instance of TestCacheGetBatcher."""
        if TestCacheGetBatcher._instance is None:
            TestCacheGetBatcher._instance = TestCacheGetBatcher()
        return TestCacheGetBatcher._instance
    
    def gen_batch(self, operations):
        yield GenResult(
            list([self.cache.get(operation._key) for operation in operations]))
