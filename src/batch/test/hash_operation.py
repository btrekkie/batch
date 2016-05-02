from batch import BatchableOperation
from batch import Batcher
from batch import GenResult


class TestHashOperation(BatchableOperation):
    """An operation that simulates fetching from a key-value data store."""
    
    # Private attributes:
    # basestring _key - The key to fetch.
    
    def __init__(self, key):
        self._key = key
    
    def batcher(self):
        return TestHashBatcher.instance()


class TestHashBatcher(Batcher):
    # The key-value pairs in the simulated data store.
    _dict = {
        'chair:60': {
            'color': 'brown',
            'legCount': 4,
            'material': 'wood',
        },
        'coolChairId': 60,
        'coolUserId': 42,
        'spouseId:42': 12,
        'uncoolUserId': 13,
        'user:12': {'favoriteFood': 'ice cream'},
        'user:13': {'favoriteFood': 'burger'},
        'user:42': {'favoriteFood': 'pizza'},
    }
    
    # The singleton instance of TestHashBatcher, or None if we have not created
    # it yet.
    _instance = None
    
    @staticmethod
    def instance():
        """Return the singleton instance of TestHashBatcher."""
        if TestHashBatcher._instance is None:
            TestHashBatcher._instance = TestHashBatcher()
        return TestHashBatcher._instance
    
    def gen_batch(self, operations):
        yield GenResult(
            list([
                TestHashBatcher._dict[operation._key]
                for operation in operations]))
