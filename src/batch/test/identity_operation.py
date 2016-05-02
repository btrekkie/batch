from batch import BatchableOperation
from batch import Batcher
from batch import GenResult


class TestIdentityOperation(BatchableOperation):
    """An operation whose result is the argument to the constructor."""
    
    # Private attributes:
    # mixed _value - The result.
    
    def __init__(self, value):
        self._value = value
    
    def batcher(self):
        return TestIdentityBatcher.instance()


class TestIdentityBatcher(Batcher):
    # The singleton instance of TestIdentityBatcher, or None if we have not
    # created it yet.
    _instance = None
    
    @staticmethod
    def instance():
        """Return the singleton instance of TestIdentityBatcher."""
        if TestIdentityBatcher._instance is None:
            TestIdentityBatcher._instance = TestIdentityBatcher()
        return TestIdentityBatcher._instance
    
    def gen_batch(self, operations):
        yield GenResult(list([operation._value for operation in operations]))
