from batch import BatchableOperation
from batch import Batcher
from batch import GenResult
from error import BatchTestError
from identity_operation import TestIdentityOperation


class TestOperationWithNestedExceptionBatcherOperation(BatchableOperation):
    """An operation whose Batcher's gen_batch method raises a BatchTestError.
    
    An operation whose Batcher's gen_batch method yields to a generator
    that raises a BatchTestError.
    """
    
    def batcher(self):
        return TestOperationWithNestedExceptionBatcher.instance()


class TestOperationWithNestedExceptionBatcher(Batcher):
    # The singleton instance of TestOperationWithNestedExceptionBatcher, or
    # None if we have not created it yet.
    _instance = None
    
    @staticmethod
    def instance():
        """Return the singleton instance."""
        if TestOperationWithNestedExceptionBatcher._instance is None:
            TestOperationWithNestedExceptionBatcher._instance = (
                TestOperationWithNestedExceptionBatcher())
        return TestOperationWithNestedExceptionBatcher._instance
    
    def _gen_nested_exception(self):
        yield TestIdentityOperation(5)
        raise BatchTestError()
    
    def gen_batch(self, operations):
        result = yield self._gen_nested_exception()
        yield GenResult(result)
