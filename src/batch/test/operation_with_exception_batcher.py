from batch import BatchableOperation
from batch import Batcher
from error import BatchTestError


class TestOperationWithExceptionBatcherOperation(BatchableOperation):
    """An operation whose Batcher's gen_batch method raises a BatchTestError.
    """
    
    def batcher(self):
        return TestOperationWithExceptionBatcher.instance()


class TestOperationWithExceptionBatcher(Batcher):
    # The singleton instance of TestOperationWithExceptionBatcher, or None if
    # we have not created it yet.
    _instance = None
    
    @staticmethod
    def instance():
        """Return the singleton instance of TestOperationWithExceptionBatcher.
        """
        if TestOperationWithExceptionBatcher._instance is None:
            TestOperationWithExceptionBatcher._instance = (
                TestOperationWithExceptionBatcher())
        return TestOperationWithExceptionBatcher._instance
    
    def gen_batch(self, operations):
        raise BatchTestError()
