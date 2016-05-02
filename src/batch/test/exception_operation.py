from batch import BatchableOperation
from error import BatchTestError


class TestExceptionOperation(BatchableOperation):
    def batcher(self):
        raise BatchTestError()
