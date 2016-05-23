from batch import BatchableOperation
from batch import Batcher
from batch import GenResult
from db_operation import TestDbOperation


class TestDbObjectOperation(BatchableOperation):
    """Fetch the database object a given type and id.

    This class is designed so that the batcher will yield a
    BatchableOperation.
    """

    # Private attributes:
    # basestring _object_type - The type of object to fetch.
    # int _object_id - The object's id.

    def __init__(self, object_type, object_id):
        self._object_type = object_type
        self._object_id = object_id

    def batcher(self):
        return TestDbObjectBatcher(self._object_type)


class TestDbObjectBatcher(Batcher):
    # Private attributes:
    # basestring _object_type - The type of object.

    def __init__(self, object_type):
        self._object_type = object_type

    def gen_batch(self, operations):
        object_ids = list([operation._object_id for operation in operations])
        query_results = yield TestDbOperation(
            ('value', self._object_type, object_ids))
        yield GenResult(
            list([
                query_results[operation._object_id]
                for operation in operations]))

    def __eq__(self, other):
        return (
            isinstance(other, TestDbObjectBatcher) and
            self._object_type == other._object_type)

    def __ne__(self, other):
        return not (self == other)

    def __hash__(self):
        return hash(self._object_type)
