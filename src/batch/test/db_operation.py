from batch import BatchableOperation
from batch import Batcher
from batch import GenResult


class TestDbOperation(BatchableOperation):
    """Operation for a database query.
    
    Database queries take two forms, represented using lists or tuples:
    
    ['value', basestring object_type, int object_id] - A query for the
        object with the specified type and id.
    ['count', basestring object_type] - A query for the number of
        objects of the specified type.
    """
    
    # Private attributes:
    # list|tuple _query - The query to execute.
    
    def __init__(self, query):
        self._query = query
    
    def batcher(self):
        return TestDbBatcher.instance()


class TestDbBatcher(Batcher):
    # dict<str, dict<int, dict<str, mixed>>> - A map from object type to
    # a map from object id to a dictionary describing the object.
    _dict = {
        'chair': {
            60: {
                'color': 'brown',
                'legCount': 4,
                'material': 'wood',
            },
        },
        'user': {
            12: {'favoriteFood': 'ice cream'},
            42: {'favoriteFood': 'pizza'},
        },
    }
    
    # The singleton instance of TestDbBatcher, or None if we have not created
    # it yet.
    _instance = None
    
    @staticmethod
    def instance():
        """Return the singleton instance of TestDbBatcher."""
        if TestDbBatcher._instance is None:
            TestDbBatcher._instance = TestDbBatcher()
        return TestDbBatcher._instance
    
    def gen_batch(self, operations):
        results = []
        for operation in operations:
            if operation._query[0] == 'value':
                result = {}
                for object_id in operation._query[2]:
                    result[object_id] = (
                        TestDbBatcher._dict[operation._query[1]][object_id])
                results.append(result)
            else:
                # Count query
                results.append(len(TestDbBatcher._dict[operation._query[1]]))
        yield GenResult(results)
