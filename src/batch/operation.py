class BatchableOperation(object):
    """An operation that is capable of being batched with related operations.
    
    The abstract superclass of operations that are capable of being
    batched with related operations.  Typically, a BatchableOperation
    describes some sort of data store operation.  A simple example of a
    BatchableOperation is a request for the value associated with a
    memcache key, which can be batched with other such requests.  Each
    BatchableOperation produces a result, which is computed by its
    Batcher.
    
    <T> - The type of the results of the operation.
    """
    
    def batcher(self):
        """Return the Batcher that can execute this operation in a batch.
        
        A group of operations that can be batched with each other return
        equal Batcher objects, as compared using ==, !=, and "hash".
        
        return Batcher<T> - The batcher.
        """
        raise NotImplementedError('Subclass must override')


class Batcher(object):
    """Executes a batch of BatchableOperations of a certain type.
    
    The abstract superclass of objects that execute a batch of
    BatchableOperations of a certain type.  For example, one Batcher
    subclass could fetch the values associated with a list of memcache
    keys.
    
    <T> - The type of the results of each operation.
    """
    
    def gen_batch(self, operations):
        """Execute a batch of BatchableOperations.
        
        This method is a batch generator that returns a list or tuple of
        the results of the operations.  The return value is parallel to
        "operations".
        
        Often, gen_batch will not require the special features
        associated with batch generation.  For example, looking up the
        values associated with a list of memcache keys does not require
        yielding any further BatchableOperations.  However, some
        Batchers may require the execution of further
        BatchableOperations.  For example, say we have a database, and
        we can batch multiple database queries.  Suppose we can also
        batch database requests for multiple users into a single
        condensed database query, which is more efficient than executing
        one query per user.  The gen_batch method for requesting users
        would compute the condensed query, yield a BatchableOperation
        that executes this query, and then yield the appropriate result.
        That way, we can batch the condensed query with other database
        queries.
        
        Question: Why does gen_batch need to be a batch generator?
        If BatchableOperations of type X return Batchers that yield
        BatchableOperations of type Y, couldn't we instead replace type
        X with a generator function F that yields BatchableOperations of
        type Y?
        
        Answer: No, because Batchers receive batches of operations from
        all generators that are running in parallel.  Such a function F
        would not have access to all of the operations we are trying to
        batch; it would only have access to one of the operations.  For
        example, take the user query batcher suggested above.  Such a
        function F would only have access to one of the user ids.  It
        would be impossible for F to compute the appropriate condensed
        database query from a single user id.
        
        list<BatchableOperation<T>> operations - A non-empty list of the
            operations to batch.  This method may assume that each
            operation's batcher() method returns a Batcher that is equal
            to this, as compared using ==, !=, and "hash".
        return list|tuple<T> - The results of the operations.
        """
        raise NotImplementedError('Subclass must override')
