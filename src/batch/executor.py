import sys
from types import GeneratorType

from gen_result import GenResult
from node import BatchNode
from operation import BatchableOperation


class BatchExecutor(object):
    """Provides the ability to performed batch execution.

    See the comments for "execute".
    """

    # Private attributes:
    # dict<Generator, BatchNode> _generator_nodes - A map to the
    #     generator nodes in the graph from their generators.
    # set<BatchNode> _leaf_generator_nodes - The generator nodes in the
    #     graph that have no children.
    # dict<Batcher, set<BatchNode>> _leaf_operation_nodes - A map from
    #     the batchers of the operation nodes in the graph that have no
    #     children to the operation nodes.
    # BatchNode _root_node - The graph's root node.

    def _generator_node(self, generator, parent, result_index):
        """Return a BatchNode for the specified Generator.

        Reuse the generator's existing node if it is present, and create
        a new node if not.  Add a mapping from "parent" to result_index
        in the node's parent_to_result_index field, and add the node to
        parent.children.

        Generator generator - The generator.
        BatchNode parent - The parent node.
        int result_index - The result index.
        return BatchNode - The generator node.
        """
        generator_node = self._generator_nodes.get(generator)
        if generator_node is None:
            generator_node = BatchNode.create_generator_node(generator)
            self._generator_nodes[generator] = generator_node
            self._leaf_generator_nodes.add(generator_node)
        generator_node.parent_to_result_index[parent] = result_index
        parent.children.add(generator_node)
        return generator_node

    def _generator_or_operation_node(
            self, generator_or_operation, parent, result_index):
        """Return a node for the specified Generator or BatchableOperation.

        If generator_or_operation is a Generator, reuse the generator's
        existing node if it is present, and create a new node if not.

        mixed generator_or_operation - The Generator or
            BatchableOperation.  If this is not a Generator or
            BatchableOperation, the method returns None.
        BatchNode parent - The parent of the created node.
        int result_index - The index in parent.results in which to store
            the result of the generator node.  This is None if "parent"
            is a batcher node.
        return BatchNode - The node.
        """
        if isinstance(generator_or_operation, GeneratorType):
            return self._generator_node(
                generator_or_operation, parent, result_index)
        elif isinstance(generator_or_operation, BatchableOperation):
            # Create an operation node
            operation_node = BatchNode.create_operation_node(
                generator_or_operation, parent, result_index)
            self._leaf_operation_nodes.setdefault(
                operation_node.batcher, set()).add(operation_node)
            return operation_node
        else:
            return None

    def __init__(self, generators_and_operations):
        """Initialize a BatchGenerator.

        Initialize a BatchGenerator for computing
        executev(generators_and_operations).  The _run()
        method will perform the computation.
        """
        self._leaf_generator_nodes = set()
        self._leaf_operation_nodes = {}
        self._generator_nodes = {}
        self._root_node = BatchNode.create_root_node()
        self._root_node.results = [None] * len(generators_and_operations)

        # Add the root node's children
        for (index, generator_or_operation) in (
                enumerate(generators_and_operations)):
            node = self._generator_or_operation_node(
                generator_or_operation, self._root_node, index)
            if node is None:
                raise TypeError(
                    'execute_batches and the like accept only generators and '
                    'BatchableOperations as arguments')

    def _transmit_result(self, generator_node, parent, result, result_index):
        """Send the result of a generator node in a parent node.

        Send the result "result" of the generator node generator_node to
        the parent node "parent".

        BatchNode generator_node - The generator node.
        BatchNode parent - The parent node.
        mixed result - The result value.
        int result_index - The index in parent.results in which to store
            the result, if "parent" is a generator node.
        """
        if not parent.is_batcher_node():
            # Transmit the result to a root or generator node
            parent.results[result_index] = result
            parent.children.remove(generator_node)
            if not parent.children and not parent.is_root_node():
                self._leaf_generator_nodes.add(parent)
        else:
            batcher_node = parent

            # Verify the return value
            if not isinstance(result, (list, tuple)):
                raise TypeError(
                    'The result of {:s}.gen_batch was of type {:s} instead of '
                    'list or tuple'.format(
                        batcher_node.batcher.__class__.__name__,
                        result.__class__.__name__))
            elif len(result) != batcher_node.operation_count:
                raise ValueError(
                    'The result of {:s}.gen_batch did not have the same '
                    'length as the argument to gen_batch'.format(
                        batcher_node.batcher.__class__.__name__))

            # Transmit the batch's results to the operation nodes
            for (operation_node, index) in (
                    batcher_node.parent_to_operation_index.iteritems()):
                operation_node.parent.results[operation_node.result_index] = (
                    result[index])
                operation_node.parent.children.remove(operation_node)
                if (not operation_node.parent.children and
                        not operation_node.parent.is_root_node()):
                    self._leaf_generator_nodes.add(operation_node.parent)

    def _transmit_exception(self, generator_node, parent, exception_info):
        """Propagate an exception from generator_node to its parent "parent".

        BatchNode generator_node - The generator node from which to
            propagate the exception.  If the exception did not occur in
            a generator node, this is None.
        BatchNode parent - The parent of generator_node to which to
            propagate the exception.
        tuple<type, mixed, traceback> exception_info - Information about
            the exception, as returned by sys.exc_info().
        """
        if parent.is_root_node():
            # Re-raise the exception.  It will propagate to the caller
            # of _run().
            raise exception_info[1], None, exception_info[2]
        elif parent.is_generator_node():
            parent.exception_info = exception_info
            parent.children.remove(generator_node)
            if not parent.children:
                self._leaf_generator_nodes.add(parent)
        else:
            # Batcher node
            for operation_node in parent.parent_to_operation_index.iterkeys():
                grandparent = operation_node.parent
                grandparent.exception_info = exception_info
                grandparent.children.remove(operation_node)
                if not grandparent.children:
                    self._leaf_generator_nodes.add(grandparent)

    def _iterate_generator_node(self, node):
        """Perform one iteration on the specified generator node's Generator.

        BatchNode node - The generator node.
        """
        try:
            if node.exception_info is not None:
                # Have the yield statement propagate the exception
                exception_info = node.exception_info
                node.exception_info = None
                yield_value = node.generator.throw(
                    exception_info[0], exception_info[1],
                    exception_info[2])
            elif node.results is None:
                # First iteration
                yield_value = node.generator.next()
            else:
                # Have the yield statement return the results
                results = node.results
                node.results = None
                if not node.is_result_list:
                    results = results[0]
                yield_value = node.generator.send(results)
        except StopIteration:
            yield_value = GenResult(None)
        except Exception:
            exception_info = sys.exc_info()
            for parent in node.parent_to_result_index.copy().iterkeys():
                if parent in node.parent_to_result_index:
                    self._transmit_exception(node, parent, exception_info)
            return

        if isinstance(yield_value, GenResult):
            # Transmit the result and destroy the generator node
            for (parent, result_index) in (
                    node.parent_to_result_index.iteritems()):
                self._transmit_result(
                    node, parent, yield_value._value, result_index)
            node.generator.close()
            self._generator_nodes.pop(node.generator)
        else:
            # Create child nodes for the generators and / or
            # BatchableOperations that node.generator just yielded
            node.is_result_list = isinstance(yield_value, (list, tuple))
            if not node.is_result_list:
                yield_value = (yield_value,)
            for (index, generator_or_operation) in enumerate(yield_value):
                try:
                    child = self._generator_or_operation_node(
                        generator_or_operation, node, index)
                except Exception:
                    # e.g. BatchableOperation.batcher() raised an exception
                    node.exception_info = sys.exc_info()
                    self._leaf_generator_nodes.add(node)
                    return
                else:
                    if child is None:
                        raise TypeError(
                            'Batch generators may only yield generators, '
                            'BatchableOperations, lists or tuples containing '
                            'the two, and GenResults')
            node.results = [None] * len(yield_value)
            if not yield_value:
                self._leaf_generator_nodes.add(node)

    def _execute_batch(self, batcher, operation_nodes):
        """Start computing the results of a batch of operations.

        Add a node to compute the results of a batch of operations for
        the specified operation nodes.

        Batcher batcher - The batcher for computing the batch's results.
        object operation_nodes - A list or tuple of operation
            BatchNodes.
        """
        batcher_node = BatchNode.create_batcher_node(batcher, operation_nodes)
        operations = list([node.operation for node in operation_nodes])
        try:
            generator = batcher.gen_batch(operations)
        except Exception:
            self._transmit_exception(None, batcher_node, sys.exc_info())
        else:
            if not isinstance(generator, GeneratorType):
                raise ValueError(
                    '{:s}.gen_batch() is not a generator method'.format(
                        batcher.__class__.__name__))
            generator_node = self._generator_node(
                generator, batcher_node, None)
            self._leaf_generator_nodes.add(generator_node)

    def _run(self):
        """Compute the executor's results.

        Compute the results of the generators and BatchableOperations
        passed to the constructor.  This method may only be called once
        per instance.
        """
        while self._leaf_generator_nodes or self._leaf_operation_nodes:
            while self._leaf_generator_nodes:
                self._iterate_generator_node(self._leaf_generator_nodes.pop())
            if self._leaf_operation_nodes:
                batcher, operation_nodes = self._leaf_operation_nodes.popitem()
                self._execute_batch(batcher, list(operation_nodes))
        if self._root_node.children:
            raise RuntimeError(
                'The generators form a cycle, i.e. there is a generator that '
                'is waiting on its own results')
        return self._root_node.results

    @staticmethod
    def execute(generator_or_operation):
        """Execute batches from a generator or BatchableOperation.

        This coroutine assists in batching BatchableOperations in the
        presence of potentially complex dependency relationships, which
        we express using generators.  Batching occurs on a single
        thread.  Typically, a callsite passes a "batch generator" to the
        "execute" method.  Each of a batch generator's yield statements
        indicate one or more operations to run in parallel, with the
        results of the operations appearing as the return values of the
        yield statements.  Once finished, a batch generator yields a
        GenResult object indicating the generator's result.

        To be precise, a "batch generator" is a generator that yields
        batch generators, BatchableOperations, lists or tuples of such
        values, and / or GenResult objects.  When run using execute*,
        except in the case of GenResult objects, the return value of a
        batch generator's yield statement is the same as the argument to
        the yield statement, but with batch generators and
        BatchableOperations replaced with their results (and with tuples
        changed to lists).  When a batch generator yields a GenResult
        object, the GenResult object's value is designated the result of
        the generator.  Upon yielding a GenResult, the function exits
        (by means of a call to Generator.close()).  (If a batch
        generator finishes without yielding a GenResult object, None is
        designated the result of the generator.)  "execute" returns the
        result of the batch generator or BatchableOperation passed to
        it.

        Consider the following example:


        # Return the User with the specified ID.
        def gen_user(user_id):
            # Compute the key associated with the user in data store,
            # which is a simple key-value hash
            data_store_key = 'user:{:d}'.format(user_id)

            # Run a DataStoreOperation to fetch the dictionary
            # associated with data_store_key.  Store the result in
            # user_data.
            user_data = yield DataStoreOperation(data_store_key)

            # Wrap the dictionary in a User object, and yield it as the
            # result of gen_user
            yield GenResult(User(user_data))

        # Return the User object for the spouse of the user with the
        # specified ID.
        def gen_spouse(user_id):
            # Compute the key for fetching the user's spouse's ID
            data_store_key = 'spouseId:{:d}'.format(user_id)

            # Run a DataStoreOperation to fetch the integer associated
            # with data_store_key.  Store the result in spouse_id.
            spouse_id = yield DataStoreOperation(data_store_key)

            # Fetch the user with ID spouse_id, and store the result in
            # "spouse"
            spouse = yield gen_user(spouse_id)

            # Yield the spouse as the result of gen_spouse
            yield GenResult(spouse)

        # Return a tuple containing the User with the specified ID and
        # his spouse.
        def gen_spouses(user_id)
            # Fetch the user with ID user_id, and in parallel, fetch his
            # spouse.  Store the user in "user" and the spouse in
            # "spouse".
            user, spouse = yield (gen_user(user_id), gen_spouse(user_id))

            # Yield (user, spouse) as the result of gen_spouses
            yield GenResult((user, spouse))

        BatchExecutor.execute(gen_spouses(12345))


        To summarize, the gen_spouses function fetches the user with
        gen_user and the spouse with gen_spouse in parallel.  The
        gen_spouse function fetches the spouse ID with
        DataStoreOperation, then fetches the spouse with gen_user.
        gen_user fetches the user dictionary with DataStoreOperation,
        then wraps it in a User object.

        By representing these dependencies as batch generators, we are
        able to effectively batch the data fetching operations.
        Specifically, we will only make two round-trip requests to the
        data store: one to fetch the user dictionary for the user with
        ID 12345 and the ID of his spouse, and one to fetch the user
        dictionary for his spouse.  By contrast, a naive approach would
        have required three round trips - one for each of the three data
        store keys.

        For performance reasons, we should call execute* methods at the
        highest level possible.  In particular, calling "execute" within
        a batch generator is harmful to performance, because it
        eliminates opportunities for batching.  Batch generators should
        use yielding instead of calling "execute".

        If a batch generator yields another generator or a
        BatchableOperation, and the yielded generator or the yielded
        operation's batcher() method raises an exception, "execute" will
        propagate the exception to the yielding generator.  If a
        Batcher's gen_batch method raises an exception, "execute" will
        propagate the exception to the generators that yielded the
        BatchableOperations it is batching.  In the case of an
        exception, we finish any generators or BatchableOperations that
        were running in parallel with the one that raised the exception
        before propagating it.

        Multiple batch generators may yield the same Generator object,
        so as to share the result of the generator.  However, we should
        only do so using the SharedGenerator class.  See that class's
        comments.

        By convention, batch generator functions begin with the prefix
        "gen".  For clarity of exposition, documentation may refer to a
        batch generator's result as the function's "return value", even
        though this is technically inaccurate, as the return value is a
        Generator.  Likewise, we may refer to functions that return
        batch generators (sometimes called "generators" for short) as
        generator functions, although this is technically incorrect.
        Two batchable generators are said to be running "in parallel" if
        the generators are both partway through execution.

        object generator_or_operation - The batch generator or
            BatchableOperation.
        return mixed - The result of generator_or_operation.
        """
        return BatchExecutor([generator_or_operation])._run()[0]

    @staticmethod
    def executev(generators_and_operations):
        """Execute batches from generators and / or BatchableOperations.

        Compute the results of the specified list or tuple of generators
        and / or BatchOperations.  This is a variant of "execute".  See
        that method's comments.

        list|tuple generators_and_operations - A list or tuple of the
            generators and / or BatchOperations.
        return list - A list of the results of the generators and / or
            BatchableOperations.  The list is parallel to the argument.
        """
        return BatchExecutor(generators_and_operations)._run()

    @staticmethod
    def executeva(*args):
        """Execute batches from generators and / or BatchableOperations.

        Compute the results of the specified generators and / or
        BatchOperations.  This is a variant of "execute".  See that
        method's comments.

        tuple args - The generators and / or BatchOperations.
        return list - A list of the results of the generators and / or
            BatchableOperations.  The list is parallel to "args".
        """
        return BatchExecutor(args)._run()
