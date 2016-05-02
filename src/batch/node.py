from operation import Batcher


class BatchNode(object):
    """A node in the dependency DAG for BatchExecutor.
    
    A node's children indicate on what a node is currently waiting.
    There are four types of nodes: root nodes, generator nodes,
    operation nodes, and batcher nodes.  A root node collects a
    BatchableExecutor's results, a generator node collects a generator's
    result, an operation node collects a BatchableOperation's result,
    and a batcher node collects the results of a batch of
    BatchableOperations having the same Batcher.
    
    BatchNodes have the following attributes:
    
    Root node:
    
    final batcher, final generator, final operation - None
    set<BatchNode> children - The children: the generator and operation
        nodes for the results of the BatchExecutor.
    list results - The list in which we store the results of the
        BatchExecutor.
    
    Generator node:
    
    final batcher, final operation - None
    set<BatchNode> children - The children: the generator and operation
        nodes on which the node is waiting, from the value that
        self.generator most recently yielded, excluding those whose
        results we have already obtained.
    tuple<type, mixed, traceback> exception_info - Information about the
        exception to propagate to self.generator, if any, as returned by
        sys.exc_info().  This is an exception produced in a generator or
        BatchableOperation from the value that self.generator most
        recently yielded.
    final Generator generator - The generator.
    bool is_result_list - Whether the value self.generator most recently
        yielded is a list or tuple.  This is not assigned until we
        execute the generator's first iteration.
    dict<BatchNode, int> parent_to_result_index - A map from the parents
        to the index in their "results" fields in which to store the
        result of the generator.  The result index is None for batcher
        nodes.  The parents are root nodes, generator nodes, and / or
        batcher nodes.
    list results - The list in which we store the results of the
        generators and BatchableOperations on which it is waiting, from
        the value that self.generator most recently yielded.  This is
        None if we have yet to execute the generator's first iteration.
        The results for a generator or BatchableOperation that raised an
        exception are None.  The list is parallel to the values that
        self.generator most recently yielded.
    
    Operation node:
    
    final generator - None
    final Batcher batcher - self.operation.batcher()
    set<BatchNode> children - The batcher node for the result of the
        operation.  This is empty if we have not yet started executing
        the operation.
    final BatchableOperation operation - The operation.
    final BatchNode parent - The parent: the generator node that will
        collect the result of self.operation.
    final int result_index - The index in self.parent.results in which
        to store the result of self.operation.
    
    Batcher node:
    
    final generator, final operation - None
    final Batcher batcher - The batcher.
    set<BatchNode> children - The children: the generator node that the
        Batcher's gen_batch method returned.
    final int operation_count - The number of BatchableOperations this
        node is batching.
    dict<BatchNode, int> parent_to_operation_index - A map from the
        operation nodes for the BatchableOperations whose results this
        node is computing to their indices in the results list.  Note
        that the length of parent_to_operation_index may differ from
        operation_count, due to exceptions.  If a generator X produced a
        BatchableOperation Y for this, and a generator or
        BatchableOperation Z that X is running in parallel with Y raises
        an exception, then X no longer requires the results of operation
        Y, and the node for Y does not appear in
        parent_to_operation_index.
    """
    
    def __init__(self, generator, operation, batcher):
        """Private constructor."""
        self.generator = generator
        self.operation = operation
        self.batcher = batcher
        self.children = set()
    
    @staticmethod
    def create_root_node():
        """Return a new root BatchNode."""
        return BatchNode(None, None, None)
    
    @staticmethod
    def create_generator_node(generator):
        """Return a new generator BatchNode for the specified Generator."""
        node = BatchNode(generator, None, None)
        node.results = None
        node.exception_info = None
        node.parent_to_result_index = {}
        return node
    
    @staticmethod
    def create_operation_node(operation, parent, result_index):
        """Return a new operation BatchNode.
        
        Assign the arguments to the attributes of the same names.  Add
        the node to the parent.children.
        """
        batcher = operation.batcher()
        if not isinstance(batcher, Batcher):
            raise TypeError(
                '{:s}.batcher() returned a {:s} rather than a Batcher'.format(
                    operation.__class__.__name__,
                    batcher.__class__.__name__))
        node = BatchNode(None, operation, batcher)
        node.parent = parent
        parent.children.add(node)
        node.result_index = result_index
        return node
    
    @staticmethod
    def create_batcher_node(batcher, operation_nodes):
        """Return a new batcher BatchNode.
        
        Batcher batcher - The batcher.
        object operation_nodes - A list or tuple of operation BatchNodes
            whose results the batcher node will compute.  We add the
            node to the operation nodes' "children" fields.
        """
        node = BatchNode(None, None, batcher)
        node.parent_to_operation_index = {}
        for (index, operation_node) in enumerate(operation_nodes):
            node.parent_to_operation_index[operation_node] = index
            operation_node.children.add(node)
        node.operation_count = len(operation_nodes)
        return node
    
    def is_root_node(self):
        return self.generator is None and self.batcher is None
    
    def is_generator_node(self):
        return self.generator is not None
    
    def is_operation_node(self):
        return self.operation is not None
    
    def is_batcher_node(self):
        return self.batcher is not None and self.operation is None
    
    def iter_parents(self):
        """Return an iterator over the node's parent BatchNodes."""
        if self.is_root_node():
            return ()
        elif self.is_generator_node():
            return self.parent_to_result_index.iterkeys()
        elif self.is_operation_node():
            return (self.parent,)
        else:
            return self.parent_to_operation_index.iterkeys()
