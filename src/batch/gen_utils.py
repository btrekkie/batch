from gen_result import GenResult


class GenUtils(object):
    """Provides utility methods for batch generation."""
    
    @staticmethod
    def _unpackage(value, generators):
        """Return "value", but with indices in place of generators.
        
        Return a result that is the same as "value", but with leaf
        values replaced with indices into "generators".  In place of a
        leaf value V, there will be an index I such that
        generators[I] == V.  Append any leaf values to "generators".
        
        object value - The value whose results we are obtaining,
            formatted as in the "value" argument to gen_structured.
        list generators - The list to which to append the leaf values.
        return object - The value with indices.
        """
        if isinstance(value, dict):
            indices = {}
            for key, val in value.iteritems():
                indices[key] = GenUtils._unpackage(val, generators)
            return indices
        elif isinstance(value, (list, tuple)):
            indices = []
            for element in value:
                indices.append(GenUtils._unpackage(element, generators))
            return indices
        else:
            index = len(generators)
            generators.append(value)
            return index
    
    @staticmethod
    def _package(indices, results):
        """Return "indices", but with results in place of indices.
        
        Return a result that is the same as "indices", but with leaf
        index values replaced with the corresponding elements in
        "results".  In place of a leaf value I, we will use results[I].
        
        object indices - The indices we are unpacking.  The set of types
            T permitted for "indices" consists of integers, lists whose
            elements are of types in T, and dictionaries whose values
            are of types in T.
        list results - The list from which we obtain the leaf values.
        return object - The value with results.
        """
        if isinstance(indices, dict):
            value = {}
            for key, ind in indices.iteritems():
                value[key] = GenUtils._package(ind, results)
            return value
        elif isinstance(indices, (list, tuple)):
            value = []
            for ind in indices:
                value.append(GenUtils._package(ind, results))
            return value
        else:
            return results[indices]
    
    @staticmethod
    def gen_structured(value):
        """Return "value", but with results in place of generators.
        
        Return a result that is the same as "value", but with leaf
        generator and BatchableOperation values replaced with their
        results.  For example,
        gen_structured({'foo' => gen_foo(), 'bar' => gen_bar()}) returns
        a map from 'foo' to the result of gen_foo() and from 'bar' to
        the result of gen_bar().
        
        object value - The value whose results we are obtaining.  The
            set of types T permitted for "value" consists of generators,
            BatchableOperations, lists whose elements are of types in T,
            and dictionaries whose values are of types in T.
        """
        generators = []
        indices = GenUtils._unpackage(value, generators)
        results = yield generators
        yield GenResult(GenUtils._package(indices, results))
    
    @staticmethod
    def gen_identity(value):
        yield GenResult(value)
