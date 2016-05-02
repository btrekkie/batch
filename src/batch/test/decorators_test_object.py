from batch import cached_generator
from batch import GenResult
from batch import GeneratorCache
from error import BatchTestError
from identity_operation import TestIdentityOperation


class GenDecoratorsTestObject(object):
    """Provides methods decorated with cached_generator for GenDecoratorsTest.
    
    Public attributes:
    
    list<tuple<int, int, int, int>> cached_generator_args - A list of
        the tuples indicating the arguments, in order of declaration,
        passed to successive calls to gen_cached_identity, in order.
    dict<int, int> cached_identity_call_counts - A map from each value
        passed to gen_cached_identity to the number of times we called
        the method with the value as an argument.
    dict<int, int> cached_identity_with_yield_call_counts - A map from
        each value passed to gen_cached_identity_with_yield to the
        number of times we called the method with the value as an
        argument.
    dict<int, int> fibonacci_call_counts - A map from each value passed
        to a gen_fibonacci_* method apart from gen_fibonacci_from_obj to
        the number of times we called such a method with the value as an
        argument.
    list<list<object>> fibonacci_obj_args - A list of the arguments
        passed to successive calls to gen_fibonacci_from_obj, in order.
    dict<mixed, int> identity_with_cache1_call_counts - A map from each
        value passed to gen_identity_with_cache1 to the number of times
        we called the method with the value as an argument.
    dict<mixed, int> identity_with_cache2_call_counts - A map from each
        value passed to gen_identity_with_cache2 to the number of times
        we called the method with the value as an argument.
    dict<tuple<int, int>, int> sum_with_cache1_call_counts - A map from
        each tuple of the positional arguments passed to
        gen_sum_with_cache1 to the number of times we called the method
        with the arguments.
    """
    
    # The GeneratorCache for gen_identity_with_cache1 and gen_sum_with_cache1
    CACHE1 = GeneratorCache()
    
    # The GeneratorCache for gen_identity_with_cache2
    CACHE2 = GeneratorCache()
    
    def __init__(self):
        self.fibonacci_call_counts = {}
        self.fibonacci_obj_args = []
        self.cached_generator_args = []
        self.cached_identity_call_counts = {}
        self.cached_identity_with_yield_call_counts = {}
        self.identity_with_cache1_call_counts = {}
        self.sum_with_cache1_call_counts = {}
        self.identity_with_cache2_call_counts = {}
    
    @cached_generator
    def gen_fibonacci_without_operations(self, i):
        self.fibonacci_call_counts[i] = (
            self.fibonacci_call_counts.get(i, 0) + 1)
        if i == 0 or i == 1:
            yield GenResult(1)
        else:
            value1, value2 = yield (
                self.gen_fibonacci_without_operations(i - 1),
                self.gen_fibonacci_without_operations(i - 2))
            yield GenResult(value1 + value2)
    
    @cached_generator
    def gen_fibonacci_with_leaf_operations(self, i):
        self.fibonacci_call_counts[i] = (
            self.fibonacci_call_counts.get(i, 0) + 1)
        if i == 0 or i == 1:
            value = yield TestIdentityOperation(1)
            yield GenResult(value)
        else:
            value1, value2 = yield (
                self.gen_fibonacci_with_leaf_operations(i - 1),
                self.gen_fibonacci_with_leaf_operations(i - 2))
            yield GenResult(value1 + value2)
    
    @cached_generator
    def gen_fibonacci_with_intermediate_operations(self, i):
        self.fibonacci_call_counts[i] = (
            self.fibonacci_call_counts.get(i, 0) + 1)
        if i == 0 or i == 1:
            value = yield TestIdentityOperation(1)
            yield GenResult(value)
        else:
            addend1, addend2 = yield (
                TestIdentityOperation(i - 1), TestIdentityOperation(i - 2))
            value1, value2 = yield (
                self.gen_fibonacci_with_intermediate_operations(addend1),
                self.gen_fibonacci_with_intermediate_operations(addend2))
            yield GenResult(value1 + value2)
    
    @cached_generator
    def gen_fibonacci_from_obj(self, obj):
        self.fibonacci_obj_args.append(obj)
        for value in obj[0]:
            pass
        values = yield (
            self.gen_fibonacci_with_intermediate_operations(value),
            self.gen_fibonacci_with_intermediate_operations(obj[1]['foo']))
        yield GenResult(values)
    
    @cached_generator
    def gen_cached_generator(self, arg1, arg2, foo=3, bar=4):
        self.cached_generator_args.append((arg1, arg2, foo, bar))
        value1, value2, value3 = yield (
            TestIdentityOperation(arg1 + arg2), TestIdentityOperation(foo),
            TestIdentityOperation(bar))
        value4 = yield TestIdentityOperation(value2 * value3)
        yield GenResult(value1 + value4)
    
    def _gen_identity(self, value):
        result = yield TestIdentityOperation(value)
        yield GenResult(result)
    
    @cached_generator
    def gen_cached_identity(self, value):
        self.cached_identity_call_counts[value] = (
            self.cached_identity_call_counts.get(value, 0) + 1)
        if value < 0:
            raise BatchTestError()
        else:
            return self._gen_identity(value)
    
    @cached_generator
    def gen_cached_identity_with_yield(self, value):
        self.cached_identity_with_yield_call_counts[value] = (
            self.cached_identity_with_yield_call_counts.get(value, 0) + 1)
        if value < 0:
            raise BatchTestError()
        else:
            result = yield TestIdentityOperation(value)
            yield GenResult(result)
    
    @cached_generator(CACHE1)
    def gen_identity_with_cache1(self, value):
        self.identity_with_cache1_call_counts[value] = (
            self.identity_with_cache1_call_counts.get(value, 0) + 1)
        result = yield TestIdentityOperation(value)
        yield GenResult(result)
    
    @cached_generator(CACHE1)
    def gen_sum_with_cache1(self, value1, value2):
        self.sum_with_cache1_call_counts[(value1, value2)] = (
            self.sum_with_cache1_call_counts.get((value1, value2), 0) + 1)
        result1, result2 = yield (
            TestIdentityOperation(value1), TestIdentityOperation(value2))
        yield GenResult(result1 + result2)
    
    @cached_generator(CACHE2)
    def gen_identity_with_cache2(self, value):
        self.identity_with_cache2_call_counts[value] = (
            self.identity_with_cache2_call_counts.get(value, 0) + 1)
        result = yield TestIdentityOperation(value)
        yield GenResult(result)
