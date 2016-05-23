import unittest

from batch import BatchExecutor
from batch import GenResult
from cache_get_operation import TestCacheGetOperation
from cache_get_operation import TestCacheGetBatcher
from cache_set_operation import TestCacheSetOperation
from db_object_operation import TestDbObjectOperation
from db_operation import TestDbOperation
from error import BatchTestError
from exception_operation import TestExceptionOperation
from hash_operation import TestHashOperation
from identity_operation import TestIdentityOperation
from operation_with_exception_batcher import (
    TestOperationWithExceptionBatcherOperation)
from operation_with_nested_exception_batcher import (
    TestOperationWithNestedExceptionBatcherOperation)
from user import TestUser


class BatchExecutorTest(unittest.TestCase):
    def _gen_result_only(self):
        yield GenResult('foo')

    def test_result_only(self):
        """Test a generator that simply yields a result."""
        self.assertEqual('foo', BatchExecutor.execute(self._gen_result_only()))

    def _gen_default_return(self):
        """Generator method that yields nothing."""
        if False:
            yield GenResult('foo')

    def test_return_none_by_default(self):
        """Test a generator that yields nothing."""
        self.assertIsNone(BatchExecutor.execute(self._gen_default_return()))

    def _gen_single_operation(self):
        user_data = yield TestHashOperation('coolUserId')
        yield GenResult(user_data)

    def test_single_operation(self):
        """Test a generator method that yields a single BatchableOperation.

        Test a generator method that yields a single BatchableOperation,
        then yields its result.
        """
        self.assertEqual(
            42, BatchExecutor.execute(self._gen_single_operation()))

    def test_execute_single_operation(self):
        """Test calling BatchExecutor.execute on a BatchableOperation."""
        self.assertEqual(
            42, BatchExecutor.execute(TestHashOperation('coolUserId')))

    def _gen_multiple_operations_list(self):
        result = yield [
            TestHashOperation('coolUserId'),
            TestHashOperation('uncoolUserId'),
            TestHashOperation('coolChairId')]
        yield GenResult(result)

    def _gen_multiple_operations_tuple(self):
        result = yield (
            TestHashOperation('coolUserId'),
            TestHashOperation('uncoolUserId'),
            TestHashOperation('coolChairId'))
        yield GenResult(result)

    def test_multiple_operations(self):
        """Test generators that yield a list or tuple of BatchableOperations.

        Test generators that yield a list or tuple of
        BatchableOperations, then yield their results.
        """
        self.assertEqual(
            [42, 13, 60],
            BatchExecutor.execute(self._gen_multiple_operations_list()))
        self.assertEqual(
            [42, 13, 60],
            BatchExecutor.execute(self._gen_multiple_operations_tuple()))

    def _gen_empty_yield(self):
        value = yield ()
        yield GenResult(value)

    def test_empty_yield(self):
        self.assertEqual([], BatchExecutor.execute(self._gen_empty_yield()))

    def _gen_fibonacci_without_operations(self, i):
        if i == 0 or i == 1:
            yield GenResult(1)
        else:
            value1, value2 = yield (
                self._gen_fibonacci_without_operations(i - 1),
                self._gen_fibonacci_without_operations(i - 2))
            yield GenResult(value1 + value2)

    def _gen_fibonacci_with_leaf_operations(self, i):
        """Designed to test nested generators with operations at leaves.

        Designed to test nested generators that yield
        BatchableOperations in base cases, but not in other cases.
        """
        if i == 0 or i == 1:
            value = yield TestIdentityOperation(1)
            yield GenResult(value)
        else:
            value1, value2 = yield (
                self._gen_fibonacci_with_leaf_operations(i - 1),
                self._gen_fibonacci_with_leaf_operations(i - 2))
            yield GenResult(value1 + value2)

    def _gen_fibonacci_with_intermediate_operations(self, i):
        """Designed to test a recursive generator with operations at all nodes.

        Designed to test nested generators that yield
        BatchableOperations in each call.
        """
        if i == 0 or i == 1:
            value = yield TestIdentityOperation(1)
            yield GenResult(value)
        else:
            addend1, addend2 = yield (
                TestIdentityOperation(i - 1), TestIdentityOperation(i - 2))
            value1, value2 = yield (
                self._gen_fibonacci_with_intermediate_operations(addend1),
                self._gen_fibonacci_with_intermediate_operations(addend2))
            yield GenResult(value1 + value2)

    def test_fibonacci(self):
        """Test various implementations of computing Fibonacci numbers.

        Test various generator-based implementations of computing
        Fibonacci numbers, which have somewhat complex generator trees.
        """
        self.assertEqual(
            233,
            BatchExecutor.execute(self._gen_fibonacci_without_operations(12)))
        self.assertEqual(
            [89, 233],
            BatchExecutor.executeva(
                self._gen_fibonacci_without_operations(10),
                self._gen_fibonacci_without_operations(12)))
        self.assertEqual(
            233,
            BatchExecutor.execute(
                self._gen_fibonacci_with_leaf_operations(12)))
        self.assertEqual(
            [89, 233],
            BatchExecutor.executeva(
                self._gen_fibonacci_with_leaf_operations(10),
                self._gen_fibonacci_with_leaf_operations(12)))
        self.assertEqual(
            233,
            BatchExecutor.execute(
                self._gen_fibonacci_with_intermediate_operations(12)))
        self.assertEqual(
            [89, 233],
            BatchExecutor.executeva(
                self._gen_fibonacci_with_intermediate_operations(10),
                self._gen_fibonacci_with_intermediate_operations(12)))

    def _gen_user_from_hash(self, user_id):
        key = 'user:{:d}'.format(user_id)
        user_data = yield TestHashOperation(key)
        yield GenResult(TestUser(user_data))

    def _gen_spouse(self, user_id):
        key = 'spouseId:{:d}'.format(user_id)
        spouse_id = yield TestHashOperation(key)
        spouse = yield self._gen_user_from_hash(spouse_id)
        yield GenResult(spouse)

    def _gen_spouses(self, user_id):
        spouses = yield (
            self._gen_user_from_hash(user_id), self._gen_spouse(user_id))
        yield GenResult(spouses)

    def test_spouses(self):
        """Test a generator that fetches a user and his spouse in parallel."""
        spouses = BatchExecutor.execute(self._gen_spouses(42))
        self.assertEqual(2, len(spouses))
        favorite_foods = list([spouse.favorite_food() for spouse in spouses])
        self.assertEqual(set(['ice cream', 'pizza']), set(favorite_foods))

    def _gen_user_from_db(self, user_id):
        user_data = yield TestDbObjectOperation('user', user_id)
        yield GenResult(TestUser(user_data))

    def _gen_db_info(self):
        yield GenResult((
            yield [
                self._gen_user_from_db(42), self._gen_user_from_db(12),
                TestDbObjectOperation('chair', 60),
                TestDbOperation(('count', 'user'))]))

    def test_db(self):
        """Test a Batcher that yields BatchableOperations.

        Test a case in which a Batcher's gen_batch method yields
        BatchableOperations that may be batched with other operations.
        """
        user1, user2, chair_data, user_count = BatchExecutor.execute(
            self._gen_db_info())
        self.assertEqual('pizza', user1.favorite_food())
        self.assertEqual('ice cream', user2.favorite_food())
        self.assertEqual('brown', chair_data['color'])
        self.assertEqual(2, user_count)

    def _gen_hash_with_cache(self, key):
        """Fetch a hash key using the cache.

        Fetches the value associated with the specified key in
        TestHashOperation.  Uses the cache suggested by
        TestCacheGetOperation as a cache for the hash operation.
        """
        result = yield TestCacheGetOperation(key)
        if result is not None:
            yield GenResult(result)
        result = yield TestHashOperation(key)
        yield TestCacheSetOperation(key, result)
        yield GenResult(result)

    def _gen_user_from_hash_with_cache(self, user_id):
        key = 'user:{:d}'.format(user_id)
        user_data = yield self._gen_hash_with_cache(key)
        yield GenResult(TestUser(user_data))

    def _gen_spouse_with_cache(self, user_id):
        key = 'spouseId:{:d}'.format(user_id)
        spouse_id = yield self._gen_hash_with_cache(key)
        user = yield self._gen_user_from_hash(spouse_id)
        yield GenResult(user)

    def _gen_spouses_with_cache(self, user_id):
        spouses = yield (
            self._gen_user_from_hash_with_cache(user_id),
            self._gen_spouse_with_cache(user_id))
        yield GenResult(spouses)

    def _gen_serial_with_cache(self):
        """Designed as a long serial operation that ends by fetching spouses.

        Designed as a long serial operation that ends by calling
        _gen_spouses_with_cache.  That way, if some other operation
        fetches spouses in the same manner, this method should be able
        to attain cache hits.
        """
        cool_chair_id = yield self._gen_hash_with_cache('coolChairId')
        chair = yield self._gen_hash_with_cache(
            'chair:{:d}'.format(cool_chair_id))
        uncool_user_id = yield self._gen_hash_with_cache('uncoolUserId')
        uncool_user = yield self._gen_user_from_hash_with_cache(uncool_user_id)
        cool_user_id = yield self._gen_hash_with_cache('coolUserId')
        spouses = yield self._gen_spouses_with_cache(cool_user_id)
        yield GenResult((chair, uncool_user, spouses))

    def _gen_with_cache(self):
        result = yield (
            self._gen_user_from_hash_with_cache(42),
            self._gen_spouses_with_cache(42),
            self._gen_serial_with_cache())
        yield GenResult(result)

    def test_cache(self):
        """Test TestHashOperations backed by a cache.

        Test fetching values using TestHashOperations, with the results
        backed by the cache suggested by TestCacheGetOperation.
        """
        TestCacheGetBatcher.instance().cache.clear()
        result = BatchExecutor.execute(self._gen_with_cache())
        self.assertEqual('pizza', result[0].favorite_food())
        favorite_foods1 = list(
            [spouse.favorite_food() for spouse in result[1]])
        self.assertEqual(set(['ice cream', 'pizza']), set(favorite_foods1))
        self.assertEqual('brown', result[2][0]['color'])
        self.assertEqual('burger', result[2][1].favorite_food())
        favorite_foods2 = list([
            spouse.favorite_food() for spouse in result[2][2]])
        self.assertEqual(set(['ice cream', 'pizza']), set(favorite_foods2))

    def _gen_result_then_exception(self):
        yield GenResult('foo')
        raise BatchTestError(
            'Generator should be closed when it yields a result')

    def test_result_then_exception(self):
        """Test that code after yielding a result is not executed.

        Test that code after yielding a result is not executed, by
        executing a generator that yields a result and then raises an
        exception.
        """
        self.assertEqual(
            'foo', BatchExecutor.execute(self._gen_result_then_exception()))

    def _gen_inner_execute_batches(self):
        chair_data = yield TestHashOperation('coolChairId')
        yield GenResult(chair_data)

    def _gen_outer_execute_batches(self):
        yield GenResult(
            BatchExecutor.execute(self._gen_inner_execute_batches()))

    def test_nested_execute_batches(self):
        """Test a nested call to BatchExecutor.execute."""
        self.assertEqual(
            60, BatchExecutor.execute(self._gen_outer_execute_batches()))

    def _gen_exception(self):
        yield TestHashOperation('coolUserId')
        raise BatchTestError()

    def _gen_catch_generator_exception(self):
        try:
            result = yield (
                self._gen_exception(), TestHashOperation('uncoolUserId'))
            yield GenResult(result)
        except BatchTestError:
            chair_data = yield TestHashOperation('coolChairId')
            yield GenResult(chair_data)

    def _gen_nested_exception(self):
        result = yield (self._gen_exception(), TestHashOperation('user:42'))
        yield GenResult(result)

    def _gen_catch_nested_generator_exception(self):
        try:
            result = yield (
                self._gen_nested_exception(),
                TestHashOperation('uncoolUserId'))
            yield GenResult(result)
        except BatchTestError:
            chair_data = yield TestHashOperation('coolChairId')
            yield GenResult(chair_data)

    def _gen_catch_operation_exception(self):
        try:
            result = yield TestExceptionOperation()
            yield GenResult(result)
        except BatchTestError:
            chair_data = yield TestHashOperation('coolChairId')
            yield GenResult(chair_data)

    def _gen_catch_batcher_exception(self):
        try:
            result = yield TestOperationWithExceptionBatcherOperation()
            yield GenResult(result)
        except BatchTestError:
            chair_data = yield TestHashOperation('coolChairId')
            yield GenResult(chair_data)

    def _gen_catch_nested_batcher_exception(self):
        try:
            result = yield TestOperationWithNestedExceptionBatcherOperation()
            yield GenResult(result)
        except BatchTestError:
            chair_data = yield TestHashOperation('coolChairId')
            yield GenResult(chair_data)

    def _gen_raise_generator_exception(self):
        result = yield (
            self._gen_exception(), TestHashOperation('uncoolUserId'))
        yield GenResult(result)

    def _gen_raise_nested_generator_exception(self):
        result = yield (
            self._gen_nested_exception(), TestHashOperation('uncoolUserId'))
        yield GenResult(result)

    def _gen_raise_operation_exception(self):
        result = yield TestExceptionOperation()
        yield GenResult(result)

    def _gen_raise_batcher_exception(self):
        result = yield TestOperationWithExceptionBatcherOperation()
        yield GenResult(result)

    def _gen_raise_nested_batcher_exception(self):
        result = yield TestOperationWithNestedExceptionBatcherOperation()
        yield GenResult(result)

    def _gen_exception_after_a_while(self):
        for index in xrange(10):
            yield TestIdentityOperation(index)
        raise BatchTestError()

    def _gen_double_exception(self):
        yield (self._gen_exception(), self._gen_exception_after_a_while())

    def _gen_take_a_while(self, reached_end):
        for index in xrange(10):
            yield TestIdentityOperation(index)
        reached_end[0] = True

    def _gen_exception_and_take_a_while(self, reached_end):
        yield (self._gen_exception(), self._gen_take_a_while(reached_end))

    def test_raise_exception(self):
        """Test exception handling.

        Test that exceptions are propagated to the parent generators or
        to the call to BatchExecutor.execute as appropriate.
        """
        self.assertEqual(
            60, BatchExecutor.execute(self._gen_catch_generator_exception()))
        self.assertEqual(
            60,
            BatchExecutor.execute(
                self._gen_catch_nested_generator_exception()))
        self.assertEqual(
            60, BatchExecutor.execute(self._gen_catch_operation_exception()))
        self.assertEqual(
            60, BatchExecutor.execute(self._gen_catch_batcher_exception()))
        self.assertEqual(
            60,
            BatchExecutor.execute(self._gen_catch_nested_batcher_exception()))

        with self.assertRaises(BatchTestError):
            BatchExecutor.execute(self._gen_exception())
        with self.assertRaises(BatchTestError):
            BatchExecutor.execute(self._gen_raise_generator_exception())
        with self.assertRaises(BatchTestError):
            BatchExecutor.execute(self._gen_raise_nested_generator_exception())
        with self.assertRaises(BatchTestError):
            BatchExecutor.execute(self._gen_raise_operation_exception())
        with self.assertRaises(BatchTestError):
            BatchExecutor.execute(self._gen_raise_batcher_exception())
        with self.assertRaises(BatchTestError):
            BatchExecutor.execute(self._gen_raise_nested_batcher_exception())
        with self.assertRaises(BatchTestError):
            BatchExecutor.execute(self._gen_double_exception())
        reached_end = [False]
        with self.assertRaises(BatchTestError):
            BatchExecutor.execute(
                self._gen_exception_and_take_a_while(reached_end))
        self.assertTrue(reached_end[0])

    def _gen_cool_user_id(self):
        user_data = yield TestHashOperation('coolUserId')
        yield GenResult(user_data)

    def _gen_uncool_user_id(self):
        user_data = yield TestHashOperation('uncoolUserId')
        yield GenResult(user_data)

    def test_execute_variants(self):
        """Test BatchExecutor.execute and BatchExecutor.executeva."""
        self.assertEqual([], BatchExecutor.executev([]))
        self.assertEqual([], BatchExecutor.executev(()))
        self.assertEqual(
            [42], BatchExecutor.executev([self._gen_cool_user_id()]))
        self.assertEqual(
            [42], BatchExecutor.executev((self._gen_cool_user_id(),)))
        self.assertEqual(
            [42, 13, 60],
            BatchExecutor.executev([
                self._gen_cool_user_id(), self._gen_uncool_user_id(),
                TestHashOperation('coolChairId')]))
        self.assertEqual(
            [42, 13, 60],
            BatchExecutor.executev((
                self._gen_cool_user_id(), self._gen_uncool_user_id(),
                TestHashOperation('coolChairId'))))
        self.assertEqual([], BatchExecutor.executeva())
        self.assertEqual(
            [42], BatchExecutor.executeva(self._gen_cool_user_id()))
        self.assertEqual(
            [42, 13, 60],
            BatchExecutor.executeva(
                self._gen_cool_user_id(), self._gen_uncool_user_id(),
                TestHashOperation('coolChairId')))

    def _gen_recursive(self, generator):
        yield generator[0]

    def test_execute_recursive(self):
        """Test executing a generator that yields itself."""
        generator = [None]
        generator[0] = self._gen_recursive(generator)
        with self.assertRaises(Exception):
            BatchExecutor.execute(generator[0])
