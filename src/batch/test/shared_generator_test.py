import unittest

from batch import BatchExecutor
from batch import GenResult
from batch import SharedGenerator
from error import BatchTestError
from hash_operation import TestHashOperation
from identity_operation import TestIdentityOperation
from user import TestUser


class SharedGeneratorTest(unittest.TestCase):
    def _gen_user(self, user_id):
        key = 'user:{:d}'.format(user_id)
        user_data = yield TestHashOperation(key)
        yield GenResult(TestUser(user_data))

    def _gen_spouse_id(self, user_id):
        key = 'spouseId:{:d}'.format(user_id)
        spouse_id = yield TestHashOperation(key)
        yield GenResult(spouse_id)

    def _gen_spouse(self, user_id):
        spouse_id = yield self._gen_spouse_id(user_id)
        spouse = yield self._gen_user(spouse_id)
        yield GenResult(spouse)
        raise BatchTestError()

    def _gen_cool_user(self):
        user_id = yield TestHashOperation('coolUserId')
        user = yield self._gen_user(user_id)
        yield GenResult(user)

    def _gen_shared_and_cool_user(self, shared_generator):
        result = yield (shared_generator.gen(), self._gen_cool_user())
        yield GenResult(result)

    def _gen_shared_and_cool_chair_id(self, shared_generator):
        result = yield (
            shared_generator.gen(), TestHashOperation('coolChairId'))
        yield GenResult(result)

    def _gen_shared(self):
        shared_generator = SharedGenerator(self._gen_spouse(42))
        yield GenResult((
            yield (
                self._gen_shared_and_cool_user(shared_generator),
                self._gen_shared_and_cool_chair_id(shared_generator))))

    def test_shared_generator(self):
        """Test using a SharedGenerator in multiple locations."""
        result = BatchExecutor.execute(self._gen_shared())
        self.assertEqual('ice cream', result[0][0].favorite_food())
        self.assertEqual('pizza', result[0][1].favorite_food())
        self.assertEqual('ice cream', result[1][0].favorite_food())
        self.assertEqual(60, result[1][1])

    def test_shared_generator_used_once(self):
        """Test using a SharedGenerator in a single location."""
        shared_generator = SharedGenerator(self._gen_spouse(42))
        result = BatchExecutor.execute(
            self._gen_shared_and_cool_user(shared_generator))
        self.assertEqual('ice cream', result[0].favorite_food())
        self.assertEqual('pizza', result[1].favorite_food())

    def _gen_cool_user_identity_then_shared(self, shared_generator):
        cool_user = yield self._gen_cool_user()
        cool_user_identity = yield TestIdentityOperation(cool_user)
        shared_result = yield shared_generator.gen()
        yield GenResult((cool_user_identity, shared_result))

    def _gen_shared_delayed(self):
        shared_generator = SharedGenerator(self._gen_spouse_id(42))
        result = yield (
            shared_generator.gen(),
            self._gen_cool_user_identity_then_shared(shared_generator))
        yield GenResult(result)

    def test_shared_generator_delayed(self):
        """Test using a SharedGenerator's cached result.

        Attempts to test a case in which a SharedGenerator finishes
        executing prior to being yielded a second time.  Technically,
        such behavior is not guaranteed, because BatchExecutor.execute
        may batch operations in any valid fashion, and need not do so in
        the straightforward manner that efficiency concerns would
        dictate.
        """
        result = BatchExecutor.execute(self._gen_shared_delayed())
        self.assertEqual(12, result[0])
        self.assertEqual('pizza', result[1][0].favorite_food())
        self.assertEqual(12, result[1][1])

    def _gen_nested_execution(self, shared_generator):
        user_id = yield TestHashOperation('coolUserId')
        yield GenResult(
            (user_id, BatchExecutor.execute(shared_generator.gen())))

    def _gen_share_with_nested_execution(self):
        shared_generator = SharedGenerator(self._gen_spouse(42))
        result = yield (
            shared_generator.gen(),
            self._gen_nested_execution(shared_generator))
        yield GenResult(result)

    def test_nested_execution(self):
        """Test executing a generator in multiple BatchExecutor.execute calls.

        Test executing a shared generator from multiple calls to
        BatchExecutor.execute at the same time.  Technically, such
        behavior is not guaranteed, because BatchExecutor.execute may
        batch operations in any valid fashion, and need not do so in the
        straightforward manner that efficiency concerns would dictate.
        """
        try:
            result = BatchExecutor.execute(
                self._gen_share_with_nested_execution())
        except BatchTestError:
            # We iterated on _gen_spouse beyond the statement that
            # yielded the result
            raise
        except Exception:
            # Hopefully, we will reach this branch
            pass
        else:
            self.assertEqual('ice cream', result[0].favorite_food())
            self.assertEqual(42, result[1][0])
            self.assertEqual('ice cream', result[1][1].favorite_food())

    def _gen_raise(self):
        value = yield TestIdentityOperation(10)
        raise BatchTestError()
        yield GenResult(value)

    def test_shared_generator_exception(self):
        """Test SharedGenerator on a function that raises an exception.

        Test SharedGenerator on a generator function that raises an
        exception.  Ensure that the SharedGenerator consistently raises
        this exception.
        """
        shared_generator = SharedGenerator(self._gen_raise())
        with self.assertRaises(BatchTestError):
            BatchExecutor.execute(shared_generator.gen())
        with self.assertRaises(BatchTestError):
            BatchExecutor.execute(shared_generator.gen())
        shared_generator = SharedGenerator(self._gen_raise())
        with self.assertRaises(BatchTestError):
            BatchExecutor.executeva(
                shared_generator.gen(), shared_generator.gen())
        with self.assertRaises(BatchTestError):
            BatchExecutor.execute(shared_generator.gen())
