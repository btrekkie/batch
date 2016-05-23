import unittest

from batch import BatchExecutor
from decorators_test_object import GenDecoratorsTestObject
from error import BatchTestError


class GenDecoratorsTest(unittest.TestCase):
    def test_cached_fibonacci(self):
        """Test various cached implementations of computing Fibonacci numbers.

        Test various generator-based implementations of computing
        Fibonacci numbers, which have somewhat complex generator trees,
        with methods that use the cached_generator decorator.
        """
        obj = GenDecoratorsTestObject()
        BatchExecutor.execute(obj.gen_fibonacci_without_operations(4))
        self.assertEqual(
            [55, 233],
            BatchExecutor.executeva(
                obj.gen_fibonacci_without_operations(9),
                obj.gen_fibonacci_without_operations(12)))
        self.assertEqual(
            obj.fibonacci_call_counts, dict.fromkeys(xrange(0, 13), 1))
        self.assertEqual(
            55, BatchExecutor.execute(obj.gen_fibonacci_without_operations(9)))
        self.assertEqual(
            obj.fibonacci_call_counts, dict.fromkeys(xrange(0, 13), 1))
        self.assertEqual(
            233,
            BatchExecutor.execute(obj.gen_fibonacci_without_operations(12)))
        self.assertEqual(
            obj.fibonacci_call_counts, dict.fromkeys(xrange(0, 13), 1))
        self.assertEqual(
            610,
            BatchExecutor.execute(obj.gen_fibonacci_without_operations(14)))
        self.assertEqual(
            obj.fibonacci_call_counts, dict.fromkeys(xrange(0, 15), 1))

        obj = GenDecoratorsTestObject()
        BatchExecutor.execute(obj.gen_fibonacci_with_leaf_operations(4))
        self.assertEqual(
            [55, 233],
            BatchExecutor.executeva(
                obj.gen_fibonacci_with_leaf_operations(9),
                obj.gen_fibonacci_with_leaf_operations(12)))
        self.assertEqual(
            obj.fibonacci_call_counts, dict.fromkeys(xrange(0, 13), 1))
        self.assertEqual(
            55,
            BatchExecutor.execute(obj.gen_fibonacci_with_leaf_operations(9)))
        self.assertEqual(
            obj.fibonacci_call_counts, dict.fromkeys(xrange(0, 13), 1))
        self.assertEqual(
            233,
            BatchExecutor.execute(obj.gen_fibonacci_with_leaf_operations(12)))
        self.assertEqual(
            obj.fibonacci_call_counts, dict.fromkeys(xrange(0, 13), 1))
        self.assertEqual(
            610,
            BatchExecutor.execute(obj.gen_fibonacci_with_leaf_operations(14)))
        self.assertEqual(
            obj.fibonacci_call_counts, dict.fromkeys(xrange(0, 15), 1))

        obj = GenDecoratorsTestObject()
        BatchExecutor.execute(
            obj.gen_fibonacci_with_intermediate_operations(4))
        self.assertEqual(
            [55, 233],
            BatchExecutor.executeva(
                obj.gen_fibonacci_with_intermediate_operations(9),
                obj.gen_fibonacci_with_intermediate_operations(12)))
        self.assertEqual(
            obj.fibonacci_call_counts, dict.fromkeys(xrange(0, 13), 1))
        self.assertEqual(
            55,
            BatchExecutor.execute(
                obj.gen_fibonacci_with_intermediate_operations(9)))
        self.assertEqual(
            obj.fibonacci_call_counts, dict.fromkeys(xrange(0, 13), 1))
        self.assertEqual(
            233,
            BatchExecutor.execute(
                obj.gen_fibonacci_with_intermediate_operations(12)))
        self.assertEqual(
            obj.fibonacci_call_counts, dict.fromkeys(xrange(0, 13), 1))
        self.assertEqual(
            610,
            BatchExecutor.execute(
                obj.gen_fibonacci_with_intermediate_operations(14)))
        self.assertEqual(
            obj.fibonacci_call_counts, dict.fromkeys(xrange(0, 15), 1))

        obj = GenDecoratorsTestObject()
        BatchExecutor.execute(
            obj.gen_fibonacci_with_intermediate_operations(4))
        self.assertEqual(
            [[55, 233], [55, 233]],
            BatchExecutor.executeva(
                obj.gen_fibonacci_from_obj([set([9]), {'foo': 12}]),
                obj.gen_fibonacci_from_obj([set([9]), {'foo': 12}])))
        self.assertEqual([[set([9]), {'foo': 12}]], obj.fibonacci_obj_args)
        self.assertEqual(
            obj.fibonacci_call_counts, dict.fromkeys(xrange(0, 13), 1))
        self.assertEqual(
            [34, 610],
            BatchExecutor.execute(
                obj.gen_fibonacci_from_obj([set([8]), {'foo': 14}])))
        self.assertEqual(
            [[set([9]), {'foo': 12}], [set([8]), {'foo': 14}]],
            obj.fibonacci_obj_args)
        self.assertEqual(
            obj.fibonacci_call_counts, dict.fromkeys(xrange(0, 15), 1))

    def test_cached_generator(self):
        """Test the cached_generator decorator.

        In particular, test that the function it wraps is not called
        more times than permitted with a given set of arguments.
        """
        obj = GenDecoratorsTestObject()
        self.assertEqual(
            15, BatchExecutor.execute(obj.gen_cached_generator(1, 2)))
        self.assertEqual([(1, 2, 3, 4)], obj.cached_generator_args)
        self.assertEqual(
            15, BatchExecutor.execute(obj.gen_cached_generator(1, 2)))
        self.assertEqual([(1, 2, 3, 4)], obj.cached_generator_args)
        self.assertEqual(
            15, BatchExecutor.execute(obj.gen_cached_generator(1, 2, bar=4)))

        obj = GenDecoratorsTestObject()
        self.assertEqual(
            32, BatchExecutor.execute(obj.gen_cached_generator(5, 6, bar=7)))
        self.assertEqual([(5, 6, 3, 7)], obj.cached_generator_args)
        self.assertEqual(
            32, BatchExecutor.execute(obj.gen_cached_generator(5, 6, 3, 7)))
        obj.cached_generator_args = []
        self.assertEqual(
            32, BatchExecutor.execute(obj.gen_cached_generator(5, 6, bar=7)))
        self.assertEqual([], obj.cached_generator_args)
        self.assertEqual(
            127, BatchExecutor.execute(obj.gen_cached_generator(8, 9, 10, 11)))
        self.assertEqual([(8, 9, 10, 11)], obj.cached_generator_args)

    def test_cached_raise(self):
        """Test that the cached_generator decorator works with exceptions.

        In particular, test the case in which the wrapped function does
        not yield values, but rather either returns a generator or
        raises an exception.
        """
        obj = GenDecoratorsTestObject()
        with self.assertRaises(BatchTestError):
            BatchExecutor.execute(obj.gen_cached_identity(-1))
        with self.assertRaises(BatchTestError):
            BatchExecutor.execute(obj.gen_cached_identity(-1))
        self.assertEqual(
            42, BatchExecutor.execute(obj.gen_cached_identity(42)))
        self.assertEqual(6, BatchExecutor.execute(obj.gen_cached_identity(6)))
        self.assertEqual(
            42, BatchExecutor.execute(obj.gen_cached_identity(42)))
        with self.assertRaises(BatchTestError):
            obj.gen_cached_identity(-5)
        self.assertEqual(
            {-5: 1, -1: 1, 6: 1, 42: 1}, obj.cached_identity_call_counts)

        with self.assertRaises(BatchTestError):
            BatchExecutor.execute(obj.gen_cached_identity_with_yield(-1))
        obj.gen_cached_identity_with_yield(-1)
        with self.assertRaises(BatchTestError):
            BatchExecutor.execute(obj.gen_cached_identity_with_yield(-1))
        with self.assertRaises(BatchTestError):
            BatchExecutor.executeva(
                obj.gen_cached_identity_with_yield(-1),
                obj.gen_cached_identity_with_yield(-1))
        self.assertEqual(
            42, BatchExecutor.execute(obj.gen_cached_identity_with_yield(42)))
        self.assertEqual(
            6, BatchExecutor.execute(obj.gen_cached_identity_with_yield(6)))
        self.assertEqual(
            42, BatchExecutor.execute(obj.gen_cached_identity_with_yield(42)))
        with self.assertRaises(BatchTestError):
            BatchExecutor.executeva(
                obj.gen_cached_identity_with_yield(-5),
                obj.gen_cached_identity_with_yield(-5))
        self.assertEqual(
            {-5: 1, -1: 1, 6: 1, 42: 1},
            obj.cached_identity_with_yield_call_counts)

    def test_generator_cache(self):
        """Test using cached_generator with GeneratorCache."""
        obj = GenDecoratorsTestObject()
        self.assertEqual(
            1, BatchExecutor.execute(obj.gen_identity_with_cache1(1)))
        self.assertEqual(
            5, BatchExecutor.execute(obj.gen_identity_with_cache1(5)))
        self.assertEqual(
            1, BatchExecutor.execute(obj.gen_identity_with_cache1(1)))
        self.assertEqual({1: 1, 5: 1}, obj.identity_with_cache1_call_counts)
        self.assertEqual(
            7, BatchExecutor.execute(obj.gen_sum_with_cache1(3, 4)))
        self.assertEqual(
            5, BatchExecutor.execute(obj.gen_identity_with_cache2(5)))
        self.assertEqual(
            7, BatchExecutor.execute(obj.gen_sum_with_cache1(3, 4)))
        self.assertEqual({1: 1, 5: 1}, obj.identity_with_cache1_call_counts)
        self.assertEqual({(3, 4): 1}, obj.sum_with_cache1_call_counts)
        self.assertEqual({5: 1}, obj.identity_with_cache2_call_counts)

        GenDecoratorsTestObject.CACHE1.clear()
        self.assertEqual(
            7, BatchExecutor.execute(obj.gen_sum_with_cache1(3, 4)))
        self.assertEqual(
            7, BatchExecutor.execute(obj.gen_sum_with_cache1(3, 4)))
        self.assertEqual(
            1, BatchExecutor.execute(obj.gen_identity_with_cache1(1)))
        self.assertEqual(
            1, BatchExecutor.execute(obj.gen_identity_with_cache1(1)))
        self.assertEqual({1: 2, 5: 1}, obj.identity_with_cache1_call_counts)
        self.assertEqual({(3, 4): 2}, obj.sum_with_cache1_call_counts)
        self.assertEqual({5: 1}, obj.identity_with_cache2_call_counts)
