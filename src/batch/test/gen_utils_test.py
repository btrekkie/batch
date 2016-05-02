import unittest

from batch import BatchExecutor
from batch import GenResult
from batch import GenUtils
from identity_operation import TestIdentityOperation


class GenUtilsTest(unittest.TestCase):
    def _gen_identity(self, value):
        result = yield TestIdentityOperation(value)
        yield GenResult(result)
    
    def test_gen_structured(self):
        """Test GenUtils.gen_structured."""
        self.assertEqual(
            15,
            BatchExecutor.execute(
                GenUtils.gen_structured(TestIdentityOperation(15))))
        self.assertEqual(
            -26,
            BatchExecutor.execute(
                GenUtils.gen_structured(self._gen_identity(-26))))
        self.assertEqual(
            [], BatchExecutor.execute(GenUtils.gen_structured([])))
        self.assertEqual(
            {}, BatchExecutor.execute(GenUtils.gen_structured({})))
        self.assertEqual(
            [42, 3],
            BatchExecutor.execute(
                GenUtils.gen_structured(
                    [self._gen_identity(42), TestIdentityOperation(3)])))
        self.assertEqual(
            {'foo': 18, 'bar': 'yo', 'baz': 4},
            BatchExecutor.execute(
                GenUtils.gen_structured({
                    'foo': self._gen_identity(18),
                    'bar': self._gen_identity('yo'),
                    'baz': TestIdentityOperation(4),
                })))
        self.assertEqual(
            {'foo': 'p', 'bar': ['yo', {'baz': 'b', 'abc': 'j'}]},
            BatchExecutor.execute(
                GenUtils.gen_structured({
                    'foo': self._gen_identity('p'),
                    'bar': [
                        self._gen_identity('yo'),
                        {
                            'baz': self._gen_identity('b'),
                            'abc': TestIdentityOperation('j'),
                        },
                    ],
                })))
        self.assertEqual(
            [-5, [27, 729], {'foo': 'bar', 'baz': 91}, {}],
            BatchExecutor.execute(
                GenUtils.gen_structured([
                    self._gen_identity(-5),
                    [self._gen_identity(27), self._gen_identity(729)],
                    {
                        'foo': self._gen_identity('bar'),
                        'baz': self._gen_identity(91),
                    },
                    {},
                ])))
    
    def test_gen_identity(self):
        """Test GenUtils.gen_identity."""
        self.assertEqual(
            None, BatchExecutor.execute(GenUtils.gen_identity(None)))
        self.assertEqual(
            'foo', BatchExecutor.execute(GenUtils.gen_identity('foo')))
        self.assertEqual(
            42, BatchExecutor.execute(GenUtils.gen_identity(42)))
        self.assertEqual(
            [12, {'foo': 'bar'}],
            BatchExecutor.execute(GenUtils.gen_identity([12, {'foo': 'bar'}])))
