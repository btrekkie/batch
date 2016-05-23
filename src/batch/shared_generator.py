import sys

from gen_result import GenResult


class SharedGenerator(object):
    """Provides the ability to safely yield a Generator from multiple places.

    SharedGenerator hides the difficulties associated with yielding a
    single batchable generator from multiple function calls.  Sample
    usage is as follows:

    class Foo(object):
        def __init__(self):
            self._bar_generator = None

        def _gen_bar(self):
            bar = yield BarOperation()
            yield GenResult(bar)

        def gen_bar(self):
            if self._bar_generator is None:
                self._bar_generator = SharedGenerator(self._gen_bar())
            return self._bar_generator.gen()

    In the above example, say that a call to BatchExecutor.execute
    resulted in multiple parallel calls to gen_bar.  Those calls would
    be able to share generators, so we are guaranteed to only create one
    BarOperation and to execute it only once.

    The main purpose of sharing a generator is to improve performance.
    We may also use sharing to prevent a function with side effects from
    being called multiple times.

    SharedGenerator hides two difficulties associated with sharing
    generators:

    - We may not execute a shared generator from multiple calls to
      BatchExecutor.execute* at the same time.  This is because both
      such calls will attempt to iterate over the same generator, and
      they will become confused.  SharedGenerator detects this condition
      and raises an exception.
    - We may not yield a generator that has finished executing.  To
      prevent this, a SharedGenerator caches and returns the result (or
      exception) of its generator when it is finished executing.

    Note that despite the name, SharedGenerator is not a Generator.
    Rather, it wraps a Generator.
    """

    # Private attributes:
    # tuple<type, mixed, traceback> _exception_info - Information about
    #     the exception the generator raised, as returned by
    #     sys.exc_info(), or None if the generator is not finished
    #     executing or did not raise an exception.
    # GenResult _result - The result of the generator, or None if the
    #     generator is not finished executing or raised an exception.
    # Generator _shared_generator - The shared Generator object to
    #     yield, or None if it is finished executing.

    def __init__(self, generator):
        """Initialize a SharedGenerator that wraps the specified Generator."""
        self._shared_generator = self._gen_wrap(generator)
        self._result = None
        self._exception_info = None

    def gen(self):
        """Return the result of the generator passed to the constructor."""
        if self._result is not None:
            yield self._result
        elif self._exception_info is not None:
            raise self._exception_info[1], None, self._exception_info[2]
        else:
            result = yield self._shared_generator
            yield GenResult(result)

    def _gen_wrap(self, generator):
        """Wrap the specified Generator.

        Specifically, this function is a generator that behaves like
        "generator", except that (a) it raises an exception if it is
        started multiple times and (b) it stores the result in _result.
        """
        try:
            value = generator.next()
            while not isinstance(value, GenResult):
                is_list = isinstance(value, (list, tuple))
                try:
                    if is_list:
                        result = yield value
                    else:
                        result = yield (value,)
                except Exception:
                    exception_info = sys.exc_info()
                    value = generator.throw(*exception_info)
                else:
                    if result is None:
                        # A result of None indicates that there was a call to
                        # this generator's next() method, where we should have
                        # called its "send" method.  Assuming the call to
                        # next() was made by BatchExecutor.execute* (which is
                        # the only function that is supposed to be iterating
                        # over batch generators), this means that multiple such
                        # calls attempted to start this shared generator.
                        raise RuntimeError(
                            'A generator may not be run from two calls to '
                            'BatchExecutor.execute* at the same time.  To '
                            'avoid this, do not call BatchExecutor.execute* '
                            'from within a generator.  Instead, yield the '
                            'value(s) you passed to BatchExecutor.execute*.')
                    if is_list:
                        value = generator.send(result)
                    else:
                        value = generator.send(result[0])
            self._result = value
            self._shared_generator = None
            yield value
        except Exception:
            # "generator" raised an exception
            self._exception_info = sys.exc_info()
            raise
