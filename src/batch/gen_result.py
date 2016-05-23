class GenResult(object):
    """Wraps the result of a batch generator.

    See the comments for BatchExecutor.execute.
    """

    # Private attributes:
    # mixed _value - The result.

    def __init__(self, value):
        self._value = value
