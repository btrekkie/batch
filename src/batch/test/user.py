class TestUser(object):
    """Wraps a user data dictionary."""
    
    # Private attributes:
    # dict<basestring, mixed> _data - The data dictionary.
    
    def __init__(self, data):
        self._data = data
    
    def favorite_food(self):
        return self._data['favoriteFood']
