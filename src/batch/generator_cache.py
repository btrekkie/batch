class GeneratorCache(object):
    """Cache for cached_generator to store SharedGenerators and exception info.
    """
    
    # Private attributes:
    # list<dict> _shared_generators - A list of the SharedGenerator and
    #     exception info maps stored in this GeneratorCache.
    
    def __init__(self):
        self._shared_generators = []
    
    def clear(self):
        """Clear the SharedGenerators from all functions using this.
        
        Clear the SharedGenerators and exception info we are using for
        all of the functions decorated with this GeneratorCache.  This
        clears any cached results of such functions.
        """
        for shared_generators in self._shared_generators:
            shared_generators.clear()
        self._shared_generators = []
    
    def _create_shared_generators_map(self):
        """Return a new map for storing SharedGenerators and exception info.
        """
        shared_generators = {}
        self._shared_generators.append(shared_generators)
        return shared_generators
