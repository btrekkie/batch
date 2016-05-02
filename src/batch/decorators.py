import collections
import frozendict
import sys

from shared_generator import SharedGenerator


def _to_hashable_value(value):
    """Return a frozen, hashable copy of "value".
    
    mixed value - The pseudo-hashable value, as defined in the comments
        for cached_generator.
    return object - The frozen, hashable copy.
    """
    if isinstance(value, (list, tuple)):
        return tuple(list([_to_hashable_value(element) for element in value]))
    elif isinstance(value, (frozenset, set)):
        return frozenset(
            list([_to_hashable_value(element) for element in value]))
    elif (isinstance(
            value,
            (dict, frozendict.frozendict, frozendict.FrozenOrderedDict))):
        if (isinstance(
                value,
                (collections.OrderedDict, frozendict.FrozenOrderedDict))):
            hashable_value = collections.OrderedDict()
        else:
            hashable_value = {}
        for key, sub_value in value.iteritems():
            hashable_value[key] = _to_hashable_value(sub_value)
        if isinstance(value, collections.OrderedDict):
            return frozendict.frozendict(hashable_value)
        else:
            return frozendict.FrozenOrderedDict(hashable_value)
    else:
        return value


def cached_generator(generator_cache_or_func):
    """Decorator for caching generator functions.
    
    Decorate a function to be the same as the decorated generator
    function, but ensure that we only call the decorated function once
    for a given set of arguments (*args and **kwargs).  Calls after the
    first share the generator and results of the first, by means of
    SharedGenerator.  For example, say we have the following:
    
    @cached_generator
    def gen_user(user_id):
        ...
    
    def gen_best_friend(user_id):
        best_friend_id = yield gen_best_friend_id(user_id)
        best_friend = yield gen_user(best_friend_id)
        yield GenResult(best_friend)
    
    def gen_next_door_neighbor(user_id):
        next_door_neighbor_id = yield gen_next_door_neighbor_id(user_id)
        next_door_neighbor = yield gen_user(next_door_neighbor_id)
        yield GenResult(next_door_neighbor)
    
    BatchExecutor.executeva(
        gen_best_friend(12345), gen_next_door_neighbor(12345))
    
    If the user with ID 12345 is best friends with his next-door
    neighbor, we will only call the implementation of gen_user on the
    friend's user ID once.  The other call will share the generator or
    the result of the first call.
    
    To use cached_generator, the arguments to the function must be
    psuedo-hashable, including the "self" or "cls" argument.  A
    "pseudo-hashable" value is a hashable value, a list, tuple, set, or
    frozenset with pseudo-hashable elements, or a dict, frozendict, or
    FrozenOrderedDict with pseudo-hashable values.
    
    mixed generator_cache_or_func - If the cached_generator decorator
        receives an argument, this must be of type GeneratorCache.  We
        may use the GeneratorCache to clear the results of previous
        calls to the function.
    """
    has_cache = not callable(generator_cache_or_func)
    
    def decorator(func):
        shared_generators_list = [{}]
        
        def gen_with_cache(*args, **kwargs):
            cache_key = _to_hashable_value((args, kwargs))
            shared_generators = shared_generators_list[0]
            if has_cache and not shared_generators:
                # Either we have never called the function or GeneratorCache
                # has cleared shared_generators
                shared_generators = (
                    generator_cache_or_func._create_shared_generators_map())
                shared_generators_list[0] = shared_generators
            
            shared_generator_or_exception_info = shared_generators.get(
                cache_key)
            if shared_generator_or_exception_info is None:
                try:
                    generator = func(*args, **kwargs)
                except:
                    shared_generators[cache_key] = sys.exc_info()
                    raise
                shared_generator = SharedGenerator(generator)
                shared_generators[cache_key] = shared_generator
                return shared_generator.gen()
            elif (isinstance(
                    shared_generator_or_exception_info, SharedGenerator)):
                return shared_generator_or_exception_info.gen()
            else:
                raise (
                    shared_generator_or_exception_info[1], None,
                    shared_generator_or_exception_info[2])
        return gen_with_cache
    if has_cache:
        return decorator
    else:
        return decorator(generator_cache_or_func)
