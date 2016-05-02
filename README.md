# batch
This project makes it possible to batch things in Python using generators, in
the presence of potentially complex dependency relationships.  For example, this
is useful for minimizing round-trip requests to a data store, if each round-trip
may ask for multiple pieces of information.  Batching occurs on a single thread.
This project is tested on Python 2.7, but I guess maybe it also works in Python
3 and Python 2.5 / 2.6.

# Usage
The coroutine `BatchExecutor.execute` assists in batching `BatchableOperations`
in the presence of potentially complex dependency relationships, which we
express using generators.  Typically, a callsite passes a "batch generator" to
the `execute` method.  Each of a batch generator's yield statements indicate one
or more operations to run in parallel, with the results of the operations
appearing as the return values of the yield statements.  Once finished, a batch
generator yields a `GenResult` object indicating the generator's result.

To be precise, a "batch generator" is a generator that yields batch generators,
`BatchableOperations`, lists or tuples of such values, and / or `GenResult`
objects.  When run using `execute`, except in the case of `GenResult` objects,
the return value of a batch generator's yield statement is the same as the
argument to the yield statement, but with batch generators and
`BatchableOperations` replaced with their results (and with tuples changed to
lists).  When a batch generator yields a `GenResult` object, the `GenResult`
object's value is designated the result of the generator.  Upon yielding a
`GenResult`, the function exits (by means of a call to `Generator.close()`).
(If a batch generator finishes without yielding a `GenResult` object, `None` is
designated the result of the generator.)  `execute` returns the result of the
batch generator or `BatchableOperation` passed to it.

Consider the following example:

<pre>
def gen_user(user_id):
    """Return the User with the specified ID."""

    # Compute the key associated with the user in data store, which is a simple
    # key-value hash
    data_store_key = 'user:{:d}'.format(user_id)

    # Run a DataStoreOperation to fetch the dictionary associated with
    # data_store_key.  Store the result in user_data.
    user_data = yield DataStoreOperation(data_store_key)

    # Wrap the dictionary in a User object, and yield it as the result of
    # gen_user
    yield GenResult(User(user_data))

def gen_spouse(user_id):
    """Return the User object for the spouse of the user with the specified ID.
    """

    # Compute the key for fetching the user's spouse's ID
    data_store_key = 'spouseId:{:d}'.format(user_id)

    # Run a DataStoreOperation to fetch the integer associated with
    # data_store_key.  Store the result in spouse_id.
    spouse_id = yield DataStoreOperation(data_store_key)

    # Fetch the user with ID spouse_id, and store the result in "spouse"
    spouse = yield gen_user(spouse_id)

    # Yield the spouse as the result of gen_spouse
    yield GenResult(spouse)

def gen_spouses(user_id)
    """Return a tuple containing the User with the specified ID and his spouse.
    """

    # Fetch the user with ID user_id, and in parallel, fetch his spouse.  Store
    # the user in "user" and the spouse in "spouse".
    user, spouse = yield (gen_user(user_id), gen_spouse(user_id))
    
    # Yield (user, spouse) as the result of gen_spouses
    yield GenResult((user, spouse))

BatchExecutor.execute(gen_spouses(12345))
</pre>

To summarize, the `gen_spouses` function fetches the user with `gen_user` and
the spouse with `gen_spouse` in parallel.  The `gen_spouse` function fetches the
spouse ID with `DataStoreOperation`, then fetches the spouse with `gen_user`.
`gen_user` fetches the user dictionary with `DataStoreOperation`, then wraps it
in a `User` object.

By representing these dependencies as batch generators, we are able to
effectively batch the data fetching operations.  Specifically, we will only make
two round-trip requests to the data store: one to fetch the user dictionary for
the user with ID 12345 and the ID of his spouse, and one to fetch the user
dictionary for his spouse.  By contrast, a naive approach would have required
three round trips - one for each of the three data store keys.

For more detailed instructions, check the source code to see the full API and
docstring documentation.
