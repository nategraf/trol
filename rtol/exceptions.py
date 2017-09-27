class RedisKeyError(Exception):
    """An exception to indicate that a Redis key does not exist (i.e. redis.get returned None)

    Attributes:
        key (str): The Redis key which was triggered the exception

    Args:
        key (str): Sets the `key` attribute
    """

    def __init__(self, key):
        self.key = key

    def __str__(self):
        return "Key '{}' does not exist in Redis".format(self.key)
