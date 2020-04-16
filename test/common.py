from redis import Redis
from redis.exceptions import ConnectionError
from subprocess import DEVNULL, Popen, SubprocessError
from threading import Lock
import time
import warnings
import weakref

REDIS_TEST_SERVER_PORT = 6379
"""Port used when creating a redis server to run tests against"""

holder_ref = None
lock = Lock()

def redis_test_client():
    """Return a Redis client, starting a test server as needed.

    Using this fucntion ensures that exactly one test server is running at a time.
    When all clients are dereferenced, the server wll be shut down.

    Returns:
       redis.Redis: A Redis client will access to the test server
    """
    token = ensure_redis_is_online()
    client = Redis(host='localhost', port=REDIS_TEST_SERVER_PORT, db=0)

    # Wait for the server to be online.
    # Retry with exponential backoff for up to ~8 seconds.
    for i in range(5):
        try:
            client.ping()
            break
        except ConnectionError:
            time.sleep(0.25 * 2**i)

    # Attach the token to the client so that when it goes out of scope, so will the token.
    setattr(client, "_trol_test_process_token", token)
    return client

def ensure_redis_is_online():
    """Ensure a redis process is up, starting it is needed, and recieve a token to hold

    Used to ensure only one process is started at a time

    Returns:
       ProcessToken: A token which should be held as long as the server is in use
            Once all references to the token are gone, the process will be killed
    """
    global holder_ref
    global lock

    with lock:
        if holder_ref is None or holder_ref() is None:
            holder = ProcessToken(["redis-server", "--port", str(REDIS_TEST_SERVER_PORT)], stdout=DEVNULL, stderr=DEVNULL)
            holder_ref = weakref.ref(holder)
        else:
            holder = holder_ref()

    return holder

class ProcessToken:
    """A token to start a process and ensure it is killed when the token goes out of scope

    Attributes:
        process (subprocess.Popen): Handle to the running process
        terminate_timeout (int): Seconds to wait when terminating the process

    Args:
        *args (list[str]): Positional args to send to the Popen constructor.
        **kwargs (dict[str, object]): Keyword args which will be passed to the Popen constructor
    """
    terminate_timeout = 10

    def __init__(self, *args, **kwargs):
        self.process = Popen(*args, **kwargs)
        self._finalizer = weakref.finalize(self, self.terminate)

    def terminate(self):
        self.process.terminate()
        try:
            self.process.wait(timeout=self.terminate_timeout)
        except SubprocessError as e:
            warnings.warn("Process {0} failed to terminate: {1}".format(self.process.pid, e))
