import threading
#import _thread
from .DAG_executor_constants import using_threads_not_processes, using_workers

class Counter(object):
    def __init__(self,initial_value=0):
        self.value = initial_value
        self._lock = threading.Lock()
        
    def increment_and_get(self):
        with self._lock:
            self.value += 1
            return self.value

counter = None
if using_workers and using_threads_not_processes:
    counter = Counter(0)