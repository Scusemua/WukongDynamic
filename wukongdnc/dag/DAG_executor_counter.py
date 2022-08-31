import threading
from .DAG_executor_constants import using_threads_not_processes, using_workers

# When using threads, this is a global counter used to atomically count the number
# of tasks that have been executed. When using multiprocessing, the DAG_executor_driver
# create a CounterMP object that is shaed by the processes.

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
