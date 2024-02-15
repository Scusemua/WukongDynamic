import threading
#from .DAG_executor_constants import using_threads_not_processes, using_workers
from . import DAG_executor_constants

# When using threads, this is a global counter used to atomically count the number
# of tasks that have been executed. When using multiprocessing, the DAG_executor_driver
# create a CounterMP object that is shared by the processes.

class Counter(object):
    def __init__(self,initial_value=0):
        self.value = initial_value
        self._lock = threading.Lock()

    def increment_and_get(self):
        with self._lock:
            self.value += 1
            return self.value
        
    def decrement_and_get(self):
        with self._lock:
            self.value -= 1
            return self.value
        
    def get(self):
        with self._lock:
            return self.value
#rhc: counter 
# tasks_completed_counter

completed_tasks_counter = None
if DAG_executor_constants.using_workers and DAG_executor_constants.using_threads_not_processes:
    completed_tasks_counter = Counter(0)

#rhc: counter 
# completed_workers_counter
completed_workers_counter = None
if DAG_executor_constants.using_workers and DAG_executor_constants.using_threads_not_processes:
    completed_workers_counter = Counter(0)
