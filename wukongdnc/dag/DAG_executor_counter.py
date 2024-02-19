import threading
#from .DAG_executor_constants import USING_THREADS_NOT_PROCESSES, USING_WORKERS
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
#brc: counter 
# tasks_completed_counter

completed_tasks_counter = None
if DAG_executor_constants.USING_WORKERS and DAG_executor_constants.USING_THREADS_NOT_PROCESSES:
    completed_tasks_counter = Counter(0)

#brc: counter 
# completed_workers_counter
completed_workers_counter = None
if DAG_executor_constants.USING_WORKERS and DAG_executor_constants.USING_THREADS_NOT_PROCESSES:
    completed_workers_counter = Counter(0)
