import queue
from .DAG_executor_constants import using_threads_not_processes, using_workers

work_queue = None
if using_workers and using_threads_not_processes:
    work_queue = queue.Queue()  
