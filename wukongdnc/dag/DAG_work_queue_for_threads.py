import queue
from .DAG_executor_constants import using_threads_not_processes, using_workers

#thread_work_queue = None
#if using_workers and using_threads_not_processes:
#    thread_work_queue = queue.Queue()  

work_queue = None
if using_workers and using_threads_not_processes:
    work_queue = queue.Queue()  

