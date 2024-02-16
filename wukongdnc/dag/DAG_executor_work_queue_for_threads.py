import queue
#from .DAG_executor_constants import USING_THREADS_NOT_PROCESSES, USING_WORKERS
from . import DAG_executor_constants

#thread_work_queue = None
#if USING_WORKERS and USING_THREADS_NOT_PROCESSES:
#    thread_work_queue = queue.Queue()  

work_queue = None
if DAG_executor_constants.USING_WORKERS and DAG_executor_constants.USING_THREADS_NOT_PROCESSES:
    work_queue = queue.Queue()