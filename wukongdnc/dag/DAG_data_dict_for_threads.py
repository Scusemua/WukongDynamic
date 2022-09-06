from .DAG_executor_constants import run_all_tasks_locally #, using_threads_not_processes

# global data dictionary for threads. For processes, the DAG_executor_driver creates a
# mutiprocessing Queue and passes it to the processes to share.
data_dict = None
#if using_threads_not_processes and run_all_tasks_locally:
if run_all_tasks_locally:
    data_dict = {}