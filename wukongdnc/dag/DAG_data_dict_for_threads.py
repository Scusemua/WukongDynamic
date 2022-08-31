from .DAG_executor_constants import using_threads_not_processes, run_all_tasks_locally

# global data dictionary for threads. For processes, the DAG_executor_driver creates a
# mutiprocessing Queue and passes it to the processes to share.
data_dict = None
if using_threads_not_processes and run_all_tasks_locally:
    data_dict = {}
