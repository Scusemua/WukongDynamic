from .DAG_executor_constants import using_threads_not_processes, run_all_tasks_locally

data_dict = None
if using_threads_not_processes and run_all_tasks_locally:
    data_dict = {}