#from .DAG_executor_constants import run_all_tasks_locally #, using_threads_not_processes
from . import DAG_executor_constants

# data dictionary for task results
data_dict = None
#if using_threads_not_processes and run_all_tasks_locally:
if DAG_executor_constants.run_all_tasks_locally or not DAG_executor_constants.run_all_tasks_locally:
    data_dict = {}