#from .DAG_executor_constants import RUN_ALL_TASKS_LOCALLY #, USING_THREADS_NOT_PROCESSES
from . import DAG_executor_constants

# data dictionary for task results
data_dict = None
#if USING_THREADS_NOT_PROCESSES and RUN_ALL_TASKS_LOCALLY:
if DAG_executor_constants.RUN_ALL_TASKS_LOCALLY or not DAG_executor_constants.RUN_ALL_TASKS_LOCALLY:
    data_dict = {}