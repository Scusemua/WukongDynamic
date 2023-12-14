import logging
#import os
#import wukongdnc.dag.DAG_executor_constants

logger = logging.getLogger(__name__)

# Basic tests for Dask DAGs

# if running real lambdas or storing synch objects in real lambdas:
#   Set SERVERLESS_SYNC to True or False in wukongdnc constants !!!!!!!!!!!!!!
#

#test_number = 1

"""
wukongdnc.dag.DAG_executor_constants.store_sync_objects_in_lambdas = False
wukongdnc.dag.DAG_executor_constants.using_Lambda_Function_Simulators_to_Store_Objects = False
wukongdnc.dag.DAG_executor_constants.sync_objects_in_lambdas_trigger_their_tasks = False
wukongdnc.dag.DAG_executor_constants.using_DAG_orchestrator = False
wukongdnc.dag.DAG_executor_constants.map_objects_to_lambda_functions = False
wukongdnc.dag.DAG_executor_constants.use_anonymous_lambda_functions = False
wukongdnc.dag.DAG_executor_constants.using_single_lambda_function = False

# For all: remote objects, using select objects:
# 1. run_all_tasks_locally = True, create objects on start = True:
# TTFFTF: no trigger and no DAG_orchestrator, but map objects 
# (anon is false) and create objects on start
# variations:
# - change D_O to T, 
# - change map to F, and anon to T: Note: no function lock since anon caled only once
# - change D_O to F, map F, anon T: Note: no function lock since anon caled only once
#
# 2. run_all_tasks_locally = False, create objects on start = True:
# Note: not running real lambdas yet, so need TTT, i.e., not using threads
#       to simulate lambdas and not running real lambdas yet, so need to
#       trigger lambdas, which means store objects in lambdas and they call
#       DAG_excutor_Lambda to execute task (i.e., "trigger task to run in 
#       the same lambda"). Eventually we'll have tests for use real 
#       non-triggered lambdas to run tasks (invoked at fanouts/faninNBS)
#       and objects stored in lambdas or on server.
# TTTTTF: trigger and DAG_orchestrator, map objects (anon is false) and create objects on start
# variations:
# - change map to F, and anon to T and create on start to F: Note: no function lock since anon called only once
# - change DAG_orchestrator to F - so not going through enqueue so will
#   create on fly in other places besides equeue.


wukongdnc.dag.DAG_executor_constants.compute_pagerank = False # True
wukongdnc.dag.DAG_executor_constants.check_pagerank_output = wukongdnc.dag.DAG_executor_constants.compute_pagerank and True
wukongdnc.dag.DAG_executor_constants.number_of_pagerank_iterations_for_partitions_groups_with_loops = 10
wukongdnc.dag.DAG_executor_constants.name_of_first_groupOrpartition_in_DAG = "PR1_1"
wukongdnc.dag.DAG_executor_constants.same_output_for_all_fanout_fanin = not wukongdnc.dag.DAG_executor_constants.compute_pagerank
wukongdnc.dag.DAG_executor_constants.use_incremental_DAG_generation = wukongdnc.dag.DAG_executor_constants.compute_pagerank and False
wukongdnc.dag.DAG_executor_constants.incremental_DAG_deposit_interval = 2
wukongdnc.dag.DAG_executor_constants.work_queue_size_for_incremental_DAG_generation_with_worker_processes =  2**10-1
wukongdnc.dag.DAG_executor_constants.tasks_use_result_dictionary_parameter = wukongdnc.dag.DAG_executor_constants.compute_pagerank and True
wukongdnc.dag.DAG_executor_constants.use_shared_partitions_groups = wukongdnc.dag.DAG_executor_constants.compute_pagerank and False
wukongdnc.dag.DAG_executor_constants.use_page_rank_group_partitions = wukongdnc.dag.DAG_executor_constants.compute_pagerank and True
wukongdnc.dag.DAG_executor_constants.use_struct_of_arrays_for_pagerank = wukongdnc.dag.DAG_executor_constants.compute_pagerank and False


## Tests ##

#running: python -m wukongdnc.dag.DAG_executor_driver and python -m wukongdnc.server.tcp_server or (tcp_server_lambda)

#Test1: simulated lambdas (A2) with non-selective-wait Sync-objects,
# create objects at the start

wukongdnc.dag.DAG_executor_constants.run_all_tasks_locally = True 
wukongdnc.dag.DAG_executor_constants.bypass_call_lambda_client_invoke = not wukongdnc.dag.DAG_executor_constants.run_all_tasks_locally and False
wukongdnc.dag.DAG_executor_constants.store_fanins_faninNBs_locally = True 
wukongdnc.dag.DAG_executor_constants.create_all_fanins_faninNBs_on_start = True
wukongdnc.dag.DAG_executor_constants.using_workers = False
wukongdnc.dag.DAG_executor_constants.using_threads_not_processes = True
wukongdnc.dag.DAG_executor_constants.num_workers = 1
wukongdnc.dag.DAG_executor_constants.use_multithreaded_multiprocessing = False
wukongdnc.dag.DAG_executor_constants.num_threads_for_multithreaded_multiprocessing = 1

wukongdnc.dag.DAG_executor_constants.FanIn_Type = "DAG_executor_FanIn"
wukongdnc.dag.DAG_executor_constants.FanInNB_Type = "DAG_executor_FanInNB"
wukongdnc.dag.DAG_executor_constants.process_work_queue_Type = "BoundedBuffer"
#FanIn_Type = "DAG_executor_FanIn_Select"
#FanInNB_Type = "DAG_executor_FanInNB_Select"
#process_work_queue_Type = "BoundedBuffer_Select"
"""
from . import DAG_executor_constants
DAG_executor_constants.set_test_number(1)

from . import DAG_executor_driver
DAG_executor_driver.run()
