"""
Important: Thsi file incudes many tests at the end which illustrate 
all the configurations and how to set the confguration flags below.
"""

import logging
import os

# log_level = logging.INFO
log_level = "INFO"
logger = logging.getLogger(__name__)

# using INFO level for this constants file - no logger.trace() calls
# and we add the TRACE level in DAG_executor_driver or BFS, not here
logger.setLevel(logging.INFO)
formatter = logging.Formatter('[%(asctime)s] [%(module)s] [%(processName)s] [%(threadName)s]: %(message)s')
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch.setFormatter(formatter)
logger.addHandler(ch)

# Configuraion:
#
# True if we are not using Lambdas, i.e., executing tasks with threads or processes
# local, i.e., on one machine.
run_all_tasks_locally = True         # vs run tasks remotely (in Lambdas)
# True if we want to bypass the call to lambda_client.invoke() so that we
# do not actually create a real Lambda; instead, invoke_lambda_DAG_executor()
# in invoker.y will call lambda_handler(payload_json,None) directly, where
# lambda_handler() is defned locally in invoker.py, i.e., is not the actual
# handler, which is defined in handlerDAG.py. This lets us test the code
# for real lambdas without actually creating real Lambdas.
# Note: if this is True then run_all_tasks_locally must be False. 
# This is asserted below.
bypass_call_lambda_client_invoke = (not run_all_tasks_locally) and True
# True if synch objects are stored locally, i.e., in the memory of the single
# machine on which the threads are executing.  If we are using multiprocessing
# or Lambdas, this must be False. When False, the synch objects are stored
# on the tcp_server or in InfiniX lambdas.
store_fanins_faninNBs_locally = False
# True when all FanIn and FanInNB objects are created locally or on the
# tcp_server or IniniX all at once at the start of the DAG execution. If
# False, synch objects are created on the fly, i.e, we execute create-and-fanin
# operations that create a synch object if it has not been created yet and then
# execute a Fan_in operaation on the created object.
# 
# This mus be false if we aer doing incremental_DAG_generation; this is asserted below.
create_all_fanins_faninNBs_on_start = False

# True if the DAG is executed by a "pool" of threads/processes. False, if we are
# using Lambdas or we are using threads to simulate the use of Lambdas. In the latter
# case, instead of, e.g., starting a Lambda at fan_out operations, we start a thread.
# This results in the creation of many threads and is only use to test the logic 
# of the Lambda code.
using_workers = True
# True when we are not using Lambas and tasks are executed by threads instead of processes. 
# False when we are not using lambdas and are using multiprocesssing 
using_threads_not_processes = True
# When using_workers, this is how many threads or processes in the pool.
num_workers = 2
# Use one or more worker processes (num_workers) with one or more threads
use_multithreaded_multiprocessing = False
num_threads_for_multithreaded_multiprocessing = 2

# if using lambdas to store synch objects, run tcp_server_lambda.
# if store in regular python functions instead of real Lambdas
# set using_Lambda_Function_Simulator = True
FanIn_Type = "DAG_executor_FanIn"
FanInNB_Type = "DAG_executor_FanInNB"
process_work_queue_Type = "BoundedBuffer"
#FanIn_Type = "DAG_executor_FanIn_Select"
#FanInNB_Type = "DAG_executor_FanInNB_Select"
#process_work_queue_Type = "BoundedBuffer_Select"

# if running real lambdas or storing synch objects in real lambdas:
#   Set SERVERLESS_SYNC to True or False in wukongdnc constants !!!!!!!!!!!!!!
#

# these are the files that use log messages:
debug_DAG_executor = False
debug_BFS = False
debug_DAG_executor_driver = False
debug_BFS_generate_DAG_info_incremental_groups = False
debug_BFS_generate_DAG_info_incremental_partitions = False
debug_BFS_generate_DAG_info = False
debug_BFS_generate_shared_partitions_groups = False
debug_BFS_pagerank = False
debug_BFS_Shared = False
debug_DAG_boundedbuffer_work_queue = False
debug_DAG_executor_create_multithreaded_multiprocessing_processes = False
debug_DAG_executor_create_threads_for_multiT_multiP = False
debug_DAG_executor_synchronizer = False

# Currently, this is for storing synch objects in simulated lambdas;
store_sync_objects_in_lambdas = False
using_Lambda_Function_Simulators_to_Store_Objects = False
sync_objects_in_lambdas_trigger_their_tasks = False
# use orchestrator to invoke functions (e.g., when all fanin/fanout results are available)
using_DAG_orchestrator = False
# map ech synch object by name to the function it resided in. if we create
# all objects on start we msut map the objects to function so we can get the
# function an onject is in. If we do not create objects on start then
# we will crate them on the fly. We can still map he objects to functions -
# we will just have to create the object in the funtion on the first function
# invocation. If we do not map objects, then we will/can only invoke tge
# function that contains the possibly pre-created object once. 
map_objects_to_lambda_functions = False
# We can use an anonymous simulated function or a single named lambda deployment.
# In this case, we can invoke the function only once snce we cannot
# refer to a function instance by name, i.e., by index for simuated functions and 
# by uniqueu deploment name for real lambda functions. For simuated functions
# we do not create an indexed list of functions, and for real Lambdas we
# just have one deployment. 
# Note: if map_objects_to_lambda_functions then use_anonymous_lambda_functions 
# must be False. We map objects to function so we can invoke a function instance 
# more than once when we access an object more than once. If we 
# use_anonymous_lambda_functions then we cannot access a specific function
# (by name or by index).
# ToDo: integrate using_single_lambda_function with this mapping stuff. that
# is, map names to lambda functions, and sometimes there is only one function.
use_anonymous_lambda_functions = False
# So if create on start then must map objects and cannot use anonymous functions.
# If want to use anonymous functions then no create objects on statr and no mapping.

# use a single lambda function to store all of the synchroization objects
# to make an easy test case. This cannot be used when using the function 
# simulators or using the DAG_orchestrator.
# Using this is ToDo - search for it and see comments
using_single_lambda_function = False

# For all: remote objects, using select objects:
# 1. run_all_tasks_locally = True, create objects on start = True:
# TTFFTF: no trigger and no DAG_orchestrator, but map objects 
# (anon is false) and create objects on start
# variations:
# a. change D_O to T, 
# b. change map to F, and anon to T: Note: no function lock since anon caled only once
# c. change D_O to F, map F, anon T: Note: no function lock since anon caled only once
#
# 2. run_all_tasks_locally = False (s0 using_workers = False), create objects on start = True:
# Note: not running real lambdas yet, so need TTT, i.e., not using threads
#       to simulate lambdas and not running real lambdas yet, so need to
#       trigger lambdas, which means store objects in lambdas and they call
#       DAG_excutor_Lambda to execute task (i.e., "trigger task to run in 
#       the same lambda"). Eventually we'll have tests for use real 
#       non-triggered lambdas to run tasks (invoked at fanouts/faninNBS)
#       and objects stored in lambdas or on server.
# TTTTTF: trigger and DAG_orchestrator, map objects (anon is false) and create objects on start
# variations:
# a. change map to F, and anon to T and create on start to F: Note: no function lock since anon called only once
# b. change DAG_orchestrator to F - so not going through enqueue so will
#    create on fly in other places besides equeue.

# Q: if map is F does anon have to be True? In theory no, use any named
#    function to store any sync ojject. e.g., use DAG_executor_i for ith
#    object accessed, but then there's a finite limit on number of functions;
#    still, using this scheme we can call same function more than once, 
#    as long as you dynamically map the objects to the name of the 
#    function (chosen at run time) that they are stored in. So either
#    way you need to map sync object names to functions if you want to 
#    invoke the function to do an op on the object more than once.

#assert:
if using_workers and not run_all_tasks_locally:
    logger.error("[Internal Error]: Configuration error: if using_workers then must run_all_tasks_locally.")
    logging.shutdown()
    os._exit(0)  

#assert
if not run_all_tasks_locally and store_fanins_faninNBs_locally:
    logger.error("[Internal Error]: Configuration error: if not run_all_tasks_locally (i.e., using real lambdas) then objects cannot be stored locally.")
    logging.shutdown()
    os._exit(0) 

#assert:
if bypass_call_lambda_client_invoke and run_all_tasks_locally:
    logger.error("[Internal Error]: Configuration error: if bypass_call_lambda_client_invoke then must be running real Lambdas"
        + " i.e., not run_all_tasks_locally.")
    logging.shutdown()
    os._exit(0)  

#assert:
if using_workers and not using_threads_not_processes:
    if store_fanins_faninNBs_locally:
        # When using worker processed, synch objects must be stored remoely
        logger.error("[Internal Error]: Configuration error: if using_workers and not using_threads_not_processes"
            + " then store_fanins_faninNBs_locally must be False.")
        logging.shutdown()
        os._exit(0)

#assert:
if create_all_fanins_faninNBs_on_start and not run_all_tasks_locally and store_sync_objects_in_lambdas:
    if not map_objects_to_lambda_functions:
        # if create sync objects on start and executing tasks in lambdas "
        # then we must map them to function so that we can determine the 
        # function an object is in.
        logger.error("[Internal Error]: Configuration error: if create_all_fanins_faninNBs_on_start"
            + " then map_objects_to_functions must be True.")
        logging.shutdown()
        os._exit(0)

#assert:
if map_objects_to_lambda_functions:
    if use_anonymous_lambda_functions:
        # if create sync objects on start then we must map them to function so
        # that we can determine the function an object is in.
        logger.error("[Internal Error]: Configuration error: if map_objects_to_lambda_functions"
            + " then use_anonymous_lambda_functions must be False.")
        logging.shutdown()
        os._exit(0)

#assert:
if sync_objects_in_lambdas_trigger_their_tasks:
    if run_all_tasks_locally:
        # if create sync objects on start then we must map them to function so
        # that we can determine the function an object is in.
        logger.error("[Internal Error]: Configuration error: if sync_objects_in_lambdas_trigger_their_tasks"
            + " then not run_all_tasks_locally must be True.")
        logging.shutdown()
        os._exit(0)

##########################################
###### PageRank settings start here ######
##########################################

# Indicates that we are computing pagerank and thus that the pagerank
# options are active and pagerank asserts should hold
compute_pagerank = True
# used in BFS_pagerank. For non-loops, we only need 1 iteration
number_of_pagerank_iterations_for_partitions_groups_with_loops = 10
name_of_first_groupOrpartition_in_DAG = "PR1_1"

# pagerank values will be saved so we can check them after execution
# in DAG_executor_check_pagerank.py

#rhc: ToDo: requires a global pagerank result so need worker threads?
check_pagerank_output = compute_pagerank and run_all_tasks_locally and (using_workers or not using_workers) and using_threads_not_processes and True

# a task that has multiple fanouts/faninNBs sends the same output
# to all of them; otherwise, the task sends a possibly different 
# output to each. This same_output_per_fanout_fanin flag is False
# for pagerank.
# Note: For DAG generation, for each state we execute a task and 
# for each task T we have to say what T's task_inputs are - these are the 
# names of tasks that give inputs to T. When we have per-fanout output
# instead of having the same output for all fanouts, we specify the 
# task_inputs as "sending task - receiving task". So a sending task
# S might send outputs to fanouts A and B so we use "S-A" and "S-B"
# as the task_inputs, instad of just using "S", which is the Dask way.
same_output_for_all_fanout_fanin = not compute_pagerank

# True if DAG generation and DAG_execution are overlapped. 
use_incremental_DAG_generation = compute_pagerank and True

# True if we are clustering fanouts that satisfy the cluster criteria
enable_runtime_task_clustering = compute_pagerank and True

# assert 
if compute_pagerank and use_incremental_DAG_generation and create_all_fanins_faninNBs_on_start:
    logger.error("[Internal Error]: Configuration error: incremental_DAG_generation"
        + " requires not create_all_fanins_faninNBs_on_start"
        + " i.e., create synch objects on the fly since we don't know all of the synch objects "
        + " at the start (the DAG is not complete)")
    logging.shutdown()
    os._exit(0)  

# generate next DAG when num_incremental_DAGs_generated mod 
# incremental_interval == 0. For example, if we set this
# value to 2, after we generate the first DAG, with a complete 
# partition 1 and an incomplete partition 2, we will generate 
# a new DAG when we process partition 4 (2+2) then 6, 8, etc.
# Note: We publish the first DAG, which is a complete DAG with 
# one partition or a DAG with a complete first partition and 
# and incomplete second partition, then we generate the 
# next DAG with complete paritions 1 and 2 and incomplete
# partition 3 and we increment num_DAGs_generated to 1. we will 
# publish the next generated DAG again when num_DAGs_generated mod
# incremental_DAG_deposit_interval is 0. So if incremental_DAG_deposit_interval
# is 2, we will not publish the DAg with complete partitions 1 and 2
# and incomplete partition 3 (since 1 mod 2 is not 0), but we will
# publish the next DAG generated which has complete parititions 1, 2, and 3
# and incomplete partition 4; so every other generated DAG is published,
# Note: when the generated DAG is complete, we publish it regardless
# of whether or not we have completed the interval. This means that 
# if are using an interval that is >= than the number of partitions 
# in the DAG (minus 2), then we are effectively doing non-incremental DAG
# generation since the DAG will be completely generated before the 
# interval is completed. (Of couse, the first DAG published is the 
# incomplete DAG with complete partitin 1 and incomplete partition 
# 2, but the next DAG publshed will be the complete DAG, which is 
# the last DAG generated.)
incremental_DAG_deposit_interval = 2

#assert:
if incremental_DAG_deposit_interval < 1:
    logger.error("[Internal Error]: Configuration error: incremental_DAG_deposit_interval"
         + " must be >= 1. We mod by incremental_DAG_deposit_interval so it"
         + " cannot be 0 and using a negative number makes no sense.")
    logging.shutdown()
    os._exit(0)

#rhc: ToDo: what should this be? Used as capacity of boundedbuffer
# Note: Pythin has no max Int
# rhc: ToDo: Make a bounded_buffer with a dynamic buffer 
work_queue_size_for_incremental_DAG_generation_with_worker_processes =  2**10-1

#assert:
if not same_output_for_all_fanout_fanin and not compute_pagerank:
    logger.error("[Internal Error]: Configuration error: if same_output_for_all_fanout_fanin"
        + " then must be computing pagerank.")
    logging.shutdown()
    os._exit(0)

# For PageRank:
# set tasks_use_result_dictionary_parameter = True
# and same_output_for_all_fanout_fanin = False.
#
# True when executed task uses a dictionary parameter that contains its inputs
# instead of a tuple Dask-style. This is True for the PageRank. For pagerank
# we use a single pagerank task and, if using lambdas, a single lambda excutor.
# PageRank tasks have varying numbers of inputs (for fanoins/faninNBs) that are
# passed to the PageRank task in a dictionary.
tasks_use_result_dictionary_parameter = compute_pagerank and True

# For PageRank:
# When we run_tasks_locally and we use threads to simulate lambdas or
# worker threads, instad of inputting each tasks's partition separately
# when the task suns, we have one global shared array with all the 
# partitions/groups and the threads access that array when they do their
# tasks.
use_shared_partitions_groups = compute_pagerank and False

#assert:
#if compute_pagerank and (use_shared_partitions_groups and not run_all_tasks_locally)):#
if compute_pagerank and (use_shared_partitions_groups and not run_all_tasks_locally):
    logger.error("[Internal Error]: Configuration error: if using a single shared array of"
        + " partitions or groups then must run_tasks_locally and be using_threads_not_processes.")
    logging.shutdown()
    os._exit(0)

# For PageRank:
# Execute page rank partitions or execute page rank groups
# If True use groups else use partitions
use_page_rank_group_partitions = compute_pagerank and True

# For PageRank:
# Use a struct of arrays to improve cache performance
use_struct_of_arrays_for_pagerank = compute_pagerank and False

# For PageRank:
# Use a multithreaded BFS where bfs() is generating the next group
# or partition and depositing this is a buffer to be withdrawn
# by a separate thread that adds information about this group/partition
# to the DAG_info. So partitions/groups can be generated concurrently 
# with generating the DAG_info object. This is not to be confused with 
# incremental DAG generation in which the DAG is executed concurrently 
# with bfs() generating the partitions/groups and the DAG_info object.
# Consider also the combination of use_incremental_DAG_generation and 
# use_multithreaded_BFS. 
use_multithreaded_BFS = compute_pagerank and False

#assert:
if use_multithreaded_BFS and use_incremental_DAG_generation:
    logger.error("[Internal Error]: Configuration error: if use_multithreaded_BFS"
        + " then must not use_incremental_DAG_generation .")
    logging.shutdown()
    os._exit(0)

#assert:
if compute_pagerank and (use_struct_of_arrays_for_pagerank and not use_shared_partitions_groups):
    logger.error("[Internal Error]: Configuration error: if use_struct_of_arrays_for_pagerank"
        + " then must use_shared_partitions_groups.")
    logging.shutdown()
    os._exit(0)

# When we use real lamdas, instead of reading the individual group 
# or partition node files from cloud storage as we need them in BFS_agerank.py, 
# we can read all the groups or partitions at the start and pass them along
# to the lambdas that are started. There is a limit on the total of the sizes
# of these files. This is not checked yet. We may want to read the files
# in batches, on demnand, etc.

# Note: Use this when running real lambdas and avoiding cloud storage 
# for I/O of the groups or partitions.
input_all_groups_partitions_at_start = compute_pagerank and (
   not run_all_tasks_locally and (not bypass_call_lambda_client_invoke) and (not use_incremental_DAG_generation)
   ) and True

# Note: Use this to test real lambda code locally without using real lambdas
# input_all_groups_partitions_at_start = compute_pagerank and (
#     not run_all_tasks_locally and (bypass_call_lambda_client_invoke) and (not use_incremental_DAG_generation)
#     ) and True

A1 = A1_Server = A1_FunctionSimulator = A1_SingleFunction = A1_Orchestrator = False
A2 = False
A3 = A3_S = A3_FunctionSimulator = A3_SingleFunction = A3_Orchestrator = False
A4_L = A4_R = False
A5 = A6 = False

# These are used to shorten the expressions in the configurations
not_using_lambda_options =  not using_Lambda_Function_Simulators_to_Store_Objects and (
    not sync_objects_in_lambdas_trigger_their_tasks) and (
    not using_DAG_orchestrator) and (
    not using_single_lambda_function)
# Note: using_DAG_orchestrator while sync_objects_in_lambdas_trigger_their_tasks
# is a non-Wukong scheme for managing lambdas - sync objects are stored in Lambdas 
# and when using sync_objects_in_lambdas_trigger_their_tasks the objects
# trigger their tasks to run within the same lambda. A1_Wukong uses
# lambas to run tasks at fanouts/faninNBs, and stores synch objects on the 
# server or in lambdas. A1_Wukong may use the DAG_orchestrator to manage the 
# sync objects/lambdas. 

# Note: for all configurations, set create_all_fanins_faninNBs_on_start = True/False

# Configurations:

# objects can be stored on the tcp server or in lambdas
A1_Wukong = not run_all_tasks_locally and not using_workers and not store_fanins_faninNBs_locally

A1_Wukong_ObjectsOnServer = A1_Wukong and not store_sync_objects_in_lambdas and (
    not not_using_lambda_options)
# This is Wukong style with sync objects stored on Server
# FanIn_Type = "DAG_executor_FanIn"
# FanInNB_Type = "DAG_executor_FanInNB"
# run tcp_server
# Set SERVERLESS_SYNC to True in wukongdnc constants
A1_Wukong_ObjectsInRealLambdas_UseManyLambdaFunction = A1_Wukong and store_sync_objects_in_lambdas and (
    not not_using_lambda_options)
# This is Wukong style with sync objects stored in two or more Lambdas to balance the load.
# Not using lamba simulators, just mapping objects (names) to lambdas.
# If using_single_lambda_function then there is a single lambda that stores all sync objects
# which makes setup on AWS simpler (i.e., using one distribution).
# FanIn_Type = "DAG_executor_FanIn_Select"
# FanInNB_Type = "DAG_executor_FanInNB_Select"
# run tcp_server_lambda
# Set SERVERLESS_SYNC to True in wukongdnc constants
A1_Wukong_ObjectsInRealLambdas_UseSingleLambdaFunction = A1_Wukong and store_sync_objects_in_lambdas and (
    using_single_lambda_function) and (
    not using_Lambda_Function_Simulators_to_Store_Objects) and (
    not sync_objects_in_lambdas_trigger_their_tasks) and (
    not using_DAG_orchestrator)
# This is Wukong style with all sync objects stored in a single Lambda function 
#   to make AWS setup easier,
# Note: We do not use using_single_lambda_function when we use simulated lambdas to store objects.
#   Using a single function is handy when we have to run lambdas on AWS, i.e., we only need
#   one deployment. With simulated lambdas, having multiple "deployments" is not painful
#   (to create the deployments, which aer just seperate Python functions.) 
# FanIn_Type = "DAG_executor_FanIn_Select"
# FanInNB_Type = "DAG_executor_FanInNB_Select"
# run tcp_server_lambda
# Set SERVERLESS_SYNC to True in wukongdnc constants
A1_Wukong_ObjectsInRealLambdas_UsingOrchestator = A1_Wukong and store_sync_objects_in_lambdas and (
    not using_Lambda_Function_Simulators_to_Store_Objects) and (
    not sync_objects_in_lambdas_trigger_their_tasks) and (
    not using_single_lambda_function) 
# This is Wukong style with all sync objects stored in a lambda functions and orchestrated.
# FanIn_Type = "DAG_executor_FanIn_Select"
# FanInNB_Type = "DAG_executor_FanInNB_Select"
# run tcp_server_lambda
# Set SERVERLESS_SYNC to True in wukongdnc constants
A1_Wukong_ObjectInSimulatedLambdas = A1_Wukong and store_sync_objects_in_lambdas and (
    using_Lambda_Function_Simulators_to_Store_Objects) and (
    not sync_objects_in_lambdas_trigger_their_tasks) and (
    not using_single_lambda_function) and (
    not using_DAG_orchestrator)
# FanIn_Type = "DAG_executor_FanIn_Select"
# FanInNB_Type = "DAG_executor_FanInNB_Select"
# run tcp_server_lambda
# Set SERVERLESS_SYNC to True in wukongdnc constants
A1_Wukong_ObjectInSimulatedLambdas_UsingOrchestator = A1_Wukong and store_sync_objects_in_lambdas and (
    using_Lambda_Function_Simulators_to_Store_Objects) and (
    using_DAG_orchestrator) and (
    not sync_objects_in_lambdas_trigger_their_tasks) and (
    not using_single_lambda_function) 
# FanIn_Type = "DAG_executor_FanIn_Select"
# FanInNB_Type = "DAG_executor_FanInNB_Select"
# run tcp_server_lambda
# Set SERVERLESS_SYNC to True in wukongdnc constants

# Note: Currently we are assuming using_Lambda_Function_Simulators_to_Store_Objects is True
# when we use the orchestrator. 
A1_Orchestrate_SyncObjectsandTasksinRealLambdas = not run_all_tasks_locally and not using_workers and not store_fanins_faninNBs_locally and (
    store_sync_objects_in_lambdas) and (
    using_DAG_orchestrator) and (
    not using_Lambda_Function_Simulators_to_Store_Objects) and (
    not sync_objects_in_lambdas_trigger_their_tasks) and (
    not using_single_lambda_function)
# FanIn_Type = "DAG_executor_FanIn_Select"
# FanInNB_Type = "DAG_executor_FanInNB_Select"
# run tcp_server_lambda
# Set SERVERLESS_SYNC to True in wukongdnc constants

A1_Orchestrate_SyncObjectsandTasksinSimulatedLambdas = not run_all_tasks_locally and not using_workers and not store_fanins_faninNBs_locally and (
    store_sync_objects_in_lambdas) and (
    using_DAG_orchestrator) and (
    using_Lambda_Function_Simulators_to_Store_Objects) and sync_objects_in_lambdas_trigger_their_tasks and (
    not using_single_lambda_function)
# FanIn_Type = "DAG_executor_FanIn_Select"
# FanInNB_Type = "DAG_executor_FanInNB_Select"
# run tcp_server_lambda
# Set SERVERLESS_SYNC to True in wukongdnc constants

using_threads_to_simulate_lambdas = run_all_tasks_locally and not using_workers

# using threads to simulate lambdas and store sync objects locally (not on server or in lambdas)
A2 = using_threads_to_simulate_lambdas and store_fanins_faninNBs_locally and not_using_lambda_options
# set FanIn_Type = "DAG_executor_FanIn_Select" or "DAG_executor_FanIn"
# set FanInNB_Type = "DAG_executor_FanInNB_Select" or "DAG_executor_FanInNB"
# Set SERVERLESS_SYNC to False in wukongdnc constants

# using threads to simulate lambdas that execute tasks and store sync objects remotely (on server or in lambdas)
# Note: All tasks executed by Wukong style lambdas that are invoked at fanouts/fanins.
A3 = using_threads_to_simulate_lambdas and not store_fanins_faninNBs_locally

A3_ObjectsOnServer = A3 and not_using_lambda_options
# set FanIn_Type = = "DAG_executor_FanIn_Select" or "DAG_executor_FanIn"
# set FanInNB_Type = "DAG_executor_FanInNB_Select" or "DAG_executor_FanInNB"
# Set SERVERLESS_SYNC to False in wukongdnc constants
A3_ObjectsInRealLambdas_UseManyLambdaFunction = A3 and store_sync_objects_in_lambdas and (
    not not_using_lambda_options)
# FanIn_Type = "DAG_executor_FanIn_Select"
# FanInNB_Type = "DAG_executor_FanInNB_Select"
# run tcp_server_lambda
# Set SERVERLESS_SYNC to False in wukongdnc constants    
A3_ObjectsInRealLambdas_UseSingleLambdaFunction = A3 and store_sync_objects_in_lambdas and (
    using_single_lambda_function) and (
    not using_Lambda_Function_Simulators_to_Store_Objects) and (
    not sync_objects_in_lambdas_trigger_their_tasks) and (
    not using_DAG_orchestrator)
# FanIn_Type = "DAG_executor_FanIn_Select"
# FanInNB_Type = "DAG_executor_FanInNB_Select"
# run tcp_server_lambda
# Set SERVERLESS_SYNC to False in wukongdnc constants 
A3_ObjectsInRealLambdas_UsingOrchestator = A3 and store_sync_objects_in_lambdas and (
    not using_Lambda_Function_Simulators_to_Store_Objects) and (
    not sync_objects_in_lambdas_trigger_their_tasks) and (
    not using_single_lambda_function) 
# This is Wukong style with all sync objects stored in a lambda functions and orchestrated.
# FanIn_Type = "DAG_executor_FanIn_Select"
# FanInNB_Type = "DAG_executor_FanInNB_Select"
# run tcp_server_lambda
# Set SERVERLESS_SYNC to True in wukongdnc constants

A3_ObjectInSimulatedLambdas = A3 and store_sync_objects_in_lambdas and (
    using_Lambda_Function_Simulators_to_Store_Objects) and (
    not sync_objects_in_lambdas_trigger_their_tasks) and (
    not using_single_lambda_function) and (
    not using_DAG_orchestrator)
# FanIn_Type = "DAG_executor_FanIn_Select"
# FanInNB_Type = "DAG_executor_FanInNB_Select"
# run tcp_server_lambda
# Set SERVERLESS_SYNC to False in wukongdnc constants
A3_ObjectInSimulatedLambdas_UsingOrchestator = A3 and store_sync_objects_in_lambdas and (
    using_Lambda_Function_Simulators_to_Store_Objects) and (
    using_DAG_orchestrator) and (
    not sync_objects_in_lambdas_trigger_their_tasks) and (
    not using_single_lambda_function) 
# FanIn_Type = "DAG_executor_FanIn_Select"
# FanInNB_Type = "DAG_executor_FanInNB_Select"
# run tcp_server_lambda
# Set SERVERLESS_SYNC to True in wukongdnc constants

A4 = run_all_tasks_locally and using_workers and using_threads_not_processes
A4_ObjectsStoredLocally = A4 and store_fanins_faninNBs_locally and (
    not store_sync_objects_in_lambdas) and not_using_lambda_options
# set num_workers
# no tcp_server since storing locally
# Set SERVERLESS_SYNC to False in wukongdnc constants
A4_ObjectsStoredRemotely = A4 and not store_fanins_faninNBs_locally and (
    not store_sync_objects_in_lambdas) and not_using_lambda_options
# set num_workers
# set FanIn_Type = "DAG_executor_FanIn_Select" or "DAG_executor_FanIn"
# set FanInNB_Type = "DAG_executor_FanInNB_Select" or "DAG_executor_FanInNB"
# set process_work_queue_Type = "BoundedBuffer"
# run tcp_server
# Set SERVERLESS_SYNC to False in wukongdnc constants
# Note: We do not run tcp_server_lambda so while we can use the "Select" 
# objects they are not stored in lambdas, they are regular objects on tcp_server.

# Note about A4: For A4, we are using worker threads, which is not going to 
# generate speedup in Python.

A5 = run_all_tasks_locally and using_workers and not using_threads_not_processes
A5_ObjectsStoredRemotely = A5 and not store_fanins_faninNBs_locally and (
    not store_sync_objects_in_lambdas) and not_using_lambda_options
# set num_workers
# set FanIn_Type = "DAG_executor_FanIn_Select" or "DAG_executor_FanIn"
# set FanInNB_Type = "DAG_executor_FanInNB_Select" or "DAG_executor_FanInNB"
# set process_work_queue_Type = "BoundedBuffer_Select" or process_work_queue_Type = "BoundedBuffer"
# run tcp_server.
# Set SERVERLESS_SYNC to False in wukongdnc constants
# Note: We do not run tcp_server_lambda so while we can use the "Select" 
# objects they are not stored in lambdas, they are regular objects on tcp_server.

# Note about A5: For A5, we are using worker processes, which requires sync objects to be
# stored remotely. Objects can only be stored on the server.

A6 = run_all_tasks_locally and use_multithreaded_multiprocessing and using_workers and not using_threads_not_processes and not store_fanins_faninNBs_locally and not_using_lambda_options
# set num_threads_for_multithreaded_multiprocessing
# set FanIn_Type = "DAG_executor_FanIn_Select" or "DAG_executor_FanIn"
# set FanInNB_Type = "DAG_executor_FanInNB_Select" or "DAG_executor_FanInNB"
# run tcp_server

"""
not_A1s = not A1_FunctionSimulator and not A1_SingleFunction and not A1_Orchestrator
not_A2 = not A2
not_A3s = not A3_Server and not A3_FunctionSimulator and not A3_SingleFunction and not A3_Orchestrator 
not_A4s = not A4_L and not A4_R
not_A5 = not A5
not_A6 = not A6
if not_A1s and not_A2 and not_A3s and not_A4s and not_A5 and not_A6:
    pass
"""

# Assert using worker processes  ==> store objects remotely
# Assert sync_objects_in_lambdas_trigger_their_tasks ==> using_DAG_orchestrator
# Assert using a lambda option ==> store objects in Lambdas 
# Assert using_DAG_orchestrator ==> not run_all_tasks_locally and not using_workers and not store_fanins_faninNBs_locally

##########################################
###### Tests start here ######
##########################################

#################################################
###### Non-Pageran tests (using Dask DAGs) ######
#################################################

#running non-pagerank test: python -m wukongdnc.dag.DAG_executor_driver and python -m wukongdnc.server.tcp_server or (tcp_server_lambda)

def non_real_lambda_base():
    global store_sync_objects_in_lambdas
    global using_Lambda_Function_Simulators_to_Store_Objects
    global sync_objects_in_lambdas_trigger_their_tasks
    global using_DAG_orchestrator
    global map_objects_to_lambda_functions
    global use_anonymous_lambda_functions
    global using_single_lambda_function
    global compute_pagerank
    global check_pagerank_output
    global number_of_pagerank_iterations_for_partitions_groups_with_loops 
    global name_of_first_groupOrpartition_in_DAG
    global same_output_for_all_fanout_fanin
    global use_incremental_DAG_generation
    global incremental_DAG_deposit_interval
    global work_queue_size_for_incremental_DAG_generation_with_worker_processes
    global tasks_use_result_dictionary_parameter
    global use_shared_partitions_groups
    global use_page_rank_group_partitions
    global use_struct_of_arrays_for_pagerank
    global run_all_tasks_locally
    global bypass_call_lambda_client_invoke
    global store_fanins_faninNBs_locally
    global create_all_fanins_faninNBs_on_start
    global using_workers
    global using_threads_not_processes
    global num_workers
    global use_multithreaded_multiprocessing
    global num_threads_for_multithreaded_multiprocessing
    global FanIn_Type
    global FanInNB_Type
    global process_work_queue_Type

    store_sync_objects_in_lambdas = False
    using_Lambda_Function_Simulators_to_Store_Objects = False
    sync_objects_in_lambdas_trigger_their_tasks = False
    using_DAG_orchestrator = False
    map_objects_to_lambda_functions = False
    use_anonymous_lambda_functions = False
    using_single_lambda_function = False

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

    compute_pagerank = False # True
    check_pagerank_output = compute_pagerank and True
    number_of_pagerank_iterations_for_partitions_groups_with_loops = 10
    name_of_first_groupOrpartition_in_DAG = "PR1_1"
    same_output_for_all_fanout_fanin = not compute_pagerank
    use_incremental_DAG_generation = compute_pagerank and False
    incremental_DAG_deposit_interval = 2
    work_queue_size_for_incremental_DAG_generation_with_worker_processes =  2**10-1
    tasks_use_result_dictionary_parameter = compute_pagerank and True
    use_shared_partitions_groups = compute_pagerank and False
    use_page_rank_group_partitions = compute_pagerank and True
    use_struct_of_arrays_for_pagerank = compute_pagerank and False


#Test1: simulated lambdas (A2) with non-selective-wait Sync-objects,
# create objects at the start
def test1():
    print("test1")
    global store_sync_objects_in_lambdas
    global using_Lambda_Function_Simulators_to_Store_Objects
    global sync_objects_in_lambdas_trigger_their_tasks
    global using_DAG_orchestrator
    global map_objects_to_lambda_functions
    global use_anonymous_lambda_functions
    global using_single_lambda_function
    global compute_pagerank
    global check_pagerank_output
    global number_of_pagerank_iterations_for_partitions_groups_with_loops 
    global name_of_first_groupOrpartition_in_DAG
    global same_output_for_all_fanout_fanin
    global use_incremental_DAG_generation
    global incremental_DAG_deposit_interval
    global work_queue_size_for_incremental_DAG_generation_with_worker_processes
    global tasks_use_result_dictionary_parameter
    global use_shared_partitions_groups
    global use_page_rank_group_partitions
    global use_struct_of_arrays_for_pagerank
    global run_all_tasks_locally
    global bypass_call_lambda_client_invoke
    global store_fanins_faninNBs_locally
    global create_all_fanins_faninNBs_on_start
    global using_workers
    global using_threads_not_processes
    global num_workers
    global use_multithreaded_multiprocessing
    global num_threads_for_multithreaded_multiprocessing
    global FanIn_Type
    global FanInNB_Type
    global process_work_queue_Type

    run_all_tasks_locally = True 
    bypass_call_lambda_client_invoke = not run_all_tasks_locally and False
    store_fanins_faninNBs_locally = True 
    create_all_fanins_faninNBs_on_start = True
    using_workers = False
    using_threads_not_processes = True
    num_workers = 1
    use_multithreaded_multiprocessing = False
    num_threads_for_multithreaded_multiprocessing = 1

    FanIn_Type = "DAG_executor_FanIn"
    FanInNB_Type = "DAG_executor_FanInNB"
    process_work_queue_Type = "BoundedBuffer"
    #FanIn_Type = "DAG_executor_FanIn_Select"
    #FanInNB_Type = "DAG_executor_FanInNB_Select"
    #process_work_queue_Type = "BoundedBuffer_Select"

#Test2: simulated lambdas (A2) with selective-wait Sync-objects,
#        create objects at the start
def test2():
    print("test2")
    global store_sync_objects_in_lambdas
    global using_Lambda_Function_Simulators_to_Store_Objects
    global sync_objects_in_lambdas_trigger_their_tasks
    global using_DAG_orchestrator
    global map_objects_to_lambda_functions
    global use_anonymous_lambda_functions
    global using_single_lambda_function
    global compute_pagerank
    global check_pagerank_output
    global number_of_pagerank_iterations_for_partitions_groups_with_loops 
    global name_of_first_groupOrpartition_in_DAG
    global same_output_for_all_fanout_fanin
    global use_incremental_DAG_generation
    global incremental_DAG_deposit_interval
    global work_queue_size_for_incremental_DAG_generation_with_worker_processes
    global tasks_use_result_dictionary_parameter
    global use_shared_partitions_groups
    global use_page_rank_group_partitions
    global use_struct_of_arrays_for_pagerank
    global run_all_tasks_locally
    global bypass_call_lambda_client_invoke
    global store_fanins_faninNBs_locally
    global create_all_fanins_faninNBs_on_start
    global using_workers
    global using_threads_not_processes
    global num_workers
    global use_multithreaded_multiprocessing
    global num_threads_for_multithreaded_multiprocessing
    global FanIn_Type
    global FanInNB_Type
    global process_work_queue_Type

    run_all_tasks_locally = True 
    store_fanins_faninNBs_locally = True 
    create_all_fanins_faninNBs_on_start = True
    using_workers = False
    using_threads_not_processes = True
    num_workers = 1
    use_multithreaded_multiprocessing = False
    num_threads_for_multithreaded_multiprocessing = 1

    #FanIn_Type = "DAG_executor_FanIn"
    #FanInNB_Type = "DAG_executor_FanInNB"
    #process_work_queue_Type = "BoundedBuffer"
    FanIn_Type = "DAG_executor_FanIn_Select"
    FanInNB_Type = "DAG_executor_FanInNB_Select"
    process_work_queue_Type = "BoundedBuffer_Select"

#Test 3: simulated lambdas (A2) with non-selective-wait Sync-objects,
#         create objects on-the-fly
def test3():
    print("test3")
    global store_sync_objects_in_lambdas
    global using_Lambda_Function_Simulators_to_Store_Objects
    global sync_objects_in_lambdas_trigger_their_tasks
    global using_DAG_orchestrator
    global map_objects_to_lambda_functions
    global use_anonymous_lambda_functions
    global using_single_lambda_function
    global compute_pagerank
    global check_pagerank_output
    global number_of_pagerank_iterations_for_partitions_groups_with_loops 
    global name_of_first_groupOrpartition_in_DAG
    global same_output_for_all_fanout_fanin
    global use_incremental_DAG_generation
    global incremental_DAG_deposit_interval
    global work_queue_size_for_incremental_DAG_generation_with_worker_processes
    global tasks_use_result_dictionary_parameter
    global use_shared_partitions_groups
    global use_page_rank_group_partitions
    global use_struct_of_arrays_for_pagerank
    global run_all_tasks_locally
    global bypass_call_lambda_client_invoke
    global store_fanins_faninNBs_locally
    global create_all_fanins_faninNBs_on_start
    global using_workers
    global using_threads_not_processes
    global num_workers
    global use_multithreaded_multiprocessing
    global num_threads_for_multithreaded_multiprocessing
    global FanIn_Type
    global FanInNB_Type
    global process_work_queue_Type
	
    run_all_tasks_locally = True 
    store_fanins_faninNBs_locally = True 
    create_all_fanins_faninNBs_on_start = True
    using_workers = False
    using_threads_not_processes = True
    num_workers = 1
    use_multithreaded_multiprocessing = False
    num_threads_for_multithreaded_multiprocessing = 1

    FanIn_Type = "DAG_executor_FanIn"
    FanInNB_Type = "DAG_executor_FanInNB"
    process_work_queue_Type = "BoundedBuffer"
    #FanIn_Type = "DAG_executor_FanIn_Select"
    #FanInNB_Type = "DAG_executor_FanInNB_Select"
    #process_work_queue_Type = "BoundedBuffer_Select"

#Test 4: simulated lambdas (A2) with selective-wait Sync-objects,
#         create objects on-the-fly
def test4():
    print("test4")
    global store_sync_objects_in_lambdas
    global using_Lambda_Function_Simulators_to_Store_Objects
    global sync_objects_in_lambdas_trigger_their_tasks
    global using_DAG_orchestrator
    global map_objects_to_lambda_functions
    global use_anonymous_lambda_functions
    global using_single_lambda_function
    global compute_pagerank
    global check_pagerank_output
    global number_of_pagerank_iterations_for_partitions_groups_with_loops 
    global name_of_first_groupOrpartition_in_DAG
    global same_output_for_all_fanout_fanin
    global use_incremental_DAG_generation
    global incremental_DAG_deposit_interval
    global work_queue_size_for_incremental_DAG_generation_with_worker_processes
    global tasks_use_result_dictionary_parameter
    global use_shared_partitions_groups
    global use_page_rank_group_partitions
    global use_struct_of_arrays_for_pagerank
    global run_all_tasks_locally
    global bypass_call_lambda_client_invoke
    global store_fanins_faninNBs_locally
    global create_all_fanins_faninNBs_on_start
    global using_workers
    global using_threads_not_processes
    global num_workers
    global use_multithreaded_multiprocessing
    global num_threads_for_multithreaded_multiprocessing
    global FanIn_Type
    global FanInNB_Type
    global process_work_queue_Type

    run_all_tasks_locally = True 
    store_fanins_faninNBs_locally = True 
    create_all_fanins_faninNBs_on_start = True
    using_workers = False
    using_threads_not_processes = True
    num_workers = 1
    use_multithreaded_multiprocessing = False
    num_threads_for_multithreaded_multiprocessing = 1

    #FanIn_Type = "DAG_executor_FanIn"
    #FanInNB_Type = "DAG_executor_FanInNB"
    #process_work_queue_Type = "BoundedBuffer"
    FanIn_Type = "DAG_executor_FanIn_Select"
    FanInNB_Type = "DAG_executor_FanInNB_Select"
    process_work_queue_Type = "BoundedBuffer_Select"

# local objects

#Test 5: worker threads (A2) with non-selective-wait Sync-objects, 
#       1 worker, Sync-objects stored locally, create objects at the start
def test5():
    print("test5")
    global store_sync_objects_in_lambdas
    global using_Lambda_Function_Simulators_to_Store_Objects
    global sync_objects_in_lambdas_trigger_their_tasks
    global using_DAG_orchestrator
    global map_objects_to_lambda_functions
    global use_anonymous_lambda_functions
    global using_single_lambda_function
    global compute_pagerank
    global check_pagerank_output
    global number_of_pagerank_iterations_for_partitions_groups_with_loops 
    global name_of_first_groupOrpartition_in_DAG
    global same_output_for_all_fanout_fanin
    global use_incremental_DAG_generation
    global incremental_DAG_deposit_interval
    global work_queue_size_for_incremental_DAG_generation_with_worker_processes
    global tasks_use_result_dictionary_parameter
    global use_shared_partitions_groups
    global use_page_rank_group_partitions
    global use_struct_of_arrays_for_pagerank
    global run_all_tasks_locally
    global bypass_call_lambda_client_invoke
    global store_fanins_faninNBs_locally
    global create_all_fanins_faninNBs_on_start
    global using_workers
    global using_threads_not_processes
    global num_workers
    global use_multithreaded_multiprocessing
    global num_threads_for_multithreaded_multiprocessing
    global FanIn_Type
    global FanInNB_Type
    global process_work_queue_Type

    run_all_tasks_locally = True 
    store_fanins_faninNBs_locally = True 
    create_all_fanins_faninNBs_on_start = True
    using_workers = True
    using_threads_not_processes = True
    num_workers = 1
    use_multithreaded_multiprocessing = False
    num_threads_for_multithreaded_multiprocessing = 1

    FanIn_Type = "DAG_executor_FanIn"
    FanInNB_Type = "DAG_executor_FanInNB"
    process_work_queue_Type = "BoundedBuffer"
    #FanIn_Type = "DAG_executor_FanIn_Select"
    #FanInNB_Type = "DAG_executor_FanInNB_Select"
    #process_work_queue_Type = "BoundedBuffer_Select"

#Test 6: worker threads (A2) with non-selective-wait Sync-objects, 
#       2 workers, Sync-objects stored locally, create objects at the start
def test6():
    print("test6")
    global store_sync_objects_in_lambdas
    global using_Lambda_Function_Simulators_to_Store_Objects
    global sync_objects_in_lambdas_trigger_their_tasks
    global using_DAG_orchestrator
    global map_objects_to_lambda_functions
    global use_anonymous_lambda_functions
    global using_single_lambda_function
    global compute_pagerank
    global check_pagerank_output
    global number_of_pagerank_iterations_for_partitions_groups_with_loops 
    global name_of_first_groupOrpartition_in_DAG
    global same_output_for_all_fanout_fanin
    global use_incremental_DAG_generation
    global incremental_DAG_deposit_interval
    global work_queue_size_for_incremental_DAG_generation_with_worker_processes
    global tasks_use_result_dictionary_parameter
    global use_shared_partitions_groups
    global use_page_rank_group_partitions
    global use_struct_of_arrays_for_pagerank
    global run_all_tasks_locally
    global bypass_call_lambda_client_invoke
    global store_fanins_faninNBs_locally
    global create_all_fanins_faninNBs_on_start
    global using_workers
    global using_threads_not_processes
    global num_workers
    global use_multithreaded_multiprocessing
    global num_threads_for_multithreaded_multiprocessing
    global FanIn_Type
    global FanInNB_Type
    global process_work_queue_Type

    run_all_tasks_locally = True 
    store_fanins_faninNBs_locally = True 
    create_all_fanins_faninNBs_on_start = True
    using_workers = True
    using_threads_not_processes = True
    num_workers = 2
    use_multithreaded_multiprocessing = False
    num_threads_for_multithreaded_multiprocessing = 1

    FanIn_Type = "DAG_executor_FanIn"
    FanInNB_Type = "DAG_executor_FanInNB"
    process_work_queue_Type = "BoundedBuffer"
    #FanIn_Type = "DAG_executor_FanIn_Select"
    #FanInNB_Type = "DAG_executor_FanInNB_Select"
    #process_work_queue_Type = "BoundedBuffer_Select"

#Test 7: worker threads (A2) with selective-wait Sync-objects, 
#       2 workers, Sync-objects stored locally, create objects at the start
def test7():
    print("test7")
    global store_sync_objects_in_lambdas
    global using_Lambda_Function_Simulators_to_Store_Objects
    global sync_objects_in_lambdas_trigger_their_tasks
    global using_DAG_orchestrator
    global map_objects_to_lambda_functions
    global use_anonymous_lambda_functions
    global using_single_lambda_function
    global compute_pagerank
    global check_pagerank_output
    global number_of_pagerank_iterations_for_partitions_groups_with_loops 
    global name_of_first_groupOrpartition_in_DAG
    global same_output_for_all_fanout_fanin
    global use_incremental_DAG_generation
    global incremental_DAG_deposit_interval
    global work_queue_size_for_incremental_DAG_generation_with_worker_processes
    global tasks_use_result_dictionary_parameter
    global use_shared_partitions_groups
    global use_page_rank_group_partitions
    global use_struct_of_arrays_for_pagerank
    global run_all_tasks_locally
    global bypass_call_lambda_client_invoke
    global store_fanins_faninNBs_locally
    global create_all_fanins_faninNBs_on_start
    global using_workers
    global using_threads_not_processes
    global num_workers
    global use_multithreaded_multiprocessing
    global num_threads_for_multithreaded_multiprocessing
    global FanIn_Type
    global FanInNB_Type
    global process_work_queue_Type

    run_all_tasks_locally = True 
    store_fanins_faninNBs_locally = True 
    create_all_fanins_faninNBs_on_start = True
    using_workers = True
    using_threads_not_processes = True
    num_workers = 2
    use_multithreaded_multiprocessing = False
    num_threads_for_multithreaded_multiprocessing = 1

    #FanIn_Type = "DAG_executor_FanIn"
    #FanInNB_Type = "DAG_executor_FanInNB"
    #process_work_queue_Type = "BoundedBuffer"
    FanIn_Type = "DAG_executor_FanIn_Select"
    FanInNB_Type = "DAG_executor_FanInNB_Select"
    process_work_queue_Type = "BoundedBuffer_Select"

#Test 8: worker threads (A2) with non-selective-wait Sync-objects, 
#       2 workers, Sync-objects stored locally, create objects on-the-fly
def test8():
    print("test8")
    global store_sync_objects_in_lambdas
    global using_Lambda_Function_Simulators_to_Store_Objects
    global sync_objects_in_lambdas_trigger_their_tasks
    global using_DAG_orchestrator
    global map_objects_to_lambda_functions
    global use_anonymous_lambda_functions
    global using_single_lambda_function
    global compute_pagerank
    global check_pagerank_output
    global number_of_pagerank_iterations_for_partitions_groups_with_loops 
    global name_of_first_groupOrpartition_in_DAG
    global same_output_for_all_fanout_fanin
    global use_incremental_DAG_generation
    global incremental_DAG_deposit_interval
    global work_queue_size_for_incremental_DAG_generation_with_worker_processes
    global tasks_use_result_dictionary_parameter
    global use_shared_partitions_groups
    global use_page_rank_group_partitions
    global use_struct_of_arrays_for_pagerank
    global run_all_tasks_locally
    global bypass_call_lambda_client_invoke
    global store_fanins_faninNBs_locally
    global create_all_fanins_faninNBs_on_start
    global using_workers
    global using_threads_not_processes
    global num_workers
    global use_multithreaded_multiprocessing
    global num_threads_for_multithreaded_multiprocessing
    global FanIn_Type
    global FanInNB_Type
    global process_work_queue_Type

    run_all_tasks_locally = True 
    store_fanins_faninNBs_locally = True 
    create_all_fanins_faninNBs_on_start = False
    using_workers = True
    using_threads_not_processes = True
    num_workers = 2
    use_multithreaded_multiprocessing = False
    num_threads_for_multithreaded_multiprocessing = 1

    FanIn_Type = "DAG_executor_FanIn"
    FanInNB_Type = "DAG_executor_FanInNB"
    process_work_queue_Type = "BoundedBuffer"
    #FanIn_Type = "DAG_executor_FanIn_Select"
    #FanInNB_Type = "DAG_executor_FanInNB_Select"
    #process_work_queue_Type = "BoundedBuffer_Select"

#Test 9: worker threads (A2) with selective-wait Sync-objects, 
#       2 workers, Sync-objects stored locally, create objects on-the-fly
def test9():
    print("test9")
    global store_sync_objects_in_lambdas
    global using_Lambda_Function_Simulators_to_Store_Objects
    global sync_objects_in_lambdas_trigger_their_tasks
    global using_DAG_orchestrator
    global map_objects_to_lambda_functions
    global use_anonymous_lambda_functions
    global using_single_lambda_function
    global compute_pagerank
    global check_pagerank_output
    global number_of_pagerank_iterations_for_partitions_groups_with_loops 
    global name_of_first_groupOrpartition_in_DAG
    global same_output_for_all_fanout_fanin
    global use_incremental_DAG_generation
    global incremental_DAG_deposit_interval
    global work_queue_size_for_incremental_DAG_generation_with_worker_processes
    global tasks_use_result_dictionary_parameter
    global use_shared_partitions_groups
    global use_page_rank_group_partitions
    global use_struct_of_arrays_for_pagerank
    global run_all_tasks_locally
    global bypass_call_lambda_client_invoke
    global store_fanins_faninNBs_locally
    global create_all_fanins_faninNBs_on_start
    global using_workers
    global using_threads_not_processes
    global num_workers
    global use_multithreaded_multiprocessing
    global num_threads_for_multithreaded_multiprocessing
    global FanIn_Type
    global FanInNB_Type
    global process_work_queue_Type

    run_all_tasks_locally = True 
    store_fanins_faninNBs_locally = True 
    create_all_fanins_faninNBs_on_start = False
    using_workers = True
    using_threads_not_processes = True
    num_workers = 2
    use_multithreaded_multiprocessing = False
    num_threads_for_multithreaded_multiprocessing = 1

    #FanIn_Type = "DAG_executor_FanIn"
    #FanInNB_Type = "DAG_executor_FanInNB"
    #process_work_queue_Type = "BoundedBuffer"
    FanIn_Type = "DAG_executor_FanIn_Select"
    FanInNB_Type = "DAG_executor_FanInNB_Select"
    process_work_queue_Type = "BoundedBuffer_Select"


# remote objects with worker threads

#Test 10: worker threads (A2) with non-selective-wait Sync-objects, 
#        1 worker, Sync-objects stored remotely (on tcp_server)
#        create objects at the start
# Note: tcp_server must be running
def test10():
    print("test10")
    global store_sync_objects_in_lambdas
    global using_Lambda_Function_Simulators_to_Store_Objects
    global sync_objects_in_lambdas_trigger_their_tasks
    global using_DAG_orchestrator
    global map_objects_to_lambda_functions
    global use_anonymous_lambda_functions
    global using_single_lambda_function
    global compute_pagerank
    global check_pagerank_output
    global number_of_pagerank_iterations_for_partitions_groups_with_loops 
    global name_of_first_groupOrpartition_in_DAG
    global same_output_for_all_fanout_fanin
    global use_incremental_DAG_generation
    global incremental_DAG_deposit_interval
    global work_queue_size_for_incremental_DAG_generation_with_worker_processes
    global tasks_use_result_dictionary_parameter
    global use_shared_partitions_groups
    global use_page_rank_group_partitions
    global use_struct_of_arrays_for_pagerank
    global run_all_tasks_locally
    global bypass_call_lambda_client_invoke
    global store_fanins_faninNBs_locally
    global create_all_fanins_faninNBs_on_start
    global using_workers
    global using_threads_not_processes
    global num_workers
    global use_multithreaded_multiprocessing
    global num_threads_for_multithreaded_multiprocessing
    global FanIn_Type
    global FanInNB_Type
    global process_work_queue_Type

    run_all_tasks_locally = True 
    store_fanins_faninNBs_locally = False 
    create_all_fanins_faninNBs_on_start = True
    using_workers = True
    using_threads_not_processes = True
    num_workers = 1
    use_multithreaded_multiprocessing = False
    num_threads_for_multithreaded_multiprocessing = 1

    FanIn_Type = "DAG_executor_FanIn"
    FanInNB_Type = "DAG_executor_FanInNB"
    process_work_queue_Type = "BoundedBuffer"
    #FanIn_Type = "DAG_executor_FanIn_Select"
    #FanInNB_Type = "DAG_executor_FanInNB_Select"
    #process_work_queue_Type = "BoundedBuffer_Select"

#Test 11: worker threads (A2) with non-selective-wait Sync-objects, 
#        2 workers, Sync-objects stored remotely (on tcp_server)
#        create objects at the start
# Note: tcp_server must be running
def test11():
    print("test11")
    global store_sync_objects_in_lambdas
    global using_Lambda_Function_Simulators_to_Store_Objects
    global sync_objects_in_lambdas_trigger_their_tasks
    global using_DAG_orchestrator
    global map_objects_to_lambda_functions
    global use_anonymous_lambda_functions
    global using_single_lambda_function
    global compute_pagerank
    global check_pagerank_output
    global number_of_pagerank_iterations_for_partitions_groups_with_loops 
    global name_of_first_groupOrpartition_in_DAG
    global same_output_for_all_fanout_fanin
    global use_incremental_DAG_generation
    global incremental_DAG_deposit_interval
    global work_queue_size_for_incremental_DAG_generation_with_worker_processes
    global tasks_use_result_dictionary_parameter
    global use_shared_partitions_groups
    global use_page_rank_group_partitions
    global use_struct_of_arrays_for_pagerank
    global run_all_tasks_locally
    global bypass_call_lambda_client_invoke
    global store_fanins_faninNBs_locally
    global create_all_fanins_faninNBs_on_start
    global using_workers
    global using_threads_not_processes
    global num_workers
    global use_multithreaded_multiprocessing
    global num_threads_for_multithreaded_multiprocessing
    global FanIn_Type
    global FanInNB_Type
    global process_work_queue_Type

    run_all_tasks_locally = True 
    store_fanins_faninNBs_locally = False 
    create_all_fanins_faninNBs_on_start = True
    using_workers = True
    using_threads_not_processes = True
    num_workers = 2
    use_multithreaded_multiprocessing = False
    num_threads_for_multithreaded_multiprocessing = 1

    FanIn_Type = "DAG_executor_FanIn"
    FanInNB_Type = "DAG_executor_FanInNB"
    process_work_queue_Type = "BoundedBuffer"
    #FanIn_Type = "DAG_executor_FanIn_Select"
    #FanInNB_Type = "DAG_executor_FanInNB_Select"
    #process_work_queue_Type = "BoundedBuffer_Select"

#Test 12: worker threads (A2) with non-selective-wait Sync-objects, 
#        2 workers, Sync-objects stored remotely (on tcp_server)
#        create objects on-the-fly
# Note: tcp_server must be running
def test12():
    print("test12")
    global store_sync_objects_in_lambdas
    global using_Lambda_Function_Simulators_to_Store_Objects
    global sync_objects_in_lambdas_trigger_their_tasks
    global using_DAG_orchestrator
    global map_objects_to_lambda_functions
    global use_anonymous_lambda_functions
    global using_single_lambda_function
    global compute_pagerank
    global check_pagerank_output
    global number_of_pagerank_iterations_for_partitions_groups_with_loops 
    global name_of_first_groupOrpartition_in_DAG
    global same_output_for_all_fanout_fanin
    global use_incremental_DAG_generation
    global incremental_DAG_deposit_interval
    global work_queue_size_for_incremental_DAG_generation_with_worker_processes
    global tasks_use_result_dictionary_parameter
    global use_shared_partitions_groups
    global use_page_rank_group_partitions
    global use_struct_of_arrays_for_pagerank
    global run_all_tasks_locally
    global bypass_call_lambda_client_invoke
    global store_fanins_faninNBs_locally
    global create_all_fanins_faninNBs_on_start
    global using_workers
    global using_threads_not_processes
    global num_workers
    global use_multithreaded_multiprocessing
    global num_threads_for_multithreaded_multiprocessing
    global FanIn_Type
    global FanInNB_Type
    global process_work_queue_Type

    run_all_tasks_locally = True 
    store_fanins_faninNBs_locally = False 
    create_all_fanins_faninNBs_on_start = False
    using_workers = True
    using_threads_not_processes = True
    num_workers = 2
    use_multithreaded_multiprocessing = False
    num_threads_for_multithreaded_multiprocessing = 1

    FanIn_Type = "DAG_executor_FanIn"
    FanInNB_Type = "DAG_executor_FanInNB"
    process_work_queue_Type = "BoundedBuffer"
    #FanIn_Type = "DAG_executor_FanIn_Select"
    #FanInNB_Type = "DAG_executor_FanInNB_Select"
    #process_work_queue_Type = "BoundedBuffer_Select"

#Test 13: worker threads (A2) with selective-wait Sync-objects, 
#        2 workers, Sync-objects stored remotely (on tcp_server)
#        create objects at the start
# Note: tcp_server must be running
def test13():
    print("test13")
    global store_sync_objects_in_lambdas
    global using_Lambda_Function_Simulators_to_Store_Objects
    global sync_objects_in_lambdas_trigger_their_tasks
    global using_DAG_orchestrator
    global map_objects_to_lambda_functions
    global use_anonymous_lambda_functions
    global using_single_lambda_function
    global compute_pagerank
    global check_pagerank_output
    global number_of_pagerank_iterations_for_partitions_groups_with_loops 
    global name_of_first_groupOrpartition_in_DAG
    global same_output_for_all_fanout_fanin
    global use_incremental_DAG_generation
    global incremental_DAG_deposit_interval
    global work_queue_size_for_incremental_DAG_generation_with_worker_processes
    global tasks_use_result_dictionary_parameter
    global use_shared_partitions_groups
    global use_page_rank_group_partitions
    global use_struct_of_arrays_for_pagerank
    global run_all_tasks_locally
    global bypass_call_lambda_client_invoke
    global store_fanins_faninNBs_locally
    global create_all_fanins_faninNBs_on_start
    global using_workers
    global using_threads_not_processes
    global num_workers
    global use_multithreaded_multiprocessing
    global num_threads_for_multithreaded_multiprocessing
    global FanIn_Type
    global FanInNB_Type
    global process_work_queue_Type

    run_all_tasks_locally = True 
    store_fanins_faninNBs_locally = False 
    create_all_fanins_faninNBs_on_start = True
    using_workers = True
    using_threads_not_processes = True
    num_workers = 2
    use_multithreaded_multiprocessing = False
    num_threads_for_multithreaded_multiprocessing = 1

    #FanIn_Type = "DAG_executor_FanIn"
    #FanInNB_Type = "DAG_executor_FanInNB"
    #process_work_queue_Type = "BoundedBuffer"
    FanIn_Type = "DAG_executor_FanIn_Select"
    FanInNB_Type = "DAG_executor_FanInNB_Select"
    process_work_queue_Type = "BoundedBuffer_Select"

#Test 14: worker threads (A2) with selective-wait Sync-objects, 
#        2 workers, Sync-objects stored remotely (on tcp_server)
#        create objects on-the-fly
# Note: tcp_server must be running
def test14():
    print("test14")
    global store_sync_objects_in_lambdas
    global using_Lambda_Function_Simulators_to_Store_Objects
    global sync_objects_in_lambdas_trigger_their_tasks
    global using_DAG_orchestrator
    global map_objects_to_lambda_functions
    global use_anonymous_lambda_functions
    global using_single_lambda_function
    global compute_pagerank
    global check_pagerank_output
    global number_of_pagerank_iterations_for_partitions_groups_with_loops 
    global name_of_first_groupOrpartition_in_DAG
    global same_output_for_all_fanout_fanin
    global use_incremental_DAG_generation
    global incremental_DAG_deposit_interval
    global work_queue_size_for_incremental_DAG_generation_with_worker_processes
    global tasks_use_result_dictionary_parameter
    global use_shared_partitions_groups
    global use_page_rank_group_partitions
    global use_struct_of_arrays_for_pagerank
    global run_all_tasks_locally
    global bypass_call_lambda_client_invoke
    global store_fanins_faninNBs_locally
    global create_all_fanins_faninNBs_on_start
    global using_workers
    global using_threads_not_processes
    global num_workers
    global use_multithreaded_multiprocessing
    global num_threads_for_multithreaded_multiprocessing
    global FanIn_Type
    global FanInNB_Type
    global process_work_queue_Type

    run_all_tasks_locally = True 
    store_fanins_faninNBs_locally = False 
    create_all_fanins_faninNBs_on_start = False
    using_workers = True
    using_threads_not_processes = True
    num_workers = 2
    use_multithreaded_multiprocessing = False
    num_threads_for_multithreaded_multiprocessing = 1

    #FanIn_Type = "DAG_executor_FanIn"
    #FanInNB_Type = "DAG_executor_FanInNB"
    #process_work_queue_Type = "BoundedBuffer"
    FanIn_Type = "DAG_executor_FanIn_Select"
    FanInNB_Type = "DAG_executor_FanInNB_Select"
    process_work_queue_Type = "BoundedBuffer_Select"

# remote objects with worker processes

#Test 15: worker processes (A2) with non-selective-wait Sync-objects, 
#        1 worker, Sync-objects stored remotely (on tcp_server)
#        create objects at the start
# Note: tcp_server must be running
def test15():
    print("test15")
    global store_sync_objects_in_lambdas
    global using_Lambda_Function_Simulators_to_Store_Objects
    global sync_objects_in_lambdas_trigger_their_tasks
    global using_DAG_orchestrator
    global map_objects_to_lambda_functions
    global use_anonymous_lambda_functions
    global using_single_lambda_function
    global compute_pagerank
    global check_pagerank_output
    global number_of_pagerank_iterations_for_partitions_groups_with_loops 
    global name_of_first_groupOrpartition_in_DAG
    global same_output_for_all_fanout_fanin
    global use_incremental_DAG_generation
    global incremental_DAG_deposit_interval
    global work_queue_size_for_incremental_DAG_generation_with_worker_processes
    global tasks_use_result_dictionary_parameter
    global use_shared_partitions_groups
    global use_page_rank_group_partitions
    global use_struct_of_arrays_for_pagerank
    global run_all_tasks_locally
    global bypass_call_lambda_client_invoke
    global store_fanins_faninNBs_locally
    global create_all_fanins_faninNBs_on_start
    global using_workers
    global using_threads_not_processes
    global num_workers
    global use_multithreaded_multiprocessing
    global num_threads_for_multithreaded_multiprocessing
    global FanIn_Type
    global FanInNB_Type
    global process_work_queue_Type

    run_all_tasks_locally = True 
    store_fanins_faninNBs_locally = False 
    create_all_fanins_faninNBs_on_start = True
    using_workers = True
    using_threads_not_processes = False
    num_workers = 1
    use_multithreaded_multiprocessing = False
    num_threads_for_multithreaded_multiprocessing = 1

    FanIn_Type = "DAG_executor_FanIn"
    FanInNB_Type = "DAG_executor_FanInNB"
    process_work_queue_Type = "BoundedBuffer"
    #FanIn_Type = "DAG_executor_FanIn_Select"
    #FanInNB_Type = "DAG_executor_FanInNB_Select"
    #process_work_queue_Type = "BoundedBuffer_Select"

#Test 16: worker processes (A2) with non-selective-wait Sync-objects, 
#        2 workers, Sync-objects stored remotely (on tcp_server)
#        create objects at the start
# Note: tcp_server must be running
def test16():
    print("test16")
    global store_sync_objects_in_lambdas
    global using_Lambda_Function_Simulators_to_Store_Objects
    global sync_objects_in_lambdas_trigger_their_tasks
    global using_DAG_orchestrator
    global map_objects_to_lambda_functions
    global use_anonymous_lambda_functions
    global using_single_lambda_function
    global compute_pagerank
    global check_pagerank_output
    global number_of_pagerank_iterations_for_partitions_groups_with_loops 
    global name_of_first_groupOrpartition_in_DAG
    global same_output_for_all_fanout_fanin
    global use_incremental_DAG_generation
    global incremental_DAG_deposit_interval
    global work_queue_size_for_incremental_DAG_generation_with_worker_processes
    global tasks_use_result_dictionary_parameter
    global use_shared_partitions_groups
    global use_page_rank_group_partitions
    global use_struct_of_arrays_for_pagerank
    global run_all_tasks_locally
    global bypass_call_lambda_client_invoke
    global store_fanins_faninNBs_locally
    global create_all_fanins_faninNBs_on_start
    global using_workers
    global using_threads_not_processes
    global num_workers
    global use_multithreaded_multiprocessing
    global num_threads_for_multithreaded_multiprocessing
    global FanIn_Type
    global FanInNB_Type
    global process_work_queue_Type

    run_all_tasks_locally = True 
    store_fanins_faninNBs_locally = False 
    create_all_fanins_faninNBs_on_start = True
    using_workers = True
    using_threads_not_processes = False
    num_workers = 2
    use_multithreaded_multiprocessing = False
    num_threads_for_multithreaded_multiprocessing = 1

    FanIn_Type = "DAG_executor_FanIn"
    FanInNB_Type = "DAG_executor_FanInNB"
    process_work_queue_Type = "BoundedBuffer"
    #FanIn_Type = "DAG_executor_FanIn_Select"
    #FanInNB_Type = "DAG_executor_FanInNB_Select"
    #process_work_queue_Type = "BoundedBuffer_Select"

#Test 17: worker processes (A2) with non-selective-wait Sync-objects, 
#        2 workers, Sync-objects stored remotely (on tcp_server)
#        create objects on-the-fly
# Note: tcp_server must be running
def test17():
    print("test17")
    global store_sync_objects_in_lambdas
    global using_Lambda_Function_Simulators_to_Store_Objects
    global sync_objects_in_lambdas_trigger_their_tasks
    global using_DAG_orchestrator
    global map_objects_to_lambda_functions
    global use_anonymous_lambda_functions
    global using_single_lambda_function
    global compute_pagerank
    global check_pagerank_output
    global number_of_pagerank_iterations_for_partitions_groups_with_loops 
    global name_of_first_groupOrpartition_in_DAG
    global same_output_for_all_fanout_fanin
    global use_incremental_DAG_generation
    global incremental_DAG_deposit_interval
    global work_queue_size_for_incremental_DAG_generation_with_worker_processes
    global tasks_use_result_dictionary_parameter
    global use_shared_partitions_groups
    global use_page_rank_group_partitions
    global use_struct_of_arrays_for_pagerank
    global run_all_tasks_locally
    global bypass_call_lambda_client_invoke
    global store_fanins_faninNBs_locally
    global create_all_fanins_faninNBs_on_start
    global using_workers
    global using_threads_not_processes
    global num_workers
    global use_multithreaded_multiprocessing
    global num_threads_for_multithreaded_multiprocessing
    global FanIn_Type
    global FanInNB_Type
    global process_work_queue_Type

    run_all_tasks_locally = True 
    store_fanins_faninNBs_locally = False 
    create_all_fanins_faninNBs_on_start = False
    using_workers = True
    using_threads_not_processes = False
    num_workers = 2
    use_multithreaded_multiprocessing = False
    num_threads_for_multithreaded_multiprocessing = 1

    FanIn_Type = "DAG_executor_FanIn"
    FanInNB_Type = "DAG_executor_FanInNB"
    process_work_queue_Type = "BoundedBuffer"
    #FanIn_Type = "DAG_executor_FanIn_Select"
    #FanInNB_Type = "DAG_executor_FanInNB_Select"
    #process_work_queue_Type = "BoundedBuffer_Select"

#Test 18: worker processes (A2) with selective-wait Sync-objects, 
#        2 workers, Sync-objects stored remotely (on tcp_server)
#        create objects at the start
# Note: tcp_server must be running
def test18():
    print("test18")
    global store_sync_objects_in_lambdas
    global using_Lambda_Function_Simulators_to_Store_Objects
    global sync_objects_in_lambdas_trigger_their_tasks
    global using_DAG_orchestrator
    global map_objects_to_lambda_functions
    global use_anonymous_lambda_functions
    global using_single_lambda_function
    global compute_pagerank
    global check_pagerank_output
    global number_of_pagerank_iterations_for_partitions_groups_with_loops 
    global name_of_first_groupOrpartition_in_DAG
    global same_output_for_all_fanout_fanin
    global use_incremental_DAG_generation
    global incremental_DAG_deposit_interval
    global work_queue_size_for_incremental_DAG_generation_with_worker_processes
    global tasks_use_result_dictionary_parameter
    global use_shared_partitions_groups
    global use_page_rank_group_partitions
    global use_struct_of_arrays_for_pagerank
    global run_all_tasks_locally
    global bypass_call_lambda_client_invoke
    global store_fanins_faninNBs_locally
    global create_all_fanins_faninNBs_on_start
    global using_workers
    global using_threads_not_processes
    global num_workers
    global use_multithreaded_multiprocessing
    global num_threads_for_multithreaded_multiprocessing
    global FanIn_Type
    global FanInNB_Type
    global process_work_queue_Type

    run_all_tasks_locally = True 
    store_fanins_faninNBs_locally = False 
    create_all_fanins_faninNBs_on_start = True
    using_workers = True
    using_threads_not_processes = False
    num_workers = 2
    use_multithreaded_multiprocessing = False
    num_threads_for_multithreaded_multiprocessing = 1

    #FanIn_Type = "DAG_executor_FanIn"
    #FanInNB_Type = "DAG_executor_FanInNB"
    #process_work_queue_Type = "BoundedBuffer"
    FanIn_Type = "DAG_executor_FanIn_Select"
    FanInNB_Type = "DAG_executor_FanInNB_Select"
    process_work_queue_Type = "BoundedBuffer_Select"

#Test 19: worker processes (A2) with selective-wait Sync-objects, 
#        2 workers, Sync-objects stored remotely (on tcp_server)
#        create objects on-the-fly
# Note: tcp_server must be running
def test19():
    print("test19")
    global store_sync_objects_in_lambdas
    global using_Lambda_Function_Simulators_to_Store_Objects
    global sync_objects_in_lambdas_trigger_their_tasks
    global using_DAG_orchestrator
    global map_objects_to_lambda_functions
    global use_anonymous_lambda_functions
    global using_single_lambda_function
    global compute_pagerank
    global check_pagerank_output
    global number_of_pagerank_iterations_for_partitions_groups_with_loops 
    global name_of_first_groupOrpartition_in_DAG
    global same_output_for_all_fanout_fanin
    global use_incremental_DAG_generation
    global incremental_DAG_deposit_interval
    global work_queue_size_for_incremental_DAG_generation_with_worker_processes
    global tasks_use_result_dictionary_parameter
    global use_shared_partitions_groups
    global use_page_rank_group_partitions
    global use_struct_of_arrays_for_pagerank
    global run_all_tasks_locally
    global bypass_call_lambda_client_invoke
    global store_fanins_faninNBs_locally
    global create_all_fanins_faninNBs_on_start
    global using_workers
    global using_threads_not_processes
    global num_workers
    global use_multithreaded_multiprocessing
    global num_threads_for_multithreaded_multiprocessing
    global FanIn_Type
    global FanInNB_Type
    global process_work_queue_Type

    run_all_tasks_locally = True 
    store_fanins_faninNBs_locally = False 
    create_all_fanins_faninNBs_on_start = False
    using_workers = True
    using_threads_not_processes = False
    num_workers = 2
    use_multithreaded_multiprocessing = False
    num_threads_for_multithreaded_multiprocessing = 1

    #FanIn_Type = "DAG_executor_FanIn"
    #FanInNB_Type = "DAG_executor_FanInNB"
    #process_work_queue_Type = "BoundedBuffer"
    FanIn_Type = "DAG_executor_FanIn_Select"
    FanInNB_Type = "DAG_executor_FanInNB_Select"
    process_work_queue_Type = "BoundedBuffer_Select"

#Test 20: multithreaded worker processes (A2) with non-selective-wait Sync-objects, 
#        1 thread for the worker process, Sync-objects stored remotely (on tcp_server)
#        create objects at the start
# Note: tcp_server must be running
def test20():
    print("test20")
    global store_sync_objects_in_lambdas
    global using_Lambda_Function_Simulators_to_Store_Objects
    global sync_objects_in_lambdas_trigger_their_tasks
    global using_DAG_orchestrator
    global map_objects_to_lambda_functions
    global use_anonymous_lambda_functions
    global using_single_lambda_function
    global compute_pagerank
    global check_pagerank_output
    global number_of_pagerank_iterations_for_partitions_groups_with_loops 
    global name_of_first_groupOrpartition_in_DAG
    global same_output_for_all_fanout_fanin
    global use_incremental_DAG_generation
    global incremental_DAG_deposit_interval
    global work_queue_size_for_incremental_DAG_generation_with_worker_processes
    global tasks_use_result_dictionary_parameter
    global use_shared_partitions_groups
    global use_page_rank_group_partitions
    global use_struct_of_arrays_for_pagerank
    global run_all_tasks_locally
    global bypass_call_lambda_client_invoke
    global store_fanins_faninNBs_locally
    global create_all_fanins_faninNBs_on_start
    global using_workers
    global using_threads_not_processes
    global num_workers
    global use_multithreaded_multiprocessing
    global num_threads_for_multithreaded_multiprocessing
    global FanIn_Type
    global FanInNB_Type
    global process_work_queue_Type

    run_all_tasks_locally = True 
    store_fanins_faninNBs_locally = False 
    create_all_fanins_faninNBs_on_start = True
    using_workers = True
    using_threads_not_processes = False
    num_workers = 1
    use_multithreaded_multiprocessing = True
    num_threads_for_multithreaded_multiprocessing = 1

    FanIn_Type = "DAG_executor_FanIn"
    FanInNB_Type = "DAG_executor_FanInNB"
    process_work_queue_Type = "BoundedBuffer"
    #FanIn_Type = "DAG_executor_FanIn_Select"
    #FanInNB_Type = "DAG_executor_FanInNB_Select"
    #process_work_queue_Type = "BoundedBuffer_Select"

#Test 21: multithreaded worker processes (A2) with non-selective-wait Sync-objects, 
#        2 threads for the worker process, Sync-objects stored remotely (on tcp_server)
#        create objects at the start
# Note: tcp_server must be running
def test21():
    print("test21")
    global store_sync_objects_in_lambdas
    global using_Lambda_Function_Simulators_to_Store_Objects
    global sync_objects_in_lambdas_trigger_their_tasks
    global using_DAG_orchestrator
    global map_objects_to_lambda_functions
    global use_anonymous_lambda_functions
    global using_single_lambda_function
    global compute_pagerank
    global check_pagerank_output
    global number_of_pagerank_iterations_for_partitions_groups_with_loops 
    global name_of_first_groupOrpartition_in_DAG
    global same_output_for_all_fanout_fanin
    global use_incremental_DAG_generation
    global incremental_DAG_deposit_interval
    global work_queue_size_for_incremental_DAG_generation_with_worker_processes
    global tasks_use_result_dictionary_parameter
    global use_shared_partitions_groups
    global use_page_rank_group_partitions
    global use_struct_of_arrays_for_pagerank
    global run_all_tasks_locally
    global bypass_call_lambda_client_invoke
    global store_fanins_faninNBs_locally
    global create_all_fanins_faninNBs_on_start
    global using_workers
    global using_threads_not_processes
    global num_workers
    global use_multithreaded_multiprocessing
    global num_threads_for_multithreaded_multiprocessing
    global FanIn_Type
    global FanInNB_Type
    global process_work_queue_Type

    run_all_tasks_locally = True 
    store_fanins_faninNBs_locally = False 
    create_all_fanins_faninNBs_on_start = True
    using_workers = True
    using_threads_not_processes = False
    num_workers = 1
    use_multithreaded_multiprocessing = True
    num_threads_for_multithreaded_multiprocessing = 2

    FanIn_Type = "DAG_executor_FanIn"
    FanInNB_Type = "DAG_executor_FanInNB"
    process_work_queue_Type = "BoundedBuffer"
    #FanIn_Type = "DAG_executor_FanIn_Select"
    #FanInNB_Type = "DAG_executor_FanInNB_Select"
    #process_work_queue_Type = "BoundedBuffer_Select"

#Test 22: multithreaded worker processes (A2) with non-selective-wait Sync-objects, 
#        2 threads for each of 2 worker process, Sync-objects stored remotely (on tcp_server)
#        create objects at the start
# Note: tcp_server must be running
def test22():
    print("test22")
    global store_sync_objects_in_lambdas
    global using_Lambda_Function_Simulators_to_Store_Objects
    global sync_objects_in_lambdas_trigger_their_tasks
    global using_DAG_orchestrator
    global map_objects_to_lambda_functions
    global use_anonymous_lambda_functions
    global using_single_lambda_function
    global compute_pagerank
    global check_pagerank_output
    global number_of_pagerank_iterations_for_partitions_groups_with_loops 
    global name_of_first_groupOrpartition_in_DAG
    global same_output_for_all_fanout_fanin
    global use_incremental_DAG_generation
    global incremental_DAG_deposit_interval
    global work_queue_size_for_incremental_DAG_generation_with_worker_processes
    global tasks_use_result_dictionary_parameter
    global use_shared_partitions_groups
    global use_page_rank_group_partitions
    global use_struct_of_arrays_for_pagerank
    global run_all_tasks_locally
    global bypass_call_lambda_client_invoke
    global store_fanins_faninNBs_locally
    global create_all_fanins_faninNBs_on_start
    global using_workers
    global using_threads_not_processes
    global num_workers
    global use_multithreaded_multiprocessing
    global num_threads_for_multithreaded_multiprocessing
    global FanIn_Type
    global FanInNB_Type
    global process_work_queue_Type

    run_all_tasks_locally = True 
    store_fanins_faninNBs_locally = False 
    create_all_fanins_faninNBs_on_start = True
    using_workers = True
    using_threads_not_processes = False
    num_workers = 2
    use_multithreaded_multiprocessing = True
    num_threads_for_multithreaded_multiprocessing = 2

    FanIn_Type = "DAG_executor_FanIn"
    FanInNB_Type = "DAG_executor_FanInNB"
    process_work_queue_Type = "BoundedBuffer"
    #FanIn_Type = "DAG_executor_FanIn_Select"
    #FanInNB_Type = "DAG_executor_FanInNB_Select"
    #process_work_queue_Type = "BoundedBuffer_Select"

#Test 23: multithreaded worker processes (A2) with selective-wait Sync-objects, 
#        2 threads for the worker process, Sync-objects stored remotely (on tcp_server)
#        create objects at the start
# Note: tcp_server must be running
def test23():
    print("test23")
    global store_sync_objects_in_lambdas
    global using_Lambda_Function_Simulators_to_Store_Objects
    global sync_objects_in_lambdas_trigger_their_tasks
    global using_DAG_orchestrator
    global map_objects_to_lambda_functions
    global use_anonymous_lambda_functions
    global using_single_lambda_function
    global compute_pagerank
    global check_pagerank_output
    global number_of_pagerank_iterations_for_partitions_groups_with_loops 
    global name_of_first_groupOrpartition_in_DAG
    global same_output_for_all_fanout_fanin
    global use_incremental_DAG_generation
    global incremental_DAG_deposit_interval
    global work_queue_size_for_incremental_DAG_generation_with_worker_processes
    global tasks_use_result_dictionary_parameter
    global use_shared_partitions_groups
    global use_page_rank_group_partitions
    global use_struct_of_arrays_for_pagerank
    global run_all_tasks_locally
    global bypass_call_lambda_client_invoke
    global store_fanins_faninNBs_locally
    global create_all_fanins_faninNBs_on_start
    global using_workers
    global using_threads_not_processes
    global num_workers
    global use_multithreaded_multiprocessing
    global num_threads_for_multithreaded_multiprocessing
    global FanIn_Type
    global FanInNB_Type
    global process_work_queue_Type

    run_all_tasks_locally = True 
    store_fanins_faninNBs_locally = False 
    create_all_fanins_faninNBs_on_start = True
    using_workers = True
    using_threads_not_processes = False
    num_workers = 1
    use_multithreaded_multiprocessing = True
    num_threads_for_multithreaded_multiprocessing = 2

    #FanIn_Type = "DAG_executor_FanIn"
    #FanInNB_Type = "DAG_executor_FanInNB"
    #process_work_queue_Type = "BoundedBuffer"
    FanIn_Type = "DAG_executor_FanIn_Select"
    FanInNB_Type = "DAG_executor_FanInNB_Select"
    process_work_queue_Type = "BoundedBuffer_Select"

#Test 24: multithreaded worker processes (A2) with selective-wait Sync-objects, 
#        2 threads for each of 2 worker process, Sync-objects stored remotely (on tcp_server)
#        create objects at the start
# Note: tcp_server must be running
def test24():
    print("test24")
    global store_sync_objects_in_lambdas
    global using_Lambda_Function_Simulators_to_Store_Objects
    global sync_objects_in_lambdas_trigger_their_tasks
    global using_DAG_orchestrator
    global map_objects_to_lambda_functions
    global use_anonymous_lambda_functions
    global using_single_lambda_function
    global compute_pagerank
    global check_pagerank_output
    global number_of_pagerank_iterations_for_partitions_groups_with_loops 
    global name_of_first_groupOrpartition_in_DAG
    global same_output_for_all_fanout_fanin
    global use_incremental_DAG_generation
    global incremental_DAG_deposit_interval
    global work_queue_size_for_incremental_DAG_generation_with_worker_processes
    global tasks_use_result_dictionary_parameter
    global use_shared_partitions_groups
    global use_page_rank_group_partitions
    global use_struct_of_arrays_for_pagerank
    global run_all_tasks_locally
    global bypass_call_lambda_client_invoke
    global store_fanins_faninNBs_locally
    global create_all_fanins_faninNBs_on_start
    global using_workers
    global using_threads_not_processes
    global num_workers
    global use_multithreaded_multiprocessing
    global num_threads_for_multithreaded_multiprocessing
    global FanIn_Type
    global FanInNB_Type
    global process_work_queue_Type

    run_all_tasks_locally = True 
    store_fanins_faninNBs_locally = False 
    create_all_fanins_faninNBs_on_start = True
    using_workers = True
    using_threads_not_processes = False
    num_workers = 2
    use_multithreaded_multiprocessing = True
    num_threads_for_multithreaded_multiprocessing = 2

    #FanIn_Type = "DAG_executor_FanIn"
    #FanInNB_Type = "DAG_executor_FanInNB"
    #process_work_queue_Type = "BoundedBuffer"
    FanIn_Type = "DAG_executor_FanIn_Select"
    FanInNB_Type = "DAG_executor_FanInNB_Select"
    process_work_queue_Type = "BoundedBuffer_Select"

#Test store_sync_objects_in_lambdas

#Test 25: multithreaded worker processes (A2) with selective-wait Sync-objects, 
#        2 threads for each of 2 worker process, Sync-objects stored remotely (on tcp_server)
#        create objects at the start
# Note: tcp_server must be running
def test25():
    print("test25")
    global store_sync_objects_in_lambdas
    global using_Lambda_Function_Simulators_to_Store_Objects
    global sync_objects_in_lambdas_trigger_their_tasks
    global using_DAG_orchestrator
    global map_objects_to_lambda_functions
    global use_anonymous_lambda_functions
    global using_single_lambda_function
    global compute_pagerank
    global check_pagerank_output
    global number_of_pagerank_iterations_for_partitions_groups_with_loops 
    global name_of_first_groupOrpartition_in_DAG
    global same_output_for_all_fanout_fanin
    global use_incremental_DAG_generation
    global incremental_DAG_deposit_interval
    global work_queue_size_for_incremental_DAG_generation_with_worker_processes
    global tasks_use_result_dictionary_parameter
    global use_shared_partitions_groups
    global use_page_rank_group_partitions
    global use_struct_of_arrays_for_pagerank
    global run_all_tasks_locally
    global bypass_call_lambda_client_invoke
    global store_fanins_faninNBs_locally
    global create_all_fanins_faninNBs_on_start
    global using_workers
    global using_threads_not_processes
    global num_workers
    global use_multithreaded_multiprocessing
    global num_threads_for_multithreaded_multiprocessing
    global FanIn_Type
    global FanInNB_Type
    global process_work_queue_Type

    run_all_tasks_locally = True 
    store_fanins_faninNBs_locally = False 
    create_all_fanins_faninNBs_on_start = True
    using_workers = True
    using_threads_not_processes = True
    num_workers = 2
    use_multithreaded_multiprocessing = False
    num_threads_for_multithreaded_multiprocessing = 1

    #FanIn_Type = "DAG_executor_FanIn"
    #FanInNB_Type = "DAG_executor_FanInNB"
    #process_work_queue_Type = "BoundedBuffer"
    FanIn_Type = "DAG_executor_FanIn_Select"
    FanInNB_Type = "DAG_executor_FanInNB_Select"
    process_work_queue_Type = "BoundedBuffer_Select"

# TTFFTFF: no trigger and no DAG_orchestrator, but map objects 
# (anon is false) and create objects on start and
# do not using_single_lambda_function

store_sync_objects_in_lambdas = True
using_Lambda_Function_Simulators_to_Store_Objects = True
sync_objects_in_lambdas_trigger_their_tasks = False
using_DAG_orchestrator = False
map_objects_to_lambda_functions = True
use_anonymous_lambda_functions = False
using_single_lambda_function = False

#Test 26: multithreaded worker processes (A2) with selective-wait Sync-objects, 
#        2 threads for each of 2 worker process, Sync-objects stored remotely (on tcp_server)
#        create objects at the start
#        Variation: a. change D_O to T, 
# Note: tcp_server must be running
def test26():
    print("test26")
    global store_sync_objects_in_lambdas
    global using_Lambda_Function_Simulators_to_Store_Objects
    global sync_objects_in_lambdas_trigger_their_tasks
    global using_DAG_orchestrator
    global map_objects_to_lambda_functions
    global use_anonymous_lambda_functions
    global using_single_lambda_function
    global compute_pagerank
    global check_pagerank_output
    global number_of_pagerank_iterations_for_partitions_groups_with_loops 
    global name_of_first_groupOrpartition_in_DAG
    global same_output_for_all_fanout_fanin
    global use_incremental_DAG_generation
    global incremental_DAG_deposit_interval
    global work_queue_size_for_incremental_DAG_generation_with_worker_processes
    global tasks_use_result_dictionary_parameter
    global use_shared_partitions_groups
    global use_page_rank_group_partitions
    global use_struct_of_arrays_for_pagerank
    global run_all_tasks_locally
    global bypass_call_lambda_client_invoke
    global store_fanins_faninNBs_locally
    global create_all_fanins_faninNBs_on_start
    global using_workers
    global using_threads_not_processes
    global num_workers
    global use_multithreaded_multiprocessing
    global num_threads_for_multithreaded_multiprocessing
    global FanIn_Type
    global FanInNB_Type
    global process_work_queue_Type

    run_all_tasks_locally = True 
    store_fanins_faninNBs_locally = False 
    create_all_fanins_faninNBs_on_start = True
    using_workers = True
    using_threads_not_processes = True
    num_workers = 2
    use_multithreaded_multiprocessing = False
    num_threads_for_multithreaded_multiprocessing = 1

    #FanIn_Type = "DAG_executor_FanIn"
    #FanInNB_Type = "DAG_executor_FanInNB"
    #process_work_queue_Type = "BoundedBuffer"
    FanIn_Type = "DAG_executor_FanIn_Select"
    FanInNB_Type = "DAG_executor_FanInNB_Select"
    process_work_queue_Type = "BoundedBuffer_Select"

    store_sync_objects_in_lambdas = True
    using_Lambda_Function_Simulators_to_Store_Objects = True
    sync_objects_in_lambdas_trigger_their_tasks = False
    using_DAG_orchestrator = True
    map_objects_to_lambda_functions = True
    use_anonymous_lambda_functions = False
    using_single_lambda_function = False

#Test 27: multithreaded worker processes (A2) with selective-wait Sync-objects, 
#        2 threads for each of 2 worker process, Sync-objects stored remotely (on tcp_server)
#        create objects at the start
#        Variation: a. change D_O to T, 
#                   b. change map to F, and anon to T:
# Note: tcp_server must be running
def test27():
    print("test27")
    global store_sync_objects_in_lambdas
    global using_Lambda_Function_Simulators_to_Store_Objects
    global sync_objects_in_lambdas_trigger_their_tasks
    global using_DAG_orchestrator
    global map_objects_to_lambda_functions
    global use_anonymous_lambda_functions
    global using_single_lambda_function
    global compute_pagerank
    global check_pagerank_output
    global number_of_pagerank_iterations_for_partitions_groups_with_loops 
    global name_of_first_groupOrpartition_in_DAG
    global same_output_for_all_fanout_fanin
    global use_incremental_DAG_generation
    global incremental_DAG_deposit_interval
    global work_queue_size_for_incremental_DAG_generation_with_worker_processes
    global tasks_use_result_dictionary_parameter
    global use_shared_partitions_groups
    global use_page_rank_group_partitions
    global use_struct_of_arrays_for_pagerank
    global run_all_tasks_locally
    global bypass_call_lambda_client_invoke
    global store_fanins_faninNBs_locally
    global create_all_fanins_faninNBs_on_start
    global using_workers
    global using_threads_not_processes
    global num_workers
    global use_multithreaded_multiprocessing
    global num_threads_for_multithreaded_multiprocessing
    global FanIn_Type
    global FanInNB_Type
    global process_work_queue_Type

    run_all_tasks_locally = True 
    store_fanins_faninNBs_locally = False 
    create_all_fanins_faninNBs_on_start = True
    using_workers = True
    using_threads_not_processes = True
    num_workers = 2
    use_multithreaded_multiprocessing = False
    num_threads_for_multithreaded_multiprocessing = 1

    #FanIn_Type = "DAG_executor_FanIn"
    #FanInNB_Type = "DAG_executor_FanInNB"
    #process_work_queue_Type = "BoundedBuffer"
    FanIn_Type = "DAG_executor_FanIn_Select"
    FanInNB_Type = "DAG_executor_FanInNB_Select"
    process_work_queue_Type = "BoundedBuffer_Select"

    store_sync_objects_in_lambdas = True
    using_Lambda_Function_Simulators_to_Store_Objects = True
    sync_objects_in_lambdas_trigger_their_tasks = False
    using_DAG_orchestrator = True
    map_objects_to_lambda_functions = False
    use_anonymous_lambda_functions = True
    using_single_lambda_function = False

#Test 28: multithreaded worker processes (A2) with selective-wait Sync-objects, 
#        2 threads for each of 2 worker process, Sync-objects stored remotely (on tcp_server)
#        create objects at the start
#        Variation: a. change D_O to T, 
#                   b. change map to F, and anon to T:
#                   c. change D_O to F, map F, anon T:
# Note: tcp_server must be running
def test28():
    print("test28")
    global store_sync_objects_in_lambdas
    global using_Lambda_Function_Simulators_to_Store_Objects
    global sync_objects_in_lambdas_trigger_their_tasks
    global using_DAG_orchestrator
    global map_objects_to_lambda_functions
    global use_anonymous_lambda_functions
    global using_single_lambda_function
    global compute_pagerank
    global check_pagerank_output
    global number_of_pagerank_iterations_for_partitions_groups_with_loops 
    global name_of_first_groupOrpartition_in_DAG
    global same_output_for_all_fanout_fanin
    global use_incremental_DAG_generation
    global incremental_DAG_deposit_interval
    global work_queue_size_for_incremental_DAG_generation_with_worker_processes
    global tasks_use_result_dictionary_parameter
    global use_shared_partitions_groups
    global use_page_rank_group_partitions
    global use_struct_of_arrays_for_pagerank
    global run_all_tasks_locally
    global bypass_call_lambda_client_invoke
    global store_fanins_faninNBs_locally
    global create_all_fanins_faninNBs_on_start
    global using_workers
    global using_threads_not_processes
    global num_workers
    global use_multithreaded_multiprocessing
    global num_threads_for_multithreaded_multiprocessing
    global FanIn_Type
    global FanInNB_Type
    global process_work_queue_Type

    run_all_tasks_locally = True 
    store_fanins_faninNBs_locally = False 
    create_all_fanins_faninNBs_on_start = True
    using_workers = True
    using_threads_not_processes = True
    num_workers = 2
    use_multithreaded_multiprocessing = False
    num_threads_for_multithreaded_multiprocessing = 1

    #FanIn_Type = "DAG_executor_FanIn"
    #FanInNB_Type = "DAG_executor_FanInNB"
    #process_work_queue_Type = "BoundedBuffer"
    FanIn_Type = "DAG_executor_FanIn_Select"
    FanInNB_Type = "DAG_executor_FanInNB_Select"
    process_work_queue_Type = "BoundedBuffer_Select"

    store_sync_objects_in_lambdas = True
    using_Lambda_Function_Simulators_to_Store_Objects = True
    sync_objects_in_lambdas_trigger_their_tasks = False
    using_DAG_orchestrator = False
    map_objects_to_lambda_functions = False
    use_anonymous_lambda_functions = True
    using_single_lambda_function = False

#Test 29: multithreaded worker processes (A2) with selective-wait Sync-objects, 
#        2 threads for each of 2 worker process, Sync-objects stored remotely (on tcp_server)
#        create objects at the start
# Note: tcp_server must be running
# Note: output is in tcp_server_lambda window, not DAG_executor window
def test29():

	#       run_all_tasks_locally = False, create objects on start = True:
	#       Note: not running real lambdas yet, so need TTT, i.e., not using threads
	#       to simulate lambdas and not running real lambdas yet, so need to
	#       trigger lambdas, which means store objects in lambdas and they call
	#       DAG_excutor_Lambda to execute task (i.e., "trigger task to run in 
	#       the same lambda"). Eventually we'll have tests for use real 
	#       non-triggered lambdas to run tasks (invoked at fanouts/faninNBS)
	#       and objects stored in lambdas or on server.
	#       TTTTTFF: trigger and DAG_orchestrator, map objects true, anon is false, and create objects on start
    print("test29")
    global store_sync_objects_in_lambdas
    global using_Lambda_Function_Simulators_to_Store_Objects
    global sync_objects_in_lambdas_trigger_their_tasks
    global using_DAG_orchestrator
    global map_objects_to_lambda_functions
    global use_anonymous_lambda_functions
    global using_single_lambda_function
    global compute_pagerank
    global check_pagerank_output
    global number_of_pagerank_iterations_for_partitions_groups_with_loops 
    global name_of_first_groupOrpartition_in_DAG
    global same_output_for_all_fanout_fanin
    global use_incremental_DAG_generation
    global incremental_DAG_deposit_interval
    global work_queue_size_for_incremental_DAG_generation_with_worker_processes
    global tasks_use_result_dictionary_parameter
    global use_shared_partitions_groups
    global use_page_rank_group_partitions
    global use_struct_of_arrays_for_pagerank
    global run_all_tasks_locally
    global bypass_call_lambda_client_invoke
    global store_fanins_faninNBs_locally
    global create_all_fanins_faninNBs_on_start
    global using_workers
    global using_threads_not_processes
    global num_workers
    global use_multithreaded_multiprocessing
    global num_threads_for_multithreaded_multiprocessing
    global FanIn_Type
    global FanInNB_Type
    global process_work_queue_Type

    run_all_tasks_locally = False 
    store_fanins_faninNBs_locally = False 
    create_all_fanins_faninNBs_on_start = True
    using_workers = True
    using_threads_not_processes = True
    num_workers = 2
    use_multithreaded_multiprocessing = False
    num_threads_for_multithreaded_multiprocessing = 1

    #FanIn_Type = "DAG_executor_FanIn"
    #FanInNB_Type = "DAG_executor_FanInNB"
    #process_work_queue_Type = "BoundedBuffer"
    FanIn_Type = "DAG_executor_FanIn_Select"
    FanInNB_Type = "DAG_executor_FanInNB_Select"
    process_work_queue_Type = "BoundedBuffer_Select"

    store_sync_objects_in_lambdas = True
    using_Lambda_Function_Simulators_to_Store_Objects = True
    sync_objects_in_lambdas_trigger_their_tasks = True
    using_DAG_orchestrator = True
    map_objects_to_lambda_functions = True
    use_anonymous_lambda_functions = False
    using_single_lambda_function = False

# Note: output is in tcp_server_lambda window, not DAG_executor window

#Test 30: multithreaded worker processes (A2) with selective-wait Sync-objects, 
#        2 threads for each of 2 worker process, Sync-objects stored remotely (on tcp_server)
#        create objects at the start
# Note: tcp_server must be running
# Note: output is in tcp_server_lambda window, not DAG_executor window
def test30():

	#       run_all_tasks_locally = False, create objects on start = True:
	#       Note: not running real lambdas yet, so need TTT, i.e., not using threads
	#       to simulate lambdas and not running real lambdas yet, so need to
	#       trigger lambdas, which means store objects in lambdas and they call
	#       DAG_excutor_Lambda to execute task (i.e., "trigger task to run in 
	#       the same lambda"). Eventually we'll have tests for use real 
	#       non-triggered lambdas to run tasks (invoked at fanouts/faninNBS)
	#       and objects stored in lambdas or on server.
	#       a. change map to F, and anon to T and create on start to F:
    print("test30")
    global store_sync_objects_in_lambdas
    global using_Lambda_Function_Simulators_to_Store_Objects
    global sync_objects_in_lambdas_trigger_their_tasks
    global using_DAG_orchestrator
    global map_objects_to_lambda_functions
    global use_anonymous_lambda_functions
    global using_single_lambda_function
    global compute_pagerank
    global check_pagerank_output
    global number_of_pagerank_iterations_for_partitions_groups_with_loops 
    global name_of_first_groupOrpartition_in_DAG
    global same_output_for_all_fanout_fanin
    global use_incremental_DAG_generation
    global incremental_DAG_deposit_interval
    global work_queue_size_for_incremental_DAG_generation_with_worker_processes
    global tasks_use_result_dictionary_parameter
    global use_shared_partitions_groups
    global use_page_rank_group_partitions
    global use_struct_of_arrays_for_pagerank
    global run_all_tasks_locally
    global bypass_call_lambda_client_invoke
    global store_fanins_faninNBs_locally
    global create_all_fanins_faninNBs_on_start
    global using_workers
    global using_threads_not_processes
    global num_workers
    global use_multithreaded_multiprocessing
    global num_threads_for_multithreaded_multiprocessing
    global FanIn_Type
    global FanInNB_Type
    global process_work_queue_Type

    run_all_tasks_locally = False 
    store_fanins_faninNBs_locally = False 
    create_all_fanins_faninNBs_on_start = False
    using_workers = True
    using_threads_not_processes = True
    num_workers = 2
    use_multithreaded_multiprocessing = False
    num_threads_for_multithreaded_multiprocessing = 1

    #FanIn_Type = "DAG_executor_FanIn"
    #FanInNB_Type = "DAG_executor_FanInNB"
    #process_work_queue_Type = "BoundedBuffer"
    FanIn_Type = "DAG_executor_FanIn_Select"
    FanInNB_Type = "DAG_executor_FanInNB_Select"
    process_work_queue_Type = "BoundedBuffer_Select"

    store_sync_objects_in_lambdas = True
    using_Lambda_Function_Simulators_to_Store_Objects = True
    sync_objects_in_lambdas_trigger_their_tasks = True
    using_DAG_orchestrator = True
    map_objects_to_lambda_functions = False
    use_anonymous_lambda_functions = True
    using_single_lambda_function = False

# Note: output is in tcp_server_lambda window, not DAG_executor window

#Test 31: multithreaded worker processes (A2) with selective-wait Sync-objects, 
#        2 threads for each of 2 worker process, Sync-objects stored remotely (on tcp_server)
#        create objects at the start
# Note: tcp_server must be running
# Note: output is in tcp_server_lambda window, not DAG_executor window
def test31():

	#       run_all_tasks_locally = False, create objects on start = True:
	#       Note: not running real lambdas yet, so need TTT, i.e., not using threads
	#       to simulate lambdas and not running real lambdas yet, so need to
	#       trigger lambdas, which means store objects in lambdas and they call
	#       DAG_excutor_Lambda to execute task (i.e., "trigger task to run in 
	#       the same lambda"). Eventually we'll have tests for use real 
	#       non-triggered lambdas to run tasks (invoked at fanouts/faninNBS)
	#       and objects stored in lambdas or on server.
	#       a. change map to F, and anon to T and create on start to F:
	#       b. change DAG_orchestrator to F (so not going through enqueue so will
	#          create on fly in other places besides equeue)
    print("test31")
    global store_sync_objects_in_lambdas
    global using_Lambda_Function_Simulators_to_Store_Objects
    global sync_objects_in_lambdas_trigger_their_tasks
    global using_DAG_orchestrator
    global map_objects_to_lambda_functions
    global use_anonymous_lambda_functions
    global using_single_lambda_function
    global compute_pagerank
    global check_pagerank_output
    global number_of_pagerank_iterations_for_partitions_groups_with_loops 
    global name_of_first_groupOrpartition_in_DAG
    global same_output_for_all_fanout_fanin
    global use_incremental_DAG_generation
    global incremental_DAG_deposit_interval
    global work_queue_size_for_incremental_DAG_generation_with_worker_processes
    global tasks_use_result_dictionary_parameter
    global use_shared_partitions_groups
    global use_page_rank_group_partitions
    global use_struct_of_arrays_for_pagerank
    global run_all_tasks_locally
    global bypass_call_lambda_client_invoke
    global store_fanins_faninNBs_locally
    global create_all_fanins_faninNBs_on_start
    global using_workers
    global using_threads_not_processes
    global num_workers
    global use_multithreaded_multiprocessing
    global num_threads_for_multithreaded_multiprocessing
    global FanIn_Type
    global FanInNB_Type
    global process_work_queue_Type

    run_all_tasks_locally = False 
    store_fanins_faninNBs_locally = False 
    create_all_fanins_faninNBs_on_start = False
    using_workers = True
    using_threads_not_processes = True
    num_workers = 2
    use_multithreaded_multiprocessing = False
    num_threads_for_multithreaded_multiprocessing = 1

    #FanIn_Type = "DAG_executor_FanIn"
    #FanInNB_Type = "DAG_executor_FanInNB"
    #process_work_queue_Type = "BoundedBuffer"
    FanIn_Type = "DAG_executor_FanIn_Select"
    FanInNB_Type = "DAG_executor_FanInNB_Select"
    process_work_queue_Type = "BoundedBuffer_Select"

    store_sync_objects_in_lambdas = True
    using_Lambda_Function_Simulators_to_Store_Objects = True
    sync_objects_in_lambdas_trigger_their_tasks = True
    using_DAG_orchestrator = False
    map_objects_to_lambda_functions = False
    use_anonymous_lambda_functions = True
    using_single_lambda_function = False

# Note: output is in tcp_server_lambda window, not DAG_executor window

#############################
###### Pagerank tests  ######
#################################################

"""
#PageRank tests
#Non-incremental
#running: python -m wukongdnc.dag.BFS using "fname = "graph_24N_3CC_fanin"  in BFS.input_graph()
"""

#Test 32: worker threads (A2) with non-selective-wait Sync-objects, 
#        2 worker threads, Sync-objects stored locally
#        create objects at the start
#        use pagerank groups
def test32():
    print("test32")
    global store_sync_objects_in_lambdas
    global using_Lambda_Function_Simulators_to_Store_Objects
    global sync_objects_in_lambdas_trigger_their_tasks
    global using_DAG_orchestrator
    global map_objects_to_lambda_functions
    global use_anonymous_lambda_functions
    global using_single_lambda_function
    global compute_pagerank
    global check_pagerank_output
    global number_of_pagerank_iterations_for_partitions_groups_with_loops 
    global name_of_first_groupOrpartition_in_DAG
    global same_output_for_all_fanout_fanin
    global use_incremental_DAG_generation
    global incremental_DAG_deposit_interval
    global work_queue_size_for_incremental_DAG_generation_with_worker_processes
    global tasks_use_result_dictionary_parameter
    global use_shared_partitions_groups
    global use_page_rank_group_partitions
    global use_struct_of_arrays_for_pagerank
    global run_all_tasks_locally
    global bypass_call_lambda_client_invoke
    global store_fanins_faninNBs_locally
    global create_all_fanins_faninNBs_on_start
    global using_workers
    global using_threads_not_processes
    global num_workers
    global use_multithreaded_multiprocessing
    global num_threads_for_multithreaded_multiprocessing
    global FanIn_Type
    global FanInNB_Type
    global process_work_queue_Type

    run_all_tasks_locally = False 
    store_fanins_faninNBs_locally = False 
    create_all_fanins_faninNBs_on_start = False
    using_workers = True
    using_threads_not_processes = True
    num_workers = 2
    use_multithreaded_multiprocessing = False
    num_threads_for_multithreaded_multiprocessing = 1

    #FanIn_Type = "DAG_executor_FanIn"
    #FanInNB_Type = "DAG_executor_FanInNB"
    #process_work_queue_Type = "BoundedBuffer"
    FanIn_Type = "DAG_executor_FanIn_Select"
    FanInNB_Type = "DAG_executor_FanInNB_Select"
    process_work_queue_Type = "BoundedBuffer_Select"

    store_sync_objects_in_lambdas = False
    using_Lambda_Function_Simulators_to_Store_Objects = False
    sync_objects_in_lambdas_trigger_their_tasks = False
    using_DAG_orchestrator = False
    map_objects_to_lambda_functions = False
    use_anonymous_lambda_functions = False
    using_single_lambda_function = False

    compute_pagerank = True
    check_pagerank_output = compute_pagerank and run_all_tasks_locally and using_workers and using_threads_not_processes and True
    name_of_first_groupOrpartition_in_DAG = "PR1_1"
    number_of_pagerank_iterations_for_partitions_groups_with_loops = 10
    same_output_for_all_fanout_fanin = not compute_pagerank
    use_incremental_DAG_generation = compute_pagerank and False
    incremental_DAG_deposit_interval = 2
    work_queue_size_for_incremental_DAG_generation_with_worker_processes =  2**10-1
    tasks_use_result_dictionary_parameter = compute_pagerank and True
    use_shared_partitions_groups = compute_pagerank and False
    use_page_rank_group_partitions = compute_pagerank and True
    use_struct_of_arrays_for_pagerank = compute_pagerank and False

#Test 33: worker threads (A2) with non-selective-wait Sync-objects, 
#        2 worker threads, Sync-objects stored locally
#        create objects at the start
#        use pagerank partitions
def test33():
    print("test33")
    global store_sync_objects_in_lambdas
    global using_Lambda_Function_Simulators_to_Store_Objects
    global sync_objects_in_lambdas_trigger_their_tasks
    global using_DAG_orchestrator
    global map_objects_to_lambda_functions
    global use_anonymous_lambda_functions
    global using_single_lambda_function
    global compute_pagerank
    global check_pagerank_output
    global number_of_pagerank_iterations_for_partitions_groups_with_loops 
    global name_of_first_groupOrpartition_in_DAG
    global same_output_for_all_fanout_fanin
    global use_incremental_DAG_generation
    global incremental_DAG_deposit_interval
    global work_queue_size_for_incremental_DAG_generation_with_worker_processes
    global tasks_use_result_dictionary_parameter
    global use_shared_partitions_groups
    global use_page_rank_group_partitions
    global use_struct_of_arrays_for_pagerank
    global run_all_tasks_locally
    global bypass_call_lambda_client_invoke
    global store_fanins_faninNBs_locally
    global create_all_fanins_faninNBs_on_start
    global using_workers
    global using_threads_not_processes
    global num_workers
    global use_multithreaded_multiprocessing
    global num_threads_for_multithreaded_multiprocessing
    global FanIn_Type
    global FanInNB_Type
    global process_work_queue_Type

    run_all_tasks_locally = False 
    store_fanins_faninNBs_locally = False 
    create_all_fanins_faninNBs_on_start = False
    using_workers = True
    using_threads_not_processes = True
    num_workers = 2
    use_multithreaded_multiprocessing = False
    num_threads_for_multithreaded_multiprocessing = 1

    #FanIn_Type = "DAG_executor_FanIn"
    #FanInNB_Type = "DAG_executor_FanInNB"
    #process_work_queue_Type = "BoundedBuffer"
    FanIn_Type = "DAG_executor_FanIn_Select"
    FanInNB_Type = "DAG_executor_FanInNB_Select"
    process_work_queue_Type = "BoundedBuffer_Select"

    store_sync_objects_in_lambdas = False
    using_Lambda_Function_Simulators_to_Store_Objects = False
    sync_objects_in_lambdas_trigger_their_tasks = False
    using_DAG_orchestrator = False
    map_objects_to_lambda_functions = False
    use_anonymous_lambda_functions = False
    using_single_lambda_function = False

    compute_pagerank = True
    check_pagerank_output = compute_pagerank and run_all_tasks_locally and using_workers and using_threads_not_processes and True
    name_of_first_groupOrpartition_in_DAG = "PR1_1"
    number_of_pagerank_iterations_for_partitions_groups_with_loops = 10
    same_output_for_all_fanout_fanin = not compute_pagerank
    use_incremental_DAG_generation = compute_pagerank and False
    incremental_DAG_deposit_interval = 2
    work_queue_size_for_incremental_DAG_generation_with_worker_processes =  2**10-1
    tasks_use_result_dictionary_parameter = compute_pagerank and True
    use_shared_partitions_groups = compute_pagerank and False
    use_page_rank_group_partitions = compute_pagerank and False
    use_struct_of_arrays_for_pagerank = compute_pagerank and False

#Test 34: worker processes (A2) with non-selective-wait Sync-objects, 
#        2 worker processes, Sync-objects stored locally
#        create objects at the start
#        use pagerank groups
def test34():
    print("test34")
    global store_sync_objects_in_lambdas
    global using_Lambda_Function_Simulators_to_Store_Objects
    global sync_objects_in_lambdas_trigger_their_tasks
    global using_DAG_orchestrator
    global map_objects_to_lambda_functions
    global use_anonymous_lambda_functions
    global using_single_lambda_function
    global compute_pagerank
    global check_pagerank_output
    global number_of_pagerank_iterations_for_partitions_groups_with_loops 
    global name_of_first_groupOrpartition_in_DAG
    global same_output_for_all_fanout_fanin
    global use_incremental_DAG_generation
    global incremental_DAG_deposit_interval
    global work_queue_size_for_incremental_DAG_generation_with_worker_processes
    global tasks_use_result_dictionary_parameter
    global use_shared_partitions_groups
    global use_page_rank_group_partitions
    global use_struct_of_arrays_for_pagerank
    global run_all_tasks_locally
    global bypass_call_lambda_client_invoke
    global store_fanins_faninNBs_locally
    global create_all_fanins_faninNBs_on_start
    global using_workers
    global using_threads_not_processes
    global num_workers
    global use_multithreaded_multiprocessing
    global num_threads_for_multithreaded_multiprocessing
    global FanIn_Type
    global FanInNB_Type
    global process_work_queue_Type

    run_all_tasks_locally = False 
    store_fanins_faninNBs_locally = False 
    create_all_fanins_faninNBs_on_start = False
    using_workers = True
    using_threads_not_processes = False
    num_workers = 2
    use_multithreaded_multiprocessing = False
    num_threads_for_multithreaded_multiprocessing = 1

    #FanIn_Type = "DAG_executor_FanIn"
    #FanInNB_Type = "DAG_executor_FanInNB"
    #process_work_queue_Type = "BoundedBuffer"
    FanIn_Type = "DAG_executor_FanIn_Select"
    FanInNB_Type = "DAG_executor_FanInNB_Select"
    process_work_queue_Type = "BoundedBuffer_Select"

    store_sync_objects_in_lambdas = False
    using_Lambda_Function_Simulators_to_Store_Objects = False
    sync_objects_in_lambdas_trigger_their_tasks = False
    using_DAG_orchestrator = False
    map_objects_to_lambda_functions = False
    use_anonymous_lambda_functions = False
    using_single_lambda_function = False

    compute_pagerank = True
    check_pagerank_output = compute_pagerank and run_all_tasks_locally and using_workers and using_threads_not_processes and True
    name_of_first_groupOrpartition_in_DAG = "PR1_1"
    number_of_pagerank_iterations_for_partitions_groups_with_loops = 10
    same_output_for_all_fanout_fanin = not compute_pagerank
    use_incremental_DAG_generation = compute_pagerank and False
    incremental_DAG_deposit_interval = 2
    work_queue_size_for_incremental_DAG_generation_with_worker_processes =  2**10-1
    tasks_use_result_dictionary_parameter = compute_pagerank and True
    use_shared_partitions_groups = compute_pagerank and False
    use_page_rank_group_partitions = compute_pagerank and True
    use_struct_of_arrays_for_pagerank = compute_pagerank and False

#Test 35: worker processes (A2) with non-selective-wait Sync-objects, 
#        2 worker processes, Sync-objects stored locally
#        create objects at the start
#        use pagerank partitions
def test35():
    print("test35")
    global store_sync_objects_in_lambdas
    global using_Lambda_Function_Simulators_to_Store_Objects
    global sync_objects_in_lambdas_trigger_their_tasks
    global using_DAG_orchestrator
    global map_objects_to_lambda_functions
    global use_anonymous_lambda_functions
    global using_single_lambda_function
    global compute_pagerank
    global check_pagerank_output
    global number_of_pagerank_iterations_for_partitions_groups_with_loops 
    global name_of_first_groupOrpartition_in_DAG
    global same_output_for_all_fanout_fanin
    global use_incremental_DAG_generation
    global incremental_DAG_deposit_interval
    global work_queue_size_for_incremental_DAG_generation_with_worker_processes
    global tasks_use_result_dictionary_parameter
    global use_shared_partitions_groups
    global use_page_rank_group_partitions
    global use_struct_of_arrays_for_pagerank
    global run_all_tasks_locally
    global bypass_call_lambda_client_invoke
    global store_fanins_faninNBs_locally
    global create_all_fanins_faninNBs_on_start
    global using_workers
    global using_threads_not_processes
    global num_workers
    global use_multithreaded_multiprocessing
    global num_threads_for_multithreaded_multiprocessing
    global FanIn_Type
    global FanInNB_Type
    global process_work_queue_Type

    run_all_tasks_locally = False 
    store_fanins_faninNBs_locally = False 
    create_all_fanins_faninNBs_on_start = False
    using_workers = True
    using_threads_not_processes = False
    num_workers = 2
    use_multithreaded_multiprocessing = False
    num_threads_for_multithreaded_multiprocessing = 1

    #FanIn_Type = "DAG_executor_FanIn"
    #FanInNB_Type = "DAG_executor_FanInNB"
    #process_work_queue_Type = "BoundedBuffer"
    FanIn_Type = "DAG_executor_FanIn_Select"
    FanInNB_Type = "DAG_executor_FanInNB_Select"
    process_work_queue_Type = "BoundedBuffer_Select"

    store_sync_objects_in_lambdas = False
    using_Lambda_Function_Simulators_to_Store_Objects = False
    sync_objects_in_lambdas_trigger_their_tasks = False
    using_DAG_orchestrator = False
    map_objects_to_lambda_functions = False
    use_anonymous_lambda_functions = False
    using_single_lambda_function = False

    compute_pagerank = True
    check_pagerank_output = compute_pagerank and run_all_tasks_locally and using_workers and using_threads_not_processes and True
    name_of_first_groupOrpartition_in_DAG = "PR1_1"
    number_of_pagerank_iterations_for_partitions_groups_with_loops = 10
    same_output_for_all_fanout_fanin = not compute_pagerank
    use_incremental_DAG_generation = compute_pagerank and False
    incremental_DAG_deposit_interval = 2
    work_queue_size_for_incremental_DAG_generation_with_worker_processes =  2**10-1
    tasks_use_result_dictionary_parameter = compute_pagerank and True
    use_shared_partitions_groups = compute_pagerank and False
    use_page_rank_group_partitions = compute_pagerank and False
    use_struct_of_arrays_for_pagerank = compute_pagerank and False

#test workers share memory. worker threads share a global array. 
#worker processes share Shared Memory

#Test 36: worker threads (A2) with non-selective-wait Sync-objects, 
#        2 worker threads, Sync-objects stored remotely
#        create objects at the start
#        use pagerank groups
#        use shared
def test36():
    print("test36")
    global store_sync_objects_in_lambdas
    global using_Lambda_Function_Simulators_to_Store_Objects
    global sync_objects_in_lambdas_trigger_their_tasks
    global using_DAG_orchestrator
    global map_objects_to_lambda_functions
    global use_anonymous_lambda_functions
    global using_single_lambda_function
    global compute_pagerank
    global check_pagerank_output
    global number_of_pagerank_iterations_for_partitions_groups_with_loops 
    global name_of_first_groupOrpartition_in_DAG
    global same_output_for_all_fanout_fanin
    global use_incremental_DAG_generation
    global incremental_DAG_deposit_interval
    global work_queue_size_for_incremental_DAG_generation_with_worker_processes
    global tasks_use_result_dictionary_parameter
    global use_shared_partitions_groups
    global use_page_rank_group_partitions
    global use_struct_of_arrays_for_pagerank
    global run_all_tasks_locally
    global bypass_call_lambda_client_invoke
    global store_fanins_faninNBs_locally
    global create_all_fanins_faninNBs_on_start
    global using_workers
    global using_threads_not_processes
    global num_workers
    global use_multithreaded_multiprocessing
    global num_threads_for_multithreaded_multiprocessing
    global FanIn_Type
    global FanInNB_Type
    global process_work_queue_Type

    run_all_tasks_locally = False 
    store_fanins_faninNBs_locally = False 
    create_all_fanins_faninNBs_on_start = True
    using_workers = True
    using_threads_not_processes = True
    num_workers = 2
    use_multithreaded_multiprocessing = False
    num_threads_for_multithreaded_multiprocessing = 1

    #FanIn_Type = "DAG_executor_FanIn"
    #FanInNB_Type = "DAG_executor_FanInNB"
    #process_work_queue_Type = "BoundedBuffer"
    FanIn_Type = "DAG_executor_FanIn_Select"
    FanInNB_Type = "DAG_executor_FanInNB_Select"
    process_work_queue_Type = "BoundedBuffer_Select"

    store_sync_objects_in_lambdas = False
    using_Lambda_Function_Simulators_to_Store_Objects = False
    sync_objects_in_lambdas_trigger_their_tasks = False
    using_DAG_orchestrator = False
    map_objects_to_lambda_functions = False
    use_anonymous_lambda_functions = False
    using_single_lambda_function = False

    compute_pagerank = True
    check_pagerank_output = compute_pagerank and run_all_tasks_locally and using_workers and using_threads_not_processes and True
    name_of_first_groupOrpartition_in_DAG = "PR1_1"
    number_of_pagerank_iterations_for_partitions_groups_with_loops = 10
    same_output_for_all_fanout_fanin = not compute_pagerank
    use_incremental_DAG_generation = compute_pagerank and False
    incremental_DAG_deposit_interval = 2
    work_queue_size_for_incremental_DAG_generation_with_worker_processes =  2**10-1
    tasks_use_result_dictionary_parameter = compute_pagerank and True
    use_shared_partitions_groups = compute_pagerank and True
    use_page_rank_group_partitions = compute_pagerank and True
    use_struct_of_arrays_for_pagerank = compute_pagerank and False

#Test 37: worker threads (A2) with non-selective-wait Sync-objects, 
#        2 worker threads, Sync-objects stored remotely
#        create objects at the start
#        use pagerank partitions
#        use shared
def test37():
    print("test37")
    global store_sync_objects_in_lambdas
    global using_Lambda_Function_Simulators_to_Store_Objects
    global sync_objects_in_lambdas_trigger_their_tasks
    global using_DAG_orchestrator
    global map_objects_to_lambda_functions
    global use_anonymous_lambda_functions
    global using_single_lambda_function
    global compute_pagerank
    global check_pagerank_output
    global number_of_pagerank_iterations_for_partitions_groups_with_loops 
    global name_of_first_groupOrpartition_in_DAG
    global same_output_for_all_fanout_fanin
    global use_incremental_DAG_generation
    global incremental_DAG_deposit_interval
    global work_queue_size_for_incremental_DAG_generation_with_worker_processes
    global tasks_use_result_dictionary_parameter
    global use_shared_partitions_groups
    global use_page_rank_group_partitions
    global use_struct_of_arrays_for_pagerank
    global run_all_tasks_locally
    global bypass_call_lambda_client_invoke
    global store_fanins_faninNBs_locally
    global create_all_fanins_faninNBs_on_start
    global using_workers
    global using_threads_not_processes
    global num_workers
    global use_multithreaded_multiprocessing
    global num_threads_for_multithreaded_multiprocessing
    global FanIn_Type
    global FanInNB_Type
    global process_work_queue_Type

    run_all_tasks_locally = False 
    store_fanins_faninNBs_locally = False 
    create_all_fanins_faninNBs_on_start = True
    using_workers = True
    using_threads_not_processes = True
    num_workers = 2
    use_multithreaded_multiprocessing = False
    num_threads_for_multithreaded_multiprocessing = 1

    #FanIn_Type = "DAG_executor_FanIn"
    #FanInNB_Type = "DAG_executor_FanInNB"
    #process_work_queue_Type = "BoundedBuffer"
    FanIn_Type = "DAG_executor_FanIn_Select"
    FanInNB_Type = "DAG_executor_FanInNB_Select"
    process_work_queue_Type = "BoundedBuffer_Select"

    store_sync_objects_in_lambdas = False
    using_Lambda_Function_Simulators_to_Store_Objects = False
    sync_objects_in_lambdas_trigger_their_tasks = False
    using_DAG_orchestrator = False
    map_objects_to_lambda_functions = False
    use_anonymous_lambda_functions = False
    using_single_lambda_function = False

    compute_pagerank = True
    check_pagerank_output = compute_pagerank and run_all_tasks_locally and using_workers and using_threads_not_processes and True
    name_of_first_groupOrpartition_in_DAG = "PR1_1"
    number_of_pagerank_iterations_for_partitions_groups_with_loops = 10
    same_output_for_all_fanout_fanin = not compute_pagerank
    use_incremental_DAG_generation = compute_pagerank and False
    incremental_DAG_deposit_interval = 2
    work_queue_size_for_incremental_DAG_generation_with_worker_processes =  2**10-1
    tasks_use_result_dictionary_parameter = compute_pagerank and True
    use_shared_partitions_groups = compute_pagerank and True
    use_page_rank_group_partitions = compute_pagerank and False
    use_struct_of_arrays_for_pagerank = compute_pagerank and False

#Test 38: worker threads (A2) with non-selective-wait Sync-objects, 
#        2 worker threads, Sync-objects stored remotely
#        create objects at the start
#        use pagerank groups
#        use shared
#        use array of structs
def test38():
    print("test38")
    global store_sync_objects_in_lambdas
    global using_Lambda_Function_Simulators_to_Store_Objects
    global sync_objects_in_lambdas_trigger_their_tasks
    global using_DAG_orchestrator
    global map_objects_to_lambda_functions
    global use_anonymous_lambda_functions
    global using_single_lambda_function
    global compute_pagerank
    global check_pagerank_output
    global number_of_pagerank_iterations_for_partitions_groups_with_loops 
    global name_of_first_groupOrpartition_in_DAG
    global same_output_for_all_fanout_fanin
    global use_incremental_DAG_generation
    global incremental_DAG_deposit_interval
    global work_queue_size_for_incremental_DAG_generation_with_worker_processes
    global tasks_use_result_dictionary_parameter
    global use_shared_partitions_groups
    global use_page_rank_group_partitions
    global use_struct_of_arrays_for_pagerank
    global run_all_tasks_locally
    global bypass_call_lambda_client_invoke
    global store_fanins_faninNBs_locally
    global create_all_fanins_faninNBs_on_start
    global using_workers
    global using_threads_not_processes
    global num_workers
    global use_multithreaded_multiprocessing
    global num_threads_for_multithreaded_multiprocessing
    global FanIn_Type
    global FanInNB_Type
    global process_work_queue_Type

    run_all_tasks_locally = False 
    store_fanins_faninNBs_locally = False 
    create_all_fanins_faninNBs_on_start = True
    using_workers = True
    using_threads_not_processes = True
    num_workers = 2
    use_multithreaded_multiprocessing = False
    num_threads_for_multithreaded_multiprocessing = 1

    #FanIn_Type = "DAG_executor_FanIn"
    #FanInNB_Type = "DAG_executor_FanInNB"
    #process_work_queue_Type = "BoundedBuffer"
    FanIn_Type = "DAG_executor_FanIn_Select"
    FanInNB_Type = "DAG_executor_FanInNB_Select"
    process_work_queue_Type = "BoundedBuffer_Select"

    store_sync_objects_in_lambdas = False
    using_Lambda_Function_Simulators_to_Store_Objects = False
    sync_objects_in_lambdas_trigger_their_tasks = False
    using_DAG_orchestrator = False
    map_objects_to_lambda_functions = False
    use_anonymous_lambda_functions = False
    using_single_lambda_function = False

    compute_pagerank = True
    check_pagerank_output = compute_pagerank and run_all_tasks_locally and using_workers and using_threads_not_processes and True
    name_of_first_groupOrpartition_in_DAG = "PR1_1"
    number_of_pagerank_iterations_for_partitions_groups_with_loops = 10
    same_output_for_all_fanout_fanin = not compute_pagerank
    use_incremental_DAG_generation = compute_pagerank and False
    incremental_DAG_deposit_interval = 2
    work_queue_size_for_incremental_DAG_generation_with_worker_processes =  2**10-1
    tasks_use_result_dictionary_parameter = compute_pagerank and True
    use_shared_partitions_groups = compute_pagerank and True
    use_page_rank_group_partitions = compute_pagerank and True
    use_struct_of_arrays_for_pagerank = compute_pagerank and True

#Test 39: worker threads (A2) with non-selective-wait Sync-objects, 
#        2 worker threads, Sync-objects stored remotely
#        create objects at the start
#        use pagerank partitions
#        use shared
#        use array of structs
def test39():
    print("test39")
    global store_sync_objects_in_lambdas
    global using_Lambda_Function_Simulators_to_Store_Objects
    global sync_objects_in_lambdas_trigger_their_tasks
    global using_DAG_orchestrator
    global map_objects_to_lambda_functions
    global use_anonymous_lambda_functions
    global using_single_lambda_function
    global compute_pagerank
    global check_pagerank_output
    global number_of_pagerank_iterations_for_partitions_groups_with_loops 
    global name_of_first_groupOrpartition_in_DAG
    global same_output_for_all_fanout_fanin
    global use_incremental_DAG_generation
    global incremental_DAG_deposit_interval
    global work_queue_size_for_incremental_DAG_generation_with_worker_processes
    global tasks_use_result_dictionary_parameter
    global use_shared_partitions_groups
    global use_page_rank_group_partitions
    global use_struct_of_arrays_for_pagerank
    global run_all_tasks_locally
    global bypass_call_lambda_client_invoke
    global store_fanins_faninNBs_locally
    global create_all_fanins_faninNBs_on_start
    global using_workers
    global using_threads_not_processes
    global num_workers
    global use_multithreaded_multiprocessing
    global num_threads_for_multithreaded_multiprocessing
    global FanIn_Type
    global FanInNB_Type
    global process_work_queue_Type

    run_all_tasks_locally = False 
    store_fanins_faninNBs_locally = False 
    create_all_fanins_faninNBs_on_start = True
    using_workers = True
    using_threads_not_processes = True
    num_workers = 2
    use_multithreaded_multiprocessing = False
    num_threads_for_multithreaded_multiprocessing = 1

    #FanIn_Type = "DAG_executor_FanIn"
    #FanInNB_Type = "DAG_executor_FanInNB"
    #process_work_queue_Type = "BoundedBuffer"
    FanIn_Type = "DAG_executor_FanIn_Select"
    FanInNB_Type = "DAG_executor_FanInNB_Select"
    process_work_queue_Type = "BoundedBuffer_Select"

    store_sync_objects_in_lambdas = False
    using_Lambda_Function_Simulators_to_Store_Objects = False
    sync_objects_in_lambdas_trigger_their_tasks = False
    using_DAG_orchestrator = False
    map_objects_to_lambda_functions = False
    use_anonymous_lambda_functions = False
    using_single_lambda_function = False

    compute_pagerank = True
    check_pagerank_output = compute_pagerank and run_all_tasks_locally and using_workers and using_threads_not_processes and True
    name_of_first_groupOrpartition_in_DAG = "PR1_1"
    number_of_pagerank_iterations_for_partitions_groups_with_loops = 10
    same_output_for_all_fanout_fanin = not compute_pagerank
    use_incremental_DAG_generation = compute_pagerank and False
    incremental_DAG_deposit_interval = 2
    work_queue_size_for_incremental_DAG_generation_with_worker_processes =  2**10-1
    tasks_use_result_dictionary_parameter = compute_pagerank and True
    use_shared_partitions_groups = compute_pagerank and True
    use_page_rank_group_partitions = compute_pagerank and False
    use_struct_of_arrays_for_pagerank = compute_pagerank and True

#Test 40: worker processes (A2) with non-selective-wait Sync-objects, 
#        2 worker processes, Sync-objects stored remotely
#        use pagerank groups
#        use shared
def test40():
    print("test40")
    global store_sync_objects_in_lambdas
    global using_Lambda_Function_Simulators_to_Store_Objects
    global sync_objects_in_lambdas_trigger_their_tasks
    global using_DAG_orchestrator
    global map_objects_to_lambda_functions
    global use_anonymous_lambda_functions
    global using_single_lambda_function
    global compute_pagerank
    global check_pagerank_output
    global number_of_pagerank_iterations_for_partitions_groups_with_loops 
    global name_of_first_groupOrpartition_in_DAG
    global same_output_for_all_fanout_fanin
    global use_incremental_DAG_generation
    global incremental_DAG_deposit_interval
    global work_queue_size_for_incremental_DAG_generation_with_worker_processes
    global tasks_use_result_dictionary_parameter
    global use_shared_partitions_groups
    global use_page_rank_group_partitions
    global use_struct_of_arrays_for_pagerank
    global run_all_tasks_locally
    global bypass_call_lambda_client_invoke
    global store_fanins_faninNBs_locally
    global create_all_fanins_faninNBs_on_start
    global using_workers
    global using_threads_not_processes
    global num_workers
    global use_multithreaded_multiprocessing
    global num_threads_for_multithreaded_multiprocessing
    global FanIn_Type
    global FanInNB_Type
    global process_work_queue_Type

    run_all_tasks_locally = False 
    store_fanins_faninNBs_locally = False 
    create_all_fanins_faninNBs_on_start = True
    using_workers = True
    using_threads_not_processes = False
    num_workers = 2
    use_multithreaded_multiprocessing = False
    num_threads_for_multithreaded_multiprocessing = 1

    #FanIn_Type = "DAG_executor_FanIn"
    #FanInNB_Type = "DAG_executor_FanInNB"
    #process_work_queue_Type = "BoundedBuffer"
    FanIn_Type = "DAG_executor_FanIn_Select"
    FanInNB_Type = "DAG_executor_FanInNB_Select"
    process_work_queue_Type = "BoundedBuffer_Select"

    store_sync_objects_in_lambdas = False
    using_Lambda_Function_Simulators_to_Store_Objects = False
    sync_objects_in_lambdas_trigger_their_tasks = False
    using_DAG_orchestrator = False
    map_objects_to_lambda_functions = False
    use_anonymous_lambda_functions = False
    using_single_lambda_function = False

    compute_pagerank = True
    check_pagerank_output = compute_pagerank and run_all_tasks_locally and using_workers and using_threads_not_processes and True
    name_of_first_groupOrpartition_in_DAG = "PR1_1"
    number_of_pagerank_iterations_for_partitions_groups_with_loops = 10
    same_output_for_all_fanout_fanin = not compute_pagerank
    use_incremental_DAG_generation = compute_pagerank and False
    incremental_DAG_deposit_interval = 2
    work_queue_size_for_incremental_DAG_generation_with_worker_processes =  2**10-1
    tasks_use_result_dictionary_parameter = compute_pagerank and True
    use_shared_partitions_groups = compute_pagerank and True
    use_page_rank_group_partitions = compute_pagerank and True
    use_struct_of_arrays_for_pagerank = compute_pagerank and False

#Test 41: worker processes (A2) with non-selective-wait Sync-objects, 
#        2 worker processes, Sync-objects stored remotely
#        create objects at the start
#        use pagerank partitions
#        use shared
def test41():
    print("test41")
    global store_sync_objects_in_lambdas
    global using_Lambda_Function_Simulators_to_Store_Objects
    global sync_objects_in_lambdas_trigger_their_tasks
    global using_DAG_orchestrator
    global map_objects_to_lambda_functions
    global use_anonymous_lambda_functions
    global using_single_lambda_function
    global compute_pagerank
    global check_pagerank_output
    global number_of_pagerank_iterations_for_partitions_groups_with_loops 
    global name_of_first_groupOrpartition_in_DAG
    global same_output_for_all_fanout_fanin
    global use_incremental_DAG_generation
    global incremental_DAG_deposit_interval
    global work_queue_size_for_incremental_DAG_generation_with_worker_processes
    global tasks_use_result_dictionary_parameter
    global use_shared_partitions_groups
    global use_page_rank_group_partitions
    global use_struct_of_arrays_for_pagerank
    global run_all_tasks_locally
    global bypass_call_lambda_client_invoke
    global store_fanins_faninNBs_locally
    global create_all_fanins_faninNBs_on_start
    global using_workers
    global using_threads_not_processes
    global num_workers
    global use_multithreaded_multiprocessing
    global num_threads_for_multithreaded_multiprocessing
    global FanIn_Type
    global FanInNB_Type
    global process_work_queue_Type

    run_all_tasks_locally = False 
    store_fanins_faninNBs_locally = False 
    create_all_fanins_faninNBs_on_start = True
    using_workers = True
    using_threads_not_processes = False
    num_workers = 2
    use_multithreaded_multiprocessing = False
    num_threads_for_multithreaded_multiprocessing = 1

    #FanIn_Type = "DAG_executor_FanIn"
    #FanInNB_Type = "DAG_executor_FanInNB"
    #process_work_queue_Type = "BoundedBuffer"
    FanIn_Type = "DAG_executor_FanIn_Select"
    FanInNB_Type = "DAG_executor_FanInNB_Select"
    process_work_queue_Type = "BoundedBuffer_Select"

    store_sync_objects_in_lambdas = False
    using_Lambda_Function_Simulators_to_Store_Objects = False
    sync_objects_in_lambdas_trigger_their_tasks = False
    using_DAG_orchestrator = False
    map_objects_to_lambda_functions = False
    use_anonymous_lambda_functions = False
    using_single_lambda_function = False

    compute_pagerank = True
    check_pagerank_output = compute_pagerank and run_all_tasks_locally and using_workers and using_threads_not_processes and True
    name_of_first_groupOrpartition_in_DAG = "PR1_1"
    number_of_pagerank_iterations_for_partitions_groups_with_loops = 10
    same_output_for_all_fanout_fanin = not compute_pagerank
    use_incremental_DAG_generation = compute_pagerank and False
    incremental_DAG_deposit_interval = 2
    work_queue_size_for_incremental_DAG_generation_with_worker_processes =  2**10-1
    tasks_use_result_dictionary_parameter = compute_pagerank and True
    use_shared_partitions_groups = compute_pagerank and True
    use_page_rank_group_partitions = compute_pagerank and False
    use_struct_of_arrays_for_pagerank = compute_pagerank and False

#Test 42: worker processes (A2) with non-selective-wait Sync-objects, 
#        2 worker processes, Sync-objects stored remotely
#        create objects at the start
#        use pagerank groups
#        use shared
#        use array of structs
def test42():
    print("test42")
    global store_sync_objects_in_lambdas
    global using_Lambda_Function_Simulators_to_Store_Objects
    global sync_objects_in_lambdas_trigger_their_tasks
    global using_DAG_orchestrator
    global map_objects_to_lambda_functions
    global use_anonymous_lambda_functions
    global using_single_lambda_function
    global compute_pagerank
    global check_pagerank_output
    global number_of_pagerank_iterations_for_partitions_groups_with_loops 
    global name_of_first_groupOrpartition_in_DAG
    global same_output_for_all_fanout_fanin
    global use_incremental_DAG_generation
    global incremental_DAG_deposit_interval
    global work_queue_size_for_incremental_DAG_generation_with_worker_processes
    global tasks_use_result_dictionary_parameter
    global use_shared_partitions_groups
    global use_page_rank_group_partitions
    global use_struct_of_arrays_for_pagerank
    global run_all_tasks_locally
    global bypass_call_lambda_client_invoke
    global store_fanins_faninNBs_locally
    global create_all_fanins_faninNBs_on_start
    global using_workers
    global using_threads_not_processes
    global num_workers
    global use_multithreaded_multiprocessing
    global num_threads_for_multithreaded_multiprocessing
    global FanIn_Type
    global FanInNB_Type
    global process_work_queue_Type

    run_all_tasks_locally = False 
    store_fanins_faninNBs_locally = False 
    create_all_fanins_faninNBs_on_start = True
    using_workers = True
    using_threads_not_processes = False
    num_workers = 2
    use_multithreaded_multiprocessing = False
    num_threads_for_multithreaded_multiprocessing = 1

    #FanIn_Type = "DAG_executor_FanIn"
    #FanInNB_Type = "DAG_executor_FanInNB"
    #process_work_queue_Type = "BoundedBuffer"
    FanIn_Type = "DAG_executor_FanIn_Select"
    FanInNB_Type = "DAG_executor_FanInNB_Select"
    process_work_queue_Type = "BoundedBuffer_Select"

    store_sync_objects_in_lambdas = False
    using_Lambda_Function_Simulators_to_Store_Objects = False
    sync_objects_in_lambdas_trigger_their_tasks = False
    using_DAG_orchestrator = False
    map_objects_to_lambda_functions = False
    use_anonymous_lambda_functions = False
    using_single_lambda_function = False

    compute_pagerank = True
    check_pagerank_output = compute_pagerank and run_all_tasks_locally and using_workers and using_threads_not_processes and True
    name_of_first_groupOrpartition_in_DAG = "PR1_1"
    number_of_pagerank_iterations_for_partitions_groups_with_loops = 10
    same_output_for_all_fanout_fanin = not compute_pagerank
    use_incremental_DAG_generation = compute_pagerank and False
    incremental_DAG_deposit_interval = 2
    work_queue_size_for_incremental_DAG_generation_with_worker_processes =  2**10-1
    tasks_use_result_dictionary_parameter = compute_pagerank and True
    use_shared_partitions_groups = compute_pagerank and True
    use_page_rank_group_partitions = compute_pagerank and True
    use_struct_of_arrays_for_pagerank = compute_pagerank and True

#Test 43: worker processes (A2) with non-selective-wait Sync-objects, 
#        2 worker processes, Sync-objects stored remotely
#        create objects at the start
#        use pagerank partitions
#        use shared
#        use array of structs
def test43():
    print("test43")
    global store_sync_objects_in_lambdas
    global using_Lambda_Function_Simulators_to_Store_Objects
    global sync_objects_in_lambdas_trigger_their_tasks
    global using_DAG_orchestrator
    global map_objects_to_lambda_functions
    global use_anonymous_lambda_functions
    global using_single_lambda_function
    global compute_pagerank
    global check_pagerank_output
    global number_of_pagerank_iterations_for_partitions_groups_with_loops 
    global name_of_first_groupOrpartition_in_DAG
    global same_output_for_all_fanout_fanin
    global use_incremental_DAG_generation
    global incremental_DAG_deposit_interval
    global work_queue_size_for_incremental_DAG_generation_with_worker_processes
    global tasks_use_result_dictionary_parameter
    global use_shared_partitions_groups
    global use_page_rank_group_partitions
    global use_struct_of_arrays_for_pagerank
    global run_all_tasks_locally
    global bypass_call_lambda_client_invoke
    global store_fanins_faninNBs_locally
    global create_all_fanins_faninNBs_on_start
    global using_workers
    global using_threads_not_processes
    global num_workers
    global use_multithreaded_multiprocessing
    global num_threads_for_multithreaded_multiprocessing
    global FanIn_Type
    global FanInNB_Type
    global process_work_queue_Type

    run_all_tasks_locally = False 
    store_fanins_faninNBs_locally = False 
    create_all_fanins_faninNBs_on_start = True
    using_workers = True
    using_threads_not_processes = False
    num_workers = 2
    use_multithreaded_multiprocessing = False
    num_threads_for_multithreaded_multiprocessing = 1

    #FanIn_Type = "DAG_executor_FanIn"
    #FanInNB_Type = "DAG_executor_FanInNB"
    #process_work_queue_Type = "BoundedBuffer"
    FanIn_Type = "DAG_executor_FanIn_Select"
    FanInNB_Type = "DAG_executor_FanInNB_Select"
    process_work_queue_Type = "BoundedBuffer_Select"

    store_sync_objects_in_lambdas = False
    using_Lambda_Function_Simulators_to_Store_Objects = False
    sync_objects_in_lambdas_trigger_their_tasks = False
    using_DAG_orchestrator = False
    map_objects_to_lambda_functions = False
    use_anonymous_lambda_functions = False
    using_single_lambda_function = False

    compute_pagerank = True
    check_pagerank_output = compute_pagerank and run_all_tasks_locally and using_workers and using_threads_not_processes and True
    name_of_first_groupOrpartition_in_DAG = "PR1_1"
    number_of_pagerank_iterations_for_partitions_groups_with_loops = 10
    same_output_for_all_fanout_fanin = not compute_pagerank
    use_incremental_DAG_generation = compute_pagerank and False
    incremental_DAG_deposit_interval = 2
    work_queue_size_for_incremental_DAG_generation_with_worker_processes =  2**10-1
    tasks_use_result_dictionary_parameter = compute_pagerank and True
    use_shared_partitions_groups = compute_pagerank and True
    use_page_rank_group_partitions = compute_pagerank and False
    use_struct_of_arrays_for_pagerank = compute_pagerank and True

# Test Pagerank with increental DAG generation

#Test 44: worker threads (A2) with non-selective-wait Sync-objects, 
#        2 worker threads, Sync-objects stored locally
#        create objects on-the-fly (required for incremenal DAG generation)
#        use pagerank groups
def test44():
    print("test44")
    global store_sync_objects_in_lambdas
    global using_Lambda_Function_Simulators_to_Store_Objects
    global sync_objects_in_lambdas_trigger_their_tasks
    global using_DAG_orchestrator
    global map_objects_to_lambda_functions
    global use_anonymous_lambda_functions
    global using_single_lambda_function
    global compute_pagerank
    global check_pagerank_output
    global number_of_pagerank_iterations_for_partitions_groups_with_loops 
    global name_of_first_groupOrpartition_in_DAG
    global same_output_for_all_fanout_fanin
    global use_incremental_DAG_generation
    global incremental_DAG_deposit_interval
    global work_queue_size_for_incremental_DAG_generation_with_worker_processes
    global tasks_use_result_dictionary_parameter
    global use_shared_partitions_groups
    global use_page_rank_group_partitions
    global use_struct_of_arrays_for_pagerank
    global run_all_tasks_locally
    global bypass_call_lambda_client_invoke
    global store_fanins_faninNBs_locally
    global create_all_fanins_faninNBs_on_start
    global using_workers
    global using_threads_not_processes
    global num_workers
    global use_multithreaded_multiprocessing
    global num_threads_for_multithreaded_multiprocessing
    global FanIn_Type
    global FanInNB_Type
    global process_work_queue_Type

    run_all_tasks_locally = False 
    store_fanins_faninNBs_locally = True 
    create_all_fanins_faninNBs_on_start = False
    using_workers = True
    using_threads_not_processes = True
    num_workers = 2
    use_multithreaded_multiprocessing = False
    num_threads_for_multithreaded_multiprocessing = 1

    #FanIn_Type = "DAG_executor_FanIn"
    #FanInNB_Type = "DAG_executor_FanInNB"
    #process_work_queue_Type = "BoundedBuffer"
    FanIn_Type = "DAG_executor_FanIn_Select"
    FanInNB_Type = "DAG_executor_FanInNB_Select"
    process_work_queue_Type = "BoundedBuffer_Select"

    store_sync_objects_in_lambdas = False
    using_Lambda_Function_Simulators_to_Store_Objects = False
    sync_objects_in_lambdas_trigger_their_tasks = False
    using_DAG_orchestrator = False
    map_objects_to_lambda_functions = False
    use_anonymous_lambda_functions = False
    using_single_lambda_function = False

    compute_pagerank = True
    check_pagerank_output = compute_pagerank and run_all_tasks_locally and using_workers and using_threads_not_processes and True
    name_of_first_groupOrpartition_in_DAG = "PR1_1"
    number_of_pagerank_iterations_for_partitions_groups_with_loops = 10
    same_output_for_all_fanout_fanin = not compute_pagerank
    use_incremental_DAG_generation = compute_pagerank and True
    incremental_DAG_deposit_interval = 1
    work_queue_size_for_incremental_DAG_generation_with_worker_processes =  2**10-1
    tasks_use_result_dictionary_parameter = compute_pagerank and True
    use_shared_partitions_groups = compute_pagerank and False
    use_page_rank_group_partitions = compute_pagerank and True
    use_struct_of_arrays_for_pagerank = compute_pagerank and False

"""
ToDo: So no logger stuff if using processes?
if not using threads:
  logger stuff
so no double output in OS boxes. puts debug lines in mplog file but not DOS box; DOS box has the prints only.
If do logger stuff then get double print to DOS box of ebug stuff, not sure why

#####
Test real lambda code with bypass_call_lambda_client_invoke = not run_all_tasks_locally and True
######
Test PR in Lambdas?
######
"""

def check_asserts():
    #assert:
    if using_workers and not run_all_tasks_locally:
        logger.error("[Internal Error]: Configuration error: if using_workers then must run_all_tasks_locally.")
        logging.shutdown()
        os._exit(0)     

    #assert:
    if bypass_call_lambda_client_invoke and run_all_tasks_locally:
        logger.error("[Internal Error]: Configuration error: if bypass_call_lambda_client_invoke then must be running real Lambdas"
            + " i.e., not run_all_tasks_locally.")
        logging.shutdown()
        os._exit(0)  

    #assert:
    if using_workers and not using_threads_not_processes:
        if store_fanins_faninNBs_locally:
            # When using worker processed, synch objects must be stored remoely
            logger.error("[Internal Error]: Configuration error: if using_workers and not using_threads_not_processes"
                + " then store_fanins_faninNBs_locally must be False.")
            logging.shutdown()
            os._exit(0)

    #assert:
    if create_all_fanins_faninNBs_on_start and not run_all_tasks_locally and store_sync_objects_in_lambdas:
        if not map_objects_to_lambda_functions:
            # if create sync objects on start and executing tasks in lambdas "
            # then we must map them to function so that we can determine the 
            # function an object is in.
            logger.error("[Internal Error]: Configuration error: if create_all_fanins_faninNBs_on_start"
                + " then map_objects_to_functions must be True.")
            logging.shutdown()
            os._exit(0)

    #assert:
    if map_objects_to_lambda_functions:
        if use_anonymous_lambda_functions:
            # if create sync objects on start then we must map them to function so
            # that we can determine the function an object is in.
            logger.error("[Internal Error]: Configuration error: if map_objects_to_lambda_functions"
                + " then use_anonymous_lambda_functions must be False.")
            logging.shutdown()
            os._exit(0)

    #assert:
    if sync_objects_in_lambdas_trigger_their_tasks:
        if run_all_tasks_locally:
            # if create sync objects on start then we must map them to function so
            # that we can determine the function an object is in.
            logger.error("[Internal Error]: Configuration error: if sync_objects_in_lambdas_trigger_their_tasks"
                + " then not run_all_tasks_locally must be True.")
            logging.shutdown()
            os._exit(0)

    # assert 
    if compute_pagerank and use_incremental_DAG_generation and create_all_fanins_faninNBs_on_start:
        logger.error("[Internal Error]: Configuration error: incremental_DAG_generation"
            + " requires not create_all_fanins_faninNBs_on_start"
            + " i.e., create synch objects on the fly since we don't know all of the synch objects "
            + " at the start (the DAG is not complete)")
        logging.shutdown()
        os._exit(0) 

    #assert:
    if incremental_DAG_deposit_interval < 1:
        logger.error("[Internal Error]: Configuration error: incremental_DAG_deposit_interval"
            + " must be >= 1. We mod by incremental_DAG_deposit_interval so it"
            + " cannot be 0 and using a negative number makes no sense.")
        logging.shutdown()
        os._exit(0) 

    #assert:
    if not same_output_for_all_fanout_fanin and not compute_pagerank:
        logger.error("[Internal Error]: Configuration error: if same_output_for_all_fanout_fanin"
            + " then must be computing pagerank.")
        logging.shutdown()
        os._exit(0)

    #assert:
    if compute_pagerank and (use_struct_of_arrays_for_pagerank and not use_shared_partitions_groups):
        logger.error("[Internal Error]: Configuration error: if use_struct_of_arrays_for_pagerank"
            + " then must use_shared_partitions_groups.")
        logging.shutdown()
        os._exit(0)

    #assert:
    if compute_pagerank and (use_struct_of_arrays_for_pagerank and not use_shared_partitions_groups):
        logger.error("[Internal Error]: Configuration error: if use_struct_of_arrays_for_pagerank"
            + " then must use_shared_partitions_groups.")
        logging.shutdown()
        os._exit(0)

test_number = 0
# called by TestAll.py to run testX
def set_test_number(number):
    global test_number
    test_number = number

# Run Tests
if not test_number == 0:
    non_real_lambda_base()

if test_number == 1:
    test1()
elif test_number == 2:
    test2()

# Check asserts after setting the configuration constants
if not test_number == 0:
    check_asserts()


# Note: Running this script in the Wndows PowerShell X to run the 
# tests one-by-one. This is the command line "Windows Powershell"
# not the "Windows Powershell ISE". The latter gives and error 
# message when runnging python. (See the comment below.)

# This script is also in file TestAllPowerShellCommands.txt in
# directory C:\Users\benrc\Desktop\Executor\DAG\WukongDynamic

#cd C:\Users\benrc\Desktop\Executor\DAG\WukongDynamic
#For ($i=1; $i -le 2; $i++) {
#    Write-Host "Test $i"
#    Write-Host "#######"
#    python -m wukongdnc.dag.TestAll($i)
#    Read-Host "Enter any input to continue"
#}

# Copy the script and paste it into PowerSshell and hit enter twice.

# The error when runnging Powershell ISE is:
"""'
python : [2023-12-16 09:38:01,703][DAG_executor_driver][MainProcess][MainThread]: DAG_executor_driver: dask version: 2023.9.1
At line:5 char:2
+     python -m wukongdnc.dag.TestAll($i)
+     ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    + CategoryInfo          : NotSpecified: ([2023-12-16 09:...rsion: 2023.9.1:String) [], RemoteException
    + FullyQualifiedErrorId : NativeCommandError
"""
# This has something to do with pythons stderror stream.
