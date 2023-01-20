import logging

logger = None
logger = logging.getLogger(__name__)

logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(asctime)s] [%(threadName)s] %(levelname)s: %(message)s')
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)
logger.addHandler(ch)

# Configuraion:
#
# True if we are not using Lambdas, i.e., executing tasks with threads or processes
# local, i.e., on one machine.
run_all_tasks_locally = False         # vs run tasks remotely (in Lambdas)
# True if synch objects are stored locally, i.e., in the memory of the single
# machine on which the threads are executing.  If we are using multiprocessing
# or Lambdas, this must be False. When False, the synch objects are stored
# on the tcp_server or in InfiniX lambdas.
store_fanins_faninNBs_locally = False    # vs remotely
# True when all FanIn and FanInNB objects are created locally or on the
# tcp_server or IniniX all at once at the start of the DAG execution. If
# False, synch objects are created on the fly, i.e, we execute create-and-fanin
# operations that create a synch object if it has not been created yet and then
# execute a Fan_in operaation on the created object.
create_all_fanins_faninNBs_on_start = False

# True if the DAG is executed by a "pool" of threads/processes. False, if we are
# using Lambdas or we are using threads to simulate the use of Lambdas. In the latter
# case, instead of, e.g., starting a Lambda at fan_out operations, we start a thread.
# This results in the creation of many threads and is only use to test the logic 
# of the Lambda code.
using_workers = False
# True when we ae not using Lambas and tasks are executed by threads instead of processes. 
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
#FanIn_Type = "DAG_executor_FanIn"
#FanInNB_Type = "DAG_executor_FanInNB"
#process_work_queue_Type = "BoundedBuffer"
FanIn_Type = "DAG_executor_FanIn_Select"
FanInNB_Type = "DAG_executor_FanInNB_Select"
process_work_queue_Type = "BoundedBuffer_Select"

# if running real lambdas or storing synch objects in real lambdas:
#   Set SERVERLESS_SYNC to True or False in wukongdnc constants !!!!!!!!!!!!!!
#

# Currently, this is for storing synch objects in simulated lambdas;
store_sync_objects_in_lambdas = True
using_Lambda_Function_Simulators_to_Store_Objects = True
sync_objects_in_lambdas_trigger_their_tasks = True
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
use_anonymous_lambda_functions = True
# For all: remote objects, using select objects:
# 1. run_all_tasks_locally = True, create objects on start = True:
# TTFFTF: no trigger and no DAG_orchestrator, but map objects (anon is false) and create objects on start
# - change D_O to T, 
# - D_O to T, change map to F, and anon to T: Note: no function lock since anon caled only once
# - change D_O to F, map F, anon T: Note: no function lock since anon caled only once
# 2. run_all_tasks_locally = False, create objects on start = True:
# Note: not running real lambdas yet, so need TTT, i.e., not using threads
#       to simulate lambdas and not running real lambdas yet, so need to
#       trigger lambdas, whcih means store objects in lambdas and they call
#       DAG_excutor_Lambda to execute task (i.e., "trigger task to run in 
#       the same lambda"). Eventually we'll have tests for use real 
#       non-triggered lambdas to run tasks (invoked at fanouts/faninNBS)
#       and objects stored in lambdas or on server.
# TTTTTF: trigger and DAG_orchestrator, map objects (anon is false) and create objects on start
# - change map to F, and anon to T and create on start to F: Note: no function lock since anon called only once
# - change DAG_orchestrator to F - so not going through enqueue so will
#   create on fly in other places besides equeue.

# Q: if map is F does anon have to be True? In theory no, use any named
#    function to store any sync ojject. e.g., use DAG_executor_i for ith
#    object accessed, but then there's a finite limit on number of functions;
#    still, using this scheme we can call same function more than once, 
#    as long as you dynamically map the objects to the name of the 
#    function (chosen at run time) that they are stored in. So either
#    way you need to map sync object names to functions if yoy want to 
#    invoke the function to do an op on the object more than once.

#assert:
if create_all_fanins_faninNBs_on_start and not run_all_tasks_locally:
    if not map_objects_to_lambda_functions:
        # if create sync objects on start and executing tasks in lambdas "
        # then we must map them to function so that we can determine the 
        # function an object is in.
        logger.error("[Error]: Configuration error: if create_all_fanins_faninNBs_on_start"
            + " then map_objects_to_functions must be True.")

#assert:
if map_objects_to_lambda_functions:
    if use_anonymous_lambda_functions:
        # if create sync objects on start then we must map them to function so
        # that we can determine the function an object is in.
        logger.error("[Error]: Configuration error: if map_objects_to_lambda_functions"
            + " then use_anonymous_lambda_functions must be False.")

#assert:
if sync_objects_in_lambdas_trigger_their_tasks:
    if run_all_tasks_locally:
        # if create sync objects on start then we must map them to function so
        # that we can determine the function an object is in.
        logger.error("[Error]: Configuration error: if sync_objects_in_lambdas_trigger_their_tasks"
            + " then not run_all_tasks_locally must be True.")

# So if create on start then must map objects and cannot use anonymous functions.
# If want to use anonymous functions then no create objects on statr and no mapping.

# use a single lambda function to store all of the synchroization objects
# to make an easy test case. This cannot be used when using the function 
# simulators or using the DAG_orchestrator
using_single_lambda_function = False



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

