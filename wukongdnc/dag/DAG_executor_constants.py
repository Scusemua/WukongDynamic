print("DAG_executor_constants XXX")
import multiprocessing
import threading
proc_name = multiprocessing.current_process().name
thread_name = threading.current_thread().name
from inspect import currentframe
frame = currentframe().f_back
while frame.f_code.co_filename.startswith('<frozen'):
    frame = frame.f_back
print(proc_name + ":" + thread_name + ":" + frame.f_code.co_filename)
"""
Important: This file incudes many tests at the end which illustrate 
all the configurations and how to set the confguration flags below.
"""

import logging
import os

# LOG_LEVEL = logging.INFO
LOG_LEVEL = "INFO"
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
# os_exit(0) program when an exception is raised
EXIT_PROGRAM_ON_EXCEPTION = True
SERVERLESS_PLATFORM_IS_AWS = True
# BFS collects a lot of information during DAG generation.
# Deallocate this information during DAG_generation when it is no longer needed.
#
# At the end_of_current_frontier in bfs() we deallocate the
# nodeIndex_to_partition_partitionIndex_group_groupIndex_map for each 
# npartition ode in the previous partition. (this map maintain information for 
# each node.) When the bfs search of the entire graph is complete we clear this
# map, i.e., the current parttion must be cleared since we will not have a chance
# to clear it as the previous_partition in bfs() (since bfs is done) - it
# is the only partiton whose nodes are in the map.
DEALLOCATE_BFS_MAIN_MAP_ON_THE_FLY = False
# Do the same for the nodes of the input graph. That is, we are done with graph nodes
# that correspond to a partition node in previous_partition. Delete each graph node using
# the ID of the partition node. Graph node i is stored with key i (in dictionary nodes{}). 
# When the bfs search of the entire graph is complete we clear the remaining 
# graph nodes, which correspond to the partition nodes in the final 
# partition - nodes.clear(). Noet: this covers both the case where we
# generate partitions and the case where we generate groups (as the nodes
# in the groups of a partition are the same as the nodes in the partition.)
DEALLOCATE_BFS_MAIN_MAP_ON_THE_FLY_BFS_GRAPH_NODES_ON_THE_FLY = False
# Right after we generate the next incremental DAG, we deallocate, we 
# deallocte the partitions/groups from partitions[]/groups[] that we 
# no longer need. That is, the previous partition / the groups in the 
# previous partition. These partition/groups have been output and we will
# not use them again. When the bfs search of the entire graph is complete 
# we clear the remaining partitions/groups, which correspond to the final 
# partition/groups in the final partition - partitions.clear()/nodes.clear()
DEALLOCATE_BFS_MAIN_MAP_ON_THE_FLY_BFS_PARTITIONS_GROUPS_NAMES = True
# In BFS_generate_DAG_info_incremental_groups.py, we clear the Senders
# and Receivers map for the groups in the previous previous partition
# del Group_senders[previous_previous_group] where previous_previous_group
# is a group in the previous previous partition. Likewise
# del Group_receivers[previous_previous_group].
# Partition_senders, Parttition_receievers, Group_senders, and Group_receivers
# represent the edges in the DAG, Partition/Group senders[x] are the partition/groups
# that receive inputs from partition/group x. Partition/Group receivers[x] are the 
# partition/groups that send inputs to partition/group x. We use these
# edges when we build the DAG. Durng incremental ADG generation,
# BFS_generate_DAG_info_incremental_groups.py and
# BFS_generate_DAG_info_incremental_partitions.py buld the ADG and 
# deallocate the Partition/Group senders and Partition/Group receivers
# that are no longer needed. Durng non-incremental DAG generation,
# the DAG is built after the bfs() search concludes and then the
# Partition/Group senders and Partition/Group receivers are deallocated.
DEALLOCATE_BFS_MAIN_MAP_ON_THE_FLY_BFS_SENDERS_AND_RECEIVERS = False
# We may want to do the above deallocations only for large input graphs
# so we guard the dellocations above with 
# (num_nodes_in_graph > DAG_executor_constants.THRESHOLD_FOR_DEALLOCATING_ON_THE_FLY)
THRESHOLD_FOR_DEALLOCATING_ON_THE_FLY = 1
#
# True if we are not using Lambdas, i.e., executing tasks with threads or processes
# local, i.e., on one machine.
RUN_ALL_TASKS_LOCALLY = True         # vs run tasks remotely (in Lambdas)
# True if we want to bypass the call to lambda_client.invoke() so that we
# do not actually create a real Lambda; instead, invoke_lambda_DAG_executor()
# in invoker.y will call lambda_handler(payload_json,None) directly, where
# lambda_handler() is defned locally in invoker.py, i.e., is not the actual
# handler, which is defined in handlerDAG.py. This lets us test the code
# for real lambdas without actually creating real Lambdas.
# Note: if this is True then RUN_ALL_TASKS_LOCALLY must be False. 
# This is assserted below.
BYPASS_CALL_TO_INVOKE_REAL_LAMBDA = (not RUN_ALL_TASKS_LOCALLY) and True
# True if synch objects are stored locally, i.e., in the memory of the single
# machine on which the threads are executing.  If we are using multiprocessing
# or Lambdas, this must be False. When False, the synch objects are stored
# on the tcp_server or in InfiniX lambdas.
# Note: When using partitions instead of groups, partition i 
# has a collapse to partition i+1, so there are no synch objects
# needed when we are using partitions, so it does not matter
# whether we set STORE_FANINS_FANINNBS_LOCALLY to True or False.
STORE_FANINS_FANINNBS_LOCALLY = True
# True when all FanIn and FanInNB objects are created locally or on the
# tcp_server or IniniX all at once at the start of the DAG execution. If
# False, synch objects are created on the fly, i.e, we execute create-and-fanin
# operations that create a synch object if it has not been created yet and then
# execute a Fan_in operaation on the created object.
# 
# This must be false if we aer doing incremental_DAG_generation; this is assserted below.
CREATE_ALL_FANINS_FANINNBS_ON_START = True

# True if the DAG is executed by a "pool" of threads/processes. False, if we are
# using Lambdas or we are using threads to simulate the use of Lambdas. In the latter
# case, instead of, e.g., starting a Lambda at fan_out operations, we start a thread.
# This results in the creation of many threads and is only use to test the logic 
# of the Lambda code.
USING_WORKERS = True
# True when we are not using Lambas and tasks are executed by threads instead of processes. 
# False when we are not using lambdas and are using multiprocesssing 
USING_THREADS_NOT_PROCESSES = True
# When USING_WORKERS, this is how many threads or processes in the pool.
# When not using workers, this value is ignored.
NUM_WORKERS = 1
# Use one or more worker processes (NUM_WORKERS) with one or more threads
USE_MULTITHREADED_MULTIPROCESSING = False
NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING = 2

# if using lambdas to store synch objects, run tcp_server_lambda.
# if store in regular python functions instead of real Lambdas
# set using_Lambda_Function_Simulator = True
FANIN_TYPE = "DAG_executor_FanIn"
FANINNB_TYPE = "DAG_executor_FanInNB"
PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer"
#FANIN_TYPE = "DAG_executor_FanIn_Select"
#FANINNB_TYPE = "DAG_executor_FanInNB_Select"
#PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer_Select"

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
STORE_SYNC_OBJECTS_IN_LAMBDAS = False
USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS = False
SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS = False
# use orchestrator to invoke functions (e.g., when all fanin/fanout results are available)
USING_DAG_ORCHESTRATOR = False
# map ech synch object by name to the function it resided in. if we create
# all objects on start we msut map the objects to function so we can get the
# function an onject is in. If we do not create objects on start then
# we will crate them on the fly. We can still map he objects to functions -
# we will just have to create the object in the funtion on the first function
# invocation. If we do not map objects, then we will/can only invoke tge
# function that contains the possibly pre-created object once. 
MAP_OBJECTS_TO_LAMBDA_FUNCTIONS = False
# We can use an anonymous simulated function or a single named lambda deployment.
# In this case, we can invoke the function only once snce we cannot
# refer to a function instance by name, i.e., by index for simuated functions and 
# by uniqueu deploment name for real lambda functions. For simuated functions
# we do not create an indexed list of functions, and for real Lambdas we
# just have one deployment. 
# Note: if MAP_OBJECTS_TO_LAMBDA_FUNCTIONS then USE_ANONYMOUS_LAMBDA_FUNCTIONS 
# must be False. We map objects to function so we can invoke a function instance 
# more than once when we access an object more than once. If we 
# USE_ANONYMOUS_LAMBDA_FUNCTIONS then we cannot access a specific function
# (by name or by index).
# ToDo: integrate USE_SINGLE_LAMBDA_FUNCTION with this mapping stuff. that
# is, map names to lambda functions, and sometimes there is only one function.
USE_ANONYMOUS_LAMBDA_FUNCTIONS = False
# So if create on start then must map objects and cannot use anonymous functions.
# If want to use anonymous functions then no create objects on statr and no mapping.

# use a single lambda function to store all of the synchroization objects
# to make an easy test case. This cannot be used when using the function 
# simulators or using the DAG_orchestrator.
# Using this is ToDo - search for it and see comments
USE_SINGLE_LAMBDA_FUNCTION = False

# For all: remote objects, using select objects:
# 1. RUN_ALL_TASKS_LOCALLY = True, create objects on start = True:
# TTFFTF: no trigger and no DAG_orchestrator, but map objects 
# (anon is false) and create objects on start
# variations:
# a. change D_O to T, 
# b. change map to F, and anon to T: Note: no function lock since anon caled only once
# c. change D_O to F, map F, anon T: Note: no function lock since anon caled only once
#
# 2. RUN_ALL_TASKS_LOCALLY = False (s0 USING_WORKERS = False), create objects on start = True:
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

try:
    msg = "[Error]: Configuration error: if USING_WORKERS then must RUN_ALL_TASKS_LOCALLY."
    assert not (USING_WORKERS and not RUN_ALL_TASKS_LOCALLY), msg
except AssertionError:
    logger.exception("[Error]: assertion failed")
    if EXIT_PROGRAM_ON_EXCEPTION:
        logging.shutdown()
        os._exit(0)
#assertOld:
#if USING_WORKERS and not RUN_ALL_TASKS_LOCALLY:
#    logger.error("[Error]: Configuration error: if USING_WORKERS then must RUN_ALL_TASKS_LOCALLY.")
#    logging.shutdown()
#    os._exit(0)  

try:
    msg = "[Error]: Configuration error: if not RUN_ALL_TASKS_LOCALLY (i.e., using real lambdas) then objects cannot be stored locally."
    assert not (not RUN_ALL_TASKS_LOCALLY and STORE_FANINS_FANINNBS_LOCALLY), msg
except AssertionError:
    logger.exception("[Error]: assertion failed")
    if EXIT_PROGRAM_ON_EXCEPTION:
        logging.shutdown()
        os._exit(0)
#assertOld:
#if not RUN_ALL_TASKS_LOCALLY and STORE_FANINS_FANINNBS_LOCALLY:
#    logger.error("[Error]: Configuration error: if not RUN_ALL_TASKS_LOCALLY (i.e., using real lambdas) then objects cannot be stored locally.")
#    logging.shutdown()
#   os._exit(0) 

try:
    msg = "[Error]: Configuration error: if BYPASS_CALL_TO_INVOKE_REAL_LAMBDA then must be running real Lambdas" \
        + " i.e., not RUN_ALL_TASKS_LOCALLY."
    assert not (BYPASS_CALL_TO_INVOKE_REAL_LAMBDA and RUN_ALL_TASKS_LOCALLY), msg
except AssertionError:
    logger.exception("[Error]: assertion failed")
    if EXIT_PROGRAM_ON_EXCEPTION:
        logging.shutdown()
        os._exit(0)
#assertOld:
#if BYPASS_CALL_TO_INVOKE_REAL_LAMBDA and RUN_ALL_TASKS_LOCALLY:
#   logger.error("[Error]: Configuration error: if BYPASS_CALL_TO_INVOKE_REAL_LAMBDA then must be running real Lambdas"
#       + " i.e., not RUN_ALL_TASKS_LOCALLY.")
#    logging.shutdown()
#    os._exit(0)  

try:
    msg = "[Error]: Configuration error: if USING_WORKERS and not USING_THREADS_NOT_PROCESSES" \
        + " then STORE_FANINS_FANINNBS_LOCALLY must be False."
    assert not (USING_WORKERS and not USING_THREADS_NOT_PROCESSES and STORE_FANINS_FANINNBS_LOCALLY), msg
except AssertionError:
    logger.exception("[Error]: assertion failed")
    if EXIT_PROGRAM_ON_EXCEPTION:
        logging.shutdown()
        os._exit(0)
#assertOld:
#if USING_WORKERS and not USING_THREADS_NOT_PROCESSES:
#    if STORE_FANINS_FANINNBS_LOCALLY:
#        # When using worker processed, synch objects must be stored remoely
#        logger.error("[Error]: Configuration error: if USING_WORKERS and not USING_THREADS_NOT_PROCESSES"
#            + " then STORE_FANINS_FANINNBS_LOCALLY must be False.")
#        logging.shutdown()
#       os._exit(0)

try:
    msg = "[Error]: Configuration error: if CREATE_ALL_FANINS_FANINNBS_ON_START" + " then map_objects_to_functions must be True."
    # if create sync objects on start and executing tasks in lambdas "
    # then we must map them to function so that we can determine the 
    # function an object is in.
    assert not (CREATE_ALL_FANINS_FANINNBS_ON_START and not RUN_ALL_TASKS_LOCALLY and STORE_SYNC_OBJECTS_IN_LAMBDAS and not MAP_OBJECTS_TO_LAMBDA_FUNCTIONS), msg
except AssertionError:
    logger.exception("[Error]: assertion failed")
    if EXIT_PROGRAM_ON_EXCEPTION:
        logging.shutdown()
        os._exit(0)
#assertOld:
#if CREATE_ALL_FANINS_FANINNBS_ON_START and not RUN_ALL_TASKS_LOCALLY and STORE_SYNC_OBJECTS_IN_LAMBDAS:
#    if not MAP_OBJECTS_TO_LAMBDA_FUNCTIONS:
#        # if create sync objects on start and executing tasks in lambdas "
#        # then we must map them to function so that we can determine the 
#        # function an object is in.
#        logger.error("[Error]: Configuration error: if CREATE_ALL_FANINS_FANINNBS_ON_START"
#            + " then map_objects_to_functions must be True.")
#        logging.shutdown()
#        os._exit(0)

try:
    msg = "[Error]: Configuration error: if MAP_OBJECTS_TO_LAMBDA_FUNCTIONS" + " then USE_ANONYMOUS_LAMBDA_FUNCTIONS must be False."
    # if create sync objects on start then we must map them to function so
    # that we can determine the function an object is in.
    assert not (MAP_OBJECTS_TO_LAMBDA_FUNCTIONS and USE_ANONYMOUS_LAMBDA_FUNCTIONS), msg
except AssertionError:
    logger.exception("[Error]: assertion failed")
    if EXIT_PROGRAM_ON_EXCEPTION:
        logging.shutdown()
        os._exit(0)
#assertOld:
#if MAP_OBJECTS_TO_LAMBDA_FUNCTIONS:
#    if USE_ANONYMOUS_LAMBDA_FUNCTIONS:
#        # if create sync objects on start then we must map them to function so
#        # that we can determine the function an object is in.
#        logger.error("[Error]: Configuration error: if MAP_OBJECTS_TO_LAMBDA_FUNCTIONS"
#            + " then USE_ANONYMOUS_LAMBDA_FUNCTIONS must be False.")
#        logging.shutdown()
#        os._exit(0)

try:
    msg = "[Error]: Configuration error: if SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS" + " then not RUN_ALL_TASKS_LOCALLY must be True."
    # if create sync objects on start then we must map them to function so
    # that we can determine the function an object is in.
    assert not (SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS and RUN_ALL_TASKS_LOCALLY), msg
except AssertionError:
    logger.exception("[Error]: assertion failed")
    if EXIT_PROGRAM_ON_EXCEPTION:
        logging.shutdown()
        os._exit(0)
#assertOld:
#if SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS:
#    if RUN_ALL_TASKS_LOCALLY:
#        # if create sync objects on start then we must map them to function so
#        # that we can determine the function an object is in.
#        logger.error("[Error]: Configuration error: if SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS"
#            + " then not RUN_ALL_TASKS_LOCALLY must be True.")
#        logging.shutdown()
 #       os._exit(0)

##########################################
###### PageRank settings start here ######
##########################################

#fname = "graph_3000"
# whiteboard graph:
# pagerank_graph_file_name = "graph_WB"
# These are whiteboard graphs with various extensions
# that, e.g., add connected components (CC)
# pagerank_graph_file_name = "graph_22N_2CC"
# pagerank_graph_file_name = "graph_23N"
# pagerank_graph_file_name = "graph_24N_3CC"
PAGERANK_GRAPH_FILE_NAME = "graph_24N_3CC_fanin"   # extended wb w/ 3CC and fanin at end
# pagerank_graph_file_name = "graph_2N_2CC"  # 2 nodes (CCs) no edges
# pagerank_graph_file_name = "graph_3N_3CC"  # 3 nodes (CCs) no edges
# pagerank_graph_file_name = "graph_2N"
# pagerank_graph_file_name = "graph_1N"
# pagerank_graph_file_name = "graph_3P"
# pagerank_graph_file_name = "graph_27_loops"

# Indicates that we are computing pagerank and thus that the pagerank
# options are active and pagerank assserts should hold
COMPUTE_PAGERANK = False
# used in BFS_pagerank. For non-loops, we only need 1 iteration
NUMBER_OF_PAGERANK_ITERATIONS_FOR_PARTITIONS_GROUPS_WITH_LOOPS = 10
NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG = "PR1_1"

# pagerank values will be saved so we can check them after execution
# in DAG_executor_check_pagerank.py

#brc: ToDo: requires a global pagerank result so need worker threads?
CHECK_PAGERANK_OUTPUT = COMPUTE_PAGERANK and RUN_ALL_TASKS_LOCALLY and (USING_WORKERS or not USING_WORKERS) and USING_THREADS_NOT_PROCESSES and True

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
SAME_OUTPUT_FOR_ALL_FANOUT_FANIN = not COMPUTE_PAGERANK

# True if DAG generation and DAG_execution are overlapped. 
USE_INCREMENTAL_DAG_GENERATION = COMPUTE_PAGERANK and False

try:
    msg = "[Error]: Configuration error: incremental_DAG_generation" + " requires not CREATE_ALL_FANINS_FANINNBS_ON_START" + " i.e., create synch objects on the fly since we don't know all of the synch objects " + " at the start (the DAG is not complete)"
    assert not (COMPUTE_PAGERANK and USE_INCREMENTAL_DAG_GENERATION and CREATE_ALL_FANINS_FANINNBS_ON_START), msg
except AssertionError:
    logger.exception("[Error]: assertion failed")
    if EXIT_PROGRAM_ON_EXCEPTION:
        logging.shutdown()
        os._exit(0)
# assertOld:
#if COMPUTE_PAGERANK and USE_INCREMENTAL_DAG_GENERATION and CREATE_ALL_FANINS_FANINNBS_ON_START:
#    logger.error("[Error]: Configuration error: incremental_DAG_generation"
#        + " requires not CREATE_ALL_FANINS_FANINNBS_ON_START"
#        + " i.e., create synch objects on the fly since we don't know all of the synch objects "
#        + " at the start (the DAG is not complete)")
#    logging.shutdown()
#    os._exit(0)  

# generate next DAG when num_incremental_DAGs_generated_since_base_DAG mod 
# incremental_interval == 0. For example, if we set this
# value to 2, after we generate the DAG with a complete 
# partition 1 and an incomplete partition 2, i.e., the base DAG, we 
# publish the base DAG and start the ADG_execution_driver. We will then
# generate the DAG with partitions, 1, and 2, whcih are complete and an
# incomplete partition 3. This is the first ADG generated sicen the base
# DAG and since 1%2 is not 0, we do not publish this DAG. We will 
# generate a new DAG with process partition 4 and publish this DAG 
# since 4%2 is 0. We will publish DAGs 6, 8, 10, ... etc.
# Note: We publish the first DAG, which is a complete DAG with 
# one partition or a DAG with a complete first partition and 
# and incomplete second partition, then we generate the 
# next DAG with complete paritions 1 and 2 and incomplete
# partition 3 and we increment num_DAGs_generated to 1. we will 
# publish the next generated DAG again when num_incremental_DAGs_generated_since_base_DAG mod
# INCREMENTAL_DAG_DEPOSIT_INTERVAL is 0. So if INCREMENTAL_DAG_DEPOSIT_INTERVAL
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
INCREMENTAL_DAG_DEPOSIT_INTERVAL = 2

try:
    msg = "[Error]: Configuration error: INCREMENTAL_DAG_DEPOSIT_INTERVAL" + " must be >= 1. We mod by INCREMENTAL_DAG_DEPOSIT_INTERVAL so it" + " cannot be 0 and using a negative number makes no sense."
    assert not (INCREMENTAL_DAG_DEPOSIT_INTERVAL < 1), msg
except AssertionError:
    logger.exception("[Error]: assertion failed")
    if EXIT_PROGRAM_ON_EXCEPTION:
        logging.shutdown()
        os._exit(0)
#assertOld:
#if INCREMENTAL_DAG_DEPOSIT_INTERVAL < 1:
#    logger.error("[Error]: Configuration error: INCREMENTAL_DAG_DEPOSIT_INTERVAL"
#         + " must be >= 1. We mod by INCREMENTAL_DAG_DEPOSIT_INTERVAL so it"
#         + " cannot be 0 and using a negative number makes no sense.")
#    logging.shutdown()
#    os._exit(0)

# True if we are clustering fanouts that satisfy the cluster criteria
ENABLE_RUNTIME_TASK_CLUSTERING = COMPUTE_PAGERANK and False
MIN_PARTITION_GROUP_SIZE_FOR_CLUSTERING  = 5
MAX_SIZE_OF_OUTPUT_TO_FANOUT_TASK = 10000

#brc: ToDo: what should this be? Used as capacity of boundedbuffer
# Note: Pythin has no max Int
#brc: ToDo: Make a bounded_buffer with a dynamic buffer 
WORK_QUEUE_SIZE_FOR_INCREMENTAL_DAG_GENERATION_WITH_WORKER_PROCESSES =  2**10-1

try:
    msg = "[Error]: Configuration error: if SAME_OUTPUT_FOR_ALL_FANOUT_FANIN" + " then must be computing pagerank."
    assert not (not SAME_OUTPUT_FOR_ALL_FANOUT_FANIN and not COMPUTE_PAGERANK), msg
except AssertionError:
    logger.exception("[Error]: assertion failed")
    if EXIT_PROGRAM_ON_EXCEPTION:
        logging.shutdown()
        os._exit(0)
#assertOld:
#if not SAME_OUTPUT_FOR_ALL_FANOUT_FANIN and not COMPUTE_PAGERANK:
#    logger.error("[Error]: Configuration error: if SAME_OUTPUT_FOR_ALL_FANOUT_FANIN"
#        + " then must be computing pagerank.")
#    logging.shutdown()
#    os._exit(0)

# For PageRank:
# set TASKS_USE_RESULT_DICTIONARY_PARAMETER = True
# and SAME_OUTPUT_FOR_ALL_FANOUT_FANIN = False.
#
# True when executed task uses a dictionary parameter that contains its inputs
# instead of a tuple Dask-style. This is True for the PageRank. For pagerank
# we use a single pagerank task and, if using lambdas, a single lambda excutor.
# PageRank tasks have varying numbers of inputs (for fanoins/faninNBs) that are
# passed to the PageRank task in a dictionary.
TASKS_USE_RESULT_DICTIONARY_PARAMETER = COMPUTE_PAGERANK and True

# For PageRank:
# When we run_tasks_locally and we use threads to simulate lambdas or
# worker threads, instad of inputting each tasks's partition separately
# when the task suns, we have one global shared array with all the 
# partitions/groups and the threads access that array when they do their
# tasks.
USE_SHARED_PARTITIONS_GROUPS = COMPUTE_PAGERANK and False

try:
    msg = "[Error]: Configuration error: if using a single shared array of" + " partitions or groups then must run_tasks_locally and be USING_THREADS_NOT_PROCESSES."
    assert not (COMPUTE_PAGERANK and (USE_SHARED_PARTITIONS_GROUPS and not RUN_ALL_TASKS_LOCALLY)), msg
except AssertionError:
    logger.exception("[Error]: assertion failed")
    if EXIT_PROGRAM_ON_EXCEPTION:
        logging.shutdown()
        os._exit(0)
#assertOld:
#if COMPUTE_PAGERANK and (USE_SHARED_PARTITIONS_GROUPS and not RUN_ALL_TASKS_LOCALLY)):#
#if COMPUTE_PAGERANK and (USE_SHARED_PARTITIONS_GROUPS and not RUN_ALL_TASKS_LOCALLY):
#    logger.error("[Error]: Configuration error: if using a single shared array of"
#        + " partitions or groups then must run_tasks_locally and be USING_THREADS_NOT_PROCESSES.")
#    logging.shutdown()
#    os._exit(0)

# For PageRank:
# Execute page rank partitions or execute page rank groups
# If True use groups else use partitions
USE_PAGERANK_GROUPS_PARTITIONS = COMPUTE_PAGERANK and True

# For PageRank:
# Use a struct of arrays to improve cache performance
USE_STRUCT_OF_ARRAYS_FOR_PAGERANK = COMPUTE_PAGERANK and False

# For PageRank:
# Use a multithreaded BFS where bfs() is generating the next group
# or partition and depositing this in a buffer to be withdrawn
# by a separate thread that adds information about this group/partition
# to the DAG_info. So partitions/groups can be generated concurrently 
# with generating the DAG_info object. This is not to be confused with 
# incremental DAG generation in which the DAG is executed concurrently 
# with bfs() generating the partitions/groups and the DAG_info object.
# Consider also the combination of USE_INCREMENTAL_DAG_GENERATION and 
# USE_MUTLITHREADED_BFS. (an incremental ADG generator thread runs 
# concurrently with bfs. In this case, we would need to put the 
# code in bfs that runs after the call to generate the next incremental
# part of the DAG (deposit the new incremental DAG in the buffer where 
# the DAG_executor withdraws it for execution, etc) at th end of the 
# code/method that generates the incremental DAG. That is, the bfs() thread
# just calls the generate incremental DAG method and continues, while
# the incemental DAG generator code generates the next version of the DAG
# and does what needs to be done with the DAG, instead of returning the 
# DAG to bfs() and letting bfs() deal with the new DAG.)
USE_MUTLITHREADED_NONINCREMENTAL_BFS = COMPUTE_PAGERANK and False

try:
    msg = "[Error]: Configuration error: if USE_MUTLITHREADED_BFS" + " then must not USE_INCREMENTAL_DAG_GENERATION ."
    assert not (USE_MUTLITHREADED_NONINCREMENTAL_BFS and USE_INCREMENTAL_DAG_GENERATION), msg
except AssertionError:
    logger.exception("[Error]: assertion failed")
    if EXIT_PROGRAM_ON_EXCEPTION:
        logging.shutdown()
        os._exit(0)
#assertOld:
#if USE_MUTLITHREADED_BFS and USE_INCREMENTAL_DAG_GENERATION:
#    logger.error("[Error]: Configuration error: if USE_MUTLITHREADED_BFS"
#        + " then must not USE_INCREMENTAL_DAG_GENERATION .")
#    logging.shutdown()
#    os._exit(0)

try:
    msg = "[Error]: Configuration error: if USE_STRUCT_OF_ARRAYS_FOR_PAGERANK" + " then must USE_SHARED_PARTITIONS_GROUPS."
    assert not (COMPUTE_PAGERANK and (USE_STRUCT_OF_ARRAYS_FOR_PAGERANK and not USE_SHARED_PARTITIONS_GROUPS)), msg
except AssertionError:
    logger.exception("[Error]: assertion failed")
    if EXIT_PROGRAM_ON_EXCEPTION:
        logging.shutdown()
        os._exit(0)
#assertOld:
#if COMPUTE_PAGERANK and (USE_STRUCT_OF_ARRAYS_FOR_PAGERANK and not USE_SHARED_PARTITIONS_GROUPS):
#    logger.error("[Error]: Configuration error: if USE_STRUCT_OF_ARRAYS_FOR_PAGERANK"
#        + " then must USE_SHARED_PARTITIONS_GROUPS.")
#    logging.shutdown()
#    os._exit(0)

# When we use real lamdas, instead of reading the individual group 
# or partition node files from cloud storage as we need them in BFS_agerank.py, 
# we can read all the groups or partitions at the start and pass them along
# to the lambdas that are started. There is a limit on the total of the sizes
# of these files. This is not checked yet. We may want to read the files
# in batches, on demnand, etc.

# Note: Use this when running real lambdas and avoiding cloud storage 
# for I/O of the groups or partitions.
INPUT_ALL_GROUPS_PARTITIONS_AT_START = COMPUTE_PAGERANK and (
   not RUN_ALL_TASKS_LOCALLY and (not BYPASS_CALL_TO_INVOKE_REAL_LAMBDA) and (not USE_INCREMENTAL_DAG_GENERATION)
   ) and True

# Note: Use this to test real lambda code locally without using real lambdas
# INPUT_ALL_GROUPS_PARTITIONS_AT_START = COMPUTE_PAGERANK and (
#     not RUN_ALL_TASKS_LOCALLY and (BYPASS_CALL_TO_INVOKE_REAL_LAMBDA) and (not USE_INCREMENTAL_DAG_GENERATION)
#     ) and True

A1 = A1_Server = A1_FunctionSimulator = A1_SingleFunction = A1_Orchestrator = False
A2 = False
A3 = A3_S = A3_FunctionSimulator = A3_SingleFunction = A3_Orchestrator = False
A4_L = A4_R = False
A5 = A6 = False

# These are used to shorten the expressions in the configurations
not_using_lambda_options =  not USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS and (
    not SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS) and (
    not USING_DAG_ORCHESTRATOR) and (
    not USE_SINGLE_LAMBDA_FUNCTION)
# Note: USING_DAG_ORCHESTRATOR while SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS
# is a non-Wukong scheme for managing lambdas - sync objects are stored in Lambdas 
# and when using SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS the objects
# trigger their tasks to run within the same lambda. A1_Wukong uses
# lambas to run tasks at fanouts/faninNBs, and stores synch objects on the 
# server or in lambdas. A1_Wukong may use the DAG_orchestrator to manage the 
# sync objects/lambdas. 

# Note: for all configurations, set CREATE_ALL_FANINS_FANINNBS_ON_START = True/False

# Configurations:

# objects can be stored on the tcp server or in lambdas
A1_Wukong = not RUN_ALL_TASKS_LOCALLY and not USING_WORKERS and not STORE_FANINS_FANINNBS_LOCALLY

A1_Wukong_ObjectsOnServer = A1_Wukong and not STORE_SYNC_OBJECTS_IN_LAMBDAS and (
    not not_using_lambda_options)
# This is Wukong style with sync objects stored on Server
# FANIN_TYPE = "DAG_executor_FanIn"
# FANINNB_TYPE = "DAG_executor_FanInNB"
# run tcp_server
# Set SERVERLESS_SYNC to True in wukongdnc constants
A1_Wukong_ObjectsInRealLambdas_UseManyLambdaFunction = A1_Wukong and STORE_SYNC_OBJECTS_IN_LAMBDAS and (
    not not_using_lambda_options)
# This is Wukong style with sync objects stored in two or more Lambdas to balance the load.
# Not using lamba simulators, just mapping objects (names) to lambdas.
# If USE_SINGLE_LAMBDA_FUNCTION then there is a single lambda that stores all sync objects
# which makes setup on AWS simpler (i.e., using one distribution).
# FANIN_TYPE = "DAG_executor_FanIn_Select"
# FANINNB_TYPE = "DAG_executor_FanInNB_Select"
# run tcp_server_lambda
# Set SERVERLESS_SYNC to True in wukongdnc constants
A1_Wukong_ObjectsInRealLambdas_UseSingleLambdaFunction = A1_Wukong and STORE_SYNC_OBJECTS_IN_LAMBDAS and (
    USE_SINGLE_LAMBDA_FUNCTION) and (
    not USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS) and (
    not SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS) and (
    not USING_DAG_ORCHESTRATOR)
# This is Wukong style with all sync objects stored in a single Lambda function 
#   to make AWS setup easier,
# Note: We do not use USE_SINGLE_LAMBDA_FUNCTION when we use simulated lambdas to store objects.
#   Using a single function is handy when we have to run lambdas on AWS, i.e., we only need
#   one deployment. With simulated lambdas, having multiple "deployments" is not painful
#   (to create the deployments, which aer just seperate Python functions.) 
# FANIN_TYPE = "DAG_executor_FanIn_Select"
# FANINNB_TYPE = "DAG_executor_FanInNB_Select"
# run tcp_server_lambda
# Set SERVERLESS_SYNC to True in wukongdnc constants
A1_Wukong_ObjectsInRealLambdas_UsingOrchestator = A1_Wukong and STORE_SYNC_OBJECTS_IN_LAMBDAS and (
    not USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS) and (
    not SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS) and (
    not USE_SINGLE_LAMBDA_FUNCTION) 
# This is Wukong style with all sync objects stored in a lambda functions and orchestrated.
# FANIN_TYPE = "DAG_executor_FanIn_Select"
# FANINNB_TYPE = "DAG_executor_FanInNB_Select"
# run tcp_server_lambda
# Set SERVERLESS_SYNC to True in wukongdnc constants
A1_Wukong_ObjectInSimulatedLambdas = A1_Wukong and STORE_SYNC_OBJECTS_IN_LAMBDAS and (
    USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS) and (
    not SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS) and (
    not USE_SINGLE_LAMBDA_FUNCTION) and (
    not USING_DAG_ORCHESTRATOR)
# FANIN_TYPE = "DAG_executor_FanIn_Select"
# FANINNB_TYPE = "DAG_executor_FanInNB_Select"
# run tcp_server_lambda
# Set SERVERLESS_SYNC to True in wukongdnc constants
A1_Wukong_ObjectInSimulatedLambdas_UsingOrchestator = A1_Wukong and STORE_SYNC_OBJECTS_IN_LAMBDAS and (
    USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS) and (
    USING_DAG_ORCHESTRATOR) and (
    not SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS) and (
    not USE_SINGLE_LAMBDA_FUNCTION) 
# FANIN_TYPE = "DAG_executor_FanIn_Select"
# FANINNB_TYPE = "DAG_executor_FanInNB_Select"
# run tcp_server_lambda
# Set SERVERLESS_SYNC to True in wukongdnc constants

# Note: Currently we are assuming USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS is True
# when we use the orchestrator. 
A1_Orchestrate_SyncObjectsandTasksinRealLambdas = not RUN_ALL_TASKS_LOCALLY and not USING_WORKERS and not STORE_FANINS_FANINNBS_LOCALLY and (
    STORE_SYNC_OBJECTS_IN_LAMBDAS) and (
    USING_DAG_ORCHESTRATOR) and (
    not USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS) and (
    not SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS) and (
    not USE_SINGLE_LAMBDA_FUNCTION)
# FANIN_TYPE = "DAG_executor_FanIn_Select"
# FANINNB_TYPE = "DAG_executor_FanInNB_Select"
# run tcp_server_lambda
# Set SERVERLESS_SYNC to True in wukongdnc constants

A1_Orchestrate_SyncObjectsandTasksinSimulatedLambdas = not RUN_ALL_TASKS_LOCALLY and not USING_WORKERS and not STORE_FANINS_FANINNBS_LOCALLY and (
    STORE_SYNC_OBJECTS_IN_LAMBDAS) and (
    USING_DAG_ORCHESTRATOR) and (
    USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS) and SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS and (
    not USE_SINGLE_LAMBDA_FUNCTION)
# FANIN_TYPE = "DAG_executor_FanIn_Select"
# FANINNB_TYPE = "DAG_executor_FanInNB_Select"
# run tcp_server_lambda
# Set SERVERLESS_SYNC to True in wukongdnc constants

using_threads_to_simulate_lambdas = RUN_ALL_TASKS_LOCALLY and not USING_WORKERS

# using threads to simulate lambdas and store sync objects locally (not on server or in lambdas)
A2 = using_threads_to_simulate_lambdas and STORE_FANINS_FANINNBS_LOCALLY and not_using_lambda_options
# set FANIN_TYPE = "DAG_executor_FanIn_Select" or "DAG_executor_FanIn"
# set FANINNB_TYPE = "DAG_executor_FanInNB_Select" or "DAG_executor_FanInNB"
# Set SERVERLESS_SYNC to False in wukongdnc constants

# using threads to simulate lambdas that execute tasks and store sync objects remotely (on server or in lambdas)
# Note: All tasks executed by Wukong style lambdas that are invoked at fanouts/fanins.
A3 = using_threads_to_simulate_lambdas and not STORE_FANINS_FANINNBS_LOCALLY

A3_ObjectsOnServer = A3 and not_using_lambda_options
# set FANIN_TYPE = = "DAG_executor_FanIn_Select" or "DAG_executor_FanIn"
# set FANINNB_TYPE = "DAG_executor_FanInNB_Select" or "DAG_executor_FanInNB"
# Set SERVERLESS_SYNC to False in wukongdnc constants
A3_ObjectsInRealLambdas_UseManyLambdaFunction = A3 and STORE_SYNC_OBJECTS_IN_LAMBDAS and (
    not not_using_lambda_options)
# FANIN_TYPE = "DAG_executor_FanIn_Select"
# FANINNB_TYPE = "DAG_executor_FanInNB_Select"
# run tcp_server_lambda
# Set SERVERLESS_SYNC to False in wukongdnc constants    
A3_ObjectsInRealLambdas_UseSingleLambdaFunction = A3 and STORE_SYNC_OBJECTS_IN_LAMBDAS and (
    USE_SINGLE_LAMBDA_FUNCTION) and (
    not USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS) and (
    not SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS) and (
    not USING_DAG_ORCHESTRATOR)
# FANIN_TYPE = "DAG_executor_FanIn_Select"
# FANINNB_TYPE = "DAG_executor_FanInNB_Select"
# run tcp_server_lambda
# Set SERVERLESS_SYNC to False in wukongdnc constants 
A3_ObjectsInRealLambdas_UsingOrchestator = A3 and STORE_SYNC_OBJECTS_IN_LAMBDAS and (
    not USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS) and (
    not SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS) and (
    not USE_SINGLE_LAMBDA_FUNCTION) 
# This is Wukong style with all sync objects stored in a lambda functions and orchestrated.
# FANIN_TYPE = "DAG_executor_FanIn_Select"
# FANINNB_TYPE = "DAG_executor_FanInNB_Select"
# run tcp_server_lambda
# Set SERVERLESS_SYNC to True in wukongdnc constants

A3_ObjectInSimulatedLambdas = A3 and STORE_SYNC_OBJECTS_IN_LAMBDAS and (
    USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS) and (
    not SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS) and (
    not USE_SINGLE_LAMBDA_FUNCTION) and (
    not USING_DAG_ORCHESTRATOR)
# FANIN_TYPE = "DAG_executor_FanIn_Select"
# FANINNB_TYPE = "DAG_executor_FanInNB_Select"
# run tcp_server_lambda
# Set SERVERLESS_SYNC to False in wukongdnc constants
A3_ObjectInSimulatedLambdas_UsingOrchestator = A3 and STORE_SYNC_OBJECTS_IN_LAMBDAS and (
    USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS) and (
    USING_DAG_ORCHESTRATOR) and (
    not SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS) and (
    not USE_SINGLE_LAMBDA_FUNCTION) 
# FANIN_TYPE = "DAG_executor_FanIn_Select"
# FANINNB_TYPE = "DAG_executor_FanInNB_Select"
# run tcp_server_lambda
# Set SERVERLESS_SYNC to True in wukongdnc constants

A4 = RUN_ALL_TASKS_LOCALLY and USING_WORKERS and USING_THREADS_NOT_PROCESSES
A4_ObjectsStoredLocally = A4 and STORE_FANINS_FANINNBS_LOCALLY and (
    not STORE_SYNC_OBJECTS_IN_LAMBDAS) and not_using_lambda_options
# set NUM_WORKERS
# no tcp_server since storing locally
# Set SERVERLESS_SYNC to False in wukongdnc constants
A4_ObjectsStoredRemotely = A4 and not STORE_FANINS_FANINNBS_LOCALLY and (
    not STORE_SYNC_OBJECTS_IN_LAMBDAS) and not_using_lambda_options
# set NUM_WORKERS
# set FANIN_TYPE = "DAG_executor_FanIn_Select" or "DAG_executor_FanIn"
# set FANINNB_TYPE = "DAG_executor_FanInNB_Select" or "DAG_executor_FanInNB"
# set PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer"
# run tcp_server
# Set SERVERLESS_SYNC to False in wukongdnc constants
# Note: We do not run tcp_server_lambda so while we can use the "Select" 
# objects they are not stored in lambdas, they are regular objects on tcp_server.

# Note about A4: For A4, we are using worker threads, which is not going to 
# generate speedup in Python.

A5 = RUN_ALL_TASKS_LOCALLY and USING_WORKERS and not USING_THREADS_NOT_PROCESSES
A5_ObjectsStoredRemotely = A5 and not STORE_FANINS_FANINNBS_LOCALLY and (
    not STORE_SYNC_OBJECTS_IN_LAMBDAS) and not_using_lambda_options
# set NUM_WORKERS
# set FANIN_TYPE = "DAG_executor_FanIn_Select" or "DAG_executor_FanIn"
# set FANINNB_TYPE = "DAG_executor_FanInNB_Select" or "DAG_executor_FanInNB"
# set PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer_Select" or PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer"
# run tcp_server.
# Set SERVERLESS_SYNC to False in wukongdnc constants
# Note: We do not run tcp_server_lambda so while we can use the "Select" 
# objects they are not stored in lambdas, they are regular objects on tcp_server.

# Note about A5: For A5, we are using worker processes, which requires sync objects to be
# stored remotely. Objects can only be stored on the server.

A6 = RUN_ALL_TASKS_LOCALLY and USE_MULTITHREADED_MULTIPROCESSING and USING_WORKERS and not USING_THREADS_NOT_PROCESSES and not STORE_FANINS_FANINNBS_LOCALLY and not_using_lambda_options
# set NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING
# set FANIN_TYPE = "DAG_executor_FanIn_Select" or "DAG_executor_FanIn"
# set FANINNB_TYPE = "DAG_executor_FanInNB_Select" or "DAG_executor_FanInNB"
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

# Suggested Assert using worker processes  ==> store objects remotely
# Suggested Assert SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS ==> USING_DAG_ORCHESTRATOR
# Suggested Assert using a lambda option ==> store objects in Lambdas 
# Suggested Assert USING_DAG_ORCHESTRATOR ==> not RUN_ALL_TASKS_LOCALLY and not USING_WORKERS and not STORE_FANINS_FANINNBS_LOCALLY

##########################################
###### Configuration tests start here ######
##########################################

#################################################
###### Non-Pageranl tests (using Dask DAGs) ######
#################################################

#running non-pagerank test: python -m wukongdnc.dag.TestAll -t n and python -m wukongdnc.server.tcp_server -t n or (use tcp_server_lambda)

def non_pagerank_non_store_objects_in_lambda_base():
    global EXIT_PROGRAM_ON_EXCEPTION
    global DEALLOCATE_BFS_MAIN_MAP_ON_THE_FLY
    global STORE_SYNC_OBJECTS_IN_LAMBDAS
    global USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS
    global SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS
    global USING_DAG_ORCHESTRATOR
    global MAP_OBJECTS_TO_LAMBDA_FUNCTIONS
    global USE_ANONYMOUS_LAMBDA_FUNCTIONS
    global USE_SINGLE_LAMBDA_FUNCTION

    global PAGERANK_GRAPH_FILE_NAME
    global COMPUTE_PAGERANK
    global NUMBER_OF_PAGERANK_ITERATIONS_FOR_PARTITIONS_GROUPS_WITH_LOOPS 
    global NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG
    global CHECK_PAGERANK_OUTPUT
    global SAME_OUTPUT_FOR_ALL_FANOUT_FANIN
    global USE_INCREMENTAL_DAG_GENERATION
    global INCREMENTAL_DAG_DEPOSIT_INTERVAL
    global ENABLE_RUNTIME_TASK_CLUSTERING
    global MIN_PARTITION_GROUP_SIZE_FOR_CLUSTERING
    global MAX_SIZE_OF_OUTPUT_TO_FANOUT_TASK
    global WORK_QUEUE_SIZE_FOR_INCREMENTAL_DAG_GENERATION_WITH_WORKER_PROCESSES
    global TASKS_USE_RESULT_DICTIONARY_PARAMETER
    global USE_SHARED_PARTITIONS_GROUPS
    global USE_PAGERANK_GROUPS_PARTITIONS
    global USE_STRUCT_OF_ARRAYS_FOR_PAGERANK
    global USE_MUTLITHREADED_NONINCREMENTAL_BFS
    global INPUT_ALL_GROUPS_PARTITIONS_AT_START

    global RUN_ALL_TASKS_LOCALLY
    global BYPASS_CALL_TO_INVOKE_REAL_LAMBDA
    global STORE_FANINS_FANINNBS_LOCALLY
    global CREATE_ALL_FANINS_FANINNBS_ON_START
    global USING_WORKERS
    global USING_THREADS_NOT_PROCESSES
    global NUM_WORKERS
    global USE_MULTITHREADED_MULTIPROCESSING
    global NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING
    global FANIN_TYPE
    global FANINNB_TYPE
    global PROCESS_WORK_QUEUE_TYPE

    STORE_SYNC_OBJECTS_IN_LAMBDAS = False
    USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS = False
    SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS = False
    USING_DAG_ORCHESTRATOR = False
    MAP_OBJECTS_TO_LAMBDA_FUNCTIONS = False
    USE_ANONYMOUS_LAMBDA_FUNCTIONS = False
    USE_SINGLE_LAMBDA_FUNCTION = False

    # For all: remote objects, using select objects:
    # 1. RUN_ALL_TASKS_LOCALLY = True, create objects on start = True:
    # TTFFTF: no trigger and no DAG_orchestrator, but map objects 
    # (anon is false) and create objects on start
    # variations:
    # - change D_O to T, 
    # - change map to F, and anon to T: Note: no function lock since anon caled only once
    # - change D_O to F, map F, anon T: Note: no function lock since anon caled only once
    #
    # 2. RUN_ALL_TASKS_LOCALLY = False, create objects on start = True:
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

    PAGERANK_GRAPH_FILE_NAME = None
    COMPUTE_PAGERANK = False
    NUMBER_OF_PAGERANK_ITERATIONS_FOR_PARTITIONS_GROUPS_WITH_LOOPS = 10
    NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG = "PR1_1"
    CHECK_PAGERANK_OUTPUT = COMPUTE_PAGERANK and True
    SAME_OUTPUT_FOR_ALL_FANOUT_FANIN = not COMPUTE_PAGERANK
    USE_INCREMENTAL_DAG_GENERATION = COMPUTE_PAGERANK and False
    INCREMENTAL_DAG_DEPOSIT_INTERVAL = 2
    ENABLE_RUNTIME_TASK_CLUSTERING = COMPUTE_PAGERANK and False
    MIN_PARTITION_GROUP_SIZE_FOR_CLUSTERING = 1
    MAX_SIZE_OF_OUTPUT_TO_FANOUT_TASK = 10000
    WORK_QUEUE_SIZE_FOR_INCREMENTAL_DAG_GENERATION_WITH_WORKER_PROCESSES =  2**10-1
    TASKS_USE_RESULT_DICTIONARY_PARAMETER = COMPUTE_PAGERANK and True
    USE_SHARED_PARTITIONS_GROUPS = COMPUTE_PAGERANK and False
    USE_PAGERANK_GROUPS_PARTITIONS = COMPUTE_PAGERANK and True
    USE_STRUCT_OF_ARRAYS_FOR_PAGERANK = COMPUTE_PAGERANK and False
    USE_MUTLITHREADED_NONINCREMENTAL_BFS = COMPUTE_PAGERANK and False
    INPUT_ALL_GROUPS_PARTITIONS_AT_START = COMPUTE_PAGERANK and False

# local objects 
    
#Test1: simulated lambdas (A2) with non-selective-wait Sync-objects,
# create objects at the start
def test1():
    print("test1")
    global EXIT_PROGRAM_ON_EXCEPTION
    global DEALLOCATE_BFS_MAIN_MAP_ON_THE_FLY
    global RUN_ALL_TASKS_LOCALLY
    global BYPASS_CALL_TO_INVOKE_REAL_LAMBDA
    global STORE_FANINS_FANINNBS_LOCALLY
    global CREATE_ALL_FANINS_FANINNBS_ON_START
    global USING_WORKERS
    global USING_THREADS_NOT_PROCESSES
    global NUM_WORKERS
    global USE_MULTITHREADED_MULTIPROCESSING
    global NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING
    global FANIN_TYPE
    global FANINNB_TYPE
    global PROCESS_WORK_QUEUE_TYPE
    
    global STORE_SYNC_OBJECTS_IN_LAMBDAS
    global USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS
    global SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS
    global USING_DAG_ORCHESTRATOR
    global MAP_OBJECTS_TO_LAMBDA_FUNCTIONS
    global USE_ANONYMOUS_LAMBDA_FUNCTIONS
    global USE_SINGLE_LAMBDA_FUNCTION

    global PAGERANK_GRAPH_FILE_NAME
    global COMPUTE_PAGERANK
    global NUMBER_OF_PAGERANK_ITERATIONS_FOR_PARTITIONS_GROUPS_WITH_LOOPS 
    global NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG
    global CHECK_PAGERANK_OUTPUT
    global SAME_OUTPUT_FOR_ALL_FANOUT_FANIN
    global USE_INCREMENTAL_DAG_GENERATION
    global INCREMENTAL_DAG_DEPOSIT_INTERVAL
    global ENABLE_RUNTIME_TASK_CLUSTERING
    global WORK_QUEUE_SIZE_FOR_INCREMENTAL_DAG_GENERATION_WITH_WORKER_PROCESSES
    global TASKS_USE_RESULT_DICTIONARY_PARAMETER
    global USE_SHARED_PARTITIONS_GROUPS
    global USE_PAGERANK_GROUPS_PARTITIONS
    global USE_STRUCT_OF_ARRAYS_FOR_PAGERANK
    global USE_MUTLITHREADED_NONINCREMENTAL_BFS
    global INPUT_ALL_GROUPS_PARTITIONS_AT_START

    RUN_ALL_TASKS_LOCALLY = True
    BYPASS_CALL_TO_INVOKE_REAL_LAMBDA = (not RUN_ALL_TASKS_LOCALLY) and True
    STORE_FANINS_FANINNBS_LOCALLY = True
    CREATE_ALL_FANINS_FANINNBS_ON_START = True
    USING_WORKERS = False
    USING_THREADS_NOT_PROCESSES = True
    NUM_WORKERS = 1
    USE_MULTITHREADED_MULTIPROCESSING = False
    NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING = 1

    FANIN_TYPE = "DAG_executor_FanIn"
    FANINNB_TYPE = "DAG_executor_FanInNB"
    PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer"
    #FANIN_TYPE = "DAG_executor_FanIn_Select"
    #FANINNB_TYPE = "DAG_executor_FanInNB_Select"
    #PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer_Select"

#Test2: simulated lambdas (A2) with selective-wait Sync-objects,
#        create objects at the start
def test2():
    print("test2")
    global EXIT_PROGRAM_ON_EXCEPTION
    global DEALLOCATE_BFS_MAIN_MAP_ON_THE_FLY
    global RUN_ALL_TASKS_LOCALLY
    global BYPASS_CALL_TO_INVOKE_REAL_LAMBDA
    global STORE_FANINS_FANINNBS_LOCALLY
    global CREATE_ALL_FANINS_FANINNBS_ON_START
    global USING_WORKERS
    global USING_THREADS_NOT_PROCESSES
    global NUM_WORKERS
    global USE_MULTITHREADED_MULTIPROCESSING
    global NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING
    global FANIN_TYPE
    global FANINNB_TYPE
    global PROCESS_WORK_QUEUE_TYPE
    
    global STORE_SYNC_OBJECTS_IN_LAMBDAS
    global USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS
    global SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS
    global USING_DAG_ORCHESTRATOR
    global MAP_OBJECTS_TO_LAMBDA_FUNCTIONS
    global USE_ANONYMOUS_LAMBDA_FUNCTIONS
    global USE_SINGLE_LAMBDA_FUNCTION

    global PAGERANK_GRAPH_FILE_NAME
    global COMPUTE_PAGERANK
    global NUMBER_OF_PAGERANK_ITERATIONS_FOR_PARTITIONS_GROUPS_WITH_LOOPS 
    global NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG
    global CHECK_PAGERANK_OUTPUT
    global SAME_OUTPUT_FOR_ALL_FANOUT_FANIN
    global USE_INCREMENTAL_DAG_GENERATION
    global INCREMENTAL_DAG_DEPOSIT_INTERVAL
    global ENABLE_RUNTIME_TASK_CLUSTERING
    global WORK_QUEUE_SIZE_FOR_INCREMENTAL_DAG_GENERATION_WITH_WORKER_PROCESSES
    global TASKS_USE_RESULT_DICTIONARY_PARAMETER
    global USE_SHARED_PARTITIONS_GROUPS
    global USE_PAGERANK_GROUPS_PARTITIONS
    global USE_STRUCT_OF_ARRAYS_FOR_PAGERANK
    global USE_MUTLITHREADED_NONINCREMENTAL_BFS
    global INPUT_ALL_GROUPS_PARTITIONS_AT_START

    RUN_ALL_TASKS_LOCALLY = True
    BYPASS_CALL_TO_INVOKE_REAL_LAMBDA = (not RUN_ALL_TASKS_LOCALLY) and True
    STORE_FANINS_FANINNBS_LOCALLY = True
    CREATE_ALL_FANINS_FANINNBS_ON_START = True
    USING_WORKERS = False
    USING_THREADS_NOT_PROCESSES = True
    NUM_WORKERS = 1
    USE_MULTITHREADED_MULTIPROCESSING = False
    NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING = 1

    #FANIN_TYPE = "DAG_executor_FanIn"
    #FANINNB_TYPE = "DAG_executor_FanInNB"
    #PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer"
    FANIN_TYPE = "DAG_executor_FanIn_Select"
    FANINNB_TYPE = "DAG_executor_FanInNB_Select"
    PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer_Select"

#Test3: simulated lambdas (A2) with non-selective-wait Sync-objects,
#         create objects on-the-fly
def test3():
    logger.info("test3")
    global EXIT_PROGRAM_ON_EXCEPTION
    global DEALLOCATE_BFS_MAIN_MAP_ON_THE_FLY
    global RUN_ALL_TASKS_LOCALLY
    global BYPASS_CALL_TO_INVOKE_REAL_LAMBDA
    global STORE_FANINS_FANINNBS_LOCALLY
    global CREATE_ALL_FANINS_FANINNBS_ON_START
    global USING_WORKERS
    global USING_THREADS_NOT_PROCESSES
    global NUM_WORKERS
    global USE_MULTITHREADED_MULTIPROCESSING
    global NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING
    global FANIN_TYPE
    global FANINNB_TYPE
    global PROCESS_WORK_QUEUE_TYPE
    
    global STORE_SYNC_OBJECTS_IN_LAMBDAS
    global USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS
    global SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS
    global USING_DAG_ORCHESTRATOR
    global MAP_OBJECTS_TO_LAMBDA_FUNCTIONS
    global USE_ANONYMOUS_LAMBDA_FUNCTIONS
    global USE_SINGLE_LAMBDA_FUNCTION

    global PAGERANK_GRAPH_FILE_NAME
    global COMPUTE_PAGERANK
    global NUMBER_OF_PAGERANK_ITERATIONS_FOR_PARTITIONS_GROUPS_WITH_LOOPS 
    global NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG
    global CHECK_PAGERANK_OUTPUT
    global SAME_OUTPUT_FOR_ALL_FANOUT_FANIN
    global USE_INCREMENTAL_DAG_GENERATION
    global INCREMENTAL_DAG_DEPOSIT_INTERVAL
    global ENABLE_RUNTIME_TASK_CLUSTERING
    global WORK_QUEUE_SIZE_FOR_INCREMENTAL_DAG_GENERATION_WITH_WORKER_PROCESSES
    global TASKS_USE_RESULT_DICTIONARY_PARAMETER
    global USE_SHARED_PARTITIONS_GROUPS
    global USE_PAGERANK_GROUPS_PARTITIONS
    global USE_STRUCT_OF_ARRAYS_FOR_PAGERANK
    global USE_MUTLITHREADED_NONINCREMENTAL_BFS
    global INPUT_ALL_GROUPS_PARTITIONS_AT_START
	
    RUN_ALL_TASKS_LOCALLY = True
    BYPASS_CALL_TO_INVOKE_REAL_LAMBDA = (not RUN_ALL_TASKS_LOCALLY) and True
    STORE_FANINS_FANINNBS_LOCALLY = True 
    CREATE_ALL_FANINS_FANINNBS_ON_START = False
    USING_WORKERS = False
    USING_THREADS_NOT_PROCESSES = True
    NUM_WORKERS = 1
    USE_MULTITHREADED_MULTIPROCESSING = False
    NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING = 1

    FANIN_TYPE = "DAG_executor_FanIn"
    FANINNB_TYPE = "DAG_executor_FanInNB"
    PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer"
    #FANIN_TYPE = "DAG_executor_FanIn_Select"
    #FANINNB_TYPE = "DAG_executor_FanInNB_Select"
    #PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer_Select"

#Test4: simulated lambdas (A2) with selective-wait Sync-objects,
#         create objects on-the-fly
def test4():
    print("test4")
    global EXIT_PROGRAM_ON_EXCEPTION
    global DEALLOCATE_BFS_MAIN_MAP_ON_THE_FLY
    global RUN_ALL_TASKS_LOCALLY
    global BYPASS_CALL_TO_INVOKE_REAL_LAMBDA
    global STORE_FANINS_FANINNBS_LOCALLY
    global CREATE_ALL_FANINS_FANINNBS_ON_START
    global USING_WORKERS
    global USING_THREADS_NOT_PROCESSES
    global NUM_WORKERS
    global USE_MULTITHREADED_MULTIPROCESSING
    global NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING
    global FANIN_TYPE
    global FANINNB_TYPE
    global PROCESS_WORK_QUEUE_TYPE
    
    global STORE_SYNC_OBJECTS_IN_LAMBDAS
    global USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS
    global SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS
    global USING_DAG_ORCHESTRATOR
    global MAP_OBJECTS_TO_LAMBDA_FUNCTIONS
    global USE_ANONYMOUS_LAMBDA_FUNCTIONS
    global USE_SINGLE_LAMBDA_FUNCTION

    global PAGERANK_GRAPH_FILE_NAME
    global COMPUTE_PAGERANK
    global NUMBER_OF_PAGERANK_ITERATIONS_FOR_PARTITIONS_GROUPS_WITH_LOOPS 
    global NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG
    global CHECK_PAGERANK_OUTPUT
    global SAME_OUTPUT_FOR_ALL_FANOUT_FANIN
    global USE_INCREMENTAL_DAG_GENERATION
    global INCREMENTAL_DAG_DEPOSIT_INTERVAL
    global ENABLE_RUNTIME_TASK_CLUSTERING
    global WORK_QUEUE_SIZE_FOR_INCREMENTAL_DAG_GENERATION_WITH_WORKER_PROCESSES
    global TASKS_USE_RESULT_DICTIONARY_PARAMETER
    global USE_SHARED_PARTITIONS_GROUPS
    global USE_PAGERANK_GROUPS_PARTITIONS
    global USE_STRUCT_OF_ARRAYS_FOR_PAGERANK
    global USE_MUTLITHREADED_NONINCREMENTAL_BFS
    global INPUT_ALL_GROUPS_PARTITIONS_AT_START

    RUN_ALL_TASKS_LOCALLY = True
    BYPASS_CALL_TO_INVOKE_REAL_LAMBDA = (not RUN_ALL_TASKS_LOCALLY) and True
    STORE_FANINS_FANINNBS_LOCALLY = True 
    CREATE_ALL_FANINS_FANINNBS_ON_START = False
    USING_WORKERS = False
    USING_THREADS_NOT_PROCESSES = True
    NUM_WORKERS = 1
    USE_MULTITHREADED_MULTIPROCESSING = False
    NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING = 1

    #FANIN_TYPE = "DAG_executor_FanIn"
    #FANINNB_TYPE = "DAG_executor_FanInNB"
    #PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer"
    FANIN_TYPE = "DAG_executor_FanIn_Select"
    FANINNB_TYPE = "DAG_executor_FanInNB_Select"
    PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer_Select"

# remote objects
    
#Test5: simulated lambdas (A2) with non-selective-wait Sync-objects,
# create objects at the start, STORE_FANINS_FANINNBS_LOCALLY = False 
# Note: tcp_server must be running: tcp_server -t 5
def test5():
    print("test5")
    global EXIT_PROGRAM_ON_EXCEPTION
    global DEALLOCATE_BFS_MAIN_MAP_ON_THE_FLY
    global RUN_ALL_TASKS_LOCALLY
    global BYPASS_CALL_TO_INVOKE_REAL_LAMBDA
    global STORE_FANINS_FANINNBS_LOCALLY
    global CREATE_ALL_FANINS_FANINNBS_ON_START
    global USING_WORKERS
    global USING_THREADS_NOT_PROCESSES
    global NUM_WORKERS
    global USE_MULTITHREADED_MULTIPROCESSING
    global NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING
    global FANIN_TYPE
    global FANINNB_TYPE
    global PROCESS_WORK_QUEUE_TYPE
    
    global STORE_SYNC_OBJECTS_IN_LAMBDAS
    global USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS
    global SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS
    global USING_DAG_ORCHESTRATOR
    global MAP_OBJECTS_TO_LAMBDA_FUNCTIONS
    global USE_ANONYMOUS_LAMBDA_FUNCTIONS
    global USE_SINGLE_LAMBDA_FUNCTION

    global PAGERANK_GRAPH_FILE_NAME
    global COMPUTE_PAGERANK
    global NUMBER_OF_PAGERANK_ITERATIONS_FOR_PARTITIONS_GROUPS_WITH_LOOPS 
    global NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG
    global CHECK_PAGERANK_OUTPUT
    global SAME_OUTPUT_FOR_ALL_FANOUT_FANIN
    global USE_INCREMENTAL_DAG_GENERATION
    global INCREMENTAL_DAG_DEPOSIT_INTERVAL
    global ENABLE_RUNTIME_TASK_CLUSTERING
    global WORK_QUEUE_SIZE_FOR_INCREMENTAL_DAG_GENERATION_WITH_WORKER_PROCESSES
    global TASKS_USE_RESULT_DICTIONARY_PARAMETER
    global USE_SHARED_PARTITIONS_GROUPS
    global USE_PAGERANK_GROUPS_PARTITIONS
    global USE_STRUCT_OF_ARRAYS_FOR_PAGERANK
    global USE_MUTLITHREADED_NONINCREMENTAL_BFS
    global INPUT_ALL_GROUPS_PARTITIONS_AT_START

    RUN_ALL_TASKS_LOCALLY = True
    BYPASS_CALL_TO_INVOKE_REAL_LAMBDA = (not RUN_ALL_TASKS_LOCALLY) and True
    STORE_FANINS_FANINNBS_LOCALLY = False 
    CREATE_ALL_FANINS_FANINNBS_ON_START = True
    USING_WORKERS = False
    USING_THREADS_NOT_PROCESSES = True
    NUM_WORKERS = 1
    USE_MULTITHREADED_MULTIPROCESSING = False
    NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING = 1

    FANIN_TYPE = "DAG_executor_FanIn"
    FANINNB_TYPE = "DAG_executor_FanInNB"
    PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer"
    #FANIN_TYPE = "DAG_executor_FanIn_Select"
    #FANINNB_TYPE = "DAG_executor_FanInNB_Select"
    #PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer_Select"

#Test6: simulated lambdas (A2) with selective-wait Sync-objects,
#        create objects at the start, STORE_FANINS_FANINNBS_LOCALLY = False 
# Note: tcp_server must be running: tcp_server -t 6
def test6():
    print("test6")
    global EXIT_PROGRAM_ON_EXCEPTION
    global DEALLOCATE_BFS_MAIN_MAP_ON_THE_FLY
    global RUN_ALL_TASKS_LOCALLY
    global BYPASS_CALL_TO_INVOKE_REAL_LAMBDA
    global STORE_FANINS_FANINNBS_LOCALLY
    global CREATE_ALL_FANINS_FANINNBS_ON_START
    global USING_WORKERS
    global USING_THREADS_NOT_PROCESSES
    global NUM_WORKERS
    global USE_MULTITHREADED_MULTIPROCESSING
    global NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING
    global FANIN_TYPE
    global FANINNB_TYPE
    global PROCESS_WORK_QUEUE_TYPE
    
    global STORE_SYNC_OBJECTS_IN_LAMBDAS
    global USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS
    global SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS
    global USING_DAG_ORCHESTRATOR
    global MAP_OBJECTS_TO_LAMBDA_FUNCTIONS
    global USE_ANONYMOUS_LAMBDA_FUNCTIONS
    global USE_SINGLE_LAMBDA_FUNCTION

    global PAGERANK_GRAPH_FILE_NAME
    global COMPUTE_PAGERANK
    global NUMBER_OF_PAGERANK_ITERATIONS_FOR_PARTITIONS_GROUPS_WITH_LOOPS 
    global NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG
    global CHECK_PAGERANK_OUTPUT
    global SAME_OUTPUT_FOR_ALL_FANOUT_FANIN
    global USE_INCREMENTAL_DAG_GENERATION
    global INCREMENTAL_DAG_DEPOSIT_INTERVAL
    global ENABLE_RUNTIME_TASK_CLUSTERING
    global WORK_QUEUE_SIZE_FOR_INCREMENTAL_DAG_GENERATION_WITH_WORKER_PROCESSES
    global TASKS_USE_RESULT_DICTIONARY_PARAMETER
    global USE_SHARED_PARTITIONS_GROUPS
    global USE_PAGERANK_GROUPS_PARTITIONS
    global USE_STRUCT_OF_ARRAYS_FOR_PAGERANK
    global USE_MUTLITHREADED_NONINCREMENTAL_BFS
    global INPUT_ALL_GROUPS_PARTITIONS_AT_START

    RUN_ALL_TASKS_LOCALLY = True
    BYPASS_CALL_TO_INVOKE_REAL_LAMBDA = (not RUN_ALL_TASKS_LOCALLY) and True
    STORE_FANINS_FANINNBS_LOCALLY = False
    CREATE_ALL_FANINS_FANINNBS_ON_START = True
    USING_WORKERS = False
    USING_THREADS_NOT_PROCESSES = True
    NUM_WORKERS = 1
    USE_MULTITHREADED_MULTIPROCESSING = False
    NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING = 1

    #FANIN_TYPE = "DAG_executor_FanIn"
    #FANINNB_TYPE = "DAG_executor_FanInNB"
    #PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer"
    FANIN_TYPE = "DAG_executor_FanIn_Select"
    FANINNB_TYPE = "DAG_executor_FanInNB_Select"
    PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer_Select"

#Test 7: simulated lambdas (A2) with non-selective-wait Sync-objects,
#         create objects on-the-fly, STORE_FANINS_FANINNBS_LOCALLY = False 
# Note: tcp_server must be running: tcp_server -t 7
def test7():
    logger.info("test7")
    global EXIT_PROGRAM_ON_EXCEPTION
    global DEALLOCATE_BFS_MAIN_MAP_ON_THE_FLY
    global RUN_ALL_TASKS_LOCALLY
    global BYPASS_CALL_TO_INVOKE_REAL_LAMBDA
    global STORE_FANINS_FANINNBS_LOCALLY
    global CREATE_ALL_FANINS_FANINNBS_ON_START
    global USING_WORKERS
    global USING_THREADS_NOT_PROCESSES
    global NUM_WORKERS
    global USE_MULTITHREADED_MULTIPROCESSING
    global NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING
    global FANIN_TYPE
    global FANINNB_TYPE
    global PROCESS_WORK_QUEUE_TYPE
    
    global STORE_SYNC_OBJECTS_IN_LAMBDAS
    global USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS
    global SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS
    global USING_DAG_ORCHESTRATOR
    global MAP_OBJECTS_TO_LAMBDA_FUNCTIONS
    global USE_ANONYMOUS_LAMBDA_FUNCTIONS
    global USE_SINGLE_LAMBDA_FUNCTION

    global PAGERANK_GRAPH_FILE_NAME
    global COMPUTE_PAGERANK
    global NUMBER_OF_PAGERANK_ITERATIONS_FOR_PARTITIONS_GROUPS_WITH_LOOPS 
    global NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG
    global CHECK_PAGERANK_OUTPUT
    global SAME_OUTPUT_FOR_ALL_FANOUT_FANIN
    global USE_INCREMENTAL_DAG_GENERATION
    global INCREMENTAL_DAG_DEPOSIT_INTERVAL
    global ENABLE_RUNTIME_TASK_CLUSTERING
    global WORK_QUEUE_SIZE_FOR_INCREMENTAL_DAG_GENERATION_WITH_WORKER_PROCESSES
    global TASKS_USE_RESULT_DICTIONARY_PARAMETER
    global USE_SHARED_PARTITIONS_GROUPS
    global USE_PAGERANK_GROUPS_PARTITIONS
    global USE_STRUCT_OF_ARRAYS_FOR_PAGERANK
    global USE_MUTLITHREADED_NONINCREMENTAL_BFS
    global INPUT_ALL_GROUPS_PARTITIONS_AT_START
	
    RUN_ALL_TASKS_LOCALLY = True
    BYPASS_CALL_TO_INVOKE_REAL_LAMBDA = (not RUN_ALL_TASKS_LOCALLY) and True
    STORE_FANINS_FANINNBS_LOCALLY = False 
    CREATE_ALL_FANINS_FANINNBS_ON_START = False
    USING_WORKERS = False
    USING_THREADS_NOT_PROCESSES = True
    NUM_WORKERS = 1
    USE_MULTITHREADED_MULTIPROCESSING = False
    NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING = 1

    FANIN_TYPE = "DAG_executor_FanIn"
    FANINNB_TYPE = "DAG_executor_FanInNB"
    PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer"
    #FANIN_TYPE = "DAG_executor_FanIn_Select"
    #FANINNB_TYPE = "DAG_executor_FanInNB_Select"
    #PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer_Select"

#Test 8: simulated lambdas (A2) with selective-wait Sync-objects,
#         create objects on-the-fly, STORE_FANINS_FANINNBS_LOCALLY = False 
# Note: tcp_server must be running: tcp_server -t 8
def test8():
    print("test8")
    global EXIT_PROGRAM_ON_EXCEPTION
    global DEALLOCATE_BFS_MAIN_MAP_ON_THE_FLY
    global RUN_ALL_TASKS_LOCALLY
    global BYPASS_CALL_TO_INVOKE_REAL_LAMBDA
    global STORE_FANINS_FANINNBS_LOCALLY
    global CREATE_ALL_FANINS_FANINNBS_ON_START
    global USING_WORKERS
    global USING_THREADS_NOT_PROCESSES
    global NUM_WORKERS
    global USE_MULTITHREADED_MULTIPROCESSING
    global NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING
    global FANIN_TYPE
    global FANINNB_TYPE
    global PROCESS_WORK_QUEUE_TYPE
    
    global STORE_SYNC_OBJECTS_IN_LAMBDAS
    global USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS
    global SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS
    global USING_DAG_ORCHESTRATOR
    global MAP_OBJECTS_TO_LAMBDA_FUNCTIONS
    global USE_ANONYMOUS_LAMBDA_FUNCTIONS
    global USE_SINGLE_LAMBDA_FUNCTION

    global PAGERANK_GRAPH_FILE_NAME
    global COMPUTE_PAGERANK
    global NUMBER_OF_PAGERANK_ITERATIONS_FOR_PARTITIONS_GROUPS_WITH_LOOPS 
    global NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG
    global CHECK_PAGERANK_OUTPUT
    global SAME_OUTPUT_FOR_ALL_FANOUT_FANIN
    global USE_INCREMENTAL_DAG_GENERATION
    global INCREMENTAL_DAG_DEPOSIT_INTERVAL
    global ENABLE_RUNTIME_TASK_CLUSTERING
    global WORK_QUEUE_SIZE_FOR_INCREMENTAL_DAG_GENERATION_WITH_WORKER_PROCESSES
    global TASKS_USE_RESULT_DICTIONARY_PARAMETER
    global USE_SHARED_PARTITIONS_GROUPS
    global USE_PAGERANK_GROUPS_PARTITIONS
    global USE_STRUCT_OF_ARRAYS_FOR_PAGERANK
    global USE_MUTLITHREADED_NONINCREMENTAL_BFS
    global INPUT_ALL_GROUPS_PARTITIONS_AT_START

    RUN_ALL_TASKS_LOCALLY = True
    BYPASS_CALL_TO_INVOKE_REAL_LAMBDA = (not RUN_ALL_TASKS_LOCALLY) and True
    STORE_FANINS_FANINNBS_LOCALLY = False 
    CREATE_ALL_FANINS_FANINNBS_ON_START = False
    USING_WORKERS = False
    USING_THREADS_NOT_PROCESSES = True
    NUM_WORKERS = 1
    USE_MULTITHREADED_MULTIPROCESSING = False
    NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING = 1

    #FANIN_TYPE = "DAG_executor_FanIn"
    #FANINNB_TYPE = "DAG_executor_FanInNB"
    #PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer"
    FANIN_TYPE = "DAG_executor_FanIn_Select"
    FANINNB_TYPE = "DAG_executor_FanInNB_Select"
    PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer_Select"

# workers
# threads
# local objects

#Test9: worker threads (A2) with non-selective-wait Sync-objects, 
#       1 worker, Sync-objects stored locally, create objects at the start
def test9():
    print("test9")
    global EXIT_PROGRAM_ON_EXCEPTION
    global DEALLOCATE_BFS_MAIN_MAP_ON_THE_FLY
    global RUN_ALL_TASKS_LOCALLY
    global BYPASS_CALL_TO_INVOKE_REAL_LAMBDA
    global STORE_FANINS_FANINNBS_LOCALLY
    global CREATE_ALL_FANINS_FANINNBS_ON_START
    global USING_WORKERS
    global USING_THREADS_NOT_PROCESSES
    global NUM_WORKERS
    global USE_MULTITHREADED_MULTIPROCESSING
    global NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING
    global FANIN_TYPE
    global FANINNB_TYPE
    global PROCESS_WORK_QUEUE_TYPE
    
    global STORE_SYNC_OBJECTS_IN_LAMBDAS
    global USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS
    global SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS
    global USING_DAG_ORCHESTRATOR
    global MAP_OBJECTS_TO_LAMBDA_FUNCTIONS
    global USE_ANONYMOUS_LAMBDA_FUNCTIONS
    global USE_SINGLE_LAMBDA_FUNCTION

    global PAGERANK_GRAPH_FILE_NAME
    global COMPUTE_PAGERANK
    global NUMBER_OF_PAGERANK_ITERATIONS_FOR_PARTITIONS_GROUPS_WITH_LOOPS 
    global NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG
    global CHECK_PAGERANK_OUTPUT
    global SAME_OUTPUT_FOR_ALL_FANOUT_FANIN
    global USE_INCREMENTAL_DAG_GENERATION
    global INCREMENTAL_DAG_DEPOSIT_INTERVAL
    global ENABLE_RUNTIME_TASK_CLUSTERING
    global WORK_QUEUE_SIZE_FOR_INCREMENTAL_DAG_GENERATION_WITH_WORKER_PROCESSES
    global TASKS_USE_RESULT_DICTIONARY_PARAMETER
    global USE_SHARED_PARTITIONS_GROUPS
    global USE_PAGERANK_GROUPS_PARTITIONS
    global USE_STRUCT_OF_ARRAYS_FOR_PAGERANK
    global USE_MUTLITHREADED_NONINCREMENTAL_BFS
    global INPUT_ALL_GROUPS_PARTITIONS_AT_START

    RUN_ALL_TASKS_LOCALLY = True
    BYPASS_CALL_TO_INVOKE_REAL_LAMBDA = (not RUN_ALL_TASKS_LOCALLY) and True 
    STORE_FANINS_FANINNBS_LOCALLY = True 
    CREATE_ALL_FANINS_FANINNBS_ON_START = True
    USING_WORKERS = True
    USING_THREADS_NOT_PROCESSES = True
    NUM_WORKERS = 1
    USE_MULTITHREADED_MULTIPROCESSING = False
    NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING = 1

    FANIN_TYPE = "DAG_executor_FanIn"
    FANINNB_TYPE = "DAG_executor_FanInNB"
    PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer"
    #FANIN_TYPE = "DAG_executor_FanIn_Select"
    #FANINNB_TYPE = "DAG_executor_FanInNB_Select"
    #PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer_Select"

#Test10: worker threads (A2) with non-selective-wait Sync-objects, 
#       2 workers, Sync-objects stored locally, create objects at the start
def test10():
    print("test10")
    global EXIT_PROGRAM_ON_EXCEPTION
    global DEALLOCATE_BFS_MAIN_MAP_ON_THE_FLY
    global RUN_ALL_TASKS_LOCALLY
    global BYPASS_CALL_TO_INVOKE_REAL_LAMBDA
    global STORE_FANINS_FANINNBS_LOCALLY
    global CREATE_ALL_FANINS_FANINNBS_ON_START
    global USING_WORKERS
    global USING_THREADS_NOT_PROCESSES
    global NUM_WORKERS
    global USE_MULTITHREADED_MULTIPROCESSING
    global NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING
    global FANIN_TYPE
    global FANINNB_TYPE
    global PROCESS_WORK_QUEUE_TYPE
    
    global STORE_SYNC_OBJECTS_IN_LAMBDAS
    global USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS
    global SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS
    global USING_DAG_ORCHESTRATOR
    global MAP_OBJECTS_TO_LAMBDA_FUNCTIONS
    global USE_ANONYMOUS_LAMBDA_FUNCTIONS
    global USE_SINGLE_LAMBDA_FUNCTION

    global PAGERANK_GRAPH_FILE_NAME
    global COMPUTE_PAGERANK
    global NUMBER_OF_PAGERANK_ITERATIONS_FOR_PARTITIONS_GROUPS_WITH_LOOPS 
    global NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG
    global CHECK_PAGERANK_OUTPUT
    global SAME_OUTPUT_FOR_ALL_FANOUT_FANIN
    global USE_INCREMENTAL_DAG_GENERATION
    global INCREMENTAL_DAG_DEPOSIT_INTERVAL
    global ENABLE_RUNTIME_TASK_CLUSTERING
    global WORK_QUEUE_SIZE_FOR_INCREMENTAL_DAG_GENERATION_WITH_WORKER_PROCESSES
    global TASKS_USE_RESULT_DICTIONARY_PARAMETER
    global USE_SHARED_PARTITIONS_GROUPS
    global USE_PAGERANK_GROUPS_PARTITIONS
    global USE_STRUCT_OF_ARRAYS_FOR_PAGERANK
    global USE_MUTLITHREADED_NONINCREMENTAL_BFS
    global INPUT_ALL_GROUPS_PARTITIONS_AT_START

    RUN_ALL_TASKS_LOCALLY = True
    BYPASS_CALL_TO_INVOKE_REAL_LAMBDA = (not RUN_ALL_TASKS_LOCALLY) and True
    STORE_FANINS_FANINNBS_LOCALLY = True 
    CREATE_ALL_FANINS_FANINNBS_ON_START = True
    USING_WORKERS = True
    USING_THREADS_NOT_PROCESSES = True
    NUM_WORKERS = 2
    USE_MULTITHREADED_MULTIPROCESSING = False
    NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING = 1

    FANIN_TYPE = "DAG_executor_FanIn"
    FANINNB_TYPE = "DAG_executor_FanInNB"
    PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer"
    #FANIN_TYPE = "DAG_executor_FanIn_Select"
    #FANINNB_TYPE = "DAG_executor_FanInNB_Select"
    #PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer_Select"

#Test11: worker threads (A2) with selective-wait Sync-objects, 
#       2 workers, Sync-objects stored locally, create objects at the start
def test11():
    print("test11")
    global EXIT_PROGRAM_ON_EXCEPTION
    global DEALLOCATE_BFS_MAIN_MAP_ON_THE_FLY
    global RUN_ALL_TASKS_LOCALLY
    global BYPASS_CALL_TO_INVOKE_REAL_LAMBDA
    global STORE_FANINS_FANINNBS_LOCALLY
    global CREATE_ALL_FANINS_FANINNBS_ON_START
    global USING_WORKERS
    global USING_THREADS_NOT_PROCESSES
    global NUM_WORKERS
    global USE_MULTITHREADED_MULTIPROCESSING
    global NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING
    global FANIN_TYPE
    global FANINNB_TYPE
    global PROCESS_WORK_QUEUE_TYPE
    
    global STORE_SYNC_OBJECTS_IN_LAMBDAS
    global USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS
    global SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS
    global USING_DAG_ORCHESTRATOR
    global MAP_OBJECTS_TO_LAMBDA_FUNCTIONS
    global USE_ANONYMOUS_LAMBDA_FUNCTIONS
    global USE_SINGLE_LAMBDA_FUNCTION

    global PAGERANK_GRAPH_FILE_NAME
    global COMPUTE_PAGERANK
    global NUMBER_OF_PAGERANK_ITERATIONS_FOR_PARTITIONS_GROUPS_WITH_LOOPS 
    global NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG
    global CHECK_PAGERANK_OUTPUT
    global SAME_OUTPUT_FOR_ALL_FANOUT_FANIN
    global USE_INCREMENTAL_DAG_GENERATION
    global INCREMENTAL_DAG_DEPOSIT_INTERVAL
    global ENABLE_RUNTIME_TASK_CLUSTERING
    global WORK_QUEUE_SIZE_FOR_INCREMENTAL_DAG_GENERATION_WITH_WORKER_PROCESSES
    global TASKS_USE_RESULT_DICTIONARY_PARAMETER
    global USE_SHARED_PARTITIONS_GROUPS
    global USE_PAGERANK_GROUPS_PARTITIONS
    global USE_STRUCT_OF_ARRAYS_FOR_PAGERANK
    global USE_MUTLITHREADED_NONINCREMENTAL_BFS
    global INPUT_ALL_GROUPS_PARTITIONS_AT_START

    RUN_ALL_TASKS_LOCALLY = True
    BYPASS_CALL_TO_INVOKE_REAL_LAMBDA = (not RUN_ALL_TASKS_LOCALLY) and True 
    STORE_FANINS_FANINNBS_LOCALLY = True 
    CREATE_ALL_FANINS_FANINNBS_ON_START = True
    USING_WORKERS = True
    USING_THREADS_NOT_PROCESSES = True
    NUM_WORKERS = 2
    USE_MULTITHREADED_MULTIPROCESSING = False
    NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING = 1

    #FANIN_TYPE = "DAG_executor_FanIn"
    #FANINNB_TYPE = "DAG_executor_FanInNB"
    #PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer"
    FANIN_TYPE = "DAG_executor_FanIn_Select"
    FANINNB_TYPE = "DAG_executor_FanInNB_Select"
    PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer_Select"

#Test12: worker threads (A2) with non-selective-wait Sync-objects, 
#       2 workers, Sync-objects stored locally, create objects on-the-fly
def test12():
    print("test12")
    global EXIT_PROGRAM_ON_EXCEPTION
    global DEALLOCATE_BFS_MAIN_MAP_ON_THE_FLY
    global RUN_ALL_TASKS_LOCALLY
    global BYPASS_CALL_TO_INVOKE_REAL_LAMBDA
    global STORE_FANINS_FANINNBS_LOCALLY
    global CREATE_ALL_FANINS_FANINNBS_ON_START
    global USING_WORKERS
    global USING_THREADS_NOT_PROCESSES
    global NUM_WORKERS
    global USE_MULTITHREADED_MULTIPROCESSING
    global NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING
    global FANIN_TYPE
    global FANINNB_TYPE
    global PROCESS_WORK_QUEUE_TYPE
    
    global STORE_SYNC_OBJECTS_IN_LAMBDAS
    global USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS
    global SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS
    global USING_DAG_ORCHESTRATOR
    global MAP_OBJECTS_TO_LAMBDA_FUNCTIONS
    global USE_ANONYMOUS_LAMBDA_FUNCTIONS
    global USE_SINGLE_LAMBDA_FUNCTION

    global PAGERANK_GRAPH_FILE_NAME
    global COMPUTE_PAGERANK
    global NUMBER_OF_PAGERANK_ITERATIONS_FOR_PARTITIONS_GROUPS_WITH_LOOPS 
    global NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG
    global CHECK_PAGERANK_OUTPUT
    global SAME_OUTPUT_FOR_ALL_FANOUT_FANIN
    global USE_INCREMENTAL_DAG_GENERATION
    global INCREMENTAL_DAG_DEPOSIT_INTERVAL
    global ENABLE_RUNTIME_TASK_CLUSTERING
    global WORK_QUEUE_SIZE_FOR_INCREMENTAL_DAG_GENERATION_WITH_WORKER_PROCESSES
    global TASKS_USE_RESULT_DICTIONARY_PARAMETER
    global USE_SHARED_PARTITIONS_GROUPS
    global USE_PAGERANK_GROUPS_PARTITIONS
    global USE_STRUCT_OF_ARRAYS_FOR_PAGERANK
    global USE_MUTLITHREADED_NONINCREMENTAL_BFS
    global INPUT_ALL_GROUPS_PARTITIONS_AT_START

    RUN_ALL_TASKS_LOCALLY = True
    BYPASS_CALL_TO_INVOKE_REAL_LAMBDA = (not RUN_ALL_TASKS_LOCALLY) and True 
    STORE_FANINS_FANINNBS_LOCALLY = True 
    CREATE_ALL_FANINS_FANINNBS_ON_START = False
    USING_WORKERS = True
    USING_THREADS_NOT_PROCESSES = True
    NUM_WORKERS = 2
    USE_MULTITHREADED_MULTIPROCESSING = False
    NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING = 1

    FANIN_TYPE = "DAG_executor_FanIn"
    FANINNB_TYPE = "DAG_executor_FanInNB"
    PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer"
    #FANIN_TYPE = "DAG_executor_FanIn_Select"
    #FANINNB_TYPE = "DAG_executor_FanInNB_Select"
    #PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer_Select"

#Test13: worker threads (A2) with selective-wait Sync-objects, 
#       2 workers, Sync-objects stored locally, create objects on-the-fly
def test13():
    print("test13")
    global EXIT_PROGRAM_ON_EXCEPTION
    global DEALLOCATE_BFS_MAIN_MAP_ON_THE_FLY
    global RUN_ALL_TASKS_LOCALLY
    global BYPASS_CALL_TO_INVOKE_REAL_LAMBDA
    global STORE_FANINS_FANINNBS_LOCALLY
    global CREATE_ALL_FANINS_FANINNBS_ON_START
    global USING_WORKERS
    global USING_THREADS_NOT_PROCESSES
    global NUM_WORKERS
    global USE_MULTITHREADED_MULTIPROCESSING
    global NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING
    global FANIN_TYPE
    global FANINNB_TYPE
    global PROCESS_WORK_QUEUE_TYPE
    
    global STORE_SYNC_OBJECTS_IN_LAMBDAS
    global USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS
    global SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS
    global USING_DAG_ORCHESTRATOR
    global MAP_OBJECTS_TO_LAMBDA_FUNCTIONS
    global USE_ANONYMOUS_LAMBDA_FUNCTIONS
    global USE_SINGLE_LAMBDA_FUNCTION

    global PAGERANK_GRAPH_FILE_NAME
    global COMPUTE_PAGERANK
    global NUMBER_OF_PAGERANK_ITERATIONS_FOR_PARTITIONS_GROUPS_WITH_LOOPS 
    global NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG
    global CHECK_PAGERANK_OUTPUT
    global SAME_OUTPUT_FOR_ALL_FANOUT_FANIN
    global USE_INCREMENTAL_DAG_GENERATION
    global INCREMENTAL_DAG_DEPOSIT_INTERVAL
    global ENABLE_RUNTIME_TASK_CLUSTERING
    global WORK_QUEUE_SIZE_FOR_INCREMENTAL_DAG_GENERATION_WITH_WORKER_PROCESSES
    global TASKS_USE_RESULT_DICTIONARY_PARAMETER
    global USE_SHARED_PARTITIONS_GROUPS
    global USE_PAGERANK_GROUPS_PARTITIONS
    global USE_STRUCT_OF_ARRAYS_FOR_PAGERANK
    global USE_MUTLITHREADED_NONINCREMENTAL_BFS
    global INPUT_ALL_GROUPS_PARTITIONS_AT_START

    RUN_ALL_TASKS_LOCALLY = True
    BYPASS_CALL_TO_INVOKE_REAL_LAMBDA = (not RUN_ALL_TASKS_LOCALLY) and True 
    STORE_FANINS_FANINNBS_LOCALLY = True 
    CREATE_ALL_FANINS_FANINNBS_ON_START = False
    USING_WORKERS = True
    USING_THREADS_NOT_PROCESSES = True
    NUM_WORKERS = 2
    USE_MULTITHREADED_MULTIPROCESSING = False
    NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING = 1

    #FANIN_TYPE = "DAG_executor_FanIn"
    #FANINNB_TYPE = "DAG_executor_FanInNB"
    #PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer"
    FANIN_TYPE = "DAG_executor_FanIn_Select"
    FANINNB_TYPE = "DAG_executor_FanInNB_Select"
    PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer_Select"


# workers
# threads
# remote  objects

#Test14: worker threads (A2) with non-selective-wait Sync-objects, 
#        1 worker, Sync-objects stored remotely (on tcp_server)
#        create objects at the start
# Note: tcp_server must be running: tcp_server -t 14
def test14():
    print("test14")
    global EXIT_PROGRAM_ON_EXCEPTION
    global DEALLOCATE_BFS_MAIN_MAP_ON_THE_FLY
    global RUN_ALL_TASKS_LOCALLY
    global BYPASS_CALL_TO_INVOKE_REAL_LAMBDA
    global STORE_FANINS_FANINNBS_LOCALLY
    global CREATE_ALL_FANINS_FANINNBS_ON_START
    global USING_WORKERS
    global USING_THREADS_NOT_PROCESSES
    global NUM_WORKERS
    global USE_MULTITHREADED_MULTIPROCESSING
    global NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING
    global FANIN_TYPE
    global FANINNB_TYPE
    global PROCESS_WORK_QUEUE_TYPE
    
    global STORE_SYNC_OBJECTS_IN_LAMBDAS
    global USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS
    global SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS
    global USING_DAG_ORCHESTRATOR
    global MAP_OBJECTS_TO_LAMBDA_FUNCTIONS
    global USE_ANONYMOUS_LAMBDA_FUNCTIONS
    global USE_SINGLE_LAMBDA_FUNCTION

    global PAGERANK_GRAPH_FILE_NAME
    global COMPUTE_PAGERANK
    global NUMBER_OF_PAGERANK_ITERATIONS_FOR_PARTITIONS_GROUPS_WITH_LOOPS 
    global NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG
    global CHECK_PAGERANK_OUTPUT
    global SAME_OUTPUT_FOR_ALL_FANOUT_FANIN
    global USE_INCREMENTAL_DAG_GENERATION
    global INCREMENTAL_DAG_DEPOSIT_INTERVAL
    global ENABLE_RUNTIME_TASK_CLUSTERING
    global WORK_QUEUE_SIZE_FOR_INCREMENTAL_DAG_GENERATION_WITH_WORKER_PROCESSES
    global TASKS_USE_RESULT_DICTIONARY_PARAMETER
    global USE_SHARED_PARTITIONS_GROUPS
    global USE_PAGERANK_GROUPS_PARTITIONS
    global USE_STRUCT_OF_ARRAYS_FOR_PAGERANK
    global USE_MUTLITHREADED_NONINCREMENTAL_BFS
    global INPUT_ALL_GROUPS_PARTITIONS_AT_START

    RUN_ALL_TASKS_LOCALLY = True
    BYPASS_CALL_TO_INVOKE_REAL_LAMBDA = (not RUN_ALL_TASKS_LOCALLY) and True 
    STORE_FANINS_FANINNBS_LOCALLY = False 
    CREATE_ALL_FANINS_FANINNBS_ON_START = True
    USING_WORKERS = True
    USING_THREADS_NOT_PROCESSES = True
    NUM_WORKERS = 1
    USE_MULTITHREADED_MULTIPROCESSING = False
    NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING = 1

    FANIN_TYPE = "DAG_executor_FanIn"
    FANINNB_TYPE = "DAG_executor_FanInNB"
    PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer"
    #FANIN_TYPE = "DAG_executor_FanIn_Select"
    #FANINNB_TYPE = "DAG_executor_FanInNB_Select"
    #PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer_Select"

#Test15: worker threads (A2) with non-selective-wait Sync-objects, 
#        2 workers, Sync-objects stored remotely (on tcp_server)
#        create objects at the start
# Note: tcp_server must be running: tcp_server -t 15
def test15():
    print("test15")
    global EXIT_PROGRAM_ON_EXCEPTION
    global DEALLOCATE_BFS_MAIN_MAP_ON_THE_FLY
    global RUN_ALL_TASKS_LOCALLY
    global BYPASS_CALL_TO_INVOKE_REAL_LAMBDA
    global STORE_FANINS_FANINNBS_LOCALLY
    global CREATE_ALL_FANINS_FANINNBS_ON_START
    global USING_WORKERS
    global USING_THREADS_NOT_PROCESSES
    global NUM_WORKERS
    global USE_MULTITHREADED_MULTIPROCESSING
    global NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING
    global FANIN_TYPE
    global FANINNB_TYPE
    global PROCESS_WORK_QUEUE_TYPE
    
    global STORE_SYNC_OBJECTS_IN_LAMBDAS
    global USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS
    global SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS
    global USING_DAG_ORCHESTRATOR
    global MAP_OBJECTS_TO_LAMBDA_FUNCTIONS
    global USE_ANONYMOUS_LAMBDA_FUNCTIONS
    global USE_SINGLE_LAMBDA_FUNCTION

    global PAGERANK_GRAPH_FILE_NAME
    global COMPUTE_PAGERANK
    global NUMBER_OF_PAGERANK_ITERATIONS_FOR_PARTITIONS_GROUPS_WITH_LOOPS 
    global NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG
    global CHECK_PAGERANK_OUTPUT
    global SAME_OUTPUT_FOR_ALL_FANOUT_FANIN
    global USE_INCREMENTAL_DAG_GENERATION
    global INCREMENTAL_DAG_DEPOSIT_INTERVAL
    global ENABLE_RUNTIME_TASK_CLUSTERING
    global WORK_QUEUE_SIZE_FOR_INCREMENTAL_DAG_GENERATION_WITH_WORKER_PROCESSES
    global TASKS_USE_RESULT_DICTIONARY_PARAMETER
    global USE_SHARED_PARTITIONS_GROUPS
    global USE_PAGERANK_GROUPS_PARTITIONS
    global USE_STRUCT_OF_ARRAYS_FOR_PAGERANK
    global USE_MUTLITHREADED_NONINCREMENTAL_BFS
    global INPUT_ALL_GROUPS_PARTITIONS_AT_START

    RUN_ALL_TASKS_LOCALLY = True
    BYPASS_CALL_TO_INVOKE_REAL_LAMBDA = (not RUN_ALL_TASKS_LOCALLY) and True 
    STORE_FANINS_FANINNBS_LOCALLY = False 
    CREATE_ALL_FANINS_FANINNBS_ON_START = True
    USING_WORKERS = True
    USING_THREADS_NOT_PROCESSES = True
    NUM_WORKERS = 2
    USE_MULTITHREADED_MULTIPROCESSING = False
    NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING = 1

    FANIN_TYPE = "DAG_executor_FanIn"
    FANINNB_TYPE = "DAG_executor_FanInNB"
    PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer"
    #FANIN_TYPE = "DAG_executor_FanIn_Select"
    #FANINNB_TYPE = "DAG_executor_FanInNB_Select"
    #PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer_Select"

#Test16: worker threads (A2) with non-selective-wait Sync-objects, 
#        2 workers, Sync-objects stored remotely (on tcp_server)
#        create objects on-the-fly
# Note: tcp_server must be running: tcp_server -t 16
def test16():
    print("test16")
    global EXIT_PROGRAM_ON_EXCEPTION
    global DEALLOCATE_BFS_MAIN_MAP_ON_THE_FLY
    global RUN_ALL_TASKS_LOCALLY
    global BYPASS_CALL_TO_INVOKE_REAL_LAMBDA
    global STORE_FANINS_FANINNBS_LOCALLY
    global CREATE_ALL_FANINS_FANINNBS_ON_START
    global USING_WORKERS
    global USING_THREADS_NOT_PROCESSES
    global NUM_WORKERS
    global USE_MULTITHREADED_MULTIPROCESSING
    global NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING
    global FANIN_TYPE
    global FANINNB_TYPE
    global PROCESS_WORK_QUEUE_TYPE
    
    global STORE_SYNC_OBJECTS_IN_LAMBDAS
    global USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS
    global SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS
    global USING_DAG_ORCHESTRATOR
    global MAP_OBJECTS_TO_LAMBDA_FUNCTIONS
    global USE_ANONYMOUS_LAMBDA_FUNCTIONS
    global USE_SINGLE_LAMBDA_FUNCTION

    global PAGERANK_GRAPH_FILE_NAME
    global COMPUTE_PAGERANK
    global NUMBER_OF_PAGERANK_ITERATIONS_FOR_PARTITIONS_GROUPS_WITH_LOOPS 
    global NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG
    global CHECK_PAGERANK_OUTPUT
    global SAME_OUTPUT_FOR_ALL_FANOUT_FANIN
    global USE_INCREMENTAL_DAG_GENERATION
    global INCREMENTAL_DAG_DEPOSIT_INTERVAL
    global ENABLE_RUNTIME_TASK_CLUSTERING
    global WORK_QUEUE_SIZE_FOR_INCREMENTAL_DAG_GENERATION_WITH_WORKER_PROCESSES
    global TASKS_USE_RESULT_DICTIONARY_PARAMETER
    global USE_SHARED_PARTITIONS_GROUPS
    global USE_PAGERANK_GROUPS_PARTITIONS
    global USE_STRUCT_OF_ARRAYS_FOR_PAGERANK
    global USE_MUTLITHREADED_NONINCREMENTAL_BFS
    global INPUT_ALL_GROUPS_PARTITIONS_AT_START

    RUN_ALL_TASKS_LOCALLY = True
    BYPASS_CALL_TO_INVOKE_REAL_LAMBDA = (not RUN_ALL_TASKS_LOCALLY) and True 
    STORE_FANINS_FANINNBS_LOCALLY = False 
    CREATE_ALL_FANINS_FANINNBS_ON_START = False
    USING_WORKERS = True
    USING_THREADS_NOT_PROCESSES = True
    NUM_WORKERS = 2
    USE_MULTITHREADED_MULTIPROCESSING = False
    NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING = 1

    FANIN_TYPE = "DAG_executor_FanIn"
    FANINNB_TYPE = "DAG_executor_FanInNB"
    PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer"
    #FANIN_TYPE = "DAG_executor_FanIn_Select"
    #FANINNB_TYPE = "DAG_executor_FanInNB_Select"
    #PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer_Select"

#Test17: worker threads (A2) with selective-wait Sync-objects, 
#        2 workers, Sync-objects stored remotely (on tcp_server)
#        create objects at the start
# Note: tcp_server must be running: tcp_server -t 17
def test17():
    print("test17")
    global EXIT_PROGRAM_ON_EXCEPTION
    global DEALLOCATE_BFS_MAIN_MAP_ON_THE_FLY
    global RUN_ALL_TASKS_LOCALLY
    global BYPASS_CALL_TO_INVOKE_REAL_LAMBDA
    global STORE_FANINS_FANINNBS_LOCALLY
    global CREATE_ALL_FANINS_FANINNBS_ON_START
    global USING_WORKERS
    global USING_THREADS_NOT_PROCESSES
    global NUM_WORKERS
    global USE_MULTITHREADED_MULTIPROCESSING
    global NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING
    global FANIN_TYPE
    global FANINNB_TYPE
    global PROCESS_WORK_QUEUE_TYPE
    
    global STORE_SYNC_OBJECTS_IN_LAMBDAS
    global USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS
    global SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS
    global USING_DAG_ORCHESTRATOR
    global MAP_OBJECTS_TO_LAMBDA_FUNCTIONS
    global USE_ANONYMOUS_LAMBDA_FUNCTIONS
    global USE_SINGLE_LAMBDA_FUNCTION

    global PAGERANK_GRAPH_FILE_NAME
    global COMPUTE_PAGERANK
    global NUMBER_OF_PAGERANK_ITERATIONS_FOR_PARTITIONS_GROUPS_WITH_LOOPS 
    global NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG
    global CHECK_PAGERANK_OUTPUT
    global SAME_OUTPUT_FOR_ALL_FANOUT_FANIN
    global USE_INCREMENTAL_DAG_GENERATION
    global INCREMENTAL_DAG_DEPOSIT_INTERVAL
    global ENABLE_RUNTIME_TASK_CLUSTERING
    global WORK_QUEUE_SIZE_FOR_INCREMENTAL_DAG_GENERATION_WITH_WORKER_PROCESSES
    global TASKS_USE_RESULT_DICTIONARY_PARAMETER
    global USE_SHARED_PARTITIONS_GROUPS
    global USE_PAGERANK_GROUPS_PARTITIONS
    global USE_STRUCT_OF_ARRAYS_FOR_PAGERANK
    global USE_MUTLITHREADED_NONINCREMENTAL_BFS
    global INPUT_ALL_GROUPS_PARTITIONS_AT_START

    RUN_ALL_TASKS_LOCALLY = True
    BYPASS_CALL_TO_INVOKE_REAL_LAMBDA = (not RUN_ALL_TASKS_LOCALLY) and True 
    STORE_FANINS_FANINNBS_LOCALLY = False 
    CREATE_ALL_FANINS_FANINNBS_ON_START = True
    USING_WORKERS = True
    USING_THREADS_NOT_PROCESSES = True
    NUM_WORKERS = 2
    USE_MULTITHREADED_MULTIPROCESSING = False
    NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING = 1

    #FANIN_TYPE = "DAG_executor_FanIn"
    #FANINNB_TYPE = "DAG_executor_FanInNB"
    #PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer"
    FANIN_TYPE = "DAG_executor_FanIn_Select"
    FANINNB_TYPE = "DAG_executor_FanInNB_Select"
    PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer_Select"

#Test18: worker threads (A2) with selective-wait Sync-objects, 
#        2 workers, Sync-objects stored remotely (on tcp_server)
#        create objects on-the-fly
# Note: tcp_server must be running: tcp_server -t 18
def test18():
    print("test18")
    global EXIT_PROGRAM_ON_EXCEPTION
    global DEALLOCATE_BFS_MAIN_MAP_ON_THE_FLY
    global RUN_ALL_TASKS_LOCALLY
    global BYPASS_CALL_TO_INVOKE_REAL_LAMBDA
    global STORE_FANINS_FANINNBS_LOCALLY
    global CREATE_ALL_FANINS_FANINNBS_ON_START
    global USING_WORKERS
    global USING_THREADS_NOT_PROCESSES
    global NUM_WORKERS
    global USE_MULTITHREADED_MULTIPROCESSING
    global NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING
    global FANIN_TYPE
    global FANINNB_TYPE
    global PROCESS_WORK_QUEUE_TYPE
    
    global STORE_SYNC_OBJECTS_IN_LAMBDAS
    global USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS
    global SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS
    global USING_DAG_ORCHESTRATOR
    global MAP_OBJECTS_TO_LAMBDA_FUNCTIONS
    global USE_ANONYMOUS_LAMBDA_FUNCTIONS
    global USE_SINGLE_LAMBDA_FUNCTION

    global PAGERANK_GRAPH_FILE_NAME
    global COMPUTE_PAGERANK
    global NUMBER_OF_PAGERANK_ITERATIONS_FOR_PARTITIONS_GROUPS_WITH_LOOPS 
    global NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG
    global CHECK_PAGERANK_OUTPUT
    global SAME_OUTPUT_FOR_ALL_FANOUT_FANIN
    global USE_INCREMENTAL_DAG_GENERATION
    global INCREMENTAL_DAG_DEPOSIT_INTERVAL
    global ENABLE_RUNTIME_TASK_CLUSTERING
    global WORK_QUEUE_SIZE_FOR_INCREMENTAL_DAG_GENERATION_WITH_WORKER_PROCESSES
    global TASKS_USE_RESULT_DICTIONARY_PARAMETER
    global USE_SHARED_PARTITIONS_GROUPS
    global USE_PAGERANK_GROUPS_PARTITIONSS
    global USE_STRUCT_OF_ARRAYS_FOR_PAGERANK
    global USE_MUTLITHREADED_NONINCREMENTAL_BFS
    global INPUT_ALL_GROUPS_PARTITIONS_AT_START

    RUN_ALL_TASKS_LOCALLY = True
    BYPASS_CALL_TO_INVOKE_REAL_LAMBDA = (not RUN_ALL_TASKS_LOCALLY) and True 
    STORE_FANINS_FANINNBS_LOCALLY = False 
    CREATE_ALL_FANINS_FANINNBS_ON_START = False
    USING_WORKERS = True
    USING_THREADS_NOT_PROCESSES = True
    NUM_WORKERS = 2
    USE_MULTITHREADED_MULTIPROCESSING = False
    NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING = 1

    #FANIN_TYPE = "DAG_executor_FanIn"
    #FANINNB_TYPE = "DAG_executor_FanInNB"
    #PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer"
    FANIN_TYPE = "DAG_executor_FanIn_Select"
    FANINNB_TYPE = "DAG_executor_FanInNB_Select"
    PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer_Select"

# remote objects with worker processes

#Test19: worker processes (A2) with non-selective-wait Sync-objects, 
#        1 worker, Sync-objects stored remotely (on tcp_server)
#        create objects at the start
# Note: tcp_server must be running: tcp_server -t 19
def test19():
    print("test19")
    global EXIT_PROGRAM_ON_EXCEPTION
    global DEALLOCATE_BFS_MAIN_MAP_ON_THE_FLY
    global RUN_ALL_TASKS_LOCALLY
    global BYPASS_CALL_TO_INVOKE_REAL_LAMBDA
    global STORE_FANINS_FANINNBS_LOCALLY
    global CREATE_ALL_FANINS_FANINNBS_ON_START
    global USING_WORKERS
    global USING_THREADS_NOT_PROCESSES
    global NUM_WORKERS
    global USE_MULTITHREADED_MULTIPROCESSING
    global NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING
    global FANIN_TYPE
    global FANINNB_TYPE
    global PROCESS_WORK_QUEUE_TYPE
    
    global STORE_SYNC_OBJECTS_IN_LAMBDAS
    global USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS
    global SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS
    global USING_DAG_ORCHESTRATOR
    global MAP_OBJECTS_TO_LAMBDA_FUNCTIONS
    global USE_ANONYMOUS_LAMBDA_FUNCTIONS
    global USE_SINGLE_LAMBDA_FUNCTION

    global PAGERANK_GRAPH_FILE_NAME
    global COMPUTE_PAGERANK
    global NUMBER_OF_PAGERANK_ITERATIONS_FOR_PARTITIONS_GROUPS_WITH_LOOPS 
    global NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG
    global CHECK_PAGERANK_OUTPUT
    global SAME_OUTPUT_FOR_ALL_FANOUT_FANIN
    global USE_INCREMENTAL_DAG_GENERATION
    global INCREMENTAL_DAG_DEPOSIT_INTERVAL
    global ENABLE_RUNTIME_TASK_CLUSTERING
    global WORK_QUEUE_SIZE_FOR_INCREMENTAL_DAG_GENERATION_WITH_WORKER_PROCESSES
    global TASKS_USE_RESULT_DICTIONARY_PARAMETER
    global USE_SHARED_PARTITIONS_GROUPS
    global USE_PAGERANK_GROUPS_PARTITIONS
    global USE_STRUCT_OF_ARRAYS_FOR_PAGERANK
    global USE_MUTLITHREADED_NONINCREMENTAL_BFS
    global INPUT_ALL_GROUPS_PARTITIONS_AT_START

    RUN_ALL_TASKS_LOCALLY = True
    BYPASS_CALL_TO_INVOKE_REAL_LAMBDA = (not RUN_ALL_TASKS_LOCALLY) and True 
    STORE_FANINS_FANINNBS_LOCALLY = False 
    CREATE_ALL_FANINS_FANINNBS_ON_START = True
    USING_WORKERS = True
    USING_THREADS_NOT_PROCESSES = False
    NUM_WORKERS = 1
    USE_MULTITHREADED_MULTIPROCESSING = False
    NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING = 1

    FANIN_TYPE = "DAG_executor_FanIn"
    FANINNB_TYPE = "DAG_executor_FanInNB"
    PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer"
    #FANIN_TYPE = "DAG_executor_FanIn_Select"
    #FANINNB_TYPE = "DAG_executor_FanInNB_Select"
    #PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer_Select"
    print("test19 end")

#Test20: worker processes (A2) with non-selective-wait Sync-objects, 
#        2 workers, Sync-objects stored remotely (on tcp_server)
#        create objects at the start
# Note: tcp_server must be running: tcp_server -t 20
def test20():
    print("test20")
    global EXIT_PROGRAM_ON_EXCEPTION
    global DEALLOCATE_BFS_MAIN_MAP_ON_THE_FLY
    global RUN_ALL_TASKS_LOCALLY
    global BYPASS_CALL_TO_INVOKE_REAL_LAMBDA
    global STORE_FANINS_FANINNBS_LOCALLY
    global CREATE_ALL_FANINS_FANINNBS_ON_START
    global USING_WORKERS
    global USING_THREADS_NOT_PROCESSES
    global NUM_WORKERS
    global USE_MULTITHREADED_MULTIPROCESSING
    global NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING
    global FANIN_TYPE
    global FANINNB_TYPE
    global PROCESS_WORK_QUEUE_TYPE
    
    global STORE_SYNC_OBJECTS_IN_LAMBDAS
    global USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS
    global SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS
    global USING_DAG_ORCHESTRATOR
    global MAP_OBJECTS_TO_LAMBDA_FUNCTIONS
    global USE_ANONYMOUS_LAMBDA_FUNCTIONS
    global USE_SINGLE_LAMBDA_FUNCTION

    global PAGERANK_GRAPH_FILE_NAME
    global COMPUTE_PAGERANK
    global NUMBER_OF_PAGERANK_ITERATIONS_FOR_PARTITIONS_GROUPS_WITH_LOOPS 
    global NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG
    global CHECK_PAGERANK_OUTPUT
    global SAME_OUTPUT_FOR_ALL_FANOUT_FANIN
    global USE_INCREMENTAL_DAG_GENERATION
    global INCREMENTAL_DAG_DEPOSIT_INTERVAL
    global ENABLE_RUNTIME_TASK_CLUSTERING
    global WORK_QUEUE_SIZE_FOR_INCREMENTAL_DAG_GENERATION_WITH_WORKER_PROCESSES
    global TASKS_USE_RESULT_DICTIONARY_PARAMETER
    global USE_SHARED_PARTITIONS_GROUPS
    global USE_PAGERANK_GROUPS_PARTITIONS
    global USE_STRUCT_OF_ARRAYS_FOR_PAGERANK
    global USE_MUTLITHREADED_NONINCREMENTAL_BFS
    global INPUT_ALL_GROUPS_PARTITIONS_AT_START

    RUN_ALL_TASKS_LOCALLY = True
    BYPASS_CALL_TO_INVOKE_REAL_LAMBDA = (not RUN_ALL_TASKS_LOCALLY) and True 
    STORE_FANINS_FANINNBS_LOCALLY = False 
    CREATE_ALL_FANINS_FANINNBS_ON_START = True
    USING_WORKERS = True
    USING_THREADS_NOT_PROCESSES = False
    NUM_WORKERS = 2
    USE_MULTITHREADED_MULTIPROCESSING = False
    NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING = 1

    FANIN_TYPE = "DAG_executor_FanIn"
    FANINNB_TYPE = "DAG_executor_FanInNB"
    PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer"
    #FANIN_TYPE = "DAG_executor_FanIn_Select"
    #FANINNB_TYPE = "DAG_executor_FanInNB_Select"
    #PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer_Select"

#Test21: worker processes (A2) with non-selective-wait Sync-objects, 
#        2 workers, Sync-objects stored remotely (on tcp_server)
#        create objects on-the-fly
# Note: tcp_server must be running: tcp_server -t 21
def test21():
    print("test21")
    global EXIT_PROGRAM_ON_EXCEPTION
    global DEALLOCATE_BFS_MAIN_MAP_ON_THE_FLY
    global RUN_ALL_TASKS_LOCALLY
    global BYPASS_CALL_TO_INVOKE_REAL_LAMBDA
    global STORE_FANINS_FANINNBS_LOCALLY
    global CREATE_ALL_FANINS_FANINNBS_ON_START
    global USING_WORKERS
    global USING_THREADS_NOT_PROCESSES
    global NUM_WORKERS
    global USE_MULTITHREADED_MULTIPROCESSING
    global NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING
    global FANIN_TYPE
    global FANINNB_TYPE
    global PROCESS_WORK_QUEUE_TYPE
    
    global STORE_SYNC_OBJECTS_IN_LAMBDAS
    global USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS
    global SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS
    global USING_DAG_ORCHESTRATOR
    global MAP_OBJECTS_TO_LAMBDA_FUNCTIONS
    global USE_ANONYMOUS_LAMBDA_FUNCTIONS
    global USE_SINGLE_LAMBDA_FUNCTION

    global PAGERANK_GRAPH_FILE_NAME
    global COMPUTE_PAGERANK
    global NUMBER_OF_PAGERANK_ITERATIONS_FOR_PARTITIONS_GROUPS_WITH_LOOPS 
    global NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG
    global CHECK_PAGERANK_OUTPUT
    global SAME_OUTPUT_FOR_ALL_FANOUT_FANIN
    global USE_INCREMENTAL_DAG_GENERATION
    global INCREMENTAL_DAG_DEPOSIT_INTERVAL
    global ENABLE_RUNTIME_TASK_CLUSTERING
    global WORK_QUEUE_SIZE_FOR_INCREMENTAL_DAG_GENERATION_WITH_WORKER_PROCESSES
    global TASKS_USE_RESULT_DICTIONARY_PARAMETER
    global USE_SHARED_PARTITIONS_GROUPS
    global USE_PAGERANK_GROUPS_PARTITIONS
    global USE_STRUCT_OF_ARRAYS_FOR_PAGERANK
    global USE_MUTLITHREADED_NONINCREMENTAL_BFS
    global INPUT_ALL_GROUPS_PARTITIONS_AT_START

    RUN_ALL_TASKS_LOCALLY = True
    BYPASS_CALL_TO_INVOKE_REAL_LAMBDA = (not RUN_ALL_TASKS_LOCALLY) and True 
    STORE_FANINS_FANINNBS_LOCALLY = False 
    CREATE_ALL_FANINS_FANINNBS_ON_START = False
    USING_WORKERS = True
    USING_THREADS_NOT_PROCESSES = False
    NUM_WORKERS = 2
    USE_MULTITHREADED_MULTIPROCESSING = False
    NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING = 1

    FANIN_TYPE = "DAG_executor_FanIn"
    FANINNB_TYPE = "DAG_executor_FanInNB"
    PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer"
    #FANIN_TYPE = "DAG_executor_FanIn_Select"
    #FANINNB_TYPE = "DAG_executor_FanInNB_Select"
    #PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer_Select"

#Test22: worker processes (A2) with selective-wait Sync-objects, 
#        2 workers, Sync-objects stored remotely (on tcp_server)
#        create objects at the start
# Note: tcp_server must be running: tcp_server -t 22
def test22():
    print("test22")
    global EXIT_PROGRAM_ON_EXCEPTION
    global DEALLOCATE_BFS_MAIN_MAP_ON_THE_FLY
    global RUN_ALL_TASKS_LOCALLY
    global BYPASS_CALL_TO_INVOKE_REAL_LAMBDA
    global STORE_FANINS_FANINNBS_LOCALLY
    global CREATE_ALL_FANINS_FANINNBS_ON_START
    global USING_WORKERS
    global USING_THREADS_NOT_PROCESSES
    global NUM_WORKERS
    global USE_MULTITHREADED_MULTIPROCESSING
    global NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING
    global FANIN_TYPE
    global FANINNB_TYPE
    global PROCESS_WORK_QUEUE_TYPE
    
    global STORE_SYNC_OBJECTS_IN_LAMBDAS
    global USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS
    global SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS
    global USING_DAG_ORCHESTRATOR
    global MAP_OBJECTS_TO_LAMBDA_FUNCTIONS
    global USE_ANONYMOUS_LAMBDA_FUNCTIONS
    global USE_SINGLE_LAMBDA_FUNCTION

    global PAGERANK_GRAPH_FILE_NAME
    global COMPUTE_PAGERANK
    global NUMBER_OF_PAGERANK_ITERATIONS_FOR_PARTITIONS_GROUPS_WITH_LOOPS 
    global NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG
    global CHECK_PAGERANK_OUTPUT
    global SAME_OUTPUT_FOR_ALL_FANOUT_FANIN
    global USE_INCREMENTAL_DAG_GENERATION
    global INCREMENTAL_DAG_DEPOSIT_INTERVAL
    global ENABLE_RUNTIME_TASK_CLUSTERING
    global WORK_QUEUE_SIZE_FOR_INCREMENTAL_DAG_GENERATION_WITH_WORKER_PROCESSES
    global TASKS_USE_RESULT_DICTIONARY_PARAMETER
    global USE_SHARED_PARTITIONS_GROUPS
    global USE_PAGERANK_GROUPS_PARTITIONSFS
    global USE_STRUCT_OF_ARRAYS_FOR_PAGERANK
    global USE_MUTLITHREADED_NONINCREMENTAL_BFS
    global INPUT_ALL_GROUPS_PARTITIONS_AT_START

    RUN_ALL_TASKS_LOCALLY = True
    BYPASS_CALL_TO_INVOKE_REAL_LAMBDA = (not RUN_ALL_TASKS_LOCALLY) and True 
    STORE_FANINS_FANINNBS_LOCALLY = False 
    CREATE_ALL_FANINS_FANINNBS_ON_START = True
    USING_WORKERS = True
    USING_THREADS_NOT_PROCESSES = False
    NUM_WORKERS = 2
    USE_MULTITHREADED_MULTIPROCESSING = False
    NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING = 1

    #FANIN_TYPE = "DAG_executor_FanIn"
    #FANINNB_TYPE = "DAG_executor_FanInNB"
    #PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer"
    FANIN_TYPE = "DAG_executor_FanIn_Select"
    FANINNB_TYPE = "DAG_executor_FanInNB_Select"
    PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer_Select"

#Test23: worker processes (A2) with selective-wait Sync-objects, 
#        2 workers, Sync-objects stored remotely (on tcp_server)
#        create objects on-the-fly
# Note: tcp_server must be running: tcp_server -t 23
def test23():
    print("test23")
    global EXIT_PROGRAM_ON_EXCEPTION
    global DEALLOCATE_BFS_MAIN_MAP_ON_THE_FLY
    global RUN_ALL_TASKS_LOCALLY
    global BYPASS_CALL_TO_INVOKE_REAL_LAMBDA
    global STORE_FANINS_FANINNBS_LOCALLY
    global CREATE_ALL_FANINS_FANINNBS_ON_START
    global USING_WORKERS
    global USING_THREADS_NOT_PROCESSES
    global NUM_WORKERS
    global USE_MULTITHREADED_MULTIPROCESSING
    global NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING
    global FANIN_TYPE
    global FANINNB_TYPE
    global PROCESS_WORK_QUEUE_TYPE
    
    global STORE_SYNC_OBJECTS_IN_LAMBDAS
    global USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS
    global SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS
    global USING_DAG_ORCHESTRATOR
    global MAP_OBJECTS_TO_LAMBDA_FUNCTIONS
    global USE_ANONYMOUS_LAMBDA_FUNCTIONS
    global USE_SINGLE_LAMBDA_FUNCTION

    global PAGERANK_GRAPH_FILE_NAME
    global COMPUTE_PAGERANK
    global NUMBER_OF_PAGERANK_ITERATIONS_FOR_PARTITIONS_GROUPS_WITH_LOOPS 
    global NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG
    global CHECK_PAGERANK_OUTPUT
    global SAME_OUTPUT_FOR_ALL_FANOUT_FANIN
    global USE_INCREMENTAL_DAG_GENERATION
    global INCREMENTAL_DAG_DEPOSIT_INTERVAL
    global ENABLE_RUNTIME_TASK_CLUSTERING
    global WORK_QUEUE_SIZE_FOR_INCREMENTAL_DAG_GENERATION_WITH_WORKER_PROCESSES
    global TASKS_USE_RESULT_DICTIONARY_PARAMETER
    global USE_SHARED_PARTITIONS_GROUPS
    global USE_PAGERANK_GROUPS_PARTITIONS
    global USE_STRUCT_OF_ARRAYS_FOR_PAGERANK
    global USE_MUTLITHREADED_NONINCREMENTAL_BFS
    global INPUT_ALL_GROUPS_PARTITIONS_AT_START

    RUN_ALL_TASKS_LOCALLY = True
    BYPASS_CALL_TO_INVOKE_REAL_LAMBDA = (not RUN_ALL_TASKS_LOCALLY) and True 
    STORE_FANINS_FANINNBS_LOCALLY = False 
    CREATE_ALL_FANINS_FANINNBS_ON_START = False
    USING_WORKERS = True
    USING_THREADS_NOT_PROCESSES = False
    NUM_WORKERS = 2
    USE_MULTITHREADED_MULTIPROCESSING = False
    NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING = 1

    #FANIN_TYPE = "DAG_executor_FanIn"
    #FANINNB_TYPE = "DAG_executor_FanInNB"
    #PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer"
    FANIN_TYPE = "DAG_executor_FanIn_Select"
    FANINNB_TYPE = "DAG_executor_FanInNB_Select"
    PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer_Select"

#Test24: multithreaded worker processes (A2) with non-selective-wait Sync-objects, 
#        1 thread for the 1 worker process, Sync-objects stored remotely (on tcp_server)
#        create objects at the start
# Note: tcp_server must be running: tcp_server -t 24
def test24():
    print("test24")
    global EXIT_PROGRAM_ON_EXCEPTION
    global DEALLOCATE_BFS_MAIN_MAP_ON_THE_FLY
    global RUN_ALL_TASKS_LOCALLY
    global BYPASS_CALL_TO_INVOKE_REAL_LAMBDA
    global STORE_FANINS_FANINNBS_LOCALLY
    global CREATE_ALL_FANINS_FANINNBS_ON_START
    global USING_WORKERS
    global USING_THREADS_NOT_PROCESSES
    global NUM_WORKERS
    global USE_MULTITHREADED_MULTIPROCESSING
    global NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING
    global FANIN_TYPE
    global FANINNB_TYPE
    global PROCESS_WORK_QUEUE_TYPE
    
    global STORE_SYNC_OBJECTS_IN_LAMBDAS
    global USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS
    global SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS
    global USING_DAG_ORCHESTRATOR
    global MAP_OBJECTS_TO_LAMBDA_FUNCTIONS
    global USE_ANONYMOUS_LAMBDA_FUNCTIONS
    global USE_SINGLE_LAMBDA_FUNCTION

    global PAGERANK_GRAPH_FILE_NAME
    global COMPUTE_PAGERANK
    global NUMBER_OF_PAGERANK_ITERATIONS_FOR_PARTITIONS_GROUPS_WITH_LOOPS 
    global NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG
    global CHECK_PAGERANK_OUTPUT
    global SAME_OUTPUT_FOR_ALL_FANOUT_FANIN
    global USE_INCREMENTAL_DAG_GENERATION
    global INCREMENTAL_DAG_DEPOSIT_INTERVAL
    global ENABLE_RUNTIME_TASK_CLUSTERING
    global WORK_QUEUE_SIZE_FOR_INCREMENTAL_DAG_GENERATION_WITH_WORKER_PROCESSES
    global TASKS_USE_RESULT_DICTIONARY_PARAMETER
    global USE_SHARED_PARTITIONS_GROUPS
    global USE_PAGERANK_GROUPS_PARTITIONS
    global USE_STRUCT_OF_ARRAYS_FOR_PAGERANK
    global USE_MUTLITHREADED_NONINCREMENTAL_BFS
    global INPUT_ALL_GROUPS_PARTITIONS_AT_START
    RUN_ALL_TASKS_LOCALLY = True
    BYPASS_CALL_TO_INVOKE_REAL_LAMBDA = (not RUN_ALL_TASKS_LOCALLY) and True 
    STORE_FANINS_FANINNBS_LOCALLY = False 
    CREATE_ALL_FANINS_FANINNBS_ON_START = True
    USING_WORKERS = True
    USING_THREADS_NOT_PROCESSES = False
    NUM_WORKERS = 1
    USE_MULTITHREADED_MULTIPROCESSING = True
    NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING = 1

    FANIN_TYPE = "DAG_executor_FanIn"
    FANINNB_TYPE = "DAG_executor_FanInNB"
    PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer"
    #FANIN_TYPE = "DAG_executor_FanIn_Select"
    #FANINNB_TYPE = "DAG_executor_FanInNB_Select"
    #PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer_Select"

#Test25: multithreaded worker processes (A2) with non-selective-wait Sync-objects, 
#        2 threads for the 1 worker process, Sync-objects stored remotely (on tcp_server)
#        create objects at the start
# Note: tcp_server must be running: tcp_server -t 25
def test25():
    print("test25")
    global EXIT_PROGRAM_ON_EXCEPTION
    global DEALLOCATE_BFS_MAIN_MAP_ON_THE_FLY
    global RUN_ALL_TASKS_LOCALLY
    global BYPASS_CALL_TO_INVOKE_REAL_LAMBDA
    global STORE_FANINS_FANINNBS_LOCALLY
    global CREATE_ALL_FANINS_FANINNBS_ON_START
    global USING_WORKERS
    global USING_THREADS_NOT_PROCESSES
    global NUM_WORKERS
    global USE_MULTITHREADED_MULTIPROCESSING
    global NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING
    global FANIN_TYPE
    global FANINNB_TYPE
    global PROCESS_WORK_QUEUE_TYPE
    
    global STORE_SYNC_OBJECTS_IN_LAMBDAS
    global USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS
    global SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS
    global USING_DAG_ORCHESTRATOR
    global MAP_OBJECTS_TO_LAMBDA_FUNCTIONS
    global USE_ANONYMOUS_LAMBDA_FUNCTIONS
    global USE_SINGLE_LAMBDA_FUNCTION

    global PAGERANK_GRAPH_FILE_NAME
    global COMPUTE_PAGERANK
    global NUMBER_OF_PAGERANK_ITERATIONS_FOR_PARTITIONS_GROUPS_WITH_LOOPS 
    global NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG
    global CHECK_PAGERANK_OUTPUT
    global SAME_OUTPUT_FOR_ALL_FANOUT_FANIN
    global USE_INCREMENTAL_DAG_GENERATION
    global INCREMENTAL_DAG_DEPOSIT_INTERVAL
    global ENABLE_RUNTIME_TASK_CLUSTERING
    global WORK_QUEUE_SIZE_FOR_INCREMENTAL_DAG_GENERATION_WITH_WORKER_PROCESSES
    global TASKS_USE_RESULT_DICTIONARY_PARAMETER
    global USE_SHARED_PARTITIONS_GROUPS
    global USE_PAGERANK_GROUPS_PARTITIONS
    global USE_STRUCT_OF_ARRAYS_FOR_PAGERANK
    global USE_MUTLITHREADED_NONINCREMENTAL_BFS
    global INPUT_ALL_GROUPS_PARTITIONS_AT_START

    RUN_ALL_TASKS_LOCALLY = True
    BYPASS_CALL_TO_INVOKE_REAL_LAMBDA = (not RUN_ALL_TASKS_LOCALLY) and True 
    STORE_FANINS_FANINNBS_LOCALLY = False 
    CREATE_ALL_FANINS_FANINNBS_ON_START = True
    USING_WORKERS = True
    USING_THREADS_NOT_PROCESSES = False
    NUM_WORKERS = 1
    USE_MULTITHREADED_MULTIPROCESSING = True
    NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING = 2

    FANIN_TYPE = "DAG_executor_FanIn"
    FANINNB_TYPE = "DAG_executor_FanInNB"
    PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer"
    #FANIN_TYPE = "DAG_executor_FanIn_Select"
    #FANINNB_TYPE = "DAG_executor_FanInNB_Select"
    #PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer_Select"

#Test26: multithreaded worker processes (A2) with non-selective-wait Sync-objects, 
#        2 threads for each of 2 worker process, Sync-objects stored remotely (on tcp_server)
#        create objects at the start
# Note: tcp_server must be running: tcp_server -t 26
def test26():
    print("test26")
    global EXIT_PROGRAM_ON_EXCEPTION
    global DEALLOCATE_BFS_MAIN_MAP_ON_THE_FLY
    global RUN_ALL_TASKS_LOCALLY
    global BYPASS_CALL_TO_INVOKE_REAL_LAMBDA
    global STORE_FANINS_FANINNBS_LOCALLY
    global CREATE_ALL_FANINS_FANINNBS_ON_START
    global USING_WORKERS
    global USING_THREADS_NOT_PROCESSES
    global NUM_WORKERS
    global USE_MULTITHREADED_MULTIPROCESSING
    global NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING
    global FANIN_TYPE
    global FANINNB_TYPE
    global PROCESS_WORK_QUEUE_TYPE
    
    global STORE_SYNC_OBJECTS_IN_LAMBDAS
    global USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS
    global SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS
    global USING_DAG_ORCHESTRATOR
    global MAP_OBJECTS_TO_LAMBDA_FUNCTIONS
    global USE_ANONYMOUS_LAMBDA_FUNCTIONS
    global USE_SINGLE_LAMBDA_FUNCTION

    global PAGERANK_GRAPH_FILE_NAME
    global COMPUTE_PAGERANK
    global NUMBER_OF_PAGERANK_ITERATIONS_FOR_PARTITIONS_GROUPS_WITH_LOOPS 
    global NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG
    global CHECK_PAGERANK_OUTPUT
    global SAME_OUTPUT_FOR_ALL_FANOUT_FANIN
    global USE_INCREMENTAL_DAG_GENERATION
    global INCREMENTAL_DAG_DEPOSIT_INTERVAL
    global ENABLE_RUNTIME_TASK_CLUSTERING
    global WORK_QUEUE_SIZE_FOR_INCREMENTAL_DAG_GENERATION_WITH_WORKER_PROCESSES
    global TASKS_USE_RESULT_DICTIONARY_PARAMETER
    global USE_SHARED_PARTITIONS_GROUPS
    global USE_PAGERANK_GROUPS_PARTITIONS
    global USE_STRUCT_OF_ARRAYS_FOR_PAGERANK
    global USE_MUTLITHREADED_NONINCREMENTAL_BFS
    global INPUT_ALL_GROUPS_PARTITIONS_AT_START

    RUN_ALL_TASKS_LOCALLY = True
    BYPASS_CALL_TO_INVOKE_REAL_LAMBDA = (not RUN_ALL_TASKS_LOCALLY) and True 
    STORE_FANINS_FANINNBS_LOCALLY = False 
    CREATE_ALL_FANINS_FANINNBS_ON_START = True
    USING_WORKERS = True
    USING_THREADS_NOT_PROCESSES = False
    NUM_WORKERS = 2
    USE_MULTITHREADED_MULTIPROCESSING = True
    NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING = 2

    FANIN_TYPE = "DAG_executor_FanIn"
    FANINNB_TYPE = "DAG_executor_FanInNB"
    PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer"
    #FANIN_TYPE = "DAG_executor_FanIn_Select"
    #FANINNB_TYPE = "DAG_executor_FanInNB_Select"
    #PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer_Select"

#Test27: multithreaded worker processes (A2) with selective-wait Sync-objects, 
#        2 threads for the 1 worker process, Sync-objects stored remotely (on tcp_server)
#        create objects at the start
# Note: tcp_server must be running: tcp_server -t 27
def test27():
    print("test27")
    global EXIT_PROGRAM_ON_EXCEPTION
    global DEALLOCATE_BFS_MAIN_MAP_ON_THE_FLY
    global RUN_ALL_TASKS_LOCALLY
    global BYPASS_CALL_TO_INVOKE_REAL_LAMBDA
    global STORE_FANINS_FANINNBS_LOCALLY
    global CREATE_ALL_FANINS_FANINNBS_ON_START
    global USING_WORKERS
    global USING_THREADS_NOT_PROCESSES
    global NUM_WORKERS
    global USE_MULTITHREADED_MULTIPROCESSING
    global NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING
    global FANIN_TYPE
    global FANINNB_TYPE
    global PROCESS_WORK_QUEUE_TYPE
    
    global STORE_SYNC_OBJECTS_IN_LAMBDAS
    global USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS
    global SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS
    global USING_DAG_ORCHESTRATOR
    global MAP_OBJECTS_TO_LAMBDA_FUNCTIONS
    global USE_ANONYMOUS_LAMBDA_FUNCTIONS
    global USE_SINGLE_LAMBDA_FUNCTION

    global PAGERANK_GRAPH_FILE_NAME
    global COMPUTE_PAGERANK
    global NUMBER_OF_PAGERANK_ITERATIONS_FOR_PARTITIONS_GROUPS_WITH_LOOPS 
    global NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG
    global CHECK_PAGERANK_OUTPUT
    global SAME_OUTPUT_FOR_ALL_FANOUT_FANIN
    global USE_INCREMENTAL_DAG_GENERATION
    global INCREMENTAL_DAG_DEPOSIT_INTERVAL
    global ENABLE_RUNTIME_TASK_CLUSTERING
    global WORK_QUEUE_SIZE_FOR_INCREMENTAL_DAG_GENERATION_WITH_WORKER_PROCESSES
    global TASKS_USE_RESULT_DICTIONARY_PARAMETER
    global USE_SHARED_PARTITIONS_GROUPS
    global USE_PAGERANK_GROUPS_PARTITIONS
    global USE_STRUCT_OF_ARRAYS_FOR_PAGERANK
    global USE_MUTLITHREADED_NONINCREMENTAL_BFS
    global INPUT_ALL_GROUPS_PARTITIONS_AT_START

    RUN_ALL_TASKS_LOCALLY = True
    BYPASS_CALL_TO_INVOKE_REAL_LAMBDA = (not RUN_ALL_TASKS_LOCALLY) and True 
    STORE_FANINS_FANINNBS_LOCALLY = False 
    CREATE_ALL_FANINS_FANINNBS_ON_START = True
    USING_WORKERS = True
    USING_THREADS_NOT_PROCESSES = False
    NUM_WORKERS = 1
    USE_MULTITHREADED_MULTIPROCESSING = True
    NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING = 2

    #FANIN_TYPE = "DAG_executor_FanIn"
    #FANINNB_TYPE = "DAG_executor_FanInNB"
    #PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer"
    FANIN_TYPE = "DAG_executor_FanIn_Select"
    FANINNB_TYPE = "DAG_executor_FanInNB_Select"
    PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer_Select"

#Test28: multithreaded worker processes (A2) with selective-wait Sync-objects, 
#        2 threads for each of 2 worker process, Sync-objects stored remotely (on tcp_server)
#        create objects at the start
# Note: tcp_server must be running: tcp_server -t 28
def test28():
    print("test28")
    global EXIT_PROGRAM_ON_EXCEPTION
    global DEALLOCATE_BFS_MAIN_MAP_ON_THE_FLY
    global RUN_ALL_TASKS_LOCALLY
    global BYPASS_CALL_TO_INVOKE_REAL_LAMBDA
    global STORE_FANINS_FANINNBS_LOCALLY
    global CREATE_ALL_FANINS_FANINNBS_ON_START
    global USING_WORKERS
    global USING_THREADS_NOT_PROCESSES
    global NUM_WORKERS
    global USE_MULTITHREADED_MULTIPROCESSING
    global NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING
    global FANIN_TYPE
    global FANINNB_TYPE
    global PROCESS_WORK_QUEUE_TYPE
    
    global STORE_SYNC_OBJECTS_IN_LAMBDAS
    global USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS
    global SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS
    global USING_DAG_ORCHESTRATOR
    global MAP_OBJECTS_TO_LAMBDA_FUNCTIONS
    global USE_ANONYMOUS_LAMBDA_FUNCTIONS
    global USE_SINGLE_LAMBDA_FUNCTION

    global PAGERANK_GRAPH_FILE_NAME
    global COMPUTE_PAGERANK
    global NUMBER_OF_PAGERANK_ITERATIONS_FOR_PARTITIONS_GROUPS_WITH_LOOPS 
    global NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG
    global CHECK_PAGERANK_OUTPUT
    global SAME_OUTPUT_FOR_ALL_FANOUT_FANIN
    global USE_INCREMENTAL_DAG_GENERATION
    global INCREMENTAL_DAG_DEPOSIT_INTERVAL
    global ENABLE_RUNTIME_TASK_CLUSTERING
    global WORK_QUEUE_SIZE_FOR_INCREMENTAL_DAG_GENERATION_WITH_WORKER_PROCESSES
    global TASKS_USE_RESULT_DICTIONARY_PARAMETER
    global USE_SHARED_PARTITIONS_GROUPS
    global USE_PAGERANK_GROUPS_PARTITIONS
    global USE_STRUCT_OF_ARRAYS_FOR_PAGERANK
    global USE_MUTLITHREADED_NONINCREMENTAL_BFS
    global INPUT_ALL_GROUPS_PARTITIONS_AT_START

    RUN_ALL_TASKS_LOCALLY = True
    BYPASS_CALL_TO_INVOKE_REAL_LAMBDA = (not RUN_ALL_TASKS_LOCALLY) and True 
    STORE_FANINS_FANINNBS_LOCALLY = False 
    CREATE_ALL_FANINS_FANINNBS_ON_START = True
    USING_WORKERS = True
    USING_THREADS_NOT_PROCESSES = False
    NUM_WORKERS = 2
    USE_MULTITHREADED_MULTIPROCESSING = True
    NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING = 2

    #FANIN_TYPE = "DAG_executor_FanIn"
    #FANINNB_TYPE = "DAG_executor_FanInNB"
    #PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer"
    FANIN_TYPE = "DAG_executor_FanIn_Select"
    FANINNB_TYPE = "DAG_executor_FanInNB_Select"
    PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer_Select"

#Test STORE_SYNC_OBJECTS_IN_LAMBDAS

#Test29: worker threads (A2) with selective-wait Sync-objects, 
#        Sync-objects stored remotely (on tcp_server)
#        create objects at the start.
#        STORE_SYNC_OBJECTS_IN_LAMBDAS
# Note: tcp_server must be running: tcp_server -t 29
def test29():
    print("test29")
    global EXIT_PROGRAM_ON_EXCEPTION
    global DEALLOCATE_BFS_MAIN_MAP_ON_THE_FLY
    global RUN_ALL_TASKS_LOCALLY
    global BYPASS_CALL_TO_INVOKE_REAL_LAMBDA
    global STORE_FANINS_FANINNBS_LOCALLY
    global CREATE_ALL_FANINS_FANINNBS_ON_START
    global USING_WORKERS
    global USING_THREADS_NOT_PROCESSES
    global NUM_WORKERS
    global USE_MULTITHREADED_MULTIPROCESSING
    global NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING
    global FANIN_TYPE
    global FANINNB_TYPE
    global PROCESS_WORK_QUEUE_TYPE
    
    global STORE_SYNC_OBJECTS_IN_LAMBDAS
    global USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS
    global SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS
    global USING_DAG_ORCHESTRATOR
    global MAP_OBJECTS_TO_LAMBDA_FUNCTIONS
    global USE_ANONYMOUS_LAMBDA_FUNCTIONS
    global USE_SINGLE_LAMBDA_FUNCTION

    global PAGERANK_GRAPH_FILE_NAME
    global COMPUTE_PAGERANK
    global NUMBER_OF_PAGERANK_ITERATIONS_FOR_PARTITIONS_GROUPS_WITH_LOOPS 
    global NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG
    global CHECK_PAGERANK_OUTPUT
    global SAME_OUTPUT_FOR_ALL_FANOUT_FANIN
    global USE_INCREMENTAL_DAG_GENERATION
    global INCREMENTAL_DAG_DEPOSIT_INTERVAL
    global ENABLE_RUNTIME_TASK_CLUSTERING
    global WORK_QUEUE_SIZE_FOR_INCREMENTAL_DAG_GENERATION_WITH_WORKER_PROCESSES
    global TASKS_USE_RESULT_DICTIONARY_PARAMETER
    global USE_SHARED_PARTITIONS_GROUPS
    global USE_PAGERANK_GROUPS_PARTITIONS
    global USE_STRUCT_OF_ARRAYS_FOR_PAGERANK
    global USE_MUTLITHREADED_NONINCREMENTAL_BFS
    global INPUT_ALL_GROUPS_PARTITIONS_AT_START

    RUN_ALL_TASKS_LOCALLY = True
    BYPASS_CALL_TO_INVOKE_REAL_LAMBDA = (not RUN_ALL_TASKS_LOCALLY) and True 
    STORE_FANINS_FANINNBS_LOCALLY = False 
    CREATE_ALL_FANINS_FANINNBS_ON_START = True
    USING_WORKERS = True
    USING_THREADS_NOT_PROCESSES = True
    NUM_WORKERS = 2
    USE_MULTITHREADED_MULTIPROCESSING = False
    NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING = 1

    #FANIN_TYPE = "DAG_executor_FanIn"
    #FANINNB_TYPE = "DAG_executor_FanInNB"
    #PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer"
    FANIN_TYPE = "DAG_executor_FanIn_Select"
    FANINNB_TYPE = "DAG_executor_FanInNB_Select"
    PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer_Select"

    # TTFFTFF: no trigger and no DAG_orchestrator, but map objects 
    # (anon is false) and create objects on start and
    # do not USE_SINGLE_LAMBDA_FUNCTION

    STORE_SYNC_OBJECTS_IN_LAMBDAS = True
    USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS = True
    SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS = False
    USING_DAG_ORCHESTRATOR = False
    MAP_OBJECTS_TO_LAMBDA_FUNCTIONS = True
    USE_ANONYMOUS_LAMBDA_FUNCTIONS = False
    USE_SINGLE_LAMBDA_FUNCTION = False

#Test30: worker threads (A2) with selective-wait Sync-objects, 
#        Sync-objects stored remotely (on tcp_server)
#        create objects at the start
#        STORE_SYNC_OBJECTS_IN_LAMBDAS
#        Variation: a. change D_O to T, 
# Note: tcp_server must be running: tcp_server -t 30
def test30():
    print("test30")
    global EXIT_PROGRAM_ON_EXCEPTION
    global DEALLOCATE_BFS_MAIN_MAP_ON_THE_FLY
    global RUN_ALL_TASKS_LOCALLY
    global BYPASS_CALL_TO_INVOKE_REAL_LAMBDA
    global STORE_FANINS_FANINNBS_LOCALLY
    global CREATE_ALL_FANINS_FANINNBS_ON_START
    global USING_WORKERS
    global USING_THREADS_NOT_PROCESSES
    global NUM_WORKERS
    global USE_MULTITHREADED_MULTIPROCESSING
    global NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING
    global FANIN_TYPE
    global FANINNB_TYPE
    global PROCESS_WORK_QUEUE_TYPE
    
    global STORE_SYNC_OBJECTS_IN_LAMBDAS
    global USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS
    global SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS
    global USING_DAG_ORCHESTRATOR
    global MAP_OBJECTS_TO_LAMBDA_FUNCTIONS
    global USE_ANONYMOUS_LAMBDA_FUNCTIONS
    global USE_SINGLE_LAMBDA_FUNCTION

    global PAGERANK_GRAPH_FILE_NAME
    global COMPUTE_PAGERANK
    global NUMBER_OF_PAGERANK_ITERATIONS_FOR_PARTITIONS_GROUPS_WITH_LOOPS 
    global NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG
    global CHECK_PAGERANK_OUTPUT
    global SAME_OUTPUT_FOR_ALL_FANOUT_FANIN
    global USE_INCREMENTAL_DAG_GENERATION
    global INCREMENTAL_DAG_DEPOSIT_INTERVAL
    global ENABLE_RUNTIME_TASK_CLUSTERING
    global WORK_QUEUE_SIZE_FOR_INCREMENTAL_DAG_GENERATION_WITH_WORKER_PROCESSES
    global TASKS_USE_RESULT_DICTIONARY_PARAMETER
    global USE_SHARED_PARTITIONS_GROUPS
    global USE_PAGERANK_GROUPS_PARTITIONS    
    global USE_STRUCT_OF_ARRAYS_FOR_PAGERANK
    global USE_MUTLITHREADED_NONINCREMENTAL_BFS
    global INPUT_ALL_GROUPS_PARTITIONS_AT_START

    RUN_ALL_TASKS_LOCALLY = True
    BYPASS_CALL_TO_INVOKE_REAL_LAMBDA = (not RUN_ALL_TASKS_LOCALLY) and True 
    STORE_FANINS_FANINNBS_LOCALLY = False 
    CREATE_ALL_FANINS_FANINNBS_ON_START = True
    USING_WORKERS = True
    USING_THREADS_NOT_PROCESSES = True
    NUM_WORKERS = 2
    USE_MULTITHREADED_MULTIPROCESSING = False
    NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING = 1

    #FANIN_TYPE = "DAG_executor_FanIn"
    #FANINNB_TYPE = "DAG_executor_FanInNB"
    #PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer"
    FANIN_TYPE = "DAG_executor_FanIn_Select"
    FANINNB_TYPE = "DAG_executor_FanInNB_Select"
    PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer_Select"

    STORE_SYNC_OBJECTS_IN_LAMBDAS = True
    USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS = True
    SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS = False
    USING_DAG_ORCHESTRATOR = True
    MAP_OBJECTS_TO_LAMBDA_FUNCTIONS = True
    USE_ANONYMOUS_LAMBDA_FUNCTIONS = False
    USE_SINGLE_LAMBDA_FUNCTION = False

#Test31: worker threads (A2) with selective-wait Sync-objects, 
#        Sync-objects stored remotely (on tcp_server)
#        create objects at the start
#        STORE_SYNC_OBJECTS_IN_LAMBDAS
#        Variation: a. change D_O to T, 
#                   b. change map to F, and anon to T:
# Note: tcp_server must be running: tcp_server -t 31
def test31():
    print("test31")
    global EXIT_PROGRAM_ON_EXCEPTION
    global DEALLOCATE_BFS_MAIN_MAP_ON_THE_FLY
    global RUN_ALL_TASKS_LOCALLY
    global BYPASS_CALL_TO_INVOKE_REAL_LAMBDA
    global STORE_FANINS_FANINNBS_LOCALLY
    global CREATE_ALL_FANINS_FANINNBS_ON_START
    global USING_WORKERS
    global USING_THREADS_NOT_PROCESSES
    global NUM_WORKERS
    global USE_MULTITHREADED_MULTIPROCESSING
    global NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING
    global FANIN_TYPE
    global FANINNB_TYPE
    global PROCESS_WORK_QUEUE_TYPE
    
    global STORE_SYNC_OBJECTS_IN_LAMBDAS
    global USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS
    global SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS
    global USING_DAG_ORCHESTRATOR
    global MAP_OBJECTS_TO_LAMBDA_FUNCTIONS
    global USE_ANONYMOUS_LAMBDA_FUNCTIONS
    global USE_SINGLE_LAMBDA_FUNCTION

    global PAGERANK_GRAPH_FILE_NAME
    global COMPUTE_PAGERANK
    global NUMBER_OF_PAGERANK_ITERATIONS_FOR_PARTITIONS_GROUPS_WITH_LOOPS 
    global NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG
    global CHECK_PAGERANK_OUTPUT
    global SAME_OUTPUT_FOR_ALL_FANOUT_FANIN
    global USE_INCREMENTAL_DAG_GENERATION
    global INCREMENTAL_DAG_DEPOSIT_INTERVAL
    global ENABLE_RUNTIME_TASK_CLUSTERING
    global WORK_QUEUE_SIZE_FOR_INCREMENTAL_DAG_GENERATION_WITH_WORKER_PROCESSES
    global TASKS_USE_RESULT_DICTIONARY_PARAMETER
    global USE_SHARED_PARTITIONS_GROUPS
    global USE_PAGERANK_GROUPS_PARTITIONS
    global USE_STRUCT_OF_ARRAYS_FOR_PAGERANK
    global USE_MUTLITHREADED_NONINCREMENTAL_BFS
    global INPUT_ALL_GROUPS_PARTITIONS_AT_START

    RUN_ALL_TASKS_LOCALLY = True
    BYPASS_CALL_TO_INVOKE_REAL_LAMBDA = (not RUN_ALL_TASKS_LOCALLY) and True 
    STORE_FANINS_FANINNBS_LOCALLY = False 
    CREATE_ALL_FANINS_FANINNBS_ON_START = True
    USING_WORKERS = True
    USING_THREADS_NOT_PROCESSES = True
    NUM_WORKERS = 2
    USE_MULTITHREADED_MULTIPROCESSING = False
    NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING = 1

    #FANIN_TYPE = "DAG_executor_FanIn"
    #FANINNB_TYPE = "DAG_executor_FanInNB"
    #PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer"
    FANIN_TYPE = "DAG_executor_FanIn_Select"
    FANINNB_TYPE = "DAG_executor_FanInNB_Select"
    PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer_Select"

    STORE_SYNC_OBJECTS_IN_LAMBDAS = True
    USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS = True
    SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS = False
    USING_DAG_ORCHESTRATOR = True
    MAP_OBJECTS_TO_LAMBDA_FUNCTIONS = False
    USE_ANONYMOUS_LAMBDA_FUNCTIONS = True
    USE_SINGLE_LAMBDA_FUNCTION = False

#Test32: worker threads (A2) with selective-wait Sync-objects, 
#        Sync-objects stored remotely (on tcp_server)
#        create objects at the start
#        STORE_SYNC_OBJECTS_IN_LAMBDAS
#        Variation: a. change D_O to T, 
#                   b. change map to F, and anon to T:
#                   c. change D_O to F, map F, anon T:
# Note: tcp_server must be running: tcp_server -t 32
def test32():
    print("test32")
    global EXIT_PROGRAM_ON_EXCEPTION
    global DEALLOCATE_BFS_MAIN_MAP_ON_THE_FLY
    global RUN_ALL_TASKS_LOCALLY
    global BYPASS_CALL_TO_INVOKE_REAL_LAMBDA
    global STORE_FANINS_FANINNBS_LOCALLY
    global CREATE_ALL_FANINS_FANINNBS_ON_START
    global USING_WORKERS
    global USING_THREADS_NOT_PROCESSES
    global NUM_WORKERS
    global USE_MULTITHREADED_MULTIPROCESSING
    global NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING
    global FANIN_TYPE
    global FANINNB_TYPE
    global PROCESS_WORK_QUEUE_TYPE
    
    global STORE_SYNC_OBJECTS_IN_LAMBDAS
    global USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS
    global SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS
    global USING_DAG_ORCHESTRATOR
    global MAP_OBJECTS_TO_LAMBDA_FUNCTIONS
    global USE_ANONYMOUS_LAMBDA_FUNCTIONS
    global USE_SINGLE_LAMBDA_FUNCTION

    global PAGERANK_GRAPH_FILE_NAME
    global COMPUTE_PAGERANK
    global NUMBER_OF_PAGERANK_ITERATIONS_FOR_PARTITIONS_GROUPS_WITH_LOOPS 
    global NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG
    global CHECK_PAGERANK_OUTPUT
    global SAME_OUTPUT_FOR_ALL_FANOUT_FANIN
    global USE_INCREMENTAL_DAG_GENERATION
    global INCREMENTAL_DAG_DEPOSIT_INTERVAL
    global ENABLE_RUNTIME_TASK_CLUSTERING
    global WORK_QUEUE_SIZE_FOR_INCREMENTAL_DAG_GENERATION_WITH_WORKER_PROCESSES
    global TASKS_USE_RESULT_DICTIONARY_PARAMETER
    global USE_SHARED_PARTITIONS_GROUPS
    global USE_PAGERANK_GROUPS_PARTITIONS
    global USE_STRUCT_OF_ARRAYS_FOR_PAGERANK
    global USE_MUTLITHREADED_NONINCREMENTAL_BFS
    global INPUT_ALL_GROUPS_PARTITIONS_AT_START

    RUN_ALL_TASKS_LOCALLY = True
    BYPASS_CALL_TO_INVOKE_REAL_LAMBDA = (not RUN_ALL_TASKS_LOCALLY) and True 
    STORE_FANINS_FANINNBS_LOCALLY = False 
    CREATE_ALL_FANINS_FANINNBS_ON_START = True
    USING_WORKERS = True
    USING_THREADS_NOT_PROCESSES = True
    NUM_WORKERS = 2
    USE_MULTITHREADED_MULTIPROCESSING = False
    NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING = 1

    #FANIN_TYPE = "DAG_executor_FanIn"
    #FANINNB_TYPE = "DAG_executor_FanInNB"
    #PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer"
    FANIN_TYPE = "DAG_executor_FanIn_Select"
    FANINNB_TYPE = "DAG_executor_FanInNB_Select"
    PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer_Select"

    STORE_SYNC_OBJECTS_IN_LAMBDAS = True
    USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS = True
    SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS = False
    USING_DAG_ORCHESTRATOR = False
    MAP_OBJECTS_TO_LAMBDA_FUNCTIONS = False
    USE_ANONYMOUS_LAMBDA_FUNCTIONS = True
    USE_SINGLE_LAMBDA_FUNCTION = False

#Test33: worker threads (A2) with selective-wait Sync-objects, 
#        Sync-objects stored remotely (on tcp_server)
#        create objects at the start
#        STORE_SYNC_OBJECTS_IN_LAMBDAS
# Note: tcp_server must be running: tcp_server_lambda -t 33
# Note: output is in tcp_server_lambda window, not DAG_executor window
def test33():

	#       RUN_ALL_TASKS_LOCALLY = False, create objects on start = True:
    #       USING_WORKERS = False. The faninNB_Select calls DAG_executor.DAG_executor_lambda
    #       so we are not using real or simulated lambdas or even workers,
    #       instead, the synch objects on tcp_server trigger their tasks,
    #       which eans in this case the object is executing the fanin task.
    #       This can be extended so that the synch object will start a new
    #       real lambda to execute the task.
	#       Note: not running real lambdas yet, so need TTT, i.e., not using threads
	#       to simulate lambdas and not running real lambdas yet, so need to
	#       trigger lambdas, which means store objects in lambdas and they call
	#       DAG_excutor_Lambda to execute task (i.e., "trigger task to run in 
	#       the same lambda"). Eventually we'll have tests for use real 
	#       non-triggered lambdas to run tasks (invoked at fanouts/faninNBS)
	#       and objects stored in lambdas or on server.
	#       TTTTTFF: trigger and DAG_orchestrator, map objects true, anon is false, and create objects on start
    print("test33")
    global EXIT_PROGRAM_ON_EXCEPTION
    global DEALLOCATE_BFS_MAIN_MAP_ON_THE_FLY
    global RUN_ALL_TASKS_LOCALLY
    global BYPASS_CALL_TO_INVOKE_REAL_LAMBDA
    global STORE_FANINS_FANINNBS_LOCALLY
    global CREATE_ALL_FANINS_FANINNBS_ON_START
    global USING_WORKERS
    global USING_THREADS_NOT_PROCESSES
    global NUM_WORKERS
    global USE_MULTITHREADED_MULTIPROCESSING
    global NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING
    global FANIN_TYPE
    global FANINNB_TYPE
    global PROCESS_WORK_QUEUE_TYPE
    
    global STORE_SYNC_OBJECTS_IN_LAMBDAS
    global USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS
    global SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS
    global USING_DAG_ORCHESTRATOR
    global MAP_OBJECTS_TO_LAMBDA_FUNCTIONS
    global USE_ANONYMOUS_LAMBDA_FUNCTIONS
    global USE_SINGLE_LAMBDA_FUNCTION

    global PAGERANK_GRAPH_FILE_NAME
    global COMPUTE_PAGERANK
    global NUMBER_OF_PAGERANK_ITERATIONS_FOR_PARTITIONS_GROUPS_WITH_LOOPS 
    global NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG
    global CHECK_PAGERANK_OUTPUT
    global SAME_OUTPUT_FOR_ALL_FANOUT_FANIN
    global USE_INCREMENTAL_DAG_GENERATION
    global INCREMENTAL_DAG_DEPOSIT_INTERVAL
    global ENABLE_RUNTIME_TASK_CLUSTERING
    global WORK_QUEUE_SIZE_FOR_INCREMENTAL_DAG_GENERATION_WITH_WORKER_PROCESSES
    global TASKS_USE_RESULT_DICTIONARY_PARAMETER
    global USE_SHARED_PARTITIONS_GROUPS
    global USE_PAGERANK_GROUPS_PARTITIONS
    global USE_STRUCT_OF_ARRAYS_FOR_PAGERANK
    global USE_MUTLITHREADED_NONINCREMENTAL_BFS
    global INPUT_ALL_GROUPS_PARTITIONS_AT_START

    RUN_ALL_TASKS_LOCALLY = False 
    STORE_FANINS_FANINNBS_LOCALLY = False 
    CREATE_ALL_FANINS_FANINNBS_ON_START = True
    USING_WORKERS = False
    USING_THREADS_NOT_PROCESSES = True
    NUM_WORKERS = 2
    USE_MULTITHREADED_MULTIPROCESSING = False
    NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING = 1

    #FANIN_TYPE = "DAG_executor_FanIn"
    #FANINNB_TYPE = "DAG_executor_FanInNB"
    #PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer"
    FANIN_TYPE = "DAG_executor_FanIn_Select"
    FANINNB_TYPE = "DAG_executor_FanInNB_Select"
    PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer_Select"

    STORE_SYNC_OBJECTS_IN_LAMBDAS = True
    USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS = True
    SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS = True
    USING_DAG_ORCHESTRATOR = True
    MAP_OBJECTS_TO_LAMBDA_FUNCTIONS = True
    USE_ANONYMOUS_LAMBDA_FUNCTIONS = False
    USE_SINGLE_LAMBDA_FUNCTION = False

# Note: output is in tcp_server_lambda window, not DAG_executor window

#Test34: multithreaded worker processes (A2) with selective-wait Sync-objects, 
#        Sync-objects stored remotely (on tcp_server)
#        create objects at the start
#        STORE_SYNC_OBJECTS_IN_LAMBDAS
# Note: tcp_server must be running: tcp_server_lambda -t 34
# Note: output is in tcp_server_lambda window, not DAG_executor window
#
#Issue: This is just like test29() but using worker processes instead
# of worker threads. The issue is that worker processes use a work queue
# that is on the server, but when we STORE_SYNC_OBJECTS_IN_LAMBDAS we 
# do not want to store the work queue in a lambda - we need the work queue
# to be stored on tcp_server as a regular synch object, i.e., not in
# a lambda. 
# Also, in tcp_server_lambda, synchronize_async assumes synch objects
# are in lambdas but we don't call fanins/fanouts asynch so not really
# using this asynch but we do call work_queue.deposit asynch so:
# maybe let this be the same as tcp_server synchronous_async and driver
# creates a work_queue as ususal and this call to wrork_queue.deposit
# will work. But: withdraw is synchronous and we need it to access the 
# work_queue not in a lambda. 
# So: work_queue need to be treated differently from the fanin/fanout 
# objects when the latter are stored in lambdas.
# Note: driver creates fanins/fanouts and the work queue but tcp_server_lambda
# does not actually create the work_queue since it assumes we are using
# lambdas and lambdas do not use work_queue. So tcp_server_lambda needs
# to create the work_queue as a regular synch object (not stored in a lambda)
# and we need to be able to all an asynch for deposit and a synch for
# withdraw that access the work queue as a regular object. Perhaps
# have "work_queue" versions of the synchronize sync/async methods
# in the top level operations of tcp_server_lambda - this is a nuisance
# since the DAG_executor would have to choose the operation to call.
# An alternative is to let tcp_server figure out wha to do based on the 
# name of the operation, e.g., if "process_work_queue"  then so this 
# regular op for work queue else do op for fanin/fanout. (Could
# pass call to synchronize_synch_work_queue() method else do current code.)
def test34():
    print("test34")

    return

    global STORE_SYNC_OBJECTS_IN_LAMBDAS
    global USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS
    global SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS
    global USING_DAG_ORCHESTRATOR
    global MAP_OBJECTS_TO_LAMBDA_FUNCTIONS
    global USE_ANONYMOUS_LAMBDA_FUNCTIONS
    global USE_SINGLE_LAMBDA_FUNCTION
    global PAGERANK_GRAPH_FILE_NAME
    global COMPUTE_PAGERANK
    global CHECK_PAGERANK_OUTPUT
    global NUMBER_OF_PAGERANK_ITERATIONS_FOR_PARTITIONS_GROUPS_WITH_LOOPS 
    global NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG
    global SAME_OUTPUT_FOR_ALL_FANOUT_FANIN
    global USE_INCREMENTAL_DAG_GENERATION
    global INCREMENTAL_DAG_DEPOSIT_INTERVAL
    global WORK_QUEUE_SIZE_FOR_INCREMENTAL_DAG_GENERATION_WITH_WORKER_PROCESSES
    global TASKS_USE_RESULT_DICTIONARY_PARAMETER
    global USE_SHARED_PARTITIONS_GROUPS
    global USE_PAGERANK_GROUPS_PARTITIONS
    global USE_STRUCT_OF_ARRAYS_FOR_PAGERANK
    global USE_MUTLITHREADED_NONINCREMENTAL_BFS
    global RUN_ALL_TASKS_LOCALLY
    global BYPASS_CALL_TO_INVOKE_REAL_LAMBDA
    global STORE_FANINS_FANINNBS_LOCALLY
    global CREATE_ALL_FANINS_FANINNBS_ON_START
    global USING_WORKERS
    global USING_THREADS_NOT_PROCESSES
    global NUM_WORKERS
    global USE_MULTITHREADED_MULTIPROCESSING
    global NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING
    global FANIN_TYPE
    global FANINNB_TYPE
    global PROCESS_WORK_QUEUE_TYPE

    """
    RUN_ALL_TASKS_LOCALLY = False 
    STORE_FANINS_FANINNBS_LOCALLY = False 
    CREATE_ALL_FANINS_FANINNBS_ON_START = False
    USING_WORKERS = True
    USING_THREADS_NOT_PROCESSES = True
    NUM_WORKERS = 2
    USE_MULTITHREADED_MULTIPROCESSING = False
    NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING = 1
    """

    RUN_ALL_TASKS_LOCALLY = True 
    STORE_FANINS_FANINNBS_LOCALLY = False 
    CREATE_ALL_FANINS_FANINNBS_ON_START = True
    USING_WORKERS = True
    USING_THREADS_NOT_PROCESSES = False
    NUM_WORKERS = 2
    USE_MULTITHREADED_MULTIPROCESSING = False
    NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING = 1

    #FANIN_TYPE = "DAG_executor_FanIn"
    #FANINNB_TYPE = "DAG_executor_FanInNB"
    #PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer"
    FANIN_TYPE = "DAG_executor_FanIn_Select"
    FANINNB_TYPE = "DAG_executor_FanInNB_Select"
    PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer_Select"

    """
    STORE_SYNC_OBJECTS_IN_LAMBDAS = True
    USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS = True
    SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS = True
    USING_DAG_ORCHESTRATOR = True
    MAP_OBJECTS_TO_LAMBDA_FUNCTIONS = False
    USE_ANONYMOUS_LAMBDA_FUNCTIONS = True
    USE_SINGLE_LAMBDA_FUNCTION = False
    """

    STORE_SYNC_OBJECTS_IN_LAMBDAS = True
    USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS = True
    SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS = False
    USING_DAG_ORCHESTRATOR = False
    MAP_OBJECTS_TO_LAMBDA_FUNCTIONS = True
    USE_ANONYMOUS_LAMBDA_FUNCTIONS = False
    USE_SINGLE_LAMBDA_FUNCTION = False

# Note: output is in tcp_server_lambda window, not DAG_executor window

# Note: output is in tcp_server_lambda window, not DAG_executor window

#############################
###### Pagerank tests  ######
#################################################

"""
#PageRank tests
#Non-incremental
#running: python -m wukongdnc.dag.BFS using "fname = "graph_24N_3CC_fanin"  in BFS.input_graph()
"""

#Test35: worker threads (A2) with non-selective-wait Sync-objects, 
#        2 worker threads, Sync-objects stored locally
#        create objects at the start
#        use pagerank groups
def test35():
    print("test35")
    global EXIT_PROGRAM_ON_EXCEPTION
    global DEALLOCATE_BFS_MAIN_MAP_ON_THE_FLY
    global RUN_ALL_TASKS_LOCALLY
    global BYPASS_CALL_TO_INVOKE_REAL_LAMBDA
    global STORE_FANINS_FANINNBS_LOCALLY
    global CREATE_ALL_FANINS_FANINNBS_ON_START
    global USING_WORKERS
    global USING_THREADS_NOT_PROCESSES
    global NUM_WORKERS
    global USE_MULTITHREADED_MULTIPROCESSING
    global NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING
    global FANIN_TYPE
    global FANINNB_TYPE
    global PROCESS_WORK_QUEUE_TYPE
    
    global STORE_SYNC_OBJECTS_IN_LAMBDAS
    global USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS
    global SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS
    global USING_DAG_ORCHESTRATOR
    global MAP_OBJECTS_TO_LAMBDA_FUNCTIONS
    global USE_ANONYMOUS_LAMBDA_FUNCTIONS
    global USE_SINGLE_LAMBDA_FUNCTION

    global PAGERANK_GRAPH_FILE_NAME
    global COMPUTE_PAGERANK
    global NUMBER_OF_PAGERANK_ITERATIONS_FOR_PARTITIONS_GROUPS_WITH_LOOPS 
    global NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG
    global CHECK_PAGERANK_OUTPUT
    global SAME_OUTPUT_FOR_ALL_FANOUT_FANIN
    global USE_INCREMENTAL_DAG_GENERATION
    global INCREMENTAL_DAG_DEPOSIT_INTERVAL
    global ENABLE_RUNTIME_TASK_CLUSTERING
    global MIN_PARTITION_GROUP_SIZE_FOR_CLUSTERING
    global MAX_SIZE_OF_OUTPUT_TO_FANOUT_TASK
    global WORK_QUEUE_SIZE_FOR_INCREMENTAL_DAG_GENERATION_WITH_WORKER_PROCESSES
    global TASKS_USE_RESULT_DICTIONARY_PARAMETER
    global USE_SHARED_PARTITIONS_GROUPS
    global USE_PAGERANK_GROUPS_PARTITIONS
    global USE_STRUCT_OF_ARRAYS_FOR_PAGERANK
    global USE_MUTLITHREADED_NONINCREMENTAL_BFS
    global INPUT_ALL_GROUPS_PARTITIONS_AT_START

    RUN_ALL_TASKS_LOCALLY = True
    BYPASS_CALL_TO_INVOKE_REAL_LAMBDA = (not RUN_ALL_TASKS_LOCALLY) and True 
    STORE_FANINS_FANINNBS_LOCALLY = True 
    CREATE_ALL_FANINS_FANINNBS_ON_START = False
    USING_WORKERS = True
    USING_THREADS_NOT_PROCESSES = True
    NUM_WORKERS = 2
    USE_MULTITHREADED_MULTIPROCESSING = False
    NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING = 1

    FANIN_TYPE = "DAG_executor_FanIn"
    FANINNB_TYPE = "DAG_executor_FanInNB"
    PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer"
    #FANIN_TYPE = "DAG_executor_FanIn_Select"
    #FANINNB_TYPE = "DAG_executor_FanInNB_Select"
    #PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer_Select"

    STORE_SYNC_OBJECTS_IN_LAMBDAS = False
    USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS = False
    SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS = False
    USING_DAG_ORCHESTRATOR = False
    MAP_OBJECTS_TO_LAMBDA_FUNCTIONS = False
    USE_ANONYMOUS_LAMBDA_FUNCTIONS = False
    USE_SINGLE_LAMBDA_FUNCTION = False

    PAGERANK_GRAPH_FILE_NAME = "graph_24N_3CC_fanin"
    COMPUTE_PAGERANK = True
    CHECK_PAGERANK_OUTPUT = COMPUTE_PAGERANK and RUN_ALL_TASKS_LOCALLY and (USING_WORKERS or not USING_WORKERS) and USING_THREADS_NOT_PROCESSES and True
    NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG = "PR1_1"
    NUMBER_OF_PAGERANK_ITERATIONS_FOR_PARTITIONS_GROUPS_WITH_LOOPS = 10
    SAME_OUTPUT_FOR_ALL_FANOUT_FANIN = not COMPUTE_PAGERANK
    USE_INCREMENTAL_DAG_GENERATION = COMPUTE_PAGERANK and True
    INCREMENTAL_DAG_DEPOSIT_INTERVAL = 2
    ENABLE_RUNTIME_TASK_CLUSTERING = COMPUTE_PAGERANK and False
    MIN_PARTITION_GROUP_SIZE_FOR_CLUSTERING = 5
    MAX_SIZE_OF_OUTPUT_TO_FANOUT_TASK = 10000
    WORK_QUEUE_SIZE_FOR_INCREMENTAL_DAG_GENERATION_WITH_WORKER_PROCESSES =  2**10-1
    TASKS_USE_RESULT_DICTIONARY_PARAMETER = COMPUTE_PAGERANK and True
    USE_SHARED_PARTITIONS_GROUPS = COMPUTE_PAGERANK and False
    USE_PAGERANK_GROUPS_PARTITIONS = COMPUTE_PAGERANK and True
    USE_STRUCT_OF_ARRAYS_FOR_PAGERANK = COMPUTE_PAGERANK and False
    USE_MUTLITHREADED_NONINCREMENTAL_BFS = COMPUTE_PAGERANK and False
    INPUT_ALL_GROUPS_PARTITIONS_AT_START = COMPUTE_PAGERANK and False

#Test36: worker threads (A2) with non-selective-wait Sync-objects, 
#        2 worker threads, Sync-objects stored locally
#        create objects at the start
#        use pagerank partitions
def test36():
    print("test36")
    global EXIT_PROGRAM_ON_EXCEPTION
    global DEALLOCATE_BFS_MAIN_MAP_ON_THE_FLY
    global RUN_ALL_TASKS_LOCALLY
    global BYPASS_CALL_TO_INVOKE_REAL_LAMBDA
    global STORE_FANINS_FANINNBS_LOCALLY
    global CREATE_ALL_FANINS_FANINNBS_ON_START
    global USING_WORKERS
    global USING_THREADS_NOT_PROCESSES
    global NUM_WORKERS
    global USE_MULTITHREADED_MULTIPROCESSING
    global NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING
    global FANIN_TYPE
    global FANINNB_TYPE
    global PROCESS_WORK_QUEUE_TYPE
    
    global STORE_SYNC_OBJECTS_IN_LAMBDAS
    global USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS
    global SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS
    global USING_DAG_ORCHESTRATOR
    global MAP_OBJECTS_TO_LAMBDA_FUNCTIONS
    global USE_ANONYMOUS_LAMBDA_FUNCTIONS
    global USE_SINGLE_LAMBDA_FUNCTION

    global PAGERANK_GRAPH_FILE_NAME
    global COMPUTE_PAGERANK
    global NUMBER_OF_PAGERANK_ITERATIONS_FOR_PARTITIONS_GROUPS_WITH_LOOPS 
    global NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG
    global CHECK_PAGERANK_OUTPUT
    global SAME_OUTPUT_FOR_ALL_FANOUT_FANIN
    global USE_INCREMENTAL_DAG_GENERATION
    global INCREMENTAL_DAG_DEPOSIT_INTERVAL
    global ENABLE_RUNTIME_TASK_CLUSTERING
    global MIN_PARTITION_GROUP_SIZE_FOR_CLUSTERING
    global MAX_SIZE_OF_OUTPUT_TO_FANOUT_TASK
    global WORK_QUEUE_SIZE_FOR_INCREMENTAL_DAG_GENERATION_WITH_WORKER_PROCESSES
    global TASKS_USE_RESULT_DICTIONARY_PARAMETER
    global USE_SHARED_PARTITIONS_GROUPS
    global USE_PAGERANK_GROUPS_PARTITIONS
    global USE_STRUCT_OF_ARRAYS_FOR_PAGERANK
    global USE_MUTLITHREADED_NONINCREMENTAL_BFS
    global INPUT_ALL_GROUPS_PARTITIONS_AT_START

    RUN_ALL_TASKS_LOCALLY = True
    BYPASS_CALL_TO_INVOKE_REAL_LAMBDA = (not RUN_ALL_TASKS_LOCALLY) and True 
    STORE_FANINS_FANINNBS_LOCALLY = True 
    CREATE_ALL_FANINS_FANINNBS_ON_START = True
    USING_WORKERS = True
    USING_THREADS_NOT_PROCESSES = True
    NUM_WORKERS = 2
    USE_MULTITHREADED_MULTIPROCESSING = False
    NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING = 1

    FANIN_TYPE = "DAG_executor_FanIn"
    FANINNB_TYPE = "DAG_executor_FanInNB"
    PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer"
    #FANIN_TYPE = "DAG_executor_FanIn_Select"
    #FANINNB_TYPE = "DAG_executor_FanInNB_Select"
    #PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer_Select"

    STORE_SYNC_OBJECTS_IN_LAMBDAS = False
    USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS = False
    SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS = False
    USING_DAG_ORCHESTRATOR = False
    MAP_OBJECTS_TO_LAMBDA_FUNCTIONS = False
    USE_ANONYMOUS_LAMBDA_FUNCTIONS = False
    USE_SINGLE_LAMBDA_FUNCTION = False

    PAGERANK_GRAPH_FILE_NAME = "graph_24N_3CC_fanin"
    COMPUTE_PAGERANK = True
    CHECK_PAGERANK_OUTPUT = COMPUTE_PAGERANK and RUN_ALL_TASKS_LOCALLY and (USING_WORKERS or not USING_WORKERS) and USING_THREADS_NOT_PROCESSES and True
    NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG = "PR1_1"
    NUMBER_OF_PAGERANK_ITERATIONS_FOR_PARTITIONS_GROUPS_WITH_LOOPS = 10
    SAME_OUTPUT_FOR_ALL_FANOUT_FANIN = not COMPUTE_PAGERANK
    USE_INCREMENTAL_DAG_GENERATION = COMPUTE_PAGERANK and False
    INCREMENTAL_DAG_DEPOSIT_INTERVAL = 2
    ENABLE_RUNTIME_TASK_CLUSTERING = COMPUTE_PAGERANK and False
    MIN_PARTITION_GROUP_SIZE_FOR_CLUSTERING  = 5
    MAX_SIZE_OF_OUTPUT_TO_FANOUT_TASK = 10000
    WORK_QUEUE_SIZE_FOR_INCREMENTAL_DAG_GENERATION_WITH_WORKER_PROCESSES =  2**10-1
    TASKS_USE_RESULT_DICTIONARY_PARAMETER = COMPUTE_PAGERANK and True
    USE_SHARED_PARTITIONS_GROUPS = COMPUTE_PAGERANK and False
    USE_PAGERANK_GROUPS_PARTITIONS = COMPUTE_PAGERANK and False
    USE_STRUCT_OF_ARRAYS_FOR_PAGERANK = COMPUTE_PAGERANK and False
    USE_MUTLITHREADED_NONINCREMENTAL_BFS = COMPUTE_PAGERANK and False
    INPUT_ALL_GROUPS_PARTITIONS_AT_START = COMPUTE_PAGERANK and False

#Test37: worker processes (A2) with non-selective-wait Sync-objects, 
#        2 worker processes, Sync-objects stored remotely
#        create objects at the start
#        use pagerank groups
def test37():
    print("test37")
    global EXIT_PROGRAM_ON_EXCEPTION
    global DEALLOCATE_BFS_MAIN_MAP_ON_THE_FLY
    global RUN_ALL_TASKS_LOCALLY
    global BYPASS_CALL_TO_INVOKE_REAL_LAMBDA
    global STORE_FANINS_FANINNBS_LOCALLY
    global CREATE_ALL_FANINS_FANINNBS_ON_START
    global USING_WORKERS
    global USING_THREADS_NOT_PROCESSES
    global NUM_WORKERS
    global USE_MULTITHREADED_MULTIPROCESSING
    global NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING
    global FANIN_TYPE
    global FANINNB_TYPE
    global PROCESS_WORK_QUEUE_TYPE
    
    global STORE_SYNC_OBJECTS_IN_LAMBDAS
    global USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS
    global SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS
    global USING_DAG_ORCHESTRATOR
    global MAP_OBJECTS_TO_LAMBDA_FUNCTIONS
    global USE_ANONYMOUS_LAMBDA_FUNCTIONS
    global USE_SINGLE_LAMBDA_FUNCTION

    global PAGERANK_GRAPH_FILE_NAME
    global COMPUTE_PAGERANK
    global NUMBER_OF_PAGERANK_ITERATIONS_FOR_PARTITIONS_GROUPS_WITH_LOOPS 
    global NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG
    global CHECK_PAGERANK_OUTPUT
    global SAME_OUTPUT_FOR_ALL_FANOUT_FANIN
    global USE_INCREMENTAL_DAG_GENERATION
    global INCREMENTAL_DAG_DEPOSIT_INTERVAL
    global ENABLE_RUNTIME_TASK_CLUSTERING
    global MIN_PARTITION_GROUP_SIZE_FOR_CLUSTERING
    global MAX_SIZE_OF_OUTPUT_TO_FANOUT_TASK
    global WORK_QUEUE_SIZE_FOR_INCREMENTAL_DAG_GENERATION_WITH_WORKER_PROCESSES
    global TASKS_USE_RESULT_DICTIONARY_PARAMETER
    global USE_SHARED_PARTITIONS_GROUPS
    global USE_PAGERANK_GROUPS_PARTITIONS
    global USE_STRUCT_OF_ARRAYS_FOR_PAGERANK
    global USE_MUTLITHREADED_NONINCREMENTAL_BFS
    global INPUT_ALL_GROUPS_PARTITIONS_AT_START

    RUN_ALL_TASKS_LOCALLY = True
    BYPASS_CALL_TO_INVOKE_REAL_LAMBDA = (not RUN_ALL_TASKS_LOCALLY) and True 
    STORE_FANINS_FANINNBS_LOCALLY = False 
    CREATE_ALL_FANINS_FANINNBS_ON_START = True
    USING_WORKERS = True
    USING_THREADS_NOT_PROCESSES = False
    NUM_WORKERS = 2
    USE_MULTITHREADED_MULTIPROCESSING = False
    NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING = 1

    FANIN_TYPE = "DAG_executor_FanIn"
    FANINNB_TYPE = "DAG_executor_FanInNB"
    PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer"
    #FANIN_TYPE = "DAG_executor_FanIn_Select"
    #FANINNB_TYPE = "DAG_executor_FanInNB_Select"
    #PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer_Select"

    STORE_SYNC_OBJECTS_IN_LAMBDAS = False
    USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS = False
    SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS = False
    USING_DAG_ORCHESTRATOR = False
    MAP_OBJECTS_TO_LAMBDA_FUNCTIONS = False
    USE_ANONYMOUS_LAMBDA_FUNCTIONS = False
    USE_SINGLE_LAMBDA_FUNCTION = False

    PAGERANK_GRAPH_FILE_NAME = "graph_24N_3CC_fanin"
    COMPUTE_PAGERANK = True
    CHECK_PAGERANK_OUTPUT = COMPUTE_PAGERANK and RUN_ALL_TASKS_LOCALLY and (USING_WORKERS or not USING_WORKERS) and USING_THREADS_NOT_PROCESSES and True
    NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG = "PR1_1"
    NUMBER_OF_PAGERANK_ITERATIONS_FOR_PARTITIONS_GROUPS_WITH_LOOPS = 10
    SAME_OUTPUT_FOR_ALL_FANOUT_FANIN = not COMPUTE_PAGERANK
    USE_INCREMENTAL_DAG_GENERATION = COMPUTE_PAGERANK and False
    INCREMENTAL_DAG_DEPOSIT_INTERVAL = 2
    ENABLE_RUNTIME_TASK_CLUSTERING = COMPUTE_PAGERANK and False
    MIN_PARTITION_GROUP_SIZE_FOR_CLUSTERING  = 5
    MAX_SIZE_OF_OUTPUT_TO_FANOUT_TASK = 10000
    WORK_QUEUE_SIZE_FOR_INCREMENTAL_DAG_GENERATION_WITH_WORKER_PROCESSES =  2**10-1
    TASKS_USE_RESULT_DICTIONARY_PARAMETER = COMPUTE_PAGERANK and True
    USE_SHARED_PARTITIONS_GROUPS = COMPUTE_PAGERANK and False
    USE_PAGERANK_GROUPS_PARTITIONS = COMPUTE_PAGERANK and True
    USE_STRUCT_OF_ARRAYS_FOR_PAGERANK = COMPUTE_PAGERANK and False
    USE_MUTLITHREADED_NONINCREMENTAL_BFS = COMPUTE_PAGERANK and False
    INPUT_ALL_GROUPS_PARTITIONS_AT_START = COMPUTE_PAGERANK and False

#Test38: worker processes (A2) with non-selective-wait Sync-objects, 
#        2 worker processes, Sync-objects stored locally
#        create objects at the start
#        use pagerank partitions
def test38():
    print("test38")
    global EXIT_PROGRAM_ON_EXCEPTION
    global DEALLOCATE_BFS_MAIN_MAP_ON_THE_FLY
    global RUN_ALL_TASKS_LOCALLY
    global BYPASS_CALL_TO_INVOKE_REAL_LAMBDA
    global STORE_FANINS_FANINNBS_LOCALLY
    global CREATE_ALL_FANINS_FANINNBS_ON_START
    global USING_WORKERS
    global USING_THREADS_NOT_PROCESSES
    global NUM_WORKERS
    global USE_MULTITHREADED_MULTIPROCESSING
    global NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING
    global FANIN_TYPE
    global FANINNB_TYPE
    global PROCESS_WORK_QUEUE_TYPE
    
    global STORE_SYNC_OBJECTS_IN_LAMBDAS
    global USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS
    global SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS
    global USING_DAG_ORCHESTRATOR
    global MAP_OBJECTS_TO_LAMBDA_FUNCTIONS
    global USE_ANONYMOUS_LAMBDA_FUNCTIONS
    global USE_SINGLE_LAMBDA_FUNCTION

    global PAGERANK_GRAPH_FILE_NAME
    global COMPUTE_PAGERANK
    global NUMBER_OF_PAGERANK_ITERATIONS_FOR_PARTITIONS_GROUPS_WITH_LOOPS 
    global NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG
    global CHECK_PAGERANK_OUTPUT
    global SAME_OUTPUT_FOR_ALL_FANOUT_FANIN
    global USE_INCREMENTAL_DAG_GENERATION
    global INCREMENTAL_DAG_DEPOSIT_INTERVAL
    global ENABLE_RUNTIME_TASK_CLUSTERING
    global MIN_PARTITION_GROUP_SIZE_FOR_CLUSTERING
    global MAX_SIZE_OF_OUTPUT_TO_FANOUT_TASK
    global WORK_QUEUE_SIZE_FOR_INCREMENTAL_DAG_GENERATION_WITH_WORKER_PROCESSES
    global TASKS_USE_RESULT_DICTIONARY_PARAMETER
    global USE_SHARED_PARTITIONS_GROUPS
    global USE_PAGERANK_GROUPS_PARTITIONS
    global USE_STRUCT_OF_ARRAYS_FOR_PAGERANK
    global USE_MUTLITHREADED_NONINCREMENTAL_BFS
    global INPUT_ALL_GROUPS_PARTITIONS_AT_START

    RUN_ALL_TASKS_LOCALLY = True
    BYPASS_CALL_TO_INVOKE_REAL_LAMBDA = (not RUN_ALL_TASKS_LOCALLY) and True 
    STORE_FANINS_FANINNBS_LOCALLY = False 
    CREATE_ALL_FANINS_FANINNBS_ON_START = True
    USING_WORKERS = True
    USING_THREADS_NOT_PROCESSES = False
    NUM_WORKERS = 2
    USE_MULTITHREADED_MULTIPROCESSING = False
    NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING = 1

    FANIN_TYPE = "DAG_executor_FanIn"
    FANINNB_TYPE = "DAG_executor_FanInNB"
    PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer"
    #FANIN_TYPE = "DAG_executor_FanIn_Select"
    #FANINNB_TYPE = "DAG_executor_FanInNB_Select"
    #PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer_Select"

    STORE_SYNC_OBJECTS_IN_LAMBDAS = False
    USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS = False
    SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS = False
    USING_DAG_ORCHESTRATOR = False
    MAP_OBJECTS_TO_LAMBDA_FUNCTIONS = False
    USE_ANONYMOUS_LAMBDA_FUNCTIONS = False
    USE_SINGLE_LAMBDA_FUNCTION = False

    PAGERANK_GRAPH_FILE_NAME = "graph_24N_3CC_fanin"
    COMPUTE_PAGERANK = True
    CHECK_PAGERANK_OUTPUT = COMPUTE_PAGERANK and RUN_ALL_TASKS_LOCALLY and (USING_WORKERS or not USING_WORKERS) and USING_THREADS_NOT_PROCESSES and True
    NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG = "PR1_1"
    NUMBER_OF_PAGERANK_ITERATIONS_FOR_PARTITIONS_GROUPS_WITH_LOOPS = 10
    SAME_OUTPUT_FOR_ALL_FANOUT_FANIN = not COMPUTE_PAGERANK
    USE_INCREMENTAL_DAG_GENERATION = COMPUTE_PAGERANK and False
    INCREMENTAL_DAG_DEPOSIT_INTERVAL = 2
    ENABLE_RUNTIME_TASK_CLUSTERING = COMPUTE_PAGERANK and False
    MIN_PARTITION_GROUP_SIZE_FOR_CLUSTERING  = 5
    MAX_SIZE_OF_OUTPUT_TO_FANOUT_TASK = 10000
    WORK_QUEUE_SIZE_FOR_INCREMENTAL_DAG_GENERATION_WITH_WORKER_PROCESSES =  2**10-1
    TASKS_USE_RESULT_DICTIONARY_PARAMETER = COMPUTE_PAGERANK and True
    USE_SHARED_PARTITIONS_GROUPS = COMPUTE_PAGERANK and False
    USE_PAGERANK_GROUPS_PARTITIONS = COMPUTE_PAGERANK and False
    USE_STRUCT_OF_ARRAYS_FOR_PAGERANK = COMPUTE_PAGERANK and False
    USE_MUTLITHREADED_NONINCREMENTAL_BFS = COMPUTE_PAGERANK and False
    INPUT_ALL_GROUPS_PARTITIONS_AT_START = COMPUTE_PAGERANK and False

#Test39: real lambda code with the call to invoke real AWS lambdas 
#        bypassed, with non-selective-wait Sync-objects, 
#        Sync-objects stored remotely (required for real lambdas)
#        create objects at the start
#        use pagerank partitions
def test39():
    print("test39")
    global EXIT_PROGRAM_ON_EXCEPTION
    global DEALLOCATE_BFS_MAIN_MAP_ON_THE_FLY
    global RUN_ALL_TASKS_LOCALLY
    global BYPASS_CALL_TO_INVOKE_REAL_LAMBDA
    global STORE_FANINS_FANINNBS_LOCALLY
    global CREATE_ALL_FANINS_FANINNBS_ON_START
    global USING_WORKERS
    global USING_THREADS_NOT_PROCESSES
    global NUM_WORKERS
    global USE_MULTITHREADED_MULTIPROCESSING
    global NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING
    global FANIN_TYPE
    global FANINNB_TYPE
    global PROCESS_WORK_QUEUE_TYPE
    
    global STORE_SYNC_OBJECTS_IN_LAMBDAS
    global USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS
    global SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS
    global USING_DAG_ORCHESTRATOR
    global MAP_OBJECTS_TO_LAMBDA_FUNCTIONS
    global USE_ANONYMOUS_LAMBDA_FUNCTIONS
    global USE_SINGLE_LAMBDA_FUNCTION

    global PAGERANK_GRAPH_FILE_NAME
    global COMPUTE_PAGERANK
    global NUMBER_OF_PAGERANK_ITERATIONS_FOR_PARTITIONS_GROUPS_WITH_LOOPS 
    global NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG
    global CHECK_PAGERANK_OUTPUT
    global SAME_OUTPUT_FOR_ALL_FANOUT_FANIN
    global USE_INCREMENTAL_DAG_GENERATION
    global INCREMENTAL_DAG_DEPOSIT_INTERVAL
    global ENABLE_RUNTIME_TASK_CLUSTERING
    global MIN_PARTITION_GROUP_SIZE_FOR_CLUSTERING
    global MAX_SIZE_OF_OUTPUT_TO_FANOUT_TASK
    global WORK_QUEUE_SIZE_FOR_INCREMENTAL_DAG_GENERATION_WITH_WORKER_PROCESSES
    global TASKS_USE_RESULT_DICTIONARY_PARAMETER
    global USE_SHARED_PARTITIONS_GROUPS
    global USE_PAGERANK_GROUPS_PARTITIONS
    global USE_STRUCT_OF_ARRAYS_FOR_PAGERANK
    global USE_MUTLITHREADED_NONINCREMENTAL_BFS
    global INPUT_ALL_GROUPS_PARTITIONS_AT_START

    RUN_ALL_TASKS_LOCALLY = False 
    BYPASS_CALL_TO_INVOKE_REAL_LAMBDA = (not RUN_ALL_TASKS_LOCALLY) and True
    STORE_FANINS_FANINNBS_LOCALLY = False 
    CREATE_ALL_FANINS_FANINNBS_ON_START = True
    USING_WORKERS = False
    USING_THREADS_NOT_PROCESSES = True
    NUM_WORKERS = 2
    USE_MULTITHREADED_MULTIPROCESSING = False
    NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING = 1

    FANIN_TYPE = "DAG_executor_FanIn"
    FANINNB_TYPE = "DAG_executor_FanInNB"
    PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer"
    #FANIN_TYPE = "DAG_executor_FanIn_Select"
    #FANINNB_TYPE = "DAG_executor_FanInNB_Select"
    #PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer_Select"

    STORE_SYNC_OBJECTS_IN_LAMBDAS = False
    USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS = False
    SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS = False
    USING_DAG_ORCHESTRATOR = False
    MAP_OBJECTS_TO_LAMBDA_FUNCTIONS = False
    USE_ANONYMOUS_LAMBDA_FUNCTIONS = False
    USE_SINGLE_LAMBDA_FUNCTION = False

    PAGERANK_GRAPH_FILE_NAME = "graph_24N_3CC_fanin"
    COMPUTE_PAGERANK = True
    CHECK_PAGERANK_OUTPUT = COMPUTE_PAGERANK and RUN_ALL_TASKS_LOCALLY and (USING_WORKERS or not USING_WORKERS) and USING_THREADS_NOT_PROCESSES and True
    NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG = "PR1_1"
    NUMBER_OF_PAGERANK_ITERATIONS_FOR_PARTITIONS_GROUPS_WITH_LOOPS = 10
    SAME_OUTPUT_FOR_ALL_FANOUT_FANIN = not COMPUTE_PAGERANK
    USE_INCREMENTAL_DAG_GENERATION = COMPUTE_PAGERANK and False
    INCREMENTAL_DAG_DEPOSIT_INTERVAL = 2
    ENABLE_RUNTIME_TASK_CLUSTERING = COMPUTE_PAGERANK and False
    MIN_PARTITION_GROUP_SIZE_FOR_CLUSTERING  = 5
    MAX_SIZE_OF_OUTPUT_TO_FANOUT_TASK = 10000
    WORK_QUEUE_SIZE_FOR_INCREMENTAL_DAG_GENERATION_WITH_WORKER_PROCESSES =  2**10-1
    TASKS_USE_RESULT_DICTIONARY_PARAMETER = COMPUTE_PAGERANK and True
    USE_SHARED_PARTITIONS_GROUPS = COMPUTE_PAGERANK and False
    USE_PAGERANK_GROUPS_PARTITIONS = COMPUTE_PAGERANK and True
    USE_STRUCT_OF_ARRAYS_FOR_PAGERANK = COMPUTE_PAGERANK and False
    USE_MUTLITHREADED_NONINCREMENTAL_BFS = COMPUTE_PAGERANK and False
    INPUT_ALL_GROUPS_PARTITIONS_AT_START = COMPUTE_PAGERANK and False

#test workers share memory. worker threads share a global array. 
#worker processes share Shared Memory

#Test40: worker threads (A2) with non-selective-wait Sync-objects, 
#        2 worker threads, Sync-objects stored locally
#        create objects at the start
#        use pagerank groups
#        use shared
# Note: tcp_server must be running: tcp_server -t 40
def test40():
    print("test40")
    global EXIT_PROGRAM_ON_EXCEPTION
    global DEALLOCATE_BFS_MAIN_MAP_ON_THE_FLY
    global RUN_ALL_TASKS_LOCALLY
    global BYPASS_CALL_TO_INVOKE_REAL_LAMBDA
    global STORE_FANINS_FANINNBS_LOCALLY
    global CREATE_ALL_FANINS_FANINNBS_ON_START
    global USING_WORKERS
    global USING_THREADS_NOT_PROCESSES
    global NUM_WORKERS
    global USE_MULTITHREADED_MULTIPROCESSING
    global NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING
    global FANIN_TYPE
    global FANINNB_TYPE
    global PROCESS_WORK_QUEUE_TYPE
    
    global STORE_SYNC_OBJECTS_IN_LAMBDAS
    global USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS
    global SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS
    global USING_DAG_ORCHESTRATOR
    global MAP_OBJECTS_TO_LAMBDA_FUNCTIONS
    global USE_ANONYMOUS_LAMBDA_FUNCTIONS
    global USE_SINGLE_LAMBDA_FUNCTION

    global PAGERANK_GRAPH_FILE_NAME
    global COMPUTE_PAGERANK
    global NUMBER_OF_PAGERANK_ITERATIONS_FOR_PARTITIONS_GROUPS_WITH_LOOPS 
    global NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG
    global CHECK_PAGERANK_OUTPUT
    global SAME_OUTPUT_FOR_ALL_FANOUT_FANIN
    global USE_INCREMENTAL_DAG_GENERATION
    global INCREMENTAL_DAG_DEPOSIT_INTERVAL
    global ENABLE_RUNTIME_TASK_CLUSTERING
    global MIN_PARTITION_GROUP_SIZE_FOR_CLUSTERING
    global MAX_SIZE_OF_OUTPUT_TO_FANOUT_TASK
    global WORK_QUEUE_SIZE_FOR_INCREMENTAL_DAG_GENERATION_WITH_WORKER_PROCESSES
    global TASKS_USE_RESULT_DICTIONARY_PARAMETER
    global USE_SHARED_PARTITIONS_GROUPS
    global USE_PAGERANK_GROUPS_PARTITIONS
    global USE_STRUCT_OF_ARRAYS_FOR_PAGERANK
    global USE_MUTLITHREADED_NONINCREMENTAL_BFS
    global INPUT_ALL_GROUPS_PARTITIONS_AT_START

    RUN_ALL_TASKS_LOCALLY = True
    BYPASS_CALL_TO_INVOKE_REAL_LAMBDA = (not RUN_ALL_TASKS_LOCALLY) and True 
    STORE_FANINS_FANINNBS_LOCALLY = True 
    CREATE_ALL_FANINS_FANINNBS_ON_START = True
    USING_WORKERS = True
    USING_THREADS_NOT_PROCESSES = True
    NUM_WORKERS = 2
    USE_MULTITHREADED_MULTIPROCESSING = False
    NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING = 1

    FANIN_TYPE = "DAG_executor_FanIn"
    FANINNB_TYPE = "DAG_executor_FanInNB"
    PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer"
    #FANIN_TYPE = "DAG_executor_FanIn_Select"
    #FANINNB_TYPE = "DAG_executor_FanInNB_Select"
    #PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer_Select"

    STORE_SYNC_OBJECTS_IN_LAMBDAS = False
    USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS = False
    SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS = False
    USING_DAG_ORCHESTRATOR = False
    MAP_OBJECTS_TO_LAMBDA_FUNCTIONS = False
    USE_ANONYMOUS_LAMBDA_FUNCTIONS = False
    USE_SINGLE_LAMBDA_FUNCTION = False

    PAGERANK_GRAPH_FILE_NAME = "graph_24N_3CC_fanin"
    COMPUTE_PAGERANK = True
    CHECK_PAGERANK_OUTPUT = COMPUTE_PAGERANK and RUN_ALL_TASKS_LOCALLY and (USING_WORKERS or not USING_WORKERS) and USING_THREADS_NOT_PROCESSES and True
    NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG = "PR1_1"
    NUMBER_OF_PAGERANK_ITERATIONS_FOR_PARTITIONS_GROUPS_WITH_LOOPS = 10
    SAME_OUTPUT_FOR_ALL_FANOUT_FANIN = not COMPUTE_PAGERANK
    USE_INCREMENTAL_DAG_GENERATION = COMPUTE_PAGERANK and False
    INCREMENTAL_DAG_DEPOSIT_INTERVAL = 2
    ENABLE_RUNTIME_TASK_CLUSTERING = COMPUTE_PAGERANK and False
    MIN_PARTITION_GROUP_SIZE_FOR_CLUSTERING  = 5
    MAX_SIZE_OF_OUTPUT_TO_FANOUT_TASK = 10000
    WORK_QUEUE_SIZE_FOR_INCREMENTAL_DAG_GENERATION_WITH_WORKER_PROCESSES =  2**10-1
    TASKS_USE_RESULT_DICTIONARY_PARAMETER = COMPUTE_PAGERANK and True
    USE_SHARED_PARTITIONS_GROUPS = COMPUTE_PAGERANK and True
    USE_PAGERANK_GROUPS_PARTITIONS = COMPUTE_PAGERANK and True
    USE_STRUCT_OF_ARRAYS_FOR_PAGERANK = COMPUTE_PAGERANK and False
    USE_MUTLITHREADED_NONINCREMENTAL_BFS = COMPUTE_PAGERANK and False
    INPUT_ALL_GROUPS_PARTITIONS_AT_START = COMPUTE_PAGERANK and False

#Test41: worker threads (A2) with non-selective-wait Sync-objects, 
#        2 worker threads, Sync-objects stored locally
#        create objects at the start
#        use pagerank partitions
#        use shared
# Note: tcp_server must be running: tcp_server -t 41
def test41():
    print("test41")
    global EXIT_PROGRAM_ON_EXCEPTION
    global DEALLOCATE_BFS_MAIN_MAP_ON_THE_FLY
    global RUN_ALL_TASKS_LOCALLY
    global BYPASS_CALL_TO_INVOKE_REAL_LAMBDA
    global STORE_FANINS_FANINNBS_LOCALLY
    global CREATE_ALL_FANINS_FANINNBS_ON_START
    global USING_WORKERS
    global USING_THREADS_NOT_PROCESSES
    global NUM_WORKERS
    global USE_MULTITHREADED_MULTIPROCESSING
    global NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING
    global FANIN_TYPE
    global FANINNB_TYPE
    global PROCESS_WORK_QUEUE_TYPE
    
    global STORE_SYNC_OBJECTS_IN_LAMBDAS
    global USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS
    global SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS
    global USING_DAG_ORCHESTRATOR
    global MAP_OBJECTS_TO_LAMBDA_FUNCTIONS
    global USE_ANONYMOUS_LAMBDA_FUNCTIONS
    global USE_SINGLE_LAMBDA_FUNCTION

    global PAGERANK_GRAPH_FILE_NAME
    global COMPUTE_PAGERANK
    global NUMBER_OF_PAGERANK_ITERATIONS_FOR_PARTITIONS_GROUPS_WITH_LOOPS 
    global NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG
    global CHECK_PAGERANK_OUTPUT
    global SAME_OUTPUT_FOR_ALL_FANOUT_FANIN
    global USE_INCREMENTAL_DAG_GENERATION
    global INCREMENTAL_DAG_DEPOSIT_INTERVAL
    global ENABLE_RUNTIME_TASK_CLUSTERING
    global MIN_PARTITION_GROUP_SIZE_FOR_CLUSTERING
    global MAX_SIZE_OF_OUTPUT_TO_FANOUT_TASK
    global WORK_QUEUE_SIZE_FOR_INCREMENTAL_DAG_GENERATION_WITH_WORKER_PROCESSES
    global TASKS_USE_RESULT_DICTIONARY_PARAMETER
    global USE_SHARED_PARTITIONS_GROUPS
    global USE_PAGERANK_GROUPS_PARTITIONS
    global USE_STRUCT_OF_ARRAYS_FOR_PAGERANK
    global USE_MUTLITHREADED_NONINCREMENTAL_BFS
    global INPUT_ALL_GROUPS_PARTITIONS_AT_START

    RUN_ALL_TASKS_LOCALLY = True
    BYPASS_CALL_TO_INVOKE_REAL_LAMBDA = (not RUN_ALL_TASKS_LOCALLY) and True 
    STORE_FANINS_FANINNBS_LOCALLY = True 
    CREATE_ALL_FANINS_FANINNBS_ON_START = True
    USING_WORKERS = True
    USING_THREADS_NOT_PROCESSES = True
    NUM_WORKERS = 2
    USE_MULTITHREADED_MULTIPROCESSING = False
    NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING = 1

    FANIN_TYPE = "DAG_executor_FanIn"
    FANINNB_TYPE = "DAG_executor_FanInNB"
    PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer"
    #FANIN_TYPE = "DAG_executor_FanIn_Select"
    #FANINNB_TYPE = "DAG_executor_FanInNB_Select"
    #PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer_Select"

    STORE_SYNC_OBJECTS_IN_LAMBDAS = False
    USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS = False
    SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS = False
    USING_DAG_ORCHESTRATOR = False
    MAP_OBJECTS_TO_LAMBDA_FUNCTIONS = False
    USE_ANONYMOUS_LAMBDA_FUNCTIONS = False
    USE_SINGLE_LAMBDA_FUNCTION = False

    PAGERANK_GRAPH_FILE_NAME = "graph_24N_3CC_fanin"
    COMPUTE_PAGERANK = True
    CHECK_PAGERANK_OUTPUT = COMPUTE_PAGERANK and RUN_ALL_TASKS_LOCALLY and (USING_WORKERS or not USING_WORKERS) and USING_THREADS_NOT_PROCESSES and True
    NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG = "PR1_1"
    NUMBER_OF_PAGERANK_ITERATIONS_FOR_PARTITIONS_GROUPS_WITH_LOOPS = 10
    SAME_OUTPUT_FOR_ALL_FANOUT_FANIN = not COMPUTE_PAGERANK
    USE_INCREMENTAL_DAG_GENERATION = COMPUTE_PAGERANK and False
    INCREMENTAL_DAG_DEPOSIT_INTERVAL = 2
    ENABLE_RUNTIME_TASK_CLUSTERING = COMPUTE_PAGERANK and False
    MIN_PARTITION_GROUP_SIZE_FOR_CLUSTERING  = 5
    MAX_SIZE_OF_OUTPUT_TO_FANOUT_TASK = 10000
    WORK_QUEUE_SIZE_FOR_INCREMENTAL_DAG_GENERATION_WITH_WORKER_PROCESSES =  2**10-1
    TASKS_USE_RESULT_DICTIONARY_PARAMETER = COMPUTE_PAGERANK and True
    USE_SHARED_PARTITIONS_GROUPS = COMPUTE_PAGERANK and True
    USE_PAGERANK_GROUPS_PARTITIONS = COMPUTE_PAGERANK and False
    USE_STRUCT_OF_ARRAYS_FOR_PAGERANK = COMPUTE_PAGERANK and False
    USE_MUTLITHREADED_NONINCREMENTAL_BFS = COMPUTE_PAGERANK and False
    INPUT_ALL_GROUPS_PARTITIONS_AT_START = COMPUTE_PAGERANK and False

#Test 42: worker threads (A2) with non-selective-wait Sync-objects, 
#        2 worker threads, Sync-objects stored remotely
#        create objects at the start
#        use pagerank groups
#        use shared
#        use array of structs
# Note: tcp_server must be running: tcp_server -t 42
def test42():
    print("test42")
    global EXIT_PROGRAM_ON_EXCEPTION
    global DEALLOCATE_BFS_MAIN_MAP_ON_THE_FLY
    global RUN_ALL_TASKS_LOCALLY
    global BYPASS_CALL_TO_INVOKE_REAL_LAMBDA
    global STORE_FANINS_FANINNBS_LOCALLY
    global CREATE_ALL_FANINS_FANINNBS_ON_START
    global USING_WORKERS
    global USING_THREADS_NOT_PROCESSES
    global NUM_WORKERS
    global USE_MULTITHREADED_MULTIPROCESSING
    global NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING
    global FANIN_TYPE
    global FANINNB_TYPE
    global PROCESS_WORK_QUEUE_TYPE
    
    global STORE_SYNC_OBJECTS_IN_LAMBDAS
    global USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS
    global SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS
    global USING_DAG_ORCHESTRATOR
    global MAP_OBJECTS_TO_LAMBDA_FUNCTIONS
    global USE_ANONYMOUS_LAMBDA_FUNCTIONS
    global USE_SINGLE_LAMBDA_FUNCTION

    global PAGERANK_GRAPH_FILE_NAME
    global COMPUTE_PAGERANK
    global NUMBER_OF_PAGERANK_ITERATIONS_FOR_PARTITIONS_GROUPS_WITH_LOOPS 
    global NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG
    global CHECK_PAGERANK_OUTPUT
    global SAME_OUTPUT_FOR_ALL_FANOUT_FANIN
    global USE_INCREMENTAL_DAG_GENERATION
    global INCREMENTAL_DAG_DEPOSIT_INTERVAL
    global ENABLE_RUNTIME_TASK_CLUSTERING
    global MIN_PARTITION_GROUP_SIZE_FOR_CLUSTERING
    global MAX_SIZE_OF_OUTPUT_TO_FANOUT_TASK
    global WORK_QUEUE_SIZE_FOR_INCREMENTAL_DAG_GENERATION_WITH_WORKER_PROCESSES
    global TASKS_USE_RESULT_DICTIONARY_PARAMETER
    global USE_SHARED_PARTITIONS_GROUPS
    global USE_PAGERANK_GROUPS_PARTITIONS
    global USE_STRUCT_OF_ARRAYS_FOR_PAGERANK
    global USE_MUTLITHREADED_NONINCREMENTAL_BFS
    global INPUT_ALL_GROUPS_PARTITIONS_AT_START

    RUN_ALL_TASKS_LOCALLY = True
    BYPASS_CALL_TO_INVOKE_REAL_LAMBDA = (not RUN_ALL_TASKS_LOCALLY) and True 
    STORE_FANINS_FANINNBS_LOCALLY = True 
    CREATE_ALL_FANINS_FANINNBS_ON_START = True
    USING_WORKERS = True
    USING_THREADS_NOT_PROCESSES = True
    NUM_WORKERS = 2
    USE_MULTITHREADED_MULTIPROCESSING = False
    NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING = 1

    FANIN_TYPE = "DAG_executor_FanIn"
    FANINNB_TYPE = "DAG_executor_FanInNB"
    PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer"
    #FANIN_TYPE = "DAG_executor_FanIn_Select"
    #FANINNB_TYPE = "DAG_executor_FanInNB_Select"
    #PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer_Select"

    STORE_SYNC_OBJECTS_IN_LAMBDAS = False
    USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS = False
    SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS = False
    USING_DAG_ORCHESTRATOR = False
    MAP_OBJECTS_TO_LAMBDA_FUNCTIONS = False
    USE_ANONYMOUS_LAMBDA_FUNCTIONS = False
    USE_SINGLE_LAMBDA_FUNCTION = False

    PAGERANK_GRAPH_FILE_NAME = "graph_24N_3CC_fanin"
    COMPUTE_PAGERANK = True
    CHECK_PAGERANK_OUTPUT = COMPUTE_PAGERANK and RUN_ALL_TASKS_LOCALLY and (USING_WORKERS or not USING_WORKERS) and USING_THREADS_NOT_PROCESSES and True
    NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG = "PR1_1"
    NUMBER_OF_PAGERANK_ITERATIONS_FOR_PARTITIONS_GROUPS_WITH_LOOPS = 10
    SAME_OUTPUT_FOR_ALL_FANOUT_FANIN = not COMPUTE_PAGERANK
    USE_INCREMENTAL_DAG_GENERATION = COMPUTE_PAGERANK and False
    INCREMENTAL_DAG_DEPOSIT_INTERVAL = 2
    ENABLE_RUNTIME_TASK_CLUSTERING = COMPUTE_PAGERANK and False
    MIN_PARTITION_GROUP_SIZE_FOR_CLUSTERING  = 5
    MAX_SIZE_OF_OUTPUT_TO_FANOUT_TASK = 10000
    WORK_QUEUE_SIZE_FOR_INCREMENTAL_DAG_GENERATION_WITH_WORKER_PROCESSES =  2**10-1
    TASKS_USE_RESULT_DICTIONARY_PARAMETER = COMPUTE_PAGERANK and True
    USE_SHARED_PARTITIONS_GROUPS = COMPUTE_PAGERANK and True
    USE_PAGERANK_GROUPS_PARTITIONS = COMPUTE_PAGERANK and True
    USE_STRUCT_OF_ARRAYS_FOR_PAGERANK = COMPUTE_PAGERANK and True
    USE_MUTLITHREADED_NONINCREMENTAL_BFS = COMPUTE_PAGERANK and False
    INPUT_ALL_GROUPS_PARTITIONS_AT_START = COMPUTE_PAGERANK and False

#Test43: worker threads (A2) with non-selective-wait Sync-objects, 
#        2 worker threads, Sync-objects stored remotely
#        create objects at the start
#        use pagerank partitions
#        use shared
#        use array of structs
# Note: tcp_server must be running: tcp_server -t 43
def test43():
    print("test43")
    global EXIT_PROGRAM_ON_EXCEPTION
    global DEALLOCATE_BFS_MAIN_MAP_ON_THE_FLY
    global RUN_ALL_TASKS_LOCALLY
    global BYPASS_CALL_TO_INVOKE_REAL_LAMBDA
    global STORE_FANINS_FANINNBS_LOCALLY
    global CREATE_ALL_FANINS_FANINNBS_ON_START
    global USING_WORKERS
    global USING_THREADS_NOT_PROCESSES
    global NUM_WORKERS
    global USE_MULTITHREADED_MULTIPROCESSING
    global NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING
    global FANIN_TYPE
    global FANINNB_TYPE
    global PROCESS_WORK_QUEUE_TYPE
    
    global STORE_SYNC_OBJECTS_IN_LAMBDAS
    global USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS
    global SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS
    global USING_DAG_ORCHESTRATOR
    global MAP_OBJECTS_TO_LAMBDA_FUNCTIONS
    global USE_ANONYMOUS_LAMBDA_FUNCTIONS
    global USE_SINGLE_LAMBDA_FUNCTION

    global PAGERANK_GRAPH_FILE_NAME
    global COMPUTE_PAGERANK
    global NUMBER_OF_PAGERANK_ITERATIONS_FOR_PARTITIONS_GROUPS_WITH_LOOPS 
    global NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG
    global CHECK_PAGERANK_OUTPUT
    global SAME_OUTPUT_FOR_ALL_FANOUT_FANIN
    global USE_INCREMENTAL_DAG_GENERATION
    global INCREMENTAL_DAG_DEPOSIT_INTERVAL
    global ENABLE_RUNTIME_TASK_CLUSTERING
    global MIN_PARTITION_GROUP_SIZE_FOR_CLUSTERING
    global MAX_SIZE_OF_OUTPUT_TO_FANOUT_TASK
    global WORK_QUEUE_SIZE_FOR_INCREMENTAL_DAG_GENERATION_WITH_WORKER_PROCESSES
    global TASKS_USE_RESULT_DICTIONARY_PARAMETER
    global USE_SHARED_PARTITIONS_GROUPS
    global USE_PAGERANK_GROUPS_PARTITIONS
    global USE_STRUCT_OF_ARRAYS_FOR_PAGERANK
    global USE_MUTLITHREADED_NONINCREMENTAL_BFS
    global INPUT_ALL_GROUPS_PARTITIONS_AT_START

    RUN_ALL_TASKS_LOCALLY = True
    BYPASS_CALL_TO_INVOKE_REAL_LAMBDA = (not RUN_ALL_TASKS_LOCALLY) and True 
    STORE_FANINS_FANINNBS_LOCALLY = True 
    CREATE_ALL_FANINS_FANINNBS_ON_START = True
    USING_WORKERS = True
    USING_THREADS_NOT_PROCESSES = True
    NUM_WORKERS = 2
    USE_MULTITHREADED_MULTIPROCESSING = False
    NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING = 1

    FANIN_TYPE = "DAG_executor_FanIn"
    FANINNB_TYPE = "DAG_executor_FanInNB"
    PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer"
    #FANIN_TYPE = "DAG_executor_FanIn_Select"
    #FANINNB_TYPE = "DAG_executor_FanInNB_Select"
    #PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer_Select"

    STORE_SYNC_OBJECTS_IN_LAMBDAS = False
    USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS = False
    SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS = False
    USING_DAG_ORCHESTRATOR = False
    MAP_OBJECTS_TO_LAMBDA_FUNCTIONS = False
    USE_ANONYMOUS_LAMBDA_FUNCTIONS = False
    USE_SINGLE_LAMBDA_FUNCTION = False

    PAGERANK_GRAPH_FILE_NAME = "graph_24N_3CC_fanin"
    COMPUTE_PAGERANK = True
    CHECK_PAGERANK_OUTPUT = COMPUTE_PAGERANK and RUN_ALL_TASKS_LOCALLY and (USING_WORKERS or not USING_WORKERS) and USING_THREADS_NOT_PROCESSES and True
    NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG = "PR1_1"
    NUMBER_OF_PAGERANK_ITERATIONS_FOR_PARTITIONS_GROUPS_WITH_LOOPS = 10
    SAME_OUTPUT_FOR_ALL_FANOUT_FANIN = not COMPUTE_PAGERANK
    USE_INCREMENTAL_DAG_GENERATION = COMPUTE_PAGERANK and False
    INCREMENTAL_DAG_DEPOSIT_INTERVAL = 2
    ENABLE_RUNTIME_TASK_CLUSTERING = COMPUTE_PAGERANK and False
    MIN_PARTITION_GROUP_SIZE_FOR_CLUSTERING  = 5
    MAX_SIZE_OF_OUTPUT_TO_FANOUT_TASK = 10000
    WORK_QUEUE_SIZE_FOR_INCREMENTAL_DAG_GENERATION_WITH_WORKER_PROCESSES =  2**10-1
    TASKS_USE_RESULT_DICTIONARY_PARAMETER = COMPUTE_PAGERANK and True
    USE_SHARED_PARTITIONS_GROUPS = COMPUTE_PAGERANK and True
    USE_PAGERANK_GROUPS_PARTITIONS = COMPUTE_PAGERANK and False
    USE_STRUCT_OF_ARRAYS_FOR_PAGERANK = COMPUTE_PAGERANK and True
    USE_MUTLITHREADED_NONINCREMENTAL_BFS = COMPUTE_PAGERANK and False
    INPUT_ALL_GROUPS_PARTITIONS_AT_START = COMPUTE_PAGERANK and False

#Test44: worker processes (A2) with non-selective-wait Sync-objects, 
#        2 worker processes, Sync-objects stored remotely
#        use pagerank groups
#        use shared
# Note: tcp_server must be running: tcp_server -t 44
def test44():
    print("test44")
    global EXIT_PROGRAM_ON_EXCEPTION
    global DEALLOCATE_BFS_MAIN_MAP_ON_THE_FLY
    global RUN_ALL_TASKS_LOCALLY
    global BYPASS_CALL_TO_INVOKE_REAL_LAMBDA
    global STORE_FANINS_FANINNBS_LOCALLY
    global CREATE_ALL_FANINS_FANINNBS_ON_START
    global USING_WORKERS
    global USING_THREADS_NOT_PROCESSES
    global NUM_WORKERS
    global USE_MULTITHREADED_MULTIPROCESSING
    global NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING
    global FANIN_TYPE
    global FANINNB_TYPE
    global PROCESS_WORK_QUEUE_TYPE
    
    global STORE_SYNC_OBJECTS_IN_LAMBDAS
    global USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS
    global SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS
    global USING_DAG_ORCHESTRATOR
    global MAP_OBJECTS_TO_LAMBDA_FUNCTIONS
    global USE_ANONYMOUS_LAMBDA_FUNCTIONS
    global USE_SINGLE_LAMBDA_FUNCTION

    global PAGERANK_GRAPH_FILE_NAME
    global COMPUTE_PAGERANK
    global NUMBER_OF_PAGERANK_ITERATIONS_FOR_PARTITIONS_GROUPS_WITH_LOOPS 
    global NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG
    global CHECK_PAGERANK_OUTPUT
    global SAME_OUTPUT_FOR_ALL_FANOUT_FANIN
    global USE_INCREMENTAL_DAG_GENERATION
    global INCREMENTAL_DAG_DEPOSIT_INTERVAL
    global ENABLE_RUNTIME_TASK_CLUSTERING
    global MIN_PARTITION_GROUP_SIZE_FOR_CLUSTERING
    global MAX_SIZE_OF_OUTPUT_TO_FANOUT_TASK
    global WORK_QUEUE_SIZE_FOR_INCREMENTAL_DAG_GENERATION_WITH_WORKER_PROCESSES
    global TASKS_USE_RESULT_DICTIONARY_PARAMETER
    global USE_SHARED_PARTITIONS_GROUPS
    global USE_PAGERANK_GROUPS_PARTITIONS
    global USE_STRUCT_OF_ARRAYS_FOR_PAGERANK
    global USE_MUTLITHREADED_NONINCREMENTAL_BFS
    global INPUT_ALL_GROUPS_PARTITIONS_AT_START

    RUN_ALL_TASKS_LOCALLY = True
    BYPASS_CALL_TO_INVOKE_REAL_LAMBDA = (not RUN_ALL_TASKS_LOCALLY) and True 
    STORE_FANINS_FANINNBS_LOCALLY = False 
    CREATE_ALL_FANINS_FANINNBS_ON_START = True
    USING_WORKERS = True
    USING_THREADS_NOT_PROCESSES = False
    NUM_WORKERS = 2
    USE_MULTITHREADED_MULTIPROCESSING = False
    NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING = 1

    FANIN_TYPE = "DAG_executor_FanIn"
    FANINNB_TYPE = "DAG_executor_FanInNB"
    PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer"
    #FANIN_TYPE = "DAG_executor_FanIn_Select"
    #FANINNB_TYPE = "DAG_executor_FanInNB_Select"
    #PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer_Select"

    STORE_SYNC_OBJECTS_IN_LAMBDAS = False
    USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS = False
    SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS = False
    USING_DAG_ORCHESTRATOR = False
    MAP_OBJECTS_TO_LAMBDA_FUNCTIONS = False
    USE_ANONYMOUS_LAMBDA_FUNCTIONS = False
    USE_SINGLE_LAMBDA_FUNCTION = False

    PAGERANK_GRAPH_FILE_NAME = "graph_24N_3CC_fanin"
    COMPUTE_PAGERANK = True
    CHECK_PAGERANK_OUTPUT = COMPUTE_PAGERANK and RUN_ALL_TASKS_LOCALLY and (USING_WORKERS or not USING_WORKERS) and USING_THREADS_NOT_PROCESSES and True
    NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG = "PR1_1"
    NUMBER_OF_PAGERANK_ITERATIONS_FOR_PARTITIONS_GROUPS_WITH_LOOPS = 10
    SAME_OUTPUT_FOR_ALL_FANOUT_FANIN = not COMPUTE_PAGERANK
    USE_INCREMENTAL_DAG_GENERATION = COMPUTE_PAGERANK and False
    INCREMENTAL_DAG_DEPOSIT_INTERVAL = 2
    ENABLE_RUNTIME_TASK_CLUSTERING = COMPUTE_PAGERANK and False
    MIN_PARTITION_GROUP_SIZE_FOR_CLUSTERING  = 5
    MAX_SIZE_OF_OUTPUT_TO_FANOUT_TASK = 10000
    WORK_QUEUE_SIZE_FOR_INCREMENTAL_DAG_GENERATION_WITH_WORKER_PROCESSES =  2**10-1
    TASKS_USE_RESULT_DICTIONARY_PARAMETER = COMPUTE_PAGERANK and True
    USE_SHARED_PARTITIONS_GROUPS = COMPUTE_PAGERANK and True
    USE_PAGERANK_GROUPS_PARTITIONS = COMPUTE_PAGERANK and True
    USE_STRUCT_OF_ARRAYS_FOR_PAGERANK = COMPUTE_PAGERANK and False
    USE_MUTLITHREADED_NONINCREMENTAL_BFS = COMPUTE_PAGERANK and False
    INPUT_ALL_GROUPS_PARTITIONS_AT_START = COMPUTE_PAGERANK and False

#Test45: worker processes (A2) with non-selective-wait Sync-objects, 
#        2 worker processes, Sync-objects stored remotely
#        create objects at the start
#        use pagerank partitions
#        use shared
# Note: tcp_server must be running: tcp_server -t 45
def test45():
    print("test45")
    global EXIT_PROGRAM_ON_EXCEPTION
    global DEALLOCATE_BFS_MAIN_MAP_ON_THE_FLY
    global RUN_ALL_TASKS_LOCALLY
    global BYPASS_CALL_TO_INVOKE_REAL_LAMBDA
    global STORE_FANINS_FANINNBS_LOCALLY
    global CREATE_ALL_FANINS_FANINNBS_ON_START
    global USING_WORKERS
    global USING_THREADS_NOT_PROCESSES
    global NUM_WORKERS
    global USE_MULTITHREADED_MULTIPROCESSING
    global NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING
    global FANIN_TYPE
    global FANINNB_TYPE
    global PROCESS_WORK_QUEUE_TYPE
    
    global STORE_SYNC_OBJECTS_IN_LAMBDAS
    global USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS
    global SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS
    global USING_DAG_ORCHESTRATOR
    global MAP_OBJECTS_TO_LAMBDA_FUNCTIONS
    global USE_ANONYMOUS_LAMBDA_FUNCTIONS
    global USE_SINGLE_LAMBDA_FUNCTION

    global PAGERANK_GRAPH_FILE_NAME
    global COMPUTE_PAGERANK
    global NUMBER_OF_PAGERANK_ITERATIONS_FOR_PARTITIONS_GROUPS_WITH_LOOPS 
    global NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG
    global CHECK_PAGERANK_OUTPUT
    global SAME_OUTPUT_FOR_ALL_FANOUT_FANIN
    global USE_INCREMENTAL_DAG_GENERATION
    global INCREMENTAL_DAG_DEPOSIT_INTERVAL
    global ENABLE_RUNTIME_TASK_CLUSTERING
    global MIN_PARTITION_GROUP_SIZE_FOR_CLUSTERING
    global MAX_SIZE_OF_OUTPUT_TO_FANOUT_TASK
    global WORK_QUEUE_SIZE_FOR_INCREMENTAL_DAG_GENERATION_WITH_WORKER_PROCESSES
    global TASKS_USE_RESULT_DICTIONARY_PARAMETER
    global USE_SHARED_PARTITIONS_GROUPS
    global USE_PAGERANK_GROUPS_PARTITIONS
    global USE_STRUCT_OF_ARRAYS_FOR_PAGERANK
    global USE_MUTLITHREADED_NONINCREMENTAL_BFS
    global INPUT_ALL_GROUPS_PARTITIONS_AT_START

    RUN_ALL_TASKS_LOCALLY = True
    BYPASS_CALL_TO_INVOKE_REAL_LAMBDA = (not RUN_ALL_TASKS_LOCALLY) and True 
    STORE_FANINS_FANINNBS_LOCALLY = False 
    CREATE_ALL_FANINS_FANINNBS_ON_START = True
    USING_WORKERS = True
    USING_THREADS_NOT_PROCESSES = False
    NUM_WORKERS = 2
    USE_MULTITHREADED_MULTIPROCESSING = False
    NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING = 1

    FANIN_TYPE = "DAG_executor_FanIn"
    FANINNB_TYPE = "DAG_executor_FanInNB"
    PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer"
    #FANIN_TYPE = "DAG_executor_FanIn_Select"
    #FANINNB_TYPE = "DAG_executor_FanInNB_Select"
    #PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer_Select"

    STORE_SYNC_OBJECTS_IN_LAMBDAS = False
    USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS = False
    SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS = False
    USING_DAG_ORCHESTRATOR = False
    MAP_OBJECTS_TO_LAMBDA_FUNCTIONS = False
    USE_ANONYMOUS_LAMBDA_FUNCTIONS = False
    USE_SINGLE_LAMBDA_FUNCTION = False

    PAGERANK_GRAPH_FILE_NAME = "graph_24N_3CC_fanin"
    COMPUTE_PAGERANK = True
    CHECK_PAGERANK_OUTPUT = COMPUTE_PAGERANK and RUN_ALL_TASKS_LOCALLY and (USING_WORKERS or not USING_WORKERS) and USING_THREADS_NOT_PROCESSES and True
    NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG = "PR1_1"
    NUMBER_OF_PAGERANK_ITERATIONS_FOR_PARTITIONS_GROUPS_WITH_LOOPS = 10
    SAME_OUTPUT_FOR_ALL_FANOUT_FANIN = not COMPUTE_PAGERANK
    USE_INCREMENTAL_DAG_GENERATION = COMPUTE_PAGERANK and False
    INCREMENTAL_DAG_DEPOSIT_INTERVAL = 2
    ENABLE_RUNTIME_TASK_CLUSTERING = COMPUTE_PAGERANK and False
    MIN_PARTITION_GROUP_SIZE_FOR_CLUSTERING  = 5
    MAX_SIZE_OF_OUTPUT_TO_FANOUT_TASK = 10000
    WORK_QUEUE_SIZE_FOR_INCREMENTAL_DAG_GENERATION_WITH_WORKER_PROCESSES =  2**10-1
    TASKS_USE_RESULT_DICTIONARY_PARAMETER = COMPUTE_PAGERANK and True
    USE_SHARED_PARTITIONS_GROUPS = COMPUTE_PAGERANK and True
    USE_PAGERANK_GROUPS_PARTITIONS = COMPUTE_PAGERANK and False
    USE_STRUCT_OF_ARRAYS_FOR_PAGERANK = COMPUTE_PAGERANK and False
    USE_MUTLITHREADED_NONINCREMENTAL_BFS = COMPUTE_PAGERANK and False
    INPUT_ALL_GROUPS_PARTITIONS_AT_START = COMPUTE_PAGERANK and False

#Test46: worker processes (A2) with non-selective-wait Sync-objects, 
#        2 worker processes, Sync-objects stored remotely
#        create objects at the start
#        use pagerank groups
#        use shared
#        use array of structs
# Note: tcp_server must be running: tcp_server -t 46
def test46():
    print("test46")
    global EXIT_PROGRAM_ON_EXCEPTION
    global DEALLOCATE_BFS_MAIN_MAP_ON_THE_FLY
    global RUN_ALL_TASKS_LOCALLY
    global BYPASS_CALL_TO_INVOKE_REAL_LAMBDA
    global STORE_FANINS_FANINNBS_LOCALLY
    global CREATE_ALL_FANINS_FANINNBS_ON_START
    global USING_WORKERS
    global USING_THREADS_NOT_PROCESSES
    global NUM_WORKERS
    global USE_MULTITHREADED_MULTIPROCESSING
    global NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING
    global FANIN_TYPE
    global FANINNB_TYPE
    global PROCESS_WORK_QUEUE_TYPE
    
    global STORE_SYNC_OBJECTS_IN_LAMBDAS
    global USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS
    global SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS
    global USING_DAG_ORCHESTRATOR
    global MAP_OBJECTS_TO_LAMBDA_FUNCTIONS
    global USE_ANONYMOUS_LAMBDA_FUNCTIONS
    global USE_SINGLE_LAMBDA_FUNCTION

    global PAGERANK_GRAPH_FILE_NAME
    global COMPUTE_PAGERANK
    global NUMBER_OF_PAGERANK_ITERATIONS_FOR_PARTITIONS_GROUPS_WITH_LOOPS 
    global NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG
    global CHECK_PAGERANK_OUTPUT
    global SAME_OUTPUT_FOR_ALL_FANOUT_FANIN
    global USE_INCREMENTAL_DAG_GENERATION
    global INCREMENTAL_DAG_DEPOSIT_INTERVAL
    global ENABLE_RUNTIME_TASK_CLUSTERING
    global MIN_PARTITION_GROUP_SIZE_FOR_CLUSTERING
    global MAX_SIZE_OF_OUTPUT_TO_FANOUT_TASK
    global WORK_QUEUE_SIZE_FOR_INCREMENTAL_DAG_GENERATION_WITH_WORKER_PROCESSES
    global TASKS_USE_RESULT_DICTIONARY_PARAMETER
    global USE_SHARED_PARTITIONS_GROUPS
    global USE_PAGERANK_GROUPS_PARTITIONS
    global USE_STRUCT_OF_ARRAYS_FOR_PAGERANK
    global USE_MUTLITHREADED_NONINCREMENTAL_BFS
    global INPUT_ALL_GROUPS_PARTITIONS_AT_START

    RUN_ALL_TASKS_LOCALLY = True
    BYPASS_CALL_TO_INVOKE_REAL_LAMBDA = (not RUN_ALL_TASKS_LOCALLY) and True 
    STORE_FANINS_FANINNBS_LOCALLY = False 
    CREATE_ALL_FANINS_FANINNBS_ON_START = True
    USING_WORKERS = True
    USING_THREADS_NOT_PROCESSES = False
    NUM_WORKERS = 2
    USE_MULTITHREADED_MULTIPROCESSING = False
    NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING = 1

    FANIN_TYPE = "DAG_executor_FanIn"
    FANINNB_TYPE = "DAG_executor_FanInNB"
    PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer"
    #FANIN_TYPE = "DAG_executor_FanIn_Select"
    #FANINNB_TYPE = "DAG_executor_FanInNB_Select"
    #PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer_Select"

    STORE_SYNC_OBJECTS_IN_LAMBDAS = False
    USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS = False
    SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS = False
    USING_DAG_ORCHESTRATOR = False
    MAP_OBJECTS_TO_LAMBDA_FUNCTIONS = False
    USE_ANONYMOUS_LAMBDA_FUNCTIONS = False
    USE_SINGLE_LAMBDA_FUNCTION = False

    PAGERANK_GRAPH_FILE_NAME = "graph_24N_3CC_fanin"
    COMPUTE_PAGERANK = True
    CHECK_PAGERANK_OUTPUT = COMPUTE_PAGERANK and RUN_ALL_TASKS_LOCALLY and (USING_WORKERS or not USING_WORKERS) and USING_THREADS_NOT_PROCESSES and True
    NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG = "PR1_1"
    NUMBER_OF_PAGERANK_ITERATIONS_FOR_PARTITIONS_GROUPS_WITH_LOOPS = 10
    SAME_OUTPUT_FOR_ALL_FANOUT_FANIN = not COMPUTE_PAGERANK
    USE_INCREMENTAL_DAG_GENERATION = COMPUTE_PAGERANK and False
    INCREMENTAL_DAG_DEPOSIT_INTERVAL = 2
    ENABLE_RUNTIME_TASK_CLUSTERING = COMPUTE_PAGERANK and False
    MIN_PARTITION_GROUP_SIZE_FOR_CLUSTERING  = 5
    MAX_SIZE_OF_OUTPUT_TO_FANOUT_TASK = 10000
    WORK_QUEUE_SIZE_FOR_INCREMENTAL_DAG_GENERATION_WITH_WORKER_PROCESSES =  2**10-1
    TASKS_USE_RESULT_DICTIONARY_PARAMETER = COMPUTE_PAGERANK and True
    USE_SHARED_PARTITIONS_GROUPS = COMPUTE_PAGERANK and True
    USE_PAGERANK_GROUPS_PARTITIONS = COMPUTE_PAGERANK and True
    USE_STRUCT_OF_ARRAYS_FOR_PAGERANK = COMPUTE_PAGERANK and True
    USE_MUTLITHREADED_NONINCREMENTAL_BFS = COMPUTE_PAGERANK and False
    INPUT_ALL_GROUPS_PARTITIONS_AT_START = COMPUTE_PAGERANK and False

#Test47: worker processes (A2) with non-selective-wait Sync-objects, 
#        2 worker processes, Sync-objects stored remotely
#        create objects at the start
#        use pagerank partitions
#        use shared
#        use array of structs
# Note: tcp_server must be running: tcp_server -t 47
def test47():
    print("test47")
    global EXIT_PROGRAM_ON_EXCEPTION
    global DEALLOCATE_BFS_MAIN_MAP_ON_THE_FLY
    global RUN_ALL_TASKS_LOCALLY
    global BYPASS_CALL_TO_INVOKE_REAL_LAMBDA
    global STORE_FANINS_FANINNBS_LOCALLY
    global CREATE_ALL_FANINS_FANINNBS_ON_START
    global USING_WORKERS
    global USING_THREADS_NOT_PROCESSES
    global NUM_WORKERS
    global USE_MULTITHREADED_MULTIPROCESSING
    global NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING
    global FANIN_TYPE
    global FANINNB_TYPE
    global PROCESS_WORK_QUEUE_TYPE
    
    global STORE_SYNC_OBJECTS_IN_LAMBDAS
    global USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS
    global SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS
    global USING_DAG_ORCHESTRATOR
    global MAP_OBJECTS_TO_LAMBDA_FUNCTIONS
    global USE_ANONYMOUS_LAMBDA_FUNCTIONS
    global USE_SINGLE_LAMBDA_FUNCTION

    global PAGERANK_GRAPH_FILE_NAME
    global COMPUTE_PAGERANK
    global NUMBER_OF_PAGERANK_ITERATIONS_FOR_PARTITIONS_GROUPS_WITH_LOOPS 
    global NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG
    global CHECK_PAGERANK_OUTPUT
    global SAME_OUTPUT_FOR_ALL_FANOUT_FANIN
    global USE_INCREMENTAL_DAG_GENERATION
    global INCREMENTAL_DAG_DEPOSIT_INTERVAL
    global ENABLE_RUNTIME_TASK_CLUSTERING
    global MIN_PARTITION_GROUP_SIZE_FOR_CLUSTERING
    global MAX_SIZE_OF_OUTPUT_TO_FANOUT_TASK
    global WORK_QUEUE_SIZE_FOR_INCREMENTAL_DAG_GENERATION_WITH_WORKER_PROCESSES
    global TASKS_USE_RESULT_DICTIONARY_PARAMETER
    global USE_SHARED_PARTITIONS_GROUPS
    global USE_PAGERANK_GROUPS_PARTITIONS
    global USE_STRUCT_OF_ARRAYS_FOR_PAGERANK
    global USE_MUTLITHREADED_NONINCREMENTAL_BFS
    global INPUT_ALL_GROUPS_PARTITIONS_AT_START

    RUN_ALL_TASKS_LOCALLY = True
    BYPASS_CALL_TO_INVOKE_REAL_LAMBDA = (not RUN_ALL_TASKS_LOCALLY) and True 
    STORE_FANINS_FANINNBS_LOCALLY = False 
    CREATE_ALL_FANINS_FANINNBS_ON_START = True
    USING_WORKERS = True
    USING_THREADS_NOT_PROCESSES = False
    NUM_WORKERS = 2
    USE_MULTITHREADED_MULTIPROCESSING = False
    NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING = 1

    FANIN_TYPE = "DAG_executor_FanIn"
    FANINNB_TYPE = "DAG_executor_FanInNB"
    PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer"
    #FANIN_TYPE = "DAG_executor_FanIn_Select"
    #FANINNB_TYPE = "DAG_executor_FanInNB_Select"
    #PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer_Select"

    STORE_SYNC_OBJECTS_IN_LAMBDAS = False
    USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS = False
    SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS = False
    USING_DAG_ORCHESTRATOR = False
    MAP_OBJECTS_TO_LAMBDA_FUNCTIONS = False
    USE_ANONYMOUS_LAMBDA_FUNCTIONS = False
    USE_SINGLE_LAMBDA_FUNCTION = False

    PAGERANK_GRAPH_FILE_NAME = "graph_24N_3CC_fanin"
    COMPUTE_PAGERANK = True
    CHECK_PAGERANK_OUTPUT = COMPUTE_PAGERANK and RUN_ALL_TASKS_LOCALLY and (USING_WORKERS or not USING_WORKERS) and USING_THREADS_NOT_PROCESSES and True
    NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG = "PR1_1"
    NUMBER_OF_PAGERANK_ITERATIONS_FOR_PARTITIONS_GROUPS_WITH_LOOPS = 10
    SAME_OUTPUT_FOR_ALL_FANOUT_FANIN = not COMPUTE_PAGERANK
    USE_INCREMENTAL_DAG_GENERATION = COMPUTE_PAGERANK and False
    INCREMENTAL_DAG_DEPOSIT_INTERVAL = 2
    ENABLE_RUNTIME_TASK_CLUSTERING = COMPUTE_PAGERANK and False
    MIN_PARTITION_GROUP_SIZE_FOR_CLUSTERING  = 5
    MAX_SIZE_OF_OUTPUT_TO_FANOUT_TASK = 10000
    WORK_QUEUE_SIZE_FOR_INCREMENTAL_DAG_GENERATION_WITH_WORKER_PROCESSES =  2**10-1
    TASKS_USE_RESULT_DICTIONARY_PARAMETER = COMPUTE_PAGERANK and True
    USE_SHARED_PARTITIONS_GROUPS = COMPUTE_PAGERANK and True
    USE_PAGERANK_GROUPS_PARTITIONS = COMPUTE_PAGERANK and False
    USE_STRUCT_OF_ARRAYS_FOR_PAGERANK = COMPUTE_PAGERANK and True
    USE_MUTLITHREADED_NONINCREMENTAL_BFS = COMPUTE_PAGERANK and False
    INPUT_ALL_GROUPS_PARTITIONS_AT_START = COMPUTE_PAGERANK and False

# Test Pagerank with incremental DAG generation

#Test48: worker threads (A2) with non-selective-wait Sync-objects, 
#        2 worker threads, Sync-objects stored locally
#        create objects on-the-fly (required for incremenal DAG generation)
#        use pagerank groups
def test48():
    print("test48")
    global EXIT_PROGRAM_ON_EXCEPTION
    global DEALLOCATE_BFS_MAIN_MAP_ON_THE_FLY
    global RUN_ALL_TASKS_LOCALLY
    global BYPASS_CALL_TO_INVOKE_REAL_LAMBDA
    global STORE_FANINS_FANINNBS_LOCALLY
    global CREATE_ALL_FANINS_FANINNBS_ON_START
    global USING_WORKERS
    global USING_THREADS_NOT_PROCESSES
    global NUM_WORKERS
    global USE_MULTITHREADED_MULTIPROCESSING
    global NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING
    global FANIN_TYPE
    global FANINNB_TYPE
    global PROCESS_WORK_QUEUE_TYPE
    
    global STORE_SYNC_OBJECTS_IN_LAMBDAS
    global USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS
    global SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS
    global USING_DAG_ORCHESTRATOR
    global MAP_OBJECTS_TO_LAMBDA_FUNCTIONS
    global USE_ANONYMOUS_LAMBDA_FUNCTIONS
    global USE_SINGLE_LAMBDA_FUNCTION

    global PAGERANK_GRAPH_FILE_NAME
    global COMPUTE_PAGERANK
    global NUMBER_OF_PAGERANK_ITERATIONS_FOR_PARTITIONS_GROUPS_WITH_LOOPS 
    global NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG
    global CHECK_PAGERANK_OUTPUT
    global SAME_OUTPUT_FOR_ALL_FANOUT_FANIN
    global USE_INCREMENTAL_DAG_GENERATION
    global INCREMENTAL_DAG_DEPOSIT_INTERVAL
    global ENABLE_RUNTIME_TASK_CLUSTERING
    global MIN_PARTITION_GROUP_SIZE_FOR_CLUSTERING
    global MAX_SIZE_OF_OUTPUT_TO_FANOUT_TASK
    global WORK_QUEUE_SIZE_FOR_INCREMENTAL_DAG_GENERATION_WITH_WORKER_PROCESSES
    global TASKS_USE_RESULT_DICTIONARY_PARAMETER
    global USE_SHARED_PARTITIONS_GROUPS
    global USE_PAGERANK_GROUPS_PARTITIONS
    global USE_STRUCT_OF_ARRAYS_FOR_PAGERANK
    global USE_MUTLITHREADED_NONINCREMENTAL_BFS
    global INPUT_ALL_GROUPS_PARTITIONS_AT_START

    RUN_ALL_TASKS_LOCALLY = True
    BYPASS_CALL_TO_INVOKE_REAL_LAMBDA = (not RUN_ALL_TASKS_LOCALLY) and True 
    STORE_FANINS_FANINNBS_LOCALLY = True 
    CREATE_ALL_FANINS_FANINNBS_ON_START = False
    USING_WORKERS = True
    USING_THREADS_NOT_PROCESSES = True
    NUM_WORKERS = 2
    USE_MULTITHREADED_MULTIPROCESSING = False
    NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING = 1

    FANIN_TYPE = "DAG_executor_FanIn"
    FANINNB_TYPE = "DAG_executor_FanInNB"
    PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer"
    #FANIN_TYPE = "DAG_executor_FanIn_Select"
    #FANINNB_TYPE = "DAG_executor_FanInNB_Select"
    #PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer_Select"

    STORE_SYNC_OBJECTS_IN_LAMBDAS = False
    USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS = False
    SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS = False
    USING_DAG_ORCHESTRATOR = False
    MAP_OBJECTS_TO_LAMBDA_FUNCTIONS = False
    USE_ANONYMOUS_LAMBDA_FUNCTIONS = False
    USE_SINGLE_LAMBDA_FUNCTION = False

    PAGERANK_GRAPH_FILE_NAME = "graph_24N_3CC_fanin"
    COMPUTE_PAGERANK = True
    CHECK_PAGERANK_OUTPUT = COMPUTE_PAGERANK and RUN_ALL_TASKS_LOCALLY and (USING_WORKERS or not USING_WORKERS) and USING_THREADS_NOT_PROCESSES and True
    NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG = "PR1_1"
    NUMBER_OF_PAGERANK_ITERATIONS_FOR_PARTITIONS_GROUPS_WITH_LOOPS = 10
    SAME_OUTPUT_FOR_ALL_FANOUT_FANIN = not COMPUTE_PAGERANK
    USE_INCREMENTAL_DAG_GENERATION = COMPUTE_PAGERANK and True
    INCREMENTAL_DAG_DEPOSIT_INTERVAL = 1
    ENABLE_RUNTIME_TASK_CLUSTERING = COMPUTE_PAGERANK and False
    MIN_PARTITION_GROUP_SIZE_FOR_CLUSTERING  = 5
    MAX_SIZE_OF_OUTPUT_TO_FANOUT_TASK = 10000
    WORK_QUEUE_SIZE_FOR_INCREMENTAL_DAG_GENERATION_WITH_WORKER_PROCESSES =  2**10-1
    TASKS_USE_RESULT_DICTIONARY_PARAMETER = COMPUTE_PAGERANK and True
    USE_SHARED_PARTITIONS_GROUPS = COMPUTE_PAGERANK and False
    USE_PAGERANK_GROUPS_PARTITIONS = COMPUTE_PAGERANK and True
    USE_STRUCT_OF_ARRAYS_FOR_PAGERANK = COMPUTE_PAGERANK and False
    USE_MUTLITHREADED_NONINCREMENTAL_BFS = COMPUTE_PAGERANK and False
    INPUT_ALL_GROUPS_PARTITIONS_AT_START = COMPUTE_PAGERANK and False

#Test49: worker threads (A2) with non-selective-wait Sync-objects, 
#        2 worker threads, Sync-objects stored locally
#        create objects on-the-fly (required for incremenal DAG generation)
#        use pagerank groups
#        use multithreaded BFS
def test49():
    print("test49")
    global EXIT_PROGRAM_ON_EXCEPTION
    global DEALLOCATE_BFS_MAIN_MAP_ON_THE_FLY
    global RUN_ALL_TASKS_LOCALLY
    global BYPASS_CALL_TO_INVOKE_REAL_LAMBDA
    global STORE_FANINS_FANINNBS_LOCALLY
    global CREATE_ALL_FANINS_FANINNBS_ON_START
    global USING_WORKERS
    global USING_THREADS_NOT_PROCESSES
    global NUM_WORKERS
    global USE_MULTITHREADED_MULTIPROCESSING
    global NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING
    global FANIN_TYPE
    global FANINNB_TYPE
    global PROCESS_WORK_QUEUE_TYPE
    
    global STORE_SYNC_OBJECTS_IN_LAMBDAS
    global USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS
    global SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS
    global USING_DAG_ORCHESTRATOR
    global MAP_OBJECTS_TO_LAMBDA_FUNCTIONS
    global USE_ANONYMOUS_LAMBDA_FUNCTIONS
    global USE_SINGLE_LAMBDA_FUNCTION

    global PAGERANK_GRAPH_FILE_NAME
    global COMPUTE_PAGERANK
    global NUMBER_OF_PAGERANK_ITERATIONS_FOR_PARTITIONS_GROUPS_WITH_LOOPS 
    global NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG
    global CHECK_PAGERANK_OUTPUT
    global SAME_OUTPUT_FOR_ALL_FANOUT_FANIN
    global USE_INCREMENTAL_DAG_GENERATION
    global INCREMENTAL_DAG_DEPOSIT_INTERVAL
    global ENABLE_RUNTIME_TASK_CLUSTERING
    global MIN_PARTITION_GROUP_SIZE_FOR_CLUSTERING
    global MAX_SIZE_OF_OUTPUT_TO_FANOUT_TASK
    global WORK_QUEUE_SIZE_FOR_INCREMENTAL_DAG_GENERATION_WITH_WORKER_PROCESSES
    global TASKS_USE_RESULT_DICTIONARY_PARAMETER
    global USE_SHARED_PARTITIONS_GROUPS
    global USE_PAGERANK_GROUPS_PARTITIONS
    global USE_STRUCT_OF_ARRAYS_FOR_PAGERANK
    global USE_MUTLITHREADED_NONINCREMENTAL_BFS
    global INPUT_ALL_GROUPS_PARTITIONS_AT_START

    RUN_ALL_TASKS_LOCALLY = True
    BYPASS_CALL_TO_INVOKE_REAL_LAMBDA = (not RUN_ALL_TASKS_LOCALLY) and True 
    STORE_FANINS_FANINNBS_LOCALLY = True 
    CREATE_ALL_FANINS_FANINNBS_ON_START = True
    USING_WORKERS = True
    USING_THREADS_NOT_PROCESSES = True
    NUM_WORKERS = 2
    USE_MULTITHREADED_MULTIPROCESSING = False
    NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING = 1

    FANIN_TYPE = "DAG_executor_FanIn"
    FANINNB_TYPE = "DAG_executor_FanInNB"
    PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer"
    #FANIN_TYPE = "DAG_executor_FanIn_Select"
    #FANINNB_TYPE = "DAG_executor_FanInNB_Select"
    #PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer_Select"

    STORE_SYNC_OBJECTS_IN_LAMBDAS = False
    USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS = False
    SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS = False
    USING_DAG_ORCHESTRATOR = False
    MAP_OBJECTS_TO_LAMBDA_FUNCTIONS = False
    USE_ANONYMOUS_LAMBDA_FUNCTIONS = False
    USE_SINGLE_LAMBDA_FUNCTION = False

    PAGERANK_GRAPH_FILE_NAME = "graph_24N_3CC_fanin"
    COMPUTE_PAGERANK = True
    CHECK_PAGERANK_OUTPUT = COMPUTE_PAGERANK and RUN_ALL_TASKS_LOCALLY and (USING_WORKERS or not USING_WORKERS) and USING_THREADS_NOT_PROCESSES and True
    NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG = "PR1_1"
    NUMBER_OF_PAGERANK_ITERATIONS_FOR_PARTITIONS_GROUPS_WITH_LOOPS = 10
    SAME_OUTPUT_FOR_ALL_FANOUT_FANIN = not COMPUTE_PAGERANK
    USE_INCREMENTAL_DAG_GENERATION = COMPUTE_PAGERANK and False
    INCREMENTAL_DAG_DEPOSIT_INTERVAL = 1
    ENABLE_RUNTIME_TASK_CLUSTERING = COMPUTE_PAGERANK and False
    MIN_PARTITION_GROUP_SIZE_FOR_CLUSTERING  = 5
    MAX_SIZE_OF_OUTPUT_TO_FANOUT_TASK = 10000
    WORK_QUEUE_SIZE_FOR_INCREMENTAL_DAG_GENERATION_WITH_WORKER_PROCESSES =  2**10-1
    TASKS_USE_RESULT_DICTIONARY_PARAMETER = COMPUTE_PAGERANK and True
    USE_SHARED_PARTITIONS_GROUPS = COMPUTE_PAGERANK and False
    USE_PAGERANK_GROUPS_PARTITIONS = COMPUTE_PAGERANK and True
    USE_STRUCT_OF_ARRAYS_FOR_PAGERANK = COMPUTE_PAGERANK and False
    USE_MUTLITHREADED_NONINCREMENTAL_BFS = COMPUTE_PAGERANK and True
    INPUT_ALL_GROUPS_PARTITIONS_AT_START = COMPUTE_PAGERANK and False

#Test50: worker threads (A2) with non-selective-wait Sync-objects, 
#        2 worker threads, Sync-objects stored locally
#        use pagerank groups
#        test graph with 1 node 0 edges
def test50():
    print("test50")
    global EXIT_PROGRAM_ON_EXCEPTION
    global DEALLOCATE_BFS_MAIN_MAP_ON_THE_FLY
    global RUN_ALL_TASKS_LOCALLY
    global BYPASS_CALL_TO_INVOKE_REAL_LAMBDA
    global STORE_FANINS_FANINNBS_LOCALLY
    global CREATE_ALL_FANINS_FANINNBS_ON_START
    global USING_WORKERS
    global USING_THREADS_NOT_PROCESSES
    global NUM_WORKERS
    global USE_MULTITHREADED_MULTIPROCESSING
    global NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING
    global FANIN_TYPE
    global FANINNB_TYPE
    global PROCESS_WORK_QUEUE_TYPE
    
    global STORE_SYNC_OBJECTS_IN_LAMBDAS
    global USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS
    global SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS
    global USING_DAG_ORCHESTRATOR
    global MAP_OBJECTS_TO_LAMBDA_FUNCTIONS
    global USE_ANONYMOUS_LAMBDA_FUNCTIONS
    global USE_SINGLE_LAMBDA_FUNCTION

    global PAGERANK_GRAPH_FILE_NAME
    global COMPUTE_PAGERANK
    global NUMBER_OF_PAGERANK_ITERATIONS_FOR_PARTITIONS_GROUPS_WITH_LOOPS 
    global NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG
    global CHECK_PAGERANK_OUTPUT
    global SAME_OUTPUT_FOR_ALL_FANOUT_FANIN
    global USE_INCREMENTAL_DAG_GENERATION
    global INCREMENTAL_DAG_DEPOSIT_INTERVAL
    global ENABLE_RUNTIME_TASK_CLUSTERING
    global MIN_PARTITION_GROUP_SIZE_FOR_CLUSTERING
    global MAX_SIZE_OF_OUTPUT_TO_FANOUT_TASK
    global WORK_QUEUE_SIZE_FOR_INCREMENTAL_DAG_GENERATION_WITH_WORKER_PROCESSES
    global TASKS_USE_RESULT_DICTIONARY_PARAMETER
    global USE_SHARED_PARTITIONS_GROUPS
    global USE_PAGERANK_GROUPS_PARTITIONS
    global USE_STRUCT_OF_ARRAYS_FOR_PAGERANK
    global USE_MUTLITHREADED_NONINCREMENTAL_BFS
    global INPUT_ALL_GROUPS_PARTITIONS_AT_START

    RUN_ALL_TASKS_LOCALLY = True
    BYPASS_CALL_TO_INVOKE_REAL_LAMBDA = (not RUN_ALL_TASKS_LOCALLY) and True 
    STORE_FANINS_FANINNBS_LOCALLY = True 
    CREATE_ALL_FANINS_FANINNBS_ON_START = True
    USING_WORKERS = True
    USING_THREADS_NOT_PROCESSES = True
    NUM_WORKERS = 2
    USE_MULTITHREADED_MULTIPROCESSING = False
    NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING = 1

    FANIN_TYPE = "DAG_executor_FanIn"
    FANINNB_TYPE = "DAG_executor_FanInNB"
    PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer"
    #FANIN_TYPE = "DAG_executor_FanIn_Select"
    #FANINNB_TYPE = "DAG_executor_FanInNB_Select"
    #PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer_Select"

    STORE_SYNC_OBJECTS_IN_LAMBDAS = False
    USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS = False
    SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS = False
    USING_DAG_ORCHESTRATOR = False
    MAP_OBJECTS_TO_LAMBDA_FUNCTIONS = False
    USE_ANONYMOUS_LAMBDA_FUNCTIONS = False
    USE_SINGLE_LAMBDA_FUNCTION = False

    PAGERANK_GRAPH_FILE_NAME = "graph_1N"
    COMPUTE_PAGERANK = True
    CHECK_PAGERANK_OUTPUT = COMPUTE_PAGERANK and RUN_ALL_TASKS_LOCALLY and (USING_WORKERS or not USING_WORKERS) and USING_THREADS_NOT_PROCESSES and True
    NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG = "PR1_1"
    NUMBER_OF_PAGERANK_ITERATIONS_FOR_PARTITIONS_GROUPS_WITH_LOOPS = 10
    SAME_OUTPUT_FOR_ALL_FANOUT_FANIN = not COMPUTE_PAGERANK
    USE_INCREMENTAL_DAG_GENERATION = COMPUTE_PAGERANK and False
    INCREMENTAL_DAG_DEPOSIT_INTERVAL = 1
    ENABLE_RUNTIME_TASK_CLUSTERING = COMPUTE_PAGERANK and False
    MIN_PARTITION_GROUP_SIZE_FOR_CLUSTERING  = 5
    MAX_SIZE_OF_OUTPUT_TO_FANOUT_TASK = 10000
    WORK_QUEUE_SIZE_FOR_INCREMENTAL_DAG_GENERATION_WITH_WORKER_PROCESSES =  2**10-1
    TASKS_USE_RESULT_DICTIONARY_PARAMETER = COMPUTE_PAGERANK and True
    USE_SHARED_PARTITIONS_GROUPS = COMPUTE_PAGERANK and False
    USE_PAGERANK_GROUPS_PARTITIONS = COMPUTE_PAGERANK and True
    USE_STRUCT_OF_ARRAYS_FOR_PAGERANK = COMPUTE_PAGERANK and False
    USE_MUTLITHREADED_NONINCREMENTAL_BFS = COMPUTE_PAGERANK and False
    INPUT_ALL_GROUPS_PARTITIONS_AT_START = COMPUTE_PAGERANK and False

#Test51: worker threads (A2) with non-selective-wait Sync-objects, 
#        2 worker threads, Sync-objects stored locally
#        use pagerank groups
#        test graph with 2 nodes 1 edge between them
def test51():
    print("test51")
    global EXIT_PROGRAM_ON_EXCEPTION
    global DEALLOCATE_BFS_MAIN_MAP_ON_THE_FLY
    global RUN_ALL_TASKS_LOCALLY
    global BYPASS_CALL_TO_INVOKE_REAL_LAMBDA
    global STORE_FANINS_FANINNBS_LOCALLY
    global CREATE_ALL_FANINS_FANINNBS_ON_START
    global USING_WORKERS
    global USING_THREADS_NOT_PROCESSES
    global NUM_WORKERS
    global USE_MULTITHREADED_MULTIPROCESSING
    global NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING
    global FANIN_TYPE
    global FANINNB_TYPE
    global PROCESS_WORK_QUEUE_TYPE
    
    global STORE_SYNC_OBJECTS_IN_LAMBDAS
    global USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS
    global SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS
    global USING_DAG_ORCHESTRATOR
    global MAP_OBJECTS_TO_LAMBDA_FUNCTIONS
    global USE_ANONYMOUS_LAMBDA_FUNCTIONS
    global USE_SINGLE_LAMBDA_FUNCTION

    global PAGERANK_GRAPH_FILE_NAME
    global COMPUTE_PAGERANK
    global NUMBER_OF_PAGERANK_ITERATIONS_FOR_PARTITIONS_GROUPS_WITH_LOOPS 
    global NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG
    global CHECK_PAGERANK_OUTPUT
    global SAME_OUTPUT_FOR_ALL_FANOUT_FANIN
    global USE_INCREMENTAL_DAG_GENERATION
    global INCREMENTAL_DAG_DEPOSIT_INTERVAL
    global ENABLE_RUNTIME_TASK_CLUSTERING
    global MIN_PARTITION_GROUP_SIZE_FOR_CLUSTERING
    global MAX_SIZE_OF_OUTPUT_TO_FANOUT_TASK
    global WORK_QUEUE_SIZE_FOR_INCREMENTAL_DAG_GENERATION_WITH_WORKER_PROCESSES
    global TASKS_USE_RESULT_DICTIONARY_PARAMETER
    global USE_SHARED_PARTITIONS_GROUPS
    global USE_PAGERANK_GROUPS_PARTITIONS
    global USE_STRUCT_OF_ARRAYS_FOR_PAGERANK
    global USE_MUTLITHREADED_NONINCREMENTAL_BFS
    global INPUT_ALL_GROUPS_PARTITIONS_AT_START

    RUN_ALL_TASKS_LOCALLY = True
    BYPASS_CALL_TO_INVOKE_REAL_LAMBDA = (not RUN_ALL_TASKS_LOCALLY) and True 
    STORE_FANINS_FANINNBS_LOCALLY = True 
    CREATE_ALL_FANINS_FANINNBS_ON_START = True
    USING_WORKERS = True
    USING_THREADS_NOT_PROCESSES = True
    NUM_WORKERS = 2
    USE_MULTITHREADED_MULTIPROCESSING = False
    NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING = 1

    FANIN_TYPE = "DAG_executor_FanIn"
    FANINNB_TYPE = "DAG_executor_FanInNB"
    PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer"
    #FANIN_TYPE = "DAG_executor_FanIn_Select"
    #FANINNB_TYPE = "DAG_executor_FanInNB_Select"
    #PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer_Select"

    STORE_SYNC_OBJECTS_IN_LAMBDAS = False
    USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS = False
    SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS = False
    USING_DAG_ORCHESTRATOR = False
    MAP_OBJECTS_TO_LAMBDA_FUNCTIONS = False
    USE_ANONYMOUS_LAMBDA_FUNCTIONS = False
    USE_SINGLE_LAMBDA_FUNCTION = False

    PAGERANK_GRAPH_FILE_NAME = "graph_2N"
    COMPUTE_PAGERANK = True
    CHECK_PAGERANK_OUTPUT = COMPUTE_PAGERANK and RUN_ALL_TASKS_LOCALLY and (USING_WORKERS or not USING_WORKERS) and USING_THREADS_NOT_PROCESSES and True
    NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG = "PR1_1"
    NUMBER_OF_PAGERANK_ITERATIONS_FOR_PARTITIONS_GROUPS_WITH_LOOPS = 10
    SAME_OUTPUT_FOR_ALL_FANOUT_FANIN = not COMPUTE_PAGERANK
    USE_INCREMENTAL_DAG_GENERATION = COMPUTE_PAGERANK and False
    INCREMENTAL_DAG_DEPOSIT_INTERVAL = 1
    ENABLE_RUNTIME_TASK_CLUSTERING = COMPUTE_PAGERANK and False
    MIN_PARTITION_GROUP_SIZE_FOR_CLUSTERING  = 5
    MAX_SIZE_OF_OUTPUT_TO_FANOUT_TASK = 10000
    WORK_QUEUE_SIZE_FOR_INCREMENTAL_DAG_GENERATION_WITH_WORKER_PROCESSES =  2**10-1
    TASKS_USE_RESULT_DICTIONARY_PARAMETER = COMPUTE_PAGERANK and True
    USE_SHARED_PARTITIONS_GROUPS = COMPUTE_PAGERANK and False
    USE_PAGERANK_GROUPS_PARTITIONS = COMPUTE_PAGERANK and True
    USE_STRUCT_OF_ARRAYS_FOR_PAGERANK = COMPUTE_PAGERANK and False
    USE_MUTLITHREADED_NONINCREMENTAL_BFS = COMPUTE_PAGERANK and False
    INPUT_ALL_GROUPS_PARTITIONS_AT_START = COMPUTE_PAGERANK and False

#Test52: worker threads (A2) with non-selective-wait Sync-objects, 
#        2 worker threads, Sync-objects stored locally
#        use pagerank groups
#        test graph with 2 nodes 0 edges, so 2 connected components
def test52():
    print("test52")
    global EXIT_PROGRAM_ON_EXCEPTION
    global DEALLOCATE_BFS_MAIN_MAP_ON_THE_FLY
    global RUN_ALL_TASKS_LOCALLY
    global BYPASS_CALL_TO_INVOKE_REAL_LAMBDA
    global STORE_FANINS_FANINNBS_LOCALLY
    global CREATE_ALL_FANINS_FANINNBS_ON_START
    global USING_WORKERS
    global USING_THREADS_NOT_PROCESSES
    global NUM_WORKERS
    global USE_MULTITHREADED_MULTIPROCESSING
    global NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING
    global FANIN_TYPE
    global FANINNB_TYPE
    global PROCESS_WORK_QUEUE_TYPE
    
    global STORE_SYNC_OBJECTS_IN_LAMBDAS
    global USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS
    global SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS
    global USING_DAG_ORCHESTRATOR
    global MAP_OBJECTS_TO_LAMBDA_FUNCTIONS
    global USE_ANONYMOUS_LAMBDA_FUNCTIONS
    global USE_SINGLE_LAMBDA_FUNCTION

    global PAGERANK_GRAPH_FILE_NAME
    global COMPUTE_PAGERANK
    global NUMBER_OF_PAGERANK_ITERATIONS_FOR_PARTITIONS_GROUPS_WITH_LOOPS 
    global NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG
    global CHECK_PAGERANK_OUTPUT
    global SAME_OUTPUT_FOR_ALL_FANOUT_FANIN
    global USE_INCREMENTAL_DAG_GENERATION
    global INCREMENTAL_DAG_DEPOSIT_INTERVAL
    global ENABLE_RUNTIME_TASK_CLUSTERING
    global MIN_PARTITION_GROUP_SIZE_FOR_CLUSTERING
    global MAX_SIZE_OF_OUTPUT_TO_FANOUT_TASK
    global WORK_QUEUE_SIZE_FOR_INCREMENTAL_DAG_GENERATION_WITH_WORKER_PROCESSES
    global TASKS_USE_RESULT_DICTIONARY_PARAMETER
    global USE_SHARED_PARTITIONS_GROUPS
    global USE_PAGERANK_GROUPS_PARTITIONS
    global USE_STRUCT_OF_ARRAYS_FOR_PAGERANK
    global USE_MUTLITHREADED_NONINCREMENTAL_BFS
    global INPUT_ALL_GROUPS_PARTITIONS_AT_START

    RUN_ALL_TASKS_LOCALLY = True
    BYPASS_CALL_TO_INVOKE_REAL_LAMBDA = (not RUN_ALL_TASKS_LOCALLY) and True 
    STORE_FANINS_FANINNBS_LOCALLY = True 
    CREATE_ALL_FANINS_FANINNBS_ON_START = True
    USING_WORKERS = True
    USING_THREADS_NOT_PROCESSES = True
    NUM_WORKERS = 2
    USE_MULTITHREADED_MULTIPROCESSING = False
    NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING = 1

    FANIN_TYPE = "DAG_executor_FanIn"
    FANINNB_TYPE = "DAG_executor_FanInNB"
    PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer"
    #FANIN_TYPE = "DAG_executor_FanIn_Select"
    #FANINNB_TYPE = "DAG_executor_FanInNB_Select"
    #PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer_Select"

    STORE_SYNC_OBJECTS_IN_LAMBDAS = False
    USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS = False
    SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS = False
    USING_DAG_ORCHESTRATOR = False
    MAP_OBJECTS_TO_LAMBDA_FUNCTIONS = False
    USE_ANONYMOUS_LAMBDA_FUNCTIONS = False
    USE_SINGLE_LAMBDA_FUNCTION = False

    PAGERANK_GRAPH_FILE_NAME = "graph_2N_2CC"
    COMPUTE_PAGERANK = True
    CHECK_PAGERANK_OUTPUT = COMPUTE_PAGERANK and RUN_ALL_TASKS_LOCALLY and (USING_WORKERS or not USING_WORKERS) and USING_THREADS_NOT_PROCESSES and True
    NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG = "PR1_1"
    NUMBER_OF_PAGERANK_ITERATIONS_FOR_PARTITIONS_GROUPS_WITH_LOOPS = 10
    SAME_OUTPUT_FOR_ALL_FANOUT_FANIN = not COMPUTE_PAGERANK
    USE_INCREMENTAL_DAG_GENERATION = COMPUTE_PAGERANK and False
    INCREMENTAL_DAG_DEPOSIT_INTERVAL = 1
    ENABLE_RUNTIME_TASK_CLUSTERING = COMPUTE_PAGERANK and False
    MIN_PARTITION_GROUP_SIZE_FOR_CLUSTERING  = 5
    MAX_SIZE_OF_OUTPUT_TO_FANOUT_TASK = 10000
    WORK_QUEUE_SIZE_FOR_INCREMENTAL_DAG_GENERATION_WITH_WORKER_PROCESSES =  2**10-1
    TASKS_USE_RESULT_DICTIONARY_PARAMETER = COMPUTE_PAGERANK and True
    USE_SHARED_PARTITIONS_GROUPS = COMPUTE_PAGERANK and False
    USE_PAGERANK_GROUPS_PARTITIONS = COMPUTE_PAGERANK and True
    USE_STRUCT_OF_ARRAYS_FOR_PAGERANK = COMPUTE_PAGERANK and False
    USE_MUTLITHREADED_NONINCREMENTAL_BFS = COMPUTE_PAGERANK and False
    INPUT_ALL_GROUPS_PARTITIONS_AT_START = COMPUTE_PAGERANK and False

#Test53: worker threads (A2) with non-selective-wait Sync-objects, 
#        2 worker threads, Sync-objects stored locally
#        use pagerank groups
#        test graph with 3 nodes 0 edges, so 3 connected components
def test53():
    print("test53")
    global EXIT_PROGRAM_ON_EXCEPTION
    global DEALLOCATE_BFS_MAIN_MAP_ON_THE_FLY
    global RUN_ALL_TASKS_LOCALLY
    global BYPASS_CALL_TO_INVOKE_REAL_LAMBDA
    global STORE_FANINS_FANINNBS_LOCALLY
    global CREATE_ALL_FANINS_FANINNBS_ON_START
    global USING_WORKERS
    global USING_THREADS_NOT_PROCESSES
    global NUM_WORKERS
    global USE_MULTITHREADED_MULTIPROCESSING
    global NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING
    global FANIN_TYPE
    global FANINNB_TYPE
    global PROCESS_WORK_QUEUE_TYPE
    
    global STORE_SYNC_OBJECTS_IN_LAMBDAS
    global USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS
    global SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS
    global USING_DAG_ORCHESTRATOR
    global MAP_OBJECTS_TO_LAMBDA_FUNCTIONS
    global USE_ANONYMOUS_LAMBDA_FUNCTIONS
    global USE_SINGLE_LAMBDA_FUNCTION

    global PAGERANK_GRAPH_FILE_NAME
    global COMPUTE_PAGERANK
    global NUMBER_OF_PAGERANK_ITERATIONS_FOR_PARTITIONS_GROUPS_WITH_LOOPS 
    global NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG
    global CHECK_PAGERANK_OUTPUT
    global SAME_OUTPUT_FOR_ALL_FANOUT_FANIN
    global USE_INCREMENTAL_DAG_GENERATION
    global INCREMENTAL_DAG_DEPOSIT_INTERVAL
    global ENABLE_RUNTIME_TASK_CLUSTERING
    global MIN_PARTITION_GROUP_SIZE_FOR_CLUSTERING
    global MAX_SIZE_OF_OUTPUT_TO_FANOUT_TASK
    global WORK_QUEUE_SIZE_FOR_INCREMENTAL_DAG_GENERATION_WITH_WORKER_PROCESSES
    global TASKS_USE_RESULT_DICTIONARY_PARAMETER
    global USE_SHARED_PARTITIONS_GROUPS
    global USE_PAGERANK_GROUPS_PARTITIONS
    global USE_STRUCT_OF_ARRAYS_FOR_PAGERANK
    global USE_MUTLITHREADED_NONINCREMENTAL_BFS
    global INPUT_ALL_GROUPS_PARTITIONS_AT_START

    RUN_ALL_TASKS_LOCALLY = True
    BYPASS_CALL_TO_INVOKE_REAL_LAMBDA = (not RUN_ALL_TASKS_LOCALLY) and True 
    STORE_FANINS_FANINNBS_LOCALLY = True 
    CREATE_ALL_FANINS_FANINNBS_ON_START = True
    USING_WORKERS = True
    USING_THREADS_NOT_PROCESSES = True
    NUM_WORKERS = 2
    USE_MULTITHREADED_MULTIPROCESSING = False
    NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING = 1

    FANIN_TYPE = "DAG_executor_FanIn"
    FANINNB_TYPE = "DAG_executor_FanInNB"
    PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer"
    #FANIN_TYPE = "DAG_executor_FanIn_Select"
    #FANINNB_TYPE = "DAG_executor_FanInNB_Select"
    #PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer_Select"

    STORE_SYNC_OBJECTS_IN_LAMBDAS = False
    USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS = False
    SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS = False
    USING_DAG_ORCHESTRATOR = False
    MAP_OBJECTS_TO_LAMBDA_FUNCTIONS = False
    USE_ANONYMOUS_LAMBDA_FUNCTIONS = False
    USE_SINGLE_LAMBDA_FUNCTION = False

    PAGERANK_GRAPH_FILE_NAME = "graph_3N_3CC"
    COMPUTE_PAGERANK = True
    CHECK_PAGERANK_OUTPUT = COMPUTE_PAGERANK and RUN_ALL_TASKS_LOCALLY and (USING_WORKERS or not USING_WORKERS) and USING_THREADS_NOT_PROCESSES and True
    NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG = "PR1_1"
    NUMBER_OF_PAGERANK_ITERATIONS_FOR_PARTITIONS_GROUPS_WITH_LOOPS = 10
    SAME_OUTPUT_FOR_ALL_FANOUT_FANIN = not COMPUTE_PAGERANK
    USE_INCREMENTAL_DAG_GENERATION = COMPUTE_PAGERANK and False
    INCREMENTAL_DAG_DEPOSIT_INTERVAL = 1
    ENABLE_RUNTIME_TASK_CLUSTERING = COMPUTE_PAGERANK and False
    MIN_PARTITION_GROUP_SIZE_FOR_CLUSTERING  = 5
    MAX_SIZE_OF_OUTPUT_TO_FANOUT_TASK = 10000
    WORK_QUEUE_SIZE_FOR_INCREMENTAL_DAG_GENERATION_WITH_WORKER_PROCESSES =  2**10-1
    TASKS_USE_RESULT_DICTIONARY_PARAMETER = COMPUTE_PAGERANK and True
    USE_SHARED_PARTITIONS_GROUPS = COMPUTE_PAGERANK and False
    USE_PAGERANK_GROUPS_PARTITIONS = COMPUTE_PAGERANK and True
    USE_STRUCT_OF_ARRAYS_FOR_PAGERANK = COMPUTE_PAGERANK and False
    USE_MUTLITHREADED_NONINCREMENTAL_BFS = COMPUTE_PAGERANK and False
    INPUT_ALL_GROUPS_PARTITIONS_AT_START = COMPUTE_PAGERANK and False

#Test54: worker threads (A2) with non-selective-wait Sync-objects, 
#        2 worker threads, Sync-objects stored locally
#        use pagerank groups
#        test graph with 3 partitions. 1 of P1 has one parent 2,
#        4 (child of 1) of P2 has one parent 3, 6 (child of 4) of P3 has one parent 5
def test54():
    print("test54")
    global EXIT_PROGRAM_ON_EXCEPTION
    global DEALLOCATE_BFS_MAIN_MAP_ON_THE_FLY
    global RUN_ALL_TASKS_LOCALLY
    global BYPASS_CALL_TO_INVOKE_REAL_LAMBDA
    global STORE_FANINS_FANINNBS_LOCALLY
    global CREATE_ALL_FANINS_FANINNBS_ON_START
    global USING_WORKERS
    global USING_THREADS_NOT_PROCESSES
    global NUM_WORKERS
    global USE_MULTITHREADED_MULTIPROCESSING
    global NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING
    global FANIN_TYPE
    global FANINNB_TYPE
    global PROCESS_WORK_QUEUE_TYPE
    
    global STORE_SYNC_OBJECTS_IN_LAMBDAS
    global USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS
    global SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS
    global USING_DAG_ORCHESTRATOR
    global MAP_OBJECTS_TO_LAMBDA_FUNCTIONS
    global USE_ANONYMOUS_LAMBDA_FUNCTIONS
    global USE_SINGLE_LAMBDA_FUNCTION

    global PAGERANK_GRAPH_FILE_NAME
    global COMPUTE_PAGERANK
    global NUMBER_OF_PAGERANK_ITERATIONS_FOR_PARTITIONS_GROUPS_WITH_LOOPS 
    global NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG
    global CHECK_PAGERANK_OUTPUT
    global SAME_OUTPUT_FOR_ALL_FANOUT_FANIN
    global USE_INCREMENTAL_DAG_GENERATION
    global INCREMENTAL_DAG_DEPOSIT_INTERVAL
    global ENABLE_RUNTIME_TASK_CLUSTERING
    global MIN_PARTITION_GROUP_SIZE_FOR_CLUSTERING
    global MAX_SIZE_OF_OUTPUT_TO_FANOUT_TASK
    global WORK_QUEUE_SIZE_FOR_INCREMENTAL_DAG_GENERATION_WITH_WORKER_PROCESSES
    global TASKS_USE_RESULT_DICTIONARY_PARAMETER
    global USE_SHARED_PARTITIONS_GROUPS
    global USE_PAGERANK_GROUPS_PARTITIONS
    global USE_STRUCT_OF_ARRAYS_FOR_PAGERANK
    global USE_MUTLITHREADED_NONINCREMENTAL_BFS
    global INPUT_ALL_GROUPS_PARTITIONS_AT_START

    RUN_ALL_TASKS_LOCALLY = True
    BYPASS_CALL_TO_INVOKE_REAL_LAMBDA = (not RUN_ALL_TASKS_LOCALLY) and True 
    STORE_FANINS_FANINNBS_LOCALLY = True 
    CREATE_ALL_FANINS_FANINNBS_ON_START = True
    USING_WORKERS = True
    USING_THREADS_NOT_PROCESSES = True
    NUM_WORKERS = 2
    USE_MULTITHREADED_MULTIPROCESSING = False
    NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING = 1

    FANIN_TYPE = "DAG_executor_FanIn"
    FANINNB_TYPE = "DAG_executor_FanInNB"
    PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer"
    #FANIN_TYPE = "DAG_executor_FanIn_Select"
    #FANINNB_TYPE = "DAG_executor_FanInNB_Select"
    #PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer_Select"

    STORE_SYNC_OBJECTS_IN_LAMBDAS = False
    USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS = False
    SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS = False
    USING_DAG_ORCHESTRATOR = False
    MAP_OBJECTS_TO_LAMBDA_FUNCTIONS = False
    USE_ANONYMOUS_LAMBDA_FUNCTIONS = False
    USE_SINGLE_LAMBDA_FUNCTION = False

    PAGERANK_GRAPH_FILE_NAME = "graph_3P"
    COMPUTE_PAGERANK = True
    CHECK_PAGERANK_OUTPUT = COMPUTE_PAGERANK and RUN_ALL_TASKS_LOCALLY and (USING_WORKERS or not USING_WORKERS) and USING_THREADS_NOT_PROCESSES and True
    NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG = "PR1_1"
    NUMBER_OF_PAGERANK_ITERATIONS_FOR_PARTITIONS_GROUPS_WITH_LOOPS = 10
    SAME_OUTPUT_FOR_ALL_FANOUT_FANIN = not COMPUTE_PAGERANK
    USE_INCREMENTAL_DAG_GENERATION = COMPUTE_PAGERANK and False
    INCREMENTAL_DAG_DEPOSIT_INTERVAL = 1
    ENABLE_RUNTIME_TASK_CLUSTERING = COMPUTE_PAGERANK and False
    MIN_PARTITION_GROUP_SIZE_FOR_CLUSTERING  = 5
    MAX_SIZE_OF_OUTPUT_TO_FANOUT_TASK = 10000
    WORK_QUEUE_SIZE_FOR_INCREMENTAL_DAG_GENERATION_WITH_WORKER_PROCESSES =  2**10-1
    TASKS_USE_RESULT_DICTIONARY_PARAMETER = COMPUTE_PAGERANK and True
    USE_SHARED_PARTITIONS_GROUPS = COMPUTE_PAGERANK and False
    USE_PAGERANK_GROUPS_PARTITIONS = COMPUTE_PAGERANK and True
    USE_STRUCT_OF_ARRAYS_FOR_PAGERANK = COMPUTE_PAGERANK and False
    USE_MUTLITHREADED_NONINCREMENTAL_BFS = COMPUTE_PAGERANK and False
    INPUT_ALL_GROUPS_PARTITIONS_AT_START = COMPUTE_PAGERANK and False

#Test55: worker threads (A2) with non-selective-wait Sync-objects, 
#        2 worker threads, Sync-objects stored locally
#        use pagerank groups
#        test graph of 3 nodes 2 edges where one node has 2 parents
#        but one parent in different group one in same group so 1 shadow node
def test55():
    print("test55")
    global EXIT_PROGRAM_ON_EXCEPTION
    global DEALLOCATE_BFS_MAIN_MAP_ON_THE_FLY
    global RUN_ALL_TASKS_LOCALLY
    global BYPASS_CALL_TO_INVOKE_REAL_LAMBDA
    global STORE_FANINS_FANINNBS_LOCALLY
    global CREATE_ALL_FANINS_FANINNBS_ON_START
    global USING_WORKERS
    global USING_THREADS_NOT_PROCESSES
    global NUM_WORKERS
    global USE_MULTITHREADED_MULTIPROCESSING
    global NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING
    global FANIN_TYPE
    global FANINNB_TYPE
    global PROCESS_WORK_QUEUE_TYPE
    
    global STORE_SYNC_OBJECTS_IN_LAMBDAS
    global USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS
    global SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS
    global USING_DAG_ORCHESTRATOR
    global MAP_OBJECTS_TO_LAMBDA_FUNCTIONS
    global USE_ANONYMOUS_LAMBDA_FUNCTIONS
    global USE_SINGLE_LAMBDA_FUNCTION

    global PAGERANK_GRAPH_FILE_NAME
    global COMPUTE_PAGERANK
    global NUMBER_OF_PAGERANK_ITERATIONS_FOR_PARTITIONS_GROUPS_WITH_LOOPS 
    global NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG
    global CHECK_PAGERANK_OUTPUT
    global SAME_OUTPUT_FOR_ALL_FANOUT_FANIN
    global USE_INCREMENTAL_DAG_GENERATION
    global INCREMENTAL_DAG_DEPOSIT_INTERVAL
    global ENABLE_RUNTIME_TASK_CLUSTERING
    global MIN_PARTITION_GROUP_SIZE_FOR_CLUSTERING
    global MAX_SIZE_OF_OUTPUT_TO_FANOUT_TASK
    global WORK_QUEUE_SIZE_FOR_INCREMENTAL_DAG_GENERATION_WITH_WORKER_PROCESSES
    global TASKS_USE_RESULT_DICTIONARY_PARAMETER
    global USE_SHARED_PARTITIONS_GROUPS
    global USE_PAGERANK_GROUPS_PARTITIONS
    global USE_STRUCT_OF_ARRAYS_FOR_PAGERANK
    global USE_MUTLITHREADED_NONINCREMENTAL_BFS
    global INPUT_ALL_GROUPS_PARTITIONS_AT_START

    RUN_ALL_TASKS_LOCALLY = True
    BYPASS_CALL_TO_INVOKE_REAL_LAMBDA = (not RUN_ALL_TASKS_LOCALLY) and True 
    STORE_FANINS_FANINNBS_LOCALLY = True 
    CREATE_ALL_FANINS_FANINNBS_ON_START = True
    USING_WORKERS = True
    USING_THREADS_NOT_PROCESSES = True
    NUM_WORKERS = 2
    USE_MULTITHREADED_MULTIPROCESSING = False
    NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING = 1

    FANIN_TYPE = "DAG_executor_FanIn"
    FANINNB_TYPE = "DAG_executor_FanInNB"
    PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer"
    #FANIN_TYPE = "DAG_executor_FanIn_Select"
    #FANINNB_TYPE = "DAG_executor_FanInNB_Select"
    #PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer_Select"

    STORE_SYNC_OBJECTS_IN_LAMBDAS = False
    USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS = False
    SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS = False
    USING_DAG_ORCHESTRATOR = False
    MAP_OBJECTS_TO_LAMBDA_FUNCTIONS = False
    USE_ANONYMOUS_LAMBDA_FUNCTIONS = False
    USE_SINGLE_LAMBDA_FUNCTION = False

    PAGERANK_GRAPH_FILE_NAME = "graph_1N_has_2Parents"
    COMPUTE_PAGERANK = True
    CHECK_PAGERANK_OUTPUT = COMPUTE_PAGERANK and RUN_ALL_TASKS_LOCALLY and (USING_WORKERS or not USING_WORKERS) and USING_THREADS_NOT_PROCESSES and True
    NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG = "PR1_1"
    NUMBER_OF_PAGERANK_ITERATIONS_FOR_PARTITIONS_GROUPS_WITH_LOOPS = 10
    SAME_OUTPUT_FOR_ALL_FANOUT_FANIN = not COMPUTE_PAGERANK
    USE_INCREMENTAL_DAG_GENERATION = COMPUTE_PAGERANK and False
    INCREMENTAL_DAG_DEPOSIT_INTERVAL = 1
    ENABLE_RUNTIME_TASK_CLUSTERING = COMPUTE_PAGERANK and False
    MIN_PARTITION_GROUP_SIZE_FOR_CLUSTERING  = 5
    MAX_SIZE_OF_OUTPUT_TO_FANOUT_TASK = 10000
    WORK_QUEUE_SIZE_FOR_INCREMENTAL_DAG_GENERATION_WITH_WORKER_PROCESSES =  2**10-1
    TASKS_USE_RESULT_DICTIONARY_PARAMETER = COMPUTE_PAGERANK and True
    USE_SHARED_PARTITIONS_GROUPS = COMPUTE_PAGERANK and False
    USE_PAGERANK_GROUPS_PARTITIONS = COMPUTE_PAGERANK and True
    USE_STRUCT_OF_ARRAYS_FOR_PAGERANK = COMPUTE_PAGERANK and False
    USE_MUTLITHREADED_NONINCREMENTAL_BFS = COMPUTE_PAGERANK and False
    INPUT_ALL_GROUPS_PARTITIONS_AT_START = COMPUTE_PAGERANK and False

#Test56: worker threads (A2) with non-selective-wait Sync-objects, 
#        2 worker threads, Sync-objects stored locally
#        use pagerank groups
#        test graph of 3 nodes 3 edges where one node has 2 parents
#        with both parents in different group so 2 shadow nodes.
#        Group one is 2-->1 and group 2 is 3 where 3 is child of 2 and 1
def test56():
    print("test56")
    global EXIT_PROGRAM_ON_EXCEPTION
    global DEALLOCATE_BFS_MAIN_MAP_ON_THE_FLY
    global RUN_ALL_TASKS_LOCALLY
    global BYPASS_CALL_TO_INVOKE_REAL_LAMBDA
    global STORE_FANINS_FANINNBS_LOCALLY
    global CREATE_ALL_FANINS_FANINNBS_ON_START
    global USING_WORKERS
    global USING_THREADS_NOT_PROCESSES
    global NUM_WORKERS
    global USE_MULTITHREADED_MULTIPROCESSING
    global NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING
    global FANIN_TYPE
    global FANINNB_TYPE
    global PROCESS_WORK_QUEUE_TYPE
    
    global STORE_SYNC_OBJECTS_IN_LAMBDAS
    global USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS
    global SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS
    global USING_DAG_ORCHESTRATOR
    global MAP_OBJECTS_TO_LAMBDA_FUNCTIONS
    global USE_ANONYMOUS_LAMBDA_FUNCTIONS
    global USE_SINGLE_LAMBDA_FUNCTION

    global PAGERANK_GRAPH_FILE_NAME
    global COMPUTE_PAGERANK
    global NUMBER_OF_PAGERANK_ITERATIONS_FOR_PARTITIONS_GROUPS_WITH_LOOPS 
    global NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG
    global CHECK_PAGERANK_OUTPUT
    global SAME_OUTPUT_FOR_ALL_FANOUT_FANIN
    global USE_INCREMENTAL_DAG_GENERATION
    global INCREMENTAL_DAG_DEPOSIT_INTERVAL
    global ENABLE_RUNTIME_TASK_CLUSTERING
    global MIN_PARTITION_GROUP_SIZE_FOR_CLUSTERING
    global MAX_SIZE_OF_OUTPUT_TO_FANOUT_TASK
    global WORK_QUEUE_SIZE_FOR_INCREMENTAL_DAG_GENERATION_WITH_WORKER_PROCESSES
    global TASKS_USE_RESULT_DICTIONARY_PARAMETER
    global USE_SHARED_PARTITIONS_GROUPS
    global USE_PAGERANK_GROUPS_PARTITIONS
    global USE_STRUCT_OF_ARRAYS_FOR_PAGERANK
    global USE_MUTLITHREADED_NONINCREMENTAL_BFS
    global INPUT_ALL_GROUPS_PARTITIONS_AT_START

    RUN_ALL_TASKS_LOCALLY = True 
    STORE_FANINS_FANINNBS_LOCALLY = True 
    CREATE_ALL_FANINS_FANINNBS_ON_START = True
    USING_WORKERS = True
    USING_THREADS_NOT_PROCESSES = True
    NUM_WORKERS = 2
    USE_MULTITHREADED_MULTIPROCESSING = False
    NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING = 1

    FANIN_TYPE = "DAG_executor_FanIn"
    FANINNB_TYPE = "DAG_executor_FanInNB"
    PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer"
    #FANIN_TYPE = "DAG_executor_FanIn_Select"
    #FANINNB_TYPE = "DAG_executor_FanInNB_Select"
    #PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer_Select"

    STORE_SYNC_OBJECTS_IN_LAMBDAS = False
    USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS = False
    SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS = False
    USING_DAG_ORCHESTRATOR = False
    MAP_OBJECTS_TO_LAMBDA_FUNCTIONS = False
    USE_ANONYMOUS_LAMBDA_FUNCTIONS = False
    USE_SINGLE_LAMBDA_FUNCTION = False

    PAGERANK_GRAPH_FILE_NAME = "graph_1N_has_2Parents_2shadownodes"
    COMPUTE_PAGERANK = True
    CHECK_PAGERANK_OUTPUT = COMPUTE_PAGERANK and RUN_ALL_TASKS_LOCALLY and (USING_WORKERS or not USING_WORKERS) and USING_THREADS_NOT_PROCESSES and True
    NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG = "PR1_1"
    NUMBER_OF_PAGERANK_ITERATIONS_FOR_PARTITIONS_GROUPS_WITH_LOOPS = 10
    SAME_OUTPUT_FOR_ALL_FANOUT_FANIN = not COMPUTE_PAGERANK
    USE_INCREMENTAL_DAG_GENERATION = COMPUTE_PAGERANK and False
    INCREMENTAL_DAG_DEPOSIT_INTERVAL = 1
    ENABLE_RUNTIME_TASK_CLUSTERING = COMPUTE_PAGERANK and False
    MIN_PARTITION_GROUP_SIZE_FOR_CLUSTERING  = 5
    MAX_SIZE_OF_OUTPUT_TO_FANOUT_TASK = 10000
    WORK_QUEUE_SIZE_FOR_INCREMENTAL_DAG_GENERATION_WITH_WORKER_PROCESSES =  2**10-1
    TASKS_USE_RESULT_DICTIONARY_PARAMETER = COMPUTE_PAGERANK and True
    USE_SHARED_PARTITIONS_GROUPS = COMPUTE_PAGERANK and False
    USE_PAGERANK_GROUPS_PARTITIONS = COMPUTE_PAGERANK and True
    USE_STRUCT_OF_ARRAYS_FOR_PAGERANK = COMPUTE_PAGERANK and False
    USE_MUTLITHREADED_NONINCREMENTAL_BFS = COMPUTE_PAGERANK and False
    INPUT_ALL_GROUPS_PARTITIONS_AT_START = COMPUTE_PAGERANK and False

"""
#PageRank tests w/ runtime clustering
#Non-incremental
#running: python -m wukongdnc.dag.BFS using PAGERANK_GRAPH_FILE_NAME = "graph_24N_3CC_fanin"
"""

#Test57: worker threads (A2) with non-selective-wait Sync-objects, 
#        2 worker threads, Sync-objects stored locally
#        create objects at the start
#        use pagerank groups
#        use runtime clustering
def test57():
    print("test57")
    global EXIT_PROGRAM_ON_EXCEPTION
    global DEALLOCATE_BFS_MAIN_MAP_ON_THE_FLY
    global RUN_ALL_TASKS_LOCALLY
    global BYPASS_CALL_TO_INVOKE_REAL_LAMBDA
    global STORE_FANINS_FANINNBS_LOCALLY
    global CREATE_ALL_FANINS_FANINNBS_ON_START
    global USING_WORKERS
    global USING_THREADS_NOT_PROCESSES
    global NUM_WORKERS
    global USE_MULTITHREADED_MULTIPROCESSING
    global NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING
    global FANIN_TYPE
    global FANINNB_TYPE
    global PROCESS_WORK_QUEUE_TYPE
    
    global STORE_SYNC_OBJECTS_IN_LAMBDAS
    global USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS
    global SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS
    global USING_DAG_ORCHESTRATOR
    global MAP_OBJECTS_TO_LAMBDA_FUNCTIONS
    global USE_ANONYMOUS_LAMBDA_FUNCTIONS
    global USE_SINGLE_LAMBDA_FUNCTION

    global PAGERANK_GRAPH_FILE_NAME
    global COMPUTE_PAGERANK
    global NUMBER_OF_PAGERANK_ITERATIONS_FOR_PARTITIONS_GROUPS_WITH_LOOPS 
    global NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG
    global CHECK_PAGERANK_OUTPUT
    global SAME_OUTPUT_FOR_ALL_FANOUT_FANIN
    global USE_INCREMENTAL_DAG_GENERATION
    global INCREMENTAL_DAG_DEPOSIT_INTERVAL
    global ENABLE_RUNTIME_TASK_CLUSTERING
    global MIN_PARTITION_GROUP_SIZE_FOR_CLUSTERING
    global MAX_SIZE_OF_OUTPUT_TO_FANOUT_TASK
    global WORK_QUEUE_SIZE_FOR_INCREMENTAL_DAG_GENERATION_WITH_WORKER_PROCESSES
    global TASKS_USE_RESULT_DICTIONARY_PARAMETER
    global USE_SHARED_PARTITIONS_GROUPS
    global USE_PAGERANK_GROUPS_PARTITIONS
    global USE_STRUCT_OF_ARRAYS_FOR_PAGERANK
    global USE_MUTLITHREADED_NONINCREMENTAL_BFS
    global INPUT_ALL_GROUPS_PARTITIONS_AT_START

    RUN_ALL_TASKS_LOCALLY = True
    BYPASS_CALL_TO_INVOKE_REAL_LAMBDA = (not RUN_ALL_TASKS_LOCALLY) and True 
    STORE_FANINS_FANINNBS_LOCALLY = True 
    CREATE_ALL_FANINS_FANINNBS_ON_START = True
    USING_WORKERS = True
    USING_THREADS_NOT_PROCESSES = True
    NUM_WORKERS = 2
    USE_MULTITHREADED_MULTIPROCESSING = False
    NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING = 1

    FANIN_TYPE = "DAG_executor_FanIn"
    FANINNB_TYPE = "DAG_executor_FanInNB"
    PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer"
    #FANIN_TYPE = "DAG_executor_FanIn_Select"
    #FANINNB_TYPE = "DAG_executor_FanInNB_Select"
    #PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer_Select"

    STORE_SYNC_OBJECTS_IN_LAMBDAS = False
    USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS = False
    SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS = False
    USING_DAG_ORCHESTRATOR = False
    MAP_OBJECTS_TO_LAMBDA_FUNCTIONS = False
    USE_ANONYMOUS_LAMBDA_FUNCTIONS = False
    USE_SINGLE_LAMBDA_FUNCTION = False

    PAGERANK_GRAPH_FILE_NAME = "graph_24N_3CC_fanin"
    COMPUTE_PAGERANK = True
    CHECK_PAGERANK_OUTPUT = COMPUTE_PAGERANK and RUN_ALL_TASKS_LOCALLY and (USING_WORKERS or not USING_WORKERS) and USING_THREADS_NOT_PROCESSES and True
    NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG = "PR1_1"
    NUMBER_OF_PAGERANK_ITERATIONS_FOR_PARTITIONS_GROUPS_WITH_LOOPS = 10
    SAME_OUTPUT_FOR_ALL_FANOUT_FANIN = not COMPUTE_PAGERANK
    USE_INCREMENTAL_DAG_GENERATION = COMPUTE_PAGERANK and False
    INCREMENTAL_DAG_DEPOSIT_INTERVAL = 2
    ENABLE_RUNTIME_TASK_CLUSTERING = COMPUTE_PAGERANK and True
    MIN_PARTITION_GROUP_SIZE_FOR_CLUSTERING  = 5
    MAX_SIZE_OF_OUTPUT_TO_FANOUT_TASK = 10000
    WORK_QUEUE_SIZE_FOR_INCREMENTAL_DAG_GENERATION_WITH_WORKER_PROCESSES =  2**10-1
    TASKS_USE_RESULT_DICTIONARY_PARAMETER = COMPUTE_PAGERANK and True
    USE_SHARED_PARTITIONS_GROUPS = COMPUTE_PAGERANK and False
    USE_PAGERANK_GROUPS_PARTITIONS = COMPUTE_PAGERANK and True
    USE_STRUCT_OF_ARRAYS_FOR_PAGERANK = COMPUTE_PAGERANK and False
    USE_MUTLITHREADED_NONINCREMENTAL_BFS = COMPUTE_PAGERANK and False
    INPUT_ALL_GROUPS_PARTITIONS_AT_START = COMPUTE_PAGERANK and False

#Test58: worker processes (A2) with non-selective-wait Sync-objects, 
#        2 worker processes, Sync-objects stored remotely
#        create objects at the start
#        use pagerank groups
#        use runtime clustering
def test58():
    print("test58")
    global EXIT_PROGRAM_ON_EXCEPTION
    global DEALLOCATE_BFS_MAIN_MAP_ON_THE_FLY
    global RUN_ALL_TASKS_LOCALLY
    global BYPASS_CALL_TO_INVOKE_REAL_LAMBDA
    global STORE_FANINS_FANINNBS_LOCALLY
    global CREATE_ALL_FANINS_FANINNBS_ON_START
    global USING_WORKERS
    global USING_THREADS_NOT_PROCESSES
    global NUM_WORKERS
    global USE_MULTITHREADED_MULTIPROCESSING
    global NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING
    global FANIN_TYPE
    global FANINNB_TYPE
    global PROCESS_WORK_QUEUE_TYPE
    
    global STORE_SYNC_OBJECTS_IN_LAMBDAS
    global USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS
    global SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS
    global USING_DAG_ORCHESTRATOR
    global MAP_OBJECTS_TO_LAMBDA_FUNCTIONS
    global USE_ANONYMOUS_LAMBDA_FUNCTIONS
    global USE_SINGLE_LAMBDA_FUNCTION

    global PAGERANK_GRAPH_FILE_NAME
    global COMPUTE_PAGERANK
    global NUMBER_OF_PAGERANK_ITERATIONS_FOR_PARTITIONS_GROUPS_WITH_LOOPS 
    global NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG
    global CHECK_PAGERANK_OUTPUT
    global SAME_OUTPUT_FOR_ALL_FANOUT_FANIN
    global USE_INCREMENTAL_DAG_GENERATION
    global INCREMENTAL_DAG_DEPOSIT_INTERVAL
    global ENABLE_RUNTIME_TASK_CLUSTERING
    global MIN_PARTITION_GROUP_SIZE_FOR_CLUSTERING
    global MAX_SIZE_OF_OUTPUT_TO_FANOUT_TASK
    global WORK_QUEUE_SIZE_FOR_INCREMENTAL_DAG_GENERATION_WITH_WORKER_PROCESSES
    global TASKS_USE_RESULT_DICTIONARY_PARAMETER
    global USE_SHARED_PARTITIONS_GROUPS
    global USE_PAGERANK_GROUPS_PARTITIONS
    global USE_STRUCT_OF_ARRAYS_FOR_PAGERANK
    global USE_MUTLITHREADED_NONINCREMENTAL_BFS
    global INPUT_ALL_GROUPS_PARTITIONS_AT_START

    RUN_ALL_TASKS_LOCALLY = True
    BYPASS_CALL_TO_INVOKE_REAL_LAMBDA = (not RUN_ALL_TASKS_LOCALLY) and True 
    STORE_FANINS_FANINNBS_LOCALLY = False 
    CREATE_ALL_FANINS_FANINNBS_ON_START = True
    USING_WORKERS = True
    USING_THREADS_NOT_PROCESSES = False
    NUM_WORKERS = 2
    USE_MULTITHREADED_MULTIPROCESSING = False
    NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING = 1

    FANIN_TYPE = "DAG_executor_FanIn"
    FANINNB_TYPE = "DAG_executor_FanInNB"
    PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer"
    #FANIN_TYPE = "DAG_executor_FanIn_Select"
    #FANINNB_TYPE = "DAG_executor_FanInNB_Select"
    #PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer_Select"

    STORE_SYNC_OBJECTS_IN_LAMBDAS = False
    USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS = False
    SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS = False
    USING_DAG_ORCHESTRATOR = False
    MAP_OBJECTS_TO_LAMBDA_FUNCTIONS = False
    USE_ANONYMOUS_LAMBDA_FUNCTIONS = False
    USE_SINGLE_LAMBDA_FUNCTION = False

    PAGERANK_GRAPH_FILE_NAME = "graph_24N_3CC_fanin"
    COMPUTE_PAGERANK = True
    CHECK_PAGERANK_OUTPUT = COMPUTE_PAGERANK and RUN_ALL_TASKS_LOCALLY and (USING_WORKERS or not USING_WORKERS) and USING_THREADS_NOT_PROCESSES and True
    NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG = "PR1_1"
    NUMBER_OF_PAGERANK_ITERATIONS_FOR_PARTITIONS_GROUPS_WITH_LOOPS = 10
    SAME_OUTPUT_FOR_ALL_FANOUT_FANIN = not COMPUTE_PAGERANK
    USE_INCREMENTAL_DAG_GENERATION = COMPUTE_PAGERANK and False
    INCREMENTAL_DAG_DEPOSIT_INTERVAL = 2
    ENABLE_RUNTIME_TASK_CLUSTERING = COMPUTE_PAGERANK and True
    MIN_PARTITION_GROUP_SIZE_FOR_CLUSTERING  = 5
    MAX_SIZE_OF_OUTPUT_TO_FANOUT_TASK = 10000
    WORK_QUEUE_SIZE_FOR_INCREMENTAL_DAG_GENERATION_WITH_WORKER_PROCESSES =  2**10-1
    TASKS_USE_RESULT_DICTIONARY_PARAMETER = COMPUTE_PAGERANK and True
    USE_SHARED_PARTITIONS_GROUPS = COMPUTE_PAGERANK and False
    USE_PAGERANK_GROUPS_PARTITIONS = COMPUTE_PAGERANK and True
    USE_STRUCT_OF_ARRAYS_FOR_PAGERANK = COMPUTE_PAGERANK and False
    USE_MUTLITHREADED_NONINCREMENTAL_BFS = COMPUTE_PAGERANK and False
    INPUT_ALL_GROUPS_PARTITIONS_AT_START = COMPUTE_PAGERANK and False

#Test59: simulated lambdas (A2) with non-selective-wait Sync-objects, 
#        Sync-objects stored locally
#        create objects at the start
#        use pagerank groups
#        use runtime clustering
def test59():
    print("test59")
    global EXIT_PROGRAM_ON_EXCEPTION
    global DEALLOCATE_BFS_MAIN_MAP_ON_THE_FLY
    global RUN_ALL_TASKS_LOCALLY
    global BYPASS_CALL_TO_INVOKE_REAL_LAMBDA
    global STORE_FANINS_FANINNBS_LOCALLY
    global CREATE_ALL_FANINS_FANINNBS_ON_START
    global USING_WORKERS
    global USING_THREADS_NOT_PROCESSES
    global NUM_WORKERS
    global USE_MULTITHREADED_MULTIPROCESSING
    global NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING
    global FANIN_TYPE
    global FANINNB_TYPE
    global PROCESS_WORK_QUEUE_TYPE
    
    global STORE_SYNC_OBJECTS_IN_LAMBDAS
    global USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS
    global SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS
    global USING_DAG_ORCHESTRATOR
    global MAP_OBJECTS_TO_LAMBDA_FUNCTIONS
    global USE_ANONYMOUS_LAMBDA_FUNCTIONS
    global USE_SINGLE_LAMBDA_FUNCTION

    global PAGERANK_GRAPH_FILE_NAME
    global COMPUTE_PAGERANK
    global NUMBER_OF_PAGERANK_ITERATIONS_FOR_PARTITIONS_GROUPS_WITH_LOOPS 
    global NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG
    global CHECK_PAGERANK_OUTPUT
    global SAME_OUTPUT_FOR_ALL_FANOUT_FANIN
    global USE_INCREMENTAL_DAG_GENERATION
    global INCREMENTAL_DAG_DEPOSIT_INTERVAL
    global ENABLE_RUNTIME_TASK_CLUSTERING
    global MIN_PARTITION_GROUP_SIZE_FOR_CLUSTERING
    global MAX_SIZE_OF_OUTPUT_TO_FANOUT_TASK
    global WORK_QUEUE_SIZE_FOR_INCREMENTAL_DAG_GENERATION_WITH_WORKER_PROCESSES
    global TASKS_USE_RESULT_DICTIONARY_PARAMETER
    global USE_SHARED_PARTITIONS_GROUPS
    global USE_PAGERANK_GROUPS_PARTITIONS
    global USE_STRUCT_OF_ARRAYS_FOR_PAGERANK
    global USE_MUTLITHREADED_NONINCREMENTAL_BFS
    global INPUT_ALL_GROUPS_PARTITIONS_AT_START

    RUN_ALL_TASKS_LOCALLY = True
    BYPASS_CALL_TO_INVOKE_REAL_LAMBDA = (not RUN_ALL_TASKS_LOCALLY) and True 
    STORE_FANINS_FANINNBS_LOCALLY = True 
    CREATE_ALL_FANINS_FANINNBS_ON_START = True
    USING_WORKERS = False
    USING_THREADS_NOT_PROCESSES = True
    NUM_WORKERS = 2
    USE_MULTITHREADED_MULTIPROCESSING = False
    NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING = 1

    FANIN_TYPE = "DAG_executor_FanIn"
    FANINNB_TYPE = "DAG_executor_FanInNB"
    PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer"
    #FANIN_TYPE = "DAG_executor_FanIn_Select"
    #FANINNB_TYPE = "DAG_executor_FanInNB_Select"
    #PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer_Select"

    STORE_SYNC_OBJECTS_IN_LAMBDAS = False
    USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS = False
    SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS = False
    USING_DAG_ORCHESTRATOR = False
    MAP_OBJECTS_TO_LAMBDA_FUNCTIONS = False
    USE_ANONYMOUS_LAMBDA_FUNCTIONS = False
    USE_SINGLE_LAMBDA_FUNCTION = False

    PAGERANK_GRAPH_FILE_NAME = "graph_24N_3CC_fanin"
    COMPUTE_PAGERANK = True
    CHECK_PAGERANK_OUTPUT = COMPUTE_PAGERANK and RUN_ALL_TASKS_LOCALLY and (USING_WORKERS or not USING_WORKERS) and USING_THREADS_NOT_PROCESSES and True
    NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG = "PR1_1"
    NUMBER_OF_PAGERANK_ITERATIONS_FOR_PARTITIONS_GROUPS_WITH_LOOPS = 10
    SAME_OUTPUT_FOR_ALL_FANOUT_FANIN = not COMPUTE_PAGERANK
    USE_INCREMENTAL_DAG_GENERATION = COMPUTE_PAGERANK and False
    INCREMENTAL_DAG_DEPOSIT_INTERVAL = 2
    ENABLE_RUNTIME_TASK_CLUSTERING = COMPUTE_PAGERANK and True
    MIN_PARTITION_GROUP_SIZE_FOR_CLUSTERING  = 5
    MAX_SIZE_OF_OUTPUT_TO_FANOUT_TASK = 10000
    WORK_QUEUE_SIZE_FOR_INCREMENTAL_DAG_GENERATION_WITH_WORKER_PROCESSES =  2**10-1
    TASKS_USE_RESULT_DICTIONARY_PARAMETER = COMPUTE_PAGERANK and True
    USE_SHARED_PARTITIONS_GROUPS = COMPUTE_PAGERANK and False
    USE_PAGERANK_GROUPS_PARTITIONS = COMPUTE_PAGERANK and True
    USE_STRUCT_OF_ARRAYS_FOR_PAGERANK = COMPUTE_PAGERANK and False
    USE_MUTLITHREADED_NONINCREMENTAL_BFS = COMPUTE_PAGERANK and False
    INPUT_ALL_GROUPS_PARTITIONS_AT_START = COMPUTE_PAGERANK and False

#Test60: real lambdas (A2),  bpass call to platform invoke 
#        non-selective-wait Sync-objects, Sync-objects stored remotely
#        create objects at the start
#        use pagerank groups
#        use runtime clustering
def test60():
    print("test60")
    global EXIT_PROGRAM_ON_EXCEPTION
    global DEALLOCATE_BFS_MAIN_MAP_ON_THE_FLY
    global RUN_ALL_TASKS_LOCALLY
    global BYPASS_CALL_TO_INVOKE_REAL_LAMBDA
    global STORE_FANINS_FANINNBS_LOCALLY
    global CREATE_ALL_FANINS_FANINNBS_ON_START
    global USING_WORKERS
    global USING_THREADS_NOT_PROCESSES
    global NUM_WORKERS
    global USE_MULTITHREADED_MULTIPROCESSING
    global NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING
    global FANIN_TYPE
    global FANINNB_TYPE
    global PROCESS_WORK_QUEUE_TYPE
    
    global STORE_SYNC_OBJECTS_IN_LAMBDAS
    global USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS
    global SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS
    global USING_DAG_ORCHESTRATOR
    global MAP_OBJECTS_TO_LAMBDA_FUNCTIONS
    global USE_ANONYMOUS_LAMBDA_FUNCTIONS
    global USE_SINGLE_LAMBDA_FUNCTION

    global PAGERANK_GRAPH_FILE_NAME
    global COMPUTE_PAGERANK
    global NUMBER_OF_PAGERANK_ITERATIONS_FOR_PARTITIONS_GROUPS_WITH_LOOPS 
    global NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG
    global CHECK_PAGERANK_OUTPUT
    global SAME_OUTPUT_FOR_ALL_FANOUT_FANIN
    global USE_INCREMENTAL_DAG_GENERATION
    global INCREMENTAL_DAG_DEPOSIT_INTERVAL
    global ENABLE_RUNTIME_TASK_CLUSTERING
    global MIN_PARTITION_GROUP_SIZE_FOR_CLUSTERING
    global MAX_SIZE_OF_OUTPUT_TO_FANOUT_TASK
    global WORK_QUEUE_SIZE_FOR_INCREMENTAL_DAG_GENERATION_WITH_WORKER_PROCESSES
    global TASKS_USE_RESULT_DICTIONARY_PARAMETER
    global USE_SHARED_PARTITIONS_GROUPS
    global USE_PAGERANK_GROUPS_PARTITIONS

    global USE_STRUCT_OF_ARRAYS_FOR_PAGERANK
    global USE_MUTLITHREADED_NONINCREMENTAL_BFS
    global INPUT_ALL_GROUPS_PARTITIONS_AT_START

    RUN_ALL_TASKS_LOCALLY = False
    BYPASS_CALL_TO_INVOKE_REAL_LAMBDA = (not RUN_ALL_TASKS_LOCALLY) and True 
    STORE_FANINS_FANINNBS_LOCALLY = False 
    CREATE_ALL_FANINS_FANINNBS_ON_START = True
    USING_WORKERS = False
    USING_THREADS_NOT_PROCESSES = True
    NUM_WORKERS = 2
    USE_MULTITHREADED_MULTIPROCESSING = False
    NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING = 1

    FANIN_TYPE = "DAG_executor_FanIn"
    FANINNB_TYPE = "DAG_executor_FanInNB"
    PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer"
    #FANIN_TYPE = "DAG_executor_FanIn_Select"
    #FANINNB_TYPE = "DAG_executor_FanInNB_Select"
    #PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer_Select"

    STORE_SYNC_OBJECTS_IN_LAMBDAS = False
    USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS = False
    SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS = False
    USING_DAG_ORCHESTRATOR = False
    MAP_OBJECTS_TO_LAMBDA_FUNCTIONS = False
    USE_ANONYMOUS_LAMBDA_FUNCTIONS = False
    USE_SINGLE_LAMBDA_FUNCTION = False

    PAGERANK_GRAPH_FILE_NAME = "graph_24N_3CC_fanin"
    COMPUTE_PAGERANK = True
    CHECK_PAGERANK_OUTPUT = COMPUTE_PAGERANK and RUN_ALL_TASKS_LOCALLY and (USING_WORKERS or not USING_WORKERS) and USING_THREADS_NOT_PROCESSES and True
    NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG = "PR1_1"
    NUMBER_OF_PAGERANK_ITERATIONS_FOR_PARTITIONS_GROUPS_WITH_LOOPS = 10
    SAME_OUTPUT_FOR_ALL_FANOUT_FANIN = not COMPUTE_PAGERANK
    USE_INCREMENTAL_DAG_GENERATION = COMPUTE_PAGERANK and False
    INCREMENTAL_DAG_DEPOSIT_INTERVAL = 2
    ENABLE_RUNTIME_TASK_CLUSTERING = COMPUTE_PAGERANK and True
    MIN_PARTITION_GROUP_SIZE_FOR_CLUSTERING  = 5
    MAX_SIZE_OF_OUTPUT_TO_FANOUT_TASK = 10000
    WORK_QUEUE_SIZE_FOR_INCREMENTAL_DAG_GENERATION_WITH_WORKER_PROCESSES =  2**10-1
    TASKS_USE_RESULT_DICTIONARY_PARAMETER = COMPUTE_PAGERANK and True
    USE_SHARED_PARTITIONS_GROUPS = COMPUTE_PAGERANK and False
    USE_PAGERANK_GROUPS_PARTITIONS = COMPUTE_PAGERANK and True
    USE_STRUCT_OF_ARRAYS_FOR_PAGERANK = COMPUTE_PAGERANK and False
    USE_MUTLITHREADED_NONINCREMENTAL_BFS = COMPUTE_PAGERANK and False
    INPUT_ALL_GROUPS_PARTITIONS_AT_START = COMPUTE_PAGERANK and False

#Test61: real lambdas (A2),  bpass call to platform invoke 
#        non-selective-wait Sync-objects, Sync-objects stored remotely
#        create objects at the start
#        use pagerank groups
#        use runtime clustering
#        input all groups/partitions at start
def test61():
    print("test61")
    global EXIT_PROGRAM_ON_EXCEPTION
    global DEALLOCATE_BFS_MAIN_MAP_ON_THE_FLY
    global RUN_ALL_TASKS_LOCALLY
    global BYPASS_CALL_TO_INVOKE_REAL_LAMBDA
    global STORE_FANINS_FANINNBS_LOCALLY
    global CREATE_ALL_FANINS_FANINNBS_ON_START
    global USING_WORKERS
    global USING_THREADS_NOT_PROCESSES
    global NUM_WORKERS
    global USE_MULTITHREADED_MULTIPROCESSING
    global NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING
    global FANIN_TYPE
    global FANINNB_TYPE
    global PROCESS_WORK_QUEUE_TYPE
    
    global STORE_SYNC_OBJECTS_IN_LAMBDAS
    global USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS
    global SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS
    global USING_DAG_ORCHESTRATOR
    global MAP_OBJECTS_TO_LAMBDA_FUNCTIONS
    global USE_ANONYMOUS_LAMBDA_FUNCTIONS
    global USE_SINGLE_LAMBDA_FUNCTION

    global PAGERANK_GRAPH_FILE_NAME
    global COMPUTE_PAGERANK
    global NUMBER_OF_PAGERANK_ITERATIONS_FOR_PARTITIONS_GROUPS_WITH_LOOPS 
    global NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG
    global CHECK_PAGERANK_OUTPUT
    global SAME_OUTPUT_FOR_ALL_FANOUT_FANIN
    global USE_INCREMENTAL_DAG_GENERATION
    global INCREMENTAL_DAG_DEPOSIT_INTERVAL
    global ENABLE_RUNTIME_TASK_CLUSTERING
    global MIN_PARTITION_GROUP_SIZE_FOR_CLUSTERING
    global MAX_SIZE_OF_OUTPUT_TO_FANOUT_TASK
    global WORK_QUEUE_SIZE_FOR_INCREMENTAL_DAG_GENERATION_WITH_WORKER_PROCESSES
    global TASKS_USE_RESULT_DICTIONARY_PARAMETER
    global USE_SHARED_PARTITIONS_GROUPS
    global USE_PAGERANK_GROUPS_PARTITIONS
    global USE_STRUCT_OF_ARRAYS_FOR_PAGERANK
    global USE_MUTLITHREADED_NONINCREMENTAL_BFS
    global INPUT_ALL_GROUPS_PARTITIONS_AT_START

    RUN_ALL_TASKS_LOCALLY = False
    BYPASS_CALL_TO_INVOKE_REAL_LAMBDA = (not RUN_ALL_TASKS_LOCALLY) and True 
    STORE_FANINS_FANINNBS_LOCALLY = False 
    CREATE_ALL_FANINS_FANINNBS_ON_START = True
    USING_WORKERS = False
    USING_THREADS_NOT_PROCESSES = True
    NUM_WORKERS = 2
    USE_MULTITHREADED_MULTIPROCESSING = False
    NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING = 1

    FANIN_TYPE = "DAG_executor_FanIn"
    FANINNB_TYPE = "DAG_executor_FanInNB"
    PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer"
    #FANIN_TYPE = "DAG_executor_FanIn_Select"
    #FANINNB_TYPE = "DAG_executor_FanInNB_Select"
    #PROCESS_WORK_QUEUE_TYPE = "BoundedBuffer_Select"

    STORE_SYNC_OBJECTS_IN_LAMBDAS = False
    USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS = False
    SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS = False
    USING_DAG_ORCHESTRATOR = False
    MAP_OBJECTS_TO_LAMBDA_FUNCTIONS = False
    USE_ANONYMOUS_LAMBDA_FUNCTIONS = False
    USE_SINGLE_LAMBDA_FUNCTION = False

    PAGERANK_GRAPH_FILE_NAME = "graph_24N_3CC_fanin"
    COMPUTE_PAGERANK = True
    CHECK_PAGERANK_OUTPUT = COMPUTE_PAGERANK and RUN_ALL_TASKS_LOCALLY and (USING_WORKERS or not USING_WORKERS) and USING_THREADS_NOT_PROCESSES and True
    NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG = "PR1_1"
    NUMBER_OF_PAGERANK_ITERATIONS_FOR_PARTITIONS_GROUPS_WITH_LOOPS = 10
    SAME_OUTPUT_FOR_ALL_FANOUT_FANIN = not COMPUTE_PAGERANK
    USE_INCREMENTAL_DAG_GENERATION = COMPUTE_PAGERANK and False
    INCREMENTAL_DAG_DEPOSIT_INTERVAL = 2
    ENABLE_RUNTIME_TASK_CLUSTERING = COMPUTE_PAGERANK and True
    MIN_PARTITION_GROUP_SIZE_FOR_CLUSTERING  = 5
    MAX_SIZE_OF_OUTPUT_TO_FANOUT_TASK = 10000
    WORK_QUEUE_SIZE_FOR_INCREMENTAL_DAG_GENERATION_WITH_WORKER_PROCESSES =  2**10-1
    TASKS_USE_RESULT_DICTIONARY_PARAMETER = COMPUTE_PAGERANK and True
    USE_SHARED_PARTITIONS_GROUPS = COMPUTE_PAGERANK and False
    USE_PAGERANK_GROUPS_PARTITIONS = COMPUTE_PAGERANK and True
    USE_STRUCT_OF_ARRAYS_FOR_PAGERANK = COMPUTE_PAGERANK and False
    USE_MUTLITHREADED_NONINCREMENTAL_BFS = COMPUTE_PAGERANK and False
    INPUT_ALL_GROUPS_PARTITIONS_AT_START = COMPUTE_PAGERANK and True

"""
ToDo: test49: mutlithtreaded DAG USE_INCREMENTAL_DAG_GENERATION

ToDo: So no logger stuff if using processes?
if not using threads:
  logger stuff
so no double output in OS boxes. puts debug lines in mplog file but not DOS box; DOS box has the prints only.
If do logger stuff then get double print to DOS box of ebug stuff, not sure why

#####
Test real lambda code with BYPASS_CALL_TO_INVOKE_REAL_LAMBDA = not RUN_ALL_TASKS_LOCALLY and True
######
Test PR in Lambdas?
######
"""

def check_asserts():
    try:
        msg = "[Error]: Configuration error: if USING_WORKERS then must RUN_ALL_TASKS_LOCALLY."
        assert not (USING_WORKERS and not RUN_ALL_TASKS_LOCALLY), msg
    except AssertionError:
        logger.exception("[Error]: assertion failed")
        if EXIT_PROGRAM_ON_EXCEPTION:
            logging.shutdown()
            os._exit(0)
    #assertOld:
    #if USING_WORKERS and not RUN_ALL_TASKS_LOCALLY:
    #    logger.error("[Error]: Configuration error: if USING_WORKERS then must RUN_ALL_TASKS_LOCALLY.")
    #    logging.shutdown()
    #    os._exit(0)     


    """
    try:
        msg = "[Error]: Configuration error: if BYPASS_CALL_TO_INVOKE_REAL_LAMBDA then must be running real Lambdas" + " i.e., not RUN_ALL_TASKS_LOCALLY."
        assert not (BYPASS_CALL_TO_INVOKE_REAL_LAMBDA and RUN_ALL_TASKS_LOCALLY), msg
    except AssertionError:
        logger.exception("[Error]: assertion failed")
        if EXIT_PROGRAM_ON_EXCEPTION:
            logging.shutdown()
            os._exit(0)
    #assertOld:
    #if BYPASS_CALL_TO_INVOKE_REAL_LAMBDA and RUN_ALL_TASKS_LOCALLY:
    #    logger.error("[Error]: Configuration error: if BYPASS_CALL_TO_INVOKE_REAL_LAMBDA then must be running real Lambdas"
    #        + " i.e., not RUN_ALL_TASKS_LOCALLY.")
    #    logging.shutdown()
    #    os._exit(0)  
    """

    try:
        msg = "[Error]: Configuration error: if USING_WORKERS and not USING_THREADS_NOT_PROCESSES" + " then STORE_FANINS_FANINNBS_LOCALLY must be False."
        assert not (USING_WORKERS and (not USING_THREADS_NOT_PROCESSES) and STORE_FANINS_FANINNBS_LOCALLY), msg
    except AssertionError:
        logger.exception("[Error]: assertion failed")
        if EXIT_PROGRAM_ON_EXCEPTION:
            logging.shutdown()
            os._exit(0)
    #assertOld:
    #if USING_WORKERS and not USING_THREADS_NOT_PROCESSES:
    #    if STORE_FANINS_FANINNBS_LOCALLY:
    #        # When using worker processed, synch objects must be stored remoely
    #        logger.error("[Error]: Configuration error: if USING_WORKERS and not USING_THREADS_NOT_PROCESSES"
    #            + " then STORE_FANINS_FANINNBS_LOCALLY must be False.")
    #        logging.shutdown()
    #        os._exit(0)

    try:
        msg = "[Error]: Configuration error: if CREATE_ALL_FANINS_FANINNBS_ON_START" + " then map_objects_to_functions must be True."
        # if create sync objects on start and executing tasks in lambdas "
        # then we must map them to function so that we can determine the 
        # function an object is in.
        assert not (CREATE_ALL_FANINS_FANINNBS_ON_START and not RUN_ALL_TASKS_LOCALLY and STORE_SYNC_OBJECTS_IN_LAMBDAS and not MAP_OBJECTS_TO_LAMBDA_FUNCTIONS), msg
    except AssertionError:
        logger.exception("[Error]: assertion failed")
        if EXIT_PROGRAM_ON_EXCEPTION:
            logging.shutdown()
            os._exit(0)
    #assertOld:
    #if CREATE_ALL_FANINS_FANINNBS_ON_START and not RUN_ALL_TASKS_LOCALLY and STORE_SYNC_OBJECTS_IN_LAMBDAS:
    #    if not MAP_OBJECTS_TO_LAMBDA_FUNCTIONS:
    #        # if create sync objects on start and executing tasks in lambdas "
    #        # then we must map them to function so that we can determine the 
    #        # function an object is in.
    #        logger.error("[Error]: Configuration error: if CREATE_ALL_FANINS_FANINNBS_ON_START"
    #            + " then map_objects_to_functions must be True.")
    #        logging.shutdown()
    #        os._exit(0)

    try:
        msg = "[Error]: Configuration error: if MAP_OBJECTS_TO_LAMBDA_FUNCTIONS" + " then USE_ANONYMOUS_LAMBDA_FUNCTIONS must be False."
        # if create sync objects on start then we must map them to function so
        # that we can determine the function an object is in.
        assert not (MAP_OBJECTS_TO_LAMBDA_FUNCTIONS and USE_ANONYMOUS_LAMBDA_FUNCTIONS), msg
    except AssertionError:
        logger.exception("[Error]: assertion failed")
        if EXIT_PROGRAM_ON_EXCEPTION:
            logging.shutdown()
            os._exit(0)
    #assertOld:
    #if MAP_OBJECTS_TO_LAMBDA_FUNCTIONS:
    #    if USE_ANONYMOUS_LAMBDA_FUNCTIONS:
    #        # if create sync objects on start then we must map them to function so
    #        # that we can determine the function an object is in.
    #        logger.error("[Error]: Configuration error: if MAP_OBJECTS_TO_LAMBDA_FUNCTIONS"
    #            + " then USE_ANONYMOUS_LAMBDA_FUNCTIONS must be False.")
    #        logging.shutdown()
    #        os._exit(0)

    try:
        msg = "[Error]: Configuration error: if SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS" + " then not RUN_ALL_TASKS_LOCALLY must be True."
        # if create sync objects on start then we must map them to function so
        # that we can determine the function an object is in.
        assert not (SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS and RUN_ALL_TASKS_LOCALLY), msg
    except AssertionError:
        logger.exception("[Error]: assertion failed")
        if EXIT_PROGRAM_ON_EXCEPTION:
            logging.shutdown()
            os._exit(0)
    #assertOld:
    #if SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS:
    #    if RUN_ALL_TASKS_LOCALLY:
    #        # if create sync objects on start then we must map them to function so
    #        # that we can determine the function an object is in.
    #        logger.error("[Error]: Configuration error: if SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS"
    #            + " then not RUN_ALL_TASKS_LOCALLY must be True.")
    #        logging.shutdown()
    #        os._exit(0)

    try:
        msg = "[Error]: Configuration error: incremental_DAG_generation" + " requires not CREATE_ALL_FANINS_FANINNBS_ON_START" \
            + " i.e., create synch objects on the fly since we don't know all of the synch objects " + " at the start (the DAG is not complete)"
        assert not (COMPUTE_PAGERANK and USE_INCREMENTAL_DAG_GENERATION and CREATE_ALL_FANINS_FANINNBS_ON_START), msg
    except AssertionError:
        logger.exception("[Error]: assertion failed")
        if EXIT_PROGRAM_ON_EXCEPTION:
            logging.shutdown()
            os._exit(0)
    # assertOld:
    #if COMPUTE_PAGERANK and USE_INCREMENTAL_DAG_GENERATION and CREATE_ALL_FANINS_FANINNBS_ON_START:
    #    logger.error("[Error]: Configuration error: incremental_DAG_generation"
    #        + " requires not CREATE_ALL_FANINS_FANINNBS_ON_START"
    #        + " i.e., create synch objects on the fly since we don't know all of the synch objects "
    #        + " at the start (the DAG is not complete)")
    #    logging.shutdown()
    #    os._exit(0) 

    try:
        msg = "[Error]: Configuration error: INCREMENTAL_DAG_DEPOSIT_INTERVAL" + " must be >= 1. We mod by INCREMENTAL_DAG_DEPOSIT_INTERVAL so it" \
            + " cannot be 0 and using a negative number makes no sense."
        assert not (INCREMENTAL_DAG_DEPOSIT_INTERVAL < 1), msg
    except AssertionError:
        logger.exception("[Error]: assertion failed")
        if EXIT_PROGRAM_ON_EXCEPTION:
            logging.shutdown()
            os._exit(0)
    #assertOld:
    #if INCREMENTAL_DAG_DEPOSIT_INTERVAL < 1:
    #    logger.error("[Error]: Configuration error: INCREMENTAL_DAG_DEPOSIT_INTERVAL"
    #        + " must be >= 1. We mod by INCREMENTAL_DAG_DEPOSIT_INTERVAL so it"
    #        + " cannot be 0 and using a negative number makes no sense.")
    #    logging.shutdown()
    #    os._exit(0) 

    try:
        msg = "[Error]: Configuration error: if SAME_OUTPUT_FOR_ALL_FANOUT_FANIN" + " then must be computing pagerank."
        assert not (not SAME_OUTPUT_FOR_ALL_FANOUT_FANIN and not COMPUTE_PAGERANK), msg
    except AssertionError:
        logger.exception("[Error]: assertion failed")
        if EXIT_PROGRAM_ON_EXCEPTION:
            logging.shutdown()
            os._exit(0)
    #assertOld:
    #if not SAME_OUTPUT_FOR_ALL_FANOUT_FANIN and not COMPUTE_PAGERANK:
    #    logger.error("[Error]: Configuration error: if SAME_OUTPUT_FOR_ALL_FANOUT_FANIN"
    #        + " then must be computing pagerank.")
    #    logging.shutdown()
    #    os._exit(0)

    try:
        msg = "[Error]: Configuration error: if using a single shared array of" + " partitions or groups then must run_tasks_locally and be USING_THREADS_NOT_PROCESSES."
        assert not (COMPUTE_PAGERANK and (USE_STRUCT_OF_ARRAYS_FOR_PAGERANK and not USE_SHARED_PARTITIONS_GROUPS)), msg
    except AssertionError:
        logger.exception("[Error]: assertion failed")
        if EXIT_PROGRAM_ON_EXCEPTION:
            logging.shutdown()
            os._exit(0)
    #assertOld:
    #if COMPUTE_PAGERANK and (USE_STRUCT_OF_ARRAYS_FOR_PAGERANK and not USE_SHARED_PARTITIONS_GROUPS):
    #    logger.error("[Error]: Configuration error: if USE_STRUCT_OF_ARRAYS_FOR_PAGERANK"
    #        + " then must USE_SHARED_PARTITIONS_GROUPS.")
    #    logging.shutdown()
    #    os._exit(0)

    try:
        msg = "[Error]: Configuration error: if USE_STRUCT_OF_ARRAYS_FOR_PAGERANK" + " then must USE_SHARED_PARTITIONS_GROUPS."
        assert not (COMPUTE_PAGERANK and (USE_STRUCT_OF_ARRAYS_FOR_PAGERANK and not USE_SHARED_PARTITIONS_GROUPS)), msg
    except AssertionError:
        logger.exception("[Error]: assertion failed")
        if EXIT_PROGRAM_ON_EXCEPTION:
            logging.shutdown()
            os._exit(0)
    #assertOld:
    #if COMPUTE_PAGERANK and (USE_STRUCT_OF_ARRAYS_FOR_PAGERANK and not USE_SHARED_PARTITIONS_GROUPS):
    #    logger.error("[Error]: Configuration error: if USE_STRUCT_OF_ARRAYS_FOR_PAGERANK"
    #        + " then must USE_SHARED_PARTITIONS_GROUPS.")
    #    logging.shutdown()
    #    os._exit(0)

# Global variable accessed in DAG_executor_constants. If test_number
# is 0 and we are testing a worker process configuration then 
# the worker process executing DAG_executor must read the test_number_file
# written by TestAll and call DAG_executor_constants.set_test_number(test_number)
# so that all worker processes use the test configuration. DAG_executor
# reads this global variable: "if DAG_executor_constants.test_number == 0:"
test_number = 0
# called by TestAll.py to run testX
# the test number is verified by TestAll to be within range.
def set_test_number_and_run_test(number):
    global test_number
    test_number = number
    #print("number: " + str(number))
    #print("test_number: " + str(test_number))

    # Run Tests

    # Set the configuration constants one time for the tests
    # that do not involve pagerank.
    non_pagerank_non_store_objects_in_lambda_base()

    # call method test<test_number>() to configure test 
    test_method_name = "test"+str(test_number)

    # can also use getattr() - how to specify this DAG_executor_constants module?
    # https://stackoverflow.com/questions/3061/calling-a-function-of-a-module-by-using-its-name-a-string
    try:
        globals()[test_method_name]()
    except Exception:
        print("[Error]: test " + test_method_name + "() not found.")
        logging.shutdown()
        os._exit(0)

    # Check assserts after setting the configuration constants
    check_asserts()


# Note: Running the script below in the Wndows PowerShell X to run the 
# tests one-by-one. This is the command line "Windows Powershell"
# not the "Windows Powershell ISE". The latter gives and error 
# message when runnging python. (See the comment below.)
#
# This script is also in file TestAllPowerShellCommands.txt in
# directory C:\Users\benrc\Desktop\Executor\DAG\WukongDynamic

#cd C:\Users\benrc\Desktop\Executor\DAG\WukongDynamic
#For ($i=1; $i -le 2; $i++) {
#    Write-Host "Test $i"
#    Write-Host "#######"
#    python -m wukongdnc.dag.TestAll($i)
#    Read-Host "Enter any input to continue"
#}

# Copy the script and paste it into PowerShell and hit enter twice.

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
