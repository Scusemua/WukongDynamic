print("DAG_executor_driver")
# pylint: disable=pointless-string-statement
# pylint: disable=logging-not-lazy
# pylint: disable=trailing-whitespace
# pylint: disable=line-too-long
# pylint: disable=invalid-name
# pylint: disable=broad-exception-caught
# pylint: disable=missing-module-docstring
# pylint: disable=missing-class-docstring
# pylint: disable=missing-function-docstring

# break stuck Python is FN + R
# try matrixMult

# Where are we:
# Note in DAG orchestrator, line 392 we tabbed this stuff
#  right so it's part of no-trigger else. this is becuase
#  of the return when we do trigger, so this code is only
#  executed when no trigger. It was correct but misleading.
# test all
# - Lock the get and set in message handler lambda createif? Note:
#   for fanouts there is only one synch op performed on
#   them so no races. but no current way to identify fanout objects 
#   since they are same type as FnInNB. Create a create-lock per
#   object using DAG_info? Yes, but not always processing DAGs.
#   Note: we can map but not create on start. We will map object "foo"
#   to some function (and lock) and just create the "Foo" object on
#   the fly, which seems reasonable. Could check for map then grab
#   lock if mapped.
# - Note: just because we map objects to functions doesn't mean we need 
#   a lock for each function. If we cal function once then we don't
#   need a lock. True for fanout/fanins/faninNBs when using D_O that 
#   enqueues all ops until the last one. Which brings up:
# - D_O can invoke function (with lock) as ops are performed to overlap
#   passing results with waiting for last operation. Do this for big
#   results? The function can open socket and after getting result can 
#   ask for moer results - if none available then stop.
#
# - integrate the non-simulated lambda stuff with the mapping and
#   anonynous stuff. Still use InfiniD? with "DAG_executor_i"
#   Set function_map directly
# - Docs
#
# DOC: 
#   When we aer using the tcp_server so tht objects are stored on the 
#   server and we are creating objects on the fly, before we do an 
#   operation on an object, we check to see whether the object has
#   been created. The methods that do ops are syncronize_(a)sync
#   and process_fanis_batch. Both of these message check whether the
#   object exists before doing their op, and if the object dos not 
#   exis the object is created.
"""
        if not CREATE_ALL_FANINS_FANINNBS_ON_START:
            # create the work_queue used by workers (when using worker pools
            # to execute the DAG instad of lambdas. When the workers are processes
            # the work queue is on the server so all the worker processes can access it.)
            with create_work_queue_lock:
                synchronizer = tcp_server.synchronizers.get(work_queue_name,None)
                if (synchronizer is None):
                    dummy_state = DAG_executor_State(function_name = "DAG_executor", function_instance_ID = str(uuid.uuid4()))
                    dummy_state.keyword_arguments['n'] = 2*number_of_tasks
                    msg_id = str(uuid.uuid4())	# for debugging
                    creation_message = {
                        "op": "create",
                        "type": PROCESS_WORK_QUEUE_TYPE,
                        "name": "process_work_queue",
                        "state": make_json_serializable(dummy_state),	
                        "id": msg_id
                    } 
                    # not created yet so create object
                    logger.trace("tcp_server: synchronize_process_faninNBs_batch: "
                        + "create sync object process_work_queue on the fly.")
                    self.create_obj_but_no_ack_to_client(creation_message)
                else:
                    logger.trace("tcp_server: synchronize_process_faninNBs_batch: object " + 
                        "process_work_queue already created.")
"""
#   Note that checking whether the object exists and then creting an object
#   if it does not must be atomic, so it is locked.
#   When we are not creating objects on start and we are using 
#   tcp_server_lambda because objects are stored in lambdas and/or 
#   lambdas execute tasks, we are either using th DAG_orchestrator (D_O)
#   or not.
#   - When we use D_O the D_O calls process_enqueued_messages to process the 
#   fanin operations when all of them have been received, enqueue creates 
#   a state with the fanin ops for process_enqueued_messages and creates
#   a create_message that gets passed as part of the control message
#   for process_enqueued_messages. The control message is what the 
#   message_hanlder_lambda looks in order to call the right method to
#   process the message, in this case process_enqueued_messages.
#   process_enqueued_messages will see that we are creating objects
#   on the fly instead of at the start and use the create_message
#   to gather the information that is passed to create() the object.
#   - If we are not using D_O then fanin ops are not enqueued, 
#   instead we will call invoke_lambda_synchronously to execute the
#   fanin operations as they are called. In this case, as the parameter
#   to invoke_lambda_synchronously, we create a control message which 
#   is a tuple [create_message, message] and call createif_and_synchronize_sync.
#   That is, since we are creating objects on the fly, we call this method
#   instead of the regular synchronize_sync. The latter assumes the object
#   was created at the start.  createif_and_synchronize_sync uses both messages, 
#   first the crete_message to create the object then the regular synchronize_sync
#   fanin messag to do the synch op on object. The places we invoke 
#   invoke_lambda_synchronously are the places that can do a synchronize_sync
#   operation, which are process_leaf_tasks_batch, process_faninNBs_batch, 
#   and synchronize (a)sync. All these place create a control_message with the
#   messages tuple, and invoke lambda synchronously to call 
#   createif_and_synchronize_sync in the message_handler_lambda.
#   Recall: When using tp_server_lambda, there is a mesage handler in
#   tcp_server on the server that receievs client messages and routes them 
#   to the method that processes them. This method will pass the message or
#   create a control message as just described and invoke a lambda, passing
#   the message as a parameter. In the lambda, the message_handler_lambda
#   will route the message to the method that handles it, just like we do 
#   on the server.
#   Note: When using tcp_server, which means we are not storing objects 
#   in lambdas, we are storing ojects on the server, the tcp_server
#   will receive a message and route it to a method of the message_handler.
#   In this case the message_handler is on the server.
#   Note: So when we move to storing objects in lambas, we still have a tcp
#   server to route messages to the message_hanlder but the message_handler
#   is inside the lambda and the mesage_handler will route the message
#   it gets rom the tcp_server_lambda to the method that handles the object.
#   The code for tp_server and tcp_server_lambda is very similar, the 
#   big difference is where the message_handler runs (in a lambda or on
#   the server.)

#    def synchronize_sync(self, message = None):
#
#        if not CREATE_ALL_FANINS_FANINNBS_ON_START:
#            ...
#            creation_message = {
#                "op": "create",
#                "type": FANIN_TYPE,
#                "name": task_name,
#                "state": make_json_serializable(dummy_state_for_create_message),	
#                "id": msg_id
#            }
#            ...
#            # message is the original synchronize_sync message
#            messages = (creation_message, message)
#            ...
#            control_message = {
#                "op": "createif_and_synchronize_sync",
#                "type": "DAG_executor_fanin_or_fanout",
#                "name": messages,   
#                "state": make_json_serializable(dummy_state_for_control_message),	
#                "id": msg_id
#            }
#
#        if USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS and USING_DAG_ORCHESTRATOR:
#            # The enqueue_and_invoke_lambda_synchronously will generte the creae message
#            logger.trace("*********************tcp_server_lambda: synchronize_sync: " + calling_task_name + ": calling infiniD.enqueue(message).")
#            returned_state = self.enqueue_and_invoke_lambda_synchronously(message)
#            logger.trace("*********************tcp_server_lambda: synchronize_sync: " + calling_task_name + ": called infiniD.enqueue(message) "
#               + "returned_state: " + str(returned_state))
#        else:
#            logger.trace("*********************tcp_server_lambda: synchronize_sync: " + calling_task_name + ": calling invoke_lambda_synchronously.")
#            if CREATE_ALL_FANINS_FANINNBS_ON_START:
#                # call synchronize_sync on the already created object
#                returned_state = self.invoke_lambda_synchronously(message)
#            else:
#               # call createif_and_synchronize_sync to create object
#                # and call synchronize_sync on it. the control_message
#                # has the creation_message and the message for snchronize_sync
#                # in a messages tuple value under its 'name' key.
#                returned_state = self.invoke_lambda_synchronously(control_message)
#
#            logger.trace("*********************tcp_server_lambda: synchronize_sync: " + calling_task_name + ": called invoke_lambda_synchronously "
#               + "returned_state: " + str(returned_state))

#
# To Do: parallel invoke of leaf tasks?
#
# To Do: Does tcpServer fannNBs_batch really need to call execute()
# instead of calling dirextly, like we do for local objects?
#
# Short-circuit call to fan_in op in lambda? Use local Fanins?
# The fanin object can be on server since DAG_orchestrator is essentially
# acting like this object anyway and fanin dos not trigger tasks so why bother putting
# it on the server? Also, fanin just returns it's results, i.e., does not trigger a 
# fanin task to run in the same lambda ss fanin. But then fanin are not in lambdas.
#  
# Consider: 
#
# - In work loop, need condition for process faninNBs batch
#   and for async. Not using run_locally now, so need "not run_locally" for process faninNBs batch
#   but when not "run locally" may or may not be SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS
#   but async_call is true in either case? i.e., nothing comes back to lamba. 
# - Check other conditions involving SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS. This includes the
#   process_faninNBS_batch in tcp_server_lambda, which needs "not run locally" for triggering.
#   Note: LOOKS LIKE GETTING QUOTIENT since although the faninNBS could not start tasks, the regular 
#   run_locally code started a new thread for running the task?
#   - Driver changes: 1. need to create fanins for fanout objects too when running tasks and
#     not create on fly. separate methods: create_fanin_and_faninNB_and_fanout_messages and
#     create_fanins_and_faninNBs_and_fanouts (so never work queue so use this and 
#     change name on tcp_Server and tcp_server_lambda to create_all_sync_objects.)
#     2. Also, need to start leaf tasks, perhaps by creating a "fanouts" 
#     and adding the fanout tasks to fanouts then call process_faninNB_batch on tcp
#     server_lambda? with inputs, which must be handled as leaf inputs by work_loop.
#     Are leaf tasks special cases for fanout triggers? Is payload of non-leaf fanout
#     same as payload of leaf incocation? (Must be same as far as DAG_executor_lambda
#     is concerned?) where for leaf nodes then using real lambdas:
#
#       for start_state, task_name, inp in zip(DAG_leaf_task_start_states, DAG_leaf_tasks, DAG_leaf_task_inputs):
#
#           payload = {
#               "input": inp,
#               "DAG_executor_state": lambda_DAG_exec_state,
#               "DAG_info": DAG_info
#            }
#      but sync ops for selects will alread have DAG_info since they got them on create?
# To DO:Yes, messages created by driver have DAG_info for all faninNBs and fanouts but
#      fanouts and faninNBs don't use DAG_info unless they are starting a lambda?
#      So could have tcp_server/tp_server_lambda, which read DAG_info, add DAG_info
#      to message only if it is needed, so drver doesn;t have to send DAG_info in 
#      all of the messages to all of the faninNBs/fanouts.
#
#  payload for lambdas when triggering (and when just storing objects) is the 
#  same in the sense that the init)( and fan_in) have the info needed for 
#  triggered task payload, so nothing special for triggered task payloads.
#
# Consider: have RDMA dictionary for DAG_orchestrator to put results and faninNB/fanouts
# to get results.
#
# Q: can we invoke simuated lambda sync or async? It's a real python function
#    so it will be sync unless we create a thread to do the invoke?
#
# what is condition for sync objects trigger tasks? e.g., in process_fanouts
#     if (not RUN_ALL_TASKS_LOCALLY) and STORE_SYNC_OBJECTS_IN_LAMBDAS and SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS:
# so not using workers and not RUN_ALL_TASKS_LOCALLY, which is like Wukong but to flip off
# of Wukong we need SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS, which may be too strong.
#
# No: use batch processing when we are using threads to simulate workers and objects
# are remote on the server, not just remote in lambdas, so that the condiditiom
# for calling batch is to always call it except when using threads to simulate 
# lambdas and storing objects locally. We currently call batch for when threads
# simulate lambdas and objects in lambdas (so using tcp_server_lambda.)
#
# check the local server changes for select/non-select
# check the async_call changes + test with simulated lambdas.
# check process_faninNBs_batch in tcp_servr_lambda make sure it works fo r
# simulated and non-simulated (real) lambda callers which are the only callers
# when objects are stored in lambdas. do assserts on asynch_call if true then rl lambdas. 
#
# How do we know when to stop clock for lamba simulation? We can't join any threads
#
# should invoke be atomic? for ==n check? as well as lock the function calls.
#  or lock the check and the function calls with same fine-grained lock?
#  Since multiple callers for invoke at the same time?
# document stuff
#
# def DAG_executor(payload):	should not be using workers, correct?
#
# matrix mult deposits results to "collector" object at end.
#
# move on the optimizations:
#
# Consider: multiple tcp_servers/work_queues and work stealing? workers generate
# their own work, till they hit a fanin/faninNB and lose. then need need to start 
# new dfs paths(s). Steal it? 
#
# Consider: compute max workers you can keep busy so don;t overprovison and shut down 
# machines as the workers are not needed or the slowdown is worth the cost.
#
# Consider creating an unbounded buufer in which deposit should never block.
# if it blocks then raise "full" exception. Used for DAG execution where the 
# size of the work_queue is blunded by number of tasks so we can create an
# unbounded buffer of size number_of_tasks + delta that should never block.
#
# Consider timestamping so can place fanin/faninNb/fanout objects in lambdas,
#  where a happened before b means a and b can be in same lambda unless a || b.
# Note: with lamba triggers, careful when a and b is same lambda so a can do 
# local fanin to b, but then a has to also enqueue something to sqs so b is
# triggered with other fanin results, but a is in same lamba so make sure lambda
# is terminated before it is triggered. Not a problem with SQS in tcp_server_lambda
# since it can trigger after a's synch call returns? Right, tcp_server_lambda 
# called for all process_faninNBs_batch? That is, any lambdas, either triggered
# by process_faninNB_batch or running do to external fanouts, will call faninNB_batch
# on tcp_server which will enqueue, which can call sycnch, and wait for return and get
# any enqueu as retrn value so that enqueue is after end of synch invocation? yes
#
# Note: In DandC graph, leaf nodes are fanouts not fanins, so leaf nodes are half the
# nodes which means half the nodes are not fanins. Other half are fanins. How many 
# groups? 1/4 - some are big and some are little (as small as one node in a group).
# The single node groups are at the bottom and are exccuted concurrently. Some way to 
# collapse groups? It's like you want nodes at bottom and nodes at top in same group since 
# they may be executed at different times though they are logically concurrent.
#
#
# PageRank: Note: Executing the whiteboard DAG: We used 2 processes to execute the 
# 7 task DAG. Only P1 executed any tasks since the DAG is small and P1 could execute
# several tasks before P2 starts:
# P1 executes PR1_1, which is a leaf task
# P1 fanout PR2_3, PR2_1 and faninNB PR2_2L, 
# so P1 becomes PR2_3, fansout PR2_1 putting PR2_1 work in the work queue and
#    does fanin on faninNB PR2_2L (where fanouts and faninNB are batched. Note
#    that P1 becomes PR2_3 so it did not need any work from batch, i.e., PR2_1
#    work was added to work queue instead of returned to P1. P1 was first caller
#    to faninNB PR2_2L.
# P1 execute PR2_3
# P1 execute PR3_3 since PR3_3 is in the collpase set for PR2_3, i.e., PR3_3
#    has only one input and it's from PR2_3, which has only on output, which is PR3_3
# P1 get work from work queue, which is PR2_1
# P2 starts and calls getwork and blocks
# P1 execute PR2_1 and do faninNB PR2_2L as a batch
# P1 is last to call fanin on PR2_2L and P1 needs work so P1 gets PR2_2L work
# P1 execute PR2_2L
# P1 do fanout PR3_1 and faninNB PR3_2 as batch. P1 is first to call Fanin on PR3_2
#    and P1 becomes PR3_1
# P1 execute PR3_1
# P1 do faninNB PR3_2 (as batch) and is last task so P1 gets PR3_2 work
# P1 execute PR3_2, this is the 7th of 7 tasks so P1 deposit -1 in work queue
# P2 whcih was bocked withdraws -1 and deposits -1 in work queue and returns from work looop
# P1 needs work and withdraws -1 and deposits -1 in work queue and returns from work loop
# (Note: work queue ends with -1 in it)

import threading
import multiprocessing
from multiprocessing import Process #, Manager
import time
import socket
import os
import logging 
import cloudpickle

#from .DAG_executor_constants import LOG_LEVEL
#from .DAG_executor_constants import RUN_ALL_TASKS_LOCALLY, STORE_FANINS_FANINNBS_LOCALLY, USE_MULTITHREADED_MULTIPROCESSING #, NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING
#from .DAG_executor_constants import CREATE_ALL_FANINS_FANINNBS_ON_START, USING_WORKERS
#from .DAG_executor_constants import NUM_WORKERS,USING_THREADS_NOT_PROCESSES
#from .DAG_executor_constants import FANIN_TYPE, FANINNB_TYPE, PROCESS_WORK_QUEUE_TYPE
#from .DAG_executor_constants import STORE_SYNC_OBJECTS_IN_LAMBDAS, SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS
#from .DAG_executor_constants #import USE_SHARED_PARTITIONS_GROUPS,USE_PAGERANK_GROUPS_PARTITIONS
#from .DAG_executor_constants import USE_STRUCT_OF_ARRAYS_FOR_PAGERANK
#from .DAG_executor_constants import COMPUTE_PAGERANK
#from .DAG_executor_constants import INPUT_ALL_GROUPS_PARTITIONS_AT_START
#from .DAG_executor_constants import EXIT_PROGRAM_ON_EXCEPTION
from . import DAG_executor_constants
from .addLoggingLevel import addLoggingLevel

#   How to use: https://stackoverflow.com/questions/2183233/how-to-add-a-custom-loglevel-to-pythons-logging-facility/35804945#35804945
#    >>> addLoggingLevel('TRACE', logging.DEBUG - 5)
#    >>> logging.getLogger(__name__).setLevel("TRACE")
#    >>> logging.getLogger(__name__).trace('that worked')
#    >>> logging.trace('so did this')
 #   >>> logging.TRACE

# If we are computing pageranks then we will run BFS first which will 
# addLoggingLevel(trace) and import DAG_executor_driver,
# so we do not want to addLoggingLevel(trace) here. If we are not
# computing pageranks we will addLoggingLevel(trace) here.
# Note that we start DAG execution either by running BFS or
# DAG_excutor_driver, so one of them will addLoggingLevel(trace).
# No other module executes addLoggingLevel.
#
# If we run TestAll.py then it will addLoggingLevel, so if
# this will be a second addLoggingLevel and it will fail. If we run 
# this DAG_executor.py file, we need to addLoggingLevel. We intend to 
# always use TestAll.
if not DAG_executor_constants.COMPUTE_PAGERANK:
    try:
        addLoggingLevel('TRACE', logging.DEBUG - 5)
        logging.basicConfig(encoding='utf-8',level=DAG_executor_constants.LOG_LEVEL, format='[%(asctime)s][%(module)s][%(processName)s][%(threadName)s]: %(message)s')
        # Added this to suppress the logging message:
        #   credentials - MainProcess - MainThread: Found credentials in shared credentials file: ~/.aws/credentials
        # But it appears that we could see other things like this:
        # https://stackoverflow.com/questions/1661275/disable-boto-logging-without-modifying-the-boto-files
        logging.getLogger('botocore').setLevel(logging.CRITICAL)
    except AttributeError:
        # comment this out
        print("Already did addLoggingLevel in TestAll so skip it here.")

    

##Function to initialize the logger, notice that it takes 2 arguments
#logger_name and logfile
#def setup_logger(logger_name,logfile):
#    mylogger = logging.getLogger(logger_name)
#    mylogger.setLevel(logging.INFO)
    #logger.setLevel("TRACE")
    # create file handler which logs even debug messages
    #fh = logging.FileHandler(logfile)
    #fh.setLevel(logging.INFO)
    # create console handler with a higher log level
#    ch = logging.StreamHandler()
#    ch.setLevel(logging.INFO)
    #ch.setLevel("TRACE")
    # create formatter and add it to the handlers
#    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    #fh.setFormatter(formatter)
#    ch.setFormatter(formatter)
    # add the handlers to the logger
    #logger.addHandler(fh)
#    mylogger.addHandler(ch)
#    return mylogger

#from .addLoggingLevel import addLoggingLevel

#   How to use: https://stackoverflow.com/questions/2183233/how-to-add-a-custom-loglevel-to-pythons-logging-facility/35804945#35804945
#    >>> addLoggingLevel('TRACE', logging.DEBUG - 5)
#    >>> logging.getLogger(__name__).setLevel("TRACE")
#    >>> logging.getLogger(__name__).trace('that worked')
#    >>> logging.trace('so did this')
#    >>> logging.TRACE

## If we are computing pageranks then we will run BFS first which will 
## addLoggingLevel(trace) and import DAG_executor_driver,
## so we do not want to addLoggingLevel(trace) here. If we are not
## computing pageranks we will addLoggingLevel(trace) here.
## Note that we start DAG execution either by running BFS or
## DAG_excutor_driver, so one of them will addLoggingLevel(trace).
## No other module executes addLoggingLevel.
#from .addLoggingLevel import addLoggingLevel
#from .DAG_executor_constants import COMPUTE_PAGERANK
#logger = None
#if not (COMPUTE_PAGERANK):
#    addLoggingLevel('TRACE', logging.DEBUG - 5)

    #setup the logger as below with mylogger being the name of the #logger and myloggerfile.log being the name of the logfile

    #mylogger=setup_logger('mylogger','myloggerfile.log')
    #mylogger.info("My Logger has been initialized")
    #logger=setup_logger('DAG_executor_driver_logger','DAG_executor.log')
    #logger.info("My Logger has been initialized")
#else:
#    logger = logging.getLogger('DAG_executor_driver_logger')

#from .DFS_visit import Node
#from .DFS_visit import state_info
#from DAG_executor_FanInNB import DAG_executor_FanInNB
from . import DAG_executor
#from wukongdnc.server.DAG_executor_FanInNB import DAG_executor_FanInNB
#from wukongdnc.server.DAG_executor_FanIn import DAG_executor_FanIn
from .DAG_executor_State import DAG_executor_State
from .DAG_info import DAG_Info
from wukongdnc.server.util import make_json_serializable

#from .DAG_work_queue_for_threads import thread_work_queue
from .DAG_executor_work_queue_for_threads import work_queue
from .DAG_executor_synchronizer import server
from wukongdnc.wukong.invoker import invoke_lambda_DAG_executor
import uuid
from wukongdnc.server.api import create_all_sync_objects, synchronize_trigger_leaf_tasks, close_all
from .multiprocessing_logging import listener_configurer, listener_process, worker_configurer
from .DAG_executor_countermp import CounterMP
from .DAG_boundedbuffer_work_queue import Work_Queue_Client
from .DAG_executor_create_multithreaded_multiprocessing_processes import create_multithreaded_multiprocessing_processes #, create_and_run_threads_for_multiT_multiP
import copy
from . import BFS_Shared
import dask

from wukongdnc.constants import TCP_SERVER_IP

logger = logging.getLogger(__name__)
#if not (not USING_THREADS_NOT_PROCESSES or USE_MULTITHREADED_MULTIPROCESSING):
    #logger.setLevel(logging.INFO)
    #logger.setLevel("TRACE")

    #logger.setLevel(LOG_LEVEL)
    #formatter = logging.Formatter('[%(asctime)s] [%(module)s] [%(processName)s] [%(threadName)s]: %(message)s')
    #ch = logging.StreamHandler()

    #ch.setLevel(logging.INFO)
    #ch.setLevel("TRACE")

    #ch.setLevel(LOG_LEVEL)
    #ch.setFormatter(formatter)
    #logger.addHandler(ch)

#logger = None


# A note about loggers:
"""
I'm not sure whether this is the cause of your problem, but by default, 
Python's loggers propagate their messages up to logging hierarchy. 
As you probably know, Python loggers are organized in a tree, with the 
root logger at the top and other loggers below it. In logger names, 
a dot (.) introduces a new hierarchy level. So if you do

logger = logging.getLogger('some_module.some_function`)
then you actually have 3 loggers:

The root logger (`logging.getLogger()`)
    A logger at module level (`logging.getLogger('some_module'))
        A logger at function level (`logging.getLogger('some_module.some_function'))
If you emit a log message on a logger and it is not discarded based on the 
loggers minimum level, then the message is passed on to the logger's handlers 
and to its parent logger. See this flowchart for more information
(https://docs.python.org/2/howto/logging.html#logging-flow)

If that parent logger (or any logger higher up in the hierarchy) also has 
handlers, then they are called, too.

A note about AWS loggers:
AWS Lambda uses the standard logging infrastructure; see denialof.services/lambda. 
The formatter they configure has '%Y-%m-%dT%H:%M:%S' set as the datefmt 
parameter, and the converter is set to time.gmtime (rather than time.localtime).

39

AWS Lambda also sets up a handler, on the root logger, and anything written to 
stdout is captured and logged as level INFO. Your log message is thus 
captured twice:

- By the AWS Lambda handler on the root logger (as log messages propagate from 
nested child loggers to the root), and this logger has its own format configured.
- By the AWS Lambda stdout-to-INFO logger.

This is why the messages all start with (asctime) [(levelname)]-(module):(lineno), 
information; the root logger is configured to output messages with that format 
and the information written to stdout is just another %(message) part in that 
output.

"""

#from .BFS_Shared import pagerank_sent_to_processes, previous_sent_to_processes, number_of_children_sent_to_processes
#from .BFS_Shared import number_of_parents_sent_to_processes, starting_indices_of_parents_sent_to_processes
#from .BFS_Shared import parents_sent_to_processes, IDs_sent_to_processes


""" 
The DAG_executor executes a DAG using multiple threads/processes/Lambdas, each excuting a DFS path through
the DAG. The DAG is encoded as a state machine; a DFS path corresponds to a sequence of state transitions.
Fanins and faninNBs are implented using synchronization objects.

Example DAG: Inputs leaf node values 0 and 1, outputs ((0+1)+(1+1) * (3*2) * 2**2) / 72 = 1.0
         1.0
          t
        divide_by_72
          t (72)
        multiply
      t     t   t
 (4) t    (6) t     t (3)
square   triple     add
   t        t       t    t
 (2) t   (2)t    t (2)   t (1)
        inc1             inc0
        t                 t
        1                 0

The state machine encoding is:

Each DAG task is executed in an assigned state:

DAG states: (task name --> state)
increment-ae88130b-bc16-45fb-8479-eb35fae7f83a 1
add-a35dba4d-ff44-4853-a814-a7804da54c11 2 
triple-a554a391-a774-4e71-91f9-a1c8e828a454 3
square-ea198bd4-0fb7-425b-8b83-acdad7d09028 4
multiply-137dd808-d202-4b59-9f38-2cdb5bf0985e 5 
divide-4311366c-ac97-4af9-a04c-1a76a586b2ad 6 
increment-b2c04dbb-da27-4bce-aac2-c01d9d69c52d 7

In a give state, the assigned task is executed, and then either 0, 1, or more faninNB and fanout operations
are excuted or 0 or 1 fanout opeation is executed. If no operations can be executed. (Thus, in a state,
we can execute faninNB and fanout operations, or we can excute fanin operations.)

The DAG_map shows the operations enabled in each state:

DAG_map:
1: task: increment-ae88130b-bc16-45fb-8479-eb35fae7f83a, fanouts:['triple-a554a391-a774-4e71-91f9-a1c8e828a454', 'square-ea198bd4-0fb7-425b-8b83-acdad7d09028'],fanins:[],faninsNB:['add-a35dba4d-ff44-4853-a814-a7804da54c11'],collapse:[]fanin_sizes:[],faninNB_sizes:[2]task_inputs: (1,)
2: task: add-a35dba4d-ff44-4853-a814-a7804da54c11, fanouts:[],fanins:['multiply-137dd808-d202-4b59-9f38-2cdb5bf0985e'],faninsNB:[],collapse:[]fanin_sizes:[3],faninNB_sizes:[]task_inputs: ('increment-ae88130b-bc16-45fb-8479-eb35fae7f83a', 'increment-b2c04dbb-da27-4bce-aac2-c01d9d69c52d')
3: task: triple-a554a391-a774-4e71-91f9-a1c8e828a454, fanouts:[],fanins:['multiply-137dd808-d202-4b59-9f38-2cdb5bf0985e'],faninsNB:[],collapse:[]fanin_sizes:[3],faninNB_sizes:[]task_inputs: ('increment-ae88130b-bc16-45fb-8479-eb35fae7f83a',)
4: task: square-ea198bd4-0fb7-425b-8b83-acdad7d09028, fanouts:[],fanins:['multiply-137dd808-d202-4b59-9f38-2cdb5bf0985e'],faninsNB:[],collapse:[]fanin_sizes:[3],faninNB_sizes:[]task_inputs: ('increment-ae88130b-bc16-45fb-8479-eb35fae7f83a',)
5: task: multiply-137dd808-d202-4b59-9f38-2cdb5bf0985e, fanouts:[],fanins:[],faninsNB:[],collapse:['divide-4311366c-ac97-4af9-a04c-1a76a586b2ad']fanin_sizes:[],faninNB_sizes:[]task_inputs: ('triple-a554a391-a774-4e71-91f9-a1c8e828a454', 'square-ea198bd4-0fb7-425b-8b83-acdad7d09028', 'add-a35dba4d-ff44-4853-a814-a7804da54c11')
6: task: divide-4311366c-ac97-4af9-a04c-1a76a586b2ad, fanouts:[],fanins:[],faninsNB:[],collapse:[]fanin_sizes:[],faninNB_sizes:[]task_inputs: ('multiply-137dd808-d202-4b59-9f38-2cdb5bf0985e',)
7: task: increment-b2c04dbb-da27-4bce-aac2-c01d9d69c52d, fanouts:[],fanins:[],faninsNB:['add-a35dba4d-ff44-4853-a814-a7804da54c11'],collapse:[]fanin_sizes:[],faninNB_sizes:[2]task_inputs: (0,)

In state 1, there are fanout operations for triple and square, and a faninNB operation for add. The faninNB
operatons are performed before fanout operations. Recall that faninNB means "fanin with No Becomes". The 
thread/process/Lambda performing a faninNB will not become the executor of the faninNB task; some other 
thread/process/Lambda will execute the faninNB.

In state 5, multiply has a "collapse" operation, which means the thread/process/Lambda that executed task
multiply will then also execute task divide.

The two increment leaf tasks have start states 1 and 7:

DAG leaf task start states
1
7

A list of leaf tasks:

DAG_leaf_tasks:
increment-ae88130b-bc16-45fb-8479-eb35fae7f83a
increment-b2c04dbb-da27-4bce-aac2-c01d9d69c52d

their leaf task inputs:

DAG_leaf_task_inputs:
(1,)
(0,)

and a list of all tasks and their Python functions:

DAG_tasks (task name --> function)
increment-ae88130b-bc16-45fb-8479-eb35fae7f83a  :  <function increment at 0x000001C04E1DE1F0>
triple-a554a391-a774-4e71-91f9-a1c8e828a454  :  <function triple at 0x000001C04E1DE280>
multiply-137dd808-d202-4b59-9f38-2cdb5bf0985e  :  <function multiply at 0x000001C04E1DE310>
divide-4311366c-ac97-4af9-a04c-1a76a586b2ad  :  <function divide at 0x000001C04E1DE3A0>
square-ea198bd4-0fb7-425b-8b83-acdad7d09028  :  <function square at 0x000001C04E1DE430>
add-a35dba4d-ff44-4853-a814-a7804da54c11  :  <function add at 0x000001C04E1DE4C0>
increment-b2c04dbb-da27-4bce-aac2-c01d9d69c52d  :  <function increment at 0x000001C04E1DE1F0>

In general, a thread/process/Lambda executes a DFS path through the DAG.  A path corresponds
to a sequence of states. In each state, a thread/process/Lambda executes the faninNB, fanout,
fanin, and collpase operations for that state.

There are 4 possble schemes for assigning states to thread/process/Lambda.

A1. We assign each leaf node state to Lambda Executor. At fanouts, a Lambda executor starts another
Executor that begins execution at the fanout's associated state. When all Lamdas have performed
a faninNB operation for a given faninNB F, F starts a new Lambda executor that begins its
execution by excuting the fanin task in the task's associated state. Fanins are processed as usual
using "becomes". This is essentially Wukong with DAGs represented as state machines. Note that
fanin/faninNBs are stored on the tcp_server or in Lambdas.

A1_Server. The fanins/faninNBs are stored remotely on the tcp_server
A1_FunctionSimulator. See A3.
A1_SingleFunction. See A3.
A1_Orchestrator. See A3.

Note: we eventually want to use the parallel invoker to invoke the fanouts.

A2. This is the same as scheme (1) using threads instead of Lambdas. This is simply a way to
test the logic of (1) by running threads locally instead of using Lambdas. In this scheme, the 
fanin/faninNbs are stored locally. The faninNBs will create threads to run the fanin tasks 
(to simulate creating Lambdas to run the fanin tasks) and these threads run locally. The 
thread that is processing the faninNB creates a new thread that actually makes the call to 
fan_in, to simulate an "async" call to the fan_in of a faninNB. (There is no reason to wait for
the fan_in to return since there are no fanin results - the fanin starts a new thread to execute 
the fanin task and passes the results as a parameter.

A3. This is the same as scheme (1) using threads instead of Lambdas except that fanins/faninNbs
are stored remotely.  Now the faninNBs cannot create threads since such threads would run on the 
tcp_server; instead, the calling create a new thread that runs locally (on the client machine). 
So this is the  same as (A2) except that the thread created to execute a fanin task is created 
by the thread that calls fanin (and is the last to call fanin) instead of the faninNB (after 
the last call to fanin.) 

Note: for A2 and A3 the results of the fan_in are returned to the calling thread
but these results are ignored, i.e., not passed to the fanin task that is created. This is 
because all threads run locally and they shara a global data dictionary. The fanin results are
already in the dictionary since the threads that executed the tasks that created these 
outputs/results (i.e., the task execution outputs were passed to the fanin and returned to the
become task as results) also put these outputs in the data dictionary. Thus, we have nothing
we need to do with the results returned by a fanin. (When we use process workers, each work
puts the fanin results in its own (local) private data dictionary as there is no global data
dictionay shared by processes.)
output/

(A3.FunctionSimulator) Note: This scheme is also used with real python functions that simulate 
lambdas (so we can test things without running real lambdas) in which we store the 
synchroization objects. If
  using_Lambda_Function_Simulator = True
then we create a list of Python functions and map the fanins/faninNBs/fanouts names 
to these functions, e.g., one name per function, or two names mapped to
the same function if ops on these named objects cannot be executed
concurrently, i.e., fanin1.fanin and fanin2.fanin cannot be executed 
concurrently since fanin1 and fanin2 are on the same DFS path.
Then we invoke a function list(i), instead of a real
lamba function, when we perform a sync op on a synch object stored in function listi). 

(A3.SingleFunction) If
   use_single_lambda_function = True
Then we use a single function to store all the fanins/faninNBs/fanouts as 
a simple test case. Otherwise we may store each synch object in its own function.

Note: There are two ways to use these Python functions. (1) when we do a synch op we
map the object name to a function index, e.g., 1, and call that function, passing 
a "message" to indicate the op, synchronous-fanIn1-fan_in-kwargs. 

(A3.Orchestrator) (2) Instead of invoking the function directly each time an op is called on an object stored in 
that function, we pass the message to an "orchestrator" similar to an AWS SQS, which 
will check to see whether a call to the function is triggered. That is, if the synch
object (always a fanin for now) has a size of n, and this is not the nth op, the 
message is saved in a list of messages to given to the synch object after all n
operations have occured (by invoking the function that stores the object, either n
times, passing the messages one by one in the order received; or all at once, i.e.
pass them all to the function and allow its MessageHandler to call fan_in n
times, which reduces the number of function calls to 1)

A4. We use a fixed-size pool of threads with a work_queue that holds the DAG states that have 
been enabled so far. The driver deposits the leaf task states into the work_queue. Pool threads 
get the leaf states and execute the task then the collapse/fanoutNB/fanout/fanin operations for 
these states. Any states that are enabled by fanout and faninNB operation are put into the 
work_queue, to be eventully withdrawn and executed by the pool threads, until all states/tasks 
have been executed. In this scheme, the fanins/faninNb synch objects can be stored locally or 
remotely.

A4_L Note: When fanins/FaninNBs are stored locally, the work queue is a global local objects shared
by all thread workers. 

A4_R. When the fanins/faninNBs are stored remotely (on the server), the work 
queue is on the server so enqueue/deque is a remote operation on a remote object.

Note: We try to reduce the number of calsls to the server for faninNB processing by
batching the faninNB operations. That is, we make one call to the server and pass
all the faninNB operations. We also pass the list of fanouts on this call. Fanouts
are added to the work_queue, which is also on the server. Thus, there is one call
to the server for processing all of the faninNBs and fanouts. This batching is 
done when we aer using worker processes (not threads since the work_queue is local 
when we are using threads), or using lambdas (which do not use a work queue, currently)
or we are not using workers and we using threads to simulated lambdas and using 
Python functions for simulating lambas for storing sync objects. 
(Config: A1, A3, A5, A6)

For process_faninNBs_batch, we pass sync_call = True, when we are using lambdas Wukong
styl or we are using worker processes and the worker does not need work. In these cases, no work 
can be returned so there is no need to wait for the return value. In these cases, the 
api function that calls the tcp_server (or tcp_server_lamda if we are storing objects in
simulated lambdas) will not do a receive to wait on the return but istead will return
a dummy erturn value that it creates with the return value set to 0, which is what t would 
have received if it waited (i.e., synchronously). The process_faninNBs_batch in tcp_server_lambda
returns a list of work (if there is work) to the thread that is simulating the calling lambda
so this thread can create more threads to simulate the lambdas, i.e., since the server cannot
create new threads to run the faninNB fanin tasks (or they would run on the server), we 
let the calling (local) thread create these threads. When using real lambdas to excute tasks,
the faninNB fanins will invoke real lambdas to execute the fanin tasks.
For process_faninNBs_batch:
- A1: when using real lambdas, no work is returned since no workers are used with real lambdas.
this might change if we benchmark decentralized scheduling using a lambda that has n workers 
then infinite workers (Wukong) as a comparision. This is an async_call.
- A2: when using threads to simulate workers and store lambdas locally, we do not use batch 
processing. FaninNBs are stored locally and are handled with normal synchronous operations 
one by one. The local FaninNBs start local threads to simulate starting real lambdas.
We are not using process_faninNB_batch so async_call is not relevant.
- A3:Same holds when FaninNBs are stored on the server. The server returns the work for the faninNB
and the calling thread starts the thread that simulates the real lamabda, (The server
can't start the thread since it would run on the server.
For A2 and A3 Performance is not an issue as we are simply testing the logic for using lambdas.
- (A3.SOIL) : S*ync O*bjects I*n L*ambdas, we do use batch processing here. When sync objects are
stored in lambdas instead of on the server, we will be running tcp_server_lambda, and 
we will call process_faninB_batch of tcp_server_lambda for this case. So when we store
sync objects in lambdas we try to call process_faninNB_batch of tcp_server_lambda.
(Real lambdas call proocess_faninNB_batch. Each FaninNB will be processed one by one by
calling fanin for each faninNB and the fanin will start a real lambda to execute the fanin task
so no work will be generated.) In this (A3.SOIL) case, we return a list of work to the calling 
thread (that is simulating a lambda) and it starts a thread for each work tuple (simulating the 
FaninNB starting a lambda for each work tuple). Note that process_faninNB_batch calls fanin
for each FaninNB being processed. It does this either by calling the real lambda that stores
the FaninnB or by calling the Python function that is simulating the lambda that stores the 
object. If an orchestrator is used, the enqueue method of the orchestrator is called.
async_call is False since the caller need to wait for the returned work, if any. Note that 
this configuration does not permit a synch object to trigger its tasks so tha the task is
executed in the same lambda as the sync object. In that case, the task is executed by a 
lambda (real or simulated) not a thread simulating a lambda, and that is a version of A1.
We support A3.SOIL so we can test the logic of using lambdas when sync obejcts are
stoted in lambdas possbly simulated by Python functions and possibly using the 
DAG_orchestrator. That is, all of this stuff is channeled through process_faninNBs_batch
in tcp_server_lambda so we usually want configurations that can store sync objects in 
lambdas to use process_faninNBs_batch. That said, when we use thread workers and store
object remotely, we do not call process_faninNBs_batch sicen we are not interested in
stroring sync_objects in lambdas for this case s this case (thread workers) is not an 
important case - using multiple thread workers will not achieve any speedup in Python
so we don't support all the various scenarios (storing objects in lambdas, using 
DAG_orchestrator, etc) for this thread workers case.
- A4_L: the workers are threads, the work queue is a shared local work queue. 
and the fanin/faninNB objects are created and stored locally as part of the 
local python program. process_faninNBs_batch is not called (since process_faninNB_batch 
for tcp_server also enques work in the remote work queue that is on the server). 
async_call is not relevant.
- A4_R: workers are threads and the work queue and sync objects are on the server. 
async_call is False since the caller needs to wait for the returned work, if any.
- A5 workers are processes and sync objects and work queue are on the server.
async_call is False since the caller needs to wait for the returned work, if any.
- A6: same as A5.

Note: If a worker processing the current state has no fanouts then it will need more 
work unless it is the become task for one of its faninNBs. Usually, the results of
a faninNB fan_in are put it the work queue. But if the worker processing the faninNBs
has no fanouts, it is given the results from a FanInNB if it is the become task
for the FaninNB. (It may be the becoe task for several FaninNBs, but it is given
the results of only one of them.) The results of any other faninNBs for which it is the 
becoe task are put it the work queue. So one call to process_faninNB_batch may produce
a lot of work - work for al of the fanouts that are passed, and work from any faninNBS
for which we are the become task. (If we are not the become task for a faninNB then 
0 is returned and we do nothing; sme other worker will be the becme task and will 
handle the results (either by "stealing the work if it needs it or adding it to the
work queue)

Note: When a worker processes a group of fanouts that can be executed in the current state, a 
thread will "become" the thread that executes one of the fanouts instead of putting the fanout 
state for that fanout in the work_queue. The same thing happens when a thread becomes the 
thread that executes a fanin task. So becomes are handled as usual. In general, when a worker
needs_work it gets work from the (shared) work_queue. A worker on puts work in the work 
queue that it cannot execute becuase it already has work to do.

Note: when we call process_fannNBs_batch and worker_needs_input is false, then this call 
is treated as an async call since the worker does not need the results, i.e.. and thus there
is no need to wait for the results. 

Note: We currently bcome the first fanout on the fanout list for the current state.
It would probably be better to choose which fanout to become. For example, become the 
fanout that will do the most fanouts/fanins, i.e., enable the most parallel operations.
Or choose the fanout that has the most fanouts/fanins on the path from the fanout to the 
end of the DAG.

A5. This is the same as (3) only we use (multi) processes as workers instead of threads. This 
scheme is expected to be faster than (3) for large enough DAGS since there is real parallelism.
When we use processes, the work queue is on the server.

A6. This scheme is like (A5) since the workers are processes, but each worker process can have multiple
threads. The threads are essentilly a pool of threads (like (A4)), each of which is executing in a 
process that is part of a pool of processes. This may make a Worker process perform better since while 
one thread is blocked, say on a socket call to the tcp_server, the other threas(s) can run.

Note: Ths scheme has intermittent failures do to corrupt messages being received by the 
tcp_server. 

We expect (1) to be faster than (6) to be faster than (5) to be faster than (4) to be 
faster than (2) to be faster than (3). Executing (4) with one thread in the pool gives us a 
baseline T(1) for comparing the speedup from (6) and (5) (Tp) (using workers) and (1) 
(using real lambdas) Tinfinity). We can also compute the COST metric - how many cores must 
we use in order for a multicore execution of (6) or (5) or (1) to be faster than the excution 
of (4) with one thread (but possibly many cores).

Thee are three schemes for using the fanin and faninNB synchronization objects:

(S1) The fanin and faninNB objects are stored locally in RAM. This scheme can be used with schemes (A2) 
and (A4) above. In both cases, we are using threads to execute tasks, not processes or Lambdas.
In (A2) FaninNBs create new (local) tasks to execute the fanin task. This is okay since the faninNBs
are stored locally and fanin runs locally. This simulates the use of Lambdas. Note
that when using real lambdas to run tasks (A1) the synch objets are stored remotely. (A2) results in the 
creation of many local threads so it  is not practical, but it tests the Lambda creation logic. In (A4) 
local FaninNBs enqueue the results to be used by the fanin task in a shared work_queue (shared by the 
local thread workers).

(S2) The fanin and faninNB objects are stored (remotely) on the TCP_server. This is used 
with schemes (A1), (A3), (A4_R), (A5), and (A6) above. Note that using (multi) processes or 
Lambas requries the fanin and faninNB objects to be stored remotely.

(S3_real) This is the same as (S2) with fanins and faninNBs stored in real lambdas instead of on the 
tcp_server. This scheme was tested with the Composer program, but it is harder to run since it 
requiers AWS lambda.

(S3_simulated) A scheme under development is to store both the synch objects and the DAG tasks in 
simulated lambdas. One scheme is to store the objects in Python functios the way they are stored
in real lambdas. The tcp_server_lambda simply calls a Python function instead of a real lambda
function. The code is the same. A second scheme is to store the objects in a simulated array of
InfiniD python functions. Objects are mapped to an array index of a function and when an op is
performed on an object the map is used to get the Python function (Array[i].()) to call.

(S3_orchestrated) 
- Triggers: Objects are stored in InfiniD functions as in (S3_smulated), but the calling
of funnction is through a DAG_orchestator. That is, the orchestrator collects all the fanin
messages and when the orchestrator gets all n of the fanins, the actual fanin op calls are triggered.
So no fanin calls are made to the FanIn object until all n calls have been collected by the 
- Code to Data: We extend the trigger scheme by having the triggered fan_n op also execute the 
fanin task. That is, the DAG_executor is called with the start state oof the fanin task and the fanin
results. 
- Fanouts are Fanins: A fanout object is stored in an infiniD function as a 
fanin of size 1. When the fanout object gets its orchestrator triggered fan_out call, it run the 
DAG_executor to execute its fanout task. 
- duplicate and short-cicuit: Duplication means that if Function1 and Function2 run DAG_executors
that faninin to FaninX, then we store a FaninX object in both Function1 and Function2. The the
DAG_executors in Function1 and Function2 *both* execute local fan_in operations when they
access the FaninX in their function. Short-circuit means that we try to avoid making the 
non-local calls to FaninX when possible. Suppose the DAG_executor1 in Function1 makes its local 
access to its FaninX and invokes process_fanins_batch on tcp_server_lambda as usual. The 
tcp_server_lambda orchestrator can see that DAG_excutor1 will not be the last executor 
to do a fan_in on FaninX; thus, the fan_in results of DAG_executor1 will be delivered
to the FanInX in Function2, since DAG_executor2 in Function2 will be the 
last to excute a fan_in on FaninX and needs the results of DAG_executor1. Note that when 
DAG_executor2 in Function2 does its local fan_in to FanInX and calls process_fanins_batch
on tcp_Server_lambda, the orchestrator will return the fanin results it collected from 
DAG_executor1 so that DAG_executor1 will be able to make the corresponding fan_in calls
on its FanInX and complete the fanin. The orchestrator will not call FanInX in function1
with DAG_excutor2's fanin results since the orchestrator knows this call to FaninX is not the
last call. So a non-global invocation of function1 is avoided. Also, since the 
process_fanins_batch call by DAG_executor2 returns the fanin results of DAG_executor1, another
invocation of function2 (to pass the results of DAG_executor1) is avoided.

This (S3_orchestrated) scheme requires an assignment of tasks and fanin/faninNBs to 
InfiniD lambdas, perhaps multiple non-current fanout/fanIns per function, possibly
with duplication. (The DAG_representation allows us to see which paths/executors a given 
named fanin/fanout/faninNB appears in) The orchestrator will orchestrate all fanout/fanin/faninNB
operations/tasks.

Note: This new scheme uses an orchestrator that is part of tcp_server_lambda. Consider
a scenario where the data analyst is using real InfiniD lambda functions storing all
fanins/fanouts/faninNBs and using her own machine to run multiple servers with orchestrators 
that are orchestrating the DAG execution, i.e., orchestrating the ops for their partition of the 
fanins/fanouts. This is very serverless since the only non-serverless component
is the data analyst's machine/server. It supplies the tcp_serer_lambdas with orchestrators and 
it stores (in the orchestrator) the intermediate results (until the operations get triggered).

Note: The orchestrator is similar to SQS, where the fanin/fanoutresults are pushed to the Lmabdas 
instead of having the Lambdas poll/pull the fanin/fanout results.
"""

"""
using threads to simulate lambdas ==> RUN_ALL_TASKS_LOCALLY and not USING_WORKERS: 
        objects stored locally (so no triggering fanout/fanin tasks)
        objects not stored locally (RUN_ALL_TASKS_LOCALLY and not USING_WORKERS and not STORE_FANINS_FANINNBS_LOCALLY)
            objects stored on server - cannot trigger fanin/fanout tasks
            objects stored in lambdas 
                do not trigger tasks
                trigger tasks not allowed since using threads to simulate lambdas
                
                
    using lambdas:
    store objects remotely in lambdas but do not run tasks in lambdas
        lambdas for storage are real not simulated
            using workers for executing tasks
            using real lambdas for excuting tasks
            using threads to simulate lambdas for executing tasks (like Wukong)
        lambdas for storage simulated by python functions
            using workers
                do not use dag orch. since lambdas do not currently use worker-scheme
            using real lambdas for excuting tasks
                use dag orch. with enqueue(), then same real lambdas store objects
                do not use dag orchestrator
            using threads to simulate lambdas for executing tasks
                do not use dag orchestrator since lambdas run triggered tasks

Note: using threads to simulate lambdas means 
- we simulate the Wukong scheme - all Lambdas are started by fanouts/fanins. The 
    sync objects may or may not be stored in Lambdas, but if they are strored in
    lambdas, simulated or real, they cannot trigger tasks since we must start a 
    thread to simulate a Lambda executing the task, a la Wukong. So p_f_b will
    return work to the calling thread and it will start threads to simulate starting
    lambdas.
- The scheme in which tasks are triggered and executed in the lambdas storing 
    sync objects is not using threads to execute lambdas. It is using lambdas
    simulated or not to store sync_objects that trigger their fanout/fanin tasks.

We typically use simulated lambdas so we can run/test the code without using AWS.

DAG_orchestrator
    storing tasks in lambdas
        simulated lambdas
            running tasks in Lambdas (a)
            not running tasks in lambdas
        not simulated lambdas
            running tasks in Lambdas (a)
            not running tasks in lambdas
==> (a) not using threads to simulate lambdas for running threads when using the 
    DAG ochestrator. And not using workers.
==> not RUN_ALL_TASKS_LOCALLY and STORE_SYNC_OBJECTS_IN_LAMBDAS
    and USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS = True or False
    and SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS = True or False
So when we are using the DAG orchestrator it is another way to manage and 
access the lambdas (simulated or not) that store objects. And so can be used
with all the various ways of running tasks (threads to simulate lambdas, 
workers, and lambdas executing tasks). Optionally, the stored objects can 
trigger the fanout tasks, which will run in the same lambda as the object.
In this case the lambas may be real or simulated and only lambdas can be
used to execute tasks, i.e., no threads that simulate lambdas.

We call process_faninNB_batch when 
    running tcp_server - not using lambdas 
    running tcp_server_lambda - storing synch objects in lambdas
        sync object may or may not trigger their tasks. 
            Triggered tasks are running in simulated or real lambdas that will call p_f_b()
- using worker processes
- !RUN_ALL_TASKS_LOCALLY so not using workers; instead using lambdas; note: always call p_f_b
- using threads to simulate lambdas (and since lambdas call p_f_b threads here should too)

So use async_call = False for p_f_b() when work can be returned
    using worker processes and worker_needs_input
    using threads to simulate lambdas - no lambas started by faninNBs or triggered since using threads to sim lambdas
So use async_call = True for p_f_b when no work can be returned
    using worker processes but worker_needs_input == False
    !RUN_ALL_TASKS_LOCALLY so using real lambdas so no threads for simulation  

Q: In code  dag-98: if (RUN_ALL_TASKS_LOCALLY and USING_WORKERS and not USING_THREADS_NOT_PROCESSES) or (not RUN_ALL_TASKS_LOCALLY) or (RUN_ALL_TASKS_LOCALLY and not USING_WORKERS and not STORE_FANINS_FANINNBS_LOCALLY and USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS):
        # Config: A1, A3, A5, A6
        # Note: calling process_faninNBs_batch when using threads to simulate lambdas and storing objects remotely
        # and USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS. 
    So what if using threads to simulate lambdas and store sync objects remotely 
    but not using simulated objects to store lambdas?
    Then we are not using batch? Since calling tcp_server and it does not handle
    the cse for using threads to simulate lambdas? 
Q: So when use threads to simulate lambdas and store objects remotely but not 
    in lambdas we do not get lsit of fanouts? and we start new threads for 
    these fanouts? And if no faninNBs then only process fanout list when ?       

store objects in lambas - workers or no workers
    lambdas are real, i.e., not simulated (IMPLEMENTED)
    lambdas are simulated by python function (IMLEMENTED)
        object does not trigger task; instead object returns results to calling thread
        object triggers task to be executd in same lamba, so no results returned

So if using threads to simulate lambdas that execute the tasks and are separate
from the lambdas that store synch objects (remotely) i.e., sync objects 
stored remotely in lambdas (real or simulated) do not trigger their fanin tasks, 
the collected results must be returned to the calling task.

So we are working now with python functions that simulate lambdas but we should
be able to switch over to real lambdas and keep things working. So we don't
need to check whether we aer using simulated lambdas or not, we just check
whether we are storing objects in lambda (simulated or not) - we set the 
options for simulated lambdas so tht we use them since we cannot run
real lambdas without using AWS.

not RUN_ALL_TASKS_LOCALLY ==> no workers
    running tasks in real lambdas
        original: Wukong: run leaf tasks in real lambdas + fanouts start real lambdas 
            + faninNBs start real lambdas to execute fanin tasks
        alternate scheme: dag orchestator invokes real lambdas to do fanout/fanins
            and to trigger fanout/fanin tasks to excute in same real lambda

workers
    threads
    processes
        objects stored on server
        objects stored in lambas
            simulated lambdas or not

We tested using workers and storing objects in real lambdas

We did not test Wukong case with real lambdas executing tasks and 
accessing objects stored on server or in lambdas.

Scheme for DAG_info: 
- For non-incremental, non-pagerank DAG generation, we are using DAGs that are generated 
from Dask DAGS using dask_dag.py. These are created and stored in DAG_info.pickle.
- For non-incremental, pagerank DAG generation, we generate DAGs using BFS.py and save them in 
DAG_info.pickle. BFS generates DAG_info.pickle and starts a thread that 
calls DAG_executor_driver.run() to execute the DAG.
- For incremental pagerank, the generation and execution of the DAG is overlapped.
BFS generates part of the DAG and deposits it in a buffer where it is 
withdrawn by the executors. So reading file DAG_info.pickle is not the primary
way of distributing DAG_info. 
- The FaninNB objects need DAG_info since when they start a Lambda to execute
the fanin task the lambda needs DAG_info. (Fanin objects do not start lambdas
to execute the fanin task, some exisiting lambda will "become" the lambda
to execute the fanin task.) Thus, DAG_info is passed in the
lmabda's payload. Note that when real lambdas are used, all synch objects are
stored "remotely" on the tcp_server. Synch objects can also be stored 
locally on the machine running workers/simulated lambdas. Note also that 
synch objects can all be created at start of DAG_execution or they can be 
created on-the-fly as fanin operations are called on them. When using incremental
DAG generation, all objects must be created on-the-fly since we do not have the 
complete ADG and thus all the synch objects at the start of DAG execution.
- When the fanin/faninNb synch objects are all created at the start of DAG
execution, the DAG_executor_driver creates them all either locally or remotely
(by sending a message with the synch objects to tcp_server) and since the DAG_excutor_driver 
can read DAG_info.pickle the DAG_executor_driver can pass DAG_info to tcp_server and then 
to all of the FaninNB objects that are created. Again, for incremental DAG_generation, the synch 
objects cannot be created at the start of DAG_execution (since we do not
know the fanin objects that will be generated) When the fanin/faninNB objects
are created on-the-fly, we must ensure that DAG_info is availabe when 
create() is called to create a faninNB object. 
  - If the objects are all created locally, (which means we are not using 
real lambas) then DAG_info.pickle is read by the worker threads/processe 
and by the threads that simulate real lambdas, and DAG_info is passed 
along to the call to create() a FaninNB.
  - If the fanin/faninNB objects are created remotely on the server, then 
DAG_info needs to be accessible on the tcp_server. As we will describe below,
either we pass DAG_info to tcp_server when we do fanin operations, or the 
tcp_server reads DAG_info.pickle from a file. (For incremental DAG generation
we do not read DAG_info from a file, the incremental DAGS are retrieved by the 
DAG_executors and passed to tcp_server on remote/tcp calls to tcp_server.)

ISSUE: if synch objects on server, we can still be running simulated lambdas
and worker threads/proceses locally. In this case, faninNBs will not create
local simulated lambdas/workers and do not need DAG_info. Workers access
work queue so no new workers are created to execure fanin tasks. For simulated
lambdas, new simulated lambda (threads) are created locally by simulated lambda
that called fan-in, and that simulated lambda has its DAG_info that it read 
from a file (non-incremental) or got in the usual way for incremental DAG
generation. For real lambdas, we always call batch for faninNBs, not synch op,
and batch takes care of DAG_info. We call synch op for Fanin objects and 
I think sumulated lambdas with remote objects call synch op for FaninNB objects
but these FaninNB objects do not need DAG_info. Also, Fanin objects never need
DAG_info. ==> synch op for FaninNb or Fanin never needs to read DAG_info and 
never needs to have DAG_info passed to it since we don't use it for FaninNB/Fanin objects.
TEST: remote objects, w/ simulated lambdas, real lambdas, worker threads, worker processes,
w/ incremental pagerank, non-inc pagerank, non-inc non-pagerank.

(Note: no store objects in lambdas and trigger lambdas)
non-incremental, not RUN_ALL_TASKS_LOCALLY (real lambdas), not store_objects_locally:
- FaninNB: using process_faninNBs_batch and faninNB_remotely_batch for FaninNB, calling FaninNB batch
  passing DAG_info keyword parm as None
- FanIn: using process_fanins and fanin_remotely, calling synch op on tcp_server
  not passing DAG_info keyword parm
non-incremental, RUN_ALL_TASKS_LOCALLY, using worker processes, not store_objects_locally:
# same as above for real lambdas
- FaninNB: using process_faninNBs_batch and faninNB_remotely_batch for FaninNB, calling FaninNB batch
  passing DAG_info keyword parm as None
- FanIn: using process_fanins and fanin_remotely, calling synch op on tcp_server
  not passing DAG_info keyword parm
non-incremental, RUN_ALL_TASKS_LOCALLY, using worker threads, not store_objects_locally:
- FaninNB: using process_faninNBs and faninNB_remotely for FaninNB, calling synch op on tcp_server
  not passing DAG_info keyword parm
- FanIn: using process_fanins and fanin_remotely, calling synch op on tcp_server 
  not passing DAG_info keyword parm
non-incremental, RUN_ALL_TASKS_LOCALLY, not using workers, not store_objects_locally:
- FaninNB: using process_faninNBs and faninNB_remotely for FaninNB, calling synch op on tcp_server
  not passing DAG_info keyword parm
- FanIn: using process_fanins and fanin_remotely, calling synch op on tcp_server
  not passing DAG_info keyword parm

incremental, not RUN_ALL_TASKS_LOCALLY, not: store_objects_locally:
- FaninNB: using process_faninNBs_batch and faninNB_remotely_batch for FaninNB, calling FaninNB batch
  passing DAG_info keyword parm as DAG_info
- FanIn: using process_fanins and fanin_remotely, calling synch op on tcp_server
  not passing DAG_info keyword parm
incremental, RUN_ALL_TASKS_LOCALLY, using worker processes, not store_objects_locally:
- FaninNB: using process_faninNBs_batch and faninNB_remotely_batch for FaninNB, calling FaninNB batch
  passing DAG_info keyword parm as DAG_info
- FanIn: using process_fanins and fanin_remotely, calling synch op on tcp_server 
  not passing DAG_info keyword parm
incremental, RUN_ALL_TASKS_LOCALLY, using worker threads, not store_objects_locally:
- FaninNB: using process_faninNBs and faninNB_remotely for FaninNB, calling synch op on tcp_server
  not passing DAG_info keyword parm
- FanIn: using process_fanins and fanin_remotely, calling synch op on tcp_server
  not passing DAG_info keyword parm
incremental, RUN_ALL_TASKS_LOCALLY, not using workers, not store_objects_locally:
- FaninNB: using process_faninNBs and faninNB_remotely for FaninNB, calling synch op on tcp_server
  not passing DAG_info keyword parm
- FanIn: using process_fanins and fanin_remotely, calling synch op on tcp_server
  not passing DAG_info keyword parm

==> synch op on tcp_server does not read DAG_info. For any fan-in operation
called on a Fanin object, the FanIn object does not need DAG_info since
it doesn't start its fanin task. For FanInNB objects, we only call
synch op on tcp_server for fan-in operation when we are not 
RUN_ALL_TASKS_LOCALLY (real lambdas) and we are not using worker
processes. So we are using simulated lambdas or we are using worker
threads and for these the FaninNB object does not start a (simulated)
lambda to execute the fanin task. The call to process_faninNBs
processes the fainNBs one by one. If a faninNB returns work it 
is queued if we are using workers and it is passed in the payload
of a simulated lambda otherwise.

For the schemes above, when incremental DAG generation is NOT used:
- A1. We assign each leaf node state to Lambda Executor. At fanouts, a Lambda executor starts another
Executor that begins execution at the fanout's associated state. When all Lamdas have performed
a faninNB operation for a given faninNB F, F starts a new Lambda executor that begins its
execution by excuting the fanin task in the task's associated state. Fanins are processed as usual
using "becomes". This is essentially Wukong with DAGs represented as state machines. Note that
fanin/faninNBs are always stored on the tcp_server or in Lambdas.
==> FaninNb synch objects are always stored on the tcp_server and these objects
need DAG_info since when a FaninNB invokes a real lambda to excute the 
fanin task, the real Lambda needs DAG_info to be part of its payload, If
all the fanins/faninNBs are created at the start, the DAG_executor_driver
sends a create() message that includes DAG_info to the tcp_server, which
creates the faninNBs and passes DAG_info on the creates. When fanin/faninNBs
are created on the fly, the tcp_server will be creating fanin/faninNB objects
on the first call to fanin for each object. Note that on-the-fly creation is
always used for incremental DAG generation. When incremental DAG generation 
is not being used, the tcp_server reads DAG_info.pickle from a file and passes
DAG_info to create(). This read is done only on the first call to fanin() for
any fanin/faninNB object. That is, there is one read for the entire execution,
not one read per fanin. The DAG_info only need to be executed onc.

- A2. This is the same as scheme (A1) using threads instead of Lambdas. This is simply a way to
test the logic of (A1) by running threads locally instead of using Lambdas. In this scheme, the 
fanin/faninNbs are stored locally. The faninNBs will create threads to run the fanin tasks 
(to simulate creating Lambdas to run the fanin tasks) and these threads run locally. The 
thread that is processing the faninNB creates a new thread that actually makes the call to 
fan_in, to simulate an "async" call to the fan_in of a faninNB. (There is no reason to wait for
a FaninNB.fan_in to return since there are no fanin results - the faninNB starts a new local thread 
to execute the fanin task and passes the results as a parameter. (tcp_server is not used.)
==> The threads that simulate lambdas read DAG_info.pickle at the start. If
the fnin/faninNB objects are created on-the-fly the workers pass DAG_info on their fanin operations.
- A3. This is the same as scheme (1) using threads instead of Lambdas except that fanins/faninNbs
are stored remotely.  Now the faninNBs cannot create threads since such threads would run on the 
tcp_server; instead, the calling threada creates a new thread that runs locally (on the client machine). 
So this is the  same as (A2) except that the thread created to execute a fanin task is created 
by the thread that calls fanin (and is the last to call fanin) and receives the results
of the fanin, instead of a thread being created by the faninNB (after 
the last call to fanin.) Note that tcp_server does not actually start any
threads that are used to simulate lambdas, so tcp_server does not need
to access DAG_info. The local threads that simuate lambdas read DAG_info
at the start of their execution and thus can pass it to create() if 
synch objects are created on the fly.
- A4_L: the workers are threads, the work queue is a shared local work queue. 
and the fanin/faninNB objects are created and stored locally as part of the 
local python program. process_faninNBs_batch is not called (since process_faninNB_batch 
for tcp_server also enques work in the remote work queue that is on the server). 
async_call is not relevant.
==> the worker threads read DAG_info.pickle when they start. Note that FaninNB
objects do not create workers to excute their fanin tasks; instead, they deposit
the fanin tasks into the work_queue to be withdrawn by the existing workers.
- A4_R: workers are threads and the work queue and sync objects are on the tcp_server. 
async_call is False since the caller needs to wait for the returned work, if any.
==> the worker threads read DAG_info.pickle when they start. If objects are creatd
on the fly, the workers pass DAG_info on their fanin operations.
- A5: This is the same as (A4) only we use (multi) processes as workers instead of threads. This 
scheme is expected to be faster than (A4_R) for large enough DAGS since there is real parallelism.
When we use processes, the work queue is on the server and so must be the 
fanins/faninNBs (as we need to use some sort of IPC for the worker processes
to access the shared fanin/faninNB objects. We use tcp/ip between worker
processes and tcp_server where the objects are stored.)
==> the worker processes read DAG_info.pickle when they start. If objects are creatd
on the fly, the workers pass DAG_info on their fanin operations. ()
- A6. This scheme is like (A5) since the workers are processes, but each worker process can have multiple
threads. The threads are essentially a pool of threads (like (A4)), each of which is executing in a 
process that is part of a pool of processes. This may make a Worker process perform better since while 
one thread is blocked, say on a socket call to the tcp_server, the other threads(s) can run.
=> DAG_info is handled like A5.

For the schemes above, when incremental DAG generation IS used: the 
DAG_executors will be widrawing new incremental DAGs from a buffer after
BFS deposits them. So the DAG_executors have the most recent version of the 
DAG that they have withdrawn.  Also, fannin/faninNB objects are always created
on-the-fly during incremental DAG generation. Note that if the tcp_server
is being used to store fanins/faninNBs remotely, the tcp_server cannot read
the DAG_info from a file since the DAG_info may not be complete. (It is 
incomplete until the last part of the DAG is generated.) So the tcp_server
must receive the incremental DAGS from the DAG_executors when the DAG_executors
call fanin() operations on FaninNB objects. (Fanin objects never need DAG_info
as they do not start fanin tasks; instead one excutor becoms the executor to 
execute the fanin task.) The key ideas is: If an executor has an incomplete
DAG_info D and it does a fanin() operation on FaninB F, then F is complete
in D and F can be executed based on the information in D. So when we create
F on the fly, we can give D to F and F can use D to handle F's fanin task,
e.g., it can invoke a new Lambda and pass D in the payload to the lambda. Even
though D is incomplete, the new lambda can excute F's fanin task since info
about F is in the partial DAG_info. After executing F's fanin task, the 
new Lambda may need to get a new incremental DAG via calling withdraw on 
buffer of DAG_infos. SO:
- A1: real lambdas pass DAG_info to tcp_server on fanin() operations and the 
tcp_server passes this partial Lambda to create() so that the created FaninNB
objects have a partital DAG_info that allows then to start a new Lambda
to excute the fanin task with the partial DAG_info in the payload.
- A2, A3: The simulated lambdas withdraw partial DAG_infos from the buffer
and pass them locally to create() or pass them remotely to the tcp_server
which passes the partial DAG_infos to create() as described above.
- A4, A5, 
- A6: Same scheme

We have an extended white board example in which we add two more connected
componets and we add a fanin node at the end. One added component is
nodes 21 and 22 and the second component is nodes 23 and 24. There is an 
edge from 21 to 22 and from 23 to 24. We created a fanin by adding an 
edge from 15 to 9. Using incremental DAG generation, with an interval of
2 between publishing DAGs (i.e., we publish every other DAG) the 
incremental DAGs are produced as follows. Note that we are generating
groups of nodes, not partitions.

First DAG: This DAG has 4 DAG states, one for each group. PR1_1 is the 
first group (and also the first partition). The PR2_X groups are the 
groups in partition 2. The first DAG always has either 2 partitions or
the groups in the first two partitions. The first group is complete and 
so can be executed. The groups in partition 2 are incomplete. Notice
that we know the fanins/faninNBs/fanouts/collapses for group 1 but not 
for the PR2_X groups. Hence, group 1 is complete and the PR2_X groups
aer not. We give this first DAG_info to the DAG_executor_driver and it 
will start workers/lambdas to excute the DAG. In this case, group 1
is a leaf task so the DAG_executor_driver will ensure this leaf task
is executed. Other leaf tasks will be detected incrementally and the 
DAG_executor_driver does not start them (the DAG_executor_driver is 
called only once at the start of execution). The workers/lambda started
by the DAG_executor_driver can excute task/group PR1_1 but there are no 
more complete tasks/groups so the workers will request a new DAG_info
and the lambda will stop executing to later be "restarted" when a new
incremental DAG is available. (For lambdas, this is essentially a
type of "no-wait" synchronization - the lambda terminates and we (re)start 
a new lambda to continue processing task/group 1 when a DAG becomes 
available. (Note: the lambda started to complete group/task 1 will do so
by performing the fanins/faninNBs/fanouts/collapses for group 1. This 
means we saved the output of task 1 on the tcp_server and gave it to 
the new lambda (in its payload) started to complete task/group 1)

# for each state, the fanins/faninNBs/fanouts/collapses and flags
to indicate if the state/group/task is complete and whether it
has fanins/faninNBs/fanouts/collapses that are to-be-continued,
i.e., that are not known until the next incremental DAG is generated.
DAG_infoBuffer_Monitor_for_Lambdas: print_DAG_info: DAG_map:
1
 task: PR1_1, fanouts:['PR2_1', 'PR2_3'], fanins:[], faninsNB:['PR2_2L'], collapse:[], fanin_sizes:[], faninNB_sizes:[2], task_inputs:(), ToBeContinued:False, fanout_fanin_faninNB_collapse_groups_are_ToBeContinued:True
2
 task: PR2_1, fanouts:[], fanins:[], faninsNB:[], collapse:[], fanin_sizes:[], faninNB_sizes:[], task_inputs:('PR1_1-PR2_1',), ToBeContinued:True, fanout_fanin_faninNB_collapse_groups_are_ToBeContinued:True
3
 task: PR2_2L, fanouts:[], fanins:[], faninsNB:[], collapse:[], fanin_sizes:[], faninNB_sizes:[], task_inputs:('PR2_1-PR2_2L', 'PR1_1-PR2_2L'), ToBeContinued:True, fanout_fanin_faninNB_collapse_groups_are_ToBeContinued:True
4
 task: PR2_3, fanouts:[], fanins:[], faninsNB:[], collapse:[], fanin_sizes:[], faninNB_sizes:[], task_inputs:('PR1_1-PR2_3',), ToBeContinued:True, fanout_fanin_faninNB_collapse_groups_are_ToBeContinued:True

DAG_infoBuffer_Monitor_for_Lambdas: print_DAG_info: DAG states:
PR1_1
1
PR2_1
2
PR2_2L
3
PR2_3
4

# start state of any leaf tasks
DAG_infoBuffer_Monitor_for_Lambdas: print_DAG_info: DAG leaf task start states
1

# python function to be executed for the pageran tasks in the DAG.
# the tasks are the same. their group/partition and inputs are different.
DAG_infoBuffer_Monitor_for_Lambdas: print_DAG_info: DAG_tasks:
PR1_1  :  <function PageRank_Function_Driver at 0x00000227638482C0>
PR2_1  :  <function PageRank_Function_Driver at 0x00000227638482C0>
PR2_2L  :  <function PageRank_Function_Driver at 0x00000227638482C0>
PR2_3  :  <function PageRank_Function_Driver at 0x00000227638482C0>

DAG_infoBuffer_Monitor_for_Lambdas: print_DAG_info: DAG_leaf_tasks:
PR1_1

DAG_infoBuffer_Monitor_for_Lambdas: print_DAG_info: DAG_leaf_task_inputs:
()

# version 2 of the DAG. The first version only had group PR1_1 in it
# and we start execution with 2 partitions or the groups in 2 partitions.
DAG_infoBuffer_Monitor_for_Lambdas: print_DAG_info: DAG_version_number:
2
# the DAG has only partially been constucted, i.e., it is still incomplete
DAG_infoBuffer_Monitor_for_Lambdas: print_DAG_info: DAG_info_is_complete:
False

The second incremental DAG is shown below. We added the groups in the 
next 2 partitions to the DAG. Partition 3 has the PR3_X groups and
partition 4 has one group PR4_1. When we added the partition 3 groups,
the partiton 2 groups became complete. When we added the partition
4 group, pthe partition 3 groups became complete. Group PR4_1 is inomplete;
it will become complete when we add the group PR5_1 in partition 5.
Group PR4_1 is the first group in a new connected component.
This group has state 8. Since PR4_1 is the start of a new connected
component it is a leaf task. This also means that the groups in 
partition 3 were the last groups in their connected component. Thus, 
they have no fanins/faninNBs/fanouts/collapses to groups that are
not in the same partition 3, which means there are no "missing"
fanins/faninNBs/fanouts/collapses and thus that these PR3_X groups
are complete.

# Groups PR2_X ad PR3_X are complete. Group PR4_1 is not.
DAG_infoBuffer_Monitor_for_Lambdas: print_DAG_info: DAG_map:
1
 task: PR1_1, fanouts:['PR2_1', 'PR2_3'], fanins:[], faninsNB:['PR2_2L'], collapse:[], fanin_sizes:[], faninNB_sizes:[2], task_inputs:(), ToBeContinued:False, fanout_fanin_faninNB_collapse_groups_are_ToBeContinued:False
2
 task: PR2_1, fanouts:[], fanins:[], faninsNB:['PR2_2L'], collapse:[], fanin_sizes:[], faninNB_sizes:[2], task_inputs:('PR1_1-PR2_1',), ToBeContinued:False, fanout_fanin_faninNB_collapse_groups_are_ToBeContinued:False
3
 task: PR2_2L, fanouts:['PR3_1'], fanins:[], faninsNB:['PR3_2'], collapse:[], fanin_sizes:[], faninNB_sizes:[2], task_inputs:('PR2_1-PR2_2L', 'PR1_1-PR2_2L'), ToBeContinued:False, fanout_fanin_faninNB_collapse_groups_are_ToBeContinued:False
4
 task: PR2_3, fanouts:[], fanins:['PR3_3'], faninsNB:[], collapse:[], fanin_sizes:[2], faninNB_sizes:[], task_inputs:('PR1_1-PR2_3',), ToBeContinued:False, fanout_fanin_faninNB_collapse_groups_are_ToBeContinued:False
5
 task: PR3_1, fanouts:[], fanins:[], faninsNB:['PR3_2'], collapse:[], fanin_sizes:[], faninNB_sizes:[2], task_inputs:('PR2_2L-PR3_1',), ToBeContinued:False, fanout_fanin_faninNB_collapse_groups_are_ToBeContinued:False
6
 task: PR3_2, fanouts:[], fanins:['PR3_3'], faninsNB:[], collapse:[], fanin_sizes:[2], faninNB_sizes:[], task_inputs:('PR2_2L-PR3_2', 'PR3_1-PR3_2'), ToBeContinued:False, fanout_fanin_faninNB_collapse_groups_are_ToBeContinued:False
7
 task: PR3_3, fanouts:[], fanins:[], faninsNB:[], collapse:[], fanin_sizes:[], faninNB_sizes:[], task_inputs:('PR3_2-PR3_3', 'PR2_3-PR3_3'), ToBeContinued:False, fanout_fanin_faninNB_collapse_groups_are_ToBeContinued:False
8
 task: PR4_1, fanouts:[], fanins:[], faninsNB:[], collapse:[], fanin_sizes:[], faninNB_sizes:[], task_inputs:(), ToBeContinued:True, fanout_fanin_faninNB_collapse_groups_are_ToBeContinued:True

DAG_infoBuffer_Monitor_for_Lambdas: print_DAG_info: DAG states:
PR1_1
1
PR2_1
2
PR2_2L
3
PR2_3
4
PR3_1
5
PR3_2
6
PR3_3
7
PR4_1
8

# PR4_1 which has state 8 is a leaf task. It will be executed when this
# DAG is generated by either starting a lambda to execute it or putting 
# it in the workers work queue.
DAG_infoBuffer_Monitor_for_Lambdas: print_DAG_info: DAG leaf task start states
1
8

DAG_infoBuffer_Monitor_for_Lambdas: print_DAG_info: DAG_tasks:
...
Same as above 
...

DAG_infoBuffer_Monitor_for_Lambdas: print_DAG_info: DAG_leaf_tasks:
PR1_1
PR4_1

# leaf taks have no input
DAG_infoBuffer_Monitor_for_Lambdas: print_DAG_info: DAG_leaf_task_inputs:
()
()

# this is version 4, version 3 has groups for partition 1, 2, and 3.
# we publish every other incremental DAG - we started by publishig 
# the DAG for PR1_1 and PR2_X, we did not publish the next DAG, which 
# added PR3_X, and we publised the DAG that added PR4_1.
DAG_infoBuffer_Monitor_for_Lambdas: print_DAG_info: DAG_version_number:
4
DAG_infoBuffer_Monitor_for_Lambdas: print_DAG_info: DAG_info_is_complete:
False

The next DAG is shown below. It adds the groups in partition 5, which is 
the single group PR5_1, and the groups in partition 6, which is PR6_1.
PR5_1 is the last partition/group in the second component that started
with PR4_1. So the second component contains PR4_1 followed by PR5_1.
When PR5_1 was generated, PR4_1 became complete. And since PR5_1 ends 
the component (it has no children nodes that are in a different 
partition's groups) it is also complete. This means thar PR4_1 has
no fanins/faninNBs/fanouts/collapses to any to-be-continued, i.e., 
incomplete groups. PR6_1 is the first partition/group in a new (third)
connected component. Thus it is a leaf task. It is incomplete and will
become complete in the next DAG generated.

DAG_infoBuffer_Monitor_for_Lambdas: print_DAG_info: DAG_map:
1
 task: PR1_1, fanouts:['PR2_1', 'PR2_3'], fanins:[], faninsNB:['PR2_2L'], collapse:[], fanin_sizes:[], faninNB_sizes:[2], task_inputs:(), ToBeContinued:False, fanout_fanin_faninNB_collapse_groups_are_ToBeContinued:False
2
 task: PR2_1, fanouts:[], fanins:[], faninsNB:['PR2_2L'], collapse:[], fanin_sizes:[], faninNB_sizes:[2], task_inputs:('PR1_1-PR2_1',), ToBeContinued:False, fanout_fanin_faninNB_collapse_groups_are_ToBeContinued:False
3
 task: PR2_2L, fanouts:['PR3_1'], fanins:[], faninsNB:['PR3_2'], collapse:[], fanin_sizes:[], faninNB_sizes:[2], task_inputs:('PR2_1-PR2_2L', 'PR1_1-PR2_2L'), ToBeContinued:False, fanout_fanin_faninNB_collapse_groups_are_ToBeContinued:False
4
 task: PR2_3, fanouts:[], fanins:['PR3_3'], faninsNB:[], collapse:[], fanin_sizes:[2], faninNB_sizes:[], task_inputs:('PR1_1-PR2_3',), ToBeContinued:False, fanout_fanin_faninNB_collapse_groups_are_ToBeContinued:False
5
 task: PR3_1, fanouts:[], fanins:[], faninsNB:['PR3_2'], collapse:[], fanin_sizes:[], faninNB_sizes:[2], task_inputs:('PR2_2L-PR3_1',), ToBeContinued:False, fanout_fanin_faninNB_collapse_groups_are_ToBeContinued:False
6
 task: PR3_2, fanouts:[], fanins:['PR3_3'], faninsNB:[], collapse:[], fanin_sizes:[2], faninNB_sizes:[], task_inputs:('PR2_2L-PR3_2', 'PR3_1-PR3_2'), ToBeContinued:False, fanout_fanin_faninNB_collapse_groups_are_ToBeContinued:False
7
 task: PR3_3, fanouts:[], fanins:[], faninsNB:[], collapse:[], fanin_sizes:[], faninNB_sizes:[], task_inputs:('PR3_2-PR3_3', 'PR2_3-PR3_3'), ToBeContinued:False, fanout_fanin_faninNB_collapse_groups_are_ToBeContinued:False
8
 task: PR4_1, fanouts:[], fanins:[], faninsNB:[], collapse:['PR5_1'], fanin_sizes:[], faninNB_sizes:[], task_inputs:(), ToBeContinued:False, fanout_fanin_faninNB_collapse_groups_are_ToBeContinued:False
9
 task: PR5_1, fanouts:[], fanins:[], faninsNB:[], collapse:[], fanin_sizes:[], faninNB_sizes:[], task_inputs:('PR4_1-PR5_1',), ToBeContinued:False, fanout_fanin_faninNB_collapse_groups_are_ToBeContinued:False
10
 task: PR6_1, fanouts:[], fanins:[], faninsNB:[], collapse:[], fanin_sizes:[], faninNB_sizes:[], task_inputs:(), ToBeContinued:True, fanout_fanin_faninNB_collapse_groups_are_ToBeContinued:True

DAG_infoBuffer_Monitor_for_Lambdas: print_DAG_info: DAG states:
PR1_1
1
...
PR5_1
9
PR6_1
10

# leaf tasks are PR1_1, PR4_1, and PR6_1 with states 1, 8, and 10
DAG_infoBuffer_Monitor_for_Lambdas: print_DAG_info: DAG leaf task start states
1
8
10

DAG_infoBuffer_Monitor_for_Lambdas: print_DAG_info: DAG_tasks:
...
SAME
...

DAG_infoBuffer_Monitor_for_Lambdas: print_DAG_info: DAG_leaf_tasks:
PR1_1
PR4_1
PR6_1

DAG_infoBuffer_Monitor_for_Lambdas: print_DAG_info: DAG_leaf_task_inputs:
()
()
()

# version 5 which added PR5_1 was not published
DAG_infoBuffer_Monitor_for_Lambdas: print_DAG_info: DAG_version_number:
6
DAG_infoBuffer_Monitor_for_Lambdas: print_DAG_info: DAG_info_is_complete:
False

The last DAG generated is shown below. It adds the final partition/group
PR7_1. PR6_1 is now complete and since PR7_1 is the last partition/group
in the DAG, PR7_1 is complete and the DAG is complete.

1
 task: PR1_1, fanouts:['PR2_1', 'PR2_3'], fanins:[], faninsNB:['PR2_2L'], collapse:[], fanin_sizes:[], faninNB_sizes:[2], task_inputs:(), ToBeContinued:False, fanout_fanin_faninNB_collapse_groups_are_ToBeContinued:False
2
 task: PR2_1, fanouts:[], fanins:[], faninsNB:['PR2_2L'], collapse:[], fanin_sizes:[], faninNB_sizes:[2], task_inputs:('PR1_1-PR2_1',), ToBeContinued:False, fanout_fanin_faninNB_collapse_groups_are_ToBeContinued:False
3
 task: PR2_2L, fanouts:['PR3_1'], fanins:[], faninsNB:['PR3_2'], collapse:[], fanin_sizes:[], faninNB_sizes:[2], task_inputs:('PR2_1-PR2_2L', 'PR1_1-PR2_2L'), ToBeContinued:False, fanout_fanin_faninNB_collapse_groups_are_ToBeContinued:False
4
 task: PR2_3, fanouts:[], fanins:['PR3_3'], faninsNB:[], collapse:[], fanin_sizes:[2], faninNB_sizes:[], task_inputs:('PR1_1-PR2_3',), ToBeContinued:False, fanout_fanin_faninNB_collapse_groups_are_ToBeContinued:False
5
 task: PR3_1, fanouts:[], fanins:[], faninsNB:['PR3_2'], collapse:[], fanin_sizes:[], faninNB_sizes:[2], task_inputs:('PR2_2L-PR3_1',), ToBeContinued:False, fanout_fanin_faninNB_collapse_groups_are_ToBeContinued:False
6
 task: PR3_2, fanouts:[], fanins:['PR3_3'], faninsNB:[], collapse:[], fanin_sizes:[2], faninNB_sizes:[], task_inputs:('PR2_2L-PR3_2', 'PR3_1-PR3_2'), ToBeContinued:False, fanout_fanin_faninNB_collapse_groups_are_ToBeContinued:False
7
 task: PR3_3, fanouts:[], fanins:[], faninsNB:[], collapse:[], fanin_sizes:[], faninNB_sizes:[], task_inputs:('PR3_2-PR3_3', 'PR2_3-PR3_3'), ToBeContinued:False, fanout_fanin_faninNB_collapse_groups_are_ToBeContinued:False
8
 task: PR4_1, fanouts:[], fanins:[], faninsNB:[], collapse:['PR5_1'], fanin_sizes:[], faninNB_sizes:[], task_inputs:(), ToBeContinued:False, fanout_fanin_faninNB_collapse_groups_are_ToBeContinued:False
9
 task: PR5_1, fanouts:[], fanins:[], faninsNB:[], collapse:[], fanin_sizes:[], faninNB_sizes:[], task_inputs:('PR4_1-PR5_1',), ToBeContinued:False, fanout_fanin_faninNB_collapse_groups_are_ToBeContinued:False
10
 task: PR6_1, fanouts:[], fanins:[], faninsNB:[], collapse:['PR7_1'], fanin_sizes:[], faninNB_sizes:[], task_inputs:(), ToBeContinued:False, fanout_fanin_faninNB_collapse_groups_are_ToBeContinued:False
11
 task: PR7_1, fanouts:[], fanins:[], faninsNB:[], collapse:[], fanin_sizes:[], faninNB_sizes:[], task_inputs:('PR6_1-PR7_1',), ToBeContinued:False, fanout_fanin_faninNB_collapse_groups_are_ToBeContinued:False

DAG_infoBuffer_Monitor_for_Lambdas: print_DAG_info: DAG states:
PR1_1
1
PR2_1
...
PR6_1
10
PR7_1
11

DAG_infoBuffer_Monitor_for_Lambdas: print_DAG_info: DAG leaf task start states
1
8
10

DAG_infoBuffer_Monitor_for_Lambdas: print_DAG_info: DAG_tasks:
...
Same as the above DAGs
...

DAG_infoBuffer_Monitor_for_Lambdas: print_DAG_info: DAG_leaf_tasks:
PR1_1
PR4_1
PR6_1

DAG_infoBuffer_Monitor_for_Lambdas: print_DAG_info: DAG_leaf_task_inputs:
()
()
()

# We published version 6 and since this the ADG is complete we publish
# version 7.
DAG_infoBuffer_Monitor_for_Lambdas: print_DAG_info: DAG_version_number:
7
# The ADG is now complete
DAG_infoBuffer_Monitor_for_Lambdas: print_DAG_info: DAG_info_is_complete:
True



Note: The call to 
  logging.shutdown()
Informs the logging system to perform an orderly shutdown by flushing 
and closing all handlers. This should be called at application exit and no 
further use of the logging system should be made after this call.
logging.shutdown()
#time.sleep(3)   #not needed due to shutdwn
os._exit(0)
"""
# Input the infomation generatd by python -m wukongdnc.dag.dask_dag
def input_DAG_info():
    with open('./DAG_info.pickle', 'rb') as handle:
        DAG_info = cloudpickle.load(handle)
    return DAG_info

def run():

    # high-level:
    # 1 create the fanins and faninNBs locally or on the server
    #   - if using worker processes, the fanins and faninNBs must be remote on the tcp_server
    #   - if using threads, the fanins and faninNBs can be ermote or local.
    # 2 start the threads/processes/lambdas
    #   - if using thrrads to simulate lambdas, start leaf node threads
    #   - if using lambas, start leaf node lambdas
    #   - if using workers, start either thread or process workers
    #   - if using multithreaded worker processes, start the worker processes which start their internal threads
    # 3 if not using lambdas, 
    #   - if not using multithreaded worker processes, join the thread/process workers
    #   - if using multithreaded worker processes. workers will start and join their threads
    #     then we join the multithreaded worker processes. 
    #assserts on configuration:
    try:
        msg = "[Error]: DAG_executor_driver: if USING_WORKERS then run_fanout_tasks_locally must also be true."
        assert not (DAG_executor_constants.USING_WORKERS and not DAG_executor_constants.RUN_ALL_TASKS_LOCALLY) , msg
    except AssertionError:
        logger.exception("[Error]: assertion failed")
        if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
            logging.shutdown()
            os._exit(0)
    #assertOld:
    #if USING_WORKERS:
    #    if not RUN_ALL_TASKS_LOCALLY:
    #        # running in Lambdas so no schedule of tasks on a pool of executors
    #        # i.e., schedule tasks using DFS_paths
    #        logger.error("Error: DAG_executor_driver: if USING_WORKERS then run_fanout_tasks_locally must also be true.")

    # reads from default file './DAG_info.pickle'
    #file_name_foo = "./DAG_info_Group" + ".pickle"
    logger.info("DAG_executor_driver: dask version: " + str(dask.__version__))
    file_name_foo = "./DAG_info" + ".pickle"
    DAG_info = DAG_Info.DAG_info_fromfilename(file_name_foo)
    logger.info("DAG_executor_driver: sucessfully read DAG_info")
    #logging.shutdown()
    #os._exit(0)
    #DAG_info = input_DAG_info()
    
    DAG_map = DAG_info.get_DAG_map()
    all_fanin_task_names = DAG_info.get_all_fanin_task_names()
    all_fanin_sizes = DAG_info.get_all_fanin_sizes()
    all_faninNB_task_names = DAG_info.get_all_faninNB_task_names()
    all_faninNB_sizes = DAG_info.get_all_faninNB_sizes()
    all_fanout_task_names = DAG_info.get_all_fanout_task_names()
    # Note: all fanout_sizes is not needed since fanouts are fanins that have size 1
    DAG_states = DAG_info.get_DAG_states()
    DAG_leaf_tasks = DAG_info.get_DAG_leaf_tasks()
    DAG_leaf_task_start_states = DAG_info.get_DAG_leaf_task_start_states()
    DAG_tasks = DAG_info.get_DAG_tasks()
    DAG_version_number = DAG_info.get_DAG_version_number()
    DAG_is_complete = DAG_info.get_DAG_info_is_complete()
    DAG_number_of_tasks = DAG_info.get_DAG_number_of_tasks()


    # Note: if we are using_lambdas, we null out DAG_leaf_task_inputs after we get it here
    # (by calling DAG_info.set_DAG_leaf_task_inputs_to_None() below). So make a copy.
    if DAG_executor_constants.RUN_ALL_TASKS_LOCALLY:
        DAG_leaf_task_inputs = DAG_info.get_DAG_leaf_task_inputs()
    else:
        DAG_leaf_task_inputs = copy.copy(DAG_info.get_DAG_leaf_task_inputs())

        # For lambdas, null out the leaf task inputs in DAG_info since we pass DAG_info in the
        # payload to all the lambda executors and the leaf task inputs may be large.
        # Note: When we are using thread or process workers then the workers read 
        # DAG_info from a file at the start of their execution. We are not nullng
        # out the leaf task inputs for workers (non-lambda) since we do not pass them
        # on invokes.

        # Null out DAG_leaf_task_inputs.
        DAG_info.set_DAG_leaf_task_inputs_to_None()
        # Null out task inputs in state infomation of leaf tasks
        for start_state in DAG_leaf_task_start_states:
            # Each leaf task's state has the leaf tasks's input. Null it out.
            state_info = DAG_map[start_state]
            state_info.task_inputs = None

#rhc cleanup
    output_DAG = True
    # add-0bec4d19-bce6-4394-ad62-9b0eab3081a9
    if output_DAG:
        logger.info("DAG_executor_driver at start: Output DAG:")
        # FYI:
        logger.info("DAG_executor_driver: DAG_map:")
        for key, value in DAG_map.items():
            logger.info(str(key))
            logger.info(str(value))
        logger.info("  ")
        
        logger.info("DAG_executor_driver: DAG states:")         
        for key, value in DAG_states.items():
            logger.info(str(key))
            logger.info(str(value))
        logger.info("   ")
        logger.info("DAG_executor_driver: DAG leaf task start states")
        for start_state in DAG_leaf_task_start_states:
            logger.info(str(start_state))
        logger.info("")
        logger.info("DAG_executor_driver: DAG_tasks:")
        for key, value in DAG_tasks.items():
            logger.info(str(key)) # logger doesn't like this part: ' : ', str(value))
        logger.info("")
        logger.info("DAG_executor_driver: DAG_leaf_tasks:")
        for task_name in DAG_leaf_tasks:
            logger.info(task_name)
        logger.info("") 
        logger.info("DAG_executor_driver: DAG_leaf_task_inputs:")
        for inp in DAG_leaf_task_inputs:
            logger.info(str(inp))
        #print() 
        logger.info("'")
        logger.info("DAG_version: " + str(DAG_version_number))
        logger.info("")
        logger.info("DAG_is_complete: " + str(DAG_is_complete))
        logger.info("")
        logger.info("DAG_number_of_tasks: " + str(DAG_number_of_tasks))
        logger.info("")
        

#rhc cleanup
        #from . import BFS_Shared
        #logger.trace("shared_groups_mapDDDD:")
        #for (k,v) in BFS_Shared.shared_groups_map.items():
        #    logger.trace(str(k) + ", (" + str(v[0]) + "," + str(v[1]) + ")")

    #ResetRedis()
    
    #start_time = time.time()
	
#############################
#Note: if using Lambdas to store synch objects: SERVERLESS_SYNC = False in constants.py; set to True
#      when storing synch objects in Lambdas.
#############################
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as websocket:

        # synch_objects are stored in local memory or on the tcp_Server or in InfinX Executors
        if DAG_executor_constants.STORE_FANINS_FANINNBS_LOCALLY:
            try:
                msg = "[Error]: DAG_executor_driver - Configuration Error: DAG_executor_driver: store objects locally but using worker processes," \
                    + " which must use remote objects (on server)."
                assert not(DAG_executor_constants.USING_WORKERS and not DAG_executor_constants.USING_THREADS_NOT_PROCESSES) , msg
            except AssertionError:
                logger.exception("[Error]: assertion failed")
                if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
                    logging.shutdown()
                    os._exit(0)
            #assertOld:
            # store fanin and faninNBs locally so not using websocket to tcp_server
            #if USING_WORKERS and not USING_THREADS_NOT_PROCESSES: # using processes
            #    logger.error("[Error]: Configuration Error: DAG_executor_driver: store objects locally but using worker processes,"
            #       + " which must use remote objects (on server).")
            # cannot be multiprocessing, may or may not be pooling, running all tasks locally (no Lambdas)
            # server is global variable obtained: from .DAG_executor_synchronizer import server
            if DAG_executor_constants.CREATE_ALL_FANINS_FANINNBS_ON_START:
                # create fanins and faninNBs using all_fanin_task_names, all_fanin_sizes, all_faninNB_task_names, all_faninNB_sizes
                # server is a global variable in DAG_executor_synchronizer.py - it is used to simulate the
                # tcp_server when running locally.
                server.create_all_fanins_and_faninNBs_locally(DAG_map,DAG_states, DAG_info, all_fanin_task_names, all_fanin_sizes, all_faninNB_task_names, all_faninNB_sizes)

                if DAG_executor_constants.USING_WORKERS:
                    # Based on asssert above, using worker threads when 
                    # using local synch objects 
                    # leaf task states (a task is identified by its state) are put in work_queue
                    for state in DAG_leaf_task_start_states:
                        #thread_work_queue.put(state)
                        #work_queue.put(DAG_states[name])
                        state_info = DAG_map[state]
                        task_inputs = state_info.task_inputs 
                        task_name = state_info.task_name
                        dict_of_results =  {}
                        dict_of_results[task_name] = task_inputs
                        work_tuple = (state,dict_of_results)
                        work_queue.put(work_tuple)

                        #work_queue.put(state)

                #else: Nothing to do; we do not use a work_queue if we are not using workers
            else:
                if DAG_executor_constants.USING_WORKERS:
                    # leaf task states (a task is identified by its state) are put in work_queue
                    for state in DAG_leaf_task_start_states:
                        #thread_work_queue.put(state)
                        state_info = DAG_map[state]
                        task_inputs = state_info.task_inputs 
                        task_name = state_info.task_name
                        dict_of_results =  {}
                        dict_of_results[task_name] = task_inputs
                        work_tuple = (state,dict_of_results)
                        work_queue.put(work_tuple)
                        #work_queue.put(state)
                #else: Nohing to do; we do not use a work_queue if we are not using workers
        else: # store remotely

            groups_partitions = {}
            # For pagerank computation we ned to read a partition of 
            # nodes. For real lambdas, this will be from an S3 bucket
            # or somewhere. To avoid this, we can let this DAG_executor_driver
            # read the partition files and pass them to the Lambdas it starts
            # in the Lambda's payload. When BYPASS_CALL_LAMBDA_CLIENT_INVOKE it means we 
            # are not actually running the real Lambda code on AWS, we are 
            # bypassing the call to invoke real AWS Lambdas and running the code
            # locally, in which case we can read the group/partition file objects
            # from local files.
#rhc: group partitions
            if DAG_executor_constants.INPUT_ALL_GROUPS_PARTITIONS_AT_START:
            # hardcoded for testing rel lambdas. May want to enabe this generally in
            # which case we will need the partition/group names, which BFS could
            # write to a file.
                group_partition_names = ["PR1_1","PR2_1","PR2_2L","PR2_3","PR3_1","PR3_2","PR3_3","PR4_1","PR5_1","PR6_1","PR7_1"]
                for task_file_name in group_partition_names:
                    # task_file_name is, e.g., "PR1_1" not "PR1_1.pickle"
                    # We check for task_file_name ending with "L" for loop below,
                    # so we make this check esy by having 'L' at the end (endswith)
                    # instead of having to parse ("PR1_1.pickle")
                    complete_task_file_name = './'+task_file_name+'.pickle'

                    try:
                        with open(complete_task_file_name, 'rb') as handle:
                            partition_or_group = (cloudpickle.load(handle))
                    except EOFError:
                        logger.exception("[Error]: PageRank_Function: EOFError:"
                            + " complete_task_file_name:" + str(complete_task_file_name))
                        if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
                            logging.shutdown()
                            os._exit(0)

                    groups_partitions[task_file_name] = partition_or_group
                #print("groups_partitions:")
                #keys = list(groups_partitions.keys())
                #for key in keys:
                #    print(key + ":")
                #    g_p = groups_partitions[key]
                #    for node in g_p:
                #        print(str(node))
                #logging.shutdown()
                #os._exit(0)

            # server will be None
            logger.trace("DAG_executor_driver: Connecting to TCP Server at %s." % str(TCP_SERVER_IP))
            websocket.connect(TCP_SERVER_IP)
            logger.trace("DAG_executor_driver: Successfully connected to TCP Server.")
            if DAG_executor_constants.CREATE_ALL_FANINS_FANINNBS_ON_START:
                # create fanins and faninNbs on tcp_server or in InfiniX lambdas 
                # all at the start of driver execution
                if DAG_executor_constants.RUN_ALL_TASKS_LOCALLY and DAG_executor_constants.USING_WORKERS:
                    # if not stored locally, i.e., and either threads or process workers, then create a remote 
                    # process queue if using processs and use a local work queue for the threads.
                    if not DAG_executor_constants.USING_THREADS_NOT_PROCESSES:
                        #Note: using workers and processes means not STORE_FANINS_FANINNBS_LOCALLY
                        #Need to create the process_work_queue; do it in the same batch
                        # of fanin and faninNB creates
                        #manager = Manager()
                        #data_dict = manager.dict()
                        #num_DAG_tasks = len(DAG_tasks)
                        #process_work_queue = manager.Queue(maxsize = num_DAG_tasks)
                        num_tasks_to_execute = len(DAG_tasks)
                        process_work_queue = Work_Queue_Client(websocket,2*num_tasks_to_execute)
                        #process_work_queue.create()
                        create_fanins_and_faninNBs_and_work_queue(websocket,num_tasks_to_execute,DAG_map,DAG_states, DAG_info, all_fanin_task_names, all_fanin_sizes, 
                            all_faninNB_task_names, all_faninNB_sizes,
                            groups_partitions)
                        # since not using real lambdas, groups_partitions is []

                        #Note: you can reversed() this list of leaf node start states to reverse the order of 
                        # appending leaf nodes during testing
                        list_of_work_queue_values = []
                        for state in DAG_leaf_task_start_states:
                            #logger.trace("dummy_state: " + str(dummy_state))
                            state_info = DAG_map[state]
                            task_inputs = state_info.task_inputs 
                            task_name = state_info.task_name
                            dict_of_results =  {}
                            dict_of_results[task_name] = task_inputs
                            work_tuple = (state,dict_of_results)
                            list_of_work_queue_values.append(work_tuple)
                            logger.trace("DAG_executor_driver: list_of_work_queue_values:"
                                + str(list_of_work_queue_values))
                            #process_work_queue.put(work_tuple)
                            #process_work_queue.put(state)
                        # batch put work in remote work_queue
                        process_work_queue.put_all(list_of_work_queue_values)
                    else:
                        create_fanins_and_faninNBs(websocket,DAG_map,DAG_states, DAG_info, all_fanin_task_names, all_fanin_sizes, 
                            all_faninNB_task_names, all_faninNB_sizes,
                            groups_partitions)
                        # since not using real lambdas, groups_partitions is []

                        # leaf task states (a task is identified by its state) are put in the work_queue
                        for state in DAG_leaf_task_start_states:
                            #thread_work_queue.put(state)
                            state_info = DAG_map[state]
                            task_inputs = state_info.task_inputs 
                            task_name = state_info.task_name
                            dict_of_results =  {}
                            dict_of_results[task_name] = task_inputs
                            work_tuple = (state,dict_of_results)
                            work_queue.put(work_tuple)

                            #work_queue.put(state)
                # This is true: not (RUN_ALL_TASKS_LOCALLY and USING_WORKERS), i.e.,
                # one of the conditions is false.
                # Note: This configuration is never used: (not RUN_ALL_TASKS_LOCALLY) and USING_WORKERS
                # as not RUN_ALL_TASKS_LOCALLY means we are using lambdas and we do not use workers
                # when we are using lambdas.
                elif DAG_executor_constants.RUN_ALL_TASKS_LOCALLY and not DAG_executor_constants.USING_WORKERS:
                    # not using workers, use threads to simulate lambdas. no work queue so do not
                    # put leaf node start states in work queue. threads are created to execute
                    # fanout tasks and fanin tasks (like lambdas)
                    try:
                        msg = "[Error]: DAG_executor_driver: not USING_WORKERS but using processes."
                        assert DAG_executor_constants.USING_THREADS_NOT_PROCESSES, msg
                    except AssertionError:
                        logger.exception("[Error]: assertion failed")
                        if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
                            logging.shutdown()
                            os._exit(0)
                    #assertOld:
                    #if not USING_THREADS_NOT_PROCESSES:
                    #    logger.error("[Error]: DAG_executor_driver: not USING_WORKERS but using processes.")
    
                    # just create a batch of fanins and faninNBs on server - no remote work queue wen using
                    # thread workers or using lambdas.         
                    create_fanins_and_faninNBs(websocket,DAG_map,DAG_states, DAG_info, all_fanin_task_names, all_fanin_sizes, 
                        all_faninNB_task_names, all_faninNB_sizes,
                        groups_partitions)
                    # since not using real lambdas, groups_partitions is []
                else:
                    # not RUN_ALL_TASKS_LOCALLY and not using workers (must be true (since 
                    # (not RUN_ALL_TASKS_LOCALLY) and USING_WORKERS is never used in that case.)
                    try:
                        msg = "[Error]: DAG_executor_driver: USING_WORKERS but using lambdas."
                        assert not DAG_executor_constants.USING_WORKERS, msg
                    except AssertionError:
                        logger.exception("[Error]: assertion failed")
                        if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
                            logging.shutdown()
                            os._exit(0)
                    #assertOld:
                    #if USING_WORKERS:
                    #    logger.error("[Error]: DAG_executor_driver: USING_WORKERS but using lambdas.")
                    try:
                        msg = "[Error]: DAG_executor_driver: interal error: DAG_executor_driver: RUN_ALL_TASKS_LOCALLY shoudl be false."
                        assert not DAG_executor_constants.RUN_ALL_TASKS_LOCALLY, msg
                    except AssertionError:
                        logger.exception("[Error]: assertion failed")
                        if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
                            logging.shutdown()
                            os._exit(0)
                    #assertOld:
                    #if RUN_ALL_TASKS_LOCALLY:
                    #    logger.error("[Error]: DAG_executor_driver: interal error: DAG_executor_driver: RUN_ALL_TASKS_LOCALLY shoudl be false.")

                    # not RUN_ALL_TASKS_LOCALLY so using lambdas (real or simulatd)
                    # So do not put leaf tasks in work queue
#rhc: groups partitions
                    if not DAG_executor_constants.RUN_ALL_TASKS_LOCALLY and DAG_executor_constants.STORE_SYNC_OBJECTS_IN_LAMBDAS and DAG_executor_constants.SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS:
                        # storing sync objects in lambdas and snc objects trigger their tasks
                        create_fanins_and_faninNBs_and_fanouts(websocket,DAG_map,DAG_states,DAG_info,
                            all_fanin_task_names,all_fanin_sizes,all_faninNB_task_names,all_faninNB_sizes,
                            all_fanout_task_names,DAG_leaf_tasks,DAG_leaf_task_start_states,
                            groups_partitions)
                        # We are not using real lambdas to execute tasks so groups_partitions is [].

                        # call server to trigger the leaf tasks
                        process_leaf_tasks_batch(websocket)
                        # Informs the logging system to perform an orderly 
                        # shutdown by flushing and closing all handlers. 
                        # This should be called at application exit and no 
                        # further use of the logging system should be made 
                        # after this call.
                    else:
                        # storing sync objects remotely; they do not trigger their tasks to run
                        # in the same lamba that stores the sync object. So, e.g., fanouts are
                        # done by calling lambdas to excute the fanout task (besides the becomes 
                        # task.)
                        
                        create_fanins_and_faninNBs(websocket,DAG_map,DAG_states, DAG_info, all_fanin_task_names, all_fanin_sizes, 
                            all_faninNB_task_names, all_faninNB_sizes,
                            groups_partitions)
                            # For pagerank computation we need to read a partition of 
                            # nodes. For real lambdas, this could be from an S3 bucket
                            # or somewhere. To avoid this, we can let this DAG_executor_driver
                            # read the partition files and pass them to the Lambdas it starts
                            # in the Lambda's payload. We pass groups_parition here when we create
                            # the faninNBs since the faninNBs will start real lambdas.
                            # When BYPASS_CALL_LAMBDA_CLIENT_INVOKE it means we 
                            # are not actually running the real Lambda code on AWS, we are 
                            # bypassing the call to invoke real AWS Lambdas and running the code
                            # locally, in which case we can read the group/partition file objects
                            # from local files.
            else:
                # going to create fanin and faninNBs on demand, i.e., as we execute
                # operations on them. But still want to create process_work_queue
                # by itself at the beginning of drivr execuion.
                if DAG_executor_constants.RUN_ALL_TASKS_LOCALLY and DAG_executor_constants.USING_WORKERS:
                    if not DAG_executor_constants.USING_THREADS_NOT_PROCESSES:
                        #Note: using workers means not STORE_FANINS_FANINNBS_LOCALLY
                        #Need to create the process_work_queue
                        #manager = Manager()
                        #data_dict = manager.dict()
                        #num_DAG_tasks = len(DAG_tasks)
                        #process_work_queue = manager.Queue(maxsize = num_DAG_tasks)
                        num_tasks_to_execute = len(DAG_tasks)
                        process_work_queue = Work_Queue_Client(websocket,2*num_tasks_to_execute)
                        process_work_queue.create()
                        list_of_work_queue_values = []
                        for state in DAG_leaf_task_start_states:
                            state_info = DAG_map[state]
                            task_inputs = state_info.task_inputs 
                            task_name = state_info.task_name
                            dict_of_results =  {}
                            dict_of_results[task_name] = task_inputs
                            work_tuple = (state,dict_of_results)
                            list_of_work_queue_values.append(work_tuple)
                            #process_work_queue.put(work_tuple)
                            #process_work_queue.put(state)
                        process_work_queue.put_all(list_of_work_queue_values)
                        #num_tasks_to_execute = len(DAG_tasks)
                        #create_fanins_and_faninNBs_and_work_queue(websocket,num_tasks_to_execute,DAG_map,DAG_states, DAG_info, all_fanin_task_names, all_fanin_sizes, all_faninNB_task_names, all_faninNB_sizes)
                    else:
                        # leaf task states (a task is identified by its state) are put in work_queue
                        for state in DAG_leaf_task_start_states:
                            #thread_work_queue.put(state) 
                            state_info = DAG_map[state]
                            task_inputs = state_info.task_inputs 
                            task_name = state_info.task_name
                            dict_of_results =  {}
                            dict_of_results[task_name] = task_inputs
                            work_tuple = (state,dict_of_results)
                            work_queue.put(work_tuple)
                            #work_queue.put(state) 

                # This is true: not (RUN_ALL_TASKS_LOCALLY and USING_WORKERS), i.e.,
                # one of the two conditions is false.
                # Note: This configuration is never used: (not RUN_ALL_TASKS_LOCALLY) and USING_WORKERS
                # as not RUN_ALL_TASKS_LOCALLY means we are using lambdas and we do not use workers
                # when we are using lambdas.
                elif DAG_executor_constants.RUN_ALL_TASKS_LOCALLY and not DAG_executor_constants.USING_WORKERS:
                    # not using workers, use threads to simulate lambdas. no work queue so do not
                    # put leaf node start states in work queue. threads are created to execute
                    # fanout tasks and fanin tasks (like lambdas)
                    try:
                        msg = "[Error]: DAG_executor_driver: not USING_WORKERS but using processes."
                        assert DAG_executor_constants.USING_THREADS_NOT_PROCESSES , msg
                    except AssertionError:
                        logger.exception("[Error]: assertion failed")
                        if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
                            logging.shutdown()
                            os._exit(0)
                    #if not USING_THREADS_NOT_PROCESSES:
                    #    logger.error("[Error]: DAG_executor_driver: not USING_WORKERS but using processes.")
                else:
                    # not RUN_ALL_TASKS_LOCALLY and not using workers must be true (since 
                    # (not RUN_ALL_TASKS_LOCALLY) and USING_WORKERS is never used.
                    try:
                        msg = "[Error]: DAG_executor_driver: USING_WORKERS but using lambdas."
                        assert not DAG_executor_constants.USING_WORKERS, msg
                    except AssertionError:
                        logger.exception("[Error]: assertion failed")
                        if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
                            logging.shutdown()
                            os._exit(0)
                    #assertOld:
                    #if USING_WORKERS:
                    #    logger.error("[Error]: DAG_executor_driver: USING_WORKERS but using lambdas.")
                    try:
                        msg = "[Error]: DAG_executor_driver: interal error: DAG_executor_driver: RUN_ALL_TASKS_LOCALLY shoudl be false."
                        assert not DAG_executor_constants.RUN_ALL_TASKS_LOCALLY, msg
                    except AssertionError:
                        logger.exception("[Error]: assertion failed")
                        if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
                            logging.shutdown()
                            os._exit(0)
                    #assertOld:
                    #if RUN_ALL_TASKS_LOCALLY:
                    #    logger.error("[Error]: DAG_executor_driver: interal error: DAG_executor_driver: RUN_ALL_TASKS_LOCALLY should be false.")

                    # not RUN_ALL_TASKS_LOCALLY so using lambdas, which do not use a work queue 
                    # So do not put leaf tasks in work queue and do not create a work queue
                    if not DAG_executor_constants.RUN_ALL_TASKS_LOCALLY and DAG_executor_constants.STORE_SYNC_OBJECTS_IN_LAMBDAS and DAG_executor_constants.SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS:
                        # storing sync objects in lambdas and snc objects trigger their tasks
                        #create_fanins_and_faninNBs_and_fanouts(websocket,DAG_map,DAG_states,DAG_info,
                        #    all_fanin_task_names,all_fanin_sizes,all_faninNB_task_names,all_faninNB_sizes,
                        #    all_fanout_task_names,DAG_leaf_tasks,DAG_leaf_task_start_states)
                        # call server to trigger the leaf tasks
                        process_leaf_tasks_batch(websocket)
                        # Informs the logging system to perform an orderly 
                        # shutdown by flushing and closing all handlers. 
                        # This should be called at application exit and no 
                        # further use of the logging system should be made 
                        # after this call.
                    #else:
                        # storing sync objects remotely; they do not trigger their tasks to run
                        # in the same lamba that strores the sync object. So, e.g., fanouts are
                        # done by calling lambdas to excute the fanout task (besides the becomes 
                        # task.)
                        #create_fanins_and_faninNBs(websocket,DAG_map,DAG_states, DAG_info, all_fanin_task_names, all_fanin_sizes, all_faninNB_task_names, all_faninNB_sizes)

        # Note: We now send a close_all() to the tcp_server so that it clears
        # the list of synchronizers that have been created. Thus, tcp_server
        # ends in a state that we can execute another DAG and create synhronizers
        # on the fly and they will be created, i.e., the tcp_server cleared
        # its list of synchronizers so on the first ue of a synhronizers we will
        # see that the synchroizer does not exist and create it (on the fly).
        # Without the clear list, the tcp_sever would thinnk the synchronizer already
        # existed and so would not create it and use the existing "old" one, which
        # does not work. 
        # 
        # We inented the rest of the code so that it would be within the with
        # clause of the websocket allowing us to call close_all(wbsocket)
        # with websocket within scope.

        # FYI
        logger.trace("DAG_executor_driver: DAG_leaf_tasks: " + str(DAG_leaf_tasks))
        logger.trace("DAG_executor_driver: DAG_leaf_task_start_states: " + str(DAG_leaf_task_start_states))
        #commented out for MM
        #logger.trace("DAG_executor_driver: DAG_leaf_task_inputs: " + str(DAG_leaf_task_inputs))

        # Done with process_work_queue 
        process_work_queue = None

        #print("work_queue:")
        #for start_state in X_work_queue.queue:
        #   print(start_state)

        #if RUN_ALL_TASKS_LOCALLY and USING_WORKERS and not USE_MULTITHREADED_MULTIPROCESSING:
        # keep list of threads/processes in pool so we can join() them
        thread_proc_list = []

        # count of threads/processes created. We will create DAG_executor_constants.py NUM_WORKERS
        # if we are USING_WORKERS. We will create some number of threads if we are simulating the 
        # use of creating Lambdas, e.g., at fan-out points.
        # We use a different counter if USE_MULTITHREADED_MULTIPROCESSING
        if DAG_executor_constants.RUN_ALL_TASKS_LOCALLY and not DAG_executor_constants.USE_MULTITHREADED_MULTIPROCESSING:
            num_threads_created = 0

        # dfined befre used - but we only use these for multiprocessing
        completed_tasks_counter = None
        completed_workers_counter = None

        if DAG_executor_constants.RUN_ALL_TASKS_LOCALLY and not DAG_executor_constants.USING_THREADS_NOT_PROCESSES:
            try:
                msg = "[Error]: DAG_executor_driver: not USING_WORKERS but using processes."
                assert DAG_executor_constants.USING_WORKERS, msg
            except AssertionError:
                logger.exception("[Error]: assertion failed")
                if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
                    logging.shutdown()
                    os._exit(0)
            #assertOld:
            #if not USING_WORKERS:
            #    logger.error("[Error]: DAG_executor_driver: not USING_WORKERS but using processes.")

            # multiprocessing. processes share a counter that counts the number of tasks that have been executed
            # and uses this counter to determine when all tasks have been excuted so workers can stop (by 
            # putting -1 in the work_queue - when worker gets -1 it puts -1 for the next worker. So execution
            # ends with -1 in the work queue, which is put there by the last worker to stop.)
    #rhc: counter
            completed_tasks_counter = CounterMP()
            completed_workers_counter = CounterMP()
            # used by a logger for multiprocessing
            log_queue = multiprocessing.Queue(-1)
            # used for multiprocessor logging - receives log messages from processes
            listener = multiprocessing.Process(target=listener_process, args=(log_queue, listener_configurer))
            listener.start()    # joined at the end

        if DAG_executor_constants.USE_MULTITHREADED_MULTIPROCESSING:
            # Config: A6
            # keep list of threads/processes in pool so we can join() them
            multithreaded_multiprocessing_process_list = []
            num_processes_created_for_multithreaded_multiprocessing = 0
            #num_processes_created_for_multithreaded_multiprocessing = create_multithreaded_multiprocessing_processes(num_processes_created_for_multithreaded_multiprocessing,multithreaded_multiprocessing_process_list,counter,process_work_queue,data_dict,log_queue,worker_configurer)
    #rhc: counter
    # # tasks_completed_counter, workers_completed_counter
            #num_processes_created_for_multithreaded_multiprocessing = create_multithreaded_multiprocessing_processes(num_processes_created_for_multithreaded_multiprocessing,multithreaded_multiprocessing_process_list,completed_tasks_counter,log_queue,worker_configurer)
            num_processes_created_for_multithreaded_multiprocessing = create_multithreaded_multiprocessing_processes(num_processes_created_for_multithreaded_multiprocessing,multithreaded_multiprocessing_process_list,completed_tasks_counter,completed_workers_counter,log_queue,worker_configurer)
            start_time = time.time()
            for thread_proc in multithreaded_multiprocessing_process_list:
                thread_proc.start()
        else: # multi threads or multi-processes, thread and processes may be workers using work_queue
            # if we are not using lambdas, and we are not using a worker pool, create a thread for each
            # leaf task. If we are not using lambdas but we are using a worker pool, create at least 
            # one worker and at most num_worker workers. If we are using workers, there may be more
            # leaf tasks than workers, but that is okay since we put all the leaf task states in the 
            # work queue and the created workers will withdraw them.

            if not (not DAG_executor_constants.RUN_ALL_TASKS_LOCALLY and DAG_executor_constants.STORE_SYNC_OBJECTS_IN_LAMBDAS and DAG_executor_constants.SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS):
                # we are not having sync objects trigger their tasks in lambdas
                for start_state, task_name, inp in zip(DAG_leaf_task_start_states, DAG_leaf_tasks, DAG_leaf_task_inputs):
                    # The state of a DAG executor contains only one application specific member, which is the
                    # state number of the task to execute. Leaf task information is in DAG_leaf_task_start_states
                    # and DAG_leaf_tasks (which are the task names).
                    DAG_exec_state = DAG_executor_State(function_name = "DAG_executor", function_instance_ID = str(uuid.uuid4()), state = start_state)
                    logger.trace("DAG_executor_driver: Starting DAG_executor for task " + task_name)

                    if DAG_executor_constants.RUN_ALL_TASKS_LOCALLY:
                        #Note: If we are using partitions, the number of worker threads should be
                        #  1 unless there are multiple connected components (#CC), in which case
                        # we can use up to #CC workers since CC leaf tasks can be processed in parallel.
                        # We would need to know the number of CCs (leaf nodes) to set #num of workers.
                        # Large graphs have many CCs?
                        
                        # not using Lambdas
                        if DAG_executor_constants.USING_THREADS_NOT_PROCESSES: # create threads
                            # Config: A4_local, A4_Remote
                            try:
                                if not DAG_executor_constants.USING_WORKERS:
                                    # pass the state/task the thread is to execute at the start of its DFS path
                                    # Note: continued_task defaults to False for DAG_exec_state
                                    DAG_exec_state = DAG_executor_State(function_name = "DAG_executor:"+task_name, function_instance_ID = str(uuid.uuid4()), state = start_state)
                                else:
                                    # workers withdraw their work, i.e., starting state, from the work_queue
                                    DAG_exec_state = None
                                logger.trace("DAG_executor_driver: Starting DAG_executor thread for leaf task " + task_name)
#rhc: lambda inc: will be reading DAG_info from payload as we need to do that for
# incremental DAG generation since pass DAG_info in payload on restarting continued tasks.
# SO ssimulated lambdas use DAG_info parm instead of reading DAG_info from file.
# The latter works when non incremental DAG generation since can read the complete DAG
# at the start. Also using input parm to pass input/output to restarted lambda
# for increnmental DAG generation. So pass it here to be consisent though we won't
# be using arg value since leaf task inputs are in state_info of the task (like Dask)
                                payload = {
                                    # What's not in the payload: DAG_info: since threads/processes read this pickled 
                                    # file at the start of their execution. server: since this is a global variable
                                    # for the threads and processes. for processes it is None since processes send
                                    # messages to the tcp_server, and thus do not use the server object, which is 
                                    # used to simulate the tcp_server when running locally. Input: threads and processes
                                    # get their input from the data_dict. Note the lambdas will be invoked with their 
                                    # input in the payload and will put this
                                    # input in their local data_dict.
                                    "input": None, # get from state_info for the leaf task
                                    "DAG_executor_state": DAG_exec_state,
                                    "DAG_info": DAG_info
                                }

                                """
                                # This is the payload for real lambdas:
                                payload = {
                                    "input": inp,  get from DAG_leaf_task_inputs
                                    "DAG_executor_state": lambda_DAG_exec_state,
                                    "DAG_info": DAG_info
                                }
                                """

                                # Note:
                                # get the current thread instance
                                # thread = current_thread()
                                # report the name of the thread
                                # print(thread.name)
                                if DAG_executor_constants.USING_WORKERS:
                                    thread_name_prefix = "Worker_Thread_leaf_"
                                else:
                                    thread_name_prefix = "Thread_leaf_"
                                thread = threading.Thread(target=DAG_executor.DAG_executor_task, name=(thread_name_prefix+"ss"+str(start_state)), args=(payload,))
                                if DAG_executor_constants.USING_WORKERS:
                                    thread_proc_list.append(thread)
                                else: 
                                    thread.start()
                                num_threads_created += 1
                            except Exception:
                                logger.exception("[ERROR] DAG_executor_driver: Failed to start DAG_executor thread for state." 
                                    + str(start_state))
                                if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
                                    logging.shutdown()
                                    os._exit(0)

                        else:   # multiprocessing - must be using a process pool
                            # Config: A5
                            try:
                                if not DAG_executor_constants.USING_WORKERS:
                                    logger.trace("[ERROR] DAG_executor_driver: Starting multi process leaf tasks but USING_WORKERS is false.")

                                logger.trace("DAG_executor_driver: Starting DAG_executor process for leaf task " + task_name)

                                payload = {
                                    # no payload. We do not need DAG_executor_state since worker processes withdraw
                                    # states from the work_queue
                                }
                                proc_name_prefix = "Worker_leaf_"
                                # processes share these objects: counter,process_work_queue,data_dict,log_queue,worker_configurer.
                                # The worker_configurer() funcion is used for multiprocess logging
                                #proc = Process(target=DAG_executor.DAG_executor_processes, name=(proc_name_prefix+"ss"+str(start_state)), args=(payload,counter,process_work_queue,data_dict,log_queue,worker_configurer,))
                                if not (DAG_executor_constants.COMPUTE_PAGERANK and DAG_executor_constants.USE_SHARED_PARTITIONS_GROUPS):
    #rhc: counter 
    # tasks_completed_counter, workers_completed_counter
                                    #proc = Process(target=DAG_executor.DAG_executor_processes, name=(proc_name_prefix+"ss"+str(start_state)), args=(payload,completed_tasks_counter,log_queue,worker_configurer,
                                    proc = Process(target=DAG_executor.DAG_executor_processes, name=(proc_name_prefix+"ss"+str(start_state)), args=(payload,completed_tasks_counter,completed_workers_counter,log_queue,worker_configurer,
                                        None,None,None,None,None,None,None,None,None,None))

                                else: 
                                    #Note: In DAG_executor_constants, we use: USE_SHARED_PARTITIONS_GROUPS = COMPUTE_PAGERANK and True
                                    # So if USE_SHARED_PARTITIONS_GROUPS is True then COMPUTE_PAGERANK is True
                                    if DAG_executor_constants.USE_PAGERANK_GROUPS_PARTITIONS:
                                        shared_nodes = BFS_Shared.shared_groups
                                        shared_map = BFS_Shared.shared_groups_map
                                        shared_frontier_map = BFS_Shared.shared_groups_frontier_parents_map
                                    else:
                                        shared_nodes = BFS_Shared.shared_partition
                                        shared_map = BFS_Shared.shared_partition_map
                                        shared_frontier_map = BFS_Shared.shared_partition_frontier_parents_map

                                    if DAG_executor_constants.USE_STRUCT_OF_ARRAYS_FOR_PAGERANK:
    #rhc: counter 
    # tasks_completed_counter, workers_completed_counter
                                        #proc = Process(target=DAG_executor.DAG_executor_processes, name=(proc_name_prefix+"ss"+str(start_state)), args=(payload,completed_tasks_counter,log_queue,worker_configurer,
                                        proc = Process(target=DAG_executor.DAG_executor_processes, name=(proc_name_prefix+"ss"+str(start_state)), args=(payload,completed_tasks_counter,completed_workers_counter,log_queue,worker_configurer,
                                            shared_nodes,shared_map,shared_frontier_map,
                                            BFS_Shared.pagerank_sent_to_processes,BFS_Shared.previous_sent_to_processes,BFS_Shared.number_of_children_sent_to_processes,
                                            BFS_Shared.number_of_parents_sent_to_processes,BFS_Shared.starting_indices_of_parents_sent_to_processes,
                                            BFS_Shared.parents_sent_to_processes,BFS_Shared.IDs_sent_to_processes,))
                                    else:
    #rhc: counter 
    # tasks_completed_counter, workers_completed_counter
                                        #proc = Process(target=DAG_executor.DAG_executor_processes, name=(proc_name_prefix+"ss"+str(start_state)), args=(payload,completed_tasks_counter,log_queue,worker_configurer,
                                        proc = Process(target=DAG_executor.DAG_executor_processes, name=(proc_name_prefix+"ss"+str(start_state)), args=(payload,completed_tasks_counter,completed_workers_counter,log_queue,worker_configurer,
                                            shared_nodes,shared_map,shared_frontier_map,
                                            None,None,None,None,None,None,None))
    
                                #proc.start()
                                thread_proc_list.append(proc)
                                #thread.start()
                                num_threads_created += 1
                                #_thread.start_new_thread(DAG_executor.DAG_executor_task, (payload,))
                            except Exception:
                                logger.exception("[ERROR] DAG_executor_driver: Failed to start DAG_executor process for state " 
                                    + str(start_state))
                                if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
                                    logging.shutdown()
                                    os._exit(0)   

                        if DAG_executor_constants.USING_WORKERS and num_threads_created == DAG_executor_constants.NUM_WORKERS:
                            break
                    else:
                        if not DAG_executor_constants.SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS:
                            # Config: A1
                            try:
                                logger.trace("DAG_executor_driver: Starting DAG_Executor_Lambda for leaf task " + task_name)
                                lambda_DAG_exec_state = DAG_executor_State(function_name = "WukongDivideAndConquer:"+task_name, function_instance_ID = str(uuid.uuid4()), state = start_state)
                                logger.trace ("DAG_executor_driver: lambda payload is DAG_info + " + str(start_state) + "," + str(inp))
                                lambda_DAG_exec_state.restart = False      # starting new DAG_executor in state start_state_fanin_task
                                lambda_DAG_exec_state.return_value = None
                                lambda_DAG_exec_state.blocking = False            
                                logger.trace("DAG_executor_driver: Starting Lambda function %s." % lambda_DAG_exec_state.function_name)

                                # We use "inp" for leaf task input otherwise all leaf task lambda Executors will 
                                # receive all leaf task inputs in the DAG_info.leaf_task_inputs and in the state_info.task_inputs
                                # - both of which are nulled out at beginning of driver when we are using lambdas.
                                # If we use "inp" then we will pass only a given leaf task's input to that leaf task. 
                                # For non-lambda, each thread/process reads the DAG_info from a file. This DAG-info has
                                # all the leaf task inputs in it so every thread/process reads all these inputs. This 
                                # can be optimized if necessary, e.g., separate files for leaf tasks and non-leaf tasks.

                                payload = {
                                    "input": inp, # get from DAG_leaf_task_inputs
                                    "DAG_executor_state": lambda_DAG_exec_state,
                                    "DAG_info": DAG_info
                                }
#rhc: group partitions
                                #print("groups_partitions:")
                                #keys = list(groups_partitions.keys())
                                #for key in keys:
                                #    print(key + ":")
                                #    g_p = groups_partitions[key]
                                #    for node in g_p:
                                #        print(str(node))
                                #logging.shutdown()
                                #os._exit(0)

                                if DAG_executor_constants.INPUT_ALL_GROUPS_PARTITIONS_AT_START:
                                    payload["groups_partitions"] = groups_partitions

                                invoke_lambda_DAG_executor(payload = payload, function_name = "WukongDivideAndConquer:" + task_name)
                            except Exception:
                                logger.exception("[ERROR] DAG_executor_driver: Failed to start DAG_executor Lambda.")
                                if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
                                    logging.shutdown()
                                    os._exit(0)  
                        else:
                            # SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS == True so
                            # above we called tcp_server_lambda.process_leaf_tasks_batch
                            # to trigger the leaf tasks.
                            # assert: this should be unreachble - if trigger tassk in their 
                            # lambdas then we should not have entered this loop for 
                            # starting tasks.
                            try:
                                msg = "[Error]: DAG_executor_driver: reached unreachable code for starting triggered tasks"
                                assert False , msg
                            except AssertionError:
                                logger.exception("[Error]: assertion failed")
                                if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
                                    logging.shutdown()
                                    os._exit(0)
                            #assertOld:
                            #logger.error("[ERROR] DAG_executor_driver: reached unreachable code for starting triggered tasks")
                
            #else we started the leaf tasks above with process_leaf_tasks_batch

            # if the number of leaf tasks is less than number_workers, we need to create more workers
            if DAG_executor_constants.RUN_ALL_TASKS_LOCALLY and DAG_executor_constants.USING_WORKERS and num_threads_created < DAG_executor_constants.NUM_WORKERS:
                # starting leaf tasks did not start NUM_WORKERS workers so start NUM_WORKERS-num_threads_created
                # more threads/processes.
                while True:
                    logger.trace("DAG_executor_driver: Starting DAG_executor for non-leaf task.")
                    # assserting if condition is True
                    try:
                        msg = "DAG_executor_driver: worker (pool) threads/processes must run locally (no Lambdas)"
                        assert DAG_executor_constants.RUN_ALL_TASKS_LOCALLY , msg
                    except AssertionError:
                        logger.exception("[Error]: assertion failed")
                        if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
                            logging.shutdown()
                            os._exit(0)
                    #assertOld:
                    if DAG_executor_constants.RUN_ALL_TASKS_LOCALLY:
                        if DAG_executor_constants.USING_THREADS_NOT_PROCESSES:
                            try:
                                # Using workers so do not pass to them a start_state (use state = 0); 
                                # they get their start state from the work_queue
                                DAG_exec_state = DAG_executor_State(function_name = "DAG_executor", function_instance_ID = str(uuid.uuid4()), state = 0)
                                payload = {
                                    "DAG_executor_state": DAG_exec_state
                                }
                                thread_name_prefix = "Worker_thread_non-leaf_"
                                non_leaf_task_name = thread_name_prefix+str(start_state)
                                logger.trace("DAG_executor_driver: Starting DAG_executor worker for non-leaf task " + non_leaf_task_name)
                                thread = threading.Thread(target=DAG_executor.DAG_executor_task, name=(non_leaf_task_name), args=(payload,))
                                thread_proc_list.append(thread)
                                #thread.start()
                                num_threads_created += 1
                            except Exception:
                                logger.exception("[ERROR] DAG_executor_driver: Failed to start DAG_executor worker thread for non-leaf task " 
                                    + task_name)
                                if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
                                    logging.shutdown()
                                    os._exit(0) 
                        else:
                            try:
                                if not DAG_executor_constants.USING_WORKERS:
                                    logger.trace("[ERROR] DAG_executor_driver: Starting multi process non-leaf tasks but USING_WORKERS is false.")
                                
                                logger.trace("DAG_executor_driver: Starting DAG_executor process for non-leaf task " + task_name)

                                payload = {
                                }
                                proc_name_prefix = "Worker_process_non-leaf_"
                                #proc = Process(target=DAG_executor.DAG_executor_processes, name=(proc_name_prefix+"p"+str(num_threads_created + 1)), args=(payload,counter,process_work_queue,data_dict,log_queue,worker_configurer,))

                                if not (DAG_executor_constants.COMPUTE_PAGERANK and DAG_executor_constants.USE_SHARED_PARTITIONS_GROUPS):
    #rhc: counter 
    # tasks_completed_counter, workers_completed_counter
                                    #proc = Process(target=DAG_executor.DAG_executor_processes, name=(proc_name_prefix+"p"+str(num_threads_created + 1)), args=(payload,completed_tasks_counter,log_queue,worker_configurer,
                                    proc = Process(target=DAG_executor.DAG_executor_processes, name=(proc_name_prefix+"p"+str(num_threads_created + 1)), args=(payload,completed_tasks_counter,completed_workers_counter,log_queue,worker_configurer,
                                        None,None,None,None,None,None,None,None,None,None))
                                else:
                                    if DAG_executor_constants.USE_PAGERANK_GROUPS_PARTITIONS:
                                        shared_nodes = BFS_Shared.shared_groups
                                        shared_map = BFS_Shared.shared_groups_map
                                        shared_frontier_map = BFS_Shared.shared_groups_frontier_parents_map
                                    else:
                                        shared_nodes = BFS_Shared.shared_partition
                                        shared_map = BFS_Shared.shared_partition_map
                                        shared_frontier_map = BFS_Shared.shared_partition_frontier_parents_map
                                    
                                    if DAG_executor_constants.USE_STRUCT_OF_ARRAYS_FOR_PAGERANK:
    #rhc: counter 
    # tasks_completed_counter, workers_completed_counter

                                        #proc = Process(target=DAG_executor.DAG_executor_processes, name=(proc_name_prefix+"p"+str(num_threads_created + 1)), args=(payload,completed_tasks_counter,log_queue,worker_configurer,
                                        proc = Process(target=DAG_executor.DAG_executor_processes, name=(proc_name_prefix+"p"+str(num_threads_created + 1)), args=(payload,completed_tasks_counter,completed_workers_counter,log_queue,worker_configurer,
                                            shared_nodes,shared_map,shared_frontier_map,
                                            BFS_Shared.pagerank_sent_to_processes,BFS_Shared.previous_sent_to_processes,BFS_Shared.number_of_children_sent_to_processes,
                                            BFS_Shared.number_of_parents_sent_to_processes,BFS_Shared.starting_indices_of_parents_sent_to_processes,
                                            BFS_Shared.parents_sent_to_processes,BFS_Shared.IDs_sent_to_processes,))
                                    else:
    #rhc: counter 
    # tasks_completed_counter, workers_completed_counter
                                        #proc = Process(target=DAG_executor.DAG_executor_processes, name=(proc_name_prefix+"p"+str(num_threads_created + 1)), args=(payload,completed_tasks_counter,log_queue,worker_configurer,
                                        proc = Process(target=DAG_executor.DAG_executor_processes, name=(proc_name_prefix+"p"+str(num_threads_created + 1)), args=(payload,completed_tasks_counter,completed_workers_counter,log_queue,worker_configurer,
                                            shared_nodes,shared_map,shared_frontier_map,
                                            None,None,None,None,None,None,None))

                                    #proc.start()
                                thread_proc_list.append(proc)
                                num_threads_created += 1                      
                            except Exception:
                                logger.exception("[ERROR] DAG_executor_driver: Failed to start DAG_executor worker process for non-leaf task " 
                                    + task_name)
                                if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
                                    logging.shutdown()
                                    os._exit(0) 

                        if DAG_executor_constants.USING_WORKERS and num_threads_created == DAG_executor_constants.NUM_WORKERS:
                            break 
                    else:
                        # above asssertion should have failed
                        logger.error("DAG_executor_driver: worker (pool) threads/processes must run locally (no Lambdas)")
                        logging.shutdown()
                        os._exit(0)

        if DAG_executor_constants.USE_MULTITHREADED_MULTIPROCESSING:
            logger.info("DAG_executor_driver: num_processes_created_for_multithreaded_multiprocessing: " + str(num_processes_created_for_multithreaded_multiprocessing))
        elif DAG_executor_constants.RUN_ALL_TASKS_LOCALLY:
            logger.info("DAG_executor_driver: num_threads/processes_created: " + str(num_threads_created))

        if not DAG_executor_constants.USE_MULTITHREADED_MULTIPROCESSING:
            start_time = time.time()
            if DAG_executor_constants.RUN_ALL_TASKS_LOCALLY:
                if DAG_executor_constants.USING_WORKERS:
                    for thread_proc in thread_proc_list:
                        thread_proc.start()

        if DAG_executor_constants.RUN_ALL_TASKS_LOCALLY:
            # Do joins if not using lambdas
            if not DAG_executor_constants.USE_MULTITHREADED_MULTIPROCESSING:
                if DAG_executor_constants.USING_WORKERS:
                    logger.info("DAG_executor_driver: joining workers.")
                    for thread in thread_proc_list:
                        thread.join()	

                if not DAG_executor_constants.USING_THREADS_NOT_PROCESSES:
                    # using processes and special process logger
                    logger.info("DAG_executor_driver: joining log_queue listener process.")
                    log_queue.put_nowait(None)
                    listener.join()
                    logger.info("DAG_executor_driver: join.")

                # Note: If we are using simukated lambdas, we are not
                # using workers and we are using threads, so neither
                # of the preceding two conditions hold. We do not 
                # join lambdas.
            else:   
                # using multithreaded with procs as workers; we have already joined the threads in each worker process
                logger.info("DAG_executor_driver: joining multithreaded_multiprocessing processes.")
                for proc in multithreaded_multiprocessing_process_list:
                    proc.join()
                # using processes and special process logger
                logger.info("DAG_executor_driver: joining log_queue listener process.")
                log_queue.put_nowait(None)
                listener.join()
        else:
            # We can have the result deposited in a bonded buffer sync object and 
            # withdraw it, in order to wait until all lambda DAG executors are done.
            logger.info("DAG_executor_driver: running (real) Lambdas - no joins, sleep instead.")

        #Note: To verify Results, see the code below.

        stop_time = time.time()
        duration = stop_time - start_time

        logger.info("DAG_executor_driver: Sleeping 3.0 seconds...")
        #print("DAG_executor_driver: Sleeping 3.0 seconds...")
        time.sleep(10.0)
        logger.info("DAG_executor_driver: done Sleeping.")
        #print(BFS_Shared.pagerank_sent_to_processes)

        # if using tcp_server (i.e., storing objects remotely)
        # clear the list of synchroization objects so we can 
        # excute a DAG again without having to stop and then
        # restart tcp_Server. (If we create synchronization objects
        # on the fly, then we must start execution with 0 objects
        # on the tcp_server; otherwise, objects will not be created 
        # since they already exist, which will not work as, e.g.,
        # exisiting "old" fanin/fanout objects will have already
        # been triggered.
        if not DAG_executor_constants.STORE_FANINS_FANINNBS_LOCALLY:
            logger.info("DAG_executor_driver: close all.")
            close_all(websocket)
                
    logger.info("DAG_executor_driver: DAG_Execution finished in %f seconds." % duration)


# create fanin and faninNB messages to be passed to the tcp_server for creating
# all fanin and faninNB synch objects
def create_fanin_and_faninNB_messages(DAG_map,DAG_states,DAG_info,all_fanin_task_names,all_fanin_sizes,
    all_faninNB_task_names,all_faninNB_sizes,
    groups_partitions):
#rhc: groups partitions
    # For pagerank computation we need to read a partition of 
    # nodes. For real lambdas, this could be from an S3 bucket
    # or somewhere. To avoid this, we can let this DAG_executor_driver
    # read the partition files and pass them to the Lambdas it starts
    # in the Lambda's payload. We pass grou_parition here when we create
    # the faninNBs since the faninNBs will start real lambdas.
    # When BYPASS_CALL_LAMBDA_CLIENT_INVOKE it means we 
    # are not actually running the real Lambda code on AWS, we are 
    # bypassing the call to invoke real AWS Lambdas and running the code
    # locally, in which case we can read the group/partition file objects
    # from local files.
    # Note: if we are not using real lambdas, groups_partitions is []
 
    """
    logger.trace("create_fanin_and_faninNB_messages: size of all_fanin_task_names: " + str(len(all_fanin_task_names))
        + " size of all_faninNB_task_names: " + str(len(all_faninNB_task_names)))
    logger.trace("create_fanin_and_faninNB_messages: size of all_fanin_sizes: " + str(len(all_fanin_sizes))
        + " size of all_faninNB_sizes: " + str(len(all_faninNB_sizes)))
    logger.trace("create_fanin_and_faninNB_messages: all_faninNB_task_names: " + str(all_faninNB_task_names))
    """

    fanin_messages = []

    # create a list of "create" messages, one for each fanin
    for fanin_name, size in zip(all_fanin_task_names,all_fanin_sizes):
        #logger.trace("iterate fanin: fanin_name: " + fanin_name + " size: " + str(size))
        # rhc: DES
        dummy_state = DAG_executor_State(function_name = "DAG_executor", function_instance_ID = str(uuid.uuid4()))
        # we will create the fanin object and call fanin.init(**keyword_arguments)
        dummy_state.keyword_arguments['n'] = size
        msg_id = str(uuid.uuid4())	# for debugging

        message = {
            "op": "create",
            "type": DAG_executor_constants.FANIN_TYPE,
            "name": fanin_name,
            "state": make_json_serializable(dummy_state),	
            "id": msg_id
        }
        fanin_messages.append(message)

    faninNB_messages = []

     # create a list of "create" messages, one for each faninNB
    for fanin_nameNB, size in zip(all_faninNB_task_names,all_faninNB_sizes):
        #logger.trace("iterate faninNB: fanin_nameNB: " + fanin_nameNB + " size: " + str(size))
        # rhc: DES
        dummy_state = DAG_executor_State(function_name = "DAG_executor", function_instance_ID = str(uuid.uuid4()))
        # passing to the fninNB object:
        # it size
        dummy_state.keyword_arguments['n'] = size
        # when the faninNB completes, if we are runnning locally and we are not pooling,
        # we start a new thread to execute the fanin task. If we are thread pooling, we put the 
        # start state in the work_queue. If we are using lambdas, we invoke a lambda to
        # execute the fanin task. If we are process pooling, then the last process to 
        # call fanin will put the start state of the fanin task in the work_queue. (FaninNb
        # cannot do this since the faninNB will be on the tcp_server.)
        dummy_state.keyword_arguments['start_state_fanin_task'] = DAG_states[fanin_nameNB]
        dummy_state.keyword_arguments['STORE_FANINS_FANINNBS_LOCALLY'] = DAG_executor_constants.STORE_FANINS_FANINNBS_LOCALLY
        # Only need DAG_info if not RUN_ALL_TASKS_LOCALLY, as we pass DAG_info to Lambas
        if not DAG_executor_constants.RUN_ALL_TASKS_LOCALLY:
            dummy_state.keyword_arguments['DAG_info'] = DAG_info
        else:
            dummy_state.keyword_arguments['DAG_info'] = None
#rhc groups partitions
        dummy_state.keyword_arguments['groups_partitions'] = groups_partitions
        msg_id = str(uuid.uuid4())

        message = {
            "op": "create",
            "type": DAG_executor_constants.FANINNB_TYPE,
            "name": fanin_nameNB,
            "state": make_json_serializable(dummy_state),	
            "id": msg_id
        }
        faninNB_messages.append(message)

    logger.trace("DAG_executor_driver: create_fanin_and_faninNB_messages: number of fanin messages: " + str(len(fanin_messages))
        + " number of faninNB messages: " + str(len(faninNB_messages)))

    return fanin_messages, faninNB_messages

# create fanin and faninNB messages to be passed to the tcp_server for creating
# all fanin and faninNB synch objects
def create_fanin_and_faninNB_and_fanout_messages(DAG_map,DAG_states,DAG_info,all_fanin_task_names,
    all_fanin_sizes,all_faninNB_task_names, all_faninNB_sizes,
    all_fanout_task_names,DAG_leaf_tasks,DAG_leaf_task_start_states,
    groups_partitions):
#rhc groups partitions
    # we are not using real lambdas to execute tasks so 
    # groups_partitions is []

 
    """
    logger.trace("create_fanin_and_faninNB_messages: size of all_fanin_task_names: " + str(len(all_fanin_task_names))
        + " size of all_faninNB_task_names: " + str(len(all_faninNB_task_names)))
    logger.trace("create_fanin_and_faninNB_messages: size of all_fanin_sizes: " + str(len(all_fanin_sizes))
        + " size of all_faninNB_sizes: " + str(len(all_faninNB_sizes)))
    logger.trace("create_fanin_and_faninNB_messages: all_faninNB_task_names: " + str(all_faninNB_task_names))
    """

    fanin_messages = []

    # create a list of "create" messages, one for each fanin
    for fanin_name, size in zip(all_fanin_task_names,all_fanin_sizes):
        #logger.trace("iterate fanin: fanin_name: " + fanin_name + " size: " + str(size))
        # rhc: DES
        dummy_state = DAG_executor_State(function_name = "DAG_executor", function_instance_ID = str(uuid.uuid4()))
        # we will create the fanin object and call fanin.init(**keyword_arguments)
        dummy_state.keyword_arguments['n'] = size
        msg_id = str(uuid.uuid4())	# for debugging

        message = {
            "op": "create",
            "type": DAG_executor_constants.FANIN_TYPE,
            "name": fanin_name,
            "state": make_json_serializable(dummy_state),	
            "id": msg_id
        }
        fanin_messages.append(message)

    faninNB_messages = []

     # create a list of "create" messages, one for each faninNB
    for fanin_nameNB, size in zip(all_faninNB_task_names,all_faninNB_sizes):
        #logger.trace("iterate faninNB: fanin_nameNB: " + fanin_nameNB + " size: " + str(size))
        # rhc: DES
        dummy_state = DAG_executor_State(function_name = "DAG_executor", function_instance_ID = str(uuid.uuid4()))
        # passing to the fninNB object:
        # it size
        dummy_state.keyword_arguments['n'] = size
        # when the faninNB completes, if we are runnning locally and we are not pooling,
        # we start a new thread to execute the fanin task. If we are thread pooling, we put the 
        # start state in the work_queue. If we are using lambdas, we invoke a lambda to
        # execute the fanin task. If we are process pooling, then the last process to 
        # call fanin will put the start state of the fanin task in the work_queue. (FaninNb
        # cannot do this since the faninNB will be on the tcp_server.)
        dummy_state.keyword_arguments['start_state_fanin_task'] = DAG_states[fanin_nameNB]
        dummy_state.keyword_arguments['STORE_FANINS_FANINNBS_LOCALLY'] = DAG_executor_constants.STORE_FANINS_FANINNBS_LOCALLY
        # Only need DAG_info if not RUN_ALL_TASKS_LOCALLY, as we pass DAG_info to Lambas
        if not DAG_executor_constants.RUN_ALL_TASKS_LOCALLY:
            dummy_state.keyword_arguments['DAG_info'] = DAG_info
        else:
            dummy_state.keyword_arguments['DAG_info'] = None

#rhc groups partitions
        dummy_state.keyword_arguments['groups_partitions'] = groups_partitions

        msg_id = str(uuid.uuid4())

        message = {
            "op": "create",
            "type": DAG_executor_constants.FANINNB_TYPE,
            "name": fanin_nameNB,
            "state": make_json_serializable(dummy_state),	
            "id": msg_id
        }
        faninNB_messages.append(message)

    fanout_messages = []

    # create a list of "create" messages, one for each fanout
    # Note: There is no all_fanout_sizes since size is always
    # Note: Also create a message for the fanin task, which is 
    # treated as a fanout.
    for leaf_task_name, leaf_task_start_state in zip(DAG_leaf_tasks,DAG_leaf_task_start_states):
        #logger.trace("iterate fanin: fanin_name: " + fanin_name + " size: " + str(size))
        # rhc: DES
        dummy_state = DAG_executor_State(function_name = "DAG_executor", function_instance_ID = str(uuid.uuid4()))
        # we will create the fanin object and call fanin.init(**keyword_arguments)
        # Note: a fanout is a fann of size 1
        dummy_state.keyword_arguments['n'] = 1
        # when the faninNB completes, if we are runnning locally and we are not pooling,
        # we start a new thread to execute the fanin task. If we are thread pooling, we put the 
        # start state in the work_queue. If we are using lambdas, we invoke a lambda to
        # execute the fanin task. If we are process pooling, then the last process to 
        # call fanin will put the start state of the fanin task in the work_queue. (FaninNb
        # cannot do this since the faninNB will be on the tcp_server.)
        dummy_state.keyword_arguments['start_state_fanin_task'] = leaf_task_start_state
        dummy_state.keyword_arguments['STORE_FANINS_FANINNBS_LOCALLY'] = DAG_executor_constants.STORE_FANINS_FANINNBS_LOCALLY
        # Only need DAG_info if not RUN_ALL_TASKS_LOCALLY, as we pass DAG_info to Lambas
        if not DAG_executor_constants.RUN_ALL_TASKS_LOCALLY:
            dummy_state.keyword_arguments['DAG_info'] = DAG_info
        else:
            dummy_state.keyword_arguments['DAG_info'] = None
        msg_id = str(uuid.uuid4())	# for debugging

        message = {
            "op": "create",
            # fanouts are just FanIns of size 1
            "type": DAG_executor_constants.FANINNB_TYPE,
            "name": leaf_task_name,
            "state": make_json_serializable(dummy_state),	
            "id": msg_id
        }
        fanout_messages.append(message)

    for fanout_name in all_fanout_task_names:
        #logger.trace("iterate fanin: fanin_name: " + fanin_name + " size: " + str(size))
        # rhc: DES
        dummy_state = DAG_executor_State(function_name = "DAG_executor", function_instance_ID = str(uuid.uuid4()))
        # we will create the fanin object and call fanin.init(**keyword_arguments)
        # Note: a fanout is a fann of size 1
        dummy_state.keyword_arguments['n'] = 1
        # when the faninNB completes, if we are runnning locally and we are not pooling,
        # we start a new thread to execute the fanin task. If we are thread pooling, we put the 
        # start state in the work_queue. If we are using lambdas, we invoke a lambda to
        # execute the fanin task. If we are process pooling, then the last process to 
        # call fanin will put the start state of the fanin task in the work_queue. (FaninNb
        # cannot do this since the faninNB will be on the tcp_server.)
        dummy_state.keyword_arguments['start_state_fanin_task'] = DAG_states[fanout_name]
        dummy_state.keyword_arguments['STORE_FANINS_FANINNBS_LOCALLY'] = DAG_executor_constants.STORE_FANINS_FANINNBS_LOCALLY
        # Only need DAG_info if not RUN_ALL_TASKS_LOCALLY, as we pass DAG_info to Lambas
        if not DAG_executor_constants.RUN_ALL_TASKS_LOCALLY:
            dummy_state.keyword_arguments['DAG_info'] = DAG_info
        else:
            dummy_state.keyword_arguments['DAG_info'] = None
        msg_id = str(uuid.uuid4())	# for debugging

        message = {
            "op": "create",
            # fanouts are just FanIns of size 1
            "type": DAG_executor_constants.FANINNB_TYPE,
            "name": fanout_name,
            "state": make_json_serializable(dummy_state),	
            "id": msg_id
        }
        fanout_messages.append(message)

    logger.trace("DAG_executor_driver: create_fanin_and_faninNB_and_fanout_messages: number of fanin messages: " 
        + str(len(fanin_messages))
        + " number of faninNB messages: " + str(len(faninNB_messages))
        + " number of fanout messages (including leaf task fanouts): " + str(len(fanout_messages)))

    return fanin_messages, faninNB_messages, fanout_messages

"""
# Not used - no case in which we create only a work queue - we always create fanins and faninNBs
# and possibly create a work queue. The work quueue creation is piggybacked on creating
# the fanins and faninNBs (on server)
#
# NOT TESTED
#
# creates all fanins and faninNBs at the start of driver executin. If we are using 
# workers and processes (not threads) then we also crate the work_queue here
def create_work_queue(websocket,number_of_tasks):
    dummy_state = DAG_executor_State(function_name = "DAG_executor", function_instance_ID = str(uuid.uuid4()))
    # we will create the fanin object and call fanin.init(**keyword_arguments)
    dummy_state.keyword_arguments['n'] = 2*number_of_tasks
    msg_id = str(uuid.uuid4())	# for debugging

    work_queue_message = {
        "op": "create_work_queue",
        "type": PROCESS_WORK_QUEUE_TYPE,
        "name": "process_work_queue",
        "state": make_json_serializable(dummy_state),	
        "id": msg_id
    } 

    logger.trace("create_work_queue: Sending a 'create_work_queue' message to server.")
    create_work_queue_on_server(websocket, work_queue_message)
"""

# creates all fanins and faninNBs at the start of driver executin. Since we are using 
# workers and processes (not threads) then we also create the work_queue here
def create_fanins_and_faninNBs_and_work_queue(websocket,number_of_tasks,DAG_map,DAG_states,DAG_info,all_fanin_task_names,all_fanin_sizes,
    all_faninNB_task_names,all_faninNB_sizes,
    groups_partitions):
#rhc: groups partitions 
# We ar not using real lambdas to excute tasks so groups_partitions is []
    dummy_state = DAG_executor_State(function_name = "DAG_executor", function_instance_ID = str(uuid.uuid4()))
    # we will create the fanin object and call fanin.init(**keyword_arguments)
    dummy_state.keyword_arguments['n'] = 2*number_of_tasks
    msg_id = str(uuid.uuid4())	# for debugging

    work_queue_message = {
        "op": "create",
        "type": DAG_executor_constants.PROCESS_WORK_QUEUE_TYPE,
        "name": "process_work_queue",
        "state": make_json_serializable(dummy_state),	
        "id": msg_id
    } 

    fanin_messages, faninNB_messages = create_fanin_and_faninNB_messages(DAG_map,DAG_states,DAG_info,all_fanin_task_names,all_fanin_sizes,
            all_faninNB_task_names,all_faninNB_sizes,
            groups_partitions)

    logger.trace("DAG_executor_driver: create_fanins_and_faninNBs_and_work_queue: Sending a 'create_fanins_and_faninNBs_and_work_queue' message to server.")
    #logger.trace("create_fanins_and_faninNBs_and_work_queue: num fanin created: "  + str(len(fanin_messages))
    #    +  " num faninNB creates; " + str(len(faninNB_messages)))

    # even if there are not fanins or faninNBs in Dag, need to create the work queue so send the message
    messages = (fanin_messages,faninNB_messages,work_queue_message)
    dummy_state2 = DAG_executor_State(function_name = "DAG_executor", function_instance_ID = str(uuid.uuid4()))
    #Note: Passing tuple messages as name
    create_all_sync_objects(websocket, "create_all_sync_objects", "DAG_executor_fanin_or_faninNB_or_work_queue", 
        messages, dummy_state2)

# creates all fanins and faninNBs and fanouts at the start of driver execution. We use this
# when we are using lambdas to execte tasks, not workers. Note that leaf tasks
# will be included in the fanouts.
def create_fanins_and_faninNBs_and_fanouts(websocket,DAG_map,DAG_states,DAG_info,all_fanin_task_names,all_fanin_sizes,
    all_faninNB_task_names,all_faninNB_sizes,all_fanout_task_names, 
    DAG_leaf_tasks, DAG_leaf_task_start_states,
    groups_partitions):
#rhc groups partitions
    # we are not using real lambdas to execute tasks so groups_partitions is []

    fanin_messages, faninNB_messages, fanout_messages = create_fanin_and_faninNB_and_fanout_messages(DAG_map,DAG_states,DAG_info,
        all_fanin_task_names,all_fanin_sizes,all_faninNB_task_names,all_faninNB_sizes,
        all_fanout_task_names, DAG_leaf_tasks,DAG_leaf_task_start_states,
        groups_partitions)

    logger.trace("DAG_executor_driver: create_fanins_and_faninNBs_and_fanouts: Sending a 'create_fanins_and_faninNBs_and_fanouts' message to server.")
    #logger.trace("create_fanins_and_faninNBs_and_work_queue: num fanin created: "  + str(len(fanin_messages))
    #    +  " num faninNB creates; " + str(len(faninNB_messages)))

    # even if there are not fanins or faninNBs in Dag, need to create the work queue so send the message
    messages = (fanin_messages,faninNB_messages,fanout_messages)
    dummy_state = DAG_executor_State(function_name = "DAG_executor", function_instance_ID = str(uuid.uuid4()))
    #Note: Passing tuple messages as name
    create_all_sync_objects(websocket, "create_all_sync_objects", "DAG_executor_fanin_or_faninNB_or_fanout", 
        messages, dummy_state)

# creates all fanins and faninNBs at the start of driver execution. 
def create_fanins_and_faninNBs(websocket,DAG_map,DAG_states,DAG_info,all_fanin_task_names,all_fanin_sizes,
    all_faninNB_task_names,all_faninNB_sizes,
    groups_partitions):
#rhc: groups partitions
    # For pagerank computation we need to read a partition of 
    # nodes. For real lambdas, this could be from an S3 bucket
    # or somewhere. To avoid this, we can let this DAG_executor_driver
    # read the partition files and pass them to the Lambdas it starts
    # in the Lambda's payload. We pass grou_parition here when we create
    # the faninNBs since the faninNBs will start real lambdas.
    # When BYPASS_CALL_LAMBDA_CLIENT_INVOKE it means we 
    # are not actually running the real Lambda code on AWS, we are 
    # bypassing the call to invoke real AWS Lambdas and running the code
    # locally, in which case we can read the group/partition file objects
    # from local files.
    # Note: if we are not using real lambdas, # since not using real lambdas, groups_partitions is []

    fanin_messages, faninNB_messages = create_fanin_and_faninNB_messages(DAG_map,DAG_states,DAG_info,all_fanin_task_names,all_fanin_sizes,
                all_faninNB_task_names,all_faninNB_sizes,
                groups_partitions)

    #logger.trace("create_fanins_and_faninNBs: Sending a 'create_all_fanins_and_faninNBs_and_possibly_work_queue' message to server.")
    #logger.trace("create_fanins_and_faninNBs: number of fanin messages: " + str(len(fanin_messages))
    #    + " number of faninNB messages: " + str(len(faninNB_messages)))
    #logger.trace("create_fanins_and_faninNBs: size of all_fanin_task_names: " + str(len(all_fanin_task_names))
    #    + " size of all_faninNB_task_names: " + str(len(all_faninNB_task_names)))
    #logger.trace("create_fanins_and_faninNBs: size of all_fanin_sizes: " + str(len(all_fanin_sizes))
    #    + " size of all_faninNB_sizes: " + str(len(all_faninNB_sizes)))
    #logger.trace("create_fanins_and_faninNBs: all_faninNB_task_names: " + str(all_faninNB_task_names))

    # Don't send a message to the server if there are no fanin or faninNBs to create
    # Not tested DAG with no fanins or faninNBs yet.
    if len(fanin_messages) > 0 or len(faninNB_messages) > 0:
        messages = (fanin_messages,faninNB_messages)
        dummy_state = DAG_executor_State(function_name = "DAG_executor", function_instance_ID = str(uuid.uuid4()))
        create_all_sync_objects(websocket, "create_all_sync_objects", "DAG_executor_fanin_or_faninNB", 
            messages, dummy_state)

    """ create_all_fanins_and_faninNBs creates:

    message = {
        "op": "create_all_fanins_and_faninNBs",
        "type": "DAG_executor_fanin_or_faninNB",
        "name": messages,		# Q: Fix this? usually it's a synch object name (string)
        "state": make_json_serializable(dummy_state),
        "id": msg_id
    }

	Then does:	

    msg = json.dumps(message).encode('utf-8')
    send_object(msg, websocket)
    logger.trace("Sent 'create_all_fanins_and_faninNBs' message to server")

    # Receive data. This should just be an ACK, as the TCP server will 'ACK' our create_all_fanins_and_faninNBs() call.
    ack = recv_object(websocket)
    """

def process_leaf_tasks_batch(websocket):
    dummy_state = DAG_executor_State(function_name = "DAG_executor", function_instance_ID = str(uuid.uuid4())) 
    synchronize_trigger_leaf_tasks(websocket, "process_leaf_tasks_batch", "process_leaf_tasks_batch_Type", 
        "leaf_tasks", dummy_state)

if __name__ == "__main__":
    print("Running.")
    run()

# xtra:
"""
def add(inp):
    logger.trace("add: " + "input: " + str(input))
    num1 = inp['inc0']
    num2 = inp['inc1']
    sum = num1 + num2
    output = {'add': sum}
    logger.trace("add output: " + str(sum))
    return output
def multiply(inp):
    logger.trace("multiply")
    num1 = inp['add']
    num2 = inp['square']
    num3 = inp['triple']
    product = num1 * num2 * num3
    output = {'multiply': product}
    logger.trace("multiply output: " + str(product))
    return output
def divide(inp):
    logger.trace("divide")
    num1 = inp['multiply']
    quotient = num1 / 72
    output = {'quotient': quotient}
    logger.trace("quotient output: " + str(quotient))
    return output
def triple(inp):
    logger.trace("triple")
    value = inp['inc1']
    value *= 3
    output = {'triple': value}
    logger.trace("triple output: " + str(output))
    return output
def square(inp):
    logger.trace("square")
    value = inp['inc1']
    value *= value
    output = {'square': value}
    logger.trace("square output: " + str(output))
    return output
def inc0(inp):
    logger.trace("inc0")
    #value = inp['input']
    input_tuple = inp['input']
    value = input_tuple[0]
    value += 1
    output = {'inc0': value}
    logger.trace("inc0 output: " + str(output))
    return output
def inc1(inp):
    logger.trace("inc1")
    #value = inp['input']
    input_tuple = inp['input']
    value = input_tuple[0]
    value += 1
    output = {'inc1': value}
    logger.trace("inc1 output: " + str(output))
    return output
"""

	# generate DAG_map using DFS_visit
    # n1 = Node(None,None,"inc0",inc0)
    # n3 = Node(None,None,"triple",triple)
    # n4 = Node(None,None,"inc1",inc1)
    # n5 = Node(Node,Node,"square",square)
    # n2 = Node(Node,Node,"add",add) 
    # n6 = Node(Node,Node,"multiply",multiply) 
    # n7 = Node(Node,Node,"divide",divide)
	
    # n1.set_succ([n2])
    # n1.set_pred([])
    # n2.set_succ([n6])	
    # n2.set_pred([n1,n4])
    # n3.set_succ([n6])
    # n3.set_pred([n4])
    # n4.set_succ([n2,n5,n3])
    # n4.set_pred([])
    # n5.set_succ([n6])
    # n5.set_pred([n4])
    # n6.set_succ([n7])
    # n6.set_pred([n2,n3,n5])
    # n7.set_succ([])
    # n7.set_pred([n6])
	
    # n1.generate_ops()
    # n4.generate_ops()
    # n2.generate_ops()
    # n3.generate_ops()
    # n5.generate_ops()
    # n6.generate_ops()
    # n7.generate_ops()
    # #Node.save_DAG_info()
	
    # logger.trace("DAG_map:")
    # for key, value in Node.DAG_map.items():
    #     logger.trace(key)
    #     logger.trace(value)
    # logger.trace("  ")
	
    # logger.trace("states:")         
    # for key, value in Node.DAG_states.items():
    #     logger.trace(key)
    #     logger.trace(value)
    # logger.trace("   ")
	
    # logger.trace("num_fanins:" + str(Node.num_fanins) + " num_fanouts:" + str(Node.num_fanouts) + " num_faninNBs:" 
    #     + " num_collapse:" + str(Node.num_collapse))
    # logger.trace("   ")
	
    # logger.trace("all_fanout_task_names")
    # for name in Node.all_fanout_task_names:
    #     logger.trace(name)
    #     logger.trace("   ")
    # logger.trace("   ")
	
    # logger.trace("all_fanin_task_names")
    # for name in Node.all_fanin_task_names :
    #     logger.trace(name)
    #     logger.trace("   ")
    # logger.trace("   ")
		  
    # logger.trace("all_faninNB_task_names")
    # for name in Node.all_faninNB_task_names:
    #     logger.trace(name)
    #     logger.trace("   ")
    # logger.trace("   ")
		  
    # logger.trace("all_collapse_task_names")
    # for name in Node.all_collapse_task_names:
    #     logger.trace(name)
    #     logger.trace("   ")
    # logger.trace("   ")
	
    # DAG_map = Node.DAG_map
    
    # logger.trace("DAG_map after assignment:")
    # for key, value in Node.DAG_map.items():
    #     logger.trace(key)
    #     logger.trace(value)   
    # logger.trace("   ")
    # states = Node.DAG_states
    
    #all_fanout_task_names = Node.all_fanout_task_names
    #all_fanin_task_names = Node.all_fanin_task_names
    #all_faninNB_task_names = Node.all_faninNB_task_names
    #all_collapse_task_names = Node.all_collapse_task_names

"""
def get_leaf_task_input(name):
    if name == "inc0":
        input_tuple = (0,)
        #return int(0)	# inc0
        return input_tuple
    else:
        input_tuple = (1,)
        #return int(1)	# inc1
        return input_tuple

def get_DAG_leaf_task_inputs(DAG_leaf_tasks):
	leaf_task_inputs = []
	for name in DAG_leaf_tasks:
		input = get_leaf_task_input(name)
		leaf_task_inputs.append(input)
	return leaf_task_inputs
"""

"""
def input_DAG_map():
    DAG_map = Node.DAG_map
    #with open('DAG_map', 'rb') as handle:
    #DAG_map = pickle.load(handle)
    return DAG_map

def input_DAG_states():
    DAG_states = Node.DAG_states
    #with open('DAG_states', 'rb') as handle:
    #DAG_states = pickle.load(handle)
    return DAG_states
    
def input_all_fanin_task_names():
    all_fanin_task_names = Node.all_fanin_task_names
    #with open('all_fanin_task_names', 'rb') as handle:
    #DAG_states = pickle.load(handle)
    return all_fanin_task_names
    
def input_all_fanin_sizes():
    all_fanin_sizes = Node.all_fanin_sizes
    #with open('input_all_fanin_sizes', 'rb') as handle:
    #DAG_states = pickle.load(handle)
    return all_fanin_sizes
    
def input_all_faninNB_task_names():
    all_faninNB_task_names = Node.all_faninNB_task_names
    #with open('all_faninNB_task_names', 'rb') as handle:
    #DAG_states = pickle.load(handle)
    return all_faninNB_task_names
    
    
def input_all_faninNB_sizes():
    all_faninNB_sizes = Node.all_faninNB_sizes
    #with open('all_faninNB_sizes', 'rb') as handle:
    #DAG_states = pickle.load(handle)
    return all_faninNB_sizes
    
def input_DAG_leaf_tasks():
    DAG_leaf_tasks = ["inc0","inc1"]
    #with open('DAG_leaf_tasks.pickle', 'rb') as handle:
    #DAG_leaf_tasks = pickle.load(handle)
    return DAG_leaf_tasks

def input_DAG_leaf_task_start_states():
    DAG_leaf_tasks_start_states = [1,3]
    #with open('DAG_leaf_task_start_states.pickle', 'rb') as handle:
    #DAG_leaf_task_start_states = pickle.load(handle)
    return DAG_leaf_tasks_start_states
"""

"""
import pickle

a = {'hello': 'world'}

with open('filename.pickle', 'wb') as handle:
    pickle.dump(a, handle, protocol=pickle.HIGHEST_PROTOCOL)

with open('filename.pickle', 'rb') as handle:
    b = pickle.load(handle)

print(a == b)
"""

"""
DAG_map = input_DAG_map()
all_fanin_task_names = input_all_fanin_task_names()
all_fanin_sizes = input_all_fanin_sizes()
all_faninNB_task_names = input_all_faninNB_task_names()
all_faninNB_sizes = input_all_faninNB_sizes()
DAG_states = input_DAG_states()
DAG_leaf_tasks = input_DAG_leaf_tasks()
DAG_leaf_task_start_states = input_DAG_leaf_task_start_states()
"""

""" verify results: this is a synch-op, but no synch-op implemented yet for synch objects stored in Infinix Lambdas
    so commented out for lambda version

    ==> Create a simple "Display" synch object with op "display() that just displays what 
    you (asynchronously) send to it.

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as websocket:
    print("Connecting to " + str(TCP_SERVER_IP))
    websocket.connect(TCP_SERVER_IP)
    default_state = State("Composer", function_instance_ID = str(uuid.uuid4()), list_of_functions = ["FuncA", "FuncB"])

    sleep_length_seconds = 20.0
    logger.trace("Sleeping for " + str(sleep_length_seconds) + " seconds before calling synchronize_sync()")
    time.sleep(sleep_length_seconds)
    logger.trace("Finished sleeping. Calling synchronize_sync() now...")

    # Note: no-try op
    state = synchronize_sync(websocket, "synchronize_sync", "final_result", "withdraw", default_state)
    answer = state.return_value 

    end_time = time.time()
    
    error_occurred = False
    if type(answer) is str:
        logger.error("Unexpected solution recovered from Redis: %s\n\n" % answer)
        error_occurred = True
    else:
        logger.trace("Solution: " + str(answer) + "\n\n")
        expected_answer = int(72)
        if expected_answer != answer:
            logger.error("Error in answer: " + str(answer) + " expected_answer: " + str(expected_answer))
            error_occurred = True 

    if not error_occurred:
        logger.trace("Verified.")
        
    # rest is performance stuff, close websocket and return
    
    ..
    
    # then main() stuff
if __name__ == "__main__":

"""	