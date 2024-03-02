import threading
import _thread
import time
import socket
import cloudpickle 
import base64 
import uuid
import queue

import logging
import logging.handlers
import multiprocessing
import os

"""
from .DFS_visit import Node
#from DAG_executor_FanInNB import DAG_executor_FanInNB
#from . import  DAG_executor_FanInNB
#from . import  DAG_executor_FanIn
#from wukongdnc.server import DAG_executor_FanInNB
#from wukongdnc.server import DAG_executor_FanIn
#from . import DAG_executor_driver
from .DAG_executor_State import DAG_executor_State
from .DAG_info import DAG_Info
from wukongdnc.server.api import synchronize_sync, synchronize_process_faninNBs_batch
from wukongdnc.constants import TCP_SERVER_IP
"""


#if not (not USING_THREADS_NOT_PROCESSES or USE_MULTITHREADED_MULTIPROCESSING):
    #logger.setLevel("TRACE")
    #logger.setLevel(logging.INFO)

    #logger.setLevel(LOG_LEVEL)
    #formatter = logging.Formatter('[%(asctime)s] [%(module)s] [%(processName)s] [%(threadName)s]: %(message)s')
    #ch = logging.StreamHandler()

    #ch.setLevel("TRACE")
    #ch.setLevel(logging.INFO)

    #ch.setLevel(LOG_LEVEL)
    #ch.setFormatter(formatter)
    #logger.addHandler(ch)

#from .DAG_executor_constants import RUN_ALL_TASKS_LOCALLY, STORE_FANINS_FANINNBS_LOCALLY 
#from .DAG_executor_constants import CREATE_ALL_FANINS_FANINNBS_ON_START, USING_WORKERS 
#from .DAG_executor_constants import USING_THREADS_NOT_PROCESSES, USE_MULTITHREADED_MULTIPROCESSING
#from .DAG_executor_constants import PROCESS_WORK_QUEUE_TYPE, FANINNB_TYPE, USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS
#from .DAG_executor_constants import SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS, STORE_SYNC_OBJECTS_IN_LAMBDAS
#from .DAG_executor_constants import TASKS_USE_RESULT_DICTIONARY_PARAMETER, SAME_OUTPUT_FOR_ALL_FANOUT_FANIN
#from .DAG_executor_constants import COMPUTE_PAGERANK, USE_SHARED_PARTITIONS_GROUPS, USE_PAGERANK_GROUPS_PARTITIONS
#from .DAG_executor_constants import USE_STRUCT_OF_ARRAYS_FOR_PAGERANK, USING_WORKERS
#from .DAG_executor_constants import USE_INCREMENTAL_DAG_GENERATION, NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG
#from .DAG_executor_constants import INPUT_ALL_GROUPS_PARTITIONS_AT_START
#from .DAG_executor_constants import WORK_QUEUE_SIZE_FOR_INCREMENTAL_DAG_GENERATION_WITH_WORKER_PROCESSES
#brc: counter:
#from .DAG_executor_constants import NUM_WORKERS
#from .DAG_executor_constants import EXIT_PROGRAM_ON_EXCEPTION
#from .DAG_executor_constants import CHECK_PAGERANK_OUTPUT
##from .DAG_executor_constants import LOG_LEVEL
from . import DAG_executor_constants
# BRC: Possibly: Try to read the test number from a file that TestAll creates
# and puts the test_number in when a mutiP test.

logger = logging.getLogger(__name__)

# Note: We intend to run TestAll whwn we execute the program, instead
# of either directly running DAG_executor_driver (non-pagerank) or BFS (pagerank)
# from the command line. That is, to run the program we need to configure
# it so we may as well set the configuration using the "-t test_number"
# command line argument, as opposed to configuring manually.
# If we do run DAG_executor_driver from the
# command line, then DAG_executor_driver will have already added 
# the TRACE logging level so the call here to addLoggingLevel will
# raise an exception, which we catch. In this ease we just keep 
# excuting. Note that since we didn't run TestAll from the command 
# line TestAll did not write the test_number so the test_number file
# would not exist and would not be read after addLoggingLevel. As we
# sai, addLoggingLevel will raise an exception so the code to 
# read the test_number is not reachable in that case. So it "works"
# to run DAG_executor_driver directly but we intent to run TestAll.
# Note: If we are running real serverless lambdas, which is what this next
# if statement chcks, we do not want to do any of this. This code is for
# when we are using TestAll and we are testing worker processes. When
# we are not testing, this code "fails" but we ignore the failure.
if not (not DAG_executor_constants.RUN_ALL_TASKS_LOCALLY and (not DAG_executor_constants.BYPASS_CALL_TO_INVOKE_REAL_LAMBDA)):
    try:
        # Check whether set_test_number has already been called.
        # if DAG_executor_driver imported this DAG_executor module
        # then set_test_number has already been called. This is 
        # the case except when we are using multiprocessing. In this 
        # case, along with DAG_executor_driver importing DAG_executor,
        # when a process is started, DAG_executor will be imported,
        # and DAG_executor_constants before that (as part of 
        # multiprocessing) but TestAll will not have set_test_number
        # for this DAG_executor_constants, so we need to do it 
        # here. this will be done for each process.)
        if DAG_executor_constants.test_number == 0:
            # add TRACE level for this worker process
            from .addLoggingLevel import addLoggingLevel
            addLoggingLevel('TRACE', logging.DEBUG - 5)
            # TestAll writes the TestNumber to this file so 
            # we can read it and set test_number for this process.
            test_number_file_name = "./test_number.txt"
            if os.path.isfile(test_number_file_name):
                with open(test_number_file_name) as test_number_file:
                    test_number = int(test_number_file.read())
                    logger.info("DAG_executor before set_test_number: test_number: " + str(DAG_executor_constants.test_number))
                    DAG_executor_constants.set_test_number_and_run_test(test_number)
                    logger.info("DAG_executor after set_test_number: DAG_executor_constants.USING_THREADS_NOT_PROCESSES: " + str(DAG_executor_constants.USING_THREADS_NOT_PROCESSES))
    except AttributeError:
        pass
        #thread_name = threading.current_thread().name
        #logger.exception("[ERROR]:" + thread_name + ": DAG_executor: already set logging level.")
        #if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
        #    logging.shutdown()
        #    os._exit(0)
    except IOError:
        pass
        #thread_name = threading.current_thread().name
        #logger.exception("[ERROR]:" + thread_name + ": DAG_executor: Failed to read test_number file.")
        #if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
        #    logging.shutdown()
        #    os._exit(0)

from .DFS_visit import Node
#from DAG_executor_FanInNB import DAG_executor_FanInNB
#from . import  DAG_executor_FanInNB
#from . import  DAG_executor_FanIn
#from wukongdnc.server import DAG_executor_FanInNB
#from wukongdnc.server import DAG_executor_FanIn
#from . import DAG_executor_driver
from .DAG_executor_State import DAG_executor_State
from .DAG_info import DAG_Info
from wukongdnc.server.api import synchronize_sync, synchronize_process_faninNBs_batch
from wukongdnc.constants import TCP_SERVER_IP

#from .DAG_work_queue_for_threads import thread_work_queue
from .DAG_executor_work_queue_for_threads import work_queue
from .DAG_data_dict_for_threads import data_dict
from .DAG_executor_counter import completed_tasks_counter, completed_workers_counter
from .DAG_executor_synchronizer import server
from wukongdnc.wukong.invoker import invoke_lambda_DAG_executor
from .DAG_boundedbuffer_work_queue import Work_Queue_Client
from .util import pack_data
#brc: continue
from .Remote_Client_for_DAG_infoBuffer_Monitor import Remote_Client_for_DAG_infoBuffer_Monitor
from .Remote_Client_for_DAG_infoBuffer_Monitor_for_Lambdas import Remote_Client_for_DAG_infoBuffer_Monitor_for_Lambdas

#if not USING_WORKERS:
#    import wukongdnc.dag.DAG_infoBuffer_Monitor_for_lambdas_for_threads 
 
#else:
#    import wukongdnc.dag.DAG_infoBuffer_Monitor_for_threads 
    
#from .DAG_infoBuffer_Monitor_for_threads import DAG_infobuffer_monitor

# Polymorphic import!: This will either be a DAG_infoBuffer_Monitor or a DAG_infoBuffer_Monitor_for_Lambdas
import  wukongdnc.dag.DAG_infoBuffer_Monitor_for_threads

#DAG_infobuffer_monitor = None

# Note: avoiding circular imports:
# https://stackoverflow.com/questions/744373/what-happens-when-using-mutual-or-circular-cyclic-imports
#brc: cleanup
#from .BFS import shared_partition, shared_groups
#from .BFS import shared_partition_map, shared_groups_map
#from .Shared import shared_partition, shared_groups, shared_partition_map,  shared_groups_map
from . import BFS_Shared
from .DAG_executor_output_checker import set_pagerank_output


"""
#brc: shared
shared_partition = []
shared_groups = []
# maps partition "P" to its position/size in shared_partition/shared_groups
shared_partition_map = {}
shared_groups_map = {}
"""

total_time = 0
num_fanins_timed = 0


def create_and_faninNB_task_locally(kwargs):
    logger.trace("create_and_faninNB_task: call create_and_faninNB_locally")
    server = kwargs['server']
    # Not using return_value from faninNB since faninNB starts the fanin task, i.e., there is No Become
    _return_value_ignored = server.create_and_faninNB_locally(**kwargs)

def faninNB_task_locally(kwargs):
    logger.trace("faninNB_task: call faninNB_locally")
    server = kwargs['server']
    # Not using return_value from faninNB since faninNB starts the fanin task, i.e., there is No Become
    _return_value_ignored = server.faninNB_locally(**kwargs)

# used to execute a task; need to give the task its "input" map
#name_to_function_map = {'inc0': inc0, 'inc1': inc1, 'add': add, 'multiply': multiply, 'triple': triple, 'square': square, 'divide':divide}

# execute task from name_to_function_map with key task_name
def execute_task(task, args):
    #commented out for MM
    thread_name = threading.current_thread().name
    logger.trace(thread_name + ": execute_task: input of execute_task is: " + str(args))
    #output = task(input)
    #for i in range(0, len(args)):
    #    print("Type of argument #%d: %s" % (i, type(args[i])))
    #    print("Argument #%d: %s" % (i, str(args[i])))
    output = task(*args)
    return output

#brc: ToDo: total_num_nodes is hardcoded
#brc: groups partitions
#def execute_task_with_result_dictionary(task,task_name,total_num_nodes,resultDictionary):
def execute_task_with_result_dictionary(task,task_name,total_num_nodes,resultDictionary,
#brc: groups partitions
    groups_partitions):
    #commented out for MM
    thread_name = threading.current_thread().name
    logger.trace(thread_name + ": execute_task_with_result_dictionary: input of execute_task is: " 
        + str(resultDictionary))
    #output = task(input)
    #for i in range(0, len(args)):
    #    print("Type of argument #%d: %s" % (i, type(args[i])))
    #    print("Argument #%d: %s" % (i, str(args[i])))
    #output = task(task_name,total_num_nodes,resultDictionary)
#brc: groups partitions
    output,result_tuple_list = task(task_name,total_num_nodes,resultDictionary,groups_partitions)
    return output, result_tuple_list

def execute_task_with_result_dictionary_shared(task,task_name,total_num_nodes,resultDictionary,shared_map, shared_nodes):
    #commented out for MM
    thread_name = threading.current_thread().name
    logger.trace(thread_name + ": execute_task_with_result_dictionary: input of execute_task is: " 
        + str(resultDictionary))
    #output = task(input)
    #for i in range(0, len(args)):
    #    print("Type of argument #%d: %s" % (i, type(args[i])))
    #    print("Argument #%d: %s" % (i, str(args[i])))
    output, result_tuple_list = task(task_name,total_num_nodes,resultDictionary,shared_map,shared_nodes)
    return output, result_tuple_list

def create_and_faninNB_remotely(websocket,**keyword_arguments):
    # pass
    # need code that returns a  DAG_exec_state = synchronize_sync()
    dummy_DAG_exec_state = faninNB_remotely(websocket,**keyword_arguments)
    return dummy_DAG_exec_state

def faninNB_remotely(websocket,**keyword_arguments):
    # create new faninNB with specified name if it hasn't been created
    #fanin_task_name = keyword_arguments['fanin_task_name']
    #n = keyword_arguments['n']
    #start_state_fanin_task = keyword_arguments['start_state_fanin_task']
    #output = keyword_arguments['result']
    #calling_task_name = keyword_arguments['calling_task_name']
    #ToDo
    #DAG_executor_state = keyword_arguments['DAG_executor_State']
    #server = keyword_arguments['server']
    #STORE_FANINS_FANINNBS_LOCALLY = keyword_arguments['store_fanins_faninNBs_locally']  # option set in DAG_executor
    #DAG_info = keyword_arguments['DAG_info']
    thread_name = threading.current_thread().name

    logger.info (thread_name + ": faninNB_remotely: calling_task_name: " + keyword_arguments['calling_task_name'] + ", calling faninNB with fanin_task_name: " + keyword_arguments['fanin_task_name'])
    #logger.trace("faninNB_remotely: DAG_executor_state.keyword_arguments[fanin_task_name]: " + str(DAG_executor_state.keyword_arguments['fanin_task_name']))
    #FanInNB = server.synchronizers[fanin_task_name]

    # Note: in real code, we would return here so caller can quit, letting server do the op.
    # Here, we can just wait for op to finish, then return. Caller has nothing to do but
    # quit since nothing to do after a fanin.

    # return is: None, restart, where restart is always 0 and return_value is None; and makes no change to DAG_executor_State
    #return_value, restart = FanInNB.fan_in(**keyword_arguments)
    #ToDo:
    # brc: DES
    DAG_exec_state = DAG_executor_State(function_name = "DAG_executor", function_instance_ID = str(uuid.uuid4()))
    DAG_exec_state.keyword_arguments = {}
    DAG_exec_state.keyword_arguments['fanin_task_name'] = keyword_arguments['fanin_task_name']
    DAG_exec_state.keyword_arguments['n'] = keyword_arguments['n']
    DAG_exec_state.keyword_arguments['start_state_fanin_task'] = keyword_arguments['start_state_fanin_task']
    DAG_exec_state.keyword_arguments['result'] = keyword_arguments['result']
    DAG_exec_state.keyword_arguments['calling_task_name'] = keyword_arguments['calling_task_name']
    #ToDo: Don't do/need this?
    #keyword_arguments['DAG_executor_State'] = new_DAG_exec_state # given to the thread/lambda that executes the fanin task.
    #DAG_exec_state.keyword_arguments['server'] = keyword_arguments['server']
    DAG_exec_state.keyword_arguments['store_fanins_faninNBs_locally'] = keyword_arguments['store_fanins_faninNBs_locally']
#brc: batch
    DAG_exec_state.keyword_arguments['FANIN_TYPE'] = keyword_arguments['FANIN_TYPE']
    # Note: This faninNB_remotely method is only called when we are 
    # running locally, i.e., we are not using real lambdas. For real lambdas
    # for faninNBs we always call process_faninNBs_batch and faninNB_remotely_batch.
    # Here, we do not need to pass DAG_info since neither the faninNBs nor
    # the fanins need DAG_info in this case. fanins never need DAG_info since
    # they do not start and lambda etc to do their fanin task; instead the
    # last caller becomes the executor of the fanin task. For faninNBs, when
    # running locally it must be simulated lambdas or worker threads/processes
    # that are running and the faninNB does not create a simulted lambda to
    # run the fanin task since that simulated lambda (thread) would run on 
    # the server and that is not local (if the server is remote.)
    # Deleted:
    #if not RUN_ALL_TASKS_LOCALLY:
        # Note: When faninNB start a Lambda, DAG_info is in the payload. 
        # (Threads and processes read it from disk.)
    #    DAG_exec_state.keyword_arguments['DAG_info'] = keyword_arguments['DAG_info']
    DAG_exec_state.return_value = None
    DAG_exec_state.blocking = False

    # When using lambdas, so faninNB will start a lambda to execute the fanin task, we can use 
    # an asynch call since we ignore the return value/
    # Note: Lambdas use process_faninNBs_batch so we are not calling this faninNB_remotely method
    # when using lambdas and the call to process_faninNBs_batch is asynch.
    DAG_exec_state = synchronize_sync(websocket, "synchronize_sync", keyword_arguments['fanin_task_name'], "fan_in", DAG_exec_state)
    return DAG_exec_state

def create_and_faninNB_remotely_batch(websocket,**keyword_arguments):
    DAG_exec_state = faninNB_remotely_batch(websocket,**keyword_arguments)
    return DAG_exec_state

# Called to process faninNBs when we are not using real lambdas
# or worker processes, which means we are using simulated lambdas
# (a lambda is simulated by a thread) or we are using worker
# threads. Worker threads share a local work queue, while worker
# processes share a remote work queue on tp_servr.
def process_faninNBs(websocket,faninNBs, faninNB_sizes, calling_task_name, DAG_states, 
    DAG_exec_state, output, DAG_info, server, work_queue,worker_needs_input,

#brc: cluster:
    cluster_queue):
    thread_name = threading.current_thread().name
    logger.info(thread_name + ": process_faninNBs")
    logger.info(thread_name + ": process_faninNBs: worker_needs_input: " + str(worker_needs_input))
	# There may be multiple faninNBs; we cannot become one, by definition.
	# Note: This thread cannot become since it may need to become a fanout.
	# Or: faninNB is asynch wo/terminate, so create a thread that does the
	# stuff in DES.create_and_fanin(), and this thread keeps going
	# Note: For real version, DAG_executor task is a Lambda? or thread of a Lambda?
	#       Do we want faninNB to run as a thread of this current Lambda? with other
	#       faninNB threads and fanout become threads? Or always start a new Lambda?
    #       Or make that decision based on this Lammba's load?

    #ToDo: not passing State to FanIn, we set it after return?
    # No changes to State - this is asynch we get no State back, but we do pass it for asynch ops
    # in general since they can be asynch w/terminate and restart
    # for name, n in [(name,n) for name in faninNBs for n in faninNB_sizes]:
    for name, n in zip(faninNBs,faninNB_sizes):

        # create new faninNB with specified name if it hasn't been created
        start_state_fanin_task  = DAG_states[name]
        # The FanINNB fan_in will change the (start) state of the DAG_exec_state. (We are making the exact same change here
        # so these assignments to state are redunadant. If this is part of a local test (any (remote) lambda created will have its own DAG_executor_State)
        # then we need a new DAG_exec_state for the FanInNB; otherwise, the DAG_exec_state of the become task of the fanouts and the
        # DAG_exec_state of the new thread that will execute the faninNB task (i.e., after the fan_in completes) will share the same
        # DAG_exec_state object, and changing the state for the fanout and for the faninNB task is a race condition. Do create a new
        # DAG_exec_state object for the faninNB to eliminate the possibility of sharing.

 #ToDo: else:
       	# keyword_arguments['state'] =
        keyword_arguments = {}
        keyword_arguments['fanin_task_name'] = name
        keyword_arguments['n'] = n
        keyword_arguments['start_state_fanin_task'] = start_state_fanin_task
#brc: batch
        keyword_arguments['FANIN_TYPE'] = "faninNB"
        # We will use local datadict for each multiprocess; process will receve
        # the faninNB results and put them in the data_dict.
        # Note: For DAG generation, for each state we execute a task and 
        # for each task T we have t say what T;s task_inputs are - these are the 
        # names of tasks that give inputs to T. When we have per-fanout output
        # instead of having the same output for all fanouts, we specify the 
        # task_inputs as "sending task - receiving task". So a sending task
        # S might send outputs to fanouts A and B so we use "S-A" and "S-B"
        # as the task_inputs, instad of just using "S", which is the Dask way.
        if DAG_executor_constants.SAME_OUTPUT_FOR_ALL_FANOUT_FANIN:
            keyword_arguments['result'] = output
            keyword_arguments['calling_task_name'] = calling_task_name
        else:
            
            logger.info("**********************" + thread_name + ": process_faninNBs:  for " + calling_task_name + " faninNB "  + name+ " output is :" + str(output))

            #if name.endswith('L'):  
            #    keyword_arguments['result'] = output[name[:-1]]
            #    qualified_name = str(calling_task_name) + "-" + str(name[:-1])
            #else:
            keyword_arguments['result'] = output[name]
            qualified_name = str(calling_task_name) + "-" + str(name)

            logger.trace(thread_name + ": process_faninNBs: name:" + str(name) 
                + " qualified_name: " + qualified_name)
            keyword_arguments['calling_task_name'] = qualified_name
        #ToDo: Don't do/need this?
        #keyword_arguments['DAG_executor_State'] = new_DAG_exec_state # given to the thread/lambda that executes the fanin task.
        keyword_arguments['server'] = server
        keyword_arguments['store_fanins_faninNBs_locally'] = DAG_executor_constants.STORE_FANINS_FANINNBS_LOCALLY
        # Note: This process_faninNBs method is only called when we are 
        # running locally, i.e., we are not using real lambdas. 
        #
        # For real lambdas, we always storing synch objects remotely.
        # Likewise for worker processes. For real lambdas and worker processes, 
        # we always call process_faninNBs_batch and faninNB_remotely_batch.
        # That is, we do not call this process_faninNBs method.
        # Here, we have a choice about whether or not to pass DAG_info 
        # to the FaninNB. (DAG_info may be large for large DAGs)
        # (Note we do not need to pass DAG_info for FanIns.
        # FanIns never need DAG_info since they do not start a lambda etc 
        # to execute their fanin task; instead the last caller to all fanin() 
        # becomes the executor of the fanin task.) For faninNBs, we only 
        # call process_faninNBs when we are running simulated lambdas (as threads)
        # or we are using worker threads. For these two cases, we distinguihs
        # between storing the synch objects (FaninNbs) on the tcp_server versus 
        # storing the sync objects locally. When the FaninNB is on 
        # tcp_server, the faninNB does not create a simulated lambda to run the 
        # fanin task since that simulated lambda (thread) would run on 
        # the server. Likewise, a FaninNb on tcp_server dos not enqueue
        # the fanin task in the worker thread's work queue since the 
        # shared work queue is not on the tcp_server (as it is for
        # worker processes); rather, it is a local object that cannot 
        # be directly accessed by tcp_server. In these cases, the
        # FaninNB on the tcp_server, just returns the fanin results 
        # to the simulated lambda or worker thread that made the remote
        # call to fanin() and the simulated lambda or worker thread 
        # deals with starting a new simulated lamba to execute the 
        # fanin task, or adding the fanin task to the work queue. In
        # either case, the simulated lambda or worker thread both have
        # access to their local DAG_info (which they raad from a 
        # file and so do not need the FaninNB to supply it.)
        # When the FaninNB is stored locally (as a regular object in 
        # the Python program) it creates a simulated lambda (thread) 
        # to execute the fanin task or it adds the fanin task 
        # to the local shared work queue of the worker threads. The 
        # question is whether FaninNB needs to pass DAG_info to the 
        # simulated lambda or worker thread that excutes the 
        # fanin task. For simulated lambdas, the answer is yes since
        # simulated lambdas expect the DAG_info to be part of their 
        # payload (just like the real lambdas they are simulating).
        # Worker threads do not need the FaninNB to pass DAG_info 
        # to them as worker threads read DAG_info from a local file.
        # (as do worker processes)
        # 
        # pass DAG_info if we are using simulated lambdas. If
        # we are using worker threads pass None - the FaninNB
        # will not use DAG_info if we aer using worker threads.
        # Note: not RUN_ALL_TASKS_LOCALLY means we are using 
        # real lambdas. When RUN_ALL_TASKS_LOCALLY and 
        # not USING_WORKERS means we are usin simulated lambdas.
        if not DAG_executor_constants.RUN_ALL_TASKS_LOCALLY or (DAG_executor_constants.RUN_ALL_TASKS_LOCALLY and not DAG_executor_constants.USING_WORKERS and DAG_executor_constants.STORE_FANINS_FANINNBS_LOCALLY):
            keyword_arguments['DAG_info'] = DAG_info
        else:
            keyword_arguments['DAG_info'] = None

        # We start a thread that makes the local call to server.create_and_faninNB_locally
        # or server.faninNB_locally. server.create_and_faninNB_locally will
        # create the local FaninB and call its init() method passing this DAG_info.

		#Q: kwargs put in DAG_executor_State keywords and on server it gets keywords from state and passes to create and fanin

        if DAG_executor_constants.STORE_FANINS_FANINNBS_LOCALLY:
            if not DAG_executor_constants.USING_WORKERS:
                # Note: We start a thread to make the fan-in call and we don't wait for it to finish.
                # So this is like an asynch call to tcp_server. The faninNB will start a new 
                # thread to execute the fanin task so the callers do not need to do anything.
                # Again, "NB" is "No Become" so no caller will become the executor of the fanin task.
                # keyword_arguments['DAG_executor_State'] = new_DAG_exec_state 
                # given to the thread/lambda that executes the fanin task.
                if not DAG_executor_constants.CREATE_ALL_FANINS_FANINNBS_ON_START:
                    try:
                        logger.trace(thread_name + ": process_faninNBs: Starting asynch simulation create_and_fanin task for faninNB " + name)
                        NBthread = threading.Thread(target=create_and_faninNB_task_locally, name=("create_and_faninNB_task_"+name), args=(keyword_arguments,))
                        NBthread.start()
                    except Exception:
                        logger.exception("[ERROR]:" + thread_name + ": process_faninNBs: Failed to start create_and_faninNB_task thread.")
                        if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
                            logging.shutdown()
                            os._exit(0) 
                else:
                    try:
                        logger.trace(thread_name + ": process_faninNBs: Starting asynch simulation faninNB_task for faninNB " + name)
                        NBthread = threading.Thread(target=faninNB_task_locally, name=("faninNB_task_"+name), args=(keyword_arguments,))
                        NBthread.start()
                    except Exception:
                        logger.exception("[ERROR]:" + thread_name 
                            + ": process_faninNBs: Failed to start faninNB_task thread.")
                        if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
                            logging.shutdown()
                            os._exit(0) 
            else:
                # When we are using workers, we use this faster code which calls the
                # faninNB locally directly instead of starting a thread to do it. (Starting
                # a thread simulates calling fanin asynchronously.)
                if not DAG_executor_constants.CREATE_ALL_FANINS_FANINNBS_ON_START:
                    logger.trace(thread_name + ": process_faninNBs: call create_and_faninNB_locally")
                    #server = kwargs['server']
                    # Not using return_value from faninNB since faninNB starts the fanin task, i.e., there is No Become
                    _return_value_ignored = server.create_and_faninNB_locally(**keyword_arguments)
                else:
                    logger.trace(thread_name + ": process_faninNBs: call faninNB_locally")
                    #server = kwargs['server']
                    # Not using return_value from faninNB since faninNB starts the fanin task, i.e., there is No Become
                    _return_value_ignored = server.faninNB_locally(**keyword_arguments)

            #Note: returning 0 since when running locally the faninNB will start 
            # the fanin task so there is nothing to do.
        else:
            # create_and_faninNB_remotely simply calls faninNB_remotely(websocket,**keyword_arguments)
            # All the create logic is on the server, so we call faninNB_remotely in
            # either case. We leave this call to create_and_faninNB_remotely 
            # as a placeholder in case we decide to do smething differet - 
            # as in create_and_faninNB_remotely could create and pass a new message to 
            # the server, i.e., move the create logic down from the server.
            # Note: keyword_arguments are used to create DAG_executor_state/keyword_arguments
            # in faninNB_remotely.
            if not DAG_executor_constants.CREATE_ALL_FANINS_FANINNBS_ON_START:
                dummy_DAG_exec_state = create_and_faninNB_remotely(websocket,**keyword_arguments)
            else:
                dummy_DAG_exec_state = faninNB_remotely(websocket,**keyword_arguments)

            #if DAG_exec_state.blocking:
            # using the "else" after the return, even though we don't need it
            logger.trace(thread_name + ": process_faninNBs:  faninNB_remotely: dummy_DAG_exec_state: " + str(dummy_DAG_exec_state))
            logger.info(thread_name + ": process_faninNBs:  faninNB_remotely: before if: dummy_DAG_exec_state.return_value: " + str(dummy_DAG_exec_state.return_value))
            if dummy_DAG_exec_state.return_value == 0:
                logger.info(thread_name + ": process_faninNBs:  faninNB_remotely: then after if: dummy_DAG_exec_state.return_value: " + str(dummy_DAG_exec_state.return_value))
                pass
            else:
                logger.info(thread_name + ": process_faninNBs:  faninNB_remotely: else after if: dummy_DAG_exec_state.return_value: " + str(dummy_DAG_exec_state.return_value))
                # Note: When we aer USING_WORKERS we now call process_fsninNBs_batch
                # so this code is not currently being used. Batch processing processes
                # all the faninNBs at once on the server, rather than calling 
                # the server to process the faninnbs one by one.
                if DAG_executor_constants.USING_WORKERS:
                    # this caller could be a thread or a process
                    
                    dict_of_results = dummy_DAG_exec_state.return_value
                    
                    if not worker_needs_input:
                        # Also, don't pass in the multp data_dict, so will use the global.
                        # Fix if in global
                        logger.trace(thread_name + ": process_faninNBs: faninNB Results: ")
                        for key, value in dict_of_results.items():
                            logger.trace(str(key) + " -> " + str(value))
#ToDo: if we need work because no fanouts then we should keep this work 
# instead of enqueing it: pass worker_needs_input here and then
# if not worker_needs_input:
#  ....
# else:
#    DAG_exec_state.state = start_state_fanin_task
#
# Problem, we out results in our local data_dict but then we might give work
# to another process by enquing it. If we need work (no fsnouts or we already
# were become task for a faninNB) then we should do the work instead of 
# enqueing it. (This will be worked out on tcp_server). If we need to enqueue work, 
# then we also need to enqueue results? If so, then do enqueue of faninNB results
# on tcp_server, as planned?
#Problem: Fanout results (for non-become fanouts) need to be enqueued too when
# we enqueue state. So enqueue a tuple (state,results) in work_queue for
# processes, not threads. 

                        #thread_work_queue.put(start_state_fanin_task)
                        if not DAG_executor_constants.USING_THREADS_NOT_PROCESSES:
                            work_tuple = (start_state_fanin_task,dict_of_results)
                            #work_queue.put(start_state_fanin_task)
                            work_queue.put(work_tuple)
                        else: 
                            # Note: if using worker processes, we call process_faninNB_batch
                            # instead of process_faninNBs, so this code is currently not executable. 
                            work_tuple = (start_state_fanin_task,dict_of_results)
                            #work_queue.put(start_state_fanin_task)
                            work_queue.put(work_tuple)
                    else:
                        # put results in our data_dict since we will use them next
                        # Note: We will be writog over our result from the task we 
                        #  did that inputs into this faninNB.

                        logger.trace(thread_name + ": process_faninNBs: add faninNB Results to data dict: ")
                        for key, value in dict_of_results.items():
                            data_dict[key] = value
                            logger.trace(str(key) + " -> " + str(value))

                        DAG_exec_state.state = start_state_fanin_task
#brc: cluster:
                        cluster_queue.put(DAG_exec_state.state)
#brc: cluster:
                        # Do this since we get it as return value
                        worker_needs_input = False
                        logger.trace(thread_name + ": process_faninNBs:  Got work (become task), added it to data_dict and cluster_queue, set worker_needs_input to False: "
                            + " start_state_fanin_task: " + str(start_state_fanin_task))

                else: 
                    # not USING_WORKERS so we are using threads to simulate using lambdas.
                    # However, since the fannNB on tcp_server cannot crate a thread to execute
                    # the fanin task (since the thread would run on the tcp_server) the start
                    # state is returned and we create the thread here. This deviates from the
                    # lambda simulation since the faninNB on the tcp_server can invoke lamdas to
                    # execute the fanin task.
                    # Note: If we are using threads to simulate using lambdas and we are storing
                    # sync objects in Lambdas (simulated or not) then we call process_faninNB_batch()
                    # which just returns a batch of work to the thread caller, which starts new
                    # threads (simulating lambdas) to execute the fanin tasks. We use batch for this
                    # case since we want to test the lambda logic when we are storing snc objects
                    # in lambdas and the lambdas may be simulated by python functions and we are
                    # optionally using the DAG_ochestrator.
                    # Note: we do not support this configuration when the sync objects can trigger
                    # their tasks and the tasks run locally in the same lambda as the object.
                    # By definition, it is lambdas tha are runnnin the triggered tasks, not threads
                    # that are simulating lambdas. Thus, using threads to simulate lambdas and havng
                    # sync ojeccts trigger local execuions of their tasks is not allowed.
                    
                    #if not worker_needs_input:
                    try:
                        msg = "[Error]: " + thread_name + ": process_faninNBs: not USING_WORKERS but worker_needs_input = True"
                        assert not worker_needs_input , msg
                    except AssertionError:
                        logger.exception("[Error]: assertion failed")
                        if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
                            logging.shutdown()
                            os._exit(0)
                    #assertOld:
                    #if worker_needs_input:
                    #    logger.error("[Error]: " + thread_name + ": process_faninNBs: not USING_WORKERS but worker_needs_input = True")
                    
                    try:
                        logger.info(thread_name + ": process_faninNBs: starting DAG_executor thread for task " + name + " with start state " + str(start_state_fanin_task))
                        #server = kwargs['server']
                        #DAG_executor_state =  kwargs['DAG_executor_State']
                        #DAG_executor_state.state = int(start_state_fanin_task)
                        new_DAG_executor_state = DAG_executor_State(function_name = "DAG_executor", function_instance_ID = str(uuid.uuid4()), state = start_state_fanin_task)
                        new_DAG_executor_state.restart = False      # starting  new DAG_executor in state start_state_fanin_task
                        #DAG_executor_state.return_value = None
                        new_DAG_executor_state.blocking = False
                        payload = {
                            #"state": int(start_state_fanin_task),
                            #"input": DAG_executor_state.return_value,
                            "input": dummy_DAG_exec_state.return_value,
                            "DAG_executor_state": new_DAG_executor_state,
                            "DAG_info": DAG_info,
                            #"server": server
                        }
                        logger.info(thread_name + ": process_faninNBs: starting DAG_executor thread for task " + name + " with payload input " + str(dummy_DAG_exec_state.return_value))
                        thread_name_prefix = "Thread_faninNB_"
                        thread = threading.Thread(target=DAG_executor_task, name=(thread_name_prefix+str(start_state_fanin_task)), args=(payload,))
                        thread.start()
                        #_thread.start_new_thread(DAG_executor.DAG_executor_task, (payload,))
                    except Exception:
                        logger.exception("[ERROR]:" + thread_name + ": process_faninNBs: Failed to start DAG_executor thread.")
                        if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
                            logging.shutdown()
                            os._exit(0) 

                    # USING_WORKERS is false so worker_needs_input should never be true
                    #else:
                    #   worker_needs_input = False
                    #   DAG_exec_state.state = start_state_fanin_task
                    #   logger.trace("process_faninNBs: set worker_needs_input to False,")

                #return 1
                #logger.trace("process_faninNBs: returning worker_needs_input: " + str(worker_needs_input))
                #return worker_needs_input
    # return value not used; will process any fanouts next; no change to DAG_executor_State
    #return 0
    logger.info(thread_name + ": process_faninNBs:  returning worker_needs_input: " + str(worker_needs_input))
    return worker_needs_input

#def faninNB_remotely_batch(websocket, faninNBs, faninNB_sizes, calling_task_name, DAG_states, 
#    DAG_exec_state, output, DAG_info, work_queue, worker_needs_input, **keyword_arguments):
# Called by process_faninNBs_batch; sends the faninNBs and more to the server
def faninNB_remotely_batch(websocket, **keyword_arguments):
    #Todo: remove DAG_exec_state from parm list
    thread_name = threading.current_thread().name
    logger.trace (thread_name + " faninNB_remotely_batch: calling_task_name: " + keyword_arguments['calling_task_name'] 
        + " calling process_faninNBs_batch with fanin_task_names: " + str(keyword_arguments['faninNBs']))

    # brc: DES
    DAG_exec_state = DAG_executor_State(function_name = "DAG_executor", function_instance_ID = str(uuid.uuid4()))
    DAG_exec_state.keyword_arguments = {}
    #DAG_exec_state.keyword_arguments['fanin_task_name'] = keyword_arguments['fanin_task_name']
    #DAG_exec_state.keyword_arguments['n'] = keyword_arguments['n']
    #DAG_exec_state.keyword_arguments['start_state_fanin_task'] = keyword_arguments['start_state_fanin_task']
    DAG_exec_state.keyword_arguments['result'] = keyword_arguments['result']
    DAG_exec_state.keyword_arguments['calling_task_name'] = keyword_arguments['calling_task_name']
    DAG_exec_state.keyword_arguments['store_fanins_faninNBs_locally'] = keyword_arguments['store_fanins_faninNBs_locally']
    # Need these on tcp_server to process the faninNBs
    DAG_exec_state.keyword_arguments['faninNBs'] = keyword_arguments['faninNBs']
    # brc: run task
    # fanouts are used when sync objects trigger their tasks to run in the same lambda as object
    DAG_exec_state.keyword_arguments['fanouts'] = keyword_arguments['fanouts']
    DAG_exec_state.keyword_arguments['faninNB_sizes'] = keyword_arguments['faninNB_sizes']
    DAG_exec_state.keyword_arguments['worker_needs_input'] = keyword_arguments['worker_needs_input']
    DAG_exec_state.keyword_arguments['work_queue_name'] = keyword_arguments['work_queue_name']
    DAG_exec_state.keyword_arguments['work_queue_type'] = keyword_arguments['work_queue_type']
    DAG_exec_state.keyword_arguments['work_queue_method'] = keyword_arguments['work_queue_method']
    DAG_exec_state.keyword_arguments['work_queue_op'] = keyword_arguments['work_queue_op']
    DAG_exec_state.keyword_arguments['DAG_states_of_faninNBs_fanouts'] = keyword_arguments['DAG_states_of_faninNBs_fanouts']
#brc: batch
    DAG_exec_state.keyword_arguments['all_faninNB_sizes_of_faninNBs'] = keyword_arguments['all_faninNB_sizes_of_faninNBs']
    DAG_exec_state.keyword_arguments['all_fanin_sizes_of_fanins'] = keyword_arguments['all_fanin_sizes_of_fanins']

    # we now read DAG_info on the tcp_server when not creating objects at the start
    # or not RUN_ALL_TASKS_LOCALLY (i.e., lambdas excute tasks and need the DAG_info.)
    # but only when not doing incremental DAG generation. When doing incremental
    # DAG generation we do not read DAG_info at the start since we would have
    # to keep reading the updated incrmental DAGs whenever one was generated.
    # We do not want to do this. Instead, when usng incrmental DAG generation.
    # we will pass the DAG_info as part of the batch processing of faninNBs/fanouts.
    # Note: We only need to pass a given version of a DAG one time to the batch
    # process method. (A real lambda may call the batch process method multiple
    # tims (one for the set of fans/fanouts of the tasks it excutes, and the lambda
    # can track the versions it has alrady sent.))
    # The batch process method can save the new versions of
    # the DAG info as it gets them. (Need to lock the version number read/write in
    # batch method since there can be concurrent calls to the batch process method.)
    # Note: For incremental DAG generation, we only do fanouts/fanins when
    # we are using groups - whwn using partitions a task only has a collapse
    # task, no fanins/fanouts. So we would only pass the DAG_info when doing
    # groups not partitions. Likewise, the batch process method on tcp_server
    # would only get a DAG_info from real lambdas when doing groups, not partitions.
    #
    # default value
    DAG_exec_state.keyword_arguments['DAG_info'] = None
    if not DAG_executor_constants.RUN_ALL_TASKS_LOCALLY:
        if DAG_executor_constants.COMPUTE_PAGERANK and DAG_executor_constants.USE_INCREMENTAL_DAG_GENERATION:
            # Note: When faninNB start a Lambda, DAG_info will be in the payload so pass DAG_info to faninNBs.
            # (Worker Threads and processes read DAG_info from disk. Real Lambdas can read DAG_info from
            # disk on the tcp_server but not when incremental DAG generation is used.
            # In that case, the DAG is not complete so tcp_server cannot read it as
            # the start. Instead, we pass DAG_info to tcp_server.
            DAG_exec_state.keyword_arguments['DAG_info'] = keyword_arguments['DAG_info']
        else:
            pass # value will be the default value None above
            
    DAG_exec_state.keyword_arguments['number_of_tasks'] = keyword_arguments['number_of_tasks']

    # piggybacking the (possibly empty) list if work_tuples generated by the fanouts, if any
    DAG_exec_state.keyword_arguments['list_of_work_queue_or_payload_fanout_values'] = keyword_arguments['list_of_work_queue_or_payload_fanout_values']
#brc: async batch
    DAG_exec_state.keyword_arguments['async_call'] = keyword_arguments['async_call']


    # Don't do/need this?
    # DAG_exec_state.keyword_arguments['DAG_executor_State'] = new_DAG_exec_state # given to the thread/lambda that executes the fanin task.
    
    # No longer putting this in payload of fanin task
    # keyword_arguments['server'] = server

    DAG_exec_state.return_value = None
    DAG_exec_state.blocking = False

    # When using lambdas, so faninNB will start a lmabda to execute the fanin task, we can use 
    # an asynch call since we ignore the return value.
    # Note: for lambdas, we use the same synchronize_process_faninNBs_batch method but just
    # pass worker_needs_input = False. FaninNBs will start Lambdas and no work will be returned and
    # we can ignore the results, same as for workers.
    DAG_exec_state = synchronize_process_faninNBs_batch(websocket, "synchronize_process_faninNBs_batch", DAG_executor_constants.FANINNB_TYPE, "fan_in", DAG_exec_state)
    return DAG_exec_state

# Called when we are storing fanins and faninNBs remotely and we are USING_WORKERS and we are 
# using processes instead of threads. This batches the faninNB processing.
# If we are storing fanins and faninNBs remotely and we are not USING_WORKERS then we are 
# using a single thread that starts other threads to do fanouts. However, since the faninNBs are 
# stored on the tcp_server, the faninNBs cannot start new threads to execute the fanin tasks,
# unlike when using lambdas where we can start new lambdas to excute the fanin tasks.
# So we cannot "simulate" the use of lambdas when faninNBs are stored remotely.
# For that we call process_faninNBs, which does not batch faninNB processing. The faninNB can 
# return work to the calling thread and this thread can start a new thread (instead of letting the
# faninNB do it) that runs locally.
def process_faninNBs_batch(websocket,faninNBs, faninNB_sizes, calling_task_name, DAG_states, 
    DAG_exec_state, output, DAG_info, work_queue,worker_needs_input,
    list_of_work_queue_or_payload_fanout_values,
    async_call, fanouts,
#brc: cluster:
    cluster_queue,
#brc: batch
    all_faninNB_task_names,all_faninNB_sizes):

    thread_name = threading.current_thread().name
    logger.trace(thread_name + ": process_faninNBs_batch: " + calling_task_name)
    logger.trace(thread_name + ": process_faninNBs_batch: " + calling_task_name + ": worker_needs_input: " + str(worker_needs_input))
    # brc: async batch
    logger.trace(thread_name + ": process_faninNBs_batch: " + calling_task_name + ": async_call: " + str(async_call))

    try:
        msg = thread_name + "[Error]: process_faninNBs_batch: worker_needs_input but using async_call"
        assert not (worker_needs_input and async_call) , msg
    except AssertionError:
        logger.exception("[Error]: assertion failed")
        if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
            logging.shutdown()
            os._exit(0)
    #assertOld: if worker needs work then we should be using synch call so we can check the results for work
    #if worker_needs_input:
    #    if async_call:
    #        logger.error(thread_name + "[Error]: process_faninNBs_batch: worker_needs_input but using async_call")

	# There may be multiple faninNBs; we cannot become one, by definition.
	# Note: This thread cannot become since it may need to become a fanout.
	# Or: faninNB is asynch wo/terminate, so create a thread that does the
	# stuff in DES.create_and_fanin(), and this thread keeps going
	# Note: For real version, DAG_executor task is a Lambda? or thread of a Lambda?
	#       Do we want faninNB to run as a thread of this current Lambda? with other
	#       faninNB threads and fanout become threads? Or always start a new Lambda?
    #       Or make that decision based on this Lammba's load?

    #ToDo: not passing State to FanIn, we set it after return?
    # No changes to State - this is asynch we get no State back, but we do pass it for asynch ops
    # in general since they can be asynch w/terminate and restart
    # for name, n in [(name,n) for name in faninNBs for n in faninNB_sizes]:

    keyword_arguments = {}
    #keyword_arguments['fanin_task_name'] = name
    #keyword_arguments['n'] = n
    # FaninNB uses this
    #keyword_arguments['start_state_fanin_task'] = start_state_fanin_task
    # We will use a local datadict for each multiprocess; process will receve
    # the faninNB results and put them in it local data_dict. When using Lambdas,
    # the faninNB will pass the results to the lambda that is ivoked to execut the fanin task.
    keyword_arguments['result'] = output
    keyword_arguments['calling_task_name'] = calling_task_name
    keyword_arguments['store_fanins_faninNBs_locally'] = DAG_executor_constants.STORE_FANINS_FANINNBS_LOCALLY
    # Need on tcp_server to process the faninNBs
    keyword_arguments['faninNBs'] = faninNBs
# brc: run task
    # fanouts are used when sync objects trigger their tasks to run in the same lambda as object
    keyword_arguments['fanouts'] = fanouts
    keyword_arguments['faninNB_sizes'] = faninNB_sizes   
    keyword_arguments['worker_needs_input'] = worker_needs_input  
    keyword_arguments['work_queue_name'] = "process_work_queue"
    # defined in DAG_executor_constants: either select or non-select version
    keyword_arguments['work_queue_type'] = DAG_executor_constants.PROCESS_WORK_QUEUE_TYPE 
    keyword_arguments['work_queue_method'] = "deposit_all"
    keyword_arguments['work_queue_op'] = "synchronize_async"
    # faninNB_remotely_batch not necessarily sending this to the tcp_server batch processing method.
    # DAG_info is not sent when we are not using incremental DAG generation
    # since the complete DAG_info is read from file by the tcp_server 
    # in the bath processing method)
    # before creating a faninNB (it is read only once). For incremental
    # DAG generation, the DAG_info is not complete so we do not read
    # it from file; instead, we pass the DAG_info to the batch processing
    # method. (Although, we only need to pass *a given version* once.)
    #Note: we only pass ADG_info when we are doing incremental DAG
    # generation with real lambdas, i.e, not wukongdnc.dag.DAG_executor_constants.RUN_ALL_TASKS_LOCALLY.
    # When we running locally with simulated lambdas (threads) and worker
    # threads or processes, the simulated lambdas and worker threads and
    # processes read DAG_info locally from a file so they have DAG_info
    # when they need it. (Simulated lambdas pass DAG_info in the payload
    # of a started simulated lambda to simulate real lambdas and the 
    # DAG_executor_driver also reads DAG_info from a file and passes
    # it to the simulated lambdas it creates in their payload.)
    # Note: When we are stroing objects in lambdas, etc, then we use 
    # tcp_server_lambda and currently we always use simulated lambdas.
    # These simulated lambdas will call the batch method in this case 
    # so if we support incremental DAG generation for this case, we 
    # may need to change this condition so that DAG_info will be passed.
    if (wukongdnc.dag.DAG_executor_constants.COMPUTE_PAGERANK and wukongdnc.dag.DAG_executor_constants.USE_INCREMENTAL_DAG_GENERATION and (
        not wukongdnc.dag.DAG_executor_constants.RUN_ALL_TASKS_LOCALLY)
    ):
        keyword_arguments['DAG_info'] = DAG_info
    # Get a slice of DAG_states that is the DAG states of just the faninNB and fanout tasks.
    # Instead of sending all the DAG_states, i.e., all the states in the DAG, to the server.
    # Need this to put any faninNB work that is not returned in the work_queue - work is added as
    # a tuple (start state of task, inputs to task). Fanouts are used when sync objects trigger 
    # their tasks to run in the same lambda as object. This occurs when we are storing objects 
    # in lambdas and running tcp_server_lamba.py
    DAG_states_of_faninNBs_fanouts = {}
#brc: batch
    all_faninNB_sizes_of_faninNBs = []
    # We only batch process faninNBs, not fanins, so this is empty
    all_fanin_sizes_of_fanins = []
    for name in faninNBs:
        DAG_states_of_faninNBs_fanouts[name] = DAG_states[name]
#brc: batch
        faninNB_index = all_faninNB_task_names.index(name)
        all_faninNB_sizes_of_faninNBs.append(all_faninNB_sizes[faninNB_index])

    for name in fanouts:
        DAG_states_of_faninNBs_fanouts[name] = DAG_states[name]

    keyword_arguments['DAG_states_of_faninNBs_fanouts'] = DAG_states_of_faninNBs_fanouts
    keyword_arguments['all_faninNB_sizes_of_faninNBs'] = all_faninNB_sizes_of_faninNBs
    keyword_arguments['all_fanin_sizes_of_fanins'] = all_fanin_sizes_of_fanins
    
    # if not creating objects at start then when we create the work queue
    # on the server we use number_of_tasks to determine the queue size.
    keyword_arguments['number_of_tasks'] = len(DAG_info.get_DAG_tasks())

    keyword_arguments['list_of_work_queue_or_payload_fanout_values'] = list_of_work_queue_or_payload_fanout_values
# brc: async batch
    keyword_arguments['async_call'] = async_call


 	#ToDo: kwargs put in DAG_executor_State keywords and on server it gets keywords from state and passes to create and fanin

    # This CREATE_ALL_FANINS_FANINNBS_ON_START is for creating sync objects
    # on the tcp_server. If we STORE_SYNC_OBJECTS_IN_LAMBDAS and 
    # not CREATE_ALL_FANINS_FANINNBS_ON_START the tcp_server_lambda or
    # or the lambdas themselves will create the object to be stored in them.
    if not DAG_executor_constants.CREATE_ALL_FANINS_FANINNBS_ON_START and (
            not DAG_executor_constants.STORE_SYNC_OBJECTS_IN_LAMBDAS):
        # create_and_faninNB_remotely_batch simply calls faninNB_remotely_batch(websocket,**keyword_arguments)
        # All the create logic is on the server, so we call faninNB_remotely_batch in
        # either case. We leave this call to create_and_faninNB_remotely_batch 
        # as a placeholder in case we decide to do smething differet - 
        # as in create_and_faninNB_remotely_batch could create and pass a new message to 
        # the server, i.e., move the create logic down from the server.
        logger.trace(thread_name + ": process_faninNBs_batch: call create_and_faninNB_remotely_batch.") 
        dummy_DAG_exec_state = create_and_faninNB_remotely_batch(websocket,**keyword_arguments)
    else:
        logger.trace(thread_name + ": process_faninNBs_batch: call faninNB_remotely_batch.") 
        dummy_DAG_exec_state = faninNB_remotely_batch(websocket,**keyword_arguments)

    #if DAG_exec_state.blocking:
    # using the "else" after the return, even though we don't need it
    if dummy_DAG_exec_state.return_value == 0:
        #DAG_exec_state.blocking = False
        # nothing to do; if worker_needs_input is True then there was no input to be gotten
        # from the faninNBs, i.e., we were not the last task to call fan_in for any faninNB in the batch.
        logger.trace(thread_name + ": process_faninNBs_batch: " + calling_task_name + " received no work with worker_needs_input: " + str(worker_needs_input))
        return worker_needs_input
    else:
        # we only get work from process_faninNBs_batch() when we are using workers and worker needs input,
        # or we are simulating lambas and storing synch objects remotely. (If we are using threads simulating lambdas
        # and storing synch objects locally, the FaninNBs start threads that simulate lambdas.
        # (RUN_ALL_TASKS_LOCALLY and USING_WORKERS and not USING_THREADS_NOT_PROCESSES) or (not RUN_ALL_TASKS_LOCALLY) or (RUN_ALL_TASKS_LOCALLY and not USING_WORKERS and not STORE_FANINS_FANINNBS_LOCALLY and USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS):reads (locally) to run the fan_in tasks.)

#brc: async task ToDo: don't need to be using sims
        try:
            msg = "[Error]: " + thread_name + ": process_faninNBs_batch: (not using worker processes, or worker_needs_input is False) and not (storing synch objects remotely in simulated lambdas" \
                + " and using threads to simulate lambdas) but using process_faninNBs batch and we got work back from server."
            assert (DAG_executor_constants.RUN_ALL_TASKS_LOCALLY and DAG_executor_constants.USING_WORKERS and not DAG_executor_constants.USING_THREADS_NOT_PROCESSES and worker_needs_input) and not (DAG_executor_constants.RUN_ALL_TASKS_LOCALLY and not DAG_executor_constants.USING_WORKERS and not DAG_executor_constants.STORE_FANINS_FANINNBS_LOCALLY and (
                DAG_executor_constants.USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS) and not DAG_executor_constants.SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS) , msg
        except AssertionError:
            logger.exception("[Error]: assertion failed")
            if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
                logging.shutdown()
                os._exit(0)
        #assertOld:
        #if not (RUN_ALL_TASKS_LOCALLY and USING_WORKERS and not USING_THREADS_NOT_PROCESSES and worker_needs_input) and not (RUN_ALL_TASKS_LOCALLY and not USING_WORKERS and not STORE_FANINS_FANINNBS_LOCALLY and (
        #    USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS) and not SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS):
        #    logger.error("[Error]: " + thread_name + ": process_faninNBs_batch: (not using worker processes, or worker_needs_input is False) and not (storing synch objects remotely in simulated lambdas"
        #        + " and using threads to simulate lambdas) but using process_faninNBs batch and we got work back from server.")

        if DAG_executor_constants.USING_WORKERS:
            # return_value is a tuple (start state fanin task, dictionary of fanin results)
            start_state_fanin_task = dummy_DAG_exec_state.return_value[0]
            fanin_task_name = DAG_info.get_DAG_map()[start_state_fanin_task].task_name
            logger.trace(thread_name + ": process_faninNBs_batch: " + calling_task_name + " received work for fanin task " + fanin_task_name 
                + " and start_state_fanin_task " + str(start_state_fanin_task) + " with worker_needs_input: " + str(worker_needs_input))
            # This must be true since we only call process_faninNBs_batch if this is true; otherwise, we call process_faninNBs
            # to process a single faninNB. Note: when we use Lambdas we do not use workers; instead, the faninNBs
            # create a new Lambda to excute the fanin task. No work is enqueued for a pool of Lambdas.
            # Leaving the if here for now.
#brc: simulated threads: move if up
            #if USING_WORKERS:
            # worker can be a thread of a process
            dict_of_results = dummy_DAG_exec_state.return_value[1]
            if not worker_needs_input:
                # Note: assserted USING_WORKERS and worker_needs_input above so ...
                # 
                # This should be unreachable; leaving it for now.
                # If we don't need work then any work from faninNBs should have been enqueued in work queue instead of being returned

                try:
                    msg = "[Error]: DAG_executor " + thread_name + ": process_faninNBs_batch: got work but not worker_needs_input." \
                        + " This should be unreachable."
                    assert False , msg
                except AssertionError:
                    logger.exception("[Error]: assertion failed")
                    if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
                        logging.shutdown()
                        os._exit(0)
                #assertOld:
                #logger.error("[Error]: " + thread_name + ": process_faninNBs_batch: got work but not worker_needs_input.")
                
                # Also, don't pass in the multprocessing data_dict, so will use the global data dict
                logger.trace(thread_name + ": process_faninNBs_batch: " + calling_task_name + ": process_faninNBs_batch Results: ")
                for key, value in dict_of_results.items():
                    logger.trace(str(key) + " -> " + str(value))
                #thread_work_queue.put(start_state_fanin_task)
                if not DAG_executor_constants.USING_THREADS_NOT_PROCESSES:
                    work_tuple = (start_state_fanin_task,dict_of_results)
                    #work_queue.put(start_state_fanin_task)
                    work_queue.put(work_tuple)
                else: 
                    work_tuple = (start_state_fanin_task,dict_of_results)
                    #work_queue.put(start_state_fanin_task)
                    work_queue.put(work_tuple)
            else:
                # put results in our data_dict since this worker will use them next
                # Note: We will be writing over our result from the task this worker
                #  did before it sent the task results to the faninNB.
                for key, value in dict_of_results.items():
                    data_dict[key] = value

                DAG_exec_state.state = start_state_fanin_task
#brc: cluster:
                cluster_queue.put(DAG_exec_state.state)
#brc: cluster: Do this
                # keep work and do it next
                worker_needs_input = False
                logger.trace(thread_name + ": process_faninNBs_batch: " + calling_task_name + ": got work, added it to data_dict and cluster_queue, set worker_needs_input to False.")
        else:
            # We call process_faninNBs_batch when we are using threads to simulate lambdas and
            # storing objects remotely in simulated lambdas and these objects do not execute 
            # the fanin tasks in the same lambda the object is stored in.
            # In this case, we will be running tcp_server_lambda, which has a 
            # synchronize_process_faninNBs_batch method that returns a list of non-empty 
            # work-tuples and a new threead (to simulate lambdas) s started
            # for each tuple. 
            """
            store objects in lambas - workers or no workers
                lambdas are real, i.e., not simulated (IMPLEMENTED)
                lambdas are simulated by python function (IMLEMENTED)
                    object does not trigger task; instead object returns results to calling thread
                    object triggers task to be executd in same lamba, so no results returned

            So if using threads to simulate lambdas that execute the tasks and are separate
            from the lambdas that store synch objects, i.e., sync objects stored in lambdas
            (real or simulated) do not trigger their fanin tasks, the collected results 
            must be returned to the calling task.

            So we are working now with python functions that simulate lambdas but we should
            be able to switch over to real lambdas and keep things working. So we don't
            need to check whether we aer using simulated lambdas or not, we just check
            whether we are storing objects in lambdas (simulated or not) - we set the 
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
            """

            # Not USING_WORKERS so we are using threads to simulate using lambdas.
            # However, since the faninNB on tcp_server cannot crate a thread to execute
            # the fanin task (since the thread would run on the tcp_server) the start
            # state is returned and we create the thread here. This deviates from using
            # real lambdas since in that case the faninNB on the tcp_server can invoke 
            # a real lamda to execute the fanin task.

            # if not USING_WORKERS then not worker_needs_input must be true. That is, we init worker_needs_input
            # to false and we never set it to true since setting worker_needs_input is guarded everywhere by USING_WORKERS.
            try:
                msg = "[Error]:" + thread_name + ": process_faninNBs_batch: not USING_WORKERS but worker_needs_input = True"
                assert not worker_needs_input , msg
            except AssertionError:
                logger.exception("[Error]: assertion failed")
                if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
                    logging.shutdown()
                    os._exit(0)
            #assertOld:
            #if worker_needs_input:
            #    logger.error("[Error]:" + thread_name + ": process_faninNBs_batch: not USING_WORKERS but worker_needs_input = True")

            list_of_work_tuples = dummy_DAG_exec_state.return_value
            # Note: On tcp_server, a work tupe is : work_tuple = (start_state_fanin_task,returned_state)
            # where returned_state is the DAG_executor_State that gets returned when you call fan_in. 
            # In this state, the return_value is the dictionary of results from the fan_in

            for work_tuple in list_of_work_tuples:
                start_state_fanin_task = work_tuple[0]
                fanin_task_name = DAG_info.get_DAG_map()[start_state_fanin_task].task_name
                logger.trace(thread_name + ": process_faninNBs_batch: " + calling_task_name + " received work for fanin task " + fanin_task_name 
                    + " and start_state_fanin_task ")
                work_tuple_state = work_tuple[1]

                if work_tuple_state.return_value == 0:
                    # we were not the become task of faninNB so there is nothing to do
                    continue

                # else we were the become task and we need to start a thread to do the fanin task
                # we do not use the dict_of_results since all of the results were previously added
                # to the global data dictionary by the threads that obtained these results when 
                # they executed the corresponding tasks.
                # Not using.
                dict_of_results = work_tuple_state.return_value

                try:
                    logger.trace(thread_name + ": process_faninNBs_batch: " + calling_task_name + ": starting DAG_executor thread for task " + name + " with start state " + str(start_state_fanin_task))
                    #server = kwargs['server']
                    #DAG_executor_state =  kwargs['DAG_executor_State']
                    #DAG_executor_state.state = int(start_state_fanin_task)
                    #dict_of_results = dummy_DAG_exec_state.return_value[1]
                    new_DAG_executor_state = DAG_executor_State(function_name = "DAG_executor", function_instance_ID = str(uuid.uuid4()), state = start_state_fanin_task)
                    new_DAG_executor_state.restart = False      # starting  new DAG_executor in state start_state_fanin_task
                    #DAG_executor_state.return_value = None
                    new_DAG_executor_state.blocking = False
                    # Note: the results in work tuple are not needed since all of those results were previously
                    # put in the global data dictionary as task outputs by the workers who executed those tasks.
                    # We only add faninNB results to the local dictionarys of worker processes, as we do for 
                    # the paylad inputs of lambdas, since worker processes and lambdas do not share a global
                    # data dictionary. (Although wrker processes on the same machine could use Python shared
                    # memory dictionaries.)
                    payload = {
                        #"state": int(start_state_fanin_task),
                        #"input": DAG_executor_state.return_value,
                        "DAG_executor_state": new_DAG_executor_state,
                        #"DAG_info": DAG_info,
                        #"server": server
                    }
                    thread_name_prefix = "Thread_faninNB_"
                    thread = threading.Thread(target=DAG_executor_task, name=(thread_name_prefix+str(start_state_fanin_task)), args=(payload,))
                    thread.start()
                    #_thread.start_new_thread(DAG_executor.DAG_executor_task, (payload,))
                except Exception:
                    logger.exception("[ERROR]: " + thread_name + ": process_faninNBs_batch: Failed to start DAG_executor thread.")
                    if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
                        logging.shutdown()
                        os._exit(0) 

            # USING_WORKERS is false so worker_needs_input should never be true
            #else:
            #    worker_needs_input = False
            #   DAG_exec_state.state = start_state_fanin_task
            #   logger.trace("process_faninNBs: set worker_needs_input to False,")

        #return 1
        logger.trace(thread_name + ": process_faninNBs_batch:  returning worker_needs_input: " + str(worker_needs_input))
        return worker_needs_input
    logger.trace(thread_name + ": process_faninNBs_batch:  returning worker_needs_input: " + str(worker_needs_input))
    return worker_needs_input

#Todo: Global fanin object in tcp_Server, which determines whether last caller or not, and delegates
#      collection of results to local fanins in infinistore executors.

#def process_fanouts(fanouts, calling_task_name, DAG_states, DAG_exec_State, output, DAG_info, server):
def  process_fanouts(fanouts, calling_task_name, DAG_states, DAG_exec_State, 
    output, DAG_info, server, work_queue, list_of_work_queue_or_payload_fanout_values,
    groups_partitions,
#brc: cluster
    fanout_partition_group_sizes, clustered_tasks
    ):
    # Not using server, which "simuates" tcp_server when we are storing 
    # sync objects locally - server only handles fanins and faninNBs.
    # For real lambdas, to avoid reading the partitions/groups from an s3
    # bucket or whatever, the DAG_executor_driver can input the partitions
    # or groups and pass them the the real lambdas it starts as part of the 
    # payload. This list group_partitions will be passed to process_fanouts
    # and added to the payloags of the real lambdas started for fanouts.
    # tcp_server can also read group_partitions and make
    

    thread_name = threading.current_thread().name

    logger.info(thread_name + ": process_fanouts: length fanouts is " + str(len(fanouts)))

    #process become task
    become_task = fanouts[0]
    logger.info(thread_name + ": process_fanouts: fanout for " + calling_task_name + " become_task is " + become_task)
    # Note:  We will keep using DAG_exec_State for the become task. If we are running everything on a single machine, i.e.,
    # no server or Lambdas, then faninNBs should use a new DAG_exec_State. Fanins and fanouts/faninNBs are mutually exclusive
    # so, e.g., the become fanout and a fanin cannot use the same DAG_exec_Stat. Same for a faninNb and a fanin, at least
    # as long as we do not generate faninNBs and a fanin for the same state/task. We could optimize so that we can have
    # a fanin (with a become task and one or more faninNBs.
    become_start_state = DAG_states[become_task]

    logger.info (thread_name + ": process_fanouts: fanout for " + calling_task_name + " become_task state is " + str(become_start_state))
    fanouts.remove(become_task)
    # if runtime clustering then slso remove the size that 
    # corresponds to fanout[0].
    if DAG_executor_constants.ENABLE_RUNTIME_TASK_CLUSTERING:
        fanout_partition_group_sizes.pop(0)
 
    logger.info(thread_name + ": process_fanouts: new fanouts after remove become task: " + str(fanouts))
    if DAG_executor_constants.ENABLE_RUNTIME_TASK_CLUSTERING:
        logger.info(thread_name + ": process_fanouts: new fanout_partition_group_sizes after remove become task: " + str(fanout_partition_group_sizes))

#brc: cluster: 
    def cluster_condition(fanout_partition_group_size,size_of_output_to_fanout_task):
        # Note that fanout_partition_group_size includes any shadow_nodes in the 
        # group or partition. The size not including shadow nodes is 
        # fanout_partition_group_size - size_of_output_to_fanout_task, since the 
        # number of shadow nodes equals size_of_output_to_fanout_task. 
        # Recall: if node N needs the parent pagerank value for node P,
        # then the "executor" of P's pagerank value "gives" (sends/writes)
        # this value to the "executor" that computes N's pagerank. This 
        # value is stord in the shadow_node for P that is in the partition/
        # group of N. So there is a shadow node for each parent value that
        # is needed to compute the pagerank of a given node. So the 
        # size_of_output_to_fanout_task is the number of parent values that 
        # are being given to the fanout task which equals the number of 
        # shadow nodes.
        # Note also that we do compute the pagerank for shadow nodes; this is 
        # not necessary since the pagerank value of a shadow node is the 
        # pagerank value of a parent node P and there is no need to recompute
        # the pagerank value of P when computing the pagerank value of a 
        # child of P. But recomputing the pagerank of shadow nodes allows
        # us to treat shadow and non-shadow nodes the same, i.e., no 
        # if-statements. Thus, we can store pageranks consecutively 
        # in a a shared array and write a cache-friendly loop with no 
        # if-statements to compute the pageranks. (See BFS_Shared.py and the
        # update_PageRank_of_PageRank_Function_Shared_Fast method.)
        ret = fanout_partition_group_size < DAG_executor_constants.MIN_PARTITION_GROUP_SIZE_FOR_CLUSTERING \
            or size_of_output_to_fanout_task > DAG_executor_constants.MAX_SIZE_OF_OUTPUT_TO_FANOUT_TASK
        logger.info(thread_name + ": process_fanouts: cluster_condition: return " + str(ret))
        return ret

    def do_task_clustering(fanouts,fanout_partition_group_sizes,clustered_tasks,
        calling_task_name, output):                 
        # assert len(fanouts) == len(fanout_partition_group_sizes)  
        new_fanouts = []
        new_fanout_partition_group_sizes = []
        for fanout_task_name, fanout_partition_group_size in zip(fanouts,fanout_partition_group_sizes):
            qualfied_name = str(calling_task_name) + "-" + str(fanout_task_name)
            dict_of_results = {}
            dict_of_results[qualfied_name] = output[fanout_task_name]
            logger.info(thread_name + ": process_fanouts: fanout_task_name: " + fanout_task_name
                + " output[fanout_task_name]: " 
                + str(output[fanout_task_name]))
            size_of_output_to_fanout_task = len(dict_of_results[qualfied_name])
            logger.info(thread_name + ": process_fanouts: calling cluster_condition for qualified fanout task name " + qualfied_name
                + ": fanout_partition_group_size : " + str(fanout_partition_group_size)
                + ", size_of_output_to_fanout_task : " + str(size_of_output_to_fanout_task))
            if cluster_condition(fanout_partition_group_size,size_of_output_to_fanout_task):
                clustered_task_start_state = DAG_states[fanout_task_name]
                clustered_tasks.append(clustered_task_start_state)
                logger.info(thread_name + ": process_fanouts: add to clustered_tasks task state : "
                    + str(clustered_task_start_state))
            else:
                new_fanouts.append(fanout_task_name)
                new_fanout_partition_group_sizes.append(fanout_partition_group_size)
                logger.info(thread_name + ": process_fanouts: do not cluster task: "
                    + str(fanout_task_name))
        fanouts.clear()
        fanouts = fanouts + new_fanouts
        fanout_partition_group_sizes.clear()
        fanout_partition_group_sizes = fanout_partition_group_sizes + new_fanout_partition_group_sizes
        logger.info(thread_name + ": process_fanouts: end of do_task_clustering:"
            + " fanouts: " + str(fanouts)
            + " fanout_partition_group_sizes: " + str(fanout_partition_group_sizes)
            + " clustered_tasks: " + str(clustered_tasks))

    if DAG_executor_constants.ENABLE_RUNTIME_TASK_CLUSTERING:
        #   assert: len(fanouts) > 0  - if become was only task then it should have been collapsed
        do_task_clustering(fanouts,fanout_partition_group_sizes,clustered_tasks,calling_task_name, output)
        #   Note: fanouts may be empty. If so, list_of_work_queue_or_payload_fanout_values is empty too
        #     and clustered_tasks is not empty and contains at least one task (based on assertion)
        #   Note: if fanouts is not empty, clustered_tasks may or may not be empty. 
        #     We can return become task and clustered_tasks can be a parameter. Upon return,
        #     we will put the become task and possbly the clustered tasks in the cluster queue.

    # process rest of fanouts
    logger.trace(thread_name + ": process_fanouts: RUN_ALL_TASKS_LOCALLY:" + str(DAG_executor_constants.RUN_ALL_TASKS_LOCALLY))

    for name in fanouts:
        if DAG_executor_constants.USING_WORKERS:
            # using worker processes: put fanout task in 
            # list_of_work_queue_or_payload_fanout_values as a work_tuple
            if not DAG_executor_constants.USING_THREADS_NOT_PROCESSES: # using processes
                dict_of_results =  {}
                if DAG_executor_constants.SAME_OUTPUT_FOR_ALL_FANOUT_FANIN:
                    # each fanout of a task gets the same output, i.e., 
                    # the output of the task.
                    dict_of_results[calling_task_name] = output
                else:
                    # Each fanout of a task gets its own output; use qualified names
                    # e.g., "PR1_1-PR2_3" as the calling task instead of 
                    # just "PR1_1". Assuming the output is a dictionary
                    # where keys are fanout/faninNB names and the valus are
                    # the outputs for that fanout/faninNB.
                    # Note: For DAG generation, for each state we execute a task and 
                    # for each task T we have t say what T;s task_inputs are - these are the 
                    # names of tasks that give inputs to T. When we have per-fanout output
                    # instead of having the same output for all fanouts, we specify the 
                    # task_inputs as "sending task - receiving task". So a sending task
                    # S might send outputs to fanouts A and B so we use "S-A" and "S-B"
                    # as the task_inputs, instad of just using "S", which is the Dask way.
                    
                    #if name.endswith('L'):  
                    #    dict_of_results[qualfied_name] = output[name[:-1]]
                    #    qualfied_name = str(calling_task_name) + "-" + str(name[:-1])
                    #else:
                    qualfied_name = str(calling_task_name) + "-" + str(name)
                    # Q: Can we pull output[name] value from data_dict since
                    # we put output in data_dict?
                    dict_of_results[qualfied_name] = output[name]

                    #dict_of_results[qualfied_name] = output[name]
                logger.trace(thread_name + ": process_fanouts: dict_of_results for fanout " + name)
                logger.trace(str(dict_of_results))
                #dict_of_results[calling_task_name] = output
                work_tuple = (DAG_states[name],dict_of_results)
                #work_queue.put(DAG_states[name])

                # We will be batch processing the faninNBs so we will also batch process
                # the fanouts at the same time. If there are any faninBs in this state
                # piggyback the fanouts on the call to process faninNBs. If there are 
                # no faninNBs we will send this list to the work_queue directly. Note:
                # we could check at the end of this method whether there are any
                # faninNBs and if, not, call work_queue.put.

                list_of_work_queue_or_payload_fanout_values.append(work_tuple)
            else: 
                # using threads and using workers. Even if the FanInNBs are stored remotely,
                # we may still be using threads. When we use threads, the work_queue is stored
                # locally and we add work here. Note: we do not batch process remote FanInNBs when 
                # we are using threads. When we are using processes, the work queue is stored
                # remotely with the FanInNBs, and we piggyback the work generated by fanouts 
                # on the process FanInNBs batch calls to reduce round trips to the tcp_server.
                # We could also batch process FanInNBs when we use threads, but we would have
                # to return all the work to the calling thread so it could put the work in the 
                # local work queue. Threads are not as useful as processes so we are not batch
                # processng FanInNBs for threads just yet, as we will likely be using processes
                # to speed up DAG processing. (For threads if piggyback: return a list of work 
                # to add to the local work queue.)
                # Note: For DAG generation, for each state we execute a task and 
                # for each task T we have t say what T;s task_inputs are - these are the 
                # names of tasks that give inputs to T. When we have per-fanout output
                # instead of having the same output for all fanouts, we specify the 
                # task_inputs as "sending task - receiving task". So a sending task
                # S might send outputs to fanouts A and B so we use "S-A" and "S-B"
                # as the task_inputs, instad of just using "S", which is the Dask way.
                dict_of_results =  {}
                if DAG_executor_constants.SAME_OUTPUT_FOR_ALL_FANOUT_FANIN:
                    # each fanout of a task gets the same output, i.e., 
                    # the output of the task.
                    dict_of_results[calling_task_name] = output
                else:
                    # Each fanout of a task gets its own output; use qualified names
                    # e.g., "PR1_1-PR2_3" as the calling task instead of 
                    # just "PR1_1". Assuming the output is a dictionary
                    # where keys are fanout/faninNB names and the valus are
                    # the outputs for that fanout/faninNB,
                   
                    #if name.endswith('L'):  
                    #    dict_of_results[qualfied_name] = output[name[:-1]]
                    #    qualfied_name = str(calling_task_name) + "-" + str(name[:-1])
                    #else:
                    qualfied_name = str(calling_task_name) + "-" + str(name)
                    dict_of_results[qualfied_name] = output[name]

                    #dict_of_results[qualfied_name] = output[name]
                logger.info(thread_name + ": process_fanouts: dict_of_results for fanout " + name)
                logger.trace(str(dict_of_results))
                work_tuple = (DAG_states[name],dict_of_results)
                #work_queue.put(DAG_states[name])
                work_queue.put(work_tuple)
        else:
#brc: run tasks
            if (not DAG_executor_constants.RUN_ALL_TASKS_LOCALLY) and DAG_executor_constants.STORE_SYNC_OBJECTS_IN_LAMBDAS and DAG_executor_constants.SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS:
            #if (RUN_ALL_TASKS_LOCALLY and not USING_WORKERS and not STORE_FANINS_FANINNBS_LOCALLY and STORE_SYNC_OBJECTS_IN_LAMBDAS and SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS):
                # Nothing to do. When we are SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS we will
                # pass the fanouts (set) to process_faninNBs_batch and it will process the 
                # fanouts by invoking real python functions that execute the fanout tasks.
                pass
            elif DAG_executor_constants.RUN_ALL_TASKS_LOCALLY:
            #elif (RUN_ALL_TASKS_LOCALLY and not USING_WORKERS and not STORE_FANINS_FANINNBS_LOCALLY and not (STORE_SYNC_OBJECTS_IN_LAMBDAS and SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS)):
                # (RUN_ALL_TASKS_LOCALLY and not USING_WORKERS and not STORE_FANINS_FANINNBS_LOCALLY and USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS):
                # or (RUN_ALL_TASKS_LOCALLY and not USING_WORKERS and not STORE_FANINS_FANINNBS_LOCALLY and USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS and SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS):
                # Note: if we are using threads to simulate lambas it does not matter whether or not we are storing 
                # objects in python functions, we will invoke a new thread to execute the fanout task. But if
                # we are running tasks in python functions that simulate lambdas, we will call process_faninNBS_batch
                # to deal with the fanouts. (If we are using real lambdas, we will eventually do this same thing.)
                try:
                    logger.trace(thread_name + ": process_fanouts: Starting fanout DAG_executor thread for " + name)
                    fanout_task_start_state = DAG_states[name]
                    # brc: DES
                    task_DAG_executor_State = DAG_executor_State(function_name = "DAG_executor:"+name, function_instance_ID = str(uuid.uuid4()), state = fanout_task_start_state)

                    # Note: if not SAME_OUTPUT_FOR_ALL_FANOUT_FANIN then
                    # we would need to extract the fanout's particular
                    # output from output and pass it in payload. See the 
                    # code for this above where we are using workers.
                    # as in:

                    dict_of_results =  {}
                    if DAG_executor_constants.SAME_OUTPUT_FOR_ALL_FANOUT_FANIN:
                        # each fanout of a task gets the same output, i.e., 
                        # the output of the task.
                        dict_of_results[calling_task_name] = output
                    else:
                        # Each fanout of a task gets its own output; use qualified names
                        # e.g., "PR1_1-PR2_3" as the calling task instead of 
                        # just "PR1_1". Assuming the output is a dictionary
                        # where keys are fanout/faninNB names and the valus are
                        # the outputs for that fanout/faninNB,
                        #if name.endswith('L'): 
                        #    qualfied_name = str(calling_task_name) + "-" + str(name[:-1]) 
                        #    dict_of_results[qualfied_name] = output[name[:-1]]
                            
                        #else:
                        qualfied_name = str(calling_task_name) + "-" + str(name)
                        dict_of_results[qualfied_name] = output[name]

                        #dict_of_results[qualfied_name] = output[name]
                    logger.trace(thread_name + ": process_fanouts: dict_of_results for fanout " + name)
                    logger.trace(str(dict_of_results))
                    # Below we would use: "input": dict_of_results,

                    #output_tuple = (calling_task_name,)
                    #output_dict[calling_task_name] = output
                    logger.trace (thread_name + ": process_fanouts: fanout payload for " + name + " is " + str(fanout_task_start_state) + "," + str(output))
                    payload = {
                        # If not using workers but running tasks locally then we are using threads
                        # to simulate Lambdas but threads currently use a global data_dict so they
                        # just put task outputs in the global data_dict. Thus, we have no need to 
                        # pass the outputs in the payload. However, We pass them here jut to check
                        # the logic used by the real Lambdas.
                        # The driver just passes the dag executor state. We do not need to pass 
                        # server, since it is available globally to all the threads. We input DAG_info 
                        # from a file but we pass it here like the real lambas do. 
                        #
                        # See the note above about dict_of_results.
                        #
                        "input": dict_of_results,
                        "DAG_executor_state": task_DAG_executor_State,
                        "DAG_info": DAG_info
                        #"server": server # used to mock server during testing
                    }

                    _thread.start_new_thread(DAG_executor_task, (payload,))
                except Exception:
                    logger.exception("[ERROR] " + thread_name + ": process_fanouts: Failed to start DAG_executor thread for " 
                        + name)
                    if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
                        logging.shutdown()
                        os._exit(0)
            elif not DAG_executor_constants.RUN_ALL_TASKS_LOCALLY:
                try:
                    logger.trace(thread_name + ": process_fanouts: Starting fanout DAG_executor Lambda for " + name)
                    fanout_task_start_state = DAG_states[name]
                    # create a new DAG_executor_State object so no DAG_executor_State object is shared by fanout/faninNB threads in a local test.
                    lambda_DAG_executor_state = DAG_executor_State(function_name = "WukongDivideAndConquer:"+name, function_instance_ID = str(uuid.uuid4()), state = fanout_task_start_state)
                    logger.trace (thread_name + ": process_fanouts: payload is DAG_info + " + str(fanout_task_start_state) + ", " + str(output))
                    lambda_DAG_executor_state.restart = False      # starting new DAG_executor in state start_state_fanin_task
                    lambda_DAG_executor_state.return_value = None
                    lambda_DAG_executor_state.blocking = False
                    logger.trace(thread_name + ": process_fanouts: Starting Lambda function %s." % lambda_DAG_executor_state.function_name)
                    #logger.trace("lambda_DAG_executor_State: " + str(lambda_DAG_executor_State))

                    dict_of_results =  {}
                    if DAG_executor_constants.SAME_OUTPUT_FOR_ALL_FANOUT_FANIN:
                        # each fanout of a task gets the same output, i.e., 
                        # the output of the task.
                        dict_of_results[calling_task_name] = output
                    else:
                        # Each fanout of a task gets its own output; use qualified names
                        # e.g., "PR1_1-PR2_3" as the calling task instead of 
                        # just "PR1_1". Assuming the output is a dictionary
                        # where keys are fanout/faninNB names and the valus are
                        # the outputs for that fanout/faninNB.
                        # Note: For DAG generation, for each state we execute a task and 
                        # for each task T we have t say what T;s task_inputs are - these are the 
                        # names of tasks that give inputs to T. When we have per-fanout output
                        # instead of having the same output for all fanouts, we specify the 
                        # task_inputs as "sending task - receiving task". So a sending task
                        # S might send outputs to fanouts A and B so we use "S-A" and "S-B"
                        # as the task_inputs, instad of just using "S", which is the Dask way.
                        
                        #if name.endswith('L'):  
                        #    dict_of_results[qualfied_name] = output[name[:-1]]
                        #    qualfied_name = str(calling_task_name) + "-" + str(name[:-1])
                        #else:
                        qualfied_name = str(calling_task_name) + "-" + str(name)
                        dict_of_results[qualfied_name] = output[name]

                        #dict_of_results[qualfied_name] = output[name]
                    logger.trace(thread_name + ": process_fanouts: dict_of_results for fanout " + name)
                    logger.trace(str(dict_of_results))

                    #results = {}
                    #results[calling_task_name] = output

                    payload = {
#ToDo: Lambda:          # use parallel invoker with list piggybacked on batch fanonNBs, as usual
                        #"state": int(fanout_task_start_state),
                        #"input": output,
                        #"input": results,
                        "input": dict_of_results,
                        "DAG_executor_state": lambda_DAG_executor_state,
                        "DAG_info": DAG_info,
                        #"server": server   # used to mock server during testing
                    }

                    if DAG_executor_constants.INPUT_ALL_GROUPS_PARTITIONS_AT_START:
                        payload["groups_partitions"] = groups_partitions
                    
                    ###### DAG_executor_State.function_name has not changed
                    invoke_lambda_DAG_executor(payload = payload, function_name = "WukongDivideAndConquer:"+name)
                except Exception:
                    logger.exception("[ERROR]: " + thread_name + " process_fanouts: Failed to start DAG_executor Lambda.")
                    if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
                        logging.shutdown()
                        os._exit(0) 
#brc: run task
            else:
                try:
                    msg = "[ERROR]: " + thread_name + " : process_fanouts: invalid configuration."
                    assert False , msg
                except AssertionError:
                    logger.exception("[Error]: assertion failed")
                    if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
                        logging.shutdown()
                        os._exit(0)
                #assertOld:
                #logger.error("[ERROR]: " + thread_name + " : process_fanouts: invalid configuration.")

    # Note: If we do not piggyback the fanouts with process_faninNBs_batch, we would add
    # all the work to the remote work queue here.
    #if USING_WORKERS and not USING_THREADS_NOT_PROCESSES:
    #   work_queue.put_all(list_of_work_queue_or_payload_fanout_values)

    logger.trace(thread_name + ": process_fanouts: return become_start_state: " + str(become_start_state))
    return become_start_state

def create_and_fanin_remotely(websocket,DAG_exec_state,**keyword_arguments):
    DAG_exec_state = fanin_remotely(websocket, DAG_exec_state, **keyword_arguments)
    return DAG_exec_state

def fanin_remotely(websocket, DAG_exec_state,**keyword_arguments):
    # create new faninNB with specified name if it hasn't been created
    #fanin_task_name = keyword_arguments['fanin_task_name']
    #n = keyword_arguments['n']
    #start_state_fanin_task = keyword_arguments['start_state_fanin_task']
    #output = keyword_arguments['result']
    #calling_task_name = keyword_arguments['calling_task_name']
    #DAG_executor_state = keyword_arguments['DAG_executor_State']
    #server = keyword_arguments['server']
    #STORE_FANINS_FANINNBS_LOCALLY = keyword_arguments['store_fanins_faninNBs_locally']  # option set in DAG_executor
    #DAG_info = keyword_arguments['DAG_info']

    thread_name = threading.current_thread().name

    logger.trace (thread_name + ": fanin_remotely: calling_task_name: " + keyword_arguments['calling_task_name'] + " calling synchronize_sync fanin with fanin_task_name: " + keyword_arguments['fanin_task_name'])

    #FanInNB = server.synchronizers[fanin_task_name]

    # Note: in real code, we would return here so caller can quit, letting server do the op.
    # Here, we can just wait for op to finish, then return. Caller has nothing to do but
    # quit since nothing to do after a fanin.

    # return is: None, restart, where restart is always 0 and return_value is None; and makes no change to DAG_executor_State	
    #return_value, restart = FanInNB.fan_in(**keyword_arguments)
    DAG_exec_state.return_value = None
    DAG_exec_state.blocking = False

    """
    # for TR 1024 w/ 1022 fanins end-to-end fanin processing approx:
    #total_time:1.3573, num_fanins_timed:1022
    #average_time:0.00132
    
    st = time.time()
    """

    DAG_exec_state = synchronize_sync(websocket, "synchronize_sync", keyword_arguments['fanin_task_name'], "fan_in", DAG_exec_state)
    
    """
    et = time.time()
    global total_time
    global num_fanins_timed
    total_time += (et - st)
    num_fanins_timed += 1
    logger.error (thread_name + ": fanin_remotely: total_time:" + str(total_time) + " num_fanins_timed:" + str(num_fanins_timed))
    logger.error (thread_name + ": fanin_remotely: average_time:" + str(total_time/num_fanins_timed) )
    """

    logger.trace (thread_name + ": fanin_remotely: calling_task_name: " + keyword_arguments['calling_task_name'] + " back from synchronize_sync")
    logger.trace (thread_name+ ": fanin_remotely: returned DAG_exec_state.return_value: " + str(DAG_exec_state.return_value))
    return DAG_exec_state

def process_fanins(websocket,fanins, faninNB_sizes, calling_task_name, DAG_states, DAG_exec_state, output, server):
    thread_name = threading.current_thread().name
    logger.trace(thread_name + ": process_fanins: calling_task_name: " + calling_task_name)

    # Suggsted assert len(fanins) == len(faninNB_sizes) ==  1

    # call synch-op try-op on the fanin with name f, passing output and start_state.
    # The return value will have [state,input] tuple? Or we know state when we call
    # the fanin, we just don't know whether we become task. If not, we return. If
    # become, then we set state = states[f] before calling fanin.
    # Note: Only one fanin object so only one state for become. We pass our State and get it back
    # with the fanin results for the other tasks that fanin. We will add our results in create_and_fanin
    # using return_value[calling_task_name] = output[calling_task_name], which is add our results from output
    # to those returned by the fanin.

    # These keyword_arguments are passed to the fan_in operation when we 
    # use local fanin objects
    keyword_arguments = {}
    logger.trace(thread_name + ": process_fanins: fanins" + str(fanins))
    # there is only one fanin name
    keyword_arguments['fanin_task_name'] = fanins[0]
    keyword_arguments['n'] = faninNB_sizes[0]
    #keyword_arguments['start_state_fanin_task'] = DAG_states[fanins[0]]
    #keyword_arguments['result'] = output
    #keyword_arguments['calling_task_name'] = calling_task_name
    if DAG_executor_constants.SAME_OUTPUT_FOR_ALL_FANOUT_FANIN:
        keyword_arguments['result'] = output
        keyword_arguments['calling_task_name'] = calling_task_name
    else:
        name = fanins[0]
        logger.trace("**********************" + thread_name + ": process_fanins:  for " + calling_task_name + " faninNB "  + name + " output is :" + str(output))

        #if name.endswith('L'):  
        #    keyword_arguments['result'] = output[name[:-1]]
        #    qualified_name = str(calling_task_name) + "-" + str(name[:-1])
        #else:
        keyword_arguments['result'] = output[name]
        qualified_name = str(calling_task_name) + "-" + str(name)

        logger.trace(thread_name + ": process_fanins: name:" + str(name) 
            + " qualified_name: " + qualified_name)
        keyword_arguments['calling_task_name'] = qualified_name
    # Don't do/need this.
    #keyword_arguments['DAG_executor_State'] = DAG_exec_state
    keyword_arguments['server'] = server

    if not DAG_executor_constants.STORE_FANINS_FANINNBS_LOCALLY:
        # These DAG_exec_state keyword_arguments are passed to the fan_in 
        # operation when we use remote fanin objects
        DAG_exec_state.keyword_arguments = {}
        DAG_exec_state.keyword_arguments['fanin_task_name'] = fanins[0]
        DAG_exec_state.keyword_arguments['n'] = faninNB_sizes[0]
        #DAG_exec_state.keyword_arguments['result'] = output
        #DAG_exec_state.keyword_arguments['calling_task_name'] = calling_task_name
        if DAG_executor_constants.SAME_OUTPUT_FOR_ALL_FANOUT_FANIN:
            DAG_exec_state.keyword_arguments['result'] = output
            DAG_exec_state.keyword_arguments['calling_task_name'] = calling_task_name
        else:
            name = fanins[0]
            logger.trace("**********************" + thread_name + ": process_fanins:  for " + calling_task_name + " faninNB "  + name + " output is :" + str(output))

            #if name.endswith('L'):  
            #    keyword_arguments['result'] = output[name[:-1]]
            #    qualified_name = str(calling_task_name) + "-" + str(name[:-1])
            #else:
            DAG_exec_state.keyword_arguments['result'] = output[name]
            qualified_name = str(calling_task_name) + "-" + str(name)

            logger.trace(thread_name + ": process_fanins: name:" + str(name) 
                + " qualified_name: " + qualified_name)
            DAG_exec_state.keyword_arguments['calling_task_name'] = qualified_name
        #DAG_exec_state.keyword_arguments['DAG_executor_State'] = DAG_exec_state
        DAG_exec_state.keyword_arguments['server'] = server
#brc: batch
        # Note: using this remotley on tcp_server in synhronize_sync
        # but not using it locally. When create fanin/faninNB locally
        # we call a create fanin/faninNB method which knows to create
        # a fanin/faninNB
        DAG_exec_state.keyword_arguments['FANIN_TYPE'] = "fanin"

    if DAG_executor_constants.STORE_FANINS_FANINNBS_LOCALLY:
        #ToDo:
        #keyword_arguments['DAG_executor_State'] = DAG_exec_state
        # The keyword arguments aer passed to the local fanin 
        # no the DAG_executor_state
        if not DAG_executor_constants.CREATE_ALL_FANINS_FANINNBS_ON_START:
            DAG_exec_state = server.create_and_fanin_locally(DAG_exec_state,keyword_arguments)
        else:
            logger.trace(thread_name + ": process_fanins: " + calling_task_name + ": call server.fanin_locally")
            DAG_exec_state = server.fanin_locally(DAG_exec_state,keyword_arguments)
    else:
#brc: ToDo: when not CREATE_ALL_FANINS_FANINNBS_ON_START create the object
# on the server, like we do for the enqueue case. If storing objects in 
# lambda we have to be in the lmabda when we creae the object, i.e.,
# in message_handler_lambda, so when create objects on server put the
# create in msg_handler's synchronize sync to be consistent (rather
# than in tcp_server?)

        if not DAG_executor_constants.CREATE_ALL_FANINS_FANINNBS_ON_START:
            # Note: might want to send the result for debugging
            #if not using_lambdas:
                # if we call a remote fanin and locally we are not using lambdas,
                # then we need not pass the result of this task since we will not
                # be passing the fanin task inputs back - each task's resulte will 
                # have been put in the data_dict an the fanin task will get those
                # results, which are its inputs, from the data_dict. This makes the 
                # cost of the send for the fanin operaton less costly.
                # Actually:
                # We will use local datadict for each multiprocess; process will receve
                # the faninNB results and put them in the data_dict
                # DAG_exec_state.keyword_arguments['result'] = None

            # create_and_fanin_remotely simply calls fanin_remotely(websocket,**keyword_arguments)
            # All the create logic is on the server, so we call fanin_remotely in
            # either case. We leave this call to create_and_fanin_remotely 
            # as a placeholder in case we decide to do smething differet - 
            # as in create_and_fanin_remotely could create and pass a new message to 
            # the server, i.e., move the create logic down from the server.
            DAG_exec_state = create_and_fanin_remotely(websocket,DAG_exec_state, **keyword_arguments)
            logger.trace (thread_name + ": process_fanins: process_fanins: call to create_and_fanin_remotely returned DAG_exec_state.return_value: " + str(DAG_exec_state.return_value))
        else:
            #if not using_lambdas:
                # if we call a remote fanin and locally we are not using lambdas,
                # then we need not pass the result of this task since we will not
                # be passing the fanin task inputs back - each task's results will 
                # have been put in the data_dict and the fanin task will get those
                # results, which are its inputs, from the data_dict. This makes the 
                # cost of the send for the fanin operaton less costly.

                # Note: A worker process/lambda that is the last caller of fan-in will be the 
                # become task and the results from the non-becme workers will be passed
                # back from the fan-in. The worker/lambda will add the results to its local
                # data dictionary. So we must pass our result to the fanin so it can possbly
                # be saved on some other worker process/lambda's data dict. 
                
                #DAG_exec_state.keyword_arguments['result'] = None

            DAG_exec_state = fanin_remotely(websocket, DAG_exec_state, **keyword_arguments)
            logger.trace (thread_name + ": process_fanins: process_fanins: call to fanin_remotely returned DAG_exec_state.return_value: " + str(DAG_exec_state.return_value))

    return DAG_exec_state
	
# Driver will not have any payload args unless it will invoke the leaf nodes with their inputs
# Example: payload = {"list of functions:" ..., 'starting_input': 0}
# Example: invoke_lambda(payload = payload, is_first_invocation = True, n = 1, initial_permits = 0, function_name = "ComposerServerlessSync")
# where invoker is: if is_first_invocation:
"""
        state = State(
            function_name = "Composer",  # this is name of Lambda function
            function_instance_ID = str(uuid.uuid4()),
            restart = False,
            pc = 0,
            return_value = None,
            blocking = False,
			# Note: could use a "starting_input" from the driver, instead of state.return_value, used for results from fanin/faninNB
			state = 0
            keyword_arguments = {
                'n': n,
                'initial_permits': initial_permits
            }
        )
        _payload["state"] = base64.b64encode(cloudpickle.dumps(state)).decode('utf-8')
		...
		payload_json = json.dumps(_payload)
		...
   		status_code = lambda_client.invoke( FunctionName = function_name, InvocationType = 'Event', Payload = payload_json) 
"""
# where handler does:
#	state = cloudpickle.loads(base64.b64decode(event["state"]))
"""
    if target == "Composer":
        composer = Composer(state = state)
        composer.execute()

	Note: We can call DAG_execute(state)
"""

#def DAG_executor_work_loop(logger, server, counter, work_queue, DAG_executor_state, DAG_info, data_dict):

#brc: counter
# tasks_completed_counter, workers_completed_counter
def DAG_executor_work_loop(logger, server, completed_tasks_counter, completed_workers_counter, DAG_executor_state, DAG_info, 
#brc: continue
    work_queue,DAG_infobuffer_monitor,
    groups_partitions):

#brc: groups partitions
    # For real lambdas, to avoid reading the partitions/groups from an s3
    # bucket or whatever, the DAG_executor_driver can input the partitions
    # or groups and pass them the the real lambdas it starts as part of the 
    # payload. This list group_partitions will be passed to process_fanouts
    # and added to the payloags of the real lambdas started for fanouts.
    # tcp_server can also read group_partitions and make.
    # IF we are not using real lambdas this list is empty.
    DAG_map = DAG_info.get_DAG_map()
    DAG_tasks = DAG_info.get_DAG_tasks()
    DAG_number_of_tasks = DAG_info.get_DAG_number_of_tasks()

#brc: continue
    # This is a local variable; each worker has their own 
    # num_tasks_to_execute, which could be different since 
    # workers may be using different versions of the incremental DAG.
    # version i includes all the tasks of earlier version i-1, i-2, etc.
    # A later version has 1 or more complete tasks that were incomplete
    # in an earlier version. This is set next.
    # Lambdas do not use this (see the note below where we set this.)
    num_tasks_to_execute = -1
#brc: lambda inc:
    continued_task = False
    if (not DAG_executor_constants.USING_WORKERS):
        # not actually using this value when using real or simulated lambdas.
        # this values tells workers when there are no more tasks to execute
        #num_tasks_to_execute = len(DAG_tasks)
        # Note: This is a Lambda, it does not try to excute all of the tasks
        # in the DAG, it ensures that the tasks on the path that starts with 
        # the current state/task are executed, either by it or some other
        # lambda for a fanout task or a fanin/faninNB. That is it will execute
        # the become task of a fanout an start other lambds for the other fanout
        # tests, or it can become the excutor of a fanin (when it is the last 
        # executor to call fanin). It des not exexecure the fanin task for a 
        # faninNB as a faninNB starts a new lambda to execute the faninNBs fanin task.
        # Note: We ar not using num_tasks_to_execute and number_of_tasks_executed
        # the way workers use them, i.e,, to determine when using incremental DAG 
        # generation whether it is time to get a new incremental DAG. Lambdas
        # get a new incremental DAG when the get to "the end" of the current DAG
        # so they need a new DAG to continue along the path they are on (i.e.,
        # they executed a task and need a new DAG befoer they can process the 
        # fanouts/fanins/collapses of this task. thelmabda then will continue
        # by executing a become task of the fanouts or a fanin or it will stop.)
        # Note: not using this for Lambdas - num_tasks_to_execute for the lambda
        # is not known, the lambda executes tasks until it does not become 
        # any task (of a set of fanout tasks or a fnin task) then it stops.
        num_tasks_to_execute = DAG_number_of_tasks
#brc: lambda inc:
        continued_task = DAG_executor_state.continued_task
        logger.info("DAG_executor_work_loop: at start: lambda executes a continued task: " + str(continued_task)
            + " for state " + str(DAG_executor_state.state))
    else:
        if not (DAG_executor_constants.COMPUTE_PAGERANK and DAG_executor_constants.USE_INCREMENTAL_DAG_GENERATION):
            #num_tasks_to_execute = len(DAG_tasks)
            num_tasks_to_execute = DAG_number_of_tasks
        else: # using incremental DAG generation
            if not DAG_executor_constants.USE_PAGERANK_GROUPS_PARTITIONS:
                # using partitions
                if not DAG_info.get_DAG_info_is_complete():
                    #num_tasks_to_execute = len(DAG_tasks) - 1
                    number_of_incomplete_tasks = DAG_info.get_DAG_number_of_incomplete_tasks()
                    try:
                        msg = "[Error]: DAG_executor_work_loop at start:" \
                            + " Using incremental DAG generation with partitions and" \
                            + " DAG is incomplete but number_of_incomplete_tasks is not 1: "
                        assert number_of_incomplete_tasks == 1 , msg
                    except AssertionError:
                        logger.exception("[Error]: assertion failed")
                        if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
                            logging.shutdown()
                            os._exit(0)
                    #assertOld:
                    #if not number_of_incomplete_tasks == 1:
                    #    logger.error("[Error]: DAG_executor_work_loop at start:"
                    #        + " Using incremental DAG generation with partitions and"
                    #        + " DAG is incomplete but number_of_incomplete_tasks is not 1: "
                    #       + str(number_of_incomplete_tasks))
    
                    num_tasks_to_execute = DAG_number_of_tasks -1
                    logger.trace("DAG_executor_work_loop: at start: DAG_info complete num_tasks_to_execute: " + str(num_tasks_to_execute))
                else:
                    #num_tasks_to_execute = len(DAG_tasks)
                    num_tasks_to_execute = DAG_number_of_tasks
                    logger.trace("DAG_executor_work_loop: at start: DAG_info complete num_tasks_to_execute: " + str(num_tasks_to_execute))
            else:
                # using groups
                if not DAG_info.get_DAG_info_is_complete():
                    number_of_incomplete_tasks = DAG_info.get_DAG_number_of_incomplete_tasks()
                    num_tasks_to_execute = DAG_number_of_tasks - number_of_incomplete_tasks
                    logger.trace("DAG_executor_work_loop: at start: DAG_info not complete: new num_tasks_to_execute: " 
                        + str(num_tasks_to_execute) + " with number_of_incomplete_tasks "
                        + str(number_of_incomplete_tasks))                
                else:
                    #num_tasks_to_execute = len(DAG_tasks)
                    num_tasks_to_execute = DAG_number_of_tasks
                    logger.trace("DAG_executor_work_loop: at start: DAG_info complete num_tasks_to_execute: " + str(num_tasks_to_execute))

    logger.info("DAG_executor: length of DAG_tasks: " + str(DAG_number_of_tasks)
        + " number of tasks in DAG to execute: " + str(num_tasks_to_execute))
    #server = payload['server']
    proc_name = multiprocessing.current_process().name
    thread_name = threading.current_thread().name
    logger.info("DAG_executor_work_loop: proc " + proc_name + " " + " thread " + thread_name + ": started.")

    #ToDo:
    #if input is None:
        #pass  # withdraw input from payload.synchronizer_name
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as websocket:
        global STORE_FANINS_FANINNBS_LOCALLY
        # Config: A1, A4_remote, A5, A6
        if not DAG_executor_constants.STORE_FANINS_FANINNBS_LOCALLY:
            logger.info("DAG_executor " + thread_name + " connecting to TCP Server at %s." % str(TCP_SERVER_IP))
            websocket.connect(TCP_SERVER_IP)
            logger.info("DAG_executor " + thread_name + " successfully connected to TCP Server.")
        # else: # Config: A2, A4_local
        # on the client side
        #print("socketname: " + websocket.getsockname())   # ->  (127.0.0.1,26386)
        #print(websocket.getpeername())   # ->  (127.0.0.1, 8888)

        # ... unless its this work_queue when we use processes. (Lambdas do not use a work_queue, for now):)
        if (DAG_executor_constants.RUN_ALL_TASKS_LOCALLY and DAG_executor_constants.USING_WORKERS and not DAG_executor_constants.USING_THREADS_NOT_PROCESSES): 
            # Config: A5, A6
            # sent the create() for work_queue to the tcp server in the DAG_executor_driver
            #
            # each thread in multithreading multiprocesssing needs its own socket.
            # each process when single threaded multiprocessing needs its own socket.
            #work_queue = Work_Queue_Client(websocket,2*num_tasks_to_execute)

#brc: continue
            # we are only using incremental_DAG_generation when we
            # are computing pagerank, so far. Pagerank DAGS are the
            # only DAGS we generate ourselves, so far.
            if DAG_executor_constants.COMPUTE_PAGERANK and DAG_executor_constants.USE_INCREMENTAL_DAG_GENERATION:
                # Note: we are USING_WORKERS with worker processes
                DAG_infobuffer_monitor = Remote_Client_for_DAG_infoBuffer_Monitor(websocket)
                # Note: BFS calls DAG_infobuffer_monitor.create() so no call should be made here
                estimated_num_tasks_to_execute = DAG_executor_constants.WORK_QUEUE_SIZE_FOR_INCREMENTAL_DAG_GENERATION_WITH_WORKER_PROCESSES
                work_queue = Work_Queue_Client(websocket,estimated_num_tasks_to_execute)        
                logger.info("DAG_executor_work_loop: created Work_Queue_Client for work queue.")

                # Note: The remote work queue is created by the DAG_executor_driver 
                # since the driver needs to deposit leaf task work in the work queue before
                # creating the worker processes. We let the driver also create the 
                # DAG_infobuffer_monitor and below we make calls to deposit and withdraw 
                # in DAG_executor, as we do for the work queue.
                # Note: the driver either creates the work queue at the start
                # in which case is does not need to call create() the first time
                # it accesses the work queue, or the fanins/fanouts/faninNBs are created 
                # on demand in which case since the driver needs to use the work queue 
                # it calls create() before its first access of the queue.
            else:
                # Config: A5, A6
                # sent the create() for work_queue to the tcp server in the DAG_executor_driver
                #
                # each thread in multithreading multiprocesssing needs its own socket.
                # each process when single threaded multiprocessing needs its own socket.
                work_queue = Work_Queue_Client(websocket,2*num_tasks_to_execute)
        elif not DAG_executor_constants.RUN_ALL_TASKS_LOCALLY and DAG_executor_constants.COMPUTE_PAGERANK and DAG_executor_constants.USE_INCREMENTAL_DAG_GENERATION:
            #websocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            #websocket.connect(TCP_SERVER_IP)
            DAG_infobuffer_monitor = Remote_Client_for_DAG_infoBuffer_Monitor_for_Lambdas(websocket)
            # Note: BFS calls DAG_infobuffer_monitor.create_Remote_Client() so no call should be made here
            #logger.info("BFS: created Remote DAG_infobuffer_monitor_for_lambdas for labdas.")

    #else: # Config: A1, A2, A3, A4_local, A4_Remote

#brc: cluster:
        # worker_needs_input initialized to false and stays false
        # for non-worker configs. Set worker_needs_input based on
        # cluster_queue is empty test below when using workers.
        # Note: if not USING_WORKERS then worker_needs_input is initialized 
        # to False and every set of worker_needs_input to True is guarded 
        # by "if USING_WORKERS" so worker_needs_input is never
        # set to True if not USING_WORKERS.
        #
        #worker_needs_input = USING_WORKERS # set to False and stays False
        worker_needs_input = False # set to False and stays False if not using workers


 
#brc: cluster:
        cluster_queue = queue.Queue()
#brc: continue 
        # Note: continued_task initialized above
        process_continue_queue = False
        continue_queue = None
        # workers and simulated labdas use the continue_queue during incremental
        # DAG generation. Real lambdas use the continue queue when they are executing 
        # a restarted task for incremental DAG generation but only on the very frist
        # iteration of the work loop.
        if DAG_executor_constants.COMPUTE_PAGERANK and DAG_executor_constants.USE_INCREMENTAL_DAG_GENERATION:
            continue_queue = queue.Queue()
#hrc lambda inc:
        # True if we are executing the first iteration of the work loop
        # and this is a lambda. We use this value in an asssertion.
        first_iteration_of_work_loop_for_lambda = True

#hc: cluster:
        # The work loop checks cluster_queue > 0 to see if there are any 
        # become states in the cluster_queue. It does this when using workers
        # and when using lambdas. (When using lambas, we do not use a work_queue
        # but we do use a cluster queue.) To be consistent with this use of the 
        # cluster queue, we add the lambas start state to the cluster_queue so 
        # it can be handled as usual by chcking for cluster_queue > 0. So here
        # we add the state to the cluster_queue and blow we will immediately
        # get it from the cluster queue. (Workers when started will not find
        # any states in their cluster_queues (they each have a queue); instead they
        # will get work from the work_queue; the driver will put leaf task inputs
        # in the work_queue before it starts the worker(s).)
        #
        # Likewise, if the ral lambda is executing a continued task,
        # we put the task in the continue queue (as a tuple). Note that 
        # we have already processed the output of the continued task
        # in DAG_executor_lambda, where we put the output in the 
        # data_dict and saved the output in state_info.task_inputs from 
        # which it will be retrieved and used to process the fanins/
        # fanouts of the continued group, or to execute the collapse
        # task of the continued parttion. Noet that for partitons,
        # we can execute the partition/task, and since a partition 
        # does not have any fanouts/fanins, it only has a collapse task, 
        # when a parition is continued, we can execute its collapse
        # task. (There is no need to "process" the fanouts/fanins of the
        # partition as it has none, we can just execute the collapse
        # task, if there is one. The last partition of any connected component
        # does not have a collapse task.)
        #
        # Note: As the next step, we will immediately retrieve the task from from the continue_queue
        # or the cluster_queue. 
        if (DAG_executor_constants.RUN_ALL_TASKS_LOCALLY and not DAG_executor_constants.USING_WORKERS) or not DAG_executor_constants.RUN_ALL_TASKS_LOCALLY:
            # simulated or real lambdas are started with a payload that has a state,
            # put this state in the cluster_queue or the continue_queue
#brc: lambda inc:
            first_iteration_of_work_loop_for_lambda == True
            if DAG_executor_constants.COMPUTE_PAGERANK and DAG_executor_constants.USE_INCREMENTAL_DAG_GENERATION and continued_task:
#brc: lambda inc: cq.put
                # On first iteration of lambda a continued_task is truly a continued_task.
                # For leaf tasks, continued_task is False.
                continue_tuple = (DAG_executor_state.state,True)
                #continue_queue.put(DAG_executor_state.state)
                continue_queue.put(continue_tuple)
                logger.trace("DAG_executor_work_loop: start of simulated or real lambda:"
                    + " put state " + str(DAG_executor_state.state) + " in continue_queue"
                    + " with first_iteration_of_work_loop_for_lambda " 
                    + str(first_iteration_of_work_loop_for_lambda))

            else:
                cluster_queue.put(DAG_executor_state.state)
                logger.trace("DAG_executor_work_loop: start of simulated or real lambda:"
                    + " put state " + str(DAG_executor_state.state) + " in cluster_queue.")
            
        # An outline of this main loop, which processes tasks and their fanins/fanouts/collapses
        """
        while (True): # main work loop: iterate until worker/lambda is finished

            if using workers:
                if process_continue_queue:
                    we are getting states from the continue queue
                    until the continue queue is empty.    
#brc: lambda inc: cq.get
                    #DAG_executor_state.state = continue_queue.get()
                    continue_tuple = continue_queue.get()
                    DAG_executor_state.state = continue_tuple[0]
                    continued_due_to_TBC = continue_tuple[1]
                    continued_task = continued_due_to_TBC

                    Each state was identified as a to-be-continued state in the 
                    previous partition. After getting a new DAG, we 
                    start by processing all the continued states
                    
                    if continue_queue.qsize() == 0:
                        # Note: When using partitions this is True, i.e., 
                        # only one partition can be to-be-continued 
                        process_continue_queue = False
                else:
                    if cluster_queue.qsize() == 0:
                        #cluster queue is empty so get work from the work queue
                        work_tuple = work_queue.get()
                        DAG_executor_state.state = work_tuple[0]
                        dict_of_results = work_tuple[1]

                        # if we are doing inremental DAG generation, a state
                        # we get from the work queue may not be executable.
                        # such a state is a leaf task that is the first state/task
                        # in a new connected component. Such states are added to
                        # the continue queue and processed after getting the next
                        # incremental DAG.
                        while (True:
                            worker does not need work since there is work
                                in the cluster queue. Get work from cluster_queue.
                                Note: Currently, we do not cluster more than one fanout and
                                we do not cluster faninNB/fanins, and there is only one
                                collapse task, so the cluster_queue will have only one
                                work task in it.
                                DAG_executor_state.state = cluster_queue.get()
                                                                
                                if we are not doing incremental DAG generation
                                    # the work is executable so break this inner loop
                                    # and excute the state
                                    break # while(True) loop
                                else: # using incremental DAG generation
                                    # the work state could be a -1, which means that we are either
                                    # (a) stopping execution for good, or until we get a new incremental DAG
                                    # (b) a non-leaf task that can be executed
                                    # (c) a leaf task. The leaf task might be unexecutable until we get a new incremental DAG
                                    if not DAG_executor_state.state == -1:
                                        # try to get the state_info for the work task (state).
                                        # this may return None
                                        state_info = DAG_map.get(DAG_executor_state.state)
                                        # if there is no state_info for the work task it must be
                                        # an unexecutable leaf task, i.e., the leaf task stat_info will be 
                                        # in the next incremental DAG but not this current incremental DAG.
                                        # Even If there is state_info for this task then this is an 
                                        # unexectuable leaf task if this task name is in the list of leaf
                                        # task names and it is a to-be-continued task.
                                        # Non-leaf tasks state_info is always in the current DAG.
                                        if is_unexecutable_leaf_task:
                                            # Cannot execute this leaf task until we get a new DAG and 
                                            # continue executing the leaf task
                                            if USING_WORKERS:
#brc: lambda inc: cq.put
                                                # An unexecutable leaf task should be executed so 
                                                # it is not truly a continued task (i.e., with TBC fanins/fanouts/collpases)
                                                continue_tuple = (DAG_executor_state.state,False)
                                                continue_queue.put(continue_tuple)
                                                # Note: we do not break the while(Tru) loop since we 
                                                # did not get excutable work or a -1. Do another
                                                # iteration of the whie(True) loop
                                            else:
                                                pass # lambdas TBD: call Continue object w/output
                                        else:
                                            # we got actul work (not -1) from the work queue (a non-leaf task),
                                            # so break the while(True) loop and do the work
                                            break # while(True) loop
                                    else:
                                        # we got -1 so break the while(True) loop and process the -1 next.
                                        break # while(True) loop
                        # end of while(True)

                        if DAG_executor_state.state == -1:
                            # we are starting or continuing the process of shutting dwon the 
                            # workers, either permanently or until we get a new incremental DAG
                            
                            # if there are workers that have not yet received a -1:
                                # add another -1 to the work queue. Each worker but the last
                                # worker to get a -1 from the work queue will add a -1 
                                # to the work queue, so that all workers will get a -1.
                                    work_tuple = (-1,None)
                                    work_queue.put(work_tuple)
                        if COMPUTE_PAGERANK and USE_INCREMENTAL_DAG_GENERATION:
                            # we are doing incremental ADG generation. The currnet 
                            # DAG has no more avaailable work. If the current DAG is
                            # not complete, the workers need to get a new 
                            # version of the incremental DAG and continue.
                            if not DAG_info.get_DAG_info_is_complete():
                                DAG_info_and_new_leaf_task_states_tuple = DAG_infobuffer_monitor.withdraw(requested_current_version_number)
                                # new DAG
                                new_DAG_info = DAG_info_and_new_leaf_task_states_tuple[0]
                                # list of new leaf task states in new DAG
                                new_leaf_task_states = DAG_info_and_new_leaf_task_states_tuple[1]

                                # we have a new DAG so we can execute the tasks in the 
                                # previous DAG that were to-be-continued
                                if continue_queue.qsize() > 0:
                                    # if this worker has incomplete tasks in its continue
                                    # queue (like a leaf task from above, or the last partition
                                    # in an incremental DAG, which is always incomplete) then 
                                    # start by executing those tasks.
#brc: lambda inc: cq.get
                                    #DAG_executor_state.state = continue_queue.get()
                                    continue_tuple = continue_queue.get()
                                    DAG_executor_state.state = continue_tuple[0]
                                    continued_due_to_TBC = continue_tuple[1]
                                    continued_task = continued_due_to_TBC
                                    if continue_queue.qsize() == 0:
                                        # only one task/state was in continue_queue
                                        process_continue_queue = False
                                    else:
                                        # process tasks/states in continue_queue
                                        logger.trace("DAG_executor_work_loop: XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXwork_loop: continue_queue.qsize()>0 set process_continue_queue = True.")
                                        process_continue_queue = True
                                        # we will continue to process tasks in the continue
                                        # queue until the continue queue is empty.

                                else:
                                    # We got a new DAG_info, but we have no continued tasks to execute.
                                    # Note that we tried to get work which ultimaely led to us getting
                                    # a new DAG and reaching this point. We have no continued tasks
                                    # to execute so we still need work. Thus, we need to try again
                                    # to get work. Note also that to to get here we have to have an 
                                    # empty cluster_queue too, i.e., we requested work from the work queue so the 
                                    # cluster_queue must have been empty. So when we now try to get 
                                    # more work we will not get it from our cluster_queue (or our
                                    # empty continue_queue)

                                    # Back to the top of the work loop where we will try again to get work.
                                    continue

                            else: 
                                # incremental DAG is complete
                                return
                        else:
                            # we are not doing incremental DAG generation, hence
                            # the DAG is complete so terminate worker. All
                            # workers will get a -1 and terminate
                            return

                    else: 
                        # worker does not need work since there is work
                        # in the cluster queue. Get work from cluster_queue.
                        # Note: Currently, we do not cluster more than one fanout and
                        # we do not cluster faninNB/fanins, and there is only one
                        # collapse task, so the cluster_queue will have only one
                        # work task in it.
                        DAG_executor_state.state = cluster_queue.get()

                # end not process_continue_queue
                
                # We have a task to execute - we got it from the work queue
                # or the cluster queue or the continue queue.

                # If this is a continued task, we will have already executed it if
                # we are using groups instead of partitions. Also, if the task is 
                # the first task in a new connected component and it is not 
                # the first task in the DAG then we have not executed the task yet. 
                # We will execute this task so here we increment
                # the number of tasks that have been excuted. Otherwise we have
                # already executed the continued task. This means we will not 
                # execute it below and here we should not increment the number of tasks
                # that have been executed.

                # Note: The first task PR1_1 in the DAG is never a continued 
                # task since the first DAG always has a PR1_1 that is not 
                # to-be-continued. So if continud_task is True then this is not PR1_1. 
                # Note that PR1_1 will be in the ist of leaf task names.

                # check if we are using groups
                incremental_dag_generation_with_groups = COMPUTE_PAGERANK and USE_INCREMENTAL_DAG_GENERATION and USE_PAGERANK_GROUPS_PARTITIONS
                # get the state info for this task/state
                continued_task_state_info = DAG_map[DAG_executor_state.state]
                # execute the task if this is False:
                # (1) we are using groups       # if we are using partitions we execute the task
                # and (2) this is a continued task     # if this is not a continued task we execute the task
                # and (3) this is leaf task PR1_1 or this is not any other leaf task
                if not (incremental_dag_generation_with_groups and continued_task and (
                    (continued_task_state_info.task_name == NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG or not continued_task_state_info.task_name in DAG_info.get_DAG_leaf_tasks())
                )):

                    # Note: This increment_and_get executes a memory barrier - so the pagerank writes to 
                    # shared memory (if used) just performed by this process P1 
                    # will be flushed, which means the downstream pagerank tasks 
                    # that read these values will get the values written by P1.
                    # So we need a memory barrier between a task and its downstream
                    # tasks. Use this counter or if we remove this counter, something 
                    # else needs to provide the barrier.

                    # We will execute the task below. Here we 
                    # increment num tasks executed and see if this is the last 
                    # task to execute. If so, start the termination process or
                    # if we are doing incremental DAG generation (so this is the 
                    # last task in the current version of the DAG)) workers
                    # get the next DAG (instead of terminating).
                    num_tasks_executed = completed_tasks_counter.increment_and_get()
                    if num_tasks_executed == num_tasks_to_execute:
                        # Note: This worker has work to do, and this work is the last
                        # task to be executed. So this worker and any other workers
                        # can finish (if the DAG is not incremental or it is incremental
                        # and complete) This worker starts the worker shutdown (or pause
                        # for the new incremental DAG) by putting a -1 in the work queue.
                        # Any worker that is waiting for work or that tries to get more 
                        # work will get this -1. Note that this worker here that is adding
                        # -1 may get this -1 when it tries to get work after executing
                        # this last task. Workers who get -1 from the work queue put
                        # a -1 back in the work queue if there are still workers who
                        # have not completed (i.e., called get work). We have a counter
                        # to track the number of completed (paused) workers.
                        #
                        # Note: No worker calls DAG_infobufer_Monitor.withdraw to get a 
                        # new incremental DAG until all the tasks in the current version
                        # of the incremental DAG have been executed. This is because,
                        # each worker must first get a -1 from the work queue, at which 
                        # point it may deposit another -1 (if some other workers have not
                        # withdrawn their -1 yet) and then they will call DAG_infobufer_Monitor.withdraw
                        # to get the net DAG, instead of returning and thus ending DAG execution. 
                        #
                        # Process: the worker tht executes the last excutable task in the 
                        # current DAD will deposit a -1 in th work queue. If one or more
                        # workers are waiting for work, one will wakepup and withdraw this
                        # -1. Otherwse, the first worher to call withdraw to get more work
                        # will withdraw this -1. The worker that gets this -1 will check to 
                        # see whether it was the only worker that had not withdrawn a -1.
                        # if so, it will deposit another -1 so that another worker can get
                        # its -1, etc. This proces stops when each worker has withdrawn its -1.
                        # 
                        # Note too that workers only call 
                        # DAG_infobufer_Monitor.withdraw (below) if the current version of the 
                        # incremental DAG is not complete, so the withdraw() will return 
                        # a new version of the increnetal DAG. Eventually, the DAG will 
                        # be complete and the workers will return (i.e., terminate) instead of 
                        # calling DAG_infobufer_Monitor.withdraw.
                        if not USING_THREADS_NOT_PROCESSES:
                            # Config: A5, A6
                            logger.trace(thread_name + ": DAG_executor: num_tasks_executed == num_tasks_to_execute: depositing -1 in work queue.")
                            work_tuple = (-1,None)
                            # for the next worker
                            work_queue.put(work_tuple)
                        else:
                            # Config: A4_local, A4_Remote
                            logger.trace(thread_name + ": DAG_executor: num_tasks_executed == num_tasks_to_execute: depositing -1 in work queue.")
                            # for the nex worker
                            work_tuple = (-1,None)
                            work_queue.put(work_tuple)
                else:
                    pass 
                    # didn't increment num_tasks_executed since we are not
                    # excuting this task (as it was already executed)
            
                    # No return here. A worker may return when it gets a -1
                    # from the work_queue (not here when it puts a -1 in the work queue.)
                    # 
                    # The worker that executes the last task will
                    # add a -1 to the work queue (-1,None) so that the next worker
                    # to try to get work will get a -1. That next worker will add a
                    # -1 to the work queue , etc. Note that the last worker
                    # will not add a -1 to the work_queue.
                    #return

            else: # not using workers: Config: A1. A2, A3
                # if we are using simulated or real lambdas, then the lambda
                # payload has a state to execute. If the lambda has not been 
                # (re)started for incremental ADG generation to do the fanouts/
                # fanins/collpased of a group the state is added to the 
                # cluster_queue at the start of the work loop. Otherwise, the 
                # state is added to the continue queue. Lambdas may also
                # add collapased states to the cluster queue and fanout/fanin
                # become states are also added to the cluster queue. Real and 
                # simulated lambdas do not use a work_queue.
                # When processing the tasks, we may
                # or may not add become tasks for fanouts and fanins and 
                # collapsed tasks to the custer_queue, so the cluster_queue
                # may be empty on later checks.
                # For lamdas, the continue queue can be non-empty only on
                # the first iteration of the work loop. the continue queue
                # is not used by lambas after that.
                elif continuer_queue.qsize() > 0:
#brc: lambda inc: cq.get
                    #DAG_executor_state.state = continue_queue.get()
                    continue_tuple = continue_queue.get()
                    DAG_executor_state.state = continue_tuple[0]
                    continued_due_to_TBC = continue_tuple[1]
                    continued_task = continued_due_to_TBC
                elif cluster_queue.qsize() > 0:
#brc: lambda inc: cq.get
                    DAG_executor_state.state = cluster_queue.get()
                else:
                    # Error - if the continue and clster queues are 
                    # empty then lambda shoudl have terminated at the 
                    # end of the last work loop iteration

            # For incremental DAG generation using groups (instead of partitions)
            # we never execute the continued task unless it is a leaf task
            # that is not the first group ("PR1_1") in the DAG. These leaf tasks are
            # groups that start a new connected component. They are added to the 
            # work queue by the DAG generator. When we get such a leaf task from the 
            # work_queue it may not yet be executable, in which case it is added
            # to the continue queue. (It will become executable when we get our
            # next incremental DAG). It then becomes a continued task and it has
            # not been executed before so we need to execute it, as opposed to 
            # executing its collapse task.
            incremental_dag_generation_with_groups = COMPUTE_PAGERANK and USE_INCREMENTAL_DAG_GENERATION and USE_PAGERANK_GROUPS_PARTITIONS

            # Note: The first task/group/partition in the DAG is a leaf task
            # PR1_1 and it is never a continued task. Assume that the DAG has 
            # more than one partition. Then the first version of the 
            # incremental DAG always has partitions 1 and 2, or if using groups,
            # group PR1_1 and the groups in partition 2, and partition 2 (and the 
            # groups therein) is to-be-continued but partition 1 is not to-be-continued.
            # PR1_1 is in DAG_info.get_DAG_leaf_tasks() and if partition 2 or its
            # groups are to-be-continued then we will put state 1 of PR1_1 in the 
            # continue queue and we may have that state here so we use 
            # state_info.task_name == "PR1_1" to make sure we do not execute PR1_1 again.
            # Note: The if-condition will be False if we are executing a leaf
            # task/group that is a new connected component (not "PR1_1"), as 
            # we want to execute these new leaf tasks. 
            # Note: "PR1_1" is a leaf task of the DAG and it is always the first
            # group/partition/task executed. New leaf tasks can be discovered
            # during incremental DAG generation (but not during non-incremental 
            # DAG generation.)
            #
            # Note: if we are using groups, and the continued task C is not a new 
            # leaf task (i.e., is not "PR1_1") then we have already executed the 
            # continued task C. After executing C, we saw that C had to-be-continued
            # fanins/fanouts/faninNBs/collapses which means they could not be 
            # processed. Thus we put executed task C in the continue queue. Now we are
            # processing that already executed continue task C. Since we got a new
            # DAG, C's to-be-continued anins/fanouts/faninNBs/collapses are no 
            # longer to-be-continued so we will skip execution of C and process 
            # C's fanins/fanouts/faninNBs/collapses.
            #
            # If we are using partitions, then the continued task C has already been 
            # executed. When we executed C, its collapse task P was to-be-continued so 
            # we could not execute P. So we put C not P in the continue queue. Here
            # we got C from the continue queue. We do not execute C, which has already
            # been excuted, instead we execute C's collpase task P.
            state_info = DAG_map[DAG_executor_state.state]

            if incremental_dag_generation_with_groups and continued_task and (
            (state_info.task_name == NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG or not state_info.task_name in DAG_info.get_DAG_leaf_tasks())
            ):
                continued_task = False # reset flag
                pass 
                # do not execute this group/task since it has been excuted before.
            else:
                # Execute task (but which task?)
                #
                # But first see if we are doing incremental DAG generation and
                # we are using partitions. If so, we may need to get the collapse
                # task of the continued state and excute the collapse task.
                # Note: We can also get here if we are doing incremental DAG generation
                # with groups and the task is a leaf task (that is not the first group in the 
                # DAG) that starts a new connected component (which is the first 
                # group generated on any call to BFS()). We will execute this
                # group leaf task. 
                #
                # Note: The if-condition will be False if we are executing a leaf
                # task/group that is a new connected component (not "PR1_1"), 
                # in which case continued_task may have been true and it's
                # still True. We need to reset it to False so it is False
                # at the start of the next iteration.
                #
                incremental_dag_generation_with_partitions = COMPUTE_PAGERANK and USE_INCREMENTAL_DAG_GENERATION and not USE_PAGERANK_GROUPS_PARTITIONS
                if incremental_dag_generation_with_partitions and continued_task:
                    continued_task = False
                    # if continued state is the first partition (which is a leaf task) or is not a leaf task
                    # then get the collapse task of the continued state; otherwise, execute the 
                    # continued state, which is a leaf task that starts a new conncted
                    # component. Note: For groups, we do not get the collapsed task.
                    # We do for partitions because partitions only have collapsed tasks,
                    # i.e., no fanouts or fanins, and if task T has a collapsed task C then 
                    # the same worker W that executed T will execute C.  
                    # For groups, we execute the task T and 
                    # if T is to be continued, we put the state of T in the continue
                    # queue and when we get this state from the continue queue we do its 
                    # fanins/fanouts/collapses. Note that if W executed T and T has 
                    # many fanouts then we do not want to put all of these fanouts in 
                    # W's continue queue since that would mean W would execute all
                    # the fanout tasks of T. Instead, when W gets the state for T 
                    # from the continue queue W can (skip the execution of T since that
                    # already happened) do T's fanouts as ususal, i.e., W will 
                    # become/cluster one of T's fanouts and put the rest on the shared
                    # worker queue to distribute the fanout tasks amoung the workers.
                    #
                    if state_info.task_name == NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG or (not state_info.task_name in DAG_info.get_DAG_leaf_tasks()):
                        # get the collapse task
                        DAG_executor_state.state = DAG_info.get_DAG_states()[state_info.collapse[0]]
                        state_info = DAG_map[DAG_executor_state.state]
                        logger.trace("DAG_executor_work_loop: got state and state_info of continued, collapsed partition for collapsed task " + state_info.task_name)
                    else:
                        # execute task - partition task is a leaf task that is not the first
                        # partition ("PR1_1") in the DAG. (Leaf tasks statr a new connected component.)
                        logger.trace("DAG_executor_work_loop: continued partition  " 
                            + state_info.task_name + " is a leaf task so do not get its collapse task"
                            + " instead, execute the leaf task.")
                else:
                    # if continued task is TRUE this will set it to False
                    continued_task = False
                    # execute task - task is a group task that is a 
                    # leaf task (but not the first group in the DAG)
                    # or it is a partition task that is not a continued task.
                    
            #execute task ...
            """
        while (True): # main work loop: iterate until worker/lambda is finished
            if DAG_executor_constants.USING_WORKERS:
                # Config: A4_local, A4_Remote, A5, A6
#brc: continue 
                # We will get work from the continue_queue (if incremental
                # DAG generation) or else the clusger_queue or else the 
                # work_queue, and that is the priority of the queues.
                # The coninue_queue is tasks that were incomplete in the 
                # last version of the incremental DAG and that can be executed
                # in the new version. The cluster_queue contains clustered
                # tasks, if any. The work_queue is where enabled fanout and 
                # faninNB tasks are placed so they aer available to all workers,
                # 
                # Note: After getting a task from one of these queues,
                # we will increment the count of excuted tasks and see
                # if we've executed all the complete tasks in the DAG. If so,
                # for incremental DAG generation, we try to get a new DAG if the 
                # current DAG is incomplete; otherwise, thr workers can terminae.
                # For non-incremental DAG generation, if we've executed all the tasks
                # then the workers can complete.
                #
                # When worker received a new incremental DAG, it will 
                # check whether there are tasks in its continue_queue.
                # These are tasks that were not complete in the DAG and could
                # not be executed until a new incremental DAG was obtained.
                # The worker will get one of these tasks right after getting
                # the DAG. If the continue_queue is not then empty, it will set
                # process_continue_queue to True so the remaining continued tasks
                # can be executed one-by-one. 
                # Note: If there is work in the continue queue we get it,
                # else if there is work in the cluster_queue we get it
                # else we try to get work from the work queue.
                # When using partitions, all the partitions in the DAG
                # that are not the last partition are complete an can be 
                # executed. The last partition may be incomplete - it is
                # complete only if the DAG is complete (all the grah nodes
                # aer in a paritition.)
                if process_continue_queue:
                    try:
                        msg = "[Error]: work loop: process_continue_queue but" +  " not COMPUTE_PAGERANK or not USE_INCREMENTAL_DAG_GENERATION."
                        assert DAG_executor_constants.COMPUTE_PAGERANK and DAG_executor_constants.USE_INCREMENTAL_DAG_GENERATION , msg
                    except AssertionError:
                        logger.exception("[Error]: assertion failed")
                        if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
                            logging.shutdown()
                            os._exit(0)
                    # assertOld:
                    #if not (COMPUTE_PAGERANK and USE_INCREMENTAL_DAG_GENERATION):
                    #    logger.error("[Error]: work loop: process_continue_queue but"
                    #        +  " not COMPUTE_PAGERANK or not USE_INCREMENTAL_DAG_GENERATION.")

                    # The first partition was obtained from the continue queue 
                    # when we got the new DAG_info and there should be no more
                    # partitions in the continue queue since there can be only 
                    # one partition that is to-be-continued.
                    try:
                        msg = "[Error]: work loop: process_continue_queue but" \
                            + " using partitions so this second to be continued" \
                            + " partition/task should not be possible."
                        assert DAG_executor_constants.USE_PAGERANK_GROUPS_PARTITIONS , msg
                    except AssertionError:
                        logger.exception("[Error]: assertion failed")
                        if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
                            logging.shutdown()
                            os._exit(0)
                    #assertOld:
                    #if not USE_PAGERANK_GROUPS_PARTITIONS:
                    #    logger.error("[Error]: work loop: process_continue_queue but"
                    #       + " using partitions so this second to be continued"
                    #        + " partition/task should not be possible.")
#brc: lambda inc: cq.get
                    #DAG_executor_state.state = continue_queue.get()
                    continue_tuple = continue_queue.get()
                    DAG_executor_state.state = continue_tuple[0]
                    continued_due_to_TBC = continue_tuple[1]
                    # Indicate whether we are executing a continued task.
                    continued_task = continued_due_to_TBC
                    #continued_task = True

                    if continued_task:
                        continued_output_for_worker = continue_tuple[2]
                    else:
                        continued_output_for_worker = None

                    # If we are done executing the continued tasks then turn
                    # process_continue_queue off. Note: When we aer executing 
                    # partitions, instead of groups, there is only ever one
                    # task in the continue_queue
                    if continue_queue.qsize() == 0:
                        # Note: When using partitions this is True, i.e., 
                        # only one partition can be to-be-continued 
                        process_continue_queue = False

                else: # not process_continue_queue
                    # Workers don't always get work from the work queue. They may get work from the 
                    # cluster queue instead. If a worker has a collapse task, it puts the collapse state in 
                    # the cluster queue and then gets it from the qeueue on the next iteration
                    # of the work loop (which it does immediately). Note that collapsed tasks
                    # are just fanout tasks when there is only one fanout - these tasks are always clustered.
                    # The become fanout task/state is put in the cluster_queue, same for fanin become tasks/states.
                    # After processing fanouts, and putting the become fanout in the 
                    # cluster_queue, or processing a fanin and if we are the become fanin
                    # task putting this task in the cluster_queue, we will immediatley 
                    # iterate the work loop and get a task from the cluster_queue (one by one
                    # until the cluster queue is empty.)
                    # For faninB there are no becomes - after the last caller callls fanin 
                    # the faninNB tasks is added to the work_queue.
                    # If we are batching faninNBs (and fanouts), and the worker calling batch() needs
                    # work (i.e., its cluster_queue is empty) then the worker may receive work back
                    # from the tcp_server and the worker will put this work in its cluser_queue.
                    # In this way, instead of adding all the batched fanout and faninNB
                    # work to the work_queue, we just return one of the fanouts/faninNBs
                    # back to the calling worker, so it does not need to access the 
                    # (possibly remote) work_queue.
                    # Note: When a worker tries to get a new incremental DAG then
                    # its cluster queue is empty, since a worker can only get a new 
                    # DAG after it tries to get work from the work_queue and for this 
                    # to occur the workers cluster_queue must be empty. (So a
                    # worker only tries to get work from the work_queue if the 
                    # worker's cluster_queue is empty.)

#brc: cluster:
                    # Just take worker_needs_input out of it.
                    #worker_needs_input = cluster_queue.qsize() == 0

                    if cluster_queue.qsize() == 0:
                        while (True):
                            # Try to get work from the work_queue.
                            # We may get work or we may get -1. 
                            # - The -1 indicates that all complete tasks in the DAG (which 
                            # may be incremental) have been executed, so either
                            # (1) we are not doing incremental DAG generation 
                            # and we are done; or (2) are are doing incremental 
                            # DAG generation and (a) the current DAG is not complete
                            # so we should get a new incremental DAG, or (b) the 
                            # current incremental DAG is complete (we have executed
                            # all the tasks) and we are done.
                            # - If we do not get -1, then we get a task
                            # that can be executed. If this task is not a 
                            # leaf task, then we can execute it. If it is a leaf
                            # task then we may have to delay its execution. That is,
                            # the increental DAG generator may add a leaf task to the 
                            # next DAG. That leaf task must be added to the work queue
                            # so it can be executed. (Such a leaf task is the first 
                            # partition/group in a new connected component that is 
                            # explored by BFS(). That is, if there are n connected 
                            # components in the graph then we will call BFS() n times.
                            # The first partition/group generated by such a call to BFS() is
                            # a leaf node.) But a worker can then get the leaf task from
                            # the work_queue *before* it gets the new incremental DAG that 
                            # contains the leaf task. That is the leaf task may not be in the
                            # current DAG, or if it is it may not be marked as "completed." If 
                            # so, we put the leaf task(s) in the worker's continue_queue
                            # to be executed after the worker gets the next DAG. It would require a 
                            # lot of synchronization to ensure that leaf tasks are added to the work
                            # queue (and thus are made available to workers) only after *every* 
                            # worker has obtained their next incremental DAG. 

                            logger.info("DAG_executor_work_loop: cluster_queue.qsize() == 0 so"
                                + " get work")
                            
                            if not DAG_executor_constants.USING_THREADS_NOT_PROCESSES: # using worker processes
                                # Config: A5, A6
                                logger.info("DAG_executor_work_loop: proc " + proc_name + " " + " thread " + thread_name + ": get work.")
                                work_tuple = work_queue.get()
                                DAG_executor_state.state = work_tuple[0]
                                dict_of_results = work_tuple[1]
                                # put the inputs for the work task to be executed, which are 
                                # in dict_of_results (i.e., the results executing one or more
                                # fanout/fanin tasks whose outputs are input to the work task)
                                # in the data_dict where they can be accessed for excuting the task.
                                if dict_of_results != None:
                                    # If the state is -1, there is no work task as all the tasks
                                    # in the (non-incremental) DAG have been executed so we can quit.
                                    logger.trace("DAG_executor_work_loop: dict_of_results from work_tuple: ")
                                    for key, value in dict_of_results.items():
                                        logger.trace(str(key) + " -> " + str(value))
                                    for key, value in dict_of_results.items():
                                        data_dict[key] = value
                                #else: dict_of_results is None 
                                #  Suggest assert state is -1

                                logger.info("DAG_executor_work_loop: got work/-1 for thread " + thread_name
                                    + " state is " + str(DAG_executor_state.state))
                                if not (DAG_executor_constants.COMPUTE_PAGERANK and DAG_executor_constants.USE_INCREMENTAL_DAG_GENERATION):
                                    # got work from the work queue so break the while(True)
                                    # loop and check if it's -1
                                    break # while(True) loop
                                else: # using incremental DAG generation
                                    # work could be a -1, or a non-leaf task or leaf task. The leaf task
                                    # might be unexecutable until we get a new incremental DAG
                                    if (not DAG_executor_constants.USE_PAGERANK_GROUPS_PARTITIONS) or DAG_executor_constants.USE_PAGERANK_GROUPS_PARTITIONS:
                                        if not DAG_executor_state.state == -1:
                                            # try to get the state_info for the work task (state).
                                            # this may return None
                                            state_info = DAG_map.get(DAG_executor_state.state)
                                            # if there is no state_info for the work task it must be
                                            # an unexecutable leaf task, i.e., the leaf task stat_info is 
                                            # in the next incremental DAG but not this current incremental DAG.
                                            # Non-leaf tasks state_info is alwaye in the current DAG.
                                            # If there is state_info for this task then this is an 
                                            # unexectuable leaf task if this task name is in the list of leaf
                                            # task names and it is a to-be-continued task.
                                            is_leaf_task = state_info is None or state_info.task_name in DAG_info.get_DAG_leaf_tasks()
                                            logger.trace("DAG_executor_work_loop: work from work_queue is_leaf_task: " + str(is_leaf_task))
                                            is_unexecutable_leaf_task = state_info is None or (
                                                state_info.task_name in DAG_info.get_DAG_leaf_tasks() and state_info.ToBeContinued
                                            )
                                            if is_unexecutable_leaf_task:
                                                # Cannot execute this leaf task until we get a new DAG. Note: the
                                                # DAG generator added this leaf task to the work queue after it
                                                # made the new DAG containing this completed leeaf task available
                                                # so we will be able to execute this leaf task after we get the new 
                                                # DAG. Note: each leaf task is the start of a new connected component
                                                # in the graph (you cannot reach one CC from another CC.) and the 
                                                # partitions of the connected components can be executed in parallel.
                                                #
                                                # Note: We add this unexecutable leaf task to the continue
                                                # queue. It is not a "usual" to be continued task, in that 
                                                # it was contnued becuase it had fanins/fanouts to incomplete
                                                # tasks; it is continued because we do not yet have an 
                                                # incremental DAG that contains this task - the next incremental
                                                # DAG we withdraw will ave this task. So we add it to the 
                                                # continue queue with a tuple having "False" for 
                                                # continued_task and no output since we have no executed this
                                                # task yet. When we get this tuple from the continue_queue we\
                                                # will see continued_tsk is False so we will not ty to get
                                                # the output (from tuple[2)). Tasks that are executed and then 
                                                # added to the continue_queue as continued_task = True, are added
                                                # using a tuple (DAG_executor_state.state,False,output). So when 
                                                # we get the tuple from the continue_queue we will get the 
                                                # output of the (previous execution of the) task and use it to 
                                                # process the task's fanins/fanouts if it is a group and 
                                                # excute the continued tasks's collapse task if it is a 
                                                # partition. (Again, this is for workers, real lambdas
                                                # that execute a task and find the tsk has to be continued
                                                # fanins/fanouts will stop after saving the task's output 
                                                # via a DAF_infoBuffer_Monitor_for_Lambdas.withdraw)() on the tcp_server.
                                                # A lambdas will be (re)started to process the continued tasks's
                                                # fanins/fanouts (using the saved task output) when a new
                                                # incremental DAG_info is deposited by bfs() via
                                                # DAF_infoBuffer_Monitor_for_Lambdas.deposit(). (That is deposit()
                                                # restarts a real lambdas for each task saved by withdraw(),
                                                # and for any new leaf tasks in the deposited DAG_info. A new
                                                # leaf task(s) will be identified for each new connected component
                                                # discovered in the new incremental DAG.
                                                if DAG_executor_constants.USING_WORKERS:
#brc: lambda inc: cq.put
                                                    # An unexecutable leaf task should be executed so 
                                                    # it is not truly a continued task (i.e., with TBC fanins/fanouts/collpases)
                                                    continue_tuple = (DAG_executor_state.state,False)
                                                    #continue_queue.put(DAG_executor_state.state)
                                                    continue_queue.put(continue_tuple)
                                                    logger.trace("DAG_executor_work_loop: work from work loop is unexecutable_leaf_task,"
                                                        + " put work in continue_queue:"
                                                        + " state is " + str(DAG_executor_state.state))
                                                else:
                                                    # This work loop is only for non-ambdas
                                                    pass
                                            else:
                                                # we got work (not -1) from the work queue (a non-leaf task),
                                                # so break the while(True) loop.
                                                logger.trace("DAG_executor_work_loop: work from work queue is not an unexecutable_leaf_task,"
                                                    + " state is " + str(DAG_executor_state.state))
                                                break # while(True) loop
                                        else:
                                            # we got -1 so break the while(True) loop and process the -1 next
                                            break # while(True) loop
                                    else:
                                        pass # finish for groups
                            else: # using worker threads
                                # Config: A4_local, A4_Remote
                                logger.info("DAG_executor_work_loop:: get work for thread " + thread_name)
                                work_tuple = work_queue.get()
                                DAG_executor_state.state = work_tuple[0]
                                dict_of_results = work_tuple[1]
                                logger.info("DAG_executor_work_loop: got work for thread " + thread_name
                                    + " state from work tuple is " + str(DAG_executor_state.state))
                                if dict_of_results != None:
                                    logger.trace("DAG_executor_work_loop: dict_of_results from work_tuple: ")
                                    for key, value in dict_of_results.items():
                                        logger.trace(str(key) + " -> " + str(value))
                                    # Threads put task outputs in a data_dict that is global to the threads
                                    # so there is no need to do it again here, when getting work from the work_queue.
                                    #for key, value in dict_of_results.items():
                                    #    data_dict[key] = value 
                                if not (DAG_executor_constants.COMPUTE_PAGERANK and DAG_executor_constants.USE_INCREMENTAL_DAG_GENERATION):
                                    break # while(True) loop
                                else: # incremental DAG generation
                                    # Work could be a -1, or a non-leaf task or leaf task. The leaf task
                                    # might be unexecutable until we get a new incremental DAG.
                                    if (not DAG_executor_constants.USE_PAGERANK_GROUPS_PARTITIONS) or DAG_executor_constants.USE_PAGERANK_GROUPS_PARTITIONS: 
                                        # work could be a -1, or a non-leaf task or leaf task. The leaf task
                                        # might be unexecutable until we get a new incremental DAG
                                        if not DAG_executor_state.state == -1:
                                            # try to get the state_info for the work task (state)
                                            # this may return None
                                            state_info = DAG_map.get(DAG_executor_state.state)
                         
                                            # if there is no state_info for the work task it must be
                                            # an unexecutable leaf task, i.e., the leaf task stat_info is 
                                            # in the next incremental DAG but not this current incremental DAG.
                                            # Non-leaf tasks state_info is alwaye in the current DAG.
                                            # If there is state_info for this task then this is an 
                                            # unexectuable leaf task if this task name is in the list of leaf
                                            # task names and it is a to-be-continued task.
                                            is_leaf_task = state_info is None or state_info.task_name in DAG_info.get_DAG_leaf_tasks()
                                            logger.trace("DAG_executor_work_loop: work from work_queue is_leaf_task: " + str(is_leaf_task))
                                            is_unexecutable_leaf_task = state_info is None or (
                                                state_info.task_name in DAG_info.get_DAG_leaf_tasks() and state_info.ToBeContinued
                                            )
                                            # See the corresponding comment above for worker processes
                                            if is_unexecutable_leaf_task:
                                                # Cannot execute this leaf task until we get a new DAG. Note: the
                                                # DAG generator added this leaf task to the work queue after it
                                                # made the new DAG containing this completed leeaf task available
                                                # so we will be able to execute this leaf task after we get the new 
                                                # DAG. Note: each leaf task is the start of a new connected component
                                                # in the graph (you cannot reach one CC from another CC.) and the 
                                                # partitions of the connected components can be executed in parallel
                                                if DAG_executor_constants.USING_WORKERS:
#brc: lambda inc: cq.put                            # An unexecutable leaf task should be executed so 
                                                    # it is not truly a continued task (i.e., with TBC fanins/fanouts/collpases)
                                                    continue_tuple = (DAG_executor_state.state,False)
                                                    #continue_queue.put(DAG_executor_state.state)
                                                    continue_queue.put(continue_tuple)
                                                    # Note: put dict of results (input to task) in data_dict -
                                                    # for processes: when we got work from work_queue.
                                                    # for threads: when we generated the output (that became
                                                    #   this input.)
                                                    logger.trace("DAG_executor_work_loop: put unexecutable_leaf_task work in continue_queue:"
                                                        + " state is " + str(DAG_executor_state.state))
                                                else:
                                                    pass # lambdas TBD: call Continue object w/output
                                            else:
                                                # we got work (not -1) from the work queue (a non-leaf task) so break the while(True)
                                                # loop and execute the work task
                                                logger.trace("DAG_executor_work_loop: work from work queue is not an unexecutable_leaf_task,:"
                                                    + " state is " + str(DAG_executor_state.state))
                                                break # while(True) loop
                                        else:
                                            # we got -1 so break the while(True) loop and process the -1 next
                                            break # while(True) loop
                                    else:
                                        pass # finish for groups
                        # end of while(True) get work from work_queue

                        logger.info("**********************DAG_executor_work_loop: withdrawn state for thread: " + thread_name + " :" + str(DAG_executor_state.state))

                        if DAG_executor_state.state == -1:
                            # Note: we are not passing the DAG_executor_state of this 
                            # DAG_executor to tcp_server. We are not using a work_queue
                            # with Lamdas so we are not going to worry about using the
                            # convention that we pass DAG_executor_state in case we want to 
                            # do a restart - again, we wll not be restarting Lambdas due
                            # to blocking on a work_queue get() so we do not pass
                            # DAG_executor_state here. 
                            # Also, this makes the thread and process work_queues have the 
                            # same interface.

                            # Keep track of how many workers have returned and when this equals NUM_WORKERS
                            # then do not add a -1 to the work queue.
                            if not DAG_executor_constants.USING_THREADS_NOT_PROCESSES:
    #brc: counter:
                                # maintain a count of completed workers. Each worker
                                # that gets s -1 from the work_queue will put another
                                # -1 in the work queue if there are workers that have
                                # not received their -1 yet
                                completed_workers = completed_workers_counter.increment_and_get()
                                if completed_workers < DAG_executor_constants.NUM_WORKERS:
                                    logger.info("DAG_executor_work_loop: workers_completed:  " + str(completed_workers)
                                        + " put -1 in work queue.")
                                    # Config: A5, A6
                                    work_tuple = (-1,None)
                                    work_queue.put(work_tuple)
                                else:
                                    logger.info("DAG_executor_work_loop: completed_workers:  " + str(completed_workers)
                                        + " do not put -1 in work queue.")
                            else:
    #brc: counter:
                                completed_workers = completed_workers_counter.increment_and_get()
                                if completed_workers < DAG_executor_constants.NUM_WORKERS:
                                    logger.trace("DAG_executor_work_loop: workers_completed:  " + str(completed_workers)
                                        + " put -1 in work queue.")

                                    # Config: A4_local, A4_Remote
                                    work_tuple = (-1,None)
                                    work_queue.put(work_tuple)
                                else:
                                    logger.trace("DAG_executor_work_loop: completed_workers:  " + str(completed_workers)
                                        + " do not put -1 in work queue.")

    #brc: continue:          If doing incremental DAG and the DAG is not complete, then do not 
                            # return; instead, get a new DAG_info. All workers will call DAG_infobuffer_monitor.withdraw
                            # and receive a new DAG_info. Those with continue tasks in their continue queue will execute 
                            # these tasks instead of getting work from the work queue. Also, presumably their cluster
                            # queues were empty since they they only try to get work from the work queue when their
                            # cluster_queue is empty.

                            if DAG_executor_constants.COMPUTE_PAGERANK and DAG_executor_constants.USE_INCREMENTAL_DAG_GENERATION:
                                if not DAG_info.get_DAG_info_is_complete():
                                    requested_current_version_number = DAG_info.get_DAG_version_number() + 1
                                    # withdraw() returns a new DAG_info. The DAG_infobuffer_monitor object is either
                                    # a Remote_Client_for_DAG_infoBuffer_Monitor or a Local_Client_or_DAG_infoBuffer_Monitor.
                                    # These are wrappers that consime the restart value returned by withdraw() so that 
                                    # here we only get the new DAG_info returned by withdraw(). The Remote wrapper
                                    # also hides all the communication to the tcp_server.
                                    # Note: for work_queue, the Local queue is a queue.Queue so there is no restart
                                    # value that can be returned and hence no wrapper is used.

                                    #new_DAG_info, new_leaf_task_states = DAG_infobuffer_monitor.withdraw(requested_current_version_number)
                                    logger.info("calling withdrawX:")
                                    DAG_info_and_new_leaf_task_states_tuple = DAG_infobuffer_monitor.withdraw(requested_current_version_number)
                                    new_DAG_info = DAG_info_and_new_leaf_task_states_tuple[0]
                                    new_leaf_task_states = DAG_info_and_new_leaf_task_states_tuple[1]
#brc: leaf tasks
                                    logger.trace("DAG_executor_work_loop: cumulative leaf task states withdrawn and added to work_queue: ")
                                    for work_tuple in new_leaf_task_states:
                                        leaf_task_state = work_tuple[0]
                                        logger.trace(str(leaf_task_state))
                                        work_queue.put(work_tuple)

                                    # worker got a new DAG_info and will keep going.
                                    completed_workers = completed_workers_counter.decrement_and_get()
                                    logger.info("DAG_executor_work_loop: after withdraw: workers_completed:  " + str(completed_workers))
                                    # make this explicit
                                    DAG_info = new_DAG_info
                                    # upate DAG_ma and DAG_tasks with their new versions in DAG_info
                                    DAG_map = DAG_info.get_DAG_map()
                                    # number of tasks in the incremental DAG. Not all tasks can 
                                    # be executed if the new DAG is still imcomplete.
                                    DAG_tasks = DAG_info.get_DAG_tasks()
                                    DAG_number_of_tasks = DAG_info.get_DAG_number_of_tasks()

                                    # we have a new DAG_info which means we have a new
                                    # number of tasks to execute (more tasks were added 
                                    # to the incremental DAG). 
                                    if not DAG_executor_constants.USE_PAGERANK_GROUPS_PARTITIONS:
                                        # using partitions.
                                        # If the new DAG is still incomplete, then the last
                                        # partition is incomplete (cannot be executed)
                                        if not DAG_info.get_DAG_info_is_complete():
                                            #num_tasks_to_execute = len(DAG_tasks) - 1
                                            number_of_incomplete_tasks = DAG_info.get_DAG_number_of_incomplete_tasks()
                                            try:
                                                msg = "[Error]: DAG_executor_work_loop:" \
                                                    + " Using incremental DAG generation with partitions and" \
                                                    + " DAG is incomplete but number_of_incomplete_tasks is not 1: " \
                                                    + str(number_of_incomplete_tasks)
                                                assert number_of_incomplete_tasks == 1 , msg
                                            except AssertionError:
                                                logger.exception("[Error]: assertion failed")
                                                if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
                                                    logging.shutdown()
                                                    os._exit(0)
                                            #assertOld:
                                            #if not number_of_incomplete_tasks == 1:
                                            #    logger.error("[Error]: DAG_executor_work_loop:"
                                            #        + " Using incremental DAG generation with partitions and"
                                            #        + " DAG is incomplete but number_of_incomplete_tasks is not 1: "
                                            #        + str(number_of_incomplete_tasks))

                                            # This is a local variabe; each worker has their own 
                                            # num_tasks_to_execute, which could be different since 
                                            # workers may be usng differetn versions of the incremental DAG.
                                            # version i includes all the tasks of earlier version i-1, i-2, etc.
                                            # A later version has 1 or more complete tasks that were incomplete
                                            # in an earlier version.
                                            num_tasks_to_execute = DAG_number_of_tasks - 1 # value of number_of_incomplete_tasks was asserted to be 1
                                            logger.info("DAG_executor_work_loop: after withdraw: DAG_info not complete: new num_tasks_to_execute: " + str(num_tasks_to_execute))
                                        else:
                                            # the new DAG is complete (so is the last incremental DAG
                                            # we will get.) We can execute all the partitions in the 
                                            # DAG. (A DAG is complete if all the graph nodes are in
                                            # some partition.)
                                            #num_tasks_to_execute = len(DAG_tasks)
                                            # This is a local variabe; each worker has their own 
                                            # num_tasks_to_execute, which could be different since 
                                            # workers may be usng differetn versions of the incremental DAG.
                                            # version i includes all the tasks of earlier version i-1, i-2, etc.
                                            # A later version has 1 or more complete tasks that were incomplete
                                            # in an earlier version.
                                            num_tasks_to_execute = DAG_number_of_tasks
                                            logger.info("DAG_executor_work_loop: after withdraw: DAG_info complete new num_tasks_to_execute: " + str(num_tasks_to_execute))
                                    else:
                                        # using groups
                                        if not DAG_info.get_DAG_info_is_complete():
                                            number_of_incomplete_tasks = DAG_info.get_DAG_number_of_incomplete_tasks()
                                            number_of_groups_of_previous_partition_that_cannot_be_executed = DAG_info.get_DAG_number_of_groups_of_previous_partition_that_cannot_be_executed()
                                            # This is a local variabe; each worker has their own 
                                            # num_tasks_to_execute, which could be different since 
                                            # workers may be usng differetn versions of the incremental DAG.
                                            # version i includes all the tasks of earlier version i-1, i-2, etc.
                                            # A later version has 1 or more complete tasks that were incomplete
                                            # in an earlier version.
                                            num_tasks_to_execute = DAG_number_of_tasks - number_of_incomplete_tasks  \
                                                - number_of_groups_of_previous_partition_that_cannot_be_executed
                                            logger.info("DAG_executor_work_loop: after withdraw: DAG_info not complete: new num_tasks_to_execute: " 
                                                + str(num_tasks_to_execute) + " with "
                                                + str(number_of_incomplete_tasks) + " other incomplete tasks"
                                                + " where number_of_groups_of_previous_partition_that_cannot_be_executed: "
                                                + str(number_of_groups_of_previous_partition_that_cannot_be_executed) 
                                                + " and DAG_number_of_tasks: " + str(DAG_number_of_tasks))
                                        else:
                                            #num_tasks_to_execute = len(DAG_tasks)
                                            # This is a local variabe; each worker has their own 
                                            # num_tasks_to_execute, which could be different since 
                                            # workers may be usng differetn versions of the incremental DAG.
                                            # version i includes all the tasks of earlier version i-1, i-2, etc.
                                            # A later version has 1 or more complete tasks that were incomplete
                                            # in an earlier version.
                                            num_tasks_to_execute = DAG_number_of_tasks
                                            logger.trace("DAG_executor_work_loop: after withdraw: DAG_info complete new num_tasks_to_execute: " + str(num_tasks_to_execute))

                                    if continue_queue.qsize() > 0:
                                        # if this worker has incomplete tasks in its continue
                                        # queue (like a leaf task from above, or the last partition
                                        # in an incremental DAG, which is always incomplete) then 
                                        # start by executing those tasks.
#brc: lambda inc: cq.get
                                        #DAG_executor_state.state = continue_queue.get()
                                        # This is stateinfo of continued task

                                        continue_tuple = continue_queue.get()
                                        DAG_executor_state.state = continue_tuple[0]
                                        continued_due_to_TBC = continue_tuple[1]
                                        # indicate whether we are rexecuting a continued_task
                                        continued_task = continued_due_to_TBC
                                        #continued_task = True
                                        if continued_task:
                                            continued_output_for_worker = continue_tuple[2]
                                        else:
                                            continued_output_for_worker = None

                                        #state_info = DAG_map[DAG_executor_state.state]
                                        # The continued task will be executed next.
                                        # We get its state info below.
                                        logger.info("DAG_executor_work_loop: For new DAG_info, continued state: " + str(DAG_executor_state.state)
                                            + " state_info: " + str(DAG_map[DAG_executor_state.state])
                                            + " continued_output_for_worker: " + str(continued_output_for_worker))
#brc: continue

                                        # if there are more continued tasks, we set flag 
                                        # process_continue_queue so that above we will 
                                        # get a task from the continue_queue instead of
                                        # trying to get work from the work_queue or our
                                        # cluster_queue.
                                        if continue_queue.qsize() == 0:
                                            # only one task/state was in continue_queue
                                            process_continue_queue = False
                                        else:
                                            # process tasks/states in continue_queue
                                            logger.trace("DAG_executor_work_loop: XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXwork_loop: continue_queue.qsize()>0 set process_continue_queue = True.")
                                            process_continue_queue = True

                                    else:
                                        logger.info("DAG_executor_work_loop: after withdraw continue queue is empty.")
                                        # We got a new DAG_info, but we have no continued tasks to execute.
                                        # Note that we tried to get work which ultimaely led to us getting
                                        # a new DAG and reaching this point. We have to continued tasks
                                        # to execute so we still need work. Thus, we need to try again
                                        # to get work. Note also that to to get here we have to have an 
                                        # empty cluster_queue too, i.e., we requested work from the work queue so the 
                                        # cluster_queue must have been empty. So when we now try to get 
                                        # more work we will not get it from our cluster_queue (or our
                                        # empty continue_queue)

                                        # Back to the top of the work loop where we will
                                        # try to get work.
                                        continue

                                else: #DAG_info  is complete
                                    logger.trace("DAG_executor_work_loop: DAG_info is_complete so return.")
                                    return
                            else: 
                                # not doing incremental and no more tasks to execute.
                                # worker is returning from work loop so worker will terminate
                                # and be joined by DAG_executor_driver
                                return  

                        # Note: USING_WORKERS is checked above and must be True

                        # old design
                        #worker_needs_input = cluster_queue.qsize() == 0

                        try:
                            msg = "[Error]: DAG_executor_work_loop: cluster_queue.qsize() == 0 was true before" \
                                + " we got work from worker queue but not after - queue size should not change."
                            assert cluster_queue.qsize() == 0 , msg
                        except AssertionError:
                            logger.exception("[Error]: assertion failed")
                            if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
                                logging.shutdown()
                                os._exit(0)
                        #assertOld:
                        # cluster_queue was empty so we got work, which means the ecluster_queue should still be empty.
                        #if not cluster_queue.qsize() == 0:
                        #    logger.error("[Error]: DAG_executor_work_loop: cluster_queue.qsize() == 0 was true before"
                        #        + " we got work from worker queue but not after - queue size should not change.")

    #brc: cluster:
                        # Do not do this
                        #worker_needs_input = False # default

                        # Note: If the continue_queue was empty after we got
                        # the new DAG_info, we executed a continue statement
                        # so we only get here if the contnue_queue was not 
                        # empty and so we have a continue task to execute.
                        #comment out for MM
                        logger.trace("DAG_executor_work_loop: Worker accessed work_queue, then maybe continue_queue: "
                            + "process state: " + str(DAG_executor_state.state))
                        
                    else: # cluster_queue is not empty so worker does not need input
    #brc: cluster:
                        # worker does not need work since there is work
                        # in the cluster queue. Get work from cluster_queue.
                        # Note: Currently, we do not cluster more than one fanout and
                        # we do not cluster faninNB/fanins, and there is only one
                        # collapse task, so the cluster_queue will have only one
                        # work task in it.
                        DAG_executor_state.state = cluster_queue.get()
                        #old design
                        #worker_needs_input = cluster_queue.qsize() == 0
                        logger.info("DAG_executor_work_loop: cluster_queue contains work:"
                            + " got state " + str(DAG_executor_state.state))

    #brc: cluster:
                        try:
                            msg = "[Error]: DAG_executor_work_loop: cluster_queue contained" \
                                + " more than one item of work - queue size > 0 after cluster_queue.get" \
                                + " but we are not using runtime clustering (which adds tasks to the" \
                                + " cluster_queue so there can be more than one task in the cluster_queue.)"
                            assert not (not DAG_executor_constants.ENABLE_RUNTIME_TASK_CLUSTERING and cluster_queue.qsize() > 0), msg
                        except AssertionError:
                            logger.exception("[Error]: assertion failed")
                            if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
                                logging.shutdown()
                                os._exit(0)
                        #assertOld:
                        #if not cluster_queue.qsize() == 0:
                        #    logger.error("[Error]: DAG_executor_work_loop: cluster_queue contained"
                        #        + " more than one item of work - queue size > 0 after cluster_queue.get")

                        logger.info(thread_name + " DAG_executor_work_loop: Worker doesn't access work_queue")
                        logger.info("**********************" + thread_name + " process cluster_queue state: " + str(DAG_executor_state.state))
                    
    #brc: counter
    
                # end not process_continue_queue
                
                # we have a task to execute - we got it from the work queue
                # or the cluster queue or the continue queue.

                incremental_dag_generation_with_groups = DAG_executor_constants.COMPUTE_PAGERANK and DAG_executor_constants.USE_INCREMENTAL_DAG_GENERATION and DAG_executor_constants.USE_PAGERANK_GROUPS_PARTITIONS
                continued_task_state_info = DAG_map[DAG_executor_state.state]
                logger.info(thread_name + " DAG_executor_work_loop: checking whether to inc num tasks executed: incremental_dag_generation_with_groups: "
                    + str(incremental_dag_generation_with_groups)
                    + " continued_task: " + str(continued_task)
                    + " continued_task_state_info.task_name == PR1_1: " + str(continued_task_state_info.task_name == DAG_executor_constants.NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG)
                    + " (not continued_task_state_info.task_name in DAG_info.get_DAG_leaf_tasks(): "
                    + str((not continued_task_state_info.task_name in DAG_info.get_DAG_leaf_tasks()))
                    + " num_tasks_executed *before* inc: " + str(completed_tasks_counter.get()))
                
#brc: lambda inc
                if not (incremental_dag_generation_with_groups and continued_task):
                #if not (incremental_dag_generation_with_groups and continued_task and (
                #    (continued_task_state_info.task_name == NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG or not continued_task_state_info.task_name in DAG_info.get_DAG_leaf_tasks())
                #)):

                    # If this is not a continued group task, we will execute this task and so we increment
                    # the number of tasks that have been excuted. Otherwise we have
                    # already executed the task. This means we will not execute it here
                    # and we should not increment the number of tasks that have been executed.

                    #Note: This increment_and_get executes a memory barrier - so the pagerank writes to 
                    # the shared memory (if used) just performed by this process P1 
                    # will be flushed, which means the downstream pagerank tasks 
                    # that read these values will get the values written by P1.
                    # So we need a memory barrier between a task and its downstream
                    # tasks. Use this counter or if we remove this counter, something 
                    # else needs to provide the barrier.

                    # Increment num tasks executed and see if this is the last 
                    # task to execute. If so, start the termination process or
                    # if we are doing incremental DAG generation (so this is the 
                    # last task in the current version of the DAG)) workers need
                    # to get the next DAG (instead of terminating).

                    num_tasks_executed = completed_tasks_counter.increment_and_get()
                    #logger.trace("DAG_executor_work_loop: " + thread_name + " increment num_tasks_executed, now check if executed all tasks: "
                    #    + " num_tasks_executed: " + str(num_tasks_executed) 
                    #    + " num_tasks_to_execute: " + str(num_tasks_to_execute))

                    # Remember that workers are calling get work and if they get -1 they 
                    # might add another -1 but then they will call withdraw. They
                    # only check num_tasks_executed == num_tasks_to_execute
                    # after they get some work. So if current DAG runs out of work 
                    # they will call withdraw then they try to get more work or process 
                    # their continue queue to get work and then they check 
                    # num_tasks_executed == num_tasks_to_execute. So if first worker
                    # to put -1 in work queue also calls withdraw and gets a new DAG 
                    # and sets num_tasks_to_execute to execute higher then that is okay, since 
                    # other workers must get a -1 and will call withdraw to get a new 
                    # DAG and then call get work or process their continue queue.
                    # That is, condition num_tasks_executed == num_tasks_to_execute
                    # does not affect whether a worker calls get work, it only affects 
                    # whether workers add -1 to the work_queue.
                    
                    if num_tasks_executed == num_tasks_to_execute:
    #brc: stop
                        # Note: This worker has work to do, and this work is the last
                        # task to be executed. So this worker and any other workers
                        # can finish (if the DAG is not incremental or it is incremental
                        # and complete) This worker starts the worker shutdown (or pause
                        # for the new incremental DAG) by putting a -1 in the work queue.
                        # Any worker that is waiting for work or that tries to get more 
                        # work will get this -1. Note that this worker here that is adding
                        # -1 may get this -1 when it tries to get work after executing
                        # this last task. Workers who get -1 from the work queue put
                        # a -1 back in the work queue if there are still workers who
                        # have not completed (i.e., called get work). We have a counter
                        # to track the number of completed (paused) workers.
                        #
                        # Note: No worker calls DAG_infobufer_Monitor.withdraw to get a 
                        # new incrmental DAG until all the tasks in the current version
                        # of the incremental DAG have been executed. This is because,
                        # the workers must first get a -1 from the work queue, at which 
                        # point they may deposit another -1 (if some workers have not
                        # got their -1 yet) and then they will call DAG_infobufer_Monitor.withdraw
                        # instead of returning. Note too that they only call 
                        # DAG_infobufer_Monitor.withdraw if the current version of the 
                        # incremental DAG is not complete, so the withdraw() will return 
                        # a new version of the increnetal DAG. Eventuallly, the DAG will 
                        # be complete and the workers will return (i.e., terminate) instead of 
                        # calling DAG_infobufer_Monitor.withdraw.
                        if not DAG_executor_constants.USING_THREADS_NOT_PROCESSES:
                            # Config: A5, A6
                            logger.info(thread_name + ": DAG_executor: num_tasks_executed == num_tasks_to_execute: depositing -1 in work queue.")
                            work_tuple = (-1,None)
                            # for the next worker
                            work_queue.put(work_tuple)
                        else:
                            # Config: A4_local, A4_Remote
                            logger.trace(thread_name + ": DAG_executor: num_tasks_executed == num_tasks_to_execute: depositing -1 in work queue.")
                            # for the nex worker
                            work_tuple = (-1,None)
                            work_queue.put(work_tuple)
                else:
                    # Note: We did not execute the 
                    #   num_tasks_executed = completed_tasks_counter.increment_and_get()
                    # above, which acts like a barrier when we are using shared
                    # memory. In this case we are not executing the current
                    # continued task, i.e., we have already executed it, so 
                    # it is not writing to shard memory values that will be 
                    # ready by downstream tasks. When we executed it earlier,
                    # i.e., it was a group task that was executed and it had
                    # TBC fanins/fanouts/collapes so it became a continued task
                    # that is being continued here (i.e., we are executing its
                    # fanins/fanouts/collapses) we did execute the above statement 
                    # which acted like a barrier to the downstream tasks of this 
                    # continud task.

                    # We will not excute the task so do not inc num_tasks_executed
                    logger.info("DAG_executor_work_loop: " + thread_name + " before processing " + str(DAG_executor_state.state) 
                        + " did not increment num_tasks_executed for continued task " 
                        + " so num_tasks_executed: " + str(num_tasks_executed) 
                        + " num_tasks_to_execute: " + str(num_tasks_to_execute)
                        + " stays the same.")
            
                    # No return here. A worker may return when it gets a -1
                    # from the work_queue (not here when it puts a -1 in the work queue.)
                    # 
                    # The worker that executes the last task will
                    # add a -1 to the work queue (-1,None) so that the next worker
                    # to try to get work will get a -1. That next worker will add a
                    # -1 to the work queue , etc. Note that the last worker
                    # will not add a -1 to the work_queue.
                    # (The last worker used to also add a -1 to the work queue, so 
                    # the work_queue has a -1 at the end of DAG_execution. We now have 
                    # a counter to count completed workers so that the last worker
                    # does not add a -1 to the work_queue
                    #return
#brc: cluster:
            else: # not using workers: Config: A1. A2, A3
                # if we are using simulated or real lambdas, then the lambda
                # payload has a state to execute. Ths state is added to the 
                # cluster_queue or continue_queue at the start of the work loop. Lambdas
                # may also add collpased states to the cluster queue and fanout/fanin
                # become states are also added to the cluster queue. Real and 
                # simulated lambdas do not use a work_queue, and they do 
                # not use a continue_queue for incremental DAG generation.
                # So they only use a cluster queue. 
                #
                # The cluster_queue may have the lambdas payload task added
                # to it and so may not be empty the first time we check here
                # on the cluster_queue size. We will add this state to the 
                # continue_queue instead of the cluster_queue when this is a
                # lambda that has been (re)started during incremental DAG
                # generation. (That is, a task that was incomplete in the 
                # previous DAG is now complete in the current DAG and can now
                # be executed.) 
                # When processing the tasks, we may
                # or may not add become tasks for fanouts and fanins and 
                # collapsed tasks and clustered tasks to the cluster_queue, 
                # so the cluster_queue may be empty on later checks.
#brc: lambda inc:   
                logger.trace("work_loop: first_iteration_of_work_loop_for_lambda: "
                    + str(first_iteration_of_work_loop_for_lambda))
                # Note: not USING_WORKERS is True
                try:
                    msg = "[Error]: DAG_executor_work_loop:" + " first_iteration_of_work_loop_for_lambda and not continued_task" \
                        + " but cluster_queue.qsize() is 0; the state of a lambda that has" \
                        + " not been restarted for incremental DAG generation should be added" \
                        + " to the cluster_queue at the start of the work loop."
                    assert not (first_iteration_of_work_loop_for_lambda and not (DAG_executor_constants.COMPUTE_PAGERANK and DAG_executor_constants.USE_INCREMENTAL_DAG_GENERATION and continued_task) and cluster_queue.qsize() == 0) , msg
                except AssertionError:
                    logger.exception("[Error]: assertion failed")
                    if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
                        logging.shutdown()
                        os._exit(0)
                #assertOld:
                #if first_iteration_of_work_loop_for_lambda and not (COMPUTE_PAGERANK and USE_INCREMENTAL_DAG_GENERATION and continued_task):
                #    if cluster_queue.qsize() == 0:
                #        logger.trace("[Error]: DAG_executor_work_loop:"
                #            + " first_iteration_of_work_loop_for_lambda and not continued_task"
                #            + " but cluster_queue.qsize() is 0; the state of a lambda that has"
                #            + " not been restarted for incremental DAG generation shoudl be added"
                #            + " to the cluster_queue at the start of the work loop.")
#brc: lambda inc:
                if DAG_executor_constants.COMPUTE_PAGERANK and DAG_executor_constants.USE_INCREMENTAL_DAG_GENERATION and continued_task and continue_queue.qsize() > 0:
                    try:
                        msg = "[Error]: DAG_executor_work_loop:" + " continue_queue.qsize() is > 0 for lambda but not" \
                            + " first_iteration_of_work_loop_for_lambda."
                        assert first_iteration_of_work_loop_for_lambda , msg
                    except AssertionError:
                        logger.exception("[Error]: assertion failed")
                        if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
                            logging.shutdown()
                            os._exit(0)
                    # assertOld: For lambdas, we never put anything in the continue_queue
                    # except the state of a restarted lambda, whcih we add at the start 
                    # of the work loop
                    #if not first_iteration_of_work_loop_for_lambda:
                    #    logger.error("[Error]: DAG_executor_work_loop:"
                    #        + " continue_queue.qsize() is > 0 for lambda but not"
                    #        + " first_iteration_of_work_loop_for_lambda.")

                    # if we fail the asssertion above we will terminate
                    # the program.
                    first_iteration_of_work_loop_for_lambda == False
#brc: lambda inc: cq.get
                    #DAG_executor_state.state = continue_queue.get()
                    continue_tuple = continue_queue.get()
                    DAG_executor_state.state = continue_tuple[0]
                    continued_due_to_TBC = continue_tuple[1]
                    try:
                        msg = "[Error]: For lambda, continued_task is True" + " but continued_due_to_TBC is False."
                        assert continued_due_to_TBC , msg
                    except AssertionError:
                        logger.exception("[Error]: assertion failed")
                        if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
                            logging.shutdown()
                            os._exit(0)
                    # assertOld: continued_due_to_TBC must be True since
                    # continued_task is True for lambda - Since we found continued_task
                    # was true for lambda at the start we added the task to the continue
                    # queue with the continued_due_to_TBC field of the continue_tuple
                    # set to True so it should be True here when we take the tuple out
                    # (immediately after adding the tuple to the continue queue.)
                    #if not continued_due_to_TBC:
                    #    logger.error("[Error]: For lambda, continued_task is True"
                    #        + " but continued_due_to_TBC is False.")

                    continued_task = continued_due_to_TBC
                    logger.info("lambda continued task is true")

                elif cluster_queue.qsize() > 0:
                    first_iteration_of_work_loop_for_lambda == False
#brc: lambda inc: cq.get
                    DAG_executor_state.state = cluster_queue.get()
                    # Question: Do not do this
                    #worker_needs_input = cluster_queue.qsize() == 0
                    dummy_state_info = DAG_map[DAG_executor_state.state]
                    logger.info("DAG_executor_work_loop: simulated or real lambda:"
                        + " cluster_queue contains work: got state " + str(DAG_executor_state.state)
                        + " for task name " + dummy_state_info.task_name)
#brc: cluster:
                    try:
                        msg = "[Error]: DAG_executor_work_loop: cluster_queue contained" \
                            + " more than one item of work - queue size > 0 after cluster_queue.get" \
                            + " but we are not using runtime clustering (which adds tasks to the" \
                            + " cluster_queue so there can be more than one task in the cluster_queue.)"
                        assert not (not DAG_executor_constants.ENABLE_RUNTIME_TASK_CLUSTERING and cluster_queue.qsize() > 0), msg
                    except AssertionError:
                        logger.exception("[Error]: assertion failed")
                        if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
                            logging.shutdown()
                            os._exit(0)
                    #assertOld:
                    #if not cluster_queue.qsize() == 0:
                    #    logger.error("[Error]: DAG_executor_work_loop: simulated or real lambda:"
                    #       + " cluster_queue contained more than one item of work - queue size > 0 after cluster_queue.get")
#brc: lambda inc:
                else:
                    logger.trace("[Error]: DAG_executor_work_loop:"
                        + " cluster_queue and continue_queue are both empty"
                        + " for a lambda but this lambda now has nothing to do"
                        + " and should have terminated at the end of the last iteration"
                        + " of the work loop instead of trying to do another iteration.")
                    logging.shutdown()
                    os._exit(0)

            # while (True) still executing from above - continues next with work to do in the form
            # of DAG_executor_state.state of some task

            # DAG_executor_state.state contains the next state to execute.

            # Note: if we are using lambdas instead of workers, we do not use the
            # continue queue so continued == False. When a lambda excutes a task
            # and finds that it has a fanin/fanout/faninNB/collapse task that is 
            # to-be-continued, the lambda does not enqueue the continued task in the 
            # continue queue. Likewise, lambdas do not use a work_queue - after a lambda
            # excutes its payload task and any become/clustered tasks it gets after that, the 
            # lambda will terminate. (The lambda may start new lambdas for fanouts.)
            # A lambda does not wait for a new incremental DAG either, as we do not
            # want lambdas waiting for anything (no-wait). Instead, the lambda will
            # send the to-be-continued task's state and the task's output, to a synch object 
            # that will start a new lambda to excute the continued task when a new incrmental 
            # DAG becomes available. (Th lambda calls withdraw to get a nwe incemental DAG.
            # If no DAG is available, the task state and output are saved in a queue and the 
            # lambda will terminate when no DAG is returned. BFS will generate a new incremental
            # DAG and call deposit(). In eposit, and enqueued (state,output) tuples are processed
            # by starting a new lambda and giving it the state and output in its payload. Method
            # deposit() will also start new landas for any new leaf tasks in the new DAG. (Leaf tasks
            # are the fitst group/partition of a new connected component that is being searched by 
            # (a new call to) bfs()))

            # If we got a become task when we processed fanouts, we set
            # DAG_executor_state.state to the become task state (that was
            # obtained from the cluster queue) so we
            # will be processing the become state here.
            logger.trace (thread_name + ": DAG_executor_work_loop: access DAG_map with state " + str(DAG_executor_state.state))
            state_info = DAG_map[DAG_executor_state.state]

            #commented out for MM
            #logger.trace("state_info: " + str(state_info))
            logger.trace(thread_name + ": DAG_executor_work_loop: task to execute: " + state_info.task_name 
                + " (though this task may be a continued task that was already executed.)")

            # This task may or may not be a continued task. In either
            # case, we need the inputs for task execution. For worker threads
            # and processes when using partitions, the needed inputs were added to the 
            # data_dict after the previous partition/task was executed, so they are
            # still available in the data_dict as usual.
            #
            # For incremental DAG generation using groups (instead of partitions)
            # we do not execute a continued task unless it is a leaf task
            # that is not the very first group in the DAG. These leaf tasks are
            # groups that start a new connected component. They are added to the 
            # work queue by the DAG generator. When we get such a leaf task from the 
            # work_queue it may not yet be executable (i.e., this leaf task is not in the 
            # current version of the DAG but it will be in the next incremental DAG we get 
            # and then it will be executable), in which case it is added
            # to the continue queue. It then becomes a continued task but unlike normal, non-leaf
            # continued tasksm, it has not been executed before so we need to execute ithe leaf task 
            # as opposed to executing its collapse task, as we do for non-lead tasks. (In general,
            # for continued tasks that are not leaf tasks, we have prviously executed the task and 
            # so when we get it from the continue queue, if we are using partitions, not groups,
            # we in theory need to process its fanins/fanouts/ollapses, but since a partition only
            # has a collapse task which is the next partition, we an "process the fanins/fanouts/collapses"
            # simply by grabbing the collapse task/partittion, which we know is there, and executing
            # this collapse task. When using groups, there may be many fanouts/faninNB, or a faninm or
            # a collapse task so "procssing the fanouts/fanins/collapse" means performing the fanouts, etc.
            # Note: We do not, e.g., put all the fanout tasks of a continued task in the continue queue
            # since we would then be stucj with excuting all of them - it seems better to proces them
            # as normal (e.g., start a lamba for each one, or put them in the work queue) so they are
            # distributed among multiple lambdas/workers, as usual.
            incremental_dag_generation_with_groups = DAG_executor_constants.COMPUTE_PAGERANK and DAG_executor_constants.USE_INCREMENTAL_DAG_GENERATION and DAG_executor_constants.USE_PAGERANK_GROUPS_PARTITIONS

            #logger.trace(thread_name + " DAG_executor_work_loop: incremental_dag_generation_with_groups: "
            #    + str(incremental_dag_generation_with_groups) + " continued_task: "
            #    + str(continued_task) + " DAG_executor_state.state == 1: " + str(DAG_executor_state.state == 1)
            #    + " (not state_info.task_name in DAG_info.get_DAG_leaf_tasks(): "
            #    + str((not state_info.task_name in DAG_info.get_DAG_leaf_tasks())))

            # Note: The first task/group/partition in the DAG is a leaf task
            # PR1_1 and it is never a continued task. The first version of the 
            # incremental DAG always has partitions 1 and 2, or if using groups,
            # group PR1_1 and the groups in partition 2, and partition 2 (and the 
            # groups therein) is to-be-continued but partition 1 is not to-be-contnued.
            # But PR1_1 is in DAG_info.get_DAG_leaf_tasks() and if partition 2 or its
            # groups are to-be-continued then we wil put statwe 1 of PR1_1 in the 
            # continue queue and we may have that state here so we use 
            # state_info.task_name == "PR1_1" to make sure we do not execute PR1_1 again.
            #if incremental_dag_generation_with_groups and continued_task and (
            #    (DAG_executor_state.state == 1 or (not state_info.task_name in DAG_info.get_DAG_leaf_tasks()))
            #):
            logger.trace(thread_name + " DAG_executor_work_loop: checking whether to execute task:"
                + " incremental_dag_generation_with_groups: "
                + str(incremental_dag_generation_with_groups)
                + " continued_task: " + str(continued_task)
                + " is state_info.task_name == NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG: " + str(state_info.task_name == DAG_executor_constants.NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG)
                + " (not state_info.task_name in DAG_info.get_DAG_leaf_tasks(): "
                + str((not state_info.task_name in DAG_info.get_DAG_leaf_tasks())))

#brc: lambda inc
            # Note that group tasks in the continue queue may or may not have 
            # already been excuted. A leaf task that was retrived from the work 
            # queue but was not yet executable is put into the continue queue.
            # Also, a group task that has been executed but that has TBC fanins/fanouts
            # to continued groups is put into the continue queue. We need
            # to know whether a task taken out of the continue queue should 
            # be executed or not. So, when we put a task ni the continue queue
            # we also mark the reason it was put in there, i.e., if it had
            # TBC fanins/fanouts then it does not need to be executed. So
            # above when we take it out of the continue queue, it it was already
            # executed and then added to the continue_queue becuase of its TBC
            # fanin/fanouts, we leave continued_queue as True, so here the task
            # will not be executed again. If the task is a leaf task that has not
            # been executed, then it is not marked as having TBC fanins/fanouts
            # and we will set continue_queue to False, so here this condition 
            # will be False (as it's not "really" a continued group task) and 
            # the leaf task will be executed. This scenario occurs for leaf 
            # task PR6_1 in the white board DAG, it is a leaf task that needs 
            # to be executed thus it is placed in the work queue and then 
            # the continue queue (since it is initially unexecutable) and then 
            # it has TBC fanins/fanouts so it is again placed in the continue
            # queue and should not be excuted again after we get it from the 
            # continue queue.

            # Note: continue_task may be true, telling us that this task is being 
            # continued and thus has already been executed. This next if-statement
            # will set continued_task to False in all of its branches.

            if incremental_dag_generation_with_groups and continued_task:
            #if incremental_dag_generation_with_groups and continued_task and (
            # (state_info.task_name == NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG or not state_info.task_name in DAG_info.get_DAG_leaf_tasks())
            #):

                continued_task = False  # reset flag
#brc: lambda inc:
                # We restart the continued group task with the output that it
                # generated. For a lambda, this output was labeled
                # "input" in its payload. For workers, this output was part of the 
                # tuple we got from the continue queue, Here we assign input to output
                # which we will use when we process the restarted task's
                # fanins/fanouts/collpases (next).

                #if not USING_WORKERS:
                #output = input
                if not DAG_executor_constants.USING_WORKERS:
                    logger.info("here2: state_info.task_inputs: "
                        + str(state_info.task_inputs))
                    output = state_info.task_inputs
                else:
                    logger.info("here2: continued_output_for_worker: "
                        + str(continued_output_for_worker))
                    output = continued_output_for_worker
                # do not execute this group/task since it has been excuted before.
            else:
                # Execute task (but which task?)
                #
                # But first see if we are doing incremental DAG generation and
                # we are using partitions. If so, we may need to get the collapse
                # task of the continued state and excute the collapse task. 
                # 
                # Note: We can also get here if we are doing incremental DAG generation
                # with groups and the task is a leaf task (that is not the first group in the 
                # DAG) that starts a new connected component (which is the first 
                # group generated on any call to BFS()). We will execute the 
                # group/leaf task since it has not ben executed before.
                #
                # Note: The above if-condition will be False if we are executing a leaf
                # task/group that is a new connected component (not "PR1_1"), since
                # when we get the tuple for such a task it will indicate that we should 
                # set continued_task to False, so that we treat the leaf task as a non-continued
                # task that needs to be executed.
                incremental_dag_generation_with_partitions = DAG_executor_constants.COMPUTE_PAGERANK and DAG_executor_constants.USE_INCREMENTAL_DAG_GENERATION and not DAG_executor_constants.USE_PAGERANK_GROUPS_PARTITIONS

                # This is not a continued task that is a group. It may be a continued task that is a 
                # partition or it may be a non-continued leaf task.
                if incremental_dag_generation_with_partitions and continued_task:
                    # it is a continued task that is a partition.
                    continued_task = False  # reset flag
                    # if continued task is the first partition in the DAG (which is a leaf task, but
                    # we have already executed this first leaf task) or it is not a leaf task
                    # then get the collapse task of the continued task. Here we are eseentally 
                    # processing the fanouts/fanins/collpase of the continued partition/task by 
                    # simply grabbing the collapse task, as we know a partition has no fanins/fanouts.
                    # if task T has a collapsed task C then 
                    # the same worker/lambda W that executed T executes C. So we add C to W's
                    # cluster queue and W will execute C. For groups, we execute the task and 
                    # if it is to be continued, we put the state in the continue
                    # queue and when we get it from the continue queue we do its 
                    # fanins/fanouts/collapses. Note that if W executed T and T has 
                    # many fanouts then we do not want to put all of these fanouts in 
                    # W's cluster queue since that would mean W would execute all
                    # the fanout tasks of T. Instead, when W gets the state for T 
                    # from the continue queue W can (skip the execution of T since that
                    # already happened) do T's fanouts as ususal, i.e., W will 
                    # become/cluster one of T's fanouts and put the rest on the shared
                    # worker queue to distribute the fanout tasks amoung the workers, or 
                    # start new lambdas to execute the fanout tasks.
                    #
                    # Example: In the white board DAG, we execute PR1_1, PR2_1L, and PR3_1
                    # and assume we added to the DAG a new connected component PR4_1 that has a collapse
                    # to PR5_1. When we deposit the new DAG with PR4_1, since it is a leaf task, we also add
                    # PR4_1 to the work_queue or start a new lambda to execute it. No task in the current conencte 
                    # component will have a fanout/fanin to PR4_1 since PR4_1 starts a new connected
                    # component; thus, we have to put PR4_1 in the work queue when we detect
                    # it is the start of a new component so Pr4_1 will be executed, or start a new 
                    # lambda to execute it.
                    # When we get PR4_1 from the work_queue, assume
                    # it is unexecutable (not in the current DAG as we did not yet get the new
                    # DAG that has PR4_1 in it, or it is in new DAG but it is to-be-contnued, i.e., we do not
                    # yet know the fanins/fanouts of PR4_1,  then we put
                    # PR4_1 in the continue_queue. When we get a new DAG it will definitely have a 
                    # completed PR4_1 (as we processed PR5_1) so we get PR4_1 from the continue_queue; however,
                    # we did not put state 4 in the continue_queue as a notmal continued state/partition that we
                    # have already executed and that when we get it from the continue queue we will just need
                    # to process its fanouts/fanins/collapse, so we shdoould not grab the collape of partition 
                    # PR4_1. Instead, we execute leaf task "PR4_1". Note: For tuple T in the continue queue,
                    # T[1] is a boolean that indicates whether this is a normal continued task (True) or a leaf
                    # task that has not been executed. T[0] is the state of the task and T[1] is the saved output.
                    # There is no saved output for a leaf task like PR4_1 since PR4_1 has not been executed yet,
                    # i.ee., we aer not really continuing it.
                    if state_info.task_name == DAG_executor_constants.NAME_OF_FIRST_GROUP_OR_PARTITION_IN_DAG or (not state_info.task_name in DAG_info.get_DAG_leaf_tasks()):
                        # task is a leaf task but its the first leaf task in the ADG (PR1_1) or it is not a leaf task.
                        # Process its fanins/fanouts/collapse by grabbing the collapse as there are no fanins/fanouts.
                        # Next we will execute the collapse task, which we now was added to the new incremental DAG.
                        DAG_executor_state.state = DAG_info.get_DAG_states()[state_info.collapse[0]]
                        state_info = DAG_map[DAG_executor_state.state]
                        logger.trace("DAG_executor_work_loop: got state and state_info of continued, collapsed partition for collapsed task " + state_info.task_name)
                    else:
                        # task is a leaf task that is not the first
                        # partition PR1_1 in the DAG. (Leaf tasks start a new connected component.)
                        logger.trace("DAG_executor_work_loop: continued partition  " 
                            + state_info.task_name + " is a leaf task so do not get its collapse task"
                            + " instead, execute the leaf task.")
                else:
                    try:
                        msg = "[Error]: continued_task is true for a leaf task that" \
                            + " is not the first leaf task/partition/group in the DAG (PR!_1)."
                        assert not continued_task , msg
                    except AssertionError:
                        logger.exception("[Error]: assertion failed")
                        if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
                            logging.shutdown()
                            os._exit(0)
                    # assertOld: continued_task should be set to False if we get a tuple from the continue_queue
                    # for a leaf task that is not the first leaf task/partition/grup PR1_1 in the DAG.
                    #if continued_task:
                    #    logger.error("[Error]: continued_task is true for a leaf task that"
                    #        + " is not the first leaf task/partition/group in the DAG (PR!_1).")
                    #   logging.shutdown()
                    #    os._exit(0)

                    # if continued tasks is TRUE this will set it to False
                    continued_task = False
                    
# ------------------------------------------------------------------
# ------------------------------------------------------------------
# TASK EXECUTION
# ------------------------------------------------------------------
# ------------------------------------------------------------------

                # Example:
                # 
                # task = (func_obj, "task1", "task2", "task3")
                # func = task[0]
                # args = task[1:] # everything but the 0'th element, ("task_id1", "taskid2", "taskid3")
                #
                # # Intermediate data; from executing other tasks.
                # # task IDs and their outputs
                # data_dict = {
                #     "task1": 1, 
                #     "task2": 10,
                #     "task3": 3
                # }
                #
                # args2 = pack_data(args, data_dict) # (1, 10, 3)
                # func(*args2)

                # A tuple of input task names, not actual inputs. The inputs retrieved from data_dict,
                # Lambdas need to put payload inputs in data_dict then get them from data_dict.
                # Note: For Lambdas, we retrieve the task inputs from the payload and pass them
                # to the work loop in state_info.task_inputs. So all of the versions, using the lambdas
                # and not using the lambdas, leaf nodes get their task inputs from state_info.task_inputs.
                # These inputs for non-lambda versions are part of the DAG_info, which is read from a file
                # at the start of each thread/process. For lambdas, we grab the task inputs for leaf
                # nodes in the DAG_executor_driver and then null out the list of leaf task inputs in DAG_info
                # and the leaf task inputs in state_info.task_inputs for each leaf task (which is also in 
                # DAG_info) We do this since for lambdas we pass DAG_info in the payload and we don't want to 
                # pass all those leaf task inputs in DAG_info in each payload.
                #
                # For example: 
                # leaf task inc0: task_inputs: (1,)
                # non-leaf task add: task_inputs: 
                #   ('increment-1cce17d3-5948-48d6-9141-01a9eaa1ca40', 'increment-ed04c96b-fcf6-4fab-9d13-d0560be0ef21')
                task_inputs = state_info.task_inputs    
                is_leaf_task = state_info.task_name in DAG_info.get_DAG_leaf_tasks()
                logger.trace("is_leaf_task: " + str(is_leaf_task))
                logger.trace("task_inputs: " + str(task_inputs))

                # we have already executed continued_tasks and put their outputs
                # in the data_dict

#brc: continue - OLD
                #if not continued_task:
#brc: continue - old

                #brc: Some DAGs, e.g., pagerank, may use a single paramaterized function, 
                # e.g., PageRank, that needs its inputs, which are captured by args, but also 
                # its task_name, so it e.g., can input values for its specific task, 
                # e.g., its partition in file task_name+".pickle".

                """
                def pack_data(o, d, key_types=object):
                    #Merge known data into tuple or dict

                    Parameters
                    ----------
                    o:
                        core data structures containing literals and keys
                    d: dict
                        mapping of keys to data

                    Examples
                    --------
                    >>> data = {'x': 1}
                    >>> pack_data(('x', 'y'), data)
                    (1, 'y')
                    >>> pack_data({'a': 'x', 'b': 'y'}, data)  # doctest: +SKIP
                    {'a': 1, 'b': 'y'}
                    >>> pack_data({'a': ['x'], 'b': 'y'}, data)  # doctest: +SKIP
                    {'a': [1], 'b': 'y'}
                    
                    typ = type(o)
                    try:
                        if isinstance(o, key_types) and str(o) in d:
                            return d[str(o)]
                    except TypeError:
                        pass

                    if typ in (tuple, list, set, frozenset):
                        return typ([pack_data(x, d, key_types=key_types) for x in o])
                    elif typ is dict:
                        return {k: pack_data(v, d, key_types=key_types) for k, v in o.items()}
                    else:
                        return o

                        # Example:
                        # 
                        # task = (func_obj, "task1", "task2", "task3")
                        # func = task[0]
                        # args = task[1:] # everything but the 0'th element, ("task1", "task2", "task3")

                        # # Intermediate data; from executing other tasks.
                        # # task IDs and their outputs
                        # data_dict = {
                        #     "task1": 1, 
                        #     "task2": 10,
                        #     "task3": 3
                        # }

                        # args2 = pack_data(args, data_dict) # (1, 10, 3)

                        # func(*args2)
                """
                # Note: For DAG generation, for each state we execute a task and 
                # for each task T we have t say what T's task_inputs are - these are the 
                # names of tasks that give inputs to T. When we have per-fanout output
                # instead of having the same output for all fanouts, we specify the 
                # task_inputs as "sending task - receiving task". So a sending task
                # S might send outputs to fanouts A and B so we use "S-A" and "S-B"
                # as the task_inputs, instad of just using "S", which is the Dask way.
                result_dictionary =  {}
                if not is_leaf_task:
                    logger.trace("Packing data for non-leaf task. Task inputs: %s. Data dict (keys only): %s" % (str(task_inputs), str(data_dict.keys())))
                    # task_inputs is a tuple of task_names
                    args = pack_data(task_inputs, data_dict)
                    logger.trace(thread_name + " argsX: " + str(args))
                    if DAG_executor_constants.TASKS_USE_RESULT_DICTIONARY_PARAMETER:
                        # task_inputs = ('task1','task2'), args = (1,2) results in a resultDictionary
                        # where resultDictionary['task1'] = 1 and resultDictionary['task2'] = 2.
                        # We pass resultDictionary of inputs instead of the tuple (1,2).

                        if len(task_inputs) == len(args):
                            result_dictionary = {task_inputs[i] : args[i] for i, _ in enumerate(args)}
                            logger.trace(thread_name + " result_dictionaryX: " + str(result_dictionary))
                    
                    """
                    # This might be useful for leaves that have more than one input?
                    # But leaves always have one input, e.g., (1, )?
                    if TASKS_USE_RESULT_DICTIONARY_PARAMETER:
                        logger.trace("Foo2a")
                        # ith arg has a key DAG_executor_driver_i that is mapped to it
                        # leaf tasks do not have a task that sent inputs to the leaf task,
                        # so we create dummy input tasks DAG_executor_driver_i.
                        task_input_tuple = () # e.g., ('DAG_executor_driver_0','DAG_executor_driver_1')
                        j = 0
                        key_list = []
                        for _ in args:
                            # make the key values in task_input_tuple unique. 
                            key = "DAG_executor_driver_" + str(j)
                            key_list.append(key)
                            j += 1
                        task_input_tuple = tuple(key_list)
                        # task_input_tuple = ('DAG_executor_driver_0'), args = (1,) results in a resultDictionary
                        # where resultDictionary['DAG_executor_driver_0'] = 1.
                        # We pass resultDictionary of inputs to the task instead of a tuple of inputs, e.g.,(1,).
                        # Lengths will match since we looped through args to create task_input_tuple
                        logger.trace(thread_name + " args: " + str(args)
                            + " len(args): " + str(len(args))
                            + " len(task_input_tuple): " + str(len(task_input_tuple))
                            + " task_input_tuple: " + str(task_input_tuple))
                        if len(task_input_tuple) == len(args):
                            # The enumerate() method adds a counter to an iterable and returns the enumerate object.
                            logger.trace("Foo2b")
                            result_dictionary = {task_input_tuple[i] : args[i] for i, _ in enumerate(args)}
                            logger.trace(thread_name + " result_dictionaryy: " + str(result_dictionary))
                        #Note:
                        #Informs the logging system to perform an orderly shutdown by flushing 
                        #and closing all handlers. This should be called at application exit and no 
                        #further use of the logging system should be made after this call.
                        logging.shutdown()
                        #time.sleep(3)   #not needed due to shutdwn
                        os._exit(0)
                    """
                else:
                    logger.trace("for leaf task args is task_inputs.")
                    # if not triggering tasks in lambdas task_inputs is a tuple of input values, e.g., (1,)
                    if not (DAG_executor_constants.STORE_SYNC_OBJECTS_IN_LAMBDAS and DAG_executor_constants.SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS):
                        args = task_inputs
                    else:
                        # else the leaf task was triggered by a fanin operation on the fanin object
                        # for the leaf task and the fanin result, as usual, maps the task that
                        # sent the input to the input. For leaf tasks, the task that sent the 
                        # input is "DAG_executor_driver", which is not a real DAG task. So
                        # we have to grab the value that DAG_executor_driver is mapped to,
                        # which is the leaf task input. The only input to a leaf task is this
                        # input. Note: When triggering tasks, the DAG_executor_driver calls
                        # the tcp_server_lambda to invoke a fanout, like any other triggered
                        # fanout, using process_leaf_tasks_batch.
                        # args will be a tuple of input values, e.g., (1,), as usual
                        args = task_inputs['DAG_executor_driver']
                        logger.trace(thread_name + " argsY: " + str(args))

                    if DAG_executor_constants.TASKS_USE_RESULT_DICTIONARY_PARAMETER:
                        # Passing an empty input tuple to the PageRank task,
                        # This results in a result_dictionary
                        # of "DAG_executor_driver_0" --> (), where
                        # DAG_executor_driver_0 is used to mean that the DAG_excutor_driver
                        # provided an empty input tuple for the leaf task. In the 
                        # PageRank_Function_Driver we just ignore empty input tuples so 
                        # that the input_tuples provided to the PageRank_Function will be an empty list.
                        result_dictionary['DAG_executor_driver_0'] = ()

                logger.trace("argsZ: " + str(args))

                logger.trace(thread_name + " result_dictionaryZ: " + str(result_dictionary))

#brc: cleanup
                #for key, value in DAG_tasks.items():
                #    logger.trace(str(key) + ' : ' + str(value))

                # using map DAG_tasks from task_name to task
                task = DAG_tasks[state_info.task_name]

#brc: num_nodes: 
                # num_nodes_in_graph is saved when we input the 
                # graph n BFS.input_graph() and the saved value
                # is added to DAG_info when bfs() generates 
                # DAG_info (BFS_generate_DAG_info.py and the 
                # "..._incemental_groups.py" and "..._incremental_partitions.py"
                # incremental versions.
                num_nodes_in_graph = DAG_info.get_DAG_num_nodes_in_graph()
                logger.trace("execute task: num_nodes_in_graph: " + str(num_nodes_in_graph))

                if not DAG_executor_constants.TASKS_USE_RESULT_DICTIONARY_PARAMETER:
                    # we will call the task with tuple args and unfold args: task(*args)
                    output = execute_task(task,args)
                else:
                    #def PageRank_Function_Driver_Shared(task_file_name,total_num_nodes,results_dictionary,shared_map,shared_nodes):
                    if not DAG_executor_constants.USE_SHARED_PARTITIONS_GROUPS:
                        # we will call the task with: task(task_name,resultDictionary)
#brc: groups partitions
                        #output = execute_task_with_result_dictionary(task,state_info.task_name,20,result_dictionary)
                        logger.info("execute task: " + state_info.task_name)
                        output, result_tuple_list = execute_task_with_result_dictionary(task,state_info.task_name,num_nodes_in_graph,result_dictionary,
                            groups_partitions)
                    else:
                        if DAG_executor_constants.USE_PAGERANK_GROUPS_PARTITIONS:
                            output, result_tuple_list = execute_task_with_result_dictionary_shared(task,state_info.task_name,num_nodes_in_graph,result_dictionary,BFS_Shared.shared_groups_map,BFS_Shared.shared_groups)
                        else: # use the partition partitions
                            output, result_tuple_list = execute_task_with_result_dictionary_shared(task,state_info.task_name,num_nodes_in_graph,result_dictionary,BFS_Shared.shared_partition_map,BFS_Shared.shared_partition)

                    # save outputs or result so we can check them after execution.
                    # Outputs are the values sent to other partitions/groups,
                    # not the pagerank values for each node. The outputs
                    # can be empty since a partition/group can have no 
                    # fanouts/fanins/faninNBs/collapses.
                    # Results are the pagerank values that are computed, one
                    # per node in the partition/group.
                    #
                    # At the end of execution, we wil lcheck whether an output/result
                    # was set for each task (partition/group) and print the outputs/results.
                    #
                    # CHECK_PAGERANK_OUTPUT will only be true if:
                    # we are computing pagerank and RUN_ALL_TASKS_LOCALLY (i.e., not using real lambdas) 
                    # and we are using workers or simulatng lambdas with threads.
                    # In these cases, we can save the results locally in a shared
                    # map, i.e., shared/accessed by worker threads or the threads 
                    # simulating lambdas. When using processes or rel lambdas we woould
                    # have to sav the results/outputs remotely. 
                    # We might want to do that, i.e., create a map synchronzation
                    # object stored on the tcp_server and send the outputs/results to it.
                    if DAG_executor_constants.CHECK_PAGERANK_OUTPUT:
                        # We can save the task's outputs or the pagerank values
                        # it computed, for debugging.
                        #set_pagerank_output(DAG_executor_state.state,output)
                        set_pagerank_output(DAG_executor_state.state,result_tuple_list)
      
                """ where:
                    def execute_task(task,args):
                        logger.trace("input of execute_task is: " + str(args))
                        output = task(*args)
                        return output
                """
                """
                    def execute_task_with_result_dictionary(task,task_name,resultDictionary):
                        output = task(task_name,resultDictionary)
                        return output
                """
                """
                    def execute_task_with_result_dictionary_shared(task,task_name,total_num_nodes,resultDictionary,shared_map, shared_nodes):
                        output = task(task_name,total_num_nodes,resultDictionary,shared_map,shared_nodes)
                        return output
                """
                """
                output is a dictionary mapping task name to list of tuples.
                output = {'PR2_1': [(2, 0.0075)], 'PR2_2': [(5, 0.010687499999999999)], 'PR2_3': [(3, 0.012042187499999999)]}
                This is the PR1_1 output. We put it in the data_dict as
                    data_dict["PR1_1"] = output
                but we then send this output to process_fanouts. The become
                task is PR2_1 and the non-become task is PR_2_3. In general,
                we fanout all non-become tasks with this same output. We should
                actually fanout tasks with their indivisual output from the 
                dictionary. Currently, for each name in set fanouts:
                    dict_of_results =  {}
                    dict_of_results[calling_task_name] = output
                    work_tuple = (DAG_states[name],dict_of_results)
                    work_queue.put(work_tuple)
                which assigns dict_of_results[calling_task_name] = output.
                But we want to assign instead:
                    dict_of_results[str(calling_task_name+"-"+name)] = output[name]
                when we have per-fanout outputs instead of all fanouts get the 
                same output. 
                Also, we currently have:
                    sender_set_for_senderX = Group_receivers.get(senderX)
                    if sender_set_for_senderX is None:
                        # senderX is a leaf task since it is not a receiver
                        Group_DAG_leaf_tasks.append(senderX)
                        Group_DAG_leaf_task_start_states.append(state)
                        task_inputs = ()
                        Group_DAG_leaf_task_inputs.append(task_inputs)
                    else:
                        # sender_set_for_senderX provide input for senderX
                        task_inputs = tuple(sender_set_for_senderX)
                So the task_input for fanout PR2_3 is ("PR1_1) since PR1_1 sends
                its output to PR2_3. And we put the output in the data_dict
                as data_dict["PR1_1"] = output so we'll be grabbing the 
                entire output as the input of PR2_1.
                Note: process_fanouts put a work tuple for PR2_1 in the work 
                queue and set the DAG_executor_state.state to the become
                task state, so the non-leaf executor, will next do the 
                faninNB and then do the become task.
                Note: The ss1 excutor will get the work_tuple for PR2_3
                and try to execute it. The inputs are "PR1_1" since PR1_1
                sends it output to PR2_3. Using the data_dict["PR1_1"] value
                the input for PR2_3 is the entire output of PR1_1.

                So: the output of a PageRank task is a dictionary that maps
                each of its fanout/faninNB/collapse/fanin tasks to a list if
                input tuples. We need to divide the output into one output
                per fanout/faninNB/collapse/fanin task. These outputs are
                keyed by a string, e.g., "PR1_1-PR2_3", and this string 
                needs to be used in the DAG_info task_inputs tuple.
                """

                # data_dict may be local (A1) to process/lambda or global (A2) to threads
                logger.trace(thread_name + " executed task " + state_info.task_name + "'s output: " + str(output))

                
                if DAG_executor_constants.SAME_OUTPUT_FOR_ALL_FANOUT_FANIN:
                    data_dict[state_info.task_name] = output
                else:
                #   Example: task PR1_1 producs an output for fanouts PR2_1
                #   and PR2_3 and faninNB PR2_2.
                #       output = {'PR2_1': [(2, 0.0075)], 'PR2_2': [(5, 0.010687499999999999)], 'PR2_3': [(3, 0.012042187499999999)]}
                    for (k,v) in output.items():
                        # example: state_info.task_name = "PR1_1" and 
                        # k is "PR2_3" so data_dict_key is "PR1_1-PR2_3"
                        data_dict_key = str(state_info.task_name+"-"+k)
                        # list of input tuples. Example: list of single tuple:
                        # [(3, 0.012042187499999999)], hich says that the pagerank
                        # value of the shadow_node in position 3 of PR2_3's 
                        # partition is 0.012042187499999999. This is the pagerank
                        # value of a parent node of the node in position 4 of 
                        # PR2_3's partition. We set the shadow nodes's value before
                        # we start the pagerank calculation. There is a trick used
                        # to make sure the hadow node's pageran value is not changed 
                        # by the pagerank calculation. (We compute the shadow node's 
                        # new paerank but we hardcode the shadow node's (dummy) parent
                        # pagerank vaue so that the new shadow node pagerank is he same 
                        # as the old value.)
                        data_dict_value = v
                        data_dict[data_dict_key] = data_dict_value

                logger.trace("data_dict: " + str(data_dict))
                logger.trace("output: " + str(output))

                # Can use this sleep to make a thread last to call FaninNB - adjust the state in which you want
                # the call to fan_in to be last. Last caller can get the faninNB task work, if it has 
                # worker_needs_input = True on call (so the state has no fanouts, as thread will be become
                # task for first fanout so that thread will not need work from its FamInNBs.)
                #if DAG_executor_state.state == 8:
                #    time.sleep(0.5)

            # If len(state_info.fanouts) > 0 then we will make one 
            # fanout task a become task and remove this task from
            # fanouts. Thus, starting_number_of_fanouts allows to 
            # remember whether we will have a become task. If
            # we are using threads to simulate lambdas and real lambdas
            # then we should not return after processing the fanouts
            # and faninNBs since we can continue and execure the 
            # become task. The check of starting_number_of_fanouts
            # is below.
            starting_number_of_fanouts = len(state_info.fanouts)

            #brc: continue
            """
            OLD --> DELETE
            if COMPUTE_PAGERANK and USE_INCREMENTAL_DAG_GENERATION and (
                state_info.ToBeContinued):
                if process_continue_queue:
                    logger.error("[Error]: DAG_executor work loop: process_continue_queue but"
                        +  " we just found a To Be Continued State, i.e., the state was"
                        +  " continued previously and in procesing it after getting a new"
                        +  " DAG_info the state is still To Be Continued (in the new DAG_info.")

                # if the DAG_info is not complete, we execute a continued
                # task and put its output in the data_dict but we do not 
                # process its fanout/fanins/faninNBs until we get the next
                # DAG_info. Put the task's state in the continue_queue to
                # be continued when we get the new DAG_info

            #brc: continue - finish for Lambdas
                if USING_WORKERS:
#brc: lambda inc: cq.put
                    # Truly a continued task (i.e., with TBC fanins/fanouts/collpases)
                    continue_tuple = (DAG_executor_state.state,True)
                    continue_queue.put(continue_tuple)
                else:
                    pass # lambdas TBD
            """
# ------------------------------------------------------------------
# ------------------------------------------------------------------
# AFTER TASK EXECUTION - process fanout/faninNB/fanin/collapse
# ------------------------------------------------------------------
# ------------------------------------------------------------------

            #Note: above we either executed the task T or it was a continued task and we skipped
            # the execution of T since T was executed before. If T was a continued task, then 
            # we know that in the new DAG its fanins/fanouts/collapse are complete and we can 
            # now process them. If T was not a continued task so T was executed, then T may 
            # be incomplete, i.e., its fanouts/fanins/collapse are to be continued (which means that 
            # state_info.fanout_fanin_faninNB_collapse_groups_partitions_are_ToBeContinued is True) 
            # so we cannot process T's fanouts/fanins/collapse. In this case, T becomes a contiued
            # task and we deal with T accordingly.
            #logger.trace("output2: " + str(output))
            if not DAG_executor_constants.USING_WORKERS and DAG_executor_constants.COMPUTE_PAGERANK and DAG_executor_constants.USE_INCREMENTAL_DAG_GENERATION and (
                DAG_executor_constants.USE_PAGERANK_GROUPS_PARTITIONS and state_info.fanout_fanin_faninNB_collapse_groups_partitions_are_ToBeContinued
            ):
                # This task now becomes a continued task. We are not using workers, i.e., we are using 
                # lamdas, so we do not put T in the continue queue. Instead we try to get a new 
                # incremental DAG. If T is in the new DAG then T's fanouts/fanins/collpase are complete
                # and we can process the,. Otherwise, this lambda will save T and its output to a 
                # tcp_server object and terminate.
                
#brc: lambda inc:
                logger.info("DAG_executor: work loop: after task execution, lambda"
                    + " checks for new DAG if TBC and if TBC and no new DAG then stops.")

                logger.trace("DAG_executor: Work_loop: lambda try to get new incremental DAG for lambda.")

                # Try to get a new incremental DAG. if we do not get one, we will 
                # terminate. When a new DAG is generated a lambda will be 
                # (re)started to excute the fanins/fanouts/collpases of this
                # group G. Otherwise, we got a new DAG in which G is a
                # completed group so we can finish processing G by executing
                # below its fanins/fanouts/collases. 
                # Note: we need to get this new DAG before we excute the 
                # next if-statement. This if-statement is the one that 
                # executes G's fanins/fanouts/collpases.
#brc: lambda inc:     
                # Object DAG_infobuffer_monitor could be one of 
                # several classes, all of which have deposit and withdraw
                # methods for incremental DAG generation. This object is
                # created before iterating the work loop.
                requested_current_version_number = DAG_info.get_DAG_version_number() + 1
                logger.trace("DAG_executor: call withdraw.")
                logger.info("type is " + str(type(DAG_infobuffer_monitor)))
                logger.info("before call witdraw: output is: " + str(output))
                new_DAG_info = DAG_infobuffer_monitor.withdraw(requested_current_version_number,DAG_executor_state.state,output)
                logger.info("DAG_executor: back from withdraw.")
                if new_DAG_info is not None:
                    logger.trace("DAG_executor: got new incremental DAG for lambda returned by withdraw().")
                    DAG_info = new_DAG_info
                    # update DAG_map and DAG_tasks with their new versions in the new DAG_info
                    DAG_map = DAG_info.get_DAG_map()
                    # The new version of state_info will have the completed fanins/fanouts/collapse for this task
                    state_info = DAG_map[DAG_executor_state.state]
                    # number of tasks in the incremental DAG. Not all tasks can 
                    # be executed if the new DAG is still imcomplete.
                    DAG_tasks = DAG_info.get_DAG_tasks()
                    DAG_number_of_tasks = DAG_info.get_DAG_number_of_tasks()
                else:
                    logger.info("DAG_executor: no new incremental DAG for lambda returned by withdraw().")
                    # Note: we are going to execute the next if-statement. Its condition
                    # will be True (It is the same condition as this if statement and nothing has
                    # changed, i.e., in particular state_info has not changed) and since USING_WORKERS 
                    # is False, ths lambda will return. So we could just return here.
                    pass 
                    # We did not get a new incremental DAG. this means that 
                    # state_info was not changed so we will be using the 
                    # same state_info as above and 
                    # state_info.fanout_fanin_faninNB_collapse_groups_partitions_are_ToBeContinued
                    # is True in the condition of the next if-statement.
                    # This means we will return/terminate as we have nothing
                    # we can do (i.e., we cannot execute this groups fanins/fanouts
                    # so we can stop.)
            else:
                if not DAG_executor_constants.USING_WORKERS and DAG_executor_constants.COMPUTE_PAGERANK and DAG_executor_constants.USE_INCREMENTAL_DAG_GENERATION and (
                    DAG_executor_constants.USE_PAGERANK_GROUPS_PARTITIONS
                ):
                    # this task is complete, i.e., it has completed fannis/fanouts/collpase so we will
                    # process the fanins/fanouts/collapse next.
                    logger.trace("DAG_executor: Lambda does NOT try to get new incremental DAG for lambda.")
                    #logging.shutdown()
                    #os._exit(0)
            
            #logger.info("output3: " + str(output))
            if (DAG_executor_constants.COMPUTE_PAGERANK and DAG_executor_constants.USE_INCREMENTAL_DAG_GENERATION and (
                    DAG_executor_constants.USE_PAGERANK_GROUPS_PARTITIONS and state_info.fanout_fanin_faninNB_collapse_groups_partitions_are_ToBeContinued)
                ):
                # Group has fanouts/fanins/faninNBs/collapses that are TBC
                # so workers will put this state in the continue_queue and lambdas
                # will terminate. 
                # Note that the lambda called withdraw() above and passed 
                # the task sate and output in case no new DAG was available in which case 
                # the (state,output) will have been saved by withdraw(), When we get a new DAG
                # we will get this state from the continue queue. We have already
                # executed this state, so we will skip task execution and do 
                # the continued fanouts/fanins/faninNBs/collapses. Note that when
                # we get a new DAG that this group that had continued 
                # fanouts/fanins/faninNBs/collapses now is complete, i.e., it has
                # no continued fanouts/fanins/faninNBs/collapses.
                # Note: we save the output in the tuple so we'll have it
                # when we continue processing the task's fanins/fanouts.
                if DAG_executor_constants.USING_WORKERS:
                    #continue_queue.put(DAG_executor_state.state)
                    #logger.trace("DAG_executor_work_loop: put TBC collapsed work in continue_queue:"
                    #    + " state is " + str(DAG_executor_state.state))
#brc: lambda inc: cq.put                    
                    # True --> Truly a continued task (i.e., with TBC fanins/fanouts/collpases)
                    continue_tuple = (DAG_executor_state.state,True,output)
                    #continue_queue.put(DAG_executor_state.state)
                    logger.info("output4: " + str(output))
                    continue_queue.put(continue_tuple)
                    logger.info("DAG_executor_work_loop: put TBC collapsed work in continue_queue:"
                        + " state is " + str(DAG_executor_state.state))
                else:
#brc: lambda inc:
                    logger.info("DAG_executor_work_loop: group has TBC fanins/fanouts and we did not"
                        + " get a new incremental DAG on withdraw() so lambda returns/terminates.")
                    # no new DAG yet so with nothing to do the lambda returns/terminates
                    return
            
            # Notes on cluster queue and clustering:
            # Note: if this just executed task T has a (non to be continued) collapse task then T has no
            # fanouts/fanins/faninNBs, so next we will execute the clustered task
            # with the result/output just placed in the data_dict.
            # Note: if the just executed task T has multiple fanouts and it clusters them
            # then the fanouts will be added to the cluster queue, i.e., their state
            # returned by DAG_info.get_DAG_states() will be added to the cluster queue.
            # We were going to add the fanouts to the work queue (minus the become
            # task) or start real lambdas to execute them, but clustered them instad. 
            # We add the becomre task's state to the cluster queue, just like a collapsed task. 
            # Notice that the input for
            # the become task (a fanout task) will be retieved from the data_dict, i.e., 
            # after T was excuted, we saved its output in the data dict so we know it
            # is there (in our local data dict if we are a process or lambda or the 
            # global data dict if we are a thread.) So we only put the state in the 
            # cluster queue, not the output of T since the output of T is in the data dict.
            # For clustered fanouts, we can also only put the state in the cluster queue 
            # since the output we need to execute them is T's output and it is in our
            # data dict.
            # Note: A fanin task can also be a become task. So we  add
            # such become tasks to the cluster queue, in the same way. Note that 
            # a fanin op returns all the results that were sent to the fanin and we 
            # put them in the data dict when the fanin op returns. So they will be
            # there when we execute the fanin task. That is, when we add the fanin 
            # task to the cluster queue it may be behind one or more clustered tasks
            # but when we eventually execute, we will have its inputs in our data dict.

# ------------------------------------------------------------------
# ------------------------------------------------------------------
# COLLAPSE
# ------------------------------------------------------------------
# ------------------------------------------------------------------

            elif len(state_info.collapse) > 0:

                try:
                    msg = "[Error]: DAG_executor_work_loop:" \
                        + " state has a collapse but also fanins/fanouts."
                    assert not(len(state_info.fanins) + len(state_info.fanouts) + len(state_info.faninNBs) > 0) , msg
                except AssertionError:
                    logger.exception("[Error]: assertion failed")
                    if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
                        logging.shutdown()
                        os._exit(0)
                #assertOld:
                #if len(state_info.fanins) + len(state_info.fanouts) + len(state_info.faninNBs) > 0:
                #    # a state with a collapse has no other fanins/fanuts.
                #    logger.error("[Error]: DAG_executor_work_loop:"
                #        + " state has a collapse but also fanins/fanouts.")

                # Note If we run-time cluster, then we may have work in the cluster queue.
                # We can put this collapse work in the cluster queue too. If the cluster queue is
                # empty, then we'll just execute the collapsed task next; otherwise, we will add
                # the collapsed task at the end of the cluster queue and excute it after all the 
                # clustered (e.g., fanout) work.

                # We will execute collapsed task next (i.e., put it in the cluster queue and on the 
                # next itertation of the work loop since the cluster queue is not empty, get the 
                # collpased task from the cluster queue, unless there are clustered (fanout) tasks 
                # at the front of the queue in which case we will excute them first.)
                # collapse is a list [] so get task name of the collapsed task which is collapse[0],
                # the only name in this collapse list. Note: DAG_states() is a map from 
                # task name to an int that can be used as a key in DAG_map to get the task's state_info, 
                # which has all the information 
                # about the task - it's fanins/fanouts/collapses, python function, whether it is
                # complete or not (for incremental DAG generation), etc.
                
                # DAG_executor_state.state is an int representing the the (current) state/task that has a 
                # collapsed partition/group. state_info is the state info
                # of the state with the collapse task.
#brc: continue 
#brc: lambda inc:               
                # The above if before the current elif if True if we are using groups and the current group/task
                # has to-be-continued fanins/fanouts/collapse. In that case, the worker put the task/group in the 
                # continue queue or the lambda returned? So since we got past that if, the curren task is a group 
                # that has has no TBC or we are using partitions or we are not doing incremental DAG generation.

                # Determine whch case we have: if this if is true then either we are not doing incremental
                # DAG gneration or we are doing incremental DAG generation with groups and the current
                # group is complete so we can process the collapse. If this if 
                # is False, then we are using partitions and the current partition/task may or may not 
                # be complete. If it is complete, we can process the collapse task; otherwise, we put the 
                # *current* partition/task in the continue queue, not the collapse task. When we get the
                # current task out of the continue_queue, we process is fanouts/fanins/collapse by 
                # grabbing its collapse (partitions have no fanins/fanouts). We will execute the collapsed
                # task using the output of the current task as the input to the collapse task.
                if not(DAG_executor_constants.COMPUTE_PAGERANK and DAG_executor_constants.USE_INCREMENTAL_DAG_GENERATION) or (DAG_executor_constants.COMPUTE_PAGERANK and DAG_executor_constants.USE_INCREMENTAL_DAG_GENERATION and DAG_executor_constants.USE_PAGERANK_GROUPS_PARTITIONS):
#brc: cluster:
                    # get the state of the collapsed partition/group (task)
                    # and put the collapsed task in the cluster_queue for execution.
                    DAG_executor_state.state = DAG_info.get_DAG_states()[state_info.collapse[0]]
                    cluster_queue.put(DAG_executor_state.state)
                    logger.trace("DAG_executor_work_loop: put collapsed work in cluster_queue:"
                        + " state is " + str(DAG_executor_state.state))
#brc: cluster:
                    # Do NOT do this
                    ## Don't add to thread_work_queue just do it
                    #if USING_WORKERS: 
                    #    # Config: A4_local, A4_Remote, A5, A6
                    #    worker_needs_input = False
                    ## else: # Config: A1. A2, A3
                else: 
#brc: lambda inc:   
                    # we are doing incremental DAG generation with partitions. The partition may
                    # or may not be complete.
                    # Get the collapsed task and see if it is to be continued - if so, then 
                    # if using workers: put current task and its output in continue queue as a tuple.
                    # if using lambdas: call withdraw() to try to get a new incremental DAG. 
                    # If we get one then the collapsed task will be in it as a completed task 
                    # so we can execute the collapsed task; otherwise, withdraw() will save the 
                    # task (state) and its output and the next call to deposit() by bfs() will 
                    # deposit a new DAG and will restart the continued tasks with their outputs
                    # in their payloads as "input". 
                    state_of_collapsed_task = DAG_info.get_DAG_states()[state_info.collapse[0]]
                    state_info_of_collapse_task = DAG_map[state_of_collapsed_task]
                    logger.info("DAG_executor_work_loop: check TBC of collapsed task state info: "
                        + str(state_info_of_collapse_task))
                    if state_info_of_collapse_task.ToBeContinued:
                        try:
                            msg = "[Error]: P has a collapse task that is TBC but" + " P does not thnk it has a TBC collapse task based on" \
                                + " fanout_fanin_faninNB_collapse_groups_partitions_are_ToBeContinued."
                            assert state_info.fanout_fanin_faninNB_collapse_groups_partitions_are_ToBeContinued , msg
                        except AssertionError:
                            logger.exception("[Error]: assertion failed")
                            if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
                                logging.shutdown()
                                os._exit(0)
                        # assertOld: if P has a collapsed task C and C's state_info says C is TBC then 
                        # P's state_info should indicate that P has a TBC collapse
                        #if not state_info.fanout_fanin_faninNB_collapse_groups_partitions_are_ToBeContinued:
                        #    logger.error("[Error]: P has a collapse task that is TBC but"
                        #        + " P does not thnk it has a TBC collapse task based on"
                        #        + " fanout_fanin_faninNB_collapse_groups_partitions_are_ToBeContinued.")
                            
                        # put current state in the continue_queue. Noet, we have excuted the current
                        # state/task, which is a partition. We put this state in the coninue queue, not the 
                        # state of the collapse task. when we get this state S from the continue queue we will 
                        # process its collapse task C, which means we will set the current state to the 
                        # collapse task C. We will then execute the current state as usual, which will be 
                        # collapse task C.
                        if DAG_executor_constants.USING_WORKERS:
                            #continue_queue.put(DAG_executor_state.state)
                            #logger.trace("DAG_executor_work_loop: put TBC collapsed work in continue_queue:"
                            #    + " state is " + str(DAG_executor_state.state))
                            # put the state with the collapsed task in the continue_queue,
                            # not the state of the collapsed task. We will get the state 
                            # out of the continue queue and then get the collapsed task
                            # of this state and excute it.
                            #
                            # The difference between incremental DAG generation with partitions
                            # vs groups: If we excute partition i we know that i only has a collapse
                            # task to partition i+1. If partition i+1 is incomplete, then we 
                            # cannot execute i so we put i and i's just collectd output in the continue
                            # queue. When we get a new DAG, we get (i,output) rom the continue
                            # queue. Since there is only a collapse task for i, we can go ahead
                            # and execute the collapse task i+1 using the output of i as the 
                            # input of i+1. That is, we do not get (i,output) and then "execute
                            # the fanins/fanouts/collapses" of i since there is only a collapse
                            # task i+1 of i and we can thus simply execute i+1. (So we get continued task i
                            # from the continue queue and we then excute is collapse task i+1,
                            # not i since i was executed previously and its output was saved.
                            # For groups, if we execute group i, i can have fanouts/fanins
                            # collapse, not just collapses. If the targets of these fanouts/fanins
                            # collpase are incomplete then we cannot process them, so we put
                            # i and i's output in the continue queue. When we get a new DAG, 
                            # we get (i,output) from the continue queue. Now we can complete
                            # the processing of the fanouts/fanin/collapse tasks of group/task i. This
                            # is unlike for partitions where we can get (i,output) then grab the
                            # collapse of i and then immediately execute this collapse task.
#brc: lambda inc: cq.put        
                            # True ==> Truly a continued task (i.e., with TBC fanins/fanouts/collpases)
                            continue_tuple = (DAG_executor_state.state,True,output)
                            #continue_queue.put(DAG_executor_state.state)
                            continue_queue.put(continue_tuple)
                            logger.info("DAG_executor_work_loop: put state with TBC collapse in continue_queue:"
                                + " state is " + str(DAG_executor_state.state))
#brc: continue
#brc: lambda inc:
                        else:
                            # Try to get a new incrmental DAG. If we get one then the collapsed
                            # task which is TBC in the current DAG will not be TBC in the new
                            # DAG so we can execute the collpase task (after we enqueue it in 
                            # the cluster queue and we get it from the cluster queue (after possibly 
                            # getting and executing other clustered tasks.)
                            requested_current_version_number = DAG_info.get_DAG_version_number() + 1
                            new_DAG_info = DAG_infobuffer_monitor.withdraw(requested_current_version_number,DAG_executor_state.state,output)
                            if new_DAG_info is not None:
                                DAG_info = new_DAG_info
                                # upate DAG_ma and DAG_tasks with their new versions in DAG_info
                                DAG_map = DAG_info.get_DAG_map()
                                state_info = DAG_map[DAG_executor_state.state]
                                # put non-TBC states in the cluster_queue
                                DAG_executor_state.state = DAG_info.get_DAG_states()[state_info.collapse[0]]
                                cluster_queue.put(DAG_executor_state.state)
                                # number of tasks in the incremental DAG. Not all tasks can 
                                # be executed if the new DAG is still imcomplete.
                                DAG_tasks = DAG_info.get_DAG_tasks()
                                DAG_number_of_tasks = DAG_info.get_DAG_number_of_tasks()
                            else:
                                # No new DAG yet so return/terminate as we have nothing to do 
                                # (i.e., this lambda cannot execute this groups collapse
                                # so the lambda can terminate.) A new lambda will be 
                                # (re)started to execute this collapsed state/task after a 
                                # new incremental DAG is generated and deposited().
                                return
                    else:
#hc: cluster:
                        # put non-TBC states in the cluster_queue
                        DAG_executor_state.state = DAG_info.get_DAG_states()[state_info.collapse[0]]
                        cluster_queue.put(DAG_executor_state.state)
                        logger.trace("DAG_executor_work_loop: put collapsed work in cluster_queue:"
                            + " state is " + str(DAG_executor_state.state))

                    try:
                        msg = "[Error]: DAG_executor_work_loop:" + " fanout_fanin_faninNB_collapse_groups of current partition is not" \
                            + " equal to ToBeContinued of collapse task (next partition)."
                        assert state_info.fanout_fanin_faninNB_collapse_groups_partitions_are_ToBeContinued == state_info_of_collapse_task.ToBeContinued , msg
                    except AssertionError:
                        logger.exception("[Error]: assertion failed")
                        if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
                            logging.shutdown()
                            os._exit(0)
                    # assertOld:
                    # For partitions, we currently grab the collapse of the current
                    # partition, which is the only thing to do since there are no 
                    # fanouts/fanins/faninNBs for partitions, and check the 
                    # collapse's TBC. If that TBC is True, then we put the
                    # collapse task in the contine queue. The TBC of the collapse, 
                    # which is effectively the TBC of the next partition, should equal
                    # the fanout_fanin_faninNB_collapse_groups_partitions_are_ToBeContinued of the current partition.
                    #if not state_info.fanout_fanin_faninNB_collapse_groups_partitions_are_ToBeContinued == state_info_of_collapse_task.ToBeContinued:
                    #    logger.error("[Error]: DAG_executor_work_loop:"
                    #       + " fanout_fanin_faninNB_collapse_groups of current partition is not"
                    #        + " equal to ToBeContinued of collapse task (next partition).")
                    #    logging.shutdown()
                    #    os._exit(0)

            # Note: for multithreaded workers, there is no difference between adding work to the 
            # work queue and adding it to the cluster_queue, i.e., the cluster_queue doesn't eliminate 
            # any overhead. The difference is when using multiprocessing (i.e., worker processses) or lambdas
            # (where lambdas are really also just processes.) So for worker processes we are putting
            # work in lists and sending it to the tcp_server for deposit into the 
            # work_queue; when we cluster a task we add the task/work to a local cluster_queue. Likewise, 
            # For Lambdas we cluster work to a local cluster_queue instead of starting a lambda for the (non-become) fanouts.

# ------------------------------------------------------------------
# ------------------------------------------------------------------
# Fanout / FaninNB
# ------------------------------------------------------------------
# ------------------------------------------------------------------

            elif len(state_info.faninNBs) > 0 or len(state_info.fanouts) > 0:
                # Suggested assert len(collapse) + len(fanin) == 0
                # If len(state_info.collapse) > 0 then there are no fanins, fanouts, or faninNBs and we will not excute this elif or the else

                
                # list of fanouts to deposit into work_queue piggybacking on call to batch fanins.
                # This is used when USING_WORKERS and not USING_THREADS_NOT_PROCESSES, which is 
                # when we process the faninNBs in a batch
#brc: run tasks changed name to include "payload"
                list_of_work_queue_or_payload_fanout_values = []

                # Check fanouts first so we know whether we have a become task for 
                # fanout. If we do, then we dont need any work generated by faninNBs.
                if len(state_info.fanouts) > 0:
                    # start DAG_executor in start state w/ pass output or deposit/withdraw it
                    # if deposit in synchronizer need to pass synchronizer name in payload. If synchronizer stored
                    # in Lambda, then fanout task executed in that Lambda.
#brc: run tasks changes in process_fanouts
#brc: cluster: 4. receive clustered_tasks too, mod/new assertions,
# add clustered_tasks states to the cluster_queue as DAG_executor_states

#brc: cluster II:
                    clustered_tasks = []
                    DAG_executor_state.state = process_fanouts(state_info.fanouts, state_info.task_name, DAG_info.get_DAG_states(), DAG_executor_state, 
                        output, DAG_info, server,work_queue,list_of_work_queue_or_payload_fanout_values,
                        groups_partitions,
#brc: cluster II:
                        state_info.fanout_partition_group_sizes,
                        clustered_tasks)
                    logger.info(thread_name + " work_loop: become state:" + str(DAG_executor_state.state))
                    logger.trace(thread_name + " work_loop: list_of_work_queue_payload fanout_values length:" + str(len(list_of_work_queue_or_payload_fanout_values)))
                    logger.info(thread_name + " work_loop: clustered_tasks (states): " + str(clustered_tasks))

                    # at this point list_of_work_queue_or_payload_fanout_values may or may not be empty. We wll
                    # piggyback this list on the call to process_faninNBs_batch if there are faninnbs.
                    # if not, we will call work_queueu.put_all() directly.

                    if DAG_executor_constants.USING_WORKERS and not DAG_executor_constants.USING_THREADS_NOT_PROCESSES:
                        # Check some assertions on process_fanouts
                        # Config: A5, A6
                        # We piggyback fanouts if we are using worker processes. In that case, if we have
                        # more than one fanout, the first wil be a become task, which will be removed from
                        # state_info.fanouts, decrementing the length of state_info.fanouts.
                        # That will make the new length greater than or equal to 0. If the length is greater
                        # than 0, that means we started with more than one fanout, which means all the fanouts
                        # except the become should be in the list_of_work_queue_or_payload_fanout_values.
                        # Note: We are not currently piggybacking this list whn we use lambdas; instead,
                        # process_fanouts start the lambdas. We may use the parallel invoker on tcp_server 
                        # afterwe piggyback the list of fanouts (as part of process_faninNBs_batch) to tcp_server, 
                        # or just use the parallel invoker in process_fanouts.
                        try:
                            # Note: this assertion is valid when not runtime clustering (see above) and also 
                            # when runtime clustering: we may cluster some or all of the fanout tasks that 
                            # are not the become task. If we cluster all of the non-become tasks then length
                            # of fanouts will be 0; otherwise, the fanouts that are not clustered will be
                            # added to list_of_work_queue_or_payload_fanout_values so the latter should not
                            # have length 0 (when fanouts > 0)
                            msg = "[Error]: work loop: after process_fanouts: fanouts > 1 but no work in list_of_work_queue_or_payload_fanout_values."
                            assert not (len(state_info.fanouts) > 0 and len(list_of_work_queue_or_payload_fanout_values) == 0), msg
                        except AssertionError:
                            logger.exception("[Error]: assertion failed")
                            if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
                                logging.shutdown()
                                os._exit(0)
                        # assertOld:
                        #if len(state_info.fanouts) > 0: # Note: this is the length after removing the become fanout 
                        #    # We became one fanout task and removed it from fanouts, but there maybe were more fanouts
                        #    # and we should have added the fanouts to list_of_work_queue_or_payload_fanout_values.
                        #    if len(list_of_work_queue_or_payload_fanout_values) == 0:
                        #       logger.error("[Error]: work loop: after process_fanouts: fanouts > 1 but no work in list_of_work_queue_or_payload_fanout_values.")

                        if not DAG_executor_constants.ENABLE_RUNTIME_TASK_CLUSTERING:
                            try:
                                msg = "[Error]: work loop: after process_fanouts: len(fanouts) != len(list_of_work_queue_or_payload_fanout_values) + 1" \
                                    + " len(state_info.fanouts): " + str(len(state_info.fanouts)) \
                                    + " list_of_work_queue_or_payload_fanout_values: " + str(len(list_of_work_queue_or_payload_fanout_values))
                                assert len(state_info.fanouts) == len(list_of_work_queue_or_payload_fanout_values), msg
                            except AssertionError:
                                logger.exception("[Error]: assertion failed")
                                if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
                                    logging.shutdown()
                                    os._exit(0)
                            try:
                                # We add fanout tasks to list_of_work_queue_or_payload_fanout_values but we do 
                                # not modify fanouts other than to remove the become task.
                                # Note that wheb we do runtime task clustering we will remove
                                # from tanouts any tasks that we add to clutered_tasks.
                                msg = "[Error]: work loop: after process_fanouts: len(fanouts) != starting_number_of_fanouts - 1."
                                assert len(state_info.fanouts) == starting_number_of_fanouts - 1, msg
                            except AssertionError:
                                logger.exception("[Error]: assertion failed")
                                if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
                                    logging.shutdown()
                                    os._exit(0)
                        else: # runtime task clustering
                            try:
                                # For the original fanout tasks, 1 is the become task, and the remaining tasks aer either
                                # clustered or added to list_of_work_queue_or_payload_fanout_values.
                                msg = "[Error]: work loop: after process_fanouts: starting_number_of_fanouts != len(list_of_work_queue_or_payload_fanout_values) + len(clustered_tasks) + 1."
                                assert starting_number_of_fanouts == len(list_of_work_queue_or_payload_fanout_values) + len(clustered_tasks) + 1, msg
                            except AssertionError:
                                logger.exception("[Error]: assertion failed")
                                if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
                                    logging.shutdown()
                                    os._exit(0) 
                            try:
                                # the number of clustered tasks is less than or equal to the original
                                # number of fanouts minus 1, since we ermove the become task from 
                                # fanouts and all of the remaining tasks in fanout can possibly
                                # be clustered. (Note: we remove clusterd tasks from fanout.)
                                msg = "[Error]: work loop: after process_fanouts: not(len(clustered_tasks) <= starting_number_of_fanouts - 1)."
                                assert len(clustered_tasks) <= starting_number_of_fanouts - 1, msg
                            except AssertionError:
                                logger.exception("[Error]: assertion failed")
                                if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
                                    logging.shutdown()
                                    os._exit(0)   
                    else:
                        # Config: A1, A2, A3, A4_local, A4_Remote
                        # We are not using worker processes. In this case, we get the become task and process
                        # the fanouts. for worker threads, we put the remaining fanouts in the work queue.
                        # For real or simulated lambda, we start real or simulated lambdas to execute the
                        # fanout tasks.
                        # Check assertions on process_fanouts
                        if not DAG_executor_constants.ENABLE_RUNTIME_TASK_CLUSTERING:
                            try:
                                msg = "[Error]: work loop: after process_fanouts: len(fanouts) != len(list_of_work_queue_or_payload_fanout_values) + 1."
                                assert len(state_info.fanouts) == starting_number_of_fanouts - 1, msg
                            except AssertionError:
                                logger.exception("[Error]: assertion failed")
                                if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
                                    logging.shutdown()
                                    os._exit(0)
                        else: # runtime task clustering
                            try:
                                # the number of clustered tasks is less than or equal to the original
                                # number of fanouts minus 1, since we ermove the become task from 
                                # fanouts and all of the remaining tasks in fanout can possibly
                                # be clustered. (Note: we remove clusterd tasks from fanout.)
                                msg = "[Error]: work loop: after process_fanouts: not(len(clustered_tasks) <= starting_number_of_fanouts - 1)."
                                assert len(clustered_tasks) <= starting_number_of_fanouts - 1, msg
                            except AssertionError:
                                logger.exception("[Error]: assertion failed")
                                if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
                                    logging.shutdown()
                                    os._exit(0)  
                            try:
                                msg = "[Error]: work loop: after process_fanouts: len(fanouts) != len(list_of_work_queue_or_payload_fanout_values) + len(clustered_tasks) + 1."
                                assert len(state_info.fanouts) == starting_number_of_fanouts - len(clustered_tasks) - 1, msg
                            except AssertionError:
                                logger.exception("[Error]: assertion failed")
                                if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
                                    logging.shutdown()
                                    os._exit(0) 

#brc: cluster:      # Add become task to cluster queue
                    cluster_queue.put(DAG_executor_state.state)
                    logger.info("DAG_executor_work_loop: put fanout become task in cluster_queue:"
                        + " state is " + str(DAG_executor_state.state))
#brc: cluster II
                    if DAG_executor_constants.ENABLE_RUNTIME_TASK_CLUSTERING:
                        for clustered_task_state in clustered_tasks:
                            cluster_queue.put(clustered_task_state)
                            logger.info("DAG_executor_work_loop: put fanout clustered task in cluster_queue:"
                                + " state is " + str(clustered_task_state))

                    #logging.shutdown()
                    #os._exit(0)
#brc: cluster:
                    #Do NOT do this.
                    ## We get new state_info and then state_info.task_inputs when we iterate
                    #if USING_WORKERS:   # we are become task so we have more work
                    #    # Config: A4_local, A4_Remote, A5, A6
                    #    worker_needs_input = False
                    #    logger.trace(thread_name + " work_loop: fanouts: set worker_needs_input to False.")
                    ## else: Config: A1, A2, A3
                else:
                    # No fanouts so no become task and if faninNBs do not generate
                    # work for us we will need input.
#brc: cluster:
                    try:
                        msg = "[Error]: No fanouts but cluster_queue.qsize() > 0."
                        assert not ((not DAG_executor_constants.ENABLE_RUNTIME_TASK_CLUSTERING) and cluster_queue.qsize() > 0), msg
                    except AssertionError:
                        logger.exception("[Error]: assertion failed")
                        if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
                            logging.shutdown()
                            os._exit(0)
                    #assertOld:
                    #if cluster_queue.qsize() > 0:
                    #    logger.error("[Error]: No fanouts but cluster_queue.qsize() > 0.")
#brc: cluster:
                    #Do NOT do this.
                    #Note: setting worker_needs_input = True must be guarded by USING_WORKERS
                    #if USING_WORKERS: 
                    #    # Config: A4_local, A4_Remote, A5, A6
                    #    worker_needs_input = True
                    #   logger.trace(thread_name + " work_loop: no fanouts: set worker_needs_input to True.")
                    ## else: Config: A1, A2, A3

#brc: cluster:
                if DAG_executor_constants.USING_WORKERS:
                    # Need worker_needs input to be set corrctly in case len(state_info.faninNBs) is 0
                    worker_needs_input = cluster_queue.qsize()==0
                    logger.info("DAG_executor_work_loop: check cluster_queue size before processing faninNBs:"
                        + " cluster_queue.qsize(): " + str(cluster_queue.qsize()))

                if len(state_info.faninNBs) > 0:
                    # this is the condition for batch processing; whether we use 
                    # async_call or not depends on whether or not we can get return values.
                    # if using worker processes and we don't need work (since we got a become task
                    # from the fanouts) or we are running tasks in lambdas then do batch processing
                    # Note: we haven't implemented real lamdba version yet, but we do 
                    # not distinguish here between real and simulated lambdas
                    if (DAG_executor_constants.RUN_ALL_TASKS_LOCALLY and DAG_executor_constants.USING_WORKERS and not DAG_executor_constants.USING_THREADS_NOT_PROCESSES) or (
                        not DAG_executor_constants.RUN_ALL_TASKS_LOCALLY):
                        # We do batch processing when we use lambdas to execute tasks, real or simulated.
                        # So this condition, which specifies the "sync objects trigger their tasks" case
                        # is not needed.
                        #not RUN_ALL_TASKS_LOCALLY and not USING_WORKERS and not STORE_FANINS_FANINNBS_LOCALLY and STORE_SYNC_OBJECTS_IN_LAMBDAS and SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS):

                        # Config: A1,  A5, A6
                        # Note: We call process_faninNBs_batch when we are simulating lambdas with threads
                        # and we are storing synch objects in lambdas, regardless of whether the objects are stored
                        # in real lambdas or simulated by python functions, and regardless of 
                        # whether we are running tasks in lambas that store sync objects and that are simulated by 
                        # python functions or we are running them in threads that are used to simulate lambdas. 
                        # Note: We use threads to simulate real lambdas that execute tasks as in Wukong.
                        # We also store sync objects in lambda functins. The Lambdas may be real or they
                        # may be simuated by using Python functions. The sync objects are fanin objects
                        # that are fanins or fanouts (where a fanout is a fanin of size 1). These sync
                        # object fanins can optionally execute their fanin tasks when they have all of
                        # their results. This option is SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS.
                        # In this case, the tasks and the synch objects are executed and stored, respectfully,
                        # in the same lambdas - so we are using lambdas to excute and store sync objects 
                        # and the lambdas may be real or simulated.
                        # Note we will only make async_calls when we are simulating lambdas by executing tasks 
                        # in real python functions and storing object in real python functions. In this case
                        # no value is returned to the caller. When we are not storing sync objects in 
                        # lambdas, work is returned so that threads can be started to simukate lambdas.
                        # Note: calling process_faninNBs_batch when using threads to simulate lambdas and storing 
                        # objects remotely and USING_LAMBDA_FUNCTION_SIMULATORS_TO_STORE_OBJECTS.
                        # Since the faninNBs cannot start new simulated threads to execute the fanin tasks,
                        # the process_faninNB_batch passes all the work back and the calling simuated thread starts one thread
                        # for each work tuple returned.
                        # Note: batching work when we are using workers and storing the FanInNBs remotey and 
                        # we are using processes, or we are using real lambdas. We can also use workers with 
                        # threads instead of processes but multithreading with remote FanInNBs is 
                        # not as useful as using processes. Multithreadng in general is not as helpful 
                        # as multiprocessing in Python.
                        #or (RUN_ALL_TASKS_LOCALLY and not USING_WORKERS and using_Lambda_Function_Simulator):
 
                        try:
                            msg = "[Error]: DAG_executor_work_loop: using processes or lambdas but storing FanINNBs locally."
                            assert not (DAG_executor_constants.STORE_FANINS_FANINNBS_LOCALLY), msg
                        except AssertionError:
                            logger.exception("[Error]: assertion failed")
                            if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
                                logging.shutdown()
                                os._exit(0)
                        # assertOld:
                        #if STORE_FANINS_FANINNBS_LOCALLY:
                        #    # Config: A2, A4_local
                        #    logger.error("[Error]: DAG_executor_work_loop: using processes or lambdas but storing FanINNBs locally.")
                                
                        if not DAG_executor_constants.RUN_ALL_TASKS_LOCALLY:
                            # Config: A1
    #brc: cluster:
                            #Okay - this was set above
                            try:
                                msg = "[Error]: DAG_executor_work_loop: using lambdas, so no workers, but worker_needs_input."
                                assert not (worker_needs_input), msg
                            except AssertionError:
                                logger.exception("[Error]: assertion failed")
                                if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
                                    logging.shutdown()
                                    os._exit(0)
                            #assertOld:
                            #if worker_needs_input:
                            #    # Note: perhaps we csn use Lmbdas where Lambdas have two thread workers - faster?
                            #    logger.error("[Error]: DAG_executor_work_loop: using lambdas, so no workers, but worker_needs_input.")

#brc: cluster:
                        #Note: we are using worker_needs_input so it needs to have been set above
                        if not DAG_executor_constants.RUN_ALL_TASKS_LOCALLY or (DAG_executor_constants.RUN_ALL_TASKS_LOCALLY and DAG_executor_constants.USING_WORKERS and not DAG_executor_constants.USING_THREADS_NOT_PROCESSES and not worker_needs_input):
#brc: run tasks
# Note: this will not be async when we allow lambdas to run tasks and then have local fanin(NB) objects
# since process_faninNBs_batch will return 0 or the preceeding results if caller is become task.
# note: for simulating lambdas, if not using python functions to run tasks then using threads to
#  simulate lambdas and run tasks. Can be using threads if we are just storing objects in lambdas
#  (whether these are real lambdas or python objects?)
                            # running lambdas to execute tasks or using worker processes and not worker_nneds_input.
                            # when a worker process does not need input no work can be returned so no need to wait for
                            # the return. We also use async call when we are using Python/Lambda functions to store
                            # objects and to run tasks.
                            # Note that the api call to the server in process_faninNBs_batch
                            # will see that this is an async_call and will not wait on (receive) a return
                            # value but instead will return 0 which is what it would have recieved from the 
                            # tcp_server if it would have waited (synchronously)
                            async_call = True
                        else:
                            # using worker processes or (using threads to simulate lambdas and storing objects in simulated lambdas).
                            # If we are using threads to simulate lambdas but storing objects on the server thn we call
                            # process_faninNBs, we do not call the process_faninNBs batch method, so we do not need 
                            # to set async_call here. process_faninNBs returns a value (0 or not o) for each faninNB.
                            # Note that when we store objects in lambdas (simulated or not) we run tcp_server_lambda.py
                            # which has a version of process_faninNBs_batch that returns a list of work if the caller
                            # is a thread that is simulating a lambda for executing tasks.
                            async_call = False

                        logger.trace("BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB")
                        logger.trace("work loop: async_call:" + str(async_call))

#brc: cluster:
                        # used for asssert below
                        save_worker_needs_input = worker_needs_input

                        #Note: using worker processes - batch calls to fan_in for FaninNBs
                        worker_needs_input = process_faninNBs_batch(websocket,state_info.faninNBs, state_info.faninNB_sizes, 
                            state_info.task_name, DAG_info.get_DAG_states(), DAG_executor_state, 
                            output, DAG_info,work_queue,worker_needs_input, list_of_work_queue_or_payload_fanout_values,

                            async_call, state_info.fanouts,
#brc: cluster:
                            cluster_queue,
#brc: batch
                            DAG_info.get_all_faninNB_task_names(),
                            DAG_info.get_all_faninNB_sizes())

                        try:
                            msg = "[Error]: after process_faninNBs_batch" + " worker_needs_input is True when not using workers."
                            assert not (worker_needs_input and not DAG_executor_constants.USING_WORKERS), msg
                        except AssertionError:
                            logger.exception("[Error]: assertion failed")
                            if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
                                logging.shutdown()
                                os._exit(0)
                        # assertOld:
                        #if worker_needs_input and not USING_WORKERS:
                        #    logger.error("[Error]: after process_faninNBs_batch"
                        #       + " worker_needs_input is True when not using workers.")

#brc: cluster:

                        try:
                            msg = "[Error]: process_faninNBs_batch set" \
                                + " worker_needs_input to False when using workers but cluster_queue" \
                                    + " size is 0."
                            assert not (DAG_executor_constants.USING_WORKERS and worker_needs_input != save_worker_needs_input and cluster_queue.qsize() == 0), msg
                        except AssertionError:
                            logger.exception("[Error]: assertion failed")
                            if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
                                logging.shutdown()
                                os._exit(0)
                        # assertOld:
                        #if USING_WORKERS and worker_needs_input != save_worker_needs_input:
                        ##    # Note: worker_needs_input was True at now it is false. 
                        ##    # Note: process_faninNBs does not set process_faninNBs to True. It
                        ##    # can only set worker_needs_input to False. So either worker_needs_input
                        ##    # statrs as True and is set to False, or it starts as False and remains
                        ##    # False. If worker_needs_input starts as True, then save_worker_needs_input
                        ##    # will be True. If process_faninNBs sets worker_needs_input to False,
                        ##    # then worker_needs_input and save_worker_needs_input will be different.
                        ##    # When process_faninNBs sets worker_needs_input to False, it is because it
                        ##    # also deposted a become task in the cluster queue, so the cluster_queue
                        ##    # shudl not be different.
                        #     if cluster_queue.qsize() == 0:
                        #        logger.error("[Error]: process_faninNBs_batch set"
                        #           + " worker_needs_input to False when using workers but cluster_queue"
                        #            + " size is 0.")
                    else: 
                        # Config: A2, A3, A4_local, A4_Remote
                        # using worker threads not processes, or using threads to simualate running lambdas,
                        # but not using lambdas to store synch objects - storing them locally instead.
                        # Note: if we are using thread workers we can still store the FanInNBs remotely, but the work queue 
                        # will be local. Batch FanInNb processing will put work (fanin tasks) in the work_queue as the 
                        # work_queue is also stored remotely (with the FanInNBs). When using threads, the work_queue is local 
                        # so we cannot put work in the work queue while processing remote FanInNBs on the server. This means we 
                        # would have to pass all the work back here to the thread and have the thread add the work to the local work
                        # queue. Doable, but maybe later - multithreading is not as useful as multprocessng, and 
                        # we do batch FaninNBs and store the FanINNBs and work_queue remotely when multiprocessing
                        # (same for multiprocessing where processes are multithreaded, which is an interesting use case).
#brc: cluster:
                        # for asssert below
                        save_worker_needs_input = worker_needs_input

                        # Note: ignoring worker_needs_input if we are not using workers.
                        worker_needs_input = process_faninNBs(websocket,state_info.faninNBs, state_info.faninNB_sizes, 
                            state_info.task_name, DAG_info.get_DAG_states(), DAG_executor_state, 
                            output, DAG_info, server,work_queue,worker_needs_input,
#brc: cluster:
                            cluster_queue)

                        try:
                            msg = "[Error]: after process_faninNBs" \
                                + " worker_needs_input is True when not using workers."
                            assert not(worker_needs_input and not DAG_executor_constants.USING_WORKERS) , msg
                        except AssertionError:
                            logger.exception("[Error]: assertion failed")
                            if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
                                logging.shutdown()
                                os._exit(0)
                        #assertOld:
                        #if worker_needs_input and not USING_WORKERS:
                        #    logger.error("[Error]: after process_faninNBs"
                        #        + " worker_needs_input is True when not using workers.")
#brc: cluster:
                        try:
                            msg = "[Error]: process_faninNBs set" + " worker_needs_input to False when using workers but cluster_queue" \
                                + " size is 0."
                            assert not (DAG_executor_constants.USING_WORKERS and worker_needs_input != save_worker_needs_input and cluster_queue.qsize() == 0), msg
                        except AssertionError:
                            logger.exception("[Error]: assertion failed")
                            if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
                                logging.shutdown()
                                os._exit(0)
                        # assertOld:
                        #if USING_WORKERS and worker_needs_input != save_worker_needs_input:
                        ##    # Note: See comment in same asssertion above.
                        #    if cluster_queue.qsize() == 0:
                        #        logger.error("[Error]: process_faninNBs set"
                        #            + " worker_needs_input to False when using workers but cluster_queue"
                        #            + " size is 0.")

                    # there can be faninNBs and fanouts.

                else:   # len(state_info.faninNBs) is 0
                    # Currently, we are not piggybacking fanouts if we are using lambdas or worker threads. 
                    # When using lambdas, we start a lambda in process_fanouts 
                    # for each fanout task. This code is for the case that we are using worker processes
                    # piggybacking fanouts on the process faninNB call but we did not 
                    # have any faninNBs so we did not get a chance to piggyback the fanouts and thus we
                    # need to process the fanouts here. For worker processes, that means put the fanout tasks
                    # in the work queue. For Lambdas, we will want to send the fanouts to the tcp_server
                    # for parallel invocation. For worker threads, we processed fanouts in process_fanouts()
                    # by adding the fanout tasks to the work queue.
                    if DAG_executor_constants.RUN_ALL_TASKS_LOCALLY and DAG_executor_constants.USING_WORKERS and not DAG_executor_constants.USING_THREADS_NOT_PROCESSES:
                        # Config: A5, A6
                        # we are batching faninNBs and piggybacking fanouts on process_faninNB_batch

#brc: cluster II: these assertions are valid when we are not runtime clstering
#brc: what are the assertions for runtime clustering?
                        if (not DAG_executor_constants.ENABLE_RUNTIME_TASK_CLUSTERING) and len(state_info.fanouts) > 0:
                            # No faninNBs (len(state_info.faninNBs) == 0) so we did not get a chance to 
                            # piggyback list_of_work_queue_or_payload_fanout_values on the call to process_faninNBs_batch.
 
# brc: cluster:
                            try:
                                # when there is at least one fanout we will become one of the fanout tasks
                                # depositing the become task in the cluster queue so the cluster queue should not be empty.
                                msg = "[Error]: work loop: len(state_info.fanouts) > 0 but worker needs work."
                                assert not (cluster_queue.qsize()==0), msg
                            except AssertionError:
                                logger.exception("[Error]: assertion failed")
                                if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
                                    logging.shutdown()
                                    os._exit(0)
                            # assertOld:
                            #if cluster_queue.qsize()==0:
                            ##if worker_needs_input:
                            #    # when there is at least one fanout we will become one of the fanout tasks
                            #    # by depositing the task in the cluster queue so the cluster queue should not be empty.
                            #    logger.error("[Error]: work loop: fanouts but worker needs work.")
 
                            if len(state_info.fanouts) > 1:
                                # We became one fanout task but there were more and we should have added the 
                                # fanouts to list_of_work_queue_or_payload_fanout_values.
                                try:
                                    msg = "[Error]: work loop: fanouts > 1 but no work in list_of_work_queue_or_payload_fanout_values."
                                    assert not (len(list_of_work_queue_or_payload_fanout_values) == 0), msg
                                except AssertionError:
                                    logger.exception("[Error]: assertion failed")
                                    if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
                                        logging.shutdown()
                                        os._exit(0)
                                #assertOld:
                                #if len(list_of_work_queue_or_payload_fanout_values) == 0:
                                #    logger.error("[Error]: work loop: fanouts > 1 but no work in list_of_work_queue_or_payload_fanout_values.")
                                #    logging.shutdown()
                                #    os._exit(0) 
                                                                      
                                # since we could not piggyback on process_faninNB_batch, enqueue the fanouts
                                # directly into the work_queue
                                logger.trace(thread_name + " work loop: no faninNBs so enqueue fanouts directly.")
                                # Note: if we use lambdas with a batch nvoker, here we will call the tcp_server
                                # method that dos the batch invokes. This method is probably used by 
                                # process_faninNBs_batch to invoke the fanouts that are piggybacked.
                                work_queue.put_all(list_of_work_queue_or_payload_fanout_values)
                                # list_of_work_queue_or_payload_fanout_values is redefined on next iteration
                            else:
                                try:
                                    # there was one fanout so we became that one fanout (by depositing it into the 
                                    # cluster_queue) and there were no other fanouts to add to the list_of_work_queue_or_payload_fanout_values.
                                    msg = "[Error]: work loop: len(state_info.fanouts) is 1 but list_of_work_queue_or_payload_fanout_values is not empty."
                                    assert len(list_of_work_queue_or_payload_fanout_values) == 0 , msg
                                except AssertionError:
                                    logger.exception("[Error]: assertion failed")
                                    if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
                                        logging.shutdown()
                                        os._exit(0)
                                # assertOld:
                                # there was one fanout so we became that one fanout and should have enqueued no fanouts
                                #if not len(list_of_work_queue_or_payload_fanout_values) == 0:
                                #    logger.error("[Error]: work loop: len(state_info.fanouts) is 1 but list_of_work_queue_or_payload_fanout_values is not empty.")
                                #    logging.shutdown()
                                #    os._exit(0)
                                        
                    # else: # Config: A1, A2, A3, A4_local, A4_Remote

                # If we are not USING_WORKERS and there were fanouts then continue with become 
                # task; otherwise, this thread (simulatng a Lambda) or Lambda is done, as it has reached the
                # end of its DFS path. (Note: if using workers and there are no fanouts and no
                # faninNBs for which we are the last thread to call fanin, the worker will get more
                # work from the work_queue instead of stopping. The worker continutes until it gets
                # a STOP state value from the work_queue (e.g., -1). Noet: If there are fanouts and/or 
                # faninNBs, there can be no fanins. Note, when we are not USING_WORKERS, the faninNBs
                # will start new threads to execute the fanin task, (or invoke new Lamdas when we 
                # are using Lambdas. So faninNBs cannot generate more work for a worker since the 
                # work is given to a new thread.)

                # brc: If we are not using workers then this is a thread simulating
                # a lambda or a real lambda. The faninNBs started a thread or a 
                # real lambda to execute the faninNB task, i.e, we never become a faninNB
                # task as all the faninNB tasks are execute by starting  new lambda.
                # So we (the simulated or real lambda) may or may not have more work to do,
                # epending on whether or not we became a fanout task. If we are the 
                # become task of a fanout then we have more work to do. In that case, 
                # the become task (a fanout task) was put in the cluster_queue, whch is 
                # now non-empty so we can just keep going and this become task will be
                # retrieved from the cluster_queue in the next iteration of the work loop. 
                # 
                # We will execute a become task if we started this iteration with 
                # len(state_info.fanouts) > 0. This is because when there are more than one 
                # fanouts we grab the first one as the become task. (The situation where the 
                # DAG has only one fanout task and no faninNB tasks is handled by making this 
                # fanout task the "collapse task" so the number of collapse tasks in the state
                # will be > 0 and the number of fanout tasks will be 0). The become
                # task is added to the cluster_queue. (It may be added after 0, 1 or
                # more runtime clustered tasks in the cluster_queue.)
                #
                # Even if len(state_info.fanouts) was 0, so we did a fanin or faninNB,
                # we may have previously runtime clustered several tasks so the 
                # cluster_queue may be non-empty
                # 
                # When we take a become task, we remove it from state_info.fanouts
                # and there may not be any other fanouts (there are faninNBs since we are 
                # here) so len(state_info.fanouts) will become 0. Thus, we capture the length of 
                # fanouts at the begining in starting_number_of_fanouts. If starting_number_of_fanouts
                # was > 0, then we made one fanout the become task and we should 
                # not return here, i.e., we should continue and execute the become task,
                # which we can get from the non-empty cluster_queue.
                # Note: without this check of starting_number_of_fanouts,
                # we will return prematurley in the case that there is 
                # one fanout and one or more faninNBs, as the number of 
                # fanouts will become 0 when we remove the become task 
                                        
                #if (not USING_WORKERS) and len(state_info.fannouts) == 0:
                #if (not DAG_executor_constants.USING_WORKERS) and starting_number_of_fanouts == 0:
                if (not DAG_executor_constants.USING_WORKERS) and cluster_queue.qsize()==0:
                    # Config: A1, A2, A3
                    # we are a thread simulating a lambda or a real lamda and there
                    # were no fanout tasks (and by definition of using lambdas the 
                    # faninNB tasks were executed by starting new threads/lambdas)
                    # so we have nothing else to do (we are not a worker that can get more
                    # work) so we can return/terinate the thread/lambda.
                    # Note: It is imporant to note that when using threads to
                    # simulate lambdas or real lambdas, we do not get work from
                    # faninNBs, i.e., we cannot become a faninNB task since faninNB
                    # tasks are always executed by starting a new thread/lambda.
                    # Note: The whole reason we have faninNBs is because of the 
                    # situation where we have fanouts (so will become a fanout
                    # task), and one or more "fanins", which means we cannot become
                    # any of the fanin tasks. In this case we use faninNBs (fanin No
                    # Becomes) objects. We only use "FanIn" objects when there are
                    # no fanouts so we can become a "Fanin" task.
                    # Note: Furthermore, we only use a FanIn object when there are
                    # no fanouts and there is only one fanin. If there are no 
                    # fanouts but multiple fanins, then all the fanins are FaninNBs.
                    # There is this note in DFS_visit:
                    #   if a fanin s has an enabler e that has more than one dependent 
                    #   then s is a faninNB. Where "an enabler e of s" is a predecessor
                    #   node of s, i.e., e has an output that is an input of s.
                    #   ToDo: Optimize fanins: if no fanouts, but many fanins/faninNBs, 
                    #     we can have one fanin object and many faninNB objcts. 
                    # That would make the work loop logic more complicated,
                    # Currently we either have one or more fanouts and one or more fanonNBs 
                    # or we have a single fanin. this optimization would add more
                    # cases to check for (the case where we have no fanouts but we
                    # have a fanin and one or more faninNBs. We would want to do 
                    # the fanin, and see whether we are the become task, and then 
                    # do the faninNBs, where if we are not the bcome task for the 
                    # fanin and we are using workers then we would become the 
                    # tanin task for one of the faninNBs.)

                    logger.info(thread_name + " : cluster_queue empty: lambda returning after process fanouts/faninNBs")
                    return
                else:
                    # we are a worker so even if we will not become a fanout task 
                    # (starting_number_of_fanouts == 0) we can get more work from 
                    # the work queue, 
                    # or we are not a worker (not USING_WORKERS), which means we are a 
                    # thread simulating a lambda or a real lambda) and there was at least one fanout task 
                    # so we will become a fanout task (not starting_number_of_fanouts == 0); 
                    # thus, we should not terminate.
                    
                    logger.info(thread_name + ": cluster_queue not empty: lambda not returning after process fanouts/faninNBs.")
                #else: # Config: A4_local, A4_Remote, A5, A6

# ------------------------------------------------------------------
# ------------------------------------------------------------------
# Fanin
# ------------------------------------------------------------------
# ------------------------------------------------------------------

            elif len(state_info.fanins) > 0:
                # Suggested assert len(state_info.faninNBs)  + len(state_info.fanouts) + len(collapse) == 0
                # if faninNBs or fanouts then can be no fanins. length of faninNBs and fanouts must be 0 
                DAG_executor_state.state = DAG_info.get_DAG_states()[state_info.fanins[0]]
                #state = DAG_info.get_DAG_states()[state_info.fanins[0]]
                #if len(state_info.fanins) > 0:
                #ToDo: Set next state before process_fanins, returned state just has return_value, which has input.
                # single fanin, try-op w/ returned_state.return_value or restart with return_value or deposit/withdraw it

#brc: cluster:
                #returned_state = process_fanins(websocket,state_info.fanins, state_info.fanin_sizes, state_info.task_name, DAG_info.get_DAG_states(),  DAG_executor_state, output, server)
                DAG_exec_state = process_fanins(websocket,state_info.fanins, state_info.fanin_sizes, state_info.task_name, DAG_info.get_DAG_states(),  DAG_executor_state, output, server)
                logger.trace(thread_name + ": " + state_info.task_name + ": after call to process_fanin: " + str(state_info.fanins[0]) + " returned_state.blocking: " + str(DAG_exec_state.blocking) + ", returned_state.return_value: "
                    + str(DAG_exec_state.return_value) + ", DAG_executor_state.state: " + str(DAG_executor_state.state))

                if DAG_exec_state.return_value == 0:
                    # we are not the become task for the fanin
#brc: cluster:
                    #Do not set worker_needs_input
                    #Note: setting worker_needs_input = True must be guarded by USING_WORKERS
                    if DAG_executor_constants.USING_WORKERS:
                        ## Config: A4_local, A4_Remote, A5, A6
                        logger.info(thread_name + ": After call to process_fanin: return value is 0; using workers so get more work.")
                        #worker_needs_input = True
                        pass
                    else:
                        # Config: A1, A2, A3
                        if cluster_queue.qsize()==0:
                            # this dfs path is finished
                            logger.info(thread_name + ": After call to process_fanin: return value is 0 and cluster_queue is empty so lamba (simulated or real) returns.")
                            return
                        else:
                            logger.info(thread_name + ": After call to process_fanin: return value is 0 and cluster_queue is not empty so lamba (simulated or real) does not return.")
                            pass
                else:
                    if (DAG_executor_constants.RUN_ALL_TASKS_LOCALLY and DAG_executor_constants.USING_WORKERS) or not DAG_executor_constants.RUN_ALL_TASKS_LOCALLY:
                        # Config: A1, A4_local, A4_Remote, A5, A6
                        # when using workers, threads or processes, each worker has its own local
                        # data dictionay. If we are the become task for a fanin, we receive the 
                        # fanin task inputs and we put them in the data dctionary. Same for lambdas.
                        #dict_of_results = returned_state.return_value
#brc: cluster: Note: using "DAG_exec_state" instead of "returned_state"
                        dict_of_results = DAG_exec_state.return_value
                        
                        # Also, don't pass in the multp data_dict, so will use the global.
                        # Fix if in global

                        logger.trace("fanin Results: ")
                        for key, value in dict_of_results.items():
                            logger.trace(str(key) + " -> " + str(value))
                        for key, value in dict_of_results.items():
                            data_dict[key] = value

                        #data_dict[state_info.task_name] = output

                        # we are the become task so execute the become task, where this is a worker
                        # or a dfs thread.
#brc: cluster:
                        #Question: Do not do this
                        logger.trace(thread_name + ": After call to process_fanin: return value not 0.")
                        #logger.trace(thread_name + ": After call to process_fanin: return value not 0, using workers so set worker_needs_input = False")
                        #worker_needs_input = False
                    #else: # Config: A2, A3
                        # do not add results to (global) data_dict when using threads
                        # to simulate lambdas (which means we are not using workers and we are RUN_ALL_TASKS_LOCALLY,
                        # or we are using real lambdas, i.e, not RUN_ALL_TASKS_LOCALLY) since
                        # these threads already added their results to the global data_dict (shared by all
                        # the threads) before calling fanin.
#brc: cluster:
                    # For (thread and process) workers and real lambdas, need to 
                    # add results to data_dict, but not for threads that
                    # simulate lamdas as these threads add their results to the 
                    # global data_dict before doing the fanin so the 
                    # results are already in the global data_dict and there 
                    # is no reason to ads them here again. 
                    # 
                    # But for all cases, we need to add the returned 
                    # DAG_exec_state.state (of the become fanin task) to the cluster_queue.
                    cluster_queue.put(DAG_exec_state.state)

                    logger.trace("DAG_executor_work_loop: add fanin becomes task to the clster_queue:"
                        + " state is " + str(DAG_exec_state.state))

# ------------------------------------------------------------------
# ------------------------------------------------------------------
# Empty Collape/Fanout/FaninNB/Fanin
# ------------------------------------------------------------------
# ------------------------------------------------------------------
            else:
                #Note: setting worker_needs_input = True must be guarded by USING_WORKERS
                if DAG_executor_constants.USING_WORKERS:
                    # Config: A4_local, A4_Remote, A5, A6
                    # do another iteration of the work loop - get work from the 
                    # continue_queue or cluster_queue, or if they are empty try to get
                    # worj from thr work_queue
                    logger.trace(thread_name + ": state " + str(DAG_executor_state.state) + " after executing task " 
                        +  state_info.task_name + " has no collapse, fanouts, fanins, or faninNBs; using workers so no return.")
#brc: cluster:
                    #Do NOT do this.
                    #logger.trace(thread_name + " set worker_needs_input to true")
                    #worker_needs_input = True
                else:
                    logger.info(thread_name + ": for state " + str(DAG_executor_state.state) + " after executing task " +  state_info.task_name + " state has no collapse, fanouts, fanins, or faninNBs.")
                    # Config: A1, A2, A3
                    if cluster_queue.qsize()==0:
                        logger.info(thread_name + ": And cluster_queue.qsize()==0; since we are a lambda not a worker, we return.")
                        return
                    else:
                        logger.info(thread_name + ": And cluster_queue.qsize()!=0 so lambda does not return.")
                        return

        # end while (True)


# Config: A2, A3
# called by DAG_executor_task
def DAG_executor(payload):		 
    # Note: could instead use a "state" parameter. Then we have state.starting_input and state.return_value so would need
    # to know which to acccess, as in: if first_invocation, where first_invocation is in state. Or always use
    # state.return_value so we don't need to check and save first_invocation:
    #   input = state.return_value
    # Note: for blocking try_fanin, it's not really a restart, it means we will be started in a specified fanin/faninNB
    #     task state - there is no other state to save/restore. So DAG_executor can get input and give to execute_task.
    #     try_fanin: with state.blocking = 0 and state.return_value = results map. Then we continue in state 
    #     states[fanin_task_name], which we can set before call. try_fanin w/state.blocking = 1 and state.return_value = 0, 
    #     in which case we terminate/return and no restart on fanins.
    #	  So: not really saving state and restoring on restart - fanin-block=>term w/ no restart; fanin-noblock means
    #         we transition to next state which is DAG_states[fanin_task_name]; faninNB => asynch continue on to fanouts.
    #	  But fanin has state.return_value and we pass keyword args to fanin/faninNB in real version.
    # Q:  Do we allow synchronous fanin? Yes if server, no if Lambda (for now)?
    #     faninNB: is like an async call with always terminate, so never restarted. Always term as in
    #      if self.state.blocking:
    #         self.state.blocking = False
    #         return
    #      where synchronize_async_terminate() doesn't wait for return value from server and sets self.state.blocking to True.
    # Note: may need to change dask leaf node inputs so, e.g., withdraw input from BB - then need a first_invocation
    # to control this?
    # Note: invoking DAG_executor() is different from executing task. The DAG_executor needs to get input and present it
    # to execute_task in the proper form.
		
	# use DAG_executor_state.state
    if not DAG_executor_constants.USING_WORKERS:
        DAG_exec_state = payload['DAG_executor_state']
    else:
        DAG_exec_state = DAG_executor_State(function_name = "DAG_executor", function_instance_ID = str(uuid.uuid4()))
    ##state = payload['state'] # refers to state var, not the usual State of DAG_executor 
    ##logger.trace("state:" + str(state))
    ##DAG_executor_state.state = payload['state']
    if not DAG_executor_constants.USING_WORKERS:
        # Config: A2
        logger.trace("DAG_executor(): payload state:" + str(DAG_exec_state.state))
    # For leaf task, we get  ['input': inp]; this is passed to the executed task using:
    #    def execute_task(task_name,input): output = DAG_info.DAG_tasks[task_name](input)
    # So the executed task gets ['input': inp], just like a non-leaf task gets ['output': X]. For leaf tasks, we use "input"
    # as the label for the value.

    #task_payload_inputs = payload['input']
    #logger.trace("DAG_executor starting payload input:" +str(task_payload_inputs) + " payload state: " + str(DAG_executor_state.state) )
  
    # reads from default file './DAG_info.pickle'
#brc: lambda inc: need to get input from payload when incremental DAG generation
# instead of always getting it at start from file, as passing DAG_infos on
# restart as part of payload (i.e., no reread DAG_info from file).
    if DAG_executor_constants.USING_WORKERS:
        DAG_info = DAG_Info.DAG_info_fromfilename()
    else:
        DAG_info = payload['DAG_info']

#brc: lambda: Need to get the continued_task flag from the payload.

# The data_dict is global to the threads, so all of the inputs to this task were previously
# outputs of some task T that T put in the data_dict. Here we just asssert
# that the key is already in the data_dict.
#
# Note: The input can be for a regular task or for a contunued task that 
# is continued during incremental DAG generation. Note however, that for
# incremental DAG generation, when we are using groups, the payload input 
# for this task T is value that this task output when it was excuted and then
# determined to have fanins/fanouts that were to-be-continued. At that point,
# the lambda L that was executing T tried to withdraw a new incremental DAG
# but no DAG was available yet, so L terminated and its output P was saved
# for when a new lambda could executed with P (i.e., afte the next incremental
# DAG is generated.) The new Lambda will not execute the task T again since
# it was already executed; instead, the new lmabda will perform T's fanins/fanouts.
#
# Note: For incemental testing, when using threads or simulated
# lambdas, the output values of a group are already in the data_dict
# so we do not need to add them to the dictionary. (The values 
# are added with qualified names, e.g., "PR1_1-PR2_1". For real 
# lambdas, a lambdas is restarted with the output value and must
# add these values to the data_dict (with qualidied names).
# For simulated lambdas, a simulated lambda is restarted by 
# to excute a continued task, and the output of the continued task
# is in the payload as "input", but we do not actually need this 
# input since simulated lambdas are threads that share a global
# data_dict so the output was added to the data_dict and is still
# there; thus we do not need the output in the payloa and we do 
# need to add the output to the data_dict, which is something we
# do for real lambdas (in DAG_executor_lambda).
    #The payload "input" should be in the shared global data_dict
    DAG_map = DAG_info.get_DAG_map()
    
    if not DAG_executor_constants.USING_WORKERS:
        state_info = DAG_map[DAG_exec_state.state]
        logger.info("DAG_executor: simulated lambda (thread) starting for " + state_info.task_name)

        continued_task = DAG_exec_state.continued_task
        logger.info("DAG_executor: state_info.task_name: " + state_info.task_name
            + " continued_task: " + str(continued_task))
    
        is_leaf_task = state_info.task_name in DAG_info.get_DAG_leaf_tasks()
        logger.info("DAG_executor: state_info.task_name: " + state_info.task_name
            + " is_leaf_task: " + str(is_leaf_task))

        if not is_leaf_task:

            def DD(dict_of_results,data_dict):
                logger.info("DAG_executor(): DD: verify inputs are in data_dict: ")
                logger.info("DAG_executor(): DD: dict_of_results: " + str(dict_of_results))
                logger.info("DAG_executor(): DD: type of dict_of_results: " + str(type(dict_of_results)))
                #for i in range ( len(dict_of_results) ):
                #    print( str(type(dict_of_results [i])) )
                #for i in range ( len(dict_of_results) ):
                #    print( dict_of_results [i] )
                for key, _value in dict_of_results.items():
                    #data_dict[key] = _value
                    value_in_dict = data_dict.get(key,None)
                    if value_in_dict is None:
                        logger.error("[Error]:(part of Asssertionrror: starting DAG_executor for simulated lambda"
                            + " data_dict missing value for input key " + str(key))
                        #logging.shutdown()
                        #os._exit(0)
                        return False
                    else:
                        logger.trace("DAG_executor:verified: " + str(key))
                return True

            try:
                dict_of_results = payload['input']
                msg = "[Error]: starting DAG_executor for simulated lambda" + " data_dict missing value for input key."
                logger.info("DAG_executor(): before call to DD for " + state_info.task_name + " : dict_of_results: " + str(dict_of_results)) 
                assert DD(dict_of_results,data_dict), msg
            except AssertionError:
                logger.exception("[Error]: assertion failed")
                if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
                    logging.shutdown()
                    os._exit(0)
            # assertOld:
            #logger.trace("DAG_executor(): verify inputs are in data_dict: ")
            ## lambdas invoked with inputs. We do not add leaf task inputs to the data
            ## dictionary, we use them directly when we execute the leaf task.
            ## Also, leaf task inputs are not in a dictionary.
            #dict_of_results = payload['input']
            #for key, _value in dict_of_results.items():
            #    #data_dict[key] = _value
            #    value_in_dict = data_dict.get(key,None)
            #    if value_in_dict is None:
            #        logger.error("[Error]: starting DAG_executor for simulated lambda"
            #            + " data_dict missing value for input key " + str(key))
            #        logging.shutdown()
            #        os._exit(0)
            #    else:
            #        logger.trace("DAG_executor:verified: " + str(key))
                    
#brc:  input output
            if continued_task:
                state_info.task_inputs = dict_of_results
                logger.info("DAG_executor: dict_of_results for continued and non-leaf task"
                    + state_info.task_name + ": state_info.task_inputs:"
                    + str(dict_of_results))
        else:
#brc:  input output
            if continued_task:
                dict_of_results = payload['input']
                state_info.task_inputs = dict_of_results
                logger.info("DAG_executor: dict_of_results for continued and leaf task"
                    + state_info.task_name + ": state_info.task_inputs:"
                    + str(dict_of_results))
            # leaf tasks have no input arguments; the inputs for leaf tasks are
            # stored in the DAG (as Dask does)']
            #
            # Not however, for incrementl DAG generation, for a continued
            # task that is a group, we will not excuet the task as it
            # has already been executed. We need the outputs of this
            # execution, which were passsed for lambdas in the payload
            # when the lambda was (re)started to exexcute the continued
            # task as the payload's "input". We gran these inputs/outputs
            # and sore them in state_info.task_inputs, from which we
            # will retrieve the outputs when we see tha the task is
            # continued, i.e., we grab te output and do the fanouts/fanins
            # of the continued task.
            # Note: It is kay to put the outputs in state_info.task_inputs since
            # we will not ty to retrieve this output as the input for
            # excuting that task; this is because the task is continued
            # so we do not try to execute it.

    # work_queue is the global shared work queue, which is none when we are using threads
    # to simulate lambdas and is a Queue when we are using worker threads. See imported file
    # DAG_executor_work_queue_for_threads.py for Queue creation.
    global work_queue
#brc: continue
    
    #global DAG_infobuffer_monitor
    #if not USING_WORKERS:
    #    DAG_infobuffer_monitor = wukongdnc.dag.DAG_infoBuffer_Monitor_for_lambdas_for_threads.DAG_infobuffer_monitor
    #else:
    #    DAG_infobuffer_monitor = wukongdnc.dag.DAG_infoBuffer_Monitor_for_threads.DAG_infobuffer_monitor

    try:
        msg = "[Error]: method DAG_executor: DAG_infobuffer_monitor is None."
        assert not (DAG_executor_constants.RUN_ALL_TASKS_LOCALLY and (DAG_executor_constants.USING_WORKERS or not DAG_executor_constants.USING_WORKERS) and DAG_executor_constants.COMPUTE_PAGERANK and DAG_executor_constants.USE_INCREMENTAL_DAG_GENERATION and DAG_executor_constants.USING_THREADS_NOT_PROCESSES and wukongdnc.dag.DAG_infoBuffer_Monitor_for_threads.DAG_infobuffer_monitor is None), msg
    except AssertionError:
        logger.exception("[Error]: assertion failed")
        if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
            logging.shutdown()
            os._exit(0)
    # assertOld:
    #if RUN_ALL_TASKS_LOCALLY and (USING_WORKERS or not USING_WORKERS) and COMPUTE_PAGERANK and USE_INCREMENTAL_DAG_GENERATION and USING_THREADS_NOT_PROCESSES:
    #    if wukongdnc.dag.DAG_infoBuffer_Monitor_for_threads.DAG_infobuffer_monitor is None:
    #        logger.error("[Error]: method DAG_executor: DAG_infobuffer_monitor is None.")
    #        logging.shutdown()
    #        os._exit(0)

#brc: groups partitions
    # For worker proceses, we do not read all the groups/partitions
    # of nodes at the start; a group/partition of nodes is read 
    # when we compute the pageranks of the group/partition
    groups_partitions = []

    #DAG_info = payload['DAG_info']
    #DAG_executor_work_loop(logger, server, counter, thread_work_queue, DAG_executor_state, DAG_info, data_dict)
#brc: counter
# tasks_completed_counter, workers_completed_counter
    DAG_executor_work_loop(logger, server, completed_tasks_counter, completed_workers_counter, DAG_exec_state, DAG_info, 
#brc: continue
        work_queue,wukongdnc.dag.DAG_infoBuffer_Monitor_for_threads.DAG_infobuffer_monitor,
        groups_partitions)
    
    logger.trace("DAG_executor() method returned from work loop.")

# Config: A5, A6
# def DAG_executor_processes(payload,counter,process_work_queue,data_dict,log_queue, configurer)
#brc: counter
# tasks_completed_counter, workers_completed_counter
def DAG_executor_processes(payload,completed_tasks_counter,completed_workers_counter,log_queue_or_logger, worker_configurer,
    shared_nodes,shared_map,shared_frontier_map,
    pagerank_sent_to_processes,previous_sent_to_processes,number_of_children_sent_to_processes,number_of_parents_sent_to_processes,starting_indices_of_parents_sent_to_processes,parents_sent_to_processes,IDs_sent_to_processes,):
    # Use for multiprocessing workers
    # Note: log_queue_or_logger is either a queue or a logger. If not 
    # USE_MULTITHREADED_MULTIPROCESSING it is a queue; otheriwise it is a logger.

    print("Worker process loaded in PID: " + str(os.getpid()))
    #- read DAG_info, create DAG_exec_state, thread_work_queue is parm
    if not DAG_executor_constants.USE_MULTITHREADED_MULTIPROCESSING:
        # Config: A5
        #global logger
#brc: logging
        worker_configurer(log_queue_or_logger)
        logger = logging.getLogger("multiP")
    else:
        # Config: A6
        #worker_configurer(log_queue)
        #logger = logging.getLogger("multiP")
        #logger.setLevel(logging.DEBUG)

        # log_queue_or_logger is the logger, which was passed to each thread
        logger = log_queue_or_logger

    if DAG_executor_constants.COMPUTE_PAGERANK and DAG_executor_constants.USE_SHARED_PARTITIONS_GROUPS:
        #print(str(pagerank_sent_to_processes[:10]))
        if DAG_executor_constants.USE_PAGERANK_GROUPS_PARTITIONS:
            BFS_Shared.shared_groups = shared_nodes
            BFS_Shared.shared_groups_map = shared_map
            BFS_Shared.shared_groups_frontier_parents_map = shared_frontier_map
        else:
            BFS_Shared.shared_partition = shared_nodes
            BFS_Shared.shared_partition_map = shared_map
            BFS_Shared.shared_partition_frontier_parents_map = shared_frontier_map
        if DAG_executor_constants.USE_STRUCT_OF_ARRAYS_FOR_PAGERANK:
            BFS_Shared.pagerank = pagerank_sent_to_processes
            BFS_Shared.previous = previous_sent_to_processes
            BFS_Shared.number_of_children = number_of_children_sent_to_processes
            BFS_Shared.number_of_parents = number_of_parents_sent_to_processes  
            BFS_Shared.starting_indices_of_parents = starting_indices_of_parents_sent_to_processes
            BFS_Shared.parents = parents_sent_to_processes
            BFS_Shared.IDs = IDs_sent_to_processes
        
    proc_name = multiprocessing.current_process().name
    thread_name = threading.current_thread().name
    logger.info("proc " + proc_name + " " + " thread " + thread_name + ": worker process started.")

    try:
        msg = "[Error]: DAG_executor_processes: executing multiprocesses but USING_WORKERS is false."
        assert DAG_executor_constants.USING_WORKERS , msg
    except AssertionError:
        print("USING_WORKERS: " + str(DAG_executor_constants.USING_WORKERS))
        logger.exception("[Error]: assertion failed")
        if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
            logging.shutdown()
            os._exit(0)

    #assertOld:
    #if not USING_WORKERS:
    #    logger.error("[Error]: DAG_executor_processes: executing multiprocesses but USING_WORKERS is false.")

    #logger = logging.getLogger('main')__name__
    #level = logging.DEBUG
    #message = (proc_name + ": testing 1 2 3.")
    #logger.log(level, message)
 
    #if not USING_WORKERS:
    #    DAG_exec_state = payload['DAG_executor_state']
    #else:
    DAG_exec_state = DAG_executor_State(function_name = "DAG_executor", function_instance_ID = str(uuid.uuid4()))

    # Note: For incemental testing, when using processes, 
    # the output values of a group are already in the data_dict
    # when a process get a continued task from the continue queue.

    #logger.trace("DAG_executor_processes: DAG_exec_state: " + str(DAG_exec_state))
    ##state = payload['state'] # refers to state var, not the usual State of DAG_executor 
    ##logger.trace("state:" + str(state))
    ##DAG_executor_state.state = payload['state']

    #if not USING_WORKERS:
    #    logger.trace("payload state:" + str(DAG_exec_state.state))

    # For leaf task, we get  ['input': inp]; this is passed to the executed task using:
    #    def execute_task(task_name,input): output = DAG_info.DAG_tasks[task_name](input)
    # So the executed task gets ['input': inp], just like a non-leaf task gets ['output': X]. For leaf tasks, we use "input"
    # as the label for the value.
    #task_payload_inputs = payload['input']
    #logger.trace("DAG_executor starting payload input:" +str(task_payload_inputs) + " payload state: " + str(DAG_executor_state.state) )
  
    # reads from default file './DAG_info.pickle'
    DAG_info = DAG_Info.DAG_info_fromfilename()
    #DAG_map = DAG_info.get_DAG_map()
    #state_info = DAG_map[DAG_exec_state.state]
    #logger.info("DAG_executor_processes(): worker (process) starting for " + state_info.task_name)


    # The work loop will create a BoundedBuffer_Work_Queue. Each process excuting the work loop
    # will create a BoundedBuffer_Work_Queue object, which wraps the websocket creatd in the 
    # work loop and the code to send work to the work queue on the tcp_server.
    work_queue = None
#brc: continue
    DAG_infobuffer_monitor = None
#brc: groups partitions
    # For worker proceses, we do not read all the groups/partitions
    # of nodes at the start; a group/partition of nodes is read 
    # when we compute the pageranks of the group/partition
    groups_partitions = []

    #DAG_info = payload['DAG_info']
    #DAG_executor_work_loop(logger, server, counter, process_work_queue, DAG_exec_state, DAG_info, data_dict)
#brc: counter
# tasks_completed_counter, workers_completed_counter

    DAG_executor_work_loop(logger, server, completed_tasks_counter, completed_workers_counter, DAG_exec_state, DAG_info, 
#brc: continue
        work_queue, DAG_infobuffer_monitor,
        groups_partitions)
    logger.trace("DAG_executor_processes: returning after work_loop.")
    return

# Config: A1
def DAG_executor_lambda(payload):
    logger.info("Lambda: started.")

    if not (DAG_executor_constants.STORE_SYNC_OBJECTS_IN_LAMBDAS and DAG_executor_constants.SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS):
        DAG_exec_state = cloudpickle.loads(base64.b64decode(payload['DAG_executor_state']))
        DAG_info = cloudpickle.loads(base64.b64decode(payload['DAG_info']))
    else:
        DAG_exec_state = payload['DAG_executor_state'] 
        DAG_info = payload['DAG_info']  

#brc: group_partitions
    # For real lambdas, to avoid reading the partitions/groups from an s3
    # bucket or whatever, the DAG_executor_driver can input the partitions
    # or groups and pass them the the real lambdas it starts as part of the 
    # payload. This list group_partitions will be passed to process_fanouts
    # and added to the payloags of the real lambdas started for fanouts.
    # tcp_server can also read group_partitions and make it available
    # to the faninNBs, which also start real lambdas.
    groups_partitions = []
    if DAG_executor_constants.INPUT_ALL_GROUPS_PARTITIONS_AT_START:
        groups_partitions = cloudpickle.loads(base64.b64decode(payload['groups_partitions']))

        #print("groups_partitions:")
        #keys = list(groups_partitions.keys())
        #for key in keys:
        #   print(key + ":")
        #    g_p = groups_partitions[key]
        #    for node in g_p:
        #        print(str(node))
        #logging.shutdown()
        #os._exit(0)

    #logger.trace("payload DAG_exec_state.state:" + str(DAG_exec_state.state))

    DAG_map = DAG_info.get_DAG_map()

    #Note: get the continued_task flag from the payload.
    
    state_info = DAG_map[DAG_exec_state.state]
    logger.info("DAG_executor_lambda() starting for " + state_info.task_name)

#brc:  input output
    #if this is a continued task, then in the DAG the state info for
    # the task will show that state_info.ToBeContinued is false, i.e., 
    # we completed the task information in the DAG and we are now
    # restarting the continud task so we can do its fanout/fanins/collpases.
    # The fact that we are restarting a continued task is given by
    # DAG_executor_state.state.
    continued_task =  DAG_exec_state.continued_task

    is_leaf_task = state_info.task_name in DAG_info.get_DAG_leaf_tasks()
    logger.info("DAG_executor_lambda(): state_info.task_name: " + state_info.task_name
        + " continued_task: " + str(continued_task)
        + " is_leaf_task: " + str(is_leaf_task))
    
    # A task is a leaf task or non-leaf task. The first group/partition
    # in the DAG is a leaf task. If the task is not a leaf task
    # and it is a continued task then we need to add the outputs
    # (which are the payload's "input") to the data_dict. If
    # the task is a leaf task and it is not a continued task,
    # then the payload "input" is the actual input to the task when we
    # execute the task. Examples of this are the first group/partition
    # when we execute it for the first time. Likewise for a 
    # group/partition that is the start of a new connected component,
    # which makes the group/partition a (new) leaf task. (The 
    # first group/partiton in the DAG is the first group/partition 
    # in its connected component. There may be other connected
    # components and they will have leaf task(s).)
    # When we execute the first group in the DAG we will find that
    # it is complete but its fanins/fanouts are to incomplete
    # partition 2 or the groups in incomplete partition 2. IF we 
    # are executing groups, the real lamba that executes this first
    # task/grup, will stop after saving the task's output (to
    # the DAG_infoBuffer_monitor_for_Lambdas on the tcp_server.)
    # When re restart a real lmabda to continue processing the 
    # task by excuting its fanouts/fanins, this first group will 
    # be a continued_task that is a leaf task. So even though it is
    # leaf task, it is also a continued task (so it has been executed
    # before) so the payload's "input" is the output of this task from 
    # its previous execution and we need to add the input/ouput
    # to the data_dict using qualified names. (For example, if we
    # execute "PR1_1" and it has a fanout to "PR2_1" with output X,
    # Then we add ("PR1_1-PR2_1",X) to the data_dict; lkewise, when 
    # we do the fanout for "PR2_1", we retrieve its input (which was 
    # an output of "PR1_1") using the key "PR1_1-PR2_1" for the data_dict.
    if not is_leaf_task or (DAG_executor_constants.COMPUTE_PAGERANK and DAG_executor_constants.USE_INCREMENTAL_DAG_GENERATION and continued_task):
        # Real lambdas are invoked with inputs. We do not add leaf task inputs to the data
        # dictionary, we use them directly when we execute the leaf task.
        # Also, leaf task inputs are not in a dictionary.
        if not (DAG_executor_constants.STORE_SYNC_OBJECTS_IN_LAMBDAS and DAG_executor_constants.SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS):
            dict_of_results = cloudpickle.loads(base64.b64decode(payload['input']))
        else:
            dict_of_results = payload['input']
        # If this is a continued task then input is actually the output
        # of a task that was executed previously and that then stopped
        # because it was a group with TBC fanouts/fanins.

        logger.info("DAG_executor_lambda(): " + state_info.task_name + " dict_of_results: " + str(dict_of_results))
        if not continued_task:
            for key, value in dict_of_results.items():
                data_dict[key] = value
        else: 
            for (k,v) in dict_of_results.items():
                # example: state_info.task_name = "PR1_1" and 
                # k is "PR2_3" so data_dict_key is "PR1_1-PR2_3"
                data_dict_key = str(state_info.task_name+"-"+k)
                # list of input tuples. Example: list of single tuple:
                # [(3, 0.012042187499999999)], hich says that the pagerank
                # value of the shadow_node in position 3 of PR2_3's 
                # partition is 0.012042187499999999. This is the pagerank
                # value of a parent node of the node in position 4 of 
                # PR2_3's partition. We set the shadow nodes's value before
                # we start the pagerank calculation. There is a trick used
                # to make sure the hadow node's pageran value is not changed 
                # by the pagerank calculation. (We compute the shadow node's 
                # new pagerank but we hardcode the shadow node's (dummy) parent
                # pagerank vaue so that the new shadow node pagerank is he same 
                # as the old value.)
                data_dict_value = v
                data_dict[data_dict_key] = data_dict_value

            logger.info("DAG_executor_lambda: data_dict after: " + str(data_dict))

#brc:  input output
        # The code for task execution will get the input for the 
        # execution from state_info.task_inputs, so we put the 
        # payload "input" there.
        if continued_task:
            state_info.task_inputs = dict_of_results
            logger.info("DAG_executor_lambda(): " + state_info.task_name + " set state_info.task_inputs: "
                + str(state_info.task_inputs))
    else:
        # Passing leaf task input as state_info.task_inputs in DAG_info; 
        # We don't want to add a leaf task input parameter to DAG_executor_work_loop(); 
        # i.e., we could get the inputs from the payload and pass then to the
        # work loop as a parameter, but this parameter would only be used by the Lambdas 
        # and we have a place already in state_info.task_inputs for these inputs.
        # Thus we read the payload inputs here n the work loop and put the
        # nputs in state_info.task_inputs from which we read the inputs for 
        # leaf and non-leaf tasks when we execuet the tasks. 
        # Note: We null out state_info.task_inputs for leaf tasks in the DAG_executor_driver
        # after we start the leaf lambdas, in order to save space, i.e., there is no need to 
        # pass this leaf task input as part of the DAG_info to all the 
        # non-leaf tasks.
        logger.info("DAG_executor_lambda(): " + state_info.task_name + " is leaf task.")
        if not (DAG_executor_constants.STORE_SYNC_OBJECTS_IN_LAMBDAS and DAG_executor_constants.SYNC_OBJECTS_IN_LAMBDAS_TRIGGER_THEIR_TASKS):
            inp = cloudpickle.loads(base64.b64decode(payload['input']))
        else:
            inp = payload['input']
        # The code for task execution will get the input for the 
        # execution from state_info.task_inputs, so we put the 
        # payload "input" there.
        state_info.task_inputs = inp

    # lambdas do not use work_queues, for now.
    work_queue = None
#brc: continue
    DAG_infobuffer_monitor = None

    # server and counter are None
    # logger is local lambda logger

#brc: counter
# tasks_completed_counter, workers_completed_counter
    DAG_executor_work_loop(logger, server, completed_tasks_counter, completed_workers_counter, DAG_exec_state, DAG_info, 
#brc: continue
        work_queue,DAG_infobuffer_monitor,
        groups_partitions)
    logger.trace("DAG_executor_lambda: returning after work_loop.")
    return
                        
# Config: A4_local, A4_Remote
def DAG_executor_task(payload):
    DAG_executor_state = payload['DAG_executor_state']
    if DAG_executor_state != None:
        # DAG_executor_state is None when using workers
        logger.trace("DAG_executor_task: call DAG_executor(), state is " + str(DAG_executor_state.state))
    DAG_executor(payload)
    logger.trace("DAG_executor_task: returned from DAG_executor()")
    
def main():

    """
	# generate DAG_map using DFS_visit
    n1 = Node(None,None,"inc0",inc0)
    n3 = Node(None,None,"triple",triple)
    n4 = Node(None,None,"inc1",inc1)
    n5 = Node(Node,Node,"square",square)
    n2 = Node(Node,Node,"add",add) 
    n6 = Node(Node,Node,"multiply",multiply) 
    n7 = Node(Node,Node,"divide",divide)
	
    n1.set_succ([n2])
    n1.set_pred([])
    n2.set_succ([n6])	
    n2.set_pred([n1,n4])
    n3.set_succ([n6])
    n3.set_pred([n4])
    n4.set_succ([n2,n5,n3])
    n4.set_pred([])
    n5.set_succ([n6])
    n5.set_pred([n4])
    n6.set_succ([n7])
    n6.set_pred([n2,n3,n5])
    n7.set_succ([])
    n7.set_pred([n6])
	
    n1.generate_ops()
    n4.generate_ops()
    n2.generate_ops()
    n3.generate_ops()
    n5.generate_ops()
    n6.generate_ops()
    n7.generate_ops()
    """
	
    logger.trace("DAG_map:")
    for key, value in Node.DAG_map.items():
        logger.trace(key)
        logger.trace(value)
    logger.trace("  ")
	
    logger.trace("states:")         
    for key, value in Node.DAG_states.items():
        logger.trace(key)
        logger.trace(value)
    logger.trace("   ")
    
    logger.trace("DAG_tasks:")         
    for key, value in Node.DAG_tasks.items():
        logger.trace(key)
        logger.trace(value)
    logger.trace("   ")
	
    logger.trace("num_fanins:" + str(Node.num_fanins) + " num_fanouts:" + str(Node.num_fanouts) + " num_faninNBs:" 
        + " num_collapse:" + str(Node.num_collapse))
    logger.trace("   ")
	
    logger.trace("all_fanout_task_names")
    for name in Node.all_fanout_task_names:
        logger.trace(name)
        logger.trace("   ")
    logger.trace("   ")
	
    logger.trace("all_fanin_task_names")
    for name in Node.all_fanin_task_names :
        logger.trace(name)
        logger.trace("   ")
    logger.trace("   ")
		  
    logger.trace("all_faninNB_task_names")
    for name in Node.all_faninNB_task_names:
        logger.trace(name)
        logger.trace("   ")
    logger.trace("   ")
		  
    logger.trace("all_collapse_task_names")
    for name in Node.all_collapse_task_names:
        logger.trace(name)
        logger.trace("   ")
    logger.trace("   ")
	
    DAG_map = Node.DAG_map
    task_name_to_function_map =  Node.DAG_tasks
    
    logger.trace("DAG_map after assignment:")
    for key, value in DAG_map.items():
        logger.trace(key)
        logger.trace(value)
    logger.trace("   ")
    logger.trace("task_name_to_function_map after assignment:")
    for key, value in task_name_to_function_map.items():
        logger.trace(key)
        logger.trace(value)
    logger.trace("   ")
    
    #states = Node.DAG_states
    #all_fanout_task_names = Node.all_fanout_task_names
    #all_fanin_task_names = Node.all_fanin_task_names
    #all_faninNB_task_names = Node.all_faninNB_task_names
    #all_collapse_task_names = Node.all_collapse_task_names

	
	# ToDo: logger.trace Node.DAG_map
	#		logger.trace Node.states (after making states global)
	# 		logger.trace Node.all_fanin_task_names (for synch objects)
	#		logger.trace Node.all_faninNB_task_names (for synch objects)

	# ToDo: get this from DAG map:
	# start states are x and y, where:
	# state 1: input 0, ("inc0": add 1), output 1 to faninNB "add",
	# state 2: input 1, ("inc1": add 1), output 2 to faninNB "add", output 2 to fanout "square"
	# state 3: input 2, ("square": square 2), output 4 to fanin "mult"
	# state 4: input 1 from "inc0" and 2 from "inc1", ("add": 1+2), output 3 to fanin "mult"
	# state 5: input 4 from "square" and 3 from "add", ("mult": 4*3), output 12 to ?
	
    #server = DAG_executor_Synchronizer()

    #ToD0: loop thru DAG_info.DAG_start_states - but need their inputs to do that
    try:
        DAG_executor_State1 = DAG_executor_State(state = int(1))
        logger.trace("Starting DAG_executor thread for state 1")
        payload = {
            #"state": int(1),
            "input": {'input': int(0)},
			"DAG_executor_state": DAG_executor_State1,
            "server": server
        }
        _thread.start_new_thread(DAG_executor_task, (payload,))
    except Exception:
        logger.exception("[ERROR] Failed to start DAG_executor thread for state 1")
        if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
            logging.shutdown()
            os._exit(0) 
        
    try:
        DAG_executor_State3 = DAG_executor_State(state = int(3))
        logger.trace("Starting DAG_executor thread for state 3")
        payload = {
            #"state": int(3),
            "input": {'input': int(1)},
			"DAG_executor_state": DAG_executor_State3,
            "server": server
        }
        _thread.start_new_thread(DAG_executor_task, (payload,))
    except Exception:
        logger.exception("[ERROR] Failed to start DAG_executor thread for state 3")
        if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
            logging.shutdown()
            os._exit(0) 
        
    logger.trace("Sleeping")
    time.sleep(5)
	
    
if __name__=="__main__":
    main()


##Xtras:
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
    value = inp['input']
    value += 1
    output = {'inc0': value}
    logger.trace("inc0 output: " + str(output))
    return output
def inc1(inp):
    logger.trace("inc1")
    value = inp['input']
    value += 1
    output = {'inc1': value}
    logger.trace("inc1 output: " + str(output))
    return output
"""