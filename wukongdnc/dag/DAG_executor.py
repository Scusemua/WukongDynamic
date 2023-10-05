
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
from .DAG_executor_constants import run_all_tasks_locally, store_fanins_faninNBs_locally 
from .DAG_executor_constants import create_all_fanins_faninNBs_on_start, using_workers 
from .DAG_executor_constants import using_threads_not_processes, use_multithreaded_multiprocessing
from .DAG_executor_constants import process_work_queue_Type, FanInNB_Type, using_Lambda_Function_Simulators_to_Store_Objects
from .DAG_executor_constants import sync_objects_in_lambdas_trigger_their_tasks, store_sync_objects_in_lambdas
from .DAG_executor_constants import tasks_use_result_dictionary_parameter, same_output_for_all_fanout_fanin
from .DAG_executor_constants import compute_pagerank, use_shared_partitions_groups, use_page_rank_group_partitions
from .DAG_executor_constants import use_struct_of_arrays_for_pagerank
from .DAG_executor_constants import use_incremental_DAG_generation, name_of_first_groupOrpartition_in_DAG
#rhc: counter:
from .DAG_executor_constants import num_workers
#from .DAG_work_queue_for_threads import thread_work_queue
from .DAG_executor_work_queue_for_threads import work_queue
from .DAG_data_dict_for_threads import data_dict
from .DAG_executor_counter import completed_tasks_counter, completed_workers_counter
from .DAG_executor_synchronizer import server
from wukongdnc.wukong.invoker import invoke_lambda_DAG_executor
from .DAG_boundedbuffer_work_queue import Work_Queue_Client
from .util import pack_data
#rhc continue
from .Remote_Client_for_DAG_infoBuffer_Monitor import Remote_Client_for_DAG_infoBuffer_Monitor
from .DAG_infoBuffer_Monitor_for_threads import DAG_infobuffer_monitor
# Note: avoiding circular imports:
# https://stackoverflow.com/questions/744373/what-happens-when-using-mutual-or-circular-cyclic-imports
#rhc cleanup
#from .BFS import shared_partition, shared_groups
#from .BFS import shared_partition_map, shared_groups_map
#from .Shared import shared_partition, shared_groups, shared_partition_map,  shared_groups_map
from . import BFS_Shared
from .DAG_executor_output_checker import set_pagerank_output
from .DAG_executor_constants import check_pagerank_output

logger = logging.getLogger(__name__)

if not (not using_threads_not_processes or use_multithreaded_multiprocessing):
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('[%(asctime)s] [%(module)s] [%(processName)s] [%(threadName)s]: %(message)s')
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

"""
#rhc shared
shared_partition = []
shared_groups = []
# maps partition "P" to its position/size in shared_partition/shared_groups
shared_partition_map = {}
shared_groups_map = {}
"""

total_time = 0
num_fanins_timed = 0


def create_and_faninNB_task_locally(kwargs):
    logger.debug("create_and_faninNB_task: call create_and_faninNB_locally")
    server = kwargs['server']
    # Not using return_value from faninNB since faninNB starts the fanin task, i.e., there is No Become
    _return_value_ignored = server.create_and_faninNB_locally(**kwargs)

def faninNB_task_locally(kwargs):
    logger.debug("faninNB_task: call faninNB_locally")
    server = kwargs['server']
    # Not using return_value from faninNB since faninNB starts the fanin task, i.e., there is No Become
    _return_value_ignored = server.faninNB_locally(**kwargs)

# used to execute a task; need to give the task its "input" map
#name_to_function_map = {'inc0': inc0, 'inc1': inc1, 'add': add, 'multiply': multiply, 'triple': triple, 'square': square, 'divide':divide}

# execute task from name_to_function_map with key task_name
def execute_task(task, args):
    #commented out for MM
    thread_name = threading.current_thread().name
    logger.debug(thread_name + ": execute_task: input of execute_task is: " + str(args))
    #output = task(input)
    #for i in range(0, len(args)):
    #    print("Type of argument #%d: %s" % (i, type(args[i])))
    #    print("Argument #%d: %s" % (i, str(args[i])))
    output = task(*args)
    return output

#rhc: ToDo: total_num_nodes is hardcoded
def execute_task_with_result_dictionary(task,task_name,total_num_nodes,resultDictionary):
    #commented out for MM
    thread_name = threading.current_thread().name
    logger.debug(thread_name + ": execute_task_with_result_dictionary: input of execute_task is: " 
        + str(resultDictionary))
    #output = task(input)
    #for i in range(0, len(args)):
    #    print("Type of argument #%d: %s" % (i, type(args[i])))
    #    print("Argument #%d: %s" % (i, str(args[i])))
    output = task(task_name,total_num_nodes,resultDictionary)
    return output

def execute_task_with_result_dictionary_shared(task,task_name,total_num_nodes,resultDictionary,shared_map, shared_nodes):
    #commented out for MM
    thread_name = threading.current_thread().name
    logger.debug(thread_name + ": execute_task_with_result_dictionary: input of execute_task is: " 
        + str(resultDictionary))
    #output = task(input)
    #for i in range(0, len(args)):
    #    print("Type of argument #%d: %s" % (i, type(args[i])))
    #    print("Argument #%d: %s" % (i, str(args[i])))
    output = task(task_name,total_num_nodes,resultDictionary,shared_map,shared_nodes)
    return output



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
    #store_fanins_faninNBs_locally = keyword_arguments['store_fanins_faninNBs_locally']  # option set in DAG_executor
    #DAG_info = keyword_arguments['DAG_info']
    thread_name = threading.current_thread().name

    logger.debug (thread_name + ": faninNB_remotely: calling_task_name: " + keyword_arguments['calling_task_name'] + "calling faninNB with fanin_task_name: " + keyword_arguments['fanin_task_name'])
    #logger.debug("faninNB_remotely: DAG_executor_state.keyword_arguments[fanin_task_name]: " + str(DAG_executor_state.keyword_arguments['fanin_task_name']))
    #FanInNB = server.synchronizers[fanin_task_name]

    # Note: in real code, we would return here so caller can quit, letting server do the op.
    # Here, we can just wait for op to finish, then return. Caller has nothing to do but
    # quit since nothing to do after a fanin.

    # return is: None, restart, where restart is always 0 and return_value is None; and makes no change to DAG_executor_State
    #return_value, restart = FanInNB.fan_in(**keyword_arguments)
    #ToDo:
    # rhc: DES
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
#rhc batch
    DAG_exec_state.keyword_arguments['fanin_type'] = keyword_arguments['fanin_type']
    if not run_all_tasks_locally:
        # Note: When faninNB start a Lambda, DAG_info is in the payload. 
        # (Threads and processes read it from disk.)
        DAG_exec_state.keyword_arguments['DAG_info'] = keyword_arguments['DAG_info']
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

#def process_faninNBs(websocket,faninNBs, faninNB_sizes, calling_task_name, DAG_states, DAG_exec_state, output, DAG_info, server):
def process_faninNBs(websocket,faninNBs, faninNB_sizes, calling_task_name, DAG_states, 
    DAG_exec_state, output, DAG_info, server, work_queue,worker_needs_input,

#rhc: cluster:
    cluster_queue):
    thread_name = threading.current_thread().name
    logger.debug(thread_name + ": process_faninNBs")
    logger.debug(thread_name + ": process_faninNBs: worker_needs_input: " + str(worker_needs_input))
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
#rhc batch
        keyword_arguments['fanin_type'] = "faninNB"
        # We will use local datadict for each multiprocess; process will receve
        # the faninNB results and put them in the data_dict.
        # Note: For DAG generation, for each state we execute a task and 
        # for each task T we have t say what T;s task_inputs are - these are the 
        # names of tasks that give inputs to T. When we have per-fanout output
        # instead of having the same output for all fanouts, we specify the 
        # task_inputs as "sending task - receiving task". So a sending task
        # S might send outputs to fanouts A and B so we use "S-A" and "S-B"
        # as the task_inputs, instad of just using "S", which is the Dask way.
        if same_output_for_all_fanout_fanin:
            keyword_arguments['result'] = output
            keyword_arguments['calling_task_name'] = calling_task_name
        else:
            
            logger.debug("**********************" + thread_name + ": process_faninNBs:  for " + calling_task_name + " faninNB "  + name+ " output is :" + str(output))

            #if name.endswith('L'):  
            #    keyword_arguments['result'] = output[name[:-1]]
            #    qualified_name = str(calling_task_name) + "-" + str(name[:-1])
            #else:
            keyword_arguments['result'] = output[name]
            qualified_name = str(calling_task_name) + "-" + str(name)

            logger.debug(thread_name + ": process_faninNBs: name:" + str(name) 
                + " qualified_name: " + qualified_name)
            keyword_arguments['calling_task_name'] = qualified_name
        #ToDo: Don't do/need this?
        #keyword_arguments['DAG_executor_State'] = new_DAG_exec_state # given to the thread/lambda that executes the fanin task.
        keyword_arguments['server'] = server
        keyword_arguments['store_fanins_faninNBs_locally'] = store_fanins_faninNBs_locally
        keyword_arguments['DAG_info'] = DAG_info

		#Q: kwargs put in DAG_executor_State keywords and on server it gets keywords from state and passes to create and fanin

        if store_fanins_faninNBs_locally:
            if not using_workers:
                # Note: We start a thread to make the fanin call and we don't wait for it to finish.
                # So this is like an asynch call to tcp_server. The faninNB will start a new 
                # thread to execute the fanin task so the callers do not need to do anything.
                # Agan, "NB" is "No Become" so no caller will become the executor of the fanin task.
                # keyword_arguments['DAG_executor_State'] = new_DAG_exec_state 
                # given to the thread/lambda that executes the fanin task.
                if not create_all_fanins_faninNBs_on_start:
                    try:
                        logger.debug(thread_name + ": process_faninNBs: Starting asynch simulation create_and_fanin task for faninNB " + name)
                        NBthread = threading.Thread(target=create_and_faninNB_task_locally, name=("create_and_faninNB_task_"+name), args=(keyword_arguments,))
                        NBthread.start()
                    except Exception as ex:
                        logger.error("[ERROR]:" + thread_name + ": process_faninNBs: Failed to start create_and_faninNB_task thread.")
                        logger.debug(ex)
                        return 0
                else:
                    try:
                        logger.debug(thread_name + ": process_faninNBs: Starting asynch simulation faninNB_task for faninNB " + name)
                        NBthread = threading.Thread(target=faninNB_task_locally, name=("faninNB_task_"+name), args=(keyword_arguments,))
                        NBthread.start()
                    except Exception as ex:
                        logger.error("[ERROR]:" + thread_name + ": process_faninNBs: Failed to start faninNB_task thread.")
                        logger.debug(ex)
                        return 0
            else:
                # When we are using workers, we use this faster code which calls the
                # faninNB locally directly instead of starting a thread to do it. (Starting
                # a thread simulates calling fanin asynchronously.)
                if not create_all_fanins_faninNBs_on_start:
                    logger.debug(thread_name + ": process_faninNBs: call create_and_faninNB_locally")
                    #server = kwargs['server']
                    # Not using return_value from faninNB since faninNB starts the fanin task, i.e., there is No Become
                    _return_value_ignored = server.create_and_faninNB_locally(**keyword_arguments)
                else:
                    logger.debug(thread_name + ": process_faninNBs: call faninNB_locally")
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
            if not create_all_fanins_faninNBs_on_start:
                dummy_DAG_exec_state = create_and_faninNB_remotely(websocket,**keyword_arguments)
            else:
                dummy_DAG_exec_state = faninNB_remotely(websocket,**keyword_arguments)

            #if DAG_exec_state.blocking:
            # using the "else" after the return, even though we don't need it
            logger.debug(thread_name + ": process_faninNBs:  faninNB_remotely dummy_DAG_exec_state: " + str(dummy_DAG_exec_state))
            logger.debug(thread_name + ": process_faninNBs:  faninNB_remotely dummy_DAG_exec_state.return_value: " + str(dummy_DAG_exec_state.return_value))
            if dummy_DAG_exec_state.return_value == 0:
                pass
            else:
                # Note: When we aer using_workers we now call process_fsninNBs_batch
                # so this code is not currently being used. Batch processing processes
                # all the faninNBs at once on the server, rather than calling 
                # the server to process the faninnbs one by one.
                if using_workers:
                    # this caller could be a thread or a process
                    
                    dict_of_results = dummy_DAG_exec_state.return_value
                    
                    if not worker_needs_input:
                        # Also, don't pass in the multp data_dict, so will use the global.
                        # Fix if in global
                        logger.debug(thread_name + ": process_faninNBs: faninNB Results: ")
                        for key, value in dict_of_results.items():
                            logger.debug(str(key) + " -> " + str(value))
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
                        if not using_threads_not_processes:
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

                        logger.debug(thread_name + ": process_faninNBs: add faninNB Results to data dict: ")
                        for key, value in dict_of_results.items():
                            data_dict[key] = value
                            logger.debug(str(key) + " -> " + str(value))

                        DAG_exec_state.state = start_state_fanin_task
#rhc: cluster:
                        cluster_queue.put(DAG_exec_state.state)
#rhc: cluster:
                        # Do this since we get it as return value
                        worker_needs_input = False
                        logger.debug(thread_name + ": process_faninNBs:  Got work (become task), added it to data_dict and cluster_queue, set worker_needs_input to False: "
                            + " start_state_fanin_task: " + str(start_state_fanin_task))

                else: 
                    # not using_workers so we are using threads to simulate using lambdas.
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
                    if worker_needs_input:
                        logger.error("[Error]: " + thread_name + ": process_faninNBs: Internal Error: not using_workers but worker_needs_input = True")
                    
                    try:
                        logger.debug(thread_name + ": process_faninNBs: starting DAG_executor thread for task " + name + " with start state " + str(start_state_fanin_task))
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
                            "DAG_executor_state": new_DAG_executor_state,
                            #"DAG_info": DAG_info,
                            #"server": server
                        }
                        thread_name_prefix = "Thread_faninNB_"
                        thread = threading.Thread(target=DAG_executor_task, name=(thread_name_prefix+str(start_state_fanin_task)), args=(payload,))
                        thread.start()
                        #_thread.start_new_thread(DAG_executor.DAG_executor_task, (payload,))
                    except Exception as ex:
                        logger.error("[ERROR]:" + thread_name + ": process_faninNBs: Failed to start DAG_executor thread.")
                        logger.debug(ex)

                    # using_workers is false so worker_needs_input should never be true
                    #else:
                    #   worker_needs_input = False
                    #   DAG_exec_state.state = start_state_fanin_task
                    #   logger.debug("process_faninNBs: set worker_needs_input to False,")

                #return 1
                #logger.debug("process_faninNBs: returning worker_needs_input: " + str(worker_needs_input))
                #return worker_needs_input
    # return value not used; will process any fanouts next; no change to DAG_executor_State
    #return 0
    logger.debug(thread_name + ": process_faninNBs:  returning worker_needs_input: " + str(worker_needs_input))
    return worker_needs_input

#def faninNB_remotely_batch(websocket, faninNBs, faninNB_sizes, calling_task_name, DAG_states, 
#    DAG_exec_state, output, DAG_info, work_queue, worker_needs_input, **keyword_arguments):
# Called by process_faninNBs_batch; sends the faninNBs and more to the server
def faninNB_remotely_batch(websocket, **keyword_arguments):
    #Todo: remove DAG_exec_state from parm list
    thread_name = threading.current_thread().name
    logger.debug (thread_name + " faninNB_remotely_batch: calling_task_name: " + keyword_arguments['calling_task_name'] 
        + " calling process_faninNBs_batch with fanin_task_names: " + str(keyword_arguments['faninNBs']))

    # rhc: DES
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
    # rhc: run task
    # fanouts are used when sync objects trigger their tasks to run in the same lambda as object
    DAG_exec_state.keyword_arguments['fanouts'] = keyword_arguments['fanouts']
    DAG_exec_state.keyword_arguments['faninNB_sizes'] = keyword_arguments['faninNB_sizes']
    DAG_exec_state.keyword_arguments['worker_needs_input'] = keyword_arguments['worker_needs_input']
    DAG_exec_state.keyword_arguments['work_queue_name'] = keyword_arguments['work_queue_name']
    DAG_exec_state.keyword_arguments['work_queue_type'] = keyword_arguments['work_queue_type']
    DAG_exec_state.keyword_arguments['work_queue_method'] = keyword_arguments['work_queue_method']
    DAG_exec_state.keyword_arguments['work_queue_op'] = keyword_arguments['work_queue_op']
    DAG_exec_state.keyword_arguments['DAG_states_of_faninNBs_fanouts'] = keyword_arguments['DAG_states_of_faninNBs_fanouts']
#rhc: batch
    DAG_exec_state.keyword_arguments['all_faninNB_sizes_of_faninNBs'] = keyword_arguments['all_faninNB_sizes_of_faninNBs']
    DAG_exec_state.keyword_arguments['all_fanin_sizes_of_fanins'] = keyword_arguments['all_fanin_sizes_of_fanins']

    # we now read DAG_info on the tcp_server when not creating objects ata the start
    # or not run_all_tasks_locally (i.e., lambdas excute tasks and need the DAG_info.)
    #if not run_all_tasks_locally:
        # Note: When faninNB start a Lambda, DAG_info is in the payload so pass DAG_info to faninNBs.
        # (Threads and processes read it from disk.)
    #    DAG_exec_state.keyword_arguments['DAG_info'] = keyword_arguments['DAG_info']
    DAG_exec_state.keyword_arguments['number_of_tasks'] = keyword_arguments['number_of_tasks']

    # piggybacking the (possibly empty) list if work_tuples generated by the fanouts, if any
    DAG_exec_state.keyword_arguments['list_of_work_queue_or_payload_fanout_values'] = keyword_arguments['list_of_work_queue_or_payload_fanout_values']
#rhc: async batch
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
    DAG_exec_state = synchronize_process_faninNBs_batch(websocket, "synchronize_process_faninNBs_batch", FanInNB_Type, "fan_in", DAG_exec_state)
    return DAG_exec_state

# Called when we are storing fanins and faninNBs remotely and we are using_workers and we are 
# using processes instead of threads. This batches the faninNB processing.
# If we are storing fanins and faninNBs remotely and we are not using_workers then we are 
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
#rhc: cluster:
    cluster_queue,
#rhc: batch
    all_faninNB_task_names,all_faninNB_sizes):

    thread_name = threading.current_thread().name
    logger.debug(thread_name + ": process_faninNBs_batch: " + calling_task_name)
    logger.debug(thread_name + ": process_faninNBs_batch: " + calling_task_name + ": worker_needs_input: " + str(worker_needs_input))
    # rhc: async batch
    logger.debug(thread_name + ": process_faninNBs_batch: " + calling_task_name + ": async_call: " + str(async_call))

    #assert: if worker needs work then we should be using synch call so we can check the results for work
    if worker_needs_input:
        if async_call:
            logger.debug(thread_name + "[Error]: Internal Error: process_faninNBs_batch: worker_needs_input but using async_call")

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
    keyword_arguments['store_fanins_faninNBs_locally'] = store_fanins_faninNBs_locally
    # Need on tcp_server to process the faninNBs
    keyword_arguments['faninNBs'] = faninNBs
# rhc: run task
    # fanouts are used when sync objects trigger their tasks to run in the same lambda as object
    keyword_arguments['fanouts'] = fanouts
    keyword_arguments['faninNB_sizes'] = faninNB_sizes   
    keyword_arguments['worker_needs_input'] = worker_needs_input  
    keyword_arguments['work_queue_name'] = "process_work_queue"
    # defined in DAG_executor_constants: either select or non-select version
    keyword_arguments['work_queue_type'] = process_work_queue_Type 
    keyword_arguments['work_queue_method'] = "deposit_all"
    keyword_arguments['work_queue_op'] = "synchronize_async"
    keyword_arguments['DAG_info'] = DAG_info
    # get a slice of DAG_states that is the DAG states of just the faninNB and fanout tasks.
    # Instead of sending all the DAG_states, i.e., all the states in the DAG, to the server.
    # Need this to put any faninNB work that is not returned in the work_queue - work is added as
    # a tuple (start state of task, inputs to task). Fanouts are used when sync objects trigger 
    # their tasks to run in the same lambda as object. This occurs when we are storing objects 
    # in lambdas and running tcp_server_lamba.py
    DAG_states_of_faninNBs_fanouts = {}
#rhc: batch
    all_faninNB_sizes_of_faninNBs = []
    # We only batch process faninNBs, not fanins, so this is empty
    all_fanin_sizes_of_fanins = []
    for name in faninNBs:
        DAG_states_of_faninNBs_fanouts[name] = DAG_states[name]
#rhc: batch
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
# rhc: async batch
    keyword_arguments['async_call'] = async_call


 	#ToDo: kwargs put in DAG_executor_State keywords and on server it gets keywords from state and passes to create and fanin

    # This create_all_fanins_faninNBs_on_start is for creating sync objects
    # on the tcp_server. If we store_sync_objects_in_lambdas and 
    # not create_all_fanins_faninNBs_on_start the tcp_server_lambda or
    # or the lambdas themselves will create the object to be stored in them.
    if not create_all_fanins_faninNBs_on_start and (
            not store_sync_objects_in_lambdas):
        # create_and_faninNB_remotely_batch simply calls faninNB_remotely_batch(websocket,**keyword_arguments)
        # All the create logic is on the server, so we call faninNB_remotely_batch in
        # either case. We leave this call to create_and_faninNB_remotely_batch 
        # as a placeholder in case we decide to do smething differet - 
        # as in create_and_faninNB_remotely_batch could create and pass a new message to 
        # the server, i.e., move the create logic down from the server.
        logger.debug(thread_name + ": process_faninNBs_batch: call create_and_faninNB_remotely_batch.") 
        dummy_DAG_exec_state = create_and_faninNB_remotely_batch(websocket,**keyword_arguments)
    else:
        logger.debug(thread_name + ": process_faninNBs_batch: call faninNB_remotely_batch.") 
        dummy_DAG_exec_state = faninNB_remotely_batch(websocket,**keyword_arguments)

    #if DAG_exec_state.blocking:
    # using the "else" after the return, even though we don't need it
    if dummy_DAG_exec_state.return_value == 0:
        #DAG_exec_state.blocking = False
        # nothing to do; if worker_needs_input is True then there was no input to be gotten
        # from the faninNBs, i.e., we were not the last task to call fan_in for any faninNB in the batch.
        logger.debug(thread_name + ": process_faninNBs_batch: " + calling_task_name + " received no work with worker_needs_input: " + str(worker_needs_input))
        return worker_needs_input
    else:
        # we only get work from process_faninNBs_batch() when we are using workers and worker needs input,
        # or we are simulating lambas and storing synch objects remotely. (If we are using threads simulating lambdas
        # and storing synch objects locally, the FaninNBs start threads that simulate lambdas.
        # (run_all_tasks_locally and using_workers and not using_threads_not_processes) or (not run_all_tasks_locally) or (run_all_tasks_locally and not using_workers and not store_fanins_faninNBs_locally and using_Lambda_Function_Simulators_to_Store_Objects):reads (locally) to run the fan_in tasks.)

#rhc: async task ToDo: don't need to be using sims
        if not (run_all_tasks_locally and using_workers and not using_threads_not_processes and worker_needs_input) and not (run_all_tasks_locally and not using_workers and not store_fanins_faninNBs_locally and (
            using_Lambda_Function_Simulators_to_Store_Objects) and not sync_objects_in_lambdas_trigger_their_tasks):
            logger.error("[Error]: " + thread_name + ": process_faninNBs_batch: (not using worker processes, or worker_needs_input is False) and not (storing synch objects remotely in simulated lambdas"
                + " and using threads to simulate lambdas) but using process_faninNBs batch and we got work back from server.")

        if using_workers:
            # return_value is a tuple (start state fanin task, dictionary of fanin results)
            start_state_fanin_task = dummy_DAG_exec_state.return_value[0]
            fanin_task_name = DAG_info.get_DAG_map()[start_state_fanin_task].task_name
            logger.debug(thread_name + ": process_faninNBs_batch: " + calling_task_name + " received work for fanin task " + fanin_task_name 
                + " and start_state_fanin_task " + str(start_state_fanin_task) + " with worker_needs_input: " + str(worker_needs_input))
            # This must be true since we only call process_faninNBs_batch if this is true; otherwise, we call process_faninNBs
            # to process a single faninNB. Note: when we use Lambdas we do not use workers; instead, the faninNBs
            # create a new Lambda to excute the fanin task. No work is enqueued for a pool of Lambdas.
            # Leaving the if here for now.
#rhc: simulated threads: move if up
            #if using_workers:
            # worker can be a thread of a process
            dict_of_results = dummy_DAG_exec_state.return_value[1]
            if not worker_needs_input:
                # Note: asserted using_workers and worker_needs_input above so ...
                # 
                # This should be unreachable; leaving it for now.
                # If we don't need work then any work from faninNBs should have been enqueued in work queue instead of being returned
                logger.error("[Error]: " + thread_name + ": process_faninNBs_batch: Internal Error: got work but not worker_needs_input.")
                
                # Also, don't pass in the multprocessing data_dict, so will use the global data dict
                logger.debug(thread_name + ": process_faninNBs_batch: " + calling_task_name + ": process_faninNBs_batch Results: ")
                for key, value in dict_of_results.items():
                    logger.debug(str(key) + " -> " + str(value))
                #thread_work_queue.put(start_state_fanin_task)
                if not using_threads_not_processes:
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
#rhc: cluster:
                cluster_queue.put(DAG_exec_state.state)
#rhc: cluster: Do this
                # keep work and do it next
                worker_needs_input = False
                logger.debug(thread_name + ": process_faninNBs_batch: " + calling_task_name + ": got work, added it to data_dict and cluster_queue, set worker_needs_input to False.")
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

            not run_all_tasks_locally ==> no workers
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

            # Not using_workers so we are using threads to simulate using lambdas.
            # However, since the faninNB on tcp_server cannot crate a thread to execute
            # the fanin task (since the thread would run on the tcp_server) the start
            # state is returned and we create the thread here. This deviates from using
            # real lambdas since in that case the faninNB on the tcp_server can invoke 
            # a real lamda to execute the fanin task.

            # if not using_workers then not worker_needs_input must be true. That is, we init worker_needs_input
            # to false and we never set it to true since setting worker_needs_input is guarded everywhere by using_workers.
            if worker_needs_input:
                logger.error("[Error]:" + thread_name + ": process_faninNBs_batch: Internal Error: not using_workers but worker_needs_input = True")

            list_of_work_tuples = dummy_DAG_exec_state.return_value
            # Note: On tcp_server, a work tupe is : work_tuple = (start_state_fanin_task,returned_state)
            # where returned_state is the DAG_executor_State that gets returned when you call fan_in. 
            # In this state, the return_value is the dictionary of results from the fan_in

            for work_tuple in list_of_work_tuples:
                start_state_fanin_task = work_tuple[0]
                fanin_task_name = DAG_info.get_DAG_map()[start_state_fanin_task].task_name
                logger.debug(thread_name + ": process_faninNBs_batch: " + calling_task_name + " received work for fanin task " + fanin_task_name 
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
                    logger.debug(thread_name + ": process_faninNBs_batch: " + calling_task_name + ": starting DAG_executor thread for task " + name + " with start state " + str(start_state_fanin_task))
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
                except Exception as ex:
                    logger.error("[ERROR]: " + thread_name + ": process_faninNBs_batch: Failed to start DAG_executor thread.")
                    logger.debug(ex)

            # using_workers is false so worker_needs_input should never be true
            #else:
            #    worker_needs_input = False
            #   DAG_exec_state.state = start_state_fanin_task
            #   logger.debug("process_faninNBs: set worker_needs_input to False,")

        #return 1
        logger.debug(thread_name + ": process_faninNBs_batch:  returning worker_needs_input: " + str(worker_needs_input))
        return worker_needs_input
    logger.debug(thread_name + ": process_faninNBs_batch:  returning worker_needs_input: " + str(worker_needs_input))
    return worker_needs_input

#Todo: Global fanin object in tcp_Server, which determines whether last caller or not, and delegates
#      collection of results to local fanins in infinistore executors.

#def process_fanouts(fanouts, calling_task_name, DAG_states, DAG_exec_State, output, DAG_info, server):
def  process_fanouts(fanouts, calling_task_name, DAG_states, DAG_exec_State, 
    output, DAG_info, server, work_queue, list_of_work_queue_values):

    thread_name = threading.current_thread().name

    logger.debug(thread_name + ": process_fanouts: length is " + str(len(fanouts)))

    #process become task
    become_task = fanouts[0]
    logger.debug(thread_name + ": process_fanouts: fanout for " + calling_task_name + " become_task is " + become_task)
    # Note:  We will keep using DAG_exec_State for the become task. If we are running everything on a single machine, i.e.,
    # no server or Lambdas, then faninNBs should use a new DAG_exec_State. Fanins and fanouts/faninNBs are mutually exclusive
    # so, e.g., the become fanout and a fanin cannot use the same DAG_exec_Stat. Same for a faninNb and a fanin, at least
    # as long as we do not generate faninNBs and a fanin for the same state/task. We could optimize so that we can have
    # a fanin (with a become task and one or more faninNBs.
    become_start_state = DAG_states[become_task]
    # change state for this thread so that this thread will become the new task, i.e., execute another iteration with the new state
    DAG_exec_State.state = DAG_states[become_task]

    logger.debug (thread_name + ": process_fanouts: fanout for " + calling_task_name + " become_task state is " + str(become_start_state))
    fanouts.remove(become_task)
    logger.debug(thread_name + ": process_fanouts: new fanouts after remove:" + str(fanouts))

    # process rest of fanouts
    logger.debug(thread_name + ": process_fanouts: run_all_tasks_locally:" + str(run_all_tasks_locally))

    for name in fanouts:
        if using_workers:
            #thread_work_queue.put(DAG_states[name])
            if not using_threads_not_processes: # using processes
                dict_of_results =  {}
                if same_output_for_all_fanout_fanin:
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
                logger.debug(thread_name + ": process_fanouts: dict_of_results for fanout " + name)
                logger.debug(str(dict_of_results))
                #dict_of_results[calling_task_name] = output
                work_tuple = (DAG_states[name],dict_of_results)
                #work_queue.put(DAG_states[name])

                # We will be batch processing the faninNBs so we will also batch process
                # the fanouts at the same time. If there are any faninBs in this state
                # piggyback the fanouts on the call to process faninNBs. If there are 
                # no faninNBs we will send this list to the work_queue directly. Note:
                # we could check at the end of this method whether there are any
                # faninNBs and if, not, call work_queue.put.

                list_of_work_queue_values.append(work_tuple)
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
                if same_output_for_all_fanout_fanin:
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
                logger.debug(thread_name + ": process_fanouts: dict_of_results for fanout " + name)
                logger.debug(str(dict_of_results))
                work_tuple = (DAG_states[name],dict_of_results)
                #work_queue.put(DAG_states[name])
                work_queue.put(work_tuple)
        else:
#rhc run tasks
            if (not run_all_tasks_locally) and store_sync_objects_in_lambdas and sync_objects_in_lambdas_trigger_their_tasks:
            #if (run_all_tasks_locally and not using_workers and not store_fanins_faninNBs_locally and store_sync_objects_in_lambdas and sync_objects_in_lambdas_trigger_their_tasks):
                # Nothing to do. When we are sync_objects_in_lambdas_trigger_their_tasks we will
                # pass the fanouts (set) to process_faninNBs_batch and it will process the 
                # fanouts by invoking real python functions that execute the fanout tasks.
                pass
            elif run_all_tasks_locally:
            #elif (run_all_tasks_locally and not using_workers and not store_fanins_faninNBs_locally and not (store_sync_objects_in_lambdas and sync_objects_in_lambdas_trigger_their_tasks)):
                # (run_all_tasks_locally and not using_workers and not store_fanins_faninNBs_locally and using_Lambda_Function_Simulators_to_Store_Objects):
                # or (run_all_tasks_locally and not using_workers and not store_fanins_faninNBs_locally and using_Lambda_Function_Simulators_to_Store_Objects and sync_objects_in_lambdas_trigger_their_tasks):
                # Note: if we are using threads to simulate lambas it does not matter whether or not we are storing 
                # objects in python functions, we will invoke a new thread to execute the fanout task. But if
                # we are running tasks in python functions that simulate lambdas, we will call process_faninNBS_batch
                # to deal with the fanouts. (If we are using real lambdas, we will eventually do this same thing.)
                try:
                    logger.debug(thread_name + ": process_fanouts: Starting fanout DAG_executor thread for " + name)
                    fanout_task_start_state = DAG_states[name]
                    # rhc: DES
                    task_DAG_executor_State = DAG_executor_State(function_name = "DAG_executor", function_instance_ID = str(uuid.uuid4()), state = fanout_task_start_state)

                    # Note: if not same_output_for_all_fanout_fanin then
                    # we would need to extract the fanout's particular
                    # output from output and pass it in payload. See the 
                    # code for this above where we are using workers.
                    # as in:
                    """
                    dict_of_results =  {}
                    if same_output_for_all_fanout_fanin:
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
                    logger.debug(thread_name + ": process_fanouts: dict_of_results for fanout " + name)
                    logger.debug(str(dict_of_results))
                    # Below we would use: "input": dict_of_results,
                    """

                    #output_tuple = (calling_task_name,)
                    #output_dict[calling_task_name] = output
                    logger.debug (thread_name + ": process_fanouts: fanout payload for " + name + " is " + str(fanout_task_start_state) + "," + str(output))
                    payload = {
                        #"state": fanout_task_start_state,
                        # If not using workers but running tasks locally then we are using threads
                        # to simulate Lambdas but threads currently use a global data_dict so they
                        # just put task outputs in the data_dict. We could pass "inp" in this case
                        # just to check the logic used by the Lambdas.
                        # The driver just passes the dag executor state. We do not use
                        # server, we input DAG_info from file. 

                        # We do not currently use the input, 
                        # but may use it to be consistent with lambdas: ==> pass state and input
                        # to the threads that simulate Lambdas. (We could also pass DAG_info to be
                        # consistent with Lambda version.)
                        #
                        # See the note above about dict_of_results.
                        #
                        #"input": output,
                        "DAG_executor_state": task_DAG_executor_State,
                        #"DAG_info": DAG_info,
                        #"server": server
                    }
                    _thread.start_new_thread(DAG_executor_task, (payload,))
                except Exception as ex:
                    logger.error("[ERROR] " + thread_name + ": process_fanouts: Failed to start DAG_executor thread for " + name)
                    logger.debug(ex)
#rhc run tasks changed to elif so can use else to check whether we aer missing a case
            elif not run_all_tasks_locally:
                try:
                    logger.debug(thread_name + ": process_fanouts: Starting fanout DAG_executor Lambda for " + name)
                    fanout_task_start_state = DAG_states[name]
                    # create a new DAG_executor_State object so no DAG_executor_State object is shared by fanout/faninNB threads in a local test.
                    lambda_DAG_executor_state = DAG_executor_State(function_name = "DAG_Executor_Lambda", function_instance_ID = str(uuid.uuid4()), state = fanout_task_start_state)
                    logger.debug (thread_name + ": process_fanouts: payload is DAG_info + " + str(fanout_task_start_state) + ", " + str(output))
                    lambda_DAG_executor_state.restart = False      # starting new DAG_executor in state start_state_fanin_task
                    lambda_DAG_executor_state.return_value = None
                    lambda_DAG_executor_state.blocking = False
                    logger.info(thread_name + ": process_fanouts: Starting Lambda function %s." % lambda_DAG_executor_state.function_name)
                    #logger.debug("lambda_DAG_executor_State: " + str(lambda_DAG_executor_State))

                    dict_of_results =  {}
                    if same_output_for_all_fanout_fanin:
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
                    logger.debug(thread_name + ": process_fanouts: dict_of_results for fanout " + name)
                    logger.debug(str(dict_of_results))

                    #results = {}
                    #results[calling_task_name] = output

                    payload = {
#ToDo: Lambda:          # use parallel invoker with list piggybacked on batch fanonNBs, as usual
                        #"state": int(fanout_task_start_state),
                        #"input": output,
                        #"input": results,
                        "input": dict_of_results,
                        "DAG_executor_state": lambda_DAG_executor_state,
                        "DAG_info": DAG_info
                        #"server": server   # used to mock server during testing
                    }
                    ###### DAG_executor_State.function_name has not changed
                    invoke_lambda_DAG_executor(payload = payload, function_name = "DAG_Executor_Lambda")
                except Exception as ex:
                    logger.error(":ERROR] " + thread_name + " process_fanouts: Failed to start DAG_executor Lambda.")
                    logger.error(ex)
#rhc: run task
            else:
                logger.error(":ERROR] " + thread_name + " Internal Error: process_fanouts: invalid configuration.")

    # Note: If we do not piggyback the fanouts with process_faninNBs_batch, we would add
    # all the work to the remote work queue here.
    #if using_workers and not using_threads_not_processes:
    #   work_queue.put_all(list_of_work_queue_or_payload_fanout_values)

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
    #store_fanins_faninNBs_locally = keyword_arguments['store_fanins_faninNBs_locally']  # option set in DAG_executor
    #DAG_info = keyword_arguments['DAG_info']

    thread_name = threading.current_thread().name

    logger.debug (thread_name + ": fanin_remotely: calling_task_name: " + keyword_arguments['calling_task_name'] + " calling synchronize_sync fanin with fanin_task_name: " + keyword_arguments['fanin_task_name'])

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

    logger.debug (thread_name + ": fanin_remotely: calling_task_name: " + keyword_arguments['calling_task_name'] + " back from synchronize_sync")
    logger.debug (thread_name+ ": fanin_remotely: returned DAG_exec_state.return_value: " + str(DAG_exec_state.return_value))
    return DAG_exec_state

def process_fanins(websocket,fanins, faninNB_sizes, calling_task_name, DAG_states, DAG_exec_state, output, server):
    thread_name = threading.current_thread().name
    logger.debug(thread_name + ": process_fanins: calling_task_name: " + calling_task_name)

    # assert len(fanins) == len(faninNB_sizes) ==  1

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
    logger.debug(thread_name + ": process_fanins: fanins" + str(fanins))
    # there is only one fanin name
    keyword_arguments['fanin_task_name'] = fanins[0]
    keyword_arguments['n'] = faninNB_sizes[0]
    #keyword_arguments['start_state_fanin_task'] = DAG_states[fanins[0]]
    #keyword_arguments['result'] = output
    #keyword_arguments['calling_task_name'] = calling_task_name
    if same_output_for_all_fanout_fanin:
        keyword_arguments['result'] = output
        keyword_arguments['calling_task_name'] = calling_task_name
    else:
        name = fanins[0]
        logger.debug("**********************" + thread_name + ": process_fanins:  for " + calling_task_name + " faninNB "  + name + " output is :" + str(output))

        #if name.endswith('L'):  
        #    keyword_arguments['result'] = output[name[:-1]]
        #    qualified_name = str(calling_task_name) + "-" + str(name[:-1])
        #else:
        keyword_arguments['result'] = output[name]
        qualified_name = str(calling_task_name) + "-" + str(name)

        logger.debug(thread_name + ": process_fanins: name:" + str(name) 
            + " qualified_name: " + qualified_name)
        keyword_arguments['calling_task_name'] = qualified_name
    # Don't do/need this.
    #keyword_arguments['DAG_executor_State'] = DAG_exec_state
    keyword_arguments['server'] = server

    if not store_fanins_faninNBs_locally:
        # These DAG_exec_state keyword_arguments are passed to the fan_in 
        # operation when we use remote fanin objects
        DAG_exec_state.keyword_arguments = {}
        DAG_exec_state.keyword_arguments['fanin_task_name'] = fanins[0]
        DAG_exec_state.keyword_arguments['n'] = faninNB_sizes[0]
        #DAG_exec_state.keyword_arguments['result'] = output
        #DAG_exec_state.keyword_arguments['calling_task_name'] = calling_task_name
        if same_output_for_all_fanout_fanin:
            DAG_exec_state.keyword_arguments['result'] = output
            DAG_exec_state.keyword_arguments['calling_task_name'] = calling_task_name
        else:
            name = fanins[0]
            logger.debug("**********************" + thread_name + ": process_fanins:  for " + calling_task_name + " faninNB "  + name + " output is :" + str(output))

            #if name.endswith('L'):  
            #    keyword_arguments['result'] = output[name[:-1]]
            #    qualified_name = str(calling_task_name) + "-" + str(name[:-1])
            #else:
            DAG_exec_state.keyword_arguments['result'] = output[name]
            qualified_name = str(calling_task_name) + "-" + str(name)

            logger.debug(thread_name + ": process_fanins: name:" + str(name) 
                + " qualified_name: " + qualified_name)
            DAG_exec_state.keyword_arguments['calling_task_name'] = qualified_name
        #DAG_exec_state.keyword_arguments['DAG_executor_State'] = DAG_exec_state
        DAG_exec_state.keyword_arguments['server'] = server
#rhc batch
        # Note: using this remotley on tcp_server in synhronize_sync
        # but not using it locally. When create fanin/faninNB locally
        # we call a create fanin/faninNB method which knows to create
        # a fanin/faninNB
        DAG_exec_state.keyword_arguments['fanin_type'] = "fanin"

    if store_fanins_faninNBs_locally:
        #ToDo:
        #keyword_arguments['DAG_executor_State'] = DAG_exec_state
        # The keyword arguments aer passed to the local fanin 
        # no the DAG_executor_state
        if not create_all_fanins_faninNBs_on_start:
            DAG_exec_state = server.create_and_fanin_locally(DAG_exec_state,keyword_arguments)
        else:
            logger.debug(thread_name + ": process_fanins: " + calling_task_name + ": call server.fanin_locally")
            DAG_exec_state = server.fanin_locally(DAG_exec_state,keyword_arguments)
    else:
#rhc: ToDo: when not create_all_fanins_faninNBs_on_start create the object
# on the server, like we do for the enqueue case. If storing objects in 
# lambda we have to be in the lmabda when we creae the object, i.e.,
# in message_handler_lambda, so when create objects on server put the
# create in msg_handler's synchronize sync to be consistent (rather
# than in tcp_server?)

        if not create_all_fanins_faninNBs_on_start:
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
            logger.debug (thread_name + ": process_fanins: process_fanins: call to create_and_fanin_remotely returned DAG_exec_state.return_value: " + str(DAG_exec_state.return_value))
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
            logger.debug (thread_name + ": process_fanins: process_fanins: call to fanin_remotely returned DAG_exec_state.return_value: " + str(DAG_exec_state.return_value))

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

#rhc: counter
# tasks_completed_counter, workers_completed_counter
def DAG_executor_work_loop(logger, server, completed_tasks_counter, completed_workers_counter, DAG_executor_state, DAG_info, 
#rhc continue
    work_queue,DAG_infobuffer_monitor):

    DAG_map = DAG_info.get_DAG_map()
    DAG_tasks = DAG_info.get_DAG_tasks()
    DAG_number_of_tasks = DAG_info.get_DAG_number_of_tasks()

#rhc continue
    num_tasks_to_execute = -1
    if (not using_workers):
        # not using this value when using real or simulated lambdas.
        # this values tells workers when there are no morfe tasks to execute
        #num_tasks_to_execute = len(DAG_tasks)
        num_tasks_to_execute = DAG_number_of_tasks
    else:
        if not (compute_pagerank and use_incremental_DAG_generation):
            #num_tasks_to_execute = len(DAG_tasks)
            num_tasks_to_execute = DAG_number_of_tasks
        else: # using incremental DAG generation
            if not use_page_rank_group_partitions:
                # using partitions
                if not DAG_info.get_DAG_info_is_complete():
                    #num_tasks_to_execute = len(DAG_tasks) - 1
                    number_of_incomplete_tasks = DAG_info.get_DAG_number_of_incomplete_tasks()
                    if not number_of_incomplete_tasks == 1:
                        logger.error("[Error]: Internal Error: DAG_executor_work_loop at start:"
                            + " Using incremental DAG generation with partitions and"
                            + " DAG is incomplete but number_of_incomplete_tasks is not 1: "
                            + str(number_of_incomplete_tasks))
                    num_tasks_to_execute = DAG_number_of_tasks -1
                    logger.debug("DAG_executor_work_loop: at start: DAG_info complete num_tasks_to_execute: " + str(num_tasks_to_execute))
                else:
                    #num_tasks_to_execute = len(DAG_tasks)
                    num_tasks_to_execute = DAG_number_of_tasks
                    logger.debug("DAG_executor_work_loop: at start: DAG_info complete num_tasks_to_execute: " + str(num_tasks_to_execute))
            else:
                # using groups
                if not DAG_info.get_DAG_info_is_complete():
                    number_of_incomplete_tasks = DAG_info.get_DAG_number_of_incomplete_tasks()
                    num_tasks_to_execute = DAG_number_of_tasks - number_of_incomplete_tasks
                    logger.debug("DAG_executor_work_loop: at start: DAG_info not complete: new num_tasks_to_execute: " 
                        + str(num_tasks_to_execute) + " with number_of_incomplete_tasks "
                        + str(number_of_incomplete_tasks))                
                else:
                    #num_tasks_to_execute = len(DAG_tasks)
                    num_tasks_to_execute = DAG_number_of_tasks
                    logger.debug("DAG_executor_work_loop: at start: DAG_info complete num_tasks_to_execute: " + str(num_tasks_to_execute))

    logger.debug("DAG_executor: length of DAG_tasks: " + str(DAG_number_of_tasks)
        + " number of tasks in DAG to execute: " + str(num_tasks_to_execute))
    #server = payload['server']
    proc_name = multiprocessing.current_process().name
    thread_name = threading.current_thread().name
    logger.debug("DAG_executor_work_loop: proc " + proc_name + " " + " thread " + thread_name + ": started.")

    #ToDo:
    #if input == None:
        #pass  # withdraw input from payload.synchronizer_name
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as websocket:
        global store_fanins_faninNBs_locally
        # Config: A1, A4_remote, A5, A6
        if not store_fanins_faninNBs_locally:
            logger.debug("DAG_executor " + thread_name + " connecting to TCP Server at %s." % str(TCP_SERVER_IP))
            websocket.connect(TCP_SERVER_IP)
            logger.debug("DAG_executor " + thread_name + " successfully connected to TCP Server.")
        # else: # Config: A2, A4_local
        # on the client side
        #print("socketname: " + websocket.getsockname())   # ->  (127.0.0.1,26386)
        #print(websocket.getpeername())   # ->  (127.0.0.1, 8888)

        # ... unless its this work_queue when we use processes. (Lambdas do not use a work_queue, for now):)
        if (run_all_tasks_locally and using_workers and not using_threads_not_processes): 
            # Config: A5, A6
            # sent the create() for work_queue to the tcp server in the DAG_executor_driver
            #
            # each thread in multithreading multiprocesssing needs its own socket.
            # each process when single threaded multiprocessing needs its own socket.
            work_queue = Work_Queue_Client(websocket,2*num_tasks_to_execute)

#rhc continue
            # we are only using incremental_DAG_generation when we
            # are computing pagerank, so far. Pagerank DAGS are the
            # only DAGS we generate ourselves, so far.
            if compute_pagerank and use_incremental_DAG_generation:
                DAG_infobuffer_monitor = Remote_Client_for_DAG_infoBuffer_Monitor(websocket)

#rhc continue
# ToDo: need to create the remote DAG_infobuffer_monitor. The remote
#       work queue is created by the DAG_executor_driver since the driver
#       needs to deposit leaf task work in the work queue before
#       creating the worker processes.
#       We can create it here? or let the driver create it and 
#       just make calls to deposit and withdraw in DAG_executor,
#       as we do for the work queue.
#       Note: the driver either creates the work queue at the start
#       in which case is does not need to call create() or the
#       fanins/fanouts/faninNBs are crated on demand in which case
#       since the driver needs to use the work queue it calls create().
#

        #else: # Config: A1, A2, A3, A4_local, A4_Remote

#rhc: cluster:
        # worker_needs_input initialized to false and stays false
        # for non-worker configs. Set worker_needs_input based on
        # cluster_queue is empty test below when using workers.
        # Note: if not using_workers then worker_needs_input is initialized 
        # to False and every set of worker_needs_input to True is guarded 
        # by "if using_workers" so worker_needs_input is never
        # set to True if not using_workers.
        #
        #worker_needs_input = using_workers # set to False and stays False
        worker_needs_input = False # set to False and stays False if not using workers


 
#rhc: cluster:
        cluster_queue = queue.Queue()
#rhc continue 
        continued_task = False
        process_continue_queue = False
        continue_queue = None
        if using_workers and compute_pagerank and use_incremental_DAG_generation:
            continue_queue = queue.Queue()

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
        if (run_all_tasks_locally and not using_workers) or not run_all_tasks_locally:
            # simulated or real lmabdas are started with a payload that has a state,
            # put this state in the cluster_queue.
            cluster_queue.put(DAG_executor_state.state)
            logger.debug("DAG_executor_work_loop: start of simulated or real lambda:"
                + " put state " + str(DAG_executor_state.state) + " in cluster_queue.")
            
        # An outline of this main loop, which processes tasks and their fanins/fanouts/collapses
        """
        while (True): # main work loop: iterate until worker/lambda is finished

            if using workers:
                if process_continue_queue:
                    we are getting states from the continue queue
                    until the continue queue is empty. 
                    DAG_executor_state.state = continue_queue.get()
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
                                            if using_workers:
                                                continue_queue.put(DAG_executor_state.state)
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
                        if compute_pagerank and use_incremental_DAG_generation:
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
                                    DAG_executor_state.state = continue_queue.get()
                                    if continue_queue.qsize() == 0:
                                        # only one task/state was in continue_queue
                                        process_continue_queue = False
                                    else:
                                        # process tasks/states in continue_queue
                                        logger.debug("DAG_executor_work_loop: XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXwork_loop: continue_queue.qsize()>0 set process_continue_queue = True.")
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
                incremental_dag_generation_with_groups = compute_pagerank and use_incremental_DAG_generation and use_page_rank_group_partitions
                # get the state info for this task/state
                continued_task_state_info = DAG_map[DAG_executor_state.state]
                # execute the task if this is False:
                # (1) we are using groups       # if we are using partitions we execute the task
                # and (2) this is a continued task     # if this is not a continued task we execute the task
                # and (3) this is leaf task PR1_1 or this is not any other leaf task
                if not (incremental_dag_generation_with_groups and continued_task and (
                    (continued_task_state_info.task_name == name_of_first_groupOrpartition_in_DAG or not continued_task_state_info.task_name in DAG_info.get_DAG_leaf_tasks())
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
                        if not using_threads_not_processes:
                            # Config: A5, A6
                            logger.debug(thread_name + ": DAG_executor: num_tasks_executed == num_tasks_to_execute: depositing -1 in work queue.")
                            work_tuple = (-1,None)
                            # for the next worker
                            work_queue.put(work_tuple)
                        else:
                            # Config: A4_local, A4_Remote
                            logger.debug(thread_name + ": DAG_executor: num_tasks_executed == num_tasks_to_execute: depositing -1 in work queue.")
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
                # payload has a state to execute. Ths state is added to the 
                # cluster_queue at the start of the work loop. Lambdas may also
                # add collpased states to the cluster queue and fanout/fanin
                # become states are also added to the cluster queue. Real and 
                # simulated lambdas do not use a work_queue, and they do 
                # not use a continue_queue for incremental DAG generation.
                # So there is only a cluster queue. 
                #
                # The cluster_queue will have the lambdas payload task added
                # to it and so will not be empty the first time we check here
                # on the cluster_queue size. When processing the tasks, we may
                # or may not add become tasks for fanouts and fanins and 
                # collapsed tasks to the custer_queue, so the cluster_queue
                # may be empty on later checks.
                if cluster_queue.qsize() > 0:
                    DAG_executor_state.state = cluster_queue.get()

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
            incremental_dag_generation_with_groups = compute_pagerank and use_incremental_DAG_generation and use_page_rank_group_partitions

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
            (state_info.task_name == name_of_first_groupOrpartition_in_DAG or not state_info.task_name in DAG_info.get_DAG_leaf_tasks())
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
                incremental_dag_generation_with_partitions = compute_pagerank and use_incremental_DAG_generation and not use_page_rank_group_partitions
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
                    if state_info.task_name == name_of_first_groupOrpartition_in_DAG or (not state_info.task_name in DAG_info.get_DAG_leaf_tasks()):
                        # get the collapse task
                        DAG_executor_state.state = DAG_info.get_DAG_states()[state_info.collapse[0]]
                        state_info = DAG_map[DAG_executor_state.state]
                        logger.debug("DAG_executor_work_loop: got state and state_info of continued, collapsed partition for collapsed task " + state_info.task_name)
                    else:
                        # execute task - partition task is a leaf task that is not the first
                        # partition ("PR1_1") in the DAG. (Leaf tasks statr a new connected component.)
                        logger.debug("DAG_executor_work_loop: continued partition  " 
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
            if using_workers:
                # Config: A4_local, A4_Remote, A5, A6
#rhc continue 
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
                    # assert
                    if not (compute_pagerank and use_incremental_DAG_generation):
                        logger.error("[Error]: work loop: process_continue_queue but"
                            +  " not compute_pagerank or not use_incremental_DAG_generation.")

                    # The first partition was obtained from the continue queue 
                    # when we got the new DAG_info and there should be no more
                    # partitions in the continue queue since there can be only 
                    # one partition that is to-be-continued.
                    if not use_page_rank_group_partitions:
                        logger.error("[Error]: work loop: process_continue_queue but"
                            + " using partitions so this second to be continued"
                            + " partition/task should not be possible.")

                    DAG_executor_state.state = continue_queue.get()

                    # Indicate that we are executing a continued task. Used below
                    # for debugging
                    continued_task = True
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

#rhc: cluster:
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

                            logger.debug("DAG_executor_work_loop: cluster_queue.qsize() == 0 so"
                                + " get work")
                            
                            if not using_threads_not_processes: # using worker processes
                                # Config: A5, A6
                                logger.debug("DAG_executor_work_loop: proc " + proc_name + " " + " thread " + thread_name + ": get work.")
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
                                    logger.debug("DAG_executor_work_loop: dict_of_results from work_tuple: ")
                                    for key, value in dict_of_results.items():
                                        logger.debug(str(key) + " -> " + str(value))
                                    for key, value in dict_of_results.items():
                                        data_dict[key] = value
                                #else: dict_of_results == None 
                                #  assert state is -1

                                logger.debug("DAG_executor_work_loop: got work/-1 for thread " + thread_name
                                    + " state is " + str(DAG_executor_state.state))
                                if not (compute_pagerank and use_incremental_DAG_generation):
                                    # got work from the work queue so break the while(True)
                                    # loop and check if it's -1
                                    break # while(True) loop
                                else: # using incremental DAG generation
                                    # work could be a -1, or a non-leaf task or leaf task. The leaf task
                                    # might be unexecutable until we get a new incremental DAG
                                    if (not use_page_rank_group_partitions) or use_page_rank_group_partitions:
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
                                            is_leaf_task = state_info == None or state_info.task_name in DAG_info.get_DAG_leaf_tasks()
                                            logger.debug("DAG_executor_work_loop: work from work_queue is_leaf_task: " + str(is_leaf_task))
                                            is_unexecutable_leaf_task = state_info == None or (
                                                state_info.task_name in DAG_info.get_DAG_leaf_tasks() and state_info.ToBeContinued
                                            )
                                            if is_unexecutable_leaf_task:
                                                # Cannot execute this leaf task until we get a new DAG. Note: the
                                                # DAG generator added this leaf task to the work queue after it
                                                # made the new DAG containing this completed leeaf task available
                                                # so we will be able to execute this leaf task after we get the new 
                                                # DAG. Note: each leaf task is the start of a new connected component
                                                # in the graph (you cannot reach one CC from another CC.) and the 
                                                # partitions of the connected components can be executed in parallel
                                                if using_workers:
                                                    continue_queue.put(DAG_executor_state.state)
                                                    logger.debug("DAG_executor_work_loop: work from work loop is unexecutable_leaf_task,"
                                                        + " put work in continue_queue:"
                                                        + " state is " + str(DAG_executor_state.state))
                                                else:
                                                    pass # lambdas TBD: call Continue object w/output
                                            else:
                                                # we got work (not -1) from the work queue (a non-leaf task),
                                                # so break the while(True) loop.
                                                logger.debug("DAG_executor_work_loop: work from work queue is not an unexecutable_leaf_task,"
                                                    + " state is " + str(DAG_executor_state.state))
                                                break # while(True) loop
                                        else:
                                            # we got -1 so break the while(True) loop and process the -1 next
                                            break # while(True) loop
                                    else:
                                        pass # finish for groups
                            else:
                                # Config: A4_local, A4_Remote
                                logger.debug("DAG_executor_work_loop:: get work for thread " + thread_name)
                                work_tuple = work_queue.get()
                                DAG_executor_state.state = work_tuple[0]
                                dict_of_results = work_tuple[1]
                                logger.debug("DAG_executor_work_loop: got work for thread " + thread_name
                                    + " state from work tuple is " + str(DAG_executor_state.state))
                                if dict_of_results != None:
                                    logger.debug("DAG_executor_work_loop: dict_of_results from work_tuple: ")
                                    for key, value in dict_of_results.items():
                                        logger.debug(str(key) + " -> " + str(value))
                                    # Threads put task outputs in a data_dict that is global to the threads
                                    # so there is no need to do it again here, when getting work from the work_queue.
                                    #for key, value in dict_of_results.items():
                                    #    data_dict[key] = value 
                                if not (compute_pagerank and use_incremental_DAG_generation):
                                    break # while(True) loop
                                else: # incremental DAG generation
                                    # Work could be a -1, or a non-leaf task or leaf task. The leaf task
                                    # might be unexecutable until we get a new incremental DAG.
                                    if (not use_page_rank_group_partitions) or use_page_rank_group_partitions: 
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
                                            is_leaf_task = state_info == None or state_info.task_name in DAG_info.get_DAG_leaf_tasks()
                                            logger.debug("DAG_executor_work_loop: work from work_queue is_leaf_task: " + str(is_leaf_task))
                                            is_unexecutable_leaf_task = state_info == None or (
                                                state_info.task_name in DAG_info.get_DAG_leaf_tasks() and state_info.ToBeContinued
                                            )
                                            if is_unexecutable_leaf_task:
                                                # Cannot execute this leaf task until we get a new DAG. Note: the
                                                # DAG generator added this leaf task to the work queue after it
                                                # made the new DAG containing this completed leeaf task available
                                                # so we will be able to execute this leaf task after we get the new 
                                                # DAG. Note: each leaf task is the start of a new connected component
                                                # in the graph (you cannot reach one CC from another CC.) and the 
                                                # partitions of the connected components can be executed in parallel

                                                if using_workers:
                                                    continue_queue.put(DAG_executor_state.state)
                                                    # Note: put dict of results (input to task) in data_dict -
                                                    # for processes: when we got work from work_queue.
                                                    # for threads: when we generated the output (that became
                                                    #   this input.)
                                                    logger.debug("DAG_executor_work_loop: put unexecutable_leaf_task work in continue_queue:"
                                                        + " state is " + str(DAG_executor_state.state))
                                                else:
                                                    pass # lambdas TBD: call Continue object w/output
                                            else:
                                                # we got work (not -1) from the work queue (a non-leaf task) so break the while(True)
                                                # loop and execute the work task
                                                logger.debug("DAG_executor_work_loop: work from work queue is not an unexecutable_leaf_task,:"
                                                    + " state is " + str(DAG_executor_state.state))
                                                break # while(True) loop
                                        else:
                                            # we got -1 so break the while(True) loop and process the -1 next
                                            break # while(True) loop
                                    else:
                                        pass # finish for groups
                        # end of while(True) get work from work_queue

                        logger.debug("**********************DAG_executor_work_loop: withdrawn state for thread: " + thread_name + " :" + str(DAG_executor_state.state))

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

                            # Keep track of how many workers have returned and when this equals num_workers
                            # then do not add a -1 to the work queue.
                            if not using_threads_not_processes:
    #rhc: counter:
                                # maintain a count of completed workers. Each worker
                                # that gets s -1 from the work_queue will put another
                                # -1 in the work queue if there are workers that have
                                # not received their -1 yet
                                completed_workers = completed_workers_counter.increment_and_get()
                                if completed_workers < num_workers:
                                    logger.debug("DAG_executor_work_loop: workers_completed:  " + str(completed_workers)
                                        + " put -1 in work queue.")
                                    # Config: A5, A6
                                    work_tuple = (-1,None)
                                    work_queue.put(work_tuple)
                                else:
                                    logger.debug("DAG_executor_work_loop: completed_workers:  " + str(completed_workers)
                                        + " do not put -1 in work queue.")
                            else:
    #rhc: counter:
                                completed_workers = completed_workers_counter.increment_and_get()
                                if completed_workers < num_workers:
                                    logger.debug("DAG_executor_work_loop: workers_completed:  " + str(completed_workers)
                                        + " put -1 in work queue.")

                                    # Config: A4_local, A4_Remote
                                    work_tuple = (-1,None)
                                    work_queue.put(work_tuple)
                                else:
                                    logger.debug("DAG_executor_work_loop: completed_workers:  " + str(completed_workers)
                                        + " do not put -1 in work queue.")

    #rhc continue:          If doing incremental DAG and the DAG is not complete, then do not 
                            # return; instead, get a new DAG_info. All workers will call DAG_infobuffer_monitor.withdraw
                            # and receive a new DAG_info. Those with continue tasks in their continue queue will execute 
                            # these tasks instead of getting work from the work queue. Also, presumably their cluster
                            # queues were empty since they they only try to get work from the work queue when their
                            # cluster_queue is empty.

                            if compute_pagerank and use_incremental_DAG_generation:
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
                                    logger.debug("calling withdraw:")
                                    DAG_info_and_new_leaf_task_states_tuple = DAG_infobuffer_monitor.withdraw(requested_current_version_number)
                                    new_DAG_info = DAG_info_and_new_leaf_task_states_tuple[0]
                                    new_leaf_task_states = DAG_info_and_new_leaf_task_states_tuple[1]
#rhc leaf tasks
                                    logger.debug("DAG_executor_work_loop: cumulative leaf task states withdrawn and added to work_queue: ")
                                    for work_tuple in new_leaf_task_states:
                                        leaf_task_state = work_tuple[0]
                                        logger.debug(str(leaf_task_state))
                                        work_queue.put(work_tuple)

                                    # worker got a new DAG_info and will keep going.
                                    completed_workers = completed_workers_counter.decrement_and_get()
                                    logger.debug("DAG_executor_work_loop: after withdraw: workers_completed:  " + str(completed_workers))
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
                                    if not use_page_rank_group_partitions:
                                        # using partitions.
                                        # If the new DAG is still incomplete, then the last
                                        # partition is incomplete (cannot be executed)
                                        if not DAG_info.get_DAG_info_is_complete():
                                            #num_tasks_to_execute = len(DAG_tasks) - 1
                                            number_of_incomplete_tasks = DAG_info.get_DAG_number_of_incomplete_tasks()
                                            if not number_of_incomplete_tasks == 1:
                                                logger.error("[Error]: Internal Error: DAG_executor_work_loop:"
                                                    + " Using incremental DAG generation with partitions and"
                                                    + " DAG is incomplete but number_of_incomplete_tasks is not 1: "
                                                    + str(number_of_incomplete_tasks))
                                            num_tasks_to_execute = DAG_number_of_tasks - 1
                                            logger.debug("DAG_executor_work_loop: after withdraw: DAG_info not complete: new num_tasks_to_execute: " + str(num_tasks_to_execute))
                                        else:
                                            # the new DAG is complete (so is the last incremental DAG
                                            # we will get.) We can execute all the partitions in the 
                                            # DAG. (A DAG is complete if all the graph nodes are in
                                            # some partition.)
                                            #num_tasks_to_execute = len(DAG_tasks)
                                            num_tasks_to_execute = DAG_number_of_tasks
                                            logger.debug("DAG_executor_work_loop: after withdraw: DAG_info complete new num_tasks_to_execute: " + str(num_tasks_to_execute))
                                    else:
                                        # using groups
                                        if not DAG_info.get_DAG_info_is_complete():
                                            number_of_incomplete_tasks = DAG_info.get_DAG_number_of_incomplete_tasks()
                                            num_tasks_to_execute = DAG_number_of_tasks - number_of_incomplete_tasks
                                            logger.debug("DAG_executor_work_loop: after withdraw: DAG_info not complete: new num_tasks_to_execute: " 
                                                + str(num_tasks_to_execute) + " with number_of_incomplete_tasks "
                                                + str(number_of_incomplete_tasks))
                                        else:
                                            #num_tasks_to_execute = len(DAG_tasks)
                                            num_tasks_to_execute = DAG_number_of_tasks
                                            logger.debug("DAG_executor_work_loop: after withdraw: DAG_info complete new num_tasks_to_execute: " + str(num_tasks_to_execute))

                                    if continue_queue.qsize() > 0:
                                        # if this worker has incomplete tasks in its continue
                                        # queue (like a leaf task from above, or the last partition
                                        # in an incremental DAG, which is always incomplete) then 
                                        # start by executing those tasks.
                                        DAG_executor_state.state = continue_queue.get()
                                        # This is stateinfo of continued task
                                        
                                        #state_info = DAG_map[DAG_executor_state.state]
                                        # The continued task will be executed next.
                                        # We get its state info below.
                                        logger.debug("DAG_executor_work_loop: For new DAG_info, continued state: " + str(DAG_executor_state.state)
                                            +" state_info: " + str(DAG_map[DAG_executor_state.state]))
#rhc continue
                                        continued_task = True
                                        # if there are more continued tasks, we set flag 
                                        # process_continue_queue so that above we will 
                                        # get a task from the continue_queue instead of
                                        # trying to get work fro the work_queue or our
                                        # cluster_queue.
                                        if continue_queue.qsize() == 0:
                                            # only one task/state was in continue_queue
                                            process_continue_queue = False
                                        else:
                                            # process tasks/states in continue_queue
                                            logger.debug("DAG_executor_work_loop: XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXwork_loop: continue_queue.qsize()>0 set process_continue_queue = True.")
                                            process_continue_queue = True

                                    else:
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
                                    logger.debug("DAG_executor_work_loop: DAG_info is_complete so return.")
                                    return
                            else: 
                                # not doing incremental and no more tasks to execute.
                                # worker is returning from work loop so worker will terminate
                                # and be joined by DAG_executor_driver
                                return  

                        # Note: using_workers is checked above and must be True

                        # old design
                        #worker_needs_input = cluster_queue.qsize() == 0

                        #assert:
                        # cluster_queue was empty so we got work, which means the ecluster_queue should still be empty.
                        if not cluster_queue.qsize() == 0:
                            logger.error("[Error]: Internal Error: DAG_executor_work_loop: cluster_queue.qsize() == 0 was true before"
                                + " we got work from worker queue but not after - queue size should not change.")

    #rhc: cluster:
                        # Do not do this
                        #worker_needs_input = False # default

                        # Note: If the continue_queue was empty after we got
                        # the new DAG_info, we executed a continue statement
                        # so we only get here if the contnue_queue was not 
                        # empty and so we have a continue task to execute.
                        #comment out for MM
                        logger.debug("DAG_executor_work_loop: Worker accessed work_queue, then maybe continue_queue: "
                            + "process state: " + str(DAG_executor_state.state))
                        
                    else: # cluster_queue is not empty so worker does not need input
    #rhc: cluster:
                        # worker does not need work since there is work
                        # in the cluster queue. Get work from cluster_queue.
                        # Note: Currently, we do not cluster more than one fanout and
                        # we do not cluster faninNB/fanins, and there is only one
                        # collapse task, so the cluster_queue will have only one
                        # work task in it.
                        DAG_executor_state.state = cluster_queue.get()

                        #old design
                        #worker_needs_input = cluster_queue.qsize() == 0
                        logger.debug("DAG_executor_work_loop: cluster_queue contains work:"
                            + " got state " + str(DAG_executor_state.state))

    #rhc: cluster:
                        #assert:
                        if not cluster_queue.qsize() == 0:
                            logger.error("[Error]: DAG_executor_work_loop: Internal Error: cluster_queue contained"
                                + " more than one item of work - queue size > 0 after cluster_queue.get")

                        logger.debug(thread_name + " DAG_executor_work_loop: Worker doesn't access work_queue")
                        logger.debug("**********************" + thread_name + " process cluster_queue state: " + str(DAG_executor_state.state))
                    
    #rhc: counter
    
                # end not process_continue_queue
                
                # we have a task to execute - we got it from the work queue
                # or the cluster queue or the continue queue.


                incremental_dag_generation_with_groups = compute_pagerank and use_incremental_DAG_generation and use_page_rank_group_partitions
                continued_task_state_info = DAG_map[DAG_executor_state.state]
                logger.debug(thread_name + " DAG_executor_work_loop: checking whether to inc num tasks executed: incremental_dag_generation_with_groups: "
                    + str(incremental_dag_generation_with_groups)
                    + " continued_task: " + str(continued_task)
                    + " continued_task_state_info.task_name == PR1_1: " + str(continued_task_state_info.task_name == name_of_first_groupOrpartition_in_DAG)
                    + " (not continued_task_state_info.task_name in DAG_info.get_DAG_leaf_tasks(): "
                    + str((not continued_task_state_info.task_name in DAG_info.get_DAG_leaf_tasks())))
                if not (incremental_dag_generation_with_groups and continued_task and (
                    (continued_task_state_info.task_name == name_of_first_groupOrpartition_in_DAG or not continued_task_state_info.task_name in DAG_info.get_DAG_leaf_tasks())
                )):
                    # If this is a continued task, we may have already executed it.
                    # If the task is the first task in a new connected
                    # component and it is not the first task in the DAG then we have not
                    # executed the task yet. We will execute this task and so we increment
                    # the number of tasks that have been excuted. Otherwise we have
                    # already executed the continues task. This means we wil not 
                    # execute it here and we should not increment th number of tasks
                    # that have been executed.

                    #Note: This increment_and_get executes a memory barrier - so the pagerank writes to 
                    # the shared memory (if used) just performed by this process P1 
                    # will be flushed, which means the downstream pagerank tasks 
                    # that read these values will get the values written by P1.
                    # So we need a memory barrier between a task and its downstream
                    # tasks. Use this counter or if we remove this counter, something 
                    # else needs to provide the barrier.

                    # Increment num tasks executed and see if this is the last 
                    # task to execute. If so, start the terminattion process or
                    # if we are doing incremental DAG generation (so this is the 
                    # last task in the current version of the DAG)) workers need
                    # to get the next DAG (instead of terminating).

                    num_tasks_executed = completed_tasks_counter.increment_and_get()
                    logger.debug("DAG_executor_work_loop: " + thread_name + " increment num_tasks_executed, now check if executed all tasks: "
                        + " num_tasks_executed: " + str(num_tasks_executed) 
                        + " num_tasks_to_execute: " + str(num_tasks_to_execute))
    
                    if num_tasks_executed == num_tasks_to_execute:
    #rhc: stop
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
                        if not using_threads_not_processes:
                            # Config: A5, A6
                            logger.debug(thread_name + ": DAG_executor: num_tasks_executed == num_tasks_to_execute: depositing -1 in work queue.")
                            work_tuple = (-1,None)
                            # for the next worker
                            work_queue.put(work_tuple)
                        else:
                            # Config: A4_local, A4_Remote
                            logger.debug(thread_name + ": DAG_executor: num_tasks_executed == num_tasks_to_execute: depositing -1 in work queue.")
                            # for the nex worker
                            work_tuple = (-1,None)
                            work_queue.put(work_tuple)
                else:
                    logger.debug("DAG_executor_work_loop: " + thread_name + " before processing " + str(DAG_executor_state.state) 
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
#rhc: cluster:
            else: # not using workers: Config: A1. A2, A3
                # if we are using simulated or real lambdas, then the lambda
                # payload has a state to execute. Ths state is added to the 
                # cluster_queue at the start of the work loop. Lambdas may also
                # add collpased states to the cluster queue and fanout/fanin
                # become states are also added to the cluster queue. Real and 
                # simulated lambdas do not use a work_queue, and they do 
                # not use a continue_queue for incremental DAG generation.
                # So they only use a cluster queue. 
                #
                # The cluster_queue will have the lambdas payload task added
                # to it and so will not be empty the first time we check here
                # on the cluster_queue size. When processing the tasks, we may
                # or may not add become tasks for fanouts and fanins and 
                # collapsed tasks to the custer_queue, so the cluster_queue
                # may be empty on later checks.
                if cluster_queue.qsize() > 0:
                    DAG_executor_state.state = cluster_queue.get()
                    # Question: Do not do this
                    #worker_needs_input = cluster_queue.qsize() == 0
                    logger.debug("DAG_executor_work_loop: simulated or real lambda:"
                        + " cluster_queue contains work: got state " + str(DAG_executor_state.state))
#rhc: cluster:
                    #assert:
                    if not cluster_queue.qsize() == 0:
                        logger.error("[Error]: DAG_executor_work_loop: Internal Error: simulated or real lambda:"
                            + " cluster_queue contained more than one item of work - queue size > 0 after cluster_queue.get")

            # while (True) still executing from above - continues next with work to do in the form
            # of DAG_executor_state.state of some task

            # DAG_executor_state.state contains the next state to execute.

            # Note: if we are using lambdas instead of workers, we do not use the
            # continue queue so continued == False. When a lambda excutes a task
            # and finds that it has a fanin/fanout/faninNB/collapse task that is 
            # to-be-continued, the lambda does not enqueue the continued task in the 
            # continue queue. Likewise, lambdas do not use a work_queue - after a lambda
            # excutes its payload task and any become tasks it gets after that, the 
            # lambda will terminate. (The lambda may start new lambdas for fanouts.)
            # The lambda does not wait for a new incremental DAG either, as we do not
            # want lambdas waiting for anything (no-wait). Instead, the lambda will
            # send the to-be-continued task and the task's input/output, whichever
            # is needed for the continued task to a synch object that will start a new
            # lambda to excute the continued tsk when a new incrmental ADG becomes
            # available. 
            #
            # We need to make sure that lambdas do not execute the incremental DAG
            # generation code that is used by workers. We can use the flag 
            # using_workers to indicate that this is worker code. Since continued_task
            # is never True when using lambdas, the continued_task flag will often prevent
            # lambdas from excuting this worker code too.


            # If we got a become task when we processed fanouts, we set
            # DAG_executor_state.state to the become task state so we
            # will be processing the become state here. Tbis being
            # changed so that become tasks are added to the cluster_queue.
            logger.debug (thread_name + ": DAG_executor_work_loop: access DAG_map with state " + str(DAG_executor_state.state))
            state_info = DAG_map[DAG_executor_state.state]

            #commented out for MM
            #logger.debug("state_info: " + str(state_info))
            logger.debug(thread_name + ": DAG_executor_work_loop: task to execute: " + state_info.task_name 
                + " (though this task may be a continued task that was already executed.)")

            # This task may or may not be a continued task In either
            # case, the inputs needed by this task are needed: For worker threads
            # and processes when using partitions, the needed inputs were added to the 
            # data_dict after the previous partition was executed, so they are
            # still available, as usual.
            #
            # For incremental DAG generation using groups (instead of partitions)
            # we never execute the continued task unless it is a leaf task
            # that is not the first group in the DAG. These leaf tasks are
            # groups that start a new connected component. They are added to the 
            # work queue by the DAG generator. When we get such a leaf task from the 
            # work_queue it may not yet be executable, in which case it is added
            # to the continue queue. (It will become executable when we get our
            # next incremental DAG). It then becomes a continued task and it has
            # not been executed before so we need to execute it as oppsed to 
            # executing its collapse task.
            incremental_dag_generation_with_groups = compute_pagerank and use_incremental_DAG_generation and use_page_rank_group_partitions
            #logger.debug(thread_name + " DAG_executor_work_loop: incremental_dag_generation_with_groups: "
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
            logger.debug(thread_name + " DAG_executor_work_loop: checking whether to execute task:"
                + " incremental_dag_generation_with_groups: "
                + str(incremental_dag_generation_with_groups)
                + " continued_task: " + str(continued_task)
                + " state_info.task_name == PR1_1: " + str(state_info.task_name == name_of_first_groupOrpartition_in_DAG)
                + " (not state_info.task_name in DAG_info.get_DAG_leaf_tasks(): "
                + str((not state_info.task_name in DAG_info.get_DAG_leaf_tasks())))

#rhc: lambda inc: if using_workers and
            if incremental_dag_generation_with_groups and continued_task and (
            (state_info.task_name == name_of_first_groupOrpartition_in_DAG or not state_info.task_name in DAG_info.get_DAG_leaf_tasks())
            ):
                continued_task = False
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
                # group generated on any call to BFS()). We will execute the 
                # group task. 
                #
                # Note: The if-condition will be False if we are executing a leaf
                # task/group that is a new connected component (not "PR1_1"), 
                # in which case continued_task may have been true and it's
                # still True. We need to set it to False.
                incremental_dag_generation_with_partitions = compute_pagerank and use_incremental_DAG_generation and not use_page_rank_group_partitions

#rhc: lambda inc: if using_workers and
# but may kep using continue_task so we can excute a group with TBC fanins/fanouts, like usual
                if incremental_dag_generation_with_partitions and continued_task:
                    continued_task = False
                    # if continued state is the first partition (which is a leaf task) or is not a leaf task
                    # then get the collapse task of the continued state; otherwise, execute the 
                    # continued state, which is a leaf task that starts a new conncted
                    # component. Note: For groups, we do not get the collapsed task.
                    # We do for partitions because partitions only have collapsed tasks,
                    # i.e., no fanouts or fanins and if task T has a collapsed task C then 
                    # the same worker W that executed T executes C. So we add C to W's
                    # contnue queue and W will execute C. For groups, we execute the task and 
                    # if it is to be continued, we put the state in the continue
                    # queue and when we get it from the continue queue we do its 
                    # fanins/fanouts/collapses. Note that if W executed T and T has 
                    # mny fanouts then we do not want to put all of these fanouts in 
                    # W's continue queue since that would mean W woudl execute all
                    # the fanout tasks of T. Instead, when W gets the state for T 
                    # from the continue queue Q can (skip th execution of T since that
                    # already happened) do T's fanouts as ususal, i.e., W will 
                    # become/cluster one of T's fanouts and put the rest on the shared
                    # worker queue to distribute the fanout tasks amoung the workers.
                    #
                    # Example: In the white board DAG, we execute PR1_1, PR2_1L, and PR3_1
                    # and assume we added to the DAG a new connected component PR4_1 that has a collapse
                    # to PR5_1. When we deposit the new DAG with PR4_1, since it is a leaf task, we also add
                    # PR4_1 to the work_queue. No task in the current conencte component 
                    # will have a fanout/fanin to PR4_1 since PR4_1 starts a new connected
                    # component; thus, we have to put PR4_1 in the work queue when we detect
                    # it is the start of a new component so Pr4_1 will be executed.
                    # When we get PR4_1 from the work_queue, assume
                    # it is unexecutable (not in the current DAG as we did not yet get the new
                    # DAG that has PR4_1 in it, or in new DAG but to-be-contnued) then we put
                    # PR4_1 in the continue_queue. When we get a new DAG it will have a 
                    # completed PR4_1 so we get PR4_1 from the continue_queue; however,
                    # we did not put state 4 in the continue_queue as a state/partition with a 
                    # TBC collapse, so we should not grab the collape of 4. Instead, we 
                    # should execute state/partition 4, known as "PR4_1". We know this since PR4_1
                    # is a leaf task and a leaf task is only added to the continue
                    # queue when we want to execute the leaf task.
                    if state_info.task_name == name_of_first_groupOrpartition_in_DAG or (not state_info.task_name in DAG_info.get_DAG_leaf_tasks()):
                        DAG_executor_state.state = DAG_info.get_DAG_states()[state_info.collapse[0]]
                        state_info = DAG_map[DAG_executor_state.state]
                        logger.debug("DAG_executor_work_loop: got state and state_info of continued, collapsed partition for collapsed task " + state_info.task_name)
                    else:
                        # execute task - partition task is a leaf task that is not the first
                        # partition in the DAG. (Leaf tasks start a new connected component.)
                        logger.debug("DAG_executor_work_loop: continued partition  " 
                            + state_info.task_name + " is a leaf task so do not get its collapse task"
                            + " instead, execute the leaf task.")
                else:
                    # if continued tasks is TRUE this will set it to False
                    continued_task = False
                    # execute task - task is a group task that is a 
                    # leaf task (but not the fitst group in the DAG)
                    # or it is a patition task that is not a continued task.
                    
                #execute task

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
                logger.debug("is_leaf_task: " + str(is_leaf_task))
                logger.debug("task_inputs: " + str(task_inputs))

                # we have already executed continued_tasks and put their outputs
                # in the data_dict

#rhc continue - OLD
                #if not continued_task:
#rhc continue - old

                #rhc: Some DAGs, e.g., pagerank, may use a single paramaterized function, 
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
                    logger.debug("Packing data. Task inputs: %s. Data dict (keys only): %s" % (str(task_inputs), str(data_dict.keys())))
                    # task_inputs is a tuple of task_names
                    args = pack_data(task_inputs, data_dict)
                    logger.debug(thread_name + " argsX: " + str(args))
                    if tasks_use_result_dictionary_parameter:
                        logger.debug("Foo1a")
                        # task_inputs = ('task1','task2'), args = (1,2) results in a resultDictionary
                        # where resultDictionary['task1'] = 1 and resultDictionary['task2'] = 2.
                        # We pass resultDictionary of inputs instead of the tuple (1,2).

                        if len(task_inputs) == len(args):
                            logger.debug("Foo1b")
                            result_dictionary = {task_inputs[i] : args[i] for i, _ in enumerate(args)}
                            logger.debug(thread_name + " result_dictionaryX: " + str(result_dictionary))
                    
                    """
                    # This might be useful for leaves that have more than one input?
                    # But leaves always have one input, e.g., (1, )?
                    if tasks_use_result_dictionary_parameter:
                        logger.debug("Foo2a")
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
                        logger.debug(thread_name + " args: " + str(args)
                            + " len(args): " + str(len(args))
                            + " len(task_input_tuple): " + str(len(task_input_tuple))
                            + " task_input_tuple: " + str(task_input_tuple))
                        if len(task_input_tuple) == len(args):
                            # The enumerate() method adds a counter to an iterable and returns the enumerate object.
                            logger.debug("Foo2b")
                            result_dictionary = {task_input_tuple[i] : args[i] for i, _ in enumerate(args)}
                            logger.debug(thread_name + " result_dictionaryy: " + str(result_dictionary))
                        #Note:
                        #Informs the logging system to perform an orderly shutdown by flushing 
                        #and closing all handlers. This should be called at application exit and no 
                        #further use of the logging system should be made after this call.
                        logging.shutdown()
                        #time.sleep(3)   #not needed due to shutdwn
                        os._exit(0)
                    """
                else:
                    # if not triggering tasks in lambdas task_inputs is a tuple of input values, e.g., (1,)
                    if not (store_sync_objects_in_lambdas and sync_objects_in_lambdas_trigger_their_tasks):
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
                        logger.debug(thread_name + " argsY: " + str(args))

                    if tasks_use_result_dictionary_parameter:
                        # Passing am emoty inut tuple to the PageRank task,
                        # This results in a rresult_dictionary
                        # of "DAG_executor_driver_0" --> (), where
                        # DAG_executor_driver_0 is used to mean that the DAG_excutor_driver
                        # provided an empty input tuple for the leaf task. In the 
                        # PageRank_Function_Driver we just ignore empty input tuples so 
                        # that the input_tuples provided to the PageRank_Function will be an empty list.
                        result_dictionary['DAG_executor_driver_0'] = ()

                logger.debug("argsZ: " + str(args))

                logger.debug(thread_name + " result_dictionaryZ: " + str(result_dictionary))

#rhc cleanup
                #for key, value in DAG_tasks.items():
                #    logger.error(str(key) + ' : ' + str(value))

                # using map DAG_tasks from task_name to task
                task = DAG_tasks[state_info.task_name]

                if not tasks_use_result_dictionary_parameter:
                    # we will call the task with tuple args and unfold args: task(*args)
                    output = execute_task(task,args)
                else:
                    #def PageRank_Function_Driver_Shared(task_file_name,total_num_nodes,results_dictionary,shared_map,shared_nodes):
                    if not use_shared_partitions_groups:
                        # we will call the task with: task(task_name,resultDictionary)
                        output = execute_task_with_result_dictionary(task,state_info.task_name,20,result_dictionary)
                    else:
                        if use_page_rank_group_partitions:
                            output = execute_task_with_result_dictionary_shared(task,state_info.task_name,20,result_dictionary,BFS_Shared.shared_groups_map,BFS_Shared.shared_groups)
                        else: # use the partition partitions
                            output = execute_task_with_result_dictionary_shared(task,state_info.task_name,20,result_dictionary,BFS_Shared.shared_partition_map,BFS_Shared.shared_partition)

                    # save outputs so we can check them after execution.
                    # These are values sent to other partitions/groups,
                    # not the pagerank values for each node. The outputs
                    # can be empty since a partition/group can have no 
                    # fanouts/fanins/faninNBs/collapses.
                    if check_pagerank_output:
                        set_pagerank_output(DAG_executor_state.state,output)
      
                """ where:
                    def execute_task(task,args):
                        logger.debug("input of execute_task is: " + str(args))
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
                    if sender_set_for_senderX == None:
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
                logger.debug(thread_name + " executed task " + state_info.task_name + "'s output: " + str(output))

                
                if same_output_for_all_fanout_fanin:
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

                logger.debug("data_dict: " + str(data_dict))

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

            #rhc continue
            """
            OLD --> DELETE
            if compute_pagerank and use_incremental_DAG_generation and (
                state_info.ToBeContinued):
                if process_continue_queue:
                    logger.error("[Error]: DAG_executor work loop: process_continue_queue but"
                        +  " we just found a To Be Continued State, i.e., the state was"
                        +  " continued previously and in procesing it after getting a new"
                        +  " DAG_info the state is still To Be Continued (in the new DAG_info.")

                # if the DAG_info is not complete, we execute a continued
                # task and put its output in the data_dict but w do not 
                # process its fanout/fanins/faninNBs until we get the next
                # DAG_info. Put the task's state in the continue_queue to
                # be continued when we get the new DAG_info

            #rhc: continue - finish for Lambdas
                if using_workers:
                    continue_queue.put(DAG_executor_state.state)
                else:
                    pass # lambdas TBD
            """

            if (compute_pagerank and use_incremental_DAG_generation and (
                    use_page_rank_group_partitions and state_info.fanout_fanin_faninNB_collapse_groups_are_ToBeContinued)
                ):
                # Group has fanouts/fanins/faninNBs/collapses that are TBC
                # so put this state in the continue_queue. When we get a new DAG
                # we will get this state from the continue queue. We have already
                # executed this state, so we will skip task execution and do 
                # the continued fanouts/fanins/faninNBs/collapses. Note that when
                # we get a new DAG that this group that had continued 
                # fanouts/fanins/faninNBs/collapses now is complete, i.e., it has
                # no continud fanouts/fanins/faninNBs/collapses.
                if using_workers:
                    #continue_queue.put(DAG_executor_state.state)
                    #logger.debug("DAG_executor_work_loop: put TBC collapsed work in continue_queue:"
                    #    + " state is " + str(DAG_executor_state.state))
                    continue_queue.put(DAG_executor_state.state)
                    logger.debug("DAG_executor_work_loop: put TBC collapsed work in continue_queue:"
                        + " state is " + str(DAG_executor_state.state))
                else:
                    pass # lambdas TBD: call Continue object w/output

#rhc: cluster queue:
# Note: if this just executed task T has a collapse task then T has no
# fanouts/fanins/faninNBs, so next it will execute the clustered task
# with the resultsleepexecutes it just placed in the data_dict.
# Note: if the just executed task T has multiple fanouts and it clusters them
# then the fanouts will be added to the cluster queue, i.e., their state
# returned by DAG_info.get_DAG_states() will be added to the cluster queue.
# We were going to add the fanouts to the work queue (minus the become
# task). For the become task, we only set DAG_executor_state.state and execute
# that task next, so just like a collapsed task. Notice that the input for
# the become task (a fanout task) will be retieved from the data_dict, i.e., 
# after T was excuted, we saved its output in the data dict so we know it
# is there (in our local data dict if we are a process or lambda or the 
# global data dict if we are a thread.) So we only put the state in the 
# cluster queue, not the output of T since the output of T is in the data dict.
# For clustered fanouts, we can also only put the state in the cluster queue 
# since the output we need to execute them is T's output and it is in our
# data dict.
# Note: A fanin task can also be a become task. So we will want to add
# such become tasks to the cluster queue, in the same way. Note that 
# a fanin op returns all the results that were sent to the fanin and we 
# put them in the data dict when the fanin op returns. So they will be
# there when we execute the fanin task. That is, when we add the fanin 
# task to the cluster queue it may be behind one or more clustered tasks
# but when we eventually execute, we will have its inputs in our data dict.

            elif len(state_info.collapse) > 0:

                if len(state_info.fanins) + len(state_info.fanouts) + len(state_info.faninNBs) > 0:
                    # a state with a collapse has no other fanins/fanuts.
                    logger.error("[Error]: Internal Error: DAG_executor_work_loop:"
                        + " state has a collapse but also fanins/fanouts.")

#rhc: If we run-time cluster, then we may have work in the cluster queue.
# We can put the collpase work in the cluster queue too. If the cluster queue is
# empty, thn we'll just execute the collapase task next; otherwise, we will add
# the collapsed work at the end of the cluster queue and do it after all the 
# clustered work. 

# The work for collapse will have to be in the same form as the clustered work,
# e.g., a becomes task from fanouts. So:
# - put all becomes work and collapsed work, etc in the cluster queue
# - get work from the non-empty cluster queue rather then from the work queue.
#   If only a become task or collased task is in the cluster queue then not 
#   cluster_queue is empty and the work is in the cluster queue. 

                # execute collapsed task next - transition to new state and iterate loop
                # collapse is a list [] so get task name of the collapsed task which is collapse[0],
                # the only name in this list. Note: DAG_states() is amap from 
                # task name to an int that is the task state.
                
                # DAG_executor_state.state is the state that has the 
                # collapsed partition. state_info is the state info
                # of the tate with the collapsed partition.
#rhc continue 
#rhc: lambda inc: above if means that this group has no to-be-continued? as in we 
# put that if at front as special case of inc with groups where task just executed
# has no TBC. So if we get past that, the group has no TBC or we aer using partitions
# or not doing inc?
# Q: what if using lambdas and groups (i.e., second conjunct is true)?

                if not(compute_pagerank and use_incremental_DAG_generation) or (compute_pagerank and use_incremental_DAG_generation and use_page_rank_group_partitions):
#rhc: lambda inc: # implied that this state has no TBC otherwise preceding if would have been true?
# So we handle group TBC collapse above and partition TBC collapse below? Here is no inc
# a group with no TBC. otherwise we handle partition which may or may not have TBC collapse.
# if it does then need to finish lambda code, which is same as group code above - 
# send info to synch object.
#rhc: cluster:
                    # get the state of the collapsed partition (task)
                    # and put the collapsed task in the cluster_queue for 
                    # execution.
                    DAG_executor_state.state = DAG_info.get_DAG_states()[state_info.collapse[0]]
                    cluster_queue.put(DAG_executor_state.state)
                    logger.debug("DAG_executor_work_loop: put collapsed work in cluster_queue:"
                        + " state is " + str(DAG_executor_state.state))
#rhc: cluster:
                    # Do NOT do this
                    ## Don't add to thread_work_queue just do it
                    #if using_workers: 
                    #    # Config: A4_local, A4_Remote, A5, A6
                    #    worker_needs_input = False
                    ## else: # Config: A1. A2, A3
                else: 
#rhc: lambda inc: note that to get here we are doing inc w/ pagerank and not using groups
                    # we are doing incremental DAG generation with partitions.
                    state_of_collapsed_task = DAG_info.get_DAG_states()[state_info.collapse[0]]
                    state_info_of_collapse_task = DAG_map[state_of_collapsed_task]
                    logger.debug("DAG_executor_work_loop: check TBC of collapsed task state info: "
                        + str(state_info_of_collapse_task))
                    if state_info_of_collapse_task.ToBeContinued:
                        # put TBC states in the continue_queue

                        if using_workers:
                            #continue_queue.put(DAG_executor_state.state)
                            #logger.debug("DAG_executor_work_loop: put TBC collapsed work in continue_queue:"
                            #    + " state is " + str(DAG_executor_state.state))
                            # put the state with the collapsed task in the continue_queue,
                            # not the state of the collapsed task. We will get the state 
                            # out of the continue queue and then get the collapsed task
                            # of this state and excute it.
                            continue_queue.put(DAG_executor_state.state)
                            logger.debug("DAG_executor_work_loop: put state with TBC collapse in continue_queue:"
                                + " state is " + str(DAG_executor_state.state))
#rhc: continue - finish for Lambdas
                        else:
                            pass # lambdas TBD: call Continue object w/output
                    else:
#hc: cluster:
                        # put non-TBC states in the cluster_queue
                        DAG_executor_state.state = DAG_info.get_DAG_states()[state_info.collapse[0]]
                        cluster_queue.put(DAG_executor_state.state)
                        logger.debug("DAG_executor_work_loop: put collapsed work in cluster_queue:"
                            + " state is " + str(DAG_executor_state.state))

                    # assert:
                    # For partitions, we currently grab the collapse of the current
                    # partition, which is the only thing to do since there are no 
                    # fanouts/fanins/faninNBs for partitions, and check the 
                    # collapse's TBC. If that TBC is True, then we put the
                    # collapse task in the contine queue. The TBC of the collapse, 
                    # which is effectively the TBC of the next partition, should equal
                    # the fanout_fanin_faninNB_collapse_groups of the current partition.
                    if not state_info.fanout_fanin_faninNB_collapse_groups_are_ToBeContinued == state_info_of_collapse_task.ToBeContinued:
                        logger.error("[Error]: Internal error: DAG_executor_work_loop:"
                            + " fanout_fanin_faninNB_collapse_groups of current partition is not"
                            + " equal to ToBeContinued of collapse task (next partition).")
                        logging.shutdown()
                        os._exit(0)

#    Note: for multithreaded worked, no difference between adding work to the 
#    work queue and adding it to the cluster_queue, i.e., doesn't eliminate 
#    any overhead. The difference is when using multiprocessing or lambdas
#    (where lambdas are really just processes.) So for multiP we are putting
#    work in lists and sending it to the tcp_server for deposit into the 
#    work_queue; instead, we will cluster work locally. Likewise, we will cluster 
#    instead of starting a lambda for the (non-become) fanouts.

            elif len(state_info.faninNBs) > 0 or len(state_info.fanouts) > 0:
                # assert len(collapse) + len(fanin) == 0
                # If len(state_info.collapse) > 0 then there are no fanins, fanouts, or faninNBs and we will not excute this elif or the else

                
                # list of fanouts to deposit into work_queue piggybacking on call to batch fanins.
                # This is used when using_workers and not using_threads_not_processes, which is 
                # when we process the faninNBs in a batch
#rhc: run tasks changed name to include "payload"
                list_of_work_queue_or_payload_fanout_values = []

                # Check fanouts first so we know whether we have a become task for 
                # fanout. If we do, then we dont need any work generated by faninNBs.
                if len(state_info.fanouts) > 0:
                    # start DAG_executor in start state w/ pass output or deposit/withdraw it
                    # if deposit in synchronizer need to pass synchronizer name in payload. If synchronizer stored
                    # in Lambda, then fanout task executed in that Lambda.
#rhc: run tasks changes in process_fanouts
                    DAG_executor_state.state = process_fanouts(state_info.fanouts, state_info.task_name, DAG_info.get_DAG_states(), DAG_executor_state, 
                        output, DAG_info, server,work_queue,list_of_work_queue_or_payload_fanout_values)
                    logger.debug(thread_name + " work_loop: become state:" + str(DAG_executor_state.state))
                    logger.debug(thread_name + " work_loop: list_of_work_queue_payload fanout_values length:" + str(len(list_of_work_queue_or_payload_fanout_values)))

                    # at this point list_of_work_queue_or_payload_fanout_values may or may not be empty. We wll
                    # piggyback this list on the call to process_faninNBs_batch if there are faninnbs.
                    # if not, we will call work_queueu.put_all() directly.

                    if using_workers and not using_threads_not_processes:
                        # Config: A5, A6
                        # We piggyback fanouts if we are using worker processes. In that case, if we have
                        # more than one fanout, the first wil be a become task, which will be removed from
                        # state_info.fanouts, decrementing the length of state_info.fanouts.
                        # That will make the new length greater than or equal to 0. If the length is greater
                        # than 0, that means we started with more than one fanout, which means all the fanouts
                        # except the become should be in the list_of_work_queue_or_payload_fanout_values.
                        # Note: We are not currently piggybacking this list whn we use lambdas; instead,
                        # process_fanouts start the lambdas. We may use the parallel invoker on tcp_server 
                        # and piggyback the list of fanouts, or use the parallel invoker in process_fanouts.
                        # assert:
                        if len(state_info.fanouts) > 0: # Note: this is the length after removing the become fanout 
                            # We became one fanout task and removed it from fanouts, but there maybe were more fanouts
                            # and we should have added the fanouts to list_of_work_queue_or_payload_fanout_values.
                            if len(list_of_work_queue_or_payload_fanout_values) == 0:
                                logger.error("[Error]: work loop: after process_fanouts: Internal Error: fanouts > 1 but no work in list_of_work_queue_or_payload_fanout_values.")
                    # else: # Config: A1, A2, A3, A4_local, A4_Remote

#rhc: cluster:      # Add become task to cluster queue
                    cluster_queue.put(DAG_executor_state.state)
                    logger.debug("DAG_executor_work_loop: put fanout bcome task in cluster_queue:"
                        + " state is " + str(DAG_executor_state.state))
#rhc: cluster:
                    #Do NOT do this.
                    ## We get new state_info and then state_info.task_inputs when we iterate
                    #if using_workers:   # we are become task so we have more work
                    #    # Config: A4_local, A4_Remote, A5, A6
                    #    worker_needs_input = False
                    #    logger.debug(thread_name + " work_loop: fanouts: set worker_needs_input to False.")
                    ## else: Config: A1, A2, A3
                else:
                    # No fanouts so no become task and if faninNBs do not generate
                    # work for us we will need input, so set to True here as default.
                    #Note: setting worker_needs_input = True must be guarded by using_workers
#rhc: cluster:
                    #assert
                    if cluster_queue.qsize() > 0:
                        logger.error("[Error] Internal error: No fanouts but cluster_queue.qsize() > 0.")
#rhc: cluster:
                    #Do NOT do this.
                    #if using_workers: 
                    #    # Config: A4_local, A4_Remote, A5, A6
                    #    worker_needs_input = True
                    #   logger.debug(thread_name + " work_loop: no fanouts: set worker_needs_input to True.")
                    ## else: Config: A1, A2, A3

#rhc: cluster:
                if using_workers:
                    # Need worker_needs input to be set corrctly in case len(state_info.faninNBs) is 0
                    worker_needs_input = cluster_queue.qsize()==0
                    logger.debug("DAG_executor_work_loop: check cluster_queue size before processing faninNBs:"
                        + " cluster_queue.qsize(): " + str(cluster_queue.qsize()))

                if len(state_info.faninNBs) > 0:
                    # this is the condition for batch processing; whether we use 
                    # async_call or not depends on whether or not we can get return values.
                    # if using worker processes and we don't need work (since we got a become task
                    # from the fanouts) or we are running tasks in lambdas then do batch processing
                    # Note: we haven't implemented real lamdba version yet, but we do 
                    # not distinguish here between real and simulated lambdas
                    if (run_all_tasks_locally and using_workers and not using_threads_not_processes) or (
                        not run_all_tasks_locally):
                        # We do batch processing when we use lambdas to execute tasks, real or simulated.
                        # So this condition, which specifies the "sync objects trigger their tasks" case
                        # is not needed.
                        #not run_all_tasks_locall1y and not using_workers and not store_fanins_faninNBs_locally and store_sync_objects_in_lambdas and sync_objects_in_lambdas_trigger_their_tasks):

                        # Config: A1, A3, A5, A6
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
                        # their results. This option is sync_objects_in_lambdas_trigger_their_tasks.
                        # In this case, the tasks and the synch objects are executed and stored, respectfully,
                        # in the same lambdas - so we are using lambdas to excute and store sync objects 
                        # and the lambdas may be real or simulated.
                        # Note we will only make async_calls when we are simulating lambdas by executing tasks 
                        # in real python functions and storing object in real python functions. In this case
                        # no value is returned to the caller. When we are not storing sync objects in 
                        # lambdas, work is returned so that threads can be started to simukate lambdas.
                        # Note: calling process_faninNBs_batch when using threads to simulate lambdas and storing 
                        # objects remotely and using_Lambda_Function_Simulators_to_Store_Objects.
                        # Since the faninNBs cannot start new simulated threads to execute the fanin tasks,
                        # the process_faninNB_batch passes all the work back and the calling simuated thread starts one thread
                        # for each work tuple returned.
                        # Note: batching work when we are using workers and storing the FanInNBs remotey and 
                        # we are using processes, or we are using real lambdas. We can also use workers with 
                        # threads instead of processes but multithreading with remote FanInNBs is 
                        # not as useful as using processes. Multithreadng in general is not as helpful 
                        # as multiprocessing in Python.
                        #or (run_all_tasks_locally and not using_workers and using_Lambda_Function_Simulator):
 
                        # assert
                        if store_fanins_faninNBs_locally:
                            # Config: A2, A4_local
                            logger.error("[Error]: DAG_executor_work_loop: using processes or lambdas but storing FanINNBs locally.")
                        if not run_all_tasks_locally:
                            # Config: A1
    #rhc: cluster:
                            #Okay - this was set above
                            if worker_needs_input:
                                # Note: perhaps we csn use Lmbdas where Lambdas have two thread workers - faster?
                                logger.error("[Error]: DAG_executor_work_loop: using lambdas, so no workers, but worker_needs_input.")

#rhc: cluster:
                        #Note: we are using worker_needs_input so it needs to have been set above
                        if not run_all_tasks_locally or (run_all_tasks_locally and using_workers and not using_threads_not_processes and not worker_needs_input):
#rhc: run tasks
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

                        logger.debug("BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB")
                        logger.debug("work loop: async_call:" + str(async_call))

#rhc: cluster:
                        # for assert below
                        save_worker_needs_input = worker_needs_input

                        #Note: using worker processes - batch calls to fan_in for FaninNBs
                        worker_needs_input = process_faninNBs_batch(websocket,state_info.faninNBs, state_info.faninNB_sizes, 
                            state_info.task_name, DAG_info.get_DAG_states(), DAG_executor_state, 
                            output, DAG_info,work_queue,worker_needs_input, list_of_work_queue_or_payload_fanout_values,

                            async_call, state_info.fanouts,
#rhc: cluster:
                            cluster_queue,
#rhc: batch
                            DAG_info.get_all_faninNB_task_names(),
                            DAG_info.get_all_faninNB_sizes())

                        # assert:
                        if worker_needs_input and not using_workers:
                            logger.error("[Error]: Internal error: after process_faninNBs_batch"
                                + " worker_needs_input is True when not using workers.")

#rhc: cluster:
                        # assert:
                        if using_workers and worker_needs_input != save_worker_needs_input:
                        #    # Note: worker_needs_input was True at now it is false. 
                        #    # Note: process_faninNBs does not set process_faninNBs to True. It
                        #    # can only set worker_needs_input to False. So either worker_needs_input
                        #    # statrs as True and is set to False, or it starts as False and remains
                        #    # False. If worker_needs_input starts as True, then save_worker_needs_input
                        #    # will be True. If process_faninNBs sets worker_needs_input to False,
                        #    # then worker_needs_input and save_worker_needs_input will be different.
                        #    # When process_faninNBs sets worker_needs_input to False, it is because it
                        #    # also deposted a become task in the cluster queue, so the cluster_queue
                        #    # shudl not be different.
                            if cluster_queue.qsize() == 0:
                                logger.error("[Error]: Internal error: process_faninNBs_batch set"
                                    + " worker_needs_input to False when using workers but cluster_queue"
                                    + " size is 0.")
                    else: 
                        # Config: A2, A4_local, A4_Remote
                        # not using workers, or using worker threads not processes, or using threads to simualate running lambdas,
                        # but not using lambdas to store synch objects - storing them locally instead.
                        # Note: if we are using thread workers we can still store the FanInNBs remotely, but the work queue 
                        # will be local. Batch FanInNb processing will put work (fanin tasks) in the work_queue as the 
                        # work_queue is also stored remotely (with the FanInNBs). When using threads, the work_queue is local 
                        # so we cannot put work in the work queue while processing remote FanInNBs on the server. This means we 
                        # would have to pass all the work back here to the thread and have the thread add the work to the local work
                        # queue. Doable, but maybe later - multithreading is not as useful as multprocessng, and 
                        # we do batch FaninNBs and store the FanINNBs and work_queue remotely when multiprocessing
                        # (same for multiprocessing where processes are multithreaded, which is an interesting use case).
#rhc: cluster:
                        # for assert below
                        save_worker_needs_input = worker_needs_input

                        # Note: ignorning worker_needs_input if we are not using workers.
                        worker_needs_input = process_faninNBs(websocket,state_info.faninNBs, state_info.faninNB_sizes, 
                            state_info.task_name, DAG_info.get_DAG_states(), DAG_executor_state, 
                            output, DAG_info, server,work_queue,worker_needs_input,
#rhc: cluster:
                            cluster_queue)

                        if worker_needs_input and not using_workers:
                            logger.error("[Error]: Internal error: after process_faninNBs"
                                + " worker_needs_input is True when not using workers.")
#rhc: cluster:
                        # assert:
                        if using_workers and worker_needs_input != save_worker_needs_input:
                        #    # Note: See comment in same assertion above.
                            if cluster_queue.qsize() == 0:
                                logger.error("[Error]: Internal error: process_faninNBs set"
                                    + " worker_needs_input to False when using workers but cluster_queue"
                                    + " size is 0.")

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
                    if run_all_tasks_locally and using_workers and not using_threads_not_processes:
                        # Config: A5, A6
                        # we are batching faninNBs and piggybacking fanouts on process_faninNB_batch
                        if len(state_info.fanouts) > 0:
                            # No faninNBs (len(state_info.faninNBs) == 0) so we did not get a chance to 
                            # piggyback list_of_work_queue_or_payload_fanout_values on the call to process_faninNBs_batch.
 
# rhc: cluster:
                            # assert:
                            if cluster_queue.qsize()==0:
                            #if worker_needs_input:
                                # when there is at least one fanout we will become one of the fanout tasks
                                # so we should not need work.
                                logger.error("[Error]: work loop: Internal Error: fanouts but worker needs work.")
 
                            if len(state_info.fanouts) > 1:
                                # We became one fanout task but there were more and we should have added the 
                                # fanouts to list_of_work_queue_or_payload_fanout_values.
                                if len(list_of_work_queue_or_payload_fanout_values) == 0:
                                    logger.error("[Error]: work loop: Internal Error: fanouts > 1 but no work in list_of_work_queue_or_payload_fanout_values.")
                                # since we could not piggyback on process_faninNB_batch, enqueue the fanouts
                                # directly into the work_queue
                                logger.debug(thread_name + " work loop: no faninNBs so enqueue fanouts directly.")
                                # Note: if we use lambdas with a batch nvoker, here we will call the tcp_server
                                # method that dos the batch invokes. This method is probably used by 
                                # process_faninNBs_batch to invoke the fanouts that are piggybacked.
                                work_queue.put_all(list_of_work_queue_or_payload_fanout_values)
                                # list_of_work_queue_or_payload_fanout_values is redefined on next iteration
                            else:
                                # assert
                                # there was one fanout so we became that one fanout and should have enqueued no fanouts
                                if not len(list_of_work_queue_or_payload_fanout_values) == 0:
                                    logger.error("[Error]: work loop: Internal Error: len(state_info.fanouts) is 1 but list_of_work_queue_or_payload_fanout_values is not empty.")
                    # else: # Config: A1, A2, A3, A4_local, A4_Remote
                # If we are not using_workers and there were fanouts then continue with become 
                # task; otherwise, this thread (simulatng a Lambda) or Lambda is done, as it has reached the
                # end of its DFS path. (Note: if using workers and there are no fanouts and no
                # faninNBs for which we are the last thread to call fanin, the worker will get more
                # work from the work_queue instead of stopping. The worker continutes until it gets
                # a STOP state value from the work_queue (e.g., -1). Noet: If there are fanouts and/or 
                # faninNBs, there can be no fanins. Note, when we are not using_workers, the faninNBs
                # will start new threads to execute the fanin task, (or invoke new Lamdas when we 
                # are using Lambdas. So faninNBs cannot generate more work for a worker since the 
                # work is given to a new thread.)

                # rhc: If we are not using workers then this is a thread simulating
                # a lambda or a real lambda. The faninNBs started a thread or a 
                # real lambda to execute the faninNB task, i.e, we never become a faninNB
                # task as all the faninNB tasks are execute by strting  new lambda.
                # So we (the simulated or real lambda) may or may not have more work to do,
                # epending on whether or not we became a fanout task. If we are the 
                # become task of a fanout then we have more work to do. In that case, 
                # the DAG_executor_state.state is the state of the become task (a fanout task) 
                # so we can just keep going and this state will be used
                # in the next iteration of the work loop. 
                # 
                # We will execute a become task if we started this iteration with len(state_info.fanouts) > 0. 
                # This is because when there are more than one fanouts we grab the first
                # one as the become task. (The situation where the ADG has only one
                # fanout task and no faninNB tasks is handled by making this fanout 
                # task the "collapse task" so the number of collapse tasks in the state
                # will be > 0 and the number of fanout tasks will be 0). 
                # 
                # When we take a become task, we remove it from state_info.fanouts
                # and there may not be any other fanouts (there are faninNBs since we are 
                # here) so len(state_info.fanouts) will become 0. Thus, we capture the length of 
                # fanouts at the begining in starting_number_of_fanouts. If starting_number_of_fanouts
                # was > 0, then we made one fanout the become task and we should 
                # not return here, i.e., we should continue and execute the become task.
                # Note: without this check of starting_number_of_fanouts,
                # we will return prematurley in the case that there is 
                # one fanout and one or more faninNBs, as the number of 
                # fanouts will become 0 when we remove the become task 
                #if (not using_workers) and len(state_info.fannouts) == 0:

                if (not using_workers) and starting_number_of_fanouts == 0:
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
                    # situation where we have fanouts, and so will become a fanout
                    # task, and one or more "fanins", which means we cannot become
                    # any of the fanin tasks. In this case we use faninNB (fanin No
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
                    # That would make the work llop logic more complicated,
                    # Currently we either have one or more fanouts and one or more fanonNBs 
                    # or we have a single fanin. this optimization would add more
                    # cases to check for (the case where we have no fanouts but we
                    # have a fanin and one or more faninNBs. We would want to do 
                    # the fanin, and see whether we are the become task, and then 
                    # do the faninNBs, where if we are not the bcome task for the 
                    # fanin and we are using workers then we would become the 
                    # tanin task for one of the faninNBs.)

                    logger.debug(thread_name + " : returning after process fanouts/faninNBs")
                    return
                else:
                    # we are a worker so even if we will not become a fanout task 
                    # (starting_number_of_fanouts == 0) we can get more work from 
                    # the work queue, 
                    # or we are not a worker (not using_workers), which means we are a 
                    # thread simulating a lambda or a real lambda) and there was at least one fanout task 
                    # so we will become a fanout task (not starting_number_of_fanouts == 0); 
                    # thus, we should not terminate.
                    
                    logger.debug(thread_name + ": Not returning after process fanouts/faninNBs.")
                #else: # Config: A4_local, A4_Remote, A5, A6

            elif len(state_info.fanins) > 0:
                # assert len(state_info.faninNBs)  + len(state_info.fanouts) + len(collapse) == 0
                # if faninNBs or fanouts then can be no fanins. length of faninNBs and fanouts must be 0 
                DAG_executor_state.state = DAG_info.get_DAG_states()[state_info.fanins[0]]
                #state = DAG_info.get_DAG_states()[state_info.fanins[0]]
                #if len(state_info.fanins) > 0:
                #ToDo: Set next state before process_fanins, returned state just has return_value, which has input.
                # single fanin, try-op w/ returned_state.return_value or restart with return_value or deposit/withdraw it

#rhc: cluster:
                #returned_state = process_fanins(websocket,state_info.fanins, state_info.fanin_sizes, state_info.task_name, DAG_info.get_DAG_states(),  DAG_executor_state, output, server)
                DAG_exec_state = process_fanins(websocket,state_info.fanins, state_info.fanin_sizes, state_info.task_name, DAG_info.get_DAG_states(),  DAG_executor_state, output, server)
                logger.debug(thread_name + ": " + state_info.task_name + ": after call to process_fanin: " + str(state_info.fanins[0]) + " returned_state.blocking: " + str(DAG_exec_state.blocking) + ", returned_state.return_value: "
                    + str(DAG_exec_state.return_value) + ", DAG_executor_state.state: " + str(DAG_executor_state.state))

                if DAG_exec_state.return_value == 0:
                    # we are not the become task for the fanin
#rhc: cluster:
                    #Do not do this.
                    #Note: setting worker_needs_input = True must be guarded by using_workers
                    if using_workers:
                        ## Config: A4_local, A4_Remote, A5, A6
                        logger.debug(thread_name + ": After call to process_fanin: return value is 0; using workers so get more work.")
                        #worker_needs_input = True
                        pass
                    else:
                        # Config: A1, A2, A3
                        # this dfs path is finished
                        logger.debug(thread_name + ": After call to process_fanin: return value is 0; not using workers so lamba (thread or real) returns.")
                        return
                else:
                    if (run_all_tasks_locally and using_workers) or not run_all_tasks_locally:
                        # Config: A1, A4_local, A4_Remote, A5, A6
                        # when using workers, threads or processes, each worker has its own local
                        # data dictionay. If we are the become task for a fanin, we receive the 
                        # fanin task inputs and we put them in the data dctionary. Same for lambdas.
                        #dict_of_results = returned_state.return_value
#rhc: cluster: Note: using "DAG_exec_state" instead of "returned_state"
                        dict_of_results = DAG_exec_state.return_value
                        
                        # Also, don't pass in the multp data_dict, so will use the global.
                        # Fix if in global

                        logger.debug("fanin Results: ")
                        for key, value in dict_of_results.items():
                            logger.debug(str(key) + " -> " + str(value))
                        for key, value in dict_of_results.items():
                            data_dict[key] = value

                        #data_dict[state_info.task_name] = output

                        # we are the become task so execute the become task, where this is a worker
                        # or a dfs thread.
#rhc: cluster:
                        #Question: Do not do this
                        logger.debug(thread_name + ": After call to process_fanin: return value not 0.")
                        #logger.debug(thread_name + ": After call to process_fanin: return value not 0, using workers so set worker_needs_input = False")
                        #worker_needs_input = False
                    #else: # Config: A2, A3
                        # do not add results to (global) data_dict when using threads
                        # to simulate lambdas (which means we are not using workers and we are run_all_tasks_locally,
                        # or we are using real lambdas, i.e, not run_all_tasks_locally) since
                        # these threads already added their results to the global data_dict (shared by all
                        # the threads) before calling fanin.
#rhc: cluster:
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

                    logger.debug("DAG_executor_work_loop: add fanin becomes task to the clster_queue:"
                        + " state is " + str(DAG_exec_state.state))

            else:
                #Note: setting worker_needs_input = True must be guarded by using_workers
                if using_workers:
                    # Config: A4_local, A4_Remote, A5, A6
                    logger.debug(thread_name + ": state " + str(DAG_executor_state.state) + " after executing task " 
                        +  state_info.task_name + " has no collapse, fanouts, fanins, or faninNBs; using workers so no return.")
#rhc: cluster:
                    #Do NOT do this.
                    #logger.debug(thread_name + " set worker_needs_input to true")
                    #worker_needs_input = True
                else:
                    # Config: A1, A2, A3
                    logger.debug(thread_name + ": state " + str(DAG_executor_state.state) + " after executing task " +  state_info.task_name + " has no collapse, fanouts, fanins, or faninNBs; not a worker so return.")
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
    if not using_workers:
        DAG_exec_state = payload['DAG_executor_state']
    else:
        DAG_exec_state = DAG_executor_State(function_name = "DAG_executor", function_instance_ID = str(uuid.uuid4()))
    ##state = payload['state'] # refers to state var, not the usual State of DAG_executor 
    ##logger.debug("state:" + str(state))
    ##DAG_executor_state.state = payload['state']
    if not using_workers:
        # Config: A2
        logger.debug("payload state:" + str(DAG_exec_state.state))
    # For leaf task, we get  ['input': inp]; this is passed to the executed task using:
    #    def execute_task(task_name,input): output = DAG_info.DAG_tasks[task_name](input)
    # So the executed task gets ['input': inp], just like a non-leaf task gets ['output': X]. For leaf tasks, we use "input"
    # as the label for the value.

    #task_payload_inputs = payload['input']
    #logger.debug("DAG_executor starting payload input:" +str(task_payload_inputs) + " payload state: " + str(DAG_executor_state.state) )
  
    # reads from default file './DAG_info.pickle'
    DAG_info = DAG_Info.DAG_info_fromfilename()

#ToDo: Lambda: modify this local storage no workers version to put a threads payload input in the data_dict?
# Add comment that we are using global but we add here too to check logic. Here we can make sure data
# is already in data_dict as assert. if not key in data_dict:
    """
    DAG_map = DAG_info.get_DAG_map()
    state_info = DAG_map[DAG_exec_state.state]
    is_leaf_task = state_info.task_name in DAG_info.get_DAG_leaf_tasks()
    if not is_leaf_task:
        # lambdas invoked with inputs. We do not add leaf task inputs to the data
        # dictionary, we use them directly when we execute the leaf task.
        # Also, leaf task inputs are not in a dictionary.
        dict_of_results = payload['input']
        for key, value in dict_of_results.items():
            data_dict[key] = value
    """

    # work_queue is the global shared work queue, which is none when we are using threads
    # to simulate lambdas and is a Queue when we are using worker threads. See imported file
    # DAG_executor_work_queue_for_threads.py for Queue creation.

    global work_queue
#rhc continue
    global DAG_infobuffer_monitor

    #DAG_info = payload['DAG_info']
    #DAG_executor_work_loop(logger, server, counter, thread_work_queue, DAG_executor_state, DAG_info, data_dict)
#rhc: counter
# tasks_completed_counter, workers_completed_counter
    DAG_executor_work_loop(logger, server, completed_tasks_counter, completed_workers_counter, DAG_exec_state, DAG_info, 
#rhc continue
        work_queue,DAG_infobuffer_monitor)
    logger.debug("DAG_executor() method returned from work loop.")

# Config: A5, A6
# def DAG_executor_processes(payload,counter,process_work_queue,data_dict,log_queue, configurer)
#rhc: counter
# tasks_completed_counter, workers_completed_counter
def DAG_executor_processes(payload,completed_tasks_counter,completed_workers_counter,log_queue_or_logger, worker_configurer,
    shared_nodes,shared_map,shared_frontier_map,
    pagerank_sent_to_processes,previous_sent_to_processes,number_of_children_sent_to_processes,number_of_parents_sent_to_processes,starting_indices_of_parents_sent_to_processes,parents_sent_to_processes,IDs_sent_to_processes,):
    # Use for multiprocessing workers
    # Note: log_queue_or_logger is either a queue or a logger. If not 
    # use_multithreaded_multiprocessing it is a queue; otheriwise it is a logger.

    #- read DAG_info, create DAG_exec_state, thread_work_queue is parm
    if not use_multithreaded_multiprocessing:
        # Config: A5
        #global logger
#rhc: logging
        worker_configurer(log_queue_or_logger)
        logger = logging.getLogger("multiP")
    else:
        # Config: A6
        #worker_configurer(log_queue)
        #logger = logging.getLogger("multiP")
        #logger.setLevel(logging.DEBUG)

        # log_queue_or_logger is the logger, which was passed to each thread
        logger = log_queue_or_logger

    if compute_pagerank and use_shared_partitions_groups:
        #print(str(pagerank_sent_to_processes[:10]))
        if use_page_rank_group_partitions:
            BFS_Shared.shared_groups = shared_nodes
            BFS_Shared.shared_groups_map = shared_map
            BFS_Shared.shared_groups_frontier_parents_map = shared_frontier_map
        else:
            BFS_Shared.shared_partition = shared_nodes
            BFS_Shared.shared_partition_map = shared_map
            BFS_Shared.shared_partition_frontier_parents_map = shared_frontier_map
        if use_struct_of_arrays_for_pagerank:
            BFS_Shared.pagerank = pagerank_sent_to_processes
            BFS_Shared.previous = previous_sent_to_processes
            BFS_Shared.number_of_children = number_of_children_sent_to_processes
            BFS_Shared.number_of_parents = number_of_parents_sent_to_processes  
            BFS_Shared.starting_indices_of_parents = starting_indices_of_parents_sent_to_processes
            BFS_Shared.parents = parents_sent_to_processes
            BFS_Shared.IDs = IDs_sent_to_processes
        
    proc_name = multiprocessing.current_process().name
    thread_name = threading.current_thread().name
    logger.debug("proc " + proc_name + " " + " thread " + thread_name + ": started.")

    if not using_workers:
        logger.error("Error: DAG_executor_processes: executing multiprocesses but using_workers is false.")

    #logger = logging.getLogger('main')__name__
    #level = logging.DEBUG
    #message = (proc_name + ": testing 1 2 3.")
    #logger.log(level, message)
 
    #if not using_workers:
    #    DAG_exec_state = payload['DAG_executor_state']
    #else:
    DAG_exec_state = DAG_executor_State(function_name = "DAG_executor", function_instance_ID = str(uuid.uuid4()))

    #logger.debug("DAG_executor_processes: DAG_exec_state: " + str(DAG_exec_state))
    ##state = payload['state'] # refers to state var, not the usual State of DAG_executor 
    ##logger.debug("state:" + str(state))
    ##DAG_executor_state.state = payload['state']

    #if not using_workers:
    #    logger.debug("payload state:" + str(DAG_exec_state.state))

    # For leaf task, we get  ['input': inp]; this is passed to the executed task using:
    #    def execute_task(task_name,input): output = DAG_info.DAG_tasks[task_name](input)
    # So the executed task gets ['input': inp], just like a non-leaf task gets ['output': X]. For leaf tasks, we use "input"
    # as the label for the value.
    #task_payload_inputs = payload['input']
    #logger.debug("DAG_executor starting payload input:" +str(task_payload_inputs) + " payload state: " + str(DAG_executor_state.state) )
  
    # reads from default file './DAG_info.pickle'
    DAG_info = DAG_Info.DAG_info_fromfilename()

    # The work loop will create a BoundedBuffer_Work_Queue. Each process excuting the work loop
    # will create a BoundedBuffer_Work_Queue object, which wraps the websocket creatd in the 
    # work loop and the code to send work to the work queue on the tcp_server.
    work_queue = None
#rhc continue
    DAG_infobuffer_monitor = None

    #DAG_info = payload['DAG_info']
    #DAG_executor_work_loop(logger, server, counter, process_work_queue, DAG_exec_state, DAG_info, data_dict)
#rhc: counter
# tasks_completed_counter, workers_completed_counter
    DAG_executor_work_loop(logger, server, completed_tasks_counter, completed_workers_counter, DAG_exec_state, DAG_info, 
#rhc continue
        work_queue, DAG_infobuffer_monitor)
    logger.debug("DAG_executor_processes: returning after work_loop.")
    return

# Config: A1
def DAG_executor_lambda(payload):
    logger.debug("Lambda: started.")

    if not (store_sync_objects_in_lambdas and sync_objects_in_lambdas_trigger_their_tasks):
        DAG_exec_state = cloudpickle.loads(base64.b64decode(payload['DAG_executor_state']))
        DAG_info = cloudpickle.loads(base64.b64decode(payload['DAG_info']))
    else:
        DAG_exec_state = payload['DAG_executor_state'] 
        DAG_info = payload['DAG_info']  

    #logger.debug("payload DAG_exec_state.state:" + str(DAG_exec_state.state))

    DAG_map = DAG_info.get_DAG_map()
    
    state_info = DAG_map[DAG_exec_state.state]
    is_leaf_task = state_info.task_name in DAG_info.get_DAG_leaf_tasks()
    if not is_leaf_task:
        # lambdas invoked with inputs. We do not add leaf task inputs to the data
        # dictionary, we use them directly when we execute the leaf task.
        # Also, leaf task inputs are not in a dictionary.
        if not (store_sync_objects_in_lambdas and sync_objects_in_lambdas_trigger_their_tasks):
            dict_of_results = cloudpickle.loads(base64.b64decode(payload['input']))
        else:
            dict_of_results = payload['input']
        for key, value in dict_of_results.items():
            data_dict[key] = value
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
        if not (store_sync_objects_in_lambdas and sync_objects_in_lambdas_trigger_their_tasks):
            inp = cloudpickle.loads(base64.b64decode(payload['input']))
        else:
            inp = payload['input']
        state_info.task_inputs = inp

    # lambdas do not use work_queues, for now.
    work_queue = None
#rhc continue
    DAG_infobuffer_monitor = None

    # server and counter are None
    # logger is local lambda logger

#rhc: counter
# tasks_completed_counter, workers_completed_counter
    DAG_executor_work_loop(logger, server, completed_tasks_counter, completed_workers_counter, DAG_exec_state, DAG_info, 
#rhc continue
        work_queue,DAG_infobuffer_monitor )
    logger.debug("DAG_executor_processes: returning after work_loop.")
    return
                        
# Config: A4_local, A4_Remote
def DAG_executor_task(payload):
    DAG_executor_state = payload['DAG_executor_state']
    if DAG_executor_state != None:
        # DAG_executor_state is None when using workers
        logger.debug("DAG_executor_task: call DAG_executor(), state is " + str(DAG_executor_state.state))
    DAG_executor(payload)
    logger.debug("DAG_executor_task: returned from DAG_executor()")
    
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
	
    logger.debug("DAG_map:")
    for key, value in Node.DAG_map.items():
        logger.debug(key)
        logger.debug(value)
    logger.debug("  ")
	
    logger.debug("states:")         
    for key, value in Node.DAG_states.items():
        logger.debug(key)
        logger.debug(value)
    logger.debug("   ")
    
    logger.debug("DAG_tasks:")         
    for key, value in Node.DAG_tasks.items():
        logger.debug(key)
        logger.debug(value)
    logger.debug("   ")
	
    logger.debug("num_fanins:" + str(Node.num_fanins) + " num_fanouts:" + str(Node.num_fanouts) + " num_faninNBs:" 
        + " num_collapse:" + str(Node.num_collapse))
    logger.debug("   ")
	
    logger.debug("all_fanout_task_names")
    for name in Node.all_fanout_task_names:
        logger.debug(name)
        logger.debug("   ")
    logger.debug("   ")
	
    logger.debug("all_fanin_task_names")
    for name in Node.all_fanin_task_names :
        logger.debug(name)
        logger.debug("   ")
    logger.debug("   ")
		  
    logger.debug("all_faninNB_task_names")
    for name in Node.all_faninNB_task_names:
        logger.debug(name)
        logger.debug("   ")
    logger.debug("   ")
		  
    logger.debug("all_collapse_task_names")
    for name in Node.all_collapse_task_names:
        logger.debug(name)
        logger.debug("   ")
    logger.debug("   ")
	
    DAG_map = Node.DAG_map
    task_name_to_function_map =  Node.DAG_tasks
    
    logger.debug("DAG_map after assignment:")
    for key, value in DAG_map.items():
        logger.debug(key)
        logger.debug(value)
    logger.debug("   ")
    logger.debug("task_name_to_function_map after assignment:")
    for key, value in task_name_to_function_map.items():
        logger.debug(key)
        logger.debug(value)
    logger.debug("   ")
    
    #states = Node.DAG_states
    #all_fanout_task_names = Node.all_fanout_task_names
    #all_fanin_task_names = Node.all_fanin_task_names
    #all_faninNB_task_names = Node.all_faninNB_task_names
    #all_collapse_task_names = Node.all_collapse_task_names

	
	# ToDo: logger.debug Node.DAG_map
	#		logger.debug Node.states (after making states global)
	# 		logger.debug Node.all_fanin_task_names (for synch objects)
	#		logger.debug Node.all_faninNB_task_names (for synch objects)

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
        logger.debug("Starting DAG_executor thread for state 1")
        payload = {
            #"state": int(1),
            "input": {'input': int(0)},
			"DAG_executor_state": DAG_executor_State1,
            "server": server
        }
        _thread.start_new_thread(DAG_executor_task, (payload,))
    except Exception as ex:
        logger.error("[ERROR] Failed to start DAG_executor thread for state 1")
        logger.debug(ex)
        
    try:
        DAG_executor_State3 = DAG_executor_State(state = int(3))
        logger.debug("Starting DAG_executor thread for state 3")
        payload = {
            #"state": int(3),
            "input": {'input': int(1)},
			"DAG_executor_state": DAG_executor_State3,
            "server": server
        }
        _thread.start_new_thread(DAG_executor_task, (payload,))
    except Exception as ex:
        logger.error("[ERROR] Failed to start DAG_executor thread for state 3")
        logger.debug(ex)
        
    logger.debug("Sleeping")
    time.sleep(5)
	
    
if __name__=="__main__":
    main()


##Xtras:
"""
def add(inp):
    logger.debug("add: " + "input: " + str(input))
    num1 = inp['inc0']
    num2 = inp['inc1']
    sum = num1 + num2
    output = {'add': sum}
    logger.debug("add output: " + str(sum))
    return output
def multiply(inp):
    logger.debug("multiply")
    num1 = inp['add']
    num2 = inp['square']
    num3 = inp['triple']
    product = num1 * num2 * num3
    output = {'multiply': product}
    logger.debug("multiply output: " + str(product))
    return output
def divide(inp):
    logger.debug("divide")
    num1 = inp['multiply']
    quotient = num1 / 72
    output = {'quotient': quotient}
    logger.debug("quotient output: " + str(quotient))
    return output
def triple(inp):
    logger.debug("triple")
    value = inp['inc1']
    value *= 3
    output = {'triple': value}
    logger.debug("triple output: " + str(output))
    return output
def square(inp):
    logger.debug("square")
    value = inp['inc1']
    value *= value
    output = {'square': value}
    logger.debug("square output: " + str(output))
    return output
def inc0(inp):
    logger.debug("inc0")
    value = inp['input']
    value += 1
    output = {'inc0': value}
    logger.debug("inc0 output: " + str(output))
    return output
def inc1(inp):
    logger.debug("inc1")
    value = inp['input']
    value += 1
    output = {'inc1': value}
    logger.debug("inc1 output: " + str(output))
    return output
"""