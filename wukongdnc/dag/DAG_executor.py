
import logging

logger = None
logger = logging.getLogger(__name__)
"""
logger.setLevel(logging.ERROR)
formatter = logging.Formatter('[%(asctime)s] [%(threadName)s] %(levelname)s: %(message)s')
ch = logging.StreamHandler()
ch.setLevel(logging.ERROR)
ch.setFormatter(formatter)
logger.addHandler(ch)
"""

import threading
import _thread
import time
import socket
import cloudpickle 
import base64 

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
import uuid
from wukongdnc.constants import TCP_SERVER_IP
from .DAG_executor_constants import run_all_tasks_locally, store_fanins_faninNBs_locally 
from .DAG_executor_constants import create_all_fanins_faninNBs_on_start, using_workers 
from .DAG_executor_constants import using_threads_not_processes, use_multithreaded_multiprocessing
from .DAG_executor_constants import process_work_queue_Type, FanInNB_Type, using_Lambda_Function_Simulators_to_Store_Objects
#from .DAG_work_queue_for_threads import thread_work_queue
from .DAG_executor_work_queue_for_threads import work_queue
from .DAG_data_dict_for_threads import data_dict
from .DAG_executor_counter import counter
from .DAG_executor_synchronizer import server
from wukongdnc.wukong.invoker import invoke_lambda_DAG_executor
from .DAG_boundedbuffer_work_queue import BoundedBuffer_Work_Queue
from .util import pack_data

import logging.handlers
import multiprocessing


def create_and_faninNB_task_locally(kwargs):
    logger.debug("create_and_faninNB_task: call create_and_faninNB_locally")
    server = kwargs['server']
    # Not using return_value from faninNB since faninNB starts the fanin task, i.e., there is No Become
    return_value_ignored = server.create_and_faninNB_locally(**kwargs)

def faninNB_task_locally(kwargs):
    logger.debug("faninNB_task: call faninNB_locally")
    server = kwargs['server']
    # Not using return_value from faninNB since faninNB starts the fanin task, i.e., there is No Become
    return_value_ignored = server.faninNB_locally(**kwargs)

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

def create_and_faninNB_remotely(websocket,**keyword_arguments):
    # pass
    # need code that returns a  DAG_exec_state = synchronize_sync()
    return DAG_executor_State()

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
    
    if not run_all_tasks_locally:
        # Note: When faninNB start a Lambda, DAG_info is in the payload. 
        # (Threads and processes read it from disk.)
        DAG_exec_state.keyword_arguments['DAG_info'] = keyword_arguments['DAG_info']
    DAG_exec_state.return_value = None
    DAG_exec_state.blocking = False

#ToDo: when using lambdas, so faninNB will start a lmabda to execute the fanin task, we can use 
# an asynch call since we ignore the return value?'
    DAG_exec_state = synchronize_sync(websocket, "synchronize_sync", keyword_arguments['fanin_task_name'], "fan_in", DAG_exec_state)
    return DAG_exec_state

def create_and_faninNB_remotely_batch(websocket,**keyword_arguments):
    pass
    return DAG_executor_State()

#def process_faninNBs(websocket,faninNBs, faninNB_sizes, calling_task_name, DAG_states, DAG_exec_state, output, DAG_info, server):
def process_faninNBs(websocket,faninNBs, faninNB_sizes, calling_task_name, DAG_states, 
    DAG_exec_state, output, DAG_info, server, work_queue,worker_needs_input):
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
        # We will use local datadict for each multiprocess; process will receve
        # the faninNB results and put them in the data_dict
        keyword_arguments['result'] = output
        keyword_arguments['calling_task_name'] = calling_task_name
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
                        logger.debug(thread_name + ": process_faninNBs: Starting create_and_fanin for faninNB " + name)
                        NBthread = threading.Thread(target=create_and_faninNB_task_locally, name=("create_and_faninNB_task_"+name), args=(keyword_arguments,))
                        NBthread.start()
                    except Exception as ex:
                        logger.error("[ERROR]:" + thread_name + ": process_faninNBs: Failed to start create_and_faninNB_task thread.")
                        logger.debug(ex)
                        return 0
                else:
                    try:
                        logger.debug(thread_name + ": process_faninNBs: Starting faninNB_task for faninNB " + name)
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
                    return_value_ignored = server.create_and_faninNB_locally(**keyword_arguments)
                else:
                    logger.debug(thread_name + ": process_faninNBs: call faninNB_locally")
                    #server = kwargs['server']
                    # Not using return_value from faninNB since faninNB starts the fanin task, i.e., there is No Become
                    return_value_ignored = server.faninNB_locally(**keyword_arguments)

            #Note: returning 0 since when running locally the faninNB will start 
            # the fanin task so there is nothing to do. Process fanouts next.
        else:
            if not create_all_fanins_faninNBs_on_start:
                dummy_DAG_exec_state = create_and_faninNB_remotely(websocket,**keyword_arguments)
            else:
                dummy_DAG_exec_state = faninNB_remotely(websocket,**keyword_arguments)

            #if DAG_exec_state.blocking:
            # using the "else" after the return, even though we don't need it
            logger.debug(thread_name + ": process_faninNBs:  faninNB_remotely dummy_DAG_exec_state: " + str(dummy_DAG_exec_state))
            logger.debug(thread_name + ": process_faninNBs:  faninNB_remotely dummy_DAG_exec_state.return_value: " + str(dummy_DAG_exec_state.return_value))
            if dummy_DAG_exec_state.return_value == 0:
                #DAG_exec_state.blocking = False
                # nothing to Do
                # return worker_needs_input
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

                        # keep work and do it next
                        worker_needs_input = False
                        DAG_exec_state.state = start_state_fanin_task
                        logger.debug(thread_name + ": process_faninNBs:  set worker_needs_input to False.")

                else: 
                    # not using_workers so we are using threads to simulate using lambdas.
                    # However, since the fannNB on tcp_server cannot crate a thread to execute
                    # the fanin task (since the thread would run on the tcp_server) the start
                    # state is returned and we create the thread here. This deviates from the
                    # lambda simulation since the faninNB on the tcp_server can invoke lamdas to
                    # execute the fanin task.
                    
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
    DAG_exec_state.keyword_arguments['faninNB_sizes'] = keyword_arguments['faninNB_sizes']
    DAG_exec_state.keyword_arguments['worker_needs_input'] = keyword_arguments['worker_needs_input']
    DAG_exec_state.keyword_arguments['work_queue_name'] = keyword_arguments['work_queue_name']
    DAG_exec_state.keyword_arguments['work_queue_type'] = keyword_arguments['work_queue_type']
    DAG_exec_state.keyword_arguments['work_queue_method'] = keyword_arguments['work_queue_method']
    DAG_exec_state.keyword_arguments['work_queue_op'] = keyword_arguments['work_queue_op']
    DAG_exec_state.keyword_arguments['DAG_states_of_faninNBs'] = keyword_arguments['DAG_states_of_faninNBs']

    if not run_all_tasks_locally:
        # Note: When faninNB start a Lambda, DAG_info is in the payload so pass DAG_info to faninNBs.
        # (Threads and processes read it from disk.)
        DAG_exec_state.keyword_arguments['DAG_info'] = keyword_arguments['DAG_info']

    # piggybacking the (possibly empty) list if work_tuples generated by the fanouts, if any
    DAG_exec_state.keyword_arguments['list_of_work_queue_fanout_values'] = keyword_arguments['list_of_work_queue_fanout_values']

    # Don't do/need this?
    # DAG_exec_state.keyword_arguments['DAG_executor_State'] = new_DAG_exec_state # given to the thread/lambda that executes the fanin task.
    
    # No longer putting this in payload of fanin task
    # keyword_arguments['server'] = server

    DAG_exec_state.return_value = None
    DAG_exec_state.blocking = False

#ToDo: when using lambdas, so faninNB will start a lmabda to execute the fanin task, we can use 
# an asynch call since we ignore the return value?
# Note: perhaps for lambdas we can use the same synchronize_process_faninNBs_batch method but just
# pass worker_needs_input = False then FaninNBs will start Lambdas and no work will be returned and
# we can ignore the results.
# ToDo: For multiprocess, could start a thread that calls synchronize_process_faninNBs_batch.
# ToDo: For lambdas, FaninNBs start fanin tasks so no work to wait for so make 
#       synchronize_process_faninNBs_batch_async which does not wait for ack. Then change existing name to 
#       synchronize_process_faninNBs_batch_sync.
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
    DAG_exec_state, output, DAG_info, work_queue,worker_needs_input,list_of_work_queue_fanout_values):
    thread_name = threading.current_thread().name
    logger.debug(thread_name + ": process_faninNBs_batch: " + calling_task_name)
    logger.debug(thread_name + ": process_faninNBs_batch: " + calling_task_name + ": worker_needs_input: " + str(worker_needs_input))
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
    keyword_arguments['faninNB_sizes'] = faninNB_sizes   
    keyword_arguments['worker_needs_input'] = worker_needs_input  
    keyword_arguments['work_queue_name'] = "process_work_queue"
    # defined in DAG_executor_constants: either select or non-select version
    keyword_arguments['work_queue_type'] = process_work_queue_Type 
    keyword_arguments['work_queue_method'] = "deposit_all"
    keyword_arguments['work_queue_op'] = "synchronize_async"
    keyword_arguments['DAG_info'] = DAG_info
    # get a slice of DAG_states that is the DAG states of just the faninNB tasks.
    # Instead of sending all the DAG_states, i.e., all the states in the DAG, to the server.
    # Need this to put any work that is not returned in the work_queue - work is added as
    # a tuple (start state of task, inputs to task)
    DAG_states_of_faninNBs = {}
    for name in faninNBs:
        DAG_states_of_faninNBs[name] = DAG_states[name]
    keyword_arguments['DAG_states_of_faninNBs'] = DAG_states_of_faninNBs

    keyword_arguments['list_of_work_queue_fanout_values'] = list_of_work_queue_fanout_values

 	#ToDo: kwargs put in DAG_executor_State keywords and on server it gets keywords from state and passes to create and fanin

    if not create_all_fanins_faninNBs_on_start:
        dummy_DAG_exec_state = create_and_faninNB_remotely_batch(websocket,**keyword_arguments)
    else:
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
        # or we are simulating lambas and storing synch objects remotely. (If we are simulating lambdas
        # and storing synch objects locally, the FaninNBs start threads (locally) to run the fan_in tasks.)
        if not (using_workers and worker_needs_input) and not (not store_fanins_faninNBs_locally and run_all_tasks_locally):
            logger.error("[Error]: " + thread_name + ": process_faninNBs_batch: not using workers, or worker_needs_input is False and not (storing synch objects remotely"
                + " and simulting lambdas) but using process_faninNBs batch and we got work.")

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
                # Note: asserted using_workers and worker_needs_input above
                # this should be unreachable; leaving it for now.
                # If we don't need work then any work from faninNBs should have been enqueued in work queue
                logger.error("[Error]: " + thread_name + ": process_faninNBs_batch: Internal Error: got work but not worker_needs_input.")
                
                # Also, don't pass in the multp data_dict, so will use the global data dict
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
                # keep work and do it next
                worker_needs_input = False
                DAG_exec_state.state = start_state_fanin_task
                logger.debug(thread_name + ": process_faninNBs_batch: " + calling_task_name + ": got work, added it to data_dict, set worker_needs_input to False.")
        else:

            if not (not store_fanins_faninNBs_locally and run_all_tasks_locally):
                logger.error(thread_name + ": process_faninNBs_batch: " + calling_task_name + ": Internal Error: not using_workers and not (storing synch objects remotely"
                    + " and simulting lambdas) but using process_faninNBs batch.")

            # Not using_workers so we are using threads to simulate using lambdas.
            # However, since the faninNB on tcp_server cannot crate a thread to execute
            # the fanin task (since the thread would run on the tcp_server) the start
            # stare is returned and we create the thread here. This deviates from the
            # lambda simulation since the faninNB on the tcp_server can invoke lamdas to
            # execute the fanin task.

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
    output, DAG_info, server, work_queue, list_of_work_queue_fanout_values):

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

    # process rest of fanins
    logger.debug(thread_name + ": process_fanouts: run_all_tasks_locally:" + str(run_all_tasks_locally))
#ToDo:
    for name in fanouts:
        if using_workers:
            #thread_work_queue.put(DAG_states[name])
            if not using_threads_not_processes: # using processes
                dict_of_results =  {}
                dict_of_results[calling_task_name] = output
                work_tuple = (DAG_states[name],dict_of_results)
                #work_queue.put(DAG_states[name])

                # We will be batch processing the faninNBs so we will also batch process
                # the fanouts at the same time. If there are any faninBs in this state
                # piggyback the fanouts on the call to process faninNBs. If there are 
                # no faninNBs we will send this list to the work_queue directly. Note:
                # we could check at the end of this method whether there are any
                # faninNBs and if, not, call work_queue.put.

                list_of_work_queue_fanout_values.append(work_tuple)
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
                dict_of_results =  {}
                dict_of_results[calling_task_name] = output
                work_tuple = (DAG_states[name],dict_of_results)
                #work_queue.put(DAG_states[name])
                work_queue.put(work_tuple)
        else:
            if run_all_tasks_locally:
                try:
                    logger.debug(thread_name + ": process_fanouts: Starting fanout DAG_executor thread for " + name)
                    fanout_task_start_state = DAG_states[name]
                    # rhc: DES
                    task_DAG_executor_State = DAG_executor_State(function_name = "DAG_executor", function_instance_ID = str(uuid.uuid4()), state = fanout_task_start_state)

                    #output_tuple = (calling_task_name,)
                    #output_dict[calling_task_name] = output
                    logger.debug (thread_name + ": process_fanouts: fanout payload for " + name + " is " + str(fanout_task_start_state) + "," + str(output))
                    payload = {
                        #"state": fanout_task_start_state,
                        # If not using workers and running tasks locally then we are using threads
                        # to simulate Lambdas but threads currently use a global data_dict so they
                        # just put task outputs in the data_dict. We pass the outputs to the fanout
                        # tasks but they do not use "inp" since the "inp" is already in the 
                        # global data dict. We could pass "inp" in this case just to check the 
                        # logic used by the Lambdas.
                        # The driver just passes the dag executor state. We do not use
                        # server, we input DAG_info from file. 

#ToDO: lambda:          # We do not currently use the input, 
                        # but may use it to be consistent with lambdas: ==> pass state and input
                        # to the threads that simulate Lambdas. (We could also pass DAG_info to be
                        # consistent with Lambda version.)

                        #"input": output,
                        "DAG_executor_state": task_DAG_executor_State,
                        #"DAG_info": DAG_info,
                        #"server": server
                    }
                    _thread.start_new_thread(DAG_executor_task, (payload,))
                except Exception as ex:
                    logger.error("[ERROR] " + thread_name + ": process_fanouts: Failed to start DAG_executor thread for " + name)
                    logger.debug(ex)
            else:
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
                    results = {}
                    results[calling_task_name] = output
                    payload = {
#ToDo: Lambda:          # use parallel invoker with list piggybacked on batch fanonNBs, as usual
                        #"state": int(fanout_task_start_state),
                        #"input": output,
                        "input": results,
                        "DAG_executor_state": lambda_DAG_executor_state,
                        "DAG_info": DAG_info
                        #"server": server   # used to mock server during testing
                    }
                    ###### DAG_executor_State.function_name has not changed
                    invoke_lambda_DAG_executor(payload = payload, function_name = "DAG_Executor_Lambda")
                except Exception as ex:
                    logger.error(":ERROR] " + thread_name + " process_fanouts: Failed to start DAG_executor Lambda.")
                    logger.error(ex)

    # Note: If we do not piggyback the fanouts with process_faninNBs_batch, we would add
    # all the work to the remote work queue here.
    #if using_workers and not using_threads_not_processes:
    #   work_queue.put_all(list_of_work_queue_fanout_values)

    return become_start_state

def create_and_fanin_remotely(websocket,DAG_exec_state,**keyword_arguments):
    pass

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
    DAG_exec_state = synchronize_sync(websocket, "synchronize_sync", keyword_arguments['fanin_task_name'], "fan_in", DAG_exec_state)
    logger.debug (thread_name + ": fanin_remotely: calling_task_name: " + keyword_arguments['calling_task_name'] + " back from synchronize_sync")
    logger.debug (thread_name+ ": fanin_remotely: returned DAG_exec_state.return_value: " + str(DAG_exec_state.return_value))
    return DAG_exec_state

def process_fanins(websocket,fanins, faninNB_sizes, calling_task_name, DAG_states, DAG_exec_state, output, server):
    thread_name = threading.current_thread().name
    logger.debug(thread_name + ": fanin_remotely: calling_task_name: " + calling_task_name)

    # assert len(fanins) == len(faninNB_sizes) ==  1

    thread_name = threading.current_thread().name

    # call synch-op try-op on the fanin with name f, passing output and start_state.
    # The return value will have [state,input] tuple? Or we know state when we call
    # the fanin, we just don't know whether we become task. If not, we return. If
    # become, then we set state = states[f] before calling fanin.
    # Note: Only one fanin object so only one state for become. We pass our State and get it back
    # with the fanin results for the other tasks that fanin. We will add our results in create_and_fanin
    # using return_value[calling_task_name] = output[calling_task_name], which is add our results from output
    # to those returned by the fanin.

    keyword_arguments = {}
    logger.debug(thread_name + ": fanin_remotely: fanins" + str(fanins))
    keyword_arguments['fanin_task_name'] = fanins[0]
    keyword_arguments['n'] = faninNB_sizes[0]
    #keyword_arguments['start_state_fanin_task'] = DAG_states[fanins[0]]
    keyword_arguments['result'] = output
    keyword_arguments['calling_task_name'] = calling_task_name
    # Don't do/need this.
    #keyword_arguments['DAG_executor_State'] = DAG_exec_state
    keyword_arguments['server'] = server

    if not store_fanins_faninNBs_locally:
        DAG_exec_state.keyword_arguments = {}
        DAG_exec_state.keyword_arguments['fanin_task_name'] = fanins[0]
        DAG_exec_state.keyword_arguments['n'] = faninNB_sizes[0]
        DAG_exec_state.keyword_arguments['result'] = output
        DAG_exec_state.keyword_arguments['calling_task_name'] = calling_task_name
        #DAG_exec_state.keyword_arguments['DAG_executor_State'] = DAG_exec_state
        DAG_exec_state.keyword_arguments['server'] = server

	     
    if store_fanins_faninNBs_locally:
        #ToDo:
        #keyword_arguments['DAG_executor_State'] = DAG_exec_state
        if not create_all_fanins_faninNBs_on_start:
            DAG_exec_state = server.create_and_fanin_locally(DAG_exec_state,keyword_arguments)
        else:
            logger.debug(thread_name + ": fanin_remotely: " + calling_task_name + ": call server.fanin_locally")
            DAG_exec_state = server.fanin_locally(DAG_exec_state,keyword_arguments)
    else:
        if not create_all_fanins_faninNBs_on_start:
            # Note: might wan to send the result for debugging
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
            create_and_fanin_remotely(websocket,DAG_exec_state, **keyword_arguments)
        else:
            #if not using_lambdas:
                # if we call a remote fanin and locally we are not using lambdas,
                # then we need not pass the result of this task since we will not
                # be passing the fanin task inputs back - each task's results will 
                # have been put in the data_dict and the fanin task will get those
                # results, which are its inputs, from the data_dict. This makes the 
                # cost of the send for the fanin operaton less costly.

                #N ote: A worker process/lambda that is the last caller of fan-in will be the 
                # become task and the results from the non-becme workers will be passed
                # back from the fan-in. The woker/lambda will add the results to its local
                # data dictionary. So we must pass our result to the fanin so it can possbly
                # be saved on some other worker process/lambda's data dict. 
                
                #DAG_exec_state.keyword_arguments['result'] = None

            DAG_exec_state = fanin_remotely(websocket, DAG_exec_state, **keyword_arguments)
            logger.debug (thread_name + ": fanin_remotely: process_fanins: call to fanin_remotely returned DAG_exec_state.return_value: " + str(DAG_exec_state.return_value))

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
def DAG_executor_work_loop(logger, server, counter, DAG_executor_state, DAG_info, work_queue):

    DAG_map = DAG_info.get_DAG_map()
    DAG_tasks = DAG_info.get_DAG_tasks()
    num_tasks_to_execute = len(DAG_tasks)
    logger.debug("DAG_executor: number of tasks in DAG to execute: " + str(num_tasks_to_execute))
    #server = payload['server']
    proc_name = multiprocessing.current_process().name
    thread_name = threading.current_thread().name
    logger.debug("DAG_executor_work_loop: proc " + proc_name + " " + " thread " + thread_name + ": started.")
    
    #ToDo:
    #if input == None:
        #pass  # withdraw input from payload.synchronizer_name
    
    # Note: if not using_workers then worker_needs_input is initialized to False and every set 
    # of worker_needs_input to True is guarded by "if using_workers" so worker_needs_input is never
    # set to True if not using_workers.
    worker_needs_input = using_workers

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as websocket:

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
            work_queue = BoundedBuffer_Work_Queue(websocket,2*num_tasks_to_execute)
        #else: # Config: A1, A2, A3, A4_local, A4_Remote

        while (True):

        # workers don't always get work from the queue. i.e., no get when no put.
        # - collapse: no get; fanout: become so not get; fanin: - become no get + not-become do gets
        # - faninB: no becomes - last caller does put, all callers need to do gets

            if using_workers:
                # Config: A4_local, A4_Remote, A5, A6
                if worker_needs_input:
                    #DAG_executor_state.state = thread_work_queue.get(block=True)
                    if not using_threads_not_processes:
                        # Config: A5, A6
                        # blocking call
                        #DAG_executor_state.state = work_queue.get()
                        logger.debug("DAG_executor_work_loop: proc " + proc_name + " " + " thread " + thread_name + ": get work.")
                        work_tuple = work_queue.get()
                        DAG_executor_state.state = work_tuple[0]
                        dict_of_results = work_tuple[1]
                        logger.debug("work_loop: got work for thread " + thread_name)
                        if dict_of_results != None:
                            logger.debug("dict_of_results: ")
                            for key, value in dict_of_results.items():
                                logger.debug(str(key) + " -> " + str(value))
                            for key, value in dict_of_results.items():
                                data_dict[key] = value
                        #if DAG_executor_state.state == -1:
                        #    logger.debug("DAG_executor: state is -1 so returning.")
                        #    work_queue.put(DAG_executor_state,-1)
                        #   return
                    else:
                        # Config: A4_local, A4_Remote
                        # blocking call
                        #DAG_executor_state.state = work_queue.get() 
                        logger.debug("work_loop: get work for thread " + thread_name)
                        work_tuple = work_queue.get()
                        DAG_executor_state.state = work_tuple[0]
                        dict_of_results = work_tuple[1]
                        logger.debug("work_loop: got work for thread " + thread_name)
                        if dict_of_results != None:
                            logger.debug("dict_of_results: ")
                            for key, value in dict_of_results.items():
                                logger.debug(str(key) + " -> " + str(value))
                            # Threads put task outputs in a data_dict that is global to the threads
                            # so there is no need to do it again, when getting work from the work_queue.
                            #for key, value in dict_of_results.items():
                            #    data_dict[key] = value                    
                        
                    logger.debug("**********************withdrawn state for thread: " + thread_name + " :" + str(DAG_executor_state.state))

                    if DAG_executor_state.state == -1:
                        logger.debug("DAG_executor: state is -1 so deposit another -1 and return.")
                        #Note: we are not passing the DAG_executor_state of this 
                        # DAG_executor to tcp_server. We are not using a work_queue
                        # with Lamdas so we are not going to woory about using the
                        # convention that we pass DAG_executor_state in case we want to 
                        # do a restart - again, we wll not be restarting Lambdas due
                        # to blocking on a work_queue get() so we do not pass
                        # DAG_executor_state here. 
                        # Also, this makes the thread and process work_queues have the 
                        # same interface.
                        if not using_threads_not_processes:
                            # Config: A5, A6
                            work_tuple = (-1,None)
                            #work_queue.put(-1)
                            work_queue.put(work_tuple)
                        else:
                            # Config: A4_local, A4_Remote
                            #thread_work_queue.put(-1)
                            work_tuple = (-1,None)
                            #work_queue.put(-1)
                            work_queue.put(work_tuple)
                        return  

                    # Note: using_workers is checked above and must be True
                    worker_needs_input = False # default
                    #commented out for MM
                    #logger.debug("DAG_executor: Worker accessed work_queue: process state: ") # + str(DAG_executor_state.state))
                else:
                    logger.debug(thread_name + "DAG_executor: Worker doesn't access work_queue")
                    logger.debug("**********************" + thread_name + " process state: " + str(DAG_executor_state.state))
                
                num_tasks_executed = counter.increment_and_get()
                logger.debug("DAG_executor: " + thread_name + " before processing " + str(DAG_executor_state.state) 
                    + " num_tasks_executed: " + str(num_tasks_executed) 
                    + " num_tasks_to_execute: " + str(num_tasks_to_execute))
                if num_tasks_executed == num_tasks_to_execute:
                    #thread_work_queue.put(-1)
                    if not using_threads_not_processes:
                        # Config: A5, A6
                        logger.debug(thread_name + ": DAG_executor: num_tasks_executed == num_tasks_to_execute: depositing -1 in work queue.")
                        work_tuple = (-1,None)
                        #work_queue.put(-1)
                        work_queue.put(work_tuple)
                    else:
                        # Config: A4_local, A4_Remote
                        logger.debug(thread_name + ": DAG_executor: num_tasks_executed == num_tasks_to_execute: depositing -1 in work queue.")
                        #thread_work_queue.put(-1)
                        work_tuple = (-1,None)
                        #work_queue.put(-1)
                        work_queue.put(work_tuple)
                    #return
            # else: # Config: A1. A2, A3

            logger.debug (thread_name + ": access DAG_map with state " + str(DAG_executor_state.state))
            state_info = DAG_map[DAG_executor_state.state]
            ##logger.debug ("access DAG_map with state " + str(state))
            ##state_info = DAG_info.DAG_map[state]

            #commented out for MM
            #logger.debug("state_info: " + str(state_info))
            logger.debug(thread_name + " execute task: " + state_info.task_name)

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
            # nodes in the GAD_executor_driver and then null out the list of leaf task inputs in DAG_info
            # and the leaf task inputs in state_info.task_inputs for each leaf task (which is also in 
            # DAG_info) We do this since for lambdas we pass DAG_info in the payload and we don't want to 
            # pass all those leaf task inputs in DAG_info in each payload.
            task_inputs = state_info.task_inputs    
            is_leaf_task = state_info.task_name in DAG_info.get_DAG_leaf_tasks()
            if not is_leaf_task:
                logger.debug("Packing data. Task inputs: %s. Data dict (keys only): %s" % (str(task_inputs), str(data_dict.keys())))
                # task_inputs is a tuple of task_names
                args = pack_data(task_inputs, data_dict)
            else:
            # task_inputs is a tuple of input values, e.g., '1'
                args = task_inputs

            # using map DAG_tasks from task_name to task
            task = DAG_tasks[state_info.task_name]
            #output = execute_task(task,input)
            output = execute_task(task,args)
            """ where:
                def execute_task(task,args):
                    logger.debug("input of execute_task is: " + str(args))
                    #output = task(input)
                    output = task(*args)
                    return output
            """

            # data_dict may be local (A1) to process/lambda or global (A2) to threads
            logger.debug(thread_name + " execute_task output: " + str(output))
            data_dict[state_info.task_name] = output

            logger.debug("data_dict: " + str(data_dict))

            # Can use this sleep to make a thread last to call FaninNB - adjust the state in which you want
            # the call to fan_in to be last. Last caller can get the faninNB task work, if it has 
            # worker_needs_input = True on call (so the state has no fanouts, as thread will be become
            # task for first fanout so that thread will not need work from its FamInNBs.)
            #if DAG_executor_state.state == 1:
            #    time.sleep(0.5)

            if len(state_info.collapse) > 0:
                if len(state_info.fanins) + len(state_info.fanouts) + len(state_info.faninNBs) > 0:
                    logger.error("Error1")
                # execute collapsed task next - transition to new state and iterate loop
                # collapse is a list [] so get task name of the collapsed task which is collapse[0],
                # the only name in this list
                DAG_executor_state.state = DAG_info.get_DAG_states()[state_info.collapse[0]]
                ##state = DAG_info.get_DAG_states()[state_info.collapse[0]]
                # output of just executed task is input of next (collapsed) task
                #input = output

                #task_inputs = (state_info.task_name,)
                # We get new state_info and then state_info.task_inputs when we iterate

                # Don't add to thread_work_queue just do it
                #thread_work_queue.put(DAG_executor_state.state)
                if using_workers: 
                    # Config: A4_local, A4_Remote, A5, A6
                    worker_needs_input = False
                # else: # Config: A1. A2, A3

            elif len(state_info.faninNBs) > 0 or len(state_info.fanouts) > 0:
                # assert len(collapse) + len(fanin) == 0
                # If len(state_info.collapse) > 0 then there are no fanins, fanouts, or faninNBs and we will not excute this elif or the else

                
                # list of fanouts to deposit into work_queue piggybacking on call to batch fanins.
                # This is used when using_workers and not using_threads_not_processes, which is 
                # when we process the faninNBs in a batch
                list_of_work_queue_fanout_values = []

                # Check fanouts first so we know whether we have a become task for 
                # fanout. If we do, then we dont need any work generated by faninNBs.
                if len(state_info.fanouts) > 0:
                    # start DAG_executor in start state w/ pass output or deposit/withdraw it
                    # if deposit in synchronizer need to pass synchronizer name in payload. If synchronizer stored
                    # in Lambda, then fanout task executed in that Lambda.
                    DAG_executor_state.state = process_fanouts(state_info.fanouts, state_info.task_name, DAG_info.get_DAG_states(), DAG_executor_state, 
                        output, DAG_info, server,work_queue,list_of_work_queue_fanout_values)
                    logger.debug(thread_name + " work_loop: become state:" + str(DAG_executor_state.state))
                    logger.debug(thread_name + " work_loop: list_of_work_queue_fanout_values length:" + str(len(list_of_work_queue_fanout_values)))

                    # at this point list_of_work_queue_fanout_values may or may not be empty. We wll
                    # piggyback this list on the call to process_faninNBs_batch if there are faninnbs.
                    # if not, we will call work_queueu.put_all() directly.

                    if using_workers and not using_threads_not_processes:
                        # Config: A5, A6
                        # We piggyback fanouts if we are using worker processes. In that case, if we have
                        # more than one fanout, the first wil be a become task, which will be removed from
                        # state_info.fanouts, decrementing the length of state_info.fanouts.
                        # That will make the new length greater than or equal to 0. If the length is greater
                        # than 0, that means we started with more than one fanout, which means all the fanouts
                        # except the become should be in the list_of_work_queue_fanout_values.
                        # Note: We are not currently piggybacking this list whn we use lambdas; instead,
                        # process_fanouts start the lambdas. We may use the parallel invoker on tcp_server 
                        # and piggyback the list of fanouts, or use the parallel invoker in process_fanouts.
                        if len(state_info.fanouts) > 0: # Note: this is the length after removing the become fanout 
                            # We became one fanout task and removed it from fanouts, but there maybe were more fanouts
                            # and we should have added the fanouts to list_of_work_queue_fanout_values.
                            if len(list_of_work_queue_fanout_values) == 0:
                                logger.error("[Error]: work loop: after process_fanouts: Internal Error: fanouts > 1 but no work in list_of_work_queue_fanout_values.")
                    # else: # Config: A1, A2, A3, A4_local, A4_Remote
    
                    ##state = process_fanouts(state_info.fanouts, DAG_info.get_DAG_states(), DAG_executor_state, output, server)
                    ##logger.debug("become state:" + str(state))
                    #input = output
                    #task_inputs = (state_info.task_name,)
                    # We get new state_info and then state_info.task_inputs when we iterate
                    if using_workers:   # we are become task so we have more work
                        # Config: A4_local, A4_Remote, A5, A6
                        worker_needs_input = False
                        logger.debug(thread_name + " work_loop: fanouts: set worker_needs_input to False")
                    #Don't add to thread_work_queue just do it = False
                    #thread_work_queue.put(DAG_executor_state.state)
                    # else: Config: A1, A2, A3
                else:
                    # No fanouts so no become task and fqninBs do not generate
                    # work for us so we will need input.
                    #Note: setting worker_needs_input = True must be guarded by using_workers
                    if using_workers: 
                        # Config: A4_local, A4_Remote, A5, A6
                        worker_needs_input = True
                    # else: Config: A1, A2, A3

                if len(state_info.faninNBs) > 0:
                    # batching work when we are using workers and storing the FanInMBs remotey and 
                    # we are using processes, or we are using real lambdas. We can also use workers 
                    # with threads instead of processes but multithreading with remote FanInNBs is 
                    # not as useful as using processes. Multithreadng in general is not as helpful 
                    # as multiprocessing in Python.

                    if (run_all_tasks_locally and using_workers and not using_threads_not_processes) or (not run_all_tasks_locally) or (run_all_tasks_locally and not using_workers and not store_fanins_faninNBs_locally and using_Lambda_Function_Simulators_to_Store_Objects):
                        # Config: A1, A3, A5, A6
                        # Note: calling process_faninNBs_batch when using threads to simulate lambdas and storing objects remotely.
                        # Since the faninNBs cannot start new simulated threads to execute the fanin tasks,
                        # the process_faninNB_batch passes all the work back and the calling simuated thread starts one thread
                        # for each work tuple returned.
                        #or (run_all_tasks_locally and not using_workers and using_Lambda_Function_Simulator):
 
                        # assert
                        if store_fanins_faninNBs_locally:
                            # Config: A2, A4_local
                            logger.error("[Error]: DAG_executor_work_loop: using processes or lambdas but storing FanINNBs locally.")
                        if not run_all_tasks_locally:
                            # Config: A1
                            if worker_needs_input:
                                # Note: perhaps we csn use Lmbdas where Lambdas have two thread workers - faster?
                                logger.error("[Error]: DAG_executor_work_loop: using lambdas, so no workers, but worker_needs_input.")
                        #Note: using worker processes - batch calls to fan_in for FaninNBs
                        worker_needs_input = process_faninNBs_batch(websocket,state_info.faninNBs, state_info.faninNB_sizes, 
                        state_info.task_name, DAG_info.get_DAG_states(), DAG_executor_state, 
                            output, DAG_info,work_queue,worker_needs_input, list_of_work_queue_fanout_values)
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
                        worker_needs_input = process_faninNBs(websocket,state_info.faninNBs, state_info.faninNB_sizes, 
                            state_info.task_name, DAG_info.get_DAG_states(), DAG_executor_state, 
                            output, DAG_info, server,work_queue,worker_needs_input)
                    # there can be faninNBs and fanouts.

                else:
                    # Currently, we are not pigybacking fanouts if we are using lambdas. when using lambdas, we 
                    # start a lambda in process_fanouts for each fanout task. This code is for the case
                    # that we are piggybacking fanouts on the process faninNB call but we did not 
                    # have any faninNBs so we did not get a chance to piggyback the fanouts and thus we
                    # need to process the fanouts here. For non-lambda, that means put the fanout tasks
                    # in the work queue. For Lambdas, we will want to send the fanouts to the tcp_server
                    # for parallel invocation.
                    if run_all_tasks_locally and using_workers and not using_threads_not_processes:
                        # Config: A5, A6
                        # we are batching faninNBs and piggybacking fanouts on process_faninNB_batch
                        if len(state_info.fanouts) > 0:
                            # No faninNBs (len(state_info.faninNBs) == 0) so we did not get a chance to 
                            # piggyback list_of_work_queue_fanout_values on the call to process_faninNBs_batch.
 
                            # assert 
                            if worker_needs_input:
                                # when there is at least one fanin we will become one of the fanout tasks
                                # so we should not need work.
                                logger.error("[Error]: work loop: Internal Error: fanouts but worker needs work.")
 
                            if len(state_info.fanouts) > 1:
                                # We became one fanout task but there were more and we should have added the 
                                # fanouts to list_of_work_queue_fanout_values.
                                if len(list_of_work_queue_fanout_values) == 0:
                                    logger.error("[Error]: work loop: Internal Error: fanouts > 1 but no work in list_of_work_queue_fanout_values.")
                                # since we could not piggyback on process_faninNB_batch, enqueue the fanouts
                                # directly into the work_queue
                                logger.debug(thread_name + " work loop: no faninNBs so enqueue fanouts directly.")
                                # Note: if we use lambdas with a batch nvoker, here we will call the tcp_server
                                # method that dos the batch invokes. This method is probably used by 
                                # process_faninNBs_batch to invoke the fanouts that are piggybacked.
                                work_queue.put_all(list_of_work_queue_fanout_values)
                                # list_of_work_queue_fanout_values is redefined on next iteration
                            else:
                                # assert
                                # there was one fanout so we became that one fanout and should have enqueued no fanouts
                                if not len(list_of_work_queue_fanout_values) == 0:
                                    logger.error("[Error]: work loop: Internal Error: len(state_info.fanouts) is 1 but list_of_work_queue_fanout_values is not empty.")
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

                if (not using_workers) and len(state_info.fanouts) == 0:
                    # Config: A1, A2, A3
                    return
                #else: # Config: A4_local, A4_Remote, A5, A6

            elif len(state_info.fanins) > 0:
                # assert len(state_info.faninNBs)  + len(state_info.fanouts) + len(collapse) == 0
                # if faninNBs or fanouts then can be no fanins. length of faninNBs and fanouts must be 0 
                DAG_executor_state.state = DAG_info.get_DAG_states()[state_info.fanins[0]]
                #state = DAG_info.get_DAG_states()[state_info.fanins[0]]
                #if len(state_info.fanins) > 0:
                #ToDo: Set next state before process_fanins, returned state just has return_value, which has input.
                # single fanin, try-op w/ returned_state.return_value or restart with return_value or deposit/withdraw it

                returned_state = process_fanins(websocket,state_info.fanins, state_info.fanin_sizes, state_info.task_name, DAG_info.get_DAG_states(),  DAG_executor_state, output, server)
                logger.debug(thread_name + ": " + state_info.task_name + ": after call to process_fanin: " + str(state_info.fanins[0]) + " returned_state.blocking: " + str(returned_state.blocking) + ", returned_state.return_value: "
                    + str(returned_state.return_value) + ", DAG_executor_state.state: " + str(DAG_executor_state.state))
                ##+ str(returned_state.return_value) + ", state: " + str(state))
                #if returned_state.blocking:
                if returned_state.return_value == 0:
                    # we are not the become task for the fanin
                    #Note: setting worker_needs_input = True must be guarded by using_workers
                    if using_workers:
                        # Config: A4_local, A4_Remote, A5, A6
                        logger.debug(thread_name + ": After call to process_fanin: return value is 0; using workers so set worker_needs_input = True")
                        worker_needs_input = True
                    else:
                        # Config: A1, A2, A3
                        # this dfs path is finished
                        return
                else:
                    if (run_all_tasks_locally and using_workers) or not run_all_tasks_locally:
                        # Config: A1, A4_local, A4_Remote, A5, A6
                        # when using workers, threads or processes, each worker has its own local
                        # data dictionay. If we are the become task for a fanin, we receive the 
                        # fanin task inputs and we put them in the data dctionary. Same for lambdas.
                        dict_of_results = returned_state.return_value
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
                        logger.debug(thread_name + ": After call to process_fanin: return value not 0, using workers so set worker_needs_input = False")
                        worker_needs_input = False
                    #else: # Config: A2, A3
                #Don't add to thread_work_queue just do it
                #thread_work_queue.put(DAG_executor_state.state)

                #else:
                #    input = returned_state.return_value
                # We get new state_info and then state_info.task_inputs when we iterate. For local exection,
                # the fanin task will get its inputs from the data dictionary -they were placed there after
                # tasks executed. For non-local, we will need to add them to the local ata dictionary.

            else:
                logger.debug(thread_name + ": state " + str(DAG_executor_state.state) + " after executing task " +  state_info.task_name + " has no fanouts, fanins, or faninNBs.")
                ##logger.debug("1state " + str(state) + " after executing task " +  state_info.task_name + " has no fanouts, fanins, or faninNBs; return")
                #Note: setting worker_needs_input = True must be guarded by using_workers
                if using_workers:
                    # Config: A4_local, A4_Remote, A5, A6
                    logger.debug(thread_name + " set worker_needs_input to true")
                    worker_needs_input = True
                else:
                    # Config: A1, A2, A3
                    logger.debug(thread_name + " return")
                    return


# Config: A2, A3
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
  
    DAG_info = DAG_Info()

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

    #DAG_info = payload['DAG_info']
    #DAG_executor_work_loop(logger, server, counter, thread_work_queue, DAG_executor_state, DAG_info, data_dict)
    DAG_executor_work_loop(logger, server, counter, DAG_exec_state, DAG_info, work_queue)

# Config: A5, A6
# def DAG_executor_processes(payload,counter,process_work_queue,data_dict,log_queue, configurer):
def DAG_executor_processes(payload,counter,log_queue, worker_configurer):
    # Use for multiprocessing workers

    #- read DAG_info, create DAG_exec_state, thread_work_queue is parm
    if not use_multithreaded_multiprocessing:
        # Config: A5
        global logger
        worker_configurer(log_queue)
        logger = logging.getLogger("multiP")
        logger.setLevel(logging.DEBUG)
    else:
        # Config: A6
        logger = log_queue

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
  
    DAG_info = DAG_Info()

    # The work loop will create a BoundedBuffer_Work_Queue. Each process excuting the work loop
    # will create a BoundedBuffer_Work_Queue object, which wraps the websocket creatd in the 
    # work loop and the code to send work to the work queue on the tcp_server.
    work_queue = None
    #DAG_info = payload['DAG_info']
    #DAG_executor_work_loop(logger, server, counter, process_work_queue, DAG_exec_state, DAG_info, data_dict)
    DAG_executor_work_loop(logger, server, counter, DAG_exec_state, DAG_info, work_queue)
    logger.debug("DAG_executor_processes: returning after work_loop.")
    return

# Config: A1
def DAG_executor_lambda(payload):
    logger.debug("Lambda: started.")
    DAG_exec_state = cloudpickle.loads(base64.b64decode(payload['DAG_executor_state']))

    logger.debug("payload DAG_exec_state.state:" + str(DAG_exec_state.state))
    DAG_info = cloudpickle.loads(base64.b64decode(payload['DAG_info']))
    DAG_map = DAG_info.get_DAG_map()
    state_info = DAG_map[DAG_exec_state.state]
    is_leaf_task = state_info.task_name in DAG_info.get_DAG_leaf_tasks()
    if not is_leaf_task:
        # lambdas invoked with inputs. We do not add leaf task inputs to the data
        # dictionary, we use them directly when we execute the leaf task.
        # Also, leaf task inputs are not in a dictionary.
        dict_of_results = cloudpickle.loads(base64.b64decode(payload['input']))
        for key, value in dict_of_results.items():
            data_dict[key] = value
    else:
        # Passing leaf task input as state_info.task_inputs in DAG_info; we don't
        # want to add a leaf task input parameter to DAG_executor_work_loop(); this 
        # parameter would only be used by the Lambdas and we ha ve a place already
        # in state_info.task_inputs. 
        # Note: We null out state_info.task_inputs for leaf tasks after we use the input.
        inp = cloudpickle.loads(base64.b64decode(payload['input']))
        state_info.task_inputs = inp

    # lambdas do not use work_queues, for now.
    work_queue = None

    # server and counter are None
    # logger is local lambda logger
    DAG_executor_work_loop(logger, server, counter, DAG_exec_state, DAG_info, work_queue )
    logger.debug("DAG_executor_processes: returning after work_loop.")
    return
                        
# Config: A4_local, A4_Remote
def DAG_executor_task(payload):
    DAG_executor_state = payload['DAG_executor_state']
    if DAG_executor_state != None:
        # DAG_executor_state is None when using workers
        logger.debug("DAG_executor_task: call DAG_excutor, state is " + str(DAG_executor_state.state))
    DAG_executor(payload)
    
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