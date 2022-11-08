#ToDo: All the timing stuff + close_all at the end
# break stuck Python is FN + R
# try matrixMult

# Where are we: 
#
# Fix sockets for multitreaded processes
#
# Don't want Lambda to wait for work or anything else? Call to process_faninNBs_batch() can 
# be asynch since faninNB starts lambdas (all the faninNB work) so no need to wait on 
# process_faninNBs_batch? Also, no work returned to calling lambda.
#
# matrix mult deposits results to "collector" object at end.
#
# move on the optimizations:
#
# Consider: multiple tcp_servers/work_queues and work stealing? workers generate
# their own work, till they hit a fanin/faninNB and lose. then need need to start 
# new dfs paths(s). Steal it? 
# Consider: compute max workers you can keep busy so don;t overprovison and shut down 
# machines as the workers are not needed or the slowdown is worth the cost.
# Consider creating an unbounded buufer in which deposit should never block.
# if it blocks then raise "full" exception. Used for DAG execution where the 
# size of the work_queue is blunded by number of tasks so we can create an
# unbounded buffer of size number_of_tasks + delta that should never block.
# Consider timestamping so can place fanin/faninNb/fanout objects in lambdas,
#  where a happened before b means a and b can be in same lambda unless a || b.
# Note: with lamba triggers, careful when a and b is same lambda so a can do 
# local fanin to b, but then a has to also enqueue something to sqs so b is
# triggered with other fanin results, but a is in same lamba so make sure lambda
# is terminated before it is triggered. Not a problem with SQS in tcp_server_lambda
# since it can trigger after a's synch call returns?

import threading
import multiprocessing
from multiprocessing import Process #, Manager
import time
import cloudpickle
import socket

#from .DFS_visit import Node
#from .DFS_visit import state_info
#from DAG_executor_FanInNB import DAG_executor_FanInNB
from . import DAG_executor
#from wukongdnc.server.DAG_executor_FanInNB import DAG_executor_FanInNB
#from wukongdnc.server.DAG_executor_FanIn import DAG_executor_FanIn
from .DAG_executor_State import DAG_executor_State
from .DAG_info import DAG_Info
from wukongdnc.server.util import make_json_serializable
from wukongdnc.constants import TCP_SERVER_IP
from .DAG_executor_constants import run_all_tasks_locally, store_fanins_faninNBs_locally, use_multithreaded_multiprocessing #, num_threads_for_multithreaded_multiprocessing
from .DAG_executor_constants import create_all_fanins_faninNBs_on_start, using_workers
from .DAG_executor_constants import num_workers,using_threads_not_processes, using_lambdas
from .DAG_executor_constants import FanIn_Type, FanInNB_Type, process_work_queue_Type
#from .DAG_work_queue_for_threads import thread_work_queue
from .DAG_work_queue_for_threads import work_queue
from .DAG_executor_synchronizer import server
from wukongdnc.wukong.invoker import invoke_lambda_DAG_executor
import uuid
from wukongdnc.server.api import create_all_fanins_and_faninNBs_and_possibly_work_queue
from .multiprocessing_logging import listener_configurer, listener_process, worker_configurer
from .DAG_executor_countermp import CounterMP
from .DAG_boundedbuffer_work_queue import BoundedBuffer_Work_Queue
from .DAG_executor_create_multithreaded_multiprocessing_processes import create_multithreaded_multiprocessing_processes #, create_and_run_threads_for_multiT_multiP
import copy

import logging 

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(asctime)s] [%(threadName)s] %(levelname)s: %(message)s')
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)
logger.addHandler(ch)

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

A1. We assign each leaf node state to Lambda Executor. At fanouts, a Lambda excutor starts another
Executor that begins execution at the fanout's associated state. When all Lamdas have performed
a faninNB operation for a given faninNB F, F starts a new Lambda executor that begins its
execution by excuting the fanin task in the task's associated state. Fanins are processed as usual
using "becomes". This is essentially Wukong with DAGs representes as state machines. Note that
fanin/faninNBs are stored on the tcp_server.

A2. This is the same as scheme (1) using threads instead of Lambdas. This is simply a way to
test the logic of (1) by running threads locally instead of using Lambdas. In this scheme, the 
fanin/faninNbs are stored locally. The faninNBs will
create threads to run the fanin tasks (to simulate creating Lambdas to run the fanin tasks) and the
threads and these threads run locally. The thread that is processing the faninNB
creates a new thread that actually makes the call to fan_in, to simulate an
"async" call to the fan_in of a faninNB. (There is no reason to wait for
the fan_in to return since there are no fanin results - the fanin starts
a new thread to execute the fanin task.)

A3. This is the same as scheme (1) using threads instead of Lambdas except that fanins/faninNbs
are stored remotely.  Now the faninNBs cannot create threads since such threads would run on the 
tcp_server; instead, a fanin's dictionary of results is returned to the client caller thread and 
this thread will create a new thread that runs locally (on the client machine). So this is the 
same as (A2) except that the thread created to execute a fanin task is created by the thread 
that calls fanin (and is the last to call fanin) instead of the faninNB (after the last call to fanin.)

This scheme is also used with real python functions that simulate lambdas
(so we can test things without running real lambdas). If
  using_Lambda_Function_Simulator = True
then we create a list of functions and map the fanins/faninNBs/fanouts names 
to these functions, e.g., one name per function, or two names mapped to
the same function if ops on these named objects cannot be executed
concurrently, i.e., fanin1.fanin and fanin2.fanin cannot be executed 
concurrently since fanin1 and fanin2 are on the same DFS path.
Then we invoke a function list(i), instead of a real
lamba function, when we perform a sync op, e.g., fan_in, on the synch object 
that was mapped to function i. If
   use_single_lambda_function = True
Then we use a single function to store all the fanins/faninNBs/fanouts as 
a simple test case.

A4. We use a fixed-size pool of threads with a work_queue that holds the states that have been enabled
so far. The driver deposits the leaf task states into the work_queue. Pool threads get the leaf 
states and execute the collapse/fanoutNB/fanout/fanin operations for these states. Any states that 
are enabled by fanout and faninNB operation are put into the work_queue, to be eventully withdrawn 
and executed by the pool threads, until all states/tasks have been executed. In this scheme,
the fanins/faninNbs can be stored locally or remotely.

#ToDo: Describe faninNB batch processing.

Note: When processing a group of fanouts that can be executed in a state, a thread will "become"
the thread that executes one of the fanouts instead of putting the fanout state in the work_queue.
The same thing happens when a thread becomes the thread that executes a fanin task. So becomes are 
handled as usual.

A5. This is the same as (3) only we use (multi) processes as worers instead of threads. This scheme is 
expected to be faster than (3) for large enough DAGS since there is real parallelism.

A6. This scheme is like (A5) since the workers are processes, but each worker process can have multiple
threads. The threads are essentilly a pool of threads (like (A4)), each of which is executing in a 
process that is part of a pool of processes. This may make a Worer process perform better since while 
one thread is blocked, say on a socket call to the tcp_server, the other threas(s) can run.

We expect (1) to be faster than (4) to be faster than (3) to be faster than (2). Executing (3)
with one thread in the pool gives us a baseline for comparing the speedup from (4) and (1).
We can also compute the COST metric - how many cores must we use in order for a multicore execution
of (4) or (1) to be faster than the excution of (3) with one thread (but possibly many cores).

Thee are three schemes for using the fanin and faninNB synchronization objects:

(S1) The fanin and faninNB objects are stored locally in RAM. This scheme can be used with schemes (A2) 
and (A4) above. In both cases, we are using threads to execute tasks, not processes or Lambdas.
In (A1) FaninNBs create new (local) tasks to execute the fanin task. This is okay since the faninNBs
are stored locally and fanin runs locally. This simulates the use of Lambdas (A2) - start a Lambda/thread
for fanouts and for faninNB fanin tasks. This results in the creation of many threads so it 
is not practical, but it tests the Lambda creation logic. In (A2) FaninNBs enqueue the work in a local
work_queue (sharedby the local threads).

(S2) The fanin and faninNB objects are stored (remotely) on the TCP_server. This is used 
when using schemes (A1), (A3), and (A4) above. Note that  using (multi) processes or Lambas requries the 
fanin and faninNB objects to be stored remotely.

(S3) This is the same as (S2) with fanins and faninNBs stored in InfiniX lambdas instead of on the 
tcp_server.

(S4) TBD: Store the DAG tasks, i.e., the Python functions that implement a task, and the fanins
and faninNBs in InfiniX lambdas. This requires an assignment of tasks and fanin/faninNBs to 
InfiniX lambdas, and potentially moving tasks/fanin/faninNBs around, say, to increase locality, etc.

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
    #asserts on configuration:
    if using_workers:
        if not run_all_tasks_locally:
            # running in Lambdas so no schedule of tasks on a pool of executors
            # i.e., schedule tasks using DFS_paths
            logger.error("Error: DAG_executor_driver: if using_workers then run_fanout_tasks_locally must also be true.")

    DAG_info = DAG_Info()
    
    DAG_map = DAG_info.get_DAG_map()
    all_fanin_task_names = DAG_info.get_all_fanin_task_names()
    all_fanin_sizes = DAG_info.get_all_fanin_sizes()
    all_faninNB_task_names = DAG_info.get_all_faninNB_task_names()
    all_faninNB_sizes = DAG_info.get_all_faninNB_sizes()
    DAG_states = DAG_info.get_DAG_states()
    DAG_leaf_tasks = DAG_info.get_DAG_leaf_tasks()
    DAG_leaf_task_start_states = DAG_info.get_DAG_leaf_task_start_states()
    DAG_tasks = DAG_info.get_DAG_tasks()

#ToDo: lambdas:
    # Note: if we are using_lambdas, we null out DAG_leaf_task_inputs after we get it here
    # (by calling DAG_info.set_DAG_leaf_task_inputs_to_None() below). So make a copy.
    if not using_lambdas:
        DAG_leaf_task_inputs = DAG_info.get_DAG_leaf_task_inputs()
    else:
        DAG_leaf_task_inputs = copy.copy(DAG_info.get_DAG_leaf_task_inputs())
#ToDo: lambdas:
        # For lambdas, null out the task inputs in DAG_info since we pass DAG_info in the
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

    # FYI:
    print("DAG_executor_driver: DAG_map:")
    for key, value in DAG_map.items():
        print(key)
        print(value)
    print("  ")
    print("DAG_executor_driver: DAG states:")         
    for key, value in DAG_states.items():
        print(key)
        print(value)
    print("   ")
    print("DAG_executor_driver: DAG leaf task start states")
    for start_state in DAG_leaf_task_start_states:
        print(start_state)
    print()
    print("DAG_executor_driver: DAG_tasks:")
    for key, value in DAG_tasks.items():
        print(key, ' : ', value)
    print()
    print("DAG_executor_driver: DAG_leaf_tasks:")
    for task_name in DAG_leaf_tasks:
        print(task_name)
    print() 
    print("DAG_executor_driver: DAG_leaf_task_inputs:")
    #for inp in DAG_leaf_task_inputs:
    #    print(inp)
    #print() 

    #ResetRedis()
    
    start_time = time.time()
	
#############################
#Note: if using Lambdas to store synch objects: SERVERLESS_SYNC = False in constants.py; set to True
#      when storing synch objects in Lambdas.
#############################
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as websocket:

        # synch_objects are stored in local memory or on the tcp_Server or in InfinX Executors
        if store_fanins_faninNBs_locally:
            # store fanin and faninNBs locally so not using websocket to tcp_server
            if not using_threads_not_processes: # using processes
                logger.error("[Error]: DAG_executor_driver: store local but using processes.")
            # cannot be multiprocessing, may or may not be pooling, running all tasks locally (no Lambdas)
            # server is global variable obtained: from .DAG_executor_synchronizer import server
            if create_all_fanins_faninNBs_on_start:
                # create fanins and faninNBs using all_fanin_task_names, all_fanin_sizes, all_faninNB_task_names, all_faninNB_sizes
                # server is a global variable in DAG_executor_synchronizer.py - it is used to simulate the
                # tcp_server when running locally.
                server.create_all_fanins_and_faninNBs_locally(DAG_map,DAG_states, DAG_info, all_fanin_task_names, all_fanin_sizes, all_faninNB_task_names, all_faninNB_sizes)

                if using_workers:
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
                #else: Nohing to do; we do not use a work_queue if we are not using workers
            else:
                if using_workers:
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
            # server will be None
            logger.debug("DAG_executor_driver: Connecting to TCP Server at %s." % str(TCP_SERVER_IP))
            websocket.connect(TCP_SERVER_IP)
            logger.debug("DAG_executor_driver: Successfully connected to TCP Server.")
            if create_all_fanins_faninNBs_on_start:
                # create fanins and faninNbs on tcp_server or in InfiniX lambdas 
                # all at the start of driver execution
                if run_all_tasks_locally and using_workers:
                    # if not stored locally, i.e., and either threads or process workers, then create a remote 
                    # process queue if using processs and use a local work queue for the threads.
                    if not using_threads_not_processes:
                        #Note: using workers and processes means not store_fanins_faninNBs_locally
                        #Need to create the process_work_queue; do it in the same batch
                        # of fanin and faninNB creates
                        #manager = Manager()
                        #data_dict = manager.dict()
                        #num_DAG_tasks = len(DAG_tasks)
                        #process_work_queue = manager.Queue(maxsize = num_DAG_tasks)
                        num_tasks_to_execute = len(DAG_tasks)
                        process_work_queue = BoundedBuffer_Work_Queue(websocket,2*num_tasks_to_execute)
                        #process_work_queue.create()
                        create_fanins_and_faninNBs_and_work_queue(websocket,num_tasks_to_execute,DAG_map,DAG_states, DAG_info, all_fanin_task_names, all_fanin_sizes, all_faninNB_task_names, all_faninNB_sizes)
                        #Note: you can reversed() this list of leaf node start states to reverse the order of 
                        # appending leaf nodes during testing
                        for state in DAG_leaf_task_start_states:
                            #logger.debug("dummy_state: " + str(dummy_state))
                            state_info = DAG_map[state]
                            task_inputs = state_info.task_inputs 
                            task_name = state_info.task_name
                            dict_of_results =  {}
                            dict_of_results[task_name] = task_inputs
                            work_tuple = (state,dict_of_results)
                            process_work_queue.put(work_tuple)
                            #process_work_queue.put(state)
                    else:
                        create_fanins_and_faninNBs(websocket,DAG_map,DAG_states, DAG_info, all_fanin_task_names, all_fanin_sizes, all_faninNB_task_names, all_faninNB_sizes)
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
                # This is true: not (run_all_tasks_locally and using_workers), i.e.,
                # one of the conditions is false.
                # Note: This configuration is never used: (not run_all_tasks_locally) and using_workers
                # as not run_all_tasks_locally means we are using lambdas and we do not use workers
                # when we are using lambdas.
                elif run_all_tasks_locally and not using_workers:
                    # not using workers, use threads to simulate lambdas. no work queue so do not
                    # put leaf node start states in work queue. threads are created to execute
                    # fanout tasks and fanin tasks (like lambdas)
                    if not using_threads_not_processes:
                        logger.error("[Error]: DAG_executor_driver: not using_workers but using processes.")
                    # just create a batch of fanins and faninNBs on server - no remote work queue wen using
                    # thread workers or using lambdas.         
                    create_fanins_and_faninNBs(websocket,DAG_map,DAG_states, DAG_info, all_fanin_task_names, all_fanin_sizes, all_faninNB_task_names, all_faninNB_sizes)
                else:
                    # not run_all_tasks_locally and not using workers must be true (since 
                    # (not run_all_tasks_locally) and using_workers is never used.
                    if using_workers:
                        logger.error("[Error]: DAG_executor_driver: using_workers but using lambdas.")
                    if run_all_tasks_locally:
                        logger.error("[Error]: DAG_executor_driver: interal error: DAG_executor_driver: run_all_tasks_locally shoudl be false.")
                    # not run_all_tasks_locally so using lambdas, which use a work queue but no workers. 
                    # So do not put leaf tasks in work queue
                    create_fanins_and_faninNBs(websocket,DAG_map,DAG_states, DAG_info, all_fanin_task_names, all_fanin_sizes, all_faninNB_task_names, all_faninNB_sizes)
            else:
                # going to create fanin and faninNBs on demand, i.e., as we execute
                # operations on them. But still want to create process_work_queue
                # by itself at the beginning of drivr execuion.
                if run_all_tasks_locally and using_workers:
                    if not using_threads_not_processes:
                        #Note: using workers means not store_fanins_faninNBs_locally
                        #Need to create the process_work_queue
                        #manager = Manager()
                        #data_dict = manager.dict()
                        #num_DAG_tasks = len(DAG_tasks)
                        #process_work_queue = manager.Queue(maxsize = num_DAG_tasks)
                        num_tasks_to_execute = len(DAG_tasks)
                        process_work_queue = BoundedBuffer_Work_Queue(websocket,2*num_tasks_to_execute)
                        process_work_queue.create()

                        for state in DAG_leaf_task_start_states:
                            state_info = DAG_map[state]
                            task_inputs = state_info.task_inputs 
                            task_name = state_info.task_name
                            dict_of_results =  {}
                            dict_of_results[task_name] = task_inputs
                            work_tuple = (state,dict_of_results)
                            process_work_queue.put(work_tuple)
                            #process_work_queue.put(state)
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
#ToDo: Lambdas
                # This is true: not (run_all_tasks_locally and using_workers), i.e.,
                # one of the two conditions is false.
                # Note: This configuration is never used: (not run_all_tasks_locally) and using_workers
                # as not run_all_tasks_locally means we are using lambdas and we do not use workers
                # when we are using lambdas.
                elif run_all_tasks_locally and not using_workers:
                    # not using workers, use threads to simulate lambdas. no work queue so do not
                    # put leaf node start states in work queue. threads are created to execute
                    # fanout tasks and fanin tasks (like lambdas)
                    if not using_threads_not_processes:
                        logger.error("[Error]: DAG_executor_driver: not using_workers but using processes.")
                else:
                    # not run_all_tasks_locally and not using workers must be true (since 
                    # (not run_all_tasks_locally) and using_workers is never used.
                    if using_workers:
                        logger.error("[Error]: DAG_executor_driver: using_workers but using lambdas.")
                    if run_all_tasks_locally:
                        logger.error("[Error]: DAG_executor_driver: interal error: DAG_executor_driver: run_all_tasks_locally should be false.")
                    # not run_all_tasks_locally so using lambdas, which do not use a work queue 
                    # So do not put leaf tasks in work queue and do not create a work queue

    # FYI
    logger.debug("DAG_executor_driver: DAG_leaf_tasks: " + str(DAG_leaf_tasks))
    logger.debug("DAG_executor_driver: DAG_leaf_task_start_states: " + str(DAG_leaf_task_start_states))
    #commented out for MM
    #logger.debug("DAG_executor_driver: DAG_leaf_task_inputs: " + str(DAG_leaf_task_inputs))

    # Done with process_work_queue 
    process_work_queue = None

    #print("work_queue:")
    #for start_state in X_work_queue.queue:
    #   print(start_state)

    if run_all_tasks_locally and using_workers and not use_multithreaded_multiprocessing:
        # keep list of threads/processes in pool so we can join() them
        thread_proc_list = []

    # count of threads/processes created. We will create DAG_executor_constants.py num_workers
    # if we are using_workers. We will create some number of threads if we are simulating the 
    # use of creating Lambdas, e.g., at fan-out points.
    # We use a different counter if use_multithreaded_multiprocessing
    if run_all_tasks_locally and not use_multithreaded_multiprocessing:
        num_threads_created = 0

    if run_all_tasks_locally and not using_threads_not_processes:
        if not using_workers:
            logger.error("[Error]: DAG_executor_driver: not using_workers but using processes.")
        # multiprocessing. processes share a counter that counts the number of tasks that have been executed
        # and uses this counter to determine when all tasks have been excuted so workers can stop (by 
        # putting -1 in the work_queue - when worker gets -1 it puts -1 for the next worker. So execution
        # ends with -1 in the work queue, which is put there by the last worker to stop.)
        counter = CounterMP()
        # used by a logger for multiprocessing
        log_queue = multiprocessing.Queue(-1)
        # used for multiprocessor logging - receives log messages from processes
        listener = multiprocessing.Process(target=listener_process, args=(log_queue, listener_configurer))
        listener.start()    # joined at the end

    if use_multithreaded_multiprocessing:
        # keep list of threads/processes in pool so we can join() them
        multithreaded_multiprocessing_process_list = []
        num_processes_created_for_multithreaded_multiprocessing = 0
        #num_processes_created_for_multithreaded_multiprocessing = create_multithreaded_multiprocessing_processes(num_processes_created_for_multithreaded_multiprocessing,multithreaded_multiprocessing_process_list,counter,process_work_queue,data_dict,log_queue,worker_configurer)
        num_processes_created_for_multithreaded_multiprocessing = create_multithreaded_multiprocessing_processes(num_processes_created_for_multithreaded_multiprocessing,multithreaded_multiprocessing_process_list,counter,log_queue,worker_configurer)
    else: # multi threads or multi-processes, thread and processes may be workers using work_queue
        # if we are not using lambdas, and we are not using a worker pool, create a thread for each
        # leaf task. If we are not using lambdas but we are using a worker pool, create at least 
        # one worker and at most num_worker workers. If we are using workers, there may be more
        # leaf tasks than workers, but that is okay since we put all the leaf task states in the 
        # work queue and the created workers will withdraw them.
        for start_state, task_name, inp in zip(DAG_leaf_task_start_states, DAG_leaf_tasks, DAG_leaf_task_inputs):

            # The state of a DAG executor contains only one application specific member, which is the
            # state number of the task to execute. Leaf task information is in DAG_leaf_task_start_states
            # and DAG_leaf_tasks (which are the task names).
            DAG_exec_state = DAG_executor_State(function_name = "DAG_executor", function_instance_ID = str(uuid.uuid4()), state = start_state)
            logger.debug("DAG_executor_driver: Starting DAG_executor for task " + task_name)

            if run_all_tasks_locally:
                # not using Lambdas
                if using_threads_not_processes: # create threads
                    try:
                        if not using_workers:
                            # pass the state/task the thread is to execute at the start of its DFS path
                            DAG_exec_state = DAG_executor_State(function_name = "DAG_executor", function_instance_ID = str(uuid.uuid4()), state = start_state)
                        else:
                            # workers withdraw their work, i.e., starting state, from the work_queue
                            DAG_exec_state = None
                        logger.debug("DAG_executor_driver: Starting DAG_executor thread for leaf task " + task_name)
                        payload = {
                            # What's not in the payload: DAG_info: since threads/processes read this pickled 
                            # file at the start of their execution. server: since this is a global variable
                            # for the threads and processes. for processes it is Non since processes send
                            # messages to the tcp_server, and tgus do not use the server object, which is 
                            # used to simulate the tcp_server when running locally. Input: threads and processes
                            # get their input from the data_dict. Note the lambdas will be invoked with their 
                            # input in the payload and will put this input in their local data_dict.
                            "DAG_executor_state": DAG_exec_state
                        }
                        # Note:
                        # get the current thread instance
                        # thread = current_thread()
                        # report the name of the thread
                        # print(thread.name)
                        if using_workers:
                            thread_name_prefix = "Worker_Thread_leaf_"
                        else:
                            thread_name_prefix = "Thread_leaf_"
                        thread = threading.Thread(target=DAG_executor.DAG_executor_task, name=(thread_name_prefix+"ss"+str(start_state)), args=(payload,))
                        if using_workers:
                            thread_proc_list.append(thread)
                        thread.start()
                        num_threads_created += 1
                    except Exception as ex:
                        logger.debug("[ERROR] DAG_executor_driver: Failed to start DAG_executor thread for state " + start_state)
                        logger.debug(ex)
                else:   # multiprocessing - must be using a process pool
                    try:
                        if not using_workers:
                            logger.debug("[ERROR] DAG_executor_driver: Starting multi process leaf tasks but using_workers is false.")

                        logger.debug("DAG_executor_driver: Starting DAG_executor process for leaf task " + task_name)
    
                        payload = {
                            # no payload. We do not need DAG_executor_state since worker processes withdraw
                            # states from the work_queue
                        }
                        proc_name_prefix = "Worker_leaf_"
                        # processes share these objects: counter,process_work_queue,data_dict,log_queue,worker_configurer.
                        # The worker_configurer() funcion is used for multiprocess logging
                        #proc = Process(target=DAG_executor.DAG_executor_processes, name=(proc_name_prefix+"ss"+str(start_state)), args=(payload,counter,process_work_queue,data_dict,log_queue,worker_configurer,))
                        proc = Process(target=DAG_executor.DAG_executor_processes, name=(proc_name_prefix+"ss"+str(start_state)), args=(payload,counter,log_queue,worker_configurer,))
                        proc.start()
                        thread_proc_list.append(proc)
                        #thread.start()
                        num_threads_created += 1
                        #_thread.start_new_thread(DAG_executor.DAG_executor_task, (payload,))
                    except Exception as ex:
                        logger.debug("[ERROR] DAG_executor_driver: Failed to start DAG_executor process for state " + start_state)
                        logger.debug(ex)     

                if using_workers and num_threads_created == num_workers:
                    break
            else:
                try:
                    logger.debug("DAG_executor_driver: Starting DAG_Executor_Lambda for leaf task " + task_name)
                    lambda_DAG_exec_state = DAG_executor_State(function_name = "DAG_executor.DAG_executor_lambda", function_instance_ID = str(uuid.uuid4()), state = start_state)
                    logger.debug ("DAG_executor_driver: lambda payload is DAG_info + " + str(start_state) + "," + str(inp))
                    lambda_DAG_exec_state.restart = False      # starting new DAG_executor in state start_state_fanin_task
                    lambda_DAG_exec_state.return_value = None
                    lambda_DAG_exec_state.blocking = False            
                    logger.info("DAG_executor_driver: Starting Lambda function %s." % lambda_DAG_exec_state.function_name)
#ToDo: lambdas: 
                    # We use "inp" for leaf task input otherwise all leaf task lambda Executors will 
                    # receive all leaf task inputs in the leaf_task_inputs and in the state_info.task_inputs
                    # - both are nulled out at beginning of driver. when we are using lambdas.
                    # If we use "inp" then we will pass only a given leaf task's input to that leaf task. 
                    # For non-lambda, each thread/process reads the DAG_info from a file. This DAG-info has
                    # all the leaf task inputs in it so every thread/process reads all these inputs. This 
                    # can be optimized if necessary, e.g., separate files for leaf tasks and non-leaf tasks.

                    payload = {
                        "input": inp,
                        "DAG_executor_state": lambda_DAG_exec_state,
                        "DAG_info": DAG_info
                    }

                    invoke_lambda_DAG_executor(payload = payload, function_name = "DAG_Executor_Lambda")
                except Exception as ex:
                    logger.error("[ERROR] DAG_executor_driver: Failed to start DAG_executor Lambda.")
                    logger.error(ex)

        # if the number of leaf tasks is less than number_workers, we need to create more workers
        if run_all_tasks_locally and using_workers and num_threads_created < num_workers:
            # starting leaf tasks did not start num_workers workers so start num_workers-num_threads_created
            # more threads/processes.
            while True:
                logger.debug("DAG_executor_driver: Starting DAG_executor for non-leaf task.")
                if run_all_tasks_locally:
                    if using_threads_not_processes:
                        try:
                            # Using workers so do not pass to them a start_state (use state = 0); 
                            # they get their start state from the work_queue
                            DAG_exec_state = DAG_executor_State(function_name = "DAG_executor", function_instance_ID = str(uuid.uuid4()), state = 0)
                            logger.debug("DAG_executor_driver: Starting DAG_executor worker for non-leaf task " + task_name)
                            payload = {
                                "DAG_executor_state": DAG_exec_state
                            }
                            thread_name_prefix = "Worker_thread_non-leaf_"
                            thread = threading.Thread(target=DAG_executor.DAG_executor_task, name=(thread_name_prefix+str(start_state)), args=(payload,))
                            thread_proc_list.append(thread)
                            thread.start()
                            num_threads_created += 1
                        except Exception as ex:
                            logger.debug("[ERROR] DAG_executor_driver: Failed to start DAG_executor worker thread for non-leaf task " + task_name)
                            logger.debug(ex)
                    else:
                        try:
                            if not using_workers:
                                logger.debug("[ERROR] DAG_executor_driver: Starting multi process non-leaf tasks but using_workers is false.")
                            
                            logger.debug("DAG_executor_driver: Starting DAG_executor process for non-leaf task " + task_name)

                            payload = {
                            }
                            proc_name_prefix = "Worker_process_non-leaf_"
                            #proc = Process(target=DAG_executor.DAG_executor_processes, name=(proc_name_prefix+"p"+str(num_threads_created + 1)), args=(payload,counter,process_work_queue,data_dict,log_queue,worker_configurer,))
                            proc = Process(target=DAG_executor.DAG_executor_processes, name=(proc_name_prefix+"p"+str(num_threads_created + 1)), args=(payload,counter,log_queue,worker_configurer,))
                            proc.start()
                            thread_proc_list.append(proc)
                            num_threads_created += 1                      
                        except Exception as ex:
                            logger.debug("[ERROR] DAG_executor_driver: Failed to start DAG_executor worker process for non-leaf task " + task_name)
                            logger.debug(ex)

                    if using_workers and num_threads_created == num_workers:
                        break 
                else:
                    logger.error("DAG_executor_driver: worker (pool) threads/processes must run locally (no Lambdas)")

    if use_multithreaded_multiprocessing:
        logger.debug("DAG_executor_driver: num_processes_created_for_multithreaded_multiprocessing: " + str(num_processes_created_for_multithreaded_multiprocessing))
    elif run_all_tasks_locally:
            logger.debug("DAG_executor_driver: num_threads_created: " + str(num_threads_created))

    if run_all_tasks_locally:
        # Do joins if not using lambdas
        if not use_multithreaded_multiprocessing:
            if using_workers:
                logger.debug("DAG_executor_driver: joining workers.")
                for thread in thread_proc_list:
                    thread.join()	

            if not using_threads_not_processes:
                # using processes and special process logger
                logger.debug("DAG_executor_driver: joining log_queue listener process.")
                log_queue.put_nowait(None)
                listener.join()
        else:   
            # using multithreaded with procs as workers; we have already joined the threads in each worker process
            logger.debug("DAG_executor_driver: joining multithreaded_multiprocessing processes.")
            for proc in multithreaded_multiprocessing_process_list:
                proc.join()
            # using processes and special process logger
            logger.debug("DAG_executor_driver: joining log_queue listener process.")
            log_queue.put_nowait(None)
            listener.join()

    #Note: To verify Results, see the code below.

    stop_time = time.time()
    duration = stop_time - start_time

    logger.debug("DAG_executor_driver: Sleeping 1.0")
    time.sleep(3.0)
    print("DAG_executor_driver: DAG_Execution finished in %f seconds." % duration)
		
    #ToDo:  close_all(websocket)

# create fanin and faninNB messages to be passed to the tcp_server for creating
# all fanin and faninNB synch objects
def create_fanin_and_faninNB_messages(DAG_map,DAG_states,DAG_info,all_fanin_task_names,all_fanin_sizes,all_faninNB_task_names,all_faninNB_sizes):
 
    """
    logger.debug("create_fanin_and_faninNB_messages: size of all_fanin_task_names: " + str(len(all_fanin_task_names))
        + " size of all_faninNB_task_names: " + str(len(all_faninNB_task_names)))
    logger.debug("create_fanin_and_faninNB_messages: size of all_fanin_sizes: " + str(len(all_fanin_sizes))
        + " size of all_faninNB_sizes: " + str(len(all_faninNB_sizes)))
    logger.debug("create_fanin_and_faninNB_messages: all_faninNB_task_names: " + str(all_faninNB_task_names))
    """

    fanin_messages = []

    # create a list of "create" messages, one for each fanin
    for fanin_name, size in zip(all_fanin_task_names,all_fanin_sizes):
        #logger.debug("iterate fanin: fanin_name: " + fanin_name + " size: " + str(size))
        # rhc: DES
        dummy_state = DAG_executor_State(function_name = "DAG_executor", function_instance_ID = str(uuid.uuid4()))
        # we will create the fanin object and call fanin.init(**keyword_arguments)
        dummy_state.keyword_arguments['n'] = size
        msg_id = str(uuid.uuid4())	# for debugging

        message = {
            "op": "create",
            "type": FanIn_Type,
            "name": fanin_name,
            "state": make_json_serializable(dummy_state),	
            "id": msg_id
        }
        fanin_messages.append(message)

    faninNB_messages = []

     # create a list of "create" messages, one for each faninNB
    for fanin_nameNB, size in zip(all_faninNB_task_names,all_faninNB_sizes):
        #logger.debug("iterate faninNB: fanin_nameNB: " + fanin_nameNB + " size: " + str(size))
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
        dummy_state.keyword_arguments['store_fanins_faninNBs_locally'] = store_fanins_faninNBs_locally
        dummy_state.keyword_arguments['DAG_info'] = DAG_info
        msg_id = str(uuid.uuid4())

        message = {
            "op": "create",
            "type": FanInNB_Type,
            "name": fanin_nameNB,
            "state": make_json_serializable(dummy_state),	
            "id": msg_id
        }
        faninNB_messages.append(message)

    logger.debug("DAG_executor_driver: create_fanin_and_faninNB_messages: number of fanin messages: " + str(len(fanin_messages))
        + " number of faninNB messages: " + str(len(faninNB_messages)))

    return fanin_messages, faninNB_messages

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
        "type": process_work_queue_Type,
        "name": "process_work_queue",
        "state": make_json_serializable(dummy_state),	
        "id": msg_id
    } 

    logger.debug("create_work_queue: Sending a 'create_work_queue' message to server.")
    create_work_queue_on_server(websocket, work_queue_message)
"""

# creates all fanins and faninNBs at the start of driver executin. If we are using 
# workers and processes (not threads) then we also crate the work_queue here
def create_fanins_and_faninNBs_and_work_queue(websocket,number_of_tasks,DAG_map,DAG_states,DAG_info,all_fanin_task_names,all_fanin_sizes,all_faninNB_task_names,all_faninNB_sizes):
    dummy_state = DAG_executor_State(function_name = "DAG_executor", function_instance_ID = str(uuid.uuid4()))
    # we will create the fanin object and call fanin.init(**keyword_arguments)
    dummy_state.keyword_arguments['n'] = 2*number_of_tasks
    msg_id = str(uuid.uuid4())	# for debugging

    work_queue_message = {
        "op": "create",
        "type": process_work_queue_Type,
        "name": "process_work_queue",
        "state": make_json_serializable(dummy_state),	
        "id": msg_id
    } 

    fanin_messages, faninNB_messages = create_fanin_and_faninNB_messages(DAG_map,DAG_states,DAG_info,all_fanin_task_names,all_fanin_sizes,all_faninNB_task_names,all_faninNB_sizes)

    logger.debug("DAG_executor_driver: create_fanins_and_faninNBs_and_work_queue: Sending a 'create_fanins_and_faninNBs_and_work_queue' message to server.")
    #logger.debug("create_fanins_and_faninNBs_and_work_queue: num fanin created: "  + str(len(fanin_messages))
    #    +  " num faninNB creates; " + str(len(faninNB_messages)))

    # even if there are not fanins or faninNBs in Dag, need to create the work queue so send the message
    messages = (fanin_messages,faninNB_messages,work_queue_message)
    dummy_state2 = DAG_executor_State(function_name = "DAG_executor", function_instance_ID = str(uuid.uuid4()))
    #Note: Passing tuple messages as name
    create_all_fanins_and_faninNBs_and_possibly_work_queue(websocket, "create_all_fanins_and_faninNBs_and_possibly_work_queue", "DAG_executor_fanin_or_faninNB", 
        messages, dummy_state2)

# creates all fanins and faninNBs at the start of driver execution. 
def create_fanins_and_faninNBs(websocket,DAG_map,DAG_states,DAG_info,all_fanin_task_names,all_fanin_sizes,all_faninNB_task_names,all_faninNB_sizes):										
    fanin_messages, faninNB_messages = create_fanin_and_faninNB_messages(DAG_map,DAG_states,DAG_info,all_fanin_task_names,all_fanin_sizes,all_faninNB_task_names,all_faninNB_sizes)

    """
    logger.debug("create_fanins_and_faninNBs: Sending a 'create_all_fanins_and_faninNBs_and_possibly_work_queue' message to server.")
    logger.debug("create_fanins_and_faninNBs: number of fanin messages: " + str(len(fanin_messages))
        + " number of faninNB messages: " + str(len(faninNB_messages)))
    logger.debug("create_fanins_and_faninNBs: size of all_fanin_task_names: " + str(len(all_fanin_task_names))
        + " size of all_faninNB_task_names: " + str(len(all_faninNB_task_names)))
    logger.debug("create_fanins_and_faninNBs: size of all_fanin_sizes: " + str(len(all_fanin_sizes))
        + " size of all_faninNB_sizes: " + str(len(all_faninNB_sizes)))
    logger.debug("create_fanins_and_faninNBs: all_faninNB_task_names: " + str(all_faninNB_task_names))
    """

    # Don't send a message to th server if there are no fanin or fanonNBs to create
    # Not tested DAG with no fanins or faninNBs yet.
    if len(fanin_messages) > 0 or len(faninNB_messages) > 0:
        messages = (fanin_messages,faninNB_messages)
        dummy_state = DAG_executor_State(function_name = "DAG_executor", function_instance_ID = str(uuid.uuid4()))
        create_all_fanins_and_faninNBs_and_possibly_work_queue(websocket, "create_all_fanins_and_faninNBs_and_possibly_work_queue", "DAG_executor_fanin_or_faninNB", 
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
    logger.debug("Sent 'create_all_fanins_and_faninNBs' message to server")

    # Receive data. This should just be an ACK, as the TCP server will 'ACK' our create_all_fanins_and_faninNBs() call.
    ack = recv_object(websocket)
    """

if __name__ == "__main__":
    run()


# xtra:
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
    #value = inp['input']
    input_tuple = inp['input']
    value = input_tuple[0]
    value += 1
    output = {'inc0': value}
    logger.debug("inc0 output: " + str(output))
    return output
def inc1(inp):
    logger.debug("inc1")
    #value = inp['input']
    input_tuple = inp['input']
    value = input_tuple[0]
    value += 1
    output = {'inc1': value}
    logger.debug("inc1 output: " + str(output))
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
	
    # logger.debug("DAG_map:")
    # for key, value in Node.DAG_map.items():
    #     logger.debug(key)
    #     logger.debug(value)
    # logger.debug("  ")
	
    # logger.debug("states:")         
    # for key, value in Node.DAG_states.items():
    #     logger.debug(key)
    #     logger.debug(value)
    # logger.debug("   ")
	
    # logger.debug("num_fanins:" + str(Node.num_fanins) + " num_fanouts:" + str(Node.num_fanouts) + " num_faninNBs:" 
    #     + " num_collapse:" + str(Node.num_collapse))
    # logger.debug("   ")
	
    # logger.debug("all_fanout_task_names")
    # for name in Node.all_fanout_task_names:
    #     logger.debug(name)
    #     logger.debug("   ")
    # logger.debug("   ")
	
    # logger.debug("all_fanin_task_names")
    # for name in Node.all_fanin_task_names :
    #     logger.debug(name)
    #     logger.debug("   ")
    # logger.debug("   ")
		  
    # logger.debug("all_faninNB_task_names")
    # for name in Node.all_faninNB_task_names:
    #     logger.debug(name)
    #     logger.debug("   ")
    # logger.debug("   ")
		  
    # logger.debug("all_collapse_task_names")
    # for name in Node.all_collapse_task_names:
    #     logger.debug(name)
    #     logger.debug("   ")
    # logger.debug("   ")
	
    # DAG_map = Node.DAG_map
    
    # logger.debug("DAG_map after assignment:")
    # for key, value in Node.DAG_map.items():
    #     logger.debug(key)
    #     logger.debug(value)   
    # logger.debug("   ")
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
    logger.debug("Sleeping for " + str(sleep_length_seconds) + " seconds before calling synchronize_sync()")
    time.sleep(sleep_length_seconds)
    logger.debug("Finished sleeping. Calling synchronize_sync() now...")

    # Note: no-try op
    state = synchronize_sync(websocket, "synchronize_sync", "final_result", "withdraw", default_state)
    answer = state.return_value 

    end_time = time.time()
    
    error_occurred = False
    if type(answer) is str:
        logger.error("Unexpected solution recovered from Redis: %s\n\n" % answer)
        error_occurred = True
    else:
        logger.debug("Solution: " + str(answer) + "\n\n")
        expected_answer = int(72)
        if expected_answer != answer:
            logger.error("Error in answer: " + str(answer) + " expected_answer: " + str(expected_answer))
            error_occurred = True 

    if not error_occurred:
        logger.debug("Verified.")
        
    # rest is performance stuff, close websocket and return
    
    ..
    
    # then main() stuff
if __name__ == "__main__":

"""	