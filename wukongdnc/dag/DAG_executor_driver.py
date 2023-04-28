#ToDo: close_all at the end
# break stuck Python is FN + R
# try matrixMult

# Where are we: 
# check code
# Note in ADG orchestrator, line 392 we tabbed this stuff
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
        if not create_all_fanins_faninNBs_on_start:
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
                        "type": process_work_queue_Type,
                        "name": "process_work_queue",
                        "state": make_json_serializable(dummy_state),	
                        "id": msg_id
                    } 
                    # not created yet so create object
                    logger.debug("tcp_server: synchronize_process_faninNBs_batch: "
                        + "create sync object process_work_queue on the fly.")
                    self.create_obj_but_no_ack_to_client(creation_message)
                else:
                    logger.debug("tcp_server: synchronize_process_faninNBs_batch: object " + 
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
"""
    def synchronize_sync(self, message = None):

        if not create_all_fanins_faninNBs_on_start:
            ...
            creation_message = {
                "op": "create",
                "type": fanin_type,
                "name": task_name,
                "state": make_json_serializable(dummy_state_for_create_message),	
                "id": msg_id
            }
            ...
            # message is the original synchronize_sync message
            messages = (creation_message, message)
            ...
            control_message = {
                "op": "createif_and_synchronize_sync",
                "type": "DAG_executor_fanin_or_fanout",
                "name": messages,   
                "state": make_json_serializable(dummy_state_for_control_message),	
                "id": msg_id
            }

        if using_Lambda_Function_Simulators_to_Store_Objects and using_DAG_orchestrator:
            # The enqueue_and_invoke_lambda_synchronously will generte the creae message
            logger.info("*********************tcp_server_lambda: synchronize_sync: " + calling_task_name + ": calling infiniD.enqueue(message).")
            returned_state = self.enqueue_and_invoke_lambda_synchronously(message)
            logger.info("*********************tcp_server_lambda: synchronize_sync: " + calling_task_name + ": called infiniD.enqueue(message) "
                + "returned_state: " + str(returned_state))
        else:
            logger.info("*********************tcp_server_lambda: synchronize_sync: " + calling_task_name + ": calling invoke_lambda_synchronously.")
            if create_all_fanins_faninNBs_on_start:
                # call synchronize_sync on the already created object
                returned_state = self.invoke_lambda_synchronously(message)
            else:
                # call createif_and_synchronize_sync to create object
                # and call synchronize_sync on it. the control_message
                # has the creation_message and the message for snchronize_sync
                # in a messages tuple value under its 'name' key.
                returned_state = self.invoke_lambda_synchronously(control_message)

            logger.info("*********************tcp_server_lambda: synchronize_sync: " + calling_task_name + ": called invoke_lambda_synchronously "
                + "returned_state: " + str(returned_state))

"""
#
# ToDo: parallel invoke of leaf tasks?
#
# ToDo: Does tcpServer fannNBs_batch really need to call execute()
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
#   but when not "run locally" may or may not be sync_objects_in_lambdas_trigger_their_tasks
#   but async_call is true in either case? i.e., nothing comes back to lamba. 
# - Check other conditions involving sync_objects_in_lambdas_trigger_their_tasks. This includes the
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
# ToDO:Yes, messages created by driver have DAG_info for all faninNBs and fanouts but
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
#     if (not run_all_tasks_locally) and store_sync_objects_in_lambdas and sync_objects_in_lambdas_trigger_their_tasks:
# so not using workers and not run_all_tasks_locally, which is like Wukong but to flip off
# of Wukong we need sync_objects_in_lambdas_trigger_their_tasks, which may be too strong.
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
# when objects are stored in lambdas. do asserts on asynch_call if true then rl lambdas. 
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

import logging 

logger = logging.getLogger(__name__)
logger.setLevel(logging.ERROR)
formatter = logging.Formatter('[%(asctime)s] [%(threadName)s] %(levelname)s: %(message)s')
ch = logging.StreamHandler()
ch.setLevel(logging.ERROR)
ch.setFormatter(formatter)
logger.addHandler(ch)

import threading
import multiprocessing
from multiprocessing import Process #, Manager
import time
import cloudpickle
import socket
import os

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
from .DAG_executor_constants import num_workers,using_threads_not_processes
from .DAG_executor_constants import FanIn_Type, FanInNB_Type, process_work_queue_Type
from .DAG_executor_constants import store_sync_objects_in_lambdas, sync_objects_in_lambdas_trigger_their_tasks
#from .DAG_work_queue_for_threads import thread_work_queue
from .DAG_executor_work_queue_for_threads import work_queue
from .DAG_executor_synchronizer import server
from wukongdnc.wukong.invoker import invoke_lambda_DAG_executor
import uuid
from wukongdnc.server.api import create_all_sync_objects, synchronize_trigger_leaf_tasks
from .multiprocessing_logging import listener_configurer, listener_process, worker_configurer
from .DAG_executor_countermp import CounterMP
from .DAG_boundedbuffer_work_queue import BoundedBuffer_Work_Queue
from .DAG_executor_create_multithreaded_multiprocessing_processes import create_multithreaded_multiprocessing_processes #, create_and_run_threads_for_multiT_multiP
import copy


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
- A4_L: the workers are threads and the work queue is a shared local work queue. 
process_faninNBs_batch is not called (since process_faninNB_batch for tcp_server also
enques work in the work queue that is on the server). async_call is not relevant.
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
using threads to simulate lambdas ==> run_all_tasks_locally and not using_workers: 
        objects stored locally (so no triggering fanout/fanin tasks)
        objects not stored locally (run_all_tasks_locally and not using_workers and not store_fanins_faninNBs_locally)
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
==> not run_all_tasks_locally and store_sync_objects_in_lambdas
    and using_Lambda_Function_Simulators_to_Store_Objects = True or False
    and sync_objects_in_lambdas_trigger_their_tasks = True or False
So when we are using the DAG orchestrator it is another way to manage and 
access the lambdas (simulated or not) that store objects. And so can be used
with all the various ways of running tasks (threads to simulate lambdas, 
workers, and lambdas executing tasks). Optionally, the stored objects can 
trigger the fanout tasks, which will run in the same lambda as the object.
In this case the lambas may be real or simulated and only lambdas can be
used to execute tasks, i.e., no threads that simulate lambdas.

Wwe call process_faninNB_batch when 
    running tcp_server - not using lambdas 
    running tcp_server_lambda - storing synch objects in lambdas
        sync object may or may not trigger their tasks. 
            Triggered tasks are running in simulated or real lambdas that will call p_f_b()
- using worker processes
- !run_all_tasks_locally so not using workers; instead using lambdas; note: always call p_f_b
- using threads to simulate lambdas (and since lambdas call p_f_b threads here should too)

So use async_call = False for p_f_b() when work can be returned
    using worker processes and worker_needs_input
    using threads to simulate lambdas - no lambas started by faninNBs or triggered since using threads to sim lambdas
So use async_call = True for p_f_b when no work can be returned
    using worker processes but worker_needs_input == False
    !run_all_tasks_locally so using real lambdas so no threads for simulation  

Q: In code  dag-98: if (run_all_tasks_locally and using_workers and not using_threads_not_processes) or (not run_all_tasks_locally) or (run_all_tasks_locally and not using_workers and not store_fanins_faninNBs_locally and using_Lambda_Function_Simulators_to_Store_Objects):
        # Config: A1, A3, A5, A6
        # Note: calling process_faninNBs_batch when using threads to simulate lambdas and storing objects remotely
        # and using_Lambda_Function_Simulators_to_Store_Objects. 
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


Note:
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
    all_fanout_task_names = DAG_info.get_all_fanout_task_names()
    # Note: all fanout_sizes is not needed since fanouts are fanins that have size 1
    DAG_states = DAG_info.get_DAG_states()
    DAG_leaf_tasks = DAG_info.get_DAG_leaf_tasks()
    DAG_leaf_task_start_states = DAG_info.get_DAG_leaf_task_start_states()
    DAG_tasks = DAG_info.get_DAG_tasks()


    # Note: if we are using_lambdas, we null out DAG_leaf_task_inputs after we get it here
    # (by calling DAG_info.set_DAG_leaf_task_inputs_to_None() below). So make a copy.
    if run_all_tasks_locally:
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
        for inp in DAG_leaf_task_inputs:
            print(inp)
        #print() 
        print()


#rhc cleanup
        from . import BFS_Shared
        logger.error("shared_groups_mapDDDD:")
        for (k,v) in BFS_Shared.shared_groups_map.items():
            logger.error(str(k) + ", (" + str(v[0]) + "," + str(v[1]) + ")")

    #ResetRedis()
    
    #start_time = time.time()
	
#############################
#Note: if using Lambdas to store synch objects: SERVERLESS_SYNC = False in constants.py; set to True
#      when storing synch objects in Lambdas.
#############################
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as websocket:

        # synch_objects are stored in local memory or on the tcp_Server or in InfinX Executors
        if store_fanins_faninNBs_locally:
            # store fanin and faninNBs locally so not using websocket to tcp_server
            if not using_threads_not_processes: # using processes
                logger.error("[Error]: Configuration Error: DAG_executor_driver: store objects locally but using worker processes,"
                    + " which must use remote objects (on server).")
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

                #else: Nothing to do; we do not use a work_queue if we are not using workers
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
                        list_of_work_queue_values = []
                        for state in DAG_leaf_task_start_states:
                            #logger.debug("dummy_state: " + str(dummy_state))
                            state_info = DAG_map[state]
                            task_inputs = state_info.task_inputs 
                            task_name = state_info.task_name
                            dict_of_results =  {}
                            dict_of_results[task_name] = task_inputs
                            work_tuple = (state,dict_of_results)
                            list_of_work_queue_values.append(work_tuple)
                            #process_work_queue.put(work_tuple)
                            #process_work_queue.put(state)
                        # batch put work in remote work_queue
                        process_work_queue.put_all(list_of_work_queue_values)
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
                    # not run_all_tasks_locally and not using workers (must be true (since 
                    # (not run_all_tasks_locally) and using_workers is never used in that case.)
                    if using_workers:
                        logger.error("[Error]: DAG_executor_driver: using_workers but using lambdas.")
                    if run_all_tasks_locally:
                        logger.error("[Error]: DAG_executor_driver: interal error: DAG_executor_driver: run_all_tasks_locally shoudl be false.")
                    # not run_all_tasks_locally so using lambdas (real or simulatd)
                    # So do not put leaf tasks in work queue

                    if not run_all_tasks_locally and store_sync_objects_in_lambdas and sync_objects_in_lambdas_trigger_their_tasks:
                        # storing sync objects in lambdas and snc objects trigger their tasks
                        create_fanins_and_faninNBs_and_fanouts(websocket,DAG_map,DAG_states,DAG_info,
                            all_fanin_task_names,all_fanin_sizes,all_faninNB_task_names,all_faninNB_sizes,
                            all_fanout_task_names,DAG_leaf_tasks,DAG_leaf_task_start_states)
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
                    if not run_all_tasks_locally and store_sync_objects_in_lambdas and sync_objects_in_lambdas_trigger_their_tasks:
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
        # Config: A6
        # keep list of threads/processes in pool so we can join() them
        multithreaded_multiprocessing_process_list = []
        num_processes_created_for_multithreaded_multiprocessing = 0
        #num_processes_created_for_multithreaded_multiprocessing = create_multithreaded_multiprocessing_processes(num_processes_created_for_multithreaded_multiprocessing,multithreaded_multiprocessing_process_list,counter,process_work_queue,data_dict,log_queue,worker_configurer)
        num_processes_created_for_multithreaded_multiprocessing = create_multithreaded_multiprocessing_processes(num_processes_created_for_multithreaded_multiprocessing,multithreaded_multiprocessing_process_list,counter,log_queue,worker_configurer)
        start_time = time.time()
        for thread_proc in multithreaded_multiprocessing_process_list:
            thread_proc.start()
    else: # multi threads or multi-processes, thread and processes may be workers using work_queue
        # if we are not using lambdas, and we are not using a worker pool, create a thread for each
        # leaf task. If we are not using lambdas but we are using a worker pool, create at least 
        # one worker and at most num_worker workers. If we are using workers, there may be more
        # leaf tasks than workers, but that is okay since we put all the leaf task states in the 
        # work queue and the created workers will withdraw them.

        if not (not run_all_tasks_locally and store_sync_objects_in_lambdas and sync_objects_in_lambdas_trigger_their_tasks):
            # we are not having sync objects trigger their tasks in lambdas
            for start_state, task_name, inp in zip(DAG_leaf_task_start_states, DAG_leaf_tasks, DAG_leaf_task_inputs):
                # The state of a DAG executor contains only one application specific member, which is the
                # state number of the task to execute. Leaf task information is in DAG_leaf_task_start_states
                # and DAG_leaf_tasks (which are the task names).
                DAG_exec_state = DAG_executor_State(function_name = "DAG_executor", function_instance_ID = str(uuid.uuid4()), state = start_state)
                logger.debug("DAG_executor_driver: Starting DAG_executor for task " + task_name)

                if run_all_tasks_locally:
                    # not using Lambdas
                    if using_threads_not_processes: # create threads
                        # Config: A4_local, A4_Remote
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
                            else: 
                                thread.start()
                            num_threads_created += 1
                        except Exception as ex:
                            logger.debug("[ERROR] DAG_executor_driver: Failed to start DAG_executor thread for state " + str(start_state))
                            logger.debug(ex)
                    else:   # multiprocessing - must be using a process pool
                        # Config: A5
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
                            #proc.start()
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
                    if not sync_objects_in_lambdas_trigger_their_tasks:
                        # Config: A1
                        try:
                            logger.debug("DAG_executor_driver: Starting DAG_Executor_Lambda for leaf task " + task_name)
                            lambda_DAG_exec_state = DAG_executor_State(function_name = "DAG_executor.DAG_executor_lambda", function_instance_ID = str(uuid.uuid4()), state = start_state)
                            logger.debug ("DAG_executor_driver: lambda payload is DAG_info + " + str(start_state) + "," + str(inp))
                            lambda_DAG_exec_state.restart = False      # starting new DAG_executor in state start_state_fanin_task
                            lambda_DAG_exec_state.return_value = None
                            lambda_DAG_exec_state.blocking = False            
                            logger.info("DAG_executor_driver: Starting Lambda function %s." % lambda_DAG_exec_state.function_name)

                            # We use "inp" for leaf task input otherwise all leaf task lambda Executors will 
                            # receive all leaf task inputs in the DAG_info.leaf_task_inputs and in the state_info.task_inputs
                            # - both of which are nulled out at beginning of driver when we are using lambdas.
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
                    else:
                        # sync_objects_in_lambdas_trigger_their_tasks == True so
                        # above we called tcp_server_lambda.process_leaf_tasks_batch
                        # to trigger the leaf tasks.
                        # assert: this should be unreachble - if trigger tassk in their 
                        # lambdas then we should not have entered this loop for 
                        # starting tasks.
                        logger.error("[ERROR] DAG_executor_driver: reached unreachable code for starting triggered tasks")
            
        #else we started the leaf tasks above with process_leaf_tasks_batch

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
                            payload = {
                                "DAG_executor_state": DAG_exec_state
                            }
                            thread_name_prefix = "Worker_thread_non-leaf_"
                            non_leaf_task_name = thread_name_prefix+str(start_state)
                            logger.debug("DAG_executor_driver: Starting DAG_executor worker for non-leaf task " + non_leaf_task_name)
                            thread = threading.Thread(target=DAG_executor.DAG_executor_task, name=(non_leaf_task_name), args=(payload,))
                            thread_proc_list.append(thread)
                            #thread.start()
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
                            #proc.start()
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
        logger.debug("DAG_executor_driver: num_threads/processes_created: " + str(num_threads_created))

    if not use_multithreaded_multiprocessing:
        start_time = time.time()
        if run_all_tasks_locally:
            if using_workers:
                for thread_proc in thread_proc_list:
                    thread_proc.start()

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
    else:
        # We can have the result deposited in a bonded buffer sync object and 
        # withdraw it, in order to wait until all lambda DAG executors are done.
        logger.debug("DAG_executor_driver: running (simulated) Lambdas - no joins, sleep instead.")

    #Note: To verify Results, see the code below.

    stop_time = time.time()
    duration = stop_time - start_time

    logger.debug("DAG_executor_driver: Sleeping 3.0 seconds...")
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
        # Only need DAG_info if not run_all_tasks_locally, as we pass DAG_info to Lambas
        if not run_all_tasks_locally:
            dummy_state.keyword_arguments['DAG_info'] = DAG_info
        else:
            dummy_state.keyword_arguments['DAG_info'] = None
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

# create fanin and faninNB messages to be passed to the tcp_server for creating
# all fanin and faninNB synch objects
def create_fanin_and_faninNB_and_fanout_messages(DAG_map,DAG_states,DAG_info,all_fanin_task_names,
    all_fanin_sizes,all_faninNB_task_names, all_faninNB_sizes,
    all_fanout_task_names,DAG_leaf_tasks,DAG_leaf_task_start_states):
 
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
        # Only need DAG_info if not run_all_tasks_locally, as we pass DAG_info to Lambas
        if not run_all_tasks_locally:
            dummy_state.keyword_arguments['DAG_info'] = DAG_info
        else:
            dummy_state.keyword_arguments['DAG_info'] = None
        msg_id = str(uuid.uuid4())

        message = {
            "op": "create",
            "type": FanInNB_Type,
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
        #logger.debug("iterate fanin: fanin_name: " + fanin_name + " size: " + str(size))
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
        dummy_state.keyword_arguments['store_fanins_faninNBs_locally'] = store_fanins_faninNBs_locally
        # Only need DAG_info if not run_all_tasks_locally, as we pass DAG_info to Lambas
        if not run_all_tasks_locally:
            dummy_state.keyword_arguments['DAG_info'] = DAG_info
        else:
            dummy_state.keyword_arguments['DAG_info'] = None
        msg_id = str(uuid.uuid4())	# for debugging

        message = {
            "op": "create",
            # fanouts are just FanIns of size 1
            "type": FanInNB_Type,
            "name": leaf_task_name,
            "state": make_json_serializable(dummy_state),	
            "id": msg_id
        }
        fanout_messages.append(message)

    for fanout_name in all_fanout_task_names:
        #logger.debug("iterate fanin: fanin_name: " + fanin_name + " size: " + str(size))
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
        dummy_state.keyword_arguments['store_fanins_faninNBs_locally'] = store_fanins_faninNBs_locally
        # Only need DAG_info if not run_all_tasks_locally, as we pass DAG_info to Lambas
        if not run_all_tasks_locally:
            dummy_state.keyword_arguments['DAG_info'] = DAG_info
        else:
            dummy_state.keyword_arguments['DAG_info'] = None
        msg_id = str(uuid.uuid4())	# for debugging

        message = {
            "op": "create",
            # fanouts are just FanIns of size 1
            "type": FanInNB_Type,
            "name": fanout_name,
            "state": make_json_serializable(dummy_state),	
            "id": msg_id
        }
        fanout_messages.append(message)

    logger.debug("DAG_executor_driver: create_fanin_and_faninNB_and_fanout_messages: number of fanin messages: " 
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
    create_all_sync_objects(websocket, "create_all_sync_objects", "DAG_executor_fanin_or_faninNB_or_work_queue", 
        messages, dummy_state2)

# creates all fanins and faninNBs and fanouts at the start of driver execution. We use this
# when we are using lambdas to execte tasks, not workers. Note that leaf tasks
# will be included in the fanouts.
def create_fanins_and_faninNBs_and_fanouts(websocket,DAG_map,DAG_states,DAG_info,all_fanin_task_names,all_fanin_sizes,
    all_faninNB_task_names,all_faninNB_sizes,all_fanout_task_names, 
    DAG_leaf_tasks, DAG_leaf_task_start_states):

    fanin_messages, faninNB_messages, fanout_messages = create_fanin_and_faninNB_and_fanout_messages(DAG_map,DAG_states,DAG_info,
        all_fanin_task_names,all_fanin_sizes,all_faninNB_task_names,all_faninNB_sizes,
        all_fanout_task_names, DAG_leaf_tasks,DAG_leaf_task_start_states)

    logger.debug("DAG_executor_driver: create_fanins_and_faninNBs_and_fanouts: Sending a 'create_fanins_and_faninNBs_and_fanouts' message to server.")
    #logger.debug("create_fanins_and_faninNBs_and_work_queue: num fanin created: "  + str(len(fanin_messages))
    #    +  " num faninNB creates; " + str(len(faninNB_messages)))

    # even if there are not fanins or faninNBs in Dag, need to create the work queue so send the message
    messages = (fanin_messages,faninNB_messages,fanout_messages)
    dummy_state = DAG_executor_State(function_name = "DAG_executor", function_instance_ID = str(uuid.uuid4()))
    #Note: Passing tuple messages as name
    create_all_sync_objects(websocket, "create_all_sync_objects", "DAG_executor_fanin_or_faninNB_or_fanout", 
        messages, dummy_state)

# creates all fanins and faninNBs at the start of driver execution. 
def create_fanins_and_faninNBs(websocket,DAG_map,DAG_states,DAG_info,all_fanin_task_names,all_fanin_sizes,all_faninNB_task_names,all_faninNB_sizes):										
    fanin_messages, faninNB_messages = create_fanin_and_faninNB_messages(DAG_map,DAG_states,DAG_info,all_fanin_task_names,all_fanin_sizes,all_faninNB_task_names,all_faninNB_sizes)

    
    #logger.error("create_fanins_and_faninNBs: Sending a 'create_all_fanins_and_faninNBs_and_possibly_work_queue' message to server.")
    #logger.error("create_fanins_and_faninNBs: number of fanin messages: " + str(len(fanin_messages))
    #    + " number of faninNB messages: " + str(len(faninNB_messages)))
    #logger.error("create_fanins_and_faninNBs: size of all_fanin_task_names: " + str(len(all_fanin_task_names))
    #    + " size of all_faninNB_task_names: " + str(len(all_faninNB_task_names)))
    #logger.error("create_fanins_and_faninNBs: size of all_fanin_sizes: " + str(len(all_fanin_sizes))
    #    + " size of all_faninNB_sizes: " + str(len(all_faninNB_sizes)))
    #logger.error("create_fanins_and_faninNBs: all_faninNB_task_names: " + str(all_faninNB_task_names))
    

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
    logger.debug("Sent 'create_all_fanins_and_faninNBs' message to server")

    # Receive data. This should just be an ACK, as the TCP server will 'ACK' our create_all_fanins_and_faninNBs() call.
    ack = recv_object(websocket)
    """

def process_leaf_tasks_batch(websocket):
    dummy_state = DAG_executor_State(function_name = "DAG_executor", function_instance_ID = str(uuid.uuid4())) 
    synchronize_trigger_leaf_tasks(websocket, "process_leaf_tasks_batch", "process_leaf_tasks_batch_Type", 
        "leaf_tasks", dummy_state)

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