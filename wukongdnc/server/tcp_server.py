#from re import A
import json
import traceback
import socketserver
import cloudpickle
import uuid
from threading import Lock

from .synchronizer import Synchronizer
from .util import decode_and_deserialize
#from ..dag.DAG_executor_State import DAG_executor_State
from .util import decode_and_deserialize, isTry_and_getMethodName, isSelect, make_json_serializable
import wukongdnc.dag.DAG_executor_constants

#from ..dag.DAG_executor_constants import run_all_tasks_locally, process_work_queue_Type
#from ..dag.DAG_executor_constants import create_all_fanins_faninNBs_on_start
#from ..dag.DAG_executor_constants import FanInNB_Type, FanIn_Type, store_fanins_faninNBs_locally
#from ..dag.DAG_executor_constants import same_output_for_all_fanout_fanin
#from ..dag.DAG_executor_constants import using_workers, using_threads_not_processes
#from ..dag.DAG_executor_constants import compute_pagerank, use_incremental_DAG_generation

from ..dag.DAG_info import DAG_Info
from ..dag.DAG_executor_State import DAG_executor_State

import logging 
from wukongdnc.dag.DAG_executor_constants import log_level
from ..dag.addLoggingLevel import addLoggingLevel
""" How to use: https://stackoverflow.com/questions/2183233/how-to-add-a-custom-loglevel-to-pythons-logging-facility/35804945#35804945
    >>> addLoggingLevel('TRACE', logging.DEBUG - 5)
    >>> logging.getLogger(__name__).setLevel("TRACE")
    >>> logging.getLogger(__name__).trace('that worked')
    >>> logging.trace('so did this')
    >>> logging.TRACE
"""
# Set up logging.
addLoggingLevel('TRACE', logging.DEBUG - 5)

logging.basicConfig(encoding='utf-8',level=log_level, format='[%(asctime)s][%(module)s][%(processName)s][%(threadName)s]: %(message)s')
# Added this to suppress the logging message:
#   credentials - MainProcess - MainThread: Found credentials in shared credentials file: ~/.aws/credentials
# But it appears that we could see other things liek this:
# https://stackoverflow.com/questions/1661275/disable-boto-logging-without-modifying-the-boto-files
logging.getLogger('botocore').setLevel(logging.CRITICAL)

logger = logging.getLogger(__name__)
#logger.setLevel(logging.DEBUG)
#formatter = logging.Formatter('[%(asctime)s] [%(module)s] [%(processName)s] [%(threadName)s]: %(message)s')
#ch = logging.StreamHandler()
#ch.setLevel(logging.DEBUG)
#ch.setFormatter(formatter)
#logger.addHandler(ch)

# DAG_info is read on by TCP_server from a file when using real lambdas.
# This DAG_info is passd to real lambdas that are invoked by faninNBs
DAG_info = None
# read DAG_info once - set to False when read
read_DAG_info = True
create_work_queue_lock = None
create_synchronization_object_lock = None

# created in main 
tcp_server = None

class TCPHandler(socketserver.StreamRequestHandler):
    def handle(self):

        #TCP handler for incoming requests from AWS Lambda functions.
        
        while True:
            logger.trace("[HANDLER] Recieved one request from {}".format(self.client_address[0]))
            logger.trace("[HANDLER] Recieved one request from {}".format(self.client_address[1]))

            self.action_handlers = {
                "create": self.create_obj,
                "setup": self.setup_server,
                "synchronize_async": self.synchronize_async,
                "synchronize_sync": self.synchronize_sync,
                "close_all": self.close_all,
                # These are DAG execution operations
                "create_all_sync_objects": self.create_all_sync_objects,
                "synchronize_process_faninNBs_batch": self.synchronize_process_faninNBs_batch,
                "create_work_queue": self.create_work_queue
            }
            #logger.trace("Thread Name:{}".format(threading.current_thread().name))

            try:
                logger.trace("[HANDLER] call receive_object")
                data = self.recv_object()
                logger.trace("[HANDLER] receive_object successful")

                # We only read DAG_info one time, and only if we are 
                # using real lambdas. When we invoke a real lambda
                # we need to give it the DAG_info. 
                # Note: We may only need to do this when we are
                # not using incremenal DAG generation. When we 
                # use incremental DAG generation, we may eed to 
                # get the newest version of the DAG from the 
                # DAG_infoBuffer_monitor before we invoke a lambda
                """
                global read_DAG_info
                if (not wukongdnc.dag.DAG_executor_constants.run_all_tasks_locally) and read_DAG_info:
                    read_DAG_info = False
                    global DAG_info # initialized to None
                    DAG_info = DAG_Info.DAG_info_fromfilename()
                    logger.trace("tcp_server: read DAG_info for real lambdas.")
#rhc: DAG_info
                    print("tcp_server: DAG_map:")
                    DAG_map = DAG_info.get_DAG_map()
                    for key, value in DAG_map.items():
                        print(key)
                        print(value)
                """
                #else:
                #    logger.trace("TCP_Server: Don't read DAG_info.")

                if data is None:
                    # Commented out to suppress warnings
                    #logger.warning("recv_object() returned None. Exiting handler now.")
                    logger.trace("recv_object() returned None. Exiting handler now.")
                    return 
                logger.trace("tcp_server: synchronize_sync: data is " + str(data))
                json_message = json.loads(data)
                message_id = json_message["id"]
                logger.trace("[HANDLER] Received message (size=%d bytes) from client %s with ID=%s" % (len(data), self.client_address[0], message_id))
                action = json_message.get("op", None)
                if action == "synchronize_sync":
                    synch_op = json_message['method_name']
                    obj_name = json_message['name']
                    name_op = ": " + obj_name+"."+synch_op
                elif action == "synchronize_process_faninNBs_batch":
                    name_op = ""
                else:
                    synch_op = ""
                    obj_name = ""
                    name_op = ""
                #logger.info("[HANDLER] for client with ID " + message_id + " action is: " + action
                #    + str(name_op))
                logger.info("[HANDLER] action is: " + action + str(name_op))
 
                self.action_handlers[action](message = json_message)
            except ConnectionResetError as ex:
                logger.error(ex)
                logger.error("Error in tcp_handler")
                logger.error(traceback.format_exc())
                return 
            except Exception as ex:
                logger.error(ex)
                logger.error("Error in tcp_handler")
                logger.error(traceback.format_exc())

    def _get_synchronizer_name(self, type_name = None, name = None):
        """
        Return the key of a synchronizer object. 

        The key is a string of the form <type>-<name>.
        """
        return str(name) # return str(type_name + "_" + name)

    # Called by client - sensd ack after creation back to client. 
    # Object must be created before client is permitted to perform ops on object.
    def create_obj(self,message = None):
        """
        Called by a remote Lambda to create an object here on the TCP server.

        Key-word arguments:
        -------------------
            message (dict):
                The payload from the AWS Lambda function.
        """        
        logger.trace("[HANDLER] server.create() called.")
        type_arg = message["type"]
        name = message["name"]
        state = decode_and_deserialize(message["state"])

        synchronizer = Synchronizer()

        synchronizer.create(type_arg, name, **state.keyword_arguments)
        
        synchronizer_name = self._get_synchronizer_name(type_name = type_arg, name = name)
        logger.trace("Caching new Synchronizer of type '%s' with name '%s'" % (type_arg, synchronizer_name))
        tcp_server.synchronizers[synchronizer_name] = synchronizer # Store Synchronizer object.

        resp = {
            "op": "ack",
            "op_performed": "create"
        }
        #############################
        # Write ACK back to client. #
        #############################
        logger.trace("Sending ACK to client %s for CREATE operation." % self.client_address[0])
        resp_encoded = json.dumps(resp).encode('utf-8')
        self.send_serialized_object(resp_encoded)
        logger.trace("Sent ACK of size %d bytes to client %s for CREATE operation." % (len(resp_encoded), self.client_address[0]))  

    # Not used and not tested. Currently create work queue in 
    # create_all_fanins_and_faninNBs_and_possibly_work_queue. 
    def create_work_queue(self, message = None):
        # used to create only a work queue. This is the case when we are creating the fanins and faninNBs
        # on the fly, i.e., not at the beginning of execution.
        self.create_obj_but_no_ack_to_client(message)

        resp = {
            "op": "ack",
            "op_performed": "create_work_queue"
        }
        #############################
        # Write ACK back to client. #
        #############################
        logger.trace("Sending ACK to client %s for create_work_queue operation." % self.client_address[0])
        resp_encoded = json.dumps(resp).encode('utf-8')
        self.send_serialized_object(resp_encoded)
        logger.trace("Sent ACK of size %d bytes to client %s for create_awork_queue operation." % (len(resp_encoded), self.client_address[0]))

    def create_all_sync_objects(self, message = None):
        """
        Called by remote driver to create fanins, faninNBs, and possibly work queue.
        Number of fanins/faninNBs may be 0.

        Key-word arguments:
        -------------------
            message (dict):
                The payload from the AWS Lambda function.
        
        where:
            message = {
                "op": "create_all_fanins_and_faninNBs",
                "type": "DAG_executor_fanin_or_faninNB",
                "name": messages,						# Q: Fix this? usually it's a synch object name (string)
                "state": make_json_serializable(dummy_state),
                "id": msg_id
            }
        """  
        logger.trace("[HANDLER] server.create_all_sync_objects called.")
        messages = message['name']
        fanin_messages = messages[0]
        faninNB_messages = messages[1]
        logger.trace("create_all_sync_objects: fanin messages: " + str(fanin_messages))
        logger.trace("create_all_sync_objects: faninNB messages: " + str(faninNB_messages))

        for msg in fanin_messages:
            self.create_obj_but_no_ack_to_client(msg)
        if len(fanin_messages) > 0:
            logger.trace("create_all_sync_objects: created fanins")

        for msg in faninNB_messages:
            self.create_obj_but_no_ack_to_client(msg)
        if len(faninNB_messages) > 0:
            logger.trace("create_all_sync_objects: created faninNBs")

        # we always create the fanin and faninNBs. We possibly create the work queue. If we send
        # a message for create work queue, in addition to the list of messages for create
        # fanins and create faninNBs, we create a work queue too. We may instead send the 
        # list of fanout messages. Check the type of the operation to determine whether we
        # create a work queue (as we are using worker with remote objects) or create fanout 
        # objects (when we store fanins/faninBs and fanouts) in Lambdas an the fanin operations
        # trigger their fanin tasks. (A fanout is implemented using a fanin of size 1 that 
        # triggers its fanout operation.)
        create_the_work_queue_or_fanouts = (len(messages)>2)
        if create_the_work_queue_or_fanouts:
            type = message['type']
            if type == "DAG_executor_fanin_or_faninNB_or_work_queue":
                logger.trace("create_all_sync_objects: create_the_work_queue:" + " len: " + str(len(messages)))
                msg = messages[2]
                self.create_obj_but_no_ack_to_client(msg)
            else:
                logger.trace("create_all_sync_objects: " + " create the fanouts.")
                fanout_messages = messages[2]
                logger.trace("create_all_sync_objects: faninout messages: " + str(fanout_messages))
                for msg in fanout_messages:
                    self.create_obj_but_no_ack_to_client(msg)
                if len(fanout_messages) > 0:
                    logger.trace("create_all_sync_objects: created fanouts.")

        resp = {
            "op": "ack",
            "op_performed": "create_all_fanins_and_faninNBs"
        }
        #############################
        # Write ACK back to client. #
        #############################
        logger.trace("create_all_sync_objects: Sending ACK to client %s for create_all_fanins_and_faninNBs operation." % self.client_address[0])
        resp_encoded = json.dumps(resp).encode('utf-8')
        self.send_serialized_object(resp_encoded)
        logger.trace("create_all_sync_objects: Sent ACK of size %d bytes to client %s for create_all_fanins_and_faninNBs operation." % (len(resp_encoded), self.client_address[0]))

    # Called locally - sso no ack sent to client. For example, this is 
    # just one of possibly many of the creates from create_all_fanins_and_faninNBs
    # Also, synchronize_process_faninNBs_batch and synchronize_sync and 
    # async call this ceate when creating the object on the fly.
    def create_obj_but_no_ack_to_client(self,message = None):
        """
        Called by create_all_fanins_and_faninNBs_and_possibly_work_queue and create_work_queue to 
        create an object here on the TCP server. No ack is sent to a client. 
        create_all_fanins_and_faninNBs_and_possibly_work_queue and create_work_queue will send the ack.

        Key-word arguments:
        -------------------
            message (dict):
                The payload from the AWS Lambda function.
        """        
        logger.trace("[HANDLER] server.create() called.")
        type_arg = message["type"]
        name = message["name"]
        state = decode_and_deserialize(message["state"])

        synchronizer = Synchronizer()

        synchronizer.create(type_arg, name, **state.keyword_arguments)

        synchronizer_name = self._get_synchronizer_name(type_name = type_arg, name = name)
        logger.trace("Caching new Synchronizer of type '%s' with name '%s'" % (type_arg, synchronizer_name))
        tcp_server.synchronizers[synchronizer_name] = synchronizer # Store Synchronizer object.
 
    def synchronize_process_faninNBs_batch(self, message = None):
        """
        Synchronous process all faninNBs for a given state during DAG execution.

        Key-word arguments:
        -------------------
            message (dict):
                The payload from the AWS Lambda function.
        """

        global DAG_info
        global read_DAG_info

        logger.trace("[HANDLER] server.synchronize_process_faninNBs_batch() called.")

        # name of the type is "DAG_executor_FanInNB" or "DAG_executor_FanInNB_Select"
        type_arg = message["type"]
        # Name of the method callled on "DAG_executor_FanInNB" is always "fanin"
        method_name = message["name"]

        DAG_exec_state = decode_and_deserialize(message["state"])
        faninNBs = DAG_exec_state.keyword_arguments['faninNBs']
        #faninNB_sizes = DAG_exec_state.keyword_arguments['faninNB_sizes']
        # FYI:
        #result = DAG_exec_state.keyword_arguments['result']
        # For debuggng
        calling_task_name = DAG_exec_state.keyword_arguments['calling_task_name'] 
        DAG_states_of_faninNBs_fanouts = DAG_exec_state.keyword_arguments['DAG_states_of_faninNBs_fanouts'] 
#rhc batch:
        all_faninNB_sizes_of_faninNBs = DAG_exec_state.keyword_arguments['all_faninNB_sizes_of_faninNBs'] 
        # This is included here for ompleteness but we are not batch procesing
        # fanins
        _all_fanin_sizes_of_fanins = DAG_exec_state.keyword_arguments['all_fanin_sizes_of_fanins'] 
        # Note: if using lambdas, then we are not usingn workers (for now) so worker_needs_input must be false
        worker_needs_input = DAG_exec_state.keyword_arguments['worker_needs_input']
        work_queue_name = DAG_exec_state.keyword_arguments['work_queue_name']
        work_queue_type = DAG_exec_state.keyword_arguments['work_queue_type']
        work_queue_method = DAG_exec_state.keyword_arguments['work_queue_method']
        # used to calculate size of work queue (2 * number of tasks in DAG)
        number_of_tasks = DAG_exec_state.keyword_arguments['number_of_tasks']
        list_of_fanout_values = DAG_exec_state.keyword_arguments['list_of_work_queue_or_payload_fanout_values']
#rhc: async batch
        async_call = DAG_exec_state.keyword_arguments['async_call']

#rhc: lambda inc
        DAG_info_passed_from_DAG_exector = DAG_exec_state.keyword_arguments['DAG_info']
        # assert: if we are doing incremental DAG generation with real Lambdas
        # then the process_faninNBs_batch method shoudl pass DAG_info since
        # tcp_server does not read DAG_info in this case.
        if (wukongdnc.dag.DAG_executor_constants.compute_pagerank and wukongdnc.dag.DAG_executor_constants.use_incremental_DAG_generation and (
            not wukongdnc.dag.DAG_executor_constants.run_all_tasks_locally)
        ):
            if DAG_info_passed_from_DAG_exector == None:
                logger.error("[Error]: Internal Error: synchronize_process_faninNBs_batch: using incremental DAG generation"
                    + " with real Lambdas but received None from DAG_executor process_faninNBs_batch.")

        logger.trace("tcp_server: synchronize_process_faninNBs_batch: calling_task_name: " + calling_task_name + ": worker_needs_input: " + str(worker_needs_input)
            + " faninNBs size: " +  str(len(faninNBs)))
#rhc: async batch
        logger.trace("BBBBBBBBB synchronize_process_faninNBs_batch BBBBBBBBBBBBBBBBBBBBBBB")
        logger.trace("tcp_server: synchronize_process_faninNBs_batch: calling_task_name: " + calling_task_name + ": async_call: " + str(async_call))

        # assert:
        if worker_needs_input:
            if not wukongdnc.dag.DAG_executor_constants.run_all_tasks_locally:
                logger.error("[Error: Internal Error: synchronize_process_faninNBs_batch: worker needs input but using lambdas.")

        #assert: if worker needs work then we should be using synch call so we can check the results for work
        if worker_needs_input:
            if async_call:
                logger.trace("[Error]: Internal Error: synchronize_process_faninNBs_batch: worker_needs_input but using async_call")

        # assert:
        if async_call:
            # must be running real lambdas to execute tasks or running workr processes
            # and not worker_needs_input so here we won't be sending any work back
            # and there is no reason to wait for this process_fannNBs method to finish.
            # Note: If we are using threads to simulate
            # lambdas we do not call tcp_server.process_faninNBs_batch. If we are not storing objects
            # in lamdas, we call process_fninNBs to process the faninNBs one by one. If we are
            # storing objects in lambdas we do call process_faninNBs_batch but we will be running
            # tcp_server_lambda so we will not call this version in tcp_server.
            #if not (not run_all_tasks_locally):
            # This matches the if statement in the work_loop.
            # Note: using_workers and not using_threads_not_processes means we 
            # will be calling process_faninNBs_batch (i.e., this method)
            if not (not wukongdnc.dag.DAG_executor_constants.run_all_tasks_locally or (wukongdnc.dag.DAG_executor_constants.run_all_tasks_locally and wukongdnc.dag.DAG_executor_constants.using_workers and not wukongdnc.dag.DAG_executor_constants.using_threads_not_processes and not worker_needs_input)):
                logger.error("[Error: Internal Error: synchronize_process_faninNBs_batch: async_call but not (not run_all_tasks_locally).")

        # Note: If we are using lambdas, then we are not using workers (for now) so worker_needs_input
        # must be false. Also, we are currently not piggybacking the fanouts so there should be no 
        # fanouts to process.

        # True if the client needs work and we got some work for the client, which are the
        # results of a faninNB.
        got_work = False
        list_of_work = []

        # We currently do not need to upate the DAG-info during incremental
        # DAG generation since we pass the info we need to this method
        # instead of getting it from DAG_info. Note that a worker that
        # calls batch processing on some fanouts and faninNBs definitely
        # has these fanouts and faninNBs in its DAG_info and can thus 
        # pass info about these fanouts and faninNBs to this batch
        # processing method.
        # We are leaving this code here in case we need it here or 
        # somewhere else later.
        if False and wukongdnc.dag.DAG_executor_constants.compute_pagerank and wukongdnc.dag.DAG_executor_constants.use_incremental_DAG_generation:
            DAG_infoBuffer_monitor_method_keyword_arguments = {}
            # call DAG_infoBuffer_monitor (bounded buffer) get_current_version_number_DAG_info()
            logger.trace("tcp_server: synchronize_process_faninNBs_batch: " + calling_task_name + ": get DAG_info version number.")
            DAG_infoBuffer_monitor_method = "get_current_version_number_DAG_info"
            base_name, isTryMethod = isTry_and_getMethodName(DAG_infoBuffer_monitor_method)
            DAG_infoBuffer_monitor_type = "DAG_infoBuffer_Monitor"
            is_select = isSelect(DAG_infoBuffer_monitor_type)
            logger.trace("tcp_server: synchronize_process_faninNBs_batch: method_name: " + DAG_infoBuffer_monitor_method + ", base_name: " + base_name + ", isTryMethod: " + str(isTryMethod))
            logger.trace("tcp_server: synchronize_process_faninNBs_batch: synchronizer_class_name: : " + DAG_infoBuffer_monitor_type + ", is_select: " + str(is_select))
            DAG_infoBuffer_monitor_name = "process_DAG_infoBuffer_Monitor"
            synchronizer_name = self._get_synchronizer_name(type_name = None, name = DAG_infoBuffer_monitor_name)
            synchronizer = tcp_server.synchronizers[synchronizer_name]

            if (synchronizer is None):
                logger.error("[Error]: Internal Error: tcp_server: synchronize_process_faninNBs_batch:"
                    + " could not find existing Synchronizer with name '%s'" % synchronizer_name)
                raise ValueError("synchronize_process_faninNBs_batch: Could not find existing Synchronizer with name '%s'" % synchronizer_name)

    #rhc select then replace
                #return_value = synchronizer.synchronize(base_name, DAG_exec_state, **work_queue_method_keyword_arguments)
    #rhc select with
            if is_select:
                #self.lock_synchronizer()
                synchronizer.lock_synchronizer()
            
            if is_select:
                wait_for_return = True
                # rhc: DES
                #return_value = self.synchronizeSelect(base_name, DAG_exec_state, wait_for_return, **DAG_exec_state.keyword_arguments)
                # This is return value of deposit_all which is 0 and is not used
                most_recently_generated_DAG_info = synchronizer.synchronizeSelect(base_name, DAG_exec_state, wait_for_return, **DAG_infoBuffer_monitor_method_keyword_arguments)
            else:
                most_recently_generated_DAG_info = synchronizer.synchronize(base_name, DAG_exec_state, **DAG_infoBuffer_monitor_method_keyword_arguments)

            #logger.trace("tcp_server: synchronize_process_faninNBs_batch:"
            #    + " most recently deposited DAG_info version number: " + str(most_recently_generated_DAG_info.get_DAG_version_number())
            #    + " version number of current DAG_info: " + str(DAG_info.get_DAG_version_number()))

            DAG_info = most_recently_generated_DAG_info

        if not wukongdnc.dag.DAG_executor_constants.create_all_fanins_faninNBs_on_start:
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
                        "type": wukongdnc.dag.DAG_executor_constants.process_work_queue_Type,   # probably a bounded_buffer
                        "name": "process_work_queue",
                        "state": make_json_serializable(dummy_state),	
                        "id": msg_id
                    } 
                    # not created yet so create object
                    logger.trace("tcp_server: synchronize_process_faninNBs_batch: "
                        + "create sync object process_work_queue on the fly.")
                    self.create_obj_but_no_ack_to_client(creation_message)
                else:
                    logger.trace("tcp_server: synchronize_process_faninNBs_batch: sync object " + 
                        "process_work_queue already created.")

            logger.trace("tcp_server: synchronize_process_faninNBs_batch: do synchronous_sync after create. ")

        # List list_of_work_queue_or_payload_fanout_values may be empty: if a state has no fanouts this list is empty. 
        # If a state has 1 fanout it will be a become task and there will be no more fanouts so this list is empty.
        # If the state has no fanouts, then worker_needs_work will be True and this fanout list will be empty.
        # otherwise, the worker will have a become task so worker_needs_input will be false (and this
        # list may or may not be empty depending on whether there are any more fanouts.)
        if len(list_of_fanout_values) > 0:
            # Note: In DAG_executor process_fanouts , if each fanout has its own output then
            # we extract that output from output and change the callin_task_name
            # to reflect that we are sending values for this specific fanout task.
            # When we batch fannNBs we also batch fanouts in which case process_fanouts
            # puts the work items in a list and returns the list which is given
            # to process_faninNBs_batch.
            #
            # if run_all_tasks_locally then we are not using lambdas so add fanouts as work in the 
            # work queue. If we are using workers, we already used one fanout as a become task
            # so these fanouts can be put in the work queue.
            # If we are using lambdas, then we can use the parallel invoker to invoke the fanout lambdas
            if wukongdnc.dag.DAG_executor_constants.run_all_tasks_locally:
                # work_queue.deposit_all(list_of_work_queue_or_payload_fanout_values)

                synchronizer = tcp_server.synchronizers[work_queue_name]
                
                #synchClass = synchronizer._synchClass
                #try:
                #    synchronizer_method = getattr(synchClass, work_queue_method)
                #except Exception as ex:
                #    logger.error("tcp_server: synchronize_process_faninNBs_batch: deposit fanout work: Failed to find method '%s' on object '%s'." % (work_queue_method, work_queue_type))
                #    raise ex

                # To call "deposit" instead of "deposit_all", change the work_queue_method above before you
                # generate synchronizer_method and here iterate over the list.
                # work_queue_method = "deposit"
                #for work_tuple in list_of_work:
                    #work_queue_method_keyword_arguments = {}
                    #work_queue_method_keyword_arguments['value'] = work_tuple
                    #returnValue, restart = synchronizer_method(synchronizer._synchronizer, **work_queue_method_keyword_arguments) 

                work_queue_method_keyword_arguments = {}
                work_queue_method_keyword_arguments['list_of_values'] = list_of_fanout_values
                # call work_queue (bounded buffer) deposit_all(list_of_work_queue_or_payload_fanout_values)
                logger.trace("tcp_server: synchronize_process_faninNBs_batch: " + calling_task_name + ": deposit all fanout work.")
    #rhc select first, replace
                #returnValue, restart = synchronizer_method(synchronizer._synchronizer, **work_queue_method_keyword_arguments) 
    #rhc select with
                base_name, isTryMethod = isTry_and_getMethodName(work_queue_method)
                is_select = isSelect(work_queue_type)
                logger.trace("tcp_server: synchronize_process_faninNBs_batch: method_name: " + work_queue_method + ", base_name: " + base_name + ", isTryMethod: " + str(isTryMethod))
                logger.trace("tcp_server: synchronize_process_faninNBs_batch: synchronizer_class_name: : " + work_queue_type + ", is_select: " + str(is_select))

    #rhc select then replace
                #return_value = synchronizer.synchronize(base_name, DAG_exec_state, **work_queue_method_keyword_arguments)
    #rhc select with
                if is_select:
                    #self.lock_synchronizer()
                    synchronizer.lock_synchronizer()
            
                if is_select:
                    wait_for_return = True
                    # rhc: DES
                    #return_value = self.synchronizeSelect(base_name, DAG_exec_state, wait_for_return, **DAG_exec_state.keyword_arguments)
                    # This is return value of deposit_all which is 0 and is not used
                    return_value_ignored = synchronizer.synchronizeSelect(base_name, DAG_exec_state, wait_for_return, **work_queue_method_keyword_arguments)
                else:
                    return_value_ignored = synchronizer.synchronize(base_name, DAG_exec_state, **work_queue_method_keyword_arguments)

                # deposit_all return value is 0 and restart is False
        else:
            logger.trace("tcp_server: synchronize_process_faninNBs_batch: " + calling_task_name + ": no fanout work to deposit")

        # if not same_output_for_all_fanout_fanin then we are nto sending 
        # the same output to each faninNB. Instead, we extract a faninNBs
        # particular output from the output and change the calling_task_name
        # to reflect this scheme. Example. Output is a dictionary with 
        # values for tasks PR2_1 and PR2_2 where calling_task_name is PR1_1.
        # then we set the calling task name to PR1_1-PR2_1 and grab
        # output["PR2_1"] and use these values for the PR2_1 faninNB. Likewise
        # for faninNB PR2_2.
        # Note: For DAG generation, for each state we execute a task and 
        # for each task T we have t say what T;s task_inputs are - these are the 
        # names of tasks that give inputs to T. When we have per-fanout output
        # instead of having the same output for all fanouts, we specify the 
        # task_inputs as "sending task - receiving task". So a sending task
        # S might send outputs to fanouts A and B so we use "S-A" and "S-B"
        # as the task_inputs, instad of just using "S", which is the Dask way.
        output = None
        calling_task_name = ""
        if not wukongdnc.dag.DAG_executor_constants.same_output_for_all_fanout_fanin:
            output = DAG_exec_state.keyword_arguments['result']
            calling_task_name = DAG_exec_state.keyword_arguments['calling_task_name']

        name_index = 0
        for name in faninNBs:
            start_state_fanin_task  = DAG_states_of_faninNBs_fanouts[name]
#rhc batch
            # the size for fannNB[i] is all_faninNB_sizes_of_faninNBs[i]
            faninNB_size = all_faninNB_sizes_of_faninNBs[name_index]
            synchronizer_name = self._get_synchronizer_name(type_name = None, name = name)
            logger.trace("tcp_server: synchronize_process_faninNBs_batch: " + calling_task_name 
                + ": Trying to retrieve existing Synchronizer '%s'" % synchronizer_name)
            
            #synchronizer = tcp_server.synchronizers[synchronizer_name]
            if not wukongdnc.dag.DAG_executor_constants.create_all_fanins_faninNBs_on_start:
                # This is one lock for all creates; we could have one lock 
                # per object: get object names from DAG_nfo and create
                # a map of object name to lock at the start of tcp_server,
                # similar to wht InfniD does when it creates mapped functions
                # and their locks.
                with create_synchronization_object_lock:
                    synchronizer = tcp_server.synchronizers.get(synchronizer_name,None)
                    if (synchronizer is None):
                        dummy_state_for_create_message = DAG_executor_State(function_name = "DAG_executor.DAG_executor_lambda", function_instance_ID = str(uuid.uuid4()))
                        # passing to the created faninNB object:
#rhc batch
                        if not (wukongdnc.dag.DAG_executor_constants.compute_pagerank and wukongdnc.dag.DAG_executor_constants.use_incremental_DAG_generation):
                            #DAG_states = DAG_info.get_DAG_states()

                            ## assert:
                            #if not DAG_states[name] == start_state_fanin_task:
                            #    logger.trace("[Error]: Internal Error: tcp_server: synchronize_process_faninNBs_batch:"
                            #       + "DAG_states[name] != start_state_fanin_task")
                                
                            #dummy_state_for_create_message.keyword_arguments['start_state_fanin_task'] = DAG_states[name]
                            dummy_state_for_create_message.keyword_arguments['start_state_fanin_task'] = start_state_fanin_task
                            dummy_state_for_create_message.keyword_arguments['store_fanins_faninNBs_locally'] = wukongdnc.dag.DAG_executor_constants.store_fanins_faninNBs_locally
                            if not wukongdnc.dag.DAG_executor_constants.run_all_tasks_locally: 
                                if read_DAG_info:
                                    read_DAG_info = False
    #rhc: DAG_info: race? no since locked
                                    DAG_info = DAG_Info.DAG_info_fromfilename()
                                    logger.trace("tcp_server: read DAG_info for real lambdas.")
    #rhc: DAG_info
                                    print("tcp_server: DAG_map:")
                                    # this required: # pylint: disable=E0601, E0118
                                    DAG_map = DAG_info.get_DAG_map() 
                                    for key, value in DAG_map.items():
                                        print(key)
                                        print(value) 
                                # do not understand why pyline flags this use of DAG_info as used-before-assignment (E0601)
                                # and used-prior-global-declaration (E0118) 
                                # this requried: pylint: disable=E0601, E0118
                                dummy_state_for_create_message.keyword_arguments['DAG_info'] = DAG_info 
                            else:
                                # we are running locally with the fanins/faninNBs on the tcp_server.
                                # The faninNBs will not try to create any new worker thread/processes
                                # or simulate lambda threads. The worker threads do not run on the 
                                # tcp_server so the tcp_server cannot create them. For worker processes,
                                # the FaninB task is added to the work_queue, which is on the tcp_server.
                                # For simulated lambda threads, the threads do not run on the tcp_server.
                                # The faninNB results are returned to the calling simulated lamba thread
                                # and that caller statrs a new simulated lambda thread to excute the FaninNB task.
                                dummy_state_for_create_message.keyword_arguments['DAG_info'] = None

                            #dummy_state_for_create_message.keyword_arguments['DAG_info'] = DAG_info
                            #all_fanin_task_names = DAG_info.get_all_fanin_task_names()
                            #all_fanin_sizes = DAG_info.get_all_fanin_sizes()
                            #all_faninNB_task_names = DAG_info.get_all_faninNB_task_names()
                            #all_faninNB_sizes = DAG_info.get_all_faninNB_sizes()
                            #is_fanin = name in all_fanin_task_names

                            ## assert: No fanins in batch procesing - this is the code
                            ## from synchronize_sync, which can be called for fan_in ops
                            ## on fanins and faninNBs, but we only batch process faninNBs
                            ## here so there should be no fanins. 
                            #if is_fanin:
                            #    logger.error("[Error]: Internal Error: tcp_server: synchronize_process_faninNBs_batch:"
                            #        + " fanin " + name + " in batch.")

                            #is_faninNB = name in all_faninNB_task_names
                            #if not is_fanin and not is_faninNB:
                            #    logger.error("[Error]: Internal Error: tcp_server: synchronize_process_faninNBs_batch:"
                            #        + " sync object for synchronize_sync is neither a fanin nor a faninNB.")

                            # compute size of fanin or faninNB 
                            # FanIn could be a non-select (monitor) or select FanIn type
                            #if is_fanin:
                            #    fanin_type = FanIn_Type
                            #    fanin_index = all_fanin_task_names.index(name)
                            #    # The name of a fanin/faninNB is the name of its fanin task.
                            #    # The index of taskname in the list of task_names is the same as the
                            #    # index of the corresponding size of the fanin/fanout
                            #    dummy_state_for_create_message.keyword_arguments['n'] = all_fanin_sizes[fanin_index]
                            #else:
                            fanin_type = wukongdnc.dag.DAG_executor_constants.FanInNB_Type
                            #faninNB_index = all_faninNB_task_names.index(name)
                            #dummy_state_for_create_message.keyword_arguments['n'] = all_faninNB_sizes[faninNB_index]
                            dummy_state_for_create_message.keyword_arguments['n'] = faninNB_size
                            
                            ##assert:
                            #    if not faninNB_size == all_faninNB_sizes[faninNB_index]:
                            #        logger.error("[Error]: Internal Error: tcp_server: synchronize_process_faninNBs_batch: "
                            #            + " not faninNB_size == all_faninNB_sizes[faninNB_index]")
                            #    else:
                            #        logger.trace("EQUAL SIZES")
                        else: # incremental DAG generation
                            # Compute pagerank for incremental DAG generation.
                            # - Rewrote this code so that it no longer needs DAG_info
                            # for faninNB creation. The issue with DAG_info is that 
                            # tcp_server used to read it once at the beginning. But when 
                            # DAG_info is incrementally updated, we must retrive a 
                            # new DAG_info each time a new one is generated so that 
                            # we can get DAG_states via DAG_info.get_DAG_states(), 
                            # i.e., unless we update DAG_info, the state we need
                            # DAG_states[name] may not be in the DAG. Another issue
                            # with readig DAG_info once at the beginning of tcp_Server is 
                            # that when we are running pagerank, the bfs() code generates
                            # the DAG and then calls DAG_executor_driver to execute
                            # the DAG. But we can't start tcp_server unti after bfs()
                            # generates the DAG since otherwise there is no DAG for 
                            # tcp_server to read. We would need to sycnronize the tcp_server
                            # and bfs() so that tcp_server only does the read after bfs()
                            # writes DAG_info. We could do this with a tcp message sent
                            # by bfs() or most likely DAG_executor_driver telling tcp_server
                            # to do the reaf. An alernative is to have bfs() start tcp_server
                            # after it has written the DAG, which we will look at. But we want the 
                            # tcp_server to start in its own DOS console and keep the console 
                            # open so we can see the tcp_server debug messages. And tcp_server
                            # needs to be able to accept connections before ADG_executor_driver
                            # tries to make a connection with tcp_server.
                            # 
                            # We no longer
                            # have tcp_server read DAG_info at the beginning.
                            # Note that 
                            # at the beginning of this batch processing method there
                            # is code to withdraw the latest version of DAG_info. 
                            # we no longer need this code; instead, we pass the 
                            # information that we need about the DAG to the batch
                            # processing method - this information is the sizes
                            # of the fninNBs being processed. So we pass the 
                            # start states of the faninNB tasks, and the sizes of the 
                            # faninNBs to the batch processing method insted of having
                            # this method retrive this info from DAG_info.
                            # - Rewrote this code to get rid of the fanin stuff. We only
                            # batch process faninNBs, not fanins, so the fanin part
                            # is not needed. This code was originally copied from 
                            # suchronize_sync, which can process faninNBs and fanins,
                            # but we don't need the fanin code, and therefor don't want
                            # to pass the fanin information to the batch process method
                            # so we removed the fanin code.

                            #DAG_states = DAG_info.get_DAG_states()

                            # assert:
                            #if not DAG_states[name] == start_state_fanin_task:
                            #    logger.trace("[Error]: Internal Error: tcp_server: synchronize_process_faninNBs_batch:"
                             #       + "DAG_states[name] != start_state_fanin_task")
                                
                            dummy_state_for_create_message.keyword_arguments['start_state_fanin_task'] = start_state_fanin_task
                            dummy_state_for_create_message.keyword_arguments['store_fanins_faninNBs_locally'] = wukongdnc.dag.DAG_executor_constants.store_fanins_faninNBs_locally
                            if not wukongdnc.dag.DAG_executor_constants.run_all_tasks_locally:
                                # using real lambdas
                                # Note: When faninNB start a Lambda, DAG_info will be in the payload so pass DAG_info to faninNBs.
                                # (Worker Threads and processes read DAG_info from disk. Real Lambdas can read DAG_info from
                                # disk on the tcp_server but not when incremental DAG generation is used.
                                # In that case, the DAG is not complete so tcp_server cannot read it as
                                # the start. Instead, we pass DAG_info to tcp_server.

                                dummy_state_for_create_message.keyword_arguments['DAG_info'] = DAG_info_passed_from_DAG_exector
#rhc: DAG_info          
                                if DAG_info_passed_from_DAG_exector == None: 
                                    logger.error(": DAG_info is None for synchronize_process_faninNBs_batch create on fly: " + synchronizer_name)
                                else:
                                    logger.error("FanInNB: fanin_task_name: DAG_info is NOT None for synchronize_process_faninNBs_batch create on fly :"  + synchronizer_name )

                            else:
                                # we are running locally with the fanins/faninNBs on the tcp_server.
                                # The faninNBs will not try to create any new worker thread/processes
                                # or simulate lambda threads. The worker threads do not run on the 
                                # tcp_server so the tcp_server cannot create them. For worker processes,
                                # the FaninB task is added to the work_queue, which is on the tcp_server.
                                # For simulated lambda threads, the threads do not run on the tcp_server.
                                # The faninNB results are returned to the calling simulated lamba thread
                                # and that caller statrs a new simulated lambda thread to excute the FaninNB task.
                                dummy_state_for_create_message.keyword_arguments['DAG_info'] = None
                            #dummy_state_for_create_message.keyword_arguments['DAG_info'] = DAG_info
                            #all_fanin_task_names = DAG_info.get_all_fanin_task_names()
                            #all_fanin_sizes = DAG_info.get_all_fanin_sizes()
                            #all_faninNB_task_names = DAG_info.get_all_faninNB_task_names()
                            #all_faninNB_sizes = DAG_info.get_all_faninNB_sizes()
                            #is_fanin = name in all_fanin_task_names

                            # assert: No fanins in batch procesing - this is the code
                            # from synchronize_sync, which can be called for fan_in ops
                            # on fanins and faninNBs, but we only batch process faninNBs
                            # here so there should be no fanins. 
                            #if is_fanin:
                            #    logger.error("[Error]: Internal Error: tcp_server: synchronize_process_faninNBs_batch:"
                            #        + " fanin " + name + " in batch.")

                            #is_faninNB = True # name in all_faninNB_task_names
                            #if not is_fanin and not is_faninNB:
                            #    logger.error("[Error]: Internal Error: tcp_server: synchronize_process_faninNBs_batch:"
                            #        + " sync object for synchronize_sync is neither a fanin nor a faninNB.")

                            # compute size of fanin or faninNB 
                            # FanIn could be a non-select (monitor) or select FanIn type
                            #if is_fanin:
                            #    fanin_type = FanIn_Type
                            #    # fanin_index = all_fanin_task_names.index(name)
                            #    # The name of a fanin/faninNB is the name of its fanin task.
                            #    # The index of taskname in the list of task_names is the same as the
                            #    # index of the corresponding size of the fanin/fanout
                            #    #dummy_state_for_create_message.keyword_arguments['n'] = all_fanin_sizes[fanin_index]
                            #   dummy_state_for_create_message.keyword_arguments['n'] = -1
                            #else:

                            fanin_type = wukongdnc.dag.DAG_executor_constants.FanInNB_Type
                            #faninNB_index = all_faninNB_task_names.index(name)
                            #dummy_state_for_create_message.keyword_arguments['n'] = all_faninNB_sizes[faninNB_index]
                            dummy_state_for_create_message.keyword_arguments['n'] = faninNB_size
                            
                            #assert:
                            #if not faninNB_size == all_faninNB_sizes[faninNB_index]:
                            #    logger.error("[Error]: Internal Error: tcp_server: synchronize_process_faninNBs_batch: "
                            #        + " not faninNB_size == all_faninNB_sizes[faninNB_index]")
                            #else:
                            #    logger.trace("EQUAL SIZES")

                        msg_id = str(uuid.uuid4())	# for debugging
                        creation_message = {
                            "op": "create",
                            "type": fanin_type,
                            "name": name,
                            "state": make_json_serializable(dummy_state_for_create_message),	
                            "id": msg_id
                        }
                        # not created yet so create object
                        logger.trace("tcp_server: synchronize_process_faninNBs_batch: "
                            + "create sync object " + name + " on the fly.")
                        self.create_obj_but_no_ack_to_client(creation_message)
                    else:
                        logger.trace("tcp_server: synchronize_process_faninNBs_batch: object " + name + " already created.")

                logger.trace("tcp_server: synchronize_process_faninNBs_batch: do synchronous_sync after create. ")
            
            synchronizer = tcp_server.synchronizers[synchronizer_name]

            if (synchronizer is None):
                raise ValueError("synchronize_process_faninNBs_batch: Could not find existing Synchronizer with name '%s'" % synchronizer_name)

            base_name, isTryMethod = isTry_and_getMethodName(method_name)
            is_select = isSelect(type_arg)
            logger.trace("tcp_server: synchronize_process_faninNBs_batch: method_name: " + method_name + ", base_name: " + base_name + ", isTryMethod: " + str(isTryMethod))
            logger.trace("tcp_server: synchronize_process_faninNBs_batch: synchronizer_class_name: : " + type_arg + ", is_select: " + str(is_select))

            # These are per FaninNB
            DAG_exec_state.keyword_arguments['fanin_task_name'] = name
            DAG_exec_state.keyword_arguments['start_state_fanin_task'] = start_state_fanin_task
   
            if wukongdnc.dag.DAG_executor_constants.same_output_for_all_fanout_fanin:
                # the result output and the callng task name have alrady 
                # been set accordingly - same output to all faninNBS.
                # Note: For DAG generation, for each state we execute a task and 
                # for each task T we have t say what T;s task_inputs are - these are the 
                # names of tasks that give inputs to T. When we have per-fanout output
                # instead of having the same output for all fanouts, we specify the 
                # task_inputs as "sending task - receiving task". So a sending task
                # S might send outputs to fanouts A and B so we use "S-A" and "S-B"
                # as the task_inputs, instad of just using "S", which is the Dask way.
                        pass
            else:
                # create per-faninNB output as shown in example above,
                qualified_name = str(calling_task_name) + "-" + str(name)
                DAG_exec_state.keyword_arguments['result'] = output[name]
                DAG_exec_state.keyword_arguments['calling_task_name'] = qualified_name


            logger.trace("tcp_server: synchronize_process_faninNBs_batch: " + calling_task_name + ": calling synchronizer.synchronize.")
#rhc: select replace this
            #return_value = synchronizer.synchronize(base_name, DAG_exec_state, **DAG_exec_state.keyword_arguments)
#rhc: select with this
# Need to call the select version if using selects
            if is_select:
                #self.lock_synchronizer()
                synchronizer.lock_synchronizer()
            
            if is_select:
                # create result_buffer, create execute() reference, call execute(), result_buffer.withdraw(), 
                # return excute's result, with no restart (by definition of synchronous non-try-op)
                # (Send result to client below.)
                wait_for_return = True
                # rhc: DES
                #return_value = self.synchronizeSelect(base_name, DAG_exec_state, wait_for_return, **DAG_exec_state.keyword_arguments)
                return_value = synchronizer.synchronizeSelect(base_name, DAG_exec_state, wait_for_return, **DAG_exec_state.keyword_arguments)
            else:
                return_value = synchronizer.synchronize(base_name, DAG_exec_state, **DAG_exec_state.keyword_arguments)

            """
            Note: It does not make sense to batch try-ops, or to execute a batch of synchronous
                ops that may block and that have return values. faninNB fan_ins are non-blocking
                and either (1) we ignore the return value since we were not the last caller to fan_in
                or (2) we are last caller for a faninNB fan_in so we save the work (for only one faninNB) 
                which is the statr state and the dict. of results for the fanin task, and return this work
                to the caller. So we batch faninNB fan_ins but we are not returning multple return values
                for multiple fan_in operations. (Again, we return either no work or the work (fanin results)
                from one of the fan_ins for which we were the last caller).
                This means that we do not need the generality of doing a batch of synchronize-sync
                operations that could be try-ops, or could block, or could each require a value
                to be returned. Thus we call synchronizer.synchronize() for each fan_in. Note that
                synchronize_sync calls synchronizer.synchronize to do the fan_in. Note: We do not call
                sychronize_sync and let it call synchronizer.synchronize since synchronize_sync sends the 
                return value of synchronizer.synchronize back to the client and we do not want that to 
                happen. So we call synchronizer.synchronize and process the return value (see if it is 
                work that can be sent to the client (if the client needs work))
            """
            if return_value != 0:
                # return value is a dictionary of results for the fanin task
                work_tuple = (start_state_fanin_task,return_value)
                if worker_needs_input:
                    # Changing local worker_needs_input; it's still True on client caller, of course
                    worker_needs_input = False
                    got_work = True
                    DAG_exec_state.return_value = work_tuple
                    DAG_exec_state.blocking = False 
                    logger.trace("tcp_server: synchronize_process_faninNBs_batch: " + calling_task_name + ": send work: %s sending name %s and return_value %s back for method %s." % (synchronizer_name, name, str(return_value), method_name))
                    logger.trace("tcp_server: synchronize_process_faninNBs_batch: " + calling_task_name + ": send work: %s sending state %s back for method %s." % (synchronizer_name, str(DAG_exec_state), method_name))
                    # Note: We send work back now, as soon as we get it, to free up the waitign client
                    # instead of waiting until the end. This delays the processing of FaninNBs and depositing
                    # any work in the work_queue. Possibly: create a thread to do this.                 
                    self.send_serialized_object(cloudpickle.dumps(DAG_exec_state))
                else:
                    # Client doesn't need work or we already got some work for the client, so add this work
                    # to the work_queue)
                    list_of_work.append(work_tuple)
                    logger.trace("tcp_server: synchronize_process_faninNBs_batch: " + calling_task_name + ": not sending work: %s sending name %s and return_value %s back for method %s." % (synchronizer_name, name, str(return_value), method_name))
                    logger.trace("tcp_server: synchronize_process_faninNBs_batch: " + calling_task_name + ": not sending work: %s sending state %s back for method %s." % (synchronizer_name, str(DAG_exec_state), method_name))
            # else we were not the last caller of fanin, so we deposited our result, which will be given to
            # the last caller.

            name_index += 1
 
        if len(list_of_work) > 0:   
            # There is work in the form of faninNB tasks for which we were the last fan_in caller; thia
            # work gets enqueued in the work queue        
            synchronizer = tcp_server.synchronizers[work_queue_name]

            #synchClass = synchronizer._synchClass
            #try:
            #    synchronizer_method = getattr(synchClass, work_queue_method)
            #except Exception as ex:
            #    logger.error("tcp_server: synchronize_process_faninNBs_batch: deposit fanin work: Failed to find method '%s' on object '%s'." % (work_queue_method, work_queue_type))
            #    raise ex

            # To call "deposit" instead of "deposit_all", change the work_queue_method above before you
            # generate synchronizer_method and here iterate over the list.
            # work_queue_method = "deposit"
            #for work_tuple in list_of_work:
                #work_queue_method_keyword_arguments = {}
                #work_queue_method_keyword_arguments['value'] = work_tuple
                #returnValue, restart = synchronizer_method(synchronizer._synchronizer, **work_queue_method_keyword_arguments) 

            work_queue_method_keyword_arguments = {}
            work_queue_method_keyword_arguments['list_of_values'] = list_of_work
            # call work_queue (bounded buffer) deposit_all(list_of_work)
            logger.trace("tcp_server: synchronize_process_faninNBs_batch: " + calling_task_name + ": deposit_all FanInNB work, list_of_work size: " + str(len(list_of_work)))
#rhc select first, replace
            #returnValue, restart = synchronizer_method(synchronizer._synchronizer, **work_queue_method_keyword_arguments) 
#rhc select with
            base_name, isTryMethod = isTry_and_getMethodName(work_queue_method)
            is_select = isSelect(work_queue_type)
            logger.trace("tcp_server: synchronize_process_faninNBs_batch: method_name: " + work_queue_method + ", base_name: " + base_name + ", isTryMethod: " + str(isTryMethod))
            logger.trace("tcp_server: synchronize_process_faninNBs_batch: synchronizer_class_name: : " + work_queue_type + ", is_select: " + str(is_select))

#rhc select then replace
            #return_value = synchronizer.synchronize(base_name, DAG_exec_state, **work_queue_method_keyword_arguments)
#rhc select with
            if is_select:
                #self.lock_synchronizer()
                synchronizer.lock_synchronizer()
            
            if is_select:
                # create result_buffer, create execute() reference, call execute(), result_buffer.withdraw(), 
                # return excute's result, with no restart (by definition of synchronous non-try-op)
                # (Send result to client below.)
                wait_for_return = True
                # rhc: DES
                #return_value = self.synchronizeSelect(base_name, DAG_exec_state, wait_for_return, **DAG_exec_state.keyword_arguments)
                return_value_ignored = synchronizer.synchronizeSelect(base_name, DAG_exec_state, wait_for_return, **work_queue_method_keyword_arguments)
            else:
                return_value_ignored = synchronizer.synchronize(base_name, DAG_exec_state, **work_queue_method_keyword_arguments)
    
            # deposit_all return value is 0 and restart is False

            logger.trace("tcp_server: synchronize_process_faninNBs_batch: " + calling_task_name + ": work_queue_method: " + str(work_queue_method))
            logger.trace("tcp_server: synchronize_process_faninNBs_batch: " + calling_task_name + ": " + str(work_queue_method) + ", return_Value " + str(return_value_ignored))
            logger.trace("tcp_server: synchronize_process_faninNBs_batch: " + calling_task_name + ": " + str(work_queue_method) + ", successfully called work_queue method. ")

#rhc: async batch
        if not got_work and not async_call:
            # if we didn't need work or we did need work but we did not get any above, 
            # then we return 0 to indicate that we didn't get work. Note if we needed 
            # work and we got work from faninNB it was sent above so we don't send
            # any work here. Here we only return 0 to indicate that we did not get
            # any work, but only f this is not an async call.
            # if async_call then the worker did not need work and so it called async and is not 
            # waiting for a (sync) result.
            # if worker_needs_input is sent from client as False, then got_work is initially False 
            # and never set to True.
            logger.trace("tcp_server: synchronize_process_faninNBs_batch: " + calling_task_name + ": no work to return, returning DAG_exec_state.return_value = 0.")           
            DAG_exec_state.return_value = 0
            DAG_exec_state.blocking = False
            # Note: if we decide not to send work back immediately to the waitign clent (see above),
            # then we can comment this send out, uncomment the else and he log mssage in the else part, 
            # and uncomment the send at the end. That send will either send the DAG_exec_state return 
            # value 0 we just set or the DAG_xec_state above with the return value containing work.
            self.send_serialized_object(cloudpickle.dumps(DAG_exec_state))
        #else:
            # we got work above so we already returned the DAG_exec_state.return_value set to work_tuple 
            # via self.send_serialized_object(work)
            #logger.trace("tcp_server: synchronize_process_faninNBs_batch: returning work in DAG_exec_state.") 

#rhc: async batch
        #debugging
        if async_call:
            logger.trace("CCCCCCCCCCCCCCCCCCCCCCCC")
            logger.trace("tcp_server: synchronize_process_faninNBs_batch: " + calling_task_name + ": async_call so not returning a value.")           

        #logger.trace("tcp_server: synchronize_process_faninNBs_batch: returning DAG_state %s." % (str(DAG_exec_state)))           
        #self.send_serialized_object(cloudpickle.dumps(DAG_exec_state))

    def synchronize_sync(self, message = None):
        """
        Synchronous synchronization.

        Key-word arguments:
        -------------------
            message (dict):
                The payload from the AWS Lambda function.
        """

        global DAG_info
        global read_DAG_info

        logger.trace("[HANDLER] server.synchronize_sync() called.")
        obj_name = message['name']
        method_name = message['method_name']
        # type_arg = message["type"]
        state = decode_and_deserialize(message["state"])
        # synchronizer_name = self._get_synchronizer_name(type_name = type_arg, name = obj_name)
        synchronizer_name = self._get_synchronizer_name(type_name = None, name = obj_name)

        logger.trace("tcp_server: synchronize_sync: Trying to retrieve existing Synchronizer '%s'" % synchronizer_name)

        logger.trace("tcp_server: synchronize_sync: create_all_fanins_faninNBs_on_start:" 
            + str(wukongdnc.dag.DAG_executor_constants.create_all_fanins_faninNBs_on_start))
        #synchronizer = tcp_server.synchronizers[synchronizer_name]
        if not wukongdnc.dag.DAG_executor_constants.create_all_fanins_faninNBs_on_start:
            # This is one lock for all creates; we could have one lock 
            # per object: get object names from DAG_nfo and create
            # a map of object name to lock at the start of tcp_server,
            # similar to wht InfniD does when it creates mapped functions
            # and their locks.
            with create_synchronization_object_lock:
                logger.trace("get synch")
                synchronizer = tcp_server.synchronizers.get(synchronizer_name,None)
                if (synchronizer is None):
                    logger.trace("got None")
                    #  On-the-fly: This part here is only for DAGs, not, e.g., Semaphores
                    #  For other typs of objects, we'll need their type so we'll need
                    #  to deal with it in DAG_executor? which will call createif (as it does now)
                    #  passing a create message with the message?
                    #  But only user nows the create info, as typically user would create semaphors, etc.
                    #  User could "register" objects so we have their information. And register
                    #  could do creates() or register objects on tcp_server for create_if?
                    #  Then user cannot do ops on the fly, which is normally the case in 
                    #  any program, considering scope this would be odd, but our objects have
                    #  "server scope"? which is global, so ...
                    if method_name == "fan_in":
#rhc batch
#rhc: Todo: These then and else branches are the same so remove one - using if alse to remove one, for now
#  BUT: need both branches now - only do read if inc?
                        if False and not (wukongdnc.dag.DAG_executor_constants.compute_pagerank and wukongdnc.dag.DAG_executor_constants.use_incremental_DAG_generation):
                            dummy_state_for_create_message = DAG_executor_State(function_name = "DAG_executor", function_instance_ID = str(uuid.uuid4()))
                            # passing to the created faninNB object:
    
                            #DAG_states = DAG_info.get_DAG_states()
                            #dummy_state_for_create_message.keyword_arguments['start_state_fanin_task'] = DAG_states[synchronizer_name]
                            dummy_state_for_create_message.keyword_arguments['store_fanins_faninNBs_locally'] = wukongdnc.dag.DAG_executor_constants.store_fanins_faninNBs_locally
                            if not wukongdnc.dag.DAG_executor_constants.run_all_tasks_locally:  
                                if read_DAG_info: 
                                    read_DAG_info = False
                                
                                    DAG_info = DAG_Info.DAG_info_fromfilename()
                                    logger.trace("tcp_server: read DAG_info for real lambdas.")
    #rhc: DAG_info
                                    logger.trace("tcp_server: DAG_map:")
                                    # do not understand why pyline flags this use of DAG_info as used-before-assignment (E0601)
                                    # and used-prior-global-declaration (E0118)
                                    # this requried: pylint: disable=E0601, E0118
                                    DAG_map = DAG_info.get_DAG_map() 
                                    for key, value in DAG_map.items():
                                        logger.trace(key)
                                        logger.trace(value) 
                                # this required: # pylint: disable=E0601, E0118
                                dummy_state_for_create_message.keyword_arguments['DAG_info'] = DAG_info 
                            else:
                                # we are running locally with the fanins/faninNBs on the tcp_server.
                                # The faninNBs will not try to create any new worker thread/processes
                                # or simulate lambda threads. The worker threads do not run on the 
                                # tcp_server so the tcp_server cannot create them. For worker processes,
                                # the FaninB task is added to the work_queue, which is on the tcp_server.
                                # For simulated lambda threads, the threads do not run on the tcp_server.
                                # The faninNB results are returned to the calling simulated lamba thread
                                # and that caller statrs a new simulated lambda thread to excute the FaninNB task.
                                dummy_state_for_create_message.keyword_arguments['DAG_info'] = None

                            """
                            #dummy_state_for_create_message.keyword_arguments['DAG_info'] = DAG_info
                            all_fanin_task_names = DAG_info.get_all_fanin_task_names()
                            all_fanin_sizes = DAG_info.get_all_fanin_sizes()
                            all_faninNB_task_names = DAG_info.get_all_faninNB_task_names()
                            all_faninNB_sizes = DAG_info.get_all_faninNB_sizes()
                            is_fanin = synchronizer_name in all_fanin_task_names
                            is_faninNB = synchronizer_name in all_faninNB_task_names
                            if not is_fanin and not is_faninNB:
                                logger.error("[Error]: Internal Error: tcp_server: synchronize_sync:"
                                    + " sync object " + synchronizer_name + " for synchronize_sync is neither a fanin nor a faninNB.")

                            # compute size of fanin or faninNB 
                            # FanIn could be a non-select (monitor) or select FanIn type
                            if is_fanin:
                                fanin_type = FanIn_Type
                                fanin_index = all_fanin_task_names.index(synchronizer_name)
                                # The name of a fanin/faninNB is the name of its fanin task.
                                # The index of taskname in the list of task_names is the same as the
                                # index of the corresponding size of the fanin/fanout
                                dummy_state_for_create_message.keyword_arguments['n'] = all_fanin_sizes[fanin_index]
                            else:
                                fanin_type = FanInNB_Type
                                faninNB_index = all_faninNB_task_names.index(synchronizer_name)
                                dummy_state_for_create_message.keyword_arguments['n'] = all_faninNB_sizes[faninNB_index]
                            """
#rhc batch
                            if state.keyword_arguments['fanin_type'] == "faninNB":
                                fanin_type = wukongdnc.dag.DAG_executor_constants.FanInNB_Type
                            else: # fanin_type is "fanin"
                                fanin_type = wukongdnc.dag.DAG_executor_constants.FanIn_Type

                            dummy_state_for_create_message.keyword_arguments['n'] = state.keyword_arguments['n']
                            if not fanin_type == wukongdnc.dag.DAG_executor_constants.FanIn_Type:
                                dummy_state_for_create_message.keyword_arguments['start_state_fanin_task'] = state.keyword_arguments['start_state_fanin_task']

                            #msg_id = str(uuid.uuid4())	# for debugging
                            #creation_message = {
                            #    "op": "create",
                            #    "type": fanin_type,
                            #    "name": synchronizer_name,
                            #    "state": make_json_serializable(dummy_state_for_create_message),	
                            #    "id": msg_id
                            #}
                            ## not created yet so create object
                            #logger.trace("tcp_server: synchronize_sync: "
                            #    + "create sync object " + synchronizer_name + " on the fly")
                            #self.create_obj_but_no_ack_to_client(creation_message)
                        else:
                            # Compute pagerank for incremental DAG generation.
                            # - Rewrote this code so that it no longer needs DAG_info
                            # for faninNB creation. The issue with DAG_info is that 
                            # tcp_server reads it once at the beginning. But since 
                            # DAG_info is incrementally updated, we must retrive a 
                            # new DAG_info each time a new one is generated so that 
                            # we can get DAG_states via DAG_info.get_DAG_states(), 
                            # i.e., unless we update DAG_info, the state we need
                            # DAG_states[name] may not be in the DAG. Note that 
                            # at the beginning of this batch processing method there
                            # is code to withdraw the latest version of DAG_info. 
                            # we no longer need this code; instead, we pass the 
                            # information that we need about the DAG to the batch
                            # processing method - this information is the sizes
                            # of the fninNBs being processed. So we pass the 
                            # start states of the faninNB tasks, and the sizes of the 
                            # faninNBs to the batch processing method insted of having
                            # this method retrive this info from DAG_info.
                            # - Note: When we use worker threads with fanins/faninNBS stored 
                            # here on tcp_server, the DG_infoBuffer_monitor, as well as 
                            # the work_queue are local, i.e., they are not stored here
                            # on the tcp_server so ths sycnronize_sync method cannot
                            # access DAG_infoBuffer_monitor thus we cannot access it
                            # to withdraw an updated DAG_info.
#rhc: ToDo: 
                            # - Rewrote this code to use the state parameters start sate of 
                            # dag tas, n, and the newly added fanin type instead of getting
                            # this nfo from the DAG_info. Thus we do not need to update the 
                            # DAG_ifo during incremental DAG generation.

                            #DAG_states = DAG_info.get_DAG_states()

                            # assert:
                            #if not DAG_states[name] == start_state_fanin_task:
                            #    logger.trace("[Error]: Internal Error: tcp_server: synchronize_process_faninNBs_batch:"
                                #       + "DAG_states[name] != start_state_fanin_task")
                            
                            # Given in DAG_executor.faninNB_remotely:
                            #DAG_exec_state.keyword_arguments['n'] = keyword_arguments['n']
                            #DAG_exec_state.keyword_arguments['start_state_fanin_task'] = keyword_arguments['start_state_fanin_task']
#rhc batch
                            dummy_state_for_create_message = DAG_executor_State(function_name = "DAG_executor", function_instance_ID = str(uuid.uuid4()))
                            dummy_state_for_create_message.keyword_arguments['store_fanins_faninNBs_locally'] = wukongdnc.dag.DAG_executor_constants.store_fanins_faninNBs_locally
                            if not wukongdnc.dag.DAG_executor_constants.run_all_tasks_locally:
                                if read_DAG_info:
                                    read_DAG_info = False 
                                    DAG_info = DAG_Info.DAG_info_fromfilename()
                                    logger.trace("tcp_server: read DAG_info for real lambdas.")
#rhc: DAG_info
                                    logger.trace("tcp_server: DAG_map:")
                                    DAG_map = DAG_info.get_DAG_map()
                                    for key, value in DAG_map.items():
                                        logger.trace(key)
                                        logger.trace(value)
                                dummy_state_for_create_message.keyword_arguments['DAG_info'] = DAG_info
                            else:
                                # we are running locally with the fanins/faninNBs on the tcp_server.
                                # The faninNBs will not try to create any new worker thread/processes
                                # or simulate lambda threads. The worker threads do not run on the 
                                # tcp_server so the tcp_server cannot create them. For worker processes,
                                # the FaninB task is added to the work_queue, which is on the tcp_server.
                                # For simulated lambda threads, the threads do not run on the tcp_server.
                                # The faninNB results are returned to the calling simulated lamba thread
                                # and that caller statrs a new simulated lambda thread to excute the FaninNB task.
                                dummy_state_for_create_message.keyword_arguments['DAG_info'] = None
                            #dummy_state_for_create_message.keyword_arguments['DAG_info'] = DAG_info
                            #all_fanin_task_names = DAG_info.get_all_fanin_task_names()
                            #all_fanin_sizes = DAG_info.get_all_fanin_sizes()
                            #all_faninNB_task_names = DAG_info.get_all_faninNB_task_names()
                            #all_faninNB_sizes = DAG_info.get_all_faninNB_sizes()
                            #is_fanin = name in all_fanin_task_names

                            # assert: No fanins in batch procesing - this is the code
                            # from synchronize_sync, which can be called for fan_in ops
                            # on fanins and faninNBs, but we only batch process faninNBs
                            # here so there should be no fanins. 
                            #if is_fanin:
                            #    logger.error("[Error]: Internal Error: tcp_server: synchronize_process_faninNBs_batch:"
                            #        + " fanin " + name + " in batch.")

                            #is_faninNB = True # name in all_faninNB_task_names
                            #if not is_fanin and not is_faninNB:
                            #    logger.error("[Error]: Internal Error: tcp_server: synchronize_process_faninNBs_batch:"
                            #        + " sync object for synchronize_sync is neither a fanin nor a faninNB.")

                            # compute size of fanin or faninNB 
                            # FanIn could be a non-select (monitor) or select FanIn type
                            #if is_fanin:
                            #    fanin_type = FanIn_Type
                            #    # fanin_index = all_fanin_task_names.index(name)
                            #    # The name of a fanin/faninNB is the name of its fanin task.
                            #    # The index of taskname in the list of task_names is the same as the
                            #    # index of the corresponding size of the fanin/fanout
                            #    #dummy_state_for_create_message.keyword_arguments['n'] = all_fanin_sizes[fanin_index]
                            #   dummy_state_for_create_message.keyword_arguments['n'] = -1
                            #else:
#rhc batch
                            if state.keyword_arguments['fanin_type'] == "faninNB":
                                fanin_type = wukongdnc.dag.DAG_executor_constants.FanInNB_Type
                            else: # fanin_type is "fanin"
                                fanin_type = wukongdnc.dag.DAG_executor_constants.FanIn_Type
                            # where in DAG_executor_constants:
                            #FanIn_Type = "DAG_executor_FanIn"
                            #FanInNB_Type = "DAG_executor_FanInNB"
                            
                            #faninNB_index = all_faninNB_task_names.index(name)
                            #dummy_state_for_create_message.keyword_arguments['n'] = all_faninNB_sizes[faninNB_index]
                            dummy_state_for_create_message.keyword_arguments['n'] = state.keyword_arguments['n']
                            if not fanin_type == wukongdnc.dag.DAG_executor_constants.FanIn_Type:
                                dummy_state_for_create_message.keyword_arguments['start_state_fanin_task'] = state.keyword_arguments['start_state_fanin_task']
                            #assert:
                            #if not faninNB_size == all_faninNB_sizes[faninNB_index]:
                            #    logger.error("[Error]: Internal Error: tcp_server: synchronize_process_faninNBs_batch: "
                            #        + " not faninNB_size == all_faninNB_sizes[faninNB_index]")
                            #else:
                            #    logger.trace("EQUAL SIZES")

                        msg_id = str(uuid.uuid4())	# for debugging
                        creation_message = {
                            "op": "create",
                            "type": fanin_type,
                            "name": synchronizer_name,
                            "state": make_json_serializable(dummy_state_for_create_message),	
                            "id": msg_id
                        }
                        # not created yet so create object
                        logger.trace("tcp_server: synchronize_sync: "
                            + "create sync object " + synchronizer_name + " on the fly")
                        self.create_obj_but_no_ack_to_client(creation_message)    
                    # else:
                        # pass
                        # what to do for non-DAG objects, we need info about them. But these are objects
                        # like Semaphore and user currently calls create, or allow create_all(). If
                        # create on fly then we need enough info in synchronize_sync call to create
                        # object - so user would have to pass this create info on called ops.
                else:
                    logger.trace("tcp_server: synchronize_sync: object "  + synchronizer_name + " already created.")
            # end with

            logger.trace("tcp_server: synchronize_sync: do synchronous_sync after create. ")
        
        synchronizer = tcp_server.synchronizers[synchronizer_name]

        if (synchronizer is None):
            raise ValueError("synchronize_sync: Could not find existing Synchronizer with name '%s'" % synchronizer_name)

        # This tcp_server passing itself so synchronizer can access tcp_server's send_serialized_object
        # return_value = synchronizer.synchronize_sync(tcp_server, obj_name, method_name, type_arg, state, synchronizer_name)
        # Note: If synchronizer is a FanInNB_Select, we call fan_in() 
        # directly, instead # of going through execute() and all the 
        # selective wait stuff; this is because fan_in guard is always 
        # true and the FanInNB has no other entry methods to call.
        # return_value = synchronizer.synchronize_sync(tcp_server, obj_name, method_name, state, synchronizer_name, self)
        return_value = synchronizer.synchronize_sync(tcp_server, synchronizer_name, method_name, state, synchronizer_name, self)

        logger.trace("tcp_server called synchronizer.synchronize_sync")

        # sysnchronizer synchronize_sync pickled the return value so sending 
        # pickled value back to client.
        return return_value

    def createif_and_synchronize_sync(self,  message = None):
        """
        Create object if it hasn't been created yet and do synchronous synchronization.

        Key-word arguments:
        -------------------
            message (dict):
               message['name'] contains a tuple of messages where
                  message[0] is the creation message and
                  message[1] is the synchrnous_sync message

            creation_message is created using:
            if is_fanin:
                fanin_type = FanIn_Type
            else:
                fanin_type = FanInNB_Type
            msg_id = str(uuid.uuid4())	# for debugging
            creation_message = {
                "op": "create",
                "type": fanin_type,
                "name": fanin_name,
                "state": make_json_serializable(dummy_state),	
                "id": msg_id
            }
        """
        logger.trace("[TCPSERVER] createif_and_synchronize_sync() called.")

        if wukongdnc.dag.DAG_executor_constants.create_all_fanins_faninNBs_on_start:
            logger.error("[Error]: Internal Error: tcp_server: createif_and_synchronize_sync: "
                + "called createif_and_synchronize_sync but create_all_fanins_faninNBs_on_start")

        messages = message['name']
        creation_message = messages[0]
        synchronous_sync_message = messages[1]

        obj_name = synchronous_sync_message['name']
        method_name = synchronous_sync_message['method_name']
        state = decode_and_deserialize(synchronous_sync_message["state"])
        # not using synchronizer class name in object name for now, i.e., use "bb" instead of "BoundedBuffer_bb"
        # type_arg = message["type"]
        # synchronizer_name = self._get_synchronizer_name(type_name = type_arg, name = obj_name)
        synchronizer_name = self._get_synchronizer_name(type_name = None, name = obj_name)

        # check if already created
        logger.trace("tcp_server: createif_and_synchronize_sync: Trying to retrieve existing Synchronizer '%s'" % synchronizer_name)
        #synchronizer = MessageHandler.synchronizers[synchronizer_name]
        # This is one ock for all creates; we could have one lock 
        # per object: get object names from DAG_nfo and create
        # a map of object name to lock at the start of tcp_server,
        # similar to wht InfniD does when it creates mapped functions
        # and their locks.
        with create_synchronization_object_lock:
            synchronizer = tcp_server.synchronizers.get(synchronizer_name,None)
            if (synchronizer is None):
                # not created yet so create object
                logger.trace("tcp_server: createif_and_synchronize_sync: "
                    + "create sync object " + obj_name + " on the fly")
                self.create_obj_but_no_ack_to_client(creation_message)

        logger.trace("message_handler_lambda: createif_and_synchronize_sync: do synchronous_sync ")
       
        synchronizer = tcp_server.synchronizers[synchronizer_name]
        
        if (synchronizer is None):
            raise ValueError("tcp_server: createif_and_synchronize_sync: Could not find existing Synchronizer with name '%s'" % synchronizer_name)
         
        # return_value = synchronizer.synchronize_sync(tcp_server, obj_name, method_name, type_arg, state, synchronizer_name)
        # return_value = synchronizer.synchronize_sync(tcp_server, obj_name, method_name, state, synchronizer_name, self)
        # MessageHandler not passing itself since synchronizer does not use send_serialized_object to send results 
        # to tcp_server - Lambda returns values synchronously.
        #return_value = synchronizer.synchronize_sync(obj_name, method_name, state, synchronizer_name, self)

        return_value = synchronizer.synchronize_sync(obj_name, method_name, state, synchronizer_name)
        
        logger.trace("tcp_server: synchronize_sync called synchronizer.synchronize_sync")

        return return_value

    # Note: We do not use asynch calls to fanin/faninNB objects. Also,
    # we do not crate objects on the fly (yet) for non-fanin/faninNB
    # objects so we do not have any code in here for create objects
    # on the fly. See the "On-the-fly" comment in synchronize_sync.
    def synchronize_async(self, message = None):
        """
        Asynchronous synchronization.

        Key-word arguments:
        -------------------
            message (dict):
                The payload from the AWS Lambda function.
        """        
        logger.trace("[HANDLER] server.synchronize_async() called.")
        obj_name = message['name']
        method_name = message['method_name']
        # type_arg = message["type"]        
        state = decode_and_deserialize(message["state"])

        synchronizer_name = self._get_synchronizer_name(type_name = None, name = obj_name)
        logger.trace("tcp_server: synchronize_async: Trying to retrieve existing Synchronizer '%s'" % synchronizer_name)
        synchronizer = tcp_server.synchronizers[synchronizer_name]

        if (synchronizer is None):
            raise ValueError("synchronize_async: Could not find existing Synchronizer with name '%s'" % synchronizer_name)

        logger.trace("tcp_server: synchronize_async: Successfully found synchronizer")

        # return_value = synchronizer.synchronize_async(obj_name, method_name, type_arg, state, synchronizer_name)
        #return_value = synchronizer.synchronize_async(obj_name, method_name, state, synchronizer_name)
        return_value = synchronizer.synchronize_async(synchronizer_name, method_name, state, synchronizer_name)
        logger.trace("tcp_server called synchronizer.synchronize_async")

        return return_value    

    def createif_and_synchronize_async(self,  message = None):
        pass

    def recv_object(self):
        """
        Receive an object from a remote entity via the given websocket.

        The TCP server uses a "streaming" API that is implemented using file handles (or rather the API looks like we're just using file handles).
        """

        data = bytearray()
        logger.trace("receive_object: Do self.rfile.read(4)")
        try:
            while (len(data)) < 4:
                # Read the size of the incoming serialized object.
                #new_data = self.rfile.read(4 - len(data)).strip() 
                new_data = self.rfile.read(4 - len(data))

                if not new_data:
                    # If we see this print a lot, then we may want to remove/comment-out the break and simply sleep for 1-10ms, then try reading again?
                    # Maybe if we fail to read any new data after ~3 tries, then we give up? But maybe we're giving up too early (i.e., trying to read data,
                    # finding no data to read, and giving up on the entire read immediately, rather than waiting and trying to read again).
                    
                    # Commented out to suppress the messages
                    #logger.warning("Stopped reading incoming message size from socket early. Have read " + str(len(data)) + " bytes of a total expected 4 bytes.")
                    logger.trace("Stopped reading incoming message size from socket early. Have read " + str(len(data)) + " bytes of a total expected 4 bytes.")
                    break 

                data.extend(new_data)

                logger.trace("recv_object: self.rfile.read an additional %d bytes from remote client: %s" % (len(new_data), str(new_data)))
        except ConnectionAbortedError as ex:
            logger.trace("Error in recv_object self.rfile.read(4) -- while reading the incoming message size.")
            logger.trace(repr(ex))
            logger.error("Established connection aborted while reading incoming size.")
            logger.error(repr(ex))
            return None 

        logger.trace("receive_object self.rfile.read(4) successful, len(data): %d. Bytes received: %s" % (len(data), str(data)))

        # Convert bytes of size to integer.
        incoming_size = int.from_bytes(data, 'big')

        logger.trace("recv_object int.from_bytes successful. Incoming message will have size: %d bytes" % incoming_size)

        if incoming_size == 0:
            logger.trace("Incoming size is 0. Client is expected to have disconnected.")
            return None 
        
        if incoming_size < 0:
            logger.error("Incoming size < 0: " + incoming_size + ". An error might have occurred...")
            return None 

        logger.trace("recv_object: Will receive another message of size %d bytes" % incoming_size)

        data = bytearray()
        logger.trace("recv_object: created second data object, incoming_size: " + str(incoming_size))
        try:
            while len(data) < incoming_size:
                # Read serialized object (now that we know how big it'll be).
                logger.trace("execute new_data ")
                #new_data = self.rfile.read(incoming_size - len(data)).strip() # Do we need to call .strip() here? What if we removed something we're not supposed to?
                new_data = self.rfile.read(incoming_size - len(data)) # Do we need to call .strip() here? What if we removed something we're not supposed to?
                logger.trace("executed new_data ")
                # Strip removes the leading and trailing bytes ASCII whitespace. I think it's probably fine, but I'm not sure.

                if not new_data:
                    # If we see this print a lot, then we may want to remove/comment-out the break and simply sleep for 1-10ms, then try reading again?
                    # Maybe if we fail to read any new data after ~3 tries, then we give up? But maybe we're giving up too early (i.e., trying to read data,
                    # finding no data to read, and giving up on the entire read immediately, rather than waiting and trying to read again).
                    logger.warn("Stopped reading from socket early. Have read " + str(len(data)) + " bytes of a total expected " + str(incoming_size) + " bytes.")
                    break 

                data.extend(new_data)
                logger.trace("recv_object: have read %d/%d bytes from remote client." % (len(data), incoming_size))
        except ConnectionAbortedError as ex:
            logger.error("Established connection aborted while reading data.")
            logger.error(repr(ex))
            return None 
        
        logger.trace("recv_object: received all %d bytes from remote client. bytes received: %s" % (len(data), str(data)))
        return data 

    def send_serialized_object(self, obj):
        """
        Send an ALREADY SERIALIZED object to the connected client.

        Serialize the object before calling this function via:
            obj = cloudpickle.dumps(obj)

        Arguments:
        ----------
            obj (bytes):
                The already-serialized object that we are sending to a remote entity (presumably an AWS Lambda executor).
        """
        logger.trace("Sending payload of size %d bytes to remote client now. Bytes to be sent: %s (size) followed by %s" % (len(obj), str(len(obj).to_bytes(4, byteorder='big')), str(obj)))
        self.wfile.write(len(obj).to_bytes(4, byteorder='big'))     # Tell the client how many bytes we're sending.
        self.wfile.write(obj)                                       # Then send the object.
        logger.trace("Sent %d bytes to remote client." % len(obj))

    def close_all(self, message = None):
        """
        Clear all known synchronizers.
        """
        logger.trace("Received close_all request.")

        tcp_server.synchronizers = {}

        #############################
        # Write ACK back to client. #
        #############################
        resp = {
            "op": "ack",
            "op_performed": "close_all"
        }        
        logger.trace("Sending ACK to client %s for 'close_all' operation." % self.client_address[0])
        resp_encoded = json.dumps(resp).encode('utf-8')
        self.send_serialized_object(resp_encoded)
        logger.trace("Sent ACK of size %d bytes to client %s for 'close_all' operation." % (len(resp_encoded), self.client_address[0]))          

    def close_obj(self, message = None):
        """
        Called by a remote Lambda to delete an object here on the TCP server.

        Key-word arguments:
        -------------------
            message (dict):
                The payload from the AWS Lambda function.
        """
        type_arg = message["type"]
        name = message["name"]
        #state = decode_and_deserialize(message["state"])

        logger.trace("Received close_obj request for object with name '%s' and type %s" % (name, type_arg))

        #############################
        # Write ACK back to client. #
        #############################
        resp = {
            "op": "ack",
            "op_performed": "close_obj"
        }        
        logger.trace("Sending ACK to client %s for CLOSE_OBJ operation." % self.client_address[0])
        resp_encoded = json.dumps(resp).encode('utf-8')
        self.send_serialized_object(resp_encoded)
        logger.trace("Sent ACK of size %d bytes to client %s for CLOSE_OBJ operation." % (len(resp_encoded), self.client_address[0]))        

    def setup_server(self, message = None):
        logger.trace("server.setup() called.")
        pass 
        
class TCPServer(object):
    def __init__(self):
        self.synchronizers =  {}    # dict of name to Synchronizer
        self.server_threads = []    # list      - not used
        self.clients =        []    # list      - not used
        self.server_address = ("0.0.0.0",25565)
        self.tcp_server = socketserver.ThreadingTCPServer(self.server_address, TCPHandler)

        if not wukongdnc.dag.DAG_executor_constants.create_all_fanins_faninNBs_on_start or not wukongdnc.dag.DAG_executor_constants.run_all_tasks_locally:
#rhc: this could be not run_all_tasks_locally and not incremental DAG generation
# since if we are using lambas and incemental then we will get the updated
# DAG_info some other way, i.e., we will wait for new DAG and then a new
# lambda will be started with the new DAG_info and the results. Saving
# results and (re)starting with results and continued state seems like
# normal restart for no-wait.
            # Need DAG_info if we are creating objects on their first
            # use or objects will be invoking lambdas (as lambdas need)
            # the DAG_info.

            
            # We only read DAG_info one time, and only if we are 
            # using real lambdas. When we invoke a real lambda
            # we need to give it the DAG_info. This read was moved up to the 
            # action handler. This is because the DAG_info object is not 
            # saved to a file until after BFS runs and generates the 
            # (complete) DAG (for non-incremental). So we don't let the 
            # TCP server rad the DAG until it gets its first input over
            # the socket. This input cannot be received until after 
            # DAG_execution starts, and since execution starts after BFS
            # finishes (the complete DAG_info) TCP server cannot try to 
            # read the DAG_info file before it is written. See BFS where
            # it generates the DAG and then calls run() of the 
            # DAG_executor_driver.
            #global DAG_info
            # reads from default file './DAG_info.pickle'
            #DAG_info = DAG_Info.DAG_info_fromfilename()
            
            global create_work_queue_lock
            create_work_queue_lock = Lock()
            global create_synchronization_object_lock
            create_synchronization_object_lock = Lock()
    
    def start(self):
        logger.trace("Starting TCP server.")
        try:
            self.tcp_server.serve_forever()
        except Exception as ex:
            logger.error("Exception encountered:" + repr(ex))

if __name__ == "__main__":
    # Create a Server Instance
#rhc: added tcp_server global variable at top or 
    tcp_server = TCPServer()
    tcp_server.start()
