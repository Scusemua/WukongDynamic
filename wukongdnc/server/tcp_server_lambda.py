#from re import A
import json
import traceback
import socketserver
import threading
#import json

import cloudpickle
#import base64
from .util import decode_and_deserialize, make_json_serializable
import uuid
from ..wukong.invoker import invoke_lambda_synchronously
from ..dag.DAG_executor_constants import using_Lambda_Function_Simulator, use_single_lambda_function, using_function_invoker
from ..dag.DAG_Executor_lambda_function_simulator import InfiniD # , Lambda_Function_Simulator
from ..dag.DAG_info import DAG_Info
from threading import Lock

# Set up logging.
import logging 
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(asctime)s] [%(threadName)s] %(levelname)s: %(message)s')

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)

logger.addHandler(ch)
class TCPHandler(socketserver.StreamRequestHandler):
    def handle(self):

        #TCP handler for incoming requests from AWS Lambda functions.
       
        while True:
            logger.info("[HANDLER] TCPHandler lambda: Recieved one request from {}".format(self.client_address[0]))
            logger.info("[HANDLER] TCPHandler lambda: Recieved one request from {}".format(self.client_address[1]))

            self.action_handlers = {
                "create": self.create_obj,
                #"create_all": self.create_all_fanins_and_faninNBs,
                "setup": self.setup_server,
                "synchronize_async": self.synchronize_async,
                "synchronize_sync": self.synchronize_sync,
                "close_all": self.close_all,
                # These are DAG execution operations
                "create_all_fanins_and_faninNBs_and_possibly_work_queue": self.create_all_fanins_and_faninNBs_and_possibly_work_queue,
                "synchronize_process_faninNBs_batch": self.synchronize_process_faninNBs_batch,
                "create_work_queue": self.create_work_queue
            }

            try:
                data = self.recv_object()

                if data is None:
                    logger.warning("recv_object() returned None. Exiting handler now.")
                    return

                json_message = json.loads(data)
                #obj_name = json_message['name']
                message_id = json_message["id"]
                logger.debug("[HANDLER] TCPHandler: Received message (size=%d bytes) from client %s with ID=%s" % (len(data), self.client_address[0], message_id))
                action = json_message.get("op", None)
                #tcp_server calls local method
                self.action_handlers[action](message = json_message)

            except ConnectionResetError as ex:
                logger.error(ex)
                logger.error(traceback.format_exc())
                return
            except Exception as ex:
                logger.error(ex)
                logger.error(traceback.format_exc())
                
    def _get_synchronizer_name(self, type_name = None, name = None):
        """
        Return the key of a synchronizer object. 

        The key is a string of the form <type>-<name>.
        """
        return str(name) # return str(type_name + "_" + name)

    #################################################################################
    # Synchronous invocation:
    # import boto3
    # lambda_client = boto3.client('lambda', region_name = "us-east-1")
    
    # Local method of tcp_server, which will synchronously invoke a Lambda
    def invoke_lambda_synchronously(self, json_message):
        #name = json_message.get("name", None)
        # For DAG with workers, we have fanins, faninNBs and the process work queue. Note that we
        # process the faninNBs in a batch and that method will access the faninNBs and 
        # the process work queue so we put all of the fanin, faninNBs, and work queue in
        # the same function.
        # Also, we create all fnins and faninNBs at once so if we wan to use more than one
        # lambda then we need to call N create alls, one for each of the N lambdas storing 
        # the fanin and faninNBs.
        # For DAG with lambdas, there is no work_queue, but we will still create all lambdas
        # at once.
        # Note:
        # type = jsom_message.get("type", None)
        # Types used will be BoundedBuffer_Select, DAG_executor_FanIn_Select, DAG_executor_FanInNB_Select
        # or the type used when passing a list of messages for creating all fanins and 
        # faninNBs: "DAG_executor_fanin_or_faninNB"7

        """
        # For simple prototype using two Lambdas to store synchronization objects.
        if name == "result" or name == "final_result":
            function_name = "LambdaBoundedBuffer"
        else: 
            #name is "finish"; 
            function_name = "LambdaSemapore"
        """

        thread_name = threading.current_thread().name
        # pass thru client message to Lambda
        payload = {"json_message": json_message}
        # return_value = invoke_lambda_synchronously(payload = payload, function_name = function_name)
        if using_Lambda_Function_Simulator:
# ToDo: when out each fanin/faninNb/fanout in simulated lambda, need to use the name from json_message
# instead of single_function.
# Also, use infiniX.enqueue() to "call fanin" instead of invoking the simulated lambda directly.
# This call here is a direct invocation of any message.op. We aer talking about the cal to
# fan_in, which is a synch_op so only those fan_n calls?  Where/when actual creates done?
# this is the create all in sqs, which creates the messages, and then we need to give
# each message to its mapped simulated function? The tcp_server_lambda interepts calls to
# fan_in and issues enqueue() instad?

            if use_single_lambda_function:
                # for function smulator prototype, using a single function to store all the fanins/faninNBs
                # i.e., all fanin/faninNBs mapped under the name 'single_function'
                sync_object_name = "single_function"
            else:
                # use the actual object name, which will be mapped to a function
                sync_object_name = json_message.get("name", None)
            logger.debug("[HANDLER] TCPHandler lambda: " + thread_name + " invoke_lambda_synchronously: using object_name: " + sync_object_name + " in map_of_Lambda_Function_Simulators")
            # tcp_server is from below: if __name__ == "__main__": # Create a Server Instance
            # tcp_server = TCPServer() tcp_server.start()
            #
            # function_key is mapped to a regular Python function:
            #   self.lambda_function = Lambda_Function_Simulator()
            #   self.list_of_Lambda_Function_Simulators = []
            #   self.num_Lambda_Function_Simulators = 1
            #   for _ in range(0,self.num_Lambda_Function_Simulators):
            #      self.list_of_Lambda_Function_Simulators.append(Lambda_Function_Simulator())
            #   self.map_of_Lambda_Function_Simulators = {}
            #   self.map_of_Lambda_Function_Simulators['single_function'] = self.list_of_Lambda_Function_Simulators[0]
            # where function_key is, e.g., the name of a fanin/faninNB object or the name/state of a task
            # so, e.g., every fanin/faninNB and every (fanout) task can be mapped to function in the 
            # list of functions (e.g., InfiniX). For the simple DAG we have fanin multiply-a6c0e4ee-e49b-4ce1-9667-8d562e2657c6
            # and faninNB add-75bbc5c1-cfca-466d-b8ca-215c80882558 and DAG tasks:
            #   increment-985b1e05-0248-4d8a-8bc0-90efe6d6c147
            #   triple-802331c1-d137-435a-af84-59f38980fc6e
            #   multiply-a6c0e4ee-e49b-4ce1-9667-8d562e2657c6
            #   divide-09d35db9-df80-44b9-bbce-9ed8eca7039f
            #   square-783038ad-fbd2-4d64-b4cf-52fc0e8554dd
            #   add-75bbc5c1-cfca-466d-b8ca-215c80882558
            #   increment-798a4bd4-061d-436c-92e0-44773293bf18
            # where the multiply and add tasks are fanin and faninNB task, respectively. Tasks
            # square and triple are fanout tasks, the increment tasks are leaf tasks and task
            # divide is clustered with fanin task multiply so divide is not a fanout task. Perhaps
            # multiply and divide would be excuted by the same mapped function. 
            # Note: fanina and task names are in DAG_info, which can be read at startup: DAG_info = DAG_Info()
            #lambda_function = tcp_server.function_map[object_name]
            simulated_lambda_function = tcp_server.infiniD.get_function(sync_object_name)
            lambda_function_lock = tcp_server.infiniD.get_function_lock(sync_object_name)
            with lambda_function_lock:
                try:
                    return_value = simulated_lambda_function.lambda_handler(payload) 
                except Exception as ex:
                    logger.error("[ERROR]: " + thread_name + ": invoke_lambda_synchronously: Failed to run lambda handler for synch object: " + sync_object_name)
                    logger.error(ex)
        else:     
            # For DAG prototype, we use one function to store process_work_queue and all fanins and faninNBs
            sync_object_name = "LambdaBoundedBuffer" 
            with lambda_function_lock:
                try:
                    # invoker.py's invoke_lambda_synchronously
                    return_value = invoke_lambda_synchronously(function_name = sync_object_name, payload = payload)
                except Exception as ex:
                    logger.error("[ERROR]: " + thread_name + ": invoke_lambda_synchronously: Failed to invoke lambda function for synch object: " + sync_object_name)
                    logger.error(ex)
            # where: lambda_client.invoke(FunctionName=function_name, InvocationType='RequestResponse', Payload=payload_json)
        
        # The return value from the Lambda function will typically be sent by tcp_server to a Lambda client of tcp_server
        return return_value

    def enqueue_and_invoke_lambda_synchronously(self,json_message):
        returned_state = tcp_server.infiniD.enqueue(json_message)
        return returned_state

    def create_obj(self, message = None):
        """
        Called by a remote Lambda to create an object here on the TCP server.

        Key-word arguments:
        -------------------
            message (dict):
                The payload from the AWS Lambda function.
        """        
        logger.debug("[HANDLER] TCPHandler lambda: server.create() called.")

        return_value_ignored = self.invoke_lambda_synchronously(message)    # makes synchronous Lambda call - return value is not meaningful

        resp = {
            "op": "ack",
            "op_performed": "create"
        }
        #############################
        # Write ACK back to client. #
        #############################
        logger.info("Sending ACK to client %s for CREATE operation." % self.client_address[0])
        resp_encoded = json.dumps(resp).encode('utf-8')
        self.send_serialized_object(resp_encoded)
        logger.info("Sent ACK of size %d bytes to client %s for CREATE operation." % (len(resp_encoded), self.client_address[0]))    
    
    """
    Not Used; using create_all_fanins_and_faninNBs_and_possibly_work_queue
    def create_all_fanins_and_faninNBs(self, messages):  
        #where parameter message created using:
        #    messages = (fanin_messages,faninNB_messages) // lists of "create" messages for fanins and faninNBS, respectively
        #    each message was created using:
        #    message = { # this is a message for op create_all_fanins_and_faninNBs; it has a tuple of two lists of regular "create" messages
        #        "op": "create_all_fanins_and_faninNBs", # op
        #        "type": "DAG_executor_fanin_or_faninNB", # this is not a type of synchronizer object; doesn't fit the usual message format
        #        "name": messages, # tuple of lists of "create" messages
        #        "state": make_json_serializable(state),
        #        "id": msg_id
        #    }
        
        logger.debug("create_all_fanins_and_faninNBs: creating " + str(len(messages[0])) + " DAG_executor fanins")

        fanin_messages = messages[0]
        for msg in fanin_messages:
            self.create_obj(msg)

        logger.debug("create_all_fanins_and_faninNBs: creating " + str(len(messages[0])) + " DAG_executor faninNBs")
        faninNB_messages = messages[1]
        for msg in faninNB_messages:
            self.create_obj(msg)
        
        #where msg was created using:
        #        message = {
        #            "op": "create",
        #            "type": "DAG_executor_FanIn/DAG_executor_FanInNB",
        #            "name": fanin_name/faninNB_name,
        #            "state": make_json_serializable(dummy_state),
        #            "id": msg_id
        #        }
        #and
        #        dummy_state = DAG_executor_State()
        #        dummy_state.keyword_arguments['n'] = size # for size in all_faninNB_sizes
        #        dummy_state.keyword_arguments['start_state_fanin_task'] = DAG_states[fanin_name] # where DAG_states maps task names to state
        #        msg_id = str(uuid.uuid4())
    """

    # Create all synch objects at the start of execution. Since they are created
    # in a lambda, for each create message, we pass the message to the 
    # message handler in the invoked lambda:
    #  return_value_ignored = self.invoke_lambda_synchronously(msg)
    # invoke_lambda_synchronously will get the 'name' value from the 
    # message, get the lambda that this name was mapped to, and 
    # invoke that lambda.
    # Note: we are not calling this method in the message_handler_lambda
    # as we need to create the sync objects in lambdas, so we execute
    # this create_all here and one-by-on we invoke the lambdas with 
    # a "create" message so the synch object os created in the invoked 
    # lambda.
    def create_all_fanins_and_faninNBs_and_possibly_work_queue(self, message = None):
        """
        Called by a remote Lambda to create fanins, faninNBs, and pssibly work queue.
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
        logger.debug("[MESSAGEHANDLER] server.create_all_fanins_and_faninNBs_and_possibly_work_queue() called.")
        messages = message['name']
        fanin_messages = messages[0]
        faninNB_messages = messages[1]
        logger.info(str(fanin_messages))
        logger.info(str(faninNB_messages))

        for msg in fanin_messages:
            #self.create_one_of_all_objs(msg)
            logger.debug("tcp_server_lambda: create_all_fanins_and_faninNBs_and_possibly_work_queue() called.")

            return_value_ignored = self.invoke_lambda_synchronously(msg)

            logger.debug("tcp_server_lambda: called Lambda at create_all_fanins_and_faninNBs_and_possibly_work_queue.")

        if len(fanin_messages) > 0:
            logger.info("created fanins")

        for msg in faninNB_messages:
            #self.create_one_of_all_objs(msg)
            logger.debug("tcp_server_lambda: create_all_fanins_and_faninNBs_and_possibly_work_queue() called.")

            return_value_ignored = self.invoke_lambda_synchronously(msg)

            logger.debug("tcp_server_lambda: called Lambda at create_all_fanins_and_faninNBs_and_possibly_work_queue.")

        if len(faninNB_messages) > 0:
            logger.info("created faninNBs")

        # we always create the fanin and faninNBs. We possibly create the work queue. If we send
        # a message for create work queue, in addition to the lst of messages for create
        # fanins and create faninNBs, we create a work queue too.
        # Note: No work_queue when using lambdas
        """
        create_the_work_queue = (len(messages)>2)
        if create_the_work_queue:
            logger.info("create_the_work_queue: " + str(create_the_work_queue) + " len: " + str(len(messages)))
            msg = messages[2]
            self.create_one_of_all_objs(msg)
        """

        resp = {
            "op": "ack",
            "op_performed": "create_all_fanins_and_faninNBs"
        }
        #############################
        # Write ACK back to client. #
        #############################
        logger.info("Sending ACK to client %s for create_all_fanins_and_faninNBs_and_possibly_work_queue operation." % self.client_address[0])
        resp_encoded = json.dumps(resp).encode('utf-8')
        self.send_serialized_object(resp_encoded)
        logger.info("Sent ACK of size %d bytes to client %s for create_all_fanins_and_faninNBs_and_possibly_work_queue operation." % (len(resp_encoded), self.client_address[0]))

        # return value not assigned
        return 0

# ToDo: We are using Lambdas, whcih means (1) using lambdas that may be real or 
# we aer simulated lambas with threads and possibly storing sync objects in 
# lambdas simulated by regular python functions. If we are using threads 
# to simulate lambdas then we return the faninNB fan_in result as work tuple
# and the calling thread will start a threa to execute the fanin task
# (2) there is no list of fanouts since we start the lambdas
# in the DAG_executor; this may change if we pass a list of fanouts and use the 
# parallel invoker to invoke them 
    def synchronize_process_faninNBs_batch(self, message = None):
        """
        Synchronous process all faninNBs for a given state during DAG execution.

        Key-word arguments:
        -------------------
            message (dict):
                The payload from the AWS Lambda function.
        where:
            message = {
                "op": op,
                "type": type,
                "name": name,
                "state": make_json_serializable(state),
                "id": msg_id
            }
        """

        logger.debug("[tcp_server_lambda] synchronize_process_faninNBs_batch() called.")

        # Name of the method called on "DAG_executor_FanInNB" is always "fanin"
        # Not using since we loop through names: for name in faninNBs
        #method_name = message["name"]
        # Note: we do not use object type on synchronize_sync cals, we use type on "create"

        DAG_exec_state = decode_and_deserialize(message["state"])
        faninNBs = DAG_exec_state.keyword_arguments['faninNBs']
        #faninNB_sizes = DAG_exec_state.keyword_arguments['faninNB_sizes']
        # FYI:
        #result = DAG_exec_state.keyword_arguments['result']
        # For debugging:
        calling_task_name = DAG_exec_state.keyword_arguments['calling_task_name'] 
        DAG_states_of_faninNBs = DAG_exec_state.keyword_arguments['DAG_states_of_faninNBs'] 
        # Note: if using lambdas, then we are not using workers (for now) so worker_needs_input 
        # must be false, which is asertd below.
        worker_needs_input = DAG_exec_state.keyword_arguments['worker_needs_input']
        
        # Commented out: since using lambdas no workers and no work to steal
        """
        work_queue_name = DAG_exec_state.keyword_arguments['work_queue_name']
        work_queue_type = DAG_exec_state.keyword_arguments['work_queue_type']
        work_queue_method = DAG_exec_state.keyword_arguments['work_queue_method']
        """
        # Commented out: since using lambdas nno list of fanouts as lambdas
        # are started by DAG_executor. May pass this list in future ad use
        # parallel invoker
        """
        list_of_fanout_values = DAG_exec_state.keyword_arguments['list_of_work_queue_fanout_values']
        """

        logger.info("tcp_server_lambda: synchronize_process_faninNBs_batch: calling_task_name: "
            + calling_task_name + " faninNBs size: " +  str(len(faninNBs)))

        # assert:
        if worker_needs_input:
            logger.error("[Error]: tcp_server_lambda: synchronize_process_faninNBs_batch: Internal Error: worker needs input but using lambdas.")
        
        # Note: If we are using lambdas, then we are not using workers (for now) so worker_needs_input
        # must be false. Also, we are currently not piggybacking the fanouts so there should be no 
        # fanouts to process.

        # True if the client needs work and we got some work for the client, which are the
        # results of a faninNB.

        # Commented out: Since usig Lambdas, there is no work stolen
        # since no workers.
        """
        got_work = False
        list_of_work = []
        """

        # List list_of_work_queue_fanout_values may be empty: if a state has no fanouts this list is empty. 
        # If a state has 1 fanout it will be a become task and there will be no moer fanouts.
        # If there are no fanouts, and using workes then worker_needs_work will be True and this list will be empty.
        # otherwise, the worker will have a become task so worker_needs_input will be false (and this
        # list may or may not be empty depending on whether there are any more fanouts.)
        #
        # If using Lambdas, then we are not using workers and currently the fanout list will be
        # empty since we start the lambdas in process_fanouts. We may pass such a list
        # and use the parallel invoker to invoke the fanouts.

        # Comment out:  When using Lambdas, currently len(list_of_fanout_values) is 
        # always 0 so this is commented out. If we are using lambdas and we want to 
        # use the parallel invoker then we should probably call a 
        # different method to proess fanouts and process the faninNB since
        # fanout processing will be different parallel) andd processing
        # faninNBs will be different too since there is no work stealing as
        # there are no workers.
        # Note: Since using lambdas we do not eposit fanouts nto a work
        # queue so this code is completely diffrent f we do pass in a list
        # of fanouts (for parallel invocation)
        """
        if len(list_of_fanout_values) > 0:
            # if run_all_tasks_locally then we are not using lambdas so add fanouts as work in the 
            # work queue.
            # If we are using lambdas, then we can use the parallel invoker to invoke the fanout lambdas
            if run_all_tasks_locally:
                # work_queue.deposit_all(list_of_work_queue_fanout_values)
                synchronizer = MessageHandler.synchronizers[work_queue_name]
                synchClass = synchronizer._synchClass
                try:
                    synchronizer_method = getattr(synchClass, work_queue_method)
                except Exception as ex:
                    logger.error("tcp_server: synchronize_process_faninNBs_batch: deposit fanout work: Failed to find method '%s' on object '%s'." % (work_queue_method, work_queue_type))
                    raise ex

                # To call "deposit" instead of "deposit_all", change the work_queue_method above before you
                # generate synchronizer_method and here iterate over the list.
                # work_queue_method = "deposit"
                #for work_tuple in list_of_work:
                    #work_queue_method_keyword_arguments = {}
                    #work_queue_method_keyword_arguments['value'] = work_tuple
                    #returnValue, restart = synchronizer_method(synchronizer._synchronizer, **work_queue_method_keyword_arguments) 

                work_queue_method_keyword_arguments = {}
                work_queue_method_keyword_arguments['list_of_values'] = list_of_fanout_values
                # call work_queue (bounded buffer) deposit_all(list_of_work_queue_fanout_values)
                logger.info("tcp_server: synchronize_process_faninNBs_batch: " + calling_task_name + ": deposit all fanout work.")
                returnValue, restart = synchronizer_method(synchronizer._synchronizer, **work_queue_method_keyword_arguments) 
                # deposit_all return value is 0 and restart is False
        else:
            logger.info("tcp_server: synchronize_process_faninNBs_batch: " + calling_task_name + ": no fanout work to deposit")
        """

        for name in faninNBs:
            #synchronizer_name = self._get_synchronizer_name(type_name = None, name = name)
            #logger.debug("tcp_server_lambda: synchronize_process_faninNBs_batch: " + calling_task_name + ": Trying to retrieve existing Synchronizer '%s'" % synchronizer_name)
            #synchronizer = MessageHandler.synchronizers[synchronizer_name]

            #if (synchronizer is None):
            #    raise ValueError("synchronize_process_faninNBs_batch: Could not find existing Synchronizer with name '%s'" % synchronizer_name)

            #base_name, isTryMethod = isTry_and_getMethodName(method_name)
            #is_select = isSelect(type_arg) # is_select = isSelect(type_arg)
    
            #logger.debug("tcp_server: synchronize_process_faninNBs_batch: method_name: " + method_name + ", base_name: " + base_name + ", isTryMethod: " + str(isTryMethod))
            #logger.debug("tcp_server: synchronize_process_faninNBs_batch: synchronizer_class_name: : " + type_arg + ", is_select: " + str(is_select))

            start_state_fanin_task  = DAG_states_of_faninNBs[name]
            # These are per FaninNB
            DAG_exec_state.keyword_arguments['fanin_task_name'] = name
            DAG_exec_state.keyword_arguments['start_state_fanin_task'] = start_state_fanin_task

            msg_id = str(uuid.uuid4())
            message = {
                "op": "synchronize_sync", 
                "name": name,
                "method_name": "fan_in",
                "state": make_json_serializable(DAG_exec_state),
                "id": msg_id
            }

            if using_Lambda_Function_Simulator and using_function_invoker:
                logger.info("*********************tcp_server_lambda: synchronize_process_faninNBs_batch: " + calling_task_name + ": calling infiniD.enqueue(message)."
                    + " start_state_fanin_task: " + str(start_state_fanin_task))
#ToDo: This is a work_tuple. if real lambda what is its form? pickeled?
                returned_state_ignored = self.enqueue_and_invoke_lambda_synchronously(message)
                logger.info("*********************tcp_server_lambda: synchronize_process_faninNBs_batch: " + calling_task_name + ": called infiniD.enqueue(message) "
                    + "returned_state_ignored: " + str(returned_state_ignored))
            else:
                logger.info("*********************tcp_server_lambda: synchronize_process_faninNBs_batch: " + calling_task_name + ": calling invoke_lambda_synchronously."
                    + " start_state_fanin_task: " + str(start_state_fanin_task))
                #return_value = synchronizer.synchronize(base_name, DAG_exec_state, **DAG_exec_state.keyword_arguments)
                returned_state_ignored = self.invoke_lambda_synchronously(message)
                logger.info("*********************tcp_server_lambda: synchronize_process_faninNBs_batch: " + calling_task_name + ": called invoke_lambda_synchronously "
                    + "returned_state_ignored: " + str(returned_state_ignored))

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

            # Comment out: Since we are using lambdas, the fninNBs start the lambdas for the 
            # fanin tasks so the return value is always 0 and there is no 
            # returned work. Unless we simulate the lambdas with threads then the calling
            # thread starts the fanin task.
            """
            returned_work = None
            if return_value != 0:
                # return value is a dictionary of results for the fanin task
                work_tuple = (start_state_fanin_task,return_value)
                if worker_needs_input:
                    # Changing local worker_needs_input; it's still True on client caller, of course
                    worker_needs_input = False
                    got_work = True
                    DAG_exec_state.return_value = work_tuple
                    DAG_exec_state.blocking = False 
                    logger.info("tcp_server: synchronize_process_faninNBs_batch: " + calling_task_name + ": send work: %s sending name %s and return_value %s back for method %s." % (synchronizer_name, name, str(return_value), method_name))
                    logger.info("tcp_server: synchronize_process_faninNBs_batch: " + calling_task_name + ": send work: %s sending state %s back for method %s." % (synchronizer_name, str(DAG_exec_state), method_name))
                    # Note: We send work back now, as soon as we get it, to free up the waitign client
                    # instead of waiting until the end. This delays the processing of FaninNBs and depositing
                    # any work in the work_queue. Possibly: create a thread to do this.   
                    #
                    # Note: this scheme is changed when we are running in a lambda, as we are here.
                    # The work must be returned back to tcp_server_lambda, whcih will send the work to
                    # the client. So save the work and return it at the end.
                    returned_work = DAG_exec_state          
                    #self.send_serialized_object(cloudpickle.dumps(DAG_exec_state))
                else:
                    # Client doesn't need work or we already got some work for the client, so add this work
                    # to the work_queue)
                    list_of_work.append(work_tuple)
                    logger.info("tcp_server: synchronize_process_faninNBs_batch: " + calling_task_name + ": not sending work: %s sending name %s and return_value %s back for method %s." % (synchronizer_name, name, str(return_value), method_name))
                    logger.info("tcp_server: synchronize_process_faninNBs_batch: " + calling_task_name + ": not sending work: %s sending state %s back for method %s." % (synchronizer_name, str(DAG_exec_state), method_name))
            # else we were not the last caller of fanin, so we deposited our result, which will be given to
            # the last caller.
            """

        # Comment out: since we are using lambdas, the fninNBs start the lambdas for the 
        # fanin tasks so the return value is always 0 and there is no 
        # returned work. Unless we simulate the lambdas with threads then the calling
        # thread starts the fanin task.
        """
        if len(list_of_work) > 0:   
            # There is work in the form of faninNB tasks for which we were the last fan_in caller; thia
            # work gets enqueued in the work queue        
            synchronizer = MessageHandler.synchronizers[work_queue_name]
            synchClass = synchronizer._synchClass

            try:
                synchronizer_method = getattr(synchClass, work_queue_method)
            except Exception as ex:
                logger.error("tcp_server: synchronize_process_faninNBs_batch: deposit fanin work: Failed to find method '%s' on object '%s'." % (work_queue_method, work_queue_type))
                raise ex

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
            logger.info("tcp_server: synchronize_process_faninNBs_batch: " + calling_task_name + ": deposit_all FanInNB work, list_of_work size: " + str(len(list_of_work)))
            returnValue, restart = synchronizer_method(synchronizer._synchronizer, **work_queue_method_keyword_arguments) 
            # deposit_all return value is 0 and restart is False

            logger.info("tcp_server: synchronize_process_faninNBs_batch: " + calling_task_name + ": work_queue_method: " + str(work_queue_method) + ", restart " + str(restart))
            logger.info("tcp_server: synchronize_process_faninNBs_batch: " + calling_task_name + ": " + str(work_queue_method) + ", returnValue " + str(returnValue))
            logger.info("tcp_server: synchronize_process_faninNBs_batch: " + calling_task_name + ": " + str(work_queue_method) + ", successfully called work_queue method. ")
        """

        # Since we are using lambdas, the fninNBs start the lambdas for the 
        # fanin tasks so the return value is always 0 and there is no 
        # returned work. Unless we simulate the lambdas with threads then the calling
        # thread starts the fanin task.

        """
        if not got_work:
            # if we didn't need work or we did need work but we did not get any above, 
            # then we return 0 to indicate that we didn't get work. 
            # if worker_needs_input is sent from client as False, then got_work is initially False and never set to True
            logger.info("tcp_server: synchronize_process_faninNBs_batch: " + calling_task_name + ": no work to return, returning DAG_exec_state.return_value = 0.")           
            DAG_exec_state.return_value = 0
            DAG_exec_state.blocking = False
            # Note: if we decide not to send work back immediately to the waitign clent (see above),
            # then we can comment this send out, uncomment the else and he log mssage in the else part, 
            # and uncomment the send at the end. That send will either send the DAG_exec_state return 
            # value 0 we just set or the DAG_xec_state above with the return value containing work.
            #
            # Note: this scheme is changed when we are running in a lambda, as we are here.
            # The work must be returned back to tcp_server_lambda, whcih will send the work to
            # the client. So save the work and return it at the end.    
            returned_work = DAG_exec_state 
            #self.send_serialized_object(cloudpickle.dumps(DAG_exec_state))
        #else:
            # we got work above so we already returned the DAG_exec_state.return_value set to work_tuple 
            # via self.send_serialized_object(work)
            #logger.debug("tcp_server: synchronize_process_faninNBs_batch: returning work in DAG_exec_state.") 

        #logger.debug("tcp_server: synchronize_process_faninNBs_batch: returning DAG_state %s." % (str(DAG_exec_state)))           
        #self.send_serialized_object(cloudpickle.dumps(DAG_exec_state))

        logger.debug("MessageHandler finished synchronize_process_faninNBs_batch")
        
        # We will assign DAG_exec_state to returned_work so returned_work cannot be None.
        # This is a DAG_executor_state with DAG_exec_state.return_value = work_tuple
        # or DAG_exec_state.return_value = 0
        pickled_returned_work = cloudpickle.dumps(returned_work)
        return pickled_returned_work
        """
#ToDo: make this process faninNBs an async call? So some are sync and some are async,
# depending on whether we can use return value from process faninNBs, e.g.,
# workers do, using threads to simulate lambdas with/without using simulated lambdas to
# store synch objects do/do not, using real lambdas do not, 
        # No return value is sent back to client for async call

#ToDo: what doing here? Is FaninNB starting thread for fanin tsk? No. So will return 
# a value for the calling thread to use to start a new thread. If fanin size > 1
# (always? except when starting a fanout task but then enqueue only 1 thing ) then
# ignore first fanin and return return value of second fanin. So here, we cant just 
# st return value to 0. Somewhere we need:
#    work_tuple = (start_state_fanin_task,return_value)

#    DAG_exec_state.return_value = work_tuple
#    DAG_exec_state.blocking = False 
# vs.
#       DAG_exec_state.return_value = 0
#       DAG_exec_state.blocking = False
        self.send_serialized_object(cloudpickle.dumps(returned_state_ignored))

    def Xsynchronize_process_faninNBs_batch(self,message):
        """
        batch process all faninNBs and for workers their fanouts, if any, are deposited
        into the work queue. One unit of work can be returned if the worker_needs_work,
        which it will if there were no fanouts.

        Key-word arguments:
        -------------------
            message (dict):
                The payload from the AWS Lambda function.
        """
       
        logger.debug("[HANDLER] TCPHandler lambda: synchronize_process_faninNBs_batch() called.")

        # this is a DAG_executor_State with  DAG_exec_state.return_value = work_tuple
        # or DAG_exec_state.return_value = 0
        returned_state = self.invoke_lambda_synchronously(message)

        logger.debug("[HANDLER] TCPHandler lambda: called Lambda at synchronize_process_faninNBs_batch.")
 
        # pickle already done by Lambda? cloudpickle.dumps(state)? If so, just pass pickled state thru to client.
        #if using_Lambda_Function_Simulator:
        #    #returned_state_pickled = cloudpickle.dumps(returned_state)
        #    self.send_serialized_object(returned_state)
        #else:
        #   self.send_serialized_object(returned_state)

        self.send_serialized_object(returned_state)
       
        # return value not assigned
        return 0

    # Not used and not tested. Currently create work queue in 
    # create_all_fanins_and_faninNBs_and_possibly_work_queue. 
    def create_work_queue(self,message):
        """
        create the work queue for workers.

        Key-word arguments:
        -------------------
            message (dict):
                The payload from the AWS Lambda function.
        """
       
        logger.debug("tcp_server_lambda: create_work_queue() called.")

        return_value_ignored = self.invoke_lambda_synchronously(message)

        resp = {
            "op": "ack",
            "op_performed": "create_work_queue"
        }
        #############################
        # Write ACK back to client. #
        #############################
        logger.info("tcp_server_lambda: Sending ACK to client %s for create_work_queue operation." % self.client_address[0])
        resp_encoded = json.dumps(resp).encode('utf-8')
        self.send_serialized_object(resp_encoded)
        logger.info("tcp_server_lambda: Sent ACK of size %d bytes to client %s for create_work_queue operation." % (len(resp_encoded), self.client_address[0]))

        # return value not assigned
        return 0

    def synchronize_sync(self, message = None):
        """
        Synchronous synchronization.

        Key-word arguments:
        -------------------
            message (dict):
                The payload from the AWS Lambda function.
        """
       
        logger.debug("tcp_server_lambda: calling server.synchronize_sync().")

        returned_state = self.invoke_lambda_synchronously(message)

        logger.debug("tcp_server_lambda called Lambda at synchronize_sync")

        """
        Note: We currently only not try-op synch calls if the ops are 
        fanin ops for DAGs as these are always non-blocking. We can 
        also allow fanin ops for non-DAGs too but we havn't yet implemented
        a fanin select object.

        Issue: need to do the equivalent of sending the returned state value:
        tcp_handler.send_serialized_object(cloudpickle.dumps(state))
        where we have the blocking case:
                    if try_return_value == True:   # synchronize op will execute wait so tell client to terminate
                        state.blocking = True
                        state.return_value = None
                    
                        tcp_handler.send_serialized_object(cloudpickle.dumps(state))
        for which we send blocking true with no return result before we make the blocking call to the synch object
        and the non-blocking case:
        [make the call]
                    state.return_value = return_value
                    state.blocking = False

                    tcp_handler.send_serialized_object(cloudpickle.dumps(state))
        for which we send blocking false and the return value
        PROBLEM: For blocking case, we want to send blocking True then do call (that blocks) but if Lambda returns
        blocking is True (via state) then it cannot do the rest.
        Could call Lambda again and have it make thhe blocking call?
        Or just let it make the blocking call and then send blocking true, time to maake call is not the long?
        Or let Lambda call client? No, client is a Lambda which does not allow incoming calls.
        Or only make asynch calls with termination, but restarts take more time than waiting for a blocked call
            and cost of time to make call is vary small?
        """
        # pickle already done by Lambda? cloudpickle.dumps(state)? If so, just pass pickled state thru to client.

        # Note: the value returned is pickled in Message_Handler_Lambda in 
        # synchronize_process_faninNBs_batch and returned by lambda function
        if using_Lambda_Function_Simulator:
            self.send_serialized_object(returned_state)
        else:
            self.send_serialized_object(returned_state)

        # return value not assigned
        return 0

    def synchronize_async(self, message = None):
        """
        Asynchronous synchronization.

        Key-word arguments:
        -------------------
            message (dict):
                The payload from the AWS Lambda function.
        """        
        logger.debug("tcp_server_lambda: calling server.synchronize_async().")

        returned_value_ignored = self.invoke_lambda_synchronously(message)
       
        logger.debug("tcp_server_lambda: called synchronizer.synchronize_async")

        # return value not assigned
        return returned_value_ignored
        
    def recv_object(self):
        """
        Receive an object from a remote entity via the given websocket.

        The TCP server uses a "streaming" API that is implemented using file handles (or rather the API looks like we're just using file handles).
        """
        try:
            # Read the size of the incoming serialized object.
            incoming_size = self.rfile.read(4) 
        except ConnectionAbortedError as ex:
            logger.error("tcp_server_lambda: Established connection aborted while reading incoming size.")
            logger.error(repr(ex))
            return None 

        # Convert bytes of size to integer.
        incoming_size = int.from_bytes(incoming_size, 'big')

        if incoming_size == 0:
            logger.debug("tcp_server_lambda: Incoming size is 0. Client is expected to have disconnected.")
            return None 
        
        if incoming_size < 0:
            logger.error("tcp_server_lambda: Incoming size < 0: " + incoming_size + ". An error might have occurred...")
            return None 

        logger.info("tcp_server_lambda: Will receive another message of size %d bytes" % incoming_size)

        data = bytearray()
        try:
            while len(data) < incoming_size:
                # Read serialized object (now that we know how big it'll be).
                new_data = self.rfile.read(incoming_size - len(data)).strip()

                if not new_data:
                    break 

                data.extend(new_data)
                logger.debug("tcp_server_lambda: Have read %d/%d bytes from remote client." % (len(data), incoming_size))
        except ConnectionAbortedError as ex:
            logger.error("tcp_server_lambda: Established connection aborted while reading data.")
            logger.error(repr(ex))
            return None 
        
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
        logger.debug("Sending payload of size %d bytes to remote client now..." % len(obj))
        self.wfile.write(len(obj).to_bytes(4, byteorder='big'))     # Tell the client how many bytes we're sending.
        self.wfile.write(obj)                                       # Then send the object.
        logger.debug("tcp_server_lambda: Sent %d bytes to remote client." % len(obj))

    def close_all(self, message = None):
        """
        Clear all known synchronizers.
        """
        logger.debug("tcp_server_lambda: Received close_all request.")

        tcp_server.synchronizers = {}

        #############################
        # Write ACK back to client. #
        #############################
        resp = {
            "op": "ack",
            "op_performed": "close_all"
        }        
        logger.info("tcp_server_lambda: Sending ACK to client %s for 'close_all' operation." % self.client_address[0])
        resp_encoded = json.dumps(resp).encode('utf-8')
        self.send_serialized_object(resp_encoded)
        logger.info("tcp_server_lambda: Sent ACK of size %d bytes to client %s for 'close_all' operation." % (len(resp_encoded), self.client_address[0]))          

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

        logger.debug("tcp_server_lambda: Received close_obj request for object with name '%s' and type %s" % (name, type_arg))

        #############################
        # Write ACK back to client. #
        #############################
        resp = {
            "op": "ack",
            "op_performed": "close_obj"
        }        
        logger.info("tcp_server_lambda: Sending ACK to client %s for CLOSE_OBJ operation." % self.client_address[0])
        resp_encoded = json.dumps(resp).encode('utf-8')
        self.send_serialized_object(resp_encoded)
        logger.info("tcp_server_lambda: Sent ACK of size %d bytes to client %s for CLOSE_OBJ operation." % (len(resp_encoded), self.client_address[0]))        

    def setup_server(self, message = None):
        logger.debug("tcp_server_lambda: server.setup() called.")
        pass 
        
class TCPServer(object):
    def __init__(self):
        self.synchronizers =  {}    # dict of name to Synchronizer - not used in Lambda version - list is in Lambda
        self.server_threads = []    # list      - not used
        self.clients =        []    # list      - not used
        self.server_address = ("0.0.0.0",25565)
        self.tcp_server = socketserver.ThreadingTCPServer(self.server_address, TCPHandler)
        self.infiniD = None

        if using_Lambda_Function_Simulator:
            """
            DAG_info = DAG_Info()
            # using regular functions instead of real lambda functions for storing synch objects 
            #self.lambda_function = Lambda_Function_Simulator()
            self.list_of_Lambda_Function_Simulators = []
            self.num_Lambda_Function_Simulators = 1
            for _ in range(0,self.num_Lambda_Function_Simulators):
                self.list_of_Lambda_Function_Simulators.append(Lambda_Function_Simulator())
            
            self.map_of_Lambda_Function_Simulators = {}
            self.map_of_Lambda_Function_Simulators['single_function'] = self.list_of_Lambda_Function_Simulators[0]
            """

            DAG_info = DAG_Info()
            # using regular functions instead of real lambda functions for storing synch objects 
	        # self.lambda_function = Lambda_Function_Simulator()
            self.infiniD = InfiniD(DAG_info)
            # create list of simulator functions, number of functions
            # is the number of fnins + faaninNBs + fanouts
            self.infiniD.create_functions() #
            """
            if use_single_lambda_function:
                # there is a single function that stores all the synchronization objects
                sync_object_name = 'single_function'
                function_index = 0
                self.infiniX.map_synchronization_object(sync_object_name,function_index)
            else:
                # map each fanin/faninNB/fanout name to a func. Currently,
                # one name per function.
                # ToDo: mapping scheme, maps multiple names to one function, e.g.,
                # based on: two fanins/fanouts that can be executed concurrently
                # are mapped to different functions
                self.infiniX.map_object_names_to_functions()
            """
            self.infiniD.map_object_names_to_functions()

            logger.debug("tcp_server_lambda: function map" + str(self.infiniD.function_map))
            # Note: call lambda_function = infiniX.get_function(sync_object_name) to get 
            # the function that stores sync_object_namej

        else:
#ToDo: need ock per lambda function so create_locks() when not using lambda simulator
# use set of names, map, etc.
            self.function_lock = Lock()
    
    def start(self):
        logger.info("tcp_server_lambda: Starting TCP Lambda server.")
        try:
            self.tcp_server.serve_forever()
        except Exception as ex:
            logger.error("tcp_server_lambda: Exception encountered:" + repr(ex))

if __name__ == "__main__":
    # Create a Server Instance
    tcp_server = TCPServer()
    tcp_server.start()