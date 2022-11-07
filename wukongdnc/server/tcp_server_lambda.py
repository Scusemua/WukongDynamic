#from re import A
import json
import traceback
import socketserver
#import traceback
#import json

#import cloudpickle
#import base64

from ..wukong.invoker import invoke_lambda_synchronously

from ..dag.DAG_executor_constants import using_Lambda_Function_Simulator
from ..dag.DAG_Executor_lambda_function_simulator import InfiniX # , Lambda_Function_Simulator
from ..dag.DAG_info import DAG_Info

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

            # for function smulator prototype, using a single function to store all the fanins/faninNBs
            # i.e., all fanin/faninNBs mapped under the name 'single_function'
            #sync_object_name = "single_function"
            sync_object_name = json_message.get("name", None)
            logger.debug("[HANDLER] TCPHandler lambda: invoke_lambda_synchronously: using object_name: " + sync_object_name + " in map_of_Lambda_Function_Simulators")
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
            lambda_function = tcp_server.infiniX.get_function(sync_object_name)
            return_value = lambda_function.lambda_handler(payload)  
        else:     
            # For DAG prototype, we use one function to store process_work_queue and all fanins and faninNBs
            sync_object_name = "LambdaBoundedBuffer" 
            return_value = invoke_lambda_synchronously(function_name = sync_object_name, payload = payload)
            # where: lambda_client.invoke(FunctionName=function_name, InvocationType='RequestResponse', Payload=payload_json)
        
        # The return value from the Lambda function will typically be sent by tcp_server to a Lambda client of tcp_server
        return return_value

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

    def Xcreate_all_fanins_and_faninNBs_and_possibly_work_queue(self, message):
        """
        create all DAG fanins and faninNBs and possibly a work queue (for workers).

        Key-word arguments:
        -------------------
            message (dict):
                The payload from the AWS Lambda function.
        """
       
        logger.debug("[HANDLER] TCPHandler lambda: create_all_fanins_and_faninNBs_and_possibly_work_queue() called.")

        return_value_ignored = self.invoke_lambda_synchronously(message)

        logger.debug("tcp_server called Lambda at create_all_fanins_and_faninNBs_and_possibly_work_queue.")
 
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

    def synchronize_process_faninNBs_batch(self,message):
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

        logger.debug("tcp_server called Lambda at synchronize_process_faninNBs_batch.")
 
        # pickle already done by Lambda? cloudpickle.dumps(state)? If so, just pass pickled state thru to client.
        if using_Lambda_Function_Simulator:
            #returned_state_pickled = cloudpickle.dumps(returned_state)
            self.send_serialized_object(returned_state)
        else:
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
       
        logger.debug("[HANDLER] TCPHandler lambda: create_work_queue() called.")

        return_value_ignored = self.invoke_lambda_synchronously(message)

        resp = {
            "op": "ack",
            "op_performed": "create_work_queue"
        }
        #############################
        # Write ACK back to client. #
        #############################
        logger.info("Sending ACK to client %s for create_work_queue operation." % self.client_address[0])
        resp_encoded = json.dumps(resp).encode('utf-8')
        self.send_serialized_object(resp_encoded)
        logger.info("Sent ACK of size %d bytes to client %s for create_work_queue operation." % (len(resp_encoded), self.client_address[0]))

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
       
        logger.debug("[HANDLER] TCPHandler lambda: calling server.synchronize_sync().")

        returned_state = self.invoke_lambda_synchronously(message)

        logger.debug("tcp_server called Lambda at synchronize_sync")

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
        logger.debug("[HANDLER] TCPHandler lambda: calling server.synchronize_async().")

        returned_value_ignored = self.invoke_lambda_synchronously(message)
       
        logger.debug("tcp_server called synchronizer.synchronize_async")

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
            logger.error("Established connection aborted while reading incoming size.")
            logger.error(repr(ex))
            return None 

        # Convert bytes of size to integer.
        incoming_size = int.from_bytes(incoming_size, 'big')

        if incoming_size == 0:
            logger.debug("Incoming size is 0. Client is expected to have disconnected.")
            return None 
        
        if incoming_size < 0:
            logger.error("Incoming size < 0: " + incoming_size + ". An error might have occurred...")
            return None 

        logger.info("Will receive another message of size %d bytes" % incoming_size)

        data = bytearray()
        try:
            while len(data) < incoming_size:
                # Read serialized object (now that we know how big it'll be).
                new_data = self.rfile.read(incoming_size - len(data)).strip()

                if not new_data:
                    break 

                data.extend(new_data)
                logger.debug("Have read %d/%d bytes from remote client." % (len(data), incoming_size))
        except ConnectionAbortedError as ex:
            logger.error("Established connection aborted while reading data.")
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
        logger.debug("Sent %d bytes to remote client." % len(obj))

    def close_all(self, message = None):
        """
        Clear all known synchronizers.
        """
        logger.debug("Received close_all request.")

        tcp_server.synchronizers = {}

        #############################
        # Write ACK back to client. #
        #############################
        resp = {
            "op": "ack",
            "op_performed": "close_all"
        }        
        logger.info("Sending ACK to client %s for 'close_all' operation." % self.client_address[0])
        resp_encoded = json.dumps(resp).encode('utf-8')
        self.send_serialized_object(resp_encoded)
        logger.info("Sent ACK of size %d bytes to client %s for 'close_all' operation." % (len(resp_encoded), self.client_address[0]))          

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

        logger.debug("Received close_obj request for object with name '%s' and type %s" % (name, type_arg))

        #############################
        # Write ACK back to client. #
        #############################
        resp = {
            "op": "ack",
            "op_performed": "close_obj"
        }        
        logger.info("Sending ACK to client %s for CLOSE_OBJ operation." % self.client_address[0])
        resp_encoded = json.dumps(resp).encode('utf-8')
        self.send_serialized_object(resp_encoded)
        logger.info("Sent ACK of size %d bytes to client %s for CLOSE_OBJ operation." % (len(resp_encoded), self.client_address[0]))        

    def setup_server(self, message = None):
        logger.debug("server.setup() called.")
        pass 
        
class TCPServer(object):
    def __init__(self):
        self.synchronizers =  {}    # dict of name to Synchronizer - not used in Lambda version - list is in Lambda
        self.server_threads = []    # list      - not used
        self.clients =        []    # list      - not used
        self.server_address = ("0.0.0.0",25565)
        self.tcp_server = socketserver.ThreadingTCPServer(self.server_address, TCPHandler)
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
            self.infiniX = InfiniX(DAG_info)
            # create list of simulator functions 
            self.infiniX.create_functions()
            #
            # ToDo: Do this when get the creates going
            self.infiniX.map_object_names_to_functions()
            #

            #No. tcp_server_lambda handles this by calling create for each message?
            #self.infiniX.create_fanin_and_faninNB_messages()

#ToDo: call infiniX.map_object_names_to_functions() to create functions for fanins/faninNBs/fanouts
# but will not use fanouts just yet.
            #
            # Use this when do single lambda
            # map synch_object_name to one of the InfiniX functions
            #sync_object_name = 'single_function'
            #function_index = 0
            #self.infiniX.map_synchronization_object(sync_object_name,function_index)
            #

            logger.debug("function map" + str(self.infiniX.function_map))
            # Note: call lambda_function = infiniX.get_function(sync_object_name) to get 
            # the function that stores sync_object_namej
    
    def start(self):
        logger.info("Starting TCP Lambda server.")
        try:
            self.tcp_server.serve_forever()
        except Exception as ex:
            logger.error("Exception encountered:" + repr(ex))

if __name__ == "__main__":
    # Create a Server Instance
    tcp_server = TCPServer()
    tcp_server.start()