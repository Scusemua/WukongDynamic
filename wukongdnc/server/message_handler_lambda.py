import traceback

from .synchronizer_lambda import Synchronizer
from .util import decode_and_deserialize #, make_json_serializable,  isTry_and_getMethodName, isSelect 
from ..dag.DAG_executor_constants import FanIn_Type, FanInNB_Type
from ..dag.DAG_executor_constants import create_all_fanins_faninNBs_on_start
from .util import decode_and_deserialize, make_json_serializable
import uuid
import os
import time

# Set up logging.
import logging 
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(asctime)s] [%(threadName)s] %(levelname)s: %(message)s')

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)

logger.addHandler(ch)

class MessageHandler(object):
    synchronizers =  {} 
        
    def handle(self,json_message):

        #Lambda message  handler for incoming requests from AWS Lambda functions.
        
        while True:
            # Q: Should we pass self.client_address[0] to LambdaBB aand LambdaSem? This is just the client's IP: 127.0.0.1?
            # logger.info("[MessageHandler] Recieved one request from {}".format(self.client_address[0]))

            self.action_handlers = {
                "create": self.create_obj,
                "setup": self.setup_server,
                "synchronize_async": self.synchronize_async,
                "synchronize_sync": self.synchronize_sync,
                "close_all": self.close_all,
                # These are DAG execution operations
                "create_all_fanins_and_faninNBs_and_possibly_work_queue": self.create_all_fanins_and_faninNBs_and_possibly_work_queue,
                # moved to tcpserver_lambda
                #"synchronize_process_faninNBs_batch": self.synchronize_process_faninNBs_batch,
                "create_work_queue": self.create_work_queue,
                "process_enqueued_fan_ins": self.process_enqueued_fan_ins
            }
            #logger.info("Thread Name:{}".format(threading.current_thread().name))

            try:    
                message_id = json_message["id"]
                action = json_message.get("op", None)
                logger.debug("[MessageHandler] Handling message from client with ID=%s, operation=%s" % (message_id, action))
                
                return_value = self.action_handlers[action](message = json_message)
            except ConnectionResetError as ex:
                logger.error(ex)
                logger.error(traceback.format_exc())
                return_value = {
                    "msg": "ConnectionResetError encountered while executing the Lambda function.",
                    "error_msg": str(ex)
                }
            except Exception as ex:
                logger.error(ex)
                logger.error(traceback.format_exc())
                return_value = {
                    "msg": str(type(ex)) + " encountered while executing the Lambda function.",
                    "error_msg": str(ex)
                }
                
            # this is return value of Lambda, sent back to tcp_server method that invoked Lambda - create, 
            # synchronize_async, synchronize_async.
            return return_value
                
    def _get_synchronizer_name(self, type_name = None, name = None):
        """
        Return the key of a synchronizer object. 

        The key is a string of the form <type>-<name>.
        """
        return str(name) # return str(type_name + "_" + name)

    def create_obj(self,message = None):
        """
        Called by a remote Lambda to create an object here on the TCP server.

        Key-word arguments:
        -------------------
            message (dict):
                The payload from the AWS Lambda function.
        """        
        logger.debug("[MESSAGEHANDLER] create() called.")
        type_arg = message["type"]
        obj_name = message["name"]
        state = decode_and_deserialize(message["state"])

        synchronizer = Synchronizer()
        synchronizer.create(type_arg, obj_name, **state.keyword_arguments)
        synchronizer_name = self._get_synchronizer_name(type_name = type_arg, name = obj_name)
        logger.debug("MessageHandler create caching new Synchronizer of type '%s' with name '%s'" % (type_arg, synchronizer_name))
        MessageHandler.synchronizers[synchronizer_name] = synchronizer # Store Synchronizer object.

        # return to handle()
        # tcp_server.create will ignore this return value and send a response to client indicating create is complete.
        return 0

    def create_one_of_all_objs(self,message = None):
        """
        Called by create_all_fanins_and_faninNBs_and_possibly_work_queue and create_work_queue to 
        create an object here on the TCP server. No ack is sent to a client. 
        create_all_fanins_and_faninNBs_and_possibly_work_queue and create_work_queue will
        return to tco_server_lambda, which will send the ack.

        Key-word arguments:
        -------------------
            message (dict):
                The payload from the AWS Lambda function.
        """        
        logger.debug("[MESSAGEHANDLER] server.create_one_of_all_objs() called.")
        type_arg = message["type"]
        name = message["name"]
        state = decode_and_deserialize(message["state"])

        synchronizer = Synchronizer()
        synchronizer.create(type_arg, name, **state.keyword_arguments)
        synchronizer_name = self._get_synchronizer_name(type_name = type_arg, name = name)
        logger.debug("Caching new Synchronizer of type '%s' with name '%s'" % (type_arg, synchronizer_name))
        MessageHandler.synchronizers[synchronizer_name] = synchronizer # Store Synchronizer object.

        # Do not send ack to client - this is just one of possibly many of the creates from create_all_fanins_and_faninNBs

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
                "name": messages,						# Q: Change this? usually it's a synch object name (string)
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
            self.create_one_of_all_objs(msg)
        if len(fanin_messages) > 0:
            logger.info("created fanins")

        for msg in faninNB_messages:
            self.create_one_of_all_objs(msg)
        if len(faninNB_messages) > 0:
            logger.info("created faninNBs")

        # we always create the fanin and faninNBs. We possibly create the work queue. If we send
        # a message for create work queue, in addition to the lst of messages for create
        # fanins and create faninNBs, we create a work queue too.
        create_the_work_queue = (len(messages)>2)
        if create_the_work_queue:
            logger.info("create_the_work_queue: " + str(create_the_work_queue) + " len: " + str(len(messages)))
            msg = messages[2]
            self.create_one_of_all_objs(msg)

        # return to handle()
        # tcp_server.create will ignore this return value and send a response to client indicating create is complete.
        return 0

    def process_enqueued_fan_ins(self,message=None):
        """
        process the enqueued fan_ins sent by an SQS trigger

        Key-word arguments:
        -------------------
            message (dict):
                The payload from the AWS Lambda function.
        where

            message = {
                "op": "process_enqueued_fan_ins",
                "type": "DAG_executor_fanin_or_faninNB",
                "name": list, # list of fan_in messages to be processed
                "state": make_json_serializable(dummy_state),
                "id": msg_id
            }

        The fan_in messaages were created by tcp_server_lambdas synchronize_process_faninNBs_batch()
        where each message is:
            for name in faninNBs:
                ...
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

        """

        list_of_messages = message['name']
        #assert:
        if len(list_of_messages) == 0:
            logger.error("[Error]: Internal error: process_enqueued_fan_ins: "
                + " length of list_of_messages is 0 but fanin size > 0.")

        if not create_all_fanins_faninNBs_on_start:
#rhc: states is jsoned? need to extract state?
            dummy_state = decode_and_deserialize(message["state"])
            fanin_name = dummy_state.keyword_arguments['fanin_name']
            is_fanin = dummy_state.keyword_arguments['is_fanin']
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
            logger.debug("message_handler_lambda: process_enqueued_fan_ins: "
            + "create sync object " + fanin_name + "on the fly")
            self.create_obj(creation_message)

            logger.debug("message_handler_lambda: process_enqueued_fan_ins: process list of messages ")

        for msg in list_of_messages:
            # We are doing all the fan_in ops one-by-one in the order they were called by clients
            # The return value of last call is the fanin results; return those to client
            return_value = self.synchronize_sync(msg)
        return return_value

    # Not used and not tested. Currently create work queue in 
    # create_all_fanins_and_faninNBs_and_possibly_work_queue. 
    def create_work_queue(self, message = None):
        # used to create only a work queue. This is the case when we are creating the fanins and faninNBs
        # on the fly, i.e., not at the beginning of execution.
        self.create_one_of_all_objs(message)

        # return to handle()
        # tcp_server.create will ignore this return value and send a response to client indicating create is complete.
        return 0

    def synchronize_sync(self, message = None):
        """
        Synchronous synchronization.

        Key-word arguments:
        -------------------
            message (dict):
                The payload from the AWS Lambda function.
        """

        logger.debug("[MESSAGEHANDLER] synchronize_sync() called.")

#rhc: ToDo (for async too)
        """
        Need to check create object on fly. If so then use the 
        createinfo_message to get the create() info and create
        the message. Then grab the op message from "name" and 
        finish.

        Consider changing parm to "createinfo_and_message"

        if not create_all_fanins_faninNBs_on_start:
            dummy_state = decode_and_deserialize(createinfo_and_message["state"])
            fanin_name = dummy_state.keyword_arguments['fanin_name']
            is_fanin = dummy_state.keyword_arguments['is_fanin']
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
            logger.debug("message_handler_lambda: process_enqueued_fan_ins: "
            + "create sync object " + fanin_name + "on the fly")
            self.create_obj(creation_message)

        message = createinfo_and_message['name']
        """
        
        obj_name = message['name']
        method_name = message['method_name']
        state = decode_and_deserialize(message["state"])

        # not using synchronizer class name in object name for now, i.e., use "bb" instead of "BoundedBuffer_bb"
        # type_arg = message["type"]
        # synchronizer_name = self._get_synchronizer_name(type_name = type_arg, name = obj_name)
        synchronizer_name = self._get_synchronizer_name(type_name = None, name = obj_name)
        
        logger.debug("MessageHandler: synchronize_sync: Trying to retrieve existing Synchronizer '%s'" % synchronizer_name)
        synchronizer = MessageHandler.synchronizers[synchronizer_name]
        
        if (synchronizer is None):
            raise ValueError("MessageHandler: synchronize_sync: Could not find existing Synchronizer with name '%s'" % synchronizer_name)
         
        # return_value = synchronizer.synchronize_sync(tcp_server, obj_name, method_name, type_arg, state, synchronizer_name)
        # return_value = synchronizer.synchronize_sync(tcp_server, obj_name, method_name, state, synchronizer_name, self)
        # MessageHandler not passing itself since synchronizer does not use send_serialized_object to send results 
        # to tcp_server - Lambda returns values synchronously.
        #return_value = synchronizer.synchronize_sync(obj_name, method_name, state, synchronizer_name, self)

        return_value = synchronizer.synchronize_sync(obj_name, method_name, state, synchronizer_name)
        
        logger.debug("MessageHandler called synchronizer.synchronize_sync")

        return return_value

    def synchronize_async(self, message = None):
        """
        Asynchronous synchronization.

        Key-word arguments:
        -------------------
            message (dict):
                The payload from the AWS Lambda function.
        """     

#rhc: ToDo: Same as for sync
   
        logger.debug("[MESSAGEHANDLER] synchronize_async() called.")
        obj_name = message['name']
        method_name = message['method_name']       
        state = decode_and_deserialize(message["state"])

        # not using synchronizer class name in object name for now, i.e., use "bb" instead of "BoundedBuffer_bb"
        # type_arg = message["type"]
        # synchronizer_name = self._get_synchronizer_name(type_name = type_arg, name = obj_name)    
        synchronizer_name = self._get_synchronizer_name(type_name = None, name = obj_name)
        logger.debug("MessageHandler: synchronize_async: Trying to retrieve existing Synchronizer '%s'" % synchronizer_name)
        synchronizer = MessageHandler.synchronizers[synchronizer_name]
        
        if (synchronizer is None):
            raise ValueError("MessageHandler: synchronize_async: Could not find existing Synchronizer with name '%s'" % synchronizer_name)
        
        logger.debug("MessageHandler: synchronize_async: Successfully found synchronizer")

        # return_value = synchronizer.synchronize_async(obj_name, method_name, type_arg, state, synchronizer_name)
        return_value = synchronizer.synchronize_async(obj_name, method_name, state, synchronizer_name)
        
        logger.debug("MessageHandler called synchronizer.synchronize_async")
        
        return return_value    

    def close_all(self, message = None):
        """
        Clear all known synchronizers.
        """
        logger.debug("MessageHandler: close_all: Received close_all request.")

        MessageHandler.synchronizers = {}

        # return to handle()
        # tcp_server.close_all will ignore this return value and send a response to client indicating create is complete.
        return 0    
    
    # Currently does not close any object - relying on close_all
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

        # return to handle()
        # tcp_server.close_obj will ignore this return value and send a response to client indicating create is complete.
        return 0
        
    def setup_server(self, message = None):
        logger.debug("server.setup() called.")
        pass 