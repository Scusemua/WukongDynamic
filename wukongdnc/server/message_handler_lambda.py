from multiprocessing import synchronize
from re import A
import traceback

from .synchronizer_lambda import Synchronizer
from .util import make_json_serializable, decode_and_deserialize, isTry_and_getMethodName, isSelect 

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
                "close_all": self.close_all
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

    def synchronize_sync(self, message = None):
        """
        Synchronous synchronization.

        Key-word arguments:
        -------------------
            message (dict):
                The payload from the AWS Lambda function.
        """
        
        logger.debug("[MESSAGEHANDLER] synchronize_sync() called.")
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
        state = decode_and_deserialize(message["state"])

        logger.debug("Received close_obj request for object with name '%s' and type %s" % (name, type_arg))

        # return to handle()
        # tcp_server.close_obj will ignore this return value and send a response to client indicating create is complete.
        return 0
        
    def setup_server(self, message = None):
        logger.debug("server.setup() called.")
        pass 