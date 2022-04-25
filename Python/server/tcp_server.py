import asyncio
from multiprocessing import synchronize
from re import A
import json
import websockets
import cloudpickle 
import _thread
import base64 
import threading
import traceback
import sys
import socket
import socketserver
import threading
import traceback
import json 

from synchronizer import Synchronizer
from util import make_json_serializable, decode_and_deserialize

# Set up logging.
import logging 
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s')

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)

logger.addHandler(ch)

def isTry_and_getMethodName(name):
    if name.startswith("try_"):
        return name[4:], True
    return name, False

class TCPHandler(socketserver.StreamRequestHandler):
    def handle(self):
        """
        TCP handler for incoming requests from AWS Lambda functions.
        """
        while True:
            logger.info("[HANDLER] Recieved one request from {}".format(self.client_address[0]))

            self.action_handlers = {
                "create": self.create_obj,
                "setup": self.setup_server,
                "synchronize_async": self.synchronize_async,
                "synchronize_sync": self.synchronize_sync
            }
            #logger.info("Thread Name:{}".format(threading.current_thread().name))

            try:
                data = self.recv_object()

                if data is None:
                    logger.warning("recv_object() returned None. Exiting handler now.")
                    return 

                json_message = json.loads(data)
                message_id = json_message["id"]
                logger.debug("[HANDLER] Received message (size=%d bytes) from client %s with ID=%s" % (len(data), self.client_address[0], message_id))
                action = json_message.get("op", None)
                self.action_handlers[action](message = json_message)
            except Exception as ex:
                logger.error(ex)
                logger.error(traceback.format_exc())

    def _get_synchronizer_name(self, obj_type = None, name = None):
        """
        Return the key of a synchronizer object. 

        The key is a string of the form <type>-<name>.
        """
        return str(name) 

    def synchronize_sync(self, message = None):
        """
        Synchronous synchronization.

        Key-word arguments:
        -------------------
            message (dict):
                The payload from the AWS Lambda function.
        """
        logger.debug("[HANDLER] server.synchronize_sync() called.")
        obj_name = message['name']
        method_name = message['method_name']
        state = decode_and_deserialize(message["state"])
        function_name = state.id

        synchronizer_name = self._get_synchronizer_name(obj_type = None, name = obj_name)
        logger.debug("Trying to retrieve existing Synchronizer '%s'" % synchronizer_name)
        synchronizer = tcp_server.synchronizers[synchronizer_name]

        base_name, isTryMethod = isTry_and_getMethodName(method_name)
    
        logger.debug("method_name: " + method_name)
        logger.debug("base_name: " + base_name)
        logger.debug("isTryMethod: " + str(isTryMethod))    
        
        # COMMENTED OUT:
        # The TCP server does not have a '_synchClass' variable, so that causes an error to be thrown.
        # We aren't even using the `_synchronizer_method` variable anywhere though, so I've just
        # commented this out. I don't think we need it?
        
        # try:
        #     _synchronizer_method = getattr(self._synchClass, method_name)
        # except Exception as x:
        #     logger.debug("Caught Error >>> %s" % x)

        if isTryMethod: 
            # check if synchronize op will block, if yes tell client to terminate then call op
            # rhc: FIX THIS here and in CREATE: let 
            return_value =  synchronizer.trySynchronize(method_name, state, **state.keyword_arguments)
        
            if return_value == True:   # synchronize op will execute wait so tell client to terminate
                state.blocking = True 
                self.send_serialized_object(cloudpickle.dumps(state))
                
                # execute synchronize op but don't send result to client
                return_value = synchronizer.synchronize(base_name, state, **state.keyword_arguments)
            else:
                # execute synchronize op and send result to client
                return_value = synchronizer.synchronize(base_name, state, **state.keyword_arguments)
                state.return_value = return_value
                state.blocking = False 
                # send tuple to be consistent, and False to be consistent, i.e., get result if False
                self.send_serialized_object(cloudpickle.dumps(state))               
        else:  # not a "try" so do synchronization op and send result to waiting client
            # rhc: FIX THIS here and in CREATE
            return_value = synchronizer.synchronize(method_name, state, **state.keyword_arguments)
                
            state.return_value = return_value
            state.blocking = False 
            # send tuple to be consistent, and False to be consistent, i.e., get result if False
            
            self.send_serialized_object(cloudpickle.dumps(state))

    def recv_object(self):
        """
        Receive an object from a remote entity via the given websocket.

        The TCP server uses a "streaming" API that is implemented using file handles (or rather the API looks like we're just using file handles).
        """
        # Read the size of the incoming serialized object.
        incoming_size = self.rfile.read(2) 
        # Convert bytes of size to integer.
        incoming_size = int.from_bytes(incoming_size, 'big')

        if incoming_size == 0:
            logger.debug("Incoming size is 0. Client is expected to have disconnected.")
            return None 
        
        if incoming_size < 0:
            logger.error("Incoming size < 0: " + incoming_size + ". An error might have occurred...")
            return None 

        logger.info("Will receive another message of size %d bytes" % incoming_size)
        # Read serialized object (now that we know how big it'll be).
        data = self.rfile.read(incoming_size).strip()

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
        self.wfile.write(len(obj).to_bytes(2, byteorder='big'))     # Tell the client how many bytes we're sending.
        self.wfile.write(obj)                                       # Then send the object.

    def create_obj(self, message = None):
        """
        Called by a remote Lambda to create an object here on the TCP server.

        Key-word arguments:
        -------------------
            message (dict):
                The payload from the AWS Lambda function.
        """        
        logger.debug("[HANDLER] server.create() called.")
        type_arg = message["type"]
        name = message["name"]
        state = decode_and_deserialize(message["state"])

        synchronizer = Synchronizer()

        synchronizer.create(type_arg, name, **state.keyword_arguments)
        
        synchronizer_name = self._get_synchronizer_name(obj_type = type_arg, name = name)
        logger.debug("Caching new Synchronizer of type '%s' with name '%s'" % (type_arg, synchronizer_name))
        tcp_server.synchronizers[synchronizer_name] = synchronizer # Store Synchronizer object.

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

    def setup_server(self, message = None):
        logger.debug("server.setup() called.")
        pass 
    
    def synchronize_async(self, message = None):
        """
        Asynchronous synchronization.

        Key-word arguments:
        -------------------
            message (dict):
                The payload from the AWS Lambda function.
        """        
        logger.debug("[HANDLER] server.synchronize_async() called.")
        obj_name = message['name']
        method_name = message['method_name']
        state = decode_and_deserialize(message["state"])

        synchronizer_name = self._get_synchronizer_name(obj_type = None, name = obj_name)
        logger.debug("Trying to retrieve existing Synchronizer '%s'" % synchronizer_name)
        synchronizer = tcp_server.synchronizers[synchronizer_name]
        
        if (synchronizer is None):
            raise ValueError("Could not find existing Synchronizer with name '%s'" % synchronizer_name)
        
        logger.debug("Successfully found synchronizer")
        
        sync_ret_val = synchronizer.synchronize(method_name, state, **state.keyword_arguments)
        
        logger.debug("Synchronize returned: %s" % str(sync_ret_val))

class TCPServer(object):
    def __init__(self):
        self.synchronizers = dict() 
        self.server_threads = []
        self.clients = []
        self.server_address = ("0.0.0.0",25565)
        self.tcp_server = socketserver.ThreadingTCPServer(self.server_address, TCPHandler)
    
    def start(self):
        logger.info("Starting TCP server.")
        try:
            self.tcp_server.serve_forever()
        except Exception as ex:
            logger.error("Exception encountered:" + repr(ex))

if __name__ == "__main__":
    # Create a Server Instance
    tcp_server = TCPServer()
    tcp_server.start()