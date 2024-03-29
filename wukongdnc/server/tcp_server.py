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

from .synchronizer import Synchronizer
from .util import make_json_serializable, decode_and_deserialize, isTry_and_getMethodName

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
        """
        TCP handler for incoming requests from AWS Lambda functions.
        """
        while True:
            logger.info("[HANDLER] Recieved one request from {}".format(self.client_address[0]))

            self.action_handlers = {
                "create": self.create_obj,
                "setup": self.setup_server,
                "synchronize_async": self.synchronize_async,
                "synchronize_sync": self.synchronize_sync,
                "close_all": self.close_all
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

        synchronizer_name = self._get_synchronizer_name(obj_type = None, name = obj_name)
        logger.debug("Trying to retrieve existing Synchronizer '%s'" % synchronizer_name)
        synchronizer = tcp_server.synchronizers[synchronizer_name]

        base_name, isTryMethod = isTry_and_getMethodName(method_name)
    
        logger.debug("method_name: " + method_name + ", base_name: " + base_name + ", isTryMethod: " + str(isTryMethod))
        #logger.debug("base_name: " + base_name)
        #logger.debug("isTryMethod: " + str(isTryMethod))
        
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
            try_return_value = synchronizer.trySynchronize(method_name, state, **state.keyword_arguments)

            logger.debug("Value of try_return_value for fan-in ID %s: %s" % (obj_name, str(try_return_value)))
        
            if try_return_value == True:   # synchronize op will execute wait so tell client to terminate
                state.blocking = True 
                state.return_value = None 
                self.send_serialized_object(cloudpickle.dumps(state))
                
                # execute synchronize op but don't send result to client
                return_value = synchronizer.synchronize(base_name, state, **state.keyword_arguments)

                logger.debug("Value of return_value (not to be sent) for fan-in ID %s: %s" % (obj_name, str(return_value)))
            else:
                # execute synchronize op and send result to client
                return_value = synchronizer.synchronize(base_name, state, **state.keyword_arguments)
                state.return_value = return_value
                state.blocking = False 
                logger.debug("Synchronizer %s sending %s back to last executor." % (synchronizer_name, str(return_value)))
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
        try:
            # Read the size of the incoming serialized object.
            incoming_size = self.rfile.read(2) 
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
        self.wfile.write(len(obj).to_bytes(2, byteorder='big'))     # Tell the client how many bytes we're sending.
        self.wfile.write(obj)                                       # Then send the object.
        logger.debug("Sent %d bytes to remote client." % len(obj))

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
        state = decode_and_deserialize(message["state"])

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
        self.synchronizers =  {}    # dict 
        self.server_threads = []    # list
        self.clients =        []    # list
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