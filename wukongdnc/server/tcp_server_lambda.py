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
import time 
import socket
import socketserver
import threading
import traceback
import json

# from .synchronizer import Synchronizer
from .util import make_json_serializable, decode_and_deserialize, isTry_and_getMethodName, isSelect 
from ..wukong.invoker import invoke_lambda_synchronously

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
            logger.info("[HANDLER] Recieved one request from {}".format(self.client_address[0]))

            self.action_handlers = {
                "create": self.create_obj,
                "setup": self.setup_server,
                "synchronize_async": self.synchronize_async,
                "synchronize_sync": self.synchronize_sync,
                "close_all": self.close_all
            }

            try:
                data = self.recv_object()

                if data is None:
                    logger.warning("recv_object() returned None. Exiting handler now.")
                    return

                json_message = json.loads(data)
                obj_name = json_message['name']
                message_id = json_message["id"]
                logger.debug("[HANDLER] Received message (size=%d bytes) from client %s with ID=%s" % (len(data), self.client_address[0], message_id))
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
        name = json_message.get("name", None)
        
        # For prototype using two Lambdas to store synchronization objects.
        if name == "result" or name == "final_result":
            function_name = "LambdaBoundedBuffer"
        else: # name is "finish"
            function_name = "LambdaSemapore"

        # pass thru client message to Lambda
        payload = {"json_message": json_message}
        # return_value = invoke_lambda_synchronously(payload = payload, function_name = function_name)
        return_value = invoke_lambda_synchronously(function_name = function_name, payload = payload)
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
        logger.debug("[HANDLER] server.create() called.")

        ignored_return_value = self.invoke_lambda_synchronously(message)    # makes synchronous Lambda call - return value is not meaningful

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

    def synchronize_sync(self, message = None):
        """
        Synchronous synchronization.

        Key-word arguments:
        -------------------
            message (dict):
                The payload from the AWS Lambda function.
        """
       
        logger.debug("[HANDLER] server.synchronize_sync() called.")

        returned_state = self.invoke_lambda_synchronously(message)

        logger.debug("tcp_server called Lambda at synchronize_sync")

        """
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
        Could call Lmabda again and have it make thhe blocking call?
        Or just let it make the blocking call and then send blocking true, time to maake call is not the long?
        Or let Lambda call client? No, client is a Lambda which does not allow incoming calls.
        Or only make asynch calls with termination, but restarts take more time than waiting for a blocked call
            and cost of time to make call is vary small?
        """
        # pickle already done by Lambda? cloudpickle.dumps(state)? If so, just pass pickled state thru to client.
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
        logger.debug("[HANDLER] server.synchronize_async() called.")

        returned_value_ignored = self.invoke_lambda_synchronously(message)
       
        logger.debug("tcp_server called synchronizer.synchronize_async")
       
        return returned_value_ignored
        
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
        
class TCPServer(object):
    def __init__(self):
        self.synchronizers =  {}    # dict of name to Synchronizer - not used in Lambda version - list is in Lambda
        self.server_threads = []    # list      - not used
        self.clients =        []    # list      - not used
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

 
