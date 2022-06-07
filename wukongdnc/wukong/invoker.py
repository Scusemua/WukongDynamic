# This class has methods for invoking AWS Lambda functions.

import boto3 
import cloudpickle
import json
import base64
import uuid
import sys 
import time 
import socket 

from ..constants import TCP_SERVER_IP
from ..server.state import State
from ..server.api import create

import logging 
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(asctime)s] [%(threadName)s] %(levelname)s: %(message)s')

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)

logger.addHandler(ch)

logger.propagate = False

lambda_client = boto3.client('lambda', region_name = "us-east-1")

def invoke_lambda(
    function_name: str = "ComposerServerlessSync",
    payload: dict = None,
    is_first_invocation: bool = False,
    n : int = 1,
    initial_permits: int = 0
):
    """
    Invoke an AWS Lambda function.

    Arguments:
    ----------
        function_name (str):
            Name of the AWS Lambda function to invoke.
        
        payload (dict):
            Dictionary to be serialized and sent via the AWS Lambda invocation payload.
            This is typically expected to contain a "state" entry with a state object.
            The only time it wouldn't is at the very beginning of the program, in which
            case we automatically create the first State object.
        
        is_first_invocation (bool):
            If True, we create the State object and put it in the payload. 
            We also call CREATE() on the TCP Server.
            This is ONLY passed (as True) by a client. Lambda functions would never pass this as 'true'.
        
        n (int):
            The 'n' keyword argument to include in the State object we create.
            This is only used when `is_first_invocation` is set to True.
    """
    logger.debug("Creating AWS Lambda invocation payload for function '%s'" % function_name)
    s = time.time()
    _payload = {}
    for k,v in payload.items():
        _payload[k] = base64.b64encode(cloudpickle.dumps(v)).decode('utf-8')
    
    if is_first_invocation:
        state = State(
            function_name = function_name,
            function_instance_ID = str(uuid.uuid4()),
            restart = False,
            pc = 0,
            return_value = None,
            blocking = False,
            i = int(0),
            ID = None,
            keyword_arguments = {
                'n': n,
                'initial_permits': initial_permits
            }
        )
        _payload["state"] = base64.b64encode(cloudpickle.dumps(state)).decode('utf-8')

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as websocket:
            logger.debug("Connecting to TCP Server at %s." % str(TCP_SERVER_IP))
            websocket.connect(TCP_SERVER_IP)
            logger.debug("Successfully connected to TCP Server at %s. Calling executor.create() now...")
            create(websocket, "create", "BoundedBuffer", "result", state)
            create(websocket, "create", "CountingSemaphore_Monitor", "finish", state)
            create(websocket, "create", "BoundedBuffer", "final_result", state)
    
    payload_json = json.dumps(_payload)
    
    logger.debug("Finished creating AWS Lambda invocation payload in %f ms." % ((time.time() - s) * 1000.0))
    
    logger.info("Invoking AWS Lambda function '" + function_name + "' with payload containing " + str(len(payload)) + " key(s).")
    s = time.time()
    #lambda_invocation_payload_serialized = cloudpickle.dumps()
    status_code = lambda_client.invoke(
        FunctionName = function_name, 
        InvocationType = 'Event',
        Payload = payload_json) #json.dumps(_payload))
    logger.info("Invoked AWS Lambda function '%s' in %f ms. Status: %s." % (function_name, (time.time() - s) * 1000.0, str(status_code)))