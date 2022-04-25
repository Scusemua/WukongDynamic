# This class has methods for invoking AWS Lambda functions.

import boto3 
import cloudpickle
import json
import base64
import uuid
import sys 
import time 

sys.path.append("..")

from server.state import State

import logging 
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s')

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)

logger.addHandler(ch)

lambda_client = boto3.client('lambda', region_name = "us-east-1")

def invoke_lambda(
    function_name: str = "WukongDivideAndConquer",
    payload: dict = None,
    is_first_invocation: bool = False
):
    """
    Invoke an AWS Lambda function.

    Arguments:
    ----------
        function_name (str):
            Name of the AWS Lambda function to invoke.
        
        payload (dict):
            Dictionary to be serialized and sent via the AWS Lambda invocation payload.
        
        is_first_invocation (bool):
            If True, we create the State object and put it in the payload.
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
            restart = False
        )
        _payload["state"] = base64.b64encode(cloudpickle.dumps(state)).decode('utf-8')
    
    payload_json = json.dumps(_payload)
    
    logger.debug("Finished creating AWS Lambda invocation payload in %f ms." % ((time.time() - s) * 1000.0))
    
    ###########################################################################
    # CREATE() could be called here if we wanted it to be in the client/user. #
    ###########################################################################
    
    logger.info("Invoking AWS Lambda function '" + function_name + "' with payload containing " + str(len(payload)) + " key(s).")
    s = time.time()
    #lambda_invocation_payload_serialized = cloudpickle.dumps()
    status_code = lambda_client.invoke(
        FunctionName = function_name, 
        InvocationType = 'Event',
        Payload = payload_json) #json.dumps(_payload))
    logger.info("Invoked AWS Lambda function '%s' in %f ms. Status: %s." % (function_name, (time.time() - s) * 1000.0, str(status_code)))