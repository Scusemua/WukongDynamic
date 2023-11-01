# In composer_driver.py, invoke is:
#   invoke_lambda(payload = payload, is_first_invocation = True, n = 1, initial_permits = 0, function_name = "ComposerServerlessSync")
# use either "ComposerServerlessSync" or "Composer_select"

# This class has methods for invoking AWS Lambda functions.

import boto3 
import cloudpickle
import json
import base64
import uuid
import time 
import socket 

from ..constants import TCP_SERVER_IP
from ..server.state import State
from ..server.api import create

import logging 
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(asctime)s] [%(module)s] [%(processName)s] [%(threadName)s]: %(message)s')
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)
logger.addHandler(ch)


lambda_client = boto3.client('lambda', region_name = "us-east-1")

# TRUE if we are testing the real lambd code by short-circuiting the 
# call to start a real lambda on AWS Lambda. i.e., the 
# invoke_lambda_DAG_executor called by the DAG app does not call
#status_code = lambda_client.invoke(
#   FunctionName = function_name, 
#   InvocationType = 'Event',
#   Payload = payload_json) 
#
# which will invoke a real AWS Lambda and call lambda_handler();
# instead, it makes a direct call to lambda_handler
#   status_code = -1
#   lambda_handler(payload_json,None)
# So the actal code for real Lambdas will be executed but if A 
# invokes lambda L, then A will be running L's code. 
# Note: That L will terminate by doing a return, which will return
# to A allowing A to continue (!!) It is important that L, and Lambdas
# in general do not block/wait so L will terminte and return to A.
TEST = True

# Used to invoke a Lambda "storage function" to store and execute synchronization objects.
# Lambda function names are "LambdaBoundedBuffer" and "LambdaSemapore"
def invoke_lambda_synchronously(function_name: str = None, payload: dict = None):
    """
    Invoke an AWS Lambda function synchronously

    Arguments:
    ----------
        function_name (str):
            Name of the AWS Lambda function to invoke.
        
        payload (dict):
            Dictionary to be serialized and sent via the AWS Lambda invocation payload.
            This is typically expected to be a message from a Client.
        
    """
    logger.trace("Creating AWS Lambda invocation payload for function '%s'" % function_name)
    logger.trace("Provided payload: " + str(payload))
    s = time.time()

    # The `_payload` variable is the one I actually pass to AWS Lambda.
    # The `payload` variable is passed by the user to `invoke_lambda`.
    # For each key-value pair in `payload`, we create a corresponding 
    # entry in `_payload`. The key is the same. But we first pickle
    # the value via cloudpickle.dumps(). This returns a `bytes` object.
    # AWS Lambda uses JSON encoding to pass invocation payloads to Lambda
    # functions, and JSON doesn't support bytes. So, we convert the bytes 
    # to a string by encoding the bytes in base64 via base64.b64encode().
    # There is ONE more step, however. base64.b64encode() returns a UTF-8-encoded
    # string, which is also bytes. So, we call .decode('utf-8') to convert it
    # to a regular python string, which is stored as the value in `_payload[k]`, where
    # k is the key.
    _payload = {}
    for k,v in payload.items():
        _payload[k] = base64.b64encode(cloudpickle.dumps(v)).decode('utf-8')
        
    # We must convert `_payload` to JSON before passing it to the lambda_client.invoke() function.
    payload_json = json.dumps(_payload)
    
    logger.trace("Finished creating AWS Lambda invocation payload in %f ms." % ((time.time() - s) * 1000.0))
    
    logger.trace("Invoking AWS Lambda function synchronously'" + function_name + "' with payload containing " + str(len(payload)) + " key(s).")
    s = time.time()
    
    """ Current asynch invocation in invoker.py:
    # This is the call to the AWS API that actually invokes the Lambda.
    status_code = lambda_client.invoke(
        FunctionName = function_name, 
        InvocationType = 'Event',
        Payload = payload_json)
    """        
    
    #Perhaps something like the following. I don't now how to access the retruned value.   
    #return_value_payload = lambda_client.invoke(FunctionName=function_name, InvocationType='RequestResponse', Payload=payload_json)
    return_value_payload = lambda_client.lambda_handler(FunctionName=function_name, InvocationType='RequestResponse', Payload=payload_json)
    #lambda_handler(payload_json,None)
    return_value = return_value_payload['Payload'].read()
    
    # Added substituted "return_value" for "status_code" here
    logger.trace("Invoked AWS Lambda function '%s' in %f ms. return_value: %s." % (function_name, (time.time() - s) * 1000.0, str(return_value)))

    return return_value

# TODO: Make this `invoke_lambda_async`
# Used for Composer program
def invoke_lambda(
    function_name: str = "ComposerServerlessSync", # Can change to ComposerServerlessSync_Select to create different types of synchronization 
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
    logger.trace("Creating AWS Lambda invocation payload for function '%s'" % function_name)
    logger.trace("Provided payload: " + str(payload))
    s = time.time()

    # The `_payload` variable is the one I actually pass to AWS Lambda.
    # The `payload` variable is passed by the user to `invoke_lambda`.
    # For each key-value pair in `payload`, we create a corresponding 
    # entry in `_payload`. The key is the same. But we first pickle
    # the value via cloudpickle.dumps(). This returns a `bytes` object.
    # AWS Lambda uses JSON encoding to pass invocation payloads to Lambda
    # functions, and JSON doesn't support bytes. So, we convert the bytes 
    # to a string by encoding the bytes in base64 via base64.b64encode().
    # There is ONE more step, however. base64.b64encode() returns a UTF-8-encoded
    # string, which is also bytes. So, we call .decode('utf-8') to convert it
    # to a regular python string, which is stored as the value in `_payload[k]`, where
    # k is the key.
    _payload = {}
    for k,v in payload.items():
        _payload[k] = base64.b64encode(cloudpickle.dumps(v)).decode('utf-8')
    
    # If this is the first invocation, we create a new State object.
    if is_first_invocation:
        logger.trace("is_first_invocation is TRUE in `invoke_lambda()`")
        state = State(
            function_name = "Composer",  # this is name of Lambda function
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
            logger.trace("Connecting to TCP Server at %s." % str(TCP_SERVER_IP))
            websocket.connect(TCP_SERVER_IP)
            logger.trace("Successfully connected to TCP Server at %s. Calling executor.create() now...")
            # if False: # not isSelect(function_name):
            #     create(websocket, "create", "BoundedBuffer", "result", state)
            #     create(websocket, "create", "CountingSemaphore_Monitor", "finish", state)
            #     create(websocket, "create", "BoundedBuffer", "final_result", state)
            # else:
            create(websocket, "create", "BoundedBuffer_Select", "result", state)
            create(websocket, "create", "CountingSemaphore_Monitor_Select", "finish", state)
            create(websocket, "create", "BoundedBuffer_Select", "final_result", state)
            
    # We must convert `_payload` to JSON before passing it to the lambda_client.invoke() function.
    payload_json = json.dumps(_payload)
    
    logger.trace("Finished creating AWS Lambda invocation payload in %f ms." % ((time.time() - s) * 1000.0))
    
    logger.trace("Invoking AWS Lambda function '" + function_name + "' with payload containing " + str(len(payload)) + " key(s).")
    s = time.time()
    
    # This is the call to the AWS API that actually invokes the Lambda.
    status_code = lambda_client.invoke(
        FunctionName = function_name, 
        InvocationType = 'Event',
        Payload = payload_json) 
    logger.trace("Invoked AWS Lambd1a function '%s' in %f ms. Status: %s." % (function_name, (time.time() - s) * 1000.0, str(status_code)))

# used by DAG_excutor and the fanins/fanout synch objects for executing DAGs
# Wukong style.
# This will call lambda_client.invoke() in ASW Lambda which call 
# lambda_handler() in WukongDynamic.
# Note: If we set TEST to True then we do not call lambda_client.invoke();
# instead we calk the lambda_handler() defined locally below. This
# lambda_handler() runs DAG_executor_lambda() whchi performs the work loop,
# executng tasks and their fanins/fanouts.
# This allows us to run the code (work loop) locally that real Lambdas wil run. 
# Note that instead of invoking a new (seperate) real Lambda, the invoker will directly call
# the local lambda_handler() which will call DAG_excutor_lambda() running on
# the invoker's thread. When the DAG_executor_lambda()
# code returns (after possibly calling and retuning from more calls to 
# DAG_executor_lambda) it returns to the lambda_handler() which returns
# back to the invoker's invoke_lambda_DAG_executor.
def invoke_lambda_DAG_executor(
    function_name: str = "DAG_executor_lambda",
    payload: dict = None
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
    """
    logger.trace("Creating AWS Lambda invocation payload for function '%s'" % function_name)
    #logger.trace("Provided payload: " + str(payload))
    DAG_exec_state = payload['DAG_executor_state']
    inp = payload['input']
    #Note: payload also includes DAG_info
    #DAG_info = payload['DAG_info']

    logger.trace ("invoke_lambda_DAG_executor: lambda payload is DAG_info + state: " + str(DAG_exec_state.state) + ", input: " + str(inp))
												
    s = time.time()

	# The `_payload` variable is the one I actually pass to AWS Lambda.
	# The `payload` variable is passed by the driver or program to `invoke_lambda_DAG_executor`.
	# For each key-value pair in `payload`, we create a corresponding 
	# entry in `_payload`. The key is the same. But we first pickle]
	# the value via cloudpickle.dumps(). This returns a `bytes` object.
	# AWS Lambda uses JSON encoding to pass invocation payloads to Lambda
	# functions, and JSON doesn't support bytes. So, we convert the bytes 
	# to a string by encoding the bytes in base64 via base64.b64encode().
	# There is ONE more step, however. base64.b64encode() returns a UTF-8-encoded
	# string, which is also bytes. So, we call .decode('utf-8') to convert it
	# to a regular python string, which is stored as the value in `_payload[k]`, where
	# k is the key.
    _payload = {}
    for k,v in payload.items():
        _payload[k] = base64.b64encode(cloudpickle.dumps(v)).decode('utf-8')
											
    payload_json = json.dumps(_payload)
    logger.trace("Finished creating AWS Lambda invocation payload in %f ms." % ((time.time() - s) * 1000.0))

    logger.trace("Invoking AWS Lambda function '" + function_name + "' with payload containing " + str(len(payload)) + " key(s).")
    s = time.time()
    
    # TEST is a global varial. Set to TRUE to test the real lambda
    # logic without invoking real AWS Lambdas.
    if not TEST:
    # This is the call to the AWS API that actually invokes the Lambda.
        status_code = lambda_client.invoke(
            FunctionName = function_name, 
            InvocationType = 'Event',
            Payload = payload_json) 
    else:
        # bridge around the call to lambda_client.invoke()
        status_code = -1
        lambda_handler(payload_json,None)
    										
    logger.trace("Invoked AWS Lambda function '%s' in %f ms. Status: %s." % (function_name, (time.time() - s) * 1000.0, str(status_code)))


#############################################################

# This is NOT the real lambda_handler. The real handler is in handlerDAG.py.
# we copied it here so we can call it for TEST.
# If we update handlerDAG.py make the changes here too before running TEST.
warm_resources = {
	'cold_start_time': time.time(),
	'invocation_count': 0,
}

def lambda_handler(event, context):
    # TEST must be True since we were called.
    import wukongdnc.dag.DAG_executor
    invocation_time = time.time()
    warm_resources['invocation_count'] = warm_resources['invocation_count'] + 1

    start_time = time.time()
    # Do not do redi calls for TEST
    #rc = redis.Redis(host = REDIS_IP_PRIVATE, port = 6379)

    logger.trace("lambda_handler: Invocation received. Starting DAG_executor_lambda: event/payload is: " + str(event))
    logger.trace(f'Invocation count: {warm_resources["invocation_count"]}, Seconds since cold start: {round(invocation_time - warm_resources["cold_start_time"], 1)}')
    #TEST is True since we called lambda_handler. 
    #DAG_executor_lambda(event)
    # lambda does the json_loads(event) so we have to do it here.
    payload = json.loads(event)
    wukongdnc.dag.DAG_executor.DAG_executor_lambda(payload)
				 
    end_time = time.time()
    duration = end_time - start_time
    logger.trace("lambda_handler: DAG_executor_lambda finished. Time elapsed: %f seconds." % duration)
    # do not do redis calls for TEST
    #rc.lpush("durations", duration)