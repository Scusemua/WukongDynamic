import boto3 
import uuid 
import json 
import time 

from state import State

import logging 
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s')

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)

logger.addHandler(ch)

from util import make_json_serializable

function_name = "PyroTest"

class Invoker(object):
    def __init__(self):
        self.client_id = str(uuid.uuid4)
        self.lambda_client = boto3.client("lambda", region_name = "us-east-1")

    def invoke(self, do_create = False, state = None):
        """
        Invoke an AWS Lambda function.

        Arguments:
            do_create (bool):
                If True, the Executor should call create(). Otherwise, the Executor does not call create().
            
            state (State):
                If this is non-None, then we pass this to the Lambda function.
                If this is None, then we create the State object in this method and pass it to the Lambda.
        """
        _state = state or State(ID = "PyroTest", restart = False, task_id = str(uuid.uuid4()))
        payload = {
            "state": make_json_serializable(_state),
            "do_create": do_create
        }
        self.lambda_client.invoke(FunctionName = function_name, InvocationType = 'Event', Payload = json.dumps(payload))

        logger.debug("Invoked AWS Lambda function '%s' with do_create=%s" % (function_name, str(do_create)))