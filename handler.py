import logging 
import base64
import re 
import socket
import time 
import redis 

import cloudpickle
from wukongdnc.wukong.dc_executor import DivideAndConquerExecutor
from wukongdnc.constants import REDIS_IP_PRIVATE

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s')

if logger.handlers:
   for handler in logger.handlers:
      handler.setFormatter(formatter)

root = logging.getLogger()
if root.handlers:
    for handler in root.handlers:
       handler.setFormatter(formatter)

def lambda_handler(event, context):
    """
    Called by AWS Lambda. This is the "main" method of the AWS Lambda function.

    We configure the AWS Lambda function to call this function by specifying the filename and the function name.

    Arguments:
    ----------
        event (dict):
            A JSON-formatted document that contains data for the Lambda to process. The AWS Lambda runtime converts
            the event to a Python (or whatever language) object and passes it to our function code. Generally, it will
            be a dict, but it could be a list, string, integer, float, or None (i.e., null).

            The event object contains information from the invoking service. The entity that invokes the AWS Lambda 
            function determines the structure and contents of the event. 

            This is the object through which we pass arguments from the client (user) to the Executors, or from Executors
            to other Executors (when we invoke AWS Lambda functions from other AWS Lambda functions).

            See: https://docs.aws.amazon.com/lambda/latest/dg/python-handler.html
        
        context (awslambdaric.lambda_context.LambdaContext):
            When Lambda runs this function, it passes it a context object. This is that context object.
            The context object provides methods and properties that provide information about the invocation, 
            function, and execution environment.

            See: https://docs.aws.amazon.com/lambda/latest/dg/python-context.html
    """
    start_time = time.time()
    rc = redis.Redis(host = REDIS_IP_PRIVATE, port = 6379)
    logger.debug("Invocation received.")

    # Extract all of the data from the payload.
    # first_executor = event["first_executor"]
    state = cloudpickle.loads(base64.b64decode(event["state"]))
    problem = cloudpickle.loads(base64.b64decode(event["problem"]))
    problem_type = cloudpickle.loads(base64.b64decode(event["problem_type"]))
    result_type = cloudpickle.loads(base64.b64decode(event["result_type"]))
    null_result = cloudpickle.loads(base64.b64decode(event["null_result"]))
    stop_result = cloudpickle.loads(base64.b64decode(event["stop_result"]))

    logger.debug("Problem: " + str(problem))
    logger.debug("Problem type: " + str(problem_type))
    logger.debug("Result type: " + str(result_type))
    logger.debug("Null result: " + str(null_result))
    logger.debug("Stop result: " + str(stop_result))

    ###################################################################################
    # CREATE() could be called here if we wanted it to be in the AWS Lambda function. #
    ###################################################################################

    # Create the Executor object.
    executor = DivideAndConquerExecutor(
        state = state,
        problem = problem,
        problem_type = problem_type, 
        result_type = result_type,   
        null_result = null_result,
        stop_result = stop_result
    )

    logger.debug("Starting executor.")
    executor.start()
    executor.join()
    end_time = time.time()
    duration = end_time - start_time
    logger.debug("Executor finished. Time elapsed: %f seconds." % duration)
    rc.lpush("durations", duration)