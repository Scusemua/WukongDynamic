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

    if "create_bounded_buffer" in event and event["create_bounded_buffer"] == True:
        # TODO: This should only happen once at the very beginning of the program.
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as websocket:
            logger.debug("Calling executor.create() for the BoundedBuffer now...")
            executor.create(websocket, "create", "BoundedBuffer", "result", state)

    logger.debug("Starting executor.")
    executor.start()
    executor.join()
    end_time = time.time()
    duration = end_time - start_time
    logger.debug("Executor finished. Time elapsed: %f seconds." % duration)
    rc.lpush("durations", duration)