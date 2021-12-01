import logging 
import base64
import re 
import time 
import redis 

import cloudpickle
from wukong.dc_executor import DivideAndConquerExecutor
from constants import REDIS_IP_PRIVATE

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

    executor = DivideAndConquerExecutor(
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