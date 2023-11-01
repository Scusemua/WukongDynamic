import time 
import redis 
import os

# Might be useful:
# https://unbiased-coder.com/detect-aws-env-python-nodejs/
def is_aws_env():
    return os.environ.get('AWS_LAMBDA_FUNCTION_NAME') or os.environ.get('AWS_EXECUTION_ENV')

from wukongdnc.constants import REDIS_IP_PRIVATE
from wukongdnc.dag.DAG_executor import DAG_executor_lambda

import logging 
from wukongdnc.dag.addLoggingLevel import addLoggingLevel
""" How to use: https://stackoverflow.com/questions/2183233/how-to-add-a-custom-loglevel-to-pythons-logging-facility/35804945#35804945
    >>> addLoggingLevel('TRACE', logging.DEBUG - 5)
    >>> logging.getLogger(__name__).setLevel("TRACE")
    >>> logging.getLogger(__name__).trace('that worked')
    >>> logging.trace('so did this')
    >>> logging.TRACE
"""
# We are starting a new real Lambda so add logging level TRACE
addLoggingLevel('TRACE', logging.DEBUG - 5)

logger = logging.getLogger(__name__)
# I believe INFO is the deault level for Lambda
logger.setLevel(logging.INFO)
formatter = logging.Formatter('[%(asctime)s] [%(module)s] [%(processName)s] [%(threadName)s]: %(message)s')
# Perhaps hekps with duplicate log entries?
logger.propagate = False

SLEEP_INTERVAL = 0.120

if logger.handlers:
    for handler in logger.handlers:
        handler.setFormatter(formatter)

root = logging.getLogger()
if root.handlers:
    for handler in root.handlers:
        handler.setFormatter(formatter)

warm_resources = {
	'cold_start_time': time.time(),
	'invocation_count': 0,
}

# implcitly called by AWS Lambda aftee we call lambda_client.invoke().
# Note: When we test real Lambda logic we bridge the call to
# lambda_client.invoke() by not calling lambda_client.invoke() and 
# calling a local lambda_handler() defined in wukongdnc\wukong\invoker.py
def lambda_handler(event, context):
    invocation_time = time.time()
    warm_resources['invocation_count'] = warm_resources['invocation_count'] + 1
    
    start_time = time.time()
    rc = redis.Redis(host = REDIS_IP_PRIVATE, port = 6379)

    logger.info("lambda_handler: is_aws_env(): " + str(is_aws_env()))
    logger.info("lambda_handler: Invocation received. Calling method DAG_executor_lambda(): event/payload is: " + str(event))
    logger.info(f'Invocation count: {warm_resources["invocation_count"]}, Seconds since cold start: {round(invocation_time - warm_resources["cold_start_time"], 1)}')
    DAG_executor_lambda(event)
				 
    end_time = time.time()
    duration = end_time - start_time
    logger.info("lambda_handler: DAG_executor_lambda() finished. Time elapsed: %f seconds." % duration)
    rc.lpush("durations", duration)    