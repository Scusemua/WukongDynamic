import logging 
#import base64
#import re 
#import socket
import time 
# import redis 
#import uuid

#import cloudpickle
#from wukongdnc.wukong.invoker import invoke_lambda
#from wukongdnc.server.state import State 
#from wukongdnc.server.api import synchronize_sync
from wukongdnc.constants import REDIS_IP_PRIVATE  #, TCP_SERVER_IP
from wukongdnc.dag.DAG_executor import DAG_executor_lambda
from wukongdnc.dag.DAG_executor_constants import log_level
from wukongdnc.dag.addLoggingLevel import addLoggingLevel
addLoggingLevel('TRACE', logging.DEBUG - 5)
logging.basicConfig(encoding='utf-8',level=log_level, format='[%(levelname)-.1s] [%(asctime)s][%(module)s][%(processName)s][%(threadName)s]: %(message)s')
logger = logging.getLogger(__name__)

# Added this to suppress the logging message:
#   credentials - MainProcess - MainThread: Found credentials in shared credentials file: ~/.aws/credentials
# But it appears that we could see other things liek this:
# https://stackoverflow.com/questions/1661275/disable-boto-logging-without-modifying-the-boto-files
logging.getLogger('botocore').setLevel(logging.CRITICAL)

#logger.setLevel(logging.INFO)
#logger.setLevel(log_level)
#formatter = logging.Formatter('[%(levelname)-.1s] [%(asctime)s] [%(module)s] [%(processName)s] [%(threadName)s]: %(message)s')

SLEEP_INTERVAL = 0.120

# Might need this:
"""
if logger.handlers:
    for handler in logger.handlers:
        handler.setFormatter(formatter)

root = logging.getLogger()
if root.handlers:
    for handler in root.handlers:
        handler.setFormatter(formatter)
"""

warm_resources = {
	'cold_start_time': time.time(),
	'invocation_count': 0,
}

def lambda_handler(event, context):
    invocation_time = time.time()
    warm_resources['invocation_count'] = warm_resources['invocation_count'] + 1
    logger.debug(f'Invocation count: {warm_resources["invocation_count"]}, Seconds since cold start: {round(invocation_time - warm_resources["cold_start_time"], 1)}')

    start_time = time.time()
    # rc = redis.Redis(host = REDIS_IP_PRIVATE, port = 6379)

    logger.debug("lambda_handler: Invocation received. Starting DAG_executor_lambda: event/payload is: " + str(event))
    DAG_executor_lambda(event)
				 
    end_time = time.time()
    duration = end_time - start_time
    logger.debug("lambda_handler: DAG_executor_lambda finished. Time elapsed: %f seconds." % duration)
    # rc.lpush("durations", duration)    