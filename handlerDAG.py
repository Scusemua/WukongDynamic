import logging 
import time 
import redis 

from wukongdnc.constants import REDIS_IP_PRIVATE
from wukongdnc.dag.DAG_executor import DAG_executor_lambda

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(asctime)s] [%(module)s] [%(processName)s] [%(threadName)s]: %(message)s')

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

def lambda_handler(event, context):
    invocation_time = time.time()
    warm_resources['invocation_count'] = warm_resources['invocation_count'] + 1
    
    start_time = time.time()
    rc = redis.Redis(host = REDIS_IP_PRIVATE, port = 6379)

    logger.debug("lambda_handler: Invocation received. Starting DAG_executor_lambda: event/payload is: " + str(event))
    logger.debug(f'Invocation count: {warm_resources["invocation_count"]}, Seconds since cold start: {round(invocation_time - warm_resources["cold_start_time"], 1)}')
    DAG_executor_lambda(event)
				 
    end_time = time.time()
    duration = end_time - start_time
    logger.debug("lambda_handler: DAG_executor_lambda finished. Time elapsed: %f seconds." % duration)
    rc.lpush("durations", duration)    