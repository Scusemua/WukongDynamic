import logging 
import time 
from wukongdnc.dag.DAG_executor import DAG_executor_lambda
from ..dag import DAG_executor_constants
from wukongdnc.dag.addLoggingLevel import addLoggingLevel
addLoggingLevel('TRACE', logging.DEBUG - 5)
logging.basicConfig(encoding='utf-8',level=DAG_executor_constants.log_level, format='[%(levelname)-.1s] [%(asctime)s][%(module)s][%(processName)s][%(threadName)s]: %(message)s')
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

# Extra information just for fun
warm_resources = {
	'cold_start_time': time.time(),
	'invocation_count': 0,
}

# lambda handler for AWS Lambda
def lambda_handler(event, context):
    # Calls DAG_executor_lambda(event)

    # Extra information
    invocation_time = time.time()
    warm_resources['invocation_count'] = warm_resources['invocation_count'] + 1
    logger.debug(f'Invocation count: {warm_resources["invocation_count"]}, Seconds since cold start: {round(invocation_time - warm_resources["cold_start_time"], 1)}')
    start_time = time.time()
    # rc = redis.Redis(host = REDIS_IP_PRIVATE, port = 6379)
    logger.debug("lambda_handler: Invocation received. Starting DAG_executor_lambda: event/payload is: " + str(event))

    #############################################################################
    # Note: This call is the only thing that the handler is really required to do
    DAG_executor_lambda(event)
    #############################################################################

    # Extra information		 
    end_time = time.time()
    duration = end_time - start_time
    logger.debug("lambda_handler: DAG_executor_lambda finished. Time elapsed: %f seconds." % duration)
    # rc.lpush("durations", duration)    