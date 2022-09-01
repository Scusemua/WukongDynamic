import logging 
import base64
import re 
import socket
import time 
import redis 
import uuid

import cloudpickle
from wukongdnc.wukong.invoker import invoke_lambda
from wukongdnc.server.state import State 
from wukongdnc.server.api import synchronize_sync, synchronize_sync, synchronize_async_terminate
from wukongdnc.constants import REDIS_IP_PRIVATE, TCP_SERVER_IP
from wukongdnc.dag.DAG_executor import DAG_executor_lambda

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s')

SLEEP_INTERVAL = 0.120

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

    logger.debug("Invocation received. event/payload: " + str(event))

    logger.debug("Starting DAG_executor: payload is: " + str(event))

    DAG_executor_lambda(event)
				 
    end_time = time.time()
    duration = end_time - start_time
    logger.debug("DAG_executor_lambda finished. Time elapsed: %f seconds." % duration)
    rc.lpush("durations", duration)    