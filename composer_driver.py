import sys

import logging
import base64 
import time 
import argparse 
import numpy as np
import cloudpickle
import pandas as pd
import uuid
import json
import redis
import socket
import random

from logging import handlers
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s')
ch = logging.StreamHandler(sys.stdout)
ch.setFormatter(formatter)
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)

if logger.handlers:
   for handler in logger.handlers:
      handler.setFormatter(formatter)

root = logging.getLogger()
if root.handlers:
    for handler in root.handlers:
       handler.setFormatter(formatter)

from wukongdnc.constants import TCP_SERVER_IP, REDIS_IP_PUBLIC
from wukongdnc.server.state import State 
from wukongdnc.server.util import make_json_serializable
from wukongdnc.server.api import send_object, recv_object, close_all, synchronize_sync
from wukongdnc.wukong.invoker import invoke_lambda

redis_client = redis.Redis(host = REDIS_IP_PUBLIC, port = 6379)

def ResetRedis():
    logger.debug("Flushing Redis DB now.")
    redis_client.flushdb()
    redis_client.flushall()
    logger.debug("Flushed Redis DB.")

def run():
    payload = {}
    
    start_time = time.time()
    invoke_lambda(payload = payload, is_first_invocation = True, n = 1, initial_permits = 0, function_name = "Composer")

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as websocket:
        print("Connecting to " + str(TCP_SERVER_IP))
        websocket.connect(TCP_SERVER_IP)
        default_state = State("Composer_driver", function_instance_ID = str(uuid.uuid4()))
        state = synchronize_sync(websocket, "synchronize_sync", "final_result", "withdraw", default_state)
        answer = state.return_value 

        end_time = time.time()
        
        error_occurred = False
        if type(answer) is str:
            logger.error("Unexpected solution recovered from Redis: %s\n\n" % answer)
            error_occurred = True
        else:
            logger.debug("Solution: " + str(answer) + "\n\n")
            expected_answer = int(2)
            if expected_answer != answer:
                logger.error("Error in answer: " + str(answer) + " expected_answer: " + str(expected_answer))
                error_occurred = True 

        if not error_occurred:
            logger.debug("Verified.")

        logger.debug("Retrieving durations...")
        time.sleep(2)
        durations = redis_client.lrange("durations",  0, -1)
        durations = [float(x) for x in durations]
        logger.info("Number of Lambdas used: " + str(len(durations)))
        logger.info("Average: %f" % np.mean(durations))
        logger.info("Min: %f" % np.min(durations))
        logger.info("Max: %f" % np.max(durations))
        aggregated_duration = np.sum(durations)
        logger.info("Aggregate duration: %f" % aggregated_duration)
        
        cost_128mb = 0.0000000021
        func_size = 256
        scale = func_size / 128.0
        cost_per_hr = cost_128mb * scale 
        duration_hour = aggregated_duration / 60.0
        estimated_cost = duration_hour * cost_per_hr
        logger.info("Estimated cost: $" + str(estimated_cost))
        #logger.info(durations)

        close_all(websocket)

        return {
            "time": end_time - start_time,
            "cost": estimated_cost,
            "num_lambdas": len(durations),
            "aggregate_duration": aggregated_duration,
            "min_duration": np.min(durations),
            "max_duration": np.max(durations),
            "avg_duration": np.mean(durations)
        }

if __name__ == "__main__":
    run()