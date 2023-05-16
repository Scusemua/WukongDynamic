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
formatter = logging.Formatter('[%(asctime)s] [%(module)s] [%(processName)s] [%(threadName)s]: %(message)s')
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
    payload = {
        "list_of_functions": ["FuncA", "FuncB"],
        "starting_input": int(0)
    }

    ResetRedis()
    
    start_time = time.time()
    invoke_lambda(payload = payload, is_first_invocation = True, n = 1, initial_permits = 0, function_name = "ComposerServerlessSync")

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as websocket:
        print("Connecting to " + str(TCP_SERVER_IP))
        websocket.connect(TCP_SERVER_IP)
        default_state = State("Composer", function_instance_ID = str(uuid.uuid4()), list_of_functions = ["FuncA", "FuncB"])

        sleep_length_seconds = 20.0
        logger.debug("Sleeping for " + str(sleep_length_seconds) + " seconds before calling synchronize_sync()")
        time.sleep(sleep_length_seconds)
        logger.debug("Finished sleeping. Calling synchronize_sync() now...")

        state = synchronize_sync(websocket, "synchronize_sync", "final_result", "try_withdraw", default_state)
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
        duration_hour = aggregated_duration * 1000.0
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
    parser = argparse.ArgumentParser()
    parser.add_argument("-n", type = int, default = 16, help = "Randomly generate an array of this size, then sort it.")
    parser.add_argument("--benchmark", action = "store_true", help = "Run a benchmark rather than a single test.")
    parser.add_argument("-t", "--trials", type = int, default = 10, help = "Number of trials to run during a benchmark.")
    parser.add_argument("-o", "--output", type = str, default = None, help = "Output file for benchmark results.")

    args = parser.parse_args()

    if not args.benchmark:
        run()
    else:
        results = []
        for i in range(args.trials):
            logger.info("===== Trial %d/%d =====" % (i+1, args.trials))
            result = run()
            results.append(result)
        
        output_file = args.output
        if output_file is None:
            output_file = "./data/composer/composer_sleep_%dms_bench_no_try.csv" % 10

        logger.info("Writing benchmark results to file %s now..." % output_file)
        df = pd.DataFrame(results)
        logger.debug("DataFrame:\n%s" % str(df))
        df.to_csv(output_file)
        
        time.sleep(1.25)