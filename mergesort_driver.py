import sys

import logging
import base64 
import time 
import argparse
from more_itertools import numeric_range 
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

from wukongdnc.server.state import State 
from wukongdnc.server.util import make_json_serializable
from wukongdnc.server.api import send_object, recv_object, close_all
from wukongdnc.wukong.invoker import invoke_lambda
from wukongdnc.wukong.wukong_problem import WukongProblem
from wukongdnc.wukong.dc_executor import DivideAndConquerExecutor
from mergesort_program import ResultType, ProblemType, MergesortProgram, root_problem_id, NullResult, StopResult
from wukongdnc.constants import REDIS_IP_PUBLIC, TCP_SERVER_IP
redis_client = redis.Redis(host = REDIS_IP_PUBLIC, port = 6379)

def ResetRedis():
    logger.debug("Flushing Redis DB now.")
    redis_client.flushdb()
    redis_client.flushall()
    logger.debug("Flushed Redis DB.")

#NUMBERS = [-81, 72, 63, -51, 96, -6, -73, -33, -63, -18, 31, 50, -88, -3, -5, 22, -56, -100, 48, -76, -4, -97, 82, 41, -65, -30, -30, 99, -94, 77, 92, 45, 99, -17, -47, -44, 46, -85, -59, 42, -69, -54, -40, -87, 45, -34, 79, 87, 83, -94, 60, -91, 78, 30, 9, 3, -77, -3, -55, 86, -33, -59, 21, 28, 16, -94, -82, 47, -79, 34, 76, 35, -30, 19, -90, -14, 41, 90, 17, 2, 18, -1, -77, -8, 36, 16, 26, 70, -70, 92, -6, -93, -52, 25, -49, -30, -40, 64, -36, 9]
# [9, -3, 5, 0, 1, 2, -1, 4, 11, 10, 13, 12, 15, 14, 17, 16]
#EXPECTED_ORDER = [-100, -97, -94, -94, -94, -93, -91, -90, -88, -87, -85, -82, -81, -79, -77, -77, -76, -73, -70, -69, -65, -63, -59, -59, -56, -55, -54, -52, -51, -49, -47, -44, -40, -40, -36, -34, -33, -33, -30, -30, -30, -30, -18, -17, -14, -8, -6, -6, -5, -4, -3, -3, -1, 2, 3, 9, 9, 16, 16, 17, 18, 19, 21, 22, 25, 26, 28, 30, 31, 34, 35, 36, 41, 41, 42, 45, 45, 46, 47, 48, 50, 60, 63, 64, 70, 72, 76, 77, 78, 79, 82, 83, 86, 87, 90, 92, 92, 96, 99, 99]
# [-3, -1, 0, 1, 2, 4, 5, 9, 10, 11, 12, 13, 14, 15, 16, 17]

def synchronize_sync(websocket, op, name, method_name, state):
    """
    Synchronize on the remote TCP server.

    Arguments:
    ----------
        websocket (socket.socket):
            Socket connection to the TCP server.
            TODO: We pass this in, but in the function body, we connect to the server.
                    In that case, we don't need to pass a websocket. We'll just create one.
                    We should only bother with passing it as an argument if its already connected.
        
        op (str):
            The operation being performed. 
        
        method_name (str):
            The name of the synchronization method we'd be calling on the server.

        name (str):
            The name (which serves as an identifier) of the object we're using for synchronization.
        
        state (state.State):
            Our current state.
    """
    # see the note below about closing the websocket or not
    msg_id = str(uuid.uuid4())
    message = {
        "op": op, 
        "name": name,
        "method_name": method_name,
        "state": make_json_serializable(state),
        "id": msg_id
    }
    logger.debug("Calling %s. Message ID=%s" % (op, msg_id))
    msg = json.dumps(message).encode('utf-8')
    send_object(msg, websocket)
    data = recv_object(websocket)               # Should just be a serialized state object.
    state_from_server = cloudpickle.loads(data) # `state_from_server` is of type State
    return state_from_server

def run(numbers: list, expected_order: list):
    logger.debug("Input array (numbers): " + str(numbers))
    logger.debug("Expected output array: " + str(expected_order))

    fan_in_stack = list() 
    rootProblem = ProblemType(
        numbers = numbers,
        from_idx = 0,
        to_idx = len(numbers) - 1,
        UserProgram = MergesortProgram())

    logger.debug("memoize is: " + str(rootProblem.memoize))
    #logger.debug("Root Problem: " + str(rootProblem))

    rootProblem.fan_in_stack = fan_in_stack
    rootProblem.problem_id = root_problem_id

    # This code is all running from the user's Desktop.
    # This is the payload that gets sent to the very first Lambda.
    payload = {
        "problem": rootProblem,
        "problem_type": ProblemType,
        "result_type": ResultType,
        "null_result": NullResult,
        "stop_result": StopResult
    }

    ResetRedis()

    redis_client.set("input", base64.b64encode(cloudpickle.dumps(numbers)))

    logger.debug("Stored input array in Redis.")
    
    start_time = time.time()
    invoke_lambda(payload = payload, is_first_invocation = True)

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as websocket:
        print("Connecting to " + str(TCP_SERVER_IP))
        websocket.connect(TCP_SERVER_IP)
        #answer_exists = redis_client.exists("solution")
        default_state = State("mergesort_driver", function_instance_ID = str(uuid.uuid4()))
        state = synchronize_sync(websocket, "synchronize_sync", "result", "withdraw", default_state)
        answer = state.return_value 

        end_time = time.time()
        # logger.debug("Answer found in Redis!")
        # logger.debug("Time elapsed: %f seconds." % (end_time - start_time))
        # answerEncoded = redis_client.get("solution")
        # answerSerialized = decode_base64(answerEncoded)
        # answer = cloudpickle.loads(answerSerialized)
        
        error_occurred = False
        if type(answer) is str:
            logger.error("Unexpected solution recovered from Redis: %s\n\n" % answer)
            error_occurred = True
        else:
            logger.debug("Solution: " + str(answer) + "\n\n")

            for i in range(0, len(numbers)):
                if answer.numbers[i] != expected_order[i]:
                    logger.error("Error in expected value: result.numbers[" + str(i) + "]: " + str(answer.numbers[i]) + " != expectedOrder[" + str(i) + "]: " + str(expected_order[i]))
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
        logger.info("Aggregate duration: %f seconds" % aggregated_duration)
        
        cost_128mb = 0.0000000021 # Cost per ms 
        func_size = 256
        scale = func_size / 128.0
        cost_per_hr = cost_128mb * scale 
        duration_ms = aggregated_duration * 1000.0
        estimated_cost = duration_ms * cost_per_hr
        logger.info("Estimated cost: $" + str(estimated_cost))
        logger.info("End-to-end time: " + str(end_time - start_time) + " sec")
        logger.info("%f,%s,%d,%f,%f,%f,%f" % (end_time - start_time, str(estimated_cost), len(durations), aggregated_duration, np.min(durations), np.max(durations), np.mean(durations)))
        logger.info("%f %s %d %f %f %f %f" % (end_time - start_time, str(estimated_cost), len(durations), aggregated_duration, np.min(durations), np.max(durations), np.mean(durations)))
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
    logger.debug("Running Mergesort")
    logger.debug("INPUT_THRESHOLD is: {}".format(WukongProblem.INPUT_THRESHOLD))
    logger.debug("OUTPUT_THRESHOLD is: {}".format(WukongProblem.OUTPUT_THRESHOLD))
    logger.debug("SEQUENTIAL_THRESHOLD is: {}".format(ProblemType.SEQUENTIAL_THRESHOLD))

    parser = argparse.ArgumentParser()
    parser.add_argument("-n", type = int, default = 16, help = "Randomly generate an array of this size, then sort it.")
    parser.add_argument("--benchmark", action = "store_true", help = "Run a benchmark rather than a single test.")
    parser.add_argument("-t", "--trials", type = int, default = 10, help = "Number of trials to run during a benchmark.")
    parser.add_argument("-o", "--output", type = str, default = None, help = "Output file for benchmark results.")

    args = parser.parse_args()

    # Assert 
    seq = None 
    try:
        seq = getattr(ProblemType, "SEQUENTIAL_THRESHOLD", None)
    except Exception:
        pass 

    if seq is None:
        logger.fatal("ProblemType.SEQUENTIAL_THRESHOLD must be defined.")

    numbers = [random.randint(-1000, 1000) for _ in range(0, args.n)]
    expected_order = sorted(numbers)

    logger.debug("Numbers: %s" % str(numbers))
    logger.debug("Expected order: %s" % str(expected_order))

    if len(numbers) > 32000:
        logger.fatal("Problem size is far too large: " + str(len(numbers)))

    if not args.benchmark:
        run(numbers, expected_order)
    else:
        results = []
        for i in range(args.trials):
            logger.info("===== Trial %d/%d =====" % (i+1, args.trials))
            result = run(numbers, expected_order)
            results.append(result)
        
        output_file = args.output
        if output_file is None:
            output_file = "./data/mergesort/mergesort_%d_seq=%d_bench.csv"  % (len(numbers), ProblemType.SEQUENTIAL_THRESHOLD)

        logger.info("Writing benchmark results to file %s now..." % output_file)
        df = pd.DataFrame(results)
        df.to_csv(output_file)

        logger.debug("DataFrame:\n%s" % str(df))

        # Save the input array so we can reuse it when doing comparison against SoCC Wukong.
        with open("./data/mergesort/wukongdc/mergesort_%d_seq=%d.txt" % (len(numbers), ProblemType.SEQUENTIAL_THRESHOLD), "w") as handle:
            handle.write(str(numbers))
        
        time.sleep(1.25)
