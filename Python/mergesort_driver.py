import sys

import logging
import base64 
import time 
import numpy as np
import cloudpickle
from mergesort_program import MergesortProgram

from wukong.invoker import invoke_lambda

from logging import handlers
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s')
ch = logging.StreamHandler(sys.stdout)
ch.setFormatter(formatter)
logger.addHandler(ch)

if logger.handlers:
   for handler in logger.handlers:
      handler.setFormatter(formatter)

root = logging.getLogger()
if root.handlers:
    for handler in root.handlers:
       handler.setFormatter(formatter)

import redis
from constants import REDIS_IP_PUBLIC
redis_client = redis.Redis(host = REDIS_IP_PUBLIC, port = 6379)

from wukong.wukong_problem import WukongProblem
from wukong.dc_executor import DivideAndConquerExecutor
from mergesort_program import ResultType, ProblemType
import mergesort_program

def decode_base64(original_data, altchars=b'+/'):
    """Decode base64, padding being optional.

    :param data: Base64 data as an ASCII byte string
    :returns: The decoded byte string.

    """
    original_data += b'==='
    return base64.b64decode(original_data, altchars)

def ResetRedis():
    print("Flushing Redis DB now.")
    redis_client.flushdb()
    redis_client.flushall()

NUMBERS = [-81, 72, 63, -51, 96, -6, -73, -33, -63, -18, 31, 50, -88, -3, -5, 22, -56, -100, 48, -76, -4, -97, 82, 41, -65, -30, -30, 99, -94, 77, 92, 45, 99, -17, -47, -44, 46, -85, -59, 42, -69, -54, -40, -87, 45, -34, 79, 87, 83, -94, 60, -91, 78, 30, 9, 3, -77, -3, -55, 86, -33, -59, 21, 28, 16, -94, -82, 47, -79, 34, 76, 35, -30, 19, -90, -14, 41, 90, 17, 2, 18, -1, -77, -8, 36, 16, 26, 70, -70, 92, -6, -93, -52, 25, -49, -30, -40, 64, -36, 9]
# [9, -3, 5, 0, 1, 2, -1, 4, 11, 10, 13, 12, 15, 14, 17, 16]
EXPECTED_ORDER = [-100, -97, -94, -94, -94, -93, -91, -90, -88, -87, -85, -82, -81, -79, -77, -77, -76, -73, -70, -69, -65, -63, -59, -59, -56, -55, -54, -52, -51, -49, -47, -44, -40, -40, -36, -34, -33, -33, -30, -30, -30, -30, -18, -17, -14, -8, -6, -6, -5, -4, -3, -3, -1, 2, 3, 9, 9, 16, 16, 17, 18, 19, 21, 22, 25, 26, 28, 30, 31, 34, 35, 36, 41, 41, 42, 45, 45, 46, 47, 48, 50, 60, 63, 64, 70, 72, 76, 77, 78, 79, 82, 83, 86, 87, 90, 92, 92, 96, 99, 99]
# [-3, -1, 0, 1, 2, 4, 5, 9, 10, 11, 12, 13, 14, 15, 16, 17]

if __name__ == "__main__":
    logger.debug("Running Mergesort")
    logger.debug("INPUT_THRESHOLD is: {}".format(WukongProblem.INPUT_THRESHOLD))
    logger.debug("OUTPUT_THRESHOLD is: {}".format(WukongProblem.OUTPUT_THRESHOLD))
    logger.debug("SEQUENTIAL_THRESHOLD is: {}".format(ProblemType.SEQUENTIAL_THRESHOLD))

    # Assert 
    seq = None 
    try:
        seq = getattr(ProblemType, "SEQUENTIAL_THRESHOLD", None)
    except Exception:
        pass 

    if seq is None:
        logger.fatal("ProblemType.SEQUENTIAL_THRESHOLD must be defined.")

    numbers = NUMBERS
    expected_order = EXPECTED_ORDER

    print("Input array (numbers): " + str(numbers))
    print("Expected output array: " + str(expected_order))

    fan_in_stack = list() 
    rootProblem = ProblemType(
        numbers = numbers,
        from_idx = 0,
        to_idx = len(numbers) - 1,
        UserProgram = MergesortProgram())

    logger.debug("memoize is: " + str(rootProblem.memoize))
    logger.debug("Root Problem: " + str(rootProblem))

    rootProblem.fan_in_stack = fan_in_stack
    rootProblem.problem_id = mergesort_program.root_problem_id

    payload = {
        "problem": rootProblem,
        "problem_type": ProblemType,
        "result_type": ResultType,
        "null_result": mergesort_program.NullResult,
        "stop_result": mergesort_program.StopResult
    }

    ResetRedis()

    redis_client.set("input", base64.b64encode(cloudpickle.dumps(NUMBERS)))
    
    start_time = time.time()
    invoke_lambda(payload = payload)

    while True:
        answer_exists = redis_client.exists("solution")

        if (answer_exists):
            end_time = time.time()
            logger.debug("Answer found in Redis!")
            logger.debug("Time elapsed: %f seconds." % (end_time - start_time))
            answerEncoded = redis_client.get("solution")
            answerSerialized = decode_base64(answerEncoded)
            answer = cloudpickle.loads(answerSerialized)
            logger.debug("Solution: " + str(answer))

            error_occurred = False
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
            logger.info("Aggregate duration: %f" % aggregated_duration)
            
            cost_128mb = 0.0000000021
            func_size = 256
            scale = func_size / 128.0
            cost_per_hr = cost_128mb * scale 
            duration_hour = aggregated_duration / 60.0
            estimated_cost = duration_hour * cost_per_hr
            logger.info("Estimated cost: $" + str(estimated_cost))
            logger.info(durations)
            break
        else:
            time.sleep(0.1)

