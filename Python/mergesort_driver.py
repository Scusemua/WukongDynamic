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

    numbers = mergesort_program.NUMBERS
    expected_order = mergesort_program.EXPECTED_ORDER

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

