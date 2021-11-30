import sys

import logging
import base64 
import time 
import cloudpickle

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

    rootID = "root"
    fan_in_stack = list() 
    rootProblem = ProblemType(
        numbers = numbers,
        from_idx = 0,
        to_idx = len(numbers) - 1
    )

    logger.debug("memoize is: " + str(rootProblem.memoize))
    logger.debug("Root Problem: " + str(rootProblem))

    rootProblem.fan_in_stack = fan_in_stack
    rootProblem.problem_id = rootID

    # root = DivideAndConquerExecutor(
    #     problem = rootProblem,
    #     problem_type = ProblemType, # ProblemType is a user-provided class.
    #     result_type = ResultType,   # ProblemType is a user-provided class.
    #     null_result = fibonnaci_program.NullResult,
    #     stop_result = fibonnaci_program.StopResult
    # )

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
            break
        else:
            time.sleep(0.1)

    # root.start()

    # root.join()

