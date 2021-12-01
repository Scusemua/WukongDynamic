import sys
import argparse 
import json
import logging
import numpy as np
import base64
import cloudpickle
import time
from functools import reduce

sys.path.append("..")

from wukong.invoker import invoke_lambda

from logging import handlers
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s')
ch = logging.StreamHandler(sys.stdout)
ch.setFormatter(formatter)
logger.addHandler(ch)

#fh = handlers.RotatingFileHandler("divide_and_conquer.log", maxBytes=(1048576*5), backupCount=7, mode='w')
#fh.setFormatter(formatter)
#logger.addHandler(fh)
import redis
from constants import REDIS_IP_PUBLIC
redis_client = redis.Redis(host = REDIS_IP_PUBLIC, port = 6379)

if logger.handlers:
   for handler in logger.handlers:
      handler.setFormatter(formatter)

root = logging.getLogger()
if root.handlers:
    for handler in root.handlers:
       handler.setFormatter(formatter)

from wukong.wukong_problem import WukongProblem
from fibonnaci_program import ResultType, ProblemType, FibonacciProgram

import fibonnaci_program

def decode_base64(original_data, altchars=b'+/'):
    """Decode base64, padding being optional.

    :param data: Base64 data as an ASCII byte string
    :returns: The decoded byte string.

    """
    if type(original_data) is str:
        original_data = original_data.encode('utf-8') # Convert to bytes.
    original_data += b'==='
    return base64.b64decode(original_data, altchars)

def ResetRedis():
    print("Flushing Redis DB now.")
    redis_client.flushdb()
    redis_client.flushall()

# Main method, so to speak.
if __name__ == "__main__":
    logger.debug("Running DivideandConquerFibonacci")
    logger.debug("INPUT_THRESHOLD is: {}".format(WukongProblem.INPUT_THRESHOLD))
    logger.debug("OUTPUT_THRESHOLD is: {}".format(WukongProblem.OUTPUT_THRESHOLD))
    logger.debug("SEQUENTIAL_THRESHOLD is: {}".format(ProblemType.SEQUENTIAL_THRESHOLD))

    parser = argparse.ArgumentParser()
    parser.add_argument("-n", type = int, default = 5, help = "This application computes fibonacci(n), so this is the n value.")
    parser.add_argument("-e", "--expected-value", default = 5, type = int, dest = "expected_value", help = "The expected solution of the application. Used for testing/debugging.")

    args = parser.parse_args()

    fib = lambda n:reduce(lambda x,n:[x[1],x[0]+x[1]], range(n),[0,1])[0]

    n = args.n
    expected_value = args.expected_value

    if (expected_value == -1):
        logger.warning("Calculating expected value manually...")
        expected_value = fib(n)
        logger.debug("Calculated expected value to be: " + str(expected_value))
    else:
        logger.debug("Expected value: " + str(expected_value))

    # Assert 
    seq = None 
    try:
        seq = getattr(ProblemType, "SEQUENTIAL_THRESHOLD", None)
    except Exception:
        pass 

    if seq is None:
        logger.fatal("ProblemType.SEQUENTIAL_THRESHOLD must be defined.")
        
    logger.debug("n: " + str(n))

    fan_in_stack = list() 
    rootProblem = ProblemType(
        value = n,
        UserProgram = FibonacciProgram()
    )

    logger.debug("memoize is: " + str(rootProblem.memoize))

    logger.debug("Root Problem: " + str(rootProblem))

    rootProblem.fan_in_stack = fan_in_stack
    rootProblem.problem_id = fibonnaci_program.root_problem_id

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
        "null_result": fibonnaci_program.NullResult,
        "stop_result": fibonnaci_program.StopResult
    }

    ResetRedis()

    start_time = time.time()
    invoke_lambda(payload = payload)

    print("redis_client.ping: " + str(redis_client.ping()))

    while True:
        answer_exists = redis_client.exists("solution")

        if (answer_exists):
            end_time = time.time()
            logger.debug("Answer found in Redis!")
            logger.debug("Time elapsed: %f seconds." % (end_time - start_time))
            resultPayloadJson = redis_client.get("solution")

            resultPayload = json.loads(resultPayloadJson)

            problem_id = resultPayload["problem_id"]
            resultEncoded = resultPayload["solution"]

            resultSerialized = decode_base64(resultEncoded)
            result = cloudpickle.loads(resultSerialized)

            logger.debug("Solution: " + str(result))

            logger.debug(problem_id + ": Fibonacci(" + str(n) + ") = " + str(result.value))

            logger.debug(problem_id + ": Verifying ....... ")
            error = False 
            if result.value != expected_value:
                error = True 
            
            if not error:
                logger.debug("Verified.")
            else:
                logger.error("ERROR: Final answer differs from expected answer.")
                logger.error("Final answer: " + str(result.value) + ", expected solution: " + str(expected_value))

            logger.debug("Retrieving durations...")
            time.sleep(2)
            durations = redis_client.lrange("durations",  0, -1)
            durations = [float(x) for x in durations]
            logger.info("Number of Lambdas used: " + str(len(durations)))
            logger.info("Average: %f\nMin: %f\nMax: %f" % (np.mean(durations), np.min(durations), np.max(durations)))
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