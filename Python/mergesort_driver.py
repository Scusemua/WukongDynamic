import sys

import logging

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

from wukong.wukong_problem import WukongProblem
from wukong.dc_executor import DivideAndConquerExecutor
from mergesort_program import ResultType, ProblemType, MergesortProgram
import mergesort_program

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
        
    numbers = [9, -3, 5, 0, 1, 2, -1, 4, 11, 10, 13, 12, 15, 14, 17, 16]
    expected_order = [-3, -1, 0, 1, 2, 4, 5, 9, 10, 11, 12, 13, 14, 15, 16, 17]

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

    mergesort_program.ResetRedis()

    invoke_lambda(payload = payload)

    # root.start()

    # root.join()

