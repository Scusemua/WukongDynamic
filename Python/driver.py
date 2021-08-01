import importlib
import yaml 
import sys

import logging
from logging import handlers
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s')
ch = logging.StreamHandler(sys.stdout)
ch.setFormatter(formatter)
logger.addHandler(ch)

fh = handlers.RotatingFileHandler("divide_and_conquer.log", maxBytes=(1048576*5), backupCount=7, mode='w')
fh.setFormatter(formatter)
logger.addHandler(fh)

if logger.handlers:
   for handler in logger.handlers:
      handler.setFormatter(formatter)

root = logging.getLogger()
if root.handlers:
    for handler in root.handlers:
       handler.setFormatter(formatter)

from wukong.wukong_problem import WukongProblem
from wukong.dc_executor import DivideAndConquerExecutor
from fibonnaci_program import ResultType, ProblemType, FibonacciProgram

import fibonnaci_program

# Main method, so to speak.
if __name__ == "__main__":
    # with open("wukong-divide-and-conquer.yaml") as f:
    #     config = yaml.load(f, Loader = yaml.FullLoader)
    #     config = config 
    #     sources_config = config["sources"]
    #     memoization_config = config["memoization"]
        
    #     source_path = sources_config["source-path"]
    #     source_module = sources_config["source-module"]
    #     spec = importlib.util.spec_from_file_location(source_module, source_path)
    #     user_module = importlib.util.module_from_spec(spec)
    #     spec.loader.exec_module(user_module)

    #     ProblemType = user_module.ProblemType    # refers to a class provided by user, e.g., ProblemType
    #     ResultType = user_module.ResultType      # refers to a class provided by user, e.g., ResultType
    #     UserProgram = getattr(user_module, sources_config["user-program-name"])

    logger.debug("Running DivideandConquerFibonacci")
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
        
    logger.debug("n: " + str(fibonnaci_program.n))

    rootID = str(fibonnaci_program.n)
    fan_in_stack = list() 
    rootProblem = ProblemType(
        value = fibonnaci_program.n,
        UserProgram = FibonacciProgram()
    )

    logger.debug("memoize is: " + str(rootProblem.memoize))

    logger.debug("Root Problem: " + str(rootProblem))

    rootProblem.fan_in_stack = fan_in_stack
    rootProblem.problem_id = fibonnaci_program.root_problem_id

    root = DivideAndConquerExecutor(
        problem = rootProblem,
        problem_type = ProblemType, # ProblemType is a user-provided class.
        result_type = ResultType,   # ProblemType is a user-provided class.
        null_result = fibonnaci_program.NullResult,
        stop_result = fibonnaci_program.StopResult,
        config_file_path = "./wukong-divide-and-conquer.yaml"
    )
    root.start()

    root.join()