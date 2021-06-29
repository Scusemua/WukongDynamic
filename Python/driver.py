
import logging
logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s')

if logger.handlers:
   for handler in logger.handlers:
      handler.setFormatter(formatter)

root = logging.getLogger()
if root.handlers:
    for handler in root.handlers:
       handler.setFormatter(formatter)

from WukongProblem import WukongProblem
from DivideAndConquerExecutor import DivideAndConquerExecutor
from DivideandConquerFibonacci import DivideandConquerFibonacci, FibbonaciProgram, ProblemType, ResultType

# Main method, so to speak.
if __name__ == "__main__":
    logger.debug("Running DivideandConquerFibonacci")
    logger.debug("INPUT_THRESHOLD is: {}".format(WukongProblem.INPUT_THRESHOLD))
    logger.debug("OUTPUT_THRESHOLD is: {}".format(WukongProblem.OUTPUT_THRESHOLD))
    logger.debug("SEQUENTIAL_THRESHOLD is: {}".format(ProblemType.SEQUENTIAL_THRESHOLD))
    logger.debug("memoize is: " + str(ProblemType.memoize))

    # Assert 
    seq = None 
    try:
        seq = getattr(ProblemType, "SEQUENTIAL_THRESHOLD", None)
    except Exception:
        pass 

    if seq is None:
        logger.fatal("ProblemType.SEQUENTIAL_THRESHOLD must be defined.")
        
    logger.debug("n: " + str(DivideandConquerFibonacci.n))

    rootID = str(DivideandConquerFibonacci.n)
    FanInStack = list() 
    rootProblem = ProblemType(
        value = DivideandConquerFibonacci.n,
        UserProgram = FibbonaciProgram()
    )

    logger.debug("Root Problem: " + str(rootProblem))

    rootProblem.FanInStack = FanInStack
    rootProblem.problemID = DivideandConquerFibonacci.root_problem_id

    root = DivideAndConquerExecutor(
        problem = rootProblem,
        problem_type = ProblemType, # ProblemType is a user-provided class.
        result_type = ResultType   # ProblemType is a user-provided class.
        )
    root.run()