import base64
import logging 
import threading 
import time 
import sys
import cloudpickle

from wukongdnc.wukong.wukong_problem import WukongProblem, WukongResult, UserProgram
from wukongdnc.server.util import make_json_serializable, decode_and_deserialize, decode_base64

# import wukong.memoization.memoization_controller as memoization_controller

import redis 
import logging
from wukongdnc.constants import REDIS_IP_PRIVATE
from logging import handlers
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(asctime)s] [%(module)s] [%(processName)s] [%(threadName)s]: %(message)s')
ch = logging.StreamHandler(sys.stdout)
ch.setFormatter(formatter)
#logger.addHandler(ch)

redis_client = redis.Redis(host = REDIS_IP_PRIVATE, port = 6379)

if logger.handlers:
   for handler in logger.handlers:
      handler.setFormatter(formatter)

root = logging.getLogger()
if root.handlers:
    for handler in root.handlers:
       handler.setFormatter(formatter)

debug_lock = threading.Lock() 

root_problem_id = "[0,1]"
final_result_id = "[1,1]"

logger.propagate = False

class ProblemType(WukongProblem):
	# The threshold at which we switch to a sequential algorithm.
    SEQUENTIAL_THRESHOLD = 99
	
	# Get input arrays when the level reaches the INPUT_THRESHOLD, e.g., don't grab the initial 256MB array,
	# wait until you reach level , say, 1, when there are two subproblems each half as big.
	# To input at the start of the problem (root), use 0; the stack has 0 elements on it for the root problem.
	# To input the first two subProblems, use 1. Etc.
    INPUT_THRESHOLD = 1

    def __init__(self, numbers = [], from_idx = -1, to_idx = -1, value = -1, UserProgram = None):
        super(ProblemType, self).__init__(UserProgram = UserProgram)
        self.numbers = numbers
        self.from_idx = from_idx
        self.to_idx = to_idx
        self.value = value     # just to keep Fibonacci happy.

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return "ProblemType(from=" + str(self.from_idx) + ", to=" + str(self.to_idx) + ", numbers=" + str(self.numbers) + ")"
    
    @property
    def memoize(self):
        return False    

class ResultType(WukongResult):
    """
    If type is 1, ResultType is a normal result.
    If type is 0, ResultType is a stopResult.
    If type is -1, ResultType is a nullResult.
    """    
    def __init__(self, numbers = [], from_idx = -1, to_idx = -1, result_type = 0, value = -1):
        super(ResultType, self).__init__()
        self.numbers = numbers
        self.from_idx = from_idx
        self.to_idx = to_idx
        self.type = result_type
        self.value = value     # just to keep Fibonacci happy.

        assert(self.type >= -1 and self.type <= 1)

    def __repr__(self):
        return self.__str__()

    def copy(self):
        return ResultType(
            value = self.value,
            from_idx = self.from_idx,
            to_idx = self.to_idx,
            numbers = self.numbers)

    def __str__(self):
        return "ResultType(from=" + str(self.from_idx) + ", to=" + str(self.to_idx) + ", numbers=" + str(self.numbers) + ")"

class MergesortProgram(UserProgram):
    def __init__(self):
        super(MergesortProgram, self).__init__()
        global final_result_id
        global root_problem_id
        self.root_problem_id = root_problem_id
        self.final_result_id = final_result_id

    def __repr__(self):
        return self.__str__()

    def base_case(self, problem: ProblemType):
        """
        The baseCase is always a sequential sort (though it could be on an array of length 1, if that is the sequential threshold.)
        """
        size = problem.to_idx - problem.from_idx + 1
        return size <= ProblemType.SEQUENTIAL_THRESHOLD
    
    def preprocess(self, problem: ProblemType):
        """
        Some problems may use a pre-processing step before anything else is done.
        """
        pass 

    def trim_problem(self, problem: ProblemType):
        """
        A problem P is by default passed to the executors that will be executing the child subproblems of P. One of these
        executors will execute method combine() for these subproblems. In some cases, it is easier to write combine() when 
        the subproblem's parent problem data is available; however, this parent data will also be sent and retrieved from 
        storage if the parent data is part of the ProblemType data. (That is, problem P's parent data will be sent 
        as part of problem P since the parent data will be on the stack of subProblems (representing the call stack) for P.) 
        So if some of the parent data is not needed for combine() it should be deleted from the parent problem.

        The field of a problem that will definitely be used is the problemID. Do not trim the problemID. 
        The FanInStack (in parent class WukongProblem) is not needed by the DivideandConquer framework and 
        can/should be trimmed. 
        One option is to create a trimProblem() method in call WukongProblem and always call this method (in method
        Fanout) in addition to calling User.trimProblem(), where User.tribProblem may be empty.        
        """
        problem.numbers = None
        problem.fan_in_stack = None 

    def problem_labeler(self, 
        subproblem : ProblemType, 
        childID : int, 
        parent_problem : ProblemType, 
        subproblems : list
    ) -> str:
        """
        User must specify how subproblems are labeled. The problem label is used as a key into Wukong Storage,
        where the value in the key-value pair is (eventually) the problem's result. Also used for Serverless
        Networking pairing names.
        
        Note: we could have used level-order IDs like:
            1            2**0 children starting with child 2**0 (at level 0)
            2     3         2**1 children starting with child 2**! (at level 1)
        4    5  6   7      2**2 children starting with child 2**2 (at level 2)
        This simplifies things since it is easy to compute the parent ID from the child's ID. With this ID scheme
        we would not need to stack the problem IDs as we recurse; however, the stack makes it easy to get the level
        since the level is just the size of the stack. 
        We'll have to see if this scheme would work in general,
        i.e., for different numbers of children, when the number of children may vary for parent nodes, etc. There has to be a 
        way to recover the parent ID from a child, either by using a formula like above, or by stacking the IDs as we recurse.

        Arguments:
        ----------
            subproblem (ProblemType)
            childID (int)
            parent_problem (ProblemType)
            subproblems (arraylike of ProblemType)
        
        Returns:
        --------
            str
        """
        mid = parent_problem.from_idx + ((parent_problem.to_idx - parent_problem.from_idx) // 2)
        id_str = None 

        if childID == 0:
            id_str = str(mid + 1)
            if (mid + 1) < parent_problem.to_idx:
                id_str += "x" + str(parent_problem.to_idx)
            
            return id_str
        else:
            id_str = str(parent_problem.from_idx)
            if parent_problem.from_idx < mid:
                id_str += "x" + str(mid)
            
            return id_str 
        
    def memoize_IDLabeler(self, problem : ProblemType) -> str:
        return None  # MergeSort is not memoized
    
    def divide(self, 
        problem : ProblemType,
        subproblems : list 
    ):
        """
        Divide the problem into (a list of) subproblems.

        Arguments:
        ----------
            problem (ProblemType)

            subproblems (arraylike of ProblemType)

        Returns:
        --------
            Nothing 
        """
        logger.debug("Divide: mergesort run: from: " + str(problem.from_idx) + ", to: " + str(problem.to_idx))
        logger.debug("Divide: problemID: " + str(problem.problem_id))
        logger.debug("Divide: FanInStack: " + str(problem.fan_in_stack))

        #size = problem.to_idx - problem.from_idx + 1
        mid = problem.from_idx + ((problem.to_idx - problem.from_idx) // 2)

        logger.debug("Divide: ID: " + str(problem.problem_id) + ", mid: " + str(mid) + ", mid+1: " + str(mid+1) + ", to: " + str(problem.to_idx))

        # At some point, we'll want to stop passing the entire array around, as the subproblems only work with a sub-array.
        right_problem = ProblemType(
            numbers = problem.numbers,
            from_idx = mid + 1,
            to_idx = problem.to_idx)
        
        left_problem = ProblemType(
            numbers = problem.numbers,
            from_idx = problem.from_idx,
            to_idx = mid)
        
        logger.debug("Divide: ID: " + str(problem.problem_id) + ": RightProblem: " + str(right_problem))
        logger.debug("Divide: ID: " + str(problem.problem_id) + ": LeftProblem: " + str(left_problem))

        subproblems.append(right_problem)
        subproblems.append(left_problem)
    
    def combine(self, subproblem_results: list, problem_result: ResultType, problem_id: str):
        """
        Combine the subproblem results. 

        This is merge, which ignores from/to values for the subproblems, as it always starts merging from position 0
        and finishes in the last positions of the arrays. The from/to values of a subproblem are w.r.t the original input array.
        """
        logger.debug("combine() called. subproblem_results=%s, problem_result=%s, problem_id=%s" % (str(subproblem_results), str(problem_result), problem_id))
        first_result = subproblem_results[0][0]
        second_result = subproblem_results[1]

        first_array = first_result.numbers
        second_array = second_result.numbers

        values = [None for i in range(len(first_array) + len(second_array))]
        from_idx = 0

        logger.debug("combine: values.length for merged arrays: " + str(len(first_array) + len(second_array)))
        logger.debug("first array: " + str(first_array))
        logger.debug("second array: " + str(second_array))

        li, ri = 0, 0

        # Merge.
        while (li < len(first_array) and ri < len(second_array)):
            if first_array[li] < second_array[ri]:
                values[from_idx] = first_array[li]
                from_idx += 1
                li += 1
            else:
                values[from_idx] = second_array[ri]
                from_idx += 1
                ri += 1
        
        while (li < len(first_array)):
            values[from_idx] = first_array[li]
            from_idx += 1
            li += 1
        
        while (ri < len(second_array)):
            values[from_idx] = second_array[ri]
            from_idx += 1
            ri += 1
        
        logger.debug("combine result: values.length: " + str(len(values)) + ", values: ")

        problem_result.numbers = values 

        if first_result.from_idx < second_result.from_idx:
            problem_result.from_idx = first_result.from_idx
            problem_result.to_idx = second_result.to_idx 
        else:
            problem_result.from_idx = second_result.from_idx
            problem_result.to_idx = first_result.to_idx 

    def computeInputsOfSubproblems(self, problem: ProblemType, subproblems: list):
        """
        User provides method to generate subproblem values, e.g., sub-array to be sorted, from problems.
        The Problem Labels identify a (sub)problem, but we still need a way to generate the subproblem
        data values. For example, if the parent array has values for 0-14, the left array has values for 0-7 and the 
        right array has values for 8-14. The right array will be passed (as an Lambda invocation argument or 
        written to Wukong storage) to a new executor for this subproblem.
        """
        problem_size = problem.to_idx - problem.from_idx + 1
        midArray = 0 + ((len(problem.numbers) - 1) // 2)

        if len(problem.fan_in_stack) >= WukongProblem.INPUT_THRESHOLD:
            logger.debug("computeInputsOfSubproblems ( >= INPUT_THRESHOLD): ID: " + str(problem.problem_id) + ", midArray: " + str(midArray) + ", to: " + str(problem.to_idx))
        else:
            logger.debug("computeInputsOfSubproblems ( < INPUT_THRESHOLD): ID: " + str(problem.problem_id) + ", midArray: " + str(midArray) + ", to: " + str(problem.to_idx))
        
        left_array = []
        right_array = []

        try:
            logger.debug("computeInputsOfSubproblems: problem.numbers: " + str(problem.numbers))
            logger.debug("computeInputsOfSubproblems: ID: " + str(problem.problem_id) + ", len(numbers): " + str(len(problem.numbers)) + ", numbers: " + str(problem.numbers))
            # Assuming that inputNumbers returns the problem's actual from-to subsegment of the complete input.
            
            # Copies are made from the parent problem's sub-segment of the input array, are a prefix of parent's copy, and start with 0.
            logger.debug("computeInputsOfSubproblems: ID: " + str(problem.problem_id) + " size < threshold, make left copy: from: 0 midArray+1 " + str((midArray+1)))
            left_array = problem.numbers[0:midArray + 1]
            logger.debug("computeInputsOfSubproblems: ID: " + str(problem.problem_id) + " size < threshold, make right copy: midArray+1: " + str((midArray+1)) + " to+1 " + str(len(problem.numbers)))
            right_array = problem.numbers[midArray + 1: len(problem.numbers)]

            # Assert 
            if problem_size != len(problem.numbers):
                logger.error("Internal Error: computeInput: size != len(numbers)-1")
                logger.error("computeInputsOfSubproblems: size: " + str(problem_size) + " problem.numbers.length-1: " + str(len(problem.numbers) - 1))
                exit(1)
        except Exception as ex:
            logger.error("Exception encountered during 'computeInputsOfSubproblems()': " + str(ex))
            exit(1)
        
        subproblems[0].numbers = right_array
        subproblems[1].numbers = left_array

    def input_problem(self, problem: ProblemType):
        """
        Alias for 'inputProblem()'
        """
        self.inputProblem(problem)

    def inputProblem(self, problem: ProblemType):
        """
        The problem data must be obtained from Wukong storage. Here, we are getting a specific subsegment of the input array,
        which is the segment from-to.
        """
        logger.debug("inputNumbers")
        
        problem_input_encoded = redis_client.get("input")
        problem_input_serialized = decode_base64(problem_input_encoded)

        numbers = cloudpickle.loads(problem_input_serialized)

        problem_size = problem.to_idx - problem.from_idx + 1

        if problem_size != len(numbers):
            numbers = numbers[problem.from_idx, problem.to_idx + 1]
        
        problem.numbers = numbers
    
    def sequential(self, problem: ProblemType, result: ResultType): 
        """
        User provides method to sequentially solve a problem
        Insertion sort
        """
        # QUESTION: Can I just call the built-in 'sorted' function here?
        # Or am I missing something by doing this (and thus I need to implement insertion sort explicitly)?
        problem.numbers = sorted(problem.numbers)
        result.numbers = problem.numbers
        result.from_idx = problem.from_idx
        result.to_idx = problem.to_idx

    def output_result(self, problem_problemID : str):
        """
        User provides method to output the problem result.
        We only call this for the final result, and this method verifies the final result.

        Note: Used to be a result parameter but that was result at top of template, which is no longer
        the final result since we create a new result object after every combine. The final result 
        is the value in "root".
        """
        resultEncoded = redis_client.get(final_result_id)

        if resultEncoded is None:
            logger.error("Final result (stored under key '" + str(final_result_id) + "' is Null")
            redis_client.set("solution", base64.b64encode(cloudpickle.dumps("ERROR")))
            exit(1)
        else:
            redis_client.set("solution", resultEncoded)
            resultSerialized = decode_base64(resultEncoded)
            result = cloudpickle.loads(resultSerialized)

        logger.debug("MergeSort Output - ProblemID: " + str(problem_problemID))

        # logger.debug("Unsorted: " + str(NUMBERS))

        logger.debug("Sorted: " + str(result.numbers))

        # logger.debug("Expected: " + str(EXPECTED_ORDER))

        # logger.debug("Verifying...")

        # error_occurred = False
        # for i in range(0, len(NUMBERS)):
        #     if result.numbers[i] != EXPECTED_ORDER[i]:
        #         logger.error("Error in expected value: result.numbers[" + str(i) + "]: " + str(result.numbers[i]) + " != expectedOrder[" + str(i) + "]: " + str(EXPECTED_ORDER[i]))
        #         error_occurred = True 

        # if not error_occurred:
        #     logger.debug("Verified.")

NullResult = ResultType(result_type = -1, value = -1)
StopResult = ResultType(result_type = 0, value = -1)