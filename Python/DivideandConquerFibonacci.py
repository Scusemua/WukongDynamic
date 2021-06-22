
import logging 
import threading 
import time 

import DivideAndConquerExecutor
from DivideAndConquerExecutor import WukongProblem, MemoizationController

logger = logging.getLogger(__name__)

debug_lock = threading.Lock() 

class ProblemType(WukongProblem):
    """ class ProblemType provided by User. """
    # The threshold at which we switch to a sequential algorithm.
    SEQUENTIAL_THRESHOLD = 1

	# Get input arrays when the level reaches the INPUT_THRESHOLD, e.g., don't grab the initial 256MB array,
	# wait until you reach level , say, 1, when there are two subproblems each half as big.
	# To input at the start of the problem (root), use 0; the stack has 0 elements on it for the root problem.
	# To input the first two subProblems, use 1. Etc.
    # INPUT_THRESHOLD = 1    
    
    # Memoize the problem results or not.
    memoize = False 

    def __init__(self):
        # self.numbers = list()
        # self._from = 0
        # self.to = 0 
        self.value = 0 
    
    def __str__(self):
        return "(ID: " + self.problemID + ", value: " + self.value + ")"

    def __repr__(self):
        return self.__str__()

class ResultType(WukongProblem):
    def __init__(self):
        self.value = 0
    
    def copy(self):
        """
	    Make copy of Problem. Shallow copy works here. This is needed only for the non-Wukong, case, i.e., 
        with no network, since Java is passing local references around among the threads.
	    For Wukong, problems and results will be copied/serialized and sent over the network, so no local copies are needed.
        """
        _copy = ResultType()
        _copy.value = self.value

        return _copy 
    
    def __str__(self):
        return "(ID: " + self.problemID + ", value: " + self.value + ")"

class User(object):
    """ class User provided by User. """

    @staticmethod
    def base_case(problem : ProblemType) -> bool:
        """ 
        No non-parallel algorithm for Fibonacci. 
        SEQUENTIAL_THRESHOL must be 1. 

        Arguments:
        ----------
            problem (ProblemType)
        
        Returns:
        --------
            bool        
        """
        if ProblemType.SEQUENTIAL_THRESHOLD != 1:
            logger.error("Internal Error: baseCase: Fibonacci base case must be value <= 1.")
            logger.fatal("Internal Error: baseCase: ProblemType.SEQUENTIAL_THRESHOLD set to: " + ProblemType.SEQUENTIAL_THRESHOLD)

        value = problem.value 
        return value <= ProblemType.SEQUENTIAL_THRESHOLD
    
    @staticmethod
    def preprocess(problem : ProblemType) -> bool:
        """
        Some problems may use a pre-processing step before anything else is done.

        Arguments:
        ----------
            problem (ProblemType)
        
        Returns:
        --------
            bool        
        """
        pass 

    @staticmethod
    def trim_problem(problem : ProblemType):
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
        # We are including a parent's stack when we create a child's stack, and we do not need the parent's stack anymore.
        problem.FanInStack = None # Defined in class WukongProblem

    @staticmethod 
    def problem_labeler(
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
        with debug_lock:
            logger.debug("labeler: subProblem ID: " + subproblem.problemID + ", parent ID: " + parent_problem.problemID)

            logger.debug("Parent stack:")
            for i in range(len(parent_problem.FanInStack)):
                logger.debug(parent_problem.FanInStack[i] + " ")

        return str(subproblem.value)
        
    @staticmethod
    def memoize_IDLabeler(problem : ProblemType) -> str:
        """
        Used for getting the memoized result of (sub)problem (for get(memoizedLabel, result)).
        """   
        memoizedID = None 
        label = problem.problemID

        # Grab the last token in token1 - token2 - ... - tokenLast.
        lastIndex = label.rindex('-')

        if lastIndex == -1:
            # no '-', e.g., for Fibonacci(4), problem label is "4", which has no '-' so memoize label is also "4"
            memoizedID = label 
        else:
            # e.g., for Fibonacci(4), problem label for Fibonacci(3) is "4-3", which has '-' so memoize label is "3"
            # which is the problem label for Fibonacci(3).
            memoizedID = label[lastIndex + 1, len(label)]
        
        return memoizedID
    
    @staticmethod
    def divide(
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
        with debug_lock:
            logger.debug("Divide: fibonacci run: value: " + str(problem.value))
            logger.debug("Divide: problemID: " + str(problem.problemID))
            logger.debug("Divide: FanInStack:")

            for x in problem.FanInStack:
                logger.debug(str(x) + " ")

            logger.debug("Divide: merge sort run: from: {} to: {}".format(problem._from, problem.to))
            logger.debug("Divide: problemID: {}".format(problem.problemID))

        minus_1 = ProblemType()
        minus_1.value = problem.value - 1

        minus_2 = ProblemType()
        minus_2.value = problem.value - 2

        with debug_lock:
            logger.debug("divide: minus_1: " + minus_1)
            logger.debug("divide: minus_2: " + minus_2)
            
        subproblems.append(minus_2)
        subproblems.append(minus_1)

    @staticmethod
    def combine(
        subproblem_results : list,
        combination : ResultType
    ):
        """
            Combine the subproblem results. 

            Ignores from/to values for the subproblems, as it always starts merging from position 0
            and finishes in the last positions of the arrays,
            The from/to values of a subproblem are w.r.t the original input array.
        
            Simple merge: place left array values before right array values., but we don't know
            which subproblem is left and which is right, so check their first values.
                

            Arguments:
            ----------
                subproblem_results (arraylike of ResultType)

                problemResult (ResultType)       

            Returns:
            --------
                Nothing
        """
        first_result = subproblem_results.get(0)
        second_result = subproblem_results.get(1)

        first_value = first_result.value 
        second_value = second_result.value 

        combination.value = first_value + second_value

        with debug_lock:
            logger.debug("combine: firstValue: " + str(first_value) + " secondValue: " + str(second_value) + " combination.value: " + combination.value)
    
    @staticmethod
    def input_problem(problem : ProblemType):
        """
        The problem data must be obtained from Wukong storage. Here, we are getting a specific subsegment of the input array, which is the segment from-to.

        Arguments:
        ----------
            problem (ProblemType)
        
        Returns:
        --------
            Nothing
        """
        pass 

    @staticmethod
    def compute_inputs_of_subproblems(
        problem : ProblemType,
        subproblems : list
    ):
        """
        User provides method to generate subproblem values, e.g., sub-array to be sorted, from problems.
        The Problem Labels identify a (sub)problem, but we still need a way to generate the subproblem
        data values. For example, if the parent array has values for 0-14, the left array has values for 0-7 and the 
        right array has values for 8-14. The right array will be passed (as an Lambda invocation argument or 
        written to Wukong storage) to a new executor for this subproblem.
        """
        pass 

    @staticmethod
    def sequential(
        problem : ProblemType,
        result : ResultType
    ):
        result.value = problem.value 
        
        with debug_lock:
            logger.debug("sequential: " + str(problem.problemID) + " result.value: " + str(result.value))

    @staticmethod
    def output_result():
        """
        User provides method to output the problem result.
        We only call this for the final result, and this method verifies the final result.

		Note: Used to be a result parameter but that was result at topof template, which is no longer
		the final result since we create a new result object after every combine. The final result 
		is the value in "root".
        """
        result = FanInSynchronizer.resultMap[DivideandConquerFibonacci.root_problem_id]

        MemoizationController.getInstance().stopThread()

        logger.debug("Fibonacci(" + DivideandConquerFibonacci.n + ") = " + str(result.value))
        logger.debug("Verifying...")

        time.sleep(2)

        error = False 

        if result.value != DivideandConquerFibonacci.expected_value:
            error = True 
        
        if not error:
            logger.debug("Verified.")
        else:
            logger.error("Error. Unexpected final result.")

class DivideandConquerFibonacci(object):
    n = 4
    expected_value = 3
    root_problem_id = "root"

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
    rootProblem = ProblemType()

    rootProblem.n = DivideandConquerFibonacci.n

    logger.debug("Root Problem: " + str(rootProblem))

    rootProblem.FanInStack = FanInStack
    rootProblem.problemID = DivideandConquerFibonacci.root_problem_id

    root = DivideAndConquerExecutor()
    root.run()