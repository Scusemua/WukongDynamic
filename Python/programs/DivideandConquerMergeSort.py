import logging 
import threading 
import time 

logger = logging.getLogger(__name__)

debug_lock = threading.Lock() 

class ProblemType(WukongProblem):
    """ class ProblemType provided by User. """
    # The threshold at which we switch to a sequential algorithm.
    SEQUENTIAL_THRESHOLD = 2

	# Get input arrays when the level reaches the INPUT_THRESHOLD, e.g., don't grab the initial 256MB array,
	# wait until you reach level , say, 1, when there are two subproblems each half as big.
	# To input at the start of the problem (root), use 0; the stack has 0 elements on it for the root problem.
	# To input the first two subProblems, use 1. Etc.
    INPUT_THRESHOLD = 1    
    
    # Memoize the problem results or not.
    memoize = False 

    def __init__(self):
        self.numbers = list()
        self._from = 0
        self.to = 0 
        self.value = 0 
    
    def __str__(self):
        res = ""
        res = "(from: " + self._from + ", to: " + self._to + ")"
        if (self.numbers is not None):
            for number in self.numbers:
                res += number + " "
        
        return res 

    def __repr__(self):
        return self.__str__()

class ResultType(WukongProblem):
    def __init__(self):
        self.numbers = list()
        self._from = 0
        self.to = 0
    
    def copy(self):
        """
	    Make copy of Problem. Shallow copy works here. This is needed only for the non-Wukong, case, i.e., 
        with no network, since Java is passing local references around among the threads.
	    For Wukong, problems and results will be copied/serialized and sent over the network, so no local copies are needed.
        """
        _copy = ResultType()
        _copy.numbers = self.numbers 
        _copy._from = self._from 
        _copy.to = self.to 

        return _copy 

class User (object):
    """ class User provided by User. """

    @staticmethod
    def base_case(problem : ProblemType) -> bool:
        """ 
        The base case is always a sequential sort (though it could be on an array of 
        length 1, if that is the sequential threshold.)

        Arguments:
        ----------
            problem (ProblemType)
        
        Returns:
        --------
            bool        
        """
        size = problem.to - problem._from + 1
        return size <= ProblemType.SEQUENTIAL_THRESHOLD
    
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

        The field of a problem that will definitely be used is the problem_id. Do not trim the problem_id. 
        The FanInStack (in parent class WukongProblem) is not needed by the DivideandConquer framework and 
        can/should be trimmed. 
        One option is to create a trimProblem() method in call WukongProblem and always call this method (in method
        Fanout) in addition to calling User.trimProblem(), where User.tribProblem may be empty.
        """
        # No need to keep the array of numbers to be sorted after we make a copy of each child's half of the numbers.
        problem.numbers = None 

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
        we would not need to stack the problem IDs as we recurse. We'll have to see if this scheme would work in general,
        i.e., for different numbers of children, when the number of children may vary for parent nodes, etc. There has to be a 
        way to recover the parent ID from a child, either by using a formula liek above, or by stacking the IDs as we recurse.

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
        mid = parent_problem._from + ((parent_problem.to - parent_problem._from) // 2)

        ID = None 

        if childID == 0:
            if (mid + 1) < parent_problem.to:
                ID = str(mid + 1) + "x" + str(parent_problem.to)
            else:
                ID = str(mid + 1)
            return ID 
        else:
            if parent_problem._from < mid:
                ID = str(parent_problem._from) + "x" + str(mid)
            else:
                ID = str(parent_problem._from)
            return ID
        
    @staticmethod
    def memoize_IDLabeler(problem : ProblemType) -> str:
        """
        Used for getting the memoized result of (sub)problem (for get(memoizedLabel, result)).
        MergeSort is not memoized
        """   
        pass         
    
    @staticmethod
    def divide(
        problem : ProblemType,
        subproblems : list 
    ):
        """

        Arguments:
        ----------
            problem (ProblemType)

            subproblems (arraylike of ProblemType)

        Returns:
        --------
            Nothing 
        """
        with debug_lock:
            logger.debug("Divide: merge sort run: from: {} to: {}".format(problem._from, problem.to))
            logger.debug("Divide: problem_id: {}".format(problem.problem_id))
            
            msg = "Divide: FanInStack:"
            for problem_type in problem.fanin_stack:
                msg = msg + "{} ".format(problem_type)
            logger.debug(msg)

        size = problem.to - problem._from + 1
        mid = problem._from + ((problem.to - problem._from) // 2) 

        logger.debug("Divide: ID: {}, mid: {}. mid + 1: {}, to: {}".format(problem.problem_id, mid, (mid + 1), problem.to))

        right = ProblemType()
        right.numbers = problem.numbers
        right._from = mid + 1 
        right.to = problem.to

        left = ProblemType()
        left.numbers = problem.numbers
        left._from = problem._from 
        left.to = mid 

        subproblems.add(right)
        subproblems.add(left)      

    @staticmethod
    def combine(
        subproblem_results : list,
        problem_result : ResultType
    ):
        """
            Combine the subproblem results. 

            This is merge, which ignores from/to values for the subproblems, as it always starts merging from position 0
            and finishes in the last positions of the arrays,
            The from/to values of a subproblem are w.r.t the original input array.

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

        first_array = first_result.numbers 
        second_array = second_result.numbers 

        with debug_lock:
            values = list() 
            _from = 0 

            logger.debug("combine: values.length for merged arrays: {}".format(len(values)))

            logger.debug("first: {}".format(first_array))
            logger.debug("second: {}".format(second_array))

            li = 0
            ri = 0
        
        while (li < len(first_array) and ri < len(second_array)):
            logger.debug("li: {} , len(first_array): {}, ri: {}, len(second_array): {}".format(li, len(first_array), ri, len(second_array)))

            if (first_array[li] < second_array[ri]):
                values[_from] = first_array[li]
                _from += 1
                li += 1
            else:
                values[_from] = second_array[ri]
                _from += 1 
                ri += 1
            
            while (li < len(first_array)):
                values[_from] = first_array[li]
                _from += 1 
                li += 1
            
            while (ri < len(second_array)):
                _from += 1
                ri += 1
        
        logger.debug("combine result: values.length: {}, values: {}".format(len(values), values))

        problem_result.numbers = values 

        if (first_result._from < second_result._from):
            problem_result._from = first_result._from 
            problem_result.to = first_result.to 
        else:
            problem_result._from = second_result._from 
            problem_result.to = second_result.to
    
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

        logger.debug("inputNumbers")

        # Hard-coded the input array.
        numbers = DivideandConquerMergeSort.values

        size = problem.to - problem._from + 1 

        # If the INPUT_THRESHOLD equals the size of the entire input array, then there is no need to 
        # copy the entire input array as we already initialized values to the input array.
        if size != DivideandConquerMergeSort.values.length:
            numbers = numbers.copy()[problem._from, problem.to + 1]
        
        problem.numbers = numbers

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

        Arguments:
        ----------
            problem (ProblemType)

            subproblems (arraylike of ProblemType)
        Returns:
        --------
            Nothing 
        """

        size = problem.to - problem._from + 1
        mid_array = 0 + ((len(problem.numbers) - 1) // 2)

        logger.debug("compute_inputs_of_subproblems ( {} INPUT_THRESHOLD) : ID: {}, mid_array: {}, to: {} "
            .format(">=" if (len(problem.FanInStack) >= WukongProblem.INPUT_THRESHOLD) else "<", problem.problem_id, mid_array, problem.to))
        
        left_array = None 
        right_array = None 

        try:
            with debug_lock:
                logger.debug("compute_inputs_of_subproblems: problem.numbers: {}".format(problem.numbers))
                logger.debug("compute_inputs_of_subproblems: ID: {}, len(numbers): {}, numbers: {}".format(problem.problem_id, len(problem.numbers), numbers))
                
                # Assuming that inputNumbers returns the problem's actual from-to subsegment of the complete input.
                # Copies are made from the parent problem's sub-segment of the input array, are a prefix of parent's copy, and start with 0
                logger.debug("compute_inputs_of_subproblems: ID: {} size < threshold, make left copy: from: 0, mid_array + 1: {}".format(problem.problem_id, mid_array + 1))    
                left_array = problem.numbers.copy()[0: mid_array + 1]
            
                logger.debug("compute_inputs_of_subproblems: ID: {}, size < threshold, make right copy: mid_array + 1: {}, to+1: {}".format(problem.problem_id, mid_array + 1, len(problem.numbers)))
                right_array = problem.numbers.copy()[mid_array + 1, len(problem.numbers)]

                # Assert:
                if (size != len(problem.numbers)):
                    logger.error("Internal Error: computeInput: size != numbers.length-1 (size = {}, problem.numbers.length-1 = {})".format(size, len(problem.numbers) - 1))
                    logger.error("compute_inputs_of_subproblems: size: {}, len(problem.numbers.length) - 1: {}".format(size, len(problem.numbers) - 1))
                    exit(1)
        except Exception as ex:
            logger.error(ex)
            exit(1) 
         
        # The Fan-out task will assign these child sub-arrays to the corresponding child problem.
        subproblems[0].numbers = right_array
        subproblems[1].numbers = left_array

    @staticmethod
    def sequential(
        problem : ProblemType,
        result : ResultType
    ):
        numbers = problem.numbers 
        _from = 0
        to = len(numbers) - 1

        logger.debug("sequential sort: {} to {}".format(_from, to))

        # for(int i = _from + 1; i <= to; i++)
        for i in range(_from + 1, to + 1):
            current = numbers[i]
            j = i - 1
            while (_from <= j and current <= numbers[j]):
                numbers[j+1] = numbers[j]
                j = j - 1
            numbers[j+1] = current 
        
        result.numbers = numbers 
        result._from = problem._from 
        result.to = problem.to

    @staticmethod
    def output_result():
        result = FanInSychronizer.resultMap[DivideandConquerMergeSort.root_problem_ID]

        # MemoizationController.getInstance().stopThread();
        _str = "Sorted ( {} - {} ): ".format(result._from, result.to)
        for num in result.numbers:
            _str = _str + str(num) + " "
        logger.debug(_str)

        _str = "Expected      :"
        for x in DivideandConquerMergeSort.expected_order:
            _str = _str + str(x) + " "
        logger.debug(_str)

        try:
            time.sleep(1)
        except Exception as ex:
            raise ex 
        
        error = False 
        for val in DivideandConquerMergeSort.values:
            if result.numbers[i] != DivideandConquerMergeSort.expected_order[i]:
                logger.debug("Error in expected value: result.numbers[{}] {} != expected_order[{}] {}".format(
                    i, result.numbers[i], i, DivideandConquerMergeSort.expected_order[i]
                ))
                error = True 
        if not error:
            logger.debug("Verified.")

class DivideandConquerMergeSort(object):
    values = [9, -3, 5, 0, 1, 2, -1, 4, 11, 10, 13, 12, 15, 14, 17, 16] # Size 16
    expected_order = [-3, -1, 0, 1, 2, 4, 5, 9, 10, 11, 12, 13, 14, 15, 16, 17]

    root_problem_ID = "root"

# Main method, so to speak.
if __name__ == "__main__":
    logger.debug("Running DivideandConquerMergeSort")
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
        logger.error("Error: ProblemType.SEQUENTIAL_THRESHOLD must be defined.")
        exit(1)
    
    # Assert 
    if len(values) < ProblemType.SEQUENTIAL_THRESHOLD:
        logger.error("Internal Error: unsorted array must have length at least SEQUENTIAL_THRESHOLD, which is the base case.")
        exit(1)
    
    logger.debug("Numbers: ")
    _str = ""
    for num in values:
        _str = _str + str(num) + " "
    logger.debug(_str)

    rootID = str(0) + "x" + str(len(values) - 1)
    fanin_stack = list()

    root_problem = ProblemType()
    root_problem.numbers = None 
    root_problem._from = 0
    root_problem.to = len(values) - 1

    logger.debug(root_problem)

    root_problem.fanin_stack = fanin_stack
    root_problem.problem_id = root_problem_ID

    root = DivideAndConquerExecutor(root_problem)

    root.run()

