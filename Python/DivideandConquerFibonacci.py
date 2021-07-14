
import logging 
import threading 
import time 

from WukongProblem import WukongProblem, FanInSychronizer, WukongResult
from UserProgram import UserProgram

import DivideAndConquerExecutor
#from DivideAndConquerExecutor import WukongProblem, MemoizationController, DivideAndConquerExecutor

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

debug_lock = threading.Lock() 

class ResultType(WukongResult):
    def __init__(self):
        super(ResultType, self).__init__()
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
        parent_to_string = super(ResultType, self).__str__()
        return parent_to_string + ": (ID: " + str(self.problemID) + "/value: " + str(self.value) + ")"

class ProblemType(WukongProblem):
    """ class ProblemType provided by User. """
    # The threshold at which we switch to a sequential algorithm.
    SEQUENTIAL_THRESHOLD = 1

	# Get input arrays when the level reaches the INPUT_THRESHOLD, e.g., don't grab the initial 256MB array,
	# wait until you reach level , say, 1, when there are two subproblems each half as big.
	# To input at the start of the problem (root), use 0; the stack has 0 elements on it for the root problem.
	# To input the first two subProblems, use 1. Etc.
    # INPUT_THRESHOLD = 1    
    
    # OUTPUT_THRESHOLD = 1

    # Memoize the problem results or not.
    # memoize = False 

    def __init__(self, value = 0, UserProgram = None):
        super(ProblemType, self).__init__(UserProgram = UserProgram)
        self.value = value
    
    def __str__(self):
        return "(ID: " + str(self.problemID) + ", value: " + str(self.value) + ")"

    def __repr__(self):
        return self.__str__()

    @property
    def memoize():
        return False        

class FibbonaciProgram(UserProgram):
    """ class User provided by User. """

    def trimProblem(self, problem : ProblemType):
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
        Fanout) in addition to calling User.trimProblem(), where User.tribProblem may be empty..
        """
        # We are cloning a parent's stack when we create a child's stack, and we do not need the parent's stack anymore.
        problem.FanInStack = None 

    #@staticmethod
    def base_case(self, problem : ProblemType) -> bool:
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

    def outputResult(self, problem_problemID : str):
        result = None 

        with debug_lock:
            result = FanInSychronizer.resultMap["root"]
        
        logger.debug("")
        logger.debug(problem_problemID + ": Fibonacci(" + str(DivideandConquerFibonacci.n) + ") = " + str(result.value))
        logger.debug("")

        logger.debug(problem_problemID + ": Verifying ....... ")
        time.sleep(2)
        error = False 
        if result.value != DivideandConquerFibonacci.expected_value:
            error = True 
        
        if not error:
            logger.debug("Verified.")
        else:
            logger.debug("Error. Expected value: %s, actual value: %s" % (str(DivideandConquerFibonacci.expected_value), str(result.value)))

    def problemLabeler(self, subProblem : ProblemType, childId : int, parentProblem : ProblemType, subProblems : list) -> str:
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
        """
        with debug_lock:
            #logger.debug("labeler: subProblem ID: " + str(subProblem.problemID) + " parent ID: " + str(parentProblem.problemID))
            logger.debug(str(parentProblem.problemID) + ": label subProblem ID (assigned in Fan-out): " + str(subProblem.problemID) + " parent ID: " + str(parentProblem.problemID))

            logger.debug(parentProblem.problemID + ":  labler: parent stack: ")
            stack_string = ""
            for i in range(0, len(parentProblem.FanInStack)):
                stack_string += str(parentProblem.FanInStack[i]) + " "
            logger.debug(stack_string)
            
        label = str(subProblem.value)

        with debug_lock:
            logger.debug(parentProblem.problemID + ": labler: generated subProblem Label: " + label)
        
        return label

    def computeInputsOfSubproblems(self, problem : ProblemType, subProblems : list):
        pass

    # ------
    # As in:
    # ------
    #
    # int fib(int n) {
    #   if(n<=1)
    #   return n;
    #   return fib(n-1) + fib(n-2);
    # }
    #@staticmethod
    def preprocess(self, problem : ProblemType) -> bool:
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

    #@staticmethod
    def trim_problem(self, problem : ProblemType):
        """
        A problem P is by default passed to the executors that will be executing the child subproblems of P. One of these
        executors will execute method combine() for these subproblems. In some cases, it is easier to write combine() when 
        the subproblem's parent problem data is available; however, this parent data will also be sent and retrieved from 
        storage if the parent data is part of the ProblemType data. (That is, problem P's parent data will be sent 
        as part of problem P since the parent data will be on the stack of subProblems (representing the call stack) for P.) 
        So if some of the parent data is not needed for combine() it should be deleted from the parent problem.

        The field of a problem that will definitely be used is the problemID. Do not trim the problemID. 
        The FanInStack (in parent class DivideAndConquerExecutor.WukongProblem) is not needed by the DivideandConquer framework and 
        can/should be trimmed. 
        One option is to create a trimProblem() method in call DivideAndConquerExecutor.WukongProblem and always call this method (in method
        Fanout) in addition to calling User.trimProblem(), where User.tribProblem may be empty.
        """
        # We are including a parent's stack when we create a child's stack, and we do not need the parent's stack anymore.
        problem.FanInStack = None # Defined in class DivideAndConquerExecutor.WukongProblem

    def inputProblem(self, problem : ProblemType):
        """
        Not used for Fibonacci.
        """
        pass 

    #@staticmethod 
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
        with debug_lock:
            logger.debug("labeler: subProblem ID: " + subproblem.problemID + ", parent ID: " + parent_problem.problemID)

            logger.debug("Parent stack:")
            for i in range(len(parent_problem.FanInStack)):
                logger.debug(parent_problem.FanInStack[i] + " ")

        return str(subproblem.value)
        
    #@staticmethod
    def memoize_IDLabeler(self, problem : ProblemType) -> str:
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
    
    #@staticmethod
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
        with debug_lock:
            logger.debug(problem.problemID + ": Divide: fibonacci run: value: " + str(problem.value))
            logger.debug(problem.problemID + ": Divide: problemID: " + str(problem.problemID))
            stack_string = problem.problemID + ": Divide: FanInStack:"
            for x in problem.FanInStack:
                stack_string += str(x) + " "
            
            logger.debug(stack_string)

            logger.debug(problem.problemID + ": Divide: problemID: {}".format(problem.problemID))

        minus_1 = ProblemType()
        minus_1.value = problem.value - 1

        minus_2 = ProblemType()
        minus_2.value = problem.value - 2

        with debug_lock:
            logger.debug(problem.problemID + ": divide: minus_1: " + str(minus_1))
            logger.debug(problem.problemID + ": divide: minus_2: " + str(minus_2))
            
        subproblems.append(minus_2)
        subproblems.append(minus_1)

    #@staticmethod
    def combine(self, 
        subproblem_results : list,
        combination : ResultType,
        problemID : str
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
        first_result = subproblem_results[0]
        second_result = subproblem_results[1]

        first_value = first_result.value 
        second_value = second_result.value 

        combination.value = first_value + second_value

        with debug_lock:
            logger.debug(problemID + ": combine: firstValue: " + str(first_value) + " secondValue: " + str(second_value) + " combination.value: " + str(combination.value))
    
    #@staticmethod
    def input_problem(self, problem : ProblemType):
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

    #@staticmethod
    def compute_inputs_of_subproblems(self, 
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

    #@staticmethod
    def sequential(self, 
        problem : ProblemType,
        result : ResultType
    ):
        result.value = problem.value 
        
        with debug_lock:
            logger.debug(str(problem.problemID) + ": Sequential: " + str(problem.problemID) + " result.value: " + str(result.value))

    #@staticmethod
    def output_result(self):
        """
        User provides method to output the problem result.
        We only call this for the final result, and this method verifies the final result.

		Note: Used to be a result parameter but that was result at topof template, which is no longer
		the final result since we create a new result object after every combine. The final result 
		is the value in "root".
        """
        result = FanInSychronizer.resultMap[DivideandConquerFibonacci.root_problem_id]

        DivideAndConquerExecutor.MemoizationController.getInstance().stopThread()

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
# if __name__ == "__main__":
#     logger.debug("Running DivideandConquerFibonacci")
#     logger.debug("INPUT_THRESHOLD is: {}".format(WukongProblem.INPUT_THRESHOLD))
#     logger.debug("OUTPUT_THRESHOLD is: {}".format(WukongProblem.OUTPUT_THRESHOLD))
#     logger.debug("SEQUENTIAL_THRESHOLD is: {}".format(ProblemType.SEQUENTIAL_THRESHOLD))
#     logger.debug("memoize is: " + str(ProblemType.memoize))

#     # Assert 
#     seq = None 
#     try:
#         seq = getattr(ProblemType, "SEQUENTIAL_THRESHOLD", None)
#     except Exception:
#         pass 

#     if seq is None:
#         logger.fatal("ProblemType.SEQUENTIAL_THRESHOLD must be defined.")
        
#     logger.debug("n: " + str(DivideandConquerFibonacci.n))
    
#     rootID = str(DivideandConquerFibonacci.n)
#     FanInStack = list() 
#     rootProblem = ProblemType()

#     rootProblem.n = DivideandConquerFibonacci.n

#     logger.debug("Root Problem: " + str(rootProblem))

#     rootProblem.FanInStack = FanInStack
#     rootProblem.problemID = DivideandConquerFibonacci.root_problem_id

#     root = DivideAndConquerExecutor.DivideAndConquerExecutor(rootProblem)
#     root.run()

"""

Execution Trace:

main: Running DivideandConquerFibonacci.
main: INPUT_THRESHOLD is: 0
main: OUTPUT_THRESHOLD is: 2147483647
main: SEQUENTIAL_THRESHOLD (base_case) is: 1
main: memoize is: true
main: n: 4

//        root           where root=Fibonacci(4)
//     /         \
//     3         2
//    /    \    /      \
//  2     1   1     0
// /  \
//1   0

// Note: This does not reflect how the Fan-ins were processed - only the values returned by the Fan-ins
// root-2-0 returns 0
// root-3-1 returns 1
// root-2-1 returns 1, a memoized result from 3-1
// root-2 returns 1+0=1
// root-3-2 returns memoized result from root-2, which was 1 + 0 = 1
//     so root-3-2-1 and root-3-2-0 are never called/computed
// root-3 returns 1+1=2
// root returns 2 + 1 = 3

// Note: Fan-in task (i.e., executor that becomes parent task) is always the Left child of parent
//  This trace was created using ServerLess Networking simulation.

// Fan-in processing:
// root-3-1 delivered 1, but was not Fan-in task for root-3 since root-3-1 is a Right child of root-3
// root-3-2 got "stop" since root-2 promised to compute result for problem "2" before root-3-2 promised; then
//  upon restart root-3-2 got memoized result 1 delivered by root-2; then was (the Left) fan-in task for         root 3
//  with result 1+1=2; then was (the Left) fan-in task for root with result 2+1=3.
// root-2-0 delivered 0, but was not fan-in task for root-2 since root-2-0 is a Right child of root-2
// root-2-1 got stop and upon restart got 1 from memoized 3-1; then was (Left) fan-in task for root-2          with result = 1 + 0 = 1,
//  but was not fan-in task for root since root-2 it is Right child of root


main: (ID:root/value:4)
root: Executor: root call pair on DivideAndConquerExecutor.MemoizationController
root: channelMap keySet:root,
root: Executor: memoized send1: PROMISEVALUE: problem.problemID root memoizedLabel: root
root: Executor: memoized rcv1: problem.problemID root receiving ack.
root: Executor: memoized rcv1: problem.problemID root received ack.
root: Executor: memoized rcv1: problem.problemID root ack was null_result.
root: Executor: memoized rcv1: problem.problemID root memoizedLabel: root memoized result: null
root: Divide: fibonacci run: value: 4
root: Divide: problemID: root
root: Divide: FanInStack:
root: Divide: minus_1: (ID:null/value:3)
root: Divide: minus_2: (ID:null/value:2)
root: Fanout: get subProblemID for non-become task.
root: label subProblem ID (assigned in Fan-out): null parent ID: root
root:  labler: parent stack:
root: labler: generated subProblem Label: 2
root: Fanout: push on childFanInStack: (poarent) problem: (ID:root/value:4)
root: Fanout: parent stack:
root: Fanout: subProblem stack: (ID:root/value:4)
root: Fanout: send ADDPAIRINGNAME message.
root: Fanout: ID: root invoking new right executor: root-2
root: Fanout: get subProblemID for become task.
root: label subProblem ID (assigned in Fan-out): null parent ID: root
root:  labler: parent stack:
root: labler: generated subProblem Label: 3
root: Fanout: ID: root becoming left executor: root-3

root-2: Executor: root-2 call pair on DivideAndConquerExecutor.MemoizationController
root-2: channelMap keySet:root,root-2,
root-2: Executor: memoized send1: PROMISEVALUE: problem.problemID root-2 memoizedLabel: 2
root-2: Executor: memoized rcv1: problem.problemID root-2 receiving ack.
root-2: Executor: memoized rcv1: problem.problemID root-2 received ack.
root-2: Executor: memoized rcv1: problem.problemID root-2 ack was null_result.
root-2: Executor: memoized rcv1: problem.problemID root-2 memoizedLabel: 2 memoized result: null   // Q"correct place for this?
root-2: Divide: fibonacci run: value: 2
root-2: Divide: problemID: root-2
root-2: Divide: FanInStack: (ID:root/value:4)
root-2: Divide: minus_1: (ID:null/value:1)
root-2: Divide: minus_2: (ID:null/value:0)
root-2: Fanout: get subProblemID for non-become task.
root-2: label subProblem ID (assigned in Fan-out): null parent ID: root-2
root-2:  labler: parent stack: (ID:root/value:4)
root-2: labler: generated subProblem Label: 0
root-2: Fanout: push on childFanInStack: (poarent) problem: (ID:root-2/value:2)
root-2: Fanout: parent stack: (ID:root/value:4)
root-2: Fanout: subProblem stack: (ID:root/value:4) (ID:root-2/value:2)
root-2: Fanout: send ADDPAIRINGNAME message.
root-2: Fanout: ID: root-2 invoking new right executor: root-2-0
root-2: Fanout: get subProblemID for become task.
root-2: label subProblem ID (assigned in Fan-out): null parent ID: root-2
root-2:  labler: parent stack: (ID:root/value:4)
root-2: labler: generated subProblem Label: 1
root-2: Fanout: ID: root-2 becoming left executor: root-2-1

root-3: Executor: root-3 call pair on DivideAndConquerExecutor.MemoizationController
root-3: channelMap keySet:root-3,root,root-2,
root-3: Executor: memoized send1: PROMISEVALUE: problem.problemID root-3 memoizedLabel: 3
root-3: Executor: memoized rcv1: problem.problemID root-3 receiving ack.
root-3: Executor: memoized rcv1: problem.problemID root-3 received ack.
root-3: Executor: memoized rcv1: problem.problemID root-3 ack was null_result.
root-3: Executor: memoized rcv1: problem.problemID root-3 memoizedLabel: 3 memoized result: null
root-3: Divide: fibonacci run: value: 3
root-3: Divide: problemID: root-3
root-3: Divide: FanInStack: (ID:root/value:4)
root-3: Divide: minus_1: (ID:null/value:2)
root-3: Divide: minus_2: (ID:null/value:1)
root-3: Fanout: get subProblemID for non-become task.
root-3: label subProblem ID (assigned in Fan-out): null parent ID: root-3
root-3:  labler: parent stack: (ID:root/value:4)
root-3: labler: generated subProblem Label: 1
root-3: Fanout: push on childFanInStack: (poarent) problem: (ID:root-3/value:3)
root-3: Fanout: parent stack: (ID:root/value:4)
root-3: Fanout: subProblem stack: (ID:root/value:4) (ID:root-3/value:3)
root-3: Fanout: send ADDPAIRINGNAME message.
root-3: Fanout: ID: root-3 invoking new right executor: root-3-1
root-3: Fanout: get subProblemID for become task.
root-3: label subProblem ID (assigned in Fan-out): null parent ID: root-3
root-3:  labler: parent stack: (ID:root/value:4)
root-3: labler: generated subProblem Label: 2
root-3: Fanout: ID: root-3 becoming left executor: root-3-2

root-2-0: Executor: root-2-0 call pair on DivideAndConquerExecutor.MemoizationController
root-2-0: channelMap keySet:root-3,root-2-0,root,root-2,
root-2-0: Executor: memoized send1: PROMISEVALUE: problem.problemID root-2-0 memoizedLabel: 0
root-2-0: Executor: memoized rcv1: problem.problemID root-2-0 receiving ack.
root-2-0: Executor: memoized rcv1: problem.problemID root-2-0 received ack.
root-2-0: Executor: memoized rcv1: problem.problemID root-2-0 ack was null_result.
root-2-0: Sequential: root-2-0 result.value: 0
root-2-0: Executor: base case: result before ProcessBaseCase(): (ID:null: (ID:null/value:0)
root-2-0: Executor: ProcessBaseCase result: (ID:root-2-0: (ID:root-2-0/value:0)
root-2-0: **********************Start Fanin operation:
root-2-0: Fan-in: ID: root-2-0
root-2-0: Fan-in: becomeExecutor: false
root-2-0: Fan-in: FanInStack: (ID:root/value:4) (ID:root-2/value:2)
root-2-0: Deliver starting Executors for promised Results:
root-2-0: Deliver end promised Results:
root-2-0: Fan-in: ID: root-2-0 parentProblem ID: root-2
root-2-0: Fan-in: ID: root-2-0 problem.becomeExecutor: false parentProblem.becomeExecutor: false
root-2-0: Fan-In: ID: root-2-0: FanInID: root-2 was not FanInExecutor:  result sent:(ID:root-2-0: (ID:root-2-0/value:0)
root-2-0: Fan-In: ID: root-2-0: FanInID: root-2: is not become Executor  and its value was: (ID:root-2-0: (ID:root-2-0/value:0) and after put is null



// Note: Does not reflect interleaving of calls to MC, e.g., 3-2's pair below occurred before this 2-1 pair
root-2-1: Executor: root-2-1 call pair on DivideAndConquerExecutor.MemoizationController
root-2-1: channelMap keySet:root-3,root-2-1,root-2-0,root,root-3-1,root-2,
root-2-1: Executor: memoized send1: PROMISEVALUE: problem.problemID root-2-1 memoizedLabel: 1
root-2-1: Executor: memoized rcv1: problem.problemID root-2-1 receiving ack.
root-2-1: Executor: memoized rcv1: problem.problemID root-2-1 ack was stop.

// Note: Calling pair() again due to previous "Stop" on PROMISEVALUE
root-2-1: Executor: root-2-1 call pair on DivideAndConquerExecutor.MemoizationController
root-2-1: channelMap keySet:root-3,root-2-1,root-2-0,root-3-2,root,root-3-1,root-2,
root-2-1: Executor: memoized send1: PROMISEVALUE: problem.problemID root-2-1 memoizedLabel: 1
root-2-1: Executor: memoized rcv1: problem.problemID root-2-1 receiving ack.
root-2-1: Executor: memoized rcv1: problem.problemID root-2-1 received ack.
root-2-1: Executor: memoized rcv1: problem.problemID root-2-1 memoizedLabel: 1 memoized result: (ID:root-2-1: (ID:root-2-1/value:1)
root-2-1: Executor: else in template: For Problem: (ID:root-2-1/value:0); Memoized result: (ID:root-2-1: (ID:root-2-1/value:1)

root-2-1: **********************Start Fanin operation:
root-2-1: Fan-in: ID: root-2-1
root-2-1: Fan-in: becomeExecutor: true
root-2-1: Fan-in: FanInStack: (ID:root/value:4) (ID:root-2/value:2)
root-2-1: Fan-in: ID: root-2-1 parentProblem ID: root-2
root-2-1: Fan-in: ID: root-2-1 problem.becomeExecutor: true parentProblem.becomeExecutor: false
root-2-1: Fan-in: root-3-2,root-2-1: ID: root-2-1: FanIn: root-2 was FanInExecutor: result received:(ID:root-2-0: (ID:root-2-0/value:0)
root-2-1: FanIn: ID: root-2-1: FanInID: root-2: : Returned from put: executor isLastFanInExecutor
root-2-1: (ID:root-2-0: (ID:root-2-0/value:0)
root-2-1: ID: root-2-1: call combine ***************
root-2-1: Combine: firstValue: 0 secondValue: 1 combination.value: 1

root-2-1: Exector: result.problemID: root-2-1 put memoizedLabel: 2 result: ID:root-2-1: (ID:root-2-1/value:1)
root-2-1: Deliver starting Executors for promised Results:
root-2-1: Deliver starting Executor for: root-3-2 problem.becomeExecutor: true problem.didInput: true
root-2-1: Deliver end promised Results:
root-2-1: Fan-in: ID: root-2-1 parentProblem ID: root
root-2-1: Fan-in: ID: root-2-1 problem.becomeExecutor: true parentProblem.becomeExecutor: false
root-2-1: Fan-In: ID: root-2-1: FanInID: root was not FanInExecutor:  result sent:(ID:root-2-1: (ID:root-2-1/value:1)
root-2-1: Fan-In: ID: root-2-1: FanInID: root: is not become Executor  and its value was: (ID:root-2-1: (ID:root-2-1/value:1) and after put is null

root-3-1: Executor: root-3-1 call pair on DivideAndConquerExecutor.MemoizationController
root-3-1: channelMap keySet:root-3,root-2-1,root-2-0,root,root-3-1,root-2,
root-3-1: Executor: memoized send1: PROMISEVALUE: problem.problemID root-3-1 memoizedLabel: 1
root-3-1: Executor: memoized rcv1: problem.problemID root-3-1 receiving ack.
root-3-1: Executor: memoized rcv1: problem.problemID root-3-1 received ack.
root-3-1: Executor: memoized rcv1: problem.problemID root-3-1 ack was null_result.
root-3-1: Executor: memoized rcv1: problem.problemID root-3-1 memoizedLabel: 1 memoized result: null

root-3-1: Sequential: root-3-1 result.value: 1
root-3-1: Executor: base case: result before ProcessBaseCase(): (ID:null: (ID:null/value:1)
root-3-1: Executor: ProcessBaseCase result: ID:root-3-1: (ID:root-3-1/value:1)
root-3-1: Deliver starting Executors for promised Results:
root-3-1: Deliver starting Executor for: root-2-1 problem.becomeExecutor: true problem.didInput: true
root-3-1: Deliver end promised Results:
root-3-1: **********************Start Fanin operation:
root-3-1: Fan-in: ID: root-3-1
root-3-1: Fan-in: becomeExecutor: false
root-3-1: Fan-in: FanInStack: (ID:root/value:4) (ID:root-3/value:3)
root-3-1: Fan-in: ID: root-3-1 parentProblem ID: root-3
root-3-1: Fan-in: ID: root-3-1 problem.becomeExecutor: false parentProblem.becomeExecutor: true
root-3-1: Fan-In: ID: root-3-1: FanInID: root-3 was not FanInExecutor:  result sent:(ID:root-3-1: (ID:root-3-1/value:1)
root-3-1: Fan-In: ID: root-3-1: FanInID: root-3: is not become Executor  and its value was: (ID:root-3-1: (ID:root-3-1/value:1) and after put is null

root-3-2: Executor: root-3-2 call pair on DivideAndConquerExecutor.MemoizationController
root-3-2: channelMap keySet:root-3,root-2-1,root-2-0,root-3-2,root,root-3-1,root-2,
root-3-2: Executor: memoized send1: PROMISEVALUE: problem.problemID root-3-2 memoizedLabel: 2
root-3-2: Executor: memoized rcv1: problem.problemID root-3-2 receiving ack.
root-3-2: Executor: memoized rcv1: problem.problemID root-3-2 received ack.
root-3-2: Executor: memoized rcv1: problem.problemID root-3-2 ack was stop.
root-3-2: Executor: root-3-2 call pair on DivideAndConquerExecutor.MemoizationController
root-3-2: channelMap keySet:root-3,root-2-1,root-2-0,root-3-2,root,root-3-1,root-2,
root-3-2: Executor: memoized send1: PROMISEVALUE: problem.problemID root-3-2 memoizedLabel: 2
root-3-2: Executor: memoized rcv1: problem.problemID root-3-2 receiving ack.
root-3-2: Executor: memoized rcv1: problem.problemID root-3-2 received ack.
root-3-2: Executor: memoized rcv1: problem.problemID root-3-2 memoizedLabel: 2 memoized result: (ID:root-3-2: (ID:root-3-2/value:1)
root-3-2: Executor: else in template: For Problem: (ID:root-3-2/value:0); Memoized result: (ID:root-3-2: (ID:root-3-2/value:1)
root-3-2: **********************Start Fanin operation:
root-3-2: Fan-in: ID: root-3-2
root-3-2: Fan-in: becomeExecutor: true
root-3-2: Fan-in: FanInStack: (ID:root/value:4) (ID:root-3/value:3)
root-3-2: Fan-in: ID: root-3-2 parentProblem ID: root-3
root-3-2: Fan-in: ID: root-3-2 problem.becomeExecutor: true parentProblem.becomeExecutor: true
root-3-2: ID: root-3-2: FanIn: root-3 was FanInExecutor: starting receive.
root-3-2: ID: root-3-2: FanIn: root-3 was FanInExecutor: result received:(ID:root-3-1: (ID:root-3-1/value:1)
root-3-2: FanIn: ID: root-3-2: FanInID: root-3: : Returned from put: executor isLastFanInExecutor
root-3-2: (ID:root-3-1: (ID:root-3-1/value:1)
root-3-2: ID: root-3-2: call combine ***************
root-3-2: Combine: firstValue: 1 secondValue: 1 combination.value: 2
root-3-2: Exector: result.problemID: root-3-2 put memoizedLabel: 3 result: ID:root-3-2: (ID:root-3-2/value:2)
// Note: May be that no Exeutors are waiting for the results
root-3-2: Deliver starting Executors for promised Results:
root-3-2: Deliver end promised Results:
root-3-2: Fan-in: ID: root-3-2 parentProblem ID: root
root-3-2: Fan-in: ID: root-3-2 problem.becomeExecutor: true parentProblem.becomeExecutor: false
root-3-2: ID: root-3-2: FanIn: root was FanInExecutor: starting receive.
root-3-2: ID: root-3-2: FanIn: root was FanInExecutor: result received:(ID:root-2-1: (ID:root-2-1/value:1)
root-3-2: FanIn: ID: root-3-2: FanInID: root: : Returned from put: executor isLastFanInExecutor
root-3-2: (ID:root-2-1: (ID:root-2-1/value:1)
root-3-2: ID: root-3-2: call combine ***************
root-3-2: Combine: firstValue: 1 secondValue: 2 combination.value: 3
root-3-2: Exector: result.problemID: root-3-2 put memoizedLabel: root result: (ID:root-3-2: (ID:root-3-2/value:3)
root-3-2: Deliver starting Executors for promised Results:
root-3-2: Deliver end promised Results:
root-3-2: Executor: Writing the final value to root: (ID:root-3-2: (ID:root-3-2/value:3)

MemoizationThread: Interrupted: returning.

root-3-2: Fibonacci(4) = 3
root-3-2: Verifying .......

Verified.


***************** MemoizationThread Trace:******************
root: MemoizationThread: pair: pairingName: root
root: MemoizationThread: promise by: root

root: MemoizationThread: add pairing name: root-2
root: MemoizationThread: ADDPAIRINGNAME: pairing names after add: root,root-2,

root: MemoizationThread: add pairing name: root-3
root: MemoizationThread: ADDPAIRINGNAME: pairing names after add: root-3,root,root-2,

root: MemoizationThread: remove pairing name: root pairingNames.size: 3
root: MemoizationThread: pairing names after remove rootroot-3,root-2,root-2:


root-2: MemoizationThread: pair: pairingName: root-2
root-2: MemoizationThread: promise by: root-2
root-2: MemoizationThread: add pairing name: root-2-0
root-2: MemoizationThread: ADDPAIRINGNAME: pairing names after add: root-3,root-2-0,root-2,
root-2: MemoizationThread: add pairing name: root-2-1
root-2: MemoizationThread: ADDPAIRINGNAME: pairing names after add: root-3,root-2-1,root-2-0,root-2,
root-2: MemoizationThread: remove pairing name: root-2 pairingNames.size: 5
root-2: MemoizationThread: pairing names after remove root-2root-3,root-2-1,root-2-0,root-3-1,root-2-0: Executor: memoized rcv1: problem.problemID root-2-0 memoizedLabel: 0 memoized result: null

root-3: MemoizationThread: pair: pairingName: root-3
root-3: MemoizationThread: promise by: root-3
root-3: MemoizationThread: add pairing name: root-3-1
root-3: MemoizationThread: ADDPAIRINGNAME: pairing names after add: root-3,root-2-1,root-2-0,root-3-1,root-2,
root-3: MemoizationThread: add pairing name: root-3-2
root-3: MemoizationThread: ADDPAIRINGNAME: pairing names after add: root-3,root-2-1,root-2-0,root-3-2,root-3-1,
root-3: MemoizationThread: pairing names after remove root-3root-2-1,root-2-0,root-3-2,root-3-1,root-2-1: Executor: memoized rcv1: problem.problemID root-2-1 received ack.

root-2-0: MemoizationThread: pair: pairingName: root-2-0
root-2-0: MemoizationThread: promise by: root-2-0
root-2-0: MemoizationThread: DELIVEREDVALUE: r2: type: PROMISEDVALUE
root-2-0: MemoizationThread: DELIVEREDVALUE: info: sender: root-2-0 problem/result ID root-2-0 memoizationLabel: 0 delivered result: (ID:root-2-0: (ID:root-2-0/value:0)
root-2-0: MemoizationThread: remove pairing name: root-2-0 pairingNames.size: 4
root-2-0: MemoizationThread: pairing names after remove root-2-0root-2-1,root-3-2,root-3-1,root-3-2: MemoizationThread: duplicate promise by: root-3-2

root-2-1: MemoizationThread: pair: pairingName: root-2-1
root-2-1: MemoizationThread: duplicate promise by: root-2-1
// repairing after restart
root-2-1: MemoizationThread: pair: pairingName: root-2-1  
root-2-1: MemoizationThread: promised and already delivered so deliver on promise to: root-2-1
root-2-1: MemoizationThread: DELIVEREDVALUE: r2: type: PROMISEDVALUE
root-2-1: MemoizationThread: DELIVEREDVALUE: info: sender: root-2-1 problem/result ID root-2-1 memoizationLabel: 2 delivered result: (ID:root-2-1: (ID:root-2-1/value:1)
root-2-1: MemoizationThread: remove pairing name: root-2-1 pairingNames.size: 2
root-2-1: MemoizationThread: pairing names after remove root-2-1root-3-2,root-3-2: MemoizationThread: promised and already delivered so deliver on promise to: root-3-2

root-3: MemoizationThread: remove pairing name: root-3 pairingNames.size: 5

root-3-1: MemoizationThread: pair: pairingName: root-3-1
root-3-1: MemoizationThread: promise by: root-3-1
root-3-1: MemoizationThread: remove pairing name: root-3-1 pairingNames.size: 3
root-3-1: MemoizationThread: pairing names after remove root-3-1root-2-1: ID: root-2-1: FanIn: root-2 was FanInExecutor: starting receive.

root-3-2: MemoizationThread: pair: pairingName: root-3-2
root-3-2: MemoizationThread: pair: pairingName: root-3-2
root-3-2: MemoizationThread: DELIVEREDVALUE: r2: type: PROMISEDVALUE
root-3-2: MemoizationThread: DELIVEREDVALUE: info: sender: root-3-2 problem/result ID root-3-2 memoizationLabel: 3 delivered result: (ID:root-3-2: (ID:root-3-2/value:2)
root-3-2: MemoizationThread: DELIVEREDVALUE: r2: type: PROMISEDVALUE
root-3-2: MemoizationThread: DELIVEREDVALUE: info: sender: root-3-2 problem/result ID root-3-2 memoizationLabel: root delivered result: (ID:root-3-2: (ID:root-3-2/value:3)
root-3-2: MemoizationThread: remove pairing name: root-3-2 pairingNames.size: 1
root-3-2: MemoizationThread: pairing names after remove root-3-2
"""