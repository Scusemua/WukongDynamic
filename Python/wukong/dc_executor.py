import sys
import threading

from threading import Thread 
from wukong.wukong_problem import WukongProblem

# from .memoization import memoization_controller
# from .memoization.util import MemoizationMessage, MemoizationMessageType

import logging
from logging import handlers
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s')
ch = logging.StreamHandler(sys.stdout)
ch.setFormatter(formatter)
#logger.addHandler(ch)

if logger.handlers:
   for handler in logger.handlers:
      handler.setFormatter(formatter)

root = logging.getLogger()
if root.handlers:
    for handler in root.handlers:
       handler.setFormatter(formatter)

debug_lock = threading.Lock() 

class DivideAndConquerExecutor(Thread):
    def __init__(
        self,
        problem = None,
        group=None, 
        target=None, 
        name=None,
        problem_type = None, 
        result_type = None,
        null_result = None,
        stop_result = None
    ):
        super(DivideAndConquerExecutor, self).__init__(group=group, target=target, name=name)
        self.problem = problem              # refers to an INSTANCE of the user-provided ProblemType class
        self.problem_type = problem_type
        self.result_type = result_type
        self.null_result = null_result
        self.stop_result = stop_result
    
    def run(self):
        ServerlessNetworkingMemoizer = None 

        # memoization_controller.StartController(
        #     #config = self.config, 
        #     user_problem_type = self.problem_type,
        #     user_program_class = self.problem.UserProgram.__class__,
        #     null_result = self.null_result, 
        #     stop_result = self.stop_result)

        logger.debug(">>>> Executor for Problem ID " + self.problem.problem_id + " has started running!")
        
        # Start fan-out task.
        # if (self.problem.memoize):
        #     logger.debug(">> Attempting to pair now using the pairing name \"" + self.problem.problem_id + "\"...")
        #     ServerlessNetworkingMemoizer = memoization_controller.Pair(self.problem.problem_id)
        #     ack = ServerlessNetworkingMemoizer.rcv1()

        # Pre-process problem, if required.
        self.problem.UserProgram.preprocess(self.problem)

        # We do not necessarily input the entire initial problem at once. We may input several sub-problems instead.
        # Note: The level of Problems is fan_in_stack.size(). Root problem has empty stack, 2 children of root have
        # root Problem on their stacks, so level 1, etc.

        # Note: Probably easier to use the level of problem, which is easy to compute based on the problem's fan_in_stack
        # i.e., level of problems is fan_in_stack.size(), instead of size of problem. Then two sibling problems would have the 
        # same level, while they might not have the same sizes. You could then run the problem for n levels on a server, capture the subProblems, and 
        # send them to the executors who will ask for them (supplying their problem ID, which is a "task" ID)) when they 
        # get to level n.
        # Levels might also be better to use with WukongProblem.OUTPUT_THRESHOLD since size doesn't work well when sibling
        # subproblems have unequal sizes, like for Quicksort.

        result = None # first and only set result is by sequential when the baseCase is reached.
        memoizedResult = False

        # UNCOMMENT FOR SERVERLESS IF WE WANT TO START MEMOIZING.
        # if self.problem.memoize:
        #     # Here, we want to get the value previously computed for this subproblem
        #     # as opposed to below where we put a computed value for a subProblem
        #     # Note that different problem tasks will have different problem IDs but the memoized
        #     # result may be e.g., "2" in both cases. So in addition to problemLabeler(),
        #     # we have memoizedLabeler().

        #     promiseMsg = MemoizationMessage(
        #         message_type = MemoizationMessageType.PROMISEVALUE,
        #         sender_id = self.problem.problem_id,
        #         problem_or_result_id = self.problem.problem_id,
        #         become_executor = self.problem.become_executor,
        #         did_input = self.problem.did_input
        #     )
            
        #     if self.problem.memoization_label_on_restart is None:
        #         # e.g., for Fibonacci, Fibonacci(2) has memoized_label "2"
        #         logger.debug("%s: >> Creating memoization label via `memoizeIDLabeler()`" % self.problem.problem_id)
        #         memoized_label = self.problem.UserProgram.memoizeIDLabeler(self.problem) 
        #     else:
        #         logger.debug("%s: >> Using `memoization_label_on_restart` for memoization label." % self.problem.problem_id)
        #         memoized_label = self.problem.memoization_label_on_restart
            
        #     promiseMsg.memoization_label = memoized_label
        #     promiseMsg.result = None    
        #     promiseMsg.fan_in_stack = self.problem.fan_in_stack

        #     logger.debug("memoized send1: problem.problem_id " + str(self.problem.problem_id) + " memoized_label: " + str(memoized_label))
            
        #     ServerlessNetworkingMemoizer.send1(promiseMsg)
            
        #     logger.debug("memoized get: problem.problem_id " + str(self.problem.problem_id) + " getting ack.")
            
        #     result = ServerlessNetworkingMemoizer.rcv1()
            
        #     logger.debug("memoized get: problem.problem_id " + str(self.problem.problem_id) + " got ack.")
            
        #     if (result == memoization_controller.NullResult):
        #         # no memoized result
        #         logger.debug("memoized get: problem.problem_id " + str(self.problem.problem_id) + " ack was None result.")
        #         result = None
        #     elif (result == memoization_controller.StopResult):
        #         # end executor, to be "restarted" later when subproblem result becomes available
        #         logger.debug("memoized get: problem.problem_id " + str(self.problem.problem_id) + " ack was stop.")
        #         return 
        #     else:
        #         logger.debug(">> memoized get: problem.problem_id " + str(self.problem.problem_id) + " was memoized result")
        #         # got a memoized result for problem, but the result's ID is the ID of the problem whose result 
        #         # was memoized and sent to us, which is not the problem we are working on here. So set ID to proper ID, which is us
        #         result.problem_id = self.problem.problem_id
            
        #     logger.debug("memoized get: problem.problem_id " + str(self.problem.problem_id) + " memoized_label: " + str(memoized_label) + " memoized result: " + str(result))
        
        if not self.problem.memoize or (self.problem.memoize and result is None):
            result = self.result_type() # result_type is a class, so let's instantiate it 

            # rhc: Can we do this if also doing Memoization? I think so.
            if (len(self.problem.fan_in_stack) == WukongProblem.INPUT_THRESHOLD and self.problem.did_input == False):
                self.problem.UserProgram.inputProblem(self.problem)
                self.problem.did_input = True
        
            # Base case is a sequential algorithm though possibly on a problem, e.g., for Fibonacci, of size 1
            if (self.problem.UserProgram.base_case(self.problem)):
                if (not self.problem.did_input):
                    logger.debug("Error: SEQUENTIAL_THRESHOLD reached before INPUT_THRESHOLD, but we cannot sort the numbers until we input them.")
                    logger.debug("problem.SEQUENTIAL_THRESHOLD: " + str(self.problem.SEQUENTIAL_THRESHOLD) + " problem.INPUT_THRESHOLD: " + str(self.problem.INPUT_THRESHOLD))
                    exit(1)
                
                self.problem.UserProgram.sequential(self.problem, result)

                logger.debug("%s base case: result before ProcessBaseCase(): %s" % (self.problem.problem_id, str(result)))
                self.problem.ProcessBaseCase(self.problem, result, ServerlessNetworkingMemoizer)

                # rhc: At this point, the recursion stops and we begin the Fan-In operations for this leaf node executor.
            else: # not baseCase
                # rhc: start Fan-Out task
                subProblems = list()
                logger.debug("%s Calling problem.divide()" % self.problem.problem_id)
                self.problem.UserProgram.divide(self.problem, subProblems)

                # rhc: end Fan-Out task

                # rhc: start Fan-Out operation
                # Makes recursive call to run() for one subproblem and a new executor for the other(s).
                # Calls self.problem.UserProgram.computeInputsOfSubproblems(problem,subProblems) when level == DivideandConquerFibonacci.ProblemType.INPUT_THRESHOLD
                # and then divides the input of parent into the two inputs of the children. 
                logger.debug("%s Calling problem.Fanout()" % self.problem.problem_id)
                self.problem.Fanout(self.problem, subProblems, ServerlessNetworkingMemoizer)
                # rhc: end Fan-Out operation

                # After the executor is the first task of a Fan-In, or computes the final result, its recursion unwinds
                # and there is nothing to do on the unwind.
                return
            # !baseCase
        else:
            logger.debug("else in template: For Problem: " + str(self.problem) + " Memoized result: " + str(result))
            memoizedResult = True
        # All of the executor that were invoked eventually become base case leaf nodes
        # as the recursive calls stop, unless they get a memoized result in which case they
        # start their unwind. At this point, the executor uses the Fan-In stack to perform Fan-In operations
        # as the recursion unwinds (logically).
        # When the final result is obtained, or an executor does not become the fan-in task, the executor
        # can just terminate. (But our Java threads unwind the recursion so their run() terminates.)
        #
        # A leaf node executor that does not get a memoized result does a base case, which is a 
        # call to sequential().For a base case X, no merge() is performed on the children of X, since no 
        # Divide is performed on X we simply do a Fan-In operation involving base 
        # case X and base case Y = sibling(X) (in the two sibling case).
        #
        # If we are the first executor of a Fan-In, we can unwind our recursion
        # or simply terminate. That is, we have made  0, 1, or more recursive calls to 
        # Divide when we reach a base case, we do not unwind the recursion instead, 
        # we let the leaf node executors perform Fan-In operations with the Fan-in stack. At each Fan-in,
        # one executor will stop and one will continue, leaving one executor to compute the final 
        # merge/result.

        #rhc: start Fan-In operation and possibly  perform Fan-In task.

        finalRemainingExecutor = self.problem.FanInOperationandTask(self.problem,result,memoizedResult,ServerlessNetworkingMemoizer)
        #rhc: end Fan-In operation and Fan-In task.

        # The executor that is the last fan-in task of the final fan-in outputs the result. the
        # result will have been saved in the map with key "root."

        # Need to output the value having key "root"
        if (finalRemainingExecutor):
            self.problem.UserProgram.output_result(self.problem.problem_id)
        
        return