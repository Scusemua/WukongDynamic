import multiprocessing
import sys
import threading

from threading import Thread 
from wukong.wukong_problem import WukongProblem
import importlib 

from .memoization import memoization_controller
from .memoization.util import MemoizationMessage, MemoizationRecord, MemoizationMessageType
#from .channel import BiChannel, UniChannel

import yaml 
import logging
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

class testBiChannel(object):
    pass

class testReceiver(Thread):
    pass

class testSender(Thread):
    pass

class ServerlessNetwork(object):
    pass

class ServerLessNetworkingUniSenderHelper(object):
    pass

class ServerLessNetworkingUniReceiverHelper(object):
    pass

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
        stop_result = None,
        config_file_path = "wukong-divide-and-conquer.yaml"
    ):
        super(DivideAndConquerExecutor, self).__init__(group=group, target=target, name=name)
        self.problem = problem              # refers to an INSTANCE of the user-provided ProblemType class
        self.problem_type = problem_type
        self.result_type = result_type
        self.null_result = null_result
        self.stop_result = stop_result
        self.config_file_path = config_file_path
        with open(config_file_path) as f:
            config = yaml.load(f, Loader = yaml.FullLoader)
            self.config = config 
        #     sources_config = config["sources"]
        #     memoization_config = config["memoization"]
        #     source_path = sources_config["source-path"]
        #     source_module = sources_config["source-module"]
        #     logger.debug(self.problem.problemID + ": Importing user-defined module \"" + source_module + "\" from file \"" + source_path + "\" now...")
        #     spec = importlib.util.spec_from_file_location(source_module, source_path)
        #     user_module = importlib.util.module_from_spec(spec)
        #     spec.loader.exec_module(user_module)

        #     self.problem_type = user_module.ProblemType    # refers to a class provided by user, e.g., ProblemType
        #     self.result_type = user_module.ResultType      # refers to a class provided by user, e.g., ResultType
    
    def run(self):
        ServerlessNetworkingMemoizer = None 
        logger.debug(">> Starting Memoization Controller now...")
        memoization_controller.StartController(self.config, null_result = self.null_result, stop_result = self.stop_result)
        
        # Start fan-out task.
        if (self.problem.memoize):
            logger.debug(">> Attempting to pair now using the pairing name \"" + self.problem.problemID + "\"...")
            ServerlessNetworkingMemoizer = memoization_controller.Pair(self.problem.problemID)
            ack = ServerlessNetworkingMemoizer.rcv1()

        #logger.debug("{}, Preprocessing the problem now...".format(self.problem.problemID))
        # Pre-process problem, if required.
        self.problem.UserProgram.preprocess(self.problem)
        #logger.debug("{}, Processing completed.".format(self.problem.problemID))

        # We do not necessarily input the entire initial problem at once. We may input several sub-problems instead.
        # Note: The level of Problems is FanInStack.size(). Root problem has empty stack, 2 children of root have
        # root Problem on their stacks, so level 1, etc.

        # Note: Probably easier to use the level of problem, which is easy to compute based on the problem's FanInStack
        # i.e., level of problems is FanInStack.size(), instead of size of problem. Then two sibling problems would have the 
        # same level, while they might not have the same sizes. You could then run the problem for n levels on a server, capture the subProblems, and 
        # send them to the executors who will ask for them (supplying their problem ID, which is a "task" ID)) when they 
        # get to level n.
        # Levels might also be better to use with WukongProblem.OUTPUT_THRESHOLD since size doesn't work well when sibling
        # subproblems have unequal sizes, like for Quicksort.

        result = None # first and only set result is by sequentialSort when the baseCase is reached.
        memoizedResult = False

        if self.problem.memoize:
            # Here, we want to get the value previously computed for this subproblem
            # as opposed to below where we put a computed value for a subProblem
            # Note that the problem ID may be "4-3-2" or "4-2" but the memoized
            # result is 2 in both cases. So in addition to problemLabeler(),
            # we have memoizedLabeler().

            promiseMsg = MemoizationMessage()
            promiseMsg.messageType = MemoizationMessageType.PROMISEVALUE
            promiseMsg.senderID = self.problem.problemID
            promiseMsg.problemOrResultID = self.problem.problemID
            promiseMsg.becomeExecutor = self.problem.becomeExecutor
            promiseMsg.didInput = self.problem.didInput
            
            # e.g., if problem.problemID is "4-3", memoizedLabel is "3"
            memoizedLabel = self.problem.UserProgram.memoizeIDLabeler(self.problem) 
            promiseMsg.memoizationLabel = memoizedLabel
            promiseMsg.result = None    
            promiseMsg.FanInStack = self.problem.FanInStack
            
            #synchronized(FanInSychronizer.getPrintLock()) {
                #System.out.println("memoized send1: problem.problemID " + problem.problemID + " memoizedLabel: " + memoizedLabel)
            logger.debug("memoized send1: problem.problemID " + str(self.problem.problemID) + " memoizedLabel: " + str(memoizedLabel))
            #}
            
            ServerlessNetworkingMemoizer.send1(promiseMsg)
            
            #synchronized(FanInSychronizer.getPrintLock()) {
                #System.out.println("memoized get: problem.problemID " + problem.problemID + " getting ack.")
            logger.debug("memoized get: problem.problemID " + str(self.problem.problemID) + " getting ack.")
            #}
            
            result = ServerlessNetworkingMemoizer.rcv1()
            
            #synchronized(FanInSychronizer.getPrintLock()) {
                #System.out.println("memoized get: problem.problemID " + problem.problemID + " got ack.")
            logger.debug("memoized get: problem.problemID " + str(self.problem.problemID) + " got ack.")
            #}
            
            if (result == memoization_controller.NullResult):
                # no memoized result
                logger.debug("memoized get: problem.problemID " + str(self.problem.problemID) + " ack was None result.")
                result = None
            elif (result == memoization_controller.StopResult):
                # end executor, to be "restarted" later when subproblem result becomes available
                logger.debug("memoized get: problem.problemID " + str(self.problem.problemID) + " ack was stop.")
                return 
            else:
                # got a memoized result for problem, but the result's ID is the ID of the problem whose result 
                # was memoized, which is not the problem we are working on here. So set ID to proper ID.
                #logger.debug(">> memoized get: problem.problemID" + str(self.problem.problemID) + " was neither stop nor null.")
                #logger.debug(">> result type: " + str(type(result)))
                result.problemID = self.problem.problemID
            
            logger.debug("memoized get: problem.problemID " + str(self.problem.problemID) + " memoizedLabel: " + str(memoizedLabel) + " memoized result: " + str(result))
        
        if not self.problem.memoize or (self.problem.memoize and result is None):
            result = self.result_type() # result_type is a class, so let's instantiate it 

            # rhc: Can we do this if also doing Memoization? I think so.
            if (len(self.problem.FanInStack) == WukongProblem.INPUT_THRESHOLD and self.problem.didInput == False):
                self.problem.UserProgram.inputProblem(self.problem)
                self.problem.didInput = True
                # Debug output is for Merge/Quick Sort only.
                # synchronized(FanInSychronizer.getPrintLock()) {
                #     int size = problem.to - problem.from + 1
                #     System.out.println("inputProblemNew: problem.from: " + problem.from + " problem.to: " + problem.to 
                #         + " problem.FanInStack.size(): " + problem.FanInStack.size() + " size: " + size)
                #     for (int i=0 i<problem.numbers.length i++)
                #         System.out.print(problem.numbers[i] + " ")
                #     System.out.println()
                # }
        
            # If using size instead of level:
            # Using <= and not just == since for odd sized arrays to be sorted, the sizes of subproblems
            # are not always the same. Since using <=, must also check whether we have already input numbers,
            # i.e., once < it will stay less than but we cannot keep inputing numbers.
            # if (size <= WukongProblem.INPUT_THRESHOLD and problem.didInput == False) {
            #     self.problem.UserProgram.inputProblem(problem) 
            #     synchronized(FanInSychronizer.getPrintLock()) {
            #         System.out.println("inputProblemNew: problem.from: " + problem.from + " problem.to: " + problem.to 
            #             + " problem.FanInStack.size(): " + problem.FanInStack.size() + " size: " + size)
            #         for (int i=0 i<problem.numbers.length i++)
            #             System.out.print(problem.numbers[i] + " ")
            #         System.out.println()
            #     }
            # }
        
            # Base case is a sequential algorithm though possibly on a problem of size 1
            if (self.problem.UserProgram.base_case(self.problem)):
                if (not self.problem.didInput):
                    logger.debug("Error: SEQUENTIAL_THRESHOLD reached before INPUT_THRESHOLD, but we cannot sort the numbers until we input them.")
                    logger.debug("problem.SEQUENTIAL_THRESHOLD: " + str(self.problem.SEQUENTIAL_THRESHOLD) + " problem.INPUT_THRESHOLD: " + str(self.problem.INPUT_THRESHOLD))
                    exit(1)
                
                self.problem.UserProgram.sequential(self.problem, result)

                logger.debug("%s base case: result before ProcessBaseCase(): %s" % (self.problem.problemID, str(result)))
                self.problem.ProcessBaseCase(self.problem, result, ServerlessNetworkingMemoizer)

                # rhc: At this point, the recursion stops and we begin the Fan-In operations for this leaf node executor.
            else: # not baseCase
                # rhc: start Fan-Out task
                subProblems = list()
                logger.debug("%s Calling problem.divide()" % self.problem.problemID)
                self.problem.UserProgram.divide(self.problem, subProblems)

                # rhc: end Fan-Out task

                # rhc: start Fan-Out operation
                # Makes recursive call to run() for one subproblem and a new executor for the other(s).
                # Calls self.problem.UserProgram.computeInputsOfSubproblems(problem,subProblems) when level == DivideandConquerFibonacci.ProblemType.INPUT_THRESHOLD
                # and then divides the input of parent into the two inputs of the children. 
                logger.debug("%s Calling problem.Fanout()" % self.problem.problemID)
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

        # if True:
        #     logger.info("Thread is exiting right before call to `FanInOperationAndTask()`")
        #     return 

        finalRemainingExecutor = self.problem.FanInOperationandTask(self.problem,result,memoizedResult,ServerlessNetworkingMemoizer)
        #rhc: end Fan-In operation and Fan-In task.

        # The executor that is the last fan-in task of the final fan-in outputs the result. the
        # result will have been saved in the map with key "root."

        # Need to output the value having key "root"
        if (finalRemainingExecutor):
            self.problem.UserProgram.output_result(self.problem.problemID)
        
        return