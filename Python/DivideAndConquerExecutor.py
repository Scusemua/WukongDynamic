import sys

from threading import Thread 
from DivideandConquerMergeSort import ProblemType

import logging
logger = logging.getLogger(__name__)

class UniChannel(object):
    pass

class MemoizationMessage(object):
    pass 

class MemoizationRecord(object):
    pass

class promisedResult(object):
    pass

class MemoizationController(object):
    pass

class MemoizationThread(Thread):
    pass

class testBiChannel(object):
    pass

class testReceiver(Thread):
    pass

class testSender(Thread):
    pass

class ServerlessNetwork(object):
    pass

class WukongProblem(object):
    # Get input arrays when the level reaches the INPUT_THRESHOLD, e.g., don't grab the initial 256MB array,
    # wait until you reach level , say, 1, when there are two subproblems each half as big.
    # To input at the start of the problem (root), use 0 the stack has 0 elements on it for the root problem.
    # To input the first two subProblems, use 1. Etc.
    #
    # This value should not be changed. If the user provides ProblemType.INPUT_THRESHOLD, we will use that value
    # otherwise, we use this default value, which causes the root problem to be input.
    # This is the default level for inputting problem. Default is at the root, i.e., level 0
    INPUT_THRESHOLD = 0

    # When unwinding recursion, output subProblme results when subProblem sizes reaches OUTPUT_THRESHOLD.
    # However, you must be sure that the entire result will be captured, i.e, that all the results are included
    # in the subproblems that reach threshold. This works for mergeSort, but not for QuickSort. In the latter,
    # you can have one subprogram that is combining itself with single element subproblems so only one subproblem
    # can reach the output threshold if you stop then, the elements in the other (smaller) subproblems will
    # never be output as these other subproblems are only being combined with the big subproblem.
    # Note: in this case, the large subproblem is a big object so task collapsing should work well in this case.
    #
    # Using MAX_VALUE so the default will be greater than SEQUENTIAL_THRESHOLD. We should never reach level Integer.MAX_VALUE
    # when making recursive calls!
    OUTPUT_THRESHOLD = sys.maxint
    
    memoize = False
    
    # For fan-in use serverless networking where invoked executor sends result to become executor,
    # or use redis-like shared storage to write and read results.
    USESERVERLESSNETWORKING = False
    #becomeExecutor = False
    
    # Identifies the SubProblem used as a key for Wukong Storage, where the value is a sorted subsegment of the array
    #String problemID
    # Stack of storage keys from root to leaf node (where there is a leaf node for each base case).
    #Stack<ProblemType> FanInStack
    
    def __init__(self):
        self.didInput = False 
        self.becomeExecutor = False
        self.problemID = None 
        self.FanInStack = list() 

    # # If users supply constants, use them, else use default values.
    # def static_init():
    #     #Field IT = None
    #     IT = False 
    #     try:
    #         #IT = ProblemType.class.getDeclaredField("INPUT_THRESHOLD")
	# 		IT = hasattr(ProblemType, "INPUT_THRESHOLD")
    #     except Exception as nsfe: # (NoSuchFieldException nsfe) {
    #         # intentionally ignored
    #         pass 

    #     if IT:
    #         try:
    #             INPUT_THRESHOLD =  ProblemType.INPUT_THRESHOLD #  IT # .getInt(INPUT_THRESHOLD)
    #         except Exception as e:
    #             logger.error(repr(e))
        
    #     #Field OT = None
    #     OT = False 
    #     try:
    #         #OT = ProblemType.class.getDeclaredField("OUTPUT_THRESHOLD")
	# 		OT = hasattr(ProblemType, "OUTPUT_THRESHOLD")
    #     except Exception as nsfe:
    #         logger.warn("Ignoring NoSuchFieldException for \"OUTPUT_THRESHOLD\"")

    #     if OT:
    #         try:
    #             OUTPUT_THRESHOLD =  ProblemType.OUTPUT_THRESHOLD #  IT # .getInt(INPUT_THRESHOLD)
    #         except Exception as e:
    #             logger.error(repr(e))
            
        
    #     #Field m = None
    #     m = None 
    #     try:
    #         m = ProblemType.class.getDeclaredField("memoize")
    #     except Exception as nsfe:
    #         # intentionally ignored
    #         logger.warn("Ignoring NoSuchFieldException for \"memoize\"")
    #         pass 

    #     if (m != None):
    #         try:
    #             memoize = ProblemType.memoize #  IT # .getInt(INPUT_THRESHOLD)
    #         except Exception as e:
    #             logger.error(repr(e))
    # # end static

    @staticmethod
    def ProcessBaseCase(problem : ProblemType, result : ResultType, ServerlessNetworkingMemoizer : ServerlessNetworkingClientServer):
        # memoizedResult True means that we got a memoized result (either at the base case or for a non-base case)
        # and we don't want to memoize this result, which would be redundant.

        # Each started executor eventually executes a base case (sequential sort) and then the executor competes
        # with its sibling at a Fan-In to do the Fan0-In task, which is a merge.
        
        # problem is a base case problem that uses sequential() result is given the same ID
        # Or result was memoized, i.e., some other problem, and we need to update the problemID of result from 
        # the other problem's ID to this problem's ID.
        
        # Need to do this here as the base case sequential() is not required to set resultlabel = problemLabel
        result.problemID = problem.problemID    

         # Note: not sending a REMOVEPAIRINGNAME message since the executor is now unwinding and may be sending
         # messages all the way up.

        # rhc: no need to check !memoizedResult since if memoizedResult we stopped, and even if we did not stop
        # we only compute base case if we don't get memoized result for the base case. (If we get memoized
        # result, we unwind by fan-in to parent of current problem.
        # if (WukongProblem.USESERVERLESSNETWORKING and not memoizedResult and WukongProblem.memoize) {
        if (WukongProblem.USESERVERLESSNETWORKING and WukongProblem.memoize):
            deliverResultMsg = new MemoizationMessage()
            deliverResultMsg.messageType = MemoizationController.MemoizationMessageType.DELIVEREDVALUE
            deliverResultMsg.senderID = problem.problemID
            deliverResultMsg.problemOrResultID = result.problemID
            #System.out.println("result: " + result)
            logger.debug("result: ", str(result))
            memoizedLabel = User.memoizeIDLabeler(problem)
            #String memoizedLabel = User.memoizeIDLabeler(result)
            deliverResultMsg.memoizationLabel = memoizedLabel
            deliverResultMsg.result = result
            deliverResultMsg.FanInStack = None
            ServerlessNetworkingMemoizer.send1(deliverResultMsg)

            ack = ServerlessNetworkingMemoizer.rcv1()

    # Replace with actual Wukong fan-out code
    # subProblems was originally of type ArrayList<ProblemType>
    @staticmethod
    def Fanout(problem: ProblemType, subProblems : list, ServerlessNetworkingMemoizer: ServerlessNetworkingClientServer):
        # Retrieve subsegment of array when the threshold is reached. Fanout is only called while 
        # recursive calls are made and the stack is thus growing in size. Fanout is not called 
        # while the recursion is unwinding and the stack is shrinking. Thus, this condition will 
        # not be True twice, i.e., it will be True as the stack is growing but not while the stack is shrinking.
        #
        # Note: If using the size of the subproblem instead of the level, use this if-statement:
        #   if ((problem.to - problem.from + 1) <= WukongProblem.INPUT_THRESHOLD)
        #
        if (problem.FanInStack.size() >= WukongProblem.INPUT_THRESHOLD):
            User.computeInputsOfSubproblems(problem, subProblems)
        
        # In General:
        # ProblemType becomeSubproblem = chooseBecome(subProblems)
        # where we choose the become subProblem based on the criterion
        # specified by the user. For example:
        # default: choose the last subProlem in list subProblems, which means
        # the user can control the choice by adding to list subProblems in
        # the best order, e.g., the left subProblem is always last
        # size: choose the largest subProblem, e.g., for Mergesort/Fibonaci the 
        # subProblems have equal size, so it does not matter which one is chosen,
        # but for Quickort one subProblem can be much larger than the other, and
        # for all Mergesort/QuickSort/Fibonaci the size of the task's output equals 
        # the size of the task's input.
        #
        # Here, I am just forking the subProblems and choosing the last as the 
        # become subProblem, which is always the left subProblem (not right).
        #
        # Note, for the become subProblem ewe do fork/invoke a new thread, and
        # call become.start, we call become.run().

        #For a fan-out of N, invoke N-1 executors and become 1
        #for (int i=0 i< subProblems.size()-1 i++) {
        for i in range(len(subProblems) - 1):
            invokedSubproblem = subProblems.get(i)

            # Generate the executor/storage label for the subproblem
            # Supply all: problem, subProblems, subProblem, i in case they are needed.
            ID = User.problemLabeler(invokedSubproblem,i, problem, subProblems)

            invokedSubproblem.problemID = problem.problemID + "-" + ID
            #invokedSubproblem.problemID = ID
            
            invokedSubproblem.becomeExecutor = False
            
            @SuppressWarnings("unchecked")
            # Add label to stack of labels from root to subproblem
            childFanInStack = (Stack<ProblemType>) problem.FanInStack.clone() # Stack<ProblemType> 

            # TODO: Convert this to Python.
            synchronized(FanInSychronizer.getPrintLock()) {
                System.out.println("fanout: push on childFanInStack: problem: " + problem)
                childFanInStack.push(problem)
                #childFanInStack.push(problem.value)
            }
            invokedSubproblem.FanInStack = childFanInStack

            # TODO: Convert this to Python.
            synchronized(FanInSychronizer.getPrintLock()) {
                logger.debug("fanout: parent stack: ")
                #for (int j=0 j< problem.FanInStack.size() j++) {
                for j in range(len(invokedSubproblem.FanInStack))
                    logger.debug(problem.FanInStack.get(j) + " ")
                logger.debug("")
                logger.debug("fanout: subProblem stack: ")
                #for (int j=0 j< invokedSubproblem.FanInStack.size() j++) {
                for j in range(len(invokedSubproblem.FanInStack))
                    logger.debug(invokedSubproblem.FanInStack.get(j) + " ")
                logger.debug("")
            }
            
            # If parent input was done then we will grab part of the parent's values and the child's input can be considered done too.
            invokedSubproblem.didInput = problem.didInput
            
            if (WukongProblem.memoize and WukongProblem.USESERVERLESSNETWORKING):
                MemoizationMessage addPairingNameMsgForInvoke = new MemoizationMessage()
                addPairingNameMsgForInvoke.messageType = MemoizationController.MemoizationMessageType.ADDPAIRINGNAME
                addPairingNameMsgForInvoke.senderID = problem.problemID
                addPairingNameMsgForInvoke.problemOrResultID = invokedSubproblem.problemID
                addPairingNameMsgForInvoke.memoizationLabel = None
                addPairingNameMsgForInvoke.result = None
                addPairingNameMsgForInvoke.FanInStack = None
                ServerlessNetworkingMemoizer.send1(addPairingNameMsgForInvoke)
                
                ResultType ack = ServerlessNetworkingMemoizer.rcv1()
            
            # New subproblem
            newExecutor = DivideAndConquerExecutor(invokedSubproblem)

            # TODO: Convert this to Python.
            #synchronized(FanInSychronizer.getPrintLock()) {
                #System.out.println("Fanout: ID: " + problem.problemID + " invoking new right executor: "  + invokedSubproblem.problemID)
            logger.debug("Fanout: ID: " + str(problem.problemID) + " invoking new right executor: "  + str(invokedSubproblem.problemID))
            #}
            # start the executor
            
            # where:
            #enum MemoizationMessageType {ADDPAIRINGNAME, REMOVEPAIRINGNAME, PROMISEDVALUE, DELIVEREDVALUE}
    
            #class MemoizationMessage {
            #    MemoizationController.MemoizationMessageType messageType
            #    String senderID
            #    String problemOrResultID
            #    String memoizationLabel
            #    ResultType result
            #    Stack<ProblemType> FanInStack
            #    boolean becomeExecutor
            #    boolean didInput
            #}
            
            newExecutor.start()
            # Note: no joins for the executor threads - when they become leaf node executors, they will perform all of the 
            # combine() operations and then return, which unwinds the recursion with nothing else to do.
        # Do the same for the become executor
        becomeSubproblem = subProblems.get(subProblems.size()-1) # ProblemType
        
        # Generate the executor/storage label for the subproblem
        # Supply all: problem, subProblems, subProblem, i in case they are needed.
        ID = User.problemLabeler(becomeSubproblem,subProblems.size()-1, problem, subProblems) # String
        
        # If two different subProblems in the divide and conquer tree can have the same label, then there would
        # be a problem since these labels are also used to label fan-in tasks and we cannot have two fan-in tasks
        # with the same label. We make sure labels are unique by using the path of labels from the root to this 
        # subproblem, e.g., instead of "3" and "2" we use "4-3" and "4-2" where "4" is the label of the 
        # root and subProblem's "#" and "4" are children of the root (as in Fibonacci).
        becomeSubproblem.problemID = problem.problemID + "-" + ID

        becomeSubproblem.becomeExecutor = True
        
        # No need to clone the problem stack for the become subproblem as we clones the stacks for the other subProblems  
        #@SuppressWarnings("unchecked")
        #Stack<ProblemType> childFanInStack = (Stack<ProblemType>) problem.FanInStack.clone()
        childFanInStack = problem.FanInStack # Stack<ProblemType> 
        childFanInStack.append(var)
        becomeSubproblem.FanInStack = childFanInStack
        
        # If parent input was done then we will grab part of the parent's values and the child's input can be considered done too.
        becomeSubproblem.didInput = problem.didInput
        DivideAndConquerExecutor become = 
            new DivideAndConquerExecutor(becomeSubproblem)
        #synchronized(FanInSychronizer.getPrintLock()) {
        #    System.out.println("Fanout: ID: " + problem.problemID  + " becoming left executor: "  + becomeSubproblem.problemID)
        logger.debug("Fanout: ID: " + str(problem.problemID)  + " becoming left executor: "  + str(becomeSubproblem.problemID))
        #}
        
        if (WukongProblem.memoize and WukongProblem.USESERVERLESSNETWORKING):

            addPairingNameMsgForBecomes = new MemoizationMessage() # MemoizationMessage
            addPairingNameMsgForBecomes.messageType = MemoizationController.MemoizationMessageType.ADDPAIRINGNAME
            addPairingNameMsgForBecomes.senderID = problem.problemID
            addPairingNameMsgForBecomes.problemOrResultID = becomeSubproblem.problemID
            addPairingNameMsgForBecomes.memoizationLabel = None
            addPairingNameMsgForBecomes.result = None
            addPairingNameMsgForBecomes.FanInStack = None
            ServerlessNetworkingMemoizer.send1(addPairingNameMsgForBecomes)
            
            ack1 = ServerlessNetworkingMemoizer.rcv1() # ResultType

            removePairingNameMsgForParent = new MemoizationMessage() # MemoizationMessage
            removePairingNameMsgForParent.messageType = MemoizationController.MemoizationMessageType.REMOVEPAIRINGNAME
            removePairingNameMsgForParent.senderID = problem.problemID
            removePairingNameMsgForParent.problemOrResultID = problem.problemID
            removePairingNameMsgForParent.memoizationLabel = None
            removePairingNameMsgForParent.result = None
            removePairingNameMsgForParent.FanInStack = None
            ServerlessNetworkingMemoizer.send1(removePairingNameMsgForParent)
            
            ack2 = ServerlessNetworkingMemoizer.rcv1() # ResultType
        
        # No need to start another Executor, current executor can recurse until the base case. At that point, the executor
        # will compete at a Fan-In point to do a merge with its "sibling" executor.
        #       1-2
        #      /   \
        #     1     2    # Executors 1 and 2 have a Fan-In at 1-2. One of executors 1 or 2 will merge input 1 and input 2 
        # and the other will stop by executing return, which will unwind its recursion. 
        become.run()  
        
        # Problem was pushed on subProblem stacks and stacks will be passed to invoked Executor. Here, user has a chance to 
        # trim any data in problem that is no longer needed (e.g., input array of problem) so less data is passed to 
        # Executor.
        
        # Option: create method WukongProblem.trimProblem() that sets WukongProblem.FanInStack.None and call 
        # WukongProblem.trimProblem() here before calling User.trimProblem(problem) I believe that problem's
        # FanInStack is no longer needed and can always be trimmed, though it might be helpful for debugging.
        User.trimProblem(problem)
        #rhc: end Fan-Out operation

        # Recursion unwinds - nothing happens along the way.
        return
    
    # The last executor to fan-in will become the Fan-In task executor. The actual executor needs to save its Merge output 
    # and increment/set counter/boolean. We handle this  here by using map.put(key,value), which returns the previous 
    # value the key is mapped to, if any, and None if not. So if put() returns None we are not the last executor to Fan-in. 
    # This is a special case for MergeSort, which always has only two Fan-In executors.

    # subproblemResults was previously ArrayList<ResultType> 
    @staticmethod
    isLastFanInExecutor(parentProblem: ProblemType, result: ResultType, subproblemResults: list) -> bool:
        # store result and check if we are the last executor to do this.
        # Add all of the other subproblem results to subproblemResults. We will add our sibling result later (before we call combine().)
        FanInID = parentProblem.problemID # String 

        copyOfResult = result.copy() # ResultType
        copyOfResult.problemID = result.problemID
        
        #synchronized(FanInSychronizer.getPrintLock()) {
        #    System.out.println("isLastFanInExecutor: Writing to " + FanInID + " the value " + copyOfResult) # result)
        logger.debug("isLastFanInExecutor: Writing to " + FanInID + " the value " + str(copyOfResult)) # result))
        #}
        ResultType siblingResult = FanInSychronizer.resultMap.put(FanInID,result) 
        
        # firstFanInResult may be None
        if (siblingResult == None):
            return False
        else:    
            ResultType copyOfSiblingResult = siblingResult.copy()
            copyOfSiblingResult.problemID = siblingResult.problemID
            
            subproblemResults.add(copyOfSiblingResult)
            return True

    # Perform Fan-in and possibly the Fan-in task 
    @ staticmethod
    FanInOperationandTask(problem: ProblemType, result: ResultType, memoizedResult: boolean, ServerlessNetworkingMemoizer: ServerlessNetworkingClientServer) -> bool:
        # memoizedResult True means that we got a memoized result (either at the base case or for a non-base case)
        # and we don't want to memoize this result, which would be redundant.
        
#rhc: start Fan-In operation

        synchronized(FanInSychronizer.getPrintLock()) {
            # System.out.println("**********************Start Fanin operation:")
            # System.out.println("Fan-in: ID: " + problem.problemID)
            # System.out.println("Fan-in: becomeExecutor: " + problem.becomeExecutor)
            # System.out.print("Fan-in: FanInStack: ")
            logger.debug("**********************Start Fanin operation:")
            logger.debug("Fan-in: ID: " + problem.problemID)
            logger.debug("Fan-in: becomeExecutor: " + problem.becomeExecutor)
            System.out.print("Fan-in: FanInStack: ")            
            #for (int i=0 i<problem.FanInStack.size() i++)
            for i in range(len(problem.FanInStack))
                #System.out.print(problem.FanInStack.get(i) + " ")
                logger.debug("{} ".format(problem.FanInStack[i]))
            logger.debug("")
            #System.out.flush()
        }
        # Each started executor eventually executes a base case (sequential sort) or gets a memoized
        # result for a duplicate subProblem, and then the executor competes with its sibling(s) at a Fan-In 
        # to do the Fan0-In task, which is a combine().
        
        # True if this is "become" rather than "invoke" problem. Set below.
        FanInExecutor = False    
        
        # For USESERVERLESSNETWORKING, whether a subProblem is become or invoked is decided when the problem is created.
        # If we instead determine at runtime which problem is the last to fan-in, then the last to fan-in is 
        # the become, and the others are invoke.
        if (WukongProblem.USESERVERLESSNETWORKING)
            FanInExecutor = problem.becomeExecutor
 
        while (problem.FanInStack.size() != 0):
            
            # Stop combining results when the results reach a certain size, and the communication delay for passing
            # the results is much larger than the time to combine them. The remaining combines can be done on one
            # processor. This is task collapsing of the remaining combine() tasks into one task.
            # However, we can only do this if we are sure all of the subproblems will reach the threshold. For example,
            # this works for mergeSort as sibling subproblems are all growing in size, more or less equally. But this
            # is not True for quicksort, which may have only one large subproblem that reaches the threshold as the 
            # other subproblems are small ones that get merged with the large one.

            #if (size >= WukongProblem.OUTPUT_THRESHOLD) { 
            if (problem.FanInStack.size() == WukongProblem.OUTPUT_THRESHOLD):
                synchronized(FanInSychronizer.getPrintLock()) {
                    logger.debug("Exector: " + str(problem.problemID) + " Reached OUTPUT_THRESHOLD for result: " + str(result.problemID)
                        + " with problem.FanInStack.size(): " + str(problem.FanInStack.size())
                        + " and WukongProblem.OUTPUT_THRESHOLD: " + str(WukongProblem.OUTPUT_THRESHOLD))
                    logger.debug(result) 
                    # return False so we are not considered to be the final Executor that displays final results. There will
                    # be many executors that reach the OUPUT_THRESHOLD and stop.
                    return False
                }

            ProblemType parentProblem = problem.FanInStack.pop()
            
            synchronized(FanInSychronizer.getPrintLock()) {
                logger.debug("Fan-in: ID: " + str(problem.problemID) + " parentProblem ID: " + str(parentProblem.problemID))
                logger.debug("Fan-in: ID: " + str(problem.problemID) + " problem.becomeExecutor: " + str(problem.becomeExecutor) + " parentProblem.becomeExecutor: " + str(parentProblem.becomeExecutor))
                logger.debug("")
                #System.out.flush()
            }
            # The last task to fan-in will become the Fan-In task executor. The actual executor needs to save its Merge output 
            # and increment/set counter/boolean. We handle this in our prototype here by using map.put(key,value), which 
            # returns the previous value the key is mapped to, if there is one, and None if not. So if put() returns None 
            # we are not the last executor to Fan-in.This is a special case for MergeSort, which always has only two 
            # Fan-In tasks.
            #
            # Both siblings write result to storage using key FanInID, The second sibling to write will get the 
            # first sibling's value in previousValue. Now the second sibling has both values.
            
            subproblemResults = new ArrayList<ResultType>() # ArrayList<ResultType> 

            if (WukongProblem.USESERVERLESSNETWORKING):
                String FanIn = parentProblem.problemID
                if (FanInExecutor):
                    System.out.println("ID: " + problem.problemID + ": FanIn: " + parentProblem.problemID + " was FanInExecutor: starting receive.")
                    ServerLessNetworkingUniReceiverHelper h = new ServerLessNetworkingUniReceiverHelper(FanIn)
                    h.start()
                    try:
                        h.join()
                    except Exception as e:
                        logger.error(repr(e))
                    # r is a copy of sent result
                    copyOfSentResult = h.result # ResultType
                    subproblemResults.append(copyOfSentResult)
                    #synchronized(FanInSychronizer.getPrintLock()) {
                    #    System.out.println("ID: " + problem.problemID + ": FanIn: " + parentProblem.problemID + " was FanInExecutor: result received:" + copyOfSentResult)
                    logger.debug("ID: " + str(problem.problemID) + ": FanIn: " + str(parentProblem.problemID) + " was FanInExecutor: result received:" + str(copyOfSentResult))
                    #}
                else:
                    # pair and send message
                    ServerLessNetworkingUniSenderHelper h = new ServerLessNetworkingUniSenderHelper(FanIn,result)
                    h.start()
                    try:
                        h.join() # not really necessary
                    except Exception:
                        pass 
                    #synchronized(FanInSychronizer.getPrintLock()) {
                    #    System.out.println("Fan-In: ID: " + problem.problemID + ": FanInID: " + parentProblem.problemID + " was not FanInExecutor:  result sent:" + result)
                    #}
                    logger.debug("Fan-In: ID: " + str(problem.problemID) + ": FanInID: " + str(parentProblem.problemID) + " was not FanInExecutor:  result sent:" + str(result))
                }
            }
            else:
                # When return we either have our result and sibling result or or our result and None. For latter, we were first
                # Executor to fan-in so we stop.
                FanInExecutor = isLastFanInExecutor(parentProblem, result, subproblemResults)
            
            # If we are not the last task to Fan-In then unwind recursion and we are done
        
            if not FanInExecutor:
                #synchronized(FanInSychronizer.getPrintLock()) {
                #    System.out.println("Fan-In: ID: " + problem.problemID + ": FanInID: " + parentProblem.problemID + ": is not become Executor "
                #        + " and its value was: " + result + " and after put is " + (FanInSychronizer.resultMap.get(parentProblem.problemID)))
                #    System.out.flush()
                #}
                logger.debug("Fan-In: ID: " + str(problem.problemID) + ": FanInID: " + str(parentProblem.problemID) + ": is not become Executor " + " and its value was: " + str(result) + " and after put is " + str((FanInSychronizer.resultMap.get(parentProblem.problemID))))
                
                #if (problem.FanInStack.size() < WukongProblem.OUTPUT_THRESHOLD) {
                #    synchronized(FanInSychronizer.getPrintLock()) {
                #        System.out.println("Exector: !lastFanInExecutor: " + problem.problemID + " Reached OUTPUT_THRESHOLD for result: " + result.problemID 
                #            + " with problem.FanInStack.size(): " + problem.FanInStack.size()
                #            + " and WukongProblem.OUTPUT_THRESHOLD: " + WukongProblem.OUTPUT_THRESHOLD)                        System.out.println(result) 
                #        # return False so we are not considered to be the final Executor that displays final results. There will
                #        # be many executors that reach the OUPUT_THRESHOLD and stop.
                #        return False
                #    }
                #}
                
                #if (size >= WukongProblem.OUTPUT_THRESHOLD) { 
                if (problem.FanInStack.size() == WukongProblem.OUTPUT_THRESHOLD):
                    # TODO: Convert to Python
                    synchronized(FanInSychronizer.getPrintLock()) {
                        logger.debug("Exector: " + str(problem.problemID) + " Reached OUTPUT_THRESHOLD for result: " + str(result.problemID)
                            + " with problem.FanInStack.size(): " + str(problem.FanInStack.size())
                            + " and WukongProblem.OUTPUT_THRESHOLD: " + str(WukongProblem.OUTPUT_THRESHOLD))
                        logger.debug(result) 
                        # return False so we are not considered to be the final Executor that displays final results. There will
                        # be many executors that reach the OUPUT_THRESHOLD and stop.
                        return False
                    }
                
                if (WukongProblem.memoize and WukongProblem.USESERVERLESSNETWORKING):
                    removePairingNameMsgForParent = MemoizationMessage()
                    removePairingNameMsgForParent.messageType = MemoizationController.MemoizationMessageType.REMOVEPAIRINGNAME
                    removePairingNameMsgForParent.senderID = problem.problemID
                    removePairingNameMsgForParent.problemOrResultID = problem.problemID
                    removePairingNameMsgForParent.memoizationLabel = None
                    removePairingNameMsgForParent.result = None
                    removePairingNameMsgForParent.FanInStack = None
                    ServerlessNetworkingMemoizer.send1(removePairingNameMsgForParent)
                    ack = ServerlessNetworkingMemoizer.rcv1()
                
                # Unwind recursion but real executor could simply terminate instead.
                return False
            else:  # we are last Executor and first executor's result is in previousValue.
                # TODO: Convert to Python
                synchronized(FanInSychronizer.getPrintLock()) {
                    logger.debug("FanIn: ID: " + problem.problemID + ": FanInID: " + parentProblem.problemID + ": " 
                        + ": Returned from put: executor isLastFanInExecutor ")
                    logger.debug(subproblemResults.get(0))
               
                    logger.debug("ID: " + str(problem.problemID) + ": call combine ***************")
                }
                    # combine takes the result for this executor and the results for the sibling subproblems obtained by
                    # the sibling executors to produce the result for this problem.
                    
                    # Add the result for this executor to the subproblems. 
                    subproblemResults.append(result)
                    
                    # When not using ServerLessNetworking:
                    # Above, we checked if we are last Executor by putting result in map, which, if we are last executor
                    # left result in map and returns the sibling result, which was first. So it is result that is sitting
                    # in the map. Now combine adds this result and the sibling's subProblem result, and 
                    # stores the result of add as
#rhc: end Fan-In operation
                    
# rhc: start Fan-In task 
                    User.combine(subproblemResults, result)
                    
                    # Note: It's possible that we got a memoized value, e.g., 1 and we added 1+0 to get 1 and we are now
                    # memoizing 1, which we do not need to do. 
                    # Option: Track locally the memoized values we get and put so we don't put duplicates, since get/put is expensive
                    # when memoized storage is remote.
                                        
                    if (WukongProblem.memoize):
                        memoizedLabel = User.memoizeIDLabeler(parentProblem)
                        # put will memoize a copy of result
                        # rhc: store result with subProblem
                        memoizationResult = FanInSychronizer.put(memoizedLabel,result)
                        #synchronized(FanInSychronizer.getPrintLock()) {
                        logger.debug("Exector: result.problemID: " + str(result.problemID) + " put memoizedLabel: " + str(memoizedLabel) + " result: " + str(result))
                        #}
                    
                    if (WukongProblem.USESERVERLESSNETWORKING and WukongProblem.memoize):
                        deliverResultMsg = new MemoizationMessage()
                        deliverResultMsg.messageType = MemoizationController.MemoizationMessageType.DELIVEREDVALUE
                        deliverResultMsg.senderID = problem.problemID
                        deliverResultMsg.problemOrResultID = result.problemID
                        String memoizedLabel = User.memoizeIDLabeler(parentProblem)
                        deliverResultMsg.memoizationLabel = memoizedLabel
                        deliverResultMsg.result = result
                        deliverResultMsg.FanInStack = None
                        ServerlessNetworkingMemoizer.send1(deliverResultMsg)
                        
                        ack = ServerlessNetworkingMemoizer.rcv1()
                    
                    if (WukongProblem.USESERVERLESSNETWORKING):
                        FanInID = parentProblem.problemID
                        if (FanInID.equals("root")):
                            # synchronized(FanInSychronizer.getPrintLock()) {
                            #     System.out.println("Executor: Writing the final value to root: " + result) # result)
                            # }
                            logger.debug("Executor: Writing the final value to root: " + str(result))
                            siblingResult = FanInSychronizer.resultMap.put("root",result) 

                        FanInExecutor = parentProblem.becomeExecutor
                    # This executor continues to do Fan-In operations with the new problem result.
                
            # end we are second executor
        # rhc: end Fan-In task 

            # Instead of doing all of the work for sorting as we unwind the recursion and call merge(),
            # we let the executors "unwind" the recursion using the explicit FanIn stack.

        # end while (stack not empty)
        
        # Assuming that we are done with all problems and so done talking to Memoization Controller
        if (WukongProblem.memoize and WukongProblem.USESERVERLESSNETWORKING):
            removePairingNameMsgForParent = MemoizationMessage() # MemoizationMessage
            removePairingNameMsgForParent.messageType = MemoizationController.MemoizationMessageType.REMOVEPAIRINGNAME
            removePairingNameMsgForParent.senderID = problem.problemID
            removePairingNameMsgForParent.problemOrResultID = problem.problemID
            removePairingNameMsgForParent.memoizationLabel = None
            removePairingNameMsgForParent.result = None
            removePairingNameMsgForParent.FanInStack = None
            ServerlessNetworkingMemoizer.send1(removePairingNameMsgForParent)
            ack = ServerlessNetworkingMemoizer.rcv1() # ResultType
        
        # Only the final executor, i.e., the last executor to execute the final Fan-in task, makes it to here.
        return True
    

class WukongResult(object):
    pass

class FanInSychronizer(object):
    pass

class ServerLessNetworkingUniSenderHelper(object):
    pass

class ServerLessNetworkingUniReceiverHelper(object):
    pass

class ServerlessNetworkingClientServer(object):
    def __init__(
        self,
        connections,
        client_channel
    ):
        self.connections = connections
        self.client_channel = client_channel
    
    def send1(
        self,
        msg : MemoizationMessage
    ):
        pass #connections.send1(msg)

class DivideAndConquerExecutor(Thread):
    def __init__(
        self,
        problem : ProblemType,
        group=None, 
        target=None, 
        name=None,
        args=(), 
        kwargs=None, 
        verbose=None
    ):
        super(DivideAndConquerExecutor, self).__init__(group=group, target=target, name=name, verbose=verbose)
        self.problem = problem 
    
    def run(self):
        # Start fan-out task.
        if (WukongProblem.memoize and WukongProblem.USESERVERLESSNETWORKING):
            ServerlessNetworkingMemoizer = MemoizationController.getInstance().pair(problem.problemID)
            ack = ServerlessNetworkingMemoizer.rcv1()

        # Pre-process problem, if required.
        User.preprocess(problem)

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

        if WukongProblem.memoize:
            # Here, we want to get the value previously computed for this subproblem
            # as opposed to below where we put a computed value for a subProblem
            # Note that the problem ID may be "4-3-2" or "4-2" but the memoized
            # result is 2 in both cases. So in addition to problemLabeler(),
            # we have memoizedLabeler().

            promiseMsg = MemoizationMessage()
            promiseMsg.messageType = MemoizationController.MemoizationMessageType.PROMISEVALUE
            promiseMsg.senderID = problem.problemID
            promiseMsg.problemOrResultID = problem.problemID
            promiseMsg.becomeExecutor = problem.becomeExecutor
            promiseMsg.didInput = problem.didInput
            
            # e.g., if problem.problemID is "4-3", memoizedLabel is "3"
            memoizedLabel = User.memoizeIDLabeler(problem) 
            promiseMsg.memoizationLabel = memoizedLabel
            promiseMsg.result = None    
            promiseMsg.FanInStack = problem.FanInStack
            
            #synchronized(FanInSychronizer.getPrintLock()) {
                #System.out.println("memoized send1: problem.problemID " + problem.problemID + " memoizedLabel: " + memoizedLabel)
            logger.debug("memoized send1: problem.problemID " + str(problem.problemID) + " memoizedLabel: " + str(memoizedLabel))
            #}
            
            ServerlessNetworkingMemoizer.send1(promiseMsg)
            
            #synchronized(FanInSychronizer.getPrintLock()) {
                #System.out.println("memoized get: problem.problemID " + problem.problemID + " getting ack.")
            logger.debug("memoized get: problem.problemID " + str(problem.problemID) + " getting ack.")
            #}
            
            result = ServerlessNetworkingMemoizer.rcv1()
            
            #synchronized(FanInSychronizer.getPrintLock()) {
                #System.out.println("memoized get: problem.problemID " + problem.problemID + " got ack.")
            logger.debug("memoized get: problem.problemID " + str(problem.problemID) + " got ack.")
            #}
            
            if (result == MemoizationController.nullResult):
                # no memoized result
                #System.out.println("memoized get: problem.problemID " + problem.problemID + " ack was None result.")
                logger.debug("memoized get: problem.problemID " + str(problem.problemID) + " ack was None result.")
                result = None
            elif (result == MemoizationController.stopResult):
                # end executor, to be "restarted" later when subproblem result becomes available
                #System.out.println("memoized get: problem.problemID " + problem.problemID + " ack was stop.")
                logger.debug("memoized get: problem.problemID " + str(problem.problemID) + " ack was stop.")
                return 
            else:
                # got a memoized result for problem, but the result's ID is the ID of the problem whose result 
                # was memoized, which is not the problem we are working on here. So set ID to proper ID.
                result.problemID = problem.problemID    
            #synchronized(FanInSychronizer.getPrintLock()) {
                #System.out.println("memoized get: problem.problemID " + problem.problemID + " memoizedLabel: " + memoizedLabel + " memoized result: " + result)
            logger.debug("memoized get: problem.problemID " + str(problem.problemID) + " memoizedLabel: " + str(memoizedLabel) + " memoized result: " + str(result))
            #}
        
        if not WukongProblem.memoize or (WukongProblem.memoize and result is None):
            result = ResultType()

            # rhc: Can we do this if also doing Memoization? I think so.
            if (problem.FanInStack.size() == WukongProblem.INPUT_THRESHOLD and problem.didInput == False):
                User.inputProblem(problem)
                problem.didInput = True
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
            #     User.inputProblem(problem) 
            #     synchronized(FanInSychronizer.getPrintLock()) {
            #         System.out.println("inputProblemNew: problem.from: " + problem.from + " problem.to: " + problem.to 
            #             + " problem.FanInStack.size(): " + problem.FanInStack.size() + " size: " + size)
            #         for (int i=0 i<problem.numbers.length i++)
            #             System.out.print(problem.numbers[i] + " ")
            #         System.out.println()
            #     }
            # }
        
            # Base case is a sequential algorithm though possibly on a problem of size 1
            if (User.baseCase(problem)):
                if (not problem.didInput):
                    #System.out.println("Error: SEQUENTIAL_THRESHOLD reached before INPUT_THRESHOLD, but we cannot sort the numbers until we input them.")
                    #System.out.println("problem.SEQUENTIAL_THRESHOLD: " + problem.SEQUENTIAL_THRESHOLD + " problem.INPUT_THRESHOLD: " + problem.INPUT_THRESHOLD)
                    logger.debug("Error: SEQUENTIAL_THRESHOLD reached before INPUT_THRESHOLD, but we cannot sort the numbers until we input them.")
                    logger.debug("problem.SEQUENTIAL_THRESHOLD: " + str(problem.SEQUENTIAL_THRESHOLD) + " problem.INPUT_THRESHOLD: " + str(problem.INPUT_THRESHOLD))
                    exit(1)
                }
                User.sequential(problem,result)

                #System.out.println("base case: result before ProcessBaseCase(): " + result)
                logger.debug("base case: result before ProcessBaseCase(): " + str(result))
                WukongProblem.ProcessBaseCase(problem,result,ServerlessNetworkingMemoizer)

                # rhc: At this point, the recursion stops and we begin the Fan-In operations for this leaf node executor.
            else: # not baseCase
# rhc: start Fan-Out task
                ArrayList<ProblemType> subProblems = new ArrayList<ProblemType>()
                User.divide(problem, subProblems)

# rhc: end Fan-Out task

# rhc: start Fan-Out operation
                # Makes recursive call to run() for one subproblem and a new executor for the other(s).
                # Calls User.computeInputsOfSubproblems(problem,subProblems) when level == ProblemType.INPUT_THRESHOLD
                # and then divides the input of parent into the two inputs of the children. 
                WukongProblem.Fanout(problem, subProblems, ServerlessNetworkingMemoizer)
# rhc: end Fan-Out operation

                # After the executor is the first task of a Fan-In, or computes the final result, its recursion unwinds
                # and there is nothing to do on the unwind.
                return
            # !baseCase
        else:
            #synchronized(FanInSychronizer.getPrintLock()) {
            #    System.out.println("else in template: For Problem: " + problem + " Memoized result: " + result)
            logger.debug("else in template: For Problem: " + str(problem) + " Memoized result: " + str(result))
            #}
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

        finalRemainingExecutor = WukongProblem.FanInOperationandTask(problem,result,memoizedResult,ServerlessNetworkingMemoizer)
#rhc: end Fan-In operation and Fan-In task.

        # The executor that is the last fan-in task of the final fan-in outputs the result. the
        # result will have been saved in the map with key "root."

        # Need to output the value having key "root"
        if (finalRemainingExecutor):
            User.outputResult()
        
        return