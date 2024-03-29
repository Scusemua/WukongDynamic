import base64
import cloudpickle
import re
import sys
import socket 
import uuid
import json
import threading 
import time 

from ..server.state import State 
from ..server.api import send_object, recv_object
from ..server.util import make_json_serializable, decode_and_deserialize

from .memoization.util import MemoizationMessage, MemoizationMessageType
from .invoker import invoke_lambda
from ..constants import REDIS_IP_PRIVATE, TCP_SERVER_IP

import redis 
import logging
from logging import handlers
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s')
ch = logging.StreamHandler(sys.stdout)
ch.setFormatter(formatter)
#logger.addHandler(ch)

# fh = handlers.RotatingFileHandler("divide_and_conquer.log", maxBytes=(1048576*5), backupCount=7)
# fh.setFormatter(formatter)
# logger.addHandler(fh)

if logger.handlers:
   for handler in logger.handlers:
      handler.setFormatter(formatter)

root = logging.getLogger()
if root.handlers:
    for handler in root.handlers:
       handler.setFormatter(formatter)

debug_lock = threading.Lock()

logger.propagate = False

redis_client = redis.Redis(host = REDIS_IP_PRIVATE, port = 6379)

class WukongProblem(object):
    # Get input arrays when the level reaches the INPUT_THRESHOLD, e.g., don't grab the initial 256MB array,
    # wait until you reach level , say, 1, when there are two subproblems each half as big.
    # To input at the start of the problem (root), use 0 the stack has 0 elements on it for the root problem.
    # To input the first two subProblems, use 1. Etc.
    #
    # This value should not be changed. If the self.UserProgram provides DivideandConquerFibonacci.ProblemType.INPUT_THRESHOLD, we will use that value
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
    OUTPUT_THRESHOLD = sys.maxsize
    
    # memoize = False
    
    # For fan-in use serverless networking where invoked executor sends result to become executor,
    # or use redis-like shared storage to write and read results.
    USESERVERLESSNETWORKING = False
    #become_executor = False
    
    # Identifies the SubProblem used as a key for Wukong Storage, where the value is a sorted subsegment of the array
    #String problem_id
    # Stack of storage keys from root to leaf node (where there is a leaf node for each base case).
    #Stack<DivideandConquerFibonacci.ProblemType> fan_in_stack
    
    def __init__(
        self, 
        did_input = False,
        become_executor = False,
        problem_id = None,
        fan_in_stack = [],
        memoization_label_on_restart = None,
        UserProgram = None
    ):
        self.did_input = did_input 
        self.become_executor = become_executor
        self.problem_id = problem_id 
        self.fan_in_stack = fan_in_stack
        self.UserProgram = UserProgram 
        self.memoization_label_on_restart = memoization_label_on_restart

        # TODO: When creating new DivideAndConquerExecutor objects, we do not supply the result type and problem type arguments.
        #       Also, the existing classes that are passed around to possibly several threads are not thread safe, I think.

    def __str__(self):
        return "WukongProblem(memoization_label_on_restart=" + str(self.memoization_label_on_restart) + ")"

    @property
    def memoize(self):
        print("WukongProblem memoize (hard-coded to be False)")
        return False 

    def ProcessBaseCase(self, problem, result, ServerlessNetworkingMemoizer):
        # memoizedResult True means that we got a memoized result (either at the base case or for a non-base case)
        # and we don't want to memoize this result, which would be redundant.

        # Each started executor eventually executes a base case (sequential sort) and then the executor competes
        # with its sibling at a Fan-In to do the Fan0-In task, which is a merge.
        
        # problem is a base case problem that uses sequential() result is given the same ID
        # Or result was memoized, i.e., some other problem, and we need to update the problem_id of result from 
        # the other problem's ID to this problem's ID.
        
        # Need to do this here as the base case sequential() is not required to set resultlabel = problemLabel
        result.problem_id = problem.problem_id    

         # Note: not sending a REMOVEPAIRINGNAME message since the executor is now unwinding and may be sending
         # messages all the way up.

        # rhc: no need to check !memoizedResult since if memoizedResult we stopped, and even if we did not stop
        # we only compute base case if we don't get memoized result for the base case. (If we get memoized
        # result, we unwind by fan-in to parent of current problem.
        # if (WukongProblem.USESERVERLESSNETWORKING and not memoizedResult and WukongProblem.memoize) {
        if (problem.memoize):
            deliverResultMsg = MemoizationMessage(
                message_type = MemoizationMessageType.DELIVEREDVALUE,
                sender_id = problem.problem_id,
                problem_or_result_id = result.problem_id,
                memoization_label = self.UserProgram.memoizeIDLabeler(problem),
                result = result,
                fan_in_stack = None
            )
            logger.debug("result: " + str(result))
            ServerlessNetworkingMemoizer.send1(deliverResultMsg)

            ack = ServerlessNetworkingMemoizer.rcv1()

    # Replace with actual Wukong fan-out code
    # subProblems was originally of type ArrayList<DivideandConquerFibonacci.ProblemType>
    #@staticmethod
    def Fanout(self, problem, subProblems : list, ServerlessNetworkingMemoizer, state : State):
        # Retrieve subsegment of array when the threshold is reached. Fanout is only called while 
        # recursive calls are made and the stack is thus growing in size. Fanout is not called 
        # while the recursion is unwinding and the stack is shrinking. Thus, this condition will 
        # not be True twice, i.e., it will be True as the stack is growing but not while the stack is shrinking.
        #
        # Note: If using the size of the subproblem instead of the level, use this if-statement:
        #   if ((problem.to - problem.from + 1) <= WukongProblem.INPUT_THRESHOLD)
        #
        if (len(problem.fan_in_stack) >= WukongProblem.INPUT_THRESHOLD):
            self.UserProgram.computeInputsOfSubproblems(problem, subProblems)
        
        # In General:
        # DivideandConquerFibonacci.ProblemType becomeSubproblem = chooseBecome(subProblems)
        # where we choose the become subProblem based on the criterion
        # specified by the self.UserProgram. For example:
        # default: choose the last subProlem in list subProblems, which means
        # the self.UserProgram can control the choice by adding to list subProblems in
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
        logger.debug("len(subProblems): " + str(len(subProblems)))
        for i in range(len(subProblems) - 1):
            invokedSubproblem = subProblems[i]

            # Generate the executor/storage label for the subproblem
            # Supply all: problem, subProblems, subProblem, i in case they are needed.
            # ID = self.UserProgram.problemLabeler(invokedSubproblem,i, problem, subProblems)
            ID = self.fanout_problem_labeler(
                parent_problem_label = problem.problem_id, 
                fanout_child_index = i, 
                num_child_problems = len(subProblems))

            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as websocket:
                websocket.connect(TCP_SERVER_IP)
                faninId = self.fanin_problem_labeler(problem_label = ID)
                state.keyword_arguments = {"n": 2}
                logger.info("Calling 'create' on TCP Server for 'FanIn', FanInID=%s" % (str(faninId)))
                self.create(websocket, "create", "FanIn", faninId, state)            

            logger.debug(">> %s: generated fan-out ID \"%s\"" % (problem.problem_id, ID))

            #invokedSubproblem.problem_id = problem.problem_id + "-" + ID
            invokedSubproblem.problem_id = ID
            
            invokedSubproblem.become_executor = False
            
            # Add label to stack of labels from root to subproblem
            childFanInStack = problem.fan_in_stack.copy() # Stack<DivideandConquerFibonacci.ProblemType>

            with debug_lock:
                logger.info(str(problem.problem_id) + ": Fanout: push on childFanInStack: (parent) problem: " + str(problem))
                childFanInStack.append(problem)

            invokedSubproblem.fan_in_stack = childFanInStack

            with debug_lock:
                problem_stack_string = "{}: Fanout: parent stack (len = {}): ".format(problem.problem_id, len(problem.fan_in_stack))
                for j in range(0, len(problem.fan_in_stack)):
                    problem_stack_string += str(problem.fan_in_stack[j]) + " "
                logger.debug(problem_stack_string)
                sub_problem_stack_string = "{}: Fanout: subProblem stack (len = {}): ".format(problem.problem_id, len(invokedSubproblem.fan_in_stack))
                for j in range(len(invokedSubproblem.fan_in_stack)):
                    sub_problem_stack_string += str(invokedSubproblem.fan_in_stack[j]) + " "
                logger.debug(sub_problem_stack_string)
            
            # If parent input was done then we will grab part of the parent's values and the child's input can be considered done too.
            invokedSubproblem.did_input = problem.did_input
            invokedSubproblem.UserProgram = self.UserProgram
            
            if (problem.memoize):
                addPairingNameMsgForInvoke = MemoizationMessage(
                    message_type = MemoizationMessageType.ADDPAIRINGNAME,
                    sender_id = problem.problem_id,
                    problem_or_result_id = invokedSubproblem.problem_id,
                    memoization_label = None,
                    result = None,
                    fan_in_stack = None
                )
                ServerlessNetworkingMemoizer.send1(addPairingNameMsgForInvoke)
                
                ack = ServerlessNetworkingMemoizer.rcv1()
            
            from .dc_executor import DivideAndConquerExecutor
            # New subproblem

            # logger.debug(">> Type of current thread: " + str(type(threading.current_thread())))

            #logger.info("Creating new DivideAndConquerExecutor object now...")
            # newExecutor = DivideAndConquerExecutor(
            #     problem = invokedSubproblem,
            #     problem_type = threading.current_thread().problem_type,
            #     result_type = threading.current_thread().result_type,
            #     null_result = threading.current_thread().null_result,
            #     stop_result = threading.current_thread().stop_result
            # )

            logger.debug(problem.problem_id + ": Fanout: ID: " + str(problem.problem_id) + " invoking new right executor: "  + str(invokedSubproblem.problem_id))
            # start the executor
            
            payload = {
                "problem": invokedSubproblem,
                "problem_type": threading.current_thread().problem_type,
                "result_type": threading.current_thread().result_type,
                "null_result": threading.current_thread().null_result,
                "stop_result": threading.current_thread().stop_result,
                "state": State(function_name = "WukongDivideAndConquer", restart = False, function_instance_ID = str(uuid.uuid4()))
            }

            invoke_lambda(payload = payload)

            # where:
            #enum MemoizationMessageType {ADDPAIRINGNAME, REMOVEPAIRINGNAME, PROMISEDVALUE, DELIVEREDVALUE}
    
            #class MemoizationMessage {
            #    MemoizationController.MemoizationMessageType message_type
            #    String sender_id
            #    String problem_or_result_id
            #    String memoization_label
            #    DivideandConquerFibonacci.ResultType result
            #    Stack<DivideandConquerFibonacci.ProblemType> fan_in_stack
            #    boolean become_executor
            #    boolean did_input
            #}
            
            #newExecutor.start()
            # Note: no joins for the executor threads - when they become leaf node executors, they will perform all of the 
            # combine() operations and then return, which unwinds the recursion with nothing else to do.
        # Do the same for the become executor
        becomeSubproblem = subProblems[len(subProblems)-1] # DivideandConquerFibonacci.ProblemType
        
        # Generate the executor/storage label for the subproblem
        # Supply all: problem, subProblems, subProblem, i in case they are needed.
        # ID = self.UserProgram.problemLabeler(becomeSubproblem, len(subProblems) - 1, problem, subProblems) # String
        ID = self.fanout_problem_labeler(
            parent_problem_label = problem.problem_id, 
            fanout_child_index = len(subProblems) - 1, 
            num_child_problems = len(subProblems))        
        
        # If two different subProblems in the divide and conquer tree can have the same label, then there would
        # be a problem since these labels are also used to label fan-in tasks and we cannot have two fan-in tasks
        # with the same label. We make sure labels are unique by using the path of labels from the root to this 
        # subproblem, e.g., instead of "3" and "2" we use "4-3" and "4-2" where "4" is the label of the 
        # root and subProblem's "#" and "4" are children of the root (as in Fibonacci).
        becomeSubproblem.problem_id = ID #problem.problem_id + "-" + ID

        becomeSubproblem.become_executor = True
        
        logger.debug(">> %s: Become fanout ID: %s" % (problem.problem_id, ID))
        #logger.debug("Exiting after call to fanout_problem_labeler()")
        #exit(0)

        # No need to clone the problem stack for the become subproblem as we clones the stacks for the other subProblems  
        #@SuppressWarnings("unchecked")
        #Stack<DivideandConquerFibonacci.ProblemType> childFanInStack = (Stack<DivideandConquerFibonacci.ProblemType>) problem.fan_in_stack.clone()
        childFanInStack = problem.fan_in_stack # Stack<DivideandConquerFibonacci.ProblemType> 
        childFanInStack.append(problem)
        becomeSubproblem.fan_in_stack = childFanInStack
        
        # If parent input was done then we will grab part of the parent's values and the child's input can be considered done too.
        becomeSubproblem.did_input = problem.did_input
        becomeSubproblem.UserProgram = self.UserProgram

        # payload = {
        #     "problem": becomeSubproblem,
        #     "problem_type": threading.current_thread().problem_type,
        #     "result_type": threading.current_thread().result_type,
        #     "null_result": threading.current_thread().null_result,
        #     "stop_result": threading.current_thread().stop_result
        # }

        # invoke_lambda(payload = payload)

        become = DivideAndConquerExecutor(
            state = state,
            problem = becomeSubproblem,
            problem_type = threading.current_thread().problem_type,
            result_type = threading.current_thread().result_type,
            null_result = threading.current_thread().null_result,
            stop_result = threading.current_thread().stop_result)
        
        with debug_lock:
            logger.debug(problem.problem_id + ": Fanout: ID: " + str(problem.problem_id)  + " becoming left executor: "  + str(becomeSubproblem.problem_id))
        
        if (problem.memoize):
            addPairingNameMsgForBecomes = MemoizationMessage(
                message_type = MemoizationMessageType.ADDPAIRINGNAME,
                sender_id = problem.problem_id,
                problem_or_result_id = becomeSubproblem.problem_id,
                memoization_label = None,
                result = None,
                fan_in_stack = None
            ) # MemoizationMessage

            ServerlessNetworkingMemoizer.send1(addPairingNameMsgForBecomes)
            
            ack1 = ServerlessNetworkingMemoizer.rcv1() # DivideandConquerFibonacci.ResultType

            removePairingNameMsgForParent = MemoizationMessage(
                message_type = MemoizationMessageType.REMOVEPAIRINGNAME,
                sender_id = problem.problem_id,
                problem_or_result_id = problem.problem_id,
                memoization_label = None,
                result = None,
                fan_in_stack = None
            ) # MemoizationMessage
            ServerlessNetworkingMemoizer.send1(removePairingNameMsgForParent)
            
            ack2 = ServerlessNetworkingMemoizer.rcv1() # DivideandConquerFibonacci.ResultType
        
        # No need to start another Executor, current executor can recurse until the base case. At that point, the executor
        # will compete at a Fan-In point to do a merge with its "sibling" executor.
        #       1-2
        #      /   \
        #     1     2    # Executors 1 and 2 have a Fan-In at 1-2. One of executors 1 or 2 will merge input 1 and input 2 
        # and the other will stop by executing return, which will unwind its recursion. 
        become.run()  

        
        # Problem was pushed on subProblem stacks and stacks will be passed to invoked Executor. Here, self.UserProgram has a chance to 
        # trim any data in problem that is no longer needed (e.g., input array of problem) so less data is passed to 
        # Executor.
        
        # Option: create method WukongProblem.trimProblem() that sets WukongProblem.fan_in_stack.None and call 
        # WukongProblem.trimProblem() here before calling self.UserProgram.trimProblem(problem) I believe that problem's
        # fan_in_stack is no longer needed and can always be trimmed, though it might be helpful for debugging.
        self.UserProgram.trim_problem(problem)
        #rhc: end Fan-Out operation

        # Recursion unwinds - nothing happens along the way.
        return
    
    def create(self, websocket, op, type, name, state):
        """
        Create a remote object on the TCP server.

        Arguments:
        ----------
            websocket (socket.socket):
                Socket connection to the TCP server.
                TODO: We pass this in, but in the function body, we connect to the server.
                      In that case, we don't need to pass a websocket. We'll just create one.
                      We should only bother with passing it as an argument if its already connected.
            
            op (str):
                The operation being performed. 
                TODO: Shouldn't this always be 'create'?
            
            type (str):
                The type of the object to be created.

            name (str):
                The name (which serves as an identifier) of the object to be created.
            
            state (state.State):
                Our current state.
        """
        # msg_id for debugging
        msg_id = str(uuid.uuid4())
        logger.debug("Sending 'create' message to server. Op='%s', type='%s', name='%s', id='%s', state=%s" % (op, type, name, msg_id, state))

        # we set state.keyword_arguments before call to create()
        message = {
            "op": op,
            "type": type,
            "name": name,
            "state": make_json_serializable(state),
            "id": msg_id
        }

        msg = json.dumps(message).encode('utf-8')
        send_object(msg, websocket)
        logger.debug("Sent 'create' message to server")

        # Receive data. This should just be an ACK, as the TCP server will 'ACK' our create() calls.
        ack = recv_object(websocket)

    def fanout_problem_labeler(
        self,
        parent_problem_label = None,
        fanout_child_index = None,
        num_child_problems = None
    ) -> str:
        """
        This function will be called once for each downstream task in a fan-out operation.

        This function will take the parent label and concatenate [`fanout_child_index`, `size`]. 

        As an example, the "root" task would be "[0,1]". If "root" forked three children, the problem 
        labels of the three children would be "[0,1][0,3]", "[0,1][1,3]", and "[0,1][2,3]".
        
        Keyword Arguments:
            parent_problem_label (str): Label of the parent problem. This is of the form L[x,y][u,v], 
                                        where L can be empty.

            fanout_child_index (int): The index of the downstream task (i.e., this is child #1, #2, etc.).

            num_child_problems (int): The number of child/downstream tasks in this fan-out operation.
        """
        return parent_problem_label + "[%d,%d]" % (fanout_child_index, num_child_problems)

    def fanin_problem_labeler(
        self,
        problem_label = None,
    ) -> str:
        """
        The parent label is of the form L[x,y][u,v]. This function returns L[<x + y>, y]. 
        That is, this replaces the [x,y][u,v] with [x+y,y]. So the resulting string is of the form
        [z,y] where z = x + y.

        Note that the root problem is a special case because it is of the form [0,1].

        Let's say we have "[0,1][0,3][1,3]" (i.e., two fork/fan-out operations). 
        The fan-in would be "[0,1][3,3]".

        Keyword Arguments:
            problem_label (str): Label of the problem. This is of the form L[x,y][u,v], where L can be empty.
        """
        # The root problem is a special case because it is of the form [0,1].
        if problem_label == self.UserProgram.root_problem_id:
            logger.debug(">> returning hard-coded " + str(self.UserProgram.final_result_id) + " for fanin problem label (problem_label = %s)" % problem_label)
            return self.UserProgram.final_result_id

        # Indexing by `[:-1]` leaves off just the last character.
        second_right_bracket_index = problem_label[:-1].rindex("]")
        associated_left_bracket_index = problem_label.rindex("[", 0, second_right_bracket_index)
        comma_index = problem_label.index(",", associated_left_bracket_index, second_right_bracket_index)

        x = int(problem_label[associated_left_bracket_index+1:comma_index])
        y = int(problem_label[comma_index+1:second_right_bracket_index])

        fanin_label = problem_label[0:associated_left_bracket_index] + "[%d,%d]" % (x + y, y)

        logger.debug(">> returning fanin label \"%s\", parent problem label = \"%s\", x = %d, y = %d" % (fanin_label, problem_label, x, y))

        return fanin_label

    def __extract_last_brackets(self, label):
        """
        Given a string `label` of the form "[a,b][c,d]...[y,z]" where the letters are integers, this returns
        a 2-tuple (i.e., a tuple) where the first element is the string "[a,b][c,d]...[w,x]" and the second
        element is another 2-tuple where the first element is the integer y and the second element is the
        integer z.
        """
        pass 

class WukongResult(object):
    """
        problem_id is of type string.
    """
    def __init__(
        self, 
        problem_id = None
    ):
        """
            problem_id (str)
        """
        self.problem_id = problem_id

class UserProgram(object):
    def __init__(self):
        pass 

    def trim_problem(self, problem : WukongProblem):
        pass 