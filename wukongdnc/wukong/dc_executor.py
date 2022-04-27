import sys
import threading
import uuid 
import socket 
import cloudpickle
import base64
import json 
import redis 

from threading import Thread 
from .wukong_problem import WukongProblem
from .fanin_synchronizer import FanInSychronizer

from ..server.state import State 

from ..constants import TCP_SERVER_IP, REDIS_IP_PRIVATE

from .memoization.util import MemoizationMessage, MemoizationMessageType

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

def make_json_serializable(obj):
    """
    Serialize and encode an object.
    """
    return base64.b64encode(cloudpickle.dumps(obj)).decode('utf-8')

def decode_and_deserialize(obj):
    """
    Decode and deserialize an object.
    """
    return cloudpickle.loads(base64.b64decode(obj))

def send_object(obj, websocket):
    """
    Send obj to a remote entity via the given websocket.
    The TCP server uses a different API (streaming via file handles), so it's implemented differently. 
    This different API is in tcp_server.py.    

    Arguments:
    ----------
        obj (bytes):
            The object to be sent. Should already be serialized via cloudpickle.dumps().
        
        websocket (socket.socket):
            Socket connected to a remote client.
    """
    logger.debug("Will be sending a message of size %d bytes." % len(obj))
    # First, we send the number of bytes that we're going to send.
    websocket.sendall(len(obj).to_bytes(2, byteorder='big'))
    # Next, we send the serialized object itself. 
    websocket.sendall(obj)

def recv_object(websocket):
    """
    Receive an object from a remote entity via the given websocket.

    This is used by clients. There's another recv_object() function in TCP server.
    The TCP server uses a different API (streaming via file handles), so it's implemented differently. 
    This different API is in tcp_server.py.

    Arguments:
    ----------
        websocket (socket.socket):
            Socket connected to a remote client.    
    """
    # First, we receive the number of bytes of the incoming serialized object.
    incoming_size = websocket.recv(2)
    # Convert the bytes representing the size of the incoming serialized object to an integer.
    incoming_size = int.from_bytes(incoming_size, 'big')
    logger.debug("Will receive another message of size %d bytes" % incoming_size)
    # Finally, we read the serialized object itself.
    return websocket.recv(incoming_size).strip()

class DivideAndConquerExecutor(Thread):
    def __init__(
        self,
        problem = None,
        group=None,     # Used by Thread.
        target=None,    # Used by Thread.
        name=None,      # Used by Thread.
        problem_type = None, 
        result_type = None,
        null_result = None,
        stop_result = None,
        state = None
    ):
        # https://docs.python.org/3/library/threading.html#threading.Thread
        super(DivideAndConquerExecutor, self).__init__(group=group, target=target, name=name)
        self.state = state
        self.problem = problem              # refers to an INSTANCE of the user-provided ProblemType class
        self.problem_type = problem_type    # This is a Class.
        self.result_type = result_type      # This is a Class.
        self.null_result = null_result
        self.stop_result = stop_result
        self.redis_client = redis.Redis(host = REDIS_IP_PRIVATE, port = 6379) 
    
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
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as websocket:
            logger.debug("Connecting to " + str(TCP_SERVER_IP))
            websocket.connect(TCP_SERVER_IP)
            logger.debug("Successfully connected!")

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

    def synchronize_sync(self, websocket, op, name, method_name, state):
        """
        Synchronize on the remote TCP server.

        Arguments:
        ----------
            websocket (socket.socket):
                Socket connection to the TCP server.
                TODO: We pass this in, but in the function body, we connect to the server.
                      In that case, we don't need to pass a websocket. We'll just create one.
                      We should only bother with passing it as an argument if its already connected.
            
            op (str):
                The operation being performed. 
            
            method_name (str):
                The name of the synchronization method we'd be calling on the server.

            name (str):
                The name (which serves as an identifier) of the object we're using for synchronization.
            
            state (state.State):
                Our current state.
        """
        # see the note below about closing the websocket or not
        msg_id = str(uuid.uuid4())
        message = {
            "op": op, 
            "name": name,
            "method_name": method_name,
            "state": make_json_serializable(state),
            "id": msg_id
        }
        logger.debug("Calling %s. Message ID=%s" % (op, msg_id))
        msg = json.dumps(message).encode('utf-8')
        send_object(msg, websocket)
        data = recv_object(websocket)               # Should just be a serialized state object.
        state_from_server = cloudpickle.loads(data) # `state_from_server` is of type State
        return state_from_server

    def synchronize_async(self, websocket, op, name, method_name, state):
        """
        Synchronize on the remote TCP server.

        Arguments:
        ----------
            websocket (socket.socket):
                Socket connection to the TCP server.
                TODO: We pass this in, but in the function body, we connect to the server.
                      In that case, we don't need to pass a websocket. We'll just create one.
                      We should only bother with passing it as an argument if its already connected.
            
            op (str):
                The operation being performed. 
            
            method_name (str):
                The name of the synchronization method we'd be calling on the server.

            name (str):
                The name (which serves as an identifier) of the object we're using for synchronization.
            
            state (state.State):
                Our current state.
        """
        # see the note below about closing the websocket or not
        msg_id = str(uuid.uuid4())
        message = {
            "op": op, 
            "name": name,
            "method_name": method_name,
            "state": make_json_serializable(state),
            "id": msg_id
        }
        logger.debug("Calling %s. Message ID=%s" % (op, msg_id))
        msg = json.dumps(message).encode('utf-8')
        send_object(msg, websocket)

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
                self.problem.Fanout(self.problem, subProblems, ServerlessNetworkingMemoizer, self.state)
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

        finalRemainingExecutor = self.FanInOperationandTask(self.problem,result,memoizedResult,ServerlessNetworkingMemoizer)
        #rhc: end Fan-In operation and Fan-In task.

        # The executor that is the last fan-in task of the final fan-in outputs the result. the
        # result will have been saved in the map with key "root."

        # Need to output the value having key "root"
        if (finalRemainingExecutor):
            self.problem.UserProgram.output_result(self.problem.problem_id)
        
        return

    # The last executor to fan-in will become the Fan-In task executor. The actual executor needs to save its Merge output 
    # and increment/set counter/boolean. We handle this  here by using map.put(key,value), which returns the previous 
    # value the key is mapped to, if any, and None if not. So if put() returns None we are not the last executor to Fan-in. 
    # This is a special case for MergeSort, which always has only two Fan-In executors.
    def isLastFanInExecutorSynchronizer(self, faninId : str, result, subproblemResults: list, websocket : socket.socket) -> bool:
        #logger.debug("isLastFanInExecutorSynchronizer: Writing to " + faninId + " the value " + str(result))
        
        if self.state.keyword_arguments is None:
            self.state.keyword_arguments = {}
        self.state.keyword_arguments["result"] = result 
        self.state.return_value = None  # Reset state values before going to TCP server.
        self.state.blocking = False     # Reset state values before going to TCP server.

        self.state = self.synchronize_sync(websocket, "synchronize_sync", faninId, "try_fan_in", self.state)
        if self.state.blocking: 
            # not last executor
            logger.debug("Not last Executor, returning False")
            self.state.blocking = False
            return False
        else:
            logger.debug("Last executor: Return value from server: " + str(self.state.return_value))
            # last executor
            subproblemResults.append(self.state.return_value)
            self.state.return_value = None # Reset state value.

        return True

    # subproblemResults was previously ArrayList<DivideandConquerFibonacci.ResultType> 
    def isLastFanInExecutor(self, FanInID : str, result, subproblemResults: list) -> bool:
        # store result and check if we are the last executor to do this.
        # Add all of the other subproblem results to subproblemResults. We will add our sibling result later (before we call combine().)
        #FanInID = parentProblem.problem_id # String 

        copyOfResult = result.copy() # DivideandConquerFibonacci.ResultType
        copyOfResult.problem_id = result.problem_id
        
        logger.debug("isLastFanInExecutor: Writing to " + FanInID + " the value " + str(copyOfResult)) # result))

        siblingResult = None
        with debug_lock:
            # Atomic get-set. We pass 'True' for the `get` kwarg, so we get the old value.
            # Previously, the debug lock ensured atomocity of the whole get/set/exists operations.
            # Since we're using multiple Lambdas, the debug log doesn't do anything anymore. So, we 
            # need atomic get-set operation.
            #
            # This will need to be generalized to support fan-ins involving more than two executors.
            # It will NOT work in its current form if there are more than two executors fanning in.
            #siblingResult = redis_client.set(FanInID, cloudpickle.dumps(result), get = True)
            resultSerialized = cloudpickle.dumps(result)
            resultEncoded = base64.b64encode(resultSerialized)
            logger.debug("Result (to be written to Redis) encoded: '" + str(resultEncoded) + "'")
            # with redis_client.pipeline() as p:
            #     while True:
            #         try:
            #             p.watch(FanInID)
            #             siblingResultEncoded = p.get(FanInID) 
            #             p.multi()
            #             if siblingResultEncoded is None:
            #                 p.set(FanInID, resultEncoded)
            #             p.execute()
            #             break
            #         except redis.WatchError:
            #             continue

            siblingResultEncoded = self.redis_client.getset(FanInID, resultEncoded)

            # Data in Redis is stored as base64-encoded strings. Specifically, we first pickle the
            # data with cloudpickle, after which we encode it in base64. Thus, we must decode
            # and deserialize (in that order) the data after reading it from Redis.
            if siblingResultEncoded is not None:
                logger.debug("Obtained the following encoded String from Redis: '" + str(siblingResultEncoded) + "'")
                siblingResultSerialized = decode_base64(siblingResultEncoded)
                print("Encoded sibling result: '" + str(siblingResultSerialized) + "'")
                siblingResult = cloudpickle.loads(siblingResultSerialized)

            # if FanInID in FanInSychronizer.resultMap:
            #    siblingResult = FanInSychronizer.resultMap[FanInID]
            # else:
            #     FanInSychronizer.resultMap[FanInID] = result
        
        # firstFanInResult may be None
        if (siblingResult == None):
            return False
        else:
            copyOfSiblingResult = siblingResult.copy()
            copyOfSiblingResult.problem_id = siblingResult.problem_id
            
            subproblemResults.append(copyOfSiblingResult)
            return True

    # Perform Fan-in and possibly the Fan-in task 
    def FanInOperationandTask(self, problem, result, memoizedResult: bool, ServerlessNetworkingMemoizer) -> bool:
        # memoizedResult True means that we got a memoized result (either at the base case or for a non-base case)
        # and we don't want to memoize this result, which would be redundant.
        #rhc: start Fan-In operation
        with debug_lock:
            logger.debug(problem.problem_id + ": **********************Start Fanin operation:")
            logger.debug(problem.problem_id + ": Fan-in: problem ID: " + problem.problem_id)
            logger.debug(problem.problem_id + ": Fan-in: become_executor: " + str(problem.become_executor))
            fan_in_stack_string = problem.problem_id + ": Fan-in: fan_in_stack: "
            for i in range(len(problem.fan_in_stack)):
                fan_in_stack_string += str(problem.fan_in_stack[i]) + " "
            logger.debug(fan_in_stack_string)
        
        # Each started executor eventually executes a base case (sequential sort) or gets a memoized
        # result for a duplicate subProblem, and then the executor competes with its sibling(s) at a Fan-In 
        # to do the Fan0-In task, which is a combine().
        
        # True if this is "become" rather than "invoke" problem. Set below.
        FanInExecutor = False    
        
        # For USESERVERLESSNETWORKING, whether a subProblem is become or invoked is decided when the problem is created.
        # If we instead determine at runtime which problem is the last to fan-in, then the last to fan-in is 
        # the become, and the others are invoke.
        if (WukongProblem.USESERVERLESSNETWORKING):
            FanInExecutor = problem.become_executor
        
        local_problem_label = problem.problem_id
        logger.debug(">> Local problem label start of Fanin: \"%s\"" % local_problem_label)

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as websocket:
            websocket.connect(TCP_SERVER_IP) 
            while (len(problem.fan_in_stack) != 0):
                # Stop combining results when the results reach a certain size, and the communication delay for passing
                # the results is much larger than the time to combine them. The remaining combines can be done on one
                # processor. This is task collapsing of the remaining combine() tasks into one task.
                # However, we can only do this if we are sure all of the subproblems will reach the threshold. For example,
                # this works for mergeSort as sibling subproblems are all growing in size, more or less equally. But this
                # is not True for quicksort, which may have only one large subproblem that reaches the threshold as the 
                # other subproblems are small ones that get merged with the large one.

                #if (size >= WukongProblem.OUTPUT_THRESHOLD) { 
                if (len(problem.fan_in_stack) == WukongProblem.OUTPUT_THRESHOLD):
                    with debug_lock:
                        logger.debug(problem.problem_id + ": Exector: " + str(problem.problem_id) + " Reached OUTPUT_THRESHOLD for result: " + str(result.problem_id)
                            + " with problem.fan_in_stack.size(): " + str(len(problem.fan_in_stack))
                            + " and WukongProblem.OUTPUT_THRESHOLD: " + str(WukongProblem.OUTPUT_THRESHOLD))
                        logger.debug(result) 
                        # return False so we are not considered to be the final Executor that displays final results. There will
                        # be many executors that reach the OUPUT_THRESHOLD and stop.
                        return False

                parentProblem = problem.fan_in_stack.pop()
                faninId = self.problem.fanin_problem_labeler(problem_label = local_problem_label)
                
                with debug_lock:
                    logger.debug(problem.problem_id + ": Fan-in: problem ID: " + str(problem.problem_id) + " parentProblem ID: " + parentProblem.problem_id)
                    logger.debug(problem.problem_id + ": faninId: " + faninId)
                    logger.debug(problem.problem_id + ": Fan-in: problem ID: " + str(problem.problem_id) + " problem.become_executor: " + str(problem.become_executor) + " parentProblem.become_executor: " + str(parentProblem.become_executor))
                    logger.debug("")
                    
                # The last task to fan-in will become the Fan-In task executor. The actual executor needs to save its Merge output 
                # and increment/set counter/boolean. We handle this in our prototype here by using map.put(key,value), which 
                # returns the previous value the key is mapped to, if there is one, and None if not. So if put() returns None 
                # we are not the last executor to Fan-in.This is a special case for MergeSort, which always has only two 
                # Fan-In tasks.
                #
                # Both siblings write result to storage using key FanInID, The second sibling to write will get the 
                # first sibling's value in previousValue. Now the second sibling has both values.
                
                subproblemResults = list() # ArrayList<DivideandConquerFibonacci.ResultType> 
                #subproblemResultsSynchronizer = list() # ArrayList<DivideandConquerFibonacci.ResultType> 

                if (WukongProblem.USESERVERLESSNETWORKING):
                    FanIn = parentProblem.problem_id
                    if (FanInExecutor):
                        logger.debug("ID: " + problem.problem_id + ": FanIn: " + faninId + " was FanInExecutor: starting receive.")
                        h = ServerLessNetworkingUniReceiverHelper(FanIn)
                        h.start()
                        try:
                            h.join()
                        except Exception as e:
                            logger.error(repr(e))
                        # r is a copy of sent result
                        copyOfSentResult = h.result # DivideandConquerFibonacci.ResultType
                        subproblemResults.append(copyOfSentResult)
                        #synchronized(FanInSychronizer.getPrintLock()) {
                        #    System.out.println("ID: " + problem.problem_id + ": FanIn: " + parentProblem.problem_id + " was FanInExecutor: result received:" + copyOfSentResult)
                        logger.debug("ID: " + str(problem.problem_id) + ": FanIn: " + faninId + " was FanInExecutor: result received:" + str(copyOfSentResult))
                        #}
                    else:
                        # pair and send message
                        h = ServerLessNetworkingUniSenderHelper(FanIn,result)
                        h.start()
                        try:
                            h.join() # not really necessary
                        except Exception:
                            pass 
                        #synchronized(FanInSychronizer.getPrintLock()) {
                        #    System.out.println("Fan-In: ID: " + problem.problem_id + ": FanInID: " + parentProblem.problem_id + " was not FanInExecutor:  result sent:" + result)
                        #}
                        logger.debug("Fan-In: ID: " + str(problem.problem_id) + ": FanInID: " + faninId + " was not FanInExecutor: result sent:" + str(result))
                else:
                    logger.debug(problem.problem_id + ": parentProblem ID: " + parentProblem.problem_id + ", calling isLastFanInExector() now...") 

                    FanInExecutor = self.isLastFanInExecutorSynchronizer(faninId, result, subproblemResults, websocket)

                    # When return we either have our result and sibling result or or our result and None. For latter, we were first
                    # Executor to fan-in so we stop.
                    #FanInExecutor = self.isLastFanInExecutor(faninId, result, subproblemResults)

                    # Specific to Fibonnaci-2.
                    # if result == 0:
                    #     logger.debug("Fan-In: ID: " + str(problem.problem_id) + ": FanInID: " + faninId + ": Result is 0, so setting FanInExecutor to False.")
                    #     FanInExecutor = False 
                    # else:
                    #     logger.debug("Fan-In: ID: " + str(problem.problem_id) + ": FanInID: " + faninId + ": Result is NOT 0, so setting FanInExecutor to True.")
                    #     FanInExecutor = True
                    #     subproblemResults.append()
                
                # If we are not the last task to Fan-In then unwind recursion and we are done
            
                # TODO: Two threads are thinking they're a fan-in executor.
                if not FanInExecutor:
                    with debug_lock:
                        #value = FanInSychronizer.resultMap[faninId]
                        valueEncoded = self.redis_client.get(faninId)
                        valueSerialized = decode_base64(valueEncoded)
                        value = cloudpickle.loads(valueSerialized)
                        logger.debug("Fan-In: ID: " + str(problem.problem_id) + ": FanInID: " + faninId + ": is not become Executor and its value was: " + str(result) + " and after put is " + str((value)))
                    
                    if (len(problem.fan_in_stack) == WukongProblem.OUTPUT_THRESHOLD):
                        with debug_lock:
                            logger.debug("Exector: " + str(problem.problem_id) + " Reached OUTPUT_THRESHOLD for result: " + str(result.problem_id)
                                + " with problem.fan_in_stack.size(): " + str(len(problem.fan_in_stack))
                                + " and WukongProblem.OUTPUT_THRESHOLD: " + str(WukongProblem.OUTPUT_THRESHOLD))
                            logger.debug(result) 
                            # return False so we are not considered to be the final Executor that displays final results. There will
                            # be many executors that reach the OUPUT_THRESHOLD and stop.
                            return False
                    
                    if (problem.memoize):
                        removePairingNameMsgForParent = MemoizationMessage(
                            message_type = MemoizationMessageType.REMOVEPAIRINGNAME,
                            sender_id = problem.problem_id,
                            problem_or_result_id = problem.problem_id,
                            memoization_label = None,
                            result = None,
                            fan_in_stack = None
                        )

                        ServerlessNetworkingMemoizer.send1(removePairingNameMsgForParent)
                        ack = ServerlessNetworkingMemoizer.rcv1()
                    
                    # Unwind recursion but real executor could simply terminate instead.
                    return False
                else:  # we are last Executor and first executor's result is in previousValue.
                    logger.debug(problem.problem_id + ": FanIn: ID: " + problem.problem_id + ": FanInID: " + faninId + ": " + ": Returned from put: executor isLastFanInExecutor ")
                    logger.debug("subproblemResults[0]: " + str(subproblemResults[0]))
                    logger.debug(problem.problem_id + ": ID: " + str(problem.problem_id) + ": call combine ***************")
                    
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

                    logger.debug(problem.problem_id + ": CALLING COMBINE NOW...")
                    # rhc: start Fan-In task 
                    self.problem.UserProgram.combine(subproblemResults, result, problem.problem_id)

                    logger.debug(problem.problem_id + ": FanIn: ID: " + problem.problem_id + ", FanInId: " + faninId + ", result: " + str(result))

                    #logger.debug(problem.problem_id + ": Thread exiting...")
                    
                    # Note: It's possible that we got a memoized value, e.g., 1 and we added 1+0 to get 1 and we are now
                    # memoizing 1, which we do not need to do. 
                    # Option: Track locally the memoized values we get and put so we don't put duplicates, since get/put is expensive
                    # when memoized storage is remote.

                    if (problem.memoize):
                        memoizedLabel = self.problem.UserProgram.memoizeIDLabeler(parentProblem)
                        # put will memoize a copy of result
                        # rhc: store result with subProblem
                        memoizationResult = FanInSychronizer.put(memoizedLabel,result)
                        #synchronized(FanInSychronizer.getPrintLock()) {
                        logger.debug(problem.problem_id + ": Exector: result.problem_id: " + str(result.problem_id) + " put memoizedLabel: " + str(memoizedLabel) + " result: " + str(result))
                        #}
                    
                    if (problem.memoize):
                        deliverResultMsg = MemoizationMessage(
                            message_type = MemoizationMessageType.DELIVEREDVALUE,
                            sender_id = problem.problem_id,
                            problem_or_result_id = result.problem_id,
                            memoization_label = self.UserProgram.memoizeIDLabeler(parentProblem),
                            result = result,
                            fan_in_stack = None
                        )
                        # deliverResultMsg.message_type = MemoizationMessageType.DELIVEREDVALUE
                        # deliverResultMsg.sender_id = problem.problem_id
                        # deliverResultMsg.problem_or_result_id = result.problem_id
                        # memoizedLabel = self.UserProgram.memoizeIDLabeler(parentProblem)
                        # deliverResultMsg.memoization_label = memoizedLabel
                        # deliverResultMsg.result = result
                        # deliverResultMsg.fan_in_stack = None
                        ServerlessNetworkingMemoizer.send1(deliverResultMsg)
                        
                        ack = ServerlessNetworkingMemoizer.rcv1()
                    
                    if (faninId == self.problem.UserProgram.final_result_id):
                        logger.debug(problem.problem_id + ": Executor: Writing the final value to root: " + str(result))
                        #self.redis_client.set(self.problem.UserProgram.final_result_id, base64.b64encode(cloudpickle.dumps(result)))
                        if self.state.keyword_arguments is None:
                            self.state.keyword_arguments = {}
                        self.state.keyword_arguments["value"] = result 
                        self.synchronize_async(websocket, "synchronize_async", "result", "deposit", self.state)                        

                    if (WukongProblem.USESERVERLESSNETWORKING):
                        if (faninId == self.problem.UserProgram.final_result_id):
                            logger.debug(problem.problem_id + ": Executor: Writing the final value to root: " + str(result))
                            siblingResult = FanInSychronizer.resultMap.put(self.problem.UserProgram.final_result_id, result) 

                        FanInExecutor = parentProblem.become_executor
                        # This executor continues to do Fan-In operations with the new problem result.
                    
                        # end we are second executor
                        # rhc: end Fan-In task 

                        # Instead of doing all of the work for sorting as we unwind the recursion and call merge(),
                        # we let the executors "unwind" the recursion using the explicit FanIn stack.
                
                with debug_lock:
                    left_bracket_index = local_problem_label.rindex("[")
                    logger.debug(problem.problem_id + ": Local problem label BEFORE chopping: \"%s\"" % local_problem_label)
                    local_problem_label = local_problem_label[0:left_bracket_index]
                    logger.debug(problem.problem_id + ": Local problem label AFTER chopping: \"%s\"" % local_problem_label)

            # end while (stack not empty)
        
        # Assuming that we are done with all problems and so done talking to Memoization Controller
        if (problem.memoize):
            removePairingNameMsgForParent = MemoizationMessage(
                message_type = MemoizationMessageType.REMOVEPAIRINGNAME,
                sender_id = problem.problem_id,
                problem_or_result_id = problem.problem_id,
                memoization_label = None,
                result = None,
                fan_in_stack = None
            ) # MemoizationMessage
            # removePairingNameMsgForParent.message_type = MemoizationMessageType.REMOVEPAIRINGNAME
            # removePairingNameMsgForParent.sender_id = problem.problem_id
            # removePairingNameMsgForParent.problem_or_result_id = problem.problem_id
            # removePairingNameMsgForParent.memoization_label = None
            # removePairingNameMsgForParent.result = None
            # removePairingNameMsgForParent.fan_in_stack = None
            ServerlessNetworkingMemoizer.send1(removePairingNameMsgForParent)
            ack = ServerlessNetworkingMemoizer.rcv1() # DivideandConquerFibonacci.ResultType
        
        # Only the final executor, i.e., the last executor to execute the final Fan-in task, makes it to here.
        return True

def decode_base64(original_data, altchars=b'+/'):
    """Decode base64, padding being optional.

    :param data: Base64 data as an ASCII byte string
    :returns: The decoded byte string.

    """
    # data = re.sub(rb'[^a-zA-Z0-9%s]+' % altchars, b'', original_data)  # normalize
    # missing_padding = len(data) % 4
    # logger.debug("Original data length: " + str(len(original_data)) + ", normalized data length: " + str(len(data)) + ", missing padding: " + str(missing_padding))
    # if missing_padding > 0:
    #     data += b'='* (4 - missing_padding)
    #     logger.debug("Length of data after adjustment: " + str(len(data)))
    # else:
    #     logger.debug("Length of (normalized) data is multiple of 4; no adjustment required.")
    original_data += b'==='
    return base64.b64decode(original_data, altchars)