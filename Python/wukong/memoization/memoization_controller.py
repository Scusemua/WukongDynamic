from DivideAndConquer.Python.wukong.dc_executor import DivideAndConquerExecutor
from DivideAndConquer.Python.fibonnaci_program import ProblemType
from wukong.channel import BiChannel, UniChannel

from wukong.client_server import ServerlessNetworkingClientServer

from .util import MemoizationMessageType, MemoizationRecord, MemoizationRecordType, PromisedResult

from threading import Thread

import time
import threading
import importlib

import logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s')

BiChannelForMemoization = BiChannel("MemoizationController")
PairingNames = set()                # Set of strings.
MemoizationRecords = dict()         # Mapping from String -> MemoizationRecord 

ChannelMap = dict()                 # Map from String -> UniChannel
ChannelMapLock = threading.Lock()   # Controls access to the ChannelMap.

print_lock = threading.Lock()

class MemoizationThread(Thread):
    def __init__(
        self,
        problem = None,
        group=None, 
        target=None, 
        name=None
    ):
        super(MemoizationThread, self).__init__(group=group, target=target, name=name)
        self.active = True

    def disableThread(self):
        """
        Sets the `active` instance variable to False.
        """
        self.active = False 

    def run(self):
        logger.debug(">> Memoization Thread has started running...")

        while self.active:
            # TODO: This is normally in a try-catch with an interrupted exception (in the Java version).
            msg = None 
            try:
                logger.debug(">> Memoization Thread awaiting message...")
                msg = BiChannelForMemoization.rcv2(timeout = 2)
                logger.debug(">> Memoization Thread received message: " + str(msg))
            except:
                time.sleep( 0.0001 )
                continue 

            if (msg.messageType == MemoizationMessageType.PAIR):
                if msg.problemOrResultID not in PairingNames:
                    logger.error("MemoizationController: Sender: pairing but receiver does not have pairingName " + str(msg.problemOrResultID))
                    exit(1)
                
                logger.debug("MemoizationController: pair: " + str(msg.problemOrResultID))

                with ChannelMapLock:
                    queuePair = ChannelMap[msg.problemOrResultID]
                    queuePair.send(NullResult)
            elif (msg.messageType == MemoizationMessageType.ADDPAIRINGNAME):
                if msg.problemOrResultID in PairingNames:
                    logger.error("Internal Error: MemoizationThread: Adding a pairing name that already exists: " + str(msg.problemOrResultID))
                    exit(1)
                
                logger.debug("MemoizationController: add pairing name: " + msg.problemOrResultID)
                PairingNames.add(msg.problemOrResultID)

                with print_lock:
                    logger.debug("MemoizationController: pairing names after add")
                    for name in PairingNames:
                        logger.debug("\tMemoizationController: " + name)

                with ChannelMapLock:
                    queuePair = ChannelMap[msg.senderID]
                    queuePair.send(NullResult)
            elif (msg.messageType == MemoizationMessageType.REMOVEPAIRINGNAME):
                if msg.problemOrResultID not in PairingNames:
                    logger.error("Internal Error: MemoizationThread: Removing a pairing name that does not exist: " + str(msg.problemOrResultID))
                    exit(1)
                
                PairingNames.remove(msg.problemOrResultID)

                with print_lock:
                    logger.debug("MemoizationController: pairing names after remove")
                    for name in PairingNames:
                        logger.debug("\tMemoization Controller: " + str(name))
                
                logger.debug("MemoizationController: remove pairing name: " + msg.problemOrResultID 
                    + " pairingNames.size: " + str(len(PairingNames)))

                with ChannelMapLock:
                    queuePair = ChannelMap[msg.problemOrResultID]
                    queuePair.send(NullResult)
            elif (msg.messageType == MemoizationMessageType.PROMISEVALUE):
                r1 = MemoizationRecords[msg.memoizationLabel]
                
                if r1 is None:
                    promise = MemoizationRecord(
                        result_id = msg.problemOrResultID,
                        type = MemoizationRecordType.PROMISEDVALUE,
                        promised_results = [],
                        promised_results_temp = [])
                        
                    MemoizationRecords[msg.memoizationLabel] = promise

                    with ChannelMapLock:
                        queuePromise = ChannelMap[msg.problemOrResultID]
                        logger.debug("MemoizationThread: promise by: " + str(msg.problemOrResultID))
                        queuePromise.send(NullResult)                    
                else:
                    # This is not first promise for msg.memoizationLabel, executor will stop and when 
                    # the result is delivered a new one will be created to get result and continue fan-ins.
                    if r1.type == MemoizationRecordType.PROMISEDVALUE:
                        logger.debug("MemoizationThread: duplicate promise by: " + str(msg.problemOrResultID))

                        promise = PromisedResult(
                            problem_result_or_id = msg.problemOrResultID,
                            fan_in_stack = msg.FanInStack,
                            become_executor = msg.becomeExecutor,
                            did_input = msg.didInput
                        )

                        r1.promised_results.add(promise)
                        r1.promised_results_temp.add(msg.problemOrResultID)

                        with ChannelMapLock:
                            queuePromise = ChannelMap[msg.problemOrResultID]
                            logger.debug("MemoizationThread: promise by: " + str(msg.problemOrResultID))
                            queuePromise.send(StopResult)  
                    elif r1.type == MemoizationRecordType.DELIVEREDVALUE:
                        with ChannelMapLock:
                            queuePromise = ChannelMap[msg.problemOrResultID]

                            copy_of_return = r1.result.copy()
                            copy_of_return.problemID = r1.result.problemID

                            logger.debug("MemoizationThread: promised and delivered so deliver to: " + msg.problemOrResultID)
                            queuePromise.send(copy_of_return) 

            elif (msg.messageType == MemoizationMessageType.DELIVEREDVALUE):
                r2 = MemoizationRecords[msg.memoizationLabel]
                logger.debug("MemoizationThread: r2: " + str(r2))

                if r2 is None:
                    # This is the first delivery of this result - there should be a promise record.
                    logger.error("Internal Error: MemoizationThread Delivered: Result Delivered but no previous promise.")
                    exit(1)
                
                # Assert:
                if r2.type == MemoizationRecordType.DELIVEREDVALUE:
                    logger.error("Internal Error: MemoizationThread: sender: " + str(msg.senderID) + 
                            " problem/result ID " + str(msg.problemOrResultID) + " memoizationLabel: " 
                            + str(msg.memoizationLabel) + " delivered result: " + str(msg.result) 
                            + " delivered twice.")
                    exit(1)
                
                """
                Must be a PROMISEDVALUE, so deliver it ...
                
                Q: Do we need to create the problem that will be given to executor(s), i.e.,
                create the problem that is getting the memoized result and give this problem the 
                stack, so passing to executor as usual a ProblemType?

                If so, need to pass the problem as part of the message to get the memoized result?
                But lots of data? but passed result when we delivered result .. No: don't need 
                value since we are not computing result we are grabbing result, so we can 
                trim the problem and push it on the stack.
                
                So don't need data to be passed to get possible memoized result, so pass copy of problem
                without its data? but user need to trim? but we have a trim
                Push problem on stack? then pop it before calling executor to unwind stack?

                This may not be the first duplicate promise for msg.memoizationLabel, i.e., other executors 
                may have been promised the result for this subproblem; so 0, 1, or more executors need to 
                be created to deliver this result All of these executors are in r2.promisedResults.                 
                """
                r2.resultID = msg.problemOrResultID
                r2.type = MemoizationRecordType.DELIVEREDVALUE

                # Note: do not create new lists of promisedResults, as we are 
                # here going to deliver the current promisedResults.
                copy_of_return = msg.result.copy()
                copy_of_return.problemID = msg.result.problemID 

                r2.result = copy_of_return
                MemoizationRecords[msg.memoizationLabel] = r2

                logger.debug("MemoizationThread: sender: " + str(msg.senderID) + " problem/result ID " 
                                + str(msg.problemOrResultID) + " memoizationLabel: " + str(msg.memoizationLabel)
                                + " delivered result: " + str(msg.result))

                with ChannelMapLock:
                    queueDeliver = ChannelMap[msg.problemOrResultID]
                    queueDeliver.send(NullResult)
                    deliver(r2.promisedResults)
            else:
                pass 

myThread = MemoizationThread()      # Memoization Controller runs, like a Lambda

NullResult = None # Serves as an Ack.
StopResult = None # Serves as an Ack.

__initialized = False 

def Deliver(promised_results : list):
    # Create a new Executor to handle the promised result.
    logger.debug("MemoizationController: Deliver starting Executors for promised Results: ")

    for i in range(0, len(promised_results)):
        executor = promised_results[i]

        queueDeliver = ChannelMap[executor.problemOrResultID]

        problem = ProblemType()
        problem.problemID = executor.problemOrResultID
        problem.FanInStack = executor.FanInStack
        problem.becomeExecutor = executor.becomeExecutor
        problem.didInput = executor.didInput 

        new_executor = DivideAndConquerExecutor(
            problem = problem,
            problem_type = threading.current_thread().problem_type,
            result_type = threading.current_thread().result_type,
            null_result = threading.current_thread().null_result,
            stop_result = threading.current_thread().stop_result,
            config_file_path = threading.current_thread().config_file_path
        )

        logger.debug("Deliver starting Executor for: " + problem.problemID
					+ " problem.becomeExecutor: " + problem.becomeExecutor 
					+ " problem.didInput: " + problem.didInput)
        
        new_executor.start()

        logger.debug("Deliver end promised Results: ")
        
        promised_results.clear()

def StopThread():
    assert(myThread is not None)

    myThread.disableThread()

    logger.debug("MemoizationThread disabled.")

def Pair(pairingName : str) -> ServerlessNetworkingClientServer:
    assert(__initialized)

    #if pairingName not in PairingNames:
    #    logger.error("MemoizationController: Sender: pairing but receiver does not have pairing name " + pairingName)
    #    exit(1) 
    
    clientChannel = UniChannel("pairingName")
    with ChannelMapLock:
        ChannelMap[pairingName] = clientChannel
    
        logger.debug("MemoizationController: pair: " + pairingName)

        with print_lock:
            logger.debug("channelMap keySet:")
            for name in ChannelMap:
                logger.debug(name)
    
    clientChannel.send(NullResult)
    connections = ServerlessNetworkingClientServer(BiChannelForMemoization, clientChannel)
    return connections

def StartController(config, null_result = None, stop_result = None):
    """
    Start the Memoization Controller.
    
    Arguments:
    ----------
        null_result (ResultType):
            Instance of the user-defined ResultType object.
        
        stop_result (ResultType):
            Instance of the user-defined ResultType object.

        memoization_config (dict):
            Contains information such as whether or not memoization is enabled and the initial pairing name.
    """
    global NullResult
    global StopResult
    global __initialized

    if (__initialized):
        logger.warn("MemoizationController already initialized.")
        return 

    memoization_config = config["memoization"]
    # sources_config = config["sources"]
    
    initial_pairing_name = memoization_config["initial-pairing-name"]
    logger.debug("Initial pairing name: \"" + initial_pairing_name + "\"")

    # source_path = sources_config["source-path"]
    # source_module = sources_config["source-module"]

    # spec = importlib.util.spec_from_file_location(source_module, source_path)
    # user_module = importlib.util.module_from_spec(spec)
    # spec.loader.exec_module(user_module)

    # NullResult = user_module.ResultType() # Serves as an Ack.
    # StopResult = user_module.ResultType() # Serves as an Ack.

    NullResult = null_result
    StopResult = stop_result

    __initialized = True 

    PairingNames.add(initial_pairing_name)
    myThread.start()

    logger.debug(">> Memoization Controller started successfully!")