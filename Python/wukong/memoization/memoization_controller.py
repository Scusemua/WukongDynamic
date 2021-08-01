from wukong.channel import BiChannel, UniChannel

from wukong.client_server import ServerlessNetworkingClientServer

from .util import MemoizationMessageType, MemoizationRecord, MemoizationRecordType, PromisedResult

from threading import Thread

import time
import threading
import importlib
import sys

import logging
from logging.handlers import RotatingFileHandler
from logging import handlers
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s')
ch = logging.StreamHandler(sys.stdout)
ch.setFormatter(formatter)
logger.addHandler(ch)

fh = handlers.RotatingFileHandler("divide_and_conquer.log", maxBytes=(1048576*5), backupCount=7)
fh.setFormatter(formatter)
logger.addHandler(fh)

BiChannelForMemoization = BiChannel("MemoizationController")
PairingNames = set()                # Set of strings.
MemoizationRecords = dict()         # Mapping from String -> MemoizationRecord 

ChannelMap = dict()                 # Map from String -> UniChannel
ChannelMapLock = threading.Lock()   # Controls access to the ChannelMap.

print_lock = threading.Lock()

# User-defined ProblemType class.
UserProblemType = None 
UserResultType = None 
UserProgramClass = None

class MemoizationThread(Thread):
    """
    Runs 'within' the MemoizationController.
    """
    def __init__(
        self,
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

    def process_add_pairing_name_message(self, msg):
        """
        Process a message whose message type is `MemoizationMessageType.ADDPAIRINGNAME`.
        """
        if msg.problem_or_result_id in PairingNames:
            logger.error("Internal Error: MemoizationThread: Adding a pairing name that already exists: " + str(msg.problem_or_result_id))
            exit(1)
        
        logger.debug("MemoizationController: add pairing name: " + msg.problem_or_result_id)
        PairingNames.add(msg.problem_or_result_id)

        with print_lock:
            logger.debug("MemoizationController: pairing names after add")
            for name in PairingNames:
                logger.debug("\tMemoizationController: " + name)

        with ChannelMapLock:
            queuePair = ChannelMap[msg.sender_id]
            queuePair.send(NullResult)
    
    def process_remove_pairing_name_message(self, msg):
        """
        Process a message whose message type is `MemoizationMessageType.REMOVEPAIRINGNAME`.
        """
        if msg.problem_or_result_id not in PairingNames:
            logger.error("Internal Error: MemoizationThread: Removing a pairing name that does not exist: " + str(msg.problem_or_result_id))
            exit(1)
        
        PairingNames.remove(msg.problem_or_result_id)

        with print_lock:
            logger.debug("MemoizationController: pairing names after remove")
            for name in PairingNames:
                logger.debug("\tMemoization Controller: " + str(name))
        
        logger.debug("MemoizationController: remove pairing name: " + msg.problem_or_result_id 
            + " pairingNames.size: " + str(len(PairingNames)))

        with ChannelMapLock:
            queuePair = ChannelMap[msg.problem_or_result_id]
            queuePair.send(NullResult)        

    def process_pairing_message(self, msg):
        """
        Process a message whose message type is `MemoizationMessageType.PAIR`.
        """
        if msg.problem_or_result_id not in PairingNames:
            logger.error("MemoizationController: Sender: pairing but receiver does not have pairingName " + str(msg.problem_or_result_id))
            exit(1)
        
        logger.debug("MemoizationController: pair: " + str(msg.problem_or_result_id))

        with ChannelMapLock:
            queuePair = ChannelMap[msg.problem_or_result_id]
            queuePair.send(NullResult)

    def process_deliver_message(self, msg):
        """
        Process a message whose message type is `MemoizationMessageType.DELIVEREDVALUE`.
        """   
        r2 = MemoizationRecords.get(msg.memoization_label, None)
        logger.debug("MemoizationThread: r2: " + str(r2))

        if r2 is None:
            # This is the first delivery of this result - there should be a promise record.
            logger.error("Internal Error: MemoizationThread Delivered: Result Delivered but no previous promise.")
            exit(1)
        
        # Assert:
        if r2.record_type == MemoizationRecordType.DELIVEREDVALUE:
            logger.error("Internal Error: MemoizationThread: sender: " + str(msg.sender_id) + 
                    " problem/result ID " + str(msg.problem_or_result_id) + " memoization_label: " 
                    + str(msg.memoization_label) + " delivered result: " + str(msg.result) 
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

        This may not be the first duplicate promise for msg.memoization_label, i.e., other executors 
        may have been promised the result for this subproblem; so 0, 1, or more executors need to 
        be created to deliver this result All of these executors are in r2.promisedResults.                 
        """
        r2.resultID = msg.problem_or_result_id
        r2.record_type = MemoizationRecordType.DELIVEREDVALUE

        # Note: do not create new lists of promisedResults, as we are 
        # here going to deliver the current promisedResults.
        copy_of_return = msg.result.copy()
        copy_of_return.problem_id = msg.result.problem_id 

        r2.result = copy_of_return
        MemoizationRecords[msg.memoization_label] = r2

        logger.debug("MemoizationThread: sender: " + str(msg.sender_id) + " problem/result ID " 
                        + str(msg.problem_or_result_id) + " memoization_label: " + str(msg.memoization_label)
                        + " delivered result: " + str(msg.result))

        with ChannelMapLock:
            queueDeliver = ChannelMap[msg.problem_or_result_id]
            queueDeliver.send(NullResult)

            # if self.problem is not None:
            #     print("self.problem.__class__: " + str(self.problem.__class__))
            #     print("Type of self.problem.__class__: " + str(type(self.problem.__class__)))

            Deliver(r2.promised_results)             

    def process_promise_message(self, msg):
        """
        Process a message whose message type is `MemoizationMessageType.PROMISEVALUE`.
        """
        r1 = MemoizationRecords.get(msg.memoization_label, None)

        with print_lock:
            logger.debug(">> msg.memoization_label: " + str(msg.memoization_label))
            sorted_keys = sorted(MemoizationRecords.keys())
            logger.debug(">> Keys of MemoizationRecords (%d): %s" % (len(MemoizationRecords), str(sorted_keys)))
            
        if r1 is None:
            promise = MemoizationRecord(
                result_id = msg.problem_or_result_id,
                record_type = MemoizationRecordType.PROMISEDVALUE,
                promised_results = [],
                promised_results_temp = [])
                
            MemoizationRecords[msg.memoization_label] = promise

            with ChannelMapLock:
                queuePromise = ChannelMap[msg.problem_or_result_id]
                logger.debug("MemoizationThread: promise by: " + str(msg.problem_or_result_id))
                queuePromise.send(NullResult)                    
        else:
            # This is not first promise for msg.memoization_label, executor will stop and when 
            # the result is delivered a new one will be created to get result and continue fan-ins.
            if r1.record_type == MemoizationRecordType.PROMISEDVALUE:
                logger.debug("MemoizationThread: duplicate promise by: " + str(msg.problem_or_result_id))

                promise = PromisedResult(
                    problem_or_result_id = msg.problem_or_result_id,
                    fan_in_stack = msg.fan_in_stack,
                    become_executor = msg.become_executor,
                    did_input = msg.did_input,
                    memoization_label_on_restart = msg.memoization_label
                )

                r1.promised_results.append(promise)
                r1.promised_results_temp.append(msg.problem_or_result_id)

                with ChannelMapLock:
                    queuePromise = ChannelMap[msg.problem_or_result_id]
                    logger.debug(">> MemoizationThread: sending StopResult for " + str(msg.problem_or_result_id))
                    queuePromise.send(StopResult)  
            elif r1.record_type == MemoizationRecordType.DELIVEREDVALUE:
                logger.debug(">> MemoizationThread: returning memoized result to problem-or-result-id: " + msg.problem_or_result_id + ", r1.result.problem_id = " + str(r1.result.problem_id)) 

                # The first promised result has been delivered, so grab the delivered result.
                with ChannelMapLock:
                    queuePromise = ChannelMap[msg.problem_or_result_id]

                    copy_of_return = r1.result.copy()
                    copy_of_return.problem_id = r1.result.problem_id

                    logger.debug("MemoizationThread: promised and delivered so deliver to: " + msg.problem_or_result_id)
                    queuePromise.send(copy_of_return) 

    def run(self):
        logger.debug(">> Memoization Thread has started running...")

        while self.active:
            # TODO: This is normally in a try-catch with an interrupted exception (in the Java version).
            try:
                logger.debug(">> Memoization Thread awaiting message...")
                msg = BiChannelForMemoization.rcv2(timeout = 2)
                logger.debug(">> Memoization Thread received message: " + str(msg))
            except:
                time.sleep( 0.0001 )
                continue
            
            if (msg.message_type == MemoizationMessageType.PAIR):
                self.process_pairing_message(msg)
            elif (msg.message_type == MemoizationMessageType.ADDPAIRINGNAME):
                self.process_add_pairing_name_message(msg)
            elif (msg.message_type == MemoizationMessageType.REMOVEPAIRINGNAME):
                self.process_remove_pairing_name_message(msg)
            elif (msg.message_type == MemoizationMessageType.PROMISEVALUE):
                self.process_promise_message(msg)
            elif (msg.message_type == MemoizationMessageType.DELIVEREDVALUE):
                self.process_deliver_message(msg)
            else:
                logger.error("Unknown message type: " + str(msg.message_type))
                exit(1)

myThread = MemoizationThread()      # Memoization Controller runs, like a Lambda

NullResult = None # Serves as an Ack.
StopResult = None # Serves as an Ack.

__initialized = False 

def Deliver(promised_results : list):
    """
    TODO: Optionally send result if it is sufficiently small. 
          Doing so would cut out the whole socket thing to get the result.

    Arguments:
    ----------
        promised_results (list of PromisedResult)
    """
    from wukong.dc_executor import DivideAndConquerExecutor

    # Create a new Executor to handle the promised result.
    logger.debug("MemoizationController: Deliver starting Executors for " + str(len(promised_results)) + " promised results: ")

    for i in range(0, len(promised_results)):
        promised_result = promised_results[i] # `promised_result` was previously called `executor`

        problem = UserProblemType(UserProgram = UserProgramClass())
        problem.problem_id = promised_result.problem_or_result_id
        problem.fan_in_stack = promised_result.fan_in_stack
        problem.become_executor = promised_result.become_executor
        problem.did_input = promised_result.did_input 
        problem.memoization_label_on_restart = promised_result.memoization_label_on_restart

        new_executor = DivideAndConquerExecutor(
            problem = problem,
            problem_type = UserProblemType,
            result_type = UserResultType,
            null_result = NullResult,
            stop_result = StopResult
        )

        logger.debug("Deliver starting Executor for: " + str(problem.problem_id)
					+ " problem.become_executor: " + str(problem.become_executor)
					+ " problem.did_input: " + str(problem.did_input))
        
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
            print_me = "channelMap keySet: "
            for name in ChannelMap:
                print_me = print_me + ". "
            
            logger.debug(print_me)
    
    clientChannel.send(NullResult)
    connections = ServerlessNetworkingClientServer(BiChannelForMemoization, clientChannel)
    return connections

def StartController(
    config = None, 
    user_problem_type = None,
    user_result_type = None,
    user_program_class = None,
    null_result = None, 
    stop_result = None
):
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
    global UserProblemType
    global UserResultType
    global UserProgramClass
    global __initialized

    if (__initialized):
        logger.warn("MemoizationController already initialized.")
        return 
    
    logger.debug(">> Starting Memoization Controller now...")

    # memoization_config = config["memoization"]
    # sources_config = config["sources"]
    
    initial_pairing_name = "[0,1]" #memoization_config["initial-pairing-name"]
    logger.debug("Initial pairing name: \"" + initial_pairing_name + "\"")

    # source_path = sources_config["source-path"]
    # source_module = sources_config["source-module"]

    # spec = importlib.util.spec_from_file_location(source_module, source_path)
    # user_module = importlib.util.module_from_spec(spec)
    # spec.loader.exec_module(user_module)

    # NullResult = user_module.ResultType() # Serves as an Ack.
    # StopResult = user_module.ResultType() # Serves as an Ack.

    UserProblemType = user_problem_type
    UserResultType = user_result_type
    UserProgramClass = user_program_class
    NullResult = null_result
    StopResult = stop_result

    __initialized = True 

    PairingNames.add(initial_pairing_name)
    myThread.start()

    logger.debug(">> Memoization Controller started successfully!")