from wukong.channel import BiChannel, UniChannel

from wukong.client_server import ServerlessNetworkingClientServer

from .util import MemoizationMessageType

from threading import Thread

import time
import threading
import importlib

import logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s')

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
                with ChannelMapLock:
                    queuePair = ChannelMap[msg.problemOrResultID]
                    queuePair.send(NullResult)
            elif (msg.messageType == MemoizationMessageType.ADDPAIRINGNAME):
                with ChannelMapLock:
                    queuePair = ChannelMap[msg.senderID]
                    queuePair.send(NullResult)
            elif (msg.messageType == MemoizationMessageType.REMOVEPAIRINGNAME):
                with ChannelMapLock:
                    queuePair = ChannelMap[msg.problemOrResultID]
                    queuePair.send(NullResult)
            elif (msg.messageType == MemoizationMessageType.PROMISEVALUE):
                # r1 = MemoizationRecords[msg.memoizationLabel]
                # logger.debug("MemoizationThread: r1: " + str(r1))

                with ChannelMapLock:
                    queuePromise = ChannelMap[msg.problemOrResultID]
                    logger.debug(">> Memoization Thread sending NullResult now...")
                    queuePromise.send(NullResult)
            elif (msg.messageType == MemoizationMessageType.DELIVEREDVALUE):
                # r2 = MemoizationRecords[msg.memoizationLabel]
                # logger.debug("MemoizationThread: r2: " + str(r2))

                with ChannelMapLock:
                    queueDeliver = ChannelMap[msg.problemOrResultID]
                    queueDeliver.send(NullResult)
            else:
                pass 

BiChannelForMemoization = BiChannel("MemoizationController")
PairingNames = set()                # Set of strings.
MemoizationRecords = dict()         # Mapping from String -> MemoizationRecord 

ChannelMap = dict()                 # Map from String -> UniChannel
ChannelMapLock = threading.Lock()   # Controls access to the ChannelMap.

print_lock = threading.Lock()

myThread = MemoizationThread()      # Memoization Controller runs, like a Lambda

NullResult = None # Serves as an Ack.
StopResult = None # Serves as an Ack.

__initialized = False 

def Pair(pairingName : str) -> ServerlessNetworkingClientServer:
    assert(__initialized)

    if pairingName not in PairingNames:
        logger.error("MemoizationController: Sender: pairing but receiver does not have pairing name " + pairingName)
        exit(1) 
    
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