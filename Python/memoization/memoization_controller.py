from DivideAndConquer.Python.channel import UniChannel
from channel import BiChannel
from memoization_thread import MemoizationThread

from ServerlessNetworkingClientServer import ServerlessNetworkingClientServer

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

myThread = MemoizationThread()      # Memoization Controller runs, like a Lambda

NullResult = None # Serves as an Ack.
StopResult = None # Serves as an Ack.

__initialized = False 

def Pair(pairingName : str) -> ServerlessNetworkingClientServer:
    assert(__initialized)

    if PairingNames[pairingName] == None:
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

def StartController(config):
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
    
    memoization_config = config["memoization"]
    sources_config = config["sources"]
    
    initial_pairing_name = memoization_config["initial-pairing-name"]
    logger.debug("Initial pairing name: \"" + initial_pairing_name + "\"")

    source_path = sources_config["source-path"]
    source_module = sources_config["source-module"]

    spec = importlib.util.spec_from_file_location(source_module, source_path)
    user_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(user_module)

    NullResult = user_module.ResultType() # Serves as an Ack.
    StopResult = user_module.ResultType() # Serves as an Ack.

    __initialized = True 

    PairingNames.add(initial_pairing_name)
    myThread.start()