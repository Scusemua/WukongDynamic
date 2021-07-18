from channel import BiChannel
from memoization_thread import MemoizationThread

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

myThread = MemoizationThread()      # Memoization Controller runs, like a Lambda

NullResult = None # Serves as an Ack.
StopResult = None # Serves as an Ack.

def StartController(config):
    """
    Start the Memoization Controller.
    
    Arguments:
    ----------

        memoization_config (dict):
            Contains information such as whether or not memoization is enabled and the initial pairing name.
    """
    memoization_config = config["memoization"]
    sources_config = config["sources"]

    assert(memoization_config["enabled"] == True)
    
    initial_pairing_name = memoization_config["initial-pairing-name"]
    logger.debug("Initial pairing name: \"" + initial_pairing_name + "\"")

    source_path = sources_config["source-path"]
    source_module = sources_config["source-module"]

    spec = importlib.util.spec_from_file_location(source_module, source_path)
    user_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(user_module)

    NullResult = user_module.ResultType() # Serves as an Ack.
    StopResult = user_module.ResultType() # Serves as an Ack.

    PairingNames.add(initial_pairing_name)
    myThread.start()