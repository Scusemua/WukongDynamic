from re import L
from .barrier import Barrier
from .state import State
#from ClientNew import CallbackHandler
import importlib
from pydoc import locate
from .synchronizer_thread import synchronizerThread
import boto3 
import json
import cloudpickle

from ..wukong.invoker import invoke_lambda 

from .barrier import Barrier
from .bounded_buffer import BoundedBuffer
from .fanin import FanIn

import logging 
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(asctime)s] [%(threadName)s] %(levelname)s: %(message)s')

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)

logger.addHandler(ch)

 # TODO: The serverless function needs to pass its name to synchronize_sync/async so that it can be restarted.

aws_region = 'us-east-1'

# This class handles all interactions with the synchronization objects. A Synchronizer wraps a sycnhronization object.
class Synchronizer(object):

    # valid synchronization objects
    synchronizers = {"barrier", "Barrier", "semaphore", "Semaphore", "bounded_buffer", "BoundedBuffer", "fanin", "FanIn", "CountingSemaphore_Monitor"}

    # Mapping from class to the file in which it is defined.
    file_map = {
        "Barrier": "barrier",
        "BoundedBuffer": "bounded_buffer",
        "FanIn": "fanin",
        "CountingSemaphore_Monitor": "CountingSemaphore_Monitor"
    }
    
    def __init__(self):
        self._name = "Synchronizer"
        self.threadID = 0
        self.lambda_client = boto3.client("lambda", region_name = aws_region)


    def create(self, synchronizer_class_name, synchronizer_object_name, **kwargs):
        # where init call by Client is init(“Barrier”,”b”,[‘n’,2]): and kwargs passed to Barrier.init
        if not synchronizer_class_name in Synchronizer.synchronizers:
            logger.error("Invalid synchronizer class name: '%s'" % synchronizer_class_name)
            raise ValueError("Invalid synchronizer class name: '%s'" % synchronizer_class_name)
        
        if not synchronizer_class_name in Synchronizer.file_map:
            logger.error("Could not find source file for Synchronizer '%s'" % synchronizer_class_name)
            raise ValueError("Could not find source file for Synchronizer '%s'" % synchronizer_class_name)

        #e.g. “Barrier_b”
        self._synchronizer_name = (str(synchronizer_class_name) + '_' + str(synchronizer_object_name))
        
        logger.debug("Attempting to locate class '%s'" % synchronizer_class_name)
        
        src_file = Synchronizer.file_map[synchronizer_class_name]
        #logger.debug("Creating synchronizer with name '%s' by calling locate('%s.%s')"  % (self._synchronizer_name, src_file, synchronizer_class_name))
        logger.debug("Creating synchronizer with name '%s'" % self._synchronizer_name)

        # Get the class object for a synchronizer object, e.g.. Barrier
        module = importlib.import_module("wukongdnc.server." + src_file)
        self._synchClass = getattr(module, synchronizer_class_name)

        if (self._synchClass is None):
            raise ValueError("Failed to locate and create synchronizer of type %s" % synchronizer_class_name)

        # Create the synchronization object
        #logger.debug("got MyClass")
        self._synchronizer = self._synchClass()
        if self._synchronizer == None:
            logger.error("Failed to locate and create synchronizer of type %s" % synchronizer_class_name)
            return -1
        
        #e.g. "b"
        self._synchronizer_object_name = synchronizer_object_name
        logger.debug("self._sycnhronizer_object_name: " + self._synchronizer_object_name)

        # init the synchronzation object
        logger.debug("Calling _synchronizer init")
        self._synchronizer.init(**kwargs)  #2
        # where Barrier init is: init(**kwargs): if len(kwargs) not == 1
	    # logger.debug(“Error: Barrier init has too many argos”) self._n = kwargs[‘n’]

        logger.debug ("Called _synchronizer init")
        return 0
        
    # For try-ops this method calls the try-op method defined by the user
    def trySynchronize(self, method_name, state, **kwargs):
        logger.debug("starting trySynchronize, method_name: " + str(method_name) + ", ID is: " + state.function_instance_ID)
        
        try:
            _synchronizer_method = getattr(self._synchClass,method_name)
        except Exception as x:
            logger.error("Caught Error >>> %s" % x)
            raise ValueError("Synchronizer of type %s does not have method called %s. Cannot complete trySynchronize() call." % (self._synchClass, method_name))

        myPythonThreadName = "Try_callerThread" + state.function_instance_ID #str(ID_arg)
        restart, returnValue = self.doMethodCall(2, myPythonThreadName, self._synchronizer, _synchronizer_method, **kwargs)
                
        logger.debug("trySynchronize " + " restart " + str(restart))
        logger.debug("trySynchronize " + " returnValue " + str(returnValue))
        
        return returnValue

    # This method makes the actual method call to the called method of the synchronization object and when that call
    # returns it restarts the serverless function, if necessary.
    def synchronize(self, method_name, state, **kwargs):
        logger.debug("starting synchronize, method_name: " + str(method_name) + ", ID is: " + state.function_instance_ID)
        
        try:
            _synchronizer_method = getattr(self._synchClass,method_name)
        except Exception as ex:
            logger.error("Failed to find method '%s' on object '%s'." % (method_name, self._synchClass))
            raise ex
        
        myPythonThreadName = "NotTrycallerThread"+str(self.threadID)
        restart, returnValue = self.doMethodCall(1, myPythonThreadName, self._synchronizer, _synchronizer_method, **kwargs) 
        
        logger.debug("synchronize restart " + str(restart))
        logger.debug("synchronize returnValue " + str(returnValue))
        logger.debug("synchronize successfully called synchronize method and acquire exited. ")

        # if the method returns restart True, restart the serverless function and pass it its saved state.
        if restart:
            state.restart = True 
            logger.info("Restarting Lambda function %s." % state.function_name)
            payload = {"state": state}
            invoke_lambda(payload = payload, is_first_invocation = False, function_name = state.function_name)
        
        return returnValue

    # invokes a method on the synchronization object
    def doMethodCall(self, PythonThreadID, myName, synchronizer, synchronizer_method, **kwargs):
        """
        Call a method.

        Arguments:
        ----------
        TODO: Make sure these descriptions are accurate...

            PythonThreadID:
                The ID of the Python thread associated with this operation/call.
            
            myName:
                The name of the synchronizer object.
            
            synchronizer:
                The synchronizer object.
            
            synchronizer_method:
                The method of the synchronizer object that we're calling.
        """
        logger.debug ("starting caller thread to make the call")
        callerThread = synchronizerThread(PythonThreadID, myName,  synchronizer, synchronizer_method, **kwargs)
        callerThread.start()
        callerThread.join()
        returnValue = callerThread.getReturnValue()
        restart = callerThread.getRestart()
        logger.debug("doMethodCall: returnValue: " + str(returnValue))
        # For 2-way: result = _synchronizer_method (self._synchronizer)(kwargs)
        # where wait_b in Barrier is wait_b(self, **kwargs):
        logger.debug("calling acquire exited.")
        synchronizer._exited.acquire()
        logger.debug("called acquire exited. returning ...")
        return restart, returnValue

