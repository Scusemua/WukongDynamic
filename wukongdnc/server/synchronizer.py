#from re import L
#from .barrier import Barrier
from .state import State
#from ClientNew import CallbackHandler
import importlib
#from pydoc import locate
#from .synchronizer_thread import synchronizerThread
#import boto3 
#import json
import cloudpickle
import os

from ..wukong.invoker import invoke_lambda 

#from ..dag.DAG_executor_constants import exit_program_on_exception
import wukongdnc.dag.DAG_executor_constants

#from .synchronizerThreadSelect import SynchronizerThreadSelect
#from .bounded_buffer import BoundedBuffer
#from .bounded_buffer_select import BoundedBuffer_Select
#from .CountingSemaphore_Monitor import CountingSemaphore_Monitor
#from .fanin import FanIn
from .result_buffer import ResultBuffer

from .util import isTry_and_getMethodName, isSelect #, make_json_serializable, decode_and_deserialize

import logging 
logger = logging.getLogger(__name__)
"""
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(asctime)s] [%(module)s] [%(processName)s] [%(threadName)s]: %(message)s')
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)
logger.addHandler(ch)
"""

aws_region = 'us-east-1'

# This class handles all interactions with the synchronization objects. A Synchronizer wraps a sycnhronization object.
class Synchronizer(object):

    # valid synchronization objects
    synchronizers = {"barrier", "Barrier", "semaphore", "Semaphore", "bounded_buffer", "BoundedBuffer", 
        "fanin", "FanIn", "CountingSemaphore_Monitor", "CountingSemaphore_Monitor_Select", 
        "BoundedBuffer_Select", "DAG_executor_FanIn", "DAG_executor_FanInNB",
        "DAG_executor_FanIn_Select", "DAG_executor_FanInNB_Select",
        "DAG_infoBuffer_Monitor", "DAG_infoBuffer_Monitor_for_Lambdas"}

    # Mapping from class to the file in which it is defined.
    file_map = {
        "Barrier": "barrier",
        "BoundedBuffer": "bounded_buffer",
        "FanIn": "fanin",
        "CountingSemaphore_Monitor": "CountingSemaphore_Monitor",
        "BoundedBuffer_Select": "bounded_buffer_select",
        "CountingSemaphore_Monitor_Select": "CountingSemaphore_Monitor_Select",
        "DAG_executor_FanIn": "DAG_executor_FanIn",
        "DAG_executor_FanInNB": "DAG_executor_FanInNB",
        "DAG_executor_FanIn_Select": "DAG_executor_FanIn_select",
        "DAG_executor_FanInNB_Select": "DAG_executor_FanInNB_select",
        "DAG_infoBuffer_Monitor": "DAG_infoBuffer_Monitor",
        "DAG_infoBuffer_Monitor_for_Lambdas": "DAG_infoBuffer_Monitor_for_Lambdas"
    }
    
    def __init__(self):
        pass
        # not used
        #self._name = "Synchronizer"
        # Not used since we unrolled a lot of methods and dn;t use the synchronizationThread or synchronizationThreadSelect
        #self.threadID = 0
        # we invoke but this is not used
        #self.lambda_client = boto3.client("lambda", region_name = aws_region)
        # Self Vars used:
        # self._synchClass
        # self._synchronizer
        # self._synchronizer_name
        # self._synchronizer_object_name

    def lock_synchronizer(self):
    
        logger.trace("synchronizer: locking synchronizer")
        
        try:
            synchronizer_method = getattr(self._synchClass,"lock")
        except Exception:
            logger.exception("[Error]: synchronizer: lock_synchronizer: Failed to find method 'lock' on object of type '%s'." % (self._synchClass))
            if wukongdnc.dag.DAG_executor_constants.exit_program_on_exception:
                logging.shutdown()
                os._exit(0)
            
        synchronizer_method(self._synchronizer)
 
    def unlock_synchronizer(self):
        logger.trace("synchronizer: unlocking synchronizer") 
        try:
            synchronizer_method = getattr(self._synchClass,"unlock")
        except Exception:
            logger.exception("[Error]: synchronizer: unlock_synchronizer: Failed to find method 'unlock' on object of type '%s'." % (self._synchClass))
            if wukongdnc.dag.DAG_executor_constants.exit_program_on_exception:
                logging.shutdown()
                os._exit(0)
            
        synchronizer_method(self._synchronizer)

    def create(self, synchronizer_class_name, synchronizer_object_name, **kwargs):
        # where init call by Client is init(“Barrier”,”b”,[‘n’,2]): and kwargs passed to Barrier.init
        self._synchronizer_class_name = synchronizer_class_name

        if not synchronizer_class_name in Synchronizer.synchronizers:
            logger.error("[Error]: synchronizer.py: create: Invalid synchronizer class name: '%s'" % synchronizer_class_name)
            logger.error("[Error]: If you developed a new synch object be sure to register it in Synchronizer.synchronizers.")
            if wukongdnc.dag.DAG_executor_constants.exit_program_on_exception:
                logging.shutdown()
                os._exit(0)
        
        if not synchronizer_class_name in Synchronizer.file_map:
            logger.error("[Error]: synchronizer.py: create: Could not find source file for Synchronizer '%s'" % synchronizer_class_name)
            logger.error("[Error]: If you developed a new synch object be sure to register it in Synchronizer.file_map.")
            if wukongdnc.dag.DAG_executor_constants.exit_program_on_exception:
                logging.shutdown()
                os._exit(0)

        #e.g. “Barrier_b”
        self._synchronizer_name = (str(synchronizer_class_name) + '_' + str(synchronizer_object_name))
        
        logger.info("create: Attempting to locate class '%s'" % synchronizer_class_name)
        
        src_file = Synchronizer.file_map[synchronizer_class_name]
        #logger.info("Creating synchronizer with name '%s' by calling locate('%s.%s')"  % (self._synchronizer_name, src_file, synchronizer_class_name))
        #logger.info("create: Creating synchronizer with name '%s'" % self._synchronizer_name)
        # Get the class object for a synchronizer object, e.g.. Barrier
        module = importlib.import_module("wukongdnc.server." + src_file)
        #module = importlib.import_module(src_file)
        self._synchClass = getattr(module, synchronizer_class_name)
        if (self._synchClass is None):
            logger.error("[Error]: synchronizer: Failed to locate and create synchronizer of type %s" % synchronizer_class_name)
            if wukongdnc.dag.DAG_executor_constants.exit_program_on_exception:
                logging.shutdown()
                os._exit(0)

        # Create the synchronization object
        #logger.trace("got MyClass")
        self._synchronizer = self._synchClass(self._synchronizer_name)
        if self._synchronizer is None:
            logger.error("[Error]: synchronizer: create: Failed to locate and create synchronizer of type %s" % synchronizer_class_name)
            if wukongdnc.dag.DAG_executor_constants.exit_program_on_exception:
                logging.shutdown()
                os._exit(0)
        
        #e.g. "b"
        self._synchronizer_object_name = synchronizer_object_name
        logger.info("synchronizer: create: self._sycnhronizer_object_name: " + self._synchronizer_object_name)

        # init the synchronzation object
        logger.info("synchronizer: create: Calling _synchronizer init")
        self._synchronizer.init(**kwargs)  #2
        # where Barrier init is: init(**kwargs): if len(kwargs) not == 1
	    # logger.trace(“Error: Barrier init has too many argos”) self._n = kwargs[‘n’]

        logger.info ("create: Called _synchronizer init")
        return 0

    # def synchronize_sync(self, tcp_server, obj_name, method_name, type_arg, state, synchronizer_name):        
    def synchronize_sync(self, tcp_server, obj_name, method_name, state, synchronizer_name, tcp_handler):
    
        logger.trace("synchronizer: synchronize_sync: called")
        logger.trace("State: " + str(state))
        #logger.trace("State: keyword_arguments: fanin_task_name:" + str(state.keyword_arguments['fanin_task_name']))

        base_name, isTryMethod = isTry_and_getMethodName(method_name)
        is_select = isSelect(self._synchronizer_class_name) # is_select = isSelect(type_arg)
    
        logger.trace("synchronizer: synchronize_sync: method_name: " + method_name + ", base_name: " + base_name + ", isTryMethod: " + str(isTryMethod))
        logger.trace(" self._synchronizer_class_name: : " + self._synchronizer_class_name + ", is_select: " + str(is_select))
        # logger.trace("synchronizer: synchronize_sync: type_arg: " + type_arg + ", is_select: " + str(is_select))
        #logger.trace("base_name: " + base_name)
        #logger.trace("isTryMethod: " + str(isTryMethod))
        
        # COMMENTED OUT:
        # The TCP server does not have a '_synchClass' variable, so that causes an error to be thrown.
        # We aren't even using the `_synchronizer_method` variable anywhere though, so I've just
        # commented this out. I don't think we need it?
        
        # try:
        #     _synchronizer_method = getattr(self._synchClass, method_name)
        # except Exception as x:
        #     logger.trace("Caught Error >>> %s" % x)

        if isTryMethod: 
        
            # selects are not monitors, i.e., no enter_monitor, so lock here
            if is_select:
                self.lock_synchronizer()
                
            # check if synchronize op will block, if yes tell client to terminate then call op 
            if is_select:
                try_return_value = self.trySynchronizeSelect(method_name, state, **state.keyword_arguments)
            else:
                try_return_value = self.trySynchronize(method_name, state, **state.keyword_arguments)

            logger.trace("synchronizer: synchronize_sync: Value of try_return_value (Block) for fan-in ID %s: %s" % (obj_name, str(try_return_value)))
            
            if try_return_value == True:   # synchronize op will execute wait so tell client to terminate
                state.blocking = True 
                state.return_value = None 
                
                # Still hold the lock;  we could move this to after the unlock so the select would be released sooner.
                # This allows client to terminate sooner than if we did this after releasing lock, but it delays
                # other callers from executing select since lock is held.
                # Could start a thread to do this asynchronously.
                tcp_handler.send_serialized_object(cloudpickle.dumps(state))
                
                # execute synchronize op, but don't send result to client
                if is_select:
                    # create result_buffer, create execute() reference, call execute(), result_buffer.withdraw(), 
                    # with executes's result and restart do restart, (by definition of synchronous try-op that Blocked)
                    # and ignore return value here.
                    wait_for_return = not try_return_value
                    return_value = self.synchronizeSelect(base_name, state, wait_for_return, **state.keyword_arguments)
                else:
                    return_value = self.synchronize(base_name, state, **state.keyword_arguments)
                    
                # release lock here and in the else so we can release the lock in the lse befor the tcp send to the client
                # if is_select:
                #     self.unlock_synchronizer()

                logger.trace("synchronizer: synchronize_sync: Value of return_value (not to be sent) for fan-in ID %s and method %s: %s" % (obj_name, method_name, str(return_value)))
            else:
                # execute synchronize op and send result to client
                if is_select:
                    logger.trace("Calling synchronizeSelect()")
                    # create result_buffer, create execute() reference, call execute(), result_buffer.withdraw(), 
                    # return execute's result, with no restart (by definition of synchronous try-op that did not Block)
                    # and send result to client below.
                    wait_for_return = not try_return_value
                    return_value = self.synchronizeSelect(base_name, state, wait_for_return, **state.keyword_arguments)
                else:
                    logger.trace("synchronizer: synchronize_sync: Calling synchronize()")
                    return_value = self.synchronize(base_name, state, **state.keyword_arguments)
                state.return_value = return_value
                state.blocking = False 
                
                # release lock before tcp sending the result to client of synchronous call
                # if is_select:
                #    self.unlock_synchronizer()
                    
                logger.trace("synchronizerXXX: synchronize_sync: %s sending return_value %s back for method %s." % (synchronizer_name, str(return_value), method_name))
                logger.trace("synchronizerYYY: synchronize_sync: %s sending state %s back for method %s." % (synchronizer_name, str(state), method_name))

                # send tuple to be consistent, and False to be consistent, i.e., get result if False.
                # This is after releasng the lock
                tcp_handler.send_serialized_object(cloudpickle.dumps(state))                         
        else:  
            # not a "try" so do synchronization op and send result to waiting client

            if is_select:
                self.lock_synchronizer()
            
            if is_select:
                # create result_buffer, create execute() reference, call execute(), result_buffer.withdraw(), 
                # return excute's result, with no restart (by definition of synchronous non-try-op)
                # (Send result to client below.)
                wait_for_return = True
                # rhc: DES
                return_value = self.synchronizeSelect(base_name, state, wait_for_return, **state.keyword_arguments)
            else:
                return_value = self.synchronize(base_name, state, **state.keyword_arguments)
                
            state.return_value = return_value
            state.blocking = False 
            
            # release lock before TCP sending the result to client.
            # if is_select:
            #    self.unlock_synchronizer()  

            logger.info("synchronizerXXX: synchronize_sync: %s sending return_value %s back for method %s." % (synchronizer_name, str(return_value), method_name))
            logger.info("synchronizerYYY: synchronize_sync: %s sending state %s back for method %s." % (synchronizer_name, str(state), method_name))
           
            tcp_handler.send_serialized_object(cloudpickle.dumps(state))
            
        return 0
        
    def synchronize_async(self, obj_name, method_name, state, synchronizer_name):
        """
        Asynchronous synchronization.
        """
        logger.trace("synchronizer: synchronize_async called. method_name = '" + method_name + "'")
        logger.trace("State: " + str(state))

        # is_select = isSelect(type_arg)
        is_select = isSelect(self._synchronizer_class_name)
        logger.trace("self._synchronizer_class_name: " + self._synchronizer_class_name + ", is_select: " + str(is_select))
        
        if is_select:
            self.lock_synchronizer()
     
        if is_select:
            wait_for_return = True #False
            sync_ret_val = self.synchronizeSelect(method_name, state, wait_for_return, **state.keyword_arguments)
        else:
            sync_ret_val = self.synchronize(method_name, state, **state.keyword_arguments)   

        # if is_select:
        #    self.unlock_synchronizer()           
        
        logger.trace("synchronizer: synchronize_async: Synchronize/synchronizeSelect returned: %s" % str(sync_ret_val))
        
        return 0

    # For try-ops this method calls the try-op method defined by the user (for non-select synchronizers). 
    # Called by synchronize_synch in tcp_server
    def trySynchronize(self, method_name, state, **kwargs):
        logger.trace("synchronizer: trySynchronize: starting trySynchronize, method_name: " + method_name + ", ID is: " + state.function_instance_ID)
        
        try:
            synchronizer_method = getattr(self._synchClass,method_name)
        except Exception:            
            logger.exception("synchronizer: trySynchronize: synchronizer of type %s does not have method called %s. Cannot complete trySynchronize() call." % (self._synchClass, method_name))
            if wukongdnc.dag.DAG_executor_constants.exit_program_on_exception:
                logging.shutdown()
                os._exit(0)

        #   
        """ replace this call 
        myPythonThreadName = "Try_callerThread" + state.function_instance_ID #str(ID_arg)
        restart, returnValue = self.doMethodCall(2, myPythonThreadName, self._synchronizer, synchronizer_method, **kwargs)
        """
        """ with this unrolled """
        # try-op methods only returning return_value not restart since restart does not apply.
        # (Since we make call directly, we can return one or two values unlike relying on 
        # synchronizer thread which always expected two return values - return_value and restart
        returnValue = synchronizer_method(self._synchronizer, **kwargs) 
        
        #logger.trace("trySynchronize (ignoring)" + " restart " + str(restart))
        logger.trace("trySynchronize: " + " returnValue " + str(returnValue))
        
        return returnValue
        
    # For try-ops this method calls the try-op method defined by the user (for synchronizers that use select)
    # Same as trySynchronize() but we are keeping the non-sect and select code separate.
    # Called by synchronize_synch in tcp_server
    def trySynchronizeSelect(self, method_name, state, **kwargs):
        logger.trace("trySynchronizeSelect: method_name: " + method_name + ", ID is: " + str(state.function_instance_ID))
        
        try:
            synchronizer_method = getattr(self._synchClass, method_name)
        except Exception:
            logger.exception("[Error]: synchronizer: Synchronizer of type %s does not have method called %s. Cannot complete trySynchronize() call." % (self._synchClass, method_name))
            if wukongdnc.dag.DAG_executor_constants.exit_program_on_exception:
                logging.shutdown()
                os._exit(0)

        """ replace this call 
        myPythonThreadName = "Try_callerThread" + state.function_instance_ID #str(ID_arg)
        returnValue, restart  = self.doMethodCallSelectTry(2, myPythonThreadName, self._synchronizer, synchronizer_method, **kwargs)
        """
        
        """ with this call """
        returnValue = synchronizer_method(self._synchronizer, **kwargs)
               
        #logger.trace("trySynchronizeSelect (ignoring)" + " restart " + str(restart))
        logger.trace("trySynchronizeSelect: " + " returnValue " + str(returnValue))
        
        return returnValue
        
    # This method makes the actual method call to the called method of the synchronization object and when that call
    # returns it restarts the serverless function, if necessary.
    # Called by synchronize_synch in tcp_server
    def synchronize(self, method_name, state, **kwargs):
        logger.trace("synchronize: method_name: " + str(method_name) + ", ID is: " + str(state.function_instance_ID))
        
        try:
            synchronizer_method = getattr(self._synchClass, method_name)

            # One line example. You can combine:
            # synchronizer_method = getattr(self._synchClass, method_name)
            # returnValue, restart = synchronizer_method(self._synchronizer, **kwargs) 
            #
            # into a single line as follows: 
            # 
            # returnValue, restart = getattr(self._synchClass, method_name)(self._synchronizer, **kwargs) 
            #
            # getattr returns a function object. You invoke the function object's __call__ method with the standard
            # function-calling syntax from every programming language (args). In your case, you're passing 
            # self._synchronizer and **kwargs as arguments.
        except Exception:
            logger.exception("synchronize: Failed to find method '%s' on object '%s'." % (method_name, self._synchClass))
            if wukongdnc.dag.DAG_executor_constants.exit_program_on_exception:
                logging.shutdown()
                os._exit(0)
        
        """ replace this call 
        myPythonThreadName = "NotTrycallerThread"+str(self.threadID)
        returnValue, restart  = self.doMethodCall(1, myPythonThreadName, self._synchronizer, synchronizer_method, **kwargs) 
        """
        
        """ with this unrolled """
        returnValue, restart = synchronizer_method(self._synchronizer, **kwargs) 
         
        logger.trace("synchronize: method_name: " + str(method_name) + ", restart " + str(restart))
        logger.trace("synchronize: method_name: " + str(method_name) + ", returnValue " + str(returnValue))
        logger.trace("synchronize: method_name: " + str(method_name) + ", successfully called synchronize method. ")

        # if the method returns restart True, restart the serverless function and pass it its saved state.
        if restart:
            state.restart = True 
            state.return_value = returnValue
            state.blocking = False            
            logger.trace("synchronize: Restarting Lambda function %s." % state.function_name)
            logger.trace("State: " + str(state))
            payload = {"state": state}
            invoke_lambda(payload = payload, is_first_invocation = False, function_name = "ComposerServerlessSync")
        
        return returnValue

    # This version calls self.doMethodCallSelectExecute() which is used for synchronization objects that use selectiveWaits.
    # Note, we still pass the synchronizer and method which are needed inside execute().
    # Called by synchronize_synch in tcp_server
    def synchronizeSelect(self, method_name: str, state: State, wait_for_result: bool, **kwargs):
        logger.trace("State: " + str(state))
        logger.trace("synchronizeSelect: method_name: " + str(method_name))
        logger.trace("synchronizeSelect: ID is: " + str(state.function_instance_ID))

        
        try:
            synchronizer_method = getattr(self._synchClass, method_name)
        except Exception:
            logger.exception("synchronizeSelect: Failed to find method '%s' on object '%s'." % (method_name, self._synchClass))
            if wukongdnc.dag.DAG_executor_constants.exit_program_on_exception:
                logging.shutdown()
                os._exit(0)
        
        """ Replacing these 4 lines, which call doMethodCallSelectExecute
        # Note: passing actual Python method reference _synchronizer_method, as well as the method_name
        myPythonThreadName = "NotTrycallerThread"+str(self.threadID)
        # create result_buffer, create execute() reference, call execute(), result_buffer.withdraw(), get executes's result and restart
        returnValue, restart  = self.doMethodCallSelectExecute(1, myPythonThreadName, method_name, self._synchronizer, synchronizer_method, self._synchClass, **kwargs) 
        """
        
        """ New call with doMethodCallSelectExecute unrolled  """
        try:
            execute = getattr(self._synchClass,"execute")
        except Exception:
            logger.exception("synchronizeSelect: Failed to find method 'execute' on object '%s'." % (self._synchClass))
            if wukongdnc.dag.DAG_executor_constants.exit_program_on_exception:
                logging.shutdown()
                os._exit(0)
            
        result_buffer = ResultBuffer(1, "resultBuffer")
            
        # Calling execute() which will make method call so need to pass class and method so call can be made.
        # This is different from synchronization objects that are regulr monitors as we call their methods
        # e.g., "deposit" here.
        # Easiest is to pass args, also x = num(1) print (type(x).__name__) Also def func(self): print(__class__) but
        # execute might be in superclass MonitorSelect of BoundedBufferSelect  
        
        #returnValueIgnored = execute(self._synchronizer, method_name, self._synchronizer, synchronizer_method, result_buffer, **kwargs)
        _return_value_ignored = execute(self._synchronizer, method_name, self._synchronizer, synchronizer_method, result_buffer, None, wait_for_result, **kwargs)
        
        # unlock the synchronizer before bocking on withdaw(). Method withdraw() may not unblock until after a call to
        # a synchronize_synch or synchronize_asynch but these methods try to lock the synchronizer so we need to 
        # release our synchronzer lock here. (Blocking on withdaw means the call to a method, e.g., method P on 
        # a CountingSeaphore_Monitor, was not accepted by execute so the call to P will only be accepted/chosen by
        # a later call to execute for method V. At that time execute will depost the result of the P operation in the 
        # result buffer to be withdrawn here (by the handler thread of the TCP server for the earlier P operation).
        self.unlock_synchronizer()
        
        result = result_buffer.withdraw()
        returnValue = result[0]
        restart = result[1] 
        
        logger.trace("synchronizeSelect: restart " + str(restart))
        logger.info("synchronizeSelect: returnValue " + str(returnValue))

        # if the method returns restart True, restart the serverless function and pass it its saved state.
        if restart:
            state.restart = True 
            state.return_value = returnValue
            state.blocking = False            
            logger.trace("Restarting Lambda function %s." % state.function_name)
            logger.trace("State: " + str(state))
            payload = {"state": state}
            ###### state.function_name has not changed (will be "FuncA", "FuncB" or "ComposerServerlessSync" )
            invoke_lambda(payload = payload, is_first_invocation = False, function_name = "ComposerServerlessSync")
        
        return returnValue

    #####################################################
    # NO LONGER USING any of the methods below this point. The methods below were unrolled into the methods
    # above that called them. No longer using synchronizerThread and synchronizerThreadSelect either since
    # they were used only in the methods below.
    #####################################################
    
    # NO LONGER USING 
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
        
        """ synchronizer_thread code to be removed
        def __init__(self, PythonThreadID, PythonThreadName, synchronizer, synchronizer_method, **kwargs):
        # Call the Thread class's init function
        super(synchronizerThread,self).__init__(name=PythonThreadName)
        self._threadID = PythonThreadID
        self._synchronizer = synchronizer
        self._synchronizer_method = synchronizer_method
        self._restart = True
        self._returnValue = 0
        self._kwargs = kwargs
        
        self._returnValue = self._synchronizer_method(self._synchronizer,**self._kwargs)
        """
        
        """ New call with synchronizer_thread unrolled + restart returned too """
        returnValue, restart = synchronizer_method(synchronizer, **kwargs)
        
        
        """ removing this synchronizerThread 
        logger.trace ("starting caller thread to make the call")
        callerThread = synchronizerThread(PythonThreadID, myName,  synchronizer, synchronizer_method, **kwargs)
        callerThread.start()
        callerThread.join()
        """

        """ replaces these five lines too      
        returnValue = callerThread.getReturnValue()
        restart = callerThread.getRestart()
        #logger.trace("calling acquire exited. returning ...")  
        #synchronizer._exited.acquire()
        #logger.trace("called acquire exited. returning ...")      
        """
        
        logger.trace("doMethodCall: returnValue: " + str(returnValue))

        return returnValue, restart

    # NO LONGER USING 
    # Same as doMethodCall since we call the try-op method directly, i.e, without using execute.
    def doMethodCallSelectTry(self, PythonThreadID, myName, synchronizer, synchronizer_method, **kwargs):
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
        
        """ synchronizer_thread code to be removed
        def __init__(self, PythonThreadID, PythonThreadName, synchronizer, synchronizer_method, **kwargs):
        # Call the Thread class's init function
        super(synchronizerThread,self).__init__(name=PythonThreadName)
        self._threadID = PythonThreadID
        self._synchronizer = synchronizer
        self._synchronizer_method = synchronizer_method
        self._restart = True
        self._returnValue = 0
        self._kwargs = kwargs
        
        self._returnValue = self._synchronizer_method(self._synchronizer,**self._kwargs)
        """
        
        """ New call with synchronizer_thread unrolled """
        returnValue, restart = synchronizer_method(synchronizer, **kwargs)
        
        """ removing this sycnhronizer_thread
        logger.trace ("starting caller thread to make the call")
        callerThread = synchronizerThread(PythonThreadID, myName,  synchronizer, synchronizer_method, **kwargs)
        callerThread.start()
        callerThread.join()
        """
         
        """ replaces these five lines too
        returnValue = callerThread.getReturnValue()
        restart = callerThread.getRestart()
        
        #logger.trace("doMethodCallSelectTry: calling acquire exited.")
        #synchronizer._exited.acquire()
        #logger.trace("doMethodCallSelectTry: called acquire exited. returning ...")
        """
        
        logger.trace("doMethodCallSelectTry: returnValue: " + str(returnValue) + ", restart: " + str(restart))     
        
        return returnValue, restart

    # NO LONGER USING 
    # invokes method execute() which will make the actual call to synchronizer_method.
    def doMethodCallSelectExecute(self, PythonThreadID, myPythonThreadName, entry_name, synchronizer, synchronizer_method, synchClass, **kwargs):
        """
        Call a method.
        Arguments:
        ----------
        TODO: Make sure these descriptions are accurate...
            PythonThreadID:
                The ID of the Python thread associated with this operation/call.
            
            myName:
                PythonThreadName
            
            synchronizer:
                The synchronizer object.
            
            synchronizer_method:
                The Python method reference of the synchronizer object that we're calling.
        """
        
        result_buffer = ResultBuffer(1, "resultBuffer")
        
        """ modify this comment """
        # Start a synchronizerThreadSelect which will call execute() which will call synchronizer.synchronizer_method
        # using the synchronizer and synchronizer_method args passed through synchronizerThreadSelect's execute()
        # call, e.g., "BoundedBufferSelect" and "deposit". The method call will return a result, e.g., the value withdrawn,
        # to the synchronizerThreadSelect, which will do result_buffer.deposit() and then return a return_value. This return
        # value is not the result of the method call, which was deposited in the result_buffer. It can be a value, e.g., 
        # related to a delay alternative or something, i.e., used to issue a calll to ddelay after a timeout. Note: if
        # synchronizerThreadSelect T finds guard G is false, it will not do the method call; instead, it will just return,
        # i.e., it is not blocked on guard. Some other synchronizerThreadSelect will do its method call, reevaluate the 
        # guards and if G is true do T's method call (saved as an arrival) and deposit the method's return value into
        # result_buffer, saved with the arrival, unblocking the server thread (that did domethodcall) waiting on its
        # call to result_buffer.withsdraw.
        
        """ SynchronizerThreadSelect code to be removed
        # Call the Thread class's init function)
        super(SynchronizerThreadSelect,self).__init__(name=PythonThreadName)
        self._threadID = PythonThreadID
        self._synchronizer = synchronizer
        self._synchronizer_method = synchronizer_method
        self._synchClass = synchClass
        self._restart = True
        self._returnValue = 0
        self._kwargs = kwargs
        self._result_buffer = result_buffer
        self._entry_name = entry_name
        
        try:
            _execute = getattr(self._synchClass,"execute")
        except Exception:
            logger.exception("[Error]: synchonizer: Failed to find method 'execute' on object '%s'." % (self._synchClass))
            if exit_program_on_exception:
                logging.shutdown()
                os._exit(0)
            
        # Calling execuute() which will make method call so need to pass class and method so call can be made.
        # This is different from synchromization objects that are regulr monitors as we call their methods
        # e.g., "deposit" here.
        # Easiest is to pass args, also x = num(1) print (type(x).__name__) Also def func(self): print(__class__) but
        # execute might be in superclass MonitorSelect of BoundedBufferSelect
        self._returnValue = _execute(self._synchronizer, self._entry_name, self._synchronizer, self._synchronizer_method, 
		self._result_buffer, self._**kwargs)
        #self._returnValue = self._synchronizer_method(self._synchronizer,**self._kwargs)

        logger.trace("SynchronizerThreadSelect: return value is " + str(self._returnValue))
        """
 
        """ removing this SynchronizerThreadSelect
        logger.trace ("doMethodCallSelectExecute starting caller thread to make the call")
        #callerThread = synchronizerThread(PythonThreadID, myName,  synchronizer, synchronizer_method, **kwargs)
        callerThread = SynchronizerThreadSelect(PythonThreadID, myPythonThreadName,  entry_name, synchronizer, synchronizer_method, synchClass, result_buffer, **kwargs)
        callerThread.start()   
        # No need to join? CallerThread will do method call and result_buffer.deposit(value) OR it will
        # find a False guard and since its arrival was saved the caller is effectively blocked and
        # callerThread will just return and die. 
        # Note: we could do join since callerThread will execute 0, 1, or more entry.accepts and 
        # possibly do result_buffer.deposit and so always return and die so join will work.
        # Q what should caller thread return 0? 1? This vaalue is not going back to caller, those
        # values always return via result_buffer.deposit()
        callerThread.join()
        """
        
        """ New call with synchronizer_thread unrolled """
        try:
            execute = getattr(synchClass,"execute")
        except Exception:
            logger.exception("doMethodCallSelectExecute: Failed to find method 'execute' on object '%s'." % (synchClass))
            if wukongdnc.dag.DAG_executor_constants.exit_program_on_exception:
                logging.shutdown()
                os._exit(0)
            
        # Calling execuute() which will make method call so need to pass class and method so call can be made.
        # This is different from synchromization objects that are regulr monitors as we call their methods
        # e.g., "deposit" here.
        # Easiest is to pass args, also x = num(1) print (type(x).__name__) Also def func(self): print(__class__) but
        # execute might be in superclass MonitorSelect of BoundedBufferSelect  
        
        _return_value_ignored = execute(synchronizer, entry_name, synchronizer, synchronizer_method, result_buffer, **kwargs)
     
        # unlock the synchrnizer before bocking on withdaw(). Method withdraw() may not unblock until after a call to
        # a synchronize_synch or synchronize_asynch but these methods try to lock the synchronizer so we need to 
        # release our synchronzer lock here. (Blocking on withdaw means the call to a method, e.g., method P on 
        # a CountingSeaphore_Monitor, was not accepted by execute so the call to P will only be accepted/chosen by
        # a later call to execute for method V. At that time execute will depost the result of the P operation in the 
        # result buffer to be withdrawn here (by the handler thread of the TCP server for the earlier P operation).
        self.unlock_synchronizer()
        
        result = result_buffer.withdraw()
        returnValue = result[0]
        restart = result[1]

        """ remove these three lines too
        #logger.trace("doMethodCallSelectExecute calling acquire exited.")
        #synchronizer._exited.acquire()
        #logger.trace("doMethodCallSelectExecute called acquire exited. returning ...")
        """
        
        logger.trace("doMethodCallSelectExecute: returnValue: " + str(returnValue) + ", restart: " + str(restart))  
        
        return restart, returnValue