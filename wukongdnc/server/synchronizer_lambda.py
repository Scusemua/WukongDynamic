#from re import L
#from .barrier import Barrier
#from .state import State
#from ClientNew import CallbackHandler
import importlib
#from pydoc import locate
#from .synchronizer_thread import synchronizerThread
#import boto3 
#import json
#import cloudpickle

from ..wukong.invoker import invoke_lambda 

#from .barrier import Barrier
#from .bounded_buffer import BoundedBuffer
#from .bounded_buffer_select import BoundedBuffer_Select
#from .CountingSemaphore_Monitor import CountingSemaphore_Monitor
#from .fanin import FanIn
from .result_buffer import ResultBuffer

from .util import isTry_and_getMethodName, isSelect #make_json_serializable, decode_and_deserialize, 

import logging 
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(asctime)s] [%(threadName)s] %(levelname)s: %(message)s')

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)

logger.addHandler(ch)

aws_region = 'us-east-1'

# This class handles all interactions with the synchronization objects. A Synchronizer wraps a sycnhronization object.
class Synchronizer(object):

    # valid synchronization objects
    synchronizers = {"barrier", "Barrier", "semaphore", "Semaphore", "bounded_buffer", "BoundedBuffer", "fanin", 
    "FanIn", "CountingSemaphore_Monitor", "CountingSemaphore_Monitor_Select", 
    "BoundedBuffer_Select", "DAG_executor_FanIn", "DAG_executor_FanInNB",
    "DAG_executor_FanIn_Select", "DAG_executor_FanInNB_Select"
    }

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
        "DAG_executor_FanIn_Select": "DAG_executor_FanIn_select" ,
        "DAG_executor_FanInNB_Select": "DAG_executor_FanInNB_select"
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
    
        print("locking synchronizer")
        
        try:
            synchronizer_method = getattr(self._synchClass,"lock")
        except Exception as ex:
            print("lock_synchronizer: Failed to find method 'lock' on object of type '%s'." % (self._synchClass))
            raise ex
            
        synchronizer_method(self._synchronizer)
 
    def unlock_synchronizer(self):
    
        print("unlocking synchronizer") 
        try:
            synchronizer_method = getattr(self._synchClass,"unlock")
        except Exception as ex:
            print("unlock_synchronizer: Failed to find method 'unlock' on object of type '%s'." % (self._synchClass))
            raise ex
            
        synchronizer_method(self._synchronizer)

    def create(self, synchronizer_class_name, synchronizer_object_name, **kwargs):
        # where init call by Client is init(“Barrier”,”b”,[‘n’,2]): and kwargs passed to Barrier.init
        self._synchronizer_class_name = synchronizer_class_name

        if not synchronizer_class_name in Synchronizer.synchronizers:
            logger.error("create: Invalid synchronizer class name: '%s'" % synchronizer_class_name)
            raise ValueError("Invalid synchronizer class name: '%s'" % synchronizer_class_name)
        
        if not synchronizer_class_name in Synchronizer.file_map:
            logger.error("create: Could not find source file for Synchronizer '%s'" % synchronizer_class_name)
            raise ValueError("Could not find source file for Synchronizer '%s'" % synchronizer_class_name)

        #e.g. “Barrier_b”
        self._synchronizer_name = (str(synchronizer_class_name) + '_' + str(synchronizer_object_name))
        
        logger.debug("create: Attempting to locate class '%s'" % synchronizer_class_name)
        
        src_file = Synchronizer.file_map[synchronizer_class_name]
        #logger.debug("Creating synchronizer with name '%s' by calling locate('%s.%s')"  
        #% (self._synchronizer_name, src_file, synchronizer_class_name))
        logger.debug("create: Creating synchronizer with name '%s'" % self._synchronizer_name)

        # Get the class object for a synchronizer object, e.g.. Barrier
        module = importlib.import_module("wukongdnc.server." + src_file)
        #module = importlib.import_module(src_file)
        self._synchClass = getattr(module, synchronizer_class_name)

        if (self._synchClass is None):
            raise ValueError("Failed to locate and create synchronizer of type %s" % synchronizer_class_name)

        # Create the synchronization object
        #logger.debug("got MyClass")
        self._synchronizer = self._synchClass(self._synchronizer_name)
        if self._synchronizer == None:
            logger.error("create: Failed to locate and create synchronizer of type %s" % synchronizer_class_name)
            return -1
        
        #e.g. "b"
        self._synchronizer_object_name = synchronizer_object_name
        logger.debug("create: self._sycnhronizer_object_name: " + self._synchronizer_object_name)

        # init the synchronzation object
        logger.debug("create: Calling _synchronizer init")
        self._synchronizer.init(**kwargs)  #2
        # where Barrier init is: init(**kwargs): if len(kwargs) not == 1
        # logger.debug(“Error: Barrier init has too many argos”) self._n = kwargs[‘n’]

        logger.debug ("create: Called _synchronizer init")
        return 0

    # def synchronize_sync(self, tcp_server, obj_name, method_name, type_arg, state, synchronizer_name):        
    # def synchronize_sync(self, tcp_server, obj_name, method_name, state, synchronizer_name, tcp_handler):
    def synchronize_sync(self, obj_name, method_name, state, synchronizer_name):
    
        logger.debug("synchronizer: synchronize_sync: caled")

        base_name, isTryMethod = isTry_and_getMethodName(method_name)
        is_select = isSelect(self._synchronizer_class_name) # is_select = isSelect(type_arg)
    
        logger.debug("synchronizer: synchronize_sync: method_name: " + method_name + ", base_name: " + base_name 
            + ", isTryMethod: " + str(isTryMethod))
        logger.debug(" self._synchronizer_class_name: : " + self._synchronizer_class_name + ", is_select: " + str(is_select))
        # logger.debug("synchronizer: synchronize_sync: type_arg: " + type_arg + ", is_select: " + str(is_select))
        #logger.debug("base_name: " + base_name)
        #logger.debug("isTryMethod: " + str(isTryMethod))
        
        # COMMENTED OUT:
        # The TCP server does not have a '_synchClass' variable, so that causes an error to be thrown.
        # We aren't even using the `_synchronizer_method` variable anywhere though, so I've just
        # commented this out. I don't think we need it?
        
        # try:
        #     _synchronizer_method = getattr(self._synchClass, method_name)
        # except Exception as x:
        #     logger.debug("Caught Error >>> %s" % x)

        if isTryMethod: 
        
            ### selects are not monitors, i.e., no enter_monitor, so lock here
            ##if is_select:
                ##self.lock_synchronizer()
                
            # check if synchronize op will block, if yes tell client to terminate then call op 
            ##if is_select:
            try_return_value = self.trySynchronizeSelect(method_name, state, **state.keyword_arguments)
            ##else:
                ##try_return_value = self.trySynchronize(method_name, state, **state.keyword_arguments)

            logger.debug("synchronizer_lambda: synchronize_sync: Value of try_return_value (Block) for fan-in ID %s: %s" % (obj_name, str(try_return_value)))
            
            if try_return_value == True:   # synchronize op will execute wait so tell client to terminate
                # Do this here instead of in execute(); execute() will find that the guard is false and do 
                # nothing (after saving caller information (including state) in an Arrival for the called entry
                state.blocking = True 
                state.return_value = None 
                
                ### Still hold the lock;  we could move this to after the unlock so the select would be released sooner.
                ### This allows client to terminate sooner than if we did this after releasing lock, but it delays
                ### other callers from executing select since lock is held.
                ### Could start a thread to do this asynchronously.
                ##tcp_handler.send_serialized_object(cloudpickle.dumps(state))
                
                # execute synchronize op, but don't send result to client
                ##if is_select:
                    # create result_buffer, create execute() reference, call execute(), result_buffer.withdraw(), 
                    # with executes's result and restart do restart, (by definition of synchronous try-op that Blocked)
                    # and ignore return value here.
                    ##return_value = self.synchronizeSelect(base_name, state, **state.keyword_arguments)
                ##else:
                    ##return_value = self.synchronize(base_name, state, **state.keyword_arguments)
                    
                logger.debug("synchronizer_lambda: synchronize_sync: Calling synchronizeLambda(): block = true")
                wait_for_result = False
                return_value_ignored = self.synchronizeLambda(base_name, state, wait_for_result, **state.keyword_arguments)
                    
                # release lock here and in the else so we can release the lock in the lse befor the tcp send to the client
                ##if is_select:
                    ##self.unlock_synchronizer()

                logger.debug("synchronizer_lambda: synchronize_sync:  Value of return_value (not to be sent) for fan-in ID %s and method %s: %s" 
                    % (obj_name, method_name, str(state.return_value)))
                # sending back the above state.blocking = True; state.return_value = None. The call to synchronizeLambda will end up
                # blocking (since the trySynchronizeSelect returned blocking true). This means at some later point the calling Lambda
                # will be restarted and given its saved state. Tbis means the return value in return_value_ignored = self.synchronizeLambda
                # is not a state, it is not a meaningful value and is ignored.
                return(state)
            
            else:
            
                # execute synchronize op and send result to client
                ##if is_select:
                    ##logger.debug("Calling synchronizeSelect()")
                    # create result_buffer, create execute() reference, call execute(), result_buffer.withdraw(), 
                    # return execute's result, with no restart (by definition of synchronous try-op that did not Block)
                    # and send result to client below.
                    ##return_value = self.synchronizeSelect(base_name, state, **state.keyword_arguments)
                ##else:
                    ##logger.debug("synchronizer: synchronize_sync: Calling synchronize()")
                    ##return_value = self.synchronize(base_name, state, **state.keyword_arguments)

                # This call to self.synchronizeLambda does not block since it is a non-blockingg try-op,
                # state.return_value is set in execuute()
                logger.debug("synchronizer_lambda: synchronize_sync: Calling synchronizeLambda(): block = false")
                wait_for_result = True
                return_value = self.synchronizeLambda(base_name, state, wait_for_result, **state.keyword_arguments)

                # Do this here, not in execute(). In execute(), if we do restart on noblock then set these state values
                # there but if no restart we will return the return_value to the lambda client.
                state.return_value = return_value
                state.blocking = False 
                
                # release lock before tcp sending the result to client of synchronous call
                ##if is_select:
                    ##self.unlock_synchronizer()
                    
                ##logger.debug("synchronizer: synchronize_sync: %s sending %s back for method %s." 
                #% (synchronizer_name, str(return_value), method_name))
                logger.debug("synchronizer_lambda: synchronize_sync:  %s returning %s for method %s." % (synchronizer_name, str(state.return_value), method_name))

                # send tuple to be consistent, and False to be consistent, i.e., get result if False.
                # This is after releasng the lock
                ##tcp_handler.send_serialized_object(cloudpickle.dumps(state))  

                # this is a non-blocking try-op so lambda client is waiting for result in state.return_value
                return state
        else:  
            
            logger.debug("****************")
            logger.debug("Error: all synchronous operations must be try-ops")
            logger.debug("****************")
            
            # not a "try" so do synchronization op and send result to waiting client

            ##if is_select:
                ##self.lock_synchronizer()
            
            ##if is_select:
                # create result_buffer, create execute() reference, call execute(), result_buffer.withdraw(), 
                # return excute's result, with no restart (by definition of synchronous non-try-op)
                # (Send result to client below.)
                ##return_value = self.synchronizeSelect(base_name, state, **state.keyword_arguments)
            ##else:
                ##return_value = self.synchronize(base_name, state, **state.keyword_arguments)
                 
            logger.debug("synchronizer_lambda: synchronize_sync: Calling synchronizeLambda(): not a try-op: block = false")
            wait_for_result = True
            return_value = self.synchronizeLambda(base_name, state, wait_for_result, **state.keyword_arguments)
                
            state.return_value = return_value
            state.blocking = False 
            
            # release lock before TCP sending the result to client.
            ##if is_select:
            ##    self.unlock_synchronizer()  

            ##logger.debug("synchronizer: synchronize_sync: %s sending %s back for method %s." % (synchronizer_name, str(return_value), method_name))  
            
            ##tcp_handler.send_serialized_object(cloudpickle.dumps(state))
            
            logger.debug("synchronizer_lambda: synchronize_sync:  %s returning %s for method %s."  % (synchronizer_name, str(return_value), method_name))

            return state
        
    def synchronize_async(self, obj_name, method_name, state, synchronizer_name):
        """
        Asynchronous synchronization.
        """
        logger.debug("synchronizer_lambda: synchronize_async:  called. method_name = '" + method_name + "'")

        # is_select = isSelect(type_arg)
        ##is_select = isSelect(self._synchronizer_class_name)
        ##logger.debug("self._synchronizer_class_name: " + self._synchronizer_class_name + ", is_select: " + str(is_select))
        
        ##if is_select:
        ##    self.lock_synchronizer()
     
        ##if is_select:
        ##    sync_ret_val = self.synchronizeSelect(method_name, state, **state.keyword_arguments)
        ##else:
        ##    sync_ret_val = self.synchronize(method_name, state, **state.keyword_arguments)  

        # In previous versions, async return value is assumed to be meaningless, and call is assumed to be non-blocking. 
        # (For example, semaphore.V().) # Thus, return value ignored, and a 0 is instead sent to client.
        
        #  Now we allow clients to always terminate after call. That is, terminate the lambda clint regardless of whether 
        #  or not the call blocks Also, we allow meaningful return values for async calls, e.g., BB.wwithdraw(). 
        #  The lambda client can always terminate and we then always restart the client. 
        #  This means we restart on block and resstart on no-block.So asynch still ignores the return valuue of call 
        #  and terminates, but gets the return value, meaningful or not on the restart
        #
        #  Note: the client code needs to be written to allow restart after the asynch call - and need matching 
        #  restart_on_no_block in the synchronization object (e.g., BoundedBuffer_Select)

        logger.debug("synchronizer_lambda: synchronize_async: Calling synchronizeLambda(): not a try-op: wait_for_result = false")
        wait_for_result = False # asynch calls never wait for a result
        return_value_ignored = self.synchronizeLambda(method_name, state, wait_for_result, **state.keyword_arguments)

        ##if is_select:
        ##    self.unlock_synchronizer()           
        
        logger.debug("synchronizer_lambda: synchronize_async:  Synchronize/synchronizeSelect returned: %s" % str(return_value_ignored))
        
        return 0

    # For try-ops this method calls the try-op method defined by the user (for non-select synchronizers). 
    # Called by synchronize_synch in tcp_server
    def trySynchronize(self, method_name, state, **kwargs):
        logger.debug("trySynchronize: starting trySynchronize, method_name: " + method_name + ", ID is: " + state.function_instance_ID)
        
        try:
            synchronizer_method = getattr(self._synchClass,method_name)
        except Exception as x:
            logger.error("trySynchronize: Caught Error >>> %s" % x)
            raise ValueError("Synchronizer of type %s does not have method called %s. Cannot complete trySynchronize() call." % (self._synchClass, method_name))

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
        
        #logger.debug("trySynchronize (ignoring)" + " restart " + str(restart))
        logger.debug("trySynchronize: " + " returnValue " + str(returnValue))
        
        return returnValue
        
    # For try-ops this method calls the try-op method defined by the user (for synchronizers that use select)
    # Same as trySynchronize() but we are keeping the non-sect and select code separate.
    # Called by synchronize_synch in tcp_server
    def trySynchronizeSelect(self, method_name, state, **kwargs):
        logger.debug("trySynchronizeSelect: method_name: " + method_name + ", ID is: " + state.function_instance_ID)
        
        try:
            synchronizer_method = getattr(self._synchClass, method_name)
        except Exception as x:
            logger.error("trySynchronizeSelect: Caught Error >>> %s" % x)
            raise ValueError("Synchronizer of type %s does not have method called %s. Cannot complete trySynchronize() call." % (self._synchClass, method_name))

        """ replace this call 
        myPythonThreadName = "Try_callerThread" + state.function_instance_ID #str(ID_arg)
        returnValue, restart  = self.doMethodCallSelectTry(2, myPythonThreadName, self._synchronizer, synchronizer_method, **kwargs)
        """
        
        """ with this call """
        returnValue = synchronizer_method(self._synchronizer, **kwargs)
               
        #logger.debug("trySynchronizeSelect (ignoring)" + " restart " + str(restart))
        logger.debug("trySynchronizeSelect: " + " returnValue " + str(returnValue))
        
        return returnValue
        
    # This method makes the actual method call to the called method of the synchronization object and when that call
    # returns it restarts the serverless function, if necessary.
    # Called by synchronize_synch in tcp_server
    def synchronize(self, method_name, state, **kwargs):
        logger.debug("synchronize: method_name: " + str(method_name) + ", ID is: " + state.function_instance_ID)
        
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
        except Exception as ex:
            logger.error("synchronize: Failed to find method '%s' on object '%s'." % (method_name, self._synchClass))
            raise ex
        
        """ replace this call 
        myPythonThreadName = "NotTrycallerThread"+str(self.threadID)
        returnValue, restart  = self.doMethodCall(1, myPythonThreadName, self._synchronizer, synchronizer_method, **kwargs) 
        """
        
        """ with this unrolled """
        returnValue, restart = synchronizer_method(self._synchronizer, **kwargs) 
         
        logger.debug("synchronize: method_name: " + str(method_name) + ", restart " + str(restart))
        logger.debug("synchronize: method_name: " + str(method_name) + ", returnValue " + str(returnValue))
        logger.debug("synchronize: method_name: " + str(method_name) + ", successfully called synchronize method. ")

        # if the method returns restart True, restart the serverless function and pass it its saved state.
        if restart:
            state.restart = True 
            state.return_value = returnValue
            state.blocking = False            
            logger.info("synchronize: Restarting Lambda function %s." % state.function_name)
            payload = {"state": state}
            invoke_lambda(payload = payload, is_first_invocation = False, function_name = "ComposerServerlessSync")
        
        return returnValue

    # Note, we still pass the synchronizer and method which are needed inside execute().
    # Called by synchronize_synch.
    def synchronizeSelect(self, method_name, state, wait_for_result, **kwargs):
        logger.debug("synchronizeSelect: method_name: " + str(method_name) + ", ID is: " + state.function_instance_ID)
        
        try:
            synchronizer_method = getattr(self._synchClass, method_name)
        except Exception as ex:
            logger.error("synchronizeSelect: Failed to find method '%s' on object '%s'." % (method_name, self._synchClass))
            raise ex
        
        """ Replacing these 4 lines, which call doMethodCallSelectExecute
        # Note: passing actual Python method reference _synchronizer_method, as well as the method_name
        myPythonThreadName = "NotTrycallerThread"+str(self.threadID)
        # create result_buffer, create execute() reference, call execute(), result_buffer.withdraw(), get executes's result and restart
        returnValue, restart  = self.doMethodCallSelectExecute(1, myPythonThreadName, method_name, self._synchronizer, 
        synchronizer_method, self._synchClass, **kwargs) 
        """
        
        """ New call with doMethodCallSelectExecute unrolled  """
        try:
            execute = getattr(self._synchClass,"execute")
        except Exception as ex:
            logger.error("synchronizeSelect: Failed to find method 'execute' on object '%s'." % (self._synchClass))
            raise ex
            
        result_buffer = ResultBuffer(1, "resultBuffer")
            
        # Calling execute() which will make method call so need to pass class and method so call can be made.
        # This is different from synchromization objects that are regulr monitors as we call their methods
        # e.g., "deposit" here.
        # Easiest is to pass args, also x = num(1) print (type(x).__name__) Also def func(self): print(__class__) but
        # execute might be in superclass MonitorSelect of BoundedBufferSelect  

#1: pass state, wait_for_result, but we do not save state in this non-lambda version so pass None for state
        returnValueIgnored = execute(self._synchronizer, method_name, self._synchronizer, synchronizer_method, 
        result_buffer, None, wait_for_result, **kwargs)
 
        # unlock the synchronizer before bocking on withdaw(). Method withdraw() may not unblock until after a call to
        # a synchronize_synch or synchronize_asynch but these methods try to lock the synchronizer so we need to 
        # release our synchronzer lock here. (Blocking on withdaw means the call to a method, e.g., method P on 
        # a CountingSeaphore_Monitor, was not accepted by execute so the call to P will only be accepted/chosen by
        # a later call to execute for method V. At that time execute will depost the result of the P operation in the 
        # result buffer to be withdrawn here (by the handler thread of the TCP server for the earlier P operation).
        self.unlock_synchronizer()
        
        # Always false for asynch; False for try-op that blocks; True for try-op that does not block
        if wait_for_result:
            result = result_buffer.withdraw()
            returnValue = result[0]
            restart = result[1] 
        
        logger.debug("synchronizeSelect: restart " + str(restart))
        logger.debug("synchronizeSelect: returnValue " + str(returnValue))

        # if the method returns restart True, restart the serverless function and pass it its saved state.
        if restart:
            state.restart = True 
            state.return_value = returnValue
            state.blocking = False            
            logger.info("Restarting Lambda function %s." % state.function_name)
            payload = {"state": state}
            invoke_lambda(payload = payload, is_first_invocation = False, function_name = "ComposerServerlessSync")
        
        return returnValue
    
    # Note, we still pass the synchronizer and method which are needed inside execute().
    # Called by synchronize_synch.
    def synchronizeLambda(self, method_name, state, wait_for_result, **kwargs):
        logger.debug("synchronizer_lambda: synchronizeLamba: method_name: " + str(method_name) + ", ID is: " + state.function_instance_ID)
        
        try:
            synchronizer_method = getattr(self._synchClass, method_name)
        except Exception as ex:
            logger.error("synchronizer_lambda: synchronizeLamba:: Failed to find method '%s' on object '%s'." % (method_name, self._synchClass))
            raise ex
        
        """ Replacing these 4 lines, which call doMethodCallSelectExecute
        # Note: passing actual Python method reference _synchronizer_method, as well as the method_name
        myPythonThreadName = "NotTrycallerThread"+str(self.threadID)
        # create result_buffer, create execute() reference, call execute(), result_buffer.withdraw(), get executes's result and restart
        returnValue, restart  = self.doMethodCallSelectExecute(1, myPythonThreadName, method_name, 
            self._synchronizer, synchronizer_method, self._synchClass, **kwargs) 
        """
        
        """ New call with doMethodCallSelectExecute unrolled  """
        try:
            execute = getattr(self._synchClass,"execute")
        except Exception as ex:
            logger.error("synchronizer_lambda: synchronizeLamba: Failed to find method 'execute' on object '%s'." % (self._synchClass))
            raise ex
           
# 1:
# async uses not wait_for_result so need a result_buufer and result will be withdrawn and returned and ignored
        if wait_for_result:
            result_buffer = ResultBuffer(1, "resultBuffer")
        else:
            result_buffer = None 
            
        # Calling execute() which will make method call so need to pass class and method so call can be made.
        # This is different from synchromization objects that are regulr monitors as we call their methods
        # e.g., "deposit" here.
        # Easiest is to pass args, also x = num(1) print (type(x).__name__) Also def func(self): print(__class__) but
        # execute might be in superclass MonitorSelect of BoundedBufferSelect  
        
# 2: passing wait_for_result to execute(), which receives is as send_result, i.e., the vaue of wait_for_result determines
#    whether we wait for result and whether execute() will send it.

        #FYI this is 0 (op did not block and op result was deposited in result_buffer) or 1 (op blocked or no meaningful 
        # return value, e.g., return value of semaphore.V)?
        returnValueIgnored = execute(self._synchronizer, method_name, self._synchronizer, synchronizer_method, result_buffer, state, wait_for_result, **kwargs)
        
        # unlock the synchronizer before blocking on withdaw(). Method withdraw() may not unblock until after a call to
        # a synchronize_synch or synchronize_asynch but these methods try to lock the synchronizer so we need to 
        # release our synchronzer lock here. (Blocking on withdaw means the call to a method, e.g., method P on 
        # a CountingSemaphore_Monitor, was not accepted by execute so the call to P will only be accepted/chosen by
        # a later call to execute for method V. At that time execute will depost the result of the P operation in the 
        # result buffer to be withdrawn here (by the handler thread of the TCP server for the earlier P operation).
        
#3:
        ##self.unlock_synchronizer()
        
#4:
        if wait_for_result:
            result = result_buffer.withdraw()
            returnValue = result[0]
            restart = result[1]
            logger.debug("synchronizer_lambda: synchronizeLamba: after result_buffer.withdraw: restart " + str(restart))
            logger.debug("synchronizer_lambda: synchronizeLamba: result_buffer.withdraw: " + str(returnValue))
            if restart: #assert
                logger.error("synchronizer_lambda: synchronizeLamba: Internal ERROR: result_buffer.withdraw returned restart true.")
#5:
        else:
            # If we did not wait_for_result we do not want to return the return_value of the operation to the user since there
            # is no meaningul return value or we will return the vallue when the lambda client is restarted.
            returnValue = returnValueIgnored

#6:
        ### if the method returns restart True, restart the serverless function and pass it its saved state.
        ##if restart:
            ##state.restart = True 
            ##state.return_value = returnValue
            ##state.blocking = False            
            ##logger.info("synchronizer_lambda: synchronizeLamba: Restarting Lambda function %s." % state.function_name)
            ##payload = {"state": state}
            ##invoke_lambda(payload = payload, is_first_invocation = False, function_name = "ComposerServerlessSync")
        
        return returnValue