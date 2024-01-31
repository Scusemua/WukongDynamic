from threading import Thread
#from .synchronizer_thread import synchronizerThread
import logging 
import os

#from ..dag.DAG_executor_constants import exit_program_on_exception
import wukongdnc.dag.DAG_executor_constants

logger = logging.getLogger(__name__)
"""
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(asctime)s] [%(module)s] [%(processName)s] [%(threadName)s]: %(message)s')
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)
logger.addHandler(ch)
"""

# A Synchronizer object wraps a user-defined object. The Synchronizer issues the actual method calls
# on the object it wraps. The Synchronizer creates a synchronizer thread that makes the actuak method call.
# Used for synchromization objects that are based on selective waits.
# synchClass is needed in order to get a reference to method "execute" of that class.
class SynchronizerThreadSelect(Thread):
    def __init__(self, PythonThreadID, PythonThreadName, entry_name, synchronizer, synchronizer_method, synchClass, result_buffer, **kwargs):
        # Call the Thread class's init function
        #Thread.__init__(self)
        super(SynchronizerThreadSelect,self).__init__(name=PythonThreadName)
        self._threadID = PythonThreadID
        self._synchronizer = synchronizer
        self._synchronizer_method = synchronizer_method
        self._synchClass = synchClass
        #self._restart = True
        self._returnValueIgnored = 0
        self._kwargs = kwargs
        self._result_buffer = result_buffer
        self._entry_name = entry_name

    #def getRestart(self):
        #return self._restart
    
    def getID(self):
        return self._threadID
    
    #def getReturnValue(self):
        #return self._returnValue
        
    # Override the run() function of Thread class
    def run(self):
        #logger.trace("kwargs serverlessFunctionID: " + self._serverlessFunctionID)

        try:
            _execute = getattr(self._synchClass,"execute")
        except Exception:
            logger.exception("[Error]: syncronizerThreadSelect: Failed to find method 'execute' on object '%s'." % (self._synchClass))
            if wukongdnc.dag.DAG_executor_constants.exit_program_on_exception:
                logging.shutdown()
                os._exit(0)
        # Calling execuute() which will make method call so need to pass class and method so call can be made.
        # This is different from synchromization objects that are regulr monitors as we call their methods
        # e.g., "deposit" here.
        # Easiest is to pass args, also x = num(1) print (type(x).__name__) Also def func(self): print(__class__) but
        # execute might be in superclass MonitorSelect of BoundedBufferSelect
        self._returnValueIgnored = _execute(self._synchronizer, self._entry_name, self._synchronizer, self._synchronizer_method, 
		self._result_buffer, **self._kwargs)
        #self._returnValue = self._synchronizer_method(self._synchronizer,**self._kwargs)
        # where wait_b in Barrier is wait_b(self, **kwargs):

        logger.trace("SynchronizerThreadSelect: return value is " + str(self._returnValue))
        
        # Note: results and restart returned through result buffer so return value should be ignored
