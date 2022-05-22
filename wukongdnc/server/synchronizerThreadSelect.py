from threading import Thread

import logging 
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s')

from .synchronizer_thread import synchronizerThread

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)

logger.addHandler(ch)

# A Synchronizer object wraps a user-defined object. The Synchronizer issues the actual method calls
# on the object it wraps. The Synchronizer creates a synchronizer thread that makes the actuak method call.
# Used for synchromization objects that are based on selective waits.
class SynchronizerThreadSelect(Thread):
    def __init__(self, PythonThreadID, PythonThreadName, entry_name, synchronizer, synchronizer_method, result_buffer, **kwargs):
        # Call the Thread class's init function
        #Thread.__init__(self)
        super(synchronizerThread,self).__init__(name=PythonThreadName)
        self._threadID = threadID
        self._synchronizer = synchronizer
        self._synchronizer_method = synchronizer_method
        self._restart = True
        self._kwargs = kwargs
        self._result_buffer = result_buffer
        self._entry_name = entry_name

    def getRestart(self):
        return self._restart
    
    def getID(self):
        return self._threadID
    
    def getReturnValue(self):
        return self._returnValue
        
    # Override the run() function of Thread class
    def run(self):
        #print("kwargs serverlessFunctionID: " + self._serverlessFunctionID)

        try:
            _execute = getattr(self._synchClass,"execute")
        except Exception as ex:
            logger.error("Failed to find method 'execute' on object '%s'." % (self._synchClass))
            raise ex
        # Calling execuute() which will make method call so need to pass class and method so call can be made.
        # This is different from synchromization objects that are regulr monitors as we call their methods
        # e.g., "deposit" here.
        # Easiest is to pass args, also x = num(1) print (type(x).__name__) Also def func(self): print(__class__) but
        # execute might be in superclass MonitorSelect of BoundedBufferSelect
        self._returnValue = self._execute(self._synchronizer, self.entry_name, self.synchronizer, self.synchronizer_method, 
		self.result_buffer, **self.kwargs)
        #self._returnValue = self._synchronizer_method(self._synchronizer,**self._kwargs)
        # where wait_b in Barrier is wait_b(self, **kwargs):

        logger.debug("return value is " + str(self._returnValue))