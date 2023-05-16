
from threading import Thread

import logging 
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(asctime)s] [%(module)s] [%(processName)s] [%(threadName)s]: %(message)s')

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)

logger.addHandler(ch)

# NO LONGER USING
# A Synchronizer object wraps a user-defined object. The Synchronizer issues the actual method calls
# on the object it wraps. The Synchronizer creates a synchronizer thread that makes the actyak method call
class synchronizerThread(Thread):
    def __init__(self, PythonThreadID, PythonThreadName, synchronizer, synchronizer_method, **kwargs):
        # Call the Thread class's init function
        #Thread.__init__(self)
        super(synchronizerThread,self).__init__(name=PythonThreadName)
        self._threadID = PythonThreadID
        self._synchronizer = synchronizer
        self._synchronizer_method = synchronizer_method
        self._restart = True
        self._returnValue = 0
        self._kwargs = kwargs

    def getRestart(self):
        return self._restart
    
    def getID(self):
        return self._threadID
    
    def getReturnValue(self):
        return self._returnValue
        
    # Override the run() function of Thread class
    def run(self):
        #print("kwargs serverlessFunctionID: " + self._serverlessFunctionID)

        self._returnValue, self._restart = self._synchronizer_method(self._synchronizer,**self._kwargs)
        #self._returnValue = result[0]
        #self._restart = result[1]

        logger.debug("return value is " + str(self._returnValue))