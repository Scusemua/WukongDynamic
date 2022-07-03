from re import L
#from monitor_su import MonitorSU, ConditionVariable
import threading
import _thread
import time
from .selector import Selector
from .selectableEntry import selectableEntry

import logging 
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(asctime)s] [%(threadName)s] %(levelname)s: %(message)s')

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)

logger.addHandler(ch)

#Monitor implementation of a counting semaphore with operations P and V
class CountingSemaphore_Monitor_Select(Selector):
    def __init__(self, selector_name = None):
        super(CountingSemaphore_Monitor_Select, self).__init__(selector_name = selector_name)
        self._permits = 1
        
    def init(self, **kwargs):
        logger.debug(kwargs)
        if kwargs is None or len(kwargs) == 0:
            raise ValueError("CountingSemaphore_Monitor_Select requires a length > 0. No kwargs provided.")
        elif len(kwargs) > 2:
            raise ValueError("Error - CountingSemaphore_Monitor_Select init has too many kwargs args. kwargs: " + str(kwargs))
        self._permits= kwargs['initial_permits']

        self._P = selectableEntry("P")
        self._V = selectableEntry("V")

        # superclass method calls
        self.add_entry(self._P)     # alternative 1
        self.add_entry(self._V)     # alternative 2
        
        #self.set_restart_on_block(False)
        self.set_restart_on_block(True)
        self.set_restart_on_noblock(True) # self.set_restart_on_noblock(False) 
        self.set_restart_on_unblock(True)
        
    def set_guards(self):
        #self._P.guard(self._permits < 1)
        self._P.guard(self._permits > 0)
        self._V.guard (True)

    def try_P(self, **kwargs):
        decremented_permits = self._permits - 1
        block = self.is_blocking(decremented_permits < 0)
        return block 

	# synchronous try version of P, restart if block; no meaningful return value expected by client
    def P(self, **kwargs):
        logger.debug("CountingSemaphore_Monitor_Select P() entered monitor, permits = " + str(self._permits))
        self._permits -= 1
        return 0

    def V(self, **kwargs):
        self._permits += 1
    
    def try_V(self, **kwargs):
        block = self.is_blocking(False)
        return block         
        
#local tests
def taskP(b : CountingSemaphore_Monitor_Select):
    logger.debug("Calling P")
    b.P()
    logger.debug("Successfully called P")

def taskV(b : CountingSemaphore_Monitor_Select):
    time.sleep(1)
    logger.debug("Calling V")
    b.V()
    logger.debug("Successfully called V")


def main():
    b = CountingSemaphore_Monitor_Select(selector_name="sem")
    b.P()
    b.V()
    b.P()
    b.V()

    try:
        logger.debug("Starting D thread")
        _thread.start_new_thread(taskP, (b,))
    except Exception as ex:
        logger.debug("[ERROR] Failed to start P thread.")
        logger.debug(ex)

    try:
        logger.debug("Starting first thread")
        _thread.start_new_thread(taskV, (b,))
    except Exception as ex:
        logger.debug("[ERROR] Failed to start V thread.")
        logger.debug(ex)

    logger.debug("Sleeping")
    time.sleep(2)
    logger.debug("Done sleeping")

if __name__=="__main__":
    main()
