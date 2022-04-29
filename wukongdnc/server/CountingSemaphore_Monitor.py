from re import L
from monitor_su import MonitorSU, ConditionVariable
import threading
import _thread
import time

import logging 
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(asctime)s] [%(threadName)s] %(levelname)s: %(message)s')

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)

logger.addHandler(ch)

class CountingSemaphore_Monitor(MonitorSU):
    def __init__(self, initial_permits = 0, monitor_name = None):
        super(CountingSemaphore_Monitor, self).__init__(monitor_name = monitor_name)
        self._permits = initial_permits

    def init(self, initial_permits=0, **kwargs):
        logger.debug(kwargs)
        self._permits= initial_permits
        self._permitAvailable = super().get_condition_variable(condition_name = "permitAvailable")
                                                               
    def P(self):
        super().enter_monitor(method_name = "P")
        logger.debug(" CountingSemaphore_Monitor P() entered monitor, len(self._notEmpty) ="+str(len(self._permitAvailable)) + " permits = " + str(self._permits))

        self._permits -= 1
        if self._permits < 0:
            self._permitAvailable.wait_c()
        else:
            threading.current_thread()._restart = False
	
        threading.current_thread()._returnValue = 1
        super().exit_monitor()

    # V should never block, so no need for restart
    def V(self):
        super().enter_monitor(method_name="V")
        logger.debug(" CountingSemaphore_Monitor V() entered monitor, len(self._notEmpty) ="+str(len(self._permitAvailable)) + " permits = " + str(self._permits))
        self._permits += 1
        threading.current_thread()._returnValue = 1
        threading.current_thread()._restart = False

        self._permitAvailable.signal_c_and_exit_monitor()



def taskP(b : CountingSemaphore_Monitor):
    logger.debug("Calling P")
    b.P()
    logger.debug("Successfully called P")

def taskV(b : CountingSemaphore_Monitor):
    time.sleep(1)
    logger.debug("Calling V")
    b.V()
    logger.debug("Successfully called V")


def main():
    b = CountingSemaphore_Monitor(initial_permits=1,monitor_name="BoundedBuffer")
    b.init(initial_permits=1)
    b.P()
    b.V()
    b.P()
    b.V()
    b.P()


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


