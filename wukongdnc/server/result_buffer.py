from re import L
from .monitor_su import MonitorSU, ConditionVariable
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

# Not a User Class, used internally
class ResultBuffer(MonitorSU):
    def __init__(self, initial_capacity = 0, monitor_name = None):
        super(ResultBuffer, self).__init__(monitor_name=monitor_name)
        self._capacity = initial_capacity
        self._fullSlots=0
        self._buffer=[]
        self._notFull=super().get_condition_variable(condition_name="notFull")
        self._notEmpty=super().get_condition_variable(condition_name="notEmpty")
        self._in=0
        self._out=0

    def deposit(self, value):
        super().enter_monitor(method_name="deposit")
        logger.debug(" result buffer deposit() entered monitor, len(self._notFull) ="+str(len(self._notFull))+",self._capacity="+str(self._capacity))
        logger.debug(" result buffer deposit() entered monitor, len(self._notEmpty) ="+str(len(self._notEmpty))+",self._capacity="+str(self._capacity))
        logger.debug(" result buffer: Value to deposit: " + str(value))
        if self._fullSlots==self._capacity:
            logger.debug(" result buffer: Full slots (%d) is equal to capacity (%d). Calling wait_c()." % (self._fullSlots, self._capacity))
            self._notFull.wait_c()
        self._buffer.insert(self._in,value)
        self._in=(self._in+1) % int(self._capacity)
        self._fullSlots+=1
        self._notEmpty.signal_c_and_exit_monitor()
        return 0

    def withdraw(self):
        logger.debug(" result buffer  withdraw() entered monitor, len(self._notEmpty) ="+str(len(self._notEmpty))
		+", self._capacity="+str(self._capacity))
        value = 0
        if self._fullSlots==0:
            logger.debug(" result buffer: Full slots (%d) is equal to 0. Calling wait_c()." % (self._fullSlots))
            self._notEmpty.wait_c()
        value=self._buffer[self._out]
        self._out=(self._out+1) % int(self._capacity)
        self._fullSlots-=1
        self._notFull.signal_c_and_exit_monitor()
        return value

#Local tests
def taskD(b : ResultBuffer):
    time.sleep(1)
    logger.debug("Calling deposit")
    b.deposit(value = "A")
    logger.debug("Successfully called deposit")

def taskW(b : ResultBuffer):
    logger.debug("Calling withdraw")
    value = b.withdraw()
    logger.debug("Successfully called withdraw")


def main():
    b = ResultBuffer(initial_capacity=1,monitor_name="ResultBuffer")
    #b.init()
    #b.deposit(value = "A")
    #value = b.withdraw()
    #logger.debug(value)
    #b.deposit(value = "B")
    #value = b.withdraw()
    #logger.debug(value)

    try:
        logger.debug("Starting D thread")
        _thread.start_new_thread(taskD, (b,))
    except Exception as ex:
        logger.debug("[ERROR] Failed to start first thread.")
        logger.debug(ex)

    try:
        logger.debug("Starting first thread")
        _thread.start_new_thread(taskW, (b,))
    except Exception as ex:
        logger.debug("[ERROR] Failed to start first thread.")
        logger.debug(ex)

    logger.debug("Sleeping")
    time.sleep(2)
    logger.debug("Done sleeping")

if __name__=="__main__":
    main()