#from re import L
from .monitor_su import MonitorSU
#from monitor_su import MonitorSU
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

# Bounded Buffer
class BoundedBuffer(MonitorSU):
    def __init__(self, initial_capacity = 0, monitor_name = None):
        super(BoundedBuffer, self).__init__(monitor_name=monitor_name)
        self._capacity = initial_capacity

    def init(self, **kwargs):
        self._fullSlots=0
        self._capacity = kwargs["n"]
        # Need there to be an element at buffer[i]. Cannot use insert() since it will shift elements down.
        # If we set buffer[0] we need an element to be at position 0 or we get an out of range error.
        #self._buffer=[]
        self._buffer= [None] * self._capacity
        self._notFull=super().get_condition_variable(condition_name="notFull")
        self._notEmpty=super().get_condition_variable(condition_name="notEmpty")
        logger.info(kwargs)
        self._in=0
        self._out=0
		
	# synchronous try version of deposit, restart when block
    def deposit_try_and_restart(self, **kwargs):
        super().enter_monitor(method_name="deposit")
        logger.info(" deposit() entered monitor, len(self._notFull) ="+str(len(self._notFull))+",self._capacity="+str(self._capacity))
        logger.info(" deposit() entered monitor, len(self._notEmpty) ="+str(len(self._notEmpty))+",self._capacity="+str(self._capacity))
        value = kwargs["value"]
        logger.info("Value to deposit: " + str(value))
        if self._fullSlots==self._capacity:
            logger.info("Full slots (%d) is equal to capacity (%d). Calling wait_c()." % (self._fullSlots, self._capacity))
            self._notFull.wait_c()
            restart = True
        else:
            restart = False
        self._buffer.insert(self._in,value)
        self._in=(self._in+1) % int(self._capacity)
        self._fullSlots+=1
        self._notEmpty.signal_c_and_exit_monitor()
        #threading.current_thread()._restart = False
        #threading.current_thread()._returnValue=1
        return 0, restart

	# synchronous no-try version of deposit, blocking w/ no restart
    def deposit(self, **kwargs):
        super().enter_monitor(method_name="deposit")
        logger.debug(" deposit() entered monitor, len(self._notFull) ="+str(len(self._notFull))+",self._capacity="+str(self._capacity))
        logger.debug(" deposit() entered monitor, len(self._notEmpty) ="+str(len(self._notEmpty))+",self._capacity="+str(self._capacity))
        value = kwargs["value"]
        logger.debug("Value to deposit: " + str(value))
        if self._fullSlots==self._capacity:
            logger.debug("Full slots (%d) is equal to capacity (%d). Calling wait_c()." % (self._fullSlots, self._capacity))
            self._notFull.wait_c()
        self._buffer.insert(self._in,value)
        #self._buffer[self._in] = value
        self._in=(self._in+1) % int(self._capacity)
        self._fullSlots+=1
        self._notEmpty.signal_c_and_exit_monitor()
        restart = False
        #threading.current_thread()._restart = False
        #threading.current_thread()._returnValue=0
        return 0, restart

	# synchronous no-try version of deposit, blocking w/ no restart
    def deposit_all(self, **kwargs):
        # assumes kwargs["list_of_values"] exists and is a list of values to deposit.
        super().enter_monitor(method_name="deposit")
        logger.debug(" deposit_all() entered monitor, len(self._notFull) ="+str(len(self._notFull))+",self._capacity="+str(self._capacity))
        logger.debug(" deposit_all() entered monitor, len(self._notEmpty) ="+str(len(self._notEmpty))+",self._capacity="+str(self._capacity))
        list_of_values = kwargs["list_of_values"]
        for value in list_of_values:
            logger.debug("Value to deposit: " + str(value))
            if self._fullSlots==self._capacity:
                logger.debug("Full slots (%d) is equal to capacity (%d). Calling wait_c()." % (self._fullSlots, self._capacity))
                self._notFull.wait_c()
            #self._buffer.insert(self._in,value)
            #logger.debug(" deposit put before: " + value + " self._in: " + str(self._in))
            self._buffer[self._in] = value
            #logger.debug(" deposit put after: " + value + " self._in: " + str(self._in))
            self._in=(self._in+1) % int(self._capacity)
            self._fullSlots+=1
            # We will wake up a consumer, if any are waiting, whihc blocks us here
            # until the consumer is done. Then we will continue. (Noet this is a 
            # signal and urgent wait monitor, not signal and continue like Java.)
            self._notEmpty.signal_c()
        restart = False
        #threading.current_thread()._restart = False
        #threading.current_thread()._returnValue=0
        super().exit_monitor()
        return 0, restart

	# synchronous try version of withdraw, restart when block
    def withdraw_try_and_restart(self, **kwargs): 
        super().enter_monitor(method_name = "withdraw")
        value = 0
        if self._fullSlots == 0:
            self._notEmpty.wait_c()
            #threading.current_thread()._restart = True
            restart = True
        else:
			#threading.current_thread()._restart = False
            restart = False
        value = self._buffer[self._out]
        self._out = (self._out+1) % int(self._capacity)
        self._fullSlots -= 1
        #threading.current_thread()._returnValue = value
        self._notFull.signal_c_and_exit_monitor()
        logger.info(" withdraw() returning value:" + str(value) + " restart:" + str(restart))
        return value, restart

	# synchronous no-try version of withdraw.
    def withdraw(self, **kwargs):
        super().enter_monitor(method_name = "withdraw")
        logger.debug(" withdraw() entered monitor, len(self._notFull) ="+str(len(self._notFull))+", self._capacity="+str(self._capacity))
        logger.debug(" withdraw() entered monitor, len(self._notEmpty) ="+str(len(self._notEmpty))+", self._capacity="+str(self._capacity))
        value = 0
        if self._fullSlots==0:
            self._notEmpty.wait_c()
        value=self._buffer[self._out]
        #logger.debug(" withdraw got " + value + " self._out: " + str(self._out))
        self._out=(self._out+1) % int(self._capacity)
        self._fullSlots-=1
        restart = False
        #threading.current_thread()._restart = False
        #threading.current_thread()._returnValue=value
        self._notFull.signal_c_and_exit_monitor()
        return value, restart

    def try_deposit(self, **kwargs):
        super().enter_monitor(method_name = "try_deposit")
        block = super().is_blocking(self._fullSlots==self._capacity)
        super().exit_monitor()
        return block

    def try_withdraw(self, **kwargs):
        super().enter_monitor(method_name = "try_withdraw")
        block = super().is_blocking(self._fullSlots==0)
        super().exit_monitor()
        return block

#Local tests
def taskD(b : BoundedBuffer):
    time.sleep(1)
    logger.debug("Calling deposit")
    #VALUEHERE = 1
    keyword_arguments = {}
    list_of_values = ['A','B','C']
    #keyword_arguments['value'] = 'A'
    keyword_arguments['list_of_values'] = list_of_values
    b.deposit_all(**keyword_arguments)
    keyword_arguments['value'] = 'D'
    b.deposit(**keyword_arguments)
    list_of_values = ['E','F','G']
    #keyword_arguments['value'] = 'A'
    keyword_arguments['list_of_values'] = list_of_values
    b.deposit_all(**keyword_arguments)
    logger.debug("Successfully called deposit/deposit_all")

def taskW(b : BoundedBuffer):
    logger.debug("Calling withdraw")
    value = b.withdraw()
    logger.debug("Successfully called withdraw: "+ value[0])
    value = b.withdraw()
    logger.debug("Successfully called withdraw: "+ value[0])
    value = b.withdraw()
    logger.debug("Successfully called withdraw: "+ value[0])
    value = b.withdraw()
    logger.debug("Successfully called withdraw: "+ value[0])
    value = b.withdraw()
    logger.debug("Successfully called withdraw: "+ value[0])
    value = b.withdraw()
    logger.debug("Successfully called withdraw: "+ value[0])
    value = b.withdraw()
    logger.debug("Successfully called withdraw: "+ value[0])

def main():
    b = BoundedBuffer(initial_capacity=1,monitor_name="BoundedBuffer")
    VALUEFOR_N = 2
    keyword_arguments = {}
    keyword_arguments['n'] = VALUEFOR_N
    b.init(**keyword_arguments)
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
    time.sleep(4)
    logger.debug("Done sleeping")

if __name__=="__main__":
    main()