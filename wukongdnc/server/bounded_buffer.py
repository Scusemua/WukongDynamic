#from re import L
from .monitor_su import MonitorSU
import _thread
import time

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

# Bounded Buffer
class BoundedBuffer(MonitorSU):
    def __init__(self, monitor_name = "BoundedBuffer"):
        super(BoundedBuffer, self).__init__(monitor_name=monitor_name)
        #self._capacity = initial_capacity

    def init(self, **kwargs):
        self._fullSlots=0
        self._capacity = kwargs["n"]
        # Need there to be an element at buffer[i]. Cannot use insert() since it will shift elements down.
        # If we set buffer[0] we need an element to be at position 0 or we get an out of range error.
        #self._buffer=[]
        self._buffer= [None] * self._capacity
        self._notFull=super().get_condition_variable(condition_name="notFull")
        self._notEmpty=super().get_condition_variable(condition_name="notEmpty")
        logger.trace(kwargs)
        self._in=0
        self._out=0
        #rhc: exp
        # used by withdraw half
        self.first = True
		
	# synchronous try version of deposit, restart when block
    def deposit_try_and_restart(self, **kwargs):
        super().enter_monitor(method_name="deposit")
        logger.trace(" deposit() entered monitor, len(self._notFull) ="+str(len(self._notFull))+",self._capacity="+str(self._capacity))
        logger.trace(" deposit() entered monitor, len(self._notEmpty) ="+str(len(self._notEmpty))+",self._capacity="+str(self._capacity))
        value = kwargs["value"]
        logger.trace("Value to deposit: " + str(value))
        if self._fullSlots==self._capacity:
            logger.trace("Full slots (%d) is equal to capacity (%d). Calling wait_c()." % (self._fullSlots, self._capacity))
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
        try:
            super(BoundedBuffer, self).enter_monitor(method_name="deposit")
        except Exception as ex:
            logger.error("[ERROR] Failed super(BoundedBuffer, self)")
            logger.error("[ERROR] self: " + str(self.__class__.__name__))
            logger.trace(ex)
            return 0

        logger.trace(" deposit() entered monitor, len(self._notFull) ="+str(len(self._notFull))+",self._capacity="+str(self._capacity))
        logger.trace(" deposit() entered monitor, len(self._notEmpty) ="+str(len(self._notEmpty))+",self._capacity="+str(self._capacity))
        value = kwargs["value"]
        logger.trace("Value to deposit: " + str(value))
        if self._fullSlots==self._capacity:
            logger.trace("Full slots (%d) is equal to capacity (%d). Calling wait_c()." % (self._fullSlots, self._capacity))
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
    # assumes kwargs["list_of_values"] exists and is a list of values to deposit.
    def deposit_all(self, **kwargs):
        super().enter_monitor(method_name="deposit_all")
        logger.trace(" deposit_all() entered monitor, len(self._notFull) ="+str(len(self._notFull))+",self._capacity="+str(self._capacity))
        logger.trace(" deposit_all() entered monitor, len(self._notEmpty) ="+str(len(self._notEmpty))+",self._capacity="+str(self._capacity))
        list_of_values = kwargs["list_of_values"]
        for value in list_of_values:
            logger.trace("Value to deposit: " + str(value))
            if self._fullSlots==self._capacity:
                logger.trace("Full slots (%d) is equal to capacity (%d). Calling wait_c()." % (self._fullSlots, self._capacity))
                self._notFull.wait_c()
            #self._buffer.insert(self._in,value)
            #logger.trace(" deposit put before: " + value + " self._in: " + str(self._in))
            self._buffer[self._in] = value
            #logger.trace(" deposit put after: " + value + " self._in: " + str(self._in))
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
        logger.trace(" withdraw() returning value:" + str(value) + " restart:" + str(restart))
        return value, restart

	# synchronous no-try version of withdraw.
    def withdraw(self, **kwargs):
        super().enter_monitor(method_name = "withdraw")
        logger.trace("withdraw() entered monitor, len(self._notFull) ="+str(len(self._notFull))+", self._capacity="+str(self._capacity))
        logger.trace("withdraw() entered monitor, len(self._notEmpty) ="+str(len(self._notEmpty))+", self._capacity="+str(self._capacity))
        logger.trace("self._fullSlots="+str(self._fullSlots))
        value = 0
        if self._fullSlots==0:
            self._notEmpty.wait_c()
        value=self._buffer[self._out]
        logger.trace(" withdraw got " + str(value) + ", self._out: " + str(self._out))
        self._out=(self._out+1) % int(self._capacity)
        self._fullSlots-=1
        restart = False
        #threading.current_thread()._restart = False
        #threading.current_thread()._returnValue=value
        self._notFull.signal_c_and_exit_monitor()
        return value, restart

#rhc exp
    # workers can get an initial batch of work. Here we experiment with giving ech of 
    # two workers half of the leaf nodes. In this experiment for TR, the workers run
    # with fanins stored locally excect for the last fanin which is shared by both
    # workers. If a worker executes all the fan_ins for a FanIn, that FanIn can be 
    # stored locally, whch speeds up FanIns.
    # In general, we can, e.g., partition leaf nodes into n partitions for n workers
    # and worker i will call to withdrw partition i. As in a map from ID i to 
    # the partition for ID i, where the partition is a list of work.
    def withdraw_half(self, **kwargs):
        super().enter_monitor(method_name = "withdraw_half")
        logger.trace("withdraw_half() entered monitor, len(self._notFull) ="+str(len(self._notFull))+", self._capacity="+str(self._capacity))
        logger.trace("withdraw_half() entered monitor, len(self._notEmpty) ="+str(len(self._notEmpty))+", self._capacity="+str(self._capacity))
        listOfValues = []
        if self.first:
            self.first = False
            batch_size = int(self._fullSlots/2)
            for _ in range(0,batch_size):
                listOfValues.append(self._buffer[self._out])
                self._out=(self._out+1) % int(self._capacity)
                self._fullSlots-=1
        else:
            logger.trace("withdraw_half() self._fullSlots at start of second batch: " + str(self._fullSlots))
            sizeOfBatch = self._fullSlots
            for _ in range(0,sizeOfBatch):
                listOfValues.append(self._buffer[self._out])
                self._out=(self._out+1) % int(self._capacity)
                self._fullSlots-=1
            if (self._fullSlots != 0):
                logger.error("[Error]: Internal Error: BoundedBuffer withdraw_half _fullSlots not 0 after second withdawl: " + str(self._fullSlots))
        restart = False
        #threading.current_thread()._restart = False
        #threading.current_thread()._returnValue=value
        super().exit_monitor()
        return listOfValues, restart

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
    logger.trace("Calling deposit")
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
    logger.trace("Successfully called deposit/deposit_all")

def taskW(b : BoundedBuffer):
    logger.trace("Calling withdraw")
    value = b.withdraw()
    logger.trace("Successfully called withdraw: "+ value[0])
    value = b.withdraw()
    logger.trace("Successfully called withdraw: "+ value[0])
    value = b.withdraw()
    logger.trace("Successfully called withdraw: "+ value[0])
    value = b.withdraw()
    logger.trace("Successfully called withdraw: "+ value[0])
    value = b.withdraw()
    logger.trace("Successfully called withdraw: "+ value[0])
    value = b.withdraw()
    logger.trace("Successfully called withdraw: "+ value[0])
    value = b.withdraw()
    logger.trace("Successfully called withdraw: "+ value[0])

def main():
    b = BoundedBuffer(monitor_name="BoundedBuffer")
    VALUEFOR_N = 2
    keyword_arguments = {}
    keyword_arguments['n'] = VALUEFOR_N
    b.init(**keyword_arguments)
    #b.deposit(value = "A")
    #value = b.withdraw()
    #logger.trace(value)
    #b.deposit(value = "B")
    #value = b.withdraw()
    #logger.trace(value)

    try:
        logger.trace("Starting D thread")
        _thread.start_new_thread(taskD, (b,))
    except Exception as ex:
        logger.trace("[ERROR] Failed to start first thread.")
        logger.trace(ex)

    try:
        logger.trace("Starting first thread")
        _thread.start_new_thread(taskW, (b,))
    except Exception as ex:
        logger.trace("[ERROR] Failed to start first thread.")
        logger.trace(ex)

    logger.trace("Sleeping")
    time.sleep(4)
    logger.trace("Done sleeping")

if __name__=="__main__":
    main()