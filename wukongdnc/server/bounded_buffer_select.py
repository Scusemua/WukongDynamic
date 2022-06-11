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

class BoundedBuffer_Select(Selector):
    def __init__(self, selector_name = None):
        super(BoundedBuffer_Select, self).__init__(selector_name=selector_name)
        
    def init(self, **kwargs):
        logger.debug(kwargs)
        
        self._capacity = kwargs["n"]
        self._fullSlots=0
        self._buffer=[]
        self._in=0
        self._out=0
        
        self._deposit = selectableEntry("deposit")
        self._withdraw = selectableEntry("withdraw")

        # superclass method calls
        self.add_entry(self._deposit)     # alternative 1
        self.add_entry(self._withdraw)    # alternative 2
        
        #self.set_restart_on_block(False)
        self.set_restart_on_block(True)
        self.set_restart_on_noblock(False)
        self.set_restart_on_unblock(True)

    def try_deposit(self,**kwargs):
        # Does try_op protocol for acquiring/releasing lock
        
        #self.enter_monitor(method_name = "try_deposit")
        
        # super.is_blocking has a side effect which is to make sure that exit_monitor below
        # does not do mutex.V, also that enter_monitor of wait_b that follows does not do mutex.P.
        # This males try_wait_b ; wait_b atomic
        
        #block = super().is_blocking(self._fullSlots == self._capacity)
        block = self.is_blocking(self._fullSlots == self._capacity)
        
        # Does not do mutex.V, so we will still have the mutex lock when we next call
        # enter_monitor in wait_b
        
        #self.exit_monitor()
        
        return block

        
    def try_withdraw(self,**kwargs):
        # Does try_op protocol for acquiring/releasing lock
        
        #self.enter_monitor(method_name = "try_withdraw")
        
        # super.is_blocking has a side effect which is to make sure that exit_monitor below
        # does not do mutex.V, also that enter_monitor of wait_b that follows does not do mutex.P.
        # This males try_wait_b ; wait_b atomic
        
        #block = super().is_blocking(self._fullSlots == 0)
        block = self.is_blocking(self._fullSlots == 0)
        
        # Does not do mutex.V, so we will still have the mutex lock when we next call
        # enter_monitor in wait_b
        
        #self.exit_monitor()
        
        return block
    
    def set_guards(self):
        self._withdraw.guard(self._fullSlots > 0)
        self._deposit.guard (self._fullSlots < self._capacity)
       
    def deposit(self,**kwargs):
        value = kwargs['value']
        #value = self._deposit.accept()  
        self._buffer.insert(self._in,value)
        self._in=(self._in+1) % int(self._capacity)
        self._fullSlots+=1
        return 0
            
    def withdraw(self,**kwargs):
        # get the sent value from _withdraw
        #self._withdraw.accept()
        value = self._buffer[self._out]
        self._out = (self._out+1) % int(self._capacity)
        self._fullSlots -= 1
        return value
