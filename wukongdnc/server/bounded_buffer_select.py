from ..constants import SERVERLESS_SYNC

if SERVERLESS_SYNC:
    from .selector_lambda import Selector
else:
    from .selector import Selector
from .selectableEntry import selectableEntry

import logging 

logger = logging.getLogger(__name__)
"""
logger.setLevel(logging.ERROR)
formatter = logging.Formatter('[%(asctime)s] [%(module)s] [%(processName)s] [%(threadName)s]: %(message)s')
ch = logging.StreamHandler()
ch.setLevel(logging.ERROR)
ch.setFormatter(formatter)
logger.addHandler(ch)
"""

class BoundedBuffer_Select(Selector):
    def __init__(self, selector_name = "BoundedBuffer_Select"):
        super(BoundedBuffer_Select, self).__init__(selector_name=selector_name)
        
    def init(self, **kwargs):
        logger.trace(kwargs)
        
        self._capacity = kwargs["n"]
        # Need there to be an element at buffer[i]. Cannot use insert() since it will shift elements down.
        # If we set buffer[0] we need an element to be at position 0 or we get an out of range error.
        #self._buffer=[]
        self._buffer= [None] * self._capacity
        self._fullSlots=0
        self._in=0
        self._out=0
        
        self._deposit = selectableEntry("deposit")
        self._withdraw = selectableEntry("withdraw")
        self._deposit_all = selectableEntry("deposit_all")

        self._deposit.set_restart_on_block(True)
        self._deposit.set_restart_on_noblock(True) 
        self._deposit.set_restart_on_unblock(True)
        self._withdraw.set_restart_on_block(True)
        self._withdraw.set_restart_on_noblock(True) 
        self._withdraw.set_restart_on_unblock(True)
        self._deposit_all.set_restart_on_block(True)
        self._deposit_all.set_restart_on_noblock(True) 
        self._deposit_all.set_restart_on_unblock(True)

        # superclass method calls
        self.add_entry(self._deposit)     # alternative 1
        self.add_entry(self._withdraw)    # alternative 2
        self.add_entry(self._deposit_all) # alternative 3

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

    # We assume the buffer is unbonded for try_deposit_all. See the comemnts below in 
    # method deposit_all()
    def try_deposit_all(self,**kwargs):
        # Does try_op protocol for acquiring/releasing lock
        
        #self.enter_monitor(method_name = "try_deposit")
        
        # super.is_blocking has a side effect which is to make sure that exit_monitor below
        # does not do mutex.V, also that enter_monitor of wait_b that follows does not do mutex.P.
        # This males try_wait_b ; wait_b atomic
        
        block = self.is_blocking(False)
        
        # Does not do mutex.V, so we will still have the mutex lock when we next call
        # enter_monitor in wait_b
        
        #self.exit_monitor()
        
        return block
    
    def set_guards(self):
        self._withdraw.guard(self._fullSlots > 0)
        self._deposit.guard (self._fullSlots < self._capacity)
        # We assume the buffer is unbonded for try_deposit_all. See the comemnts below in 
        # method deposit_all()
        self._deposit_all.guard (True)
       
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

    # This deposits all values in list_of_values.
    # Important:
    #  This is assuming an unbounded buffer since it does not check
    #  for buffer full (self._fullSlots == self._capacity) after inserting each value.
    #  A slower alternative is to have the caller deosit a single item at a time.
    #  Another alternative is to create an UnboundedBuffer_Select class. User can make
    #  bounded buffer practically unbounded by setting capacity to e.g., max int. For the
    #  DAG engine, we set the work queue buffer capacity to 2*numberoftasks, which is 
    #  effectively makes the work queue unbounded (since we deposit <= numberoftasks.)
    def deposit_all(self, **kwargs):
        list_of_values = kwargs["list_of_values"]
        for value in list_of_values:
            self._buffer.insert(self._in,value)
            self._in=(self._in+1) % int(self._capacity)
            self._fullSlots+=1
        return 0
