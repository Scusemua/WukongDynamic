#from monitor_su import MonitorSU
from .selectivewait import selectiveWait
# from .counting_semaphore import CountingSemaphore
from threading import RLock

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

class Selector():
    def __init__(self, selector_name = None):
        #super(Selector, self).__init__(monitor_name=monitor_name)
        self._select = selectiveWait()
        self._entry_map = {}  # map entry_name to entry object
        # initialized by user calling appropriate set_restart_on_block/unblock/noblock method
        # If SQS then always restart
        # self._restart_on_block = True #None 
        # self._restart_on_unblock = True
        # self._restart_on_noblock = True #None
        if selector_name == None:
            selector_name == ""
        self._mutex = RLock() #self._mutex = CountingSemaphore(initial_permits = 1, semaphore_name = "Selector-" + str(selector_name) + "-mutex-CountingSemaphore") 

    def is_blocking(self,condition):
        # called by try_foo()
        return condition
        
    def lock(self):
        self._mutex.acquire() # self._mutex.P()
        
    def unlock(self):
        self._mutex.release() # self._mutex.V()

    def add_entry(self,entry):
        self._select.add_entry(entry) 
        # can get the name from entry and add [name,entry] to entry_map, if needed, or vice versa
        self._entry_map[entry._entry_name] = entry
        
    def get_entry(self,i):
        entry = self._select._entry_list[i]
        return entry

    def get_num_entries(self):
        return self._select.get_number_entries()

    def set_guards(self):
        raise ValueError("set_guards in parent class Selector should not be called.")

    # Could call [try-op; addArrival (no lock since it's private); execute]?
    # Q: Don't want to pass anything to choose(), or why not since user is not messing with
    # execute/choose in parent class.
             
    #def execute(self, entry_name, synchronizer, synchronizer_method, result_buffer, **kwargs):
    def execute(self, entry_name, synchronizer, synchronizer_method, result_buffer, state, send_result, **kwargs):
        # state will be None if running the non-Lambda version, as state need not be saved in Arrival. 
            
        # guards could be using count (of arrivals) atribute so add arrrival first

        # The tcp_server thread that called synchronizeSelect() is blocked on result_buffer.withdraw; thus, a deposit
        # must always be done. As mentioned above, this return value may be ignored, but we need to unblock the
        # tcp_server thread in any case. In the case where an asynch call w/ terminate blocks, which is handled below,
        # the calling lambda will terminate, get_restart_on_unblock() will be true, so we will restart and we will
        # deposit the result, which will be ignored.

        # Arrivals are timestamped with a global static incremented integer
        called_entry = self._entry_map[entry_name]
        called_entry.add_arrival(entry_name, synchronizer, synchronizer_method, result_buffer, state, **kwargs)
        
        #Debug
        num_entries = self.get_num_entries()
        logger.trace("num_entries: "+ str(num_entries))
        i = 0
        while i < num_entries:
            entry = self.get_entry(i)
            logger.trace("execute: call to: entry " + str(i) + " is " + str(entry.get_entry_name()) + ", number of arrivals: " + str(entry.get_num_arrivals()))
            i += i+1

        #entry0 = self.get_entry(0)
        #logger.trace("after add: entry " + entry0.get_entry_name() + ": " + str(entry0.get_num_arrivals()))
        #entry1 = self.get_entry(1)
        #logger.trace("after add: entry " + entry1.get_entry_name() + ": " + str(entry1.get_num_arrivals()))

        # this is a method of the synchronizer objct, e.g., BoundedBufferSelect
        self.set_guards()
            
        # number of arrivals is at least 1 since we just added one. Is this now the only arrival?
        # If so: then check its guard to see if it should be accepted. Note: all other entrie must have
        # false guards otherwise they would have been selected on a previous execution of execute().
        # If not: then its guard cannot be true otherwise it would have been selected on a previous execution 
        # of execute(). This is asserted.
        if called_entry.get_num_arrivals() > 1 or called_entry.testGuard() == False:
            # assert: 
            if (called_entry.get_num_arrivals() > 1) and called_entry.testGuard():
                logger.trace("execute: called_entry.testGuard() is True but this is not the first arrival."
                + " A previous arrival thus had a True guard and should have been selected earlier.")
            
            # Q return what? return 0 for now. Eventually the value may be part of delay alternative processing, which is TBD.
            logger.trace("execute returning: called_entry.get_num_arrivals(): " + str(called_entry.get_num_arrivals())
                + " called_entry.testGuard() == False: " + str(called_entry.testGuard() == False))
            return 0 
        else:
            return_value = self.domethodcall(entry_name, synchronizer, synchronizer_method, **kwargs)
            # restart is only true if this is an asynch call after which the caller always terminates, blocking call or not.
            restart = called_entry.get_restart_on_noblock() # restart = self._restart_on_noblock
            logger.trace("Value of 'called_entry.get_restart_on_noblock()' in execute() [line 106]: " + str(restart))
            return_tuple = (return_value, restart)
            # return value is deposited into a bounded buffer for withdraw by the tcp_server thread that
            # is handling the client lambda's call. This value will be ignored for all asynch calls and for
            # try-ops that blocked. If a restart is done, which is the case for a try-op that blocked and an async call
            # that always terminates, the client will receive the return value upon restarting. In synchronizeSelect,
            # we do result = result_buffer.withdraw() followed by if restart: ... state.return_value = returnValue ...
            result_buffer.deposit(return_tuple)
            called_entry.remove_first_arrival()
            logger.trace("execute called " + entry_name)
        
        logger.trace("execute: choosing")
        
        # Debug
        num_entries = self.get_num_entries()
        for i in range(0, (num_entries-1)):
            entry = self.get_entry(i)
            logger.trace("execute: choosing: entry " + str(i) + " is " + entry.get_entry_name() + ", number of arrivals: " + str(entry.get_num_arrivals()))

        #entry0 = self.get_entry(0)
        #logger.trace("choosing: entry " + entry0.get_entry_name() + ": " + str(entry0.get_num_arrivals()))
        #entry1 = self.get_entry(1)
        #logger.trace("choosing: entry " + entry1.get_entry_name() + ": " + str(entry1.get_num_arrivals()))      
       
        while(True):
            # just added an Arrival for an entry so update guards (which may consider the number of arrivals for an entry)
            self.set_guards()
            choice = self._select.choose()
            # entries start at number 0; the value 0 indicates something else (see below)
            if choice >= 1 and choice <= self._select.get_number_entries():
                logger.trace("Execute: choice is " + str(choice) + " so use list entry " + str(choice-1))
                called_entry = self.get_entry(choice-1)
                # entry information was stored in the Arrival for the selected entry
                arrival = called_entry.getOldestArrival()
                # make entry call
                synchronizer = arrival._synchronizer
                synchronizer_method = arrival._synchronizer_method
                kwargs = arrival._kwargs
                result_buffer = arrival._result_buffer
                return_value = self.domethodcall(entry_name, synchronizer, synchronizer_method, **kwargs)
                logger.trace("Execute: called chosen method " + arrival._entry_name)
                # if restart is True, the a restart of client Lambda will be done when execute() returns to synchronizeSelect
                restart = called_entry.get_restart_on_unblock() # restart = self.get_restart_on_unblock()
                # return value is deposited into a bounded buffer for withdraw by the tcp_server thread that
                # is handling the client lambda's call. This value will be ignored for all asynch calls and for
                # try-ops that blocked. If a restart is done, the client will receive the return value upon restarting.
                return_tuple = (return_value, restart)
                result_buffer.deposit(return_tuple)
                # remove entry if done with it (result_buffer, etc)
                called_entry.remove_first_arrival()
                break  # while-loop
            elif choice > self._select.get_number_entries()+1:
                # error
                print("Illegal choice in selective wait: " + choice + " number of entries" + self._select.get_number_entries())
                break # while-loop
            elif choice == -1: # else or delay processing TBD
            # ToDo: on timeout we are just calling "delay" method? so no choice of delay/else
                # if hasElse:                   
                    # else part
                # if hasDelay:
                    # delay part
                # no one called so no return to caller but may set timeout for delay
                break # while-loop
            elif choice == 0:    # currently we assume at least one True guard
                #something like throw select_exception(...)
                break # while-loop
        logger.trace("execute: completed normally: return 0")
        return 0

    def execute_old(self, entry_name, synchronizer, synchronizer_method, result_buffer, **kwargs):
        # many multitthreaded server threads can be calling so get mutual exclusion 
        # Using try_op/no-try-op protocol for acquiring/releasing lock

        #super().enter_monitor("execute")
            
        # guards could be using count (of arrivals) atribute so add arrrival first
        #index_of_entry_name, called_entry = index_of(entry_name,self._select)
        # Save arrival information that we need to process the entry call. The
        # sent value is in kwargs, the entry_name may be useful for debugging.
        # the result_buffer is used to return the return_value of the entry,
        # but we need the result_buffer after entry is processed. We also 
        # need to call the entry which means we need the synchronizer and
        # synchronizer_method. We have these things here, but in the while-loop
        # below we'll need to get the saved values from the arrival information,
        # i.e., if we choose() entry 1, get first arrival of this entry and 
        # grab alll the info we need. Don't remove entry until we are done
        # with it.
        # Arrivals are timestamped with global static
        called_entry = self._entry_map[entry_name]

        # WARNING: If we use this, it will cause an error because this now has a `state` argument before **kwargs.
        called_entry.add_arrival(entry_name, synchronizer, synchronizer_method, result_buffer, **kwargs)
            
        entry0 = self.get_entry(0)
        logger.trace("after add: entry " + entry0.get_entry_name() + ": " + str(entry0.get_num_arrivals()))
        entry1 = self.get_entry(1)
        logger.trace("after add: entry " + entry1.get_entry_name() + ": " + str(entry1.get_num_arrivals()))

        self.set_guards()
            
        # number of arrivals is at least 1 since we just added one. Is this the only arrival?
        if called_entry.get_num_arrivals() > 1 or called_entry.testGuard() == False:
            # This is not the first wating arrival; guard cannot be true or the 
            # earlier arrivals would have been chosen on previous call to
            # execute and then processed.
            
            # ToDo: this is effectively wait(), so nothing to return now.
            # If this is fan-in then no need to wait since done with caller - they
            # did try-wait_b assume and got "Block" so they terminated and are done.
            # if not, then they did a no-try synch with no restart so we will be
            # sending back a value to indicate they were not last (False,Foo)
            
            #super().exit_monitor()
            
            # But to be like wait() we need to keep the serverthread blocked assuming
            # it's doing a synch and waiting for wait_b to complete. But need to exit
            # execute and exit_monitor so other callers can enter execute? So don't
            # do result_buffer.deposit and this will keep serverthread blocked. And 
            # since serverthread can do synchronizerThreadSelect join or not it is just 
            # eventually blocked on withdraw, and SynchronnizerThreadSelect can just die 
            # silently since we don't need it anymore.
            
            # return what? Part of delay alternative processing?
            logger.trace("execute returning: called_entry.get_num_arrivals(): " + str(called_entry.get_num_arrivals())
                + " called_entry.testGuard() == False: " + str(called_entry.testGuard() == False))
            return 0
            
        else:
            return_value = self.domethodcall(entry_name, synchronizer, synchronizer_method, **kwargs)
            # ToDo: remove the arrival or whatever choice() does 
            restart = called_entry.get_restart_on_noblock() # self._restart_on_noblock
            logger.trace("Value of 'called_entry.get_restart_on_noblock()' in execute() [line 235]: " + str(restart))
            return_tuple = (return_value, restart)
            result_buffer.deposit(return_tuple)
            called_entry.remove_first_arrival()
            logger.trace("execute called " + entry_name + ". returning")
        
        logger.trace("execute: choosing")
        entry0 = self.get_entry(0)
        logger.trace("choosing: entry " + entry0.get_entry_name() + ": " + str(entry0.get_num_arrivals()))
        entry1 = self.get_entry(1)
        logger.trace("choosing: entry " + entry1.get_entry_name() + ": " + str(entry1.get_num_arrivals()))        
        while(True):
           # just did entry_name so update guards
           self.set_guards()
           choice = self._select.choose()
           if choice >= 1 and choice <= self._select.get_number_entries():
                logger.trace("Execute: choice is " + str(choice) + " so use list entry " + str(choice-1))
                called_entry = self.get_entry(choice-1)
                arrival = called_entry.getOldestArrival()
                synchronizer = arrival._synchronizer
                synchronizer_method = arrival._synchronizer_method
                kwargs = arrival._kwargs
                result_buffer = arrival._result_buffer
                return_value = self.domethodcall(entry_name, synchronizer, synchronizer_method, **kwargs)
                logger.trace("Execute: called chosen method " + arrival._entry_name)
                restart = called_entry.get_restart_on_unblock() #self._restart_on_unblock
                return_tuple = (return_value, restart)
                result_buffer.deposit(return_tuple)
                # remove entry if done with it (result_buffer, etc)
                called_entry.remove_first_arrival()
                break  # while-loop
           elif choice > self._select.get_number_entries()+1:
                # error
                print("Illegal choice in selective wait: " + choice + " number of entries" + self._select.get_number_entries())
                break # while-loop
           elif choice == -1: # else or delay
            # ToDo: on timeout we are just calling "delay" method so no choice of delay/else
                # if hasElse:
                    
                    # else part
                # if hasDelay:
                   
                    # delay part
                # no one called so no return to caller but may set timeout for delay
                break # while-loop
           elif choice == 0:
                #something like throw select_exception(...)
                break # while-loop
                
        #super().exit_monitor()
        logger.trace("execute: return 0")
        return 0
        
    # For example, call withdraw and return the value withdrawn.
    def domethodcall(self, entry_name, synchronizer, synchronizer_method, **kwargs):
        try:
           return_value = synchronizer_method(synchronizer, **kwargs)
        except Exception as x:
            logger.exception("Caught MonitorSelect domethodcall Error >>> %s" % x)
            raise ValueError("Error calling method " + entry_name + " in domethodcall of MonitorSelect")  
        return return_value
       
    # def set_restart_on_block(self,T_or_F):
    #     self._restart_on_block = T_or_F
       
    # def get_restart_on_block(self):
    #     return self._restart_on_block
            
    # def set_restart_on_unblock(self,T_or_F):
    #     self._restart_on_unblock = T_or_F
       
    # def get_restart_on_unblock(self):
    #     return self._restart_on_unblock
            
    # def set_restart_on_noblock(self,T_or_F):
    #     logger.trace("Setting value of '_restart_on_noblock' to " + str(T_or_F))
    #     self._restart_on_noblock = T_or_F
   
    # def get_restart_on_noblock(self):
    #     return self._restart_on_noblock     
