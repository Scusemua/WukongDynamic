#from monitor_su import MonitorSU
from .selectivewait import selectiveWait
from .counting_semaphore import CountingSemaphore

import logging 
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(asctime)s] [%(threadName)s] %(levelname)s: %(message)s')

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)

logger.addHandler(ch)

class Selector():
    def __init__(self, selector_name = None):
        #super(Selector, self).__init__(monitor_name=monitor_name)
        self._select = selectiveWait()
        self._entry_map = {}  # map entry_name to entry object
        # initialized by user calling appropriate set_restart_on_block/unblock/noblock method
        # If SQS then always restart
        self._restart_on_block = None 
        self._restart_on_unblock = None
        self._restart_on_noblock = None
        if selector_name == None:
            selector_name == ""
        self._mutex = CountingSemaphore(initial_permits = 1, semaphore_name = "Selector-" + str(selector_name) + "-mutex-CountingSemaphore") 

    def is_blocking(self,condition):
        # called by try_foo()
        return condition
        
    def lock(self):
        self._mutex.P()
        
    def unlock(self):
        self._mutex.V()

    def add_entry(self,entry):
        self._select.add_entry(entry) 
        # can get the name from entry and add [name,entry] to entry_map, if needed, or vice versa
        self._entry_map[entry._entry_name] = entry
        
    def get_entry(self,i):
        entry = self._select._entry_list[i]
        return entry

    # Could call [try-op; addArrival (no lock since it's private); execute]?
    # Q: Don't want to pass anything to choose(), or why not since user is not messing with
    # execute/choose in parent class.
             
    def execute(self, entry_name, synchronizer, synchronizer_method, result_buffer, **kwargs):
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
        called_entry.add_arrival(entry_name, synchronizer, synchronizer_method, result_buffer, **kwargs)
            
        self.set_guards();
            
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
            logger.debug("execute returning: called_entry.get_num_arrivals(): " + str(called_entry.get_num_arrivals())
            + " called_entry.testGuard() == False: " + str(called_entry.testGuard() == False))
            return 0
			
        else:
            return_value = self.domethodcall(entry_name, synchronizer, synchronizer_method, **kwargs)
            # ToDo: remove the arrival or whatever choice() does 
            restart = self.get_restart_on_noblock()
            return_tuple = (return_value, restart)
            result_buffer.deposit(return_tuple)
            called_entry.remove_first_arrival()
            logger.debug("execute called " + entry_name + ". returning")
          
        logger.debug("execute: choosing")
        while(True):
           # just did entry_name so update guards
           self.set_guards();
           choice = self._select.choose()
           if choice >= 1 and choice <= self._select.get_number_entries():
                logger.debug("Execute: choice is " + str(choice) + " so use list entry " + str(choice-1))
                called_entry = self.get_entry(choice-1)
                arrival = called_entry.getOldestArrival()
                synchronizer = arrival._synchronizer;
                synchronizer_method = arrival._synchronizer_method
                kwargs = arrival._kwargs
                result_buffer = arrival._result_buffer
                return_value = self.domethodcall(entry_name, synchronizer, synchronizer_method, **kwargs)
                logger.debug("Execute: called chosen method " + arrival._entry_name)
                restart = self.get_restart_on_unblock()
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
        logger.debug("execute: return 0")
        return 0
        
    # For example, call withdraw and return the value withdrawn.
    def domethodcall(self, entry_name, synchronizer, synchronizer_method, **kwargs):
        try:
           return_value = synchronizer_method(synchronizer, **kwargs)
        except Exception as x:
            logger.error("Caught MonitorSelect domethodcall Error >>> %s" % x)
            raise ValueError("Error calling method " + entry_name + " in domethodcall of MonitorSelect")  
        return return_value
       
    def set_restart_on_block(self,T_or_F):
        self._restart_on_block = T_or_F
       
    def get_restart_on_block(self):
        return self._restart_on_block
            
    def set_restart_on_unblock(self,T_or_F):
        self._restart_on_unblock = T_or_F
       
    def get_restart_on_unblock(self):
        return self._restart_on_unblock
            
    def set_restart_on_noblock(self,T_or_F):
        self._restart_on_noblock = T_or_F
   
    def get_restart_on_noblock(self):
        return self._restart_on_noblock     
