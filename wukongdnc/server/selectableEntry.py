from collections import deque
from .arrival import Arrival
import threading

import logging 
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(asctime)s] [%(module)s] [%(processName)s] [%(threadName)s]: %(message)s')

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)

logger.addHandler(ch)

class selectableEntry:
# these methods are not locked since only one thread at a time can access them

	# global (static) timestamp for arrivals: 0, 1, 2, 3, ...
    _global_timestamp = 0
    _lockForTimeStamp = threading.RLock()

    def __init__(self, entry_name):
        self._entry_name = entry_name
        self._open = False  
        self._ready = 0
        self._guard = True
        self._arrivals = deque()
        self._restart_on_block = True #None 
        self._restart_on_unblock = True
        self._restart_on_noblock = True #None        
		# Ops on deque:
        # _arrivals.popleft()
        #  front = _arrivals[0]
        # _arrivals.append(A)
        # _arrivals.index(element,start.finish) return index search betwenn start to finish, raise ValueError if not found
	
    def getTimeStamp(self):
        with selectableEntry._lockForTimeStamp:
            ts = selectableEntry._global_timestamp
            selectableEntry._global_timestamp +=  1
            return ts
          
    def add_arrival(self, entry_name, synchronizer, synchronizer_method, result_buffer, state, **kwargs):
        self._ready += 1
        ts = self.getTimeStamp()
        entry_arrival = Arrival(entry_name, synchronizer, synchronizer_method, result_buffer, ts, state, **kwargs)
        self._arrivals.append(entry_arrival)
		
    def remove_first_arrival(self):
        self.decReady()
        self._arrivals.popleft()

    def getOldestArrival(self):
        #only called from choose() when ready>0, i.e., arrivals.size()>0
		#return (((Long)arrivals.firstElement()).longValue());
        return self._arrivals[0]
        
    def get_entry_name(self):
        return self._entry_name

    def get_num_arrivals(self):
        return len(self._arrivals)

    def count(self):
        return self._ready

    def testReady(self):
        return self._ready>0

    def decReady(self):
        self._ready -= 1

    def setOpen(self):
        self._open = True

    def clear_open(self):
        self._open = False

    def guard(self,g):
        self._guard = g
    
    def testGuard(self):
        return self._guard

    def set_restart_on_block(self,T_or_F):
        self._restart_on_block = T_or_F
       
    def get_restart_on_block(self):
        return self._restart_on_block
            
    def set_restart_on_unblock(self,T_or_F):
        self._restart_on_unblock = T_or_F
       
    def get_restart_on_unblock(self):
        return self._restart_on_unblock
            
    def set_restart_on_noblock(self,T_or_F):
        logger.trace("Setting value of '_restart_on_noblock' to " + str(T_or_F))
        self._restart_on_noblock = T_or_F
   
    def get_restart_on_noblock(self):
        return self._restart_on_noblock     