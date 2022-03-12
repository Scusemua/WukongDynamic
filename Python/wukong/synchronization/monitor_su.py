from imp import release_lock
from multiprocessing import Semaphore, RLock
from counting_semaphore import CountingSemaphore
import queue

class MonitorSU(object):
    def __init__(self, monitor_name = None):
        self.__mutex = CountingSemaphore() 
        self.__reenetry = CountingSemaphore() 
        self.reentry_count = 0 
        self.monitor_name = monitor_name

    def get_name(self):
        return self.monitor_name

    def enter_monitor(self, method_name = None):
        self.__mutex.P() 
    
    def exit_monitor(self):
        if self.reentry_count > 0:
            self.__reenetry.V()
        else:
            self.__mutex.V()
    
class ConditionVariable(object):
    def __init__(self, monitor : MonitorSU, condition_name = None):
        self.__parent_monitor = monitor 
        self.__thread_queue = CountingSemaphore(initial_permits = 0, condition_name = condition_name + ":threadQueue", id = 1)
        self.__num_waiting_threads = 0
        self.__condition_name = condition_name 
    
    def signal_c(self):
        if (self.__num_waiting_threads > 0):
            self.__parent_monitor.reentry_count += 1
            self.__parent_monitor.__reenetry.VP(self.__thread_queue)
            self.__parent_monitor.reentry_count -= 1
    
    def signal_c_and_exit_monitor(self):
        if self.__num_waiting_threads > 0:
            self.__thread_queue.V()
        elif self.__parent_monitor.reentry_count > 0:
            self.__parent_monitor.__reenetry.V()
        else:
            self.__parent_monitor.__mutex.V() 
    
    def wait_c(self):
        self.__num_waiting_threads += 1
        if self.__parent_monitor.reentry_count > 0:
            self.__thread_queue.VP(self.__parent_monitor.__reenetry)
        else:
            self.__thread_queue.VP(self.__parent_monitor.__mutex)
        
        self.__num_waiting_threads -= 1
    
    def empty(self) -> bool:
        return self.__num_waiting_threads == 0
    
    def __len__(self) -> int:
        return self.__num_waiting_threads