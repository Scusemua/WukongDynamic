from counting_semaphore import CountingSemaphore

class MonitorSU(object):
    def __init__(self, monitor_name = None):
        self.__mutex = CountingSemaphore(initial_permits = 1, semaphore_name = "Monitor-" + str(monitor_name) + "-__mutex-CountingSemaphore") 
        self.__reenetry = CountingSemaphore(initial_permits = 0,  semaphore_name = "Monitor-" + str(monitor_name) + "-__reentry-CountingSemaphore") 
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
    def __init__(self, monitor = None, condition_name = None):
        self.__parent_monitor = monitor 
        self.__thread_queue = CountingSemaphore(initial_permits = 0, semaphore_name = condition_name + ":threadQueue", id = 1)
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
        print("MonitorSU.wait_c() called. __num_waiting_threads=" + str(self.__num_waiting_threads))
        if self.__parent_monitor.reentry_count > 0:
            self.__thread_queue.VP(self.__parent_monitor.__reenetry)
        else:
            print("blocking. type(self.__parent_monitor) = " + str(type(self.__parent_monitor)))
            print(str(self.__parent_monitor.__dict__))
            self.__thread_queue.VP(self.__parent_monitor._MonitorSU__mutex)
        
        self.__num_waiting_threads -= 1
    
    def empty(self) -> bool:
        return self.__num_waiting_threads == 0
    
    def __len__(self) -> int:
        #print("Returning length of ConVar: " + str(self.__num_waiting_threads))
        return self.__num_waiting_threads