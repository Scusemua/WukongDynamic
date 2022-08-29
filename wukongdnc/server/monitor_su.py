from .counting_semaphore import CountingSemaphore
from .util import isTry_and_getMethodName
import queue

# Hoare-style Monitor class with methods enter_monitor, exit_monitor, and ConditionVariable methods wait_c and signal_c
class MonitorSU(object):
    def __init__(self, monitor_name = None):
        self._mutex = CountingSemaphore(initial_permits = 1, semaphore_name = "Monitor-" + str(monitor_name) + "-_mutex-CountingSemaphore") 
        self._reentry = CountingSemaphore(initial_permits = 0,  semaphore_name = "Monitor-" + str(monitor_name) + "-_reentry-CountingSemaphore")
        self._exited = CountingSemaphore(initial_permits = 0, semaphore_name = "Monitor-" + str(monitor_name) + "-_exited-CountingSemaphore")

        self._reentry_count = Integer(0)
        self._monitor_name = monitor_name

        self._doingTry = False

    def get_name(self):
        return self._monitor_name


    def enter_monitor(self, method_name = None):
        # assert method_name starts with "try_" implies not self._doingTry
        base_name_ignored, isTryMethod = isTry_and_getMethodName(method_name)
        if isTryMethod:
            # try methods always need to lock monitor 
            self._mutex.P()
            self._doingTry = True # tell exit_monitor to keep the muutex_lock for foo
            return
        elif not self._doingTry:
            # not a try-method and not in the middle of doing (atomically) [try_foo(); foo()] so we need to lock mutex.
            self._mutex.P()
            return
        else:
            #we are entering foo after doing try_foo; we currently have the mutex lock; tell exit_monitor to do a normal exit
            self._doingTry = False
            return

    def exit_monitor(self):
        if not self._doingTry:  # normal exit
            # this is exit_monitor for foo -  so we need to do a normal exit
            if self._reentry_count > 0:
                self._reentry.V()
            else:
                self._mutex.V()
        
        # Cannot leave `exit_monitor()` without calling .release().
        # The logic is that you cannot call the next `wait_b()` until the previous one is done.
        # Basically, we are just waiting for `exit()` to say that it is done.
        self._exited.release() 
        return 
    # else this is exit_monitor for try_foo, so keep the mutex lock for foo's enter_monitor, i..e., do nothing.

    def is_blocking(self, condition):
        #assert self._doingTry

        # called by try_foo(). Setting _doingTry to true ensures
        # no attempt to do mutex.P when exit_monitor of try_foo
        # or enter_monitor of foo(). Enter_monitor of foo will
        # set _doingTry to false so exit_monitor of foo() will
        # execute mutex.V
        # self._doingTry = True
        return condition

    # Note: Passing self to the ConditionVarable as the parent_monitor should work since all members are named with a single underscore instead of a
    # double underscore. Double underscores cause member names to be mangled (by prefixing with the class name).
    def get_condition_variable(self, condition_name = "Condition"):
        return ConditionVariable(mutex = self._mutex, reentry = self._reentry, reentry_count = self._reentry_count, exited = self._exited, name = condition_name) # 

class Integer:
    def __init__(self, val=0):
        self._val = int(val)
    def get_val(self):
        return self._val
    def set_val(self,val):
        self._val = val
    def inc(self):
        self._val = self._val+1
    def dec(self):
        self._val = self._val-1
    def __eq__(self, other):
        return self._val == other
    def __gt__(self, other):
        return self._val > other
    def __lt__(self, other):
        return self._val < other
    
    
class ConditionVariable(object):

    def __init__(self, mutex = None, reentry = None, reentry_count = None, exited = None, name = None): # 
        self._mutex = mutex
        self._reentry = reentry
        self._reentry_count = reentry_count
        self._thread_queue = CountingSemaphore(initial_permits = 0, semaphore_name = name + ":threadQueue", id = 1)
        self._num_waiting_threads = 0
        self._condition_name = name
        self._exited = exited
    

    def signal_c(self):
        if (self._num_waiting_threads > 0):
            self._reentry_count.inc()
            self._reentry.VP(self._thread_queue)
            self._reentry_count.dec()

    def signal_c_and_exit_monitor(self):
        if self._num_waiting_threads > 0:
            self._thread_queue.V() # Somebody waiting.
        elif self._reentry_count > 0:
            self._reentry.V()     # Somebody trying to get back in.
        else:
            self._mutex.V()        # Somebody new.
        self._exited.release()

    def wait_c(self):
        self._num_waiting_threads += 1
        #print("MonitorSU.wait_c() called. _num_waiting_threads=" + str(self._num_waiting_threads))
        if self._reentry_count > 0:
            self._thread_queue.VP(self._reentry)
        else:
            #print("blocking. type(self._parent_monitor) = " + str(type(self._parent_monitor)))
            #print(str(self._parent_monitor.__dict__))
            self._thread_queue.VP(self._mutex)
        self._num_waiting_threads -= 1
    
    def empty(self) -> bool:
        return self._num_waiting_threads == 0
    
    def __len__(self) -> int:
        #print("Returning length of ConVar: " + str(self._num_waiting_threads))
        return self._num_waiting_threads