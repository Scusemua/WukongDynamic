from re import L
from .monitor_su import MonitorSU, ConditionVariable
import threading

import logging 
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(asctime)s] [%(module)s] [%(processName)s] [%(threadName)s]: %(message)s')

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)

logger.addHandler(ch)
logger.propagate = False

# Reusable Barrier. Clients call wait_b()
class Barrier(MonitorSU):
    def __init__(self, initial_n = 0, monitor_name = None):
        super(Barrier, self).__init__(monitor_name = monitor_name)
        self._n = initial_n
        self._go = self.get_condition_variable(condition_name = "go")
    
    @property
    def n(self):
        return self._n 

    @n.setter
    def n(self, value):
        logger.debug("Setting value of n to " + str(value))
        self._n = value

    def init(self, **kwargs):
        logger.debug(kwargs)
        if kwargs is None or len(kwargs) == 0:
            raise ValueError("Barrier requires a length. No length provided.")
        elif len(kwargs) > 1:
           raise ValueError("Error - Barrier init has too many args.")
        self._n = kwargs['n']

    # This try-method must not block - it passes a condition to is_blocking
    # and returns the result: true if try_wait will execute go.wait(); false otherwiss.
    # Note: A thread may wait behind other threads to enter the monitor here and
    # in wait_b; we are checking only for go.wait_c(), not whether the current thread
    # is next to get the mutex lock (as in a Java try_acquire() on a semaphore).
    def try_wait_b(self, **kwargs):
        # Does mutex.P as usual
        super().enter_monitor(method_name = "try_wait_b")
        
        # super.is_blocking has a side effect which is to make sure that exit_monitor below
        # does not do mutex.V, also that enter_monitor of wait_b that follows does not do mutex.P.
        # This makes executes_wait ; wait_b atomic
        
        block = super().is_blocking(len(self._go) < (self._n - 1))
        
        # Does not do mutex.V, so we will still have the mutex lock when we next call
        # enter_monitor in wait_b
        super().exit_monitor()
        
        return block

	# synchronous try version of withdraw, restart when block
    def wait_b(self, **kwargs):
        # if we called executes_wait first, we still have the mutex so this enter_monitor does not do mutex.P
        super().enter_monitor(method_name = "wait_b")
        logger.debug("Entered monitor in wait_b()")
        logger.debug("wait_b() entered monitor. len(self._go) = " + str(len(self._go)) + ", self._n=" + str(self._n))
        if len(self._go) < (self._n - 1):
            logger.debug("Calling _go.wait_c() from Barrier")
            self._go.wait_c()
            restart = True
        else:
            # Tell Synchronizer that this serverless function should not be restarted.
            # Assuming serverless function call to wait_b is 2-way so the function will
            # block until wait_b finishes. In this case we are avoiding restart time for
            # last serverless function to call Barrier.
            # If this were a fan-in instead of Barrier:
            # - functions that are not the last/become function should not be restarted, so
            #   after go.wait() call threading.current_thread()._restart = False. In fact,
            #   can they call exit_monitor instead since we are done with them.
            #   Assuming these serverless functions called isBecome() and got False, so the functions
            #   terminated after getting False returned on 2-way cal to wait_b
            # - The last/become thread can receive the outputs of the other serverless functions
            #   as return object(s) of 2-way cal to wait_b.
            #threading.current_thread()._restart = False
            restart = False
            logger.debug("Last thread in Barrier so not calling self._go.wait_c")

        logger.debug("Client exiting Barrier wait_b")
        # does mutex.V
        self._go.signal_c_and_exit_monitor()
        #threading.current_thread()._returnValue = 1
        return 0, restart
        
        #No logger.debugs here. main Client can exit while other threads are
        #doing this logger.debug so main thread/interpreter can't get stdout lock?
        
	# synchronous no-try version of withdraw, restart when block
    def wait_b_for_no_try(self, **kwargs):
        # if we called executes_wait first, we still have the mutex so this enter_monitor does not do mutex.P
        super().enter_monitor(method_name = "wait_b")
        logger.debug("Entered monitor in wait_b()")
        logger.debug("wait_b() entered monitor. len(self._go) = " + str(len(self._go)) + ", self._n=" + str(self._n))
        if len(self._go) < (self._n - 1):
            logger.debug("Calling _go.wait_c() from Barrier")
            self._go.wait_c()
        else:
            # Tell Synchronizer that this serverless function should not be restarted.
            # Assuming serverless function call to wait_b is 2-way so the function will
            # block until wait_b finishes. In this case we are avoiding restart time for
            # last serverless function to call Barrier.
            # If this were a fan-in instead of Barrier:
            # - functions that are not the last/become function should not be restarted, so
            #   after go.wait() call threading.current_thread()._restart = False. In fact,
            #   can they call exit_monitor instead since we are done with them.
            #   Assuming these serverless functions called isBecome() and got False, so the functions
            #   terminated after getting False returned on 2-way cal to wait_b
            # - The last/become thread can receive the outputs of the other serverless functions
            #   as return object(s) of 2-way cal to wait_b.
            #threading.current_thread()._restart = False
            logger.debug("Last thread in Barrier so not calling self._go.wait_c")
        restart = False
        logger.debug("Exiting Barrier wait_b")
        # does mutex.V
        self._go.signal_c_and_exit_monitor()
        #threading.current_thread()._returnValue = 1
        return 0, restart
        
        #No logger.debugs here. main Client can exit while other threads are
        #doing this logger.debug so main thread/interpreter can't get stdout lock?
        
        
        
