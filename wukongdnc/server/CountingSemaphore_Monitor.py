#from re import L
from .monitor_su import MonitorSU #, ConditionVariable
#import threading
import _thread
import time

import logging 

logger = logging.getLogger(__name__)
logger.setLevel(logging.ERROR)
formatter = logging.Formatter('[%(asctime)s] [%(module)s] [%(processName)s] [%(threadName)s]: %(message)s')
ch = logging.StreamHandler()
ch.setLevel(logging.ERROR)
ch.setFormatter(formatter)
logger.addHandler(ch)

#Monitor implementation of a counting semaphore with operations P and V
class CountingSemaphore_Monitor(MonitorSU):
    def __init__(self, monitor_name = "CountingSemaphore_Monitor"):
        super(CountingSemaphore_Monitor, self).__init__(monitor_name = monitor_name)
        #self._permits = initial_permits

    def init(self, **kwargs):     # delete initial_permits parameter
        logger.trace(kwargs)
        if kwargs is None or len(kwargs) == 0:
            raise ValueError("CountingSemaphore_Monitor requires a length > 0. No kwargs provided.")

        elif len(kwargs) > 2:
            raise ValueError("Error - CountingSemaphore_Monitor init has too many kwargs args. kwargs: " + str(kwargs))
        self._permits= kwargs['initial_permits']
        self._permitAvailable = super().get_condition_variable(condition_name = "permitAvailable")

    def try_P(self, **kwargs):
        super().enter_monitor(method_name = "try_P")
        decremented_permits = self._permits - 1
        block = super().is_blocking(decremented_permits < 0)
        super().exit_monitor()
        return block 

	# synchronous try version of P, restart if block; no meaningful return value expected by client
    def P(self, **kwargs):
        super().enter_monitor(method_name = "P")
        logger.trace("CountingSemaphore_Monitor P() entered monitor, len(self._notEmpty) = " + str(len(self._permitAvailable)) + ", permits = " + str(self._permits))
        self._permits -= 1
        if self._permits < 0:
            self._permitAvailable.wait_c()
            restart = True
            #threading.current_thread()._restart = False
            #threading.current_thread()._returnValue = 1
            super().exit_monitor()
            return 1, restart           # Note: return 1 when restart and 0 when no restart
        else:
            restart = False
            #threading.current_thread()._restart = False
            #threading.current_thread()._returnValue = 0
            super().exit_monitor()
            return 0, restart
			
    # synchronous no-try version of P; no meaningful return value expected by client
    # Change name to "P" if no-try version of P is to be used.
    def P_no_try(self, **kwargs):
        super().enter_monitor(method_name = "P")
        self._permits -= 1
        if self._permits < 0:
            self._permitAvailable.wait_c()
        restart = False
        super().exit_monitor()
        return 0, restart
        
    # asynchronous version of P; client always terminates; no meaningful return value expected by client 
    # Change name to "P" if no-try version of P is to be used.
    def asynch_P_terminate(self, **kwargs):
        super().enter_monitor(method_name = "P")
        self._permits -= 1
        if self._permits < 0:
            self._permitAvailable.wait_c()
        restart = True
        super().exit_monitor()
        return 0, restart
		
	# This is the no-try version of V;         
    # V() should never block, so no need for client to terminate and restart, and no meaningful return value expected by client.
    #
	# This can be called as an asynchronous operation or as a synchronous operation. For asynchronous, the client will
	# not block waiting for a reply from the server. This is because the client can assume that V will not block, and V 
    # does not return a meaningful value, so there is no reason to wait for the server's reply. For synchronous, the client can 
    # also assume that V does not block, but the client, for no good reason, will wait for the server to reply with V's return value, 
    # which is an unnecessary delay since V's return value is not meaningful. (V always returns 0.)
	# (This is the same as the asychronous version of V that assumes the client does not terminate ("asynch_V_no_terminate" below)). 
	# This method is the default naturall name and behavior for V, i.e., do not wait for a return value and do not terminate.
    #
	# Note: When a method never blocks but has a meaningful return value, the client can use a synchronous try or no-try
	# version of V, or an asynchronous version where the client terminates. With a try-version of V that blocks, or
	# an asynchronous version where the client terminates, the client will get the return value upon restart. Thus, the client
	# gets a return value with a blocking try upon restart, or a synchronous no-try, or asynchronous operations with 
	# terminations and restarts.
    def V(self, **kwargs):
        super().enter_monitor(method_name="V")
        logger.trace(" CountingSemaphore_Monitor V() entered monitor, len(self._notEmpty) ="+str(len(self._permitAvailable)) + " permits = " + str(self._permits))
        self._permits += 1
        #threading.current_thread()._returnValue = 1
        #threading.current_thread()._restart = False
        restart = False
        self._permitAvailable.signal_c_and_exit_monitor()
        return 0, restart

	# asychronous version of V that assumes client does not terminate (since V is assumed to never block); 
    # no meaningful return value expected by client
    def asynch_V_no_terminate(self, **kwargs):
        super().enter_monitor(method_name="V")
        logger.trace(" CountingSemaphore_Monitor asynch_V_no_terminate() entered monitor, len(self._notEmpty) ="+str(len(self._permitAvailable)) + " permits = " + str(self._permits))
        self._permits += 1
        #threading.current_thread()._returnValue = 1
        #threading.current_thread()._restart = False
        restart = False
        self._permitAvailable.signal_c_and_exit_monitor()
        return 0, restart
		
	# asychronous version of V that assumes client terminates (so no unnecessary delays for client - just call and terminate); 
    # no meaningful return value expected by client
    def asynch_V_terminate(self, **kwargs):
        super().enter_monitor(method_name="V")
        logger.trace(" CountingSemaphore_Monitor asynch_V_terminate() entered monitor, len(self._notEmpty) ="+str(len(self._permitAvailable)) + " permits = " + str(self._permits))
        self._permits += 1
        #threading.current_thread()._returnValue = 1
        #threading.current_thread()._restart = False
        restart = True
        self._permitAvailable.signal_c_and_exit_monitor()
        return 1, restart
	
	# we do not define a try version of V() above since try_V() here always returns False, i.e., no blocking.
	# Clients can call the asynch version of V, V(), which assumes that clients do not terminate 
    # and do not wait for V()'s return_value since V() never blocks and never returns a meaningful
    # return value.
    def try_V(self, **kwargs):
        super().enter_monitor(method_name = "try_V")
        block = super().is_blocking(False)
        super().exit_monitor()
        return block 

#local tests
def taskP(b : CountingSemaphore_Monitor):
    logger.trace("Calling P")
    b.P()
    logger.trace("Successfully called P")

def taskV(b : CountingSemaphore_Monitor):
    time.sleep(1)
    logger.trace("Calling V")
    b.V()
    logger.trace("Successfully called V")


def main():
    b = CountingSemaphore_Monitor(monitor_name="BoundedBuffer")
    b.init(initial_permits=1)
    b.P()
    b.V()
    b.P()
    b.V()
    b.P()

    try:
        logger.trace("Starting D thread")
        _thread.start_new_thread(taskP, (b,))
    except Exception as ex:
        logger.trace("[ERROR] Failed to start P thread.")
        logger.trace(ex)

    try:
        logger.trace("Starting first thread")
        _thread.start_new_thread(taskV, (b,))
    except Exception as ex:
        logger.trace("[ERROR] Failed to start V thread.")
        logger.trace(ex)

    logger.trace("Sleeping")
    time.sleep(2)
    logger.trace("Done sleeping")

if __name__=="__main__":
    main()
