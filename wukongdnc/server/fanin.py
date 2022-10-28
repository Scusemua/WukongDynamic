#from re import L
from .monitor_su import MonitorSU   #, ConditionVariable
#import threading
import time 

from threading import Thread

import logging 
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(asctime)s] [%(threadName)s] %(levelname)s: %(message)s')

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)

logger.addHandler(ch)

#Fanin object. For a fan-in of n, the first n-1 serverless functions to call fan-in will 
#terminate. Only the last function that calls fan-in will continue executing. fan-in returns
#a list of the results of the n-1 threads that will terminate.
class FanIn(MonitorSU):
    def __init__(self, monitor_name = "FanIn"):
        super(FanIn, self).__init__(monitor_name = monitor_name)
        #self._n = initial_n
        self._num_calling = 0
        self._results = [] # fan_in results of executors
        self._go = self.get_condition_variable(condition_name = "go")
    
    @property
    def n(self):
        return self._n 

    @n.setter
    def n(self, value):
        logger.debug("Setting value of FanIn n to " + str(value))
        self._n = value

    def init(self, fanin_id = None, **kwargs):
        logger.debug(kwargs)
        if kwargs is None or len(kwargs) == 0:
            raise ValueError("FanIn requires a length. No length provided.")
        elif len(kwargs) > 1:
           raise ValueError("Error - FanIn init has too many args.")
        self._n = kwargs['n']
        self.fanin_id = fanin_id

    def try_fan_in(self, **kwargs):
        # Does mutex.P as usual
        super().enter_monitor(method_name = "try_fan_in")     
        # super.is_blocking has a side effect which is to make sure that exit_monitor below
        # does not do mutex.V, also that enter_monitor of wait_b that follows does not do mutex.P.
        # This makes executes_wait ; wait_b atomic
        
        block = super().is_blocking(self._num_calling < (self._n - 1))
        
        # Does not do mutex.V, so we will still have the mutex lock when we next call
        # enter_monitor in wait_b
        super().exit_monitor()
        return block

    # synchronous try version of fan-in, no restart if block since clients are done if they are not the last to call; 
    # Assumes clients that get block = True will terminate and do NOT expect to be restarted. A client that is not 
    # the last to call fan_in is expected to terminate. The last client to call fan_in will become the fan-in task.
    # no meaningful return value expected by client
    def fan_in(self, **kwargs):
        logger.debug("fan_in %s calling enter_monitor" % self.fanin_id)
        # if we called try_fan_in first, we still have the mutex so this enter_monitor does not do mutex.P
        super().enter_monitor(method_name = "fan_in")
        logger.debug("Fan-in %s entered monitor in fan_in()" % self.fanin_id)
        logger.debug("fan_in() " + str(self.fanin_id) + " entered monitor. self._num_calling = " + str(self._num_calling) + ", self._n=" + str(self._n))

        if self._num_calling < (self._n - 1):
            logger.debug("Fan-in %s calling _go.wait_c() from FanIn" % self.fanin_id)
            self._num_calling += 1

            # No need to block non-last thread since we are done with them - they will terminate and not restart.
            # self._go.wait_c()
            result = kwargs['result']
            logger.debug("Result (saved by the non-last executor) for fan-in %s: %s" % (self.fanin_id, str(result)))
            self._results.append(result)
            
            #threading.current_thread()._restart = False
            #threading.current_thread()._returnValue = 0
            restart = False
            logger.debug(" !!!!! non-last Client exiting FanIn fan_in id = %s!!!!!" % self.fanin_id)
            super().exit_monitor()
            # Note: Typcally we would return 1 when try_fan_in returns block is True, but the Fanin currently
            # used by wukong D&C is expecting a return value of 0 for this case.
            return 0, restart
        else:  
            # Last thread does synchronize_synch and will wait for result since False returned by try_fan_in().
            # Last thread does not append results. It will recieve list of results of other threads and append 
            # its result locally to the returned list.
            logger.debug("Last thread in FanIn %s so not calling self._go.wait_c" % self.fanin_id)
            
            if (self._results is not None):
                logger.debug("Returning (to last executor) for fan-in %s: %s" % (self.fanin_id, str(self._results)))
            else:
                logger.error("Result to be returned to last executor is None for fan-in %s!" % self.fanin_id)

            #threading.current_thread()._returnValue = self._results
            #threading.current_thread()._restart = False 
            restart = False
            logger.debug(" !!!!! last Client exiting FanIn fan_in id=%s!!!!!" % self.fanin_id)
            # No signal of non-last client; they did not block and they are done executing. 
            # does mutex.V
            super().exit_monitor()
            return self._results, restart  # all threads have called so return results

        #No logger.debugs here. main Client can exit while other threads are
        #doing this logger.debug so main thread/interpreter can't get stdout lock?

# Local tests  
#def task1(b : FanIn):
    #time.sleep(1)
    #logger.debug("task 1 Calling fan_in")
    #result = b.fan_in(ID = "task 1", result = "task1 result")
    #logger.debug("task 1 Successfully called fan_in")
    #if result == 0:
    #    print("result is o")
    #else:
        #result is a list, print it

#def task2(b : FanIn):
    #logger.debug("task 2 Calling fan_in")
    #result = b.fan_in(ID = "task 2", result = "task2 result")
    #logger.debug("task 2  Successfully called fan_in")
    #if result == 0:
    #    print("result is o")
    #else:
        #result is a list, print it

class testThread(Thread):
    def __init__(self, ID, b):
        # Call the Thread class's init function
        #Thread.__init__(self)
        super(testThread,self).__init__(name="testThread")
        self._ID = ID
        self._restart = True
        self._return = None
        self.b = b

    # Override the run() function of Thread class
    def run(self):
        time.sleep(1)
        logger.debug("task " + self._ID + " Calling fan_in")
        r = self.b.fan_in(ID = self._ID, result = "task1 result")
        logger.debug("task " + self._ID + ", Successfully called fan_in, result: " + str(r))

def main():
    b = FanIn(monitor_name="FanIn")
    b.init(**{"n": 2})

    #try:
    #    logger.debug("Starting thread 1")
    #   _thread.start_new_thread(task1, (b,))
    #except Exception as ex:
    #    logger.debug("[ERROR] Failed to start first thread.")
    #    logger.debug(ex)
    
    try:
        callerThread1 = testThread("T1", b)
        callerThread1.start()
    except Exception as ex:
        logger.debug("[ERROR] Failed to start first thread.")
        logger.debug(ex)      

    #try:
    #    logger.debug("Starting first thread")
    #    _thread.start_new_thread(task2, (b,))
    #except Exception as ex:
    #   logger.debug("[ERROR] Failed to start first thread.")
    #    logger.debug(ex)
    
    try:
        callerThread2 = testThread("T2", b)
        callerThread2.start()
    except Exception as ex:
        logger.debug("[ERROR] Failed to start second thread.")
        logger.debug(ex)
        
    callerThread1.join()
    callerThread2.join()
    
    logger.debug("joined threads")
    print("callerThread1 restart " + str(callerThread1._restart))
    print("callerThread2._returnValue=" + str(callerThread1._return))

    print("callerThread2 restart " + str(callerThread2._restart))
    print("callerThread2._returnValue=" + str(callerThread2._return))
    # if callerThread2._result == 0:
    #     print("callerThread2 result is 0")
    # else:
    #     #result is a list, print it
        
if __name__=="__main__":
    main()