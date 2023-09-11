#from re import L
# ToDo:
#from ..server.monitor_su import MonitorSU, ConditionVariable
from .monitor_su import MonitorSU
import time 

from threading import Thread

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

#Fanin object. For a fan-in of n, the first n-1 serverless functions to call fan-in will 
#terminate. Only the last function that calls fan-in will continue executing. fan-in returns
#a list of the results of the n-1 threads that will terminate.
#
# Note: Differences between DAG_executor_FanIn and FanIn: Hee, in __init__: self.monitor_name = monitor_name; can do this in FanIn too.
# Here, we use a map of task names to results so fanin task can get its inputs per name. FanIn could do this to - could choose between
# per name (key) and just gettting a list of the values.
class DAG_executor_FanIn(MonitorSU):
    #num_fanins = 1
    def __init__(self, monitor_name = "DAG_executor_FanIn"):
        super(DAG_executor_FanIn, self).__init__(monitor_name = monitor_name)
        self.monitor_name = monitor_name    # this is fanin_task_name
        #logger.debug("DAG_executor_FanIn: __init__: monitor_name: " + str(monitor_name))
        #self._n = initial_n
        self._num_calling = 0
        # For faninNB, results are collected in a nmap of task_name to result
        self._results = {} # fan_in results of executors
        self._go = self.get_condition_variable(condition_name = "go")
    
    @property
    def n(self):
        return self._n 

    @n.setter
    def n(self, value):
        logger.debug("Setting value of FanIn n to " + str(value))
        self._n = value

    def init(self, **kwargs):
        #logger.debug(kwargs)
        #   These are the 6 keyword_arguments passed:
        #   keyword_arguments['fanin_task_name'] = fanins[0]    # used for debugging
        #   keyword_arguments['n'] = faninNB_sizes[0]
        #   # used by FanInNB to start new DAG_executor in given state - FanIn uses become task
        #   #keyword_arguments['start_state_fanin_task'] = DAG_states[fanins[0]]  
        #   keyword_arguments['result'] = output
        #   keyword_arguments['calling_task_name'] = calling_task_name
        #   keyword_arguments['DAG_executor_State'] = DAG_executor_State
        #   keyword_arguments['server'] = server    # used to mock server when running local test
        if kwargs is None or len(kwargs) == 0:
            raise ValueError("FanIn requires a length. No length provided.")
        elif len(kwargs) > 6:
           raise ValueError("Error - FanIn init has too many args.")
        self._n = kwargs['n']
        # This is used by FanInNB to start a new DAG_executor. FanIn uses a become task but could do
        # a restart on asynch calls w/terminate, etc.
        #self.fanin_task_name = kwargs['fanin_task_name']
        #self.start_state_fanin_task = kwargs['start_state_fanin_task']

    def try_fan_in(self, **kwargs):
        # Does mutex.P as usual
        super().enter_monitor(method_name = "try_fan_in")     
        # super.is_blocking has a side effect which is to make sure that exit_monitor below
        # does not do mutex.V, also that enter_monitor of wait_b that follows does not do mutex.P.
        # This makes executes_wait ; wait_b atomic
        
        # Note: fan_in never blocks so we are not using this:
        #block = super().is_blocking(self._num_calling < (self._n - 1))
        block = self.is_blocking(False)
        
        # Does not do mutex.V, so we will still have the mutex lock when we next call
        # enter_monitor in wait_b
        super().exit_monitor()
        return block

    # synchronous try version of fan-in, no restart if block since clients are done if they are not the last to call; 
    # Assumes clients that get block = True will terminate and do NOT expect to be restarted. A client that is not 
    # the last to call fan_in is expected to terminate. The last client to call fan_in will become the fan-in task.
    # no meaningful return value expected by client
    def fan_in(self, **kwargs):
        #logger.error("fanin: " + str(DAG_executor_FanIn.num_fanins))
        #DAG_executor_FanIn.num_fanins += 1
        logger.debug("fan_in %s calling enter_monitor" % self.monitor_name)
        # if we called try_fan_in first, we still have the mutex so this enter_monitor does not do mutex.P
        super().enter_monitor(method_name = "fan_in")
        logger.debug("Fan-in %s entered monitor in fan_in()" % self.monitor_name)
        logger.debug("fan_in() " + str(self.monitor_name) + " entered monitor. self._num_calling = " + str(self._num_calling) + ", self._n=" + str(self._n))
        
        if self._num_calling < (self._n - 1):
            logger.debug("Fan-in %s calling _go.wait_c() from FanIn" % self.monitor_name)
            self._num_calling += 1

            # No need to block non-last thread since we are done with them - they will terminate and not restart.
            # self._go.wait_c()
            result = kwargs['result']
            calling_task_name = kwargs['calling_task_name']
            self._results[calling_task_name] = result
            logger.debug("FanIn: Result (saved by the non-last executor) " + calling_task_name + " for fan-in %s: %s" % (self.monitor_name, str(result)))
            #time.sleep(0.1)
            #threading.current_thread()._restart = False
            #threading.current_thread()._returnValue = 0
            restart = False
            logger.debug(" FanIn: !!!!! non-last Client: " + calling_task_name + " exiting FanIn fan_in id = %s!!!!!" % self.monitor_name)
            super().exit_monitor()
            # Note: Typcally we would return 1 when try_fan_in returns block is True, but the Fanin currently
            # used by wukong D&C is expecting a return value of 0 for this case.
            return 0, restart
        else:  
            # Last thread does synchronize_synch and will wait for result since False returned by try_fan_in().
            # Last thread does not append results. It will recieve list of results of other threads and append 
            # its result locally to the returned list.
            logger.debug("Last thread in FanIn %s so not calling self._go.wait_c" % self.monitor_name)

            result = kwargs['result']
            calling_task_name = kwargs['calling_task_name']
            
            if (self._results is not None):
                logger.debug("fanin collected results from " + calling_task_name + " for fan-in %s: %s" % (self.monitor_name, str(self._results)))
 
            #threading.current_thread()._returnValue = self._results
            #threading.current_thread()._restart = False 
            restart = False
            logger.debug(" FanIn: !!!!! last Client: " + calling_task_name + " exiting FanIn fan_in id=%s!!!!!" % self.monitor_name)
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
    #    logger.debug("result is o")
    #else:
        #result is a list, logger.debug it

#def task2(b : FanIn):
    #logger.debug("task 2 Calling fan_in")
    #result = b.fan_in(ID = "task 2", result = "task2 result")
    #logger.debug("task 2  Successfully called fan_in")
    #if result == 0:
    #    logger.debug("result is o")
    #else:
        #result is a list, logger.debug it

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
        logger.debug("task " + self._ID + ", Successfully called fan_in, returned r: " + r)

def main():
    b = DAG_executor_FanIn(monitor_name="DAG_executor_FanIn")
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
    logger.debug("callerThread1 restart " + str(callerThread1._restart))
    logger.debug("callerThread2._returnValue=" + str(callerThread1._return))

    logger.debug("callerThread2 restart " + str(callerThread2._restart))
    logger.debug("callerThread2._returnValue=" + str(callerThread2._return))
    # if callerThread2._result == 0:
    #     logger.debug("callerThread2 result is 0")
    # else:
    #     #result is a list, logger.debug it
        
if __name__=="__main__":
    main()