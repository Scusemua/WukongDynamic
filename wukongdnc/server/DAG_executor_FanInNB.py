from re import L
# ToDo:
#from ..server.monitor_su import MonitorSU, ConditionVariable
from .monitor_su import MonitorSU, ConditionVariable
import threading
import time 

from threading import Thread

import _thread


#from DAG_executor import DAG_executor
from ..dag import  DAG_executor
#from wukongdnc.dag.DAG_executor import DAG_executor
#from . import DAG_executor_driver
#from .DAG_executor_State import DAG_executor_State
from wukongdnc.dag.DAG_executor_State import DAG_executor_State
from wukongdnc.dag.DAG_executor_constants import run_all_tasks_locally, store_fanins_faninNBs_locally, create_all_fanins_faninNBs_on_start, using_workers, num_workers, using_threads_not_processes
from wukongdnc.dag.DAG_work_queue_for_threads import thread_work_queue
import uuid

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
class DAG_executor_FanInNB(MonitorSU):
    def __init__(self, initial_n = 0, monitor_name = "DAG_executor_FanInNB"):
        super(DAG_executor_FanInNB, self).__init__(monitor_name = monitor_name)
        self.monitor_name = monitor_name    # this is fanin_task_name
        self._n = initial_n
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
        #   These are the 8 keyword_arguments passed:
        #   keyword_arguments['fanin_task_name'] = name     # for debugging
        #   keyword_arguments['n'] = n
        #   keyword_arguments['start_state_fanin_task'] = start_state_fanin_task    # start DAG_executor in this state to run fanin task
        #   keyword_arguments['result'] = output        # output of running task that is now executing this fan_in
        #   keyword_arguments['calling_task_name'] = calling_task_name  # used to label output of task that is executing this fan_in
        #   keyword_arguments['DAG_executor_State'] = new_DAG_exec_State # given to the thread/lambda that executes the fanin task.
        #   keyword_arguments['server'] = server     # used to mock server when running local test
        #   keyword_arguments['store_fanins_faninNBs_locally'] = store_fanins_faninNBs_locally    # option set in DAG_executor
        if kwargs is None or len(kwargs) == 0:
            raise ValueError("FanIn requires a length. No length provided.")
        elif len(kwargs) > 9:
           raise ValueError("Error - FanIn init has too many args.")
        self._n = kwargs['n']
        #self.fanin_task_name = kwargs['fanin_task_name']
        self.start_state_fanin_task = kwargs['start_state_fanin_task']
        self.store_fanins_faninNBs_locally = kwargs['store_fanins_faninNBs_locally']
        self.DAG_info = kwargs['DAG_info'] 

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
        logger.debug("FanInNB: fan_in %s calling enter_monitor" % self.monitor_name)
        # if we called try_fan_in first, we still have the mutex so this enter_monitor does not do mutex.P
        super().enter_monitor(method_name = "fan_in")
        logger.debug("FanInNB: Fan-in %s entered monitor in fan_in()" % self.monitor_name)
        logger.debug("FanInNB: fan_in() " + self.monitor_name + " entered monitor. self._num_calling = " + str(self._num_calling) + ", self._n=" + str(self._n))

        if self._num_calling < (self._n - 1):
            self._num_calling += 1

            # No need to block non-last thread since we are done with them - they will terminate and not restart.
            # self._go.wait_c()
            result = kwargs['result']
            logger.debug("FanInNB: result is " + str(result))
            calling_task_name = kwargs['calling_task_name']
            #self._results[calling_task_name] = result[calling_task_name]
            self._results[calling_task_name] = result
            logger.debug("FanInNB: Result (saved by the non-last executor) for fan-in %s: %s" % (self.monitor_name, str(result)))
            
            #threading.current_thread()._restart = False
            #threading.current_thread()._returnValue = 0
            restart = False
            logger.debug(" FanInNB: !!!!! non-last Client exiting FanInNB fan_in id = %s!!!!!" % self.monitor_name)
            super().exit_monitor()
            # Note: Typcally we would return 1 when try_fan_in returns block is True, but the Fanin currently
            # used by wukong D&C is expecting a return value of 0 for this case.
            return 0, restart
        else:  
            # Last thread does synchronize_synch and will not wait for result since this is fanin NB.
            # Last thread does append results (unlike FanIn)
            logger.debug("FanInNB:Last thread in FanIn %s so not calling self._go.wait_c" % self.monitor_name)
            result = kwargs['result']
            calling_task_name = kwargs['calling_task_name']
            self._results[calling_task_name] = result
            start_state_fanin_task = kwargs['start_state_fanin_task']
            
            if (self._results is not None):
                logger.debug("faninNB collected results for fan-in %s: %s" % (self.monitor_name, str(self._results)))
 
            #threading.current_thread()._returnValue = self._results
            #threading.current_thread()._restart = False 
            restart = False
            logger.debug("FanInNB: !!!!! last Client exiting FanIn fan_in id=%s!!!!!" % self.monitor_name)
            # for debugging
            fanin_task_name = kwargs['fanin_task_name']

            if using_workers and using_threads_not_processes:
                # if using worker pools of threads, add fanin task's state to the work_queue.
                # Note: if we are using worker pools of processes, then the process will call fan_in and
                # the last process to execute fanin will put the fanin task's state in the
                # work_queue. This last process does not become the fanin task, as this is a
                # faninNB (No Become). This FaninNB is running on he tcp_server, as multiprocessing
                # requires pools to be process pools, not thread pools, and it requires synch objects
                # to be stored on the tcp_server or InfiniX lambdas, so this faninNB cannot start
                # a new thread/process or add a sate to the processes work_queue.
                logger.debug("FanInNB: using_workers and threads so add start state of fanin task to thread_work_queue.")
                thread_work_queue.put(start_state_fanin_task)
            else:
                # 
                if self.store_fanins_faninNBs_locally and run_all_tasks_locally:
                    try:
                        logger.debug("FanInNB: starting DAG_executor thread for task " + fanin_task_name + " with start state " + str(start_state_fanin_task))
                        server = kwargs['server']
                        #DAG_executor_state =  kwargs['DAG_executor_State']
        ##rhc
                        #DAG_executor_state.state = int(start_state_fanin_task)
                        DAG_executor_state = DAG_executor_State(function_name = "DAG_executor", function_instance_ID = str(uuid.uuid4()), state = start_state_fanin_task)
                        DAG_executor_state.restart = False      # starting  new DAG_executor in state start_state_fanin_task
                        DAG_executor_state.return_value = None
                        DAG_executor_state.blocking = False
        ##rhc
                        logger.debug("FanInNB: calling_task_name:" + calling_task_name + " DAG_executor_state.state: " + str(DAG_executor_state.state))
                        #logger.debug("DAG_executor_state.function_name: " + DAG_executor_state.function_name)
                        payload = {
                            #"state": int(start_state_fanin_task),
                            "input": self._results,
                            "DAG_executor_State": DAG_executor_state,
                            "DAG_info": self.DAG_info,
                            "server": server
                        }
                        thread_name_prefix = "Thread_leaf_"
                        thread = threading.Thread(target=DAG_executor.DAG_executor_task, name=(thread_name_prefix+str(start_state_fanin_task)), args=(payload,))
                        thread.start()
                        #_thread.start_new_thread(DAG_executor.DAG_executor_task, (payload,))
                    except Exception as ex:
                        logger.debug("FanInNB:[ERROR] Failed to start DAG_executor thread.")
                        logger.debug(ex)
                    
                elif not self.store_fanins_faninNBs_locally and not run_all_tasks_locally:
                    try:
        ##rhc
                        DAG_executor_state = DAG_executor_State(function_name = "DAG_executor", function_instance_ID = str(uuid.uuid4()), state = start_state_fanin_task)
                        DAG_executor_state.restart = False      # starting  new DAG_executor in state start_state_fanin_task
                        DAG_executor_state.return_value = self._results
                        DAG_executor_state.blocking = False            
                        logger.debug("FanInNB: starting Starting Lambda function for task " + fanin_task_name + " with start state " + str(DAG_executor_state.state))
                        #logger.debug("DAG_executor_state: " + str(DAG_executor_state))
                        payload = {
        ##rhc
                            #"state": int(start_state_fanin_task),
                            "input": self._results,
                            "DAG_executor_State": DAG_executor_state,
                            "DAG_info": self.DAG_info
                            #"server": server   # used to mock server during testing
                        }
                        ###### DAG_executor_State.function_name has not changed
                        
                        invoke_lambda_DAG_executor(payload = payload, function_name = "DAG_executor")
                    except Exception as ex:
                        logger.debug("FanInNB:[ERROR] Failed to start DAG_executor Lambda.")
                        logger.debug(ex)
            # No signal of non-last client; they did not block and they are done executing. 
            # does mutex.V
            super().exit_monitor()
            
            #return self._results, restart  # all threads have called so return results
            return 1, restart  # all threads have called so return results
        

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
        logger.debug("task " + self._ID + ", Successfully called fan_in, returned r:" + r)

def main():
    b = DAG_executor_FanInNB(initial_n=2,monitor_name="DAG_executor_FanInNB")
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