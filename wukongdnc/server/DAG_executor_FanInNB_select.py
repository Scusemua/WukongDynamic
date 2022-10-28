from ..constants import SERVERLESS_SYNC

if SERVERLESS_SYNC:
    from .selector_lambda import Selector
else:
    from .selector import Selector

import threading
#import time 
#from threading import Thread

#from DAG_executor import DAG_executor
#from wukongdnc.dag 

#from wukongdnc.dag.DAG_executor import DAG_executor
#from . import DAG_executor_driver
#from .DAG_executor_State import DAG_executor_State
from wukongdnc.dag.DAG_executor_State import DAG_executor_State
from wukongdnc.dag.DAG_executor_constants import run_all_tasks_locally, using_workers, using_threads_not_processes
#from wukongdnc.dag.DAG_work_queue_for_threads import thread_work_queue
from wukongdnc.dag.DAG_work_queue_for_threads import work_queue
from wukongdnc.wukong.invoker import invoke_lambda_DAG_executor
import uuid
from wukongdnc.dag import DAG_executor

from .selectableEntry import selectableEntry

import logging 
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(asctime)s] [%(threadName)s] %(levelname)s: %(message)s')
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)
logger.addHandler(ch)

class DAG_executor_FanInNB_Select(Selector):
    def __init__(self, selector_name = "DAG_executor_FanInNB_Select"):
        super(DAG_executor_FanInNB_Select, self).__init__(selector_name=selector_name)
        self.selector_name = selector_name
        self._num_calling = 0
        #self.n = initial_n
        # For faninNB, results are collected in a map of task_name to result
        self._results = {} # fan_in map of results of executors
    
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
        elif len(kwargs) > 9:
           raise ValueError("Error - FanIn init has too many args.")
        self._n = kwargs['n']
        #self.fanin_task_name = kwargs['fanin_task_name']
        self.start_state_fanin_task = kwargs['start_state_fanin_task']
        self.store_fanins_faninNBs_locally = kwargs['store_fanins_faninNBs_locally']
        self.DAG_info = kwargs['DAG_info'] 

        self._fan_in = selectableEntry("fan_in")

        # fan_in never blocks and we are not doing an restarts during DAG_execution.
        self._fan_in.set_restart_on_block(False)
        self._fan_in.set_restart_on_noblock(False) 
        self._fan_in.set_restart_on_unblock(False)

        # superclass method calls
        self.add_entry(self._fan_in)     # alternative 1

    def try_fan_in(self,**kwargs):
        # Does try_op protocol for acquiring/releasing lock
        
        #self.enter_monitor(method_name = "try_deposit")
        
        # super.is_blocking has a side effect which is to make sure that exit_monitor below
        # does not do mutex.V, also that enter_monitor of wait_b that follows does not do mutex.P.
        # This males try_wait_b ; wait_b atomic
        
        #block = super().is_blocking(self._fullSlots == self._capacity)
        block = self.is_blocking(False)
        
        # Does not do mutex.V, so we will still have the mutex lock when we next call
        # enter_monitor in wait_b
        
        #self.exit_monitor()
        
        return block
    
    def set_guards(self):
        self._fan_in.guard(True)

    # synchronous try version of fan-in, no restart if block since clients are done if they are not the last to call; 
    # Assumes clients that get block = True will terminate and do NOT expect to be restarted. A client that is not 
    # the last to call fan_in is expected to terminate. The last client to call fan_in will become the fan-in task.
    # no meaningful return value expected by client
    def fan_in(self, **kwargs):
        # if we called try_fan_in first, we still have the mutex so this enter_monitor does not do mutex.P
        logger.debug("DAG_executor_FanInNB_Select: fan_in: entered fan_in()")
        
        if self._num_calling < (self._n - 1):
            self._num_calling += 1

            # No need to block non-last thread since we are done with them - they will terminate and not restart.
            # self._go.wait_c()
            result = kwargs['result']
            logger.debug("DAG_executor_FanInNB_Select: fan_in:  result is " + str(result))
            calling_task_name = kwargs['calling_task_name']
            #self._results[calling_task_name] = result[calling_task_name]
            self._results[calling_task_name] = result
            logger.debug("DAG_executor_FanInNB_Select: fan_in: result (saved by the non-last executor) for fan-in %s: %s" % (calling_task_name, str(result)))
            
            #threading.current_thread()._restart = False
            #threading.current_thread()._returnValue = 0

            logger.debug(" !!!!! non-last Client: DAG_executor_FanInNB_Select: fan_in: " + calling_task_name 
                + " exiting FanInNB fan_in")
            # Note: Typcally we would return 1 when try_fan_in returns block is True, but the Fanin currently
            # used by wukong D&C is expecting a return value of 0 for this case.
            return 0
        else:  
            # Last thread does synchronize_synch and will not wait for result since this is fanin NB.
            # Last thread does append results (unlike FanIn)
            logger.debug("DAG_executor_FanInNB_Select: fan_in: Last thread in FanIn %s so not calling self._go.wait_c" % self.selector_name)
            result = kwargs['result']
            calling_task_name = kwargs['calling_task_name']
            self._results[calling_task_name] = result
            start_state_fanin_task = kwargs['start_state_fanin_task']
            
            if (self._results is not None):
                logger.debug("DAG_executor_FanInNB_Select: fan_in: faninNB %s: collected results of fan_in: %s" % (self.selector_name, str(self._results)))
 
            #threading.current_thread()._returnValue = self._results
            #threading.current_thread()._restart = False 

            logger.debug("!!!!! last Client: DAG_executor_FanInNB_Select: fan_in: faninNB " + self.selector_name
                +  " calling task name: " + calling_task_name + "exiting fan_in")
            # for debugging
            fanin_task_name = kwargs['fanin_task_name']

            if using_workers:
                if using_threads_not_processes:
                    if self.store_fanins_faninNBs_locally:
                        # if using worker pools of threads, add fanin task's state to the work_queue.
                        # Note: if we are using worker pools of processes, then the process will call fan_in and
                        # the last process to execute fanin will put the fanin task's state in the
                        # work_queue (if we are not batching calls) This last process does not become the 
                        # fanin task, as this is a faninNB (No Become).
                        # This FaninNB is running on the tcp_server, as multiprocessing
                        # requires pools to be process pools, not thread pools, and it requires synch objects
                        # to be stored on the tcp_server or InfiniX lambdas, so this faninNB cannot start
                        # a new thread/process or add a sate to the processes work_queue.
                        # If we are batching calls to fan_in, we are storing FanInNBs remotely so we cannot
                        # start a thread (on the tcp_server)
                        logger.debug("DAG_executor_FanInNB_Select: fan_in: using_workers and threads so add start state of fanin task to thread_work_queue.")
                        #thread_work_queue.put(start_state_fanin_task)
                        work_tuple = (start_state_fanin_task,self._results)
                        work_queue.put(work_tuple)
                        #work_queue.put(start_state_fanin_task)
           
                        # No signal of non-last client; they did not block and they are done executing. 
                        # does mutex.V

                        # no one should be calling fan_in again since this is last caller
                        return self._results  # all threads have called so return results
                        #return 1, restart  # all threads have called so return results
                    else:
                        # FanInNB is stored remotely so return work to tcp_server. If we are not batching calls
                        # to fan_in, the work/results will be returned to the client caller. If we are batching 
                        # calls, one result will be returned to the client if they need work. If not, no work
                        # is returned and all work is put into the work_queue, which is also stored on tcp_server.

                        # no one should be calling fan_in again since this is last caller
                        return self._results  # all threads have called so return results        
                else:
                    if self.store_fanins_faninNBs_locally:
                        logger.error("[Error]: DAG_executor_FanInNB_Select: fan_in: using workers and processes but storing fanins locally,")
                    # No signal of non-last client; they did not block and they are done executing. 
                    # does mutex.V

                    # no one should be calling fan_in again since this is last calle
                    return self._results  # all threads have called so return results
                    #return 1, restart  # all threads have called so return results
            elif self.store_fanins_faninNBs_locally and run_all_tasks_locally:
                if not using_threads_not_processes:
                    logger.error("[Error]: DAG_executor_FanInNB_Select: fan_in:  storing fanins locally but not using threads.")
  
                try:
                    logger.debug("DAG_executor_FanInNB_Select: fan_in:  starting DAG_executor thread for task " + fanin_task_name + " with start state " + str(start_state_fanin_task))
                    server = kwargs['server']
                    #DAG_executor_state =  kwargs['DAG_executor_State']
                    #DAG_executor_state.state = int(start_state_fanin_task)
                    DAG_executor_state = DAG_executor_State(function_name = "DAG_executor", function_instance_ID = str(uuid.uuid4()), state = start_state_fanin_task)
                    DAG_executor_state.restart = False      # starting  new DAG_executor in state start_state_fanin_task
                    DAG_executor_state.return_value = None
                    DAG_executor_state.blocking = False
                    logger.debug("DAG_executor_FanInNB_Select: fan_in: calling_task_name:" + calling_task_name + " DAG_executor_state.state: " + str(DAG_executor_state.state))
                    #logger.debug("DAG_executor_state.function_name: " + DAG_executor_state.function_name)
                    payload = {
                        #"state": int(start_state_fanin_task),
                        "input": self._results,
                        "DAG_executor_state": DAG_executor_state,
                        "DAG_info": self.DAG_info,
                        "server": server
                    }
                    thread_name_prefix = "Thread_leaf_"
                    thread = threading.Thread(target=DAG_executor.DAG_executor_task, name=(thread_name_prefix+str(start_state_fanin_task)), args=(payload,))
                    thread.start()
                    #_thread.start_new_thread(DAG_executor.DAG_executor_task, (payload,))
                except Exception as ex:
                    logger.debug("[ERROR]: DAG_executor_FanInNB_Select: fan_in: Failed to start DAG_executor thread.")
                    logger.debug(ex)

                # No signal of non-last client; they did not block and they are done executing. 
                # does mutex.V
                # no one should be calling fan_in again since this is last caller
                return self._results  # all threads have called so return results
                #return 1, restart  # all threads have called so return results
                    
            elif not self.store_fanins_faninNBs_locally and not run_all_tasks_locally:
                #ToDO: use using_lambdas (=> self.store_fanins_faninNBs_locally and not run_all_tasks_locally)
                try:
                    DAG_executor_state = DAG_executor_State(function_name = "DAG_executor.DAG_executor_lambda", function_instance_ID = str(uuid.uuid4()), state = start_state_fanin_task)
                    DAG_executor_state.restart = False      # starting  new DAG_executor in state start_state_fanin_task
                    DAG_executor_state.return_value = self._results
                    DAG_executor_state.blocking = False            
                    logger.debug("DAG_executor_FanInNB_Select: fan_in: starting Starting Lambda function for task " + fanin_task_name + " with start state " + str(DAG_executor_state.state))
                    #logger.debug("DAG_executor_state: " + str(DAG_executor_state))
                    payload = {
                        #"state": int(start_state_fanin_task),
                        "input": self._results,
                        "DAG_executor_state": DAG_executor_state,
                        "DAG_info": self.DAG_info
                        #"server": server   # used to mock server during testing
                    }
                    ###### DAG_executor_State.function_name has not changed
                    
                    invoke_lambda_DAG_executor(payload = payload, function_name = "DAG_Executor_Lambda")
                except Exception as ex:
                    logger.debug("[ERROR]: DAG_executor_FanInNB_Select: fan_in: Failed to start DAG_executor Lambda.")
                    logger.debug(ex)

                # No signal of non-last client; they did not block and they are done executing. 
                # does mutex.V
                # no one should be calling fan_in again since this is last caller
                # results given to invoked lambda so nothing to return; can't return results
                # to tcp_serve \r or tcp_server might try to put them in the non-existent 
                # work_queue.   
                #return self._results, restart  # all threads have called so return results
                return 0
                #return 1, restart  # all threads have called so return results

            else:
                logger.error("[ERROR]: Internal Error: DAG_executor_FanInNB_Select: fan_in: reached else: error at end of fanin")

            # No signal of non-last client; they did not block and they are done executing. 
            # does mutex.V
            #super().exit_monitor()
            
            #return self._results, restart  # all threads have called so return results
            #return 1, restart  # all threads have called so return results