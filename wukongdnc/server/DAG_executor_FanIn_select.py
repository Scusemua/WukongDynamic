from ..constants import SERVERLESS_SYNC

if SERVERLESS_SYNC:
    from .selector_lambda import Selector
else:
    from .selector import Selector

from .selectableEntry import selectableEntry

import logging 
logger = logging.getLogger(__name__)

logger.setLevel(logging.ERROR)
formatter = logging.Formatter('[%(asctime)s] [%(module)s] [%(processName)s] [%(threadName)s]: %(message)s')
ch = logging.StreamHandler()
ch.setLevel(logging.ERROR)
ch.setFormatter(formatter)
logger.addHandler(ch)


class DAG_executor_FanIn_Select(Selector):
    def __init__(self, selector_name = "DAG_executor_FanIn_Select"):
        super(DAG_executor_FanIn_Select, self).__init__(selector_name=selector_name)
        self.selector_name = selector_name
        self._num_calling = 0
        # For faninNB, results are collected in a map of task_name to result
        self._results = {} # fan_in map of results of executors
    
    @property
    def n(self):
        return self._n 

    @n.setter
    def n(self, value):
        logger.debug("Setting value of FanIn n to " + str(value))
        self._n = value

    # inherit from parent
    def lock(self):
        pass
        
    def unlock(self):
        pass

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
        logger.debug("DAG_executor_FanIn_Select: fan_in: entered fan_in()")  
        if self._num_calling < (self._n - 1):
            self._num_calling += 1

            # No need to block non-last thread since we are done with them - they will terminate and not restart.
            # self._go.wait_c()
            result = kwargs['result']
            calling_task_name = kwargs['calling_task_name']
            self._results[calling_task_name] = result
            logger.debug("DAG_executor_FanIn_Select: fan_in: Result (saved by the non-last executor) " + calling_task_name + " for fan-in %s: %s" % (self.selector_name, str(result)))
            #time.sleep(0.1)
            #threading.current_thread()._restart = False
            #threading.current_thread()._returnValue = 0
            #restart = False
            logger.debug(" FanIn_Select: !!!!! non-last Client " + calling_task_name + " exiting FanIn fan_in id = %s" % self.selector_name)
            # Note: Typcally we would return 1 when try_fan_in returns block is True, but the Fanin currently
            # used by wukong D&C is expecting a return value of 0 for this case.
            return 0
        else:  
            # Last thread does synchronize_synch and will wait for result since False returned by try_fan_in().
            # Last thread does not append results. It will recieve list of results of other threads and append 
            # its result locally to the returned list.
            logger.debug("DAG_executor_FanIn_Select: fan_in: last thread in FanIn %s" % self.selector_name)

            result = kwargs['result']
            calling_task_name = kwargs['calling_task_name']
            
            if (self._results is not None):
                logger.debug("DAG_executor_FanIn_Select: fan_in: fanin collected results from " + calling_task_name + " for fan-in %s: %s" % (self.selector_name, str(self._results)))
 
            #threading.current_thread()._returnValue = self._results
            #threading.current_thread()._restart = False 
            #restart = False
            logger.debug(" FanIn_Select: !!!!! last Client: " + calling_task_name + " exiting FanIn fan_in id=%s!!!!!" % self.selector_name)
            # No signal of non-last client; they did not block and they are done executing. 
            # does mutex.
            
            return self._results # all threads have called so return results

        #No logger.debugs here. main Client can exit while other threads are
        #doing this logger.debug so main thread/interpreter can't get stdout lock?
