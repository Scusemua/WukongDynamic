from ..constants import SERVERLESS_SYNC

if SERVERLESS_SYNC:
    from .selector_lambda import Selector
else:
    from .selector import Selector

import threading
import time
import os
#import time 
#from threading import Thread

#from DAG_executor import DAG_executor
#from wukongdnc.dag 

#from wukongdnc.dag.DAG_executor import DAG_executor
#from . import DAG_executor_driver
#from .DAG_executor_State import DAG_executor_State
from wukongdnc.dag.DAG_executor_State import DAG_executor_State
from wukongdnc.dag.DAG_executor_constants import run_all_tasks_locally, using_workers, using_threads_not_processes
from wukongdnc.dag.DAG_executor_constants import sync_objects_in_lambdas_trigger_their_tasks
from wukongdnc.dag.DAG_executor_constants import store_sync_objects_in_lambdas
# Note: we init self.store_fanins_faninNBs_locally in init()
#from wukongdnc.dag.DAG_work_queue_for_threads import thread_work_queue
from wukongdnc.dag.DAG_executor_work_queue_for_threads import work_queue
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
    # Actual init is via local init() method, which is clled after this object is create
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
        elif len(kwargs) > 9:
           raise ValueError("Error - FanIn init has too many args.")
        self._n = kwargs['n']
        #self.fanin_task_name = kwargs['fanin_task_name']
        self.start_state_fanin_task = kwargs['start_state_fanin_task']
        #ToDo: Should we just access the gobal constant like we do for the other constants?
        self.store_fanins_faninNBs_locally = kwargs['store_fanins_faninNBs_locally']
        self.DAG_info = kwargs['DAG_info'] 

        # this is the enty that is selected when its guard is true
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
        
        # Note: fan_in never blocks so we are not using this:
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
                        # FanInNB is stored remotely so return work to tcp_server. 
                        # Since we are using thread workers, the work queue is local, not on the server,
                        # and in this case, we return the results dictionary, not a work tuple.
                        # the caller process_faninNBs() will create a work tuple and add it to the work queue.                        super().exit_monitor()
                        # No one should be calling fan_in again since this is last caller
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
                # we store_fanins_faninNBs_locally means the the threasd that simulate lambdas
                # executing tasks and the sync objects are both running locally, i.e., the 
                # sync objects are not on the server or in lambdas. We are not using workers either.
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
                # we are executing tasks in lambdas. The sync objects can be stored on the
                # tcp_server or in lambdas. If this object, which is a select object, is on the 
                # tcp_server is is stored as a regular python object on the server. It may also
                # be stored in a lamba, in which case we are running tcp_serverlambda.
                # Either this object starts a new lambda to execute the fanin task or we trigger 
                # the task by simply callng it. (Note: When we are using lambdas, the sync 
                # objcects cannot be stored locally.)
#rhc: run task
                if store_sync_objects_in_lambdas and sync_objects_in_lambdas_trigger_their_tasks:
                    #  trigger fanni task to run in this lambda
                    try:
                        logger.debug("DAG_executor_FanInNB_Select: triggering DAG_Executor_Lambda() for task " + fanin_task_name)
                        lambda_DAG_exec_state = DAG_executor_State(function_name = "DAG_executor.DAG_executor_lambda", function_instance_ID = str(uuid.uuid4()), state = start_state_fanin_task)
                        logger.debug ("DAG_executor_FanInNB_Select: lambda payload is DAG_info + " + str(start_state_fanin_task) + "," + str(self._results))
                        lambda_DAG_exec_state.restart = False      # starting new DAG_executor in state start_state_fanin_task
                        lambda_DAG_exec_state.return_value = None
                        lambda_DAG_exec_state.blocking = False            
                        logger.info("DAG_executor_FanInNB_Select: Starting Lambda function %s." % lambda_DAG_exec_state.function_name) 
                        payload = {
                            "input": self._results,
                            "DAG_executor_state": lambda_DAG_exec_state,
                            "DAG_info": self.DAG_info
                        }
                        DAG_executor.DAG_executor_lambda(payload)
                    except Exception as ex:
                        logger.error("[ERROR] DAG_executor_FanInNB_Select: Failed to start DAG_executor.DAG_executor_lambda"
                            + " for triggered task " + fanin_task_name)
                        logger.error(ex) 
                else:
                    # invoke a new lambda to run fanin task
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
                # does mutex.V. No one should be calling fan_in again since this is last caller
                # results given to invoked lambda so nothing to return; can't return results
                # to tcp_serve \r or tcp_server might try to put them in the non-existent 
                # work_queue.   
                #return self._results, restart  # all threads have called so return results
                return 0
                #return 1, restart  # all threads have called so return results
            
            elif not self.store_fanins_faninNBs_locally and run_all_tasks_locally:
                # When not self.store_fanins_faninNBs_locally and run_all_tasks_locally we 
                # are simulating lambdas with threads and synch objects are stored remotely. 
                # The objects could be stored on the server or in real lambdas or simulated
                # lambdas. For:
                # - threads simulate lambdas with remote objects on tcp_server, we do not
                #   call processfaninNBs batch, we call fan_in on faninNB objects one by one.
                # - threads simulate lambdas with remote objects in lambdas (tcp_server_lambda)
                #   we call process_faninNBs_batch, which is what we expect when we are sstoring
                #   objects in  simuated or real lambdas. When using real Lambas to execute tasks, 
                #   the FaninNB is supposed to start a real Lambda to do the fanin task. But when we 
                #   are siumlating lambdas with threads, we can't start new threads on 
                #   the server (or they would run there (on server or in lambda)) so we let 
                #   calling threads do that. 
                #   So: When using threads to simulate lambdas to excute tasks, when we do not 
                #   store objects in lambdas, we do not call process_faninNBs_batch; instead, we call 
                #   process_faninNBs, which calls regular tcp_server synchronize_sync w/fan_in on
                #   faninNBs, then tcp_server will call fan_in.
                #   When using threads to simulate lambdas to excute tasks, when we store objects 
                #   in lambdas, we call process_faninNBs_batch on tcp_servr_lambda. This allows us
                #   to call simulated Python functions that store the sync_objects when we are using
                #   threads to simulate lambas tht execute threads. So we can test all this logic
                #   without messing with AWS Lambdas. 
                # - For real lambdas (not simulated by threads), with objects stored in
                #   lambdas (real or simulated) or not, the faninNBs will start real 
                #   lambdas and return 0. We call process_faninNBs_batch but since
                #   there is no fanout list, and no workers, and all returns values are 0.
        
                # When using a thread to simulate lambdas that execute tasks and storing sync objects
                # in lambdas, we call process_faninNBs_batch on tcp_server_lambda. If we are not
                # allowing the fannNB to trigger its tasks to run in this same lamba, 
                # process_faninNBs_batch uses the value returned here to create a work tuple.
                # that has the results returned here and the start_state_fanin_task.
                # Note that we are not using workers so this is not "work" but we need the 
                # start_state_fanin_task from the tuple as the thread that will be started
                # to simulate a lambda needs this start state. The self._results are not 
                # used since the threads that executed the tasks that generated these
                # fanin results already put the results in the data dictionary (before they 
                # called process_faninNBs_batch), which is a global, shared, dictionary when 
                # we are running local threads that simulate lambas

                # From FaninNB:
                # Note: we are not using workers so we do not return a work tuple.
                # The DAG_executor_work_loop will check return_value == 0 to determine 
                # whether we need to start a thread to do fanin task, i.e., to determine whether
                # the fanin caller was last to fanin; so we return self._results (non-zero), even 
                # though we will not use these results; this is inefficient but in this case
                # we are simulating lambdas with threads, which is just to test the 
                # logic witout worrying about performance.
                #return 0, restart
                logger.debug("DAG_executor_FanInNB_Select: fan_in: return self._results for "
                    + " case where simuated lambdas with threads and storing objects remotely, "
                    + " possibly in lambas (simulated or real) and not triggering tasks.")
                return self._results
                #work_tuple = (start_state_fanin_task,self._results)
                #return work_tuple 
            else:
                logger.error("[ERROR]: Internal Error: DAG_executor_FanInNB_Select: fan_in: reached else: error at end of fanin")

            # No signal of non-last client; they did not block and they are done executing. 
            # does mutex.V
            #super().exit_monitor()
            
            #return self._results, restart  # all threads have called so return results
            #return 1, restart  # all threads have called so return results