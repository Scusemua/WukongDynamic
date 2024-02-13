#from re import L
# ToDo:
#from ..server.monitor_su import MonitorSU, ConditionVariable
from .monitor_su import MonitorSU
import threading
import time 
from threading import Thread
import os

#from DAG_executor import DAG_executor
#from wukongdnc.dag 

#from wukongdnc.dag.DAG_executor import DAG_executor
#from . import DAG_executor_driver
#from .DAG_executor_State import DAG_executor_State
from wukongdnc.dag.DAG_executor_State import DAG_executor_State

#from wukongdnc.dag.DAG_executor_constants import run_all_tasks_locally, using_workers, using_threads_not_processes
#from wukongdnc.dag.DAG_executor_constants import store_sync_objects_in_lambdas, sync_objects_in_lambdas_trigger_their_tasks
#from wukongdnc.dag.DAG_executor_constants import input_all_groups_partitions_at_start
#from wukongdnc.dag.DAG_executor_constants import store_fanins_faninNBs_locally
#from wukongdnc.dag.DAG_executor_constants import exit_program_on_exception
import wukongdnc.dag.DAG_executor_constants
#Try: from ..dag import DAG_executor_constants and see if DAG_executor_constants imported once

#from wukongdnc.dag.DAG_work_queue_for_threads import thread_work_queue
from wukongdnc.dag.DAG_executor_work_queue_for_threads import work_queue
from wukongdnc.wukong.invoker import invoke_lambda_DAG_executor
import uuid
from wukongdnc.dag import DAG_executor

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
class DAG_executor_FanInNB(MonitorSU):
    def __init__(self, monitor_name = "DAG_executor_FanInNB"):
        super(DAG_executor_FanInNB, self).__init__(monitor_name = monitor_name)
        self.monitor_name = monitor_name    # this is fanin_task_name
        #logger.trace("DAG_executor_FanInNB: __init__: monitor_name: " + str(monitor_name))
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
        logger.trace("Setting value of FanIn n to " + str(value))
        self._n = value

    def init(self, **kwargs):
        #logger.trace(kwargs)
        #   These are the 8 keyword_arguments passed:
        #   keyword_arguments['fanin_task_name'] = name     # for debugging
        #   keyword_arguments['n'] = n
        #   keyword_arguments['start_state_fanin_task'] = start_state_fanin_task    # start DAG_executor in this state to run fanin task
        #   keyword_arguments['result'] = output        # output of running task that is now executing this fan_in
        #   keyword_arguments['calling_task_name'] = calling_task_name  # used to label output of task that is executing this fan_in
        #   keyword_arguments['DAG_executor_state'] = new_DAG_exec_State # given to the thread/lambda that executes the fanin task.
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

        if self.DAG_info is None:
            # When running real lambdas, DAG_info must be non-null since
            # the FaninNB will start a real lambda to excute its fanin task
            # and pass the DAG_info on the payload. For simulated lambdas
            # (run_all_tasks_locally and not using_workers), when this 
            # FaninNB is stored on the tcp_servr, the fanin task will be executed 
            # by a new simulated lambda thread that is started by the simulated 
            # lambda that was the last simulated lambda to called fanin on the faninNB. 
            # (A remote FaninNB on the tcpServer cannot start a local thread to simuate 
            # a lambda as that thread would run on the tcp_server. When 
            # this FaninNB is stored locally, it does start a new simulated
            # lambda to execute the fanin task and it puts DG_info in the
            # payload of he simulated lambda. Thus, DAG_info is needed 
            # when we aer using real lambdas (not run_all_tasks_locally) or
            # weare using simulated lambdas (run_all_tasks_locally and not using_workers) 
            # and the FaninNB object is stored locally 
            # (store_fanins_faninNBs_locally)
            try:
                msg =  "[Error]: FanInNB_select: fanin_task_name: DAG_info is None for FaninNB init()."
                assert not ((not wukongdnc.dag.DAG_executor_constants.run_all_tasks_locally) or (wukongdnc.dag.DAG_executor_constants.run_all_tasks_locally and not wukongdnc.dag.DAG_executor_constants.using_workers and wukongdnc.dag.DAG_executor_constants.store_fanins_faninNBs_locally)) , msg
            except AssertionError:
                logger.exception("[Error]: assertion failed")
                if wukongdnc.dag.DAG_executor_constants.exit_program_on_exception:
                    logging.shutdown()
                    os._exit(0)
            #assertOld:
            #if (not run_all_tasks_locally) or (run_all_tasks_locally and not using_workers and store_fanins_faninNBs_locally):
            #    logger.error("Error: FanInNB: fanin_task_name: DAG_info is None for FaninNB init().")
            #    logging.shutdown()
            #    os._exit(0)
        else:
            logger.trace("FanInNB: fanin_task_name: DAG_info is not None for init().")

#rhc: groups partitions
        # When running real lambdas, in order to avoid reading the 
        # individual groups/partitions when testing pagerank with 
        # real lambdas, the DAG_executor_driver can input all the 
        # groups/partitions at the start of execution and pass them
        # to the real lambdas it starts. Those real lambdas will
        # pass the groups/partitions to the real lambdas (in their
        # payloads) statrfed for fanouts. If the fanins/faninNBs are
        # all created at the start, we pass the groups/partitions
        # to the create() on the tcp_server and it will call this 
        # # init() method with the groups_partitions parameter.
        # Real lambdas call process faninNBs batch on the tcp_server, 
        # and if we are creating fanins/faninNB on the fly, we pass 
        # groups_partitions to the create().
        # 
        if wukongdnc.dag.DAG_executor_constants.input_all_groups_partitions_at_start:
            self.groups_partitions = kwargs['groups_partitions'] 

            #print("groups_partitions:")
            #keys = list(self.groups_partitions.keys())
            #for key in keys:
            #    print(key + ":")
            #    g_p = self.groups_partitions[key]
            #    for node in g_p:
            #        print(str(node))
            #logging.shutdown()
            #os._exit(0)

#rhc: groups partitions
        # get it if real lambdas etc
        # also, in select version

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
        logger.trace("FanInNB: fan_in %s calling enter_monitor" % self.monitor_name)
        # if we called try_fan_in first, we still have the mutex so this enter_monitor does not do mutex.P
        super().enter_monitor(method_name = "fan_in")
        logger.trace("FanInNB: Fan-in %s entered monitor in fan_in()" % self.monitor_name)
        logger.trace("FanInNB: fan_in() " + self.monitor_name + " entered monitor. self._num_calling = " + str(self._num_calling) + ", self._n=" + str(self._n))

        if self._num_calling < (self._n - 1):
            self._num_calling += 1

            # No need to block non-last thread since we are done with them - they will terminate and not restart.
            # self._go.wait_c()
            result = kwargs['result']
            logger.trace("FanInNB: result is " + str(result))
            calling_task_name = kwargs['calling_task_name']
            #self._results[calling_task_name] = result[calling_task_name]
            self._results[calling_task_name] = result

            logger.trace("FanInNB: Result (saved by the non-last executor) for fan-in %s: %s" % (calling_task_name, str(result)))
            
            #threading.current_thread()._restart = False
            #threading.current_thread()._returnValue = 0
            restart = False
            logger.info(" FanInNB: !!!!! non-last Client: " + calling_task_name 
                + " exiting FanInNB fan_in id = %s!!!!!" % calling_task_name)
            super().exit_monitor()
            # Note: Typcally we would return 1 when try_fan_in returns block is True, but the Fanin currently
            # used by wukong D&C is expecting a return value of 0 for this case.
            return 0, restart
        else:  
            # Last thread does synchronize_synch and will not wait for result since this is fanin NB.
            # Last thread does append results (unlike FanIn)
            logger.trace("FanInNB:Last thread in FanIn %s so not calling self._go.wait_c" % self.monitor_name)
            result = kwargs['result']
            calling_task_name = kwargs['calling_task_name']
            self._results[calling_task_name] = result
            start_state_fanin_task = kwargs['start_state_fanin_task']
            
            if (self._results is not None):
                logger.trace("faninNB collected results for fan-in %s: %s" % (self.monitor_name, str(self._results)))
 
            #threading.current_thread()._returnValue = self._results
            #threading.current_thread()._restart = False 
            restart = False
            logger.info(" FanInNB: !!!!! last Client " + calling_task_name 
                + " exiting FanIn fan_in id=%s!!!!!" % self.monitor_name)
            # for debugging
            fanin_task_name = kwargs['fanin_task_name']

            if wukongdnc.dag.DAG_executor_constants.using_workers:
                if wukongdnc.dag.DAG_executor_constants.using_threads_not_processes:
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
                        logger.trace("FanInNB: using_workers and threads so add start state of fanin task to thread_work_queue.")
                        #thread_work_queue.put(start_state_fanin_task)
                        work_tuple = (start_state_fanin_task,self._results)
                        work_queue.put(work_tuple)
                        #work_queue.put(start_state_fanin_task)
           
                        # No signal of non-last client; they did not block and they are done executing. 
                        # does mutex.V
                        super().exit_monitor()
                        # no one should be calling fan_in again since this is last caller
                        return self._results, restart  # all threads have called so return results
                        #return 1, restart  # all threads have called so return results
                    else:
                        # FanInNB is stored remotely so return work to tcp_server. 
                        # Since we are using thread workers, the work queue is local, not on the server,
                        # and in this case, we return the results dictionary, not a work tuple.
                        # the caller process_faninNBs() will create a work tuple and add it to the work queue.                        super().exit_monitor()
                        # No one should be calling fan_in again since this is last caller
                        return self._results, restart  # all threads have called so return results        
                else:
                    try:
                        msg = "[Error]: FaninB: using workers and processes but storing fanins locally."
                        assert not self.store_fanins_faninNBs_locally , msg
                    except AssertionError:
                        logger.exception("[Error]: assertion failed")
                        if wukongdnc.dag.DAG_executor_constants.exit_program_on_exception:
                            logging.shutdown()
                            os._exit(0)
                    #assertOld:
                    #if self.store_fanins_faninNBs_locally:
                    #    logger.error("[Error]: FaninB: using workers and processes but storing fanins locally.")

                    # No signal of non-last client; they did not block and they are done executing. 
                    # does mutex.V
                    super().exit_monitor()
                    # no one should be calling fan_in again since this is last calle
                    return self._results, restart  # all threads have called so return results
                    #return 1, restart  # all threads have called so return results
            elif self.store_fanins_faninNBs_locally and wukongdnc.dag.DAG_executor_constants.run_all_tasks_locally:
                # Note this FaNInNBs is stored locally; using simulated lambdas
                try:
                    msg = "[Error]: FaninB: storing fanins locally but not using threads."
                    assert wukongdnc.dag.DAG_executor_constants.using_threads_not_processes , msg
                except AssertionError:
                    logger.exception("[Error]: assertion failed")
                    if wukongdnc.dag.DAG_executor_constants.exit_program_on_exception:
                        logging.shutdown()
                        os._exit(0)
                #assertOld
                #if not using_threads_not_processes:
                #    logger.error("[Error]: FaninB: storing fanins locally but not using threads.")
  
                try:
                    logger.trace("FanInNB: starting DAG_executor thread for task " + fanin_task_name + " with start state " + str(start_state_fanin_task))
                    server = kwargs['server']
                    #DAG_executor_state =  kwargs['DAG_executor_State']
                    #DAG_executor_state.state = int(start_state_fanin_task)
                    DAG_executor_state = DAG_executor_State(function_name = "DAG_executor", function_instance_ID = str(uuid.uuid4()), state = start_state_fanin_task)
                    DAG_executor_state.restart = False      # starting  new DAG_executor in state start_state_fanin_task
                    DAG_executor_state.return_value = None
                    DAG_executor_state.blocking = False
                    logger.trace("FanInNB: calling_task_name:" + calling_task_name + " DAG_executor_state.state: " + str(DAG_executor_state.state))
                    #logger.trace("DAG_executor_state.function_name: " + DAG_executor_state.function_name)
                    payload = {
                        #"state": int(start_state_fanin_task),
                        # We aer using threads to simulate lambdas. The data_dict in 
                        # this case is a global object visible to all threads. Each thread
                        # will put its results in the data_dict befoer sending the results
                        # to the FanInNB, so the fanin results collected by the FanInNB 
                        # are already available in the global data_dict. Thus, we do not 
                        # really need to put the results in the payload for the started
                        # thread (simulating a real lambda) but we do to be conistent 
                        # with real lambdas.
                        "input": self._results,
                        "DAG_executor_state": DAG_executor_state,
                        # Using threads to simulate lambdas and th threads
                        # just read DAG_info locally, we do not need to pass it 
                        # to each Lambda.
                        # passing DAG_info to be consistent with real lambdas
                        "DAG_info": self.DAG_info,
                        # server takes the place of tcp_server which the real lambdas
                        # use. server is accessibl to the lambda simulator threads as 
                        # a global variable
                        "server": server
                    }
                    thread_name_prefix = "Thread_leaf_"
                    thread = threading.Thread(target=DAG_executor.DAG_executor_task, name=(thread_name_prefix+str(start_state_fanin_task)), args=(payload,))
                    thread.start()
                    #_thread.start_new_thread(DAG_executor.DAG_executor_task, (payload,))
                except Exception:
                    logger.exception("FanInNB:[ERROR] Failed to start DAG_executor thread.")
                    if wukongdnc.dag.DAG_executor_constants.exit_program_on_exception:
                        logging.shutdown()
                        os._exit(0)

                # No signal of non-last client; they did not block and they are done executing. 
                # does mutex.V
                super().exit_monitor()
                # no one should be calling fan_in again since this is last caller
                #Note: we are not using workers so we do not return a work tuple
                return self._results, restart  # all threads have called so return results
                #return 1, restart  # all threads have called so return results
                    
            elif not self.store_fanins_faninNBs_locally and not wukongdnc.dag.DAG_executor_constants.run_all_tasks_locally:
                # using real lambdas to execure DAGs Wukong style
#rhc: run task
                # Note: we are store_sync_objects_in_lambdas so usually we would not 
                # use a non-select faninNB in this case as we use select faninNBs when 
                # we store sync objects in Lambdas. But if the non-select sync object
                # triggers its fanin task, then this Lambda will be invoked 
                # (by the DAG_orchestrator) only one time so we can use the non-select
                # fanin. It is also the case that fan_in operations never block so we
                # do not have to worry about a proxy thread blocking during a 
                # fan_in op, which would prebent the Lmabda from terminatng (which 
                # is why we have the select version of FanInNB.)
                if wukongdnc.dag.DAG_executor_constants.store_sync_objects_in_lambdas and wukongdnc.dag.DAG_executor_constants.sync_objects_in_lambdas_trigger_their_tasks:
                    try:
                        logger.info("DAG_executor_FanInNB_Select: triggering DAG_Executor_Lambda() for task " + fanin_task_name)
                        lambda_DAG_exec_state = DAG_executor_State(function_name = "WukongDivideAndConquer:"+fanin_task_name, function_instance_ID = str(uuid.uuid4()), state = start_state_fanin_task)
                        logger.info ("DAG_executor_FanInNB_Select: lambda payload is DAG_info + " + str(start_state_fanin_task) + "," + str(self._results))
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
                    except Exception:
                        logger.exception("[ERROR] DAG_executor_FanInNB_Select: Failed to start DAG_executor.DAG_executor_lambda" \
                            + " for triggered task " + fanin_task_name)
                        if wukongdnc.dag.DAG_executor_constants.exit_program_on_exception:
                            logging.shutdown()
                            os._exit(0)
                else:      
                    try:
                        DAG_executor_state = DAG_executor_State(function_name = "WukongDivideAndConquer:"+fanin_task_name, function_instance_ID = str(uuid.uuid4()), state = start_state_fanin_task)
                        DAG_executor_state.restart = False      # starting  new DAG_executor in state start_state_fanin_task
                        DAG_executor_state.return_value = self._results
                        DAG_executor_state.blocking = False            
                        logger.trace("FanInNB: starting Lambda function for task " + fanin_task_name + " with start state " + str(DAG_executor_state.state))
                        try:
                            msg = "FanInNB: fanin_task_name:" + fanin_task_name + " DAG_info is None for Lambda start."
                            assert self.DAG_info is not None , msg
                        except AssertionError:
                            logger.exception("[Error]: assertion failed")
                            if wukongdnc.dag.DAG_executor_constants.exit_program_on_exception:
                                logging.shutdown()
                                os._exit(0)
                        #assertOld:
                        #if self.DAG_info is None:
                        #    logger.error("[Error]: FanInNB: fanin_task_name:" + fanin_task_name + " DAG_info is None for Lambda start.")
                        #else:
                        logger.trace("FanInNB: fanin_task_name:" + fanin_task_name + " DAG_info is NOT None for Lambda start.")

                        #logger.trace("DAG_executor_state: " + str(DAG_executor_state))
                        payload = {
                            #"state": int(start_state_fanin_task),
                            "input": self._results,
                            "DAG_executor_state": DAG_executor_state,
                            "DAG_info": self.DAG_info
                            #"server": server   # used to mock server during testing; we use tcp_server with ral lambdas
                        }

                        if wukongdnc.dag.DAG_executor_constants.input_all_groups_partitions_at_start:
                            # add the groups_partitions to payload 
                            payload['groups_partitions'] = self.groups_partitions 
                        
                        ###### DAG_executor_State.function_name has not changed
                        invoke_lambda_DAG_executor(payload = payload, function_name = "WukongDivideAndConquer:"+fanin_task_name)
                    except Exception:
                        logger.exception("FanInNB:[ERROR] Failed to start DAG_executor Lambda.")
                        if wukongdnc.dag.DAG_executor_constants.exit_program_on_exception:
                            logging.shutdown()
                            os._exit(0)

                    # No signal of non-last client; they did not block and they are done executing. 
                    # does mutex.V
                    super().exit_monitor()
                    # no one should be calling fan_in again since this is last caller
                    # results given to invoked lambda so nothing to return; can't return results
                    # to tcp_serve \r or tcp_server might try to put them in the non-existent 
                    # work_queue.   
                    #return self._results, restart  # all threads have called so return results
                    #Note: we are not using workers so we do not return a work tuple
                    return 0, restart
                    #return 1, restart  # all threads have called so return results
           
            elif not self.store_fanins_faninNBs_locally and wukongdnc.dag.DAG_executor_constants.run_all_tasks_locally:
                # not using workers and using threads to simulate lambdas. Here
                # there is nothing to do since a thread will be created localy
                # in DAG work loop. (Can't create threads here or it would run here
                # (on server or in lambda))
                
                #Note: we are not using workers so we do not return a work tuple
                # We check return_value == 0 to determine whether we need to
                # start a thread to do fanin tas, i.e., to determine whether
                # we become the fanin task, so we return self._results, even though
                # we will not use these results - this is inefficient but in this case
                # we are simulating lambdas with threads, which is just to test the 
                # logic witout worrying about performance.
                #return 0, restart
                return self._results, restart

            else:
                # This should be unreachable
                try:
                    msg = "[ERROR]: FanInNB: reached unreachable else: error at end of fanin"
                    assert False , msg
                except AssertionError:
                    logger.exception("[Error]: assertion failed")
                    if wukongdnc.dag.DAG_executor_constants.exit_program_on_exception:
                        logging.shutdown()
                        os._exit(0)
                #assertOld:
                #   logger.error("[Error]: FanInNB: reached else: error at end of fanin")

            # No signal of non-last client; they did not block and they are done executing. 
            # does mutex.V
            #super().exit_monitor()
            
            #return self._results, restart  # all threads have called so return results
            #return 1, restart  # all threads have called so return results
        
        #No logger.debugs here. main Client can exit while other threads are
        #doing this logger.trace so main thread/interpreter can't get stdout lock?

# Local tests  
#def task1(b : FanIn):
    #time.sleep(1)
    #logger.trace("task 1 Calling fan_in")
    #result = b.fan_in(ID = "task 1", result = "task1 result")
    #logger.trace("task 1 Successfully called fan_in")
    #if result == 0:
    #    logger.trace("result is o")
    #else:
        #result is a list, logger.trace it

#def task2(b : FanIn):
    #logger.trace("task 2 Calling fan_in")
    #result = b.fan_in(ID = "task 2", result = "task2 result")
    #logger.trace("task 2  Successfully called fan_in")
    #if result == 0:
    #    logger.trace("result is o")
    #else:
        #result is a list, logger.trace it

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
        logger.trace("task " + self._ID + " Calling fan_in")
        r = self.b.fan_in(ID = self._ID, result = "task1 result")
        logger.trace("task " + self._ID + ", Successfully called fan_in, returned r:" + r)

def main():
    b = DAG_executor_FanInNB(monitor_name="DAG_executor_FanInNB")
    b.init(**{"n": 2})

    #try:
    #    logger.trace("Starting thread 1")
    #   _thread.start_new_thread(task1, (b,))
    #except Exception as ex:
    #    logger.trace("[ERROR] Failed to start first thread.")
    #    logger.trace(ex)
    
    try:
        callerThread1 = testThread("T1", b)
        callerThread1.start()
    except Exception as ex:
        logger.exception("[ERROR] Failed to start first thread.")
        logger.exception(ex)      

    #try:
    #    logger.trace("Starting first thread")
    #    _thread.start_new_thread(task2, (b,))
    #except Exception as ex:
    #   logger.trace("[ERROR] Failed to start first thread.")
    #    logger.trace(ex)
    
    try:
        callerThread2 = testThread("T2", b)
        callerThread2.start()
    except Exception as ex:
        logger.exception("[ERROR] Failed to start second thread.")
        logger.exception(ex)
        
    callerThread1.join()
    callerThread2.join()
    
    logger.trace("joined threads")
    logger.trace("callerThread1 restart " + str(callerThread1._restart))
    logger.trace("callerThread2._returnValue=" + str(callerThread1._return))

    logger.trace("callerThread2 restart " + str(callerThread2._restart))
    logger.trace("callerThread2._returnValue=" + str(callerThread2._return))
    # if callerThread2._result == 0:
    #     logger.trace("callerThread2 result is 0")
    # else:
    #     #result is a list, logger.trace it
        
if __name__=="__main__":
    main()