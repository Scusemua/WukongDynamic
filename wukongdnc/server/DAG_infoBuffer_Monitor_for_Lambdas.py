from .monitor_su import MonitorSU
import _thread
import threading
import time
import uuid
from ..dag.DAG_executor_constants import run_all_tasks_locally
from ..dag.DAG_executor_State import DAG_executor_State
#from ..dag.DAG_executor import DAG_executor
#from wukongdnc.dag import DAG_executor
# needs to invoke method DAG_executor()
import wukongdnc.dag.DAG_executor
# Note: avoiding circular imports:
# https://stackoverflow.com/questions/744373/what-happens-when-using-mutual-or-circular-cyclic-imports

from wukongdnc.wukong.invoker import invoke_lambda_DAG_executor

import logging 

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(asctime)s] [%(module)s] [%(processName)s] [%(threadName)s]: %(message)s')
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)
logger.addHandler(ch)

# Controls workers when using incremental DAG generation
class DAG_infoBuffer_Monitor_for_Lambdas(MonitorSU):
    def __init__(self, monitor_name = "DAG_infoBuffer_Monitor_for_Lambdas"):
        super(DAG_infoBuffer_Monitor_for_Lambdas, self).__init__(monitor_name=monitor_name)
        # For testing, if we havn't called init() then version number will be 1
        self.current_version_DAG_info = None
        self.current_version_number_DAG_info = 1
#rhc leaf tasks
        # The initial DAG has the initial leaf task(s) in it. As later we find
        # more leaf tasks (tht start new connected components), we supply them 
        # with the DAG so the leaf tasks can be aded to the work_queue and
        # and executed by workers (when we are using workers).
        self.current_version_new_leaf_tasks = []
#rhc: lambda inc:
        #self._next_version=super().get_condition_variable(condition_name="_next_version")
#rhc: lambda inc
        #self._capacity = kwargs["n"]
        # Need there to be an element at buffer[i]. Cannot use insert() since it will shift elements down.
        # If we set buffer[0] we need an element to be at position 0 or we get an out of range error.
        self._buffer=[]
        #self._buffer= [None] * self._capacity
        #self._in=0
        #logger.info(kwargs)

    #def init(self, **kwargs):
    def init(self,**kwargs):
        pass
        # initialize with a DAG_info object. This will be version 1 of the DAG
        #self.current_version_DAG_info = kwargs['current_version_DAG_info']
        #self.current_version_number_DAG_info = self.current_version_DAG_info.get_version_number()
#rhc leaf tasks
        # The initial DAG has the initial leaf task(s) in it. As later we find
        # more leaf tasks (tht start new connected components), we supply them 
        # with the DAG so the leaf tasks can be aded to the work_queue and
        # and executed by workers (when we aer using workers).
        #self.current_version_new_leaf_tasks = []
        # logger.info(kwargs)


    def print_DAG_info(self,DAG_info):
        DAG_map = DAG_info.get_DAG_map()
        #all_fanin_task_names = DAG_info.get_all_fanin_task_names()
        #all_fanin_sizes = DAG_info.get_all_fanin_sizes()
        #all_faninNB_task_names = DAG_info.get_all_faninNB_task_names()
        #all_faninNB_sizes = DAG_info.get_all_faninNB_sizes()
        #all_fanout_task_names = DAG_info.get_all_fanout_task_names()
        #all_collapse_task_nams = DAG_info.get_all_collapse_task_names()
        # Note: all fanout_sizes is not needed since fanouts are fanins that have size 1
        DAG_states = DAG_info.get_DAG_states()
        DAG_leaf_tasks = DAG_info.get_DAG_leaf_tasks()
        DAG_leaf_task_start_states = DAG_info.get_DAG_leaf_task_start_states()
        DAG_tasks = DAG_info.get_DAG_tasks()
        DAG_leaf_task_inputs = DAG_info.get_DAG_leaf_task_inputs()

        print("DAG_infoBuffer_Monitor_for_Lambdas: DAG_map:")
        for key, value in DAG_map.items():
            print(key)
            print(value)
        print("  ")
        print("DAG_infoBuffer_Monitor_for_Lambdas: DAG states:")         
        for key, value in DAG_states.items():
            print(key)
            print(value)
        print("   ")
        print("DAG_infoBuffer_Monitor_for_Lambdas: DAG leaf task start states")
        for start_state in DAG_leaf_task_start_states:
            print(start_state)
        print()
        print("DAG_infoBuffer_Monitor_for_Lambdas: DAG_tasks:")
        for key, value in DAG_tasks.items():
            print(key, ' : ', value)
        print()
        print("DAG_infoBuffer_Monitor_for_Lambdas: DAG_leaf_tasks:")
        for task_name in DAG_leaf_tasks:
            print(task_name)
        print() 
        print("DAG_infoBuffer_Monitor_for_Lambdas: DAG_leaf_task_inputs:")
        for inp in DAG_leaf_task_inputs:
            print(inp)
        #print() 
        print()
        print("DAG_infoBuffer_Monitor_for_Lambdas: DAG_version_number:")
        print(DAG_info.get_DAG_version_number())
        print("DAG_infoBuffer_Monitor_for_Lambdas: DAG_info_is_complete:")
        print(DAG_info.get_DAG_info_is_complete())
        print()

    def get_current_version_number_DAG_info(self):
        try:
            super(DAG_infoBuffer_Monitor_for_Lambdas, self).enter_monitor(method_name="get_current_version_number_DAG_info")
        except Exception as ex:
            logger.error("[ERROR]: DAG_infoBuffer_Monitor_for_Lambdas:  Failed super(DAG_infoBuffer, self)")
            logger.error("[ERROR] self: " + str(self.__class__.__name__))
            logger.debug(ex)
            return 0
        
#rhc: lambda inc
        #logger.debug("DAG_infoBuffer_Monitor_for_Lambdas: get_current_version_number_DAG_info() entered monitor, len(self._new_version) ="+str(len(self._next_version)))
        logger.debug("DAG_infoBuffer_Monitor_for_Lambdas: get_current_version_number_DAG_info() entered monitor")

        restart = False
        current_DAG_info = self.current_version_DAG_info
        super().exit_monitor()
        return current_DAG_info, restart

    def deposit(self,**kwargs):
        # deposit a new DAG_info object. It's version number will be one more
        # than the current DAG_info object.
        # Wake up any workes waiting for the next version of the DAG. Workers
        # that finish the current version i of the DAG will wait for the next 
        # version i+1. Note: a worker may be finishing an earlier versio of 
        # the DAG. When they request a nwe version, it may be the current
        # version or an older version that they are requesting, We will give
        # them the current version, which may be newer than the version they
        # requsted. This is fine. We assume tha tthe DAG grows incrementally
        # and we add new states but do not delete old states from the DAG.
        try:
            super(DAG_infoBuffer_Monitor_for_Lambdas, self).enter_monitor(method_name="deposit")
        except Exception as ex:
            logger.error("[ERROR]:DAG_infoBuffer_Monitor_for_Lambdas: Failed super(DAG_infoBuffer_Monitor_for_Lambdas, self)")
            logger.error("[ERROR] self: " + str(self.__class__.__name__))
            logger.debug(ex)
            return 0

#rhc: lambda inc
        #logger.debug("DAG_infoBuffer_Monitor_for_Lambdas: deposit() entered monitor, len(self._new_version) ="+str(len(self._next_version)))
        logger.debug("DAG_infoBuffer_Monitor_for_Lambdas: deposit() entered monitor")
        self.current_version_DAG_info = kwargs['new_current_version_DAG_info']
        self.current_version_number_DAG_info = self.current_version_DAG_info.get_DAG_version_number()
#rhc leaf tasks
        new_leaf_tasks = kwargs['new_current_version_new_leaf_tasks']
        self.current_version_new_leaf_tasks += new_leaf_tasks
        logger.debug("DAG_infoBuffer_Monitor_for_Lambdas: DAG_info deposited: ")
        self.print_DAG_info(self.current_version_DAG_info)

#rhc leaf tasks
        logger.debug("DAG_infoBuffer_Monitor_for_Lambdas: new leaf task states deposited: ")
        for work_tuple in new_leaf_tasks:
            leaf_task_state = work_tuple[0]
            logger.debug(str(leaf_task_state))
        logger.debug("DAG_infoBuffer_Monitor_for_Lambdas: cumulative leaf task states deposited: ")
        for work_tuple in self.current_version_new_leaf_tasks:
            leaf_task_state = work_tuple[0]
            logger.debug(str(leaf_task_state))

        restart = False

#rhc: lanbda inc: start lambdas for all continued states in buffer and leaf tasks
        #self._next_version.signal_c_and_exit_monitor()


        if run_all_tasks_locally:
            # not using real lambdas
            try:
                for start_tuple in self._buffer:
                    # pass the state/task the thread is to execute at the start of its DFS path
                    start_state = start_tuple[0]
                    # Note: for incremental DAG generation, when we restart a lambda
                    # for a continued task, if the task is a group, we give it the output
                    # it generated previously (before terminating) sand use the output
                    # to do the group's/task's fanins/fanout. If the task is a partition,
                    # we give the partition the input for its execution. 
                    #(Tricky: after executing a group with TBC fanins/fanout/collpases,
                    # if it has TBC fanouts/faninNBs/fanins, we cannot do any of them so we
                    # have to wait until we get a new DAG and restart the group/task
                    # so we can complete the fanouts/faninNBs/fanins. After excuting 
                    # a partition the partition can only have a collapse, i.e., we know
                    # we must execute the colapse task. So when we restart the partition/task,
                    # we supply the input for the collpase task and execute the collapse task.)
                    input_or_output = start_tuple[1]
                    DAG_exec_state = DAG_executor_State(function_name = "DAG_executor", function_instance_ID = str(uuid.uuid4()), state = start_state, continued_task = True)
                    DAG_exec_state.restart = False      # starting  new DAG_executor in state start_state_fanin_task
                    DAG_exec_state.return_value = None
                    DAG_exec_state.blocking = False
                    logger.debug("DAG_infoBuffer_Monitor_for_Lambdas: starting lambda with DAG_executor_state.state: " + str(DAG_exec_state.state)
                        + " continued task: " + str(DAG_exec_state.continued_task))
                    #logger.debug("DAG_executor_state.function_name: " + DAG_executor_state.function_name)
                    payload = {
                        #"state": int(start_state_fanin_task),
                        # We are using threads to simulate lambdas. The data_dict in 
                        # this case is a global object visible to all threads. Each thread
                        # will put its results in the data_dict befoer sending the results
                        # to the FanInNB, so the fanin results collected by the FanInNB 
                        # are already available in the global data_dict. Thus, we do not 
                        # really need to put the results in the payload for the started
                        # thread (simulating a real lambda) but we do to be conistent 
                        # with real lambdas.
                        "input": input_or_output,
                        "DAG_executor_state": DAG_exec_state,
                        # Using threads to simulate lambdas and th threads
                        # just read DAG_info locally, we do not need to pass it 
                        # to each Lambda.
                        # passing DAG_info to be consistent with real lambdas
                        "DAG_info": self.current_version_DAG_info,
                        # server takes the place of tcp_server which the real lambdas
                        # use. server is accessibl to the lambda simulator threads as 
                        # a global variable
                        #"server": server
                    }
                    # Note:
                    # get the current thread instance
                    #    thread = current_thread()
                    # report the name of the thread
                    #    print(thread.name)
                    thread_name_prefix = "Thread_leaf_"
                    thread = threading.Thread(target=wukongdnc.dag.DAG_executor.DAG_executor_task, name=(thread_name_prefix+"ss"+str(start_state)), args=(payload,))
                    thread.start()
                self._buffer.clear()

                for work_tuple in self.current_version_new_leaf_tasks:
                    # pass the state/task the thread is to execute at the start of its DFS path
                    start_state = work_tuple[0]
                    # leaf tasks have no inputs. 
                    # Note: for incremental DAG generation, when we restart a lambda
                    # for a continued task, if the task is a group, we give it the output
                    # it generated previously (before terminating) sand use the output
                    # to do the group's/task's fanins/fanout. If the task is a partition,
                    # we give the partition the input for its execution. 
                    #(Tricky: after executing a group with TBC fanins/fanout/collpases,
                    # if it has TBC fanouts/faninNBs/fanins, we cannot do any of them so we
                    # have to wait until we get a new DAG and restart the group/task
                    # so we can complete the fanouts/faninNBs/fanins. After excuting 
                    # a partition the partition can only have a collapse, i.e., we know
                    # we must execute the colapse task. So when we restart the partition/task,
                    # we supply the input for the collpase task and execute the collapse task.)
                    input_or_output = [] 
                    DAG_exec_state = DAG_executor_State(function_name = "DAG_executor", function_instance_ID = str(uuid.uuid4()), state = start_state, continued_task = True)
                    DAG_exec_state.restart = False      # starting  new DAG_executor in state start_state_fanin_task
                    DAG_exec_state.return_value = None
                    DAG_exec_state.blocking = False
                    logger.debug("DAG_infoBuffer_Monitor_for_Lambdas: starting lambda with DAG_executor_state.state:" + str(DAG_exec_state.state)
                        + " continued task: " + str(DAG_exec_state.continued_task))
                    #logger.debug("DAG_executor_state.function_name: " + DAG_executor_state.function_name)
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
                        "input": input_or_output, # will be [] for leaf task
                        "DAG_executor_state": DAG_exec_state,
                        # Using threads to simulate lambdas and th threads
                        # just read DAG_info locally, we do not need to pass it 
                        # to each Lambda.
                        # passing DAG_info to be consistent with real lambdas
                        "DAG_info": self.current_version_DAG_info,
                        # server takes the place of tcp_server which the real lambdas
                        # use. server is accessibl to the lambda simulator threads as 
                        # a global variable
                        #"server": server
                    }
                    # Note:
                    # get the current thread instance
                    #    thread = current_thread()
                    # report the name of the thread
                    #    print(thread.name)
                    thread_name_prefix = "Thread_leaf_"
                    thread = threading.Thread(target=wukongdnc.dag.DAG_executor.DAG_executor_task, name=(thread_name_prefix+"ss"+str(start_state)), args=(payload,))
                    thread.start()
                self.current_version_new_leaf_tasks.clear()
            except Exception as ex:
                logger.debug("[ERROR] DAG_executor_driver: Failed to start DAG_executor thread for state " + str(start_state))
                logger.debug(ex)
        else:
            # using real lambdas
            try:
                for start_tuple in self._buffer:
                    # pass the state/task the thread is to execute at the start of its DFS path
                    start_state = start_tuple[0]
                    # Note: for incremental DAG generation, when we restart a lambda
                    # for a continued task, if the task is a group, we give it the output
                    # it generated previously (before terminating) sand use the output
                    # to do the group's/task's fanins/fanout. If the task is a partition,
                    # we give the partition the input for its execution. 
                    #(Tricky: after executing a group with TBC fanins/fanout/collpases,
                    # if it has TBC fanouts/faninNBs/fanins, we cannot do any of them so we
                    # have to wait until we get a new DAG and restart the group/task
                    # so we can complete the fanouts/faninNBs/fanins. After excuting 
                    # a partition the partition can only have a collapse, i.e., we know
                    # we must execute the colapse task. So when we restart the partition/task,
                    # we supply the input for the collpase task and execute the collapse task.)
                    input_or_output = start_tuple[1]
                    DAG_exec_state = DAG_executor_State(function_name = "DAG_executor.DAG_executor_lambda", function_instance_ID = str(uuid.uuid4()), state = start_state, continued_task = True)
                    DAG_exec_state.restart = False      # starting  new DAG_executor in state start_state_fanin_task
                    DAG_exec_state.return_value = None
                    DAG_exec_state.blocking = False
                    logger.debug("DAG_infoBuffer_Monitor_for_Lambdas: starting lambda with DAG_executor_state.state: " + str(DAG_exec_state.state)
                        + " continued task: " + str(DAG_exec_state.continued_task))
                    #logger.debug("DAG_executor_state.function_name: " + DAG_executor_state.function_name)

                    payload = {
                        #"state": int(start_state_fanin_task),
                        # We are using threads to simulate lambdas. The data_dict in 
                        # this case is a global object visible to all threads. Each thread
                        # will put its results in the data_dict befoer sending the results
                        # to the FanInNB, so the fanin results collected by the FanInNB 
                        # are already available in the global data_dict. Thus, we do not 
                        # really need to put the results in the payload for the started
                        # thread (simulating a real lambda) but we do to be conistent 
                        # with real lambdas.
                        "input": input_or_output,
                        "DAG_executor_state": DAG_exec_state,
                        # Using threads to simulate lambdas and th threads
                        # just read DAG_info locally, we do not need to pass it 
                        # to each Lambda.
                        # passing DAG_info to be consistent with real lambdas
                        "DAG_info": self.current_version_DAG_info,
                        # server takes the place of tcp_server which the real lambdas
                        # use. server is accessibl to the lambda simulator threads as 
                        # a global variable
                        #"server": server
                    }
                    invoke_lambda_DAG_executor(payload = payload, function_name = "DAG_Executor_Lambda")
                self._buffer.clear()

                for work_tuple in self.current_version_new_leaf_tasks:
                    # pass the state/task the thread is to execute at the start of its DFS path
                    start_state = work_tuple[0]
                    # leaf tasks have no inputs. 
                    # Note: for incremental DAG generation, when we restart a lambda
                    # for a continued task, if the task is a group, we give it the output
                    # it generated previously (before terminating) sand use the output
                    # to do the group's/task's fanins/fanout. If the task is a partition,
                    # we give the partition the input for its execution. 
                    #(Tricky: after executing a group with TBC fanins/fanout/collpases,
                    # if it has TBC fanouts/faninNBs/fanins, we cannot do any of them so we
                    # have to wait until we get a new DAG and restart the group/task
                    # so we can complete the fanouts/faninNBs/fanins. After excuting 
                    # a partition the partition can only have a collapse, i.e., we know
                    # we must execute the colapse task. So when we restart the partition/task,
                    # we supply the input for the collpase task and execute the collapse task.)
                    input_or_output = [] 
                    DAG_exec_state = DAG_executor_State(function_name = "DAG_executor.DAG_executor_lambda", function_instance_ID = str(uuid.uuid4()), state = start_state, continued_task = True)
                    DAG_exec_state.restart = False      # starting  new DAG_executor in state start_state_fanin_task
                    DAG_exec_state.return_value = None
                    DAG_exec_state.blocking = False
                    logger.debug("DAG_infoBuffer_Monitor_for_Lambdas: starting lambda with DAG_executor_state.state:" + str(DAG_exec_state.state)
                        + " continued task: " + str(DAG_exec_state.continued_task))
                    #logger.debug("DAG_executor_state.function_name: " + DAG_executor_state.function_name)
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
                        "input": input_or_output, # will be [] for leaf task
                        "DAG_executor_state": DAG_exec_state,
                        # Using threads to simulate lambdas and th threads
                        # just read DAG_info locally, we do not need to pass it 
                        # to each Lambda.
                        # passing DAG_info to be consistent with real lambdas
                        "DAG_info": self.current_version_DAG_info,
                        # server takes the place of tcp_server which the real lambdas
                        # use. server is accessibl to the lambda simulator threads as 
                        # a global variable
                        #"server": server
                    }
                    invoke_lambda_DAG_executor(payload = payload, function_name = "DAG_Executor_Lambda")
                self.current_version_new_leaf_tasks.clear()
            except Exception as ex:
                logger.debug("[ERROR] DAG_executor_driver: Failed to start DAG_executor thread for state " + str(start_state))
                logger.debug(ex)
        try:
            super(DAG_infoBuffer_Monitor_for_Lambdas, self).exit_monitor()
        except Exception as ex:
            logger.error("[ERROR]:DAG_infoBuffer_Monitor_for_Lambdas: Failed super(DAG_infoBuffer_Monitor_for_Lambdas, self)")
            logger.error("[ERROR] self: " + str(self.__class__.__name__))
            logger.debug(ex)
            return 0
        
        return 0, restart
    
    """
    where after withdraw we put leaf tasks in work queue
        logger.debug("calling withdraw:")
        DAG_info_and_new_leaf_task_states_tuple = DAG_infobuffer_monitor.withdraw(requested_current_version_number)
        new_DAG_info = DAG_info_and_new_leaf_task_states_tuple[0]
        new_leaf_task_states = DAG_info_and_new_leaf_task_states_tuple[1]
    #rhc leaf tasks
        logger.debug("DAG_executor_work_loop: cumulative leaf task states withdrawn and added to work_queue: ")
        for work_tuple in new_leaf_task_states:
            leaf_task_state = work_tuple[0]
            logger.debug(str(leaf_task_state))
            work_queue.put(work_tuple)
    """

    def withdraw(self, **kwargs):
        # request a new version of the DAG. A worker that finishes version 
        # i will request i+1. Noet that i+1 may <= current_version. If so
        # return the current version. If not, then the worker is requesting
        # the next version of the DAG, which hasn't been generated yet.
        super().enter_monitor(method_name = "withdraw")
        requested_current_version_number = kwargs['requested_current_version_number']
#rhc: lambad inc
        #logger.debug("DAG_infoBuffer_Monitor_for_Lambdas: withdraw() entered monitor, requested_current_version_number = "
        #    + str(requested_current_version_number) + " len(self._next_version) = " + str(len(self._next_version)))
        logger.debug("DAG_infoBuffer_Monitor_for_Lambdas: withdraw() entered monitor, requested_current_version_number = "
            + str(requested_current_version_number))

        DAG_info = None
        restart = False
        if requested_current_version_number <= self.current_version_number_DAG_info:
            DAG_info = self.current_version_DAG_info
#rhc leaf tasks

#rhc: lambda inc: Not returning leaf tasks, deposit is starting them
            #new_leaf_task_states = copy.copy(self.current_version_new_leaf_tasks)
            #self.current_version_new_leaf_tasks.clear()
            #logger.debug("DAG_infoBuffer_Monitor_for_Lambdas: withdraw: got DAG_info with version number " 
            #    + str(DAG_info.get_DAG_version_number()))
             
            # Note: This is disabled so that we do not try to iterate
            # over the dictionaries in current_version_DAG_info while
            # the ADG generator is changing these dictionaries after
            # having deposited current_version_DAG_info. If we iterate
            # while the dictiionary is being changed we can get a 
            # RUNTIME error saying the sixe of the dictionary changed
            # during iteration. We do print the version number of 
            # current_version_DAG_info so we can match the 
            # current_version_DAG_info with the versions printed by
            # deposit.
            #logger.debug("DAG_infoBuffer_Monitor_for_Lambdas: DAG_info withdrawn: ")
            #self.print_DAG_info(self.current_version_DAG_info)
#rhc leaf tasks

#rhc lambda inc: not returning leaf task states, deposit is starting them
            #logger.debug("DAG_infoBuffer_Monitor_for_Lambdas: withdraw: new leaf task states returned: ")
            #for work_tuple in new_leaf_task_states:
            #    leaf_task_state = work_tuple[0]
            #    logger.debug(str(leaf_task_state))

            super().exit_monitor()
#rhc leaf tasks

#rhc: lambda inc: not returning leaf task states, deposit is starting them
            #DAG_info_and_new_leaf_task_states_tuple = (DAG_info,new_leaf_task_states)
            ##return DAG_info, new_leaf_task_states, restart
#rhc: lambda inc: 
            #return DAG_info_and_new_leaf_task_states_tuple, restart
            return DAG_info, restart
        else:

#rhc: lambda inc: not waiting for DAG, stopping then restartd by deposit
            #logger.debug("DAG_infoBuffer_Monitor_for_Lambdas: withdraw waiting for version " + str(requested_current_version_number))
            #self._next_version.wait_c()

#rhc: lambda inc: returning None 
            #DAG_info = self.current_version_DAG_info
#rhc leaf tasks
            #new_leaf_task_states = copy.copy(self.current_version_new_leaf_tasks)
            #self.current_version_new_leaf_tasks.clear()

            # cascaded wakeup, i.e., if there are more than one worker waiting,
            # the deposit() will wakeup the first worker with its
            # signal_c_and_exit_monitor(). The firsy waitng worker will wakeup
            # the second worker here with signal_c_and_exit_monitor(); the 
            # secnd workers will wakeup the third worker wit signal_c_and_exit_monitor()
            # etc. Note that all calls to signal are using signal_c_and_exit_monitor()
            # So no worker that signals another worker will wait to reenter the monitor;
            # instead, the signalling worker will just return, The last waiting worker will 
            # call signal_c_and_exit_monitor() and since no workers are waiting on 
            # the condition or to reenter the monitor, this will have no effect 
            # other than to release mutual exclusion. Note: Any calls to deposit()
            # for verson i+1 after a previous call to deposit() for version i
            # will have to wait for all the workers who were waiting for version i 
            # when the first deposit() occurred to return with version i, before the 
            # call to deposit with version i+1 can start. These workers will just
            # make a later call to withdraw to get version i+1, but we don't
            # expect deposits to occur so soon after each other.
            #
            # Note: We could use an SC monitor in which case the second deposit
            # might be allowed to enter the monitor before waiting workers, in 
            # which case the workers would get verson i+1, which is not bad.

#rhc: lambda inc: returning None
            #logger.debug("DAG_infoBuffer_Monitor_for_Lambdas: withdraw: got DAG_info with version number " 
            #    + str(DAG_info.get_DAG_version_number()))
            ##logger.debug("DAG_infoBuffer_Monitor_for_Lambdas: DAG_info withdrawn: ")
            ##self.print_DAG_info(self.current_version_DAG_info)
#rhc leaf tasks
            #logger.debug("DAG_infoBuffer_Monitor_for_Lambdas: withdraw: new leaf task states to return: ")
            #for work_tuple in new_leaf_task_states:
            #    leaf_task_state = work_tuple[0]
            #    logger.debug(str(leaf_task_state))

#rhc: lambda inc: no waiting, stop and then retarted later
            #self._next_version.signal_c_and_exit_monitor()
#rhc leaf tasks
            #DAG_info_and_new_leaf_task_states_tuple = (DAG_info,new_leaf_task_states)
            #return DAG_info, new_leaf_task_states, restart

#rhc: lambda inc
            value = kwargs["value"]
            logger.debug("Value to deposit: " + str(value))
            self._buffer.append(value)
            #self._buffer.insert(self._in,value)
            #self._buffer[self._in] = value
            #self._in=(self._in+1) % int(self._capacity)
            
            logger.debug("DAG_infoBuffer_Monitor_for_Lambdas: return None")
#rhc: lambda inc
            #return DAG_info_and_new_leaf_task_states_tuple, restart
            return None, restart
        
        try:
            super(DAG_infoBuffer_Monitor_for_Lambdas, self).exit_monitor()
        except Exception as ex:
            logger.error("[ERROR]:DAG_infoBuffer_Monitor_for_Lambdas: Failed super(DAG_infoBuffer_Monitor_for_Lambdas, self)")
            logger.error("[ERROR] self: " + str(self.__class__.__name__))
            logger.debug(ex)
            return 0
        


class Dummy_DAG_info:
    def __init__(self,value,version_number):
        self.value = value
        self.version_number = version_number

    def get_value(self):
        return self.value
    def get_version_number(self):
        return self.version_number

#Local tests
def taskD(b : DAG_infoBuffer_Monitor_for_Lambdas):
    time.sleep(3)
    DAG_info = Dummy_DAG_info("DAG_info2",2)
    keyword_arguments = {}
    keyword_arguments['new_current_version_DAG_info'] = DAG_info
    logger.debug("taskD Calling withdraw")
    b.deposit(**keyword_arguments)
    logger.debug("Successfully called deposit version 2")

def taskW1(b : DAG_infoBuffer_Monitor_for_Lambdas):
    logger.debug("taskW1 Calling withdraw")
    keyword_arguments = {}
    keyword_arguments['requested_current_version_number'] = 1
    DAG_info, restart = b.withdraw(**keyword_arguments)
    logger.debug("Successfully called withdraw, ret is " 
        + str(DAG_info.get_value()) + "," + str(DAG_info.get_version_number())
        + " restart " + str(restart))

def taskW2(b : DAG_infoBuffer_Monitor_for_Lambdas):
    logger.debug("taskW2 Calling withdraw")
    keyword_arguments = {}
    keyword_arguments['requested_current_version_number'] = 2
    DAG_info, restart = b.withdraw(**keyword_arguments)
    logger.debug("Successfully called withdraw, ret is " 
        + str(DAG_info.get_value()) + "," + str(DAG_info.get_version_number())
        + " restart " + str(restart))

def main(): 
    b = DAG_infoBuffer_Monitor_for_Lambdas(monitor_name="DAG_infoBuffer_Monitor_for_Lambdas")
    DAG_info = Dummy_DAG_info("DAG_info1",1)
    keyword_arguments = {}
    keyword_arguments['current_version_DAG_info'] = DAG_info
    b.init(**keyword_arguments)
    try:
        logger.debug("Starting D thread")
        _thread.start_new_thread(taskD, (b,))
    except Exception as ex:
        logger.debug("[ERROR] Failed to start first thread.")
        logger.debug(ex)

    try:
        logger.debug("Starting taskW1 thread")
        _thread.start_new_thread(taskW1, (b,))
    except Exception as ex:
        logger.debug("[ERROR] Failed to start taskW1 thread.")
        logger.debug(ex)

    try:
        logger.debug("Starting first taskW2 thread")
        _thread.start_new_thread(taskW2, (b,))
    except Exception as ex:
        logger.debug("[ERROR] Failed to start first taskW2 thread.")
        logger.debug(ex)

    try:
        logger.debug("Starting second taskW2 thread")
        _thread.start_new_thread(taskW2, (b,))
    except Exception as ex:
        logger.debug("[ERROR] Failed to start second taskW2 thread.")
        logger.debug(ex)

    time.sleep(4)
    logger.debug("Done sleeping")

if __name__=="__main__":
     main()
