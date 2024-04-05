from .monitor_su import MonitorSU
import _thread
import threading
import time
import uuid
import os

#from ..dag.DAG_executor_constants import RUN_ALL_TASKS_LOCALLY
#from ..dag.DAG_executor_constants import exit_program_on_exception
#import wukongdnc.dag.DAG_executor_constants
from ..dag import DAG_executor_constants

from ..dag.DAG_executor_State import DAG_executor_State
#from ..dag.DAG_executor import DAG_executor
#from wukongdnc.dag import DAG_executor
# needs to invoke method DAG_executor()
#import wukongdnc.dag.DAG_executor
from ..dag import DAG_executor
# Note: avoiding circular imports:
# https://stackoverflow.com/questions/744373/what-happens-when-using-mutual-or-circular-cyclic-imports

#from wukongdnc.wukong.invoker import invoke_lambda_DAG_executor
from ..wukong.invoker import invoke_lambda_DAG_executor

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

# Controls workers when using incremental DAG generation
class DAG_infoBuffer_Monitor_for_Lambdas(MonitorSU):
    def __init__(self, monitor_name = "DAG_infoBuffer_Monitor_for_Lambdas"):
        super(DAG_infoBuffer_Monitor_for_Lambdas, self).__init__(monitor_name=monitor_name)
        # For testing, if we havn't called init() then version number will be 1
        self.current_version_DAG_info = None
        self.current_version_number_DAG_info = 1
#brc leaf tasks
        # The initial DAG has the initial leaf task(s) in it. As later we find
        # more leaf tasks (that start new connected components), we supply them 
        # with the DAG so the leaf tasks can be added to the continue_queue,
        # since they are incomplete, and started on the next deposit since they
        # will be complete
        #
        # stores incomplete leaf tasks; there should be at most only one incomplete
        # leaf task per deposit.
        self.continue_queue = []
        # For debugging, track all of the leaf tasks we have seen.
        self.cummulative_leaf_tasks = []
#brc: lambda inc:
        #self._next_version=super().get_condition_variable(condition_name="_next_version")
#brc: lambda inc
        #self._capacity = kwargs["n"]
        # Need there to be an element at buffer[i]. Cannot use insert() since it will shift elements down.
        # If we set buffer[0] we need an element to be at position 0 or we get an out of range error.
        self._buffer=[]
        #self._buffer= [None] * self._capacity
        #self._in=0
        #logger.trace(kwargs)

    #def init(self, **kwargs):
    def init(self,**kwargs):
        pass
        # initialize with a DAG_info object. This will be version 1 of the DAG
        #self.current_version_DAG_info = kwargs['current_version_DAG_info']
        #self.current_version_number_DAG_info = self.current_version_DAG_info.get_version_number()
#brc leaf tasks
        # The initial DAG has the initial leaf task(s) in it. As later we find
        # more leaf tasks (tht start new connected components), we supply them 
        # with the DAG so the leaf tasks can be aded to the work_queue and
        # and executed by workers (when we aer using workers).
        #self.current_version_new_leaf_tasks = []
        # logger.trace(kwargs)


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

        print("DAG_infoBuffer_Monitor_for_Lambdas: print_DAG_info: DAG_map:")
        for key, value in DAG_map.items():
            print(key)
            print(value)
            print(str(hex(id(value)))) 
        print("  ")
        print("DAG_infoBuffer_Monitor_for_Lambdas: print_DAG_info: DAG states:")         
        for key, value in DAG_states.items():
            print(key)
            print(value)
        print("   ")
        print("DAG_infoBuffer_Monitor_for_Lambdas: print_DAG_info: DAG leaf task start states")
        for start_state in DAG_leaf_task_start_states:
            print(start_state)
        print()
        print("DAG_infoBuffer_Monitor_for_Lambdas: print_DAG_info: DAG_tasks:")
        for key, value in DAG_tasks.items():
            print(key, ' : ', value)
        print()
        print("DAG_infoBuffer_Monitor_for_Lambdas: print_DAG_info: DAG_leaf_tasks:")
        for task_name in DAG_leaf_tasks:
            print(task_name)
        print() 
        print("DAG_infoBuffer_Monitor_for_Lambdas: print_DAG_info: DAG_leaf_task_inputs:")
        for inp in DAG_leaf_task_inputs:
            print(inp)
        #print() 
        print()
        print("DAG_infoBuffer_Monitor_for_Lambdas: print_DAG_info: DAG_version_number:")
        print(DAG_info.get_DAG_version_number())
        print("DAG_infoBuffer_Monitor_for_Lambdas: print_DAG_info: DAG_info_is_complete:")
        print(DAG_info.get_DAG_info_is_complete())
        print()

    def get_current_version_number_DAG_info(self):
        try:
            super(DAG_infoBuffer_Monitor_for_Lambdas, self).enter_monitor(method_name="get_current_version_number_DAG_info")
        except Exception:
            logger.exception("[ERROR]: DAG_infoBuffer_Monitor_for_Lambdas:  Failed super(DAG_infoBuffer, self)")
            logger.exception("[ERROR] self: " + str(self.__class__.__name__))
            if DAG_executor_constants.exit_program_on_exception:
                logging.shutdown()
                os._exit(0)
        
#brc: lambda inc
        #logger.trace("DAG_infoBuffer_Monitor_for_Lambdas: get_current_version_number_DAG_info() entered monitor, len(self._new_version) ="+str(len(self._next_version)))
        logger.trace("DAG_infoBuffer_Monitor_for_Lambdas: get_current_version_number_DAG_info() entered monitor")

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
        except Exception:
            logger.exception("[ERROR]: DAG_infoBuffer_Monitor_for_Lambdas: Failed super(DAG_infoBuffer_Monitor_for_Lambdas, self)")
            logger.exception("[ERROR] self: " + str(self.__class__.__name__))
            if DAG_executor_constants.exit_program_on_exception:
                logging.shutdown()
                os._exit(0)

#brc: lambda inc
        #logger.trace("DAG_infoBuffer_Monitor_for_Lambdas: deposit() entered monitor, len(self._new_version) ="+str(len(self._next_version)))
        logger.info("DAG_infoBuffer_Monitor_for_Lambdas: deposit() entered monitor")
        self.current_version_DAG_info = kwargs['new_current_version_DAG_info']
        self.current_version_number_DAG_info = self.current_version_DAG_info.get_DAG_version_number()
#brc leaf tasks
        # For lambdas, we can't start a leaf task when we get it in list of leaf tasks
        # as it is not complete. So we put it in the continue queue and start it when 
        # we get the next deposit() - the leaf task will be complete in the next deposit.
        # (Note: when we generate dag incrementally, when we get a new leaf task, we will
        # make the previous group/partitition complete and output the group's pickle file for 
        # the previous group/paritition, but we do not output the group's picklr file for 
        # for the leaf task, which is the current partition/group.)
        # 
        # if DAG_info is complete then we can start leaf task now as leaf is start of new 
        # component and DAG_info is complete. In fact, since the leaf task is the last
        # group/partition to be generated, we need to start it now since there
        # will be no more deposits.
        DAG_info_is_complete = kwargs['DAG_info_is_complete']
        new_leaf_tasks = kwargs['new_current_version_new_leaf_tasks']
        # Note: cummulative_leaf_tasks gets cleared after we start the new leaf tasks
        # for debugging - all leaf tasks started so far
        self.cummulative_leaf_tasks += new_leaf_tasks

        try:
            msg = "[ERROR]:DAG_infoBuffer_Monitor_for_Lambdas:" \
                + " deposit received more than 1 leaf task."
            assert not (len(new_leaf_tasks)>1) , msg
        except AssertionError:
            logger.exception("[Error]: assertion failed")
            if DAG_executor_constants.exit_program_on_exception:
                logging.shutdown()
                os._exit(0)
        # assertOld: at most one leaf task found
        #if len(new_leaf_tasks)>1:
        #    logger.error("[ERROR]:DAG_infoBuffer_Monitor_for_Lambdas:"
        #        + " deposit received more than 1 leaf task.")
        #    logger.error(ex)
        #    logging.shutdown()
        #    os._exit(0) 

        logger.info("DAG_infoBuffer_Monitor_for_Lambdas: DAG_info deposited: ")
        self.print_DAG_info(self.current_version_DAG_info)

#brc leaf tasks
        # debugging:
        if len(new_leaf_tasks) > 0:
            logger.info("DAG_infoBuffer_Monitor_for_Lambdas: new leaf task states deposited: ")
            for work_tuple in new_leaf_tasks:
                leaf_task_state = work_tuple[0]
                logger.info(str(leaf_task_state))
            logger.info("DAG_infoBuffer_Monitor_for_Lambdas: new cumulative leaf task states: ")
            for work_tuple in self.cummulative_leaf_tasks:
                leaf_task_state = work_tuple[0]
                logger.info(str(leaf_task_state))

        restart = False

#brc: lanbda inc: start lambdas for all continued states in buffer and leaf tasks
        #self._next_version.signal_c_and_exit_monitor()

        if DAG_executor_constants.RUN_ALL_TASKS_LOCALLY:
            # not using real lambdas
            try:
                # start simulated lambdas that with the new DAG_info
                for start_tuple in self._buffer:
                    # pass the state/task the thread is to execute at the start of its DFS path
                    start_state = start_tuple[0]
                    # Note: for incremental DAG generation, when we restart a lambda
                    # for a continued task, if the task is a group, we give it the output
                    # it generated previously (before terminating) and use the output
                    # to do the group's/task's fanins/fanout. If the task is a partition,
                    # we give the partition the input for its execution. 
                    # (Tricky: after executing a group with TBC fanins/fanout/collpases,
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
                    logger.info("DAG_infoBuffer_Monitor_for_Lambdas: (re)starting lambda with DAG_executor_state.state: " + str(DAG_exec_state.state)
                        + " continued task: " + str(DAG_exec_state.continued_task))
                    #logger.trace("DAG_executor_state.function_name: " + DAG_executor_state.function_name)
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
                    thread = threading.Thread(target=DAG_executor.DAG_executor_task, name=(thread_name_prefix+"ss"+str(start_state)), args=(payload,))
                    thread.start()
                self._buffer.clear()

                # If this is the first DAG_info and the only DAG_info, i.e., 
                # it contains only partitions 1 and 2 (and the groups therein.)
                # (The first partition is laways a single group that starts a component, and 
                # the second partition may or may not be the start of a new component. 
                # If it is not, then the second partition can have a lot of groups, like the 
                # whiteboard example. If it is the start of a new component then the second
                # partition is also a single group. A graph with two nodes and no edges has
                # 2 components. The first node is component/group/partition 1 and the 
                # second node is component/group/partition 2). Both of these groups/partitions
                # are leaf tasks and both will be started by DAG_executor_driver since they are
                # leaf tasks detected in the first 2 partitions. If there is a third node then 
                # this node is group/partition 3 and it is a leaf task that will be started 
                # by deposit(). Since group/partition 3 is the final group/partition in the 
                # graph, deposit will start it immediately, instead of waiting for the 
                # next deposit (as there will be no moer deposit since the graph is complete.)
                #
                # Note that the DAG_info read by DAG_executor_driver will have both of 
                # these leaf tasks and the DAG_executor_driver will start both leaf tasks 
                # so we cannot start the second leaf task(s) here or it will be executed twice.
                # To prevent the second leaf task/group/partitio from being started twice
                # in BFS the secon leaf node is not added to to the new_leaf_tasks so 
                # new_leaf_tasks will be empty.
                #
                # If the DAG_info is complete, then the leaf task is complete and
                # so does not need to be continued later, we can start a labda for the
                # leaf task now (as oppsed to putting the leaf tsk in the continue queue
                # and starting a lambda for it on the next deposit - since the DAG_info
                # is complete incremental DAG generation is over and there will be no 
                # more deposits.)
                # Note: adding the leaf task to the continue queue menas a lambda
                # will be started for it in this deposit since below we start a 
                # lambda for all leaf tasks in the continue queue. (There may be 
                # a leaf task currently in the continue queue that will also be 
                # started in this deposit().

#brc: ToDo: If the DAG_info is complete then we can start the leaf task 
# as it is also complete. We can also start a leaf task if we know it 
# is the last group/partition of a component even if the DAG_nfo is 
# not complete. For a partition (and the groups therein) we can track
# whether any nodes in this partition (and grooups therein) have any
# children. If not, then this partition (and the groups therein) is the 
# last partition in the current connected component and we can start 
# a lambda for the leaf task now.


                if DAG_info_is_complete:
                    self.continue_queue += new_leaf_tasks
                else:
                    # leaf task is unexecutable now. Execute it on next deposit()
                    pass
                
                # start a lambda to excute the leaf task found on the previous deposit()
                for work_tuple in self.continue_queue:
                    # pass the state/task the thread is to execute at the start of its DFS path
                    start_state = work_tuple[0]
                    # leaf tasks have no inputs. 
                    # Note: for incremental DAG generation, when we restart a lambda
                    # for a continued task, if the task is a group, we give it the output
                    # it generated previously (before terminating) sand use the output
                    # to do the group's/task's fanins/fanout. If the task is a partition,
                    # we give the partition the input for its execution. 
                    # (Tricky: after executing a group with TBC fanins/fanout/collpases,
                    # if it has TBC fanouts/faninNBs/fanins, we cannot do any of them so we
                    # have to wait until we get a new DAG and restart the group/task
                    # so we can complete the fanouts/faninNBs/fanins. After excuting 
                    # a partition the partition can only have a collapse, i.e., we know
                    # we must execute the collapse task. So when we restart the partition/task,
                    # we supply the input for the collpase task and execute the collapse task.)
                    input_or_output = [] 
                    DAG_exec_state = DAG_executor_State(function_name = "DAG_executor", function_instance_ID = str(uuid.uuid4()), state = start_state, continued_task = False)
                    DAG_exec_state.restart = False      # starting  new DAG_executor in state start_state_fanin_task
                    DAG_exec_state.return_value = None
                    DAG_exec_state.blocking = False
                    logger.info("DAG_infoBuffer_Monitor_for_Lambdas: starting lambda for leaf task with DAG_executor_state.state:" + str(DAG_exec_state.state)
                        + " leaf task: " + str(DAG_exec_state.continued_task))
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
                    thread = threading.Thread(target=DAG_executor.DAG_executor_task, name=(thread_name_prefix+"ss"+str(start_state)), args=(payload,))
                    thread.start()
                #self.current_version_new_leaf_tasks.clear()
                # clear the started leaf tasks
                self.continue_queue.clear()
                # add the leaf task to the continue queue since it is not 
                # complete and start a labda for it in next deposit()
                if not DAG_info_is_complete:
                    self.continue_queue += new_leaf_tasks
            except Exception:
                logger.exception("[ERROR] DAG_infoBuffer_Monitor_for_Lambdas: Failed to start DAG_executor thread for state " + str(start_state))
                if DAG_executor_constants.exit_program_on_exception:
                    logging.shutdown()
                    os._exit(0)
        else:
            # using real lambdas
            try:
                # start simulated lambdas that with the new DAG_info
                for start_tuple in self._buffer:
                    try: 
                        logger.info("get tuple")
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
                        logger.info("got tuple: start_state: " + str(start_state) 
                            + " input_or_output: " + str(input_or_output))
                        DAG_map = self.current_version_DAG_info.get_DAG_map()
                        task_name = DAG_map[start_state].task_name            
                        DAG_exec_state = DAG_executor_State(function_name = "WukongDivideAndConquer:"+task_name, function_instance_ID = str(uuid.uuid4()), 
                            state = start_state, continued_task = True)
                        logger.info("create state")  
                        DAG_exec_state.restart = False      # starting  new DAG_executor in state start_state_fanin_task
                        DAG_exec_state.return_value = None
                        DAG_exec_state.blocking = False
                        logger.info("DAG_infoBuffer_Monitor_for_Lambdas: (re)starting lambda with DAG_executor_state.state: " + str(DAG_exec_state.state)
                            + " continued task: " + str(DAG_exec_state.continued_task))
                        #logger.trace("DAG_executor_state.function_name: " + DAG_executor_state.function_name)

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
                        invoke_lambda_DAG_executor(payload = payload, function_name = "WukongDivideAndConquer:"+task_name)
                    except Exception:
                        logger.exception("[ERROR]: DAG_infoBuffer_Monitor_for_Lambdas: Failed to start DAG_executor thread for state " + str(start_state))
                        if DAG_executor_constants.exit_program_on_exception:
                            logging.shutdown()
                            os._exit(0)

                   
                self._buffer.clear()
                # If the DAG_info is complete, then the leaf task is complete and
                # so dos not need to be continued later, we can start a labda for the
                # leaf task now (as oppsed to putting the leaf tsk in the continue queue
                # and starting a lambda for it on the next deposit - since the DAG_info
                # is complete incremental DAG generation is over and there will be no 
                # more deposits.)
                # Note: adding the leaf task to the continue queue menas a lambda
                # will be started for it in this deposit since below we start a 
                # lambda for all leaf tasks in the continue queue. (There may be 
                # a leaf task currently in the continue queue that will also be 
                # started in this deposit().
                if DAG_info_is_complete:
                    self.continue_queue += new_leaf_tasks
                else:
                    # leaf task is unexecutable now. Execute it on next deposit()
                    pass
                # start a lambda to excute the leaf task found on the previous deposit()
                for work_tuple in self.continue_queue:
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
                    DAG_map = self.current_version_DAG_info.get_DAG_map()
                    task_name = DAG_map[start_state].task_name            
                    DAG_exec_state = DAG_executor_State(function_name = "WukongDivideAndConquer:"+task_name, function_instance_ID = str(uuid.uuid4()), state = start_state, continued_task = False)
                    DAG_exec_state.restart = False      # starting  new DAG_executor in state start_state_fanin_task
                    DAG_exec_state.return_value = None
                    DAG_exec_state.blocking = False
                    logger.info("DAG_infoBuffer_Monitor_for_Lambdas: starting lambda for leaf task with DAG_executor_state.state:" + str(DAG_exec_state.state)
                        + " leaf task: " + str(DAG_exec_state.continued_task))
                    #logger.trace("DAG_executor_state.function_name: " + DAG_executor_state.function_name)
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
                    invoke_lambda_DAG_executor(payload = payload, function_name = "WukongDivideAndConquer"+task_name)
                # clear the started leaf tasks
                self.continue_queue.clear()
                # add the leaf task to the continue queue since it is not 
                # complete and start a labda for it in next deposit()
                if not DAG_info_is_complete:
                    self.continue_queue += new_leaf_tasks
            except Exception:
                logger.exception("[ERROR] DAG_infoBuffer_Monitor_for_Lambdas: Failed to start DAG_executor thread for state " + str(start_state))
                if DAG_executor_constants.exit_program_on_exception:
                    logging.shutdown()
                    os._exit(0)
        try:
            super(DAG_infoBuffer_Monitor_for_Lambdas, self).exit_monitor()
        except Exception:
            logger.exception("[ERROR]:DAG_infoBuffer_Monitor_for_Lambdas: deposit: exit_monitor: Failed super(DAG_infoBuffer_Monitor_for_Lambdas, self)")
            logger.exception("[ERROR] self: " + str(self.__class__.__name__))
            if DAG_executor_constants.exit_program_on_exception:
                logging.shutdown()
                os._exit(0)
        
        return 0, restart
    

    def withdraw(self, **kwargs):
        # request a new version of the DAG. A worker that finishes version 
        # i will request i+1. Note that i+1 may <= current_version. If so
        # return the current version. If not, then the worker is requesting
        # the next version of the DAG, which hasn't been generated yet.
        super().enter_monitor(method_name = "withdraw")
        requested_current_version_number = kwargs['requested_current_version_number']
#brc: lambad inc
        logger.info("DAG_infoBuffer_Monitor_for_Lambdas: withdraw() entered monitor, requested_current_version_number = "
            + str(requested_current_version_number))

        DAG_info = None
        restart = False
        if requested_current_version_number <= self.current_version_number_DAG_info:
            DAG_info = self.current_version_DAG_info
            logger.info("DAG_infoBuffer_Monitor_for_Lambdas: withdraw return None")
#brc leaf tasks

#brc: lambda inc: 
            # For lambdas, we do not return leaf tasks, deposit is starting them instead.
#brc: lambda inc: 
            #return DAG_info_and_new_leaf_task_states_tuple, restart
            try:
                super(DAG_infoBuffer_Monitor_for_Lambdas, self).exit_monitor()
            except Exception:
                logger.exception("[ERROR]:DAG_infoBuffer_Monitor_for_Lambdas: withdraw: exit_monitor: Failed super(DAG_infoBuffer_Monitor_for_Lambdas, self)")
                logger.exception("[ERROR] self: " + str(self.__class__.__name__))
                if DAG_executor_constants.exit_program_on_exception:
                    logging.shutdown()
                    os._exit(0)

            return DAG_info, restart
        else:

#brc: lambda inc: 
            # For lambdas, we are not waiting for a new DAG_info to be 
            # deposited; instead lambda stops then is (re)startd by deposit

#brc: lambda inc
            # save start tuple which deposit wil use to restart lambda
            value = kwargs["value"]
            logger.info("DAG_infoBuffer_Monitor_for_Lambdas: withdraw: value to deposit: " + str(value))
            self._buffer.append(value)
            #self._buffer.insert(self._in,value)
            #self._buffer[self._in] = value
            #self._in=(self._in+1) % int(self._capacity)
            
            logger.info("DAG_infoBuffer_Monitor_for_Lambdas: withdraw return None")
#brc: lambda inc
            # For lambdas, we do not return leaf tasks, deposit is starting them instead.
            #return DAG_info_and_new_leaf_task_states_tuple, restart

            try:
                super(DAG_infoBuffer_Monitor_for_Lambdas, self).exit_monitor()
            except Exception:
                logger.exception("[ERROR]:DAG_infoBuffer_Monitor_for_Lambdas: withdraw: exit_monitor: Failed super(DAG_infoBuffer_Monitor_for_Lambdas, self)")
                logger.exception("[ERROR] self: " + str(self.__class__.__name__))
                if DAG_executor_constants.exit_program_on_exception:
                    logging.shutdown()
                    os._exit(0) 

            return None, restart
        
        return 0
        
# For testing:
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
    logger.trace("taskD Calling withdraw")
    b.deposit(**keyword_arguments)
    logger.trace("Successfully called deposit version 2")

def taskW1(b : DAG_infoBuffer_Monitor_for_Lambdas):
    logger.trace("taskW1 Calling withdraw")
    keyword_arguments = {}
    keyword_arguments['requested_current_version_number'] = 1
    DAG_info, restart = b.withdraw(**keyword_arguments)
    logger.trace("Successfully called withdraw, ret is " 
        + str(DAG_info.get_value()) + "," + str(DAG_info.get_version_number())
        + " restart " + str(restart))

def taskW2(b : DAG_infoBuffer_Monitor_for_Lambdas):
    logger.trace("taskW2 Calling withdraw")
    keyword_arguments = {}
    keyword_arguments['requested_current_version_number'] = 2
    DAG_info, restart = b.withdraw(**keyword_arguments)
    logger.trace("Successfully called withdraw, ret is " 
        + str(DAG_info.get_value()) + "," + str(DAG_info.get_version_number())
        + " restart " + str(restart))

def main(): 
    b = DAG_infoBuffer_Monitor_for_Lambdas(monitor_name="DAG_infoBuffer_Monitor_for_Lambdas")
    DAG_info = Dummy_DAG_info("DAG_info1",1)
    keyword_arguments = {}
    keyword_arguments['current_version_DAG_info'] = DAG_info
    b.init(**keyword_arguments)
    try:
        logger.trace("Starting D thread")
        _thread.start_new_thread(taskD, (b,))
    except Exception as ex:
        logger.exception("[ERROR] Failed to start first thread.")
        logger.exception(ex)

    try:
        logger.trace("Starting taskW1 thread")
        _thread.start_new_thread(taskW1, (b,))
    except Exception as ex:
        logger.exception("[ERROR] Failed to start taskW1 thread.")
        logger.exception(ex)

    try:
        logger.trace("Starting first taskW2 thread")
        _thread.start_new_thread(taskW2, (b,))
    except Exception as ex:
        logger.exception("[ERROR] Failed to start first taskW2 thread.")
        logger.exception(ex)

    try:
        logger.trace("Starting second taskW2 thread")
        _thread.start_new_thread(taskW2, (b,))
    except Exception as ex:
        logger.exception("[ERROR] Failed to start second taskW2 thread.")
        logger.exception(ex)

    time.sleep(4)
    logger.trace("Done sleeping")

if __name__=="__main__":
     main()
