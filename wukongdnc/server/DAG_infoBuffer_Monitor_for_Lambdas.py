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

#from ..dag import BFS

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
        # We need to remember the start indices, i.e., after we deallocaet a structure from start 
        # to end, we will set start = end so that the next deallocation starts where we left off (end).
        # We deallocate partition by partition, so we have start and end partition indices and 
        # when we use groups we deallocate group by group (as we iterate partition by partition).
        # So we get the partitions from start to end, and within the partitions, we get the groups
        # from start to end. Partition names are stored in partition_names[] and likewise for
        # group names and group_names[]. We are going sequentially through partition names 
        # to dealloctate partition structures in DAG_info, and sequentially through partition names and 
        # within each partition sequentially through group names to deallocate greup structures in DAG_info.
        # For this we deallocate start to end and then set start = end so we can continue on the 
        # next deallocation.
        self.deallocation_start_index_groups = 1
        self.deallocation_start_index_partitions = 1
        self.most_recent_deallocation_end_index = 0
        # on each call ot deposit, we pass a list L of lists. These are 
        # the lists identifie since the last pubication (call to deposit). We then do 
        # self.groups_of_partitions.extend(L), which appends all the lists in 
        # L to self.groups_of_partitions. We cannot access BFS.groups_of_partitions
        # in the code in this file since this code is running on the server and BFS
        # is not. Instead, we pass the lists of groups on deposit and so assemble
        # groups_of_partitions incrementally on the serverdeposiy-by-deposit.
        # We do the same for the group names, i.e., we assmble the group names by 
        # appending to self.group_names the contents of the lists added to groups_of_partitions.
        # Example: list 1 is "PR1_1" and list 2 is "PR2_1, PR2_2L, PR2_3" so the group names
        # at this point will be "PR1_1, PR2_1, PR2_2L, PR2_3".
        self.groups_of_partitions = []
        self.group_names = []
        self.partition_names = []
        
        self.current_version_DAG_info_DAG_map_save = {}
        self.current_version_DAG_info_DAG_tasks_save = {}
        self.current_version_DAG_info_DAG_states_save = {}

        self.num_nodes = 0
        self.version_number_for_most_recent_deallocation = 0

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
            #print(str(hex(id(value)))) 
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
    
    def deallocate_DAG_structures_lambda(self,i):
        # need to save the deallocated items? Since requested version numbers arw 
        # mono increasing we only dealloc more as we restart waiting withdrawers.
        # At that point, we have max deallocs. Then we can get a withdraw that doesn't
        # wait and this might require putting some deallocs back. Or we can get a 
        # withdraw that waits and its requested version will result in more deallocs.
        # Or we can get a deposit for a larger version number - for this, there will be
        # more deallocs poasible, but the requested versions may be less than the just 
        # deposited version, so may need to put some back (from prev. version) or dealloc
        # more.
        #Noet: Working on self.current_version_DAG_info
        #brc: deallocate DAG map-based structures

        partition_or_group_names = []
        if not DAG_executor_constants.USE_PAGERANK_GROUPS_INSTEAD_OF_PARTITIONS:
#brc: ToDo: we need partition names too?
            partition_or_group_names = self.partition_names
        else:
            partition_or_group_names = self.group_names # BFS.group_names
        logger.info("deallocate_DAG_structures_lambda: partition or group namesXXX: ")
        for n in partition_or_group_names:
            logger.info(n)
        name = partition_or_group_names[i-1]
        state = self.current_version_DAG_info.DAG_states[name]
        logger.info("deallocate_DAG_structures_lambda: partition or group name: " + str(name))
        logger.info("deallocate_DAG_structures_lambda: state: " + str(state))

        # We may be iterating through these data structures in, e.g., DAG_executor concurrently 
        # with attempts to to the del on the data structure, which would be an error. We only 
        # iterate when we are tracing the DAG for debugging so we could turn these iterations
        # off when we set the deallocate option on. 
        # Note: we can change Partition_DAG_tasks and Partition_DAG_states to maps instead of 
        # lists and use del for the deallocation, which might be a lot faster (O(1)) than deleting
        # from the lists (O(n))for large maps/lists. This would require that we dal with the 
        # itertions as just mentioned and we would need atomic ops on the dictionary and 
        # they should not requre the use of the GIL. Or we coould copy the DAG data structurs
        # when we make a new incremental DAG but this would be time consuming.
        # Currently, we set item referenced to None, which would save space sine None takes
        # less space than the class state_info (see DFS_visit.py) objects in Partition_DAG_map.
        # Also, None presumably takes less space than a pagerank function object. It is not clear
        # how Noen compares to an integer object.
        # Note: we can easily determine the pagerank function to call at excution time instead of 
        # storing a function in the DAG for each task. (It's the same code that determines which 
        # function to store in the DG) This would save space in the DAG representation.
        # USE self.DAG_map 
        #self.current_version_DAG_info.DAG_map[state]
        # save the deallocated infoormation in case we need to restore it
        self.current_version_DAG_info_DAG_map_save[state] = self.current_version_DAG_info.DAG_map[state]
        self.current_version_DAG_info.DAG_map[state] = None

        # USE self.DAG_tasks
        #self.current_version_DAG_info.DAG_tasks[partition_name]
        # save the deallocated infoormation in case we need to restore it
        self.current_version_DAG_info_DAG_tasks_save[name] = self.current_version_DAG_info.DAG_tasks[name]
        self.current_version_DAG_info.DAG_tasks[name] = None
        # USE self.DAG_states
        #del Partition_DAG_states[partition_name]
        # save the deallocated infoormation in case we need to restore it
        self.current_version_DAG_info_DAG_states_save[name] = self.current_version_DAG_info.DAG_states[name]
        self.current_version_DAG_info.DAG_states[name] = None

        # We are not doing deallocations for these collections; they are not 
        # per-partition collections, they are collections of fanin names, etc.
        #all_fanin_task_names 
        #all_fanin_sizes
        #all_faninNB_task_names
        #all_faninNB_sizes
        #all_fanout_task_names
        #all_collapse_task_names
        #DAG_leaf_tasks
        #DAG_leaf_task_start_states
        #DAG_leaf_task_inputs

        # We are not doing deallocations for the non-collections
        #DAG_version_number
        #DAG_is_complete
        #DAG_number_of_tasks
        #DAG_number_of_incomplete_tasks
        #DAG_num_nodes_in_graph
        #DAG_number_of_groups_of_previous_partition_that_cannot_be_executed

    def restore_item(self,i):

        partition_or_group_names = []
        if not DAG_executor_constants.USE_PAGERANK_GROUPS_INSTEAD_OF_PARTITIONS:
#brc: ToDo: we need partition names too?
            partition_or_group_names = self.partition_names
        else:
            partition_or_group_names = self.group_names # BFS.group_names
        #logger.info("restore_DAG_structures_lambda: partition or group namesXXX: ")
        #for n in partition_or_group_names:
        #    logger.info(n)
        name = partition_or_group_names[i-1]
        state = self.current_version_DAG_info_DAG_states_save[name]
        logger.info("restore_DAG_structures_lambda: partition or group name: " + str(name))
        logger.info("restore_DAG_structures_lambda: state: " + str(state))
        self.current_version_DAG_info.DAG_map[state] = self.current_version_DAG_info_DAG_map_save[state]
        self.current_version_DAG_info.DAG_states[name] = self.current_version_DAG_info_DAG_states_save[name]
        self.current_version_DAG_info.DAG_tasks[name] = self.current_version_DAG_info_DAG_tasks_save[name]

    def restore_DAG_structures_partitions(self,requested_version_number_DAG_info):
        """
        We have:
        requested_current_version_number < self.version_number_for_most_recent_deallocation:
        so we have deallocated too many items given the next requested_current_version_number.
        This means we need to restore some of these items. We have saved every item that\
        we have deallocaed so we can out them back.
        The last deallocation was from the saved start to the computed end.
        For partitions, this is:
            deallocation_end_index = (2+((requested_version_number_DAG_info-2)*DAG_executor_constants.INCREMENTAL_DAG_DEPOSIT_INTERVAL))-2
        which means we have deallocated from 1 to end, though we may not have started
        at 1, i.e., if we did a sequence of deallocations in deposit(). Still, we
        have deallocated and saved the items from 1 to end.
        For groups, we iterate through the partitions from start to end and for each
        partition we deallocate its n groups. In this case we also end up at an
        end index for the deallocated groups, and we have deallocated from 
        1 to the group end index.
        Tehe restoration is always a suffx of the deallocated items. That is,
        for the new requested_current_version_number which we know is less than 
        the version_number_for_most_recent_deallocation, we still want to 
        deallocate all the items starting at 1, but we need to stop deallovating
        items that are passed the end index that is computed by 
        requested_current_version_number. Note that for partitions, we can get ths
        end index from the "deallocation_end_index =" formula. For groups, this
        end index depends on the number of groups in each of the partitions to be 
        deallocated. We can compte this based on the partitions to be deallocated,
        i.e., this number is computed with the same code that we used to deallocate
        groups but with taking the sum of the lengths instead of actually doing
        the deallocations. Is there a shortcut?
        
        Example: The input graph graph_24N_3CC_fanin_restoredealloc.gr has partitions:
            partitions, number of partitions: 15 (length):
            PR1_1: (3): 5 17 1
            PR2_1L: (15): 2 10 5-s 16 20 8 11 3 17-s 19 4 6 14 1-s 12
            PR3_1: (9): 8-s 13 7 11-s 15 6-s 9 4-s 18
            PR4_1: (1): 21
            PR5_1: (2): 21-s 22
            PR6_1: (2): 22-s 25
            PR7_1: (2): 25-s 26
            PR8_1: (2): 26-s 27
            PR9_1: (2): 27-s 28
            PR10_1: (1): 23
            PR11_1: (2): 23-s 24
            PR12_1: (2): 24-s 29
            PR13_1: (2): 29-s 30
            PR14_1: (2): 30-s 31
            PR15_1: (2): 31-s 32
        with Connected Components (CC) PR1_1 - PR3_1, PR4_1 - PR9_1, and PR10+1 - PR15_1. The 
        first (base) DAG is PR1_1 - PR2_1 with PR2_1 tobecontinued (TBC). With pubication
        interval 1, the next DAG (version 2) is PR1_1 - PR3_1, with partition 2 now complete
        and partition 3 TBC. Tbhe executor will execute partition 2 and see that partiton 3
        is TBC and request a new DAG. In the new DAG, Partition 3 is the last DAG in the first CC 
        and it is complete. Since partiton 3 has no collapse tas, i.e., succeeding task, the executor
        will terminate. The DAG generator will eposit() a new DAG that has partition 4, which is the
        first partition of the next CC. Partition 4 is TBC so a new lambda is not started for
        executing partiton 4. The next DAG has a complete partition 4 and a TBC partition 5.
        A new lambda is started to execute leaf task partition 4. (A leaf task is the first task
        of a CC.) This nw lambda will execute 4, see that 5 is TBC and request a new DAG. In the 
        new DAG, 5 is complete and 6 is TBC so the lambda requests a new DAG. In the new DAG,
        6 is complete and 7 is TBC. Suppose 6 is executed an it takes a long time to execute 6.
        In the mean time, the ADG generator, will continue to produce new DAGS that contain 
        partitions 7, 8, 9, 10, 11, 12, 13, 14, 15. When a new DAG is produced containing
        partitons 10 and 11, where 10 is complete and 11 is TBC, and 10 is the first partition
        of the 3rd CC. and new lambda will be started to execte leaf task 10. This lambda X
        will execute 10, 11, 12, 13, 14, and 15 and terminate. Note that after the lamda L
        that is executing partition 6 finishes 6 and sees the TBC partition 7 in its (old)
        version 6 of the DAG, it will call deposit() requesting version 7 of the DAG. Assume
        the last deposited version of the DAG is version 11, which contains all partitions from 
        PR1_1 to PR1_12. Since 7 < 11, i.e., requested version 7 is older than the last
        version 11 deposited, we can return version 11 of the ADG to L. However, as lambda
        X was exexcuting the partitions in its CC (10, 11, 12, 13, 14, 15) it was also requesting
        new versions of the DAG. In these new versions of the DAG that were given to X,
        we will have deallocated some of the partition information in these DAGs, which is 
        the "old" partitions that are no longer needed to execute the newer partitions
        in the DAG. For example, for version 11, we may have deallocted information about 
        partitions 1 - 9. This means that if L calsl withdraw() and requests version 7 and
        we retun version 11 to L, version 11 will not have informarion about partitions 
        1 - 7 but L neds this deallocated information to execute its DAG. Thus, we must 
        restore the missing information that L needs. In this case, L does NOT need
        information about 1-5 but it does need information about partition 6 and the 
        partitons after 6. Since our version 11 DAG had deallocations for 1 - 9, we
        need to restore the information for 6, 7, 6, 9, which are partitions that L
        needs that were deallocated from version 11. (Version 11 stil has information
        about partition 10 etc.) So the version of the AG we return to L is version 
        11 with partitions 1 - 5 being deallocated. Noet that when we deallocate infomation
        about partition p we save it so that it is avaialble f we need to restore p.
        """
        # This is where deallocation should end
        deallocation_end_index = (2+((requested_version_number_DAG_info-2)*DAG_executor_constants.INCREMENTAL_DAG_DEPOSIT_INTERVAL))-2

        #Q: Can this be false? Yes, if trying to restore version 2? 
        # But No: we use >= 0 and 0 is the min so no?  For verstion 2, 
        # end index is 2+0-2=0 since you can't actually dealloc until you get to version 3.
        # If most recent is, e.g., 3, then we did some deallocs for 3 and we need
        # to restore these for 2 whose end index is 0.
        #Q: "if deallocation_end_index == 0" since it is 0 fr 2 and we need to retore.
        # so for 3 with interval 2, end index is 2+2-2=2, so dealloc 1..2 (as for 
        # 3 we have: 1, 2, 3, 4, 5, 6 and we can dealloc 1 and 2 of version 1; so
        # we resore 1 and 2 of version 1 if we dealloc for 3 and get a request for 2.
        # So we want to inc end index 0 to 1 and restore from 1 to 2, which is range(1,3)
        #
        # For restore we use >= 0. For deallocation, we use > 0. For deallocation, 
        # we do not want to do a dealloc if end index is 0, which is the case for 
        # requested version 2, as start will be 1 (the initial value) and if end is 
        # 0 the range is from 1 to 0 so no dealloations will be done. For restore,
        # when the version requsted is less than the most recent deallocation end index 
        # we need to restore. If the most recent is for version 3 and the end index (for 
        # the just requested) is for version 2, then we need to restore the deallocs that were
        # done for 3. These deallocs were states 1 and 2. Thus, the end index for 2 is 
        # 0 and we increment it to 1, where the end index for 3, the most recent, is 2
        # which we also increment getting 3, giving a range of (1,3), where 3 is exclusive
        # so we restore states 1 and 2. States 3, 4, 5, 6 were never deallocated so a
        # this point no deallocations have been done. (Note the request is for 2 and 
        # the version available is 3 so we will return 3 but without any deallocations,
        # i.e., the deallocs will have been restored.)
        #if deallocation_end_index > 0:
        if deallocation_end_index >= 0:
            deallocation_end_index += 1
        
        # Can self.most_recent_deallocation_end_index > 0 be false? No, since we 
        # know we did a dealloation (for most recent) so end index is greater than 0.
        try:
            msg = "[ERROR]:DAG_infoBuffer_Monitor_for_Lambdas:" \
                + " self.most_recent_deallocation_end_index is not greater than 0."
            assert self.most_recent_deallocation_end_index > 0 , msg
        except AssertionError:
            logger.exception("[Error]: assertion failed")
            if DAG_executor_constants.exit_program_on_exception:
                logging.shutdown()
                os._exit(0)

        try:
            msg = "[ERROR]:DAG_infoBuffer_Monitor_for_Lambdas:" \
                + " self.most_recent_deallocation_end_index is not equal to " \
                + "computed_most_recent_deallocation_end_index."
            computed_most_recent_deallocation_end_index = \
                (2+((self.version_number_for_most_recent_deallocation-2)*DAG_executor_constants.INCREMENTAL_DAG_DEPOSIT_INTERVAL))-2
            assert computed_most_recent_deallocation_end_index == self.most_recent_deallocation_end_index, msg
        except AssertionError:
            logger.exception("[Error]: assertion failed")
            if DAG_executor_constants.exit_program_on_exception:
                logging.shutdown()
                os._exit(0)

        # Note: we used to compute the most_recent_deallocation_end_index but now 
        # when we do a deallocation we set the value of most_recent_deallocation_end_index.
        # It is probably okay to just compute it here when we need it since we
        # only need it here in restore.
        #self.most_recent_deallocation_end_index = (2+((self.version_number_for_most_recent_deallocation-2)*DAG_executor_constants.INCREMENTAL_DAG_DEPOSIT_INTERVAL))-2

        # This is used in a range() below and it will be exclusive so increment by 1.
        # We will set this to a new value at the end of restore to reflect that the 
        # last dealloated value has changed (since we restored some of the values.)
        # Note that the restored values are a suffix of the deallocated values 
        # i.e., are a sequence of values at the end of the sequence of deallocted values.
        if self.most_recent_deallocation_end_index > 0:
            self.most_recent_deallocation_end_index += 1

        logger.info("restore_DAG_structures_partitions: deallocation_end_index: " + str(deallocation_end_index)
            + " self.most_recent_deallocation_end_index: " + str(self.most_recent_deallocation_end_index))
        for i in range(deallocation_end_index, self.most_recent_deallocation_end_index):
            logger.info("restore_DAG_structures_partitions: restore " + str(i))
            self.restore_item(i)
        
        if deallocation_end_index <= self.most_recent_deallocation_end_index:
            # remember that this is the most recent version number for which we did
            # a deallocation
            self.version_number_for_most_recent_deallocation = requested_version_number_DAG_info
            # This was incremented above to be the position one past that of 
            # the last item deallocated, which is where we would start the
            # next deallocation
            self.deallocation_start_index_partitions = deallocation_end_index
            # But the actual new end of deallocation for this deallocation (after
            # restoring items) is deallocation_end_index - 1. We decrement
            # since deallocation_end_index was incremented above t be one
            # past the position of the last decrement so the (exclusive)
            # range value is correct)
            self.most_recent_deallocation_end_index = deallocation_end_index -1
            logger.info("deallocate_DAG_structures_partitions: new index values after restores: "
                    + "self.version_number_for_most_recent_deallocation: " 
                    + str(self.version_number_for_most_recent_deallocation)
                    + " self.deallocation_start_index_partitions: "
                    + str(self.deallocation_start_index_partitions)
                    + " self.most_recent_deallocation_end_index: "
                    + str(self.most_recent_deallocation_end_index))
        else:
            logger.info("deallocate_DAG_structures_partitions: " + " no restorations.")


    def deallocate_DAG_structures_partitions(self,requested_version_number_DAG_info):
        # current_partition_number is not currently used
        #
        # Version 1 of the incremental DAG is given to the DAG_executor_driver for execution
        # (assuming thw DAG has more than 1 partition). So the first version workers can request 
        # is version 2. At that point, they will have executed partition 1, found that 
        # partition 2 was to-be-continued, saw that partition 1, which is not to-be-continued but has a collapse
        # to a continued partition 2, and that the DAG was not complete, and so requested version 2.
        # (The worker that executed partition 1, actually the task corresponding to computing the 
        # pagerank values of partition 1, will save the state and output for that task in its continue queue. When 
        # that worker gets a new incremental DAG, it will get the state and output from its continue
        # queue and retart DAG execution by getting the collapse for partition 1 which is partition 2
        # and executing partition 2 as usual. In general if workers request version n, n>1, (all workers
        # request the same version of the DAG each round), then they have already executed partitions 1 through
        # 2+((n-2)*DAG_executor_constants.INCREMENTAL_DAG_DEPOSIT_INTERVAL), where the last 
        # partition 2+((n-2)*DAG_executor_constants.INCREMENTAL_DAG_DEPOSIT_INTERVAL) is continued and 
        # the preceding partition has a collapse to the continued task. Again, the workers have already executed
        # partitions 1 and 2 and a certain number of partitions that have been added in each new version
        # of the incremental DAG. Version 1 has partitions 1 and 2 and version 2 is the first version that 
        # can be requested by the workers. Note that if they request version 2, they have only excuted
        # partitions 1 and 2 (and really only executed partition 1 as partiton 2 was to-be-continued)
        # so that use of n-2 in this case is 2-2=0 and the last partition 2+(0*DAG_executor_constants.INCREMENTAL_DAG_DEPOSIT_INTERVAL)
        # is 2 as expected.(So 2 is the last partition in version 1, which doesn't man 2 has been executed 
        # since the last partition in an incomplete graph is to-be-continued.)
        #
        # Example: DAG_executor_constants.INCREMENTAL_DAG_DEPOSIT_INTERVAL is 4 and workers request Version 3. 
        # Then the partitions that have been executed are 1, 2, 3, 4, 5, and 6, where version 2 has partitons 1, 
        # 2, 3, 4, 5, and 6, where 6 is to-be-continued, and version 1 has partitions 1 and 2. Workers are 
        # requesting Version 3, which will add the 4 partitions 7, 8, 9, and 10 as 
        # DAG_executor_constants.INCREMENTAL_DAG_DEPOSIT_INTERVAL is 4. In the new version 3 of the DAG, 
        # partition 6 is not-continued and partition 5  has no collapse to a continued state. 
        # (Note that when we add partition 7 to the DAG, we change partition 6 to be non-to-be-continued
        # and we change partition 5 so that it has no collapse to a to-be-continued partition. The new DAG with
        # partition 7, which is a to-be-continued partition (due to DEPOSIT INTERVAL = 4), is not published, and neither 
        # are the DAGS created by adding partitions 8, and 9, respectively. The DAG created by adding 10 is published.)
        # (We have added 8 partitions since we generated the base DAG (with partitions 1 and 2) and
        # 8 mod 4 (DAG INTERVAL) is 0). Note that we added 7, 8, 9, and 10 but we also changed 5
        # and 6, so we need 5 and 6 to be in version 3 of the DAG, i.e., we do not deallocate 5 and 6 so they are in
        # version 3. So we can deallocate the info in the DAG strctures about DAG states 1, 2, 3, 4, which 
        # is 1 through (2+((n-2)*pub_interval))-2, n=3, which is 1 through 2+(1*4)-2 = 4. Note that when workers request 
        # version 3 we can deallocate DAG structure information about partitions 1, 2, 3, 4, in version 2 but 
        # not partitions 5 and 6. This is because the status of 5 and 6 in version 3 is changed from that of version 2.
        # Partition 5 has a collapse to continued task 6 in version 2 but in version 3 task 6 is not
        # to-be-continued and thus 5 no longer has a collapse to a continued task. So when the workers
        # get version 3 of the DAG they restart execution by getting the state and output for already
        # executed task 5 from the continue queue, retrieving the collapse task 6 of state 5, and 
        # executing task 6. Thus we do not dealloate the DAG states for 5 and 6, even though they
        # were in version 2 and we are getting a new version 3.
        # Note that we don't always start deallocation at 1, we start where the last deallocation ended.
        # So in this example, after deallocating 1, 2, 3, and 4, start_deallocation becomes 5. Note that if 
        # workers request version 2, then 2+((n-2)*pub_interval))-2 is 2+(0*4)-2 = 0 so we deallocate from 1 to 0 
        # so no deallocations will be done. As mentioned above, the states for partitions 1 and 2 in 
        # version 1 will change in version 2, so we do not deallocate 1 and 2 when the workers
        # request version 2. Partitions 1 and 2 will be among the partitions deallocated when workers
        # request version 3. (From above, we will deallocate 1, 2, 3, 4.)
        #
        # Note that the initial value of the version number in the DAG_infoBufferMonitor that 
        # maintains the last version number requested by the workers is 1. Workers don't actually 
        # request version 1, as version 1, which contains partitions 1 and 2, is given to
        # the DAG_executor_driver, which starts DAG execution with version 1 of the incremental DAG. 
        # (Partition 1 is a leaf task. Partition 2 either depends on partition 1 or is another leaf task.
        # More leaf tasks can be discovered during incremental DAG generation. This is unlike non-incremental 
        # DAG generation, which discovers all of the leaf tasks during DAG gneration and gives all these leaf tasks
        # to the DAD_executor_driver (as part of the DAG INFO structure). The DAG_executor_driver will 
        # start a lambda for each leaf task, or if workers are being used it will enqueue the leaf tasks
        # in the work queue.)
        # So the first version requested by the users is 2, and the first version deposited
        # in the DAG_infoBufferMonitor is 2. The workers can excute DAG version 1 and request
        # DAG version 2 before bfs() has deposited version 2 into the DAG_infoBufferMonitor.
        # (Deposit makes the new version available to workers, who call withdraw() to get a new
        # version.) Before bfs() deposits new version 2, it will ask DAG_infoBufferMonitor for 
        # the most recent version requested by the workers. This will be the initial value 1
        # since bfs() has not called deposit for the first time to deposit version 2 (which 
        # bfs will do next). bfs will pass 1 to deallocate_DAG_structures as the value
        # of requested_version_number_DAG_info so the value of 
        # 2+((requested_version_number_DAG_info-2)*DAG_executor_constants.INCREMENTAL_DAG_DEPOSIT_INTERVAL))-2
        # will be 2+((1-2) * 4)-2 = -4 so we will deallocate from 1 to -4 so no deallocations
        # as expected. (We cannot deallocate 1 or 2 yet as we are not done with them - in version
        # 2 partition 2 becomes not to-be-continued and partition 1 now has no collapse to a 
        # continued state, so we restart executing the DAG by accessing 1's state information
        # to get 1's collpase task 2, which we can now execute. We already excuted 1 and then
        # stopped execution when we saw 1's collapse was to a to-be-continued state 2)
        # Note: Given the formula 2+((requested_version_number_DAG_info-2)*DAG_executor_constants.INCREMENTAL_DAG_DEPOSIT_INTERVAL))-2,
        # the value given by this formula cannot become greater than 1 until workers request version 3. For
        # version 2, requested_version_number_DAG_info-2 is 2-2 = 0, so we get 2+0-2=0.
        # Note: if the DAG interval is 2, then for requesting version 3, we get 2+((3-2)*2)-2 = 2. Version 2 
        # has partitions 1 2 3 4, which means we can deallocate partitions 1 up to the end partition which is 2.
        # We keep 3 and 4 from version 2 and add 5 and 6 to get a version 3 of 3 4 5 6 where 6 is to-be-continued
        # and 4 is no longer to-be-continued and thus can be executed. (Partition 3 was executed previously then
        # execution stopped at to-be-continued taak/partition 4.)

        # Note: the version number gets incremented only when we publish a new DAG. The first 
        # DAG we publish is the base DAG with partitions 1 and 2. If the interval is 4,
        # the next DAG we generate has partitions 1, 2, and 3 but it is not published since
        # we publish every 4 partitions (after the base DAG) So the next verson is version 2,
        # which will have partitions 1, 2, 3, 4, 5, 6 for a total of 2+4 partitions.

        deallocation_end_index = (2+((requested_version_number_DAG_info-2)*DAG_executor_constants.INCREMENTAL_DAG_DEPOSIT_INTERVAL))-2

        # we will use deallocation_end_index in a range so it needs to be one past the last partition
        # to be dealloctated. Example, to deallocate "1 to 1" use range(1,2), where 1 is inclusive 
        # but 2 is exclusive; to deallocate "2 , 3, and 4" use range(2,5).
        # 
        # However, don't increment deallocation_end_index unless we can 
        # actually do a deallocation. We can do a deallocation unless start index is 1 and end index is less
        # than 1. If end index > 0 we can deallocate as the value of the above formula for end index
        # just keeps growing as the version number increases.
        # which is implemented as range(1,2), 
        if deallocation_end_index > 0:
            deallocation_end_index += 1

        logger.info("deallocate_DAG_structures_partitions: self.deallocation_start_index_partitions: " + str(self.deallocation_start_index_partitions)
            + " deallocation_end_index: " + str(deallocation_end_index) + " (exclusive)")
        for i in range(self.deallocation_start_index_partitions, deallocation_end_index):
            logger.info("deallocate_DAG_structures_partitions: generate_DAG_info_incremental_partitions: deallocate " + str(i))
            self.deallocate_DAG_structures_lambda(i)
        
        # Set start to end if we did a deallocation, i.e., if start < end. 
        # Note that if start equals end, then we did not do a deallocation since 
        # end is exclusive. (And we may have just incremented end, so dealllocating 
        # "1 to 1", with start = 1 and end = 1, was implemented as incrementing 
        # end to 2 and using range(1,2) so start < end for the deallocation "1 to 1"
        # Note that end was exclusive so we can set start to end instead of end+1.
        # Set most recent to deallocation_end_index - 1; we use - 1 since most
        # recent should be the last position actually deallocated, and since 
        # end index is exclusive (i.e., is 1 past the actual end) we use end - 1.
        if self.deallocation_start_index_partitions < deallocation_end_index:
            # Remember that this is the most recent version number for which we did
            # a deallocation
            self.version_number_for_most_recent_deallocation = requested_version_number_DAG_info
            self.deallocation_start_index_partitions = deallocation_end_index
            # The most_recent_deallocation_end_index is the position before 
            # the deallocation_start_index_partitions, i.e., we wil start the next
            # deallocation right after the end of the last dealloations we have performed.
            self.most_recent_deallocation_end_index = deallocation_end_index - 1
            logger.info("deallocate_DAG_structures_partitions: new index values after deallocation: "
                + "self.version_number_for_most_recent_deallocation: " 
                + str(self.version_number_for_most_recent_deallocation)
                + " self.deallocation_start_index_partitions: "
                + str(self.deallocation_start_index_partitions)
                + " self.most_recent_deallocation_end_index: "
                + str(self.most_recent_deallocation_end_index))
        else:
            logger.info("deallocate_DAG_structures_partitions: " + " no deallocations.")

#brc: ToDo: set most recent here. Also, what is init value of most recent?

    def deallocate_DAG_structures_groups(self,requested_version_number_DAG_info):
        # Version 1 of the incremental DAG is given to the DAG_executor_driver for execution
        # (assuming the DAG has more than 1 partition). So the first version workers can request 
        # is version 2. At that point, they will have executed partition 1, found that 
        # partition 2 was to-be-continued, saw that partition 1, which is not to-be-continued but has a collapse
        # to a continued partition 2, and that the DAG was not complete, and so requested version 2.
        # (The worker that executed partition 1, actually the task corresponding to computing the 
        # pagerank values of partition 1, will save the state and output for that task in its continue queue. When 
        # that worker gets a new incremental DAG, it will get the state and output from its continue
        # queue and retart DAG execution by getting the collapse for partition 1 which is partition 2
        # and executing partition 2 as usual. In general if workers request version n, n>1, (all workers
        # request the same version of the DAG each round), then they have already executed partitions 1 through
        # 2+((n-2)*DAG_executor_constants.INCREMENTAL_DAG_DEPOSIT_INTERVAL), where the last 
        # partition 2+((n-2)*DAG_executor_constants.INCREMENTAL_DAG_DEPOSIT_INTERVAL) is continued and 
        # the preceding partition has a collapse to the continued task. Again, the workers have already executed
        # partitions 1 and 2 and a certain number of partitions that have been added in each new version
        # of the incremental DAG. Version 1 has partitions 1 and 2 and version 2 is the first version that 
        # can be requested by the workers. Note that if they request version 2, they have only excuted
        # partitions 1 and 2 (and really only executed partition 1 as partiton 2 was to-be-continued)
        # so that use of n-2 in this case is 2-2=0 and the last partition 2+(0*DAG_executor_constants.INCREMENTAL_DAG_DEPOSIT_INTERVAL)
        # is 2 as expected.(So 2 is the last partition in version 1, which doesn't man 2 has been executed 
        # since the last partition in an incomplete graph is to-be-continued.)
        #
        # Example: DAG_executor_constants.INCREMENTAL_DAG_DEPOSIT_INTERVAL is 4 and workers request Version 3. 
        # Then the partitions that have been executed are 1, 2, 3, 4, 5, and 6, where version 2 has partitons 1, 
        # 2, 3, 4, 5, and 6, where 6 is to-be-continued, and version 1 has partitions 1 and 2. Workers are 
        # requesting Version 3, which will add the 4 partitions 7, 8, 9, and 10 as 
        # DAG_executor_constants.INCREMENTAL_DAG_DEPOSIT_INTERVAL is 4. In the new version 3 of the DAG, 
        # partition 6 is not-continued and partition 5  has no collapse to a continued state. 
        # (Note that when we add partition 7 to the DAG, we change partition 6 to be non-to-be-continued
        # and we change partition 5 so that it has no collapse to a to-be-continued partition. The new DAG with
        # partition 7, which is a to-be-continued partition (due to DEPOSIT INTERVAL = 4), is not published, and neither 
        # are the DAGS created by adding partitions 8, and 9, respectively. The DAG created by adding 10 is published.)
        # (We have added 8 partitions since we generated the base DAG (with partitions 1 and 2) and
        # 8 mod 4 (DAG INTERVAL) is 0). Note that we added 7, 8, 9, and 10 but we also changed 5
        # and 6, so we need 5 and 6 to be in version 3 of the DAG, i.e., we do not deallocate 5 and 6 so they are in
        # version 3. So we can deallocate the info in the DAG strctures about DAG states 1, 2, 3, 4, which 
        # is 1 through (2+((n-2)*pub_interval))-2, n=3, which is 1 through 2+(1*4)-2 = 4. Note that when workers request 
        # version 3 we can deallocate DAG structure information about partitions 1, 2, 3, 4, in version 2 but 
        # not partitions 5 and 6. This is because the status of 5 and 6 in version 3 is changed from that of version 2.
        # Partition 5 has a collapse to continued task 6 in version 2 but in version 3 task 6 is not
        # to-be-continued and thus 5 no longer has a collapse to a continued task. So when the workers
        # get version 3 of the DAG they restart execution by getting the state and output for already
        # executed task 5 from the continue queue, retrieving the collapse task 6 of state 5, and 
        # executing task 6. Thus we do not dealloate the DAG states for 5 and 6, even though they
        # were in version 2 and we are getting a new version 3.
        # Note that we don't always start deallocation at 1, we start where the last deallocation ended.
        # So in this example, after deallocating 1, 2, 3, and 4, start_deallocation becomes 5. Note that if 
        # workers request version 2, then 2+((n-2)*pub_interval))-2 is 2+(0*4)-2 = 0 so we deallocate from 1 to 0 
        # so no deallocations will be done. As mentioned above, the states for partitions 1 and 2 in 
        # version 1 will change in version 2, so we do not deallocate 1 and 2 when the workers
        # request version 2. Partitions 1 and 2 will be among the partitions deallocated when workers
        # request version 3. (From above, we will deallocate 1, 2, 3, 4.)
        #
        # Note that the initial value of the version number in the DAG_infoBufferMonitor that 
        # maintains the last version number requested by the workers is 1. Workers don't actually 
        # request version 1, as version 1, which contains partitions 1 and 2, is given to
        # the DAG_executor_driver, which starts DAG execution with version 1 of the incremental DAG. 
        # (Partition 1 is a leaf task. Partition 2 either depends on partition 1 or is another leaf task.
        # More leaf tasks can be discovered during incremental DAG generation. This is unlike non-incremental 
        # DAG generation, which discovers all of the leaf tasks during DAG gneration and gives all these leaf tasks
        # to the DAD_executor_driver (as part of the DAG INFO structure). The DAG_executor_driver will 
        # start a lambda for each leaf task, or if workers are being used it will enqueue the leaf tasks
        # in the work queue.)
        # So the first version requested by the users is 2, and the first version deposited
        # in the DAG_infoBufferMonitor is 2. The workers can excute DAG version 1 and request
        # DAG version 2 before bfs() has deposited version 2 into the DAG_infoBufferMonitor.
        # (Deposit makes the new version available to workers, who call withdraw() to get a new
        # version.) Before bfs() deposits new version 2, it will ask DAG_infoBufferMonitor for 
        # the most recent version requested by the workers. This will be the initial value 1
        # since bfs() has not called deposit for the first time to deposit version 2 (which 
        # bfs will do next). bfs will pass 1 to deallocate_DAG_structures as the value
        # of requested_version_number_DAG_info so the value of 
        # 2+((requested_version_number_DAG_info-2)*DAG_executor_constants.INCREMENTAL_DAG_DEPOSIT_INTERVAL))-2
        # will be 2+((1-2) * 4)-2 = -4 so we will deallocate from 1 to -4 so no deallocations
        # as expected. (We cannot deallocate 1 or 2 yet as we are not done with them - in version
        # 2 partition 2 becomes not to-be-continued and partition 1 now has no collapse to a 
        # continued state, so we restart executing the DAG by accessing 1's state information
        # to get 1's collpase task 2, which we can now execute. We already excuted 1 and then
        # stopped execution when we saw 1's collapse was to a to-be-continued state 2)
        # Note: Given the formula 2+((requested_version_number_DAG_info-2)*DAG_executor_constants.INCREMENTAL_DAG_DEPOSIT_INTERVAL))-2,
        # the value given by this formula cannot become greater than 1 until workers request version 3. For
        # version 2, requested_version_number_DAG_info-2 is 2-2 = 0, so we get 2+0-2=0.
        # Note: if the DAG interval is 2, then for requesting version 3, we get 2+((3-2)*2)-2 = 2. Version 2 
        # has partitions 1 2 3 4, which means we can deallocate partitions 1 up to the end partition which is 2.
        # We keep 3 and 4 from version 2 and add 5 and 6 to get a version 3 of 3 4 5 6 where 6 is to-be-continued
        # and 4 is no longer to-be-continued and thus can be executed. (Partition 3 was executed previously then
        # execution stopped at to-be-continued taak/partition 4.)

        # Note: the version number gets incremented only when we publish a new DAG. The first 
        # DAG we publish is the base DAG with partitions 1 and 2. If the interval is 4,
        # the next DAG we generate has partitions 1, 2, and 3 but it is not published since
        # we publish every 4 partitions (after the base DAG) So the next verson is version 2,
        # which will have partitions 1, 2, 3, 4, 5, 6 for a total of 2+4 partitions.

        deallocation_end_index_partitions = (2+((requested_version_number_DAG_info-2)*DAG_executor_constants.INCREMENTAL_DAG_DEPOSIT_INTERVAL))-2

        # we will use deallocation_end_index in a range so it needs to be one past the last partition
        # to be dealloctated. Example, to deallocate "1 to 1" use range(1,2), where 1 is inclusive 
        # but 2 is exclusive; to deallocate "2 , 3, and 4" use range(2,5).
        # 
        # However, don't increment deallocation_end_index unless we can 
        # actually do a deallocation. We can do a deallocation unless start index is 1 and end index is less
        # than 1. If end index > 0 we can deallocate as the value of the above formula for end index
        # just keeps growing as the version number increases.
        # which is implemented as range(1,2), 
        if deallocation_end_index_partitions > 0:
            deallocation_end_index_partitions += 1

        logger.info("deallocate_DAG_structures_groups: self.deallocation_start_index: " + str(self.deallocation_start_index_partitions)
            + " deallocation_end_index_partitions: " + str(deallocation_end_index_partitions))
        for i in range(self.deallocation_start_index_partitions, deallocation_end_index_partitions):
            logger.info("deallocate_DAG_structures_groups: deallocate " + str(i))
            groups_of_partition_i = self.groups_of_partitions[i-1]
            # number of groups >= 1
            number_of_groups_of_partition_i = len(groups_of_partition_i)
            # end index is now 1 past the last group to be deallocated, e.g., 
            # if start is 1 and number of groups is 3 we want to deallocate 
            # 1, 2, and 3. The value 4 is fine for end since we will use a 
            # range(1,4) and 1 is inclusive but 4 is exclusive.
            # After adding number_of_groups_of_partition_i to deallocation_end_index_groups, 
            # deallocation_start_index_groups < deallocation_end_index_groups.

            deallocation_end_index_groups = self.deallocation_start_index_groups+number_of_groups_of_partition_i

            try:
                msg = "[Error]: deallocate_DAG_structures_groups:" \
                    + " deallocation_start_index_groups is not less than deallocation_end_index_groups after add." \
                    + " deallocation_start_index_groups: " + str(self.deallocation_start_index_groups) \
                    + " deallocation_end_index_groups: "  + str(deallocation_end_index_groups)
                assert self.deallocation_start_index_groups < deallocation_end_index_groups , msg
            except AssertionError:
                logger.exception("[Error]: assertion failed")
                if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
                    logging.shutdown()
                    os._exit(0)

            logger.info("deallocate_DAG_structures_groups: "
                + " number_of_groups_of_partition_i: " + str(number_of_groups_of_partition_i)
                + " deallocation_start_index_groups: " + str(self.deallocation_start_index_groups)
                + " deallocation_end_index_groups: "  + str(deallocation_end_index_groups))

            for j in range(self.deallocation_start_index_groups, deallocation_end_index_groups):
                # Note: This deallocation uses group_name = BFS.partition_names[j-1]
                # so deallocate_Group_DAG_structures deallocates group in position j-1
                # This deallocates structures in the DAG_info, which we presumably do 
                # when we have very large DAGs that we do not wanr to pass to each lambda.
                self.deallocate_DAG_structures_lambda(j)
 
                # set start to end if we did a deallocation, i.e., if start < end. 
                # Note that if start equals end, then we did not do a deallocation since 
                # end is exclusive. (And we may have just incremented end, so dealllocating 
                # "1 to 1", with start = 1 and end = 1, was implemented as incrementing 
                # end to 2 and using range(1,2) so start < end for the deallocation "1 to 1"
                # Note that end was exclusive so we can set start to end instead of end+1.
                if self.deallocation_start_index_groups < deallocation_end_index_groups:
                    # remember that this is the most recent version number for which we did
                    # a deallocation
                    self.version_number_for_most_recent_deallocation = requested_version_number_DAG_info
                    self.deallocation_start_index_groups = deallocation_end_index_groups

#brc: ToDo: set most recent here. Also, what is init value of most recent?

#brc: Todo: Note: we do not dealloc groups_of_partitions incrementally? When could we
# do it since we need it here?
        
        # Possibly reset deallocation_start_index_partitions. Note that 
        # deallocation_start_index_groups was reset afer the last group 
        # deallocation, and we know that we did deallocate 1 or more groups
        # of a partition, since we are in ths code. However, we don't know 
        # whether we actually tried to deallocate any partitions, i.e., whether
        # deallocation_start_index_partitions < deallocation_end_index_partitions
        # was true for the for i in range() loop. If not, then we did not 
        # deallocate any groups in any partitions.
        #
        # Set start to end if we did a deallocation, i.e., if start < end. 
        # Note that if start equals end, then we did not do a deallocation since 
        # end is exclusive. (And we may have just incremented end, so dealllocating 
        # "1 to 1", with start = 1 and end = 1, was implemented as incrementing 
        # end to 2 and using range(1,2) so start < end for the deallocation "1 to 1"
        # Note that end was exclusive so we can set start to end instead of end+1.
        if self.deallocation_start_index_partitions < deallocation_end_index_partitions:
            # remember that this is the most recent version number for which we did
            # a deallocation
            self.version_number_for_most_recent_deallocation = requested_version_number_DAG_info
            self.deallocation_start_index_partitions = deallocation_end_index_partitions

        # Example. Suppose the publishing interval is 4, i.e., after publishing the
        # base DAG (version 1) with partitions 1 and 2, we publish version 2 with 
        # 1 2 3 4 5 6 (base DAG plus 4 partitions). Before bfs() calls depost()
        # to publish this DAG it gets the last requsted DAG version which is initializd
        # to 1 and passes 1 to deallocate_DAG_structures.
        # For deallocation_end_index_partitions = (2+((requested_version_number_DAG_info-2)
        #    * DAG_executor_constants.INCREMENTAL_DAG_DEPOSIT_INTERVAL))-2
        # we get end is -4. Thus the for loop for i in range() does not execute
        # and we do not do any deallocations. Also, since deallocation_start_index_partitions
        # is not less than  deallocation_end_index_partitions we do not set start to end.
        # 
        # Next, bfs() will generate version 3 with 1 2 3 4 5 6 7 8 9 10, which has
        # 2 partitions from the base DAG version 1 and 2*4 partitions for versions
        # 2 and 3. bfs() generats DAG version 3 and before bfs()
        # calls deposit() to publish this new DAG version 3 it gets the last requsted 
        # DAG version which we assume is 2 and passes 2 to deallocate_DAG_structures.
        # For deallocation_end_index_partitions = (2+((requested_version_number_DAG_info-2)
        #    * DAG_executor_constants.INCREMENTAL_DAG_DEPOSIT_INTERVAL))-2
        # we get end is 0. Thus the for loop for i in range() does not execute
        # and we do not do any deallocations. Also, since deallocation_start_index_partitions
        # is not less than deallocation_end_index_partitions we do not set start to end.
        # Recall that the first version for which deallocations can be done is 3, since
        # the formula for end will evaluate to 1 when the version number is 3.
        #
        # Next, bfs() will generate version 4 with partitions 1 - 14, which has
        # 2 partitions from the base DAG version 1 and 2*4 partitions for versions
        # 2 and 3. bfs() generates DAG version 3 and before bfs()
        # calls deposit() to publish this new DAG version 3 it gets the last requested 
        # DAG version which we assume is 3 and passes 3 to deallocate_DAG_structures.
        # For deallocation_end_index_partitions = (2+((requested_version_number_DAG_info-2)
        #    * DAG_executor_constants.INCREMENTAL_DAG_DEPOSIT_INTERVAL))-2
        # we get end is 2. Thus the for loop for i in range(1,3) will execute. We
        # describe the groups deallocations for partitions 1 and 2 below, Also,
        # since deallocation_start_index_partitions is less than deallocation_end_index_partitions 
        # we will set start to end which sets start to 3, as in the next partition 
        # whose groups will be deallocated is partition 3 (after just deallocating the 
        # groups in partitions 1 and 2.)
        # Assume we are building a DAG for the white board example.
        # For the for i in range(1,3) loop, with i = 1 we do 
        # groups_of_partition_i = BFS.groups_of_partitions[i-1]
        # number_of_groups_of_partition_i = len(groups_of_partition_i)
        # which sets groups_of_partition_i to a list containing the single group PR1_1 in 
        # partition 1 so that number_of_groups_of_partition_i is 1. Now
        # deallocation_end_index_groups = deallocation_start_index_groups+number_of_groups_of_partition_i
        # = 1 + 1 = 2, which gives the for loop for j in range(1,2) (where 2 is exclusive).
        # This loop will deallocate the single group in partition 1:
        # deallocate_Group_DAG_structures(j) where this method accesses position j-1
        # since the positions in structures we are deallocating start with position 0,
        # i.e., the first group PR1_1 is in position 0, as in group_name = BFS.partition_names[j-1]
        # After this for j in range (1,2) ends we set 
        # deallocation_start_index_groups = deallocation_end_index_groups
        # so deallocation_start_index_groups is set to 2. This means the next group
        # that we deallocate is group 2 (in position 1).
        # 
        # The for loop for i in range(1,3) continues with i = 2. We do
        # groups_of_partition_i = BFS.groups_of_partitions[i-1]
        # number_of_groups_of_partition_i = len(groups_of_partition_i).
        # This sets groups_of_partition_i to a list containing the groups of 
        # partition PR2_1L, which are PR2_1, PR2_2L, and PR2_3, so that
        # number_of_groups_of_partition_i is 3. (Note that partition 2 is PR2_1L
        # where the L means that the nodes in partition 2 have a loop (cycle) so
        # there must be a group in partition 2 that has a cycle, this is group PR2_2L,
        # which is the 2nd group of the 3 groups in parititon PR2_1L.
        # Now deallocation_end_index_groups = deallocation_start_index_groups+number_of_groups_of_partition_i
        # = 2 + 3 = 5, which gives the for loop for j in range(2,5) (where 5 is exclusive).
        # This loop will deallocate the three groups in partition 2 using
        # deallocate_Group_DAG_structures(j). After this for j in range (2,5) loop ends we set 
        # deallocation_start_index_groups = deallocation_end_index_groups
        # so deallocation_start_index_groups is set to 5. This means the next group
        # that we deallocate will be group 5, the first group of partition 3.

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
        self.num_nodes = kwargs['num_nodes']

        # Note: cummulative_leaf_tasks gets cleared after we start the new leaf tasks.
        # For debugging - all leaf tasks started so far
        self.cummulative_leaf_tasks += new_leaf_tasks
        if DAG_executor_constants.DEALLOCATE_DAG_INFO_STRUCTURES_FOR_LAMBDAS \
            and (self.num_nodes > DAG_executor_constants.THRESHOLD_FOR_DEALLOCATING_ON_THE_FLY):
            if DAG_executor_constants.USE_PAGERANK_GROUPS_INSTEAD_OF_PARTITIONS:
                # Note: if we are not using groups then groups_of_partitions_in_current_batch is []
                # and nothing is added to self.groups_of_partitions on the extend, which works too
                #
                # this is a list of lists
                groups_of_partitions_in_current_batch = kwargs['groups_of_partitions_in_current_batch']
                # each of the lists in groups_of_partitions_in_current_batch is appended to list self.groups_of_partitions
                self.groups_of_partitions.extend(groups_of_partitions_in_current_batch)
                logger.info("DAG_infoBuffer_Monitor_for_Lambdas: extended list of groups_of_partitions:")
                logger.info(self.groups_of_partitions)
                for list_of_group_names in groups_of_partitions_in_current_batch:
                    self.group_names.extend(list_of_group_names)
            else:
                # this is a list of lists
                partition_names_in_current_batch = kwargs['partition_names_in_current_batch']
                # each of the lists in groups_of_partitions_in_current_batch is appended to list self.groups_of_partitions
                self.partition_names.extend(partition_names_in_current_batch)
                logger.info("DAG_infoBuffer_Monitor_for_Lambdas: extended list of partition names:")
                logger.info(self.partition_names)

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
            # not using real lambdas, using threads that simulate a lambda.
            try:
                # start simulated lambdas with the new DAG_info
                first = True
                logger.info("DAG_infoBuffer_Monitor_for_Lambdas: number of withdraw tuples: " + str(len(self._buffer)))
                if len(self._buffer) == 0:
                    logger.info("DAG_infoBuffer_Monitor_for_Lambdas: no withdraw tuples so no lambdas started.")

#brc: The start index needs to be reset even if there are no tuples since we 
# deposit a new DAG with no Nones?
# Need these deallocs to be conditional on options

#brc: ToDo: What about the save structures? clear them? or just remove key when we
# grab the restored value. We can clear since we are adding key-value pairs so
# not using list.
                if DAG_executor_constants.DEALLOCATE_DAG_INFO_STRUCTURES_FOR_LAMBDAS \
                        and (self.num_nodes > DAG_executor_constants.THRESHOLD_FOR_DEALLOCATING_ON_THE_FLY):
                    self.deallocation_start_index_groups = 1
                    self.deallocation_start_index_partitions = 1
                    self.most_recent_deallocation_end_index = 0

                # If we are using incremental partitions, then we generating the
                # DAG by searching the the connected components of partitions one at
                # a time. If lambda L finds a partition P that has a collapse partition 
                # C that is to be continued, then L will call withdraw() to get another
                # incemental DAG, but no other lambda is executing, i.e.,
                # there is no other CC of partitions being executed so if
                # L adds a withdraw tuple in withdraw() representing that a new Lambda
                # # must be started to resume L's execution of C then there will be 
                # only one withdraw tuple (which is for L) when a new DAG is deposited.'
                # If the yobecontinued partition C is the last partition in the CC being 
                # executed by E, then C will be complete in the new ADG and after executing 
                # this now complete partition C, lambda E will terminate. So again
                # only one lambda will be executing (which is the lambda for executing
                # the next CC.) In general, only one lambda can have an
                # incomplete partition and thus only one can call withdraw(). This is true
                # even if the publishing interval for new incremental DAGs is a number n
                # such that m, m>1, CCs are processed in this interval. In this case, at least
                # m-1 CCs will not have an incomplete CC, i.e., all if these m-1 CCs
                # will have a last partition that is not tobecontinued. It is possible
                # the one CC, which is the CC currently being processed has a tobecontinued
                # partition. Note that whenever we publsh a new incremental ADG, the 
                # last partition that was generated is tobecontinued unless it is the 
                # last partition in the DAG, i.e., there are no more partitions to be 
                # generated. Noet: When we generate a parttion P that is the last partitions
                # of a CC ad there are more CCs to process, then P will be marked as 
                # tobecontinued and thrn we we procss the first partition of the next CC
                # we will know that P was the last partition of its CC and we will mark P
                # as not tobecontinued. (Thus we may publish a DAG that has P being tobecontinud
                # and then publish the next DAG with P being not tobecontinued, if the 
                # publishing interval happends to require a new DAG to be generated with P 
                # tobecontinued so the next DAG will be published with P not tobecontinued.
                # We assert all of this next:
                if not DAG_executor_constants.USE_PAGERANK_GROUPS_INSTEAD_OF_PARTITIONS:
                    try:
                        msg = "[ERROR]:DAG_infoBuffer_Monitor_for_Lambdas:" \
                            + " using partitions, but self._buffer has more than one withdraw tuple. "
                        assert len(self._buffer <= 1) , msg
                    except AssertionError:
                        logger.exception("[Error]: assertion failed")
                        if DAG_executor_constants.exit_program_on_exception:
                            logging.shutdown()
                            os._exit(0)

                for withdraw_tuple in self._buffer:
#brc: dealloc: get requested_current_version_number and get start_tuple from _buffer tuple
# for withdraw_tuple in self._buffer:
#   requested_current_version_number = withdraw_tuple[0]; 
#   start_tuple = withdraw_tuple[1]
# if DEALLOCATE_DAG_INFO_STRUCTURES_FOR_LAMBDAS and (num_nodes_in_graph > DAG_executor_constants.THRESHOLD_FOR_DEALLOCATING_ON_THE_FLY):
#    use requested_current_version_number to prune
# in sorted order by requsted version numbr so the higher the number the more you can dealloc
# So: keep deallocating more and more: I think you can always start where you left off, i.e.,
# if you last dealloc 10, and the next version requsted is at lest 1 verion higher thaen you 
# can start dealloc at 11 (if version is the same then do nothing).
# We are dealloc structures in a DAG_info, not a Partition/Group internal structure.
# But eventuallly you get a new Deposit which has no deallocs; also, you can get a withdraw
# with a requsted version number that is smaller than the last round, and so has fewer
# deallocs than the resulring DAG_info from the last round.
# Note: we are saving the deallocated items in case we need them but since they aer sorted
# we do not need them in the current round?
#
# Consider: when you deposit, you only deposit the new suffix? so yuo can keep the deallocs
# that you have done so far? but if you get a smaller request number then yuo need to 
# put some deallocated items back?
#
# For simulated lambdas, the incremental DAG generation and dealloc methods are running
# locally and concurrently:
# Note: doing dealloc in deposit so not concurrently dealloc items and adding new items
# in incremental DAG generation methods (as bfs() thread calls both the incemental
# DAG generation method and the deposit())
# Note: do dealloc in withdraw too, which can be concurrent with incemental DAG generation
# methods adding to DAG. As lambdas call withdraw to get a new DAG and bfs
# calls incremental DAG generation methods and workers/lambdas run concurrently 
# with bfs()
# For real lambdas, the monitor object with deposit() method is on the tcp_server process
# and bfs() is running in a different process (tastAll) so the DAG_info object in deposit()
# is seperte from the Partition/Group structures used to create the DAG_info being 
# accessed by bfs(), i.e., no concurrent access.
#
# Note: For real lambdas, the monitor is on the server so dag info is not a reference to
# partition/group internal info. Not so for simuated lambdas. For the latter 
# we make a deep copy so they are seperate, i.e., so changing one does not change the other.

                    requested_current_version_number = withdraw_tuple[0]
                    logger.info("DAG_infoBuffer_Monitor_for_Lambdas: deposit: "
                        + "requested_current_version_number: " +  str(requested_current_version_number))
                    start_tuple = withdraw_tuple[1]
                    # pass the state/task the thread is to execute at the start of its DFS path
                    start_state = start_tuple[0]

                    # we know this is true: requested_current_version_number <= self.current_version_number_DAG_info:
                    # The requested_current_version_number is either:
                    # - less than version_number_for_most_recent_deallocation: need to restore a suffix
                    #      of deallocation 
                    # - equal to version_number_for_most_recent_deallocation: nothing to do
                    # - greater than version_number_for_most_recent_deallocation: Can deallocate a suffix
                    #
                    # Note: First request is for version number 2. This will be greater than
                    # version for last deallocation, which was inited to 0, so try to deallocate
                    # but nothing deallocated. We have above that 
                    # requested_current_version_number <= self.current_version_number_DAG_info
                    # so we will get version 2 or greater. If we get version 2, we will 
                    # request version 3, and we can do the first deallocation, setting
                    # version for last deallocation to n, where n >=3. At that point, some
                    # lambda can request 2, which will be less then n, so we will do a restore.

                    # Note: We only do deallocations/restors if there is at leat one
                    # withdraw_tuple since we need to know the current requested 
                    # version in order to do deallocations.
                    # Q: Can we do sme deallocations based on the most recent requested
                    # version considering that the next requested version may be 
                    # higher than the last? Or just wait until we get the next
                    # request, which will be when we get a call to withdraw()
                    if DAG_executor_constants.DEALLOCATE_DAG_INFO_STRUCTURES_FOR_LAMBDAS \
                            and (self.num_nodes > DAG_executor_constants.THRESHOLD_FOR_DEALLOCATING_ON_THE_FLY):

                        if not DAG_executor_constants.USE_PAGERANK_GROUPS_INSTEAD_OF_PARTITIONS:
                            if requested_current_version_number < self.version_number_for_most_recent_deallocation:
                                try:
                                    msg = "[Error]: deposit:" \
                                        + " requested_current_version_number < self.version_number_for_most_recent_deallocation:" \
                                        + " but first is False."
                                    assert first , msg
                                except AssertionError:
                                    logger.exception("[Error]: assertion failed")
                                    if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
                                        logging.shutdown()
                                        os._exit(0)

                                # We will set self.version_number_for_most_recent_deallocation in restore
                                # if we do restores
                                self.restore_DAG_structures_partitions(requested_current_version_number)
                            elif requested_current_version_number > self.version_number_for_most_recent_deallocation:
                                self.deallocate_DAG_structures_partitions(requested_current_version_number)
                                # We will set self.version_number_for_most_recent_deallocation in restore
                                # if we do restores
                            else:   # requested_current_version_number == self.version_number_for_most_recent_deallocation:
                                pass
                        else:
                            if requested_current_version_number < self.version_number_for_most_recent_deallocation:     
                                try:
                                    msg = "[Error]: deposit:" \
                                        + " requested_current_version_number < self.version_number_for_most_recent_deallocation:" \
                                        + " but first is False."
                                    assert first, msg
                                except AssertionError:
                                    logger.exception("[Error]: assertion failed")
                                    if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
                                        logging.shutdown()
                                        os._exit(0)

                                # We will set self.version_number_for_most_recent_deallocation in restore
                                # if we do restores
                                self.restore_DAG_structures_groups(requested_current_version_number)
                            elif requested_current_version_number > self.version_number_for_most_recent_deallocation:
                                # We will set self.version_number_for_most_recent_deallocation in restore
                                # if we do restore
                                self.deallocate_DAG_structures_groups(requested_current_version_number)
                            else:   # requested_current_version_number == self.version_number_for_most_recent_deallocation:
                                pass  

                    if first:
                        first = False        

                    #ToDo: Consider passing delta of DAG_info instead of entire DAG.

                    """ OLD
                    if DAG_executor_constants.DEALLOCATE_DAG_INFO_STRUCTURES_FOR_LAMBDAS \
                        and (self.num_nodes > DAG_executor_constants.THRESHOLD_FOR_DEALLOCATING_ON_THE_FLY):
                        if not DAG_executor_constants.USE_PAGERANK_GROUPS_PARTITIONS:
                            self.deallocate_DAG_structures_partitions(requested_current_version_number)
                        else:
                            self.deallocate_DAG_structures_groups(requested_current_version_number)
                    """

                    # If no restore/dealloc then saved wil be the asm as previous
                    if DAG_executor_constants.DEALLOCATE_DAG_INFO_STRUCTURES_FOR_LAMBDAS \
                            and (self.num_nodes > DAG_executor_constants.THRESHOLD_FOR_DEALLOCATING_ON_THE_FLY):

                        logger.info("DAG_infoBuffer_Monitor_for_Lambdas: DAG_info deposited after deallocation: ")
    
                        logger.info("DAG_infoBuffer_Monitor_for_Lambdas: all saved deallocations for deposit:")
                        logger.info("DAG_infoBuffer_Monitor_for_Lambdas: saved DAG_map:")
                        for key, value in self.current_version_DAG_info_DAG_map_save.items():
                            logger.info(key)
                            logger.info(value)
                        logger.info(" ")
                        logger.info("DAG_infoBuffer_Monitor_for_Lambdas: saved DAG states:")         
                        for key, value in self.current_version_DAG_info_DAG_states_save.items():
                            logger.info(key)
                            logger.info(value)
                        logger.info(" ")
                        logger.info("DAG_infoBuffer_Monitor_for_Lambdas: saved DAG tasks:")
                        for key, value in  self.current_version_DAG_info_DAG_tasks_save.items():
                            logger.info(str(key) + ' : ' + str(value))
                        logger.info(" ")

                    self.print_DAG_info(self.current_version_DAG_info)
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
                    # we supply the input for the collpase task and execute the collapse task.
                    # That is, if partion P has a collapse task task that is a continued task,
                    # we send P's output and its collapse task C as parameters of deposit() then
                    # when we "restart" we execute C with P's output as C's input.
                    input_or_output = start_tuple[1]    # output of P and input of collapse task C
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

                if DAG_info_is_complete:
                    # If the DAG_info is complete then we can start the leaf task 
                    # as it is also complete. 
                    self.continue_queue += new_leaf_tasks
                else:
                    # leaf task is unexecutable now. Execute it on next deposit().
                    # Note: # We can also start a leaf task if we know it 
                    # is the last group/partition of a component even if the DAG_ifo is 
                    # not complete. For a partition (and the groups therein) bfs can track
                    # whether any nodes in this partition (and grooups therein) have any
                    # children. If not, then this partition (and the groups therein) is the 
                    # last partition in the current connected component and we can start 
                    # a lambda for the leaf task now. But this is a lot of work.
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
                if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
                    logging.shutdown()
                    os._exit(0)
        else:
            # using real lambdas
            try:
                # start real lambdas with the new DAG_info
                #for start_tuple in self._buffer:
                for withdraw_tuple in self._buffer:
                    requested_current_version_number = withdraw_tuple[0]
                    logger.info("DAG_infoBuffer_Monitor_for_Lambdas: deposit: "
                        + "requested_current_version_number: " +  str(requested_current_version_number))
                    start_tuple = withdraw_tuple[1]
                    try: 
                        logger.info("DAG_infoBuffer_Monitor_for_Lambdas: deposit: get start tuple fields.")
                        # pass the state/task the thread is to execute at the start of its DFS path
                        start_state = start_tuple[0]
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
            + str(requested_current_version_number)
            + ", self.current_version_number_DAG_info: " 
            + str(self.current_version_number_DAG_info))

        DAG_info = None
        restart = False
        if requested_current_version_number <= self.current_version_number_DAG_info:
            DAG_info = self.current_version_DAG_info
            logger.info("DAG_infoBuffer_Monitor_for_Lambdas: withdraw will return current Lambda"
                " with version " + str(self.current_version_number_DAG_info))

#brc: ToDo: do deallocation here. But if next call is also to withdraw(), then requested version may
# be out of order i.e., this version is less than that of the previous dealloc. (Note: by 
# definition requested_current_version_number <= self.current_version_number_DAG_info.) In that
# case, we need to restore at least part of the old DAG_info states. Note also that if the next
# call is to deposit then we have a new DAG_info to work with so whatever we did in withdraw
# is gone, perhaps unless we deposit just the new art and go from there.
# Note: Can we carry over some of the withdraw stuff? Or soem of the previous eposits stuff?
# otherwise we have to start over since we do not prune the Partition/Group structures.
# And there is a good chance that version numbers are getting higher. 
# Note: If the version number is a lot lower we can do the prunes on the new DAG instead of 
# restoring from the saved DAG states. (but we may just deposit the new stuff)
#
# Note, we pass the entire new DAg from bfs to deposit() which could be a lot, just like
# passing the entire DAG to the lamdas.

            # Note: We cannot get here until after the first deposit() and deposit()
            # saves the num_nodes parameter value passed to it in self.num_nodes.

            # Possibly need to restore deallocs from prev deposit, i.e., if requsted version
            # number for this withdraw is less than the requested version number for last deposit or
            # last non-blocking withdraw, then restore deallocs at the end.

            if DAG_executor_constants.DEALLOCATE_DAG_INFO_STRUCTURES_FOR_LAMBDAS \
                    and (self.num_nodes > DAG_executor_constants.THRESHOLD_FOR_DEALLOCATING_ON_THE_FLY):
                # we know this is true: requested_current_version_number <= self.current_version_number_DAG_info:
                # The requested_current_version_number is either:
                # - less than version_number_for_most_recent_deallocation: need to restore a suffix
                #      of deallocation 
                # - equal to version_number_for_most_recent_deallocation: nothing to do
                # - greater than version_number_for_most_recent_deallocation: Can deallocate a suffix
                #
                # Note: First request is for version number 2. This will be greater than
                # version for last deallocation, which was inited to 0, so try to deallocate
                # but nothing deallocated. We have above that 
                # requested_current_version_number <= self.current_version_number_DAG_info
                # so we will get version 2 or greater. If we get version 2, we will 
                # request version 3, and we can do the first deallocation, setting
                # version for last deallocation to n, where n >=3. At that point, some
                # lambda can request 2, which will be less then n, so we will do a restore.
                if not DAG_executor_constants.USE_PAGERANK_GROUPS_INSTEAD_OF_PARTITIONS:
                    if requested_current_version_number < self.version_number_for_most_recent_deallocation:
                        # We will set self.version_number_for_most_recent_deallocation in restore
                        # if we do restores
                        self.restore_DAG_structures_partitions(requested_current_version_number)
                    elif requested_current_version_number > self.version_number_for_most_recent_deallocation:
                        self.deallocate_DAG_structures_partitions(requested_current_version_number)
                        # We will set self.version_number_for_most_recent_deallocation in restore
                        # if we do restores
                    else:   # requested_current_version_number == self.version_number_for_most_recent_deallocation:
                        pass
                else:
                    if requested_current_version_number < self.version_number_for_most_recent_deallocation:
                        # We will set self.version_number_for_most_recent_deallocation in restore
                        # if we do restores
                        self.restore_DAG_structures_groups(requested_current_version_number)
                    elif requested_current_version_number > self.version_number_for_most_recent_deallocation:
                        # We will set self.version_number_for_most_recent_deallocation in restore
                        # if we do restores
                        self.deallocate_DAG_structures_groups(requested_current_version_number)
                    else:   # requested_current_version_number == self.version_number_for_most_recent_deallocation:
                        pass          

                #ToDo: Consider passing delta of DAG_info instead of entire DAG.

            """ OLD
            if DAG_executor_constants.DEALLOCATE_DAG_INFO_STRUCTURES_FOR_LAMBDAS \
                and (self.num_nodes > DAG_executor_constants.THRESHOLD_FOR_DEALLOCATING_ON_THE_FLY):

                if not DAG_executor_constants.USE_PAGERANK_GROUPS_PARTITIONS:
                    self.deallocate_DAG_structures_partitions(requested_current_version_number)
                else:
                    self.deallocate_DAG_structures_groups(requested_current_version_number)
            """

            # If no restore/dealloc then saved wil be the asm as previous
            if DAG_executor_constants.DEALLOCATE_DAG_INFO_STRUCTURES_FOR_LAMBDAS \
                    and (self.num_nodes > DAG_executor_constants.THRESHOLD_FOR_DEALLOCATING_ON_THE_FLY):

                logger.info("DAG_infoBuffer_Monitor_for_Lambdas: DAG_info deposited after deallocation: ")

                logger.info("DAG_infoBuffer_Monitor_for_Lambdas: all saved deallocations for deposit:")
                logger.info("DAG_infoBuffer_Monitor_for_Lambdas: saved DAG_map:")
                for key, value in self.current_version_DAG_info_DAG_map_save.items():
                    logger.info(key)
                    logger.info(value)
                logger.info(" ")
                logger.info("DAG_infoBuffer_Monitor_for_Lambdas: saved DAG states:")         
                for key, value in self.current_version_DAG_info_DAG_states_save.items():
                    logger.info(key)
                    logger.info(value)
                logger.info(" ")
                logger.info("DAG_infoBuffer_Monitor_for_Lambdas: saved DAG tasks:")
                for key, value in  self.current_version_DAG_info_DAG_tasks_save.items():
                    logger.info(str(key) + ' : ' + str(value))
                logger.info(" ")

            logger.info("DAG_infoBuffer_Monitor_for_Lambdas: DAG_info withdrawn after deallocation: ")
            self.print_DAG_info(self.current_version_DAG_info)

            
#brc: dealloc: here too 
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
            # save start tuple which deposit will use to restart lambda
            # Call is: requested_current_version_number,DAG_executor_state.state,output
            # of which value is DAG_executor_state.state and output which is the info needed
            # to restart the lambda.
            start_tuple = kwargs["value"]
            logger.info("DAG_infoBuffer_Monitor_for_Lambdas: withdraw: value to deposit: " + str(start_tuple))

#brc: dealloc: 
            """
            from collections import namedtuple
            from operator import attrgetter
            from bisect import insort
            # Q use _buffer since it is a list so use _buffer for both dealloc and not dealloc?
            # Then we want the tuple in buffer to be (requested version number, start_tuple) for both
            Restarted_Lambda = namedtuple('Restarted_Lambda', ('requested_current_version_number', 'start_tuple'))
            by_requested_current_version_number = attrgetter('requested_current_version_number')
            restarted_lambda_tuple = Restarted_Lambda(requested_current_version_number,start_tuple)
            # getting attr requested_current_version_number of restarted_lambda_tuple
            insort(self._buffer, restarted_lambda_tuple, key=by_requested_current_version_number)
            """

#brc: dealloc: : put requested_current_version_number and start_tuple as a tuple in _buffer
            withdraw_tuple = (requested_current_version_number,start_tuple)
            #self._buffer.append(start_tuple)
            self._buffer.append(withdraw_tuple)
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
