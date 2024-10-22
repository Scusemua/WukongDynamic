from .monitor_su import MonitorSU
import _thread
import threading
import time
import uuid
import os
import traceback

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
        # Note: We refer to "start_index" and "end_index" but we are not deallocating elements
        # in some specified "position" or at "index" x in an array or list. The elements we
        # deallocate are in a map. The keys in a map are integers 1, 2, 3, etc, referring to
        # partitions/groups 1, 2, 3, etc so when we dealocate from "1 to 3" we delete the key-value
        # pairs with keys 1, 2, 3 from the map. The fact that keys 1, 2, 3 form a "range" makes
        # it helpful to use the notion of index as in 'from start_index to end_index". We always 
        # deallocate consecutive key values 1, 3, 3 etc.
        #
        # The first partition/group is partition/group 1 not 0, where 1 will be the key and the 
        # value will be information about partition/group 1, e.g., its fanins/fanouts and task.
        self.deallocation_start_index_groups = 1
        self.deallocation_start_index_partitions = 1
        # if a lambda L1 requests a new version i of the DAG, and new DAG version j, j>=i is available
        # we may be able to deallocaet some of the information in the new DAG before we send it
        # to the lambda. This deallocation is based on the i the requested version number. The
        # value of i is a measure of the progress the lambda has made in executing its ADG, i.e.,
        # it has executed all the tasks in version1, 2, 3, ... i-2 so we can delete/deallocate infomation
        # about these old alrady executed tasks. If we do deallocate information, then we save the
        # value of i that was usd as the basis of these deallocations in self.version_number_for_most_recent_deallocation.
        # We will need this value in case some other lamda L2 issues a request for version k, k<i, in which 
        # case some of the deallocated information needs to be restored as the information is needed
        # by L2. We will restore the deallocated info before we send the resulting DAG to L2
        self.version_number_for_most_recent_deallocation = 0
        # We also save the index for the last deallocation that was made. This is needed for the restore
        # operation. for example, if we dealloctat 1, 2, 3, 4, 5, 6 and we need to restore values,
        # the restored values will always be a suffix of the deallocated values, e.g., we may 
        # restore suffix 4, 5, and 6. Restore will compuet the start_index of the restore
        # and use self.most_recent_deallocation_end_index as the end ndex of the restore.
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
        # list of all group names, which is generted from the list of groups (names)
        # in each partition,
        self.group_names = []
        # list of all partition ames. (See preceding comment)
        self.partition_names = []
        
        # when we dealloctate values we save them in cse we need to restoer them. Essentially
        # we move deallocated values to the save collection and possble move them back.
        self.current_version_DAG_info_DAG_map_save = {}
        self.current_version_DAG_info_DAG_tasks_save = {}
        self.current_version_DAG_info_DAG_states_save = {}

        self.num_nodes = 0


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
            partition_or_group_names = self.partition_names
        else:
            partition_or_group_names = self.group_names # BFS.group_names
        #logger.info("restore_DAG_structures_lambda: partition or group namesXXX: ")
        #for n in partition_or_group_names:
        #    logger.info(n)
        # get the name of the partition or group
        name = partition_or_group_names[i-1]
        # use the name to get the state/task whose information is being restord,
        # e.g., restore DAG info about partition "PR2_1"
        state = self.current_version_DAG_info_DAG_states_save[name]
        logger.info("restore_DAG_structures_lambda: partition or group name: " + str(name))
        logger.info("restore_DAG_structures_lambda: state: " + str(state))
        # These are the collections from which we previously deallocated information
        # and saved the deallocated info. Restore this info.
        # We remove key-value pairs from saved structures. This raises an exception 
        # if the key is not found. This is a terminal error. 
        self.current_version_DAG_info.DAG_map[state] = self.current_version_DAG_info_DAG_map_save[state]
        try:
            del self.current_version_DAG_info_DAG_map_save[state]
        except KeyError:
            logger.exception("[Error]: KeyError: key " + str(state))
            traceback.print_exc() 
            if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
                logging.shutdown()
                os._exit(0)

        self.current_version_DAG_info.DAG_states[name] = self.current_version_DAG_info_DAG_states_save[name]
        try:
            del self.current_version_DAG_info_DAG_states_save[name]
        except KeyError:
            logger.exception("[Error]: KeyError: key " + str(state))
            traceback.print_exc() 
            if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
                logging.shutdown()
                os._exit(0)
        self.current_version_DAG_info.DAG_tasks[name] = self.current_version_DAG_info_DAG_tasks_save[name]
        try:
            del self.current_version_DAG_info_DAG_tasks_save[name]
        except KeyError:
            logger.exception("[Error]: KeyError: key " + str(state))
            traceback.print_exc() 
            if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
                logging.shutdown()
                os._exit(0)

    def restore_DAG_structures_partitions(self,requested_version_number_DAG_info):
        """
        This if-condition is true when we call restore_DAG_structures_partitions:
            requested_current_version_number < self.version_number_for_most_recent_deallocation:
        so when we did deallocations for version self.version_number_for_most_recent_deallocation
        we did a certain number of deallocations based on that version number (we use the version 
        number to compute end in the range (start,end+1) of the deallocations). But the 
        requested_current_version_number is less than the version number used for the last deallocation
        so we have deallocated too many items given the current requested_current_version_number.
        That is the current DAG_info (the last DAG_info deposited) has items that were deallocated 
        when it was given to the lambda that made the request for version_number_for_most_recent_deallocation, 
        but these item are needed in the DAG_info that we will give to the lambda that made the request for 
        requested_current_version_number that we are now processing. This means we need to restore some of 
        these items in the current DAG_info. We have saved every item that we have deallocaed so we can 
        restore items.

        Example: Suppose the current DAG_info is version 3, and we have processed a request for version 3
        from a lambda. Assume the publishing interval is 4. Then the current DAG_info version 3 had partitions
            1 2 3 4 5 6 7 8 9 10
        where 1 and 2 were added for version 1, 3, 4, 5, and 6 were added for version 2, and 7, 8, 9, and 10
        were added for version 3. For the version 3 request, the end index for deallocations was
            deallocation_end_index = (2+((requested_version_number_DAG_info-2)*DAG_executor_constants.INCREMENTAL_DAG_DEPOSIT_INTERVAL))-2
        = 2 + ((3-2)*4) - 2 = 4 so we deallocated items 1, 2, 3, and 4. (In version 2, 6 was tobecontinued and 
        5 had a collapse to tobecontinued 6. We generate version 3 by adding 7, 8, 9, and 10 and we also
        change 6 to be not-tobecontinued and 5 no lober has a collapse to a tobecontinued partition so
        version 3 contains 5 and 6 since we changed 5 and 6 and 7, 8, 9 and 10 since we added them. Recall
        that some lmabda was unable to do the coallpse for 5 so it requested a new DAG with version 3 
        but one was not available. Whe version 3 was deposited, we do the deallocations just described and 
        start a lambda that will continue where it left off in version 2, i.e., it will execute the collapse 
        task 6 of 5. (We pass the output of 5 to the lambda which will use it as the input of 6) At the 
        end of these dealloctions, we set self.version_number_for_most_recent_deallocation to 3
        and self.most_recent_deallocation_end_index to 4. Thus the DAG_info for a lambda that will 
        rstart its execution by completing the execution of the partitions in version 1 should 
        have no deallocations, i.e., it needs the version 1 partititions 1 and 2 to be in the 
        version 2 DAG it receives. The current DAG_info does not have information for partitions
        1, 2, 3, and 4 since they were deallocated. (It has 5, 6, 7, 8, 9, and 10). This means 
        we need to restore information about 1, 2, 3, and 4 in the current version 3 DAG_info.
        The end index computed for version 2 was 0, i.e., the last valid partition to be deallocated
        is 0 so everything deallocated beyond 0 needs to be restored. The end of the last 
        dealloation is self.most_recent_deallocation_end_index which is 4, so we restore
        items (end index+1) to self.most_recent_deallocation_end_index, which is (0+1)
        to self.most_recent_deallocation_end_index, which is 1 to 4. After the restoration
        version 3 contains 1 - 10. (The lambda requested version 2 but it is okay to give it 
        version 3 or any version >= 2)
        
        Suppose now a lambda requests version 2. The end index for deallocations is 2 + ((2-2)*4) -2 = 0
        which means that no deallocations can be done. (Partitions 1 and 2 in version 1 need to also 
        be in version 2 since the version 2 DAG changes the tobecntinued infromation for 1 and 2 and 
        a lambda that gets version 2 will begin by executing the collapse task 2 of 1, and then continue
        by excuting 4, 4, etc.)

        The restoration is always a suffix of the deallocated items. That is,
        for the new requested_current_version_number which we know is less than 
        the version_number_for_most_recent_deallocation, we still want to 
        deallocate all the items starting at 1, but we need to stop deallovating
        items that are past the end index that is computed using
        requested_current_version_number. Note that for partitions, we can get this
        end index from the usual "deallocation_end_index =" formula. 
        ...
        
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
        with Connected Components (CC) PR1_1 - PR3_1, PR4_1 - PR9_1, and PR10_1 - PR15_1. The 
        version 1 (base) DAG is PR1_1 - PR2_1, with PR1_1 complete and PR2_1 tobecontinued (TBC). 
        With pubication interval 1, the next DAG (version 2) is PR1_1 - PR3_1, with partition 2 now complete
        and partition 3 TBC. The lambda executor will execute partition 2 and see that partiton 3
        is TBC and request a new DAG. Assume the new DAG has partition 3, which is the last DAG in the first CC 
        and it is complete, while partition 4 is the first partition in the new DAG and is TBC.
        Partition 3 is executed and since partiton 3 has no collapse task, i.e., succeeding task, the executor
        will terminate. The DAG generator will deposit() a new DAG that has a complete partition 4, which is the
        first partition of the next CC and a TBC partition 5.
        A new lambda is started to execute leaf task partition 4. (A leaf task is the first task
        of a CC.) This new lambda will execute 4, see that 5 is TBC and request a new DAG. In the 
        new DAG, assume 5 is complete and 6 is TBC. 5 is executed and since 6 is incomplete, a 
        new DAG is requsted. In the new DAG, assume 6 is complete and 7 is TBC. Suppose 6 is executed anf
        it takes a long time to execute 6.
        In the mean time, the DAG generator, will continue to produce new DAGS that contain 
        partitions 7, 8, 9, 10, 11, 12, 13, 14, 15. When a new DAG is produced containing
        partitions 10 and 11, where 10 is complete and 11 is TBC, and 10 is the first partition
        of the 3rd CC. and new lambda will be started to execte leaf task 10. This lambda X (or
        if X requests a new DAG and doesn't get one then some other lambda Y that is started)
        will execute 10, 11, 12, 13, 14, and 15 and terminate. Note that after the lamda L
        that is executing partition 6 finishes 6 and sees the TBC partition 7 in its (old)
        version 6 of the DAG, it will call withdraw() requesting version 7 of the DAG. Assume
        the last deposited version of the DAG is version 11, which contains all partitions from 
        PR1_1 to PR12_. Since 7 < 11, i.e., requested version 7 is older than the last
        version 11 deposited, we can return version 11 of the DAG to L. However, as lambda
        X/Y was exexcuting the partitions in its CC (10, 11, 12, 13, 14, 15) it was also requesting
        new versions of the DAG. In these new versions of the DAG that were given to X/Y,
        we will have deallocated some of the partition information in these DAGs, which is 
        the "old" partitions that are no longer needed to execute the newer partitions
        in the newer versions DAG. For example, for version 11, we deallocted information about 
        partitions 1 - 9. This means that if L calls withdraw() and requests version 7 and
        we retun version 11 to L, version 11 will not have informarion about partitions 
        1 - 7 but L neds this deallocated information to execute its DAG. Thus, we must 
        restore the missing information in version 11 that L needs before we return it to L.
        In this case, L does NOT need information about 1-5 but it does need information 
        about partition 6 and the partitons after 6. This is because L is requesting version 
        7, which means some partitions at the beginning of version 7 can be deallocated but
        parttions at the end of version 7 and the partitions that were added in versions after 7
        are also needed. Since our version 11 DAG had deallocations for 1 - 9, we
        need to restore the information for 6, 7, 8, and 9, which are partitions that L
        needs that were deallocated from version 11. (Version 11 stil has information
        about partition 10 etc.) So the version of the DAG we return to L is version 
        11 with partitions 1 - 5 being deallocated. Note that when we deallocate infomation
        about partition p we save it so that it is avaialble if we need to restore p.
        Note: When an executor requests version i of the DAG, it is willing to receive
        version i or any version after i. In the above scenario, we may have assumed that
        a request for version i returned version i but it could have insted returned a
        later version, depending on the interleaving of deposits and withdraws that occurs.
        """
        # This is where deallocation should end
        deallocation_end_index = (2+((requested_version_number_DAG_info-2)*DAG_executor_constants.INCREMENTAL_DAG_DEPOSIT_INTERVAL))-2

       # Note: this condition cannot be false. We called restore_DAG_structures_groups
        # with a requested_version_number_DAG_info that was smaller than the version number
        # for the most recent deallocation so we know some values should be restored 
        # given the requestd version number, i.e., the deallocations for the requsted version 
        # number are a prefix of the deallotions made so far so the end index computed
        # for the rquested version number must be >= 0. We use 0 since for a requested version
        # number of 2 the end index is 0, which menas that no deallocations shoud be made
        # for version 2 and if some have been made then we need to rstore them all.
        # Note: version 3 is the first version for which we can do deallocations.
        # Example: For requested version 3 with publishing interval 2, end index is 
        # 2+((3-2)*2)-2=2. start index will have its initil=al value so dealloc 1..2.
        # Version 3 will have artitions 1, 2, 3, 4, 5, 6. We can dealloc 1..2, where 1 and 2
        # were the partitions added to create vrsion 1. So we restore 1 and 2 of version 1 
        # if we do the deallocations for verion 3 and get a request for version 2.
        # The deallocation_end_index_partitions is computed above as 0, which means the end of the 
        # valid deallocations for version 2 is 0, i.e., no dallocations are allowed. We need
        # to restore the deallocations after that, which start in 1 and end with the end index
        # for the most recent deallocation, which was index 2 for version 3. Thus, below we
        # increment 0 to 1 and restore from 1 to 2, which is range(1,3).
        #
        # Note, for restore in the condition below we use >= 0. For deallocation, 
        # in the corresonding condition we use > 0. For deallocation, 
        # we do not want to do a dealloc if end index is 0, which is the case for 
        # requested version 2. For version 2 start will be 1 (the initial value) and if end is 
        # 0 the range is from "1 to 0", which using range is ange (1,1) so no dealloations 
        # will be done, as needed. Version 3 is the first version for which we can do
        # deallocations. For restore, when the version requsted is less than the most 
        # recent version for which deallocations were done, end index,  we need to restore
        # some deallocations. If the most recent is for version 3 and the end index 0 (for 
        # the just requested) is for version 2, then we need to restore the deallocs that were
        # done for 3. These deallocs were states 1 and 2. Thus, the end index for 2 is 
        # 0 and we increment it to 1, where the end index for the most recent version for which 
        # deallocs done, which is version 3, is 2. We increment 2 to 3 for the range,
        # iving a range of (1,3), where 3 is exclusive.
        # so we restore states 1 and 2. States 3, 4, 5, 6 were never deallocated so at
        # this point no deallocations have been done in the current version 3. (Note the 
        # request is for 2 and the version available is 3 so we will return 3 but without 
        # any deallocations, i.e., the deallocs will have been restored. If the next request 
        # is for version 3, we will do the deallocations again before returning version 3,
        # with the deallocations, to the lambda requesting version 3. The idea is that
        # the deallocations can make the DAG info object much smaller and this will make up
        # for the time to do the deallocations. 
        #if deallocation_end_index > 0:
        if deallocation_end_index >= 0:
            deallocation_end_index += 1
        
        # Can self.most_recent_deallocation_end_index > 0 (see the next condition) be false? 
        # No, since we  know we did a deallocation (for most recent version) so the most recent
        # end index for deallocatios, which was saved when the deallocatins
        # were done, is greater than 0.
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
            # a deallocation.
            self.version_number_for_most_recent_deallocation = requested_version_number_DAG_info
            # deallocation_end_index was incremented above to be the position one past that of 
            # the last item deallocated, which is where we would start the
            # next deallocation. So we are using the value used in the range.
            self.deallocation_start_index_partitions = deallocation_end_index
            # But the actual new end of deallocation for this just done deallocation (after
            # restoring items) is deallocation_end_index - 1. We decrement by 1
            # since deallocation_end_index was incremented above to be one
            # past the position of the last decrement so the (exclusive)
            # range value would be correct). Recall that deallocation_end_index is the 
            # index where we started restoring, so deallocation_end_index-1 is the 
            # index of the last deallocation that has been done.
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
        # (assuming the DAG has more than 1 partition). Th driver will start a lambda for 
        # executing partiton 1, which is a leaf node, and partition 2 is tobecontinued and 
        # may also be a leaf node (it is a leaf node if partition 1 is the only node in
        # its conected component. This firt lambda is given version 1 of the DAG. 
        # So the first version a lambda can request is version 2. At that point, the
        # lambda will have executed partition 1, found that partition 2 was to-be-continued, 
        # saw that partition 1 is not to-be-continued but has a collapse
        # to a continued partition 2, and that the DAG was not complete, and so requested version 2.
        # (The lambda that executed partition 1, actually the task corresponding to computing the 
        # pagerank values of partition 1, will send its state and output on the call to withdraw() 
        # and if no new DAG is avaiable withdraw() will save the state and output in a a withdraw
        # tuple. When a new DAG is deposited, a lambda will be started for the withdraw tuple
        # and the lambda will receive the state/output from the withdraw tuple (in its payload)
        # The lambda will start executing the new DAG by processing the collapse task for the 
        # task identife by the state the lambda received. In this case, the state is for partiton 1
        # and the collapse for partition 1 is partition 2 so th lambda begings by
        # executing partition 2 as usual. In general if a lambda requests version n, n>1, then it
        # has already executed partitions 1 through 2+((n-2)*DAG_executor_constants.INCREMENTAL_DAG_DEPOSIT_INTERVAL), 
        # where the last partition 2+((n-2)*DAG_executor_constants.INCREMENTAL_DAG_DEPOSIT_INTERVAL) is continued and 
        # the preceding partition has a collapse to the continued task. So the lambda has already executed
        # partitions 1 and 2 and a certain number of partitions that have been added in each new version
        # of the incremental DAG. Version 1 has partitions 1 and 2 and version 2 is the first version that 
        # can be requested by a lambda. Note that if a lambda requests version 2, it has only excuted
        # partitions 1 and 2 (and really only executed partition 1 as partiton 2 was to-be-continued)
        # so that the formula's use of n-2 in this case is 2-2=0 and the last partition 
        # 2+(0*DAG_executor_constants.INCREMENTAL_DAG_DEPOSIT_INTERVAL) is 2 as expected.(So 2 is the last partition 
        # in version 1, which doesn't mean 2 has been executed since the last partition in an incomplete graph 
        # is to-be-continued as its fanins/fanouts are not known.
        #
        # Example: DAG_executor_constants.INCREMENTAL_DAG_DEPOSIT_INTERVAL is 4 and a lambda requests Version 3. 
        # Then the partitions that have been executed are 1, 2, 3, 4, 5, and 6, where version 2 has partitons 1, 
        # 2, 3, 4, 5, and 6, where 6 is to-be-continued, and version 1 has partitions 1 and 2. A lambda is 
        # requesting Version 3, which will add the 4 partitions 7, 8, 9, and 10 as 
        # DAG_executor_constants.INCREMENTAL_DAG_DEPOSIT_INTERVAL is 4. In the new version 3 of the DAG, 
        # partition 6 is not-tobecontinued and partition 5  has no collapse to a continued state. 
        # (Note that when we add partition 7 to the DAG, we change partition 6 to be not-to-be-continued
        # and we change partition 5 so that it has no collapse to a to-be-continued partition. The new DAG with
        # partition 7, which is a to-be-continued partition (due to DEPOSIT INTERVAL = 4), is not published, and neither 
        # are the DAGS created by adding partitions 8, and 9, respectively. The DAG created by adding 10 is published.)
        # (We have added 8 partitions since we generated the base DAG (with partitions 1 and 2) and
        # 8 mod 4 (DAG INTERVAL) is 0). Note that we added 7, 8, 9, and 10 but we also changed 5
        # and 6, so we need 5 and 6 to be in version 3 of the DAG, i.e., we do not deallocate 5 and 6 so they are in
        # version 3. So we can deallocate the info in the DAG strctures about DAG states 1, 2, 3, 4, which 
        # is 1 through (2+((n-2)*pub_interval))-2, where n=3, which is 1 through 2+(1*4)-2 = 4. Note that when lambdas request 
        # version 3 we can deallocate DAG structure information about partitions 1, 2, 3, 4, but 
        # not partitions 5 and 6. This is because the status of 5 and 6 in version 3 is changed from that of version 2.
        # Partition 5 has a collapse to continued task 6 in version 2 but in version 3 task 6 is not
        # to-be-continued and thus 5 no longer has a collapse to a continued task. So when lambdas
        # get version 3 of the DAG they restart execution by getting the state and output for already
        # executed task 5 from the continue queue, retrieving the collapse task 6 of state 5, and 
        # executing task 6. Thus we do not dealloate the DAG states for 5 and 6, even though they
        # were in version 2 and we are getting a new version 3.
        # Note that we don't always start deallocation at 1, we start where the last deallocation ended.
        # So in this example, after deallocating 1, 2, 3, and 4, start_deallocation becomes 5. Note that if 
        # a lambda requests version 2, then 2+((n-2)*pub_interval))-2 is 2+(0*4)-2 = 0 so we deallocate from 1 to 0 
        # so no deallocations will be done. As mentioned above, the states for partitions 1 and 2 in 
        # version 1 will change in version 2, so we do not deallocate 1 and 2 when the workers
        # request version 2. Partitions 1 and 2 will be among the partitions deallocated when workers
        # request version 3. (From above, for pubishing interval 4, we will deallocate 1, 2, 3, 4.)
        #
        # Note that the initial value of the version number in the DAG_infoBufferMonitor that 
        # maintains the last version number requested by a lambda is 1. Lambdas don't actually 
        # request version 1, as version 1, which contains partitions 1 and 2, is given to
        # the DAG_executor_driver, which starts a lambda with version 1 of the incremental DAG. 
        # (Partition 1 is a leaf task. Partition 2 either depends on partition 1 or is another leaf task.)
        # More leaf tasks can be discovered during incremental DAG generation. This is unlike non-incremental 
        # DAG generation, which discovers all of the leaf tasks during DAG gneration and gives all these leaf tasks
        # to the DAD_executor_driver (as part of the DAG INFO structure). The DAG_executor_driver will 
        # start a lambda for each leaf task, or if workers are being used it will enqueue the leaf tasks
        # in the work queue.)
        # So the first version requested by a lambda is 2, and the first version deposited
        # in the DAG_infoBufferMonitor is 2. The first leaf node lambda can excute DAG version 1 and request
        # DAG version 2 before bfs() has deposited version 2 into the DAG_infoBufferMonitor.
        # (Deposit makes the new version available to lambdas, who call withdraw() to get a new
        # version.) Before bfs() deposits new version 2, it will ask DAG_infoBufferMonitor for 
        # the most recent version requested by the lambdas. This will be the initial value 1
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
        # DAG we publish is the base DAG (version 1) with partitions 1 and 2. If the interval is 4,
        # the next DAG we generate has partitions 1, 2, and 3 but it is not published since
        # we publish every 4 partitions (after the base DAG) So the next verson is version 2,
        # which will have partitions 1, 2, 3, 4, 5, 6 for a total of 2+4 partitions.

        deallocation_end_index = (2+((requested_version_number_DAG_info-2)*DAG_executor_constants.INCREMENTAL_DAG_DEPOSIT_INTERVAL))-2

        # we will use deallocation_end_index in a range so it needs to be one past the last partition
        # to be dealloctated. Example, to deallocate "1 to 1" use range(1,2), where 1 is inclusive 
        # but 2 is exclusive; to deallocate "2 , 3, and 4" use range(2,5).
        # 
        # However, don't increment deallocation_end_index unless we will 
        # actually do a deallocation. We can do a deallocation unless start index is 1 and end index is less
        # than 1. If end index > 0 we can deallocate as the value of the above formula for end index
        # just keeps growing as the version number increases.
        # which is implemented as range(1,2), 
        if deallocation_end_index > 0:
            deallocation_end_index += 1

        # self.deallocation_start_index_partitions initialized to 1, which is the first state 
        # for which we can deallocate
        # Q: Use state instead of index since we are using keys in maps? not positions in arrays?
        logger.info("deallocate_DAG_structures_partitions: self.deallocation_start_index_partitions: " + str(self.deallocation_start_index_partitions)
            + " deallocation_end_index: " + str(deallocation_end_index) + " (exclusive)")
        for i in range(self.deallocation_start_index_partitions, deallocation_end_index):
            logger.info("deallocate_DAG_structures_partitions: generate_DAG_info_incremental_partitions: deallocate " + str(i))
            self.deallocate_DAG_structures_lambda(i)
        
        # Set start to end if we did a deallocation, i.e., if start < end in range. 
        # Note that if start equals end, then we did not do a deallocation since 
        # end is exclusive. (And we may have just incremented end, so dealllocating 
        # "1 to 1" not as a range, with start = 1 and end = 1, was implemented with a 
        # range as incrementing end to 2 and using range(1,2) so start < end for the 
        # deallocation "1 to 1" not as a range.
        # Note that end was exclusive so we can set start to end instead of end+1.
        # Set most recent dealloc to deallocation_end_index - 1; we use - 1 since most
        # recent should be the last position actually deallocated, and since 
        # end index is exclusive (i.e., is 1 past the actual end) we use end - 1.
        if self.deallocation_start_index_partitions < deallocation_end_index:
            # Remember that this is the most recent version number for which we did
            # a deallocation. 
            self.version_number_for_most_recent_deallocation = requested_version_number_DAG_info
            # The index of the next element to be deallocated is one past the last element
            # to be deallocated. Since deallocation_end_index was exclusive in the range
            # (start,end) its value is one past the last element to be deallocated
            # (so there is no need to subtract 1)
            self.deallocation_start_index_partitions = deallocation_end_index
            # The most_recent_deallocation_end_index is the position before 
            # the deallocation_start_index_partitions, i.e., we will start the next
            # deallocation right after the end of the last deallocations we have performed,
            # where we just set deallocation_start_index_partitions = deallocation_end_index
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

    def restore_DAG_structures_groups(self,requested_version_number_DAG_info):
        """
        This if-condition is true when we call restore_DAG_structures_partitions:
            requested_current_version_number < self.version_number_for_most_recent_deallocation:
        so when we did deallocations for version self.version_number_for_most_recent_deallocation
        we did a certain number of deallocations based on that version number (we use the version 
        number to compute end in the range (start,end+1) of the deallocations). But the 
        requested_current_version_number is less than the version number of the last deallocation
        so we have deallocated too many items given this requested_current_version_number.
        That is the DAG_info has items that were deallocated in the DAG_info that was given to the
        lambda that made the request for version_number_for_most_recent_deallocation but these items
        are needed in the DAG_info that we will give to the lambda that made the request for 
        requested_current_version_number that we are now processing.
        This means we need to restore some of these items in DAG_info. We have saved every item that
        we have deallocaed so we can restore items.
        A deallocation is from the current start (one past the end of the last dealloc) to the computed end.
        For partitions, the computed value of end is:
            deallocation_end_index = (2+((requested_version_number_DAG_info-2)*DAG_executor_constants.INCREMENTAL_DAG_DEPOSIT_INTERVAL))-2
        which means we deallocate the groups that are in partitions from the current start index to the
        deallocation_end_index. The first deallocation has a start index of 1. After we have done a sequence 
        of deallocations, we have deallocated and saved information about the groups from the first 
        partition to the last partition deallocated, which is the deallocation_end_index computed for the 
        last deallocation. When we deallocate groups, we iterate through the partitions from start to end 
        and for each partition we deallocate its n groups. Groups have their own indices since each
        partition can have multiple groups e.g., partition 2 may have three groups each with their 
        own index. So we deallocate group 
        information from start index to end index which are computed for each artition as we go. (If the 
        first group of a partition has inex i and the partition has 3 groups, then start is i and end 
        is i+(3-1) = i+2 but end is exclusive in the range so we use a range vale of i+3 for end.
        
        Fix: compute start using end coputation and end is saved value for last dealloc

        The restoration is always a suffix of the deallocated items. That is,
        for the new requested_current_version_number which we know is less than 
        the version_number_for_most_recent_deallocation, we still want to 
        deallocate all the items starting at 1, but we need to stop deallocating
        items that are past the end index that is computed by requested_current_version_number. 
        Note that for partitions, we can get this end index from the usual "deallocation_end_index =" formula. 
        For groups, this group end index also depends on the number of groups in each of the partitions whose
        groups are to be deallocated. We can compute this group end index on the number of groups in each 
        partition. Assume the start index is currently 1. Using the deallocation_end_index =" formula to 
        compute the end index for the partitions whose groups are to be deallocate we might get the value 3, 
        which means we want to deallocate the DAG info for the groups in partitions start to end, which is 1 to 3.
        The first partition always has one group. Assume partitions 2 and 3 each have 2 groups.
        Then we want to deallocate info for 1 + 2 + 2 = 5 groups. The start index for groups
        is also 1. Thus we deallocate group info from 1 to 5. At the end of this deallocation,
        the sart index for partitions is set to 4, which is one past the end 3 and is the index
        of the next partition. Likewise, te start index for groups is set to 6. Note that 
        the start inddex for partitions and the start index for groups may have different 
        values. Note also that when we are deallocating group info, we still compute the end 
        index for partitions in the usual manner and then determine how many groups are
        in these partitions to get the number of groups whose info we are deallocating
        (by summing the sizes of the partitions the to be deallocated groups are in).
        We emphasize that DAG generattion is primarily partition-based, that is, even when 
        we generate groups, we do this by generating partitions and  dividing the paritions
        into groups. So deallocation is also partition-based - we get the groups to deallocate
        by first computing the partitions and then we compute the (number of consecutive)
        groups in these partitions.

        Example: The input graph graph_24N_3CC_fanin_restoredealloc.gr has groups:
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
        with Connected Components (CC) the groups in PR1_1 - PR3_1, the groups in PR4_1 - PR9_1, and the 
        groups in PR10_1 - PR15_1. 
        The version 1 (base) DAG is PR1_1 - (PR2_1, PR2_1L, PR2_3), with PR1_1 complete and the groups
        in PR2_1 tobecontinued (TBC). 
        With pubication interval 1, the next DAG (version 2) has the groups in PR1_1 - PR3_1, with the 
        groups in partition 2 now complete and the groups in partition 3 TBC. 
        The lambda executors will execute the groups in partition 2 and see that the groups in partiton 3
        are TBC and request a new DAG. Note that partition/group PR1_1 has fanouts to groups PR2_1,
        PR2_2L, and PR2_3 so there are three executors finvolved in excuting PR2_1, PR2_2L, and PR2_3.
        We will not be concerned here with th details about fanouts/fanins and which executor is 
        executing which task/group.
        Assume the new DAG has the groups in partition 3, which is the last DAG in the first CC 
        and it is complete, while the partition 4 is the first partition in the new DAG and its groups are TBC.
        The groups in Partition 3 are executed (as fanouts/fanins, etc of groups in partition 2) and since 
        partiton 3 has no fanins/fanouts, their executors will terminate. 
        The DAG generator will deposit() a new DAG that has the groups of partition 4, which are complete,
        where 4 is the first partition of the next CC and TBC groups of partition 5.
        A new lambda is started to execute the single group of partition 4, which is a leaf task. (A leaf task 
        is the first task of a CC, for which a new lambda will be started to excute the leaf) This new lambda 
        will execute the single (leaf) group) of 4, see that the single group of 5 is TBC and request a new 
        DAG. In the new DAG, assume the group of 5 is complete and the grooup of 6 is TBC. The group of 5 is 
        executed and since the group of 6 is incomplete, a new DAG is requsted. In the new DAG, assume the group 
        of 6 is complete and the group of 7 is TBC. Suppose the group of 6 is executed and it takes a long time 
        to execute this group. In the mean time, the DAG generator, will continue to produce new DAGS that contain 
        the groupss of partitions 7, 8, 9, 10, 11, 12, 13, 14, 15. When a new DAG is produced containing
        partitions 10 and 11, where the group in 10 is complete and the group in 11 is TBC, and 10 is the 
        first partition of the 3rd CC a new lambda will be started to execute leaf task 10. This lambda X (or
        if X requests a new DAG and doesn't get one then some other lambda Y that is started to continue where
        X left off) will execute the groups in 10, 11, 12, 13, 14, and 15 (all in its version of the DAG) 
        and terminate. Note that after the lamda L that is executing the group in partition 6 finishes this 
        group and sees the TBC group in partition 7 in its (old) version 6 of the DAG, it will call withdraw() 
        requesting version 7 of the DAG. Assume the last deposited version of the DAG is version 11, which 
        which contains all partitions from PR1_1 to PR12_. Since 7 < 11, i.e., requested version 7 is older 
        than the last version 11 deposited, we can return version 11 of the DAG to L. However, as lambda
        X/Y was exexcuting the partitions in its CC (10, 11, 12, 13, 14, 15) it was also requesting
        new versions of the DAG. In these new versions of the DAG that were given to X/Y,
        we will have deallocated some of the group information in these DAGs, which is 
        the "old" groups that are no longer needed to execute the newer groups
        in the newer versions DAG. For example, for version 11, we deallocted information about 
        the groups in partitions 1 - 9. This means that if L calls withdraw() and requests version 7 and
        we return version 11 to L, version 11 will not have the deallocated information about partitions 
        1 - 7 but L needs this deallocated information to execute its DAG. Thus, we must 
        restore the missing information in version 11 that L needs before we return it to L.
        In this case, L does NOT need information about 1-5 but it does need information 
        about the group in partition 6 and the groups in partitions after 6. This is because L is
        requesting version 7, which means some groups (in partitions) at the beginning of version 7 
        can be deallocated but groups in partitions at the end of version 7 and the partitions that 
        were added in versions after 7 are also needed. Since our version 11 DAG had deallocations for 
        groups in partitions 1 - 9, we need to restore the information for the groups in partitions 
        6, 7, 8, and 9, which are groups in partitions that L needs that were deallocated from version 11. 
        (Version 11 stil has information about the groups in partition 10 etc.) So the version of the 
        DAG we return to L is version 11 with partitions 1 - 5 being deallocated. Note that when we 
        deallocate infomation about the groups in partition p we save it so that it is avaialble if we 
        need to restore the groups in p.
        Note: When an executor requests version i of the DAG, it is willing to receive
        version i or any version after i. In the above scenario, we may have assumed that
        a request for version i returned version i but it could have instead returned a
        later version, depending on the interleaving of deposits of new DAGs and DAG withdraws of
        that occurs.
        """

        # This is what the end index for the deallocations should be, but we may have 
        # deallocated past that end index when we processed earlier requests for versions
        # of the incremental DAG that were greater than this one. The index of the last
        # dealloction that we have done is self.most_recent_deallocation_end_index. If
        # deallocation_end_index_partitions is less than self.most_recent_deallocation_end_index 
        # then we need to restore the deallocated values in positions after deallocation_end_index_partitions.
        # The first such restored value would be in position deallocation_end_index_partitions+1
        # and the last would be in position self.most_recent_deallocation_end_index.
        deallocation_end_index_partitions = (2+((requested_version_number_DAG_info-2)*DAG_executor_constants.INCREMENTAL_DAG_DEPOSIT_INTERVAL))-2

        # Note: this condition cannot be false. We called restore_DAG_structures_groups
        # with a requested_version_number_DAG_info that was smaller than the version number
        # for the most recent deallocation so we know some values should be restored 
        # given the requestd version number, i.e., the deallocations for the requsted version 
        # number are a prefix of the deallotions made so far so the end index computed
        # for the rquested version number must be >= 0. We use 0 since for a requested version
        # number of 2 the end index is 0, which menas that no deallocations shoud be made
        # for version 2 and if some have been made then we need to rstore them all.
        # Note: version 3 is the first version for which we can do deallocations.
        # Example: For requested version 3 with publishing interval 2, end index is 
        # 2+((3-2)*2)-2=2. start index will have its initil=al value so dealloc 1..2.
        # Version 3 will have artitions 1, 2, 3, 4, 5, 6. We can dealloc 1..2, where 1 and 2
        # were the partitions added to create vrsion 1. So we restore 1 and 2 of version 1 
        # if we do the deallocations for verion 3 and get a request for version 2.
        # The deallocation_end_index_partitions is computed above as 0, which means the end of the 
        # valid deallocations for version 2 is 0, i.e., no dallocations are allowed. We need
        # to restore the deallocations after that, which start in 1 and end with the end index
        # for the most recent deallocation, which was index 2 for version 3. Thus, below we
        # increment 0 to 1 and restore from 1 to 2, which is range(1,3).
        #
        # Note, for restore in the condition below we use >= 0. For deallocation, 
        # in the corresonding condition we use > 0. For deallocation, 
        # we do not want to do a dealloc if end index is 0, which is the case for 
        # requested version 2. For version 2 start will be 1 (the initial value) and if end is 
        # 0 the range is from "1 to 0", which using range is ange (1,1) so no dealloations 
        # will be done, as needed. Version 3 is the first version for which we can do
        # deallocations. For restore, when the version requsted is less than the most 
        # recent version for which deallocations were done, end index,  we need to restore
        # some deallocations. If the most recent is for version 3 and the end index 0 (for 
        # the just requested) is for version 2, then we need to restore the deallocs that were
        # done for 3. These deallocs were states 1 and 2. Thus, the end index for 2 is 
        # 0 and we increment it to 1, where the end index for the most recent version for which 
        # deallocs done, which is version 3, is 2. We increment 2 to 3 for the range,
        # iving a range of (1,3), where 3 is exclusive.
        # so we restore states 1 and 2. States 3, 4, 5, 6 were never deallocated so at
        # this point no deallocations have been done in the current version 3. (Note the 
        # request is for 2 and the version available is 3 so we will return 3 but without 
        # any deallocations, i.e., the deallocs will have been restored. If the next request 
        # is for version 3, we will do the deallocations again before returning version 3,
        # with the deallocations, to the lambda requesting version 3. The idea is that
        # the deallocations can make the DAG info object much smaller and this will make up
        # for the time to do the deallocations. 

        if deallocation_end_index_partitions >= 0:
            deallocation_end_index_partitions += 1

        # Can self.most_recent_deallocation_end_index > 0 (see the next condition) be false? 
        # No, since we  know we did a deallocation (for most recent version) so the most recent
        # end index for deallocatios, which was saved when the deallocatins
        # were done, is greater than 0.
        try:
            msg = "[ERROR]:DAG_infoBuffer_Monitor_for_Lambdas:" \
                + " self.most_recent_deallocation_end_index is not greater than 0."
            assert self.most_recent_deallocation_end_index > 0 , msg
        except AssertionError:
            logger.exception("[Error]: assertion failed")
            if DAG_executor_constants.exit_program_on_exception:
                logging.shutdown()
                os._exit(0)

        # Note: we used to compute self.most_recent_deallocation_end_index using self.version_number_for_most_recent_deallocation
        # but now when we do a deallocation we set the value of self.most_recent_deallocation_end_index so
        # we do not need to compute it.
        # It is probably okay to just compute it here when we need it since we only need it here in restore.
        #self.most_recent_deallocation_end_index = (2+((self.version_number_for_most_recent_deallocation-2)*DAG_executor_constants.INCREMENTAL_DAG_DEPOSIT_INTERVAL))-2
       
        # self.most_recent_deallocation_end_index is used in a range() below and it will be exclusive so 
        # increment by 1.
        # We will set this to a new value at the end of restore to reflect that the 
        # last dealloated value has changed (since we restored some of the values.)
        # Note that the restored values are a suffix of the deallocated values 
        # i.e., are a sequence of values at the end of the sequence of deallocted values.
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

        # This is used in a range() below and it will be exclusive so increment by 1.
        # We will set this to a new value at the end of restore to reflect that the 
        # last deallocated index has changed (since we restored some of the values.)
        # Note that the restored values are a suffix of the deallocated values 
        # i.e., are a sequence of values at the end of the sequence of values that 
        # have been deallocated so far. Likewise, the deallocations that remain for the 
        # current requsted version are a prefix of the deallocations that had been done
        # previously.
        if self.most_recent_deallocation_end_index > 0:
            self.most_recent_deallocation_end_index += 1

        logger.info("restore_DAG_structures_partitions: deallocation_end_index_partitions: " + str(deallocation_end_index_partitions)
            + " self.most_recent_deallocation_end_index: " + str(self.most_recent_deallocation_end_index))

        for i in range(self.deallocation_end_index_partitions, self.most_recent_deallocation_end_index):
            logger.info("deallocate_DAG_structures_groups: deallocate " + str(i))
            groups_of_partition_i = self.groups_of_partitions[i-1]
            # number of groups must be >= 1
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
                # Note: This deallocation uses group_names[j-1]
                # so restore_item restores the group in position j-1.
                self.restore_item(j)
 
            # set start to end if we did a deallocation, i.e., if start < end in range(). 
            # Note that if start equals end, then we did not do a deallocation since 
            # end is exclusive. (And we may have just incremented end, so deallocating 
            # "1 to 1", with start = 1 and end = 1, was implemented as as a range by incrementing 
            # end to 2 and using range(1,2), so start < end for the deallocation "1 to 1"
            if self.deallocation_start_index_groups < deallocation_end_index_groups:
                # remember that this is the most recent version number for which we did
                # a deallocation
                self.version_number_for_most_recent_deallocation = requested_version_number_DAG_info
                # Note that end was exclusive so we can set start to end instead of end+1.
                # Start will be one past the index of the last deallocation; this deallocation  
                # was at index deallocation_end_index_groups-1.
                self.deallocation_start_index_groups = deallocation_end_index_groups

        # Possibly reset deallocation_start_index_partitions. We don't know 
        # whether we actually tried to deallocate any partitions/groups, i.e., whether
        # self.deallocation_end_index_partitions < self.most_recent_deallocation_end_index
        # was true for the outer "for i in range()" loop above. If not, then we did not 
        # deallocate any groups in any partitions.
        #
        # Set start to end if we did a deallocation, i.e., if
        # self.deallocation_end_index_partitions < self.most_recent_deallocation_end_index.
        # Note that if the values are equal, then we did not do a deallocation since 
        # end is exclusive. (And we may have just incremented end, so deallocating 
        # "1 to 1", with start = 1 and end = 1, was implemented as a range by incrementing 
        # end to 2 and using range(1,2) so 
        # self.deallocation_end_index_partitions < self.most_recent_deallocation_end_index 
        # for the deallocation "1 to 1".
        if self.deallocation_end_index_partitions < self.most_recent_deallocation_end_index:
            # remember that this is the most recent version number for which we did
            # a deallocation
            self.version_number_for_most_recent_deallocation = requested_version_number_DAG_info
            # deallocation_end_index_partitions was incremented above to be the position one past that of 
            # the last item that should be deallocated, which is where we would start the
            # next deallocation. So we are using the value used in the range.
            self.deallocation_start_index_partitions = deallocation_end_index_partitions
            # But the actual new end of deallocation for this just done deallocation (i.e., after
            # restoring items) is deallocation_end_index_partitions - 1. We decrement by 1
            # since deallocation_end_index_partitions was incremented above to be one
            # past the position of the last decrement so the (exclusive)
            # range value would be correct). Recall that deallocation_end_index_partitions is the 
            # index where we started restoring, so deallocation_end_index_partitions-1 is the 
            # index of the last deallocation that has been done (and not restored).
            self.most_recent_deallocation_end_index = deallocation_end_index_partitions - 1
            logger.info("deallocate_DAG_structures_partitions: new index values after restores: "
                + "self.version_number_for_most_recent_deallocation: " 
                + str(self.version_number_for_most_recent_deallocation)
                + " self.deallocation_start_index_partitions: "
                + str(self.deallocation_start_index_partitions)
                + " self.most_recent_deallocation_end_index: "
                + str(self.most_recent_deallocation_end_index))
        else:
            logger.info("deallocate_DAG_structures_partitions: " + " no restorations.")

#brc: Todo: Note: we do not dealloc groups_of_partitions incrementally? When could we
# do it since we need it here?

    def deallocate_DAG_structures_groups(self,requested_version_number_DAG_info):
        # For the whiteboard example
        # Version 1 of the incremental DAG is given to the DAG_executor_driver for execution
        # (assuming the DAG has more than 1 partition). The driver will start a lambda for 
        # executing group/partiton 1, which is a leaf node. The groups in partition 2 are 
        # tobecontinued and 2 may be a leaf node (it is a leaf node if partition 1 is the 
        # only node in its connected component (CC) in which case partition 2 is a single group).
        # (The first partition of a new CC has a single group (since none of the nodes in 
        # this group have parent nodes (since previous partiton was the last in its CC)))) 
        # This firt lambda is given version 1 of the DAG. So the first version a lambda can 
        # request is version 2. At that point, the lambda will have seen that partition/group 1 '
        # is not to-be-continued executed partition/group 1, seen that partition/group 1
        # has fanouts/fanins to tobecontinued groups in partition 2, and that the DAG was not 
        # complete (there are more partitions to be generated in the DAG), and so requested version 2.
        # (The lambda that executed partition/group 1, actually the task corresponding to computing the 
        # pagerank values of partition/group 1, will send its state (1) and output on the call to withdraw() 
        # and if no new DAG is avaiable withdraw() will save the state and output in a withdraw
        # tuple. When a new DAG is deposited, a lambda will be started for the withdraw tuple
        # and the lambda will receive the saved state/output from the withdraw tuple (in its payload).
        # The lambda will start executing the new DAG by processing the fanins/fanouts for the 
        # task identified by the state (1) the lambda received. In this case, the state is for partiton/group 1
        # and the fanouts are to group PR2_1 and PR2_3 and the fanin is to PR2_2L.
        # In general if a lambda requests version n of the DAG, n>1, then it
        # has already processed partitions 1 through 2+((n-2)*DAG_executor_constants.INCREMENTAL_DAG_DEPOSIT_INTERVAL), 
        # where the last partition 2+((n-2)*DAG_executor_constants.INCREMENTAL_DAG_DEPOSIT_INTERVAL) is continued and 
        # the preceding partition was excuuted and has fanins/fanouts to the continued tasks/groups in the last partition.
        # So the lambda has already executed partitions 1 and 2 (of version 1) and a certain number of partitions
        # that have been added in each new generated version of the incremental DAG. Version 1 has partitions 1 
        # and 2 and version 2 is the first version that can be requested by a lambda. Note that if a lambda 
        # requests version 2, it has processed partitions 1 and 2 (i.e., executed partition 1 as partiton 2 was 
        # to-be-continued) so that the formula's use of n-2 in this case is 2-2=0 and the last partition 
        # 2+(0*DAG_executor_constants.INCREMENTAL_DAG_DEPOSIT_INTERVAL) is 2 as expected. (So 2 is the last partition 
        # in version 1, which doesn't mean 2 has been executed since the last partition in an incomplete graph 
        # is to-be-continued as the fanins/fanouts from groups in this partition to the groups in the next
        # partition are unknown. They will become known in the next DAG that is generated and this DAG or 
        # a succeeding DAG that is the next da/g to be published is published. this publshed DAG will be 
        # given to the lambdas who request a new DAG.
        #
        # Example: DAG_executor_constants.INCREMENTAL_DAG_DEPOSIT_INTERVAL is 4 and a lambda requests Version 3. 
        # Then the partitions an the groups therein that have been executed are 1, 2, 3, 4, 5, and 6, where 
        # version 2 has partitions 1, 2, 3, 4, 5, and 6, where 6 is to-be-continued, and version 1 has partitions 
        # 1 and 2. A lambda is  requesting Version 3, which will add the groups in the 4 partitions 7, 8, 9, and 10 as 
        # DAG_executor_constants.INCREMENTAL_DAG_DEPOSIT_INTERVAL is 4. In the new version 3 of the DAG, the grops n
        # partition 6 are not-tobecontinued and the groups in partition 5 now have no fanins/fanouts to a continued
        # group,(Note that when we add the groups in partition 7 to the DAG, we change he groups in partition 6 
        # to be not-to-be-continued and we change the groups in partition 5 so that they have has no fanins/fanouts 
        # to a to-be-continued group. The new DAG with the groups in partition 7, which are to-be-continued groups
        # is not published, due to DEPOSIT INTERVAL = 4)and neither are the DAGS created by adding the groups 
        # in partitions 8, and 9, respectively. The DAG created by adding th groups in partition 10 is published.)
        # (We have added the groups of 8 partitions since we generated the base DAG (with partitions 1 and 2) and
        # 8 mod 4 (DAG INTERVAL) is 0). Note that we added 7, 8, 9, and 10 but we also changed the groups in 5
        # and 6, so we need the groups in 5 and 6 to be in version 3 of the DAG, i.e., we do not deallocate 
        # DAG info for the groups in 5 and 6 so this info will be in version 3. So we can deallocate the info 
        # in the DAG structures for the DAG groups in partitions 1, 2, 3, 4, which 
        # is 1 through partition (2+((n-2)*pub_interval))-2, where n=3, which is 1 through 2+(1*4)-2 = 4. 
        # Note that when lambdas requests version 3 we can deallocate DAG structure information about 
        # the groups in partitions 1, 2, 3, 4, but not the groups in partitions 5 and 6. This is because 
        # the status of the groups in 5 and 6 in version 3 is changed from that of version 2.
        # Partition 5 has fanins/fanouts to continued groups in partition 6 in version 2 but in version 3 
        # the groups in partition 6 are not to-be-continued and thus the groups in partiton 5 no longer habe
        # a fanon/fanout to a continued group. So when lambdas get version 3 of the DAG they restart execution
        # by getting the state and output for an already executed group of partition 5 (this state/output are 
        # given to the lambda in its payload), retrieving the fanin/fanouts of the group given by the state, and 
        # executing the fanins/fanouts of this group. Thus we do not dealloate the DAG states for groups in 
        # partitions 5 and 6, even though they were in version 2 and we are getting a new version 3.
        # Note that we don't always start deallocation at 1, we start where the last deallocation ended.
        # So in this example, after deallocating the groups in partitions 1, 2, 3, and 4, start_deallocation 
        # for the partitions becomes 5. Note that if a lambda requests version 2, then 
        # 2+((n-2)*pub_interval))-2 is 2+(0*4)-2 = 0 so we deallocate from 1 to 0 which means that 
        # no deallocations will be done. As mentioned above, the states for partitions 1 and 2 in 
        # version 1 will change in version 2, so we do not deallocate the groups in partitions 1 and 2 when 
        # the lambda that excutes partition/group 1 request version 2. The groups in partitions 1 and 2 will 
        # be among the groups deallocated when lambdas request version 3. (From above, for pubishing interval
        # 4, we will deallocate groups in partitions 1, 2, 3, 4 when lambdaas request version 3; the requested
        # version number is a measure of the progres the lamba has mad in exeuting itd part of the ADG, i.e.,
        # some partitions/groups are old partitions/groups that have been processed and are no loner needed.
        #
        # Note that the initial value of the version number in the DAG_infoBufferMonitor that 
        # maintains the last version number requested by a lambda is 1. Lambdas don't actually 
        # request version 1, as version 1, which contains partitions 1 and 2, is given to
        # the DAG_executor_driver, which starts a lambda with version 1 of the incremental DAG. 
        # (Partition 1 is a leaf task. The groups in partition 2 either depend on partition 1 or 
        # partition 2 is a single grooup that is another leaf task (so partition/group 1 is the first 
        # and last of CC 1 and partition/group 2 is the first of its CC)
        # More leaf tasks can be discovered during incremental DAG generation. This is unlike non-incremental 
        # DAG generation, which discovers all of the leaf tasks during DAG gneration and gives all these leaf tasks
        # to the DAG_executor_driver (as part of the DAG_INFO structure). The DAG_executor_driver will 
        # start a lambda for each leaf task, or if workers are being used it will enqueue the leaf tasks
        # in the work queue and tart the workers.
        # So the first version requested by a lambda is 2, and the first version deposited
        # in the DAG_infoBufferMonitor is 2. The first leaf node lambda will execute DAG version 1 and then may
        # request# DAG version 2 before bfs() has deposited version 2 into the DAG_infoBufferMonitor.
        # (Deposit makes the new version available to lambdas, who call withdraw() to get a new
        # version.) Before bfs() deposits new version 2, it will ask DAG_infoBufferMonitor for 
        # the most recent version requested by the lambdas. This will be the initial value 1
        # since bfs() has not called deposit() for the first time to deposit version 2 (which 
        # bfs will do next). bfs will pass 1 to deallocate_DAG_structures as the value
        # of requested_version_number_DAG_info so the value of 
        # 2+((requested_version_number_DAG_info-2)*DAG_executor_constants.INCREMENTAL_DAG_DEPOSIT_INTERVAL))-2
        # will be 2+((1-2) * 4)-2 = -4 so we will deallocate from 1 to -4 so no deallocations are done
        # as expected. (We cannot deallocate 1 or 2 yet as we are not done with them - in version
        # 2 the groups in partition 2 becomes not to-be-continued and partition/group 1 now has no fanons/fanouts to a 
        # continued group (in partition 2), so we restart executing the DAG by accessing partition/group 1's 
        # state information to get 1's fanins/fanouts, which are processed as usual. We already excuted 1 and then
        # stopped execution when we saw 1's fanins/fanouts were to to-be-continued groups in partition 2)
        # Note: Given the formula 2+((requested_version_number_DAG_info-2)*DAG_executor_constants.INCREMENTAL_DAG_DEPOSIT_INTERVAL))-2,
        # the value given by this formula cannot become greater than 0 until workers request version 3. For
        # version 2, requested_version_number_DAG_info-2 is 2-2 = 0, so we get 2+0-2=0.
        # Note: if the DAG interval is 2, then for requesting version 3, we get 2+((3-2)*2)-2 = 2. Version 2 
        # has partitions 1 2 3 4, which means we can deallocate partitions 1 up to the end partition which is 2.
        # We keep 3 and 4 from version 2 and add 5 and 6 to get a version 3 of 3 4 5 6 where 6 is to-be-continued
        # and 4 is no longer to-be-continued and thus can be executed. (Partition 3 was executed previously then
        # execution stopped at to-be-continued taak/partition 4.)

        # Note: the version number gets incremented only when we publish a new DAG. The first 
        # DAG we publish is the base DAG (version 1) with the groups in partitions 1 and 2. If the publishing 
        # interval is 4, the next DAG we generate has partitions 1, 2, and 3 but it is not published since
        # we publish every 4 partitions (after the base DAG version 1) So the next verson to be published is
        # version 5 which will have partitions 1, 2, 3, 4, 5, 6 for a total of 2+4=6 partitions.

        deallocation_end_index_partitions = (2+((requested_version_number_DAG_info-2)*DAG_executor_constants.INCREMENTAL_DAG_DEPOSIT_INTERVAL))-2

        # we will use deallocation_end_index in a range so it needs to be one past the last partition
        # whose groups are to be dealloctated. Example, to deallocate "1 to 1" use range(1,2), where 1 is inclusive 
        # but 2 is exclusive; to deallocate "2 , 3, and 4" uses range(2,5).
        # 
        # However, don't increment deallocation_end_index unless we can 
        # actually do a deallocation. We can do a deallocation unless start index is 1 and end index is less
        # than 1. If end index > 0 we can deallocate as the value of the above formula for end index
        # just keeps growing as the version number increases.
        # Note that deallocate "1 to 1, i.e., deallocte 1, is implemented with a range as range(1,2), 
        if deallocation_end_index_partitions > 0:
            deallocation_end_index_partitions += 1

        logger.info("deallocate_DAG_structures_groups: self.deallocation_start_index: " + str(self.deallocation_start_index_partitions)
            + " deallocation_end_index_partitions: " + str(deallocation_end_index_partitions))
        for i in range(self.deallocation_start_index_partitions, deallocation_end_index_partitions):
            logger.info("deallocate_DAG_structures_groups: deallocate " + str(i))
            groups_of_partition_i = self.groups_of_partitions[i-1]
            # number of groups in a partition is >= 1
            number_of_groups_of_partition_i = len(groups_of_partition_i)
            # end index is now 1 past the last group to be deallocated, e.g., 
            # if start is 1 and number of groups is 3 we want to deallocate 
            # 1, 2, and 3. The computed value 4 is fine for end since we will use a 
            # range(1,4) and 1 is inclusive but 4 is exclusive.
            # After adding number_of_groups_of_partition_i to deallocation_end_index_groups, 
            # deallocation_start_index_groups < deallocation_end_index_groups.

            # As the preceding comment just explained, this computation sets deallocation_end_index_groups
            # to the correct exclusive value for the range, so there is no need to increment it like
            # we do for the other end index values.
            deallocation_end_index_groups = self.deallocation_start_index_groups+number_of_groups_of_partition_i

            #Example: partition 1 (always PR1_1) has one group (PR1_1), partition 2 has 3 groups PR2_1,
            # PR2_2L, PR2_3, and partition 3 has 3 groups PR3_1, PR3_2, PR3_3. If we want to deallocate
            # DAG info for the groups in these 3 partitions, stat will be 1 (the intial value), the sum
            # of the number of groups in these 3 partitions is 1 + 3 + 3 = 7 so we will deallocate
            # group indices 1 .. 7 which are in the partitions witj indices 1 .. 3. Thusm we compute
            # the start and end of the partition indice and then the end of the group indices. The 
            # start index of the grooup is maintained (stored) as we go. So after these deallocations,
            # we set start = 8. e also remember the last index deallocated, which here is 7. If we need
            # to restore some deallocations (which aer always a suffic of the deallocs that have been
            # done), the restoration will end at 7 (whcih is start - 1).

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
 
            # set start to end if we did a deallocation, i.e., if start < end. The new
            # start will be in position for deallocating the next (if any) partition of groups.
            # Note that if start equals end, then we did not do a deallocation since 
            # end is exclusive. (And we may have just incremented end, so dealllocating 
            # "1 to 1", with start = 1 and end = 1, was implemented as incrementing 
            # end to 2 and using range(1,2) so start < end for the deallocation "1 to 1"
            # Note that end was exclusive so we can set start to end instead of end+1,
            # i.e., we alrady incremened end.
            if self.deallocation_start_index_groups < deallocation_end_index_groups:
                # Not that we bump deallocation_start_index_groups down as we deallocate the 
                # groups of the partitions. After the last position, deallocation_start_index_groups
                # will be ready for the next round of deallocations, i.e., next iteration of
                # the outer loop above. We calclate deallocation_end_index_groups each time 
                # inner for-loop is excuteded.
                self.deallocation_start_index_groups = deallocation_end_index_groups

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
            # a deallocation. And set deallocation_start_index_partitions so that it
            # is ready for the next round of deallocations, i.e., the next call to this method.
            self.version_number_for_most_recent_deallocation = requested_version_number_DAG_info
            # The start position of Whichever partitions of groups we just restored becomes 
            # the new start index for future deallocations. Since the first poaition 
            # retored was position deallocation_end_index_partitions it becomes the new
            # value for self.deallocation_start_index_partitions. For example, if the 
            # next version requsted is greater than self.version_number_for_most_recent_deallocation
            # we will do some deallocations starting in position self.deallocation_start_index_partitions.
            self.deallocation_start_index_partitions = deallocation_end_index_partitions

        # Example. Suppose the publishing interval is 4, i.e., after publishing the
        # base DAG (version 1) with partitions 1 and 2, we publish version 2 with 
        # 1 2 3 4 5 6 (base DAG plus 4 partitions). Before bfs() calls depost()
        # to publish this DAG it gets the last requested DAG version (using the method above) 
        # which is initializd to 1 and passes 1 to deallocate_DAG_structures.
        # (No one requests version 1, it is given to th DAG_execution_driver but it is 
        # logially the last requested version.)
        # For deallocation_end_index_partitions = (2+((requested_version_number_DAG_info-2)
        #    * DAG_executor_constants.INCREMENTAL_DAG_DEPOSIT_INTERVAL))-2
        # we get end is -4. Thus the for loop for i in range() does not execute
        # and we do not do any deallocations. Also, since deallocation_start_index_partitions
        # is not less than  deallocation_end_index_partitions we do not set start to end.
        # 
        # Next, bfs() will generate version 3 with 1 2 3 4 5 6 7 8 9 10, which has
        # 2 partitions from the base DAG version 1 and 2*4 partitions added for versions
        # 2 and 3 (4 added for version 2 and 4 added for version 3). bfs() generats 
        # DAG version 3 and before bfs()
        # calls deposit() to publish this new DAG version 3 it gets the last requsted 
        # DAG version, which we assume is 2 and passes 2 to deallocate_DAG_structures.
        # For deallocation_end_index_partitions = (2+((requested_version_number_DAG_info-2)
        #    * DAG_executor_constants.INCREMENTAL_DAG_DEPOSIT_INTERVAL))-2
        # we get end is 0 (and we add 1 to end to use end in the range) Thus the for loop for 
        # i in range(1,1) does not execute
        # and we do not do any deallocations. Also, since deallocation_start_index_partitions
        # 1 is not less than deallocation_end_index_partitions 1 we do not set start to end.
        # Recall that the first version for which deallocations can be done is 3, since
        # the formula for end will evaluate to a value n greater than 0 when the version number 
        # is 3 (so we use anend of n+1 in the range). The value of n will e the same as the value
        # of the publishing interval.
        #
        # Next, bfs() will generate version 4 with partitions 1 - 14, which has
        # 2 partitions from the base DAG version 1 and 3*4 partitions for versions
        # 2, 3 and 4. bfs() generates DAG version 4 and before bfs()
        # calls deposit() to publish this new DAG version 4 it gets the last requested
        # DAG version which we assume is 3 and passes 3 to deallocate_DAG_structures.
        # For deallocation_end_index_partitions = (2+((requested_version_number_DAG_info-2)
        #    * DAG_executor_constants.INCREMENTAL_DAG_DEPOSIT_INTERVAL))-2
        # we get end is 12. Thus the for loop for i in range(1,13) will execute. We
        # describe the groups deallocations for partitions 1 and 2 below, Also,
        # since deallocation_start_index_partitions is less than deallocation_end_index_partitions 
        # we will set start to end which sets start to 13, as in the next partition 
        # whose groups will be deallocated is partition 13 (after just deallocating the 
        # groups in partitions 1 - 12.)
        #
        # Assume we are building a DAG for the white board example with publishing interval 2
        # and the version requested is 3. Then end will be calculated using the above formula as
        # 2 + ((3-2)*2) - 2 = 2. Increment end for use in the range gives range (1,3)
        # For the "for i in range(1,3)"" loop, with i = 1 we do 
        # groups_of_partition_i = BFS.groups_of_partitions[i-1]
        # number_of_groups_of_partition_i = len(groups_of_partition_i)
        # which sets groups_of_partition_i to a list containing the single group PR1_1 in 
        # partition/group 1 so that number_of_groups_of_partition_i is 1. Now
        # deallocation_end_index_groups = deallocation_start_index_groups+number_of_groups_of_partition_i
        # = 1 + 1 = 2, which gives the for loop "for j in range(1,2) (where 2 is exclusive).
        # This loop will deallocate the single group in partition 1:
        # deallocate_Group_DAG_structures(j) where this method accesses position j-1
        # since the positions in the structures we are deallocating start with position 0,
        # i.e., the first group PR1_1 is in position 0, as in group_name = BFS.partition_names[j-1]
        # After this "for j in range (1,2) ends we set 
        # deallocation_start_index_groups = deallocation_end_index_groups
        # so deallocation_start_index_groups is set to 2. This means the next group
        # that we deallocate is group 2 (which is in position 1).
        # 
        # The for loop "for i in range(1,3)" continues with i = 2. We do
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
        # deallocate_Group_DAG_structures(j). After this "for j in range (2,5)"" loop ends we set 
        # deallocation_start_index_groups = deallocation_end_index_groups
        # so deallocation_start_index_groups is set to 5. This means the next group
        # that we deallocate will be group 5, the first group of partition 3.
        # We also set self.version_number_for_most_recent_deallocation to the requested 
        # version 3 for this deallocation.

    def deposit(self,**kwargs):
        # deposit a new DAG_info object. It's version number will be one more
        # than the current DAG_info object, which is in self.current_version_DAG_info.
        # Whether a newly generated DAG_info by bfs() is published to the workers/lambdas,
        # i.e., bfs() calls deposit(DAG_info) depends on the publishing interval - if the 
        # interval is i bfs() publishes every ith DAG_info object that is generated.
        # Wake up any workers waiting for the next version of the DAG. Workers
        # that finish executing the tasks in their version i of the DAG will request the next 
        # version i+1. If version i+1 or a later version hs not been published yet
        # then the workers (who call withdraw() to get a new DAG_info) will wait.
        # Note: a worker may be finishing executing an earlier version j of 
        # the DAG. When they request new version j+1, it may be the current
        # version in self.current_version_DAG_info they are requesting or an older 
        # version that they are requesting, that is, the workers are requesting j+1
        # but bfs() has already deosited a version k where k>j+1. In this case, we will give
        # them the current version in self.current_version_DAG_info, which is newer than the 
        # version theyrequsted. This is fine. Note that it is an option to deallocate old 
        # info in the DAG_INFO object when we ar using lambdas; this decreases the size of
        # the potentially large DAG_info object that is sent to the lambdas.
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
        # For lambdas, we can't start a leaf task when we get it in list parameter new_leaf_tasks
        # if the leaf task is not complete, i.e., it is to-be-continued (TBC). So we put it in the continue 
        # queue and start it when 
        # we get the next deposit() - the leaf task will be complete in the next deposit.
        # (Note: when we generate dag incrementally, when we get a new leaf task, we will
        # make the previous group/partitition complete and output the group's pickle file for 
        # the previous group/paritition, but we do not output the group's pickle file for 
        # for the new leaf task, which is the current partition/group.)
        # 
        # if DAG_info is complete then we can start leaf task now as leaf is start of new 
        # component and DAG_info is complete. See below where leaf tasks are started.
        # In fact, since the leaf task is the last group/partition to be generated in the DAG, 
        # we need to start it now since there will be no more deposits.
        DAG_info_is_complete = kwargs['DAG_info_is_complete']
        new_leaf_tasks = kwargs['new_current_version_new_leaf_tasks']
        self.num_nodes = kwargs['num_nodes']

        # Note: cummulative_leaf_tasks is for debugging - collection of all leaf tasks identified so far.
        self.cummulative_leaf_tasks += new_leaf_tasks
        if DAG_executor_constants.DEALLOCATE_DAG_INFO_STRUCTURES_FOR_LAMBDAS \
            and (self.num_nodes > DAG_executor_constants.THRESHOLD_FOR_DEALLOCATING_ON_THE_FLY):
            if DAG_executor_constants.USE_PAGERANK_GROUPS_INSTEAD_OF_PARTITIONS:
                # Note: if we are not using groups then groups_of_partitions_in_current_batch is []
                # and nothing ever is added to self.groups_of_partitions, which works too since
                # we will not try to use self.groups_of_partitions.
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

#brc: ToDo: can be more than one leaf task
        """
        try:
            msg = "[ERROR]:DAG_infoBuffer_Monitor_for_Lambdas:" \
                + " deposit received more than 1 leaf task; received: " \
                + str(len(new_leaf_tasks))
            assert not (len(new_leaf_tasks)>1) , msg
        except AssertionError:
            logger.exception("[Error]: assertion failed")
            if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
                logging.shutdown()
                os._exit(0)
        """
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

        restart = False # not using restart

#brc: lambda inc: 

        # Start lambdas for all continued states in buffer and for new leaf tasks 
        # if the DAG_info is complete.

        if DAG_executor_constants.RUN_ALL_TASKS_LOCALLY:
            # not using real lambdas, using threads that simulate a lambda.
            try:
                # start simulated lambdas with the new DAG_info if DAG_info was 
                # requested by lambdas that previously called withdraw()

                # we will be processing the first withdraw tuple. We thn set first to False.
                # Used below to assert something about the first withdraw_tuple.
                first = True

                logger.info("DAG_infoBuffer_Monitor_for_Lambdas: number of withdraw tuples: " + str(len(self._buffer)))
                if len(self._buffer) == 0:
                    logger.info("DAG_infoBuffer_Monitor_for_Lambdas: no withdraw tuples so no lambdas started.")

                # Reset global variables.
                # The start index needs to be reset even if there are no tuples since a
                # new DAG that is deposited has not had any deallocations done on it.
#brc: ToDo: Thus will change when we deposit an increment.
                if DAG_executor_constants.DEALLOCATE_DAG_INFO_STRUCTURES_FOR_LAMBDAS \
                        and (self.num_nodes > DAG_executor_constants.THRESHOLD_FOR_DEALLOCATING_ON_THE_FLY):
                    # no dealocations have been done, so start by deallocating 1
                    self.deallocation_start_index_groups = 1
                    self.deallocation_start_index_partitions = 1
                    # no dealocations have been done, so end index is 0
                    self.most_recent_deallocation_end_index = 0
                    # no dealocations have been done, this will be set whenevr we do 
                    # a deallocation
                    self.version_number_for_most_recent_deallocation  = 0
                    # rest saved structures
                    #The clear() method removes all items from the dictionary. 
                    self.current_version_DAG_info_DAG_map_save.clear()
                    self.current_version_DAG_info_DAG_states_save.clear()
                    self.current_version_DAG_info_DAG_tasks_save.clear()

                # If we are using incremental partitions, then we generate the
                # DAG by searching the connected components of partitions one component at
                # a time, i.e., all the partitions in component 1, then all the partitions 
                # in componet 2, etc. If lambda L finds a partition P that has a collapse partition 
                # C that is to be continued, then L will call withdraw() to get another
                # incremental DAG. Note that no other lambda can be executing, i.e.,
                # there is no other CC of partitions being executed so if
                # L adds a withdraw tuple in withdraw() representing that a new Lambda E
                # must be started to resume L's execution of C then there will be 
                # only one withdraw tuple (which is for L) when a new DAG is deposited.
                # If the tobecontinued partition C is the last partition in the CC being 
                # executed by lambda E, then C will be complete in the new DAG and after executing 
                # this now complete partition C, lambda E will terminate. So again
                # only one lambda will be executing (which is the next lambda started for executing
                # (the leaf task in) the next CC.) In general, only one lambda can have an
                # incomplete partition and thus only one can call withdraw(). This is true
                # even if the publishing interval for new incremental DAGs is a number n
                # such that m, m>1, CCs are processed in this interval. In this case, at least
                # m-1 CCs will not have an incomplete CC, i.e., all if these m-1 CCs
                # will have a last partition that is not tobecontinued. It is possible
                # the one CC, which is the CC currently being processed has a tobecontinued
                # partition. Note that whenever we publsh a new incremental DAG, the 
                # last partition that was generated is tobecontinued unless it is the 
                # last partition in the DAG, i.e., there are no more partitions to be 
                # generated. Note: When we generate a parttion P that is the last partitions
                # of a CC and there are more CCs to process, then P will be marked as 
                # tobecontinued and then we we generate the first partition of the next CC
                # we will know that P was the last partition of its CC and we will mark P
                # as not tobecontinued. (Thus we may publish a DAG that has P being tobecontinud
                # and then publish the next DAG with P being not tobecontinued, this happens if the 
                # publishing interval happends to require a new DAG to be generated with P 
                # tobecontinued in which case the next DAG will be published with P not tobecontinued.
                # We assert there is at most one withdraw tuple when using partitions next:
                if not DAG_executor_constants.USE_PAGERANK_GROUPS_INSTEAD_OF_PARTITIONS:
                    try:
                        msg = "[ERROR]:DAG_infoBuffer_Monitor_for_Lambdas:" \
                            + " using partitions, but self._buffer has more than one withdraw tuple. "
                        assert len(self._buffer) <= 1 , msg
                    except AssertionError:
                        logger.exception("[Error]: assertion failed")
                        if DAG_executor_constants.exit_program_on_exception:
                            logging.shutdown()
                            os._exit(0)

                for withdraw_tuple in self._buffer:
                    # The withdraw_tuples are sorted in ascending order of request_number.
                    # So if we dealloc for request i then we can dealloc more for request 
                    # j. j>i. We dealloc from start index to end index and then 
                    # assign end to start. Start is then ready for the next deallocation.
                    #
                    # Note that after deposit finishes, all the withdraw tuples have been
                    # processed (i.e., lambdas have been started for each tuple and a DAG
                    # (with deallocations) is passed to the started lambda. Th next monitor
                    # entry might be for withdraw(). The requested version on the withdraw
                    # might be less than the last requested version (for the last withdraw
                    # tuple) processed by this loop. In that case, the current version of
                    # the DAG has more deallocs than it should have based on the version
                    # requested by withdraw(), e.g., if the last withdraw tuple is for 
                    # version 10 but the version requested in withdraw() is 5, then the 
                    # current version of the DAG has deallocs that are appropriate for 
                    # version 10, but there are more deallocs than would be appropriate
                    # for version 5. In that case, we need to restore some of the deallocs 
                    # that weer done for version 10 (we restore a suffix of the version 10
                    # deallocs.) Note that when we do a dealloc of task/state i in the DAG,
                    # we save the informarion for i so that we can restore the information
                    # if necessary.
                    # Note that a new call to deposit starts with a DAG that has no 
                    # deallocatios, so the previous deallocations done to previously
                    # deposited DAGs aer lost.

#brc: #ToDo:
                    # ToDo: For new DAGs we pass to deposit only the differences between the 
                    # new DAG and the current DAG. That allows us to keep the deallocations
                    # that we have been done on the current DAG. If the requested
                    # version number in the first withdraw tuple is less than the version 
                    # number for the last deallocation (which couldl have been for the last 
                    # withdraw tuple in the previos call to deposit()  or the request in the
                    # most recent call to withdraw()) then we will have to do a restore
                    # when we process the first withdraw tuple 
                    # Note: for simulated lambdas, DAG execution and DAG generation and 
                    # dealloc (called by bfs()) are running locally and concurrently. That 
                    # is we can access the DAG for execution concurrently with generating 
                    # and deallocating the DAG. Thus, the DAG given to the executor is a 
                    # deep copy of the generated DAG so that the executor and bfs() are
                    # not sharing a DAG object (so there are no concurrent read/write 
                    # accesses of the same object.)
                    # For real lambdas, the DAG returned to a lambda executor is returned
                    # over the network as a serialized copy, so the lambda executor is
                    # not sharing a DAG object with bfs() generatin and deallocation.
                    # In this case the monitor object with method deposit() is on the tcp_server 
                    # process and bfs() is running in a different process (tastAll) so the 
                    # DAG_info object in deposit() is seperate from the Partition/Group 
                    # structures used by bfs() to create the next DAG_info, i.e., no 
                    # concurrent access.
                    # Note: the bfs() thread calls both the incemental
                    # DAG generation method and the deposit() method (where deposit()
                    # does any deallocs) so DAG generation and deallocation are both
                    # done by bfs() and thus are not concurrent.

# ToDo: do dealloc in withdraw too, which can be concurrent with incremental DAG generation
# methods adding to DAG. As lambdas call withdraw to get a new DAG and bfs
# calls incremental DAG generation methods and workers/lambdas run concurrently 
# with bfs(). So problem for simulated lambdas: withdraw  deallocs/restores concurrent
# with bfs() generation.? So deposut a deep copy of incremental changes, where 
# gen methods ar just keeping a window of states (current,previous,prevous previous)

                    requested_current_version_number = withdraw_tuple[0]
                    logger.info("DAG_infoBuffer_Monitor_for_Lambdas: deposit: "
                        + "requested_current_version_number: " +  str(requested_current_version_number))
                    start_tuple = withdraw_tuple[1]
                    # pass the state/task the thread is to execute at the start of its DFS path
                    start_state = start_tuple[0]
                    # Note: we get the input/output from start_tuple[1] below.

                    # We know this is true: requested_current_version_number <= self.current_version_number_DAG_info:
                    # This is because if a lambda excutor E currently is executing DAG version i, when it is 
                    # finished with version i, which is either the first version 1 of the DAG, 
                    # or a version obtained by a call to withdraw(), which is the version deposited by
                    # the most recent deposit() D1, E will request version i+1, so that any deposit()
                    # D2 after D1 will have a version number greater than i (otherwise deposit D2
                    # deposits a DAG with a version number <= that of D1, which is impossible.
                    #
                    # The requested_current_version_number is either:
                    # - less than version_number_for_most_recent_deallocation: need to restore a suffix
                    #      of previous deallocation 
                    # - equal to version_number_for_most_recent_deallocation: nothing to do
                    # - greater than version_number_for_most_recent_deallocation: Can extend the previous od deallocs
                    # Note: Here we are using the version number of the DAg for the most recent dellocation
                    # not the version number of the current version of the DAG (i.e., last version deposited).
                    #
                    # Note: The First request is for version number 2 - the DAG driver starts
                    # a lambda for the first partition/group that is collected (which is a leaf
                    # node) and this lambda will exexcute partition/group 1 and see that the next
                    # partition/groups are to-be-continued, so it will call withdraw to request a 
                    # new incremental DAG with version number 2. This 2 will be greater than the
                    # version for last deallocation, which was inited to 0, so if version 2
                    # has been deposited (by bfs()) and no higher version has been deposited,
                    # we will try to deallocate but no deallocation is possible for version 2 so 
                    # nothing will be deallocated. It's also possible that several calls to deposit()
                    # have benn made and the current version is version 3 or higher. In that case
                    # deallocations can and will be done - the deallocations done are the ones
                    # appropriate for the requested version. That is, if a lambda requests version, 
                    # say, 2, and version 5 has been deposited, then based on the progress that
                    # the lambda has made, which is that it has executed partition/group 1 and it needs
                    # to continue with the excution of the partitions/groups after that, we will 
                    # deallocat the "old" partitions/groups that are no loger needed. The current
                    # version is 5, which conains partitons/groups that he lambda wuill need to 
                    # excute so we cannot do a deallocation based on version 5 - version 5 does not 
                    # indicate the progress that the lambda has made, it only indicates the progress
                    # that has been made in generating the DAG (not excuting it),
                    # 
                    # If we start a lambda L1 and pass it version 2 in its payload, it will eventally 
                    # request version 3. At some point, either before or after the request is made
                    # by lambda L1 (which calls withdraw() to make a request), 1 or more deposits()
                    # can be made. For example, bfs() may deposit version 3, 4, and 5, before the 
                    # call to withdraw() enters the monitor, or bfs() may deposit version 3 after 
                    # withdraw() completes. For version 3 or higher, we can do the first deallocation, 
                    # setting the version number for last deallocation to 3, n>=3, where 3 was the 
                    # version requested by lamnda L1. At that point, some other lambda L2 can request 
                    # version 2, which will be less then 3 the version number for last deallocation,
                    # so we will do a restore to restore elements that were deallocated but that are
                    # needed by L2. That is, L2, which requests version 2 has made less progres than
                    # L1, which requestd version 3, so we have to restore some of the elments that
                    # were deallocated based on L1's progress but tht are needed by P2 since L2
                    # has made less progress.
                    #
                    # Note: We only do deallocations/restore if there is at least one
                    # withdraw_tuple since we need to know the current requested 
                    # version in order to do deallocations and the requested version 
                    # numbers are in the withdraw_tuples.

                    if DAG_executor_constants.DEALLOCATE_DAG_INFO_STRUCTURES_FOR_LAMBDAS \
                            and (self.num_nodes > DAG_executor_constants.THRESHOLD_FOR_DEALLOCATING_ON_THE_FLY):

                        if not DAG_executor_constants.USE_PAGERANK_GROUPS_INSTEAD_OF_PARTITIONS:
                            if requested_current_version_number < self.version_number_for_most_recent_deallocation:
                                # This condition can only be True if we are processing the first withdraw_tuple,
                                # since we update self.version_number_for_most_recent_deallocation (to the requested
                                # version number in the first withdraw_tuple) and the requested
                                # version numbers made by withdraw_tuples after the first are equal to or higher 
                                # than that of the first withdraw_tuple (since the tuples are in ascending sorted order ot
                                # requested versions).
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
                                # We will set self.version_number_for_most_recent_deallocation in deallocate
                                # if we do deallocate
                            else:   
                                # requested_current_version_number == self.version_number_for_most_recent_deallocation
                                # so the deallocations that have been made are also valid for this request.
                                pass
                        else:
                            if requested_current_version_number < self.version_number_for_most_recent_deallocation:     
                                # This condition can only be True if we are processing the first withdraw_tuple,
                                # since we update self.version_number_for_most_recent_deallocation (to the requested
                                # version number in the first withdraw_tuple) and the requested
                                # version numbers made by withdraw_tuples after the first are equal to or higher 
                                # than that ofthe first withdraw_tuple (since the tuples are in ascending sorted order ot
                                # requested versions).
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
                                # We will set self.version_number_for_most_recent_deallocation in deallocate
                                # if we do deallocate
                                self.deallocate_DAG_structures_groups(requested_current_version_number)
                            else:   
                                # requested_current_version_number == self.version_number_for_most_recent_deallocation
                                # so the deallocations that have been made are also valid for this request.
                                pass  

                    if first: # don't need this if
                        first = False        

#brc: ToDo: Consider passing delta of DAG_info instead of entire DAG.

                    """ OLD
                    if DAG_executor_constants.DEALLOCATE_DAG_INFO_STRUCTURES_FOR_LAMBDAS \
                        and (self.num_nodes > DAG_executor_constants.THRESHOLD_FOR_DEALLOCATING_ON_THE_FLY):
                        if not DAG_executor_constants.USE_PAGERANK_GROUPS_PARTITIONS:
                            self.deallocate_DAG_structures_partitions(requested_current_version_number)
                        else:
                            self.deallocate_DAG_structures_groups(requested_current_version_number)
                    """

                    # If no restore/dealloc then saved will not have changed
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
                    # for a continued task, if the task is a group, we give the lambda the output
                    # it generated previously (before terminating) and use the output
                    # to do the group's/task's fanins/fanout. If the task is a partition,
                    # we give the lambda the output that was generated when the continued
                    # task was xecuted, which becomes the input for the execution of the 
                    # continued task's collapse task.
                    # (Tricky: after executing a group with TBC fanins/fanout/collpases,
                    # if it has TBC fanouts/faninNBs/fanins, we cannot do any of them so we
                    # have to wait until we get a new DAG and restart the group/task
                    # so we can complete the fanouts/faninNBs/fanins. After excuting 
                    # a partition P with a TBC collapse task/partition C, we cannot execute
                    # C. A partition can only have a collapse task (which is the only fanin/fanout
                    # task of the partition) so we know we will execute C when we get a new
                    # incremental DAG. Therefore, when we restart the partition/task P
                    # we supply the input (P's output) for the collapse task C and execute the collapse task.
                    # That is, if partition P has a collapse task task is a continued task,
                    # we send P's output and the state of P as parameters of deposit(), which 
                    # are saved in a withdraw_tuple) then when we "restart" the lambda here (using
                    # the information in the withdraw_tuple) the lambda gets the state of P, 
                    # then gets the collapse task C of P and then executes C with P's output as C's input.
                    # (P's output is in the lambda's payload)
                    input_or_output = start_tuple[1]    # output of P is input of collapse task C
                    # starting new DAG_executor in state start_state
                    DAG_exec_state = DAG_executor_State(function_name = "DAG_executor", function_instance_ID = str(uuid.uuid4()), state = start_state, continued_task = True)
                    DAG_exec_state.restart = False    # not using these fields; these are for synch objects  
                    DAG_exec_state.return_value = None
                    DAG_exec_state.blocking = False
                    logger.info("DAG_infoBuffer_Monitor_for_Lambdas: (re)starting lambda with DAG_executor_state.state: " + str(DAG_exec_state.state)
                        + " continued task: " + str(DAG_exec_state.continued_task))
                    #logger.trace("DAG_executor_state.function_name: " + DAG_executor_state.function_name)
                    payload = {
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
                        # Using threads to simulate lambdas and the threads just read DAG_info 
                        # locally room global map, so we do not actually need to pass it 
                        # to each Lambda. Passing DAG_info to be consistent with real lambdas
                        "DAG_info": self.current_version_DAG_info,
                        # server takes the place of tcp_server which the real lambdas
                        # use. server is accessible to the lambda simulator threads as 
                        # a global shared variable.
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

                # DAG_info version 1, if there is more than one partition, contains 
                # partition/group 1 (i.e., partition 1 always has one group) and 
                # partition 2/the one or more groups in partition 2.
                # The first partition is always a single group that starts a component, and 
                # the second partition may or may not be the start of a new component.
                # If it is not, then the second partition can have a lot of groups, like the 
                # whiteboard example. If it is the start of a new component then the second
                # partition is also a single group. A graph with two nodes and no edges has
                # 2 components. The first node is component/group/partition 1 and the 
                # second node is component/group/partition 2). Both of these groups/partitions
                # are leaf tasks and both will be started by DAG_executor_driver since they are
                # leaf tasks detected in the first 2 partitions. If there is a third node (an no others) in
                # this graph then this node is the only node in partition 3/group of partiton 3, and it is a 
                # leaf task that will be started by deposit(). (The first partition (which is also a group)
                # of a connected component is a leaf task.) Since group/partition 3 is the final 
                # group/partition in the graph, deposit will start it immediately, instead of waiting for the 
                # next deposit (as there will be no more deposits since the graph is completely visited.)
                #
                # Note that the DAG_info read by DAG_executor_driver will have both of 
                # these leaf tasks (partition/group 1 and partition/group 2) and the DAG_executor_driver
                # will start both leaf tasks so we cannot start the second leaf task(s) here or it will 
                # be executed twice. To prevent the second leaf task/group/partition from being started twice
                # in BFS the second leaf node is not added to to the new_leaf_tasks so 
                # new_leaf_tasks will be empty.
                #
                # If the DAG_info is complete, then the leaf task is complete (it has no fanins/fanouts) and
                # so does not need to be continued later, we can start a lambda for the
                # leaf task now (as oppsed to putting the leaf tsk in the continue queue
                # and starting a lambda for it on the next deposit - since the DAG_info
                # is complete incremental DAG generation is over and there will be no 
                # more deposits.)
                # Note: adding the leaf task to the continue queue menas a lambda
                # will be started for it in this deposit since below we start a 
                # lambda for all leaf tasks in the continue queue. (There may be 
                # a leaf task currently in the continue queue that will also be 
                # started in this deposit().

#brc: ToDo: loop throigh and check whether TBC
# get the state, get the state_info and check whether it is to-be-continued.
# if not then add it to the continue queue to be excuted now. If not,
# it is added below so it will be executed next deposit()

                if DAG_info_is_complete:
                    # If the DAG_info is complete then we can start the leaf task 
                    # as it is also complete. 
                    self.continue_queue += new_leaf_tasks
                else:
                    # The leaf task is tobecontinued so we cannot start it - we will wait
                    # until the next deposit to start/execute it since we know it it not TBC n 
                    # the next DAG_info object generated. (A task that is TBC mean that we do 
                    # not now the fanins/fanouts of the tssk; we do not want to execuet the task
                    # until we know its fanins/fanouts; when we execute a task we label each task  
                    # output with the fanin/fanout it is intended for, so for now we need to know
                    # the fanins/fanouts when we execute the task. We could concievably execute
                    # a task, save its outputs and then tag the outputs with their fanin/fanout
                    # destination once we know the fanins/fanouts.
                    # Note: Option: We could also start a leaf task if we knew it 
                    # is the first (def. of leaf) and last group/partition of a connected component 
                    # even if the DAG_info is not complete. For a partition (and the groups therein) bfs could
                    # track whether any nodes in this partition (and groups therein) have any
                    # children. If not, then this partition (and the groups therein) is the 
                    # last partition in the current connected component and if it is a leaf task and
                    # thus the only partiton in the connected component we can start 
                    # a lambda for the leaf task now. But this is a lot of work and it would only
                    # be worth it if a publication boundary occured right after the leaf partition
                    # was added to DAG_info.
                    pass
                
                # start a lambda to excute the leaf task found on the previous deposit(). When
                # the leaf task was found it was marked as tobecontinued in the DAG. This is 
                # the next DAG and in this DAG the leaf task is not tobocontinued and so can 
                # be executed.
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
                    # starting  new DAG_executor in state start_state
                    DAG_exec_state = DAG_executor_State(function_name = "DAG_executor", function_instance_ID = str(uuid.uuid4()), state = start_state, continued_task = False)
                    DAG_exec_state.restart = False      
                    DAG_exec_state.return_value = None
                    DAG_exec_state.blocking = False
                    logger.info("DAG_infoBuffer_Monitor_for_Lambdas: starting lambda for leaf task with DAG_executor_state.state:" + str(DAG_exec_state.state)
                        + " leaf task: " + str(DAG_exec_state.continued_task))
                    #logger.trace("DAG_executor_state.function_name: " + DAG_executor_state.function_name)
                    payload = {
                        # Note: for incremental DAG generation, when we restart a lambda
                        # for a continued task, if the task is a group, we give the lambda the output
                        # it generated previously (before terminating) and use the output
                        # to do the group's/task's fanins/fanout. If the task is a partition,
                        # we give the lambda the output that was generated when the continued
                        # task was xecuted, which becomes the input for the execution of the 
                        # continued task's collapse task.
                        # (Tricky: after executing a group with TBC fanins/fanout/collpases,
                        # if it has TBC fanouts/faninNBs/fanins, we cannot do any of them so we
                        # have to wait until we get a new DAG and restart the group/task
                        # so we can complete the fanouts/faninNBs/fanins. After excuting 
                        # a partition P with a TBC collapse task/partition C, we cannot execute
                        # C. A partition can only have a collapse task (which is the only fanin/fanout
                        # task of the partition) so we know we will execute C when we get a new
                        # incremental DAG. Therefore, when we restart the partition/task P
                        # we supply the input (P's output) for the collapse task C and execute the collapse task.
                        # That is, if partition P has a collapse task task is a continued task,
                        # we send P's output and the state of P as parameters of deposit(), which 
                        # are saved in a withdraw_tuple) then when we "restart" the lambda here (using
                        # the information in the withdraw_tuple) the lambda gets the state of P, 
                        # then gets the collapse task C of P and then executes C with P's output as C's input.
                        # (P's output is in the lambda's payload)
                        "input": input_or_output, # will be [] for leaf task
                        "DAG_executor_state": DAG_exec_state,
                        # Using threads to simulate lambdas and the threads just read DAG_info 
                        # locally from a global shared map, so we do not actually need to pass it 
                        # to each Lambda. Passing DAG_info to be consistent with real lambdas.
                        "DAG_info": self.current_version_DAG_info,
                        # server takes the place of tcp_server which the real lambdas
                        # use. server is accessible to the lambda simulator threads as 
                        # a global shared variable.
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
                # clear the started leaf tasks from the continue_queue
                self.continue_queue.clear()
                # add the leaf tasks just found to the continue queue since it is not 
                # complete and start a lambda for it in next deposit()
                if not DAG_info_is_complete:
                    self.continue_queue += new_leaf_tasks
                else:
                    # The leaf task is tobecontinued so we cannot start it - we will wait
                    # until the next deposit to start/execute it since we know it it not TBC n 
                    # the next DAG_info object generated. (A task that is TBC mean that we do 
                    # not now the fanins/fanouts of the tssk; we do not want to execuet the task
                    # until we know its fanins/fanouts; when we execute a task we label each task  
                    # output with the fanin/fanout it is intended for, so for now we need to know
                    # the fanins/fanouts when we execute the task. We could concievably execute
                    # a task, save its outputs and then tag the outputs with their fanin/fanout
                    # destination once we know the fanins/fanouts.
                    # Note: Option: We could also start a leaf task if we knew it 
                    # is the first (def. of leaf) and last group/partition of a connected component 
                    # even if the DAG_info is not complete. For a partition (and the groups therein) bfs could
                    # track whether any nodes in this partition (and groups therein) have any
                    # children. If not, then this partition (and the groups therein) is the 
                    # last partition in the current connected component and if it is a leaf task and
                    # thus the only partiton in the connected component we can start 
                    # a lambda for the leaf task now. But this is a lot of work and it would only
                    # be worth it if a publication boundary occured right after the leaf partition
                    # was added to DAG_info.
                    pass
            except Exception:
                logger.exception("[ERROR] DAG_infoBuffer_Monitor_for_Lambdas: Failed to start DAG_executor thread for state " + str(start_state))
                if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
                    logging.shutdown()
                    os._exit(0)
        else:
            # using real lambdas
            try:
                # start real lambdas with the new DAG_info
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
                    # The leaf task is tobecontinued so we cannot start it - we will wait
                    # until the next deposit to start/execute it since we know it it not TBC n 
                    # the next DAG_info object generated. (A task that is TBC mean that we do 
                    # not now the fanins/fanouts of the tssk; we do not want to execuet the task
                    # until we know its fanins/fanouts; when we execute a task we label each task  
                    # output with the fanin/fanout it is intended for, so for now we need to know
                    # the fanins/fanouts when we execute the task. We could concievably execute
                    # a task, save its outputs and then tag the outputs with their fanin/fanout
                    # destination once we know the fanins/fanouts.
                    # Note: Option: We could also start a leaf task if we knew it 
                    # is the first (def. of leaf) and last group/partition of a connected component 
                    # even if the DAG_info is not complete. For a partition (and the groups therein) bfs could
                    # track whether any nodes in this partition (and groups therein) have any
                    # children. If not, then this partition (and the groups therein) is the 
                    # last partition in the current connected component and if it is a leaf task and
                    # thus the only partiton in the connected component we can start 
                    # a lambda for the leaf task now. But this is a lot of work and it would only
                    # be worth it if a publication boundary occured right after the leaf partition
                    # was added to DAG_info.
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
                else:
                    # The leaf task is tobecontinued so we cannot start it - we will wait
                    # until the next deposit to start/execute it since we know it it not TBC n 
                    # the next DAG_info object generated. (A task that is TBC mean that we do 
                    # not now the fanins/fanouts of the tssk; we do not want to execuet the task
                    # until we know its fanins/fanouts; when we execute a task we label each task  
                    # output with the fanin/fanout it is intended for, so for now we need to know
                    # the fanins/fanouts when we execute the task. We could concievably execute
                    # a task, save its outputs and then tag the outputs with their fanin/fanout
                    # destination once we know the fanins/fanouts.
                    # Note: Option: We could also start a leaf task if we knew it 
                    # is the first (def. of leaf) and last group/partition of a connected component 
                    # even if the DAG_info is not complete. For a partition (and the groups therein) bfs could
                    # track whether any nodes in this partition (and groups therein) have any
                    # children. If not, then this partition (and the groups therein) is the 
                    # last partition in the current connected component and if it is a leaf task and
                    # thus the only partiton in the connected component we can start 
                    # a lambda for the leaf task now. But this is a lot of work and it would only
                    # be worth it if a publication boundary occured right after the leaf partition
                    # was added to DAG_info.
                    pass
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
        # request a new version of the DAG. A lambda L that finishes version 
        # i will request i+1. Note that i+1 may <= current_version. If so
        # return the current version. If not, then L is requesting
        # the next version of the DAG, which hasn't been generated yet so
        # we do not return a new DAG to L. Instead, L will see that no new DAG
        # was returned and terminate. When a new DAG is depsited, we will 
        # "restart" a new lambda to continue excuting the new DAG where L left off.
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

            # We have requested a new DAG version this is less than the current version (i.e.,,
            # the last version deposited. If we request version 2 and the current version is 2 then
            # we can return version 2. If the current version is 3, then we can return 3.
            # Before we return the DAG we may be able to eallocate some information in the DAG.
            # That is, the DAG may contain old information that is no longer needed by the lambda
            # that made the request. In this case we can perform deallocations. On the other hand
            # we may have just performed some deallocatiions in the previous call to wtihdraw to
            # remove old information that we not needed by the lambda that made the previous request,
            # but this information may be needed by the lambda that made the current request,
            # This is becuase the current requested version is less than that of the previous erquested
            # version. dealloc. For example, the previous request by lambda 1 was for version 5 where 
            # the current version was version 6. In that case, we deallocated soem information in 
            # the current version 6 that was no longer needed by lambda L1 based on the "progress"
            # it had made executed its DAG, i.e., the fact that it was requsting version 5 means that
            # there is information that was in versions 1, 2, 3, etc that L1 does not need since it has
            # already executed the part of the DAG in versions 1, 2, 3, etc. But L2 is requesting version,
            # say, 3, which is less than 5 (where the last dealocations were done based on version 5)
            # so L2 needs information that was deallocated from the current version 6 based on the fact
            # that its requested version 3 indicates that it has not made as much progres as L1.
            # In that case, we need to restore at least part of the old DAG_info states of the current version.
            #
            # Note: When we deposit() a new DAG_info we start over with a ADG that has no 
            # deallocations.
            # Option: Do not start over.
            #
            # Note: We cannot get here until after the first deposit() and deposit()
            # saves the num_nodes parameter value passed to it in self.num_nodes.
            # self.num_nodes is used next.

            if DAG_executor_constants.DEALLOCATE_DAG_INFO_STRUCTURES_FOR_LAMBDAS \
                    and (self.num_nodes > DAG_executor_constants.THRESHOLD_FOR_DEALLOCATING_ON_THE_FLY):
                # we know this is true from the above if-satement: 
                #   requested_current_version_number <= self.current_version_number_DAG_info:
                # The requested_current_version_number is either:
                # - less than version_number_for_most_recent_deallocation ==> need to restore a suffix
                #      of last deallocation 
                # - equal to version_number_for_most_recent_deallocation ==> nothing to do
                # - greater than version_number_for_most_recent_deallocation ==> Can continue 
                #       with dellocations at the point where the last deallocation ended,
                #
                # Note: First request is alays for version number 2. This will be greater than
                # version for last deallocation since no deallocations have occurred yet and 
                # version_number_for_most_recent_deallocation was inited to 0. So we will call 
                # deallocate but nothing will be deallocated. We know from above that 
                #   requested_current_version_number <= self.current_version_number_DAG_info
                # so we will return version 2 or greater to the calling lambda. If that lambda
                # gets version 2, it will eventually request version 3, and we will do the first 
                # deallocation, setting version for last deallocation to 3. Note that we remember
                # the version that was requested 3 which was used as the bsis for the deallocations.
                # We may return version 3 or a higher version, depending on the last version 
                # deposited, which could be version 6. but we use version 3 as the basis for 
                # deallocation since the request of 3 indicates the progress that the calling 
                # lambda has made in excuting its DAG, i.e., it has executed at lest some of the 
                # tasks that were in version 1. Noet that some lambda can then request version 2, 
                # which will be a request that is less than the requested version 3 that was the 
                # basis of the last dealloation that was done. In that csae we will need to do a restore
                # to restore some of the deallocations that were done based on requested version 3.
                # (Basically, if a request is made for version 3, we can deallocate information 
                # in the current version (last deposited version) of the DAG about partition/group/task 1 as
                # this partition/group/task 1 was already executed and it is not a continued task.
                # When a request is then received for version 2, we have to restore the information 
                # about version 1 since it will be needed by the lambda requested version 2. 
                # In detail: In version 1 of DAG, partition/group 1 has a tobecontinued collapse
                # task partition/group 2. A lambda L can excute 1, but it will need to requst a 
                # new DAG version 2 that will have complete information about 2 (the fanins/fanouts of 2).
                # Even though L executed 1, it needs information about 1 in the ndw DAG it receives
                # because it needs to acess the info about 1 to get the fanins/fanouts task of 1, which is 
                # partiton 2 or the groups in 2.
                # In general, just because we execute a task T in version i does not mean we can deallocate 
                # all info about T in version i+1 since T may have fanins/fanouts to tasks that were
                # continued in version i and we need info about T in version i+1 to complete the
                # procesing of T (i.e., we have executed T but we need to do its fanins/fanouts which in
                # the new DAG version i+1 are no longer to tobecontined tasks).
                if not DAG_executor_constants.USE_PAGERANK_GROUPS_INSTEAD_OF_PARTITIONS:
                    if requested_current_version_number < self.version_number_for_most_recent_deallocation:
                        # We will set self.version_number_for_most_recent_deallocation in restore
                        # if we do restores
                        self.restore_DAG_structures_partitions(requested_current_version_number)
                    elif requested_current_version_number > self.version_number_for_most_recent_deallocation:
                        # We will set self.version_number_for_most_recent_deallocation in deallocate
                        # if we do deallocations
                        self.deallocate_DAG_structures_partitions(requested_current_version_number)

                    else:   # requested_current_version_number == self.version_number_for_most_recent_deallocation:
                        pass
                else:
                    if requested_current_version_number < self.version_number_for_most_recent_deallocation:
                        # We will set self.version_number_for_most_recent_deallocation in restore
                        # if we do restores
                        self.restore_DAG_structures_groups(requested_current_version_number)
                    elif requested_current_version_number > self.version_number_for_most_recent_deallocation:
                        # We will set self.version_number_for_most_recent_deallocation in deallocate
                        # if we do deallocations
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
