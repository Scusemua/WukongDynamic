import logging
import os
import copy

import cloudpickle
from .DAG_info import DAG_Info
from .DFS_visit import state_info
from .BFS_pagerank import PageRank_Function_Driver, PageRank_Function_Driver_Shared
from .BFS_Shared import PageRank_Function_Driver_Shared_Fast
from .DAG_executor_constants import use_shared_partitions_groups
from .DAG_executor_constants import use_struct_of_arrays_for_pagerank
#from .DAG_executor_constants import using_threads_not_processes, use_multithreaded_multiprocessing
from .DAG_executor_constants import enable_runtime_task_clustering

from .BFS_generate_DAG_info import Group_senders, Group_receivers
from .BFS_generate_DAG_info import leaf_tasks_of_groups_incremental
from .BFS_generate_DAG_info import groups_num_shadow_nodes_map
#from .BFS_generate_DAG_info import num_nodes_in_graph

# Note: avoiding circular imports:
# https://stackoverflow.com/questions/744373/what-happens-when-using-mutual-or-circular-cyclic-imports
#from . import BFS


logger = logging.getLogger(__name__)

"""
if not (not using_threads_not_processes or use_multithreaded_multiprocessing):
    logger.setLevel(logging.ERROR)
    formatter = logging.Formatter('[%(asctime)s] [%(module)s] [%(processName)s] [%(threadName)s]: %(message)s')
    ch = logging.StreamHandler()
    ch.setLevel(logging.ERROR)
    #ch.setLevel(logging.INFO)
    ch.setFormatter(formatter)
    logger.addHandler(ch)
"""

#rhc: num_nodes
# this is set BFS.input_graph when doing non-incremental DAG generation with groups
num_nodes_in_graph = 0

# See the comments in BFS_generate_DAG_info_incremental_partitions.py. Processng
# group is very similar to processing partitons. We generate a partition and 
# process the groups within the partition. Partition processing should be understood
# before attempting to understand group partitioning.

Group_all_fanout_task_names = []
Group_all_fanin_task_names = []
Group_all_faninNB_task_names = []
Group_all_collapse_task_names = []
Group_all_fanin_sizes = []
Group_all_faninNB_sizes = []
Group_DAG_leaf_tasks = []
Group_DAG_leaf_task_start_states = []
# no inputs for leaf tasks
Group_DAG_leaf_task_inputs = []
# maps task name to an integer ID for that task
Group_DAG_states = {}
# maps integer ID of a task to the state for that task; the state 
# contains the fanin/fanout information for the task.
Group_DAG_map = {}
# references to the code for the tasks
Group_DAG_tasks = {}

# version of DAG, incremented for each DAG generated
Group_DAG_version_number = 0

#rhc: ToDo: Not using this for groups - we have to get *all* the groups
# of previous partition and iterate through them.

## Saving current_partition_name as previous_partition_name at the 
## end. We cannot just subtract one, e.g. PR3_1 becomes PR2_1 since
## the name of partition 2 might actually be PR2_1L, so we need to 
## save the actual name "PR2_1L" and retrive it when we process PR3_1
#Group_DAG_previous_partition_name = "PR1_1"

# number of tasks in the DAG
Group_DAG_number_of_tasks = 0
# the tasks in the last partition of a generated DAG may be incomplete,
# which means we cannot execute these tasks until the next DAG is 
# incrementally published. Maybe greater than 1, unlike for partitions
# as there is only one partition but there may be many groups in this partition.
Group_DAG_number_of_incomplete_tasks = 0

# used to generate IDs; starting with 1, not 0
Group_next_state = 1

#rhc: num_nodes:
Group_DAG_num_nodes_in_graph = 0

# Called by generate_DAG_info_incremental_partitions below to generate 
# the DAG_info object when we are using partitions.
def generate_DAG_for_groups(to_be_continued,number_of_incomplete_tasks):
    global Group_all_fanout_task_names
    global Group_all_fanin_task_names
    global Group_all_faninNB_task_names
    global Group_all_collapse_task_names
    global Group_all_fanin_sizes
    global Group_all_faninNB_sizes
    global Group_DAG_leaf_tasks
    global Group_DAG_leaf_task_start_states
    # no inputs for leaf tasks
    global Global_DAG_leaf_task_inputs
    global Group_DAG_map
    global Group_DAG_states
    global Group_DAG_tasks

    # version of DAG, incremented for each DAG generated
    global Group_DAG_version_number
    # Saving current_partition_name as previous_partition_name at the 
    # end. We cannot just subtract one, e.g. PR3_1 becomes PR2_1 since
    # the name of partition 2 might actually be PR2_1L, so we need to 
    # save the actual name "PR2_1L" and retrive it when we process PR3_1.
    # Not using previous_partition_name for groups, only partitions.
    # Note: Used for partitons but this is not used for groups.
    #global Group_DAG_previous_partition_name

    global Group_DAG_number_of_tasks
    global Group_DAG_number_of_incomplete_tasks
#rhc: num_nodes
    global Group_DAG_num_nodes_in_graph

    # used for debugging
    show_generated_DAG_info = True

    """
    # Note: This method will change the state_info of the previous state,
    # for which we either change only the TBC field or we change the TBC and 
    # the collapse field:
        # add a collapse for this current partition to the previous state
        previous_state = current_state - 1
        state_info_previous_state = Partition_DAG_map[previous_state]
        Partition_all_collapse_task_names.append(current_partition_name)
        collapse_of_previous_state = state_info_previous_state.collapse
        # adding a collapsed task to state info of previous task
        collapse_of_previous_state.append(current_partition_name)

        # previous partition is now complete 
        state_info_previous_state.ToBeContinued = False
    # Note: this is the partiton code. we will be looping through the groups
    # in the previous partition and doing this for each group.

    # Since this state info is read by the DAG executor, we make a copy
    # of the state info and change the copy. This state info is the only 
    # read-write object that is shared by the DAG executor and the DAG
    # generator (here). By making  copy, the DAG executor can read the 
    # state info in the previously generated DAG while the DAG generator
    # writes a copy of this state info for the next ADG to be generated,
    # i.e., there is no (concurrent) sharing.
    """
    
    # we construct a dictionary of DAG information 
    logger.trace("")
    DAG_info_dictionary = {}
    DAG_info_dictionary["DAG_map"] = Group_DAG_map
    DAG_info_dictionary["DAG_states"] = Group_DAG_states
    DAG_info_dictionary["DAG_leaf_tasks"] = Group_DAG_leaf_tasks
    DAG_info_dictionary["DAG_leaf_task_start_states"] = Group_DAG_leaf_task_start_states
    DAG_info_dictionary["DAG_leaf_task_inputs"] = Group_DAG_leaf_task_inputs
    DAG_info_dictionary["all_fanout_task_names"] = Group_all_fanout_task_names
    DAG_info_dictionary["all_fanin_task_names"] = Group_all_fanin_task_names
    DAG_info_dictionary["all_faninNB_task_names"] = Group_all_faninNB_task_names
    DAG_info_dictionary["all_collapse_task_names"] = Group_all_collapse_task_names
    DAG_info_dictionary["all_fanin_sizes"] = Group_all_fanin_sizes
    DAG_info_dictionary["all_faninNB_sizes"] = Group_all_faninNB_sizes
    DAG_info_dictionary["DAG_tasks"] = Group_DAG_tasks

    # These key/value pairs were added for incremental DAG generation.

    # If there is only one partition in the DAG then it is complete and is version 1.
    # It is returned above. Otherwise, version 1 is the DAG_info with partitions
    # 1 and 2, where 1 is complete and 2 is complete or incomplete.
    Group_DAG_version_number += 1
    # if the last partition has incomplete information, then the DAG is 
    # incomplete. When groups in partition i is added to the DAG, they are incomplete
    # unless it is the last partition in the DAG). They becomes complete
    # when we add partition i+1 to the DAG. (So grous in partition i needs information
    # that is generated when we create grops in partition i+1. The  
    # nodes in the groups of partition i can ony have children that are in partition 
    # i or partition i+1. We need to know partition i's children
    # in order for partition i to be complete. Grops n partition i's childen
    # are discovered while generating grops in partition i+1) 
    Group_DAG_is_complete = not to_be_continued # to_be_continued is a parameter
    # number of tasks in the current incremental DAG, including the
    # incomplete last partition, if any.
    Group_DAG_number_of_tasks = len(Group_DAG_tasks)
    # For partitions, this is at most 1. When we are generating a DAG
    # of groups, there may be many groups in the incomplete last
    # partition and they will all be considered to be incomplete.
    Group_DAG_number_of_incomplete_tasks = number_of_incomplete_tasks # parameter of method
#rhc: num_nodes
    # The value of num_nodes_in_graph is set by BFS_input_graph
    # at the beginning of execution, which is before we start
    # DAG generation. This value does not change.
    Group_DAG_num_nodes_in_graph = num_nodes_in_graph
    DAG_info_dictionary["DAG_version_number"] = Group_DAG_version_number
    DAG_info_dictionary["DAG_is_complete"] = Group_DAG_is_complete
    DAG_info_dictionary["DAG_number_of_tasks"] = Group_DAG_number_of_tasks
    DAG_info_dictionary["DAG_number_of_incomplete_tasks"] = Group_DAG_number_of_incomplete_tasks

#rhc: num_nodes:
    DAG_info_dictionary["DAG_num_nodes_in_graph"] = Group_DAG_num_nodes_in_graph

    # Note: we are saving all the incemental DAG_info files for debugging but 
    # we probably want to turn this off otherwise.

    # filename is based on version number - Note: for partition, say 3, we
    # have output the DAG_info with partitions 1 and 2 as version 1 so 
    # the DAG_info for partition 3 will have partitions 1, 2, and 3 and will
    # be version 2 but named "DAG_info_incremental_Partition_3"
    file_name_incremental = "./DAG_info_incremental_Group_" + str(Group_DAG_version_number) + ".pickle"
    #Note: closes the file when the with statement ends, even if an exception occurs
    with open(file_name_incremental, 'wb') as handle:
        cloudpickle.dump(DAG_info_dictionary, handle) #, protocol=pickle.HIGHEST_PROTOCOL)  

    num_fanins = len(Group_all_fanin_task_names)
    num_fanouts = len(Group_all_fanout_task_names)
    num_faninNBs = len(Group_all_faninNB_task_names)
    num_collapse = len(Group_all_collapse_task_names)

    # for debugging
    if show_generated_DAG_info:
        logger.trace("DAG_map:")
        for key, value in Group_DAG_map.items():
            logger.trace(str(key) + ' : ' + str(value))
        logger.trace("")
        logger.trace("states:")        
        for key, value in Group_DAG_states.items():
            logger.trace(str(key) + ' : ' + str(value))
        logger.trace("")
        logger.trace("num_fanins:" + str(num_fanins) + " num_fanouts:" + str(num_fanouts) + " num_faninNBs:"
        + str(num_faninNBs) + " num_collapse:" + str(num_collapse))
        logger.trace("")  
        logger.trace("all_fanout_task_names:")
        for name in Group_all_fanout_task_names:
            logger.trace(name)
        logger.trace("all_fanin_task_names:")
        for name in Group_all_fanin_task_names :
            logger.trace(name)
        logger.trace("all_fanin_sizes:")
        for s in Group_all_fanin_sizes :
            logger.trace(s)
        logger.trace("all_faninNB_task_names:")
        for name in Group_all_faninNB_task_names:
            logger.trace(name)
        logger.trace("all_faninNB_sizes:")
        for s in Group_all_faninNB_sizes:
            logger.trace(s)
        logger.trace("all_collapse_task_names:")
        for name in Group_all_collapse_task_names:
            logger.trace(name)
        logger.trace("")
        logger.trace("leaf task start states:")
        for start_state in Group_DAG_leaf_task_start_states:
            logger.trace(start_state)
        logger.trace("")
        logger.trace("DAG_tasks:")
        for key, value in Group_DAG_tasks.items():
            logger.trace(str(key) + ' : ' + str(value))
        logger.trace("")
        logger.trace("DAG_leaf_tasks:")
        for task_name in Group_DAG_leaf_tasks:
            logger.trace(task_name)
        logger.trace("")
        logger.trace("DAG_leaf_task_inputs:")
        for inp in Group_DAG_leaf_task_inputs:
            logger.trace(inp)
        logger.trace("")
        logger.trace("DAG_version_number:")
        logger.trace(Group_DAG_version_number)
        logger.trace("")
        logger.trace("DAG_is_complete:")
        logger.trace(Group_DAG_is_complete)
        logger.trace("")
        logger.trace("DAG_number_of_tasks:")
        logger.trace(Group_DAG_number_of_tasks)
        logger.trace("")
        logger.trace("DAG_number_of_incomplete_tasks:")
        logger.trace(Group_DAG_number_of_incomplete_tasks)
        logger.trace("")
#rhc: num_nodes
        logger.trace("DAG_num_nodes_in_graph:")
        logger.trace(Group_DAG_num_nodes_in_graph)
        logger.trace("")
    # for debugging
    # read file file_name_incremental just written and display contents 
    if False:
        DAG_info_Group_read = DAG_Info.DAG_info_fromfilename(file_name_incremental)
        
        DAG_map = DAG_info_Group_read.get_DAG_map()
        # these are not displayed
        all_collapse_task_names = DAG_info_Group_read.get_all_collapse_task_names()
        # Note: prefixing name with '_' turns off th warning about variabel not used
        _all_fanin_task_names = DAG_info_Group_read.get_all_fanin_task_names()
        _all_faninNB_task_names = DAG_info_Group_read.get_all_faninNB_task_names()
        _all_faninNB_sizes = DAG_info_Group_read.get_all_faninNB_sizes()
        _all_fanout_task_names = DAG_info_Group_read.get_all_fanout_task_names()
        # Note: all fanout_sizes is not needed since fanouts are fanins that have size 1
        DAG_states = DAG_info_Group_read.get_DAG_states()
        DAG_leaf_tasks = DAG_info_Group_read.get_DAG_leaf_tasks()
        DAG_leaf_task_start_states = DAG_info_Group_read.get_DAG_leaf_task_start_states()
        DAG_tasks = DAG_info_Group_read.get_DAG_tasks()

        DAG_leaf_task_inputs = DAG_info_Group_read.get_DAG_leaf_task_inputs()

        Group_DAG_is_complete = DAG_info_Group_read.get_DAG_info_is_complete()
        DAG_version_number = DAG_info_Group_read.get_DAG_version_number()
        DAG_number_of_tasks = DAG_info_Group_read.get_DAG_number_of_tasks()
        DAG_number_of_incomplete_tasks = DAG_info_Group_read.get_DAG_number_of_incomplete_tasks()
#rhc: num_nodes
        DAG_num_nodes_in_graph = DAG_info_Group_read.get_DAG_num_nodes_in_graph()

        logger.trace("")
        logger.trace("DAG_info Group after read:")
        output_DAG = True
        if output_DAG:
            # FYI:
            logger.trace("DAG_map:")
            for key, value in DAG_map.items():
                logger.trace(str(key) + ' : ' + str(value))
                #logger.trace(key)
                #logger.trace(value)
            logger.trace("  ")
            logger.trace("DAG states:")      
            for key, value in DAG_states.items():
                logger.trace(str(key) + ' : ' + str(value))
            logger.trace("   ")
            logger.trace("DAG leaf task start states")
            for start_state in DAG_leaf_task_start_states:
                logger.trace(start_state)
            logger.trace("")
            logger.trace("all_collapse_task_names:")
            for name in all_collapse_task_names:
                logger.trace(name)
            logger.trace("")
            logger.trace("DAG_tasks:")
            for key, value in DAG_tasks.items():
                logger.trace(str(key) + ' : ' + str(value))
            logger.trace("")
            logger.trace("DAG_leaf_tasks:")
            for task_name in DAG_leaf_tasks:
                logger.trace(task_name)
            logger.trace("") 
            logger.trace("DAG_leaf_task_inputs:")
            for inp in DAG_leaf_task_inputs:
                logger.trace(inp)
            logger.trace("")
            logger.trace("DAG_version_number:")
            logger.trace(DAG_version_number)
            logger.trace("")
            logger.trace("DAG_info_is_complete:")
            logger.trace(Group_DAG_is_complete)
            logger.trace("")
            logger.trace("DAG_number_of_tasks:")
            logger.trace(DAG_number_of_tasks)
            logger.trace("")
            logger.trace("DAG_number_of_incomplete_tasks:")
            logger.trace(DAG_number_of_incomplete_tasks)
            logger.trace("")
#rhc: num_nodes
            logger.trace("DAG_num_nodes_in_graph:")
            logger.trace(DAG_num_nodes_in_graph)
            logger.trace("")

    DAG_info = DAG_Info.DAG_info_fromdictionary(DAG_info_dictionary)
    return  DAG_info

"""
Note: The code for DAG_info_fromdictionary is below. The info in a 
DAG_info object is obtained from its DAG_info_dictionary.

    def __init__(self,DAG_info_dictionary,file_name = './DAG_info.pickle'):
        self.file_name = file_name
        if not use_incremental_DAG_generation:
            self.DAG_map = DAG_info_dictionary["DAG_map"]
        else:
            # Q: this is the same as DAG_info_dictionary["DAG_map"].copy()?
            self.DAG_map = copy.copy(DAG_info_dictionary["DAG_map"]
        ...

    @classmethod
    def DAG_info_fromfilename(cls, file_name = './DAG_info.pickle'):
        file_name = file_name
        DAG_info_dictionary = input_DAG_info(file_name)
        return cls(DAG_info_dictionary,file_name)

    @classmethod
    def DAG_info_fromdictionary(cls, DAG_info_dict):
        DAG_info_dictionary = DAG_info_dict
        return cls(DAG_info_dictionary)
"""

# called by bfs()
def generate_DAG_info_incremental_groups(current_partition_name,
    current_partition_number, groups_of_current_partition,
    groups_of_partitions,
    to_be_continued):
# to_be_continued is True if BFS (caller) finds num_nodes_in_partitions < num_nodes in graph, 
# which means that incremeental DAG generation is not complete 
# (some graph nodes are not in any partition.)
# groups_of_current_partition is a list of groups in the current partition.
# groups_of_partitions is a list of the groups_of_current_partition. We need
# this to get the groups of the previous partition and th previous previous partition. 

    global Group_all_fanout_task_names
    global Group_all_fanin_task_names
    global Group_all_faninNB_task_names
    global Group_all_collapse_task_names
    global Group_all_fanin_sizes
    global Group_all_faninNB_sizes
    global Group_DAG_leaf_tasks
    global Group_DAG_leaf_task_start_states
    # no inputs for leaf tasks
    global Group_DAG_leaf_task_inputs
    global Group_DAG_map
    global Group_DAG_states
    global Group_DAG_tasks

    # version of DAG, incremented for each DAG generated
    global Group_DAG_version_number
    # Saving current_partition_name as previous_partition_name at the 
    # end. We cannot just subtract one, e.g. PR3_1 becomes PR2_1 since
    # the name of partition 2 might actually be PR2_1L, so we need to 
    # save the actual name "PR2_1L" and retrive it when we process PR3_1
    global Group_DAG_previous_partition_name
    global Group_DAG_number_of_tasks

    # used to generate IDs; state for next group added to DAG
    global Group_next_state 

    #rhc_ num_nodes
    # This was set above and will not be changed in this method
    global Group_DAG_num_nodes_in_graph

    logger.info("generate_DAG_info_incremental_groups: to_be_continued: " + str(to_be_continued))
    logger.info("generate_DAG_info_incremental_groups: current_partition_number: " + str(current_partition_number))

    logger.info("")

    # Using copy() here and below to avoid the error: "RuntimeError: dictionary changed size during iteration"
    # when we are using multithreaded bfs(). That is, while the generator thread is
    # iterating here bfs() could add a key:value to the dictionary
    # and an exceptioj is thrown when a dictionary is changed in size (i.e., an item is added or removed) 
    # while it is being iterated over in a loop. We also use copy() for thr 
    # list we are iterating over.

    logger.info("generate_DAG_info_incremental_groups: Group_senders:")
    for sender_name,receiver_name_set in Group_senders.copy().items():
        logger.info("sender:" + sender_name)
        logger.info("receiver_name_set:" + str(receiver_name_set))
    logger.info("")
    logger.info("")
    logger.info("generate_DAG_info_incremental_groups: Group_receivers:")
    for receiver_name,sender_name_set in Group_receivers.copy().items():
        logger.info("receiver:" + receiver_name)
        logger.info("sender_name_set:" + str(sender_name_set))
    logger.info("")
    logger.info("")
    logger.info("generate_DAG_info_incremental_groups: Leaf nodes of groups:")
    for name in leaf_tasks_of_groups_incremental.copy():
        logger.info(name + " ")
    logger.info("")

    logger.info("generate_DAG_info_incremental_groups: Partition DAG incrementally:")


    # in the DAG_map, partition/group i is state i. The first group is also 
    # the first partition, and they both have the number 1.
    current_partition_state = current_partition_number

    # partition/group 1 is a special case, it does not access the previous 
    # state as states start with 1. This is not used when
    # current_partition_number is 1.
    previous_partition_state = current_partition_number-1
    #previous_partition_name = "PR" + str(current_partition_number-1) + "_1"

    # a list of groups that have a fanot/fanin/collapse to the 
    # target group. (They "send" to the target group which "receives".)
    #senders = Group_receivers.get(current_partition_name)
    # Note: a group that starts a new connected component (which is 
    # the first partition collected on a call to BFS(), of which there 
    # may be many calls if the graph is not connected) is a leaf
    # node and thus has no senders. This is true about partition/group 1 and
    # this is asserted by the caller (BFS()) of this method.

    """
    Outline: 
    Each call to generate_DAG_info_incremental_pgroups adds the groups in one partition 
    (the current partition) to the DAG_info. The added groups are incomplete unless it is the last partition 
    that will be added to the DAG (or it is the last partition in its connected component,
    as this partition has no fanouts/fanins/etc.. ) The previous partition is now marked as complete.
    The previous partition's next partition is this current partition, which is 
    either complete or incomplete. If the current partition is incomplete then 
    the previous partition is marked as having an incomplete next partition. We also
    comsider the previous partition of the previous partition. It was marked as complete
    when we processed the previous partition, but it was considered to have an incomplete
    next partition, which is the partition previous to the current partition.
    Now that we marked the previous partition as complete, the previous 
    previous partition is marked as not having an incomplete next partition.
    
    There are 3 cases:
    1. current_partition_number == 1: This is the first group/partition. This means 
    there are no groups of the previous partition or the previous previous partition. The current
    group is marked as complete if the entire DAG has only one partition/group; otherwise
    it is marked as complete. Note: If the current partition/group (which is partition/group 1) is
    the only partition/group in its connected component, i.e., its component has size 1,
    then it can also be marked as complete since it has no fanouts/fanins and thus we have
    all the info we need about partition/group 1 (i.e., its fanouts/fanins) and it can be marked complete.
    (Currently, when we get the first
    group of a connected component we know the groups in the previous partition have no
    fanins/fanouts to a group that is not in the same partition. But when we processed
    thee groups we did not know they were in a partition that was the last partition in 
    its connected component. So we assumed they were incomplete, when they were not. That 
    is not an error but it delays marking them as complete. To identify the last partition
    in a conncted component (besides the partitio that is the last partition
    to be connected in the DAG) we would have to look at all the nodes in a 
    partition and determibe whethr they had any child nodes that were not in
    the same partition (i.e., these child nodes will be i the next partition).
    This would have to be done for each partition and it's not clear whether
    all that work would be worth it just to mark the last partition of a 
    connected component completed a little earlier than it otherwise would.
    Note ths is only helpful if the incremental DAG generatd happens to 
    end with a partition that is the last partition in its connected compoent.
    If the interval n between incremental DAGs (i.e., add n partitions before
    publishng the new DAG) then it may be rare to have such a partition.

    2. (senders == None): This is a leaf group, which could be group 2 or any 
    group after that. This means that the current group is the first group
    of a new connected component. We will add this leaf group to a list of leaf
    groups so that when we return we can make sure this leaf group is 
    executed. (No other group has a fanin/fanout/collapse to this group so no
    other group can cause this leaf group to be executed. We will start its execution
    ourselves.) The groups in the previous and previous previous partitions are marked as described above.
    Note the the groups in the previous partition can be marked complete as usual. Also, we now know that the 
    groups in the previous partition, which were marked as having an incomplete next group, can now
    be marked as not having an incomplete next group - this is because the groups in the previous
    partition were in the the last partition of a connected component and thus have no 
    fanins/fanouts/collapses at all - this allows us to mark them as not having an incomplete
    next group.
    3. else: # current_partition_number >= 2: This is not a leaf partition and this partition 
    is not the first partition. Process the groups in the previous and previous previous partitions as
    described above. Note: assume the current partition has groups GC1, GC2, and GC3 and the 
    previous partition has groups GP1, GP2, and GP3. We will loop through the groups 
    in the current partition. We need to mark the groups in the previous partition as
    complete, but we only want to do this once. So for the first group GC1, we will loop
    through the previous groupa GP1, GP2, and GP3, but we will not loop through 
    GP1, GP2, and GP3 when we process GC2, and GC3. We keep some "first group" flags
    to turn looping off.
    """

    if current_partition_number == 1:
        # There is always one group in the groups of partition 1. So group 1 and 
        # partition 1 are the same, i.e., have the same nodes.
        # If the nodes in this partition/group have parents then 
        # the parents are in this partition/group..
        # There is only one group in the first partition, and this 
        # group is also the first partition. Since there is only 
        # one group in this partition there are no edges between 
        # the groups in this first partition. If this is the only
        # group/partition in the DAG, then it has no edges to any
        # other group/partition.  That is, the DAG has one node qnd 
        # no edges.
        # In general, a partition may have many groups and a group in a 
        # partition may have edges to anoher group in the partition or
        # to a group in the next partition. So if group G is the 
        # last group processed in the DAG, G may have edges to other
        # groups in the same partition (but since it is in the last 
        # partition processed it has no edges to groups in other 
        # partitions.)

        #assert:
        if not len(groups_of_current_partition) == 1:
            logger.error("[Error]: Internal error: generate_DAG_info_incremental_groups"
                + " number of groups in first partition is not 1 it is "
                + str(len(groups_of_current_partition)))
        #assert:
        # Note: Group_next_state is inited to 1. as is current_partition_state
        if not current_partition_state == Group_next_state:
            logger.error("[Error]: Internal error:generate_DAG_info_incremental_groups"
                + " current_partition_state for first partition is not equal to"
                + " Group_next_state - both should be 1.")
          
        name_of_first_group_in_DAG = groups_of_current_partition[0]
        # a list of groups that have a fanout/fanin/collapse to this
        # group. (They "send" to this group which "receives".)
        senders = Group_receivers.get(name_of_first_group_in_DAG)
        # Note: the groups in a partition that starts a new connected component (which is 
        # the first partition collected on a call to BFS(), of which there 
        # may be many calls if the graph is not connected) are leaf
        # nodes and thus have no senders. This is true about partition 1 and
        # this is asserted by the caller (BFS()) of this method.
        
        # Group 1 is a leaf; so there is no previous partition that can 
        # send (its outputs as inputs) to the first group
        if not senders == None:
            logger.error("[Error]: Internal error: generate_DAG_info_incremental_groups"
                + " leaf node has non-None senders.")

        logger.info("generate_DAG_info_incremental_groups: generate empty state_info for first group "
            + name_of_first_group_in_DAG + " with Group_next_state: "  + str(Group_next_state))
        # record DAG information 
        # leaf task name
        Group_DAG_leaf_tasks.append(name_of_first_group_in_DAG)
        # leaf task state. Group_next_state is a global variable
        Group_DAG_leaf_task_start_states.append(Group_next_state)
        # leaf tasks have no input
        task_inputs = ()
        Group_DAG_leaf_task_inputs.append(task_inputs)

        # we will check that current_group_name is in leaf_tasks_of_groups
        # upon return to BFS() (when we see tht leaf tasks have been added to the DAG)

        fanouts = []
#rhc: clustering
        fanout_partition_group_sizes = []
        faninNBs = []
        fanins = []
        collapse = []
        fanin_sizes = []
        faninNB_sizes = []

        # generate the state for this partition/DAG task
        Group_DAG_map[Group_next_state] = state_info(name_of_first_group_in_DAG, fanouts, fanins, faninNBs, collapse, fanin_sizes, 
            faninNB_sizes, task_inputs,
            to_be_continued,
            # We do not know whether this first group will have fanout_fanin_faninNB_collapse_groups_are_ToBeContinued
            # that are incomplete until we process the 2nd partition, except if to_be_continued
            # is False in which case there are no more partitions and no fanout_fanin_faninNB_collapse_groups_are_ToBeContinued_are_ToBeContinued
            # that are incomplete. If to_be_continued is True then we set fanout_fanin_faninNB_collapse_groups_are_ToBeContinued_are_ToBeContinued
            # to True but we may change this value when we process partition 2.
            to_be_continued,
#rhc: clustering
            fanout_partition_group_sizes)

        Group_DAG_states[name_of_first_group_in_DAG] = Group_next_state

        # identify the function that will be used to execute this task
        if not use_shared_partitions_groups:
            # the partition of graph nodes for this task will be read 
            # from a file when the task is executed. 
            Group_DAG_tasks[name_of_first_group_in_DAG] = PageRank_Function_Driver
        else:
            # the partition is part of a shared array, so all of the nodes will 
            # for the pagerank computation will be stored in a shared array
            # that is accessed by worker threads or processes (not lambdas).
            # For worker processes, the shared array uses Python Shared Memory
            # from the mutiprocessing lib.
            # The shared array is essentially an array of structs
            if not use_struct_of_arrays_for_pagerank:
                # using struct of arrays for fast cache access, one array
                # for each Node member, e.g., array of IDs, array of pagerank values
                # array of previous values. 
                # Function to compute pagerank values when using struct of arrays
                Group_DAG_tasks[name_of_first_group_in_DAG] = PageRank_Function_Driver_Shared 
            else:
                # using a single array of Nodes
                # Function to compute pagerank values when using array of structs
                Group_DAG_tasks[name_of_first_group_in_DAG] = PageRank_Function_Driver_Shared_Fast  

        logger.trace("generate_DAG_info_incremental_groups: Group_DAG_map[current_partition_state]: " + str(Group_DAG_map[Group_next_state] ))

        # Note: setting version number and to_be_continued in generate_DAG_for_groups()
        # Note: setting number of tasks in in generate_DAG_for_groups()

        # For partitions, if the DAG is not yet complete, to_be_continued
        # parameter will be TRUE, and there is one incomplete partition 
        # in the just generated version of the DAG, which is the last parition.
        if to_be_continued:
            # len(groups_of_current_partition) must be 1 fo the first group 
            # as asserted above.
            number_of_incomplete_tasks = len(groups_of_current_partition)
        else:
            number_of_incomplete_tasks = 0
        DAG_info = generate_DAG_for_groups(to_be_continued,number_of_incomplete_tasks)

        logger.trace("generate_DAG_info_incremental_groups: returning from generate_DAG_info_incremental_groups for"
            + " group " + str(name_of_first_group_in_DAG))
        
        # This will be set to 2
        Group_next_state += 1
        
        return DAG_info

    else:
        # Flag to indicate whether we are processing the first group_name of the groups in the 
        # previous partition. Used and reset below.
        #first_previous_group = True

#rhc: undo 1
# Possible fix:
# - Take these out of original place
# - get rid of sender_set_for_group_name
# - get rid of second group_name loop since we only need one iteration now?
# - comment on changes: we need stat_info of al groups in previous partition and curret
#   partition if not to be continued since we will be setting fina lstate info 
# - for groups in current partition too. (Though they have empty state info when we get
#   then for final set so we could just use an empty state info if not in there,
#   but we may as well clean this up.) 
#   group with no senders is a specail cse in tht we know no
#   group in previous partition or current group sends to it
#   so don;t have to add it to groups_to_consider
# - commented out the assert #if len(senders) == 0:
# 
# Do we need the two cases? Maybe but perhaps put the edge generation in a method that both can call?
        
        for group_name in groups_of_current_partition:
            senders = Group_receivers.get(group_name) 
            # The two cases differ in that if senders is None then 
            # no other group sends to group_name, so it is a leaf 
            # task. Thus, we we add it to the collection of leaf tasks,
            # set its task_inputs to empty (). For non-leaf tasks,
            # we generate ots inputs using qualified names, e.g.,
            # for PR2_1 its input is from PR1_1 so we use "PR1_1-PR2_!"
            # since PR1_1 has other outputs to other tasks and it only
            # sends its output for PR2_1 to task PR2_1. So the fanout
            # outputs can be different, unlke for Dask which sends all 
            # of its outputs to all of its fanout/fanin tasks.
            # So: If PRX_1 has snders PRY_1 and PRZ_1, then its inputs
            # are "PRY_1-PRX_1" and "PRZ_1-PRX-1". These inputs strings
            # are used to access the data_dict during execution, which 
            # is a map which would contain keys "PRY_1-PRX_1" and "PRZ_1-PRX-1"
            # and their mapped values. The values for PRX_1 will be obtained from 
            # the map using keys "PRY_1-PRX_1" and "PRZ_1-PRX-1" and used
            # to execute task PRX_1.
            if (senders == None):
                # This is a leaf group since it gets no inputs from any other groups.
                # This means group_name is the only group in groups_of_current_partition.
                # (So no group in any other partition or in this current partition outputs
                # to group_name.) 
                # That is, all leaf groups are detected as the first
                # group identified on a call to bfs(). This group is 
                # also a partition, i.e., the first partition hs a single group.
                # There may be many calls to bfs(), each call starts a new 
                # connected component. For example, in the whiteboard example,
                # with addtional components 4-->5 and 6-->7, the first group
                # partition in the DAG is PR1_1, it is the start of the first
                # connected component (CC) searched by bfs(). The second CC
                # starts with a group/partition containing 4, and ends with 
                # group/partition conaining 5, the second CC starts with 
                # the group/partition containing 6, and ends with the group
                # /partition containing 7. bfs() is called each time we start
                # the search of a CC.
                # assert:
                if len(groups_of_current_partition) > 1:
                    logger.error("[Error]: Internal error: generate_DAG_info_incremental_groups:"
                        + " start of new connected component (i.e., called BFS()) but there is more than one group.")

                # This is not group 1. But it is a leaf group, which means
                # it was the first group generated by some call to BFS(), i.e., 
                # it is the start of a new connected component. This also means there
                # are no fanouts/fanins/faninNBs/collapses from any group in the previous
                # partition to this group or from any group in this current
                # partition to this group (since this group is the only group
                # in the current partition.) Note however, that we are computing 
                # the fanouts/fanins/collapse of the groups in the previous 
                # partition and the groups in that partition may have fanouts/
                # fanins to each other (left to right), so we still need to 
                # generate these edges in the DAG.
                # 
                # Note: when we call BFS() we will collect a single partition/group
                # that is the start of a new connected component. Thus group_name is the 
                # only group in groups_of_current_partition.

                # Since this is a leaf group (it has no predecessor) we will need to add 
                # this partition/group to the work queue or start a new lambda for it (
                # like the DAG_executor_driver does. (Note that since this partition/group has
                # no predecessor, no worker or lambda can enable this task via a fanout, collapse,
                # or fanin, thus we must add this partition/group as work explicitly ourselves.)
                # This is done when BFS deposits a new DAG, i.e., in method deposit.
            
                # Mark this partition/group as a leaf task/group. If any more of these leaf task 
                # partitions/groups are found (by later calls to BFS()) they will accumulate 
                # in these lists. BFS() uses these lists to identify leaf tasks - when BFS generates an 
                # incremental DAG_info, it adds work to the work queue or starts a
                # lambda for each leaf task that is not the very first partition/group in the 
                # DAG. The first partition/group is always a leaf task and it is handled by the 
                # DAG_executor_driver.
                
                
                logger.info("generate_DAG_info_incremental_groups: start of new connected component is group "
                    + group_name)
                logger.info("generate_DAG_info_incremental_groups: generate empty state_info for leaf task"
                    + group_name + " with Group_next_state: "  + str(Group_next_state))

                # save leaf task. Group_next_state is a global variable
                Group_DAG_leaf_tasks.append(group_name)
                Group_DAG_leaf_task_start_states.append(Group_next_state)
                
                # compute task inputs == task input is same as for leaf task group 1 above - empty
                task_inputs = ()
                Group_DAG_leaf_task_inputs.append(task_inputs)

                fanouts = []
#rhc: clustering
                fanout_partition_group_sizes = []
                faninNBs = []
                fanins = []
                collapse = []
                fanin_sizes = []
                faninNB_sizes = []

                # generate state_info for group group_name. Group_next_state is glob
                Group_DAG_map[Group_next_state] = state_info(group_name, fanouts, fanins, faninNBs, collapse, fanin_sizes, 
                    faninNB_sizes, task_inputs,
                    to_be_continued,
                    # We do not know whether this first group will have fanout_fanin_faninNB_collapse_groups_are_ToBeContinued_are_ToBeContinued
                    # that are incomplete until we process the 2nd partition, except if to_be_continued
                    # is False in which case there are no more partitions and no fanout_fanin_faninNB_collapse_groups_are_ToBeContinued_are_ToBeContinued
                    # that are incomplete. If to_be_continued is True then we set fanout_fanin_faninNB_collapse_groups_are_ToBeContinued_are_ToBeContinued
                    # to True but we may change this value when we process partition 2.
                    to_be_continued,
#rhc: clustering
                    fanout_partition_group_sizes)
                
                Group_DAG_states[group_name] = Group_next_state

                # identify the function that will be used to execute this task
                if not use_shared_partitions_groups:
                    Group_DAG_tasks[group_name] = PageRank_Function_Driver
                else:
                    if not use_struct_of_arrays_for_pagerank:
                        Group_DAG_tasks[group_name] = PageRank_Function_Driver_Shared 
                    else:
                        Group_DAG_tasks[group_name] = PageRank_Function_Driver_Shared_Fast  

                logger.info("generate_DAG_info_incremental_groups: state_info for current " + group_name)

            else:

                logger.info("generate_DAG_info_incremental_groups: generate empty state_info for "
                    + group_name + " with Group_next_state: "  + str(Group_next_state))
                
                #sender_set_for_group_name = Group_receivers.get(group_name)
                logger.info("senders, i.e., the groups that send to " + group_name + ":" + str(senders))

                # compute task inputs
                # Not a leaf group so it has inputs. Generate the names of the inputs.
                # These names are used to get the inputs from the data dictiionary that 
                # holds all outputs/inputs during DAG excution.
                # Create a new set from set sender_set_for_group_name. For 
                # each name in sender_set_for_group_name, qualify the name by
                # prexing it with "name-". Example: name in sender_set_for_group_name 
                # is "PR1_1" and group_name is "PR2_3" so the qualified name is 
                # "PR1_1-PR2_3". We will use "PR1_1-PR2_3" as a key in the data dictioary
                # to get the output PR1_1 produced for PR2_3. Recall that for pagerank
                # a task lie PR1_1 can have different outputs for its fanouts/fanin
                # tasks, so we use "PR1_1-PR2_1", "PR1_1-PR2_2" etc to denote PR1_1's
                # specific output for PR2_1 and PR2_2. 
                #
                # We use qualified names since the fanouts/faninNBs for a 
                # task in a pagerank DAG may all have diffent values. This
                # is unlike Dask DAGs in which all fanouts/faninNBs of a task
                # have the same value. We denote the different outputs
                # of a task A having, e.g., fanouts B and C as "A-B" and "A-C"
                # Note: Here we are calculating the tuple of task inputs, which 
                # is the set of tasks that send their outputs to this group. This 
                # set is Group_receivers.get(group_name).  Task name "T-X" is
                # used as a key in the data dictionary to get the output value 
                # of "T" sent to task "X"
                sender_set_for_group_name_with_qualified_names = set()
                # For each sender task "name" that sends output to group_name, the 
                # qualified name of the output is: name+"-"+group_name
                #for name in sender_set_for_group_name:
                for name in senders:
                    qualified_name = str(name) + "-" + str(group_name)
                    sender_set_for_group_name_with_qualified_names.add(qualified_name)
                # sender_set_for_senderX provides input for group_name
                task_inputs = tuple(sender_set_for_group_name_with_qualified_names)

                # generate empty state_info for group group_name. This will be filled in 
                # when we process the groups in the next partition collected. See below.
                # That is, this group (recall that we are iterating
                # through the groups of the current partition) is 
                # incomplete and we will complete it when we process
                # the (groups in the) next partition.
                fanouts = []
#rhc: clustering
                fanout_partition_group_sizes = []
                faninNBs = []
                fanins = []
                collapse = []
                fanin_sizes = []
                faninNB_sizes = []
                Group_DAG_map[Group_next_state] = state_info(group_name, fanouts, fanins, faninNBs, collapse, fanin_sizes, 
                    faninNB_sizes, task_inputs,
                    # to_be_continued parameter can be true or false
                    to_be_continued,
                    # We do not know whether this frist group will have fanout_fanin_faninNB_collapse_groups_partitions_are_ToBeContinued
                    # that are incomplete until we process the 2nd partition, except if to_be_continued
                    # is False in which case there are no more partitions and no fanout_fanin_faninNB_collapse_groups_partitions_are_ToBeContinued
                    # that are incomplete. If to_be_continued is True then we set fanout_fanin_faninNB_collapse_groups_partitions_are_ToBeContinued
                    # to True but we may change this value when we process partition 2.
                    to_be_continued,
#rhc: clustering
                    fanout_partition_group_sizes)
                
                Group_DAG_states[group_name] = Group_next_state

                # identify function used to execute this pagerank task (see comments above)
                if not use_shared_partitions_groups:
                    Group_DAG_tasks[group_name] = PageRank_Function_Driver
                else:
                    if not use_struct_of_arrays_for_pagerank:
                        Group_DAG_tasks[group_name] = PageRank_Function_Driver_Shared 
                    else:
                        Group_DAG_tasks[group_name] = PageRank_Function_Driver_Shared_Fast  

                logger.info("generate_DAG_info_incremental_groups: Group_DAG_map[Group_next_state]: " + str(Group_DAG_map[Group_next_state] ))

            Group_next_state += 1



        # This is not the first partition and it is not a leaf partition.
        # So current_partition_state is 2 or more (states start at 1)
        # Positions in groups_of_partitions start at 0.
        previous_partition_state = current_partition_state - 1
        groups_of_previous_partition = groups_of_partitions[previous_partition_state-1]
#rhc: Problem: If not to_be_Continued then there is no next partition 
# of groups to process, e.g., for th white board we have partition 3
# with groups 3_1, 3_2, and 3_3, where 3_1 has a faninB to 3_2.
# We won;t get a chance to compuet this faninNB uness we do it 
# now, i.e., can;t wait until next partition since there is no next
# partition .So if not to be continued, we need to look at 
# groups within partition.
# No? When current partition is 3, we look at groups n partition 2,
# which is fine for seeing the PR2_2L has a faninNB to PR3_2,
# but we fail to detect that PR3_1 has a faninNB to PR3_2. 
# This is because we only look at the groups in the previous
# partition, like for using partitions, but we should also "Add"
# groups in the current partition that are before group_name in 
# that list (assuming groups are added left to right in this list
# as the are detected.)
#rhc: ToDo: fixes bug
        
#rhc: undo 2
        # Note: if the current group/partition is a leaf group/partition (that is not the first
        # group in the DAG PR1_1) then it has no senders, i.e., no task sends its output
        # to it; it is the only group in its partititon; and if not to_be_continued then
        # it has no receivers, i.e., since it is the only group in the last partition in the 
        # DAG. So groups_of_current_partition is this single group, and when not to_be_continued
        # we add it to groups_to_consider. We will get groups that this group sends inputs to
        # and since this will be empty, we will not generate any edges for this group in the 
        # DAG, which is correct since it does not send any outputs to any other group.
        # This requires only the execution of one if-statement so we do not try to avoid adding this group.
        # Note that groups_to_consider are the groups for which we are identifying edges,
        # i.e., adding an edge for a group G in groups_to_consider to the groups that G
        # sends outputs to (which are in the same partition as G or are groups in the 
        # current partition). The edges always go from the previous groups to the 
        # current groups, or the edges go from left to right for the groups in a partition since
        # if node P is a parent of node C in the same partition, then P has an index in the partition (list) 
        # that is less than the index of C, and the edges go in the parent to child direction, i.e., left to right.
        # The general idea is that the parent nodes of a node C are always either in the same partition
        # (at a position with a smaller index than C (so "to the left of C in the list of nodes")) or are in the
        # prevous partition (but no other partition). This restricts the flow of parent values 
        # to: between one partition and the next partition, or between grooups in the same partition.
        # As opposed to one group can send to any other groups in the DAG, so we try to have communication of
        # parent values (say, between Lambdas executing pagerank tasks) that is "one group/task/lambda
        # to a few groups/tasks/lambdas" instead of "one group/task/lambda to many groups/tasks/lambdas".
        groups_to_consider = []
        if not to_be_continued:
            groups_to_consider += groups_of_previous_partition
            groups_to_consider += groups_of_current_partition
            #groups_of_previous_partition += groups_of_current_partition 
            # add current groups to groups_of_previous_partition so we will
            # ad edges to DAG for their outputs too.
        else:
            groups_to_consider += groups_of_previous_partition

        logger.trace("generate_DAG_info_incremental_groups: current_partition_state: " 
            + str(current_partition_state) + ", previous_partition_state: "
            + str(previous_partition_state))
        
        # Do this one time, i.e., there may be many groups in the current partition
        # and we are iterating through these groups. But all of these groups have the 
        # same previous paritition and we only need to process the groups in the
        # previous partition once, which we do here.
        # We are completing the state information for the groups in the 
        # previous partition. Those groups can send inputs to the groups in the 
        # current partition (which we are processing here) or to other
        # groups in their same partition, which is the previous partition.

        # Note that above we are setting the state_info for group_name:
        #   Group_DAG_map[Group_next_state] = state_info(group_name,...
        # and we need to do this for each group_name. So the logic is:
        #    for each group_name in the current group {
        #       set state info
        #       Do this one time in this loop {
        #          for each previous_group in the previous partition {
        #               compute the edges for previous_group
        #          }
        #       }
        #    }
        # Consider taking "Do this one time" out of the loop.
        #
        # Note: For group_name we set its state info above. This
        # state info is incomplete, i.e., we will not not know
        # which groups group_name outputs to until we process the 
        # next partition. But we are now processing the current
        # partition (i.e., the one containing group_name) so we 
        # have discovered all the edges from the groups in the 
        # previous partition to the groups in the current partition.
        # Also, a group in the previous partition can have abn edge 
        # to a group that is in the previous partition. (We could
        # see these edges whwn we processed the previous partition,
        # but we don't add them until we process the next partition,
        # which is the current_partition here. Note that if there 
        # is no next partition then we need to do something to 
        # cover the edges between the groups of the previous partition,
        # which we described above - add the groups of the current 
        # partiton to the groups of the previous partition.)

        #if first_previous_group:
        #    first_previous_group = False

        logger.trace("generate_DAG_info_incremental_groups: complete the state_info for previous groups: "
            + str(groups_of_previous_partition))
        # When we added these previous groups to the DAG we added them with empty
        # fanouts/fanins/faninNBs/collapse sets. Now that we collected the 
        # groups in the current partition, which is the next partition of groups 
        # collected, we can compute these sets for the groups in the previous
        # partition. Note: a group can only have a fanout/fanin/faninNB/collapse to 
        # a group in its same partition or to a group in the next partition. So 
        # when we collect the groups in (current) partition i, we know the behavior 
        # of the groups in (previous) partition i-1.

        # Note: From above, we described the logic for processing
        # the groups in the previous partition. When we process the groups
        # in the previous partition, we also need to process the groups
        # in the partition previous to the previous partition, which 
        # we refer to as previous_previous_group, But similar to the 
        # processing of previous groups, we only need to process the 
        # previous previous groups one time, not once for each previous
        # group. So the total logic is:
        # flag so we only do this for the first group of groups in previous previous partition
        #    A: for each group_name in the current group {
        #       set state info;
        #       Do this one time in this A loop {
        #          B: for each previous_group in the previous partition {
        #                compute the edges for previous_group and set its state_info;
        #                Do this one time in this B loop {
        #                   C: for each previous_previous group in the previous previous partition {
        #                          set the state info for previous_previous_group; 
        #                   }C
        #                }Do
        #          }B
        #       }Do
        #    }A
        #  Note: We do the one time code on the first iterations
        #  of the A and B loops, which is why we have first_itration 
        #  flags to identify the first iteration and turn off the 
        #  do one time code for the succeeding iterations. There is
        #  nothing special about the first iteration.
        # Consider taking the Do one time code out of the loops.
        #first_previous_previous_group = True
#rhc: undo 3
        for previous_group in groups_to_consider:
        #for previous_group in groups_of_previous_partition:
            # sink nodes, i.e., nodes that do not send any outputs to other nodes


# START
            # As commented above, we do not need this
            Group_sink_set = set()

            fanouts = []
#rhc: clustering
            fanout_partition_group_sizes = []
            fanins = []
            faninNBs = []
            collapse = []
            fanin_sizes = []
            faninNB_sizes = []

            # Get groups that previous_group sends outputs to. Recall that 
            # previous_group is a group in the previous partition. Group
            # previous_group may send outputs to a group in the previous 
            # partition, i.e., the same partition as group previous_group,
            # or a group in the current partition, or it may send no outputs at all.

            logger.trace("generate_DAG_info_incremental_groups: previous_group: " + previous_group)

            # get groups that previous group sends inputs to. These
            # groups "receive" inputs from the sender

            #receiver_set_for_previous_group = Group_senders[previous_group]
            receiver_set_for_previous_group = Group_senders.get(previous_group,[])

            # (Note: Not sure whether we can have a length 0 Group_senders list, 
            # for current_partition_name. That is, we only create a 
            # Group_senders set when we get the first sender.)
            # assert: no length 0 Group_senders lists
#rhc: 0 list for PR3_2, which is in current group; we have not processed partition 3 so there are
# no senders for it yet?
            
            #if len(receiver_set_for_previous_group) == 0:
            #    logger.error("[Error]: Internal Error: generate_DAG_info_incremental_groups:"
            #        + " group " + previous_group + " has a receiver_set_for_previous_group list with length 0.")
            #    logging.shutdown()
            #    os._exit(0)

            # for each group that receives an input from the previous_group
            for receiverY in receiver_set_for_previous_group:
                # Get the groups that receive inputs from receiverY.
                # Note that we know that the previous_group sends 
                # inputs to receiverY, but we need to know if there are
                # any other groups that send inputs to receiverY in order
                # to know whether receiverY is a task for a fanin/fanout/faniNB/collapse
                # of previous_group.

                # Here we check whether rceiverY is a sink, i.e., it does not
                # send inputs to any other group.
                receiver_set_for_receiverY = Group_senders.get(receiverY)
                if receiver_set_for_receiverY == None:  # None is default
                    # receiverY does not send any inputs to other groups
                    # so it is a sink.
                    # 
                    # For non-incremental generation, this receiverY will not show
                    # up in Group_senders so we will not process receiverY
                    # as part of the major loop for generating a DAG during
                    # non-incremental DAG generation, That means, after the
                    # major loop terminates, we look at the groups we added to
                    # Group_sink_set. For these groups, they have no 
                    # fanouts/fanins/faninNBs/collapses (since they 
                    # were never a sender and thus are not a key in 
                    # Group_senders) but they may have inputs. Thus
                    # we will generate inputs for these groups (like
                    # receiverY).
                    # Note: Such a receiver will have on inputs if it is
                    # the first group of a connected component and the
                    # only group of the connected component (so it sends inputs
                    # to no other groups)
                    # 
                    # As commented above, we do not need this
                    Group_sink_set.add(receiverY)

                # Get groups that send inputs to receiverY, this could be one 
                # or more groups (since we know that previous_group sends to receiverY)
                # Note: if other groups also send their inputs to receiverY,
                # then receiverY is a task of a fanin or faninNB, not a fanout
                # since a fanout task receives inputs from only one group.
                # Note: if no other group, i.e., only previous_group sends 
                # inputs to receiverY, then receiverY is a task of a fanout or a collpase
                # of previous_group. If previous_group only sends inputs to
                # reeiverY and no other group, then receiverY is a collapse
                # for previous_group; otherwise, receiverY is a fanout of
                # previous_group. Previous_group may have other fanouts or 
                # faninNBs, no no fanins.
                sender_set_for_receiverY = Group_receivers[receiverY]
                # number of groups that send inputs to receiverY
                length_of_sender_set_for_receiverY = len(sender_set_for_receiverY) # must be > 0, from above
                # number of groups that receive input from previous_group
                length_of_receiver_set_for_previous_group = len(receiver_set_for_previous_group)

                if length_of_sender_set_for_receiverY == 1:
                    # receiverY receives input from only one group; so receiverY must be a
                    # collapse or fanout (as fanins and faninNB tasks receive two or more inputs.)
                    if length_of_receiver_set_for_previous_group == 1:
                        # only one group, previous_group, sends outputs to receiverY and this sending 
                        # group previous_group only sends inputs to one group (receiverY), so collapse 
                        # receiverY, i.e., previous_group becomes receiverY via a collapse.
                        logger.trace("sender " + previous_group + " --> " + receiverY + " : Collapse")
                        if not receiverY in Group_all_collapse_task_names:
                            Group_all_collapse_task_names.append(receiverY)
                        else:
                            logger.error("[Error]: Internal Error: generate_DAG_info_incremental_groups:"
                                + "group " + receiverY + " is in the collapse set of two groups.")
                        # we are generating the sets of collapse/fanin/fanout/faninNB
                        # of previous_group
                        collapse.append(receiverY)
                    else:
                        # only one task, group_name, sends output to receiverY and this sending 
                        # group sends to other roups too, so group_name does a fanout 
                        # to group receiverY.  
                        logger.trace("sender " + previous_group + " --> " + receiverY + " : Fanout")
                        if not receiverY in Group_all_fanout_task_names:
                            Group_all_fanout_task_names.append(receiverY)
                        # we are generating the sets of collapse/fanin/fanout/faninNB
                        # of previous_group
                        fanouts.append(receiverY)
#rhc: clustering
                        if enable_runtime_task_clustering:
                            num_shadow_nodes = groups_num_shadow_nodes_map[receiverY]
                            logger.trace("number of shadow nodes for " + receiverY + " is " + str(num_shadow_nodes)) 
                            fanout_partition_group_sizes.append(num_shadow_nodes)
                            logger.trace("fanout_partition_group_sizes after append: " + str(fanout_partition_group_sizes))

                else:
                    # previous_group has fanin or fannNB to group receiverY since 
                    # receiverY receives inputs from multiple groups.
                    isFaninNB = False
                    # Recall sender_set_for_receiverY is the groups that send inputs
                    # to receiverY, this could be one or more groups (since we know previous_group 
                    # sends output to receiverY)
                    #
                    # senderZ sends an output to receiverY
                    for senderZ in sender_set_for_receiverY:
                        # groups to which senderz sends an output
                        receiver_set_for_senderZ = Group_senders[senderZ]
                        if len(receiver_set_for_senderZ) > 1:
                            # if any group G sends output to reciverY and any other group(s)
                            # then receiverY must be a faninNB task since G cannot 
                            # become receiverY. (Even if some other group could become receiverY
                            # G cannot, so we must use a faninNB for receiverY here instead of a fanin.)
                            isFaninNB = True
                            break

                    if isFaninNB:
                        logger.trace("group " + previous_group + " --> " + receiverY + " : FaninNB")
                        if not receiverY in Group_all_faninNB_task_names:
                            Group_all_faninNB_task_names.append(receiverY)
                            Group_all_faninNB_sizes.append(length_of_sender_set_for_receiverY)
                        logger.trace ("after Group_all_faninNBs_sizes append: " + str(Group_all_faninNB_sizes))
                        logger.trace ("faninNBs append: " + receiverY)
                        faninNBs.append(receiverY)
                        faninNB_sizes.append(length_of_sender_set_for_receiverY)
                    else:
                        # all tasks that send inputs to receiverY don't send inputs to any other
                        # task/grup, so receiverY is a fanin task.
                        logger.trace("group " + previous_group + " --> " + receiverY + " : Fanin")
                        if not receiverY in Group_all_fanin_task_names:
                            Group_all_fanin_task_names.append(receiverY)
                            Group_all_fanin_sizes.append(length_of_sender_set_for_receiverY)
                        fanins.append(receiverY)
                        fanin_sizes.append(length_of_sender_set_for_receiverY)

            # We just calculated the fanouts/fanins/faninNBs/collapses sets of 
            # previous_group, so get the state info of this previous
            # group and change the state info by adding these sets.
            #
            # get the state (number) of previous group
            previous_group_state = Group_DAG_states[previous_group]
            # get the state_info of previous group
            state_info_of_previous_group = Group_DAG_map[previous_group_state]

            logger.trace("before update to TBC and fanout_fanin_faninNB_collapse_groups_are_ToBeContinued_are_ToBeContinued"
                + " for previous_group " + previous_group + " state_info_of_previous_group: " + str(state_info_of_previous_group))
            if state_info_of_previous_group == None:
                logger.error("[Error] Internal Error: generate_DAG_info_incremental_groups: state_info_of_previous_group: "
                    + "state_info_of_previous_group is None.")
                logger.error("DAG_map:")
                for key, value in Group_DAG_map.items():
                    logger.trace(str(key) + ' : ' + str(value))
                logging.shutdown()
                os._exit(0)

            # The fanouts/fanins/faninNBs/collapses in state_info are 
            # empty so just add the fanouts/fanins/faninNBs/collapses that
            # we just calculated. Note: we are modifying the info in the
            # (dictionary of information for the) DAG that is being 
            # constructed incrementally. 
            fanouts_of_previous_state = state_info_of_previous_group.fanouts
            fanouts_of_previous_state += fanouts

            fanout_partition_group_sizes_of_previous_state = state_info_of_previous_group.fanout_partition_group_sizes
            fanout_partition_group_sizes_of_previous_state += fanout_partition_group_sizes                      

            fanins_of_previous_state = state_info_of_previous_group.fanins
            fanins_of_previous_state += fanins

            faninNBs_of_previous_state = state_info_of_previous_group.faninNBs
            faninNBs_of_previous_state += faninNBs

            collapse_of_previous_state = state_info_of_previous_group.collapse
            collapse_of_previous_state += collapse

            fanin_sizes_of_previous_state = state_info_of_previous_group.fanin_sizes
            fanin_sizes_of_previous_state += fanin_sizes

            faninNB_sizes_of_previous_state = state_info_of_previous_group.faninNB_sizes
            faninNB_sizes_of_previous_state += faninNB_sizes
# END
            # the previous group was constructed as to_be_continued. Now
            # that we have completed previous_group it is no longer
            # to_be_continued. So in the next DAG that is generated,
            # previous_group is not to_be_continued and so can be 
            # executed.
            state_info_of_previous_group.ToBeContinued = False
            # if the current partition is to_be_continued then previous_group has incomplete
            # groups so we set fanout_fanin_faninNB_collapse_groups_partitions_are_ToBeContinued of the previous
            # groups to True; otherwise, we set fanout_fanin_faninNB_collapse_groups_partitions_are_ToBeContinued to False.
            # Note: state_info_of_previous_group.ToBeContinued = False inicates that the
            # previous groups are not to be continued, while
            # state_info_of_previous_group.fanout_fanin_faninNB_collapse_groups_partitions_are_ToBeContinued indicates
            # whether the previous groups have fanout_fanin_faninNB_collapse_groups_partitions_are_ToBeContinued 
            # that are to be continued, i.e., the fanout_fanin_faninNB_collapse are 
            # to groups in this current partition and whether these groups in the current
            # partiton are to be continued is indicated by parameter to_be_continued.
            # (When bfs() calls this method it may determine that some of the graph
            # nodes have not yet been assigned to any partition so the DAG is
            # still incomplete and thus to_be_continued = True )
            state_info_of_previous_group.fanout_fanin_faninNB_collapse_groups_partitions_are_ToBeContinued = to_be_continued

            # Say that the current partition is C , which has a 
            # previous partition B which has a previous partition A.
            # In a previous DAG, suppose A is incomplete. When we process
            # B, we set A to complete (i.e., to_be_continued for A is False)
            # and B is set to incomplete (i.e., to_be_continued of B is True.)
            # We also set fanout_fanin_faninNB_collapse_groups_partitions_are_ToBeContinued
            # of A to True, to indicate that A has fanins/fanouts/faninNBs/collapses
            # to incomplete groups (of B). When we process C, we can set B to complete
            # and C to incomplete but we can also reset fanout_fanin_faninNB_collapse_groups_partitions_are_ToBeContinued
            # to False since B is complete so all of A's fanins/fanouts/faninNBs/collpases
            # are to complete groups. That means if C is group_name, then B
            # is a previous_group, and C is a previous_previous_group.
            #
            # For the previous_group (e.g., B), we need to reset a flag 
            # for its previous groups, hence "previous_previous"
            # but we only need to do this once. That is, the current
            # group group_name (e.g., A) may have many previous_groups, and these
            # previous groups may have many previous_groups. However two
            # previous_groups of A, say, B1 and B2 have the same previous_groups, 
            # which are the previous previous groups of A, so when we reset
            # the previous groups of B1 we are also resetting the previous
            # groups of B2. So do this resetting of the previous groups 
            # of B1 and B2 (which are the previous previous groups of A) for
            # only one of B1 or B2. In our case, we always choose the first group
            """
            # in the list of groups.
            if first_previous_previous_group:
                first_previous_previous_group = False
                if current_partition_number > 2:
                    previous_previous_partition_state = previous_partition_state - 1
                    groups_of_previous_previous_partition = groups_of_partitions[previous_previous_partition_state-1]
                    for previous_previous_group in groups_of_previous_previous_partition:
                        state_of_previous_previous_group = Group_DAG_states[previous_previous_group]
                        state_info_of_previous_previous_group = Group_DAG_map[state_of_previous_previous_group]
                        state_info_of_previous_previous_group.fanout_fanin_faninNB_collapse_groups_partitions_are_ToBeContinued = False
                        
                        logger.trace("The state_info_of_previous_previous_group for group " 
                            + previous_previous_group + " after update fanout_fanin_faninNB_collapse_groups_partitions_are_ToBeContinued is: " 
                            + str(state_info_of_previous_previous_group))
            """

            logger.trace("after update to TBC and fanout_fanin_faninNB_collapse_groups_are_ToBeContinued_are_ToBeContinued"
                + " for previous_group " + previous_group + " state_info_of_previous_group: " 
                + str(state_info_of_previous_group))

        # Say that the current partition is C , which has a 
        # previous partition B which has a previous partition A.
        # In a previous DAG, suppose A is incomplete. When we process
        # B, we set A to complete (i.e., to_be_continued for A is False)
        # and B is set to incomplete (i.e., to_be_continued of B is True.)
        # We also set fanout_fanin_faninNB_collapse_groups_partitions_are_ToBeContinued
        # of A to True, to indicate that A has fanins/fanouts/faninNBs/collapses
        # to incomplete groups (of B). When we process C, we can set B to complete
        # and C to incomplete but we can also reset fanout_fanin_faninNB_collapse_groups_partitions_are_ToBeContinued
        # to False since B is complete so all of A's fanins/fanouts/faninNBs/collpases
        # are to complete groups. That means if C is group_name, then B
        # is a previous_group, and C is a previous_previous_group.
        #
        # For the previous_group (e.g., B), we need to reset a flag 
        # for its previous groups, hence "previous_previous"
        # but we only need to do this once. That is, the current
        # group group_name (e.g., A) may have many previous_groups, and these
        # previous groups may have many previous_groups. However two
        # previous_groups of A, say, B1 and B2 have the same previous_groups, 
        # which are the previous previous groups of A, so when we reset
        # the previous groups of B1 we are also resetting the previous
        # groups of B2. So do this resetting of the previous groups 
        # of B1 and B2 (which are the previous previous groups of A) for
        # only one of B1 or B2. In our case, we always choose the first group
        # in the list of groups.
        #if first_previous_previous_group:
        #    first_previous_previous_group = False
        if current_partition_number > 2:
            previous_previous_partition_state = previous_partition_state - 1
            groups_of_previous_previous_partition = groups_of_partitions[previous_previous_partition_state-1]
            for previous_previous_group in groups_of_previous_previous_partition:
                state_of_previous_previous_group = Group_DAG_states[previous_previous_group]
                state_info_of_previous_previous_group = Group_DAG_map[state_of_previous_previous_group]
                state_info_of_previous_previous_group.fanout_fanin_faninNB_collapse_groups_partitions_are_ToBeContinued = False
                
                logger.trace("The state_info_of_previous_previous_group for group " 
                    + previous_previous_group + " after update fanout_fanin_faninNB_collapse_groups_partitions_are_ToBeContinued is: " 
                    + str(state_info_of_previous_previous_group))     
        """
        Note: We handle the shared state_info objects for all the groups
        at the end of the group loop below.
        """

        logger.trace("generate_DAG_info_incremental_groups: generate_DAG_info_incremental_groups for"
            + " group " + str(group_name))

        # Generate the new DAG_info
        if to_be_continued:
            number_of_incomplete_tasks = len(groups_of_current_partition)
        else:
            number_of_incomplete_tasks = 0               
        DAG_info = generate_DAG_for_groups(to_be_continued,number_of_incomplete_tasks)

        # We are adding state_info objects for the groups of the current
        # partition to the DAG as incmplete (to_be_continued). They will 
        # be accessed (read) by the DAG_executor and we cannot modify them 
        # during execution. However, when we process
        # the next partition, these incomplete groups that we are adding
        # to the DAG now, will need to be modified as we will generate their
        # fanin/fanout/faninNB/collase sets. So here we do not 

        #STOP
        #

        # modify these previous state_info objects; instead, we create
        # a deep copy of these state_info objects so that the DAG_executor
        # and the DAG we are incrementally generating access different 
        # (deep) copies. This means when we modify the copy in the ongoing
        # incremental DAG, we are not modifying the copy given to the 
        # ADG_executor.  Thus, the DAG_executor and
        # the DAG_generator do not share state_info objects so there
        # is no need to synchronize their access to state_info objects.
        # The other objects in DAG_info that are accessed by
        # DAG_executor and DAG_generator are immutable, so that when
        # the DAG_generator writes one of these objects it is generating
        # a new reference that is different from the reference in the ADG_info
        # that the DAG_executor references, e.g., for all Booleans. That means
        # these other ojects, which are only read by DAG_executor and are 
        # written be DAG_generator, are not really being shared. Funny.
        if to_be_continued:
            # Make deep copies of the state_info objects of the current groups
            #
            # Example: Next partition's first group is assigned Group_next_state of 2
            # and len(groups_of_current_partition) is 3. Then we will process
            # three groups and assign them states 2, 3, and 4. Note that
            # after the last group is processed, Group_next_state is 5, not 4.
            # so start_of_incomplete_states = 5 - 3 = 2. Then
            # range(start_of_incomplete_states,Group_next_state) is (2,5)
            # where 2 is inclusive and 5 is exclusive.
            logger.info("Group_next_state: " + str(Group_next_state) 
                + " len(groups_of_current_partition): " + str(len(groups_of_current_partition)))
            start_of_incomplete_states = Group_next_state - len(groups_of_current_partition)
            logger.info("start_of_incomplete_states: " + str(start_of_incomplete_states))

            for state in range(start_of_incomplete_states,Group_next_state): #range(inclusive,exclusive)
                DAG_info_DAG_map = DAG_info.get_DAG_map()

                # The DAG_info object is shared between this DAG_info generator
                # and the DAG_executor, i.e., we execute the DAG generated so far
                # while we generate the next incremental DAGs. The current 
                # state is part of the DAG given to the DAG_executor and we 
                # will modify the current state when we generate the next DAG.
                # (We modify the collapse list and the toBeContiued  of the state.)
                # So we do not share the current state object, that is the DAG_map in
                # the DAG_info given to the DAG_executor has a state_info reference
                # this is different from the reference in the DAG_info_DAG_map of 
                # the ongoing incremental DAG.
                # 
                # Get the state_info from the DAG_map
                logger.info("state is: " + str(state))
                state_info_of_current_group_state = DAG_info_DAG_map[state]

                # Note: in DAG_info __init__:
                """
                if not use_incremental_DAG_generation:
                    self.DAG_map = DAG_info_dictionary["DAG_map"]
                else:
                    # Q: this is the same as DAG_info_dictionary["DAG_map"].copy()?
                    self.DAG_map = copy.copy(DAG_info_dictionary["DAG_map"])
                # where:
                old_Dict = {'name': 'Bob', 'age': 25}
                new_Dict = old_Dict.copy()
                new_Dict['name'] = 'xx'
                print(old_Dict)
                # Prints {'age': 25, 'name': 'Bob'}
                print(new_Dict)
                # Prints {'age': 25, 'name': 'xx'}
                """

                # Note: the only parts of the states that are changed 
                # for partitions are the collapse list in the state_info and the 
                # TBC boolean. Yet we deepcopy the entire state_info object. 
                # But all other parts of the state info are empty for partitions 
                # (fanouts, fanins, aninNBs, etc) except for the pagerank function.
                # Note: Each state has a reference to the Python function that
                # will excute the task. This is how Dask does it - each task
                # has a reference to its function. For pagernk, we will use
                # the same function for all the pagerank tasks. There can be 
                # three different functions, but we could identify this 
                # function when we excute the task, instead of doing it above
                # and saving this same function in the DAG for each task,
                # which wastes space.

                # make a deep copy of this state_info object, which is in the DAG_info 
                # given to the DAG_executor.
                copy_of_state_info_of_current_group_state = copy.deepcopy(state_info_of_current_group_state)

                # Give the deep copy to the DAG_map (in the DAG_info) given to the 
                # DAG_executor. Now the DAG_executor and the DAG_generator will be 
                # using different state_info objects. That is, we are maintaining
                # Group_DAG_map = {} as part of the ongoing incremental DAG generation.
                # This is used to make the DAG_info object that is gven to the 
                # DAG_executor. We then get the DAG_info_DAG_map of this DAG_info
                # object:
                #   DAG_info_DAG_map = DAG_info.get_DAG_map()
                # and get the state_info object:
                #   state_info_of_current_group_state = DAG_info_DAG_map[state]
                # and make a deep copy of this state_info object:
                #   copy_of_state_info_of_current_group_state = copy.deepcopy(state_info_of_current_group_state)
                # and put this deep copy in DAG_info_DAG_ma which is part of the DAG_info 
                # object given to the DAG_executor.
                DAG_info_DAG_map[state] = copy_of_state_info_of_current_group_state

                # This code was used to test the deep copy - modify the state info
                # of the generator and make sure this modification does 
                # not show up in the state_info object given to the DAG_executor.
                """
                # modify the fanin state info maintained by the generator.
                Group_DAG_map[Group_next_state].fanins.append("goo")

                # display DAG_executor's state_info objects
                logger.trace("address DAG_info_DAG_map: " + str(hex(id(DAG_info_DAG_map))))
                logger.trace("generate_DAG_info_incremental_groups: DAG_info_DAG_map after state_info copy:")
                for key, value in DAG_info_DAG_map.items():
                    logger.trace(str(key) + ' : ' + str(value) + " addr value: " + str(hex(id(value))))

                # display generator's state_info objects
                logger.trace("address Group_DAG_map: " + str(hex(id(Group_DAG_map))))
                logger.trace("generate_DAG_info_incremental_groups: Group_DAG_map:")
                for key, value in Group_DAG_map.items():
                    logger.trace(str(key) + ' : ' + str(value) + " addr value: " + str(hex(id(value))))

                # fanin values should be different for current_state
                # one with "goo" and the other empty

                # undo the modification to the generator's state_info
                Group_DAG_map[Group_next_state].fanins.clear()

                # display generator's state_info objects
                logger.trace("generate_DAG_info_incremental_groups: DAG_info_DAG_map after clear:")
                for key, value in DAG_info_DAG_map.items():
                    logger.trace(str(key) + ' : ' + str(value))

                # display DAG_executor's state_info ojects
                logger.trace("generate_DAG_info_incremental_groups: Group_next_state:")
                for key, value in Group_next_state_DAG_map.items():
                    logger.trace(str(key) + ' : ' + str(value))

                # fanin values should be the same for current_state (empty)

                # logging.shutdown()
                # os._exit(0)
            """ 
                
    logger.trace("generate_DAG_info_incremental_groups: returning from generate_DAG_info_incremental_groups for"
        + " group " + str(group_name))
    
    # To stop after DAG is completely generated, whcih is combined with 
    # a sleep at the start of the DAG_executor_driver_Invoker_Thread 
    # so that DAG excution does not start before we get here and exit,
    #def DAG_executor_driver_Invoker_Thread():
    #time.sleep(3)
    #run()
    #if DAG_info.get_DAG_info_is_complete():
    #    logging.shutdown()
    #    os._exit(0)

    return DAG_info