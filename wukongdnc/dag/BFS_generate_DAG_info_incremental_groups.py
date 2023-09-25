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

from .BFS_generate_DAG_info import Group_senders, Group_receivers
from .BFS_generate_DAG_info import leaf_tasks_of_groups_incremental

# Note: avoiding circular imports:
# https://stackoverflow.com/questions/744373/what-happens-when-using-mutual-or-circular-cyclic-imports
#from . import BFS


logger = logging.getLogger(__name__)

logger.setLevel(logging.INFO)
#logger.setLevel(logging.INFO)
formatter = logging.Formatter('[%(asctime)s] [%(module)s] [%(processName)s] [%(threadName)s]: %(message)s')
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
#ch.setLevel(logging.INFO)
ch.setFormatter(formatter)
logger.addHandler(ch)


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

#rhc: ToDo: Not using for groups, we have to get all the groups
# of previous partition and iterate through them?

## Saving current_partition_name as previous_partition_name at the 
## end. We cannot just subtract one, e.g. PR3_1 becomes PR2_1 since
## the name of partition 2 might actually be PR2_1L, so we need to 
## save the actual name "PR2_1L" and retrive it when we process PR3_1
#Group_DAG_previous_partition_name = "PR1_1"

# number of tasks in the DAG
Group_DAG_number_of_tasks = 0
# the tasks in the last partition of a generated DAG may be incomplete,
# which means we cannot execute these tasks until the next DAG is 
# incrementally published.
Group_DAG_number_of_incomplete_tasks = 0

# used to generate IDs
Group_next_state = 1

# Called by generate_DAG_info_incremental_partitions below to generate 
# the DAG_info object.= when we are using partitions.
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
    # save the actual name "PR2_1L" and retrive it when we process PR3_1
    global Group_DAG_previous_partition_name
    global Group_DAG_number_of_tasks
    global Group_DAG_number_of_incomplete_tasks

    # used for debugging
    show_generated_DAG_info = True

    """
    #rhc: ToDo: copy.copy vs copy.deepcopy(): Need a copy
    # but the only read-write shared object is the state_info of
    # the previous state, for which we either change only the TBC or we 
    # change the TBC and the collapse.

        # add a collapse for this current partitio to the previous state
        previous_state = current_partition_state - 1
        state_info_previous_state = Partition_DAG_map[previous_state]
        Partition_all_collapse_task_names.append(current_partition_name)
        collapse_of_previous_state = state_info_previous_state.collapse
        collapse_of_previous_state.append(current_partition_name)

        # previous partition is now complete
        state_info_previous_state.ToBeContinued = False

    """
    
    # we construct a dictionary of DAG information 
    logger.info("")
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
    # incomplete. When partition i is added to the DAG, it is incomplete
    # unless it is the last partition in the DAG). It becomes complete
    # when we add partition i+1 to the DAG. (So partition i needs information
    # that is generated when we create partition i+1. The  
    # nodes of partition i can ony have children that are in partition 
    # i or partition i+1. We need to know partition i's children
    # in order for partition i to be complete. Partition i's childen
    # are discovered while generating partition i+1) 
    Group_DAG_is_complete = not to_be_continued # to_be_continued is a parameter
        # number of tasks in the current incremental DAG, including the
    # incomplete last partition, if any.
    Group_DAG_number_of_tasks = len(Group_DAG_tasks)
    # For partitions, this is at most 1. When we are generating a DAG
    # of groups, there may be many groups in the incomplete last
    # partition and they will all be considered to be incomplete.
    Group_DAG_number_of_incomplete_tasks = number_of_incomplete_tasks
    DAG_info_dictionary["DAG_version_number"] = Group_DAG_version_number
    DAG_info_dictionary["DAG_is_complete"] = Group_DAG_is_complete
    DAG_info_dictionary["DAG_number_of_tasks"] = Group_DAG_number_of_tasks
    DAG_info_dictionary["DAG_number_of_incomplete_tasks"] = Group_DAG_number_of_incomplete_tasks

#rhc: Note: we are saving all the incemental DAG_info files for debugging but 
# we probably want to turn this off otherwise.

    # filename is based on version number - Note: for partition, say 3, we
    # have output the DAG_info with partitions 1 and 2 as version 1 so 
    # the DAG_info for partition 3 will have partitions 1, 2, and 3 and will
    # be version 2 but named "DAG_info_incremental_Partition_3"
    file_name_incremental = "./DAG_info_incremental_Group_" + str(Group_DAG_version_number) + ".pickle"
    with open(file_name_incremental, 'wb') as handle:
        cloudpickle.dump(DAG_info_dictionary, handle) #, protocol=pickle.HIGHEST_PROTOCOL)  

    num_fanins = len(Group_all_fanin_task_names)
    num_fanouts = len(Group_all_fanout_task_names)
    num_faninNBs = len(Group_all_faninNB_task_names)
    num_collapse = len(Group_all_collapse_task_names)

    # for debugging
    if show_generated_DAG_info:
        logger.info("DAG_map:")
        for key, value in Group_DAG_map.items():
            logger.info(str(key) + ' : ' + str(value))
        logger.info("")
        logger.info("states:")        
        for key, value in Group_DAG_states.items():
            logger.info(str(key) + ' : ' + str(value))
        logger.info("")
        logger.info("num_fanins:" + str(num_fanins) + " num_fanouts:" + str(num_fanouts) + " num_faninNBs:"
        + str(num_faninNBs) + " num_collapse:" + str(num_collapse))
        logger.info("")  
        logger.info("all_fanout_task_names:")
        for name in Group_all_fanout_task_names:
            logger.info(name)
        logger.info("all_fanin_task_names:")
        for name in Group_all_fanin_task_names :
            logger.info(name)
        logger.info("all_fanin_sizes:")
        for s in Group_all_fanin_sizes :
            logger.info(s)
        logger.info("all_faninNB_task_names:")
        for name in Group_all_faninNB_task_names:
            logger.info(name)
        logger.info("all_faninNB_sizes:")
        for s in Group_all_faninNB_sizes:
            logger.info(s)
        logger.info("all_collapse_task_names:")
        for name in Group_all_collapse_task_names:
            logger.info(name)
        logger.info("")
        logger.info("leaf task start states:")
        for start_state in Group_DAG_leaf_task_start_states:
            logger.info(start_state)
        logger.info("")
        logger.info("DAG_tasks:")
        for key, value in Group_DAG_tasks.items():
            logger.info(str(key) + ' : ' + str(value))
        logger.info("")
        logger.info("DAG_leaf_tasks:")
        for task_name in Group_DAG_leaf_tasks:
            logger.info(task_name)
        logger.info("")
        logger.info("DAG_leaf_task_inputs:")
        for inp in Group_DAG_leaf_task_inputs:
            logger.info(inp)
        logger.info("")
        logger.info("DAG_version_number:")
        logger.info(Group_DAG_version_number)
        logger.info("")
        logger.info("DAG_is_complete:")
        logger.info(Group_DAG_is_complete)
        logger.info("")
        logger.info("DAG_number_of_tasks:")
        logger.info(Group_DAG_number_of_tasks)
        logger.info("")
        logger.info("DAG_number_of_incomplete_tasks:")
        logger.info(Group_DAG_number_of_incomplete_tasks)
        logger.info("")

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

        logger.info("")
        logger.info("DAG_info Group after read:")
        output_DAG = True
        if output_DAG:
            # FYI:
            logger.info("DAG_map:")
            for key, value in DAG_map.items():
                logger.info(str(key) + ' : ' + str(value))
                #logger.info(key)
                #logger.info(value)
            logger.info("  ")
            logger.info("DAG states:")      
            for key, value in DAG_states.items():
                logger.info(str(key) + ' : ' + str(value))
            logger.info("   ")
            logger.info("DAG leaf task start states")
            for start_state in DAG_leaf_task_start_states:
                logger.info(start_state)
            logger.info("")
            logger.info("all_collapse_task_names:")
            for name in all_collapse_task_names:
                logger.info(name)
            logger.info("")
            logger.info("DAG_tasks:")
            for key, value in DAG_tasks.items():
                logger.info(str(key) + ' : ' + str(value))
            logger.info("")
            logger.info("DAG_leaf_tasks:")
            for task_name in DAG_leaf_tasks:
                logger.info(task_name)
            logger.info("") 
            logger.info("DAG_leaf_task_inputs:")
            for inp in DAG_leaf_task_inputs:
                logger.info(inp)
            logger.info("")
            logger.info("DAG_version_number:")
            logger.info(DAG_version_number)
            logger.info("")
            logger.info("DAG_info_is_complete:")
            logger.info(Group_DAG_is_complete)
            logger.info("")
            logger.info("DAG_number_of_tasks:")
            logger.info(DAG_number_of_tasks)
            logger.info("")
            logger.info("DAG_number_of_incomplete_tasks:")
            logger.info(DAG_number_of_incomplete_tasks)
            logger.info("")

    DAG_info = DAG_Info.DAG_info_fromdictionary(DAG_info_dictionary)
    return  DAG_info

"""
Note: The code for DAG_info_fromdictionary is below. The info in a 
DAG_info object is in its DAG_info_dictionary.

    def __init__(self,DAG_info_dictionary,file_name = './DAG_info.pickle'):
        self.file_name = file_name
        self.DAG_info_dictionary = DAG_info_dictionary

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
#rhc: Q: can we just pass groups_of_previous_partition?
    to_be_continued):
# to_be_continued is True if num_nodes_in_partitions < num_nodes, which means that incremeental DAG generation
# is not complete (some gtaph nodes are not in any partition.)

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

    logger.info("generate_DAG_info_incremental_groups: to_be_continued: " + str(to_be_continued))
    logger.info("generate_DAG_info_incremental_groups: current_partition_number: " + str(current_partition_number))

    print()
    print("generate_DAG_info_incremental_groups: Group_senders:")
    for sender_name,receiver_name_set in Group_senders.items():
        print("sender:" + sender_name)
        print("receiver_name_set:" + str(receiver_name_set))
    print()
    print()
    print("generate_DAG_info_incremental_groups: Group_receivers:")
    for receiver_name,sender_name_set in Group_receivers.items():
        print("receiver:" + receiver_name)
        print("sender_name_set:" + str(sender_name_set))
    print()
    print()
    print("generate_DAG_info_incremental_groups: Leaf nodes of groups:")
    for name in leaf_tasks_of_groups_incremental:
        print(name + " ")
    print()

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
    senders = Group_receivers.get(current_partition_name)
    # Note: a group that starts a new connected component (which is 
    # the first partition collected on a call to BFS(), of which there 
    # may be many calls if the graph is not connected) is a leaf
    # node and thus has no senders. This is true about partition/group 1 and
    # this is asserted by the caller (BFS()) of this method.

    if current_partition_number == 1:
        # there is one group in the groups of partition 1

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
        
        # record DAG information 
        # leaf task name
        Group_DAG_leaf_tasks.append(name_of_first_group_in_DAG)
        # leaf task state
        Group_DAG_leaf_task_start_states.append(Group_next_state)
        # leaf tasks have no input
        task_inputs = ()
        Group_DAG_leaf_task_inputs.append(task_inputs)

        # we will check that current_group_name is in leaf_tasks_of_groups
        # upon return to BFS() (when we see tht leaf tasks have been added to the DAG)

        fanouts = []
        faninNBs = []
        fanins = []
        collapse = []
        fanin_sizes = []
        faninNB_sizes = []

        # generate the state for this partition/DAG task
        Group_DAG_map[Group_next_state] = state_info(name_of_first_group_in_DAG, fanouts, fanins, faninNBs, collapse, fanin_sizes, 
            faninNB_sizes, task_inputs,
            to_be_continued,
            # We do not know whether this first group will have fanout_fanin_faninNB_collapse_groups_are_ToBeContinued_are_ToBeContinued
            # that are incomplete until we process the 2nd partition, except if to_be_continued
            # is False in which case there are no more partitions and no fanout_fanin_faninNB_collapse_groups_are_ToBeContinued_are_ToBeContinued
            # that are incomplete. If to_be_continued is True then we set fanout_fanin_faninNB_collapse_groups_are_ToBeContinued_are_ToBeContinued
            # to True but we may change this value when we process partition 2.
            to_be_continued)
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

        logger.info("generate_DAG_info_incremental_groups: Group_DAG_map[current_partition_state]: " + str(Group_DAG_map[Group_next_state] ))

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

        logger.info("generate_DAG_info_incremental_groups: returning from generate_DAG_info_incremental_groups for"
            + " group " + str(name_of_first_group_in_DAG))
        
        # This will be set to 2
        Group_next_state += 1
        
        return DAG_info

    else:
        # Flag to indicate whether we are processing the first group_name of groups in the 
        # previous partition. 
        first_previous_group = True
        for group_name in groups_of_current_partition:
            # Get groups that output to group group_name. These are groups in 
            # previous partition or in this current partition.
            senders = Group_receivers.get(group_name) 
            if (senders == None):

                # assert:
                if len(groups_of_current_partition) > 1:
                    logger.error("[Error]: Internal error: generate_DAG_info_incremental_groups:"
                        + " start of new connected component (i.e., called BFS()) but there is more than one group.")

                # This is not partition 1. But it is a leaf partition, which means
                # it was the first partition generated by some call to BFS(), i.e., 
                # it is the start of a new connected component. This also means there
                # is no collapse from the groups n the previous partition to the
                # groups in ths partition, i.e., the groups in the previous partition 
                # has no fanouys/fanins/faninNBs/collapses to the groups in this patition.
                # Note: when we call BFS() we will collect a single partition/group
                # that is the start of a new connected component.

                # Since this is a leaf node (it has no predecessor) we will need to add 
                # this partition/group to the work queue or start a new lambda for it (
                # like the DAG_executor_driver does. (Note that since this partition/group has
                # no predecessor, no worker or lambda can enable this task via a fanout, collapse,
                # or fanin, thus we must add this partition/group as work explicitly ourselves.)
                # This is done when BFS deposits a new DAG, i.e., in method deposit.
                # This prevents workers from getting the leaf task work until the DAG_info 
                # with the leaf tasks newly added is made available (via deposit).
            
                # Mark this partition/group as a leaf task. If any more of these leaf task 
                # partitions/groups are found (by later calls to BFS()) they will accumulate 
                # in these lists. BFS() uses these lists to identify leaf tasks - when BFS generates an 
                # incremental DAG_info, it adds work to the work queue or starts a
                # lambda for each leaf task that is not the first partition/group in the 
                # DAG. The first partition is always a leaf task and it is handled by the 
                # DAG_executor_driver.

                logger.info("generate_DAG_info_incremental_groups: start of new connected component is group "
                    + group_name)

                # task input is same as for leaf task group 1 above - empty
                Group_DAG_leaf_tasks.append(group_name)
                Group_DAG_leaf_task_start_states.append(Group_next_state)
                task_inputs = ()
                Group_DAG_leaf_task_inputs.append(task_inputs)

                fanouts = []
                faninNBs = []
                fanins = []
                collapse = []
                fanin_sizes = []
                faninNB_sizes = []

                # generate state_info for group group_name
                Group_DAG_map[Group_next_state] = state_info(group_name, fanouts, fanins, faninNBs, collapse, fanin_sizes, 
                    faninNB_sizes, task_inputs,
                    to_be_continued,
                    # We do not know whether this frist group will have fanout_fanin_faninNB_collapse_groups_are_ToBeContinued_are_ToBeContinued
                    # that are incomplete until we process the 2nd partition, except if to_be_continued
                    # is False in which case there are no more partitions and no fanout_fanin_faninNB_collapse_groups_are_ToBeContinued_are_ToBeContinued
                    # that are incomplete. If to_be_continued is True then we set fanout_fanin_faninNB_collapse_groups_are_ToBeContinued_are_ToBeContinued
                    # to True but we may change this value when we process partition 2.
                    to_be_continued)
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

                ## save current name as previous name. If this is partition PRi_1
                ## we cannot just use PRi-1_1 since the name might have an L at the
                ## end to mark it as a loop partition. So save the name so we have it.
                #Group_DAG_previous_partition_name = current_partition_name

                # None of the groups in the previous partiton output to 
                # group_name since senders is None. So we initialized the state
                # info of these groups in the previous partition to have empty sets for
                # fanouts/fanins/faninNBs/collapses/fanin_sizes/faninNB_sizes
                # and we do not need to modify these empty sets with respect to group_name
                # since the previous groups do not output to group_name. This
                # group group_name is a leaf task.
                # Note: None of the groups that are in the same partition as 
                # group_name output to group_name either since if they did they 
                # would show up as a sender to group_name, i.e., they would be
                # in senders but senders is None.
                
                previous_partition_state = current_partition_state - 1
                groups_of_previous_partition = groups_of_partitions[previous_partition_state-1]
                logger.info("generate_DAG_info_incremental_groups: current_partition_state: " 
                    + str(current_partition_state) + ", previous_partition_state: "
                    + str(previous_partition_state))

                # Do this one time, .e., for first group, not for all the groups
                if first_previous_group:
                    first_previous_group = False

                    logger.info("generate_DAG_info_incremental_groups: update the state_info for previous groups: "
                        + str(groups_of_previous_partition))
                    # flag so we only do this for the first group of groups in previous previous partition
                    first_previous_previous_group = True
                    for previous_group in groups_of_previous_partition:
                        logger.info("generate_DAG_info_incremental_groups: previous_group: " + previous_group)
                        #for key, value in Group_DAG_map.items():
                        #    logger.info(str(key) + ' : ' + str(value))
                        #logger.info("")
                        # get the state (number) of previous group


                        # This is the first partition in a new connected 
                        # component. This means there is no other partition/group
                        # that has a fanout/fanin/fainNB/collpase to this first 
                        # partition. This then means that the previous partition 
                        # processed, and the groups therein, has no fanouts/fanins/
                        # fannNBs/collapses to the groups in this partition; however, 
                        # the groups in the previous partition might have fanouts/fanins/
                        # faninNBs/collpases to the groups in their same partition.
                        # For example, in the white board example ,PR2_1 has a faninNB
                        # to PR2_2L, and PR3_1 has a fanout to PR#_2.
                        # That previous partition is the last partition 
                        # processed in the previous connected component - there are
                        # no partitions (with groups in it) after that partition.
                        # (If there were, we would not have called BFS() again to 
                        # visit the unvicited part of the state space,)
                        #
                        # Groups in previous partition do not have a fanout/fanin/faninNB/collapse
                        # to a group in current partition but they may have fanout/fanin/faninNB/collapse
                        # to the groups in that same previous partiton. 
                    
# START
# rhc: Note: Can assert group to group is within previous grooup since current is new CC
                        Group_sink_set = set()
                        #for senderX in Group_senders:

                        fanouts = []
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

        #rhc: ToDo: Q: Issue: But we are computing the state info for the 
        # groups in the previous partition, not the groups in the current
        # partition, of which group_name is one? So here we iterate through groups
        # of previous partition (which should be in Group_receivers.get(group_name))
        # and compuet their state_info which is complete now. Note: state info 
        # of group_name in current partition is not complete.
        # So like:
        #  previous_partition_state = current_partition_state - 1
        #  groups_of_previous_partition = groups_of_partitions[previous_partition_state-1]
        #  for name_of_group_in_previous_partition in groups_of_previous_partition:
        # where change "group_name" to name_of_group_in_previous_partition

                        logger.info("generate_DAG_info_incremental_groups: previous_group: " + previous_group)

                        receiver_set_for_previous_group = Group_senders.get(previous_group,set())
                        # for each group that receives output from previous_group
                        for receiverY in receiver_set_for_previous_group:
                            # Get the groups that receive output from receiverY
                            receiver_set_for_receiverY = Group_senders.get(receiverY)
                            if receiver_set_for_receiverY == None:
                                # receiverY does not send any outputs so it is a sink.
                                # 
                                # For non-incremental, this receiverY will not show
                                # up in Group_senders so we will not process receiverY
                                # as part of the major loop for generating a DAG during
                                # non-incremental DAG generation, That means, after the
                                # major loop terminates, we look at the groups we added
                                # Group_sink_set. For these groups, they have no 
                                # fanouts/fanins/faninNBs/collapses (since they 
                                # were never a sender and thus are not a key in 
                                # Group_senders) but they may have inputs. Thus
                                # we will generate inputs for these groups (like
                                # receiverY).
                                # Note: Such a receiver will have o inputs if it is
                                # the first group of a connected component and the
                                # only group of the connected component (so it sends
                                # to no other groups)
                                # 
                                # For incremental DAG generation, ...

        #rhc: ToDo: Q: do this? or ?? Note we are iterating through 
        # groups in current partition and we generate inputs for them
        # which would include any sink? that is, we do not iterate though
        # senders, which may not include sinks (since  sink is not a sender)
        # but we iterate through each group in every partition so we will
        # catch senders in this main group_name loop?

                                Group_sink_set.add(receiverY)
                            # get groups that send outputs to receiverY, this could be one 
                            # or more groups (since we know that previous_group sends to receiverY)
                            sender_set_for_receiverY = Group_receivers[receiverY]
                            # number of groups that send outputs to receiverY
                            length_of_sender_set_for_receiverY = len(sender_set_for_receiverY)
                            # number of groups that receive outputs from group_name
                            length_of_receiver_set_for_previous_group = len(receiver_set_for_previous_group)

                            if length_of_sender_set_for_receiverY == 1:
                                # receiverY receives input from only one group; so it must be from a
                                # collapse or fanout (as fanins and faninNB tasks receive two or more inputs.)
                                if length_of_receiver_set_for_previous_group == 1:
                                    # only one group, previous_group, sends outputs to receiverY and this sending 
                                    # group previous_group only sends to one group, so collapse receiverY, i.e.,
                                    # previous_group becomes receiverY via a collapse.
                                    logger.info("sender " + previous_group + " --> " + receiverY + " : Collapse")
                                    if not receiverY in Group_all_collapse_task_names:
                                        Group_all_collapse_task_names.append(receiverY)
                                    else:
                                        logger.error("[Error]: Internal Error: generate_DAG_info_incremental_groups:"
                                            + "group " + receiverY + " is in the collapse set of two groups.")
                                    collapse.append(receiverY)
                                else:
                                    # only one task, group_name, sends output to receiverY and this sending 
                                    # group sends to other roups too, so group_name does a fanout 
                                    # to group receiverY.  
                                    logger.info("sender " + previous_group + " --> " + receiverY + " : Fanout")
                                    if not receiverY in Group_all_fanout_task_names:
                                        Group_all_fanout_task_names.append(receiverY)
                                    fanouts.append(receiverY)
                            else:
                                # previous_group has fanin or fannNB to group receiverY since 
                                # receiverY receives inputs from multiple groups.
                                isFaninNB = False
                                # Recall sender_set_for_receiverY is the groups that send outputs
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
                                    logger.info("group " + previous_group + " --> " + receiverY + " : FaninNB")
                                    if not receiverY in Group_all_faninNB_task_names:
                                        Group_all_faninNB_task_names.append(receiverY)
                                        Group_all_faninNB_sizes.append(length_of_sender_set_for_receiverY)
                                    logger.info ("after Group_all_faninNBs_sizes append: " + str(Group_all_faninNB_sizes))
                                    logger.info ("faninNBs append: " + receiverY)
                                    faninNBs.append(receiverY)
                                    faninNB_sizes.append(length_of_sender_set_for_receiverY)
                                else:
                                    # senderX sends its output only to receiverY, same for any other
                                    # tasks that sends outputs to receiverY so receiverY is a fanin task.
                                    logger.info("group " + previous_group + " --> " + receiverY + " : Fanin")
                                    if not receiverY in Group_all_fanin_task_names:
                                        Group_all_fanin_task_names.append(receiverY)
                                        Group_all_fanin_sizes.append(length_of_sender_set_for_receiverY)
                                    fanins.append(receiverY)
                                    fanin_sizes.append(length_of_sender_set_for_receiverY)

                        # get the tasks that send to senderX, i.e., provide inputs for senderX
                        """
                        WE DID THIS ALREADY: leaf tasks and set  task_inputs

                        sender_set_for_senderX = Group_receivers.get(senderX)
                        if sender_set_for_senderX == None:
                            # senderX is a leaf task since it is not a receiver
                            Group_DAG_leaf_tasks.append(senderX)
                            Group_DAG_leaf_task_start_states.append(state)
                            task_inputs = ()
                            Group_DAG_leaf_task_inputs.append(task_inputs)

                            if not senderX in leaf_tasks_of_groups:
                                logger.error("partition " + senderX + " receives no inputs"
                                    + " but it is not in leaf_tasks_of_groups.")
                            else:
                                # we have generated a state for leaf task senderX. 
                                leaf_tasks_of_groups.remove(senderX)
                        else:
                            # create a new set from sender_set_for_senderX. For 
                            # each name in sender_set_for_senderX, qualify name by
                            # prexing it with "senderX-". Example: senderX is "PR1_1"
                            # and name is "PR2_3" so the qualified name is "PR1_1-PR2_3".
                            # We use qualified names since the fanouts/faninNBs for a 
                            # task in a pagerank DAG may al have diffent values. This
                            # is unlike Dask DAGs in which all fanouts/faninNBs of a task
                            # receive the same value. We denote the different outputs
                            # of a task A having, e.g., fanouts B and C as "A-B" and "A-C"
                            sender_set_for_senderX_with_qualified_names = set()
                            # for each task name that sends input to senderX, the 
                            # qualified name of the sender is name+"-"+senderX
                            for name in sender_set_for_senderX:
                                qualified_name = str(name) + "-" + str(senderX)
                                sender_set_for_senderX_with_qualified_names.add(qualified_name)
                            # sender_set_for_senderX provides input for senderX
                            task_inputs = tuple(sender_set_for_senderX_with_qualified_names)
                        """

                        # We just calculated the fanouts/fanins/faninNBs/collapses sets of 
                        # previous_group, so get the state info of this previous
                        # group and chnage the state info by adding these sets.
                        #
                        # get the state (number) of previous group
                        previous_group_state = Group_DAG_states[previous_group]
                        # get the state_info of previous group
                        state_info_of_previous_group = Group_DAG_map[previous_group_state]

                        if state_info_of_previous_group == None:
                            logger.error("[Error] Internal Error: generate_DAG_info_incremental_groups: state_info_of_previous_group: "
                                + "state_info_of_previous_group is None.")
                            logger.error("DAG_map:")
                            for key, value in Group_DAG_map.items():
                                logger.info(str(key) + ' : ' + str(value))
                            logging.shutdown()
                            os._exit(0)

                        # The fanouts/fanins/faninNBs/collapses in state_info are 
                        # empty so just add the fanouts/fanins/faninNBs/collapses that
                        # we just calculated. Note: we are modifying the info in the
                        # DAG that is being constructed incrementally. 
                        fanouts_of_previous_state = state_info_of_previous_group.fanouts
                        fanouts_of_previous_state += fanouts

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

                        logger.info("before update to TBC and fanout_fanin_faninNB_collapse_groups_are_ToBeContinued_are_ToBeContinued"
                            + " for previous_group " + previous_group + " state_info_of_previous_group: " + str(state_info_of_previous_group))

                        # Groups in previous partition are now complete so TBC is set to False.
                        # Note: The current partition cannot be partition 1.
                        state_info_of_previous_group.ToBeContinued = False
                        # The last group of a connected component does not do any fanouts/fanins/etc
                        state_info_of_previous_group.fanout_fanin_faninNB_collapse_groups_are_ToBeContinued_are_ToBeContinued = False

                        if first_previous_previous_group:
                            first_previous_previous_group = False
                            if current_partition_number > 2:
                                previous_previous_partition_state = previous_partition_state - 1
                                groups_of_previous_previous_partition = groups_of_partitions[previous_previous_partition_state-1]
                                for previous_previous_group in groups_of_previous_previous_partition:
                                    state_of_previous_previous_group = Group_DAG_states[previous_previous_group]
                                    state_info_of_previous_previous_group = Group_DAG_map[state_of_previous_previous_group]
                                    state_info_of_previous_previous_group.fanout_fanin_faninNB_collapse_groups_are_ToBeContinued = False
                                    
                                    logger.info("The state_info_of_previous_previous_group " 
                                        + previous_previous_group + " after update fanout_fanin_faninNB_collapse_groups_are_ToBeContinued is: " 
                                        + str(state_info_of_previous_previous_group))

                        logger.info("after update to TBC and fanout_fanin_faninNB_collapse_groups_are_ToBeContinued_are_ToBeContinued"
                            + " for previous_group " + previous_group + " state_info_of_previous_group: " 
                            + str(state_info_of_previous_group))

                """
                if to_be_continued:
                    number_of_incomplete_tasks = len(groups_of_current_partition)
                else:
                    number_of_incomplete_tasks = 0               
                DAG_info = generate_DAG_for_groups(to_be_continued,number_of_incomplete_tasks)

                if to_be_continued:
                    DAG_info_DAG_map = DAG_info.get_DAG_map()

                    # The DAG_info object is shared between this DAG_info generator
                    # and the DAG_executor, i.e., we execute the DAG generated so far
                    # while we generate the next incremental DAGs. The current 
                    # state is part of the DAG given to the DAG_executor and we 
                    # will modify the current state when we generate the next DAG.
                    # (We modify the collapse list and the toBeContiued  of the state.)
                    # So we do not share the current state object, that is the 
                    # DAG_info given to the DAG_executor has a state_info reference
                    # this is different from the reference we maintain here in the
                    # DAG_map. 
                    # 
                    # Get the state_info for the DAG_map
                    state_info_of_current_group_state = DAG_info_DAG_map[Group_next_state]

                    # Note: the only parts of the states that can be changed 
                    # for partitions are the colapse list and the TBC boolean. Yet 
                    # we deepcopy the entire state_info object. But all other
                    # parts of the stare are empty for partitions (fanouts, fanins)
                    # except for the pagerank function.
                    # Note: Each state has a reference to the Python function that
                    # will excute the task. This is how Dask does it - each task
                    # has a reference to its function. For pagernk, we will use
                    # the same function for all the pagerank tasks. There can be 
                    # three different functions, but we could identify this 
                    # function whrn we excute the task, instead of doing it above
                    # and saving this same function in the DAG for each task,
                    # which wastes space

                    # make a deep copy of this state_info object which is the atate_info
                    # object tha the DAG generator will modify
                    copy_of_state_info_of_current_partition_state = copy.deepcopy(state_info_of_current_group_state)

                    # give the copy to the DAG_map given to the DAG_executor. Now
                    # the DAG_executor and the DG_generator will be using different 
                    # state_info objects 
                    DAG_info_DAG_map[Group_next_state] = copy_of_state_info_of_current_partition_state

                    # this used to test the deep copy - modify the state info
                    # of the generator and make sure this modification does 
                    # not show up in the state_info object given to the DAG_executor.


                    # modify generator's state_info 
                    Group_DAG_map[Group_next_state].fanins.append("goo")

                    # display DAG_executor's state_info objects
                    logger.info("address DAG_info_DAG_map: " + str(hex(id(DAG_info_DAG_map))))
                    logger.info("generate_DAG_info_incremental_groups: DAG_info_DAG_map after state_info copy:")
                    for key, value in DAG_info_DAG_map.items():
                        logger.info(str(key) + ' : ' + str(value) + " addr value: " + str(hex(id(value))))

                    # display generator's state_info objects
                    logger.info("address Group_DAG_map: " + str(hex(id(Group_DAG_map))))
                    logger.info("generate_DAG_info_incremental_groups: Group_DAG_map:")
                    for key, value in Group_DAG_map.items():
                        logger.info(str(key) + ' : ' + str(value) + " addr value: " + str(hex(id(value))))

                    # undo the modification to the generator's state_info
                    Group_DAG_map[Group_next_state].fanins.clear()

                    # display generator's state_info objects
                    logger.info("generate_DAG_info_incremental_groups: DAG_info_DAG_map after clear:")
                    for key, value in DAG_info_DAG_map.items():
                        logger.info(str(key) + ' : ' + str(value))
                
                    # display DAG_executor's state_info ojects
                    logger.info("generate_DAG_info_incremental_groups: Group_next_state:")
                    for key, value in Group_next_state_DAG_map.items():
                        logger.info(str(key) + ' : ' + str(value))

                    # logging.shutdown()
                    # os._exit(0)
                    """ 
                    # Note: There should be only one group in a partition that is the 
                    # start of a new connected component; this is asserted above.
                    # Thus we should next execute the return at the end.
    
            else: # current_partition_number >= 2

                # (Note: Not sure whether we can have a length 0 senders, 
                # for current_partition_name. That is, we only create a 
                # senders set when we get the first sender.)

                # assert: no length 0 senders lists
                if len(senders) == 0:
                    logger.error("[Error]: Internal Error: generate_DAG_info_incremental_groups:"
                        + " group has a senders list with length 0.")

                # This is not the first partition and it is not a leaf partition.

                # Create a new set from set sender_set_for_group_name. For 
                # each name in sender_set_for_group_name, qualify the name by
                # prexing it with "name-". Example: name in sender_set_for_group_name 
                # is "PR1_1" and group_name is "PR2_3" so the qualified name is 
                # "PR1_1-PR2_3".
                # We use qualified names since the fanouts/faninNBs for a 
                # task in a pagerank DAG may all have diffent values. This
                # is unlike Dask DAGs in which all fanouts/faninNBs of a task
                # have the same value. We denote the different outputs
                # of a task A having, e.g., fanouts B and C as "A-B" and "A-C"
                # Note: Here we are calculating the tuple of task inputs, which 
                # is the set of tasks that send inputs to this group. This 
                # set is Group_receivers.get(group_name).

                sender_set_for_group_name = Group_receivers.get(group_name)
                logger.info("sender_set_for_group_name, i.e., Receivers " + group_name + ":" + str(sender_set_for_group_name))

                sender_set_for_group_name_with_qualified_names = set()
                # For each sender task "name" that sends output to group_name, the 
                # qualified name of the output is: name+"-"+group_name
                for name in sender_set_for_group_name:
                    qualified_name = str(name) + "-" + str(group_name)
                    sender_set_for_group_name_with_qualified_names.add(qualified_name)
                # sender_set_for_senderX provides input for group_name
                task_inputs = tuple(sender_set_for_group_name_with_qualified_names)

                # generate empty state_info for group group_name. This will be filled in 
                # when we process the groups in the next partition collected. See below.
                # That is, this group (recall that we are iterating
                # through the grous of the current partition) is 
                # incomplete and we will complete it when we process
                # the (groups in the) next partition.
                fanouts = []
                faninNBs = []
                fanins = []
                collapse = []
                fanin_sizes = []
                faninNB_sizes = []
                Group_DAG_map[Group_next_state] = state_info(group_name, fanouts, fanins, faninNBs, collapse, fanin_sizes, 
                    faninNB_sizes, task_inputs,
                    # to_be_continued parameter can be true or false
                    to_be_continued,
                    # We do not know whether this frist group will have fanout_fanin_faninNB_collapse_groups_are_ToBeContinued
                    # that are incomplete until we process the 2nd partition, except if to_be_continued
                    # is False in which case there are no more partitions and no fanout_fanin_faninNB_collapse_groups_are_ToBeContinued
                    # that are incomplete. If to_be_continued is True then we set fanout_fanin_faninNB_collapse_groups_are_ToBeContinued
                    # to True but we may change this value when we process partition 2.
                    to_be_continued)
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
                
                # This is not the first partition and it is not a leaf partition.
                # So current_partition_state is 2 or more (states start at 1)
                # Positions in groups_of_partitions statr at 0.
                previous_partition_state = current_partition_state - 1
                groups_of_previous_partition = groups_of_partitions[previous_partition_state-1]
                logger.info("generate_DAG_info_incremental_groups: current_partition_state: " 
                    + str(current_partition_state) + ", previous_partition_state: "
                    + str(previous_partition_state))
                
                # Do this one time, i.e., there may be many groups in the current partition
                # and we are iterating through these groups. But all of these groups have the 
                # same previous paritition and we only need to process the groups in the
                # previous partition once, which we do here.
                # We are completing the state information for the groups in the 
                # previous partition. Those groups can send inputs to the groups in the 
                # current partition (whihc we are processing here) or to other
                # groups in their same partition, which is the previous partition.
                if first_previous_group:
                    first_previous_group = False

                    logger.info("generate_DAG_info_incremental_groups: complete the state_info for previous groups: "
                        + str(groups_of_previous_partition))
                    # When we added these previous groups to the DAG we added them with empty
                    # fanouts/fanins/faninNBs/collapse sets. Now that we collected the 
                    # groups in the current partition, which is the next partition of groups 
                    # collected, we can compute these sets for the groups in the previous
                    # partition. Note: a group can only have a fanout/fanin/faninNB/collapse to 
                    # a group in its same partition or to a group in the next partition. So 
                    # when we collect the groups in (current) partition i, we know the behavior 
                    # of the groups in (previous) partition i-1.
                    first_previous_previous_group = True
                    for previous_group in groups_of_previous_partition:
                        # sink nodes, i.e., nodes that do not send any outputs to other nodes
#rhc: need this?

# START
                        Group_sink_set = set()
                        #for senderX in Group_senders:

                        fanouts = []
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

        #rhc: ToDo: Q: Issue: But we are computing the state info for the 
        # groups in the previous partition, not the groups in the current
        # partition, of which group_name is one? So here we iterate through groups
        # of previous partition (which should be in Group_receivers.get(group_name))
        # and compuet their state_info which is complete now. Note: state info 
        # of group_name in current partition is not complete.
        # So like:
        #  previous_partition_state = current_partition_state - 1
        #  groups_of_previous_partition = groups_of_partitions[previous_partition_state-1]
        #  for name_of_group_in_previous_partition in groups_of_previous_partition:
        # where change "group_name" to name_of_group_in_previous_partition

                        logger.info("generate_DAG_info_incremental_groups: previous_group: " + previous_group)

                        # get groups that previous group sends inputs to. These
                        # groups "reveive" inputs from the sender
                        receiver_set_for_previous_group = Group_senders[previous_group]
                        # for each group that receives an input from the previous_group
                        for receiverY in receiver_set_for_previous_group:
                            # Get the groups that receive inputs from receiverY.
                            # Note that we know that the previous_group sends 
                            # inputs to receiverY, but we need to know if there are
                            # any other groups that send inputs to receiverY in order
                            # to know whether receiverY is a task for a fanin/fanout/faniNB/collapse
                            # of previous_group.

                            # Here we chck whether rceiverY is a sink, i.e., it does not
                            # send inputs to any other group.
                            receiver_set_for_receiverY = Group_senders.get(receiverY)
                            if receiver_set_for_receiverY == None:
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
                                # For incremental DAG generation, ...

        #rhc: ToDo: Q: do this? or ?? Note we are iterating through 
        # groups in current partition and we generate inputs for them
        # which would include any sink? that is, we do not iterate though
        # senders, which may not include sinks (since  sink is not a sender)
        # but we iterate through each group in every partition so we will
        # catch senders in this main group_name loop?

                                Group_sink_set.add(receiverY)

                            # get groups that send inputs to receiverY, this could be one 
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
                            length_of_sender_set_for_receiverY = len(sender_set_for_receiverY)
                            # number of groups that receive input from previous_group
                            length_of_receiver_set_for_previous_group = len(receiver_set_for_previous_group)

                            if length_of_sender_set_for_receiverY == 1:
                                # receiverY receives input from only one group; so receiverY must be a
                                # collapse or fanout (as fanins and faninNB tasks receive two or more inputs.)
                                if length_of_receiver_set_for_previous_group == 1:
                                    # only one group, previous_group, sends outputs to receiverY and this sending 
                                    # group previous_group only sends inputs to one group (receiverY), so collapse 
                                    # receiverY, i.e., previous_group becomes receiverY via a collapse.
                                    logger.info("sender " + previous_group + " --> " + receiverY + " : Collapse")
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
                                    logger.info("sender " + previous_group + " --> " + receiverY + " : Fanout")
                                    if not receiverY in Group_all_fanout_task_names:
                                        Group_all_fanout_task_names.append(receiverY)
                                    # we are generating the sets of collapse/fanin/fanout/faninNB
                                    # of previous_group
                                    fanouts.append(receiverY)
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
                                    logger.info("group " + previous_group + " --> " + receiverY + " : FaninNB")
                                    if not receiverY in Group_all_faninNB_task_names:
                                        Group_all_faninNB_task_names.append(receiverY)
                                        Group_all_faninNB_sizes.append(length_of_sender_set_for_receiverY)
                                    logger.info ("after Group_all_faninNBs_sizes append: " + str(Group_all_faninNB_sizes))
                                    logger.info ("faninNBs append: " + receiverY)
                                    faninNBs.append(receiverY)
                                    faninNB_sizes.append(length_of_sender_set_for_receiverY)
                                else:
                                    # all tasks that send inputs to receiverY don't send inputs to any other
                                    # task/grup, so receiverY is a fanin task.
                                    logger.info("group " + previous_group + " --> " + receiverY + " : Fanin")
                                    if not receiverY in Group_all_fanin_task_names:
                                        Group_all_fanin_task_names.append(receiverY)
                                        Group_all_fanin_sizes.append(length_of_sender_set_for_receiverY)
                                    fanins.append(receiverY)
                                    fanin_sizes.append(length_of_sender_set_for_receiverY)

                        # get the tasks that send to senderX, i.e., provide inputs for senderX
                        """
                        WE DID THIS ALREADY: leaf tasks and set  task_inputs

                        sender_set_for_senderX = Group_receivers.get(senderX)
                        if sender_set_for_senderX == None:
                            # senderX is a leaf task since it is not a receiver
                            Group_DAG_leaf_tasks.append(senderX)
                            Group_DAG_leaf_task_start_states.append(state)
                            task_inputs = ()
                            Group_DAG_leaf_task_inputs.append(task_inputs)

                            if not senderX in leaf_tasks_of_groups:
                                logger.error("partition " + senderX + " receives no inputs"
                                    + " but it is not in leaf_tasks_of_groups.")
                            else:
                                # we have generated a state for leaf task senderX. 
                                leaf_tasks_of_groups.remove(senderX)
                        else:
                            # create a new set from sender_set_for_senderX. For 
                            # each name in sender_set_for_senderX, qualify name by
                            # prexing it with "senderX-". Example: senderX is "PR1_1"
                            # and name is "PR2_3" so the qualified name is "PR1_1-PR2_3".
                            # We use qualified names since the fanouts/faninNBs for a 
                            # task in a pagerank DAG may al have diffent values. This
                            # is unlike Dask DAGs in which all fanouts/faninNBs of a task
                            # receive the same value. We denote the different outputs
                            # of a task A having, e.g., fanouts B and C as "A-B" and "A-C"
                            sender_set_for_senderX_with_qualified_names = set()
                            # for each task name that sends input to senderX, the 
                            # qualified name of the sender is name+"-"+senderX
                            for name in sender_set_for_senderX:
                                qualified_name = str(name) + "-" + str(senderX)
                                sender_set_for_senderX_with_qualified_names.add(qualified_name)
                            # sender_set_for_senderX provides input for senderX
                            task_inputs = tuple(sender_set_for_senderX_with_qualified_names)
                        """

                        # We just calculated the fanouts/fanins/faninNBs/collapses sets of 
                        # previous_group, so get the state info of this previous
                        # group and change the state info by adding these sets.
                        #
                        # get the state (number) of previous group
                        previous_group_state = Group_DAG_states[previous_group]
                        # get the state_info of previous group
                        state_info_of_previous_group = Group_DAG_map[previous_group_state]

                        logger.info("before update to TBC and fanout_fanin_faninNB_collapse_groups_are_ToBeContinued_are_ToBeContinued"
                            + " for previous_group " + previous_group + " state_info_of_previous_group: " + str(state_info_of_previous_group))
                        if state_info_of_previous_group == None:
                            logger.error("[Error] Internal Error: generate_DAG_info_incremental_groups: state_info_of_previous_group: "
                                + "state_info_of_previous_group is None.")
                            logger.error("DAG_map:")
                            for key, value in Group_DAG_map.items():
                                logger.info(str(key) + ' : ' + str(value))
                            logging.shutdown()
                            os._exit(0)

                        # The fanouts/fanins/faninNBs/collapses in state_info are 
                        # empty so just add the fanouts/fanins/faninNBs/collapses that
                        # we just calculated. Note: we are modifying the info in the
                        # (dictionary of information for the) DAG that is being 
                        # constructed incrementally. 
                        fanouts_of_previous_state = state_info_of_previous_group.fanouts
                        fanouts_of_previous_state += fanouts

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
                        # the previous group was consructed as tobe_continued. Now
                        # that we have completed previous_group it is no longer
                        # to_be_continued. So in the next DAG that is generated,
                        # previous_group is not to_be_contnued and so can be 
                        # executed.
                        state_info_of_previous_group.ToBeContinued = False
                        # if the current partition is to_be_continued then it has incomplete
                        # groups so we set fanout_fanin_faninNB_collapse_groups_are_ToBeContinued of the previous
                        # groups to True; otherwise, we set fanout_fanin_faninNB_collapse_groups_are_ToBeContinued to False.
                        # Note: state_info_of_previous_group.ToBeContinued = False inicates that the
                        # previous groups are not to be continued, while
                        # state_info_of_previous_group.fanout_fanin_faninNB_collapse_groups_are_ToBeContinued indicates
                        # whether the previous groups have fanout_fanin_faninNB_collapse_groups_are_ToBeContinued 
                        # that are to be continued, i.e., the fanout_fanin_faninNB_collapse are 
                        # to groups in this current partition and whether these groups in the current
                        # partiton are to be contnued is indicated by parameter to_be_continued.
                        # (When bfs() calls this method it may determine that some of the graph
                        # nodes have not yet been assigned to any partition so the DAG is
                        # still incomplete and to_be_continued = True )
                        state_info_of_previous_group.fanout_fanin_faninNB_collapse_groups_are_ToBeContinued = to_be_continued

                        # Say that the current partition is C , which has a 
                        # previous partition B which has a previous partition A.
                        # In a previous DAG, suppose A is incomplete. When we process
                        # B, we set A to complete (i.e., to_be_continued for A is False)
                        # and B is set to incomplete (i.e., to_be_continued of B is True.)
                        # We also set fanout_fanin_faninNB_collapse_groups_are_ToBeContinued
                        # of A to True, to indicate that A has fannis/fanouts/faninNBs/collapses
                        # to incomplete groups. When we process C, we can set B to complete
                        # and C to incomplete but we can also reset fanout_fanin_faninNB_collapse_groups_are_ToBeContinued
                        # to False since B is complete so all of A's fanins/fanouts/faninNBs/collpases
                        # are to complete groups. That means if C is group_name, then B
                        # is a previous_group, and C is a previous_pervious_group.
                        #
                        # For the previous_group (e.g., B), we need to reset a flag 
                        # for its previous groups, hence "previous_previous"
                        # but we only need to do this once. That is, the current
                        # group group_name (e.g., A) may have many previous_groups, and these
                        # previous groups may have many previous_groups, but 
                        # previous_groups, say, B1 and B2 have the same previous_groups,
                        # so when we reset the previous groups of B1 we are resetting 
                        # the previous groups of B2. So do this resetting only for one
                        # previous group, e.g., B1.
                        if first_previous_previous_group:
                            first_previous_previous_group = False
                            if current_partition_number > 2:
                                previous_previous_partition_state = previous_partition_state - 1
                                groups_of_previous_previous_partition = groups_of_partitions[previous_previous_partition_state-1]
                                for previous_previous_group in groups_of_previous_previous_partition:
                                    state_of_previous_previous_group = Group_DAG_states[previous_previous_group]
                                    state_info_of_previous_previous_group = Group_DAG_map[state_of_previous_previous_group]
                                    state_info_of_previous_previous_group.fanout_fanin_faninNB_collapse_groups_are_ToBeContinued = False
                                    
                                    logger.info("The state_info_of_previous_previous_group for group " 
                                        + previous_previous_group + " after update fanout_fanin_faninNB_collapse_groups_are_ToBeContinued is: " 
                                        + str(state_info_of_previous_previous_group))


                        logger.info("after update to TBC and fanout_fanin_faninNB_collapse_groups_are_ToBeContinued_are_ToBeContinued"
                            + " for previous_group " + previous_group + " state_info_of_previous_group: " 
                            + str(state_info_of_previous_group))


                        """
                        We will catch these sinks since we process groups in partitions
                        as we get then groups.

                        # Finish by doing the receivers that are not senders (opposite of leaf tasks);
                        # these are receivers that send no inputs to other tasks. They have no fanins/
                        # faninBs, fanouts or collapses, but they do have task inputs.
                        for receiverY in Group_sink_set: # Partition_receivers:
                            #if not receiverY in Partition_DAG_states:
                                fanouts = []
                                faninNBs = []
                                fanins = []
                                collapse = []
                                fanin_sizes = []
                                faninNB_sizes = []

                                sender_set_for_receiverY = Group_receivers[receiverY]
                                #task_inputs = tuple(sender_set_for_receiverY)

                                # create a new set from sender_set_for_senderX. For 
                                # each name in sender_set_for_senderX, qualify name by
                                # prexing it with "senderX-". Example: senderX is "PR1_1"
                                # and name is "PR2_3" so the qualified name is "PR1_1-PR2_3".
                                # We use qualified names since the fanouts/faninNBs for a 
                                # task in a pagerank DAG may al have diffent values. This
                                # is unlike Dask DAGs in which all fanouts/faninNBs of a task
                                # receive the same value. We denote the different outputs
                                # of a task A having, e.g., fanouts B and C as "A-B" and "A-C"
                                sender_set_for_receiverY_with_qualified_names = set()
                                for senderX in sender_set_for_receiverY:
                                    qualified_name = str(senderX) + "-" + str(receiverY)
                                    sender_set_for_receiverY_with_qualified_names.add(qualified_name)
                                # sender_set_for_senderX provides input for senderX
                                task_inputs = tuple(sender_set_for_receiverY_with_qualified_names)

                                Group_DAG_map[state] = state_info(receiverY, fanouts, fanins, faninNBs, collapse, fanin_sizes, faninNB_sizes, task_inputs)
                                Group_DAG_states[receiverY] = state
                                state += 1

                        # previous group is now complete
                        state_info_previous_state.ToBeContinued = False
                        logger.info("generate_DAG_info_incremental_groups: for current partition, the previous_state_info after update collpase and TBC: " 
                            + str(state_info_previous_state))

                        """
                ## save current_partition_name as previous_partition_name so we
                ## can access previous_partition_name on the next call.
                #Group_DAG_previous_partition_name = current_partition_name

                """
                Note: We handle the shared objects for all the groups
                at the end of the group loop.
                """

            Group_next_state += 1  

        logger.info("generate_DAG_info_incremental_groups: generate_DAG_info_incremental_groups for"
            + " group " + str(group_name))

        if to_be_continued:
            number_of_incomplete_tasks = len(groups_of_current_partition)
        else:
            number_of_incomplete_tasks = 0               
        DAG_info = generate_DAG_for_groups(to_be_continued,number_of_incomplete_tasks)

        # We are adding state_info objects for the groups of the current
        # partition to the DAG as incmplete (to_be_continued) They will 
        # be accessed (read) by the DAG_executor and we cannot modify them 
        # during execution. However, we we process
        # the next partition, these incomplete groups that we are adding
        # to the DAG now, will need to be modified as we will generate their
        # fanin/fanout/faninNB/collase sets. So here 

        #STOP
        #

        # modify these previous state_info objects, instead we create
        # a deep copy of these state_info objects and modify these 
        # copies. These state_info copies are used in the new DAG
        # that we are generating here. Thus, the DAG_executor and
        # the DAG_generator do not share state_info objects so there
        # is no need to synchronize their access to state_info objects.
        # The other objects in DAG_info that are accessed by
        # DAG_executor and DAG_generator are immutable, so that when
        # the DAG_generator writes one of these objects it is generating
        # a new reference that is different from the reference that the
        # DAG_executor references, e.g., for all Booleans. That means
        # these other ojects, which are only read by DAG_executor and are 
        # written be DAG_generator, are not really being shared. Funny.
        if to_be_continued:
            # Make deep copies of the state_info objects of the current groups
            #
            # Example: Next partition's first group is assigned Group_next_state of 2
            # and len(groups_of_current_partition) is 3. Thrn we will process
            # three groups and assign them states 2, 3, and 4. Note that
            # after the last group is processed, Group_next_state is 5, not 4.
            # so start_of_incomplete_states = 5 - 3 = 2. Then
            # range(start_of_incomplete_states,Group_next_state) is (2,5)
            # where 2 is inclusive and 5 is exclusive.
            start_of_incomplete_states = Group_next_state - len(groups_of_current_partition)
            for state in range(start_of_incomplete_states,Group_next_state):
                DAG_info_DAG_map = DAG_info.get_DAG_map()

                # The DAG_info object is shared between this DAG_info generator
                # and the DAG_executor, i.e., we execute the DAG generated so far
                # while we generate the next incremental DAGs. The current 
                # state is part of the DAG given to the DAG_executor and we 
                # will modify the current state when we generate the next DAG.
                # (We modify the collapse list and the toBeContiued  of the state.)
                # So we do not share the current state object, that is the 
                # DAG_info given to the DAG_executor has a state_info reference
                # this is different from the reference we maintain here in the
                # DAG_map. 
                # 
                # Get the state_info for the DAG_map
                state_info_of_current_group_state = DAG_info_DAG_map[state]

                # where in DAG_info __init__:
                """
                if not use_incremental_DAG_generation:
                    self.DAG_map = DAG_info_dictionary["DAG_map"]
                else:
                    # Q: this is the same as DAG_info_dictionary["DAG_map"].copy()?
                    self.DAG_map = copy.copy(DAG_info_dictionary["DAG_map"])
                #where:
                old_Dict = {'name': 'Bob', 'age': 25}
                new_Dict = old_Dict.copy()
                new_Dict['name'] = 'xx'
                print(old_Dict)
                # Prints {'age': 25, 'name': 'Bob'}
                print(new_Dict)
                # Prints {'age': 25, 'name': 'xx'}
                """

                # Note: the only parts of the states that can be changed 
                # for partitions are the collapse list and the TBC boolean. Yet 
                # we deepcopy the entire state_info object. But all other
                # parts of the stare are empty for partitions (fanouts, fanins)
                # except for the pagerank function.
                # Note: Each state has a reference to the Python function that
                # will excute the task. This is how Dask does it - each task
                # has a reference to its function. For pagernk, we will use
                # the same function for all the pagerank tasks. There can be 
                # three different functions, but we could identify this 
                # function whrn we excute the task, instead of doing it above
                # and saving this same function in the DAG for each task,
                # which wastes space

                # make a deep copy of this state_info object which is the atate_info
                # object that the DAG generator will modify
                copy_of_state_info_of_current_group_state = copy.deepcopy(state_info_of_current_group_state)

                # give the copy to the DAG_map given to the DAG_executor. Now
                # the DAG_executor and the DAG_generator will be using different 
                # state_info objects 
                DAG_info_DAG_map[state] = copy_of_state_info_of_current_group_state

                # this code is used to test the deep copy - modify the state info
                # of the generator and make sure this modification does 
                # not show up in the state_info object given to the DAG_executor.
                """
                # modify the fanin state info maintained by the generator.
                Group_DAG_map[Group_next_state].fanins.append("goo")

                # display DAG_executor's state_info objects
                logger.info("address DAG_info_DAG_map: " + str(hex(id(DAG_info_DAG_map))))
                logger.info("generate_DAG_info_incremental_groups: DAG_info_DAG_map after state_info copy:")
                for key, value in DAG_info_DAG_map.items():
                    logger.info(str(key) + ' : ' + str(value) + " addr value: " + str(hex(id(value))))

                # display generator's state_info objects
                logger.info("address Group_DAG_map: " + str(hex(id(Group_DAG_map))))
                logger.info("generate_DAG_info_incremental_groups: Group_DAG_map:")
                for key, value in Group_DAG_map.items():
                    logger.info(str(key) + ' : ' + str(value) + " addr value: " + str(hex(id(value))))

                # fanin values should be different for current_state
                # one with "goo" and the other empty

                # undo the modification to the generator's state_info
                Group_DAG_map[Group_next_state].fanins.clear()

                # display generator's state_info objects
                logger.info("generate_DAG_info_incremental_groups: DAG_info_DAG_map after clear:")
                for key, value in DAG_info_DAG_map.items():
                    logger.info(str(key) + ' : ' + str(value))

                # display DAG_executor's state_info ojects
                logger.info("generate_DAG_info_incremental_groups: Group_next_state:")
                for key, value in Group_next_state_DAG_map.items():
                    logger.info(str(key) + ' : ' + str(value))

                # fanin values should be the same for current_state (empty)

                # logging.shutdown()
                # os._exit(0)
            """ 
                
    logger.info("generate_DAG_info_incremental_groups: returning from generate_DAG_info_incremental_groups for"
        + " group " + str(group_name))
    
    # To stop after DAG is completely generated, whcih is combined with 
    # a sleep at the start of the DAG_executor_driver_Invoker_Thread 
    # so that DAG excution does not start.
    #def DAG_executor_driver_Invoker_Thread():
    #time.sleep(3)
    #run()
    #if DAG_info.get_DAG_info_is_complete():
    #    logging.shutdown()
    #    os._exit(0)

    return DAG_info