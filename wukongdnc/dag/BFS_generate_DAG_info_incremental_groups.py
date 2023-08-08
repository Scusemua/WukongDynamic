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
from .BFS_generate_DAG_info import leaf_tasks_of_partitions_incremental


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
Group_DAG_map = {}
Group_DAG_states = {}
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

Group_DAG_number_of_tasks = 0

Group_next_state = 1

# Called by generate_DAG_info_incremental_partitions below to generate 
# the DAG_info object.= when we are using partitions.
def generate_DAG_for_groups(to_be_continued):
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

    # If there is only one partition in the DAG then it is complete and is version 1.
    # It is returned above. Otherwise, version 1 is the DAG_info with partitions
    # 1 and 2, where 1 is complete and 2 is complete or incomplete.
    Group_DAG_version_number += 1
    Group_DAG_is_complete = not to_be_continued # to_be_continued is a parameter
    Group_DAG_number_of_tasks = len(Group_DAG_tasks)
    DAG_info_dictionary["DAG_version_number"] = Group_DAG_version_number
    DAG_info_dictionary["DAG_is_complete"] = Group_DAG_is_complete
    DAG_info_dictionary["DAG_number_of_tasks"] = Group_DAG_number_of_tasks

#rhc: Note: we are saving all the incemental DAG_info files for debugging but 
# we probably want to turn this off otherwise.

    # filename is based on version number - Note: for partition, say 3, we
    # have output the DAG_info with partitions 1 and 2 as version 1 so 
    # the DAG_info for partition 3 will have partitions 1, 2, and 3 and will
    # be version 2 but named "DAG_info_incremental_Partition_3"
    file_name_incremental = "./DAG_info_incremental_Partition_" + str(Group_DAG_version_number) + ".pickle"
    with open(file_name_incremental, 'wb') as handle:
        cloudpickle.dump(DAG_info_dictionary, handle) #, protocol=pickle.HIGHEST_PROTOCOL)  

    num_fanins = len(Group_all_fanin_task_names)
    num_fanouts = len(Group_all_fanout_task_names)
    num_faninNBs = len(Group_all_faninNB_task_names)
    num_collapse = len(Group_all_collapse_task_names)

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
        logger.info("Group_all_fanout_task_names:")
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
        logger.info("Group_all_collapse_task_names:")
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

def generate_DAG_info_incremental_groups(current_partition_name,
    current_partition_number,groups_of_current_partition,
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

    # state for next group added to DAG
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
    print("generate_DAG_info_incremental_groups: Leaf nodes of partitions:")
    for name in leaf_tasks_of_partitions_incremental:
        print(name + " ")
    print()

    logger.info("generate_DAG_info_incremental_groups: Partition DAG incrementally:")

    # These lists aer part of the state_info gnerated for the current 
    # partition. For partitions, only collpase will be non-empty. That is,
    # after ach task, we only have a collapsed task, no fanouts/fanins.
    # A collapse task is a fanout where there are no other fanouts/fanins.
    fanouts = []
    faninNBs = []
    fanins = []
    collapse = []
    fanin_sizes = []
    faninNB_sizes = []

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
                + " number of groups in first partition is not 1.")
        #assert:
        if not current_partition_state == Group_next_state:
            logger.debug("[Error]: Internal error:generate_DAG_info_incremental_groups"
                + " current_partition_state for first partition is not equal to"
                + " Group_next_state - both should be 1.")
          
        name_of_first_group_in_DAG = groups_of_current_partition[0]
        # a list of groups that have a fanot/fanin/collapse to this
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

        # we will heck that current_group_name is in leaf_tasks_of_groups
        # upon return to BFS()

        # generate the state for this partition/DAG task
        Group_DAG_map[Group_next_state] = state_info(name_of_first_group_in_DAG, fanouts, fanins, faninNBs, collapse, fanin_sizes, 
            faninNB_sizes, task_inputs,
            to_be_continued)
        Group_DAG_states[name_of_first_group_in_DAG] = Group_next_state

        # identify the function that will be used to execute this task
        if not use_shared_partitions_groups:
            # partition read from a file by the task
            Group_DAG_tasks[name_of_first_group_in_DAG] = PageRank_Function_Driver
        else:
            # partition is part of a shared array
            if not use_struct_of_arrays_for_pagerank:
                # using struct of arrays for fast cache access, one array
                # for each Node member, e.g., array of IDs, array of pagerank values
                # array of previous values
                Group_DAG_tasks[name_of_first_group_in_DAG] = PageRank_Function_Driver_Shared 
            else:
                # using a single array of Nodes
                Group_DAG_tasks[name_of_first_group_in_DAG] = PageRank_Function_Driver_Shared_Fast  

        logger.info("generate_DAG_info_incremental_groups: Group_DAG_map[current_partition_state]: " + str(Group_DAG_map[current_partition_state] ))

        # Note: setting version number and to_be_continued in generate_DAG_for_groups()
        # Note: setting number of tasks in in generate_DAG_for_groups()
        DAG_info = generate_DAG_for_groups(to_be_continued)

        logger.info("generate_DAG_info_incremental_groups: returning from generate_DAG_info_incremental_groups for"
            + " group " + str(name_of_first_group_in_DAG))
        
        Group_next_state += 1
        
        return DAG_info

    else:
        for group_name in groups_of_current_partition:
            senders = Group_receivers.get(group_name) 
            if (senders == None) or (len(senders) == 0):
                # (Note: Not sure whether we can have a length 0 senders, 
                # for current_partition_name. That is, we only create a 
                # senders when we get the first sender.
                #
                # Note: This is not partition 1.

                if not senders == None and len(senders) == 0:
                    logger.error("[Error]: Internal Error: generate_DAG_info_incremental_partitions:"
                        + " partition has a senders with length 0.")

                # This is not the first partition but it is a leaf partition, which means
                # it was the first partition generated by some call to BFS(), i.e., 
                # it is the start of a new connected component. This also means there
                # is no collapse from the previous partition to this partition, i.e.,
                # the previous partition has no collapse task.

                # Since this is a leaf node (it has no predecessor) we will need to add 
                # this partition to the work queue or start a new lambda for it (
                # like the DAG_executor_driver does. (Note that since this partition has
                # no predecessor, no worker or lambda can enable this task via a fanout, collapse,
                # or fanin, thus we must add this partition/task as work explicitly ourselves.)
                # This is done by the caller (BFS()) of this method. BFS() does this only when 
                # it actually deposits a new DAG_info since workers should not get this work 
                # until the DAG_info is available. 
            
                # Mark this partition as a leaf task. If any more of these leaf task 
                # partitions are found they will accumulate in these lists. BFS()
                # uses these lists to identify leaf tasks - when BFS generates an 
                # incremental DAG_info, it adds work to the work queue or starts a
                # lambda for each leaf task that is not the first partition. The
                # first partition is always a leaf task and it is handled by the 
                # DAG_executor_driver.

                # Same as for leaf task group 1 above
                Group_DAG_leaf_tasks.append(group_name)
                Group_DAG_leaf_task_start_states.append(Group_next_state)
                task_inputs = ()
                Group_DAG_leaf_task_inputs.append(task_inputs)

                previous_partition_state = current_partition_state - 1

                groups_of_previous_partition = groups_of_partitions[previous_partition_state-1]
                for group_name in groups_of_previous_partition:

                    state_info_previous_group = Group_DAG_map[group_name]

                    # Groups in previous partition do not have a collapse to a group in current partition
                    # so no collapse is added to state_info_previous_group; however,
                    # groups in previous partition are now complete so TBC is set to False.
                    # Note: The current partition cannot be partition 1.
                    state_info_previous_group.ToBeContinued = False
                    logger.info("generate_DAG_info_incremental_groups: new connected component for group "
                        + group_name + ", the state_info_previous_group for this group " 
                        + " after update TBC is: " 
                        + str(state_info_previous_group))

                    # generate state in DAG
                    Group_DAG_map[Group_next_state] = state_info(group_name, fanouts, fanins, faninNBs, collapse, fanin_sizes, 
                        faninNB_sizes, task_inputs,
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

                    logger.info("generate_DAG_info_incremental_groups: Group_DAG_map[Group_next_state]: " + str(Group_DAG_map[Group_next_state] ))

                ## save current name as previous name. If this is partition PRi_1
                ## we cannot just use PRi-1_1 since the name might have an L at the
                ## end to mark it as a loop partition. So save the name so we have it.
                #Group_DAG_previous_partition_name = current_partition_name

                DAG_info = generate_DAG_for_groups(to_be_continued)

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
                """
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

                logger.info("generate_DAG_info_incremental_groups: returning from generate_DAG_info_incremental_groups for"
                    + " partition " + str(current_partition_name))
    
            else: # current_partition_number >= 2
                # This is not the first partition and it is not a leaf partition.

#rhc: ToDo: This assert does not hold for groups
                ## assert: For partitions, there can be only one sender, i.e., each 
                ## task is a collpased task of the previous task: A->B->C..
                #if len(senders) != 1:
                #    logger.error("[Error]: Internal Error: generate_DAG_info_incremental_partitions: using partitions and a"
                #        + " partition has more than one sending partition.")
                #    sender = next(iter(senders)) # sender is first and only element in set
                #    #assert: the sender should be equal to the previous_partition_name
                #    if not sender == Group_DAG_previous_partition_name:
                #        logger.error("[Error]: Internal Error: generate_DAG_info_incremental_partitionsusing partitions and"
                #           + " the sender for a partition is not previous_partition_name.")

                # generate current partition's input - current partiton rceives its 
                # only input from the previous partition. 
                # current partition's input is referred to using the previous
                # partition name and the current partition name, e.g.,
                # "PR1_1-PR2_1". In general, for pagerank, a task can output 
                # different outputs to different tasks, So PR1_1 can have outputs
                # PR1_1-PR2_1, PR1_1-PR3_1, etc. For partitions, a task has a
                # single output to the next partition, if any.

#rhc: ToDo: need groups of previous partition that output to me
                qualified_name = str(Group_DAG_previous_partition_name) + "-" + str(current_partition_name)
                qualified_names = set()
                qualified_names.add(qualified_name)
                # set has a single name in it, e.g., for PR2_1, the task input
                # is a set with element "PR1_1-PR2_1"
                task_inputs = tuple(qualified_names)

#rhc: ToDo: do fanin/fanout/faninNB/collapse
                # add a collapse to the state for the previous partiton to 
                # the current partition. This is not a leaf partition so it
                # definitely has a previous partition.
                previous_state = current_partition_state - 1
                # state for previous partition
                state_info_previous_state = Group_DAG_map[previous_state]

                # keeping list of all collpased task names. Add new collpase.
                Group_all_collapse_task_names.append(current_partition_name)

                # add a collapse to this current partition
                collapse_of_previous_state = state_info_previous_state.collapse
                collapse_of_previous_state.append(current_partition_name)

                # previous partition is now complete - it is not complete
                # until we know who its collapse partition is, which we don't
                # now until we process that partition as the current partition.
                state_info_previous_state.ToBeContinued = False
                logger.info("generate_DAG_info_incremental_groups: for current partition, the previous_state_info after update collpase and TBC: " 
                    + str(state_info_previous_state))

                # generate DAG information
                Group_DAG_map[Group_next_state] = state_info(group_name, fanouts, fanins, faninNBs, collapse, fanin_sizes, 
                    faninNB_sizes, task_inputs,
                    # to_be_continued parameter can be true or false
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

                logger.info("generate_DAG_info_incremental_groups: generate_DAG_info_incremental_groups for"
                    + " group " + str(group_name))
                logger.info("generate_DAG_info_incremental_groups: Group_DAG_map[Group_next_state]: " + str(Group_DAG_map[Group_next_state] ))

                ## save current_partition_name as previous_partition_name so we
                ## can access previous_partition_name on the next call.
                #Group_DAG_previous_partition_name = current_partition_name

                DAG_info = generate_DAG_for_groups(to_be_continued)

                DAG_info_DAG_map = DAG_info.get_DAG_map()

                # The DAG_info object is shared between this DAG_info generator
                # and the DAG_executor, i.e., we execute the DAG generated so far
                # while we generate the next incremental DAGs. The current 
                # state is part of the DAG given to the DAG_executor and we 
                # will modify the current state when we generate the next DAG.
                # (We modify the collapse list and the toBeContiued  of the state.)
                # So we do not share the current state object, that is the 
                # ADG_info given to the DAG_executor has a state_info reference
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
                copy_of_state_info_of_current_group_state = copy.deepcopy(state_info_of_current_group_state)

                # give the copy to the DAG_map given to the DAG_executor. Now
                # the DAG_executor and the DG_generator will be using different 
                # state_info objects 
                DAG_info_DAG_map[Group_next_state] = copy_of_state_info_of_current_group_state

                # this used to test the deep copy - modify the state info
                # of the generator and make sure this modification does 
                # not show up in the state_info object given to the DAG_executor.
                """
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

                logger.info("generate_DAG_info_incremental_groups: returning from generate_DAG_info_incremental_groups for"
                    + " group " + str(group_name))

            Group_next_state += 1  
    return DAG_info