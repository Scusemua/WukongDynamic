import logging
#import os
import copy

import cloudpickle
from .DAG_info import DAG_Info
from .DFS_visit import state_info
from .BFS_pagerank import PageRank_Function_Driver, PageRank_Function_Driver_Shared
from .BFS_Shared import PageRank_Function_Driver_Shared_Fast
from .DAG_executor_constants import use_shared_partitions_groups
from .DAG_executor_constants import use_struct_of_arrays_for_pagerank

from .BFS_generate_DAG_info import Partition_senders, Partition_receivers
from .BFS_generate_DAG_info import leaf_tasks_of_partitions_incremental


logger = logging.getLogger(__name__)

logger.setLevel(logging.ERROR)
#logger.setLevel(logging.INFO)
formatter = logging.Formatter('[%(asctime)s] [%(module)s] [%(processName)s] [%(threadName)s]: %(message)s')
ch = logging.StreamHandler()
ch.setLevel(logging.ERROR)
#ch.setLevel(logging.INFO)
ch.setFormatter(formatter)
logger.addHandler(ch)

# Global variables for DAG_info. These are updated as the DAG
# is incrementally generated. 
# Note: We generate a DAG but we may or may not oublish it. That is
# we may only "publish" every ith DAG that is generated. Value i is
# controlled by the constant incremental_DAG_deposit_interval
Partition_all_fanout_task_names = []
Partition_all_fanin_task_names = []
Partition_all_faninNB_task_names = []
Partition_all_collapse_task_names = []
Partition_all_fanin_sizes = []
Partition_all_faninNB_sizes = []
Partition_DAG_leaf_tasks = []
Partition_DAG_leaf_task_start_states = []
# no inputs for leaf tasks
Partition_DAG_leaf_task_inputs = []
# maps task name to an integer ID for that task
Partition_DAG_states = {}
# maps integer ID of a task to the state for that task; the state 
# contains the fanin/fanout information for the task.
Partition_DAG_map = {}
# references to the code for the tasks
Partition_DAG_tasks = {}

# version of DAG, incremented for each DAG generated
Partition_DAG_version_number = 0
# Saving current_partition_name as previous_partition_name at the 
# end. We cannot just subtract one, e.g. PR3_1 becomes PR2_1 since
# the name of partition 2 might actually be PR2_1L, so we need to 
# save the actual name "PR2_1L" and retrive it when we process PR3_1
Partition_DAG_previous_partition_name = "PR1_1"
# number of tasks in the DAG
Partition_DAG_number_of_tasks = 0
# the tasks in the last partition of a generated DAG may be incomplete,
# which means we cannot execute these tasks until the next DAG is 
# incrementally published.
Partition_DAG_number_of_incomplete_tasks = 0

# Called by generate_DAG_info_incremental_partitions below to generate 
# the DAG_info object when we are using a DAG of partitions.
def generate_DAG_for_partitions(to_be_continued,number_of_incomplete_tasks):
    global Partition_all_fanout_task_names
    global Partition_all_fanin_task_names
    global Partition_all_faninNB_task_names
    global Partition_all_collapse_task_names
    global Partition_all_fanin_sizes
    global Partition_all_faninNB_sizes
    global Partition_DAG_leaf_tasks
    global Partition_DAG_leaf_task_start_states
    # no inputs for leaf tasks
    global Partition_DAG_leaf_task_input
    global Partition_DAG_map
    global Partition_DAG_states
    global Partition_DAG_tasks

    # version of DAG, incremented for each DAG generated
    global Partition_DAG_version_number
    # Saving current_partition_name as previous_partition_name at the 
    # end. We cannot just subtract one, e.g. PR3_1 becomes PR2_1 since
    # the name of partition 2 might actually be PR2_1L, so we need to 
    # save the actual name "PR2_1L" and retrive it when we process PR3_1
    global Partition_DAG_previous_partition_name
    global Partition_DAG_number_of_tasks
    global Partition_DAG_number_of_incomplete_tasks

    # for debugging
    show_generated_DAG_info = True

    """
    #rhc: ToDo: copy.copy vs copy.deepcopy(): Need a copy
    # but the only read-write shared object is the state_info of
    # the previous state, for which we either change only the TBC or we 
    # change the TBC and the collapse.

        # add a collapse for this current partitio to the previous state
        previous_state = current_state - 1
        state_info_previous_state = Partition_DAG_map[previous_state]
        Partition_all_collapse_task_names.append(current_partition_name)
        collapse_of_previous_state = state_info_previous_state.collapse
        collapse_of_previous_state.append(current_partition_name)

        # previous partition is now complete
        state_info_previous_state.ToBeContinued = False

    """
    
    # construct a dictionary of DAG information 
    logger.info("")
    DAG_info_dictionary = {}
    DAG_info_dictionary["DAG_map"] = Partition_DAG_map
    DAG_info_dictionary["DAG_states"] = Partition_DAG_states
    DAG_info_dictionary["DAG_leaf_tasks"] = Partition_DAG_leaf_tasks
    DAG_info_dictionary["DAG_leaf_task_start_states"] = Partition_DAG_leaf_task_start_states
    DAG_info_dictionary["DAG_leaf_task_inputs"] = Partition_DAG_leaf_task_inputs
    DAG_info_dictionary["all_fanout_task_names"] = Partition_all_fanout_task_names
    DAG_info_dictionary["all_fanin_task_names"] = Partition_all_fanin_task_names
    DAG_info_dictionary["all_faninNB_task_names"] = Partition_all_faninNB_task_names
    DAG_info_dictionary["all_collapse_task_names"] = Partition_all_collapse_task_names
    DAG_info_dictionary["all_fanin_sizes"] = Partition_all_fanin_sizes
    DAG_info_dictionary["all_faninNB_sizes"] = Partition_all_faninNB_sizes
    DAG_info_dictionary["DAG_tasks"] = Partition_DAG_tasks

    # These key/value pairs were added for incremental DAG generation.

    # If there is only one partition in the DAG then it is complete and is version 1.
    # It is returned above. Otherwise, version 1 is the DAG_info with partitions
    # 1 and 2, where 1 is complete and 2 is complete or incomplete.
    Partition_DAG_version_number += 1
    # if the last partition has incomplete information, then the DAG is 
    # incomplete. When partition i is added to the DAG, it is incomplete
    # unless it is the last partition in the DAG). It becomes complete
    # when we add partition i+1 to the DAG. (So partition i needs information
    # that is generated when we create partition i+1. The  
    # nodes of partition i can ony have children that are in partition 
    # i or partition i+1. We need to know partition i's children
    # in order for partition i to be complete. Partition i's childen
    # are discovered while generating partition i+1) 
    Partition_DAG_is_complete = not to_be_continued # to_be_continued is a parameter
    # number of tasks in the current incremental DAG, including the
    # incomplete last partition, if any.
    Partition_DAG_number_of_tasks = len(Partition_DAG_tasks)
    # For partitions, this is at most 1. When we are generating a DAG
    # of groups, there may be many groups in the incomplete last
    # partition and they will all be considered to be incomplete.
    Partition_DAG_number_of_incomplete_tasks = number_of_incomplete_tasks
    DAG_info_dictionary["DAG_version_number"] = Partition_DAG_version_number
    DAG_info_dictionary["DAG_is_complete"] = Partition_DAG_is_complete
    DAG_info_dictionary["DAG_number_of_tasks"] = Partition_DAG_number_of_tasks
    DAG_info_dictionary["DAG_number_of_incomplete_tasks"] = Partition_DAG_number_of_incomplete_tasks

#rhc: Note: we are saving all the incemental DAG_info files for debugging but 
#     we probably want to turn this off otherwise.

    # filename is based on version number - Note: for partition, say 3, we
    # have output the DAG_info with partitions 1 and 2 as version 1 so 
    # the DAG_info for partition 3 will have partitions 1, 2, and 3 and will
    # be version 2 but named "DAG_info_incremental_Partition_3"
    file_name_incremental = "./DAG_info_incremental_Partition_" + str(Partition_DAG_version_number) + ".pickle"
    with open(file_name_incremental, 'wb') as handle:
        cloudpickle.dump(DAG_info_dictionary, handle) #, protocol=pickle.HIGHEST_PROTOCOL)  

    num_fanins = len(Partition_all_fanin_task_names)
    num_fanouts = len(Partition_all_fanout_task_names)
    num_faninNBs = len(Partition_all_faninNB_task_names)
    num_collapse = len(Partition_all_collapse_task_names)

    # for debugging
    if show_generated_DAG_info:
        logger.info("DAG_map:")
        for key, value in Partition_DAG_map.items():
            logger.info(str(key) + ' : ' + str(value))
        logger.info("")
        logger.info("states:")        
        for key, value in Partition_DAG_states.items():
            logger.info(str(key) + ' : ' + str(value))
        logger.info("")
        logger.info("num_fanins:" + str(num_fanins) + " num_fanouts:" + str(num_fanouts) + " num_faninNBs:"
        + str(num_faninNBs) + " num_collapse:" + str(num_collapse))
        logger.info("")  
        logger.info("Note: partitions only have collapse sets.")
        logger.info("Partition_all_fanout_task_names:")
        for name in Partition_all_fanout_task_names:
            logger.info(name)
        logger.info("all_fanin_task_names:")
        for name in Partition_all_fanin_task_names :
            logger.info(name)
        logger.info("all_fanin_sizes:")
        for s in Partition_all_fanin_sizes :
            logger.info(s)
        logger.info("all_faninNB_task_names:")
        for name in Partition_all_faninNB_task_names:
            logger.info(name)
        logger.info("all_faninNB_sizes:")
        for s in Partition_all_faninNB_sizes:
            logger.info(s)
        logger.info("Partition_all_collapse_task_names:")
        for name in Partition_all_collapse_task_names:
            logger.info(name)
        logger.info("")
        logger.info("leaf task start states:")
        for start_state in Partition_DAG_leaf_task_start_states:
            logger.info(start_state)
        logger.info("")
        logger.info("DAG_tasks:")
        for key, value in Partition_DAG_tasks.items():
            logger.info(str(key) + ' : ' + str(value))
        logger.info("")
        logger.info("DAG_leaf_tasks:")
        for task_name in Partition_DAG_leaf_tasks:
            logger.info(task_name)
        logger.info("")
        logger.info("DAG_leaf_task_inputs:")
        for inp in Partition_DAG_leaf_task_inputs:
            logger.info(inp)
        logger.info("")
        logger.info("DAG_version_number:")
        logger.info(Partition_DAG_version_number)
        logger.info("")
        logger.info("DAG_is_complete:")
        logger.info(Partition_DAG_is_complete)
        logger.info("")
        logger.info("DAG_number_of_tasks:")
        logger.info(Partition_DAG_number_of_tasks)
        logger.info("")
        logger.info("DAG_number_of_incomplete_tasks:")
        logger.info(Partition_DAG_number_of_incomplete_tasks)
        logger.info("")

    # for debugging
    # read file file_name_incremental just written and display contents 
    if False:
        DAG_info_partition_read = DAG_Info.DAG_info_fromfilename(file_name_incremental)
        
        DAG_map = DAG_info_partition_read.get_DAG_map()
        # these are not displayed
        all_collapse_task_names = DAG_info_partition_read.get_all_collapse_task_names()
        # Note: prefixing name with '_' turns off th warning about variabel not used
        _all_fanin_task_names = DAG_info_partition_read.get_all_fanin_task_names()
        _all_faninNB_task_names = DAG_info_partition_read.get_all_faninNB_task_names()
        _all_faninNB_sizes = DAG_info_partition_read.get_all_faninNB_sizes()
        _all_fanout_task_names = DAG_info_partition_read.get_all_fanout_task_names()
        # Note: all fanout_sizes is not needed since fanouts are fanins that have size 1
        DAG_states = DAG_info_partition_read.get_DAG_states()
        DAG_leaf_tasks = DAG_info_partition_read.get_DAG_leaf_tasks()
        DAG_leaf_task_start_states = DAG_info_partition_read.get_DAG_leaf_task_start_states()
        DAG_tasks = DAG_info_partition_read.get_DAG_tasks()

        DAG_leaf_task_inputs = DAG_info_partition_read.get_DAG_leaf_task_inputs()

        Partition_DAG_is_complete = DAG_info_partition_read.get_DAG_info_is_complete()
        DAG_version_number = DAG_info_partition_read.get_DAG_version_number()
        DAG_number_of_tasks = DAG_info_partition_read.get_DAG_number_of_tasks()
        DAG_number_of_incomplete_tasks = DAG_info_partition_read.get_DAG_number_of_incomplete_tasks()

        logger.info("")
        logger.info("DAG_info partition after read:")
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
            logger.info(Partition_DAG_is_complete)
            logger.info("")
            logger.info("DAG_number_of_tasks:")
            logger.info(DAG_number_of_tasks)
            logger.info("")
            logger.info("DAG_number_of_incomplete_tasks:")
            logger.info(DAG_number_of_incomplete_tasks)
            logger.info("")

    # create the DAG_info object from the dictionary
    # Note: The DAG_map in this DAG_info is a shallow copy of the
    # DAG_map for the DAG info that we are maintaining during
    # incremental ADG generation. So the DAG_executor (who rceives
    # this DAG_info object will be using a DAG_map rference that 
    # is different from Partition_DAG_map. (We put Partition_DAG_map
    # in a DAG_info_dctionary and give this dictionary to DAG_info.__init__()
    # and init makes a shallow copy of DAG_info_dictionary['DAG_map'])
    # for the DAG_map in the DAG_info object returned here.
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
def generate_DAG_info_incremental_partitions(current_partition_name,current_partition_number,to_be_continued):
# to_be_continued is True if num_nodes_in_partitions < num_nodes, which means that incremeental DAG generation
# is not complete (some gtaph nodes are not in any partition.)

    global Partition_all_fanout_task_names
    global Partition_all_fanin_task_names
    global Partition_all_faninNB_task_names
    global Partition_all_collapse_task_names
    global Partition_all_fanin_sizes
    global Partition_all_faninNB_sizes
    global Partition_DAG_leaf_tasks
    global Partition_DAG_leaf_task_start_states
    # no inputs for leaf tasks
    global Partition_DAG_leaf_task_input
    global Partition_DAG_map
    global Partition_DAG_states
    global Partition_DAG_tasks

    # version of DAG, incremented for each DAG generated
    global Partition_DAG_version_number
    # Saving current_partition_name as previous_partition_name at the 
    # end. We cannot just subtract one, e.g. PR3_1 becomes PR2_1 since
    # the name of partition 2 might actually be PR2_1L, so we need to 
    # save the actual name "PR2_1L" and retrive it when we process PR3_1
    global Partition_DAG_previous_partition_name
    global Partition_DAG_number_of_tasks

    logger.info("generate_DAG_info_incremental_partitions: to_be_continued: " + str(to_be_continued))
    logger.info("generate_DAG_info_incremental_partitions: current_partition_number: " + str(current_partition_number))

    # for debugging
    # we track the edges between tasks. If task A has a fanin/fanout
    # to task B then A is the "sender" and B is the "receiver". When 
    # we generate an incemental DAG, we build it using these senders
    # and receivers.
    print()
    print("generate_DAG_info_incremental_partitions: Partition_senders:")
    for sender_name,receiver_name_set in Partition_senders.items():
        print("sender:" + sender_name)
        print("receiver_name_set:" + str(receiver_name_set))
    print()
    print()
    print("generate_DAG_info_incremental_partitions: Partition_receivers:")
    for receiver_name,sender_name_set in Partition_receivers.items():
        print("receiver:" + receiver_name)
        print("sender_name_set:" + str(sender_name_set))
    print()
    print()
    print("generate_DAG_info_incremental_partitions: Leaf nodes of partitions:")
    for name in leaf_tasks_of_partitions_incremental:
        print(name + " ")
    print()

    logger.info("generate_DAG_info_incremental_partitions: Partition DAG incrementally:")

    # These lists are part of the state_info gnerated for the current 
    # partition. For partitions, only collpase will be non-empty. That is,
    # after ach task, we only have a collapsed task, no fanouts/fanins.
    # A collapse task is a fanout where there are no other fanouts/fanins.
    fanouts = []
    faninNBs = []
    fanins = []
    collapse = []
    fanin_sizes = []
    faninNB_sizes = []

    # in the DAG_map, partition i has number i
    current_state = current_partition_number
    # partition 1 is a special case, it does not access the previous 
    # state as states start with 1
    previous_state = current_partition_number-1
    #previous_partition_name = "PR" + str(current_partition_number-1) + "_1"

    # a list of partitions that have a fanot/fanin/collapse to the 
    # current partition. (They "send" to the current partition which "receives".)
    senders = Partition_receivers.get(current_partition_name)
    # Note: a partition that starts a new connected component (which is 
    # the first partition collected on a call to BFS(), of which there 
    # may be many calls if the graph is not connected) is a leaf
    # node and thus has no senders. This is true about partition 1 and
    # this is asserted by the caller (BFS()) of this method.

    if current_partition_number == 1:
        # Partition 1 is a leaf; so there is no previous partition
        # i.e., sender

        #assert:
        if not senders == None:
            logger.error("[Error]: Internal error: def generate_DAG_info_incremental_partitions"
                + " leaf node has non-None senders.")
        
        # record DAG information 
        # save leaf task name - we use task name as the key in DAG_map
        # to get the state information about the task, i.e., its
        # fannis/fanouts
        Partition_DAG_leaf_tasks.append(current_partition_name)
        # save leaf task state number
        Partition_DAG_leaf_task_start_states.append(current_state)
        # leaf tasks have no input
        task_inputs = ()
        # When we execute the DAG, we start execution by excuting 
        # these leaf tasks
        Partition_DAG_leaf_task_inputs.append(task_inputs)

        # we will heck that current_partition_name is in leaf_tasks_of_partitions
        # upon return to BFS()

        # generate the state for this partition/DAG task
        Partition_DAG_map[current_state] = state_info(current_partition_name, fanouts, fanins, faninNBs, collapse, fanin_sizes, 
            faninNB_sizes, task_inputs,
            to_be_continued,
            # We do not know whether this first partition will have fanout_fanin_faninNB_collapse_groups_are_ToBeContinued
            # that are incomplete until we process the 2nd partition, except if to_be_continued
            # is False in which case there are no more partitions and no fanout_fanin_faninNB_collapse_groups_are_ToBeContinued
            # that are incomplete. If to_be_continued is True then we set fanout_fanin_faninNB_collapse_groups_are_ToBeContinued
            # to True but we may change this value when we process partition 2.
            to_be_continued)
        # we map task name to its integer ID. We use this ID as a key
        # in DAG_map which maps task IDs to task states.
        Partition_DAG_states[current_partition_name] = current_state

        # identify the function that will be used to execute this task
        if not use_shared_partitions_groups:
            # the partition of graph nodes for this task will be read 
            # from a file when the task is executed. 
            Partition_DAG_tasks[current_partition_name] = PageRank_Function_Driver
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
                Partition_DAG_tasks[current_partition_name] = PageRank_Function_Driver_Shared 
            else:
                # using a single array of Nodes
                # Function to compute pagerank values when using array of structs
                Partition_DAG_tasks[current_partition_name] = PageRank_Function_Driver_Shared_Fast  

        logger.info("generate_DAG_info_incremental_partitions: Partition_DAG_map[current_state]: " + str(Partition_DAG_map[current_state] ))

        # Note: setting version number and to_be_continued of DAG_info in generate_DAG_for_partitions()
        # Note: setting number of tasks of DAG_info in generate_DAG_for_partitions()

        # For partitions, if the DAG is not yet complete, to_be_continued
        # parameter will be TRUE, and there is one incomplete partition 
        # in the just generated version of the DAG, which is the last parition.
        if to_be_continued:
            number_of_incomplete_tasks = 1
        else:
            number_of_incomplete_tasks = 0
        
        # uses dictionry to create the DAG_info object
        DAG_info = generate_DAG_for_partitions(to_be_continued,number_of_incomplete_tasks)

        logger.info("generate_DAG_info_incremental_partitions: returning from generate_DAG_info_incremental_partitions for"
            + " partition " + str(current_partition_name))
        
        return DAG_info

    elif (senders == None):
        # (Note: Don't think we can have a length 0 senders, 
        # for current_partition_name. That is, we only create a 
        # senders when we get the first sender.
        #
        # Note: This is not partition 1.

        # assert
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

        # Same as for leaf task partition 1 above
        Partition_DAG_leaf_tasks.append(current_partition_name)
        Partition_DAG_leaf_task_start_states.append(current_state)
        task_inputs = ()
        Partition_DAG_leaf_task_inputs.append(task_inputs)

        # Now that we have generated partition i, we can complete the 
        # information for partition i-1. Get the state info 
        # of the previous partition.
        previous_state = current_state - 1
        state_info_of_previous_partition = Partition_DAG_map[previous_state]

        # Previous partition does not have a collapse to this partition
        # so no collapse is added to state_info_previous_state; however,
        # previous partition is now complete so TBC is set to False.
        # Note: This current partition cannot be partition 1.
        state_info_of_previous_partition.ToBeContinued = False
        # The last partition of a connected component does not do any fanouts/fanins/etc.
        state_info_of_previous_partition.fanout_fanin_faninNB_collapse_groups_are_ToBeContinued = False

        # we also track the partition previous to the previous partition.
        if current_partition_number > 2:
            previous_previous_state = previous_state - 1
            state_info_of_previous_previous_partition = Partition_DAG_map[previous_previous_state]
            state_info_of_previous_previous_partition.fanout_fanin_faninNB_collapse_groups_are_ToBeContinued = False

        logger.info("generate_DAG_info_incremental_partitions: new connected component for current partition "
            + str(current_partition_number) + ", the previous_state_info for previous state " 
            + str(previous_state) + " after update TBC (no collapse) is: " 
            + str(state_info_of_previous_partition))

        # generate state in DAG
        Partition_DAG_map[current_state] = state_info(current_partition_name, fanouts, fanins, faninNBs, collapse, fanin_sizes, 
            faninNB_sizes, task_inputs,
            to_be_continued,
            # We do not know whether this first group will have fanout_fanin_faninNB_collapse_groups_are_ToBeContinued
            # that are incomplete until we process the 2nd partition, except if to_be_continued
            # is False in which case there are no more partitions and no fanout_fanin_faninNB_collapse_groups_are_ToBeContinued
            # that are incomplete. If to_be_continued is True then we set fanout_fanin_faninNB_collapse_groups_are_ToBeContinued
            # to True but we may change this value when we process partition 2.
            to_be_continued)
        Partition_DAG_states[current_partition_name] = current_state

        # identify the function that will be used to execute this task
        if not use_shared_partitions_groups:
            Partition_DAG_tasks[current_partition_name] = PageRank_Function_Driver
        else:
            if not use_struct_of_arrays_for_pagerank:
                Partition_DAG_tasks[current_partition_name] = PageRank_Function_Driver_Shared 
            else:
                Partition_DAG_tasks[current_partition_name] = PageRank_Function_Driver_Shared_Fast  

        logger.info("generate_DAG_info_incremental_partitions: Partition_DAG_map[current_state]: " + str(Partition_DAG_map[current_state] ))

        # save current name as previous name. If this is partition PRi_1
        # we cannot just use PRi-1_1 since the name might have an L at the
        # end to mark it as a loop partition. So save the name so we have it.
        Partition_DAG_previous_partition_name = current_partition_name
        
        # For partitions, if the DAG is not yet complete, to_be_continued
        # parameter will be TRUE, and there is one incomplete partition 
        # in the just generated version of the DAG, which is the last parition.
        if to_be_continued:
            number_of_incomplete_tasks = 1
        else:
            number_of_incomplete_tasks = 0
        DAG_info = generate_DAG_for_partitions(to_be_continued,number_of_incomplete_tasks)

        # If we will generate another DAG make sure state_info of 
        # current partition is not read/write shared with the DAG_executor.
        # We will change this state_info when we generate the 
        # next DAG but the DAG_executor needs to read the original state
        # information, i.e., make sure the write of (new) state_info during
        # next DAG generation is not read by DAG_executor until the 
        # DAG_executor gets a new DAG, which will have this new 
        # state information. 
        # So DAG_executor(s) and DAG_generator are excuting concurrently
        # and DAG_executor is reading some of the DAG_info members that 
        # are written by the DAG_generator. Thus we give the DAG_executor
        # a private state_info that is separate from the state_info in
        # the DAG_info that we are generating. So state_info is not
        # being shared as a variable. 
        # Note: we write other DAG info variables but these are 
        # immutable so a write to immutable X creates a new X which means
        # X is not shared.
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
            state_info_of_current_state = DAG_info_DAG_map[current_state]

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
            copy_of_state_info_of_current_state = copy.deepcopy(state_info_of_current_state)

            # give the copy to the DAG_map given to the DAG_executor. Now
            # the DAG_executor and the DG_generator will be using different 
            # state_info objects 
            DAG_info_DAG_map[current_state] = copy_of_state_info_of_current_state

            # this used to test the deep copy - modify the state info
            # of the generator and make sure this modification does 
            # not show up in the state_info object given to the DAG_executor.
            """
            # modify generator's state_info 
            Partition_DAG_map[current_state].fanins.append("goo")

            # display DAG_executor's state_info objects
            logger.info("address DAG_info_DAG_map: " + str(hex(id(DAG_info_DAG_map))))
            logger.info("generate_DAG_info_incremental_partitions: DAG_info_DAG_map after state_info copy:")
            for key, value in DAG_info_DAG_map.items():
                logger.info(str(key) + ' : ' + str(value) + " addr value: " + str(hex(id(value))))

            # display generator's state_info objects
            logger.info("address Partition_DAG_map: " + str(hex(id(Partition_DAG_map))))
            logger.info("generate_DAG_info_incremental_partitions: Partition_DAG_map:")
            for key, value in Partition_DAG_map.items():
                logger.info(str(key) + ' : ' + str(value) + " addr value: " + str(hex(id(value))))

            # undo the modification to the generator's state_info
            Partition_DAG_map[current_state].fanins.clear()

            # display generator's state_info objects
            logger.info("generate_DAG_info_incremental_partitions: DAG_info_DAG_map after clear:")
            for key, value in DAG_info_DAG_map.items():
                logger.info(str(key) + ' : ' + str(value))
        
            # display DAG_executor's state_info ojects
            logger.info("generate_DAG_info_incremental_partitions: Partition_DAG_map:")
            for key, value in Partition_DAG_map.items():
                logger.info(str(key) + ' : ' + str(value))

            # logging.shutdown()
            # os._exit(0)
            """ 

        logger.info("generate_DAG_info_incremental_partitions: returning from generate_DAG_info_incremental_partitions for"
            + " partition " + str(current_partition_name))
        
        return DAG_info
    
    else: # current_partition_number >= 2

        # assert: no length 0 senders lists
        if len(senders) == 0:
            logger.error("[Error]: Internal Error: generate_DAG_info_incremental_groups:"
                + " partition has a senders with length 0.")
        # This is not the first partition and it is not a leaf partition.

        # assert: For partitions, there can be only one sender, i.e., each 
        # task is a collapsed task of the previous task: A->B->C. This means
        # A, B, and C aer executed sequentially. Each connected component 
        # in the graph has its own sequence of partitions and these sequences
        # can be executed concurrently. So the eecution of a graph with two or more 
        # components can have some parallelism.
        if len(senders) != 1:
            logger.error("[Error]: Internal Error: generate_DAG_info_incremental_partitions: using partitions and a"
                + " partition has more than one sending partition.")
            sender = next(iter(senders)) # sender is first and only element in set
            #assert: the sender should be equal to the previous_partition_name
            if not sender == Partition_DAG_previous_partition_name:
                logger.error("[Error]: Internal Error: generate_DAG_info_incremental_partitionsusing partitions and"
                    + " the sender for a partition is not previous_partition_name.")

        # generate current partition's input - current partiton rceives its 
        # only input from the previous partition. 
        # current partition's input is referred to using the previous
        # partition name and the current partition name, e.g.,
        # "PR1_1-PR2_1". In general, for pagerank, a task can output 
        # different outputs to different tasks, So PR1_1 can have outputs
        # PR1_1-PR2_1, PR1_1-PR3_1, etc. For partitions, a task has a
        # single output to the next partition, if any.
        qualified_name = str(Partition_DAG_previous_partition_name) + "-" + str(current_partition_name)
        qualified_names = set()
        qualified_names.add(qualified_name)
        # set has a single name in it, e.g., for PR2_1, the task input
        # is a set with element "PR1_1-PR2_1"
        task_inputs = tuple(qualified_names)

        # add a collapse to the state for the previous partiton to 
        # the current partition. This is not a leaf partition so it
        # definitely has a previous partition.
        previous_state = current_state - 1
        # state for previous partition
        state_info_of_previous_partition = Partition_DAG_map[previous_state]

        # keeping list of all collpased task names. Add new collpase.
        Partition_all_collapse_task_names.append(current_partition_name)

        # add a collapse to this current partition
        collapse_of_previous_partition = state_info_of_previous_partition.collapse
        collapse_of_previous_partition.append(current_partition_name)

        # previous partition is now complete - it is not complete
        # until we know who its collapse partition is, which we don't
        # now until we process that partition as the current partition.
        state_info_of_previous_partition.ToBeContinued = False
        # if the current partition is to_be_continued then it has incomplete
        # groups so we set fanout_fanin_faninNB_collapse_groups_are_ToBeContinued of the previous
        # groups to True; otherwise, we set fanout_fanin_faninNB_collapse_groups_are_ToBeContinued to False.
        # Note: state_info_of_previous_group.ToBeContinued = False inicates that the
        # previous groups are not to be continued, while
        # state_info_of_previous_group.fanout_fanin_faninNB_collapse_groups_are_ToBeContinued indicates
        # whether the previous groups have fanout_fanin_faninNB_collapse_groups_are_ToBeContinued 
        # that are to be continued, i.e., the fanout_fanin_faninNB_collapse are 
        # to groups in this current partition and whether these groups in the current
        # partiton are to be contnued is indicated by to_be_continued.
        state_info_of_previous_partition.fanout_fanin_faninNB_collapse_groups_are_ToBeContinued = to_be_continued
        
        # we track the previous partition of the previous partition
        if current_partition_number > 2:
            previous_previous_state = previous_state - 1
            state_info_of_previous_previous_partition = Partition_DAG_map[previous_previous_state]
            state_info_of_previous_previous_partition.fanout_fanin_faninNB_collapse_groups_are_ToBeContinued = False

        logger.info("generate_DAG_info_incremental_partitions: for current partition, the previous_state_info after update collpase and TBC: " 
            + str(state_info_of_previous_partition))

        # generate DAG information
        Partition_DAG_map[current_state] = state_info(current_partition_name, fanouts, fanins, faninNBs, collapse, fanin_sizes, 
            faninNB_sizes, task_inputs,
            # to_be_continued parameter can be true or false
            to_be_continued,
            # We do not know whether this first group will have fanout_fanin_faninNB_collapse_groups_are_ToBeContinued
            # that are incomplete until we process the 2nd partition, except if to_be_continued
            # is False in which case there are no more partitions and no fanout_fanin_faninNB_collapse_groups_are_ToBeContinued
            # that are incomplete. If to_be_continued is True then we set fanout_fanin_faninNB_collapse_groups_are_ToBeContinued
            # to True but we may change this value when we process partition 2.
            to_be_continued)
        Partition_DAG_states[current_partition_name] = current_state

        # identify function used to execute this pagerank task (see comments above)
        if not use_shared_partitions_groups:
            Partition_DAG_tasks[current_partition_name] = PageRank_Function_Driver
        else:
            if not use_struct_of_arrays_for_pagerank:
                Partition_DAG_tasks[current_partition_name] = PageRank_Function_Driver_Shared 
            else:
                Partition_DAG_tasks[current_partition_name] = PageRank_Function_Driver_Shared_Fast  

        logger.info("generate_DAG_info_incremental_partitions: generate_DAG_info_incremental_partitions for"
            + " partition " + str(current_partition_name))
        logger.info("generate_DAG_info_incremental_partitions: Partition_DAG_map[current_state]: " + str(Partition_DAG_map[current_state] ))

        # save current_partition_name as previous_partition_name so we
        # can access previous_partition_name on the next call.
        Partition_DAG_previous_partition_name = current_partition_name
    
        # For partitions, if the DAG is not yet complete, to_be_continued
        # parameter will be TRUE, and there is one incomplete partition 
        # in the just generated version of the DAG, which is the last parition.
        if to_be_continued:
            number_of_incomplete_tasks = 1
        else:
            number_of_incomplete_tasks = 0
        DAG_info = generate_DAG_for_partitions(to_be_continued,number_of_incomplete_tasks)

        # If we will generate another DAG make sure that the state_info of 
        # current partition is not shared with the DAG_executor
        # as we will change (write) this state_info when we generate the 
        # next DAG and this change might be concurrent with the DAG_executor
        # reading this state_info. This would be a problem since we do not 
        # want to make the reads/writes of the state_info atomic. Instead,
        # we deep copy the state_info object so we have two seperate state_info
        # objects for the current_state, i.e., the state_info object read by the DAG_executor
        # is different from the state_info object in the DAG info we are maintaining
        # for incremental ADG generation. (We add partitions to this information 
        # one by one) This ensures that the state_info object (for current_state) we write in the 
        # during next DAG generation (at which point current_state is previous_state)
        # is not the same state_info object for current_state read by DAG_executor.
        if to_be_continued:
            DAG_info_DAG_map = DAG_info.get_DAG_map()

            # Note: The DAG is generated incrementally. The current version
            # of the DAG is the value of global variables Partition_DAG_leaf_tasks,
            # Partition_DAG_leaf_task_start_states, Partition_DAG_states,   
            # Partition_DAG_map, etc. When we generate the next version of the 
            # DAG, we pack a DAG_info_dictionary with the values of these
            # variables, e.g., DAG_info_dictionary["DAG_map"] = Partition_DAG_map,
            # and pass this DAG_info_dictionary to DAG_info.__init__. Method
            # init extracts that dictionary elements into the DAG_info
            # data members, e.g., self.DAG_states = DAG_info_dictionary["DAG_states"].
            # However, init creates a shallow copy of the DAG_info_dictionary['DAG_map'] 
            # and this copy is in the DAG_info object given to the DAG_generator. Thus,
            # there are two DAG_maps, one in the DAG_info object given to
            # the DAG_excucutor and the Partition_DAG_map that is maintanied for 
            # incremental DAG generation. 
            """ In DAG_info __init__:
            if not use_incremental_DAG_generation:
                self.DAG_map = DAG_info_dictionary["DAG_map"]
            else:
                # Q: this is the same as DAG_info_dictionary["DAG_map"].copy()?
                self.DAG_map = copy.copy(DAG_info_dictionary["DAG_map"])
            """
            # Note that the copy of Partition_DAG_map is a shallow copy but we can 
            # change the vaue for a key in Partition_DAG_map without chaning the 
            # value in the other map:
            """
            old_Dict = {'name': 'Bob', 'age': 25}
            new_Dict = copy.copy(old_Dict)
            new_Dict['name'] = 'xx'
            print(old_Dict)
            # Prints {'age': 25, 'name': 'Bob'}
            print(new_Dict)
            # Prints {'age': 25, 'name': 'xx'}
            """
            # Thus, when we change the state_info object for current_state in Partition_DAG_map,
            # which we are maintaining for incremental DAG 
            # generation, we are not changing the state_info object for current_state in the 
            # DAG_map being used by the DAG_executor.
            # Note: A shallow copy of Partition_DAG_map suffices, so we avoid making
            # a deep copy of Partition_DAG_map, which would make deep copies of all
            # the DAG states in Partition_DAG_map. As shown below, we only make a deep copy 
            # of the state_info object for the current_state.
            # 
            # While we have two separate DAG_maps, one a shallow copy of the other,
            # the key/value references in the maps are the same (since we made
            # a shallow copy not a deep copy). Now we want to make sure that the
            # value references for key current_state are different in the 
            # two maps. This is ensured by making a deep copy of the state_info 
            # (value) object for current_state, so that we have two separate 
            # value objects.
            #
            # Get the state_info from the DAG_map being given to the DAG_executor
            # (which is in the DAG_info object just created and returned below). 
            # This state_info vaue object has the same reference in both maps so 
            # t does not matter which map we retrieve the state_info reference from.
            # Again, this is the state_info object for the current just processed, partition, 
            # which is incomplete (to_be_continued = True). When we process the next 
            # partition, we will set this current_partition to be complete (where
            # current_partition is referred to as the previous_partitition).
            state_info_of_current_state = DAG_info_DAG_map[current_state]

            # Note: the only parts of the state_info that can be changed 
            # are the collapse list and the to_be_continued boolean. Yet 
            # we deepcopy the entire state_info object. Note that all other
            # parts of the state_info object are empty for partitions 
            # (fanouts, fanins) except for the pagerank function, so deep copying
            # the entire state_info object is not a big deal.
            # Note: Each state_info object has a reference to the Python function that
            # will excute the task. This is how Dask does it - each task
            # has a reference to its function. For pagernk, we will use
            # the same function for all the pagerank tasks. There can be 
            # three different functions, but we could identify this 
            # function whrn we excute the task, instead of doing it above
            # and saving this same function in the DAG for each task,
            # which wastes space.

            # make a deep copy D of this state_info object. 
            copy_of_state_info_of_current_state = copy.deepcopy(state_info_of_current_state)

            # Set the state_info value object to be this deep copy.
            # Now the DAG_executor and the DAG_generator will be reading and 
            # writing, respectively, different state_info objects for 
            # current_state. (Note that current state is the integer ID of
            # the current partition being processed.)
            DAG_info_DAG_map[current_state] = copy_of_state_info_of_current_state

            # This code is used to test the deep copy - modify the state info
            # maintained by the generator and make sure this modification does 
            # not show up in the state_info object given to the DAG_executor.
            """
            # modify the fanin state info maintained by the generator.
            Partition_DAG_map[current_state].fanins.append("goo")
            
            # display DAG_executor's state_info objects
            logger.info("address DAG_info_DAG_map: " + str(hex(id(DAG_info_DAG_map))))
            logger.info("generate_DAG_info_incremental_partitions: DAG_info_DAG_map after state_info copy:")
            for key, value in DAG_info_DAG_map.items():
                logger.info(str(key) + ' : ' + str(value) + " addr value: " + str(hex(id(value))))

            # display generator's state_info objects
            logger.info("address Partition_DAG_map: " + str(hex(id(Partition_DAG_map))))
            logger.info("generate_DAG_info_incremental_partitions: Partition_DAG_map:")
            for key, value in Partition_DAG_map.items():
                logger.info(str(key) + ' : ' + str(value) + " addr value: " + str(hex(id(value))))

            # fanin values should be different for current_state
            # one with "goo" and the other empty
            
            # undo the modification to the generator's maintained state_info
            Partition_DAG_map[current_state].fanins.clear()

            # display DAG_executor's state_info objects
            logger.info("generate_DAG_info_incremental_partitions: DAG_info_DAG_map after clear:")
            for key, value in DAG_info_DAG_map.items():
                logger.info(str(key) + ' : ' + str(value))
        
            # display generator's state_info ojects
            logger.info("generate_DAG_info_incremental_partitions: Partition_DAG_map:")
            for key, value in Partition_DAG_map.items():
                logger.info(str(key) + ' : ' + str(value))

            # fanin values should be the same for current_state (empty)

            # logging.shutdown()
            # os._exit(0)
            """

        logger.info("generate_DAG_info_incremental_partitions: returning from generate_DAG_info_incremental_partitions for"
            + " partition " + str(current_partition_name))

        return DAG_info
    