import logging
import os

import cloudpickle
from .DAG_info import DAG_Info
from .DFS_visit import state_info
from .BFS_pagerank import PageRank_Function_Driver, PageRank_Function_Driver_Shared
from .BFS_Shared import PageRank_Function_Driver_Shared_Fast
from .DAG_executor_constants import use_shared_partitions_groups
from .DAG_executor_constants import use_struct_of_arrays_for_pagerank

from .BFS_generate_DAG_info import Partition_senders, Partition_receivers
from .BFS_generate_DAG_info import leaf_tasks_of_partitions


logger = logging.getLogger(__name__)

logger.setLevel(logging.INFO)
#logger.setLevel(logging.INFO)
formatter = logging.Formatter('[%(asctime)s] [%(module)s] [%(processName)s] [%(threadName)s]: %(message)s')
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
#ch.setLevel(logging.INFO)
ch.setFormatter(formatter)
logger.addHandler(ch)


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
Partition_DAG_map = {}
Partition_DAG_states = {}
Partition_DAG_tasks = {}

version_number = 0
# Saving current_partition_name as previous_partition_name at the 
# end. We cannot just subtract one, e.g. PR3_1 becomes PR2_1 since
# the name of partition 2 might actually be PR2_1L, so we need to 
# save the actual name "PR2_1L" and retrive it when we process PR3_1
previous_partition_name = "PR1_1"

def generate_DAG_info_incremental_partitions(current_partition_name,current_partition_number,to_be_continued):
# to_be_continued is True if num_nodes_in_partitions < num_nodes

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

    global version_number
    global previous_partition_name

    logger.info("to_be_continued: " + str(to_be_continued))
    logger.info("current_partition_number: " + str(current_partition_number))

    print()
    print("Partition_senders:")
    for sender_name,receiver_name_set in Partition_senders.items():
        print("sender:" + sender_name)
        print("receiver_name_set:" + str(receiver_name_set))
    print()
    print()
    print("Partition_receivers:")
    for receiver_name,sender_name_set in Partition_receivers.items():
        print("receiver:" + receiver_name)
        print("sender_name_set:" + str(sender_name_set))
    print()
    print()
    print("Leaf nodes of partitions:")
    for name in leaf_tasks_of_partitions:
        print(name + " ")
    print()

    logger.info("Partition DAG incrementally:")

    fanouts = []
    faninNBs = []
    fanins = []
    collapse = []
    fanin_sizes = []
    faninNB_sizes = []

    current_state = current_partition_number
    previous_state = current_partition_number-1
    #previous_partition_name = "PR" + str(current_partition_number-1) + "_1"

#rhc: ToDo: There may be multiple leaf nodes
    #if current_partition_name in leaf_tasks_of_partitions:
    #  if current_partition_number == 1:
    #       going to give this to the DAG_executor_driver
    #  else:
    #       this is start of a new connected component, 
    #          so do its DAG info and will have to eventually put
    #          leaf task state in the work queue with no input,
    #          i.e., after the ADG_info is published so worker
    #          that gets work will have the DAG_info for it.
    #          Note: for lambdas we will start a lambda.

    if current_partition_number == 1:
        # partition 1 is a leaf; no previous partition
        # assert len ssfsX = None
        Partition_DAG_leaf_tasks.append(current_partition_name)
        Partition_DAG_leaf_task_start_states.append(current_state)
        task_inputs = ()
        Partition_DAG_leaf_task_inputs.append(task_inputs)

        if not current_partition_name in leaf_tasks_of_partitions:
            logger.error("partition " + current_partition_name + " is the first partition"
                + " but it is not in leaf_tasks_of_partitions.")
        else:
            # we have generated a state for leaf task senderX. 
            leaf_tasks_of_partitions.remove(current_partition_name)

        Partition_DAG_map[current_state] = state_info(current_partition_name, fanouts, fanins, faninNBs, collapse, fanin_sizes, 
            faninNB_sizes, task_inputs,
            to_be_continued)
        Partition_DAG_states[current_partition_name] = current_state

        if not use_shared_partitions_groups:
            Partition_DAG_tasks[current_partition_name] = PageRank_Function_Driver
        else:
            if not use_struct_of_arrays_for_pagerank:
                Partition_DAG_tasks[current_partition_name] = PageRank_Function_Driver_Shared 
            else:
                Partition_DAG_tasks[current_partition_name] = PageRank_Function_Driver_Shared_Fast  

        logger.info("BFS: returning from generate_DAG_info_incremental_partitions for"
            + " partition " + str(current_partition_name))
        logger.info("Partition_DAG_map[current_state]: " + str(Partition_DAG_map[current_state] ))

        # Note: setting version number and is complete below

    else: # this is not the first partition

        # Process current partition 
        qualified_name = str(previous_partition_name) + "-" + str(current_partition_name)
        qualified_names = set()
        qualified_names.add(qualified_name)
        # sender_set_for_senderX provides input for senderX
        task_inputs = tuple(qualified_names)

        senders = Partition_receivers.get(current_partition_name)
        # a partition that starts a new connected component (which is 
        # the first partition collected on a call to BFS() of whihc there 
        # may be many calls if the graph is not connected) is a leaf
        # node and thus has no senders.)
        if (not senders == None) and len(senders) > 0:
            # assert:
            if len(senders) != 1:
                logger.error("[Error]: Internal Error:  using partitions and a"
                    + " partition has more than one sending partition.")
            #assert:
            sender = next(iter(senders)) # first and only element in set
            if not sender == previous_partition_name:
                logger.error("[Error]: Internal Error:  using partitions and"
                    + " the sender for a partition is not previous_partition_name.")
           
            previous_state = current_state - 1
            state_info_previous_state = Partition_DAG_map[previous_state]
            Partition_all_collapse_task_names.append(current_partition_name)
            collapse_of_previous_state = state_info_previous_state.collapse
            collapse_of_previous_state.append(current_partition_name)
            # previous partition is now complete
            state_info_previous_state.ToBeContinued = False
            logger.info("for current partition, the previous_state_info after update collpase and TBC: " 
                + str(state_info_previous_state))
        else:
            # the current partition is not partiton 1 but it starts a new
            # connected component and so there is no collapse from the previousr 
            # partition to this current_partition. This is a leaf node (it has no 
            # preecssor) so we will need to add this partition to the 
            # work queue or start a new lambda for it (like the DAG_executor_driver
            # does.)
#rhc: ToDo:
            # ToDo: If we return this DAG_info with this partition that is a
            # new leaf node (to BFS) then BFS will need to eventually add work
            # to the work queue representing this new leaf node? But does this
            # only when it actually deposits the DAG_info since workers should not
            # get this work until the DAG_info is available. (So BFS will need to
            # remember that it needs to add work to the work queue for this leaf node
            # so BFS wil need to know that there is a new leaf node (by watching
            # the length of leaf_tasks_of_partitions? and saving the leaf tasks
            # as it gets them or getting the new ones from DAG_info?))
#rhc ToDo: 
            pass

        Partition_DAG_map[current_state] = state_info(current_partition_name, fanouts, fanins, faninNBs, collapse, fanin_sizes, 
            faninNB_sizes, task_inputs,
            # to_be_continued parameter can be true or false
            to_be_continued)
        Partition_DAG_states[current_partition_name] = current_state

        if not use_shared_partitions_groups:
            Partition_DAG_tasks[current_partition_name] = PageRank_Function_Driver
        else:
            if not use_struct_of_arrays_for_pagerank:
                Partition_DAG_tasks[current_partition_name] = PageRank_Function_Driver_Shared 
            else:
                Partition_DAG_tasks[current_partition_name] = PageRank_Function_Driver_Shared_Fast  


        logger.info("BFS: generate_DAG_info_incremental_partitions for"
            + " partition " + str(current_partition_name))
        logger.info("Partition_DAG_map[current_state]: " + str(Partition_DAG_map[current_state] ))

        # save current_partition_name as previous_partition_name so we
        # can access previous_partition_name on the next call.
        previous_partition_name = current_partition_name

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

    # If there is only one partition n the DAG then it is complete and is version 1.
    # It is returned above. Otherwise, version 1 is the ADG_info with partitions
    # 1 and 2, where 1 is complete and 2 is complete or incmplete.
    version_number += 1
    DAG_info_version_number = version_number
    DAG_info_is_complete = not to_be_continued
    DAG_info_dictionary["version_number"] = DAG_info_version_number
    DAG_info_dictionary["DAG_info_is_complete"] = DAG_info_is_complete

    # filename is based on version number - Note: for partition, say 3, we
    # have output the DAG_info with partitions 1 and 2 as version 1 so 
    # the DAG_info for partition 3 will have partitions 1, 2, and 3 and will
    # be version 2.
    file_name_incremental = "./DAG_info_incremental_Partition_" + str(DAG_info_version_number) + ".pickle"
    with open(file_name_incremental, 'wb') as handle:
        cloudpickle.dump(DAG_info_dictionary, handle) #, protocol=pickle.HIGHEST_PROTOCOL)  

#rhc: Do this? We only read at start.

    #if not use_page_rank_group_partitions:
    #    file_name = "./DAG_info.pickle"
    #    with open(file_name, 'wb') as handle:
    #        cloudpickle.dump(DAG_info, handle) #, protocol=pickle.HIGHEST_PROTOCOL)  

    num_fanins = len(Partition_all_fanin_task_names)
    num_fanouts = len(Partition_all_fanout_task_names)
    num_faninNBs = len(Partition_all_faninNB_task_names)
    num_collapse = len(Partition_all_collapse_task_names)

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
    logger.info(DAG_info_version_number)
    logger.info("")
    logger.info("DAG_info_is_complete:")
    logger.info(DAG_info_is_complete)
    logger.info("")

    DAG_info_partition_read = DAG_Info.DAG_info_fromfilename(file_name_incremental)
    
    DAG_map = DAG_info_partition_read.get_DAG_map()
    #all_fanin_task_names = DAG_info_partition_read.get_all_fanin_task_names()
    #all_faninNB_task_names = DAG_info_partition_read.get_all_faninNB_task_names()
    #all_faninNB_sizes = DAG_info_partition_read.get_all_faninNB_sizes()
    #all_fanout_task_names = DAG_info_partition_read.get_all_fanout_task_names()
    # Note: all fanout_sizes is not needed since fanouts are fanins that have size 1
    DAG_states = DAG_info_partition_read.get_DAG_states()
    DAG_leaf_tasks = DAG_info_partition_read.get_DAG_leaf_tasks()
    DAG_leaf_task_start_states = DAG_info_partition_read.get_DAG_leaf_task_start_states()
    DAG_tasks = DAG_info_partition_read.get_DAG_tasks()

    DAG_leaf_task_inputs = DAG_info_partition_read.get_DAG_leaf_task_inputs()

    DAG_info_is_complete = DAG_info_partition_read.get_DAG_info_is_complete()
    DAG_info_version_number = DAG_info_partition_read.get_DAG_version_number()

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
        logger.info(DAG_info_version_number)
        logger.info("")
        logger.info("DAG_info_is_complete:")
        logger.info(DAG_info_is_complete)
        logger.info("")

    #if not to_be_continued:
    #   logging.shutdown()
    #    os._exit(0)

    DAG_info = DAG_Info.DAG_info_fromdictionary(DAG_info_dictionary)
    return  DAG_info