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

from .BFS_generate_DAG_info import Partition_senders, Partition_receivers
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

# Called by generate_DAG_info_incremental_partitions below to generate 
# the DAG_info object.
def generate_DAG(to_be_continued):
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

    # If there is only one partition in the DAG then it is complete and is version 1.
    # It is returned above. Otherwise, version 1 is the DAG_info with partitions
    # 1 and 2, where 1 is complete and 2 is complete or incomplete.
    version_number += 1
    DAG_info_version_number = version_number
    DAG_info_is_complete = not to_be_continued # TBC is a parameter
    DAG_info_dictionary["version_number"] = DAG_info_version_number
    DAG_info_dictionary["DAG_info_is_complete"] = DAG_info_is_complete

    # filename is based on version number - Note: for partition, say 3, we
    # have output the DAG_info with partitions 1 and 2 as version 1 so 
    # the DAG_info for partition 3 will have partitions 1, 2, and 3 and will
    # be version 2 but named "DAG_info_incremental_Partition_3"
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

    logger.info("generate_DAG_info_incremental_partitions: to_be_continued: " + str(to_be_continued))
    logger.info("generate_DAG_info_incremental_partitions: current_partition_number: " + str(current_partition_number))

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

    senders = Partition_receivers.get(current_partition_name)
    #Note: a partition that starts a new connected component (which is 
    # the first partition collected on a call to BFS(), of which there 
    # may be many calls if the graph is not connected) is a leaf
    # node and thus has no senders. This is true about partition 1 and
    # this is asserted by the caller (BFS()) of this method.

    if current_partition_number == 1:
        # Partition 1 is a leaf; no previous partition
        # assert len ssfsX = None
        Partition_DAG_leaf_tasks.append(current_partition_name)
        Partition_DAG_leaf_task_start_states.append(current_state)
        task_inputs = ()
        Partition_DAG_leaf_task_inputs.append(task_inputs)

        # Check that current_partition_name is in leaf_tasks_of_partitions
        # upon return to BFS()

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

        logger.info("generate_DAG_info_incremental_partitions: returning from generate_DAG_info_incremental_partitions for"
            + " partition " + str(current_partition_name))
        logger.info("generate_DAG_info_incremental_partitions: Partition_DAG_map[current_state]: " + str(Partition_DAG_map[current_state] ))

        # Note: setting version number and is complete below

        DAG_info = generate_DAG(to_be_continued)
        return DAG_info

    elif (senders == None) or (len(senders) == 0):
        # (Note: Not sure whether we can have a length 0 senders, 
        # for current_partition_name.)

        # assert len(senders) != 0

        # This is not the first partition but it is a leaf partition, which means
        # it was the first partition generated by some call to BFS(), i.e., 
        # it is the start of a new connected component. This also means there
        # is no collapse from the previous partition to this partition, i.e.,
        # the previous partition has no collapse task.

        # Since this is a leaf node (it has no predecessor) we will need to add 
        # this partition to the work queue or start a new lambda for it (
        # like the DAG_executor_driver does. (Noet that since this partition has
        # no predecessor, no worker or lambda can enable this task via a fanout
        # or fanin, thus we must add this partition/task as work explicitly ourselves.)
        # This is done by the caller (BFS()) of this method. BFS() does this only when 
        # it actually deposits a new DAG_info since workers should not get this work 
        # until the DAG_info is available. 
       
        # Mark this partition as a leaf task. If any moer of these leaf task 
        # partitions are found they will accumulate in these lists. BFS()
        # uses these lists to identify leaf tasks - when BFS genertes an 
        # incremental DAG_info, it adds work to the work queue or starts a
        # lambda for each leaf task.
        Partition_DAG_leaf_tasks.append(current_partition_name)
        Partition_DAG_leaf_task_start_states.append(current_state)
        task_inputs = ()
        Partition_DAG_leaf_task_inputs.append(task_inputs)

        previous_state = current_state - 1
        state_info_previous_state = Partition_DAG_map[previous_state]

        # previous partition does not have a collapse to this partition
        # so no collpase is added to state_info_previous_state; however,
        # previous partition is complete so TBC is set to False.
        state_info_previous_state.ToBeContinued = False
        logger.info("generate_DAG_info_incremental_partitions: for current partition"
            + str(current_partition_number) + ", the previous_state_info for previous state " 
            + str(previous_state) + " after update TBC (no collapse) is: " 
            + str(state_info_previous_state))

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

        logger.info("generate_DAG_info_incremental_partitions: returning from generate_DAG_info_incremental_partitions for"
            + " partition " + str(current_partition_name))
        logger.info("generate_DAG_info_incremental_partitions: Partition_DAG_map[current_state]: " + str(Partition_DAG_map[current_state] ))

        DAG_info = generate_DAG(to_be_continued)

#rhc: We need a copy of this with a new reference for the 
# previous state_info in the DAG_map.
        """
        DAG_info_dictionary = DAG_info.DAG_info_dictionary
        DAG_info_dictionary_DAG_map = DAG_info_dictionary["DAG_map"]
        copy_of_DAG_info_dictionary_DAG_map = copy.copy(DAG_info_dictionary_DAG_map)
        state_info_of_previous_state = copy_of_DAG_info_dictionary_DAG_map[previous_state]
        copy_of_state_info_previous_state = state_info(state_info_of_previous_state)
        copy_of_DAG_info_dictionary_DAG_map[previous_state] = copy_of_state_info_previous_state

        logger.info("generate_DAG_info_incremental_partitions: copy_of_DAG_info_dictionary_DAG_map after state_info copy:")
        for key, value in copy_of_DAG_info_dictionary_DAG_map.items():
            logger.info(str(key) + ' : ' + str(value))
        """

        DAG_info_DAG_map = DAG_info.get_DAG_map()
        state_info_of_previous_state = DAG_info_DAG_map[previous_state]
        copy_of_state_info_of_previous_state = state_info.state_info_fromstate_info(state_info_of_previous_state)
        copy_of_state_info_of_previous_state = copy.deepcopy(state_info_of_previous_state)
        copy_of_state_info_of_previous_state.fanins.append("foo")  
        DAG_info_DAG_map[previous_state] = copy_of_state_info_of_previous_state

#rhc: ToDo: No! they have the same maps. stat_info uses members instead of
# single dict where map is a copy of dict parm map and we change that
# member map state info here.
        
        logger.info("address DAG_info_DAG_map: " + str(hex(id(DAG_info_DAG_map))))
        logger.info("generate_DAG_info_incremental_partitions: DAG_info_DAG_map after state_info copy:")
        for key, value in DAG_info_DAG_map.items():
            logger.info(str(key) + ' : ' + str(value) + " addr " + str(hex(id(value))))

        logger.info("address Partition_DAG_map: " + str(hex(id(Partition_DAG_map))))
        logger.info("generate_DAG_info_incremental_partitions: Partition_DAG_map:")
        for key, value in Partition_DAG_map.items():
            logger.info(str(key) + ' : ' + str(value) + " addr " + str(hex(id(value))))

        copy_of_state_info_of_previous_state.fanins.clear()

        logger.info("generate_DAG_info_incremental_partitions: DAG_info_DAG_map after clear:")
        for key, value in DAG_info_DAG_map.items():
            logger.info(str(key) + ' : ' + str(value))
    
        logger.info("generate_DAG_info_incremental_partitions: Partition_DAG_map:")
        for key, value in Partition_DAG_map.items():
            logger.info(str(key) + ' : ' + str(value))

        return DAG_info
    
    else: # current_partition_number >= 2
        # This is not the first partition and it is not a leaf partition.

        # assert:
        if len(senders) != 1:
            logger.error("[Error]: Internal Error: generate_DAG_info_incremental_partitions: using partitions and a"
                + " partition has more than one sending partition.")
            #assert:
            sender = next(iter(senders)) # first and only element in set
            if not sender == previous_partition_name:
                logger.error("[Error]: Internal Error: generate_DAG_info_incremental_partitionsusing partitions and"
                    + " the sender for a partition is not previous_partition_name.")

        # Process current partition 
        qualified_name = str(previous_partition_name) + "-" + str(current_partition_name)
        qualified_names = set()
        qualified_names.add(qualified_name)
        # sender_set_for_senderX provides input for senderX
        task_inputs = tuple(qualified_names)

        # add a collapse for this current partitio to the previous state
        previous_state = current_state - 1
        state_info_previous_state = Partition_DAG_map[previous_state]
        Partition_all_collapse_task_names.append(current_partition_name)
        collapse_of_previous_state = state_info_previous_state.collapse
        collapse_of_previous_state.append(current_partition_name)

        # previous partition is now complete
        state_info_previous_state.ToBeContinued = False
        logger.info("generate_DAG_info_incremental_partitions: for current partition, the previous_state_info after update collpase and TBC: " 
            + str(state_info_previous_state))

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

        logger.info("generate_DAG_info_incremental_partitions: generate_DAG_info_incremental_partitions for"
            + " partition " + str(current_partition_name))
        logger.info("generate_DAG_info_incremental_partitions: Partition_DAG_map[current_state]: " + str(Partition_DAG_map[current_state] ))

        # save current_partition_name as previous_partition_name so we
        # can access previous_partition_name on the next call.
        previous_partition_name = current_partition_name
        DAG_info = generate_DAG(to_be_continued)

#rhc: We need a copy of this with a new reference for the 
# previous state_info in the DAG_map.

        """
        DAG_info_dictionary = DAG_info.DAG_info_dictionary
        DAG_info_dictionary_DAG_map = DAG_info_dictionary["DAG_map"]
        copy_of_DAG_info_dictionary_DAG_map = copy.copy(DAG_info_dictionary_DAG_map)
        state_info_of_previous_state = copy_of_DAG_info_dictionary_DAG_map[previous_state]
        copy_of_state_info_of_previous_state = state_info.state_info_fromstate_info(state_info_of_previous_state)
        copy_of_state_info_of_previous_state.fanins.append("foo")  
        copy_of_DAG_info_dictionary_DAG_map[previous_state] = copy_of_state_info_of_previous_state
        DAG_info_dictionary['DAG_map'] = copy_of_DAG_info_dictionary_DAG_map
        """
        DAG_info_DAG_map = DAG_info.get_DAG_map()
        state_info_of_current_state = DAG_info_DAG_map[current_state]
        #copy_of_state_info_of_previous_state = state_info.state_info_fromstate_info(state_info_of_previous_state)
#rhc: incremental: the only parts of the states that can be changed for partitions
# are the colapse set and the TBC boolean. Yet we deepcopy the entire
#state_info. Of course, everyting else is empty except for the pagerank function.
# Still, we only need a refs for the coapse set and the TBC boolean,
# which is less copying
        copy_of_state_info_of_current_state = copy.deepcopy(state_info_of_current_state)
        #copy_of_state_info_of_previous_state.fanins.append("foo")  
        DAG_info_DAG_map[current_state] = copy_of_state_info_of_current_state
        Partition_DAG_map[current_state].fanins.append("goo")
        
        logger.info("address DAG_info_DAG_map: " + str(hex(id(DAG_info_DAG_map))))
        logger.info("generate_DAG_info_incremental_partitions: DAG_info_DAG_map after state_info copy:")
        for key, value in DAG_info_DAG_map.items():
            logger.info(str(key) + ' : ' + str(value) + " addr value: " + str(hex(id(value))))

        logger.info("address Partition_DAG_map: " + str(hex(id(Partition_DAG_map))))
        logger.info("generate_DAG_info_incremental_partitions: Partition_DAG_map:")
        for key, value in Partition_DAG_map.items():
            logger.info(str(key) + ' : ' + str(value) + " addr value: " + str(hex(id(value))))

        #copy_of_state_info_of_previous_state.fanins.clear()
        Partition_DAG_map[current_state].fanins.clear()

        logger.info("generate_DAG_info_incremental_partitions: DAG_info_DAG_map after clear:")
        for key, value in DAG_info_DAG_map.items():
            logger.info(str(key) + ' : ' + str(value))
    
        logger.info("generate_DAG_info_incremental_partitions: Partition_DAG_map:")
        for key, value in Partition_DAG_map.items():
            logger.info(str(key) + ' : ' + str(value))

        # logging.shutdown()
        # os._exit(0)  

        return DAG_info
    