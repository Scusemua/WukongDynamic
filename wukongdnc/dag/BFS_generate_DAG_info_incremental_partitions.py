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
from .DAG_executor_constants import  exit_program_on_exception
from .BFS_generate_DAG_info import Partition_senders, Partition_receivers
from .BFS_generate_DAG_info import leaf_tasks_of_partitions_incremental
#from .BFS_generate_DAG_info import num_nodes_in_graph

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


"""
Fist, consider the group-based DAG for the whiteboard example:

                G1
             /  |   \
            /   |    \
          v     v     v
        G2----> G3L   G4     # G3L indicates G3 is a loop group, likewise for P2L
             /   |    |
            /    |    |
          v      v    v
        G5----> G6    G7

Expanding the groups to see the nodes:


                 G1: 5       17               1
                 /          |                \
                /           |                 \
              v             |                  \ 
    G2: 2 10 16             |                   \
        |       G3L:        v    G4:             v
        |------>20 8  11 3 19      4       6 13 12
                  /    |            \     /
                /      |             \   /
               v       |              \ /   
           G5:13  G6:  v               \     # 18 is a child of 4
                     7 15             / \    # 9 is a child of 6
                                    v   v
                               G7: 9    18


Since here we are using partitions. not groups, the complete DAG looks like:

          P1: 5 17 1
                | 
                |  
                v
P2L: 2 10 16 20 8 11 3 19 4 6 13 12       # P2L indicates P2 has a cycle of nodes
                |
                |
                v
        P3: 13 7 15 9 28

Each partition has a single edge to the next partition. Thus,
the partitions of a connected component in a graph are executed
sequentally. If there are muliple connected components, then 
the we can excute the components concurrently. Also note that
nofes 20 8 11 3 19 are in a loop (20 is the parent of 8 is the
parent of 11 .... is the parent of 19 is the parent of 20.) When 
we excute P2L, we will iterate over all of the nodes in P2L
many times, e.g., 80 times, but the nodes outside the loop 
require only one iteration. This is what would happen if we 
executed groups instead of nodes, i.e., we would iterate over
the noodes in G2 and G4 only once, while iterating many times
over the nodes in G3L. 
Note: When using partitions, we could still identify the groups
within a partition, and use the groups to reduce th number
of iterations for the nodes in groups of the partition that do 
not have a loop. That is, pagerabk valus for the nodes in P2L 
in G2 would be computd first, using one iteration, then the nodes
in G3L using many itetations, then the nodes in G4 using one iteration.
This would reduce the total number of iterations.

To construct the DAG incrementally:
- generate the first partition P1. This partition is 5 --> 17 --> 1
  (note: B --> A means node B is the parent of node A) where we added 
  1 to the bfs queue at the start, then we dequeued 1 and 
  did dfs(1) to get the parent 17 of 1 and the parent 5 of 17.
- This first partition is also the first group.
- We know the nodes of P1 but P1 is incomplete since
  we do not know which nodes in P1 have children. The child
  nodes of the nodes in P1 that are not in P1 will be in P2.
  We say P1 is "to-be-continued". The BFS queue
  will contain 5, 17 an 1 (there are added to the bfs queue
  in the reverse order that they weer visited since dfs is 
  recursive and they are added as the recursion unwinds).
  We will visit the child of 5, 17, and 1, in that order
  to get partition 2. 
- At this point, P1 is now complete - we know its nodes 
  and we know the nodes in P1 that are parent nodes of nodes
  in P2 2. Partition P2 is incomplete. It becomes complete when 
  we visit the unvisited children of the nodes in P2 (some 
  children are in P2 and thus have been vsited)
- In general, when we identify the nodes in partition i, 
   partition i-1 becomes complete.
Note: Pi has a single fanout, which is to Pi+1, and Pi+1
has a single fanin which is from Pi. thus, we can cluster
Pi and Pi+1, which means that Pi has a collpase set that 
Pi+1. (The fanout and fanin sets of Pi are empty) The worker/
lambda that excutes the first partition of a connected
component in the graph will (sequentially) execute all of the 
partitions in the component. 

In the code below, we add the current partition to the DAG.

The first partition is added as an incomplete
partition, i.e., it is to-be-continued.

For the remaining partitions, when bfs() identifies the
current partition Pi:
- add Pi as an incomplete partition. Essentially, this means that 
  the fanins/fanouts/collapse for the partition are empty.
  The next partition Pi+1 will be added to the collapse set
  for Pi when Pi+1 is identified by bfs(). At that point. 
  Pi+1 will be the current partition and Pi will be the 
  previous partition. Note also that Pi-1 is the 
  previous-previous partition.
- Mark the previous partition Pi-1 as complete.  Generate the 
  collapse set for Pi-1, which contains Pi.
  For example, in the whiteboard DAG above, P1 is incomplete. 
  When we identify (the nodes in) P2, we mark P1 as complete
  and add P2 the the collapse set of P1. 
- As we just said, when we process current partition Pi,
  we compute the collapse of Pi-1 and mark Pi-1 as complete 
  (or not to-be-continued). We also track for each partition
  P, in addition to whether or not it is complete, whether
  whether it has a collapse to an incomplete partition.
  groups that are incomplete. For example, when we process
  partition P2, we set P1 to complete, but P1 has a collapse
  to the incomplete partition P2. So P1 is marked as complete 
  but we also mark P1 as having a collapse to an incomplete
  partition. Note that when we process current group Pi, we 
  set Pi-1 to complete (as described in the previous step).
  We also partition Pi-2 to have NO collapse to an incomplete 
  partition, since Pi-1 was set to complete. For example, when we
  process partition P3 in the DAG above, we set P2 to complete,
  and we set group P1 to have no collapse to an incomplete group.
  In the code below, we maintain the index of the current, 
  previous, and previous-previous partitions.

"""

#rhc: num_nodes
# this is set BFS.input_graph when doing non-incremental DAG generation with partitions
num_nodes_in_graph = 0

# Global variables for DAG_info. These are updated as the DAG
# is incrementally generated. 
# Note: We generate a DAG but we may or may not publish it. That is,
# we may only "publish" every ith DAG that is generated. Value i is
# controlled by the global constant incremental_DAG_deposit_interval.
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
# contains the task/function/code and the fanin/fanout information for the task.
Partition_DAG_map = {}
# references to the code for the tasks
Partition_DAG_tasks = {}

# version of DAG, incremented for each DAG generated
Partition_DAG_version_number = 0
# Saving current_partition_name as previous_partition_name after processing the 
# current partition. We cannot just subtract one, e.g. PR3_1 becomes PR2_1, since
# the name of partition 2 might actually be PR2_1L, so we need to 
# save the actual name "PR2_1L" and retrive it when we process PR3_1
Partition_DAG_previous_partition_name = "PR1_1"
# number of tasks in the DAG
Partition_DAG_number_of_tasks = 0
# the tasks in the last partition of a generated DAG may be incomplete,
# which means we cannot execute these tasks until the next DAG is 
# incrementally published. For partitions this is 1 or 0.
Partition_DAG_number_of_incomplete_tasks = 0

#rhc: num_nodes: 
Partition_DAG_num_nodes_in_graph = 0

# Called by generate_DAG_info_incremental_partitions below to generate 
# the DAG_info object when we are using an incremental DAG of partitions.
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
#rhc: num_nodes
    global Partition_DAG_num_nodes_in_graph

    # for debugging
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

    # Since this state info is read by the DAG executor, we make a copy
    # of the state info and change the copy. This state info is the only 
    # read-write object that is shared by the DAG executor and the DAG
    # generator (here). By making  copy, the DAG executor can read the 
    # state info in the previously generated DAG while the DAG generator
    # writes a copy of this state info for the next ADG to be generated,
    # i.e., there is no (concurrent) sharing.
    """
    
    # construct a dictionary of DAG information 
    logger.trace("")
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

    # If there is only one partition in the entire DAG then it is complete and is version 1.
    # Otherwise, version 1 is the DAG_info with partitions 1 and 2, where 1 is complete 
    # and 2 is complete, if there are 2 partitons in the entire DAG, or incomplete otherwise.
    Partition_DAG_version_number += 1
    # if the last partition has incomplete information, then the DAG is 
    # incomplete. When partition i is added to the DAG, it is incomplete
    # unless it is the last partition in the DAG). It becomes complete
    # when we add partition i+1 to the DAG. (So partition i needs information
    # that is generated when we create partition i+1. The  
    # (graph) nodes in partition i can only have children that are in partition 
    # i or partition i+1. We need to know partition i's children
    # in order for partition i to be complete. Partition i's childen
    # are discovered while generating partition i+1.
    # to_be_continued is a parameter to this method. It is true if all of the
    # grpah nodes are in some partition, i.e., the graph is complete and is not
    # to be continued.
    Partition_DAG_is_complete = not to_be_continued # to_be_continued is a parameter
    # number of tasks in the current incremental DAG, including the
    # incomplete last partition, if any. For computing pagerank, the task/function
    # is the same for all of the partitions. Thus, we really do not need to 
    # save the task/function in the DAG, once for each task in the ADG. That is 
    # what Dask does so we keep this for now.
    Partition_DAG_number_of_tasks = len(Partition_DAG_tasks)
    # For partitions, this is at most 1. When we are generating a DAG
    # of groups, there may be many groups in the incomplete last
    # partition and they will all be considered to be incomplete.
    Partition_DAG_number_of_incomplete_tasks = number_of_incomplete_tasks
#rhc: num_nodes
    # The value of num_nodes_in_graph is set by BFS_input_graph
    # at the beginning of execution, which is before we start
    # DAG generation.  This value does not change.
    Partition_DAG_num_nodes_in_graph = num_nodes_in_graph

    DAG_info_dictionary["DAG_version_number"] = Partition_DAG_version_number
    DAG_info_dictionary["DAG_is_complete"] = Partition_DAG_is_complete
    DAG_info_dictionary["DAG_number_of_tasks"] = Partition_DAG_number_of_tasks
    DAG_info_dictionary["DAG_number_of_incomplete_tasks"] = Partition_DAG_number_of_incomplete_tasks
#rhc: num_nodes:
    DAG_info_dictionary["DAG_num_nodes_in_graph"] = Partition_DAG_num_nodes_in_graph

#rhc: Note: we are saving all the incemental DAG_info files for debugging but 
#     we probably want to turn this off otherwise.

    # filename is based on version number - Note: for partition, say 3, we
    # have output the DAG_info with partitions 1 and 2 as version 1 so 
    # the DAG_info for the newly added partition 3 will have partitions 1, 2, and 3 
    # and will be version 2 but named "DAG_info_incremental_Partition_3"
    file_name_incremental = "./DAG_info_incremental_Partition_" + str(Partition_DAG_version_number) + ".pickle"
    with open(file_name_incremental, 'wb') as handle:
        cloudpickle.dump(DAG_info_dictionary, handle) #, protocol=pickle.HIGHEST_PROTOCOL)  

    # for debugging
    num_fanins = len(Partition_all_fanin_task_names)
    num_fanouts = len(Partition_all_fanout_task_names)
    num_faninNBs = len(Partition_all_faninNB_task_names)
    num_collapse = len(Partition_all_collapse_task_names)
    if show_generated_DAG_info:
        logger.trace("DAG_map:")
        for key, value in Partition_DAG_map.items():
            logger.trace(str(key) + ' : ' + str(value))
        logger.trace("")
        logger.trace("states:")        
        for key, value in Partition_DAG_states.items():
            logger.trace(str(key) + ' : ' + str(value))
        logger.trace("")
        logger.trace("num_fanins:" + str(num_fanins) + " num_fanouts:" + str(num_fanouts) + " num_faninNBs:"
        + str(num_faninNBs) + " num_collapse:" + str(num_collapse))
        logger.trace("")  
        logger.trace("Note: partitions only have collapse sets.")
        logger.trace("Partition_all_fanout_task_names:")
        for name in Partition_all_fanout_task_names:
            logger.trace(name)
        logger.trace("all_fanin_task_names:")
        for name in Partition_all_fanin_task_names :
            logger.trace(name)
        logger.trace("all_fanin_sizes:")
        for s in Partition_all_fanin_sizes :
            logger.trace(s)
        logger.trace("all_faninNB_task_names:")
        for name in Partition_all_faninNB_task_names:
            logger.trace(name)
        logger.trace("all_faninNB_sizes:")
        for s in Partition_all_faninNB_sizes:
            logger.trace(s)
        logger.trace("Partition_all_collapse_task_names:")
        for name in Partition_all_collapse_task_names:
            logger.trace(name)
        logger.trace("")
        logger.trace("leaf task start states:")
        for start_state in Partition_DAG_leaf_task_start_states:
            logger.trace(start_state)
        logger.trace("")
        logger.trace("DAG_tasks:")
        for key, value in Partition_DAG_tasks.items():
            logger.trace(str(key) + ' : ' + str(value))
        logger.trace("")
        logger.trace("DAG_leaf_tasks:")
        for task_name in Partition_DAG_leaf_tasks:
            logger.trace(task_name)
        logger.trace("")
        logger.trace("DAG_leaf_task_inputs:")
        for inp in Partition_DAG_leaf_task_inputs:
            logger.trace(inp)
        logger.trace("")
        logger.trace("DAG_version_number:")
        logger.trace(Partition_DAG_version_number)
        logger.trace("")
        logger.trace("DAG_is_complete:")
        logger.trace(Partition_DAG_is_complete)
        logger.trace("")
        logger.trace("DAG_number_of_tasks:")
        logger.trace(Partition_DAG_number_of_tasks)
        logger.trace("")
        logger.trace("DAG_number_of_incomplete_tasks:")
        logger.trace(Partition_DAG_number_of_incomplete_tasks)
        logger.trace("")
    #rhc: num_nodes
        logger.trace("DAG_num_nodes_in_graph:")
        logger.trace(Partition_DAG_num_nodes_in_graph)
        logger.trace("")

    # for debugging:
    # read file file_name_incremental just written and display its contents 
    if False:
        DAG_info_Partition_read = DAG_Info.DAG_info_fromfilename(file_name_incremental)
        
        DAG_map = DAG_info_Partition_read.get_DAG_map()
        # these are not displayed
        all_collapse_task_names = DAG_info_Partition_read.get_all_collapse_task_names()
        # Note: prefixing name with '_' turns off th warning about variabel not used
        _all_fanin_task_names = DAG_info_Partition_read.get_all_fanin_task_names()
        _all_faninNB_task_names = DAG_info_Partition_read.get_all_faninNB_task_names()
        _all_faninNB_sizes = DAG_info_Partition_read.get_all_faninNB_sizes()
        _all_fanout_task_names = DAG_info_Partition_read.get_all_fanout_task_names()
        # Note: all fanout_sizes is not needed since fanouts are fanins that have size 1
        DAG_states = DAG_info_Partition_read.get_DAG_states()
        DAG_leaf_tasks = DAG_info_Partition_read.get_DAG_leaf_tasks()
        DAG_leaf_task_start_states = DAG_info_Partition_read.get_DAG_leaf_task_start_states()
        DAG_tasks = DAG_info_Partition_read.get_DAG_tasks()

        DAG_leaf_task_inputs = DAG_info_Partition_read.get_DAG_leaf_task_inputs()

        Partition_DAG_is_complete = DAG_info_Partition_read.get_DAG_info_is_complete()
        DAG_version_number = DAG_info_Partition_read.get_DAG_version_number()
        DAG_number_of_tasks = DAG_info_Partition_read.get_DAG_number_of_tasks()
        DAG_number_of_incomplete_tasks = DAG_info_Partition_read.get_DAG_number_of_incomplete_tasks()
#rhc: num_nodes
        DAG_num_nodes_in_graph = DAG_info_Partition_read.get_DAG_num_nodes_in_graph()
        logger.trace("")
        logger.trace("DAG_info partition after read:")
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
            logger.trace(Partition_DAG_is_complete)
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

    # create the DAG_info object from the dictionary
    # Note: The DAG_map in this DAG_info is a shallow copy of the
    # DAG_map for the DAG info that we are maintaining during
    # incremental DAG generation. So the DAG_executor (who receives
    # this DAG_info object will be using a DAG_map reference that 
    # is different from Partition_DAG_map. (We put Partition_DAG_map
    # in a DAG_info_dctionary and give this dictionary to DAG_info.__init__()
    # and init makes a shallow copy of DAG_info_dictionary['DAG_map'])
    # and assigns it to the DAG_map in the DAG_info object returned here.
    # where in DAG_info __init__:
    #if not use_incremental_DAG_generation:
    #    self.DAG_map = DAG_info_dictionary["DAG_map"]
    #else:
    #    # Q: this is the same as DAG_info_dictionary["DAG_map"].copy()?
    #    self.DAG_map = copy.copy(DAG_info_dictionary["DAG_map"]
    # get the rest of the DAG_info member var values from the dictionary:
    # self.DAG_states = DAG_info_dictionary["DAG_states"]
    # self.all_fanin_task_names = DAG_info_dictionary["all_fanin_task_names"]
    # ...

    DAG_info = DAG_Info.DAG_info_fromdictionary(DAG_info_dictionary)
    return  DAG_info

"""
Note: The code for DAG_info_fromdictionary is below. The info in a 
DAG_info object about the DAG is retrieved from the DAG_info object's DAG_info_dictionary.

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
        return cls(DAG_info_dictionary) # give dictionary to init
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

#rhc_ num_nodes
    # This was set above and will not be changed in this method
    global Partition_DAG_num_nodes_in_graph

    logger.trace("generate_DAG_info_incremental_partitions: to_be_continued: " + str(to_be_continued))
    logger.trace("generate_DAG_info_incremental_partitions: current_partition_number: " + str(current_partition_number))
    logger.info("generate_DAG_info_incremental_partitions: current_partition_name: " + str(current_partition_name))

    # for debugging:
    # we track the edges between DAG tasks. If task A has a fanin/fanout
    # to task B then A is the "sender" and B is the "receiver". When 
    # we generate an incemental DAG, we build it using these senders
    # and receivers that are constructed by BFS as it builds the 
    # partitions of nodes. So BFS builds the sender/receivers and 
    # passes then here so that the DAG_info object can be generated.
    print()
    # Using copy() here and below to avoid the error: "RuntimeError: dictionary changed size during iteration"
    # when we are using multithreaded bfs(). That is, while the generator thread is
    # iterating here bfs() could add a key:value to the dictionary
    # and an exceptioj is thrown when a dictionary is changed in size (i.e., an item is added or removed) 
    # while it is being iterated over in a loop. We also use copy() for thr 
    # list we are iterating over.
    print("generate_DAG_info_incremental_partitions: Partition_senders:")
    for sender_name,receiver_name_set in Partition_senders.copy().items():
        print("sender:" + sender_name)
        print("receiver_name_set:" + str(receiver_name_set))
    print()
    print()
    print("generate_DAG_info_incremental_partitions: Partition_receivers:")
    for receiver_name,sender_name_set in Partition_receivers.copy().items():
        print("receiver:" + receiver_name)
        print("sender_name_set:" + str(sender_name_set))
    print()
    print()
    print("generate_DAG_info_incremental_partitions: Leaf nodes of partitions (incremental):")
    for name in leaf_tasks_of_partitions_incremental.copy():
        print(name + " ")
    print()

    logger.trace("generate_DAG_info_incremental_partitions: Partition DAG incrementally:")

    # These lists are part of the state_info gnerated for the current 
    # partition. For partitions, only collapse will be non-empty. That is,
    # after each task, we only have a collapsed task, no fanouts/fanins.
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
    # state as states start with 1 (there is no partition 0)
    previous_state = current_partition_number-1
    #previous_partition_name = "PR" + str(current_partition_number-1) + "_1"

    # a list of partitions that have a fanot/fanin/collapse to the 
    # current partition. (They "send" to the current partition which "receives".)
    senders = Partition_receivers.get(current_partition_name)
    # Note: a partition that starts a new connected component (which is 
    # the first partition collected on a call to BFS(), of which there 
    # may be many calls if the graph is not connected) is a leaf
    # node and thus has no senders. This is true about partition 1 and
    # this is assserted by the caller (BFS()) of this method.

    """
    Outline: 
    Each call to generate_DAG_info_incremental_partitions adds one partition to the
    DAG_info. The added partition is incomplete unless it is the last partition 
    that will be added to the DAG. The previous partition is now marked as complete.
    The previous partition's next partition is this current partition, which is 
    either complete or incomplete. If the current partition is incomplete then 
    the previous partition is marked as having an incomplete next partition. We also
    comsider the previous partition of the previous partition. It was marked as complete
    when we processed the previous partition, but it was considered to have an incomplete
    next partition. Now that we marked the previous partition as complete, the prvious 
    previous partition is marked as not having an incomplete next partition.
    
    There are 3 cases:
    1. current_partition_number == 1: This is the first partition. This means 
    there is no previous partition or previous previous partition. The current
    partition is marked as complete if the entire DAG has only one partition; otherwise
    it is marked as complete. Note: If the current partition (which is partition 1) is
    the only partition in its connected component, i.e., its component has size 1,
    then it can also be marked as complete since it has no children and thus we have
    all the info we need about partition 1. To identify the last partition
    in a conncted component (besides the partitio that is the last partition
    to be connected in the DAG) we would have to look at all the nodes in a 
    partition and determibe whethr they had any child nodes that were not in
    the same partition (i.e., these child nodes will be i the next partition).
    This would have to be done for each partition and it's not clear whether
    all that work would be worth it just to mark the last partition of a 
    connected component completed a little earlier than it otherwise would.
    Note this is only helpful if the incremental DAG generatd happens to 
    end with a partition that is the last partition in its connected compoent.
    If the interval n between incremental DAGs (i.e., add n partitions before
    publishng the new DAG) then it may be rare to have such a partition.
    2. (senders == None): This is a leaf node, which could be partition 2 or any 
    partition after that. This measn that the current partition is the first partition
    of a new connected component. We will add this leaf partition to a list of leaf
    partitions so that when we return we can mke sure this leaf partition is 
    executed. (No other partition has a fanin/fanout/collapse to this partition so no
    other partition can cause this leaf node to be executed. We will start its execution
    ourselves.) The previous and previous previous partitons are marked as described above.
    Note the the previous partition can be marked complete as usual. Also, we now knowthat the 
    previous partition, which was marked as having an incomplete next partition, can now
    be marked as not having an incimplete next partition - this is because the previous
    partition was the last partition of its connected component and thus has no 
    fanins/fanouts/collapses at all - this allows us to mark it as not having an incomplete
    next partition.
    3. else: # current_partition_number >= 2: This is not a leaf partition and this partition 
    is not the first partition. Process the previous and previous previous partitions as
    described above.
    """

    if current_partition_number == 1:
        # Partition 1 is a leaf; so there is no previous partition
        # i.e., sender

        try:
            msg = "[Error]: def generate_DAG_info_incremental_partitions" \
            + " leaf node has non-None senders."
            assert senders == None , msg
        except AssertionError:
            logger.exception("[Error]: assertion failed")
            if exit_program_on_exception:
                logging.shutdown()
                os._exit(0)
        #assertOld:
        #if not senders == None:
        #    logger.error("[Error]: def generate_DAG_info_incremental_partitions"
        #        + " leaf node has non-None senders.")
        
        # record DAG information 
        # save leaf task name - we use task name as the key in DAG_map
        # to get the state information about the task, i.e., its
        # fanins/fanouts. ADG_map['foo'] is the state_info for task foo,
        # which includes its task/function and its collapses/fanins/faninNBs/fanouts
        # and whether it is incomplete = if it is incomplete, then it will become
        # complete in the next DAG generated.
        Partition_DAG_leaf_tasks.append(current_partition_name)
        # save leaf task state number
        Partition_DAG_leaf_task_start_states.append(current_state)
        # leaf tasks have no input
        task_inputs = ()
        # When we execute the incremental DAG, we start execution by excuting 
        # the leaf tasks, of whihc there will only be one, which is the first
        # partition generated for the DAG. If the DAG is not connected, new
        # leaf tasks will be detected and executed.
        # Note: For non-incremental DAG generation, all leaf tasks are detected
        # when the complete/entire DAG_info object is generated. These leaf
        # tasks are all started by the DAG_execution_driver at the start of 
        # execution, so there can be some parallelism during the execution
        # of a DAG that uses partitions.
        Partition_DAG_leaf_task_inputs.append(task_inputs)

        # we will check that current_partition_name is in leaf_tasks_of_partitions
        # (found by BFS) upon return to BFS() (as an asssertion that the leaf task
        # that we found was also found by BFS as it partitioned the nodes.

        # generate the state info for this partition/DAG task
        Partition_DAG_map[current_state] = state_info(current_partition_name, fanouts, fanins, faninNBs, collapse, fanin_sizes, 
            faninNB_sizes, task_inputs,
            to_be_continued,
            # We do not know whether this first partition will have anf fanout_fanin_faninNB_collapse_groups
            # that are incomplete (i.e., to-be-continued) until we process the 2nd partition, 
            # except if to_be_continued is False in which case there are no more partitions and thus this partition has 
            # no fanout_fanin_faninNB_collapse_groups that are incomplete. If to_be_continued is True then we set 
            # fanout_fanin_faninNB_collapse_groups_partitions_are_ToBeContinued to True but we may change this value for process 1
            # when we process partition 2. That is, when we process partition 2, we may find it is nit
            # incomplete (it is the final partition in the DAG or the final partition in its
            # connected compoment) so that we can change fanout_fanin_faninNB_collapse_groups_partitions_are_ToBeContinued
            # of partition 1 to False.
            to_be_continued) # this is assgned to fanout_fanin_faninNB_collapse_groups_partitions_are_ToBeContinued
        # we map task name to its integer ID. We use this ID as a key
        # in DAG_map which maps task IDs to task states#
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

        logger.trace("generate_DAG_info_incremental_partitions: Partition_DAG_map[current_state]: " + str(Partition_DAG_map[current_state] ))

        # Note: setting version number and to_be_continued of DAG_info in generate_DAG_for_partitions()
        # Note: setting number of tasks of DAG_info in generate_DAG_for_partitions()

        # For partitions, if the DAG is not yet complete, to_be_continued
        # parameter will be TRUE, and there is always just one incomplete partition 
        # in the just generated version of the DAG, which is the last parition.
        if to_be_continued:
            number_of_incomplete_tasks = 1
        else:
            number_of_incomplete_tasks = 0
        
        # uses global dictionary to create the DAG_info object
        DAG_info = generate_DAG_for_partitions(to_be_continued,number_of_incomplete_tasks)

        logger.info("generate_DAG_info_incremental_partitions: returning from generate_DAG_info_incremental_partitions for"
            + " partition " + str(current_partition_name))
        
        return DAG_info

    elif (senders == None):
        # (Note: Don't think we can have a length 0 senders, 
        # for current_partition_name. That is, we only create a 
        # senders when we get the first sender.
        #
        # Note: We know this is not partition 1 based on if-condition

        try:
            msg = "[Error]: generate_DAG_info_incremental_partitions:" \
                + " partition has a senders with length 0."
            assert not(not senders == None and len(senders) == 0) , msg
        except AssertionError:
            logger.exception("[Error]: assertion failed")
            if exit_program_on_exception:
                logging.shutdown()
                os._exit(0)
        # assertOld:
        #if not senders == None and len(senders) == 0:
        #    logger.error("[Error]: generate_DAG_info_incremental_partitions:"
        #        + " partition has a senders with length 0.")

        # This is not the first partition but it is a leaf partition, which means
        # it was the first partition generated by some call to BFS(), i.e., 
        # it is the start of a new connected component. This also means there
        # is no collapse from the previous partition to this partition, i.e.,
        # the previous partition has no collapse task.

        # Since this is a leaf node (it has no predecessor) we will need to add 
        # this partition to the work queue or start a new lambda for it
        # like the DAG_executor_driver does. (Note that since this partition has
        # no predecessor, no worker or lambda can enable this task via a fanout, collapse,
        # or fanin, thus we must make sure this leaf task gets executed.)
        # This is done by the caller (BFS()) of this method. BFS() 
        # deposits a new DAG_info and the deposit() processes the leaf tasks. 
        # (See deposit()) 
       
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
        # of the previous partition i-1
        previous_state = current_state - 1
        state_info_of_previous_partition = Partition_DAG_map[previous_state]

        # Previous partition does not have a collapse to this leaf partition
        # so no collapse is added to state_info_previous_state; however,
        # previous partition is now complete so its TBC is set to False.
        # Note: This current partition cannot be partition 1 based on the if-condition
        state_info_of_previous_partition.ToBeContinued = False
        # previous partiton i-1 is the last partition of its connected component as 
        # this current partition i is the first partition of its connected component,
        # The last partition of a connected component does not do any fanouts/fanins/etc.
        state_info_of_previous_partition.fanout_fanin_faninNB_collapse_groups_partitions_are_ToBeContinued = False

        # we also track the partition previous to the previous partition.
        if current_partition_number > 2:
            # if current is 3 then there is a previous 2 and a previous previous 1;
            # partitions statr at 0 so we need to be at least 3 in order to have a 
            # prvious previous.
            previous_previous_state = previous_state - 1
            state_info_of_previous_previous_partition = Partition_DAG_map[previous_previous_state]
            state_info_of_previous_previous_partition.fanout_fanin_faninNB_collapse_groups_partitions_are_ToBeContinued = False
        # For example, we processed partition 1 and it was incomplete and we assumed it had
        # to be continued collapses/fanins/faninNBs/fanouts. We processed 2 and considered
        # 2 incomplete but 1 became complete. We assumed 2 had to be continued collapses/fanins/faninNBs/fanouts
        # so that 1's fanout_fanin_faninNB_collapse_groups_partitions_are_ToBeContinued was still True.
        # We process 3 here, which we assume is the first partition of its (new) connected
        # component. Now 3 is considered incomplete, 2 becomes complete and 2's 
        # fanout_fanin_faninNB_collapse_groups_partitions_are_ToBeContinued can be set to False since
        # 2 was the last partition of its connected component. Aslo 1's
        # fanout_fanin_faninNB_collapse_groups_partitions_are_ToBeContinuedfanout_fanin_faninNB_collapse_groups_partitions_are_ToBeContinued can be set to False since
        # we now know that 2 has no to be continued collapses/fanins/faninNBs/fanouts.
        # 1 was already considered to be complete.

        logger.info("generate_DAG_info_incremental_partitions: new connected component for current partition "
            + str(current_partition_number) + ", the previous_state_info for previous state " 
            + str(previous_state) + " after update TBC (no collapse) is: " 
            + str(state_info_of_previous_partition))

        # generate state in DAG
        Partition_DAG_map[current_state] = state_info(current_partition_name, fanouts, fanins, faninNBs, collapse, fanin_sizes, 
            faninNB_sizes, task_inputs,
            to_be_continued,
            # We do not know whether this first group will have fanout_fanin_faninNB_collapse_groups_partitions_are_ToBeContinued
            # that are incomplete until we process the 2nd partition, except if to_be_continued
            # is False in which case there are no more partitions and no fanout_fanin_faninNB_collapse_groups_partitions_are_ToBeContinued
            # that are incomplete. If to_be_continued is True then we set fanout_fanin_faninNB_collapse_groups_partitions_are_ToBeContinued
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

        logger.trace("generate_DAG_info_incremental_partitions: Partition_DAG_map[current_state]: " + str(Partition_DAG_map[current_state] ))

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
        # this next DAG generation is not read by DAG_executor until the 
        # DAG_executor gets a new DAG, which will have this new 
        # state information. 
        # So DAG_executor(s) and DAG_generator are excuting concurrently
        # and DAG_executor is reading some of the DAG_info members that 
        # are written by the DAG_generator. Thus we give the DAG_executor
        # a private state_info that is separate from the state_info in
        # the DAG_info that we are generating. Thus, state_info is not
        # being shared as a variable. 
        # Note: here we write other DAG info variables but these are 
        # immutable so a write to immutable X creates a new X which means
        # this X is not shared with the DAG executor.
        #
        # if the new DAG is not complete, we will be generating
        # more DAGS and we need to guard against sharing.
        # Note: see the comment below for this same block of code.It has 
        # additional explanation.
        if to_be_continued:
            DAG_info_DAG_map = DAG_info.get_DAG_map()

            # The DAG_info object is shared between this DAG_info generator
            # and the DAG_executor, i.e., we execute the DAG generated so far
            # while we generate the next incremental DAGs. The current 
            # state is part of the DAG given to the DAG_executor and we 
            # will modify the current state when we generate the next DAG.
            # (We modify the collapse list and the toBeContiued  of the state.)
            # So we do not want to share the current state object, that is the 
            # DAG_info given to the DAG_executor has a state_info reference
            # this is different from the reference we maintain here in the
            # DAG_map. 
            # 
            # Get the state_info for the DAG_map
            state_info_of_current_state = DAG_info_DAG_map[current_state]

            # Note: the only parts of the state info for the current state that can 
            # be changed for partitions are the collapse list and the TBC boolean. Yet 
            # we deepcopy the entire this state_info object. This os not so bad since
            # all other parts of the state info are empty for partitions (fanouts, fanins)
            # except for the pagerank function (reference)
            # Note: Each state has a reference to the Python function that
            # will excute the task. This is how Dask does it - each task
            # has a reference to its function. For pagernk, we will use
            # the same function for all the pagerank tasks. There can be 
            # three different functions, but we could identify this 
            # function whrn we excute the task, instead of doing it above
            # and saving this same function in the DAG for each task,
            # which wastes space

            # make a deep copy of this state_info object which is the state_info
            # object that he DAG generator will modify and the DG_executor will read
            copy_of_state_info_of_current_state = copy.deepcopy(state_info_of_current_state)

            # put the copy in the DAG_map given to the DAG_executor. Now
            # the DAG_executor and the DAG_generator will be reading/writing different 
            # state_info objects 
            DAG_info_DAG_map[current_state] = copy_of_state_info_of_current_state

            # This used to test the deep copy - modify the state info
            # of the generator and make sure this modification does 
            # not show up in the state_info object given to the DAG_executor.
            """
            # modify generator's state_info 
            Partition_DAG_map[current_state].fanins.append("goo")

            # display DAG_executor's state_info objects
            logger.trace("address DAG_info_DAG_map: " + str(hex(id(DAG_info_DAG_map))))
            logger.trace("generate_DAG_info_incremental_partitions: DAG_info_DAG_map after state_info copy:")
            for key, value in DAG_info_DAG_map.items():
                logger.trace(str(key) + ' : ' + str(value) + " addr value: " + str(hex(id(value))))

            # display generator's state_info objects
            logger.trace("address Partition_DAG_map: " + str(hex(id(Partition_DAG_map))))
            logger.trace("generate_DAG_info_incremental_partitions: Partition_DAG_map:")
            for key, value in Partition_DAG_map.items():
                logger.trace(str(key) + ' : ' + str(value) + " addr value: " + str(hex(id(value))))

            # undo the modification to the generator's state_info
            Partition_DAG_map[current_state].fanins.clear()

            # display generator's state_info objects
            logger.trace("generate_DAG_info_incremental_partitions: DAG_info_DAG_map after clear:")
            for key, value in DAG_info_DAG_map.items():
                logger.trace(str(key) + ' : ' + str(value))
        
            # display DAG_executor's state_info ojects
            logger.trace("generate_DAG_info_incremental_partitions: Partition_DAG_map:")
            for key, value in Partition_DAG_map.items():
                logger.trace(str(key) + ' : ' + str(value))

            # logging.shutdown()
            # os._exit(0)
            """ 

        logger.trace("generate_DAG_info_incremental_partitions: returning from generate_DAG_info_incremental_partitions for"
            + " partition " + str(current_partition_name))
        
        return DAG_info
    
    else: # current_partition_number >= 2

        try:
            msg = "[Error]: generate_DAG_info_incremental_groups:" \
                + " partition has a senders with length 0."
            assert not len(senders) == 0 , msg
        except AssertionError:
            logger.exception("[Error]: assertion failed")
            if exit_program_on_exception:
                logging.shutdown()
                os._exit(0)
        # assertOld: no length 0 senders lists
        #if len(senders) == 0:
        #    logger.error("[Error]: generate_DAG_info_incremental_groups:"
        #        + " partition has a senders with length 0.")

        # This is not the first partition and it is not a leaf partition.

        try:
            msg = "[Error]: generate_DAG_info_incremental_partitions: using partitions and a" \
                + " partition has more than one sending partition."
            assert not (len(senders) != 1) , msg
        except AssertionError:
            logger.exception("[Error]: assertion failed")
            if exit_program_on_exception:
                logging.shutdown()
                os._exit(0)

        # assertOld: For partitions, there can be only one sender, i.e., each 
        # task is a collapsed task of the previous task: A->B->C. This means
        # A, B, and C are executed sequentially. Each connected component 
        # in the graph has its own sequence of partitions and these sequences
        # can be executed concurrently. So the eecution of a graph with two or more 
        # components can have some parallelism.
        #if len(senders) != 1:
        #    logger.error("[Error]: generate_DAG_info_incremental_partitions: using partitions and a"
        #        + " partition has more than one sending partition.")
                
        sender = next(iter(senders)) # sender is first and only element in set

        try:
            msg = "[Error]: generate_DAG_info_incremental_partitionsusing partitions and" \
                + " the sender for a partition is not previous_partition_name."
            assert sender == Partition_DAG_previous_partition_name , msg
        except AssertionError:
            logger.exception("[Error]: assertion failed")
            if exit_program_on_exception:
                logging.shutdown()
                os._exit(0)
        #assertOld: the sender should be equal to the previous_partition_name
        #if not sender == Partition_DAG_previous_partition_name:
        #    logger.error("[Error]: generate_DAG_info_incremental_partitionsusing partitions and"
        #        + " the sender for a partition is not previous_partition_name.")

        # generate current partition's input - current partiton rrceives its 
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
        # qualified_names has a single name in it, e.g., for current partiton PR2_1, the task input
        # is a set with element "PR1_1-PR2_1" where PR1_1 is the previous partition.
        # Recall these inputs ar added to the data dictionary during DAG execution 
        # and then retrieved from the data dictionary and passed to the task when the 
        # task is executed. (See DAG_executor.py, which uses Dask style code for this.)
        # Note: In Dask, the output for a task is the same for all of its downstream
        # tasks, which is unlike for pagerank as a pagerank task can output different 
        # values to different downstream tasks.)
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
        # until we know who its collapse partition is, which is the current partition.
        state_info_of_previous_partition.ToBeContinued = False
        # if the current partition is to_be_continued then it has incomplete
        # groups so we set fanout_fanin_faninNB_collapse_groups_partitions_are_ToBeContinued of the previous
        # groups to True; otherwise, we set fanout_fanin_faninNB_collapse_groups_partitions_are_ToBeContinued to False.
        # Note: state_info_of_previous_group.ToBeContinued = False inicates that the
        # previous groups are not to be continued, while
        # state_info_of_previous_group.fanout_fanin_faninNB_collapse_groups_partitions_are_ToBeContinued indicates
        # whether the previous groups have fanout_fanin_faninNB_collapse_groups_partitions_are_ToBeContinued 
        # that are to be continued, i.e., the fanout_fanin_faninNB_collapse are 
        # to groups in this current partition and whether these groups in the current
        # partiton are to be continued is indicated by parameter to_be_continued,
        # which is True if the DAG is not yet complete, i.e., there are more partitions
        # to be processed. (If every graph node is in a partition, this parameter will
        # be set to False by BFS.)
        state_info_of_previous_partition.fanout_fanin_faninNB_collapse_groups_partitions_are_ToBeContinued = to_be_continued
        
        # we track the previous partition of the previous partition
        if current_partition_number > 2:
            previous_previous_state = previous_state - 1
            state_info_of_previous_previous_partition = Partition_DAG_map[previous_previous_state]
            state_info_of_previous_previous_partition.fanout_fanin_faninNB_collapse_groups_partitions_are_ToBeContinued = False

        logger.info("generate_DAG_info_incremental_partitions: for current partition, the previous_state_info after update collpase and TBC: " 
            + str(state_info_of_previous_partition))

        # generate DAG information
        Partition_DAG_map[current_state] = state_info(current_partition_name, fanouts, fanins, faninNBs, collapse, fanin_sizes, 
            faninNB_sizes, task_inputs,
            # to_be_continued parameter can be true or false
            to_be_continued,
            # We do not know whether this first group will have fanout_fanin_faninNB_collapse_groups_partitions_are_ToBeContinued
            # that are incomplete until we process the 2nd partition, except if to_be_continued
            # is False in which case there are no more partitions and no fanout_fanin_faninNB_collapse_groups_partitions_are_ToBeContinued
            # that are incomplete. If to_be_continued is True then we set fanout_fanin_faninNB_collapse_groups_partitions_are_ToBeContinued
            # to True but we may change this value when we process partition 2.
            to_be_continued)
        Partition_DAG_states[current_partition_name] = current_state
        # See the example above

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
            # there are two DAG_map references, one in the DAG_info object given to
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
            # change the value for a key in Partition_DAG_map without changing the 
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
            # That is, we are maintaining
            # Partition_DAG_map = {} as part of the ongoing incremental DAG generation.
            # This is used to make the DAG_info object that is gven to the 
            # DAG_executor. We then get the DAG_info_DAG_map of this DAG_info
            # object:
            #   DAG_info_DAG_map = DAG_info.get_DAG_map()
            # and get the state_info object:
            #   state_info_of_current_state = DAG_info_DAG_map[state]
            # and make a deep copy of this state_info object:
            #   copy_of_state_info_of_current_state = copy.deepcopy(state_info_of_current_state)
            # and put this deep copy in DAG_info_DAG_map which is part of the DAG_info 
            # object given to the DAG_executor.
            DAG_info_DAG_map[current_state] = copy_of_state_info_of_current_state

            # This code is used to test the deep copy - modify the state info
            # maintained by the generator and make sure this modification does 
            # not show up in the state_info object given to the DAG_executor.
            """
            # modify the fanin state info maintained by the generator.
            Partition_DAG_map[current_state].fanins.append("goo")
            
            # display DAG_executor's state_info objects
            logger.trace("address DAG_info_DAG_map: " + str(hex(id(DAG_info_DAG_map))))
            logger.trace("generate_DAG_info_incremental_partitions: DAG_info_DAG_map after state_info copy:")
            for key, value in DAG_info_DAG_map.items():
                logger.trace(str(key) + ' : ' + str(value) + " addr value: " + str(hex(id(value))))

            # display generator's state_info objects
            logger.trace("address Partition_DAG_map: " + str(hex(id(Partition_DAG_map))))
            logger.trace("generate_DAG_info_incremental_partitions: Partition_DAG_map:")
            for key, value in Partition_DAG_map.items():
                logger.trace(str(key) + ' : ' + str(value) + " addr value: " + str(hex(id(value))))

            # fanin values should be different for current_state
            # one with "goo" and the other empty
            
            # undo the modification to the generator's maintained state_info
            Partition_DAG_map[current_state].fanins.clear()

            # display DAG_executor's state_info objects
            logger.trace("generate_DAG_info_incremental_partitions: DAG_info_DAG_map after clear:")
            for key, value in DAG_info_DAG_map.items():
                logger.trace(str(key) + ' : ' + str(value))
        
            # display generator's state_info ojects
            logger.trace("generate_DAG_info_incremental_partitions: Partition_DAG_map:")
            for key, value in Partition_DAG_map.items():
                logger.trace(str(key) + ' : ' + str(value))

            # fanin values should be the same for current_state (empty)

            # logging.shutdown()
            # os._exit(0)
            """

        logger.info("generate_DAG_info_incremental_partitions: returning from generate_DAG_info_incremental_partitions for"
            + " partition " + str(current_partition_name))

        return DAG_info
    