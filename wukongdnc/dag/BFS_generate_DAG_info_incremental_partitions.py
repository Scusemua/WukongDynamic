import logging
import os
import copy

import cloudpickle
from .DAG_info import DAG_Info
from .DFS_visit import state_info
from .BFS_pagerank import PageRank_Function_Driver, PageRank_Function_Driver_Shared
from .BFS_Shared import PageRank_Function_Driver_Shared_Fast
#from .DAG_executor_constants import USE_SHARED_PARTITIONS_GROUPS
#from .DAG_executor_constants import USE_STRUCT_OF_ARRAYS_FOR_PAGERANK
#from .DAG_executor_constants import USING_THREADS_NOT_PROCESSES, USE_MULTITHREADED_MULTIPROCESSING
#from .DAG_executor_constants import  EXIT_PROGRAM_ON_EXCEPTION
from . import DAG_executor_constants
from .BFS_generate_DAG_info import Partition_senders, Partition_receivers
from .BFS_generate_DAG_info import leaf_tasks_of_partitions_incremental
#from .BFS_generate_DAG_info import num_nodes_in_graph
from . import BFS
#from ..server import DAG_infoBuffer_Monitor

logger = logging.getLogger(__name__)

"""
if not (not USING_THREADS_NOT_PROCESSES or USE_MULTITHREADED_MULTIPROCESSING):
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

Note: During incremental DAG generation, we can deallocate memory on-the-fly in the 
data structures that hold the data collected for DAG gemeration. This is helpful when
large DAGs need to be built. Also, we can delete input graph nodes on-the-fly also,
which deallocate memory for storing the input graph as we allocate memory for building
the DAG. Id the file that stores the input graph has nodes listed in the order that
they appear in the partitions, we can stream the input graph, or read sections of it
at a time, so that we do not need to read the entire input graph into memory before
we start incremental DAG generation. (We might need to synchronize DAG generation with
inputting the graph, so that we only try to build parts of the DAG that we have already input.
S3 supports file streaming.)

"""

#brc: num_nodes
# this is set BFS.input_graph when doing non-incremental DAG generation with partitions
num_nodes_in_graph = 0

# Global variables for DAG_info. These are updated as the DAG
# is incrementally generated. 
# Note: We generate a DAG but we may or may not publish it. That is,
# we may only "publish" every ith DAG that is generated. Value i is
# controlled by the global constant INCREMENTAL_DAG_DEPOSIT_INTERVAL.

# Note: This group of names and sizes is for debugging only. We probably do not want
# to collect these for large graphs.
Partition_all_fanout_task_names = []
Partition_all_fanin_task_names = []
Partition_all_faninNB_task_names = []
Partition_all_collapse_task_names = []
Partition_all_fanin_sizes = []
Partition_all_faninNB_sizes = []

# It is not clear how large these leaf task lists wil be for large graphs. We 
# may want to delete leaf tasks on the fly
Partition_DAG_leaf_tasks = []
Partition_DAG_leaf_task_start_states = []
# no inputs for leaf tasks
Partition_DAG_leaf_task_inputs = []

# maps task name to an integer ID for that task
# We access Partition_DAG_states for current_partition_name.
Partition_DAG_states = {}
# maps integer ID of a task to the state for that task; the state 
# contains the task/function/code and the fanin/fanout information for the task.
# We access Partition_DAG_map for current_state and previous_state and previous_previous_state
Partition_DAG_map = {}
# references to the code for the tasks
# we access Partition_DAG_tasks for current state
Partition_DAG_tasks = {}

# version of DAG, incremented for each DAG generated. So DAG with partiton is version
# 1 and DAG with partitions 1 and 2 is version 2. First requested DAG is version 3,
# i.e., after executing partition 1 task and seeing partiton 2 is contined, we
# know we need a new DAG so we will request a new DAG having version: 
# current version number + 1 
Partition_DAG_version_number = 0
# Saving current_partition_name as previous_partition_name after processing the 
# current partition. We cannot just subtract one, e.g. PR3_1 becomes PR2_1, since
# the name of partition 2 might actually be PR2_1L, so we need to 
# save the actual name "PR2_1L" and retrive it when we process PR3_1.
# Used to generate the input (label) for the current partition (i.e., the input
# of the current partition is the outut of the previous partition. Note that 
# a partition i outputs only to partition i+1 (unless partition i is a sink, i.e.,
# the last partition in a connected component.)
Partition_DAG_previous_partition_name = "PR1_1"
# number of tasks in the DAG
Partition_DAG_number_of_tasks = 0
# the tasks in the last partition of a generated DAG may be incomplete,
# which means we cannot execute these tasks until the next DAG is 
# incrementally published. For partitions this is 1 or 0.
Partition_DAG_number_of_incomplete_tasks = 0
# True if we have built the entire graph (incrementally)
Partition_DAG_is_complete = False

#brc: num_nodes: 
Partition_DAG_num_nodes_in_graph = 0

deallocation_start_index = 1

def deallocate_Partition_DAG_structures(i):
    global Partition_all_fanout_task_names
    global Partition_all_fanin_task_names
    global Partition_all_faninNB_task_names
    global Partition_all_collapse_task_names
    global Partition_all_fanin_sizes
    global Partition_all_faninNB_sizes
    global Partition_DAG_leaf_tasks
    global Partition_DAG_leaf_task_start_states
    global Partition_DAG_leaf_task_inputs
    global Partition_DAG_states
    global Partition_DAG_map
    global Partition_DAG_tasks

    #brc: deallocate DAG map-based structures
    logger.info("deallocate_Partition_DAG_structures: partition_names: ")
    for name in BFS.partition_names:
        logger.info(name)
    partition_name = BFS.partition_names[i-1]
    state = Partition_DAG_states[partition_name]
    logger.info("deallocate_Partition_DAG_structures: partition_name: " + str(partition_name))
    logger.info("deallocate_Partition_DAG_structures: state: " + str(state))

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
    #del Partition_DAG_map[state]
    Partition_DAG_map[state] = None
    #del Partition_DAG_tasks[partition_name]
    Partition_DAG_tasks[partition_name] = None
    #del Partition_DAG_states[partition_name]
    Partition_DAG_states[partition_name] = None

    # We are not doing deallocations for these collections; they are not 
    # per-partition collectins, they are collections of fanin names, etc.

    #Partition_all_fanout_task_names
    #Partition_all_fanin_task_names
    #Partition_all_faninNB_task_names
    #Partition_all_collapse_task_names
    #Partition_all_fanin_sizes
    #Partition_all_faninNB_sizes
    #Partition_DAG_leaf_tasks
    #Partition_DAG_leaf_task_start_states
    #Partition_DAG_leaf_task_inputs

    # We are not doing deallocations for the non-collections
    #Partition_DAG_version_number
    #Partition_DAG_previous_partition_name
    #Partition_DAG_number_of_tasks
    #Partition_DAG_number_of_incomplete_tasks
    #Partition_DAG_is_complete
    #Partition_DAG_num_nodes_in_graph

def destructor():
    # deallocate memory. Called at the end of bfs()
    global Partition_all_fanout_task_names
    global Partition_all_fanin_task_names
    global Partition_all_faninNB_task_names
    global Partition_all_collapse_task_names
    global Partition_all_fanin_sizes
    global Partition_all_faninNB_sizes
    global Partition_DAG_leaf_tasks
    global Partition_DAG_leaf_task_start_states
    global Partition_DAG_leaf_task_inputs
    global Partition_DAG_states
    global Partition_DAG_map
    global Partition_DAG_tasks

    Partition_all_fanout_task_names = None
    Partition_all_fanin_task_names = None
    Partition_all_faninNB_task_names = None
    Partition_all_collapse_task_names = None
    Partition_all_fanin_sizes = None
    Partition_all_faninNB_sizes = None
    Partition_DAG_leaf_tasks = None
    Partition_DAG_leaf_task_start_states = None
    Partition_DAG_leaf_task_inputs = None
    Partition_DAG_states = None
    Partition_DAG_map = None
    Partition_DAG_tasks = None

# If we are not going to save or publish the DAG then there is no need
# to generate a DAG with all of its information. We instead generate a 
# partial DAG with the information that is needed for processing it.
# The required information is whether or not the DAG is complete. Note
# that the other information that we include in the DAG may be useful
# for debugging. This information is not proportional to the number of
# nodes/edges in the DAG.
def generate_partial_DAG_for_partitions(to_be_continued,number_of_incomplete_tasks):
    # The only partial information we need is Partition_DAG_is_complete
    # since BFS does access this in the incrmental DAG that is returned to it.
    # The other members aer small and may be useful for debugging.
    # version of DAG, incremented for each DAG generated
    global Partition_DAG_version_number
    # Saving current_partition_name as previous_partition_name at the 
    # end. We cannot just subtract one, e.g. PR3_1 becomes PR2_1 since
    # the name of partition 2 might actually be PR2_1L, so we need to 
    # save the actual name "PR2_1L" and retrive it when we process PR3_1
    global Partition_DAG_previous_partition_name
    global Partition_DAG_number_of_tasks
    global Partition_DAG_number_of_incomplete_tasks
#brc: num_nodes
    global Partition_DAG_num_nodes_in_graph
    global Partition_DAG_is_complete

    # for debugging
    show_generated_DAG_info = True

    logger.trace("")
    DAG_info_dictionary = {}
    # These key/value pairs were added for incremental DAG generation.

    # If there is only one partition in the entire DAG then it is complete and is version 1
    # and version 1 is given to the DAG_executor_driver,
    # DAG with partitions 1 and 2 is version 2, etc. Version 2 is the firat version executed
    # if version 1 is not complete, as it is given to the DAG_executor_driver. Version 3 is 
    # the first version requested by workers/lambdas (if Version 2 isn't complete,
    #  i.e., the last generated version of the DAG). If the interval for publishing DAGs
    # is, say, 4, then Version 3 has partitions 1 and 2, and 3, 4, 5, and 6 (i.e,
    # partitions 1 through 2+4=6)
    #
    # We do not increment the version numbr for incremental DAGs that are not 
    # published. In the trace, we print, e.g., "2P" for version "2 P"artial.
    #Partition_DAG_version_number += 1

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
#brc: issue: we are doing del; should we increment it instead?
# might work if we leave tasks alone and do other two? assuming execute before dealloc)
# then try inc.
# Some way to deal with deslloc before exec? perhaps do copies, which won't be too bad
# if we are doing dealloc. No this doesn't help. Need to know what's been executed and
# the monitor knows.
# Also, comment about workers can hev difft dags. Why did we change that? They have
# difft values of num tasks in DAG and num tasks executed so it messes up -1's?
# So give them local vars of both? Then requesting difft versions
    Partition_DAG_number_of_tasks = len(Partition_DAG_tasks)
    # For partitions, this is at most 1. When we are generating a DAG
    # of groups, there may be many groups in the incomplete last
    # partition and they will all be considered to be incomplete.
    Partition_DAG_number_of_incomplete_tasks = number_of_incomplete_tasks
#brc: num_nodes
    # The value of num_nodes_in_graph is set by BFS_input_graph
    # at the beginning of execution, which is before we start
    # DAG generation.  This value does not change.
    Partition_DAG_num_nodes_in_graph = num_nodes_in_graph

    DAG_info_dictionary["DAG_version_number"] = Partition_DAG_version_number
    DAG_info_dictionary["DAG_is_complete"] = Partition_DAG_is_complete
    DAG_info_dictionary["DAG_number_of_tasks"] = Partition_DAG_number_of_tasks
    DAG_info_dictionary["DAG_number_of_incomplete_tasks"] = Partition_DAG_number_of_incomplete_tasks
#brc: num_nodes:
    DAG_info_dictionary["DAG_num_nodes_in_graph"] = Partition_DAG_num_nodes_in_graph

    DAG_info_dictionary["DAG_map"] = None
    DAG_info_dictionary["DAG_states"] = None
    DAG_info_dictionary["DAG_leaf_tasks"] = None
    DAG_info_dictionary["DAG_leaf_task_start_states"] = None
    DAG_info_dictionary["DAG_leaf_task_inputs"] = None
    DAG_info_dictionary["all_fanout_task_names"] = None
    DAG_info_dictionary["all_fanin_task_names"] = None
    DAG_info_dictionary["all_faninNB_task_names"] = None
    DAG_info_dictionary["all_collapse_task_names"] = None
    DAG_info_dictionary["all_fanin_sizes"] = None
    DAG_info_dictionary["all_faninNB_sizes"] = None
    DAG_info_dictionary["DAG_tasks"] = None

#brc: Note: we are saving all the incemental DAG_info files for debugging but 
#     we probably want to turn this off otherwise.

    # filename is based on version number - Note: for partition, say 3, we
    # have output the DAG_info with partitions 1 and 2 as version 1 so 
    # the DAG_info for the newly added partition 3 will have partitions 1, 2, and 3 
    # and will be version 2 but named "DAG_info_incremental_Partition_3"
    file_name_incremental = "./DAG_info_incremental_Partition_" + str(Partition_DAG_version_number) + ".pickle"
    with open(file_name_incremental, 'wb') as handle:
        cloudpickle.dump(DAG_info_dictionary, handle) #, protocol=pickle.HIGHEST_PROTOCOL)  

    # for debugging
    if show_generated_DAG_info:
        logger.info("generate_DAG_info_incremental_partitions: generate_partial_DAG_for_partitions: partial DAG:")
        logger.info("DAG_version_number (which was not incemented for partial DAG): ")
        logger.info(str(Partition_DAG_version_number) + "P")
        logger.info("")
        logger.info("DAG_is_complete:")
        logger.info(str(Partition_DAG_is_complete))
        logger.info("")
        logger.info("DAG_number_of_tasks:")
        logger.info(str(Partition_DAG_number_of_tasks))
        logger.info("")
        logger.info("DAG_number_of_incomplete_tasks:")
        logger.info(str(Partition_DAG_number_of_incomplete_tasks))
        logger.info("")
    #brc: num_nodes
        logger.info("DAG_num_nodes_in_graph:")
        logger.info(str(Partition_DAG_num_nodes_in_graph))
        logger.info("")

    DAG_info = DAG_Info.DAG_info_fromdictionary(DAG_info_dictionary)
    return  DAG_info

# Called by generate_DAG_info_incremental_partitions below to generate 
# the DAG_info object when we are using an incremental DAG of partitions.
def generate_full_DAG_for_partitions(to_be_continued,number_of_incomplete_tasks):
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
#brc: num_nodes
    global Partition_DAG_num_nodes_in_graph
    global Partition_DAG_is_complete

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
#brc: num_nodes
    # The value of num_nodes_in_graph is set by BFS_input_graph
    # at the beginning of execution, which is before we start
    # DAG generation.  This value does not change.
    Partition_DAG_num_nodes_in_graph = num_nodes_in_graph

    DAG_info_dictionary["DAG_version_number"] = Partition_DAG_version_number
    DAG_info_dictionary["DAG_is_complete"] = Partition_DAG_is_complete
    DAG_info_dictionary["DAG_number_of_tasks"] = Partition_DAG_number_of_tasks
    DAG_info_dictionary["DAG_number_of_incomplete_tasks"] = Partition_DAG_number_of_incomplete_tasks
#brc: num_nodes:
    DAG_info_dictionary["DAG_num_nodes_in_graph"] = Partition_DAG_num_nodes_in_graph

#brc: Note: we are saving all the incemental DAG_info files for debugging but 
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
        logger.info("generate_DAG_info_incremental_partitions: generate_full_DAG_for_partitions: Full DAG:")
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
    #brc: num_nodes
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
#brc: num_nodes
        DAG_num_nodes_in_graph = DAG_info_Partition_read.get_DAG_num_nodes_in_graph()
        logger.trace("")
        logger.trace("DAG_info partition after read:")
        output_DAG = True
        if output_DAG:
            # FYI:
            logger.trace("DAG_map:")
            # We are in an incremental DAG generation method so we can 
            # iterate over the DAG; we cannot iterate over the map while 
            # some other thread is changing the map but we are the thread
            # that changes the map.
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
#brc: num_nodes
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
    #if not USE_INCREMENTAL_DAG_GENERATION:
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
        if not USE_INCREMENTAL_DAG_GENERATION:
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
#def generate_DAG_info_incremental_partitions(current_partition_name,current_partition_number,to_be_continued):
#brc: use of DAG_info: 
def generate_DAG_info_incremental_partitions(current_partition_name,current_partition_number,to_be_continued,
       num_incremental_DAGs_generated_since_base_DAG):

# to_be_continued is True if num_nodes_in_partitions < num_nodes, which means that incremeental DAG generation
# is not complete (some graph nodes are not in any partition.)
# current_partition_name generated as: "PR" + str(current_partition_number) + "_1".
# The base DAG is the DAG with complete partition 1 and incomplete partition 2. This is 
# the first DAG to be executed assuming the DAG has more than one partition.
# num_incremental_DAGs_generated_since_base_DAG is the number incremental DAGs generated since
# the base DAG; we publish every ith incremental DAG generated, where i can be set by user.
# 
# Note: references "num_incremental_DAGs_generated_since_base_DAG+1" 


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

#brc: num_nodes
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
    # Note: the only access of Partition_senders is here to print it.
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
    # This is static task clustering.
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

    # a list of partitions that have a fanout/fanin/collapse to the 
    # current partition. (They "send" to the current partition which "receives".)
    senders = Partition_receivers.get(current_partition_name)
    # Note: a partition that starts a new connected component (which is 
    # the first partition collected on a call to BFS(), of which there 
    # may be many calls if the graph is not connected) is a leaf
    # node and thus has no senders (value is None). This is true about 
    # partition 1 and this is assserted by the caller (BFS()) of this method.

    if DAG_executor_constants.DEALLOCATE_BFS_SENDERS_AND_RECEIVERS and (num_nodes_in_graph > DAG_executor_constants.THRESHOLD_FOR_DEALLOCATING_ON_THE_FLY):
        # Between calls to generate_DAG_info_incremental_partitions we add names to
        # Partition_senders, we can clear all of them.
        Partition_senders.clear()
        # if current_partition_name was a key in the map then delete. Note that we
        # grabbed Partition_receivers.get(current_partition_name) above so we are
        # done with Partition_receivers[current_partition_name] and can delete it.
        # (We know that current_partition either received input from the previous partition 
        # or the current_partition is a leaf task in which case it receives no input. We 
        # use "senders" to determine whether current_partition is a leaf node. 
        if senders is not None:
            # partitions that send to current_partition_name, which can only
            # be the previous partition
            del Partition_receivers[current_partition_name]

    """
    Outline: 
    Each call to generate_DAG_info_incremental_partitions adds one partition to the
    DAG_info. The added (current) is incomplete unless it is the last partition 
    that will be added to the DAG. The previous partition is now marked as complete.
    The previous partition's next partition is this current partition, which is 
    either complete or incomplete. If the current partition is incomplete then 
    the previous partition is marked as having an incomplete next partition. We also
    consider the previous partition of the previous partition. It was marked as complete
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
    in a connected component (besides the partition that is the last partition
    to be connected in the DAG) we would have to look at all the nodes in a 
    partition and determine whether they had any child nodes that were not in
    the same partition (i.e., these child nodes will be in the next partition).
    This would have to be done for each partition and it's not clear whether
    all that work would be worth it just to mark the last partition of a 
    connected component completed a little earlier than it otherwise would.
    Note this is only helpful if the incremental DAG generatd happens to 
    end with a partition that is the last partition in its connected compoent.
    If the interval n between incremental DAGs (i.e., add n partitions before
    publishng the new DAG) then it may be rare to have such a partition.
    2. (senders is None): This is a leaf node, which is true for partition 1 and
    could be true for any partition after that. (Consider several one node connected
    components.) This mena that the current partition is the first partition
    of a new connected component. We will add this leaf partition to a list of leaf
    partitions so that when we return we can make sure this leaf partition is 
    executed. (No other partition has a fanin/fanout/collapse to this partition so no
    other partition can cause this leaf node to be executed. We will start its execution
    ourselves.) The previous and previous previous partitons are marked as described above.
    Note the the previous partition can be marked complete as usual. Also, we now know that the 
    previous partition, which was marked as having an incomplete next partition, can now
    be marked as not having an incomplete next partition - this is because the previous
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
            assert senders is None , msg
        except AssertionError:
            logger.exception("[Error]: assertion failed")
            if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
                logging.shutdown()
                os._exit(0)
        #assertOld:
        #if senders is not None:
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
#brc: Q: use set?
        Partition_DAG_states[current_partition_name] = current_state

        # identify the function that will be used to execute this task
        if not DAG_executor_constants.USE_SHARED_PARTITIONS_GROUPS:
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
            if not DAG_executor_constants.USE_STRUCT_OF_ARRAYS_FOR_PAGERANK:
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
        #DAG_info = generate_full_DAG_for_partitions(to_be_continued,number_of_incomplete_tasks)
#brc: use of DAG_info:
        # bfs will save the DAG_info if the DAG is complete, i.e., the DAG has 
        # only one partition. In that case, we need a full DAG_info; otherwise, we can generate a
        # partial DAG info (since the DAG will not be executed - the first DAG executed in the base DAG
        # (with complete partition 1 and incomplete partition 2))
        if not to_be_continued:
            DAG_info = generate_full_DAG_for_partitions(to_be_continued,number_of_incomplete_tasks)
        else:
            DAG_info = generate_partial_DAG_for_partitions(to_be_continued,number_of_incomplete_tasks)

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
        #
#brc: use of DAG_info:
        # We only execute the first DAG, i.e., the DAG that only has
        # partition 1 if this DAG is complete. In that case, bfs will
        # not need to modify the incremental DAG since there are no more 
        # partitions. This means we do not actually have to do this copy
        # business; we do it here to be conistent with the other cases.
        # i.e., whenever we will execute the DAG we make these copies.
        # This is not alot of code to excute since the DAG has only 1 partition.
        #
        # Note: The DAG_executor gets a copy so that the 
        # DAG_executor is not reading a field of DAG_info that bfs can 
        # write/modify, which creates a race condition - wull DAG_excutor
        # read the DAG info of current DAG before bfs modifies the info as 
        # part of generating the next DAG.)

        #if to_be_continued:
        if not to_be_continued:
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
            #state_info_of_current_state_after_set = DAG_info_DAG_map[current_state]

            #logger.info("XXXXXXXXXX generate_DAG_info_incremental_partitions:: address of state_info_of_current_state for PR1_1 from DAG_info_DAG_map: " \
            #    + str(hex(id(state_info_of_current_state))))
            
            #logger.info("XXXXXXXXXX generate_DAG_info_incremental_partitions:: address of copy_of_state_info_of_current_state for PR1_1: " \
            #    + str(hex(id(copy_of_state_info_of_current_state))))
                
            DAG_info.set_DAG_map(DAG_info_DAG_map)
           
            #DAG_info_DAG_map_after_set_DAG_Map = DAG_info.get_DAG_map()

            #logger.info("generate_DAG_info_incremental_partitions: address of DAG_info_DAG_map: " \
            #       + str(hex(id(DAG_info_DAG_map_after_set_DAG_Map))))
            
            #state_info_of_current_state_after_set_DAG_Map = DAG_info_DAG_map_after_set_DAG_Map[current_state]

            #logger.info("XXXXXXXXXX generate_DAG_info_incremental_partitions:: address of state_info_of_current_state_after_set_DAG_Map for PR1_1 from DAG_info_DAG_map: " \
            #    + str(hex(id(state_info_of_current_state_after_set_DAG_Map))))

            #logger.info("XXXXXXXXXX generate_DAG_info_incremental_partitions:: address of Partition_DAG_map[current_state] for PR1_1: " \
            #   + str(hex(id(Partition_DAG_map[current_state]))))
                
            """
            # modify generator's state_info 
            Partition_DAG_map[current_state].fanins.append("goo")
            DAG_info_DAG_map[current_state].fanins.append("boo")

            # display DAG_executor's state_info objects
            logger.info("address DAG_info_DAG_map: " + str(hex(id(DAG_info_DAG_map))))
            logger.info("generate_DAG_info_incremental_partitions: DAG_info_DAG_map after state_info copy and adding goo/boo" 
                + " to fanins of Partition_DAG_map/DAG_info_DAG_map:")
            for key, value in DAG_info_DAG_map.items():
                logger.info(str(key) + ' : ' + str(value) + " addr of value: " + str(hex(id(value))))

            # display generator's state_info objects
            logger.info("address Partition_DAG_map (should be different from DAG_info_DAG_map): " + str(hex(id(Partition_DAG_map))))
            logger.info("generate_DAG_info_incremental_partitions: Partition_DAG_map after adding goo/boo to"                 
                + " to fanins of Partition_DAG_map/DAG_info_DAG_map:")
            for key, value in Partition_DAG_map.items():
                logger.info(str(key) + ' : ' + str(value) + " addr of value: " + str(hex(id(value))))

            # undo the modification to the generator's state_info
            Partition_DAG_map[current_state].fanins.clear()

            # display DAG_executor's state_info ojects
            logger.info("generate_DAG_info_incremental_partitions: Partition_DAG_map (should be no goo now):")
            for key, value in Partition_DAG_map.items():
                logger.info(str(key) + ' : ' + str(value))

            # display generator's state_info objects
            logger.info("generate_DAG_info_incremental_partitions: DAG_info_DAG_map after clear current state's fanins (so should still have boo):")
            for key, value in DAG_info_DAG_map.items():
                logger.info(str(key) + ' : ' + str(value))
            """

        logger.info("generate_DAG_info_incremental_partitions: returning from generate_DAG_info_incremental_partitions for"
            + " partition " + str(current_partition_name))
        
        #return DAG_info

    elif (senders is None):
        # Note: current_partition_number >= 2 (since we checked == 1 above)
        # (Note: Don't think we can have a length 0 senders, 
        # for current_partition_name. That is, we only create a 
        # senders when we get the first sender.
        #
        # Note: We know this is not partition 1 based on the above if-condition.
        # Note: Partition 2 can also be a leaf node, e.g., if partition 1 is a
        # leaf node of a single partition connected component then partition 2
        # will also be a leaf node (which is the first (and possibly only) partition
        # of the second connected component).

        try:
            msg = "[Error]: generate_DAG_info_incremental_partitions:" \
                + " partition has a senders with length 0."
            assert not(senders is not None and len(senders) == 0) , msg
        except AssertionError:
            logger.exception("[Error]: assertion failed")
            if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
                logging.shutdown()
                os._exit(0)
        # assertOld:
        #if senders is not None and len(senders) == 0:
        #    logger.error("[Error]: generate_DAG_info_incremental_partitions:"
        #        + " partition has a senders with length 0.")

        # This is not the first partition but it is a leaf partition, which means
        # it was the first partition generated by some call to bfs(), i.e., 
        # it is the start of a new connected component. This also means there
        # is no collapse from the previous partition to this partition, i.e.,
        # the previous partition has no collapse task, it is a sink.

        # Since this is a leaf node (it has no predecessor) we will need to add 
        # this partition to the work queue or start a new lambda for it
        # like the DAG_executor_driver does. (Note that since this partition has
        # no predecessor, no worker or lambda can enable this task via a fanout, collapse,
        # or fanin, thus we must make sure this leaf task gets executed.)
        # This is done by the caller (bfs()) of this method. bfs() 
        # deposits a new DAG_info and the deposit() processes the leaf tasks. 
        # (See deposit()) 
       
        # Mark this partition as a leaf task. If any more of these leaf task 
        # partitions are found they will accumulate in these lists. bfs()
        # uses these lists to identify leaf tasks - when BFS generates an 
        # incremental DAG_info, it adds work to the work queue or starts a
        # lambda for each leaf task that is not the first partition. The
        # first partition is always a leaf task and it is handled by the 
        # DAG_executor_driver. (For non-incremental ADG generation, all leaf
        # tasks are executed at the beginning.)

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
        # The last partition of a connected component does not do any collapse so it 
        # has no collapse to an incomplete partition. (Partitions only do collapses, not fanins/fanouts)
        state_info_of_previous_partition.fanout_fanin_faninNB_collapse_groups_partitions_are_ToBeContinued = False

        # we also track the partition previous to the previous partition.

        previous_previous_state = None
        state_info_of_previous_previous_partition = None
        if current_partition_number > 2:
            # if current is 3 then there is a previous 2 and a previous previous 1;
            # partitions statr at 0 so we need to be at least 3 in order to have a 
            # prvious previous.
            previous_previous_state = previous_state - 1
            state_info_of_previous_previous_partition = Partition_DAG_map[previous_previous_state]
            state_info_of_previous_previous_partition.fanout_fanin_faninNB_collapse_groups_partitions_are_ToBeContinued = False
        # For example, for the whiteboard example, we processed partition 1 and it was incomplete and we assumed it had
        # to be continued collapses/fanins/faninNBs/fanouts. We processed 2 and considered
        # 2 incomplete but 1 became complete but 1 has a collapse to 2 so 1' fanout_fanin_faninNB_collapse_groups_partitions_are_ToBeContinued 
        # is still True. The same happens for partition 3, which is set to incomplete while partition
        # 2 is set to complete and partition 1's fanout_fanin_faninNB_collapse_groups_partitions_are_ToBeContinued
        # is set to False as 2 is complete.
        # Partition 3 is the last partition in its connected component so the first call to bfs() 
        # complete and since some of the input graph's nodes aer unvisited bfs() is called again.
        # We process partition 4 here, which is the first partition of its (new) connected
        # component containing partitions 4 and 5. Now 4 is considered incomplete. The previously processed
        # partiton was partition 3, which was the last partition if the first connected componwnt
        # (containing partitions 1, 2, and 3). The previous partition 3 becomes complete and the
        # 3's fanout_fanin_faninNB_collapse_groups_partitions_are_ToBeContinued is set to False.
        # Also, the previous previous partitions 2's fanout_fanin_faninNB_collapse_groups_partitions_are_ToBeContinued 
        # can be set to False since partition 3 was set to complete (i.e., partiton 2 has no 
        # fanins/fanouts/collapses to an incomplete partition.)

        logger.info("generate_DAG_info_incremental_partitions: new connected component for current partition "
            + str(current_partition_number) + ", the previous_state_info for previous state " 
            + str(previous_state) + " after update TBC (no collapse) is: " 
            + str(state_info_of_previous_partition))

        # generate state in DAG
        Partition_DAG_map[current_state] = state_info(current_partition_name, fanouts, fanins, faninNBs, collapse, fanin_sizes, 
            faninNB_sizes, task_inputs,
            to_be_continued,
            # We do not know whether this first partition will have fanout_fanin_faninNB_collapse_groups_partitions_are_ToBeContinued
            # that are incomplete until we process the 2nd partition, except if to_be_continued
            # is False in which case there are no more partitions and no fanout_fanin_faninNB_collapse_groups_partitions_are_ToBeContinued
            # that are incomplete. If to_be_continued is True then we set fanout_fanin_faninNB_collapse_groups_partitions_are_ToBeContinued
            # to True but we may change this value when we process partition 2.
            to_be_continued)
        Partition_DAG_states[current_partition_name] = current_state

        # identify the function that will be used to execute this task
        if not DAG_executor_constants.USE_SHARED_PARTITIONS_GROUPS:
            Partition_DAG_tasks[current_partition_name] = PageRank_Function_Driver
        else:
            if not DAG_executor_constants.USE_STRUCT_OF_ARRAYS_FOR_PAGERANK:
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


#brc: use of DAG_info:
        #DAG_info = generate_full_DAG_for_partitions(to_be_continued,number_of_incomplete_tasks)

        # We publish the DAG if the graph is complete (not to be continued, i.e., all).
        # partitions are in the DAG. This is true whether the current partition is 1 or 2, 
        # or a later partition. If the current partition is 1 and the graph is complete,
        # then we will save the DAG and start the DAG_executor_driver, which 
        # will read and execute the one partition DAG. If the current partition is 2, we save 
        # the DAG whether it is complete or not and start the DAG_excutor_driver,
        # which will read and execute the DAG. (If partition 1 is not compplete
        # we do not save the DAG; DAG execution will start with a DAG that
        # has a complete partition 1 and an (in)complete partition 2.) For partitions
        # greater than 2, we publish the DAG on an interval that is set in 
        # DAG_executor_constants. We also use num_incremental_DAGs_generated_since_base_DAG
        # to determine whether to publish such a DAG. The base DAG is the one
        # with complete partition 1 and incomplete partition 2. After we save this base 
        # DAG, we increment num_incremental_DAGs_generated_since_base_DAG every time we generate
        # a DAG. So when the current partition is 3 we increment 
        # num_incremental_DAGs_generated_since_base_DAG and it becomes 1. If
        # the interval is 2, we do not publish this new DAG with partitions 
        # 1, 2, and 3. We will instead publish the DAG with partitions 1 thru 4,
        # since num_incremental_DAGs_generated_since_base_DAG will have the value
        # 2 and 2%2 == 0. Note however that the increment of 
        # num_incremental_DAGs_generated_since_base_DAG doesn't occur 
        # until *after* the bfs() call to generate_full/partitil_DAG_info_incremental_partitions(),
        # (which is the method we are in here).
        # That is, bfs() calls method generate_full/partial_DAG_info_incremental_partitions()
        # and after that bfs() increments num_incremental_DAGs_generated_since_base_DAG,
        # (if the current_partition is > 2). So bfs() generates the new incremental DAG using 
        # this method we aer in and then if bfs() finds current_partition > 2 it increments
        # num_incremental_DAGs_generated_since_base_DAG and uses it to check
        # whether it needs to publish the new DAG. Note that the condition for 
        # publishing, in addition to checking the publication interval, also checks whether 
        # the DAG is complete or whether the current partition is 2. (We always publish the 
        # DAG if it is complete (i.e., regardless of the interval calculation) and we save the 
        # DAG and start the DAG_excutor_driver if partition is 2 (and the 
        # DAG is complete or incomplete))
        #
        # Since bfs() will increment num_incremental_DAGs_generated_since_base_DAG 
        # and use the new value (num_incremental_DAGs_generated_since_base_DAG+1) 
        # to check whether to publish the DAG (returned by this method),
        #  in this method we use (num_incremental_DAGs_generated_since_base_DAG+1).

        if current_partition_number <= 2:
            #The current_partition_number is 2 - if it were 1 we would have 
            # taken the branch above.
            try:
                msg = "[Error]: generate_DAG_info_incremental_partitions:" \
                    + " current_partition_number <= 2 but it is not 2, it is " \
                    + str(current_partition_number)
                assert current_partition_number == 2 , msg
            except AssertionError:
                logger.exception("[Error]: assertion failed")
                if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
                    logging.shutdown()
                    os._exit(0)
            # if the current partition is 2 (whether
            # or not the DAG is complete) bfs will save the DAG and start 
            # executing it, so generate a full DAG
            DAG_info = generate_full_DAG_for_partitions(to_be_continued,number_of_incomplete_tasks)
        else:
            #   The current_partition_number is 3 or more
            #   Note: This next condition is True if the DAG is complete; 
            #   or if the current partition should be published based on the 
            #   interval. Note that we use (num_incremental_DAGs_generated_since_base_DAG+1)
            #   For example, if we just added partition 3 to the incremental
            #   DAG, the current value of num_incremental_DAGs_generated_since_base_DAG
            #   is 0 since no other DAG have been generated since we generated
            #   the base DAG (with partitions 1 and 2). So after generating this DAG with
            #   partitions 1, 2, and 3, bfs will increment num_incremental_DAGs_generated_since_base_DAG
            #   to 1, and use the new value 1 to determine whether to publish
            #   this new DAG. Thus we use (num_incremental_DAGs_generated_since_base_DAG+1)
            #   which will be 1 to determine whether to generate a full or partial 
            #   DAG for this DAG with partitions 1, 2, and 3. If the interval for 
            #   publishing DAGs is 2, then 1%2 does not equal 0, so we generate a 
            #   partial DAG, which is fine since we will not publish the DAG.
            #   The next incremental DAG, with partitions 1-4 will be published and 
            #   thus we will generate a full DAG (since 2%2 == 0). Note that 
            #   if the DAG with partitions 1-3 is complete (i.e., not to be continued)
            #   then it will be published since this condition also checks not to_be_continued).
            if (not to_be_continued) \
                 or (num_incremental_DAGs_generated_since_base_DAG+1) % DAG_executor_constants.INCREMENTAL_DAG_DEPOSIT_INTERVAL == 0:
                DAG_info = generate_full_DAG_for_partitions(to_be_continued,number_of_incomplete_tasks)
            else:
                DAG_info = generate_partial_DAG_for_partitions(to_be_continued,number_of_incomplete_tasks)

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
        # Note: see the comment below for this same block of code. It has 
        # additional explanation.
        # Note: see the comment above about the use of num_incremental_DAGs_generated_since_base_DAG+1

#brc: use of DAG_info:
        # We only need to make the copies if we will be publishing/executing 
        # this DAG. Note: not DAG is to_be_continued ==> DAG is complete
        if current_partition_number == 2 or (
             not to_be_continued or (
             (num_incremental_DAGs_generated_since_base_DAG+1) % DAG_executor_constants.INCREMENTAL_DAG_DEPOSIT_INTERVAL == 0
             )):
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
                # we deepcopy the entire this state_info object. This ss not so bad since
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
                
    #brc: We need to do this for the previous_state too, since when we generate the incremental
    # DAG for the next state, we change the state info for the previous state and the previous
    # previous state. When we are processing the next state, the previou_state is what are
    # are now calling the current_state and the previous_previous state is what we are now
    # calling the previous_state. So when we are processing the next state, we need to make
    # sure that here in the DAG generator we are not writing the same state info object that 
    # the DAG_executor is reading for execution. (The DAG_executor, when it is executing a
    # state s that is continued, has an assertion that is compares the TBC of s and the 
    # fanin/fanout TBC of the previous state. They should be equal. But if the DAG_generator
    # changes the fanin/fanout TBC of the previous state concurrently these values may be unequal.
    # Note that we will have cloned s but not the state previous to s since here we clone
    # current state but not previous state.)

                if current_partition_number > 1:
                    copy_of_state_info_of_previous_partition = copy.deepcopy(state_info_of_previous_partition)
                    DAG_info_DAG_map[previous_state] = copy_of_state_info_of_previous_partition

                if current_partition_number > 2:
                    copy_of_state_info_of_previous_previous_partition = copy.deepcopy(state_info_of_previous_previous_partition)
                    DAG_info_DAG_map[previous_previous_state] = copy_of_state_info_of_previous_previous_partition

                #logger.info("XXXXXXXXXX generate_DAG_info_incremental_partitions:: address of state_info_of_current_state from DAG_info_DAG_map: " \
                #    + str(hex(id(state_info_of_current_state))))
                
                #logger.info("XXXXXXXXXX generate_DAG_info_incremental_partitions:: address of copy_of_state_info_of_current_state: " \
                #    + str(hex(id(copy_of_state_info_of_current_state))))
                
                #if current_partition_number > 1:           
                    #logger.info("XXXXXXXXXX generate_DAG_info_incremental_partitions:: address of state_info_of_previous_partition from DAG_info_DAG_map: " \
                    #    + str(hex(id(state_info_of_previous_partition))))
                
                    #logger.info("XXXXXXXXXX generate_DAG_info_incremental_partitions:: address of copy_of_state_info_of_previous_partition: " \
                    #    + str(hex(id(copy_of_state_info_of_previous_partition))))

                #if current_partition_number > 2:
                    #logger.info("XXXXXXXXXX generate_DAG_info_incremental_partitions:: address of state_info_of_previous_previous_partition from DAG_info_DAG_map: " \
                    #    + str(hex(id(state_info_of_previous_previous_partition))))
                    
                    #logger.info("XXXXXXXXXX generate_DAG_info_incremental_partitions:: address of copy_of_state_info_of_previous_previous_partition: " \
                    #    + str(hex(id(copy_of_state_info_of_previous_previous_partition))))
                
                DAG_info.set_DAG_map(DAG_info_DAG_map)

                #logger.info("generate_DAG_info_incremental_partitions: address of DAG_info_DAG_map: " \
                #        + str(hex(id(DAG_info_DAG_map))))

                #state_info_of_current_state_after_set = DAG_info_DAG_map[current_state]
                #if current_partition_number > 1:  
                #    state_info_of_previous_state_after_set = DAG_info_DAG_map[previous_state]
                #if current_partition_number > 2:  
                #    state_info_of_previous_previous_state_after_set = DAG_info_DAG_map[previous_previous_state]

                #logger.info("XXXXXXXXXX generate_DAG_info_incremental_partitions:: address of state_info_of_current_state from DAG_info_DAG_map: " \
                #    + str(hex(id(state_info_of_current_state))))
                
                #logger.info("XXXXXXXXXX generate_DAG_info_incremental_partitions:: address of copy_of_state_info_of_current_state: " \
                #    + str(hex(id(copy_of_state_info_of_current_state))))
                
                #logger.info("XXXXXXXXXX generate_DAG_info_incremental_partitions:: address of state_info_of_current_state_after_set from DAG_info_DAG_map: " \
                #   + str(hex(id(state_info_of_current_state_after_set))))

                #logger.info("XXXXXXXXXX generate_DAG_info_incremental_partitions:: address of Partition_DAG_map[current_state]: " \
                #    + str(hex(id(Partition_DAG_map[current_state]))))
                
                #if current_partition_number > 1:             
                    #logger.info("XXXXXXXXXX generate_DAG_info_incremental_partitions:: address of state_info_of_previous_state_after_set from DAG_info_DAG_map: " \
                    #    + str(hex(id(state_info_of_previous_state_after_set))))

                    #logger.info("XXXXXXXXXX generate_DAG_info_incremental_partitions:: address of Partition_DAG_map[previous_state]: " \
                    #    + str(hex(id(Partition_DAG_map[previous_state]))))
                
                #if current_partition_number > 2:
                    #logger.info("XXXXXXXXXX generate_DAG_info_incremental_partitions:: address of state_info_of_previous_previous_state_after_set: " \
                    #    + str(hex(id(state_info_of_previous_previous_state_after_set))))

                    #logger.info("XXXXXXXXXX generate_DAG_info_incremental_partitions:: address of Partition_DAG_map[previous_previous_state]: " \
                    #    + str(hex(id(Partition_DAG_map[previous_previous_state]))))
    
                # This used to test the deep copy - modify the state info
                # of the generator and make sure this modification does 
                # not show up in the state_info object given to the DAG_executor.
                """
                # modify generator's state_info 
                Partition_DAG_map[current_state].fanins.append("goo")
                DAG_info_DAG_map[current_state].fanins.append("boo")

                # display DAG_executor's state_info objects
                logger.info("generate_DAG_info_incremental_partitionsX: address DAG_info_DAG_map: " + str(hex(id(DAG_info_DAG_map))))
                logger.info("generate_DAG_info_incremental_partitionsX: DAG_info_DAG_map after state_info copy and adding goo/boo" 
                    + " to fanins of Partition_DAG_map/DAG_info_DAG_map:")
                for key, value in DAG_info_DAG_map.items():
                    logger.info("generate_DAG_info_incremental_partitionsX: " + str(key) + ' : ' + str(value) + " addr of value: " + str(hex(id(value))))

                # display generator's state_info objects
                logger.info("generate_DAG_info_incremental_partitionsX: address Partition_DAG_map (should be different from DAG_info_DAG_map): " + str(hex(id(Partition_DAG_map))))
                logger.info("generate_DAG_info_incremental_partitionsX: Partition_DAG_map after adding goo/boo to"                 
                    + " to fanins of Partition_DAG_map/DAG_info_DAG_map:")
                for key, value in Partition_DAG_map.items():
                    logger.info("generate_DAG_info_incremental_partitionsX: " + str(key) + ' : ' + str(value) + " addr of value: " + str(hex(id(value))))

                # undo the modification to the generator's state_info
                Partition_DAG_map[current_state].fanins.clear()

                # display generator's state_info objects
                logger.info("generate_DAG_info_incremental_partitionsX: DAG_info_DAG_map after clear current state's fanins (so should still have boo):")
                for key, value in DAG_info_DAG_map.items():
                    logger.info("generate_DAG_info_incremental_partitionsX: " + str(key) + ' : ' + str(value))

                # display DAG_executor's state_info ojects
                logger.info("generate_DAG_info_incremental_partitionsX: Partition_DAG_map (should be no goo now):")
                for key, value in Partition_DAG_map.items():
                    logger.info("generate_DAG_info_incremental_partitionsX: " + str(key) + ' : ' + str(value))

                # Do same for previous state
                # Note: DAG_info_DAG_map[current_state] still has "boo" in fanins. Now both current_state
                # and previous_state have "boo"
                Partition_DAG_map[previous_state].fanins.append("goo")
                DAG_info_DAG_map[previous_state].fanins.append("boo")

                # display DAG_executor's state_info objects
                logger.info("generate_DAG_info_incremental_partitionsX: address DAG_info_DAG_map: " + str(hex(id(DAG_info_DAG_map))))
                logger.info("generate_DAG_info_incremental_partitionsX: DAG_info_DAG_map after state_info copy and adding goo/boo" 
                    + " to fanins of Partition_DAG_map/DAG_info_DAG_map:")
                for key, value in DAG_info_DAG_map.items():
                    logger.info("generate_DAG_info_incremental_partitionsX: " + str(key) + ' : ' + str(value) + " addr of value: " + str(hex(id(value))))

                # display generator's state_info objects
                logger.info("generate_DAG_info_incremental_partitionsX: address Partition_DAG_map (should be different from DAG_info_DAG_map): " + str(hex(id(Partition_DAG_map))))
                logger.info("generate_DAG_info_incremental_partitionsX: Partition_DAG_map after adding goo/boo to"                 
                    + " to fanins of Partition_DAG_map/DAG_info_DAG_map:")
                for key, value in Partition_DAG_map.items():
                    logger.info("generate_DAG_info_incremental_partitionsX: " + str(key) + ' : ' + str(value) + " addr of value: " + str(hex(id(value))))

                # undo the modification to the generator's state_info
                Partition_DAG_map[previous_state].fanins.clear()

                # display generator's state_info objects
                logger.info("generate_DAG_info_incremental_partitionsX: DAG_info_DAG_map after clear current state's fanins (so should still have boo):")
                for key, value in DAG_info_DAG_map.items():
                    logger.info("generate_DAG_info_incremental_partitionsX: " + str(key) + ' : ' + str(value))

                # display DAG_executor's state_info ojects
                logger.info("generate_DAG_info_incremental_partitionsX: Partition_DAG_map (should be no goo now):")
                for key, value in Partition_DAG_map.items():
                    logger.info("generate_DAG_info_incremental_partitionsX: " + str(key) + ' : ' + str(value))

                #logging.shutdown()
                #os._exit(0)
                """

        logger.info("generate_DAG_info_incremental_partitions: returning from generate_DAG_info_incremental_partitions for"
            + " partition " + str(current_partition_name))
        
        #return DAG_info
    
    else: # current_partition_number >= 2

        try:
            msg = "[Error]: generate_DAG_info_incremental_partitions:" \
                + " partition has a senders with length 0."
            assert not (len(senders) == 0) , msg
        except AssertionError:
            logger.exception("[Error]: assertion failed")
            if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
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
            if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
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
                
        # sender is first and only element in set senders since a partition i is followed by (sends to) partition i+1
        # We checked above if senders is None so it is not None here. (Senders is none only for leaf tasks.)
        # Note: We only use this in the assert that follows. We know that the sender is the previous partition.
        sender = next(iter(senders)) # first element of senders

        try:
            msg = "[Error]: generate_DAG_info_incremental_partitions using partitions and" \
                + " the sender for a partition is not previous_partition_name."
            assert sender == Partition_DAG_previous_partition_name , msg
        except AssertionError:
            logger.exception("[Error]: assertion failed")
            if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
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

        logger.info("generate_DAG_info_incremental_partitions: for current partition " 
            + current_partition_name + " the previous_state_info after update collpase and TBC: " 
            + str(state_info_of_previous_partition))
 
        # we track the previous partition of the previous partition
        previous_previous_state = None
        state_info_of_previous_previous_partition = None

        if current_partition_number > 2:
            previous_previous_state = previous_state - 1
            state_info_of_previous_previous_partition = Partition_DAG_map[previous_previous_state]
            state_info_of_previous_previous_partition.fanout_fanin_faninNB_collapse_groups_partitions_are_ToBeContinued = False
            logger.info("XXXXXXXXXX generate_DAG_info_incremental_partitions: for current partition "
                + current_partition_name + " the previous_previous_state_info after update collpase and TBC: " 
                + str(state_info_of_previous_previous_partition)
                + " address of state_info_of_previous_previous_partition: " + str(hex(id(state_info_of_previous_previous_partition))))

        # generate DAG information
        Partition_DAG_map[current_state] = state_info(current_partition_name, fanouts, fanins, faninNBs, collapse, fanin_sizes, 
            faninNB_sizes, task_inputs,
            # to_be_continued parameter can be true or false
            to_be_continued,
            # We do not know whether this current partition will have fanout_fanin_faninNB_collapse_groups_partitions_are_ToBeContinued
            # that are incomplete until we process the next partition, except if to_be_continued
            # is False in which case there are no more partitions and no fanout_fanin_faninNB_collapse_groups_partitions_are_ToBeContinued
            # that are incomplete. If to_be_continued is True then we set fanout_fanin_faninNB_collapse_groups_partitions_are_ToBeContinued
            # to True but we may change this value when we process the next partition. Note that 
            # if the current partition turns out to be the last partition in its connected component,
            # then when we process the next partition, which is the first partition in the next connected
            # component, we know that this current partition is complete and has no fanins/fanouts/collapses
            # to an incomplete partition as it is the last partition in its connected component. The 
            # first partition of the next connected component can be complete (if there are moer partitions in
            # this component but this current partion, which is the last partition of its component by definition
            # has no fanins/fanouts/collapses to the first partition of te next component.))
            to_be_continued)
        Partition_DAG_states[current_partition_name] = current_state
        # See the example above

        # identify function used to execute this pagerank task (see comments above)
        if not DAG_executor_constants.USE_SHARED_PARTITIONS_GROUPS:
            Partition_DAG_tasks[current_partition_name] = PageRank_Function_Driver
        else:
            if not DAG_executor_constants.USE_STRUCT_OF_ARRAYS_FOR_PAGERANK:
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

#brc: use of DAG_info:
        #DAG_info = generate_full_DAG_for_partitions(to_be_continued,number_of_incomplete_tasks)
        # We publish the DAG if the graph is complete (not to be continued).
        # This is true whether the current partition is 1 or 2, or a later
        # partition. If the current partition is 1 and the graph is complete,
        # then we will save the DAG and start the DAG_executor_driver, which 
        # will read and execute the DAG. If the current partition is 2, we save 
        # the DAG whether it is complete or not and start the DAG_excutor_driver,
        # which will read and execute the DAG. (If partition 1 is not complete
        # we do not save the DAG; DAG execution will start later with a DAG that
        # has a complete partition 1 and an (in)complete partition 2.) For partitions
        # greater than 2, we publish the DAG on an interval that is set in 
        # DAG_executor_constants. We also use num_incremental_DAGs_generated_since_base_DAG
        # to determine whether to publish such a DAG. The base DAG is the one
        # with partitions 1 and 2. After we save this base DAG, we increment 
        # num_incremental_DAGs_generated_since_base_DAG every time we generate
        # a DAG. So when the current partition is 3 we increment 
        # num_incremental_DAGs_generated_since_base_DAG and it becomes 1. If
        # the interval is 2, we do not publish this new DAG with partitions 
        # 1, 2, and 3. We will instead publish the DAG with partitions 1 thru 4,
        # since num_incremental_DAGs_generated_since_base_DAG will have the value
        # 2 and 2%2 == 0. Note however that the increment of 
        # num_incremental_DAGs_generated_since_base_DAG doesn't occur 
        # until *after* this call to generate_DAG_info_incremental_partitions(),
        # that is bfs() calls this method generate_DAG_info_incremental_partitions()
        # and after that it will increment num_incremental_DAGs_generated_since_base_DAG
        # if current_partition is > 2. So we generate the new incremental DAG
        # and then if we find current_partition is > 2 we increment
        # num_incremental_DAGs_generated_since_base_DAG and use it to check
        # whether we need to publish the new DAG. Note that the condition for 
        # this also checks whether the DAG is complete or whether the 
        # current partition is 2. (We always publish the DAG if it complete
        # (i.e., regardless of the interval calculation) and we save the 
        # DAG and start the DAG_excutor_driver if partition is 2 (nd the 
        # DAG is complete or incomplete))
        #
        # This condition reflects the fact that we will have incremented
        # num_incremental_DAGs_generated_since_base_DAG by the time we check
        # if current_partition_number>2, i.e, we use 
        # (num_incremental_DAGs_generated_since_base_DAG+1) here.

        if current_partition_number <= 2:
            #The current_partition_number is 2 - if it were 1 we would have 
            # taken the first main branch above.
            try:
                msg = "[Error]: generate_DAG_info_incremental_partitions:" \
                    + " current_partition_number <= 2 but it is not 2, it is " \
                    + str(current_partition_number)
                assert current_partition_number == 2 , msg
            except AssertionError:
                logger.exception("[Error]: assertion failed")
                if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
                    logging.shutdown()
                    os._exit(0)
            # if the current partition is 2 (whether
            # or not the DAG is complete) bfs will save the DAG and start 
            # executing it, so generate a full DAG
            DAG_info = generate_full_DAG_for_partitions(to_be_continued,number_of_incomplete_tasks)
        else:
            #   The current_partition_number is 3 or more
            #   Note: This next condition is True if the DAG is complete; 
            #   or if the current partition should be published based on the 
            #   interval. Note that we use (num_incremental_DAGs_generated_since_base_DAG+1)
            #   For example, if we just added partition 3 to the incremental
            #   DAG, the current value of num_incremental_DAGs_generated_since_base_DAG
            #   is 0 since no other DAG have been generated since we generated
            #   the base DAG (with partitions 1 and 2). So after generating this DAG with
            #   partitions 1, 2, and 3, bfs will increment num_incremental_DAGs_generated_since_base_DAG
            #   to 1, and use the new value 1 to determine whether to publish
            #   this new DAG. Thus, here we use (num_incremental_DAGs_generated_since_base_DAG+1)
            #   which will be 1 to determine whether to generate a full or partial 
            #   DAG for this DAG with partitions 1, 2, and 3. If the interval for 
            #   publishing DAGs is 2, then 1%2 does not equal 0, so we generate a 
            #   partial DAG, which is fine since we will not publish the DAG.
            #   The next incremental DAG, with partitions 1-4 will be published and 
            #   thus we will generate a full DAG (since 2%2 == 0). Note that 
            #   if the DAG with partitions 1-3 is complete (i.e., not to be continued)
            #   then it will be published since this condition also checks not to_be_continued).
            # 
            # Note: we also did generate_full_DAG_for_partitions when current_partition_number is 2
            if (not to_be_continued) \
                 or (num_incremental_DAGs_generated_since_base_DAG+1) % DAG_executor_constants.INCREMENTAL_DAG_DEPOSIT_INTERVAL == 0:
                DAG_info = generate_full_DAG_for_partitions(to_be_continued,number_of_incomplete_tasks)
            else:
                DAG_info = generate_partial_DAG_for_partitions(to_be_continued,number_of_incomplete_tasks)

        # If we will generate another DAG make sure that the state_info of 
        # current partition is not shared with the DAG_executor
        # as we will change (write) this state_info when we generate the 
        # next DAG and this change might be concurrent with the DAG_executor
        # reading this state_info. This would be a problem since we do not 
        # want to make the reads/writes of the state_info atomic. Instead,
        # we deep copy the state_info object so we have two seperate state_info
        # objects for the current_state, i.e., the state_info object read by the DAG_executor
        # is different from the state_info object in the DAG info we are maintaining
        # for incremental DAG generation. (We add partitions to this information 
        # one by one) This ensures that the state_info object (for current_state) we write in the 
        # during next DAG generation (at which point current_state is previous_state)
        # is not the same state_info object for current_state read by DAG_executor.
        #
#brc: use of DAG_info:
        # We only need to make the copies if we will be publishing/executing 
        # this DAG. Note: not DAG is to_be_continued ==> DAG is complete
        if current_partition_number == 2 or (
             not to_be_continued or (
             (num_incremental_DAGs_generated_since_base_DAG+1) % DAG_executor_constants.INCREMENTAL_DAG_DEPOSIT_INTERVAL == 0
             )):
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
                if not USE_INCREMENTAL_DAG_GENERATION:
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
                # This state_info value object has the same reference in both maps so 
                # it does not matter which map we retrieve the state_info reference from.
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

    #brc: We need to do this for the previous_state too, since when we generate the incremental
    # DAG for the next state, we change the state info for the previous state and the previous
    # previous state. When we are processing the next state, the previou_state is what are
    # are now calling the current_state and the previous_previous state is what we are now
    # calling the previous_state. So when we are processing the next state, we need to make
    # sure that here in the DAG generator we are not writing the same state info object that 
    # the DAG_executor is reading for execution. (The DAG_executor, when it is executing a
    # state s that is continued, has an assertion that is compares the TBC of s and the 
    # fanin/fanout TBC of the previous state. They should be equal. But if the DAG_generator
    # changes the fanin/fanout TBC of the previous state concurrently these values may be unequal.
    # Note that we will have cloned s but not the state previous to s since here we clone
    # current state but not previous state.)

                copy_of_state_info_of_previous_partition = copy.deepcopy(state_info_of_previous_partition)
                DAG_info_DAG_map[previous_state] = copy_of_state_info_of_previous_partition
                
                if current_partition_number > 2:
                    copy_of_state_info_of_previous_previous_partition = copy.deepcopy(state_info_of_previous_previous_partition)
                    DAG_info_DAG_map[previous_previous_state] = copy_of_state_info_of_previous_previous_partition

                #logger.info("XXXXXXXXXX generate_DAG_info_incremental_partitions:: address of state_info_of_current_state from DAG_info_DAG_map: " \
                #    + str(hex(id(state_info_of_current_state))))
                
                #logger.info("XXXXXXXXXX generate_DAG_info_incremental_partitions:: address of copy_of_state_info_of_current_state: " \
                #    + str(hex(id(copy_of_state_info_of_current_state))))
                
                #logger.info("XXXXXXXXXX generate_DAG_info_incremental_partitions:: address of state_info_of_previous_partition from DAG_info_DAG_map: " \
                #    + str(hex(id(state_info_of_previous_partition))))
                
                #logger.info("XXXXXXXXXX generate_DAG_info_incremental_partitions:: address of copy_of_state_info_of_previous_partition: " \
                #    + str(hex(id(copy_of_state_info_of_previous_partition))))
                
                #if current_partition_number > 2:
                    #logger.info("XXXXXXXXXX generate_DAG_info_incremental_partitions:: address of state_info_of_previous_previous_partition from DAG_info_DAG_map: " \
                    #    + str(hex(id(state_info_of_previous_previous_partition))))
                    
                    #logger.info("XXXXXXXXXX generate_DAG_info_incremental_partitions:: address of copy_of_state_info_of_previous_previous_partition: " \
                    #    + str(hex(id(copy_of_state_info_of_previous_previous_partition))))          

                DAG_info.set_DAG_map(DAG_info_DAG_map)
                    
                #logger.info("generate_DAG_info_incremental_partitions: address of DAG_info_DAG_map: " \
                #        + str(hex(id(DAG_info_DAG_map))))

                #state_info_of_current_state_after_set = DAG_info_DAG_map[current_state]
                #state_info_of_previous_state_after_set = DAG_info_DAG_map[previous_state]
                #if current_partition_number > 2:
                #    state_info_of_previous_previous_state_after_set = DAG_info_DAG_map[previous_previous_state]

                #logger.info("XXXXXXXXXX generate_DAG_info_incremental_partitions:: address of state_info_of_current_state from DAG_info_DAG_map: " \
                #    + str(hex(id(state_info_of_current_state))))
                
                #logger.info("XXXXXXXXXX generate_DAG_info_incremental_partitions:: address of copy_of_state_info_of_current_state: " \
                #   + str(hex(id(copy_of_state_info_of_current_state))))
                
                #logger.info("XXXXXXXXXX generate_DAG_info_incremental_partitions:: address of state_info_of_current_state_after_set from DAG_info_DAG_map: " \
                #    + str(hex(id(state_info_of_current_state_after_set))))

                #logger.info("XXXXXXXXXX generate_DAG_info_incremental_partitions:: address of Partition_DAG_map[current_state]: " \
                #    + str(hex(id(Partition_DAG_map[current_state]))))

                #logger.info("XXXXXXXXXX generate_DAG_info_incremental_partitions:: address of state_info_of_previous_state_after_set from DAG_info_DAG_map: " \
                #    + str(hex(id(state_info_of_previous_state_after_set))))

                #logger.info("XXXXXXXXXX generate_DAG_info_incremental_partitions:: address of Partition_DAG_map[previous_state]: " \
                #    + str(hex(id(Partition_DAG_map[previous_state]))))
                    
                #if current_partition_number > 2:
                
                    #logger.info("XXXXXXXXXX generate_DAG_info_incremental_partitions:: address of state_info_of_previous_previous_state_after_set from DAG_info_DAG_map: " \
                    #    + str(hex(id(state_info_of_previous_previous_state_after_set))))

                    #logger.info("XXXXXXXXXX generate_DAG_info_incremental_partitions:: address of Partition_DAG_map[previous_previous_state]: " \
                    #    + str(hex(id(Partition_DAG_map[previous_previous_state]))))

                # This code is used to test the deep copy - modify the state info
                # maintained by the generator and make sure this modification does 
                # not show up in the state_info object given to the DAG_executor.

                """
                # modify generator's state_info 
                Partition_DAG_map[current_state].fanins.append("goo")
                DAG_info_DAG_map[current_state].fanins.append("boo")

                # display DAG_executor's state_info objects
                logger.info("generate_DAG_info_incremental_partitionsX: address DAG_info_DAG_map: " + str(hex(id(DAG_info_DAG_map))))
                logger.info("generate_DAG_info_incremental_partitionsX: DAG_info_DAG_map after state_info copy and adding goo/boo" 
                    + " to fanins of Partition_DAG_map/DAG_info_DAG_map:")
                for key, value in DAG_info_DAG_map.items():
                    logger.info("generate_DAG_info_incremental_partitionsX: " + str(key) + ' : ' + str(value) + " addr of value: " + str(hex(id(value))))

                # display generator's state_info objects
                logger.info("generate_DAG_info_incremental_partitionsX: address Partition_DAG_map (should be different from DAG_info_DAG_map): " + str(hex(id(Partition_DAG_map))))
                logger.info("generate_DAG_info_incremental_partitionsX: Partition_DAG_map after adding goo/boo to"                 
                    + " to fanins of Partition_DAG_map/DAG_info_DAG_map:")
                for key, value in Partition_DAG_map.items():
                    logger.info("generate_DAG_info_incremental_partitionsX: " + str(key) + ' : ' + str(value) + " addr of value: " + str(hex(id(value))))

                # undo the modification to the generator's state_info
                Partition_DAG_map[current_state].fanins.clear()

                # display generator's state_info objects
                logger.info("generate_DAG_info_incremental_partitionsX: DAG_info_DAG_map after clear current state's fanins (so should still have boo):")
                for key, value in DAG_info_DAG_map.items():
                    logger.info("generate_DAG_info_incremental_partitionsX: " + str(key) + ' : ' + str(value))

                # display DAG_executor's state_info ojects
                logger.info("generate_DAG_info_incremental_partitionsX: Partition_DAG_map (should be no goo now):")
                for key, value in Partition_DAG_map.items():
                    logger.info("generate_DAG_info_incremental_partitionsX: " + str(key) + ' : ' + str(value))

                # Do same for previous state
                # Note: DAG_info_DAG_map[current_state] still has "boo" in fanins. Now both current_state
                # and previous_state have "boo"
                Partition_DAG_map[previous_state].fanins.append("goo")
                DAG_info_DAG_map[previous_state].fanins.append("boo")

                # display DAG_executor's state_info objects
                logger.info("generate_DAG_info_incremental_partitionsX: address DAG_info_DAG_map: " + str(hex(id(DAG_info_DAG_map))))
                logger.info("generate_DAG_info_incremental_partitionsX: DAG_info_DAG_map after state_info copy and adding goo/boo" 
                    + " to fanins of Partition_DAG_map/DAG_info_DAG_map:")
                for key, value in DAG_info_DAG_map.items():
                    logger.info("generate_DAG_info_incremental_partitionsX: " + str(key) + ' : ' + str(value) + " addr of value: " + str(hex(id(value))))

                # display generator's state_info objects
                logger.info("generate_DAG_info_incremental_partitionsX: address Partition_DAG_map (should be different from DAG_info_DAG_map): " + str(hex(id(Partition_DAG_map))))
                logger.info("generate_DAG_info_incremental_partitionsX: Partition_DAG_map after adding goo/boo to"                 
                    + " to fanins of Partition_DAG_map/DAG_info_DAG_map:")
                for key, value in Partition_DAG_map.items():
                    logger.info("generate_DAG_info_incremental_partitionsX: " + str(key) + ' : ' + str(value) + " addr of value: " + str(hex(id(value))))

                # undo the modification to the generator's state_info
                Partition_DAG_map[previous_state].fanins.clear()

            # display generator's state_info objects
                logger.info("generate_DAG_info_incremental_partitionsX: DAG_info_DAG_map after clear current state's fanins (so should still have boo):")
                for key, value in DAG_info_DAG_map.items():
                    logger.info("generate_DAG_info_incremental_partitionsX: " + str(key) + ' : ' + str(value))

                # display DAG_executor's state_info ojects
                logger.info("generate_DAG_info_incremental_partitionsX: Partition_DAG_map (should be no goo now):")
                for key, value in Partition_DAG_map.items():
                    logger.info("generate_DAG_info_incremental_partitionsX: " + str(key) + ' : ' + str(value))

                #logging.shutdown()
                #os._exit(0)
                """

        logger.info("generate_DAG_info_incremental_partitions: returning from generate_DAG_info_incremental_partitions for"
            + " partition " + str(current_partition_name))

    # If we aew are doing incemental DAG generation and we are using simulated lambas
    # and we are deallocating pats of the DAG_info before we send the DAG_info
    # to a lambda then we need to make a deep copy of the DAG_info.
    # This is because it is possible that the deallocations for/to DAG_info will be
    # done concurrently with the simulated lambda referecing the ADG_info. Thus
    # we give the simulated lambda a (seperate) copy of the DAG_info. When we use
    # real lambdas, the deallocations to DAG_info are done on the tcp_server and
    # then a serialized copy of DAG_info is sent to the lambdas, so the real lambdas 
    # have a seperate copy of the DAG_info.
    if DAG_executor_constants.RUN_ALL_TASKS_LOCALLY and not DAG_executor_constants.USING_WORKERS \
            and DAG_executor_constants.USE_INCREMENTAL_DAG_GENERATION \
            and DAG_executor_constants.DEALLOCATE_DAG_INFO_STRUCTURES_FOR_LAMBDAS \
            and (num_nodes_in_graph > DAG_executor_constants.THRESHOLD_FOR_DEALLOCATING_ON_THE_FLY):
        DAG_info_copy = copy.deepcopy(DAG_info)
        return DAG_info_copy
    else:
        return DAG_info

def deallocate_DAG_structures(current_version_number_DAG_info):
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
    # of current_version_number_DAG_info so the value of 
    # 2+((current_version_number_DAG_info-2)*DAG_executor_constants.INCREMENTAL_DAG_DEPOSIT_INTERVAL))-2
    # will be 2+((1-2) * 4)-2 = -4 so we will deallocate from 1 to -4 so no deallocations
    # as expected. (We cannot deallocate 1 or 2 yet as we are not done with them - in version
    # 2 partition 2 becomes not to-be-continued and partition 1 now has no collapse to a 
    # continued state, so we restart executing the DAG by accessing 1's state information
    # to get 1's collpase task 2, which we can now execute. We already excuted 1 and then
    # stopped execution when we saw 1's collapse was to a to-be-continued state 2)
    # Note: Given the formula 2+((current_version_number_DAG_info-2)*DAG_executor_constants.INCREMENTAL_DAG_DEPOSIT_INTERVAL))-2,
    # the value given by this formula cannot become greater than 1 until workers request version 3. For
    # version 2, current_version_number_DAG_info-2 is 2-2 = 0, so we get 2+0-2=0.
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
    global deallocation_start_index
    deallocation_end_index = (2+((current_version_number_DAG_info-2)*DAG_executor_constants.INCREMENTAL_DAG_DEPOSIT_INTERVAL))-2

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

    logger.info("deallocate_DAG_structures: deallocation_start_index: " + str(deallocation_start_index)
        + " deallocation_end_index: " + str(deallocation_end_index))
    for i in range(deallocation_start_index, deallocation_end_index):
        logger.info("generate_DAG_info_incremental_partitions: deallocate " + str(i))
        deallocate_Partition_DAG_structures(i)
    
    # set start to end if we did a deallocation, i.e., if start < end. 
    # Note that if start equals end, then we did not do a deallocation since 
    # end is exclusive. (And we may have just incremented end, so dealllocating 
    # "1 to 1", with start = 1 and end = 1, was implemented as incrementing 
    # end to 2 and using range(1,2) so start < end for the deallocation "1 to 1"
    # Note that end was exclusive so we can set start to end instead of end+1.
    if deallocation_start_index < deallocation_end_index:
        deallocation_start_index = deallocation_end_index

"""
Suppose we have generated version 5. then we added ... to the partitions in version 4. If
DAG execution has kept up and workers are requsting version 5, the, we can deallocate
... in version 4 and we can assume partitions ... were deallocated when workers requested 
version 3, i.e.,  deallocation_start_index is x which was the value of deallocation_end_index+1
when ...

Note that whe workers request version i they can get version i or a later version j, j>i,
(i.e., the last version deposited >= version i). Version j will include all the partitions 
that were in version i so we give workers the latest version generated.

"""

""" OLD
def deallocate_DAG_structures(current_partition_number,current_version_number_DAG_info,
        num_incremental_DAGs_generated_since_base_DAG):
#brc: deallocate DAG structures

    # Set the deallocation_indices if current_partition_number is 2, in which case we publish 
    # the DAG or current_partition_number > 2 and we will publish the DAG. Note that we do not 
    # set the deallocation_indices if current_partition_number is 1.
    # Note that we do this regardless of the value of to_be_continued. If to_be_continued is 
    # True then incremental DAG generation is continuing so we want to deallocate space as 
    # we go. If to_be_continued is False then incremental DAG generation is over so but
    # we may still have some time before the execution of the DAG ends. In that case, we are 
    # still going to deallocate memory now. If execution is in fact ending soon then we did not 
    # need to deallocate now since we are just going to terminate DAG generation and execution
    # soon.

    logger.info("generate_DAG_info_incremental_partitions: current_version_number_DAG_info: "
        + str(current_version_number_DAG_info))

    if current_partition_number == 2 or \
        (
        current_partition_number > 2 
        and
        ((num_incremental_DAGs_generated_since_base_DAG+1) % DAG_executor_constants.INCREMENTAL_DAG_DEPOSIT_INTERVAL == 0)
        ):

        # Note: need to declare these as global variables:
        start_deallocation_index = -1
        stop_deallocation_index = -1
        next_deallocation_index = -1

        global start_deallocation_index
        global stop_deallocation_index
        global next_deallocation_index

        start_deallocation_index = stop_deallocation_index
        stop_deallocation_index = next_deallocation_index
        next_deallocation_index = current_partition_number
        logger.info("generate_DAG_info_incremental_partitions: set start, stop next: start_deallocation_index: " + str(start_deallocation_index)
            + ", stop_deallocation_index: " + str(stop_deallocation_index) 
            + ", next_deallocation_index: " + str(next_deallocation_index))

        # Examples:
        # If interval is 2, then we process partition 1 and publish 2 to get 
        # start = -1, stop = -1, next = 2. We then process 3 and publish 4, to get
        # start = -1, stop = 2, next = 4. We then process 5 and publish 6, to get
        # start = 2, stop = 4, next = 6. 

        # If interval is 1, then we process partition 1 and publish 2 to get 
        # start = -1, stop = -1, next = 2. We then publish 3, to get
        # start = -1, stop = 2, next = 3. We then publish 4 to get
        # start = 2, stop = 3, next = 4. 

        # If interval is 4, then we process partition 1 and publish 2 to get 
        # start = -1, stop = -1, next = 2. We then publish 6, to get
        # start = -1, stop = 2, next = 6. We then publish 10 to get
        # start = 2, stop = 6, next = 10. 
    else:
        logger.info("generate_DAG_info_incremental_partitions: do not set start, stop next: start_deallocation_index: ")
    # Don't do any deallocation until after we have processed/published partition 2. Actually,
    # start is -1 until we have published 3 times, i.e., partition 2 (always published)
    # and two more partitions. If the publishing intervals is 1, then we publish 2, 3, 
    # and 4, which is the earliest we can do a deallocation, i.e., we have processed
    # 4 partitions and published 3.
    if start_deallocation_index != -1:
        logger.info("generate_DAG_info_incremental_partitions: start_deallocation_index is not -1.")
        if current_partition_number > 2:
            logger.info("generate_DAG_info_incremental_partitions: current_partition_number is " 
                + str(current_partition_number) + ", so greater than 2.")
            if ((num_incremental_DAGs_generated_since_base_DAG+1) % DAG_executor_constants.INCREMENTAL_DAG_DEPOSIT_INTERVAL == 0):
                # deallocate from start_deallocation_index-1 (including start_deallocation_index-1) to stop_deallocation_index-1 (not including stop_deallocation_index-1),
                # where range(2,6) means from 2 (including 2) to 6 (but not including 6):
                logger.info("generate_DAG_info_incremental_partitions: publish so do deallocations:")
                for i in range(start_deallocation_index-1, stop_deallocation_index-1):
                    logger.info("generate_DAG_info_incremental_partitions: deallocate " + str(i))


                    deallocate_Partition_DAG_structures(i)

                # Examples:
                # If interval is 2, then we get start = 2, stop = 4, next = 6 and we do
                # for i in range(1, 3) # from 1 (including 1) to 2 (not including 5)
                #    deallocate_Partition_DAG_structures(i)
                # which will deallocate Partition_DAG structures for indices 1 and 2

                # If interval is 1, then we get start = 2, stop = 3, next = 4 and we do
                # for i in range(1, 2) # from 1 (including 1) to 2 (not including 2)
                #    deallocate_Partition_DAG_structures(i)
                # which will deallocate Partition_DAG structures for index 1.

                # If interval is 4, then we get start = 2, stop = 6, next = 10 and we do
                # for i in range(1, 5)  # from 1 (including 1) to 5 (not including 5)
                #    deallocate_Partition_DAG_structures(i)
                # which will deallocate for 1, 2, 3 and 4. Next deallocation will be 
                # with start = 6, stop = 10, next = 14, which deallocates 
                # Partition_DAG structures for indices 5, 6, 7, and 8.
            else:
                logger.info("generate_DAG_info_incremental_partitions: no publish so np deallocations:")
        else:
            logger.info("generate_DAG_info_incremental_partitions: current_partition_number is " 
            + str(current_partition_number) + " no deallocation.")  
    else:
        logger.info("generate_DAG_info_incremental_partitions: start_deallocation_index is -1, no deallocation.")
"""