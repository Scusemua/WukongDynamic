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
#from .DAG_executor_constants import ENABLE_RUNTIME_TASK_CLUSTERING
#from .DAG_executor_constants import EXIT_PROGRAM_ON_EXCEPTION
from . import DAG_executor_constants

from .BFS_generate_DAG_info import Group_senders, Group_receivers
from .BFS_generate_DAG_info import leaf_tasks_of_groups_incremental
from .BFS_generate_DAG_info import groups_num_shadow_nodes_map
#from .BFS_generate_DAG_info import num_nodes_in_graph
from . import BFS

# Note: avoiding circular imports:
# https://stackoverflow.com/questions/744373/what-happens-when-using-mutual-or-circular-cyclic-imports
#from . import BFS

"""
Consider the group-based DAG for the whiteboard example:

                G1
             /  |   \
            /   |    \
          v     v     v
        G2----> G3L   G4     # G3L indicates group G3 has a cycle of nodes
             /   |    |
            /    |    |
          v      v    v
        G5----> G6    G7

To construct the DAG incrementally:
- generate the first group G1. This group is 5 --> 17 --> 1 (note: B --> A means B is the parent of A) where we added 
  1 to the bfs queue at the start, then we dequeued 1 and  did dfs(1) to get the parent 17 of 1 and the parent 5 of 17.
- This first group is also the first partition (which has one group)
- We know the nodes of group 1 but group 1 is incomplete since we do not know which groups have parent nodes that are in 
  group 1. We say group 1 is "to-be-continued". the BFS queue will contain 5, 17 an 1 (there are added to the bfs queueu
  in the reverse order that they weer visited since dfs is recursive and they are added as the recursion unwinds).
  We will visit the child of 5, 17, and 1, in that order to get the groups of partition 2. These groups are G2,
  G3L ('L' since G3 contains a cycle of nodes), and G4.
- At this point, group 1 is now complete - we know its nodes and we know the edges from group 1 to the other groups,
  which are all in partition 2. the groups G3, G3L, and G4 are all incomplete, since we know their nodes but we have 
  not visited the childen of these nodes, which will all be nodes in (the group of) partition 3. 
- In general, as we identify the nodes/groups in partition i, we complete the groups in partition i-1.
Note: When we execute the DAG, a group Gi wil be executed by a lambda/worker process. After execution, we will process all 
the pagerank values of of the nodes that are parent nodes to nodes that are in another group . For example, for group G1
above, ndoe 5 in G1 is the parent of node 16 in G2, so group G2 needs the pagerank value computed for 5 in order to compute
the pagerank value for 16. (Noet that 16 has two parents 10 and 5 and 10's value is local to group G2) Since G1 has a fanout
to G2, G1 will send the pagerank value of 5 to G2 as part of the fanout from G1 to G2. G1 will also send its pagerank
(output) value for node 17 in G1 to group G3L as part of the fanin from G1 to G3L. (This is actally a FaninNB ("NB" = No Become)
since G1 also has fanouts to G2 and G4 so G1 cannot "become" G3L) So communication between lammbdas/workers executing groups happen
through the synchronization objects (fanin or faninNB) or through fanouts. The values communicated are the pagerank values of the
parent nodes that are needed by children nodes in other groups. Note: Group G2 in partition 2 has the parent value of node 2
that is needed by node 20 in group G3L.

An expanded version of the above whiteboard DAG that shows all the nodes is shown below. The parent edges between nodes in a group 
are not shown, e.g., 5-->17 as 5 is the paretn of 17. The edges shown are edges between groups/tasks in the DAG, whcih are formed
from child edges between the nodes, e.g., G1 --> G2 since 5 in G1 has a child 16 in G2. So we visit 1 then 17 then 5, add 
them to the bfs queue in rverse order 5, 17, 1. Visit the child 16 of 5 to get G2, visit the child 19 of 17 to get G2L,
and visit the child 12 of 1 to get G4. 

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

In the code below, we add the groups of the current partition to the DAG.s

The first partition has one group G1. It is added as an incomplete group, i.e., it is to-be-continued.

For the remaining groups/partition, when bfs() identifies the current groups in the current partition Pi, we:
- add each group of Pi as an incomplete group. Essentially, this mans that the fanins/fanouts for the group are empty since we only
  identify these fanins/fanouts when we generate the groups in the next partition Pi+1. At that point. Pi+1 will be the 
  current partition and Pi will be the previous partition. Note alsothat Pi-1 is the previous-previous partition.
- The groups in the previous partition Pi-1 are now complete. Thus we mark the groups is Pi-1 as complete groups. We also 
  generate the fanin/fanout sets for these groups since we identified all the edges between the groups of the previous
  partition and the groups of the current partition when we  generated the groups of the current partition. For example, 
  in the whiteboard DAg above, G1 is incomplete. We identify groups G2, G3L and G4 and the edges between G1 and these
  groups. So we mark G1 as complete and fill in it fanout/ fanin sets as fanouts = {G2, G4}, fanins = {}, faninNBs={G3L}.
  Note that G1's collapse = {}. A collapse is the case where a group S has one out edge anf that edge is to a group T 
  where T has one in edge which is from S. In the whiteboard DAG, G4 has a collapse to G7; this is a cluster that is done
  when we generate the ADAG instead of at runtime. So the worker/lambda that executes G4 will clster G7, i.e., this 
  lambda/worker will also execute G7. G4 has two parent pagerank values that are needed by the child nodes in G7.
- As we just said, when we process current partition Pi, we compute the fanins/fanout/collapse of Pi-1 and mark
  Pi-1 as complete (so not to-be-continued). We also track for each group G, in addition to whether or not it
  is complete, whether it has fanins/fanouts/collapse to groups that are incomplete. For example, when we process
  the groups G2, G3L, and G4 in the current partition P2,we set G1 to complete, but G1 has fanins/fanouts/collapse
  (that are crrently unknown) to the incomplete groups G2, G3L, G4 in P2. So G2 is marked as complete but we also 
  mark G1 as having fanins/fanouts/collapses to incomplete
  groups (in this case G2, G3L and G4). Note that when  we process current group Pi, we set all the groups
  in Pi-1 to complete (as described in the previous step). We also set all the groups in partition Pi-2 to have 
  NO fanins/fanouts/collapse to incomplete groups, since the groups in Pi-1 were set to complete. For example, when we
  process partition 3 above, we set the groups G2, G3L, and G4 to complete, and we set group G1 to have no complete 
  fanins/fanouts/collapse.
Complication: Note that partition 3 is the last partition in the DAG. We will see this since all of the nodes will have
been added to some partition P1, P2, or P#. Thus, when we process partition 3, we can set its groups to be complete as none
of the nodes in th groups of this partition have any child edges to nodes in another partition (as there are no more partitons). 
This means we also can set the groups in partition 2 to be compete (as usual) but also we can set the groups in partition 2 to have
no fanins/fanouts/collapse to an incomplete group. Lkewise as usual for the group in partition 1. The complication is that 
the groups in partition 3, while not having any edges to groups in the next partition (as there is no next partition) do 
have intra-partition edges, e.g., G2-->G3L. In the normal case, these intra-partition dges would be detected when we processed
partition 4, i.e., we would find the intra-partition edges between, the groups of partition 3, and the inter-partition edges between 
parttiton 3 and partiton 4, when we processed partition 4. But there, is no partition 4 so we need to ensure that the intra-partition
edges of partition 3 aer detected when we process partition 3 as the current partition. When processing the current partiton, 
the code for detecting edges would typically only consider the groups in the previous partition, since we just detected the
edges between the groups in the previous partition and the groups in the current partition. But we also detectd the 
intra-partiton edges between the groups in the current partition. So to handle this complication, we compute a 
set of "groups to consider" which is normally the groups in the previous partition but when the current partition 
is the last partition in the DAG we also add the groups of the current partition to the "groups to consider". 
For exmaple, when partitin 3 is the current partition, it is the last partiton in the DAG. Thus we need to compute
the fanins/fanouts/collapse of the groups G3, G3L, and G4 in previous partition 2 (based on the edges that were 
idetified by bfs() when it generated current partition 3) and we need to compute the fanous/fanins/collapse of the 
groups G5, G6, and G7 in current partition P3. We do this by letting "groups to consider" be the groups G2, G3L,and 
G4 plus the groups G5, G6, and G7. This ensures that G5 will have a faninNB to G6. (G3L also has a faninNB to G6., 
as well as a fanout to G5.)
Complication:
We need to detect the edges between the groups in the previous partition (the previous groups) and between
the groups in the previous partition and the groups in the current partition (and as described above, possibly
between the groups of the current partition.) If not to_be_continued, groups_to_consider contains groups from 
groups_of_previous_partition and groups from groups_of_current_partition. We have:
    for previous_group in groups_to_consider:
        # get groups that previous_group sends inputs to. These
        # groups "receive" inputs from the sender (which is previous_group)
        #receiver_set_for_previous_group = Group_senders[previous_group]
        receiver_set_for_previous_group = Group_senders.get(previous_group,[])
        for receiverY in receiver_set_for_previous_group:

Here, previous_group can be from groups_of_previous_partition or groups_of_current_partition. Group receiverY can 
be a group in groups_of_previous_partition (i.e., it is a group in the same partition as previous_group) or a group in 
# groups_of_current_partition. We need to know whether previous_group and receiverY are both in groups_of_previous_partition. Thus:
    if previous_group in groups_of_previous_partition  \
    and receiverY in groups_of_previous_partition:
    # previous_group sends output to receiverY
    # and both are in groups_of_previous_partition
    if to_be_continued:
        previous_groups_with_TBC_faninsfanoutscollapses.append(receiverY)
# The previous_group and group receiverY are both in groups_of_previous_partition. They both assumed
# to have fanins/fanouts/collapses to groups in the current partition, and the groups in the current partition
# are incomplete (i.e., they are to-be-continued (TBC)). This the groups in the previous partition (including previous
# group and receiverY are marked as having fanins/fanouts to TBC groups (i.e., to the groups in the current 
# partition).This is a simplifying assumption - a group in the previous partition might not have a fanin/fanout
# to a group in the current partition. Note that the previous groups are processed left to right in the 
# previous partition, and the leftmost previous group will either be a fanout of the previous previous group or
# a fanin/faninNB of groups in the previous previous group. The point is that there can be no edge from
# some other previous group to the leftmost previous group. Thus, when we execute the fanins/fanouts of the
# previous previous groups, the left most previous group will be excuted. For example, in the white board 
# DAG, group PR2_1 is the left most group of partition 2. It is a fanout of group PR1_1 in partition 1. When
# we execute the fanouts of group PR1_1, group PR2_1 is a fanout of PR1_1 so group PR2_1 will get executed.
# After previous group is excuted, we check whether we can excute its fanouts/fanins. IF previous group
# has fanouts/fanins to groups that are incomplete, we cannot execute those fanouts/fanins; instead, we
# make previous_group a continued task. This means we will process the fanis/fanouts of previous_group after
# we obtains a new incremental DAG. Fron the if-statement above, previous_group has a fanout/fanin to receiverY 
# which means we will not process this fanin/fanout and group reveiverY will not be executed until after
# we obtain a new incremental DAG. 
# For example, in the extended whiteboard DAG (with extra connected components 4->5 and 6-7) the first DAG 
# has group PR1_1 and the groups PR2_1, PR2_2L, and PR2_3. PR1_1 will be continued since it has TBC fanouts 
# and a faninN to PR2_1 and PE2_3 and to PR2_2L, respectively. (That is, groups PR2_1, PR2_2L, and PR2_3
@ are incomplete (to-be-continued (TBC)) so PR1_1 has fanins/fanouts to TBC groups.) When the new DAG
# is published with the groups in partition 3, PR1_1 will be continued which means its fanout and faninNB are 
# executed. PR2_3 is a fanout task so PR1_1 will fanout PR2_3 and PR2_3 will be executed. Group PR2_2L is a 
# faninNB so PR1_1 will execute a fanin for PR2_2L and since this is not the last fanin (which will be 
# done later by PR2_1) group/task PR2_2L (i.e., the fanin task of PR2_2L) is not executed. Group PR2_1 is 
# the become task for PR1_1 (one of PR1_1's fanout tasks is the become task for PR1_1) so PR2_1 is executed. 
# Note the PR2_1 has a faninNB to group PR2_2L in the same partition. PR2_1, since it is a previous group
# is assumed to have fanins/fanouts to incomplete/TBC groups. (These incomplete/TBC groups are groups
# in the current partition. PR2_1 may not actually have fanins/fanouts to groups in the current partition
# and in the extended WB grsph it does not. The assumption simplifies things.) Since PR2_1 is marked as
# having fanins/fanouts to TBC groups, we cannot process these fanins/fanouts of PR2_1 so PR2_1 is 
# saved as a continued task. (When we get a new DAG, we will process the fanins/fanouts of all the 
# continued tasks, whcih no longer will have fanins/fanouts to TBC groups. Note that the current partition 
# in the current DAG becomes the previous partition in the next DAG and the groups in the previous partiton
# of the next DAG all become complete. By adding the current partition to the incremental DAG, all of the
# edges from the groups in the previous partition to the groups in the current partition became known 
# (like PR2_2L --> PR3_2) and the (lists of edges for the) groups in the previous partition are now complete. 
# This includes the edges between groups of the previous partition like PR2_1 --> PR2_2L.) This means 
# PR2_1 will execute its fanin on PR2_2L when the next incremental DAG is generated. Note that whwn 
# PR1_1 was continued, it did execute all of its fanouts (PR2_1 and PR2_3) and its fanin to PR2_2L, 
# but PR2_2L is a faninNB so this single fanin from PR1_1 was not enough to execute PR2_2L (the fanin from
# PR2_1 is also required before PR2_2L can be executed.) Then when PR2_1 was executed we could not do 
# its fanin to PR2_2L since PR2_1 is assumed to have TBC fanins/fanouts (to groups in the current 
# partition which will be partiton 3.) In this case, it really is okay to execute PR2_2L, but we cannot see 
# that without looking closer at the DAG. That is, we assumed PR2_1 has TBC fanins/fanouts to partition
# 3, and maybe it does or maybe it doesn't (in the extended white board example it does not), but this 
# means we cannot process its fanins/fanouts, including the fanin the PR2_2L, even though PR2_2L is 
# not a TBC group. One possible optimization is to see that PR2_2L is not incomplete and thus exexcute
# the fanin to PR2_2L. Then when we saved PR2_1 as a continued task, we would have to rememeber that 
# we did process its fanin to PR2_2L, so that when we continued PR2_1 (after getting a new DAG) we don't
# process this fanin to PR2_2L again. (Of course, PR2_1 in this case has no other fanins/faninNBs so 
# we do not actually have to continue it, and on and on ... Likewise, PR2_1 actually has no fanins/fanouts
# to partition 3 so we do not actually have to mark it as having such fanins/fanouts, which is another
# possible optimization. It is not clear that these optimizations would be worth the extra work.
# Note that in the original whiteboard example, partition 3 is the last partition of the DAG, so partition 3 is 
# marked as complete (i.e., not TBC) thus group PR2_2L does not have any TBC fanouts/fanins (as the groups in partition 3 are 
# not TBC (they are complete)). So PR2_1 will do its fanin for PR2_2L and PR2_2L will execute and do 
# its fanout to PR3_1 and its fanin to faninNB PR3_2. Also, consider the extended white board example.
# If the second incremental DAG has not just partitions 1 - 3 but also partition 4 (and possibly 
# partition 5, 6, and 7) then groups PR2_1, PR2_2L, and PR2_3 and all the groups in partition 3 will be 
# complete and none of the groups in partition 2 will have TBC fanouts/fanins to any grroup in 
# partition 3 (since partition 3 will be complete). So the above scenario involving PR2_1 and R2_2L
# does not occur at all unless the are "previous groups", i.e., the current partition (last partition 
# added to the incrmental DAG) is partition 3.
#
# So in the above scenario, receiverY (e.g., PR2_2L) cannot be executed even though it is not incomplete.
# When we get a new incremental DAG, we compute the number of tasks (groups or partitions) that can be 
# executed befre we need to get a new incremental DAG. (This is needed when we ar using worker threads
# or processes). The number of tasks that can be executed is the number of tasks in the incremental
# DAG minus the number of incomplete tasks minus the number of coomplete tasks like receiverY that
# cannot be executed. We track a list of such tasks (groups or partitions) so we can compute the
# number of such tasks (as the length of this list)

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

#brc: num_nodes
# this is set by BFS.input_graph when doing non-incremental DAG generation with groups
num_nodes_in_graph = 0

# See the comments in BFS_generate_DAG_info_incremental_partitions.py. Processng
# group is very similar to processing partitons. We generate a partition and 
# process the groups within the partition. Partition processing should be understood
# before attempting to understand group partitioning.

# Note: This group of names and sizes is for debugging only. We probably do not want
# to collect these for large graphs.
Group_all_fanout_task_names = []
Group_all_fanin_task_names = []
Group_all_faninNB_task_names = []
Group_all_collapse_task_names = []
Group_all_fanin_sizes = []
Group_all_faninNB_sizes = []

# It is not clear how large these leaf task lists can be. We mya want to delete 
# leaf tasks from these lists on-the-fly
Group_DAG_leaf_tasks = []
Group_DAG_leaf_task_start_states = []
# no inputs for leaf tasks
Group_DAG_leaf_task_inputs = []

# maps task name to an integer ID for that task.
# We access Group_DAG_states[current_state] and Group_DAG_states[previous_state]
Group_DAG_states = {}
# maps integer ID of a task to the state for that task; the state 
# contains the fanin/fanout information for the task.
Group_DAG_map = {}
# references to the code for the tasks
Group_DAG_tasks = {}
#brc: bug fix:
Group_DAG_number_of_groups_of_previous_partition_that_cannot_be_executed = 0
# version of DAG, incremented for each DAG generated
Group_DAG_version_number = 0

#brc: ToDo: Not using this for groups - we have to get *all* the groups
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


#brc: num_nodes:
Group_DAG_num_nodes_in_graph = 0

deallocation_start_index_partitions = 1
deallocation_start_index_groups = 1

def deallocate_Group_DAG_structures(i):
    global Group_all_fanout_task_names
    global Group_all_fanin_task_names
    global Group_all_faninNB_task_names
    global Group_all_collapse_task_names
    global Group_all_fanin_sizes
    global Group_all_faninNB_sizes
    global Group_DAG_leaf_tasks
    global Group_DAG_leaf_task_start_states
    global Group_DAG_leaf_task_inputs
    global Group_DAG_states
    global Group_DAG_map
    global Group_DAG_tasks

    #brc: deallocate DAG map-based structures
    logger.info("deallocate_Group_DAG_structures: group_names: ")
    for name in BFS.group_names:
        logger.info(name)
    group_name = BFS.group_names[i-1]
    state = Group_DAG_states[group_name]
    logger.info("deallocate_Group_DAG_structures: Group_name: " + str(group_name))
    logger.info("deallocate_Group_DAG_structures: state: " + str(state))

    # We may be iterating through these data structures in, e.g., DAG_executor concurrently 
    # with attempts to to the del on the data structure, which would be an error. We only 
    # iterate when we are tracing the DAG for debugging so we could turn these iterations
    # off when we set the deallocate option on. 
    # Note: we can change Group_DAG_tasks and Group_DAG_states to maps instead of 
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
    Group_DAG_map[state] = None
    #del Partition_DAG_tasks[partition_name]
    Group_DAG_tasks[group_name] = None
    #del Partition_DAG_states[partition_name]
    Group_DAG_states[group_name] = None

    # We are not doing deallocations for these collections; they are not 
    # per-partition collectins, they are collections of fanin names, etc.

    #Group_all_fanout_task_names
    #Group_all_fanin_task_names
    #Group_all_faninNB_task_names
    #Group_all_collapse_task_names
    #Group_all_fanin_sizes
    #Group_all_faninNB_sizes
    #Group_DAG_leaf_tasks
    #Group_DAG_leaf_task_start_states
    #Group_DAG_leaf_task_inputs

    # We are not doing deallocations for the non-collections
    #Group_DAG_version_number
    #Group_DAG_previous_partition_name
    #Group_DAG_number_of_tasks
    #Group_DAG_number_of_incomplete_tasks
    #Group_DAG_is_complete
    #Group_DAG_num_nodes_in_graph

def destructor(): 
    global Group_all_fanout_task_names
    global Group_all_fanin_task_names
    global Group_all_faninNB_task_names
    global Group_all_collapse_task_names
    global Group_all_fanin_sizes
    global Group_all_faninNB_sizes
    global Group_DAG_leaf_tasks
    global Group_DAG_leaf_task_start_states
    global Group_DAG_leaf_task_inputs
    global Group_DAG_states
    global Group_DAG_map
    global Group_DAG_tasks

    Group_all_fanout_task_names = None
    Group_all_fanin_task_names = None
    Group_all_faninNB_task_names = None
    Group_all_collapse_task_names = None
    Group_all_fanin_sizes = None
    Group_all_faninNB_sizes = None
    Group_DAG_leaf_tasks = None
    Group_DAG_leaf_task_start_states = None
    Group_DAG_leaf_task_inputs = None
    Group_DAG_states = None
    Group_DAG_map = None
    Group_DAG_tasks = None


# If we are not going to save or publish the DAG then there is no need
# to generate a DAG with all of its information. We instead generate a 
# partial DAG with the information that is needed for processing it.
# The required information is whether or not the DAG is complete. Note
# that the other information that we include in the DAG may be useful
# for debugging. This information is not proportional to the number of
# nodes/edges in the DAG.
def generate_partial_DAG_for_groups(to_be_continued,number_of_incomplete_tasks,
#brc: bug fix
    number_of_groups_of_previous_partition_that_cannot_be_executed):
    # The only partial information we need is Partition_DAG_is_complete
    # since BFS does access this in the incrmental DAG that is returned to it.
    # The other members aer small and may be useful for debugging.
    # version of DAG, incremented for each DAG generated
    global Group_DAG_version_number
    # Saving current_partition_name as previous_partition_name at the 
    # end. We cannot just subtract one, e.g. PR3_1 becomes PR2_1 since
    # the name of partition 2 might actually be PR2_1L, so we need to 
    # save the actual name "PR2_1L" and retrive it when we process PR3_1
    global Group_DAG_previous_partition_name
    global Group_DAG_number_of_tasks
    global Group_DAG_number_of_incomplete_tasks
    global Group_DAG_number_of_groups_of_previous_partition_that_cannot_be_executed
#brc: num_nodes
    global Group_DAG_num_nodes_in_graph
    global Group_DAG_is_complete

    # for debugging
    show_generated_DAG_info = True

    logger.trace("")
    DAG_info_dictionary = {}
    # These key/value pairs were added for incremental DAG generation.

    # If there is only one partition in the entire DAG then it is complete and is version 1.
    # Otherwise, version 1 is the DAG_info with partitions 1 and 2, where 1 is complete 
    # and 2 is complete if there are 2 partitons in the entire DAG, or incomplete otherwise.
    #    
    # # We do not increment the version numbr for incremental DAGs that are not 
    # published. In the trace, we print, e.g., "2P" for version "2 P"artial.
    #Group_DAG_version_number += 1
    
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
    Group_DAG_is_complete = not to_be_continued # to_be_continued is a parameter
    # number of tasks in the current incremental DAG, including the
    # incomplete last partition, if any. For computing pagerank, the task/function
    # is the same for all of the partitions. Thus, we really do not need to 
    # save the task/function in the DAG, once for each task in the ADG. That is 
    # what Dask does so we keep this for now.
    Group_DAG_number_of_tasks = len(Group_DAG_tasks)
    # For partitions, this is at most 1. When we are generating a DAG
    # of groups, there may be many groups in the incomplete last
    # partition and they will all be considered to be incomplete.
    Group_DAG_number_of_incomplete_tasks = number_of_incomplete_tasks
    Group_DAG_number_of_groups_of_previous_partition_that_cannot_be_executed = number_of_groups_of_previous_partition_that_cannot_be_executed
#brc: num_nodes
    # The value of num_nodes_in_graph is set by BFS_input_graph
    # at the beginning of execution, which is before we start
    # DAG generation.  This value does not change.
    Group_DAG_num_nodes_in_graph = num_nodes_in_graph

    DAG_info_dictionary["DAG_version_number"] = Group_DAG_version_number
    DAG_info_dictionary["DAG_is_complete"] = Group_DAG_is_complete
    DAG_info_dictionary["DAG_number_of_tasks"] = Group_DAG_number_of_tasks
    DAG_info_dictionary["DAG_number_of_incomplete_tasks"] = Group_DAG_number_of_incomplete_tasks
#brc: bug_fix:
    DAG_info_dictionary["DAG_number_of_groups_of_previous_partition_that_cannot_be_executed"] = Group_DAG_number_of_groups_of_previous_partition_that_cannot_be_executed
#brc: num_nodes:
    DAG_info_dictionary["DAG_num_nodes_in_graph"] = Group_DAG_num_nodes_in_graph

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
    file_name_incremental = "./DAG_info_incremental_Partition_" + str(Group_DAG_version_number) + ".pickle"
    with open(file_name_incremental, 'wb') as handle:
        cloudpickle.dump(DAG_info_dictionary, handle) #, protocol=pickle.HIGHEST_PROTOCOL)  

    # for debugging
    if show_generated_DAG_info:
        logger.info("generate_DAG_info_incremental_partitions: generate_partial_DAG_for_partitions: partial DAG:")
        logger.info("DAG_version_number:")
        logger.info(str(Group_DAG_version_number))
        logger.info("")
        logger.info("DAG_is_complete:")
        logger.info(str(Group_DAG_is_complete))
        logger.info("")
        logger.info("DAG_number_of_tasks:")
        logger.info(str(Group_DAG_number_of_tasks))
        logger.info("")
        logger.info("DAG_number_of_incomplete_tasks:")
        logger.info(str(Group_DAG_number_of_incomplete_tasks))
        logger.info("")
#brc: bug fix:
        logger.info("DAG_number_of_groups_of_previous_partition_that_cannot_be_executed:")
        logger.info(Group_DAG_number_of_groups_of_previous_partition_that_cannot_be_executed)
        logger.info("")
    #brc: num_nodes
        logger.info("DAG_num_nodes_in_graph:")
        logger.info(str(Group_DAG_num_nodes_in_graph))
        logger.info("")

    DAG_info = DAG_Info.DAG_info_fromdictionary(DAG_info_dictionary)
    return  DAG_info

# Called by generate_DAG_info_incremental_partitions below to generate 
# the DAG_info object when we are using partitions.
def generate_full_DAG_for_groups(to_be_continued,number_of_incomplete_tasks,
#brc: bug_fix:                    
    number_of_groups_of_previous_partition_that_cannot_be_executed):
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
    global Group_DAG_is_complete
    global Group_DAG_number_of_tasks
    global Group_DAG_number_of_incomplete_tasks
#brc: bug fix:
    global Group_DAG_number_of_groups_of_previous_partition_that_cannot_be_executed
#brc: num_nodes
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
#brc: bug fix:
    Group_DAG_number_of_groups_of_previous_partition_that_cannot_be_executed = number_of_groups_of_previous_partition_that_cannot_be_executed
#brc: num_nodes
    # The value of num_nodes_in_graph is set by BFS_input_graph
    # at the beginning of execution, which is before we start
    # DAG generation. This value does not change.
    Group_DAG_num_nodes_in_graph = num_nodes_in_graph

    DAG_info_dictionary["DAG_version_number"] = Group_DAG_version_number
    DAG_info_dictionary["DAG_is_complete"] = Group_DAG_is_complete
    DAG_info_dictionary["DAG_number_of_tasks"] = Group_DAG_number_of_tasks
    DAG_info_dictionary["DAG_number_of_incomplete_tasks"] = Group_DAG_number_of_incomplete_tasks
#brc: bug_fix:
    DAG_info_dictionary["DAG_number_of_groups_of_previous_partition_that_cannot_be_executed"] = Group_DAG_number_of_groups_of_previous_partition_that_cannot_be_executed
#brc: num_nodes:
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
        logger.trace("generate_DAG_for_groups: DAG_map:")
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
#brc: bug fix:
        logger.trace("DAG_number_of_groups_of_previous_partition_that_cannot_be_executed:")
        logger.trace(Group_DAG_number_of_groups_of_previous_partition_that_cannot_be_executed)
        logger.trace("")
#brc: num_nodes
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

        DAG_is_complete = DAG_info_Group_read.get_DAG_info_is_complete()
        DAG_version_number = DAG_info_Group_read.get_DAG_version_number()
        DAG_number_of_tasks = DAG_info_Group_read.get_DAG_number_of_tasks()
        DAG_number_of_incomplete_tasks = DAG_info_Group_read.get_DAG_number_of_incomplete_tasks()
#brc: bug fix:
        DAG_number_of_groups_of_previous_partition_that_cannot_be_executed = DAG_info_Group_read.get_DAG_number_of_groups_of_previous_partition_that_cannot_be_executed()
#brc: num_nodes
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
            logger.trace(DAG_is_complete)
            logger.trace("")
            logger.trace("DAG_number_of_tasks:")
            logger.trace(DAG_number_of_tasks)
            logger.trace("")
            logger.trace("DAG_number_of_incomplete_tasks:")
            logger.trace(DAG_number_of_incomplete_tasks)
            logger.trace("")
#brc: bug fix:
            logger.trace("DAG_number_of_groups_of_previous_partition_that_cannot_be_executed:")
            logger.trace(DAG_number_of_groups_of_previous_partition_that_cannot_be_executed)
            logger.trace("")
#brc: num_nodes
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
        return cls(DAG_info_dictionary)
"""

# called by bfs()
#def generate_DAG_info_incremental_groups(current_partition_name,
#    current_partition_number, groups_of_current_partition,
#    groups_of_partitions,
#    to_be_continued):

#brc: use of DAG_info: 
def generate_DAG_info_incremental_groups(current_partition_name,
    current_partition_number, groups_of_current_partition,
    groups_of_partitions,
    to_be_continued,
    num_incremental_DAGs_generated_since_base_DAG):

# to_be_continued is True if BFS (caller) finds num_nodes_in_partitions < num_nodes in graph, 
# which means that incremeental DAG generation is not complete 
# (some graph nodes are not in any partition.)
# groups_of_current_partition is a list of groups in the current partition.
# groups_of_partitions is a list of the groups_of_current_partition. We need
# this to get the groups of the previous partition and th previous previous partition. 
# The base DAG is the DAG with complete partition 1 and incomplete partition 2. This is 
# the first DAG to be executed assuming the DAG has more than one partition.
# num_incremental_DAGs_generated_since_base_DAG is the number of DAGs generated since
# the base DAG; we publish every ith incremental ADG generated, where i can be set. 

# Note: We use num_nodes_in_graph when we are deallocating on-the-fly - we only deallocate
# when the number of nodes in the input graph is large.

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

    #brc: num_nodes
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
    # this is assserted by the caller (BFS()) of this method.

    """
    Outline: 
    Each call to generate_DAG_info_incremental_groups adds the groups in one partition 
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
    these groups we did not know they were in a partition that was the last partition in 
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

    2. (senders is None): This is a leaf group, which could be group 2 or any 
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

        try:
            msg = "[Error]: generate_DAG_info_incremental_groups" \
                + " number of groups in first partition is not 1 it is " \
                + str(len(groups_of_current_partition))
            assert len(groups_of_current_partition) == 1 , msg
        except AssertionError:
            logger.exception("[Error]: assertion failed")
            if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
                logging.shutdown()
                os._exit(0)
        #assertOld:
        #if not len(groups_of_current_partition) == 1:
        #    logger.error("[Error]: generate_DAG_info_incremental_groups"
        #        + " number of groups in first partition is not 1 it is "
        #        + str(len(groups_of_current_partition)))

        try:
            msg = "[Error]: generate_DAG_info_incremental_groups" \
                + " current_partition_state for first partition is not equal to" \
                + " Group_next_state - both should be 1."
            assert current_partition_state == Group_next_state , msg
        except AssertionError:
            logger.exception("[Error]: assertion failed")
            if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
                logging.shutdown()
                os._exit(0)
        #assertOld:
        # Note: Group_next_state is inited to 1. as is current_partition_state
        #if not current_partition_state == Group_next_state:
        #    logger.error("[Error]: generate_DAG_info_incremental_groups"
        #        + " current_partition_state for first partition is not equal to"
        #        + " Group_next_state - both should be 1.")
          
        name_of_first_group_in_DAG = groups_of_current_partition[0]
        # a list of groups that have a fanout/fanin/collapse to this
        # group. (They "send" to this group which "receives".)
        senders = Group_receivers.get(name_of_first_group_in_DAG)
        # Note: the groups in a partition that starts a new connected component (which is 
        # the first partition collected on a call to BFS(), of which there 
        # may be many calls if the graph is not connected) are leaf
        # nodes and thus have no senders. This is true about partition 1 and
        # this is assserted by the caller (BFS()) of this method.

#brc: inc group deallocate
        # Note: senders is asserted to be None since there should be no 
        # name_of_first_group_in_DAG key in the Group_receivers dictionary.
        # Thus there is no need to deallocate this key.
        
        # Group 1 is a leaf; so there is no previous partition that can 
        # send (its outputs as inputs) to the first group

        try:
            msg = "[Error]: generate_DAG_info_incremental_groups" \
                + " leaf node has non-None senders."
            assert senders is None , msg
        except AssertionError:
            logger.exception("[Error]: assertion failed")
            if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
                logging.shutdown()
                os._exit(0)
        # assertOld:
        #if senders is not None:
        #    logger.error("[Error]: generate_DAG_info_incremental_groups"
        #        + " leaf node has non-None senders.")

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
#brc: clustering
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
#brc: clustering
            fanout_partition_group_sizes)

        Group_DAG_states[name_of_first_group_in_DAG] = Group_next_state

        # identify the function that will be used to execute this task
        if not DAG_executor_constants.USE_SHARED_PARTITIONS_GROUPS:
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
            if not DAG_executor_constants.USE_STRUCT_OF_ARRAYS_FOR_PAGERANK:
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
            # as assserted above.
            number_of_incomplete_tasks = len(groups_of_current_partition)
        else:
            number_of_incomplete_tasks = 0
        # first DAG generated has one current group (group 1) and no previous group
        number_of_groups_of_previous_partition_that_cannot_be_executed = 0

#brc: use of DAG_info:
       
        #DAG_info = generate_full_DAG_for_groups(to_be_continued,number_of_incomplete_tasks,
#brc: bug fix:
        #    number_of_groups_of_previous_partition_that_cannot_be_executed)
        
        # Save the DAG_info if the DAG is complete, i.e., the DAG has 
        # only one partition. In that case, we need a full DAG_info; otherwise, we can generate a
        # partial DAG info (since the DAG will not be executed - the first DAG executed is the base DAG
        # (with complete partition 1 and incomplete partition 2))
        if not to_be_continued:
            DAG_info = generate_full_DAG_for_groups(to_be_continued,number_of_incomplete_tasks,    #brc: bug fix:
                number_of_groups_of_previous_partition_that_cannot_be_executed)
        else:
            DAG_info = generate_partial_DAG_for_groups(to_be_continued,number_of_incomplete_tasks,
                #brc: bug fix:
                number_of_groups_of_previous_partition_that_cannot_be_executed)

        logger.trace("generate_DAG_info_incremental_groups: returning from generate_DAG_info_incremental_groups for"
            + " group " + str(name_of_first_group_in_DAG))
        
        # This will be set to 2
        Group_next_state += 1

#brc: use of DAG_info:
        # We only execute the first DAG, i.e., the DAG that only has
        # partition 1 if this DAG is complete. In that case, bfs will
        # not need to modify the incremental DAG since there are no more 
        # partitions. So here we do not do the copies like we do at the end
        # of the other branches. (The DAG_executor gets a copy so that the 
        # DAG_executor is not reading a field of DAG_info that bfs can 
        # write/modify, which creates a race condition - wull DAG_excutor
        # read the DAG info of current DAG before bfs modifies the info as 
        # part of generating the next DAG.)
        
        return DAG_info

    else: # current_partition_number >= 2
        # Flag to indicate whether we are processing the first group_name of the groups in the 
        # previous partition. Used and reset below.
        #first_previous_group = True

#brc: undo 1
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
# - commented out the asssert #if len(senders) == 0:
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

#brc: inc group dealloctate
            # Note: senders is asserted to be None since there should be no 
            # name_of_first_group_in_DAG key in the Group_receivers dictionary.
            # Thus there is no need to deallocate this key.

            if (senders is None):
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
                try:
                    msg = "[Error]: generate_DAG_info_incremental_groups:" \
                        + " start of new connected component (i.e., called BFS()) but there is more than one group."
                    assert not (len(groups_of_current_partition) > 1), msg
                except AssertionError:
                    logger.exception("[Error]: assertion failed")
                    if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
                        logging.shutdown()
                        os._exit(0)
                # assertOld:
                #if len(groups_of_current_partition) > 1:
                #    logger.error("[Error]: generate_DAG_info_incremental_groups:"
                #        + " start of new connected component (i.e., called BFS()) but there is more than one group.")

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
#brc: clustering
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
#brc: clustering
                    fanout_partition_group_sizes)
                
                Group_DAG_states[group_name] = Group_next_state

                # identify the function that will be used to execute this task
                if not DAG_executor_constants.USE_SHARED_PARTITIONS_GROUPS:
                    Group_DAG_tasks[group_name] = PageRank_Function_Driver
                else:
                    if not DAG_executor_constants.USE_STRUCT_OF_ARRAYS_FOR_PAGERANK:
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
#brc: clustering
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
#brc: clustering
                    fanout_partition_group_sizes)
                
                Group_DAG_states[group_name] = Group_next_state

                # identify function used to execute this pagerank task (see comments above)
                if not DAG_executor_constants.USE_SHARED_PARTITIONS_GROUPS:
                    Group_DAG_tasks[group_name] = PageRank_Function_Driver
                else:
                    if not DAG_executor_constants.USE_STRUCT_OF_ARRAYS_FOR_PAGERANK:
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

        # If not to_be_Continued then there is no next partition 
        # of groups to process, e.g., for the white board we have partition 3
        # with groups 3_1, 3_2, and 3_3, where 3_1 has a faninB to 3_2.
        # We won't get a chance to compute this faninNB unless we do it 
        # now, i.e., we can't wait until the next partition since there is no next
        # partition. So if not to be continued, we need to look at  the
        # groups within the current partition too.
        # When current partition is 3, we look at the previous groups, whch are in partition
        # 2, which is fine for seeing the PR2_2L has a faninNB to PR3_2,
        # but if we only looked at the prvious groups, we would fail to detect that PR3_1 
        # has a faninNB to PR3_2. So we should also look at the groups in the current partition.
        
#brc: undo 2
        # Note: if the current group/partition is a leaf group/partition (that is not the first
        # group in the DAG PR1_1) then it has no senders, i.e., no task sends its output
        # to it; it is the only group in its (current) partititon; and if not to_be_continued then
        # it has no receivers, i.e., it does not send its output to any other group, since it is 
        # the only group in the last partition in the DAG. 
        # So groups_of_current_partition is this single group, and when not to_be_continued
        # we add it to groups_to_consider. We will get groups that this group sends inputs to
        # and since this will be empty, we will not generate any edges for this group in the 
        # DAG, which is correct since it does not send any outputs to any other group.
        # This requires only the execution of one if-statement so we do not try to avoid adding this group.
        # Note that groups_to_consider are the groups for which we are identifying edges,
        # i.e., adding an edge for a group G in groups_to_consider to the groups that G
        # sends outputs to (which are in the same (previous or current) partition as G or are groups in the 
        # current partition (where G is in the previous partition). The edges always go from the previous groups to the 
        # current groups, or the edges go from left to right for the groups in the same partition since
        # if node P is a parent of node C in the same partition, then P has an index in the partition (list) 
        # that is less than the index of C, and the edges go in the parent to child direction, i.e., left to right.
        # The general idea is that the parent nodes of a node C are always either in the same partition
        # (at a position with a smaller index than C (so "to the left of C in the list of nodes")) or are in the
        # prevous partition (but no other partition). This restricts the flow of parent values 
        # to: between one partition and the next partition, or between groups in the same partition.
        # As opposed to one group can send to any other groups in the DAG, so we try to have communication of
        # parent values (between Lambdas executing different pagerank tasks) that is "one group/task/lambda
        # to a few groups/tasks/lambdas" instead of "one group/task/lambda to many groups/tasks/lambdas".
        # If a graph with n nodes has one node N with an edge to each of the other n-1 nodes then N will 
        # send its paent pagerank value to all the other nodes. We would generate one group with node N in 
        # partition 1 and a partition 2 having n-1 groops each with 1 node. For this case we would might want 
        # to use task clustering to cluster all of the groups together, either at runtime or compile time.
        groups_to_consider = []
        if not to_be_continued:
            # determine the edges beween the groups in the previous and current partitions. Edges can be
            # between grooups in the same partition (previous to previous or current to current) or in
            # different partitions (previous to current).
            groups_to_consider += groups_of_previous_partition
            groups_to_consider += groups_of_current_partition
        else:
            # determine the edges beween the groups in the previous and current partitions. Edges can be
            # between groups in the same partition (previous to previous) or in
            # different partitions (previous to current). We are not looking for current to current edges.
            # These will be determined on the DAG extension when the current partition becomes the 
            # previous partition.
   
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
#brc: undo 3
        
        # Note: Possible Optimzation:
        # If we will not publish this extension are we wasting
        # time generating the new DAG_info? Note that bfs() gets the 
        # returned DAG_info and decides what to do with it - we can move
        # all this bfs() decision code out of bfs() and into this dag 
        # generator code. So bfs() can call a new method generate_incremental_groups()
        # and generate_incremental_groups calls this method 
        # generate_DAG_info_incremental_groups, which returns
        # DAG_info to generate_incremental_groups, which does what 
        # bfs() currently does, i.e., decides what to do with the new
        # DAG_info. (We would need to make sure DAG_info is not generated
        # if we are not going to use it - either generate_incremental_groups
        # tells this method generate_DAG_info_incremental_groups whether to 
        # generate DAG_info or generate_DAG_info_incremental_groups
        # doesn't ever generate DAG_info and instead generate_incremental_groups 
        # decides whether to generate the new DAG_info. Note generate_DAG_info_incremental_groups
        # does stuff with DAG_info after generating it so considr this code too.)
        # Note: If we are not publshing this DAG then we do not need to 
        # generate previous_groups_with_TBC_faninsfanoutscollapses and all the rest
        # since we only need it for the previous_groups of a published DAG. 
        # The groups preceding previous_groups in a published DAG are all
        # complete and have no fanins/fanouts/collapes to TBC groups.
        
        # this list is intialized to [] on every call to 
        # generate_DAG_info_incremental_groups
        previous_groups_with_TBC_faninsfanoutscollapses = []

        for previous_group in groups_to_consider:
        #for previous_group in groups_of_previous_partition:
            # sink nodes, i.e., nodes that do not send any outputs to other nodes


# START
            # As commented above, we do not need this
            Group_sink_set = set()

            fanouts = []
#brc: clustering
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

            # get groups that previous_group sends inputs to. These
            # groups "receive" inputs from the sender (which is previous_group)

            #receiver_set_for_previous_group = Group_senders[previous_group]
            receiver_set_for_previous_group = Group_senders.get(previous_group,[])
 
            # (Note: Not sure whether we can have a length 0 Group_senders list, 
            # for current_partition_name. That is, we only create a 
            # Group_senders set when we get the first sender.)
            # assert: no length 0 Group_senders lists
            # 0 list for PR3_2, which is in current group; we have not processed partition 3 so there are
            # no senders for it yet?
            
            #try:
            #    msg = "[Error]: generate_DAG_info_incremental_groups:" \
            #    + " group " + previous_group + " has a receiver_set_for_previous_group list with length 0."
            #    assert not (len(receiver_set_for_previous_group) == 0) , msg
            #except AssertionError:
            #    logger.exception("[Error]: assertion failed")
            #    if EXIT_PROGRAM_ON_EXCEPTION:
            #        logging.shutdown()
            #        os._exit(0)
            # assertOld:
            #if len(receiver_set_for_previous_group) == 0:
            #    logger.error("[Error]: generate_DAG_info_incremental_groups:"
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

#brc: bug fix:
                # if not to_be_continued, groups_to_consider contains
                # groups from groups_of_previous_partition and groups from
                # groups_of_current_partition. That is, previous_group
                # can be from groups_of_previous_partition or 
                # groups_of_current_partition. Group receiverY can be 
                # a group in groups_of_previous_partition (i.e., it is a 
                # group in the same partition as previous_group) or a 
                # group in groups_of_current_partition. We need to know 
                # whether previous_group and receiverY are both in
                # groups_of_previous_partition.
                if previous_group in groups_of_previous_partition  \
                and receiverY in groups_of_previous_partition:
                    # previous_group sends output to receiverY
                    # and both are in groups_of_previous_partition
                    if to_be_continued:
                        # receiverY is not incomplete but it cannot be executed. Track these complete
                        # but unexecutable tasks so we can compute the number of these tasks at the end.
                        logger.info("generate_DAG_for_groups: add " + str(receiverY) + " to previous_groups_with_TBC_faninsfanoutscollapses"
                            + " previous_group: " + str(previous_group)
                            + " receiverY: " + str(receiverY))
                        previous_groups_with_TBC_faninsfanoutscollapses.append(receiverY)
                    else:
                        logger.info("generate_DAG_for_groups: do not add " + str(receiverY) + " to previous_groups_with_TBC_faninsfanoutscollapses since to_be_continued is False.")
                
                # Here we check whether rceiverY is a sink, i.e., it does not
                # send inputs to any other group.
                receiver_set_for_receiverY = Group_senders.get(receiverY)
                if receiver_set_for_receiverY is None:  # None is default
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
                        try:
                            msg = "[Error]: generate_DAG_info_incremental_groups:" \
                                + "group " + receiverY + " is in the collapse set of two groups."
                            assert not receiverY in Group_all_collapse_task_names , msg
                        except AssertionError:
                            logger.exception("[Error]: assertion failed")
                            if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
                                logging.shutdown()
                                os._exit(0)
                        #assertOld:
                        #if not receiverY in Group_all_collapse_task_names:
                        Group_all_collapse_task_names.append(receiverY)
                        #else:
                        #    # assertOld
                        #    logger.error("[Error]: generate_DAG_info_incremental_groups:"
                        #        + "group " + receiverY + " is in the collapse set of two groups.")
                            
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
#brc: clustering
                        if DAG_executor_constants.ENABLE_RUNTIME_TASK_CLUSTERING:
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
            logger.info("previous_group: " + str(previous_group)
                + " previous_group_state: " + str(previous_group_state))
            state_info_of_previous_group = Group_DAG_map[previous_group_state]

            logger.trace("before update to TBC and fanout_fanin_faninNB_collapse_groups_are_ToBeContinued_are_ToBeContinued"
                + " for previous_group " + previous_group + " state_info_of_previous_group: " + str(state_info_of_previous_group))
            try:
                msg = "[Error]: generate_DAG_info_incremental_groups: state_info_of_previous_group: " \
                    + "state_info_of_previous_group is None."
                assert not (state_info_of_previous_group is None) , msg
            except AssertionError:
                logger.exception("[Error]: assertion failed")
                logger.error("DAG_map:")
                for key, value in Group_DAG_map.items():
                    logger.trace(str(key) + ' : ' + str(value))
                if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
                    logging.shutdown()
                    os._exit(0)            
            #assertOld:
            #if state_info_of_previous_group is None:
            #    logger.error("[Error]: generate_DAG_info_incremental_groups: state_info_of_previous_group: "
            #        + "state_info_of_previous_group is None.")
            #    logger.error("DAG_map:")
            #    for key, value in Group_DAG_map.items():
            #       logger.trace(str(key) + ' : ' + str(value))
            #    logging.shutdown()
            #    os._exit(0)

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
#brc: bug fix: Note: the previous group may not have any fanins/fanouts/collapses
# to the current partition. In that case using to_be_continued is weak, i.e., it will work 
# but it may prevent an executable task from being executed. Also, note that 
# above we are adding receiverY to the list of previous_groups_with_TBC_faninsfanoutscollapses
# when it might not have TBC_faninsfanoutscollapses. We would have to remove it when we fnd
# it actually has no TBC_faninsfanoutscollapses to groups in the current partition, which 
# seems like we would detect when receiverY is previous_group, i.e., track whether previous
# group sends its outputs to a group that is not in groups_of_previous_partition (or is in
# groups_of_current_partition. Consider: iterate through previous
# groups in reverse order (right to left).
            # if the current partition is to_be_continued then previous_group has TBC fanins/fanouts/colapses to
            # (current groups) so we set fanout_fanin_faninNB_collapse_groups_partitions_are_ToBeContinued of the previous
            # groups to True; otherwise, we set fanout_fanin_faninNB_collapse_groups_partitions_are_ToBeContinued to False.
            # Note: state_info_of_previous_group.ToBeContinued = False inicates that the
            # previous groups are not to be continued, while
            # state_info_of_previous_group.fanout_fanin_faninNB_collapse_groups_partitions_are_ToBeContinued indicates
            # whether the previous groups have fanout/fanin/faninNB/collapse groups/partitions
            # that are to be continued, i.e., the fanout/fanin/faninNB/collapse are 
            # to groups in this current partition and whether these groups in the current
            # partition are to be continued is indicated by parameter to_be_continued.
            # (When bfs() calls this method it may determine that some of the graph
            # nodes have not yet been assigned to any partition so the DAG is
            # still incomplete and thus to_be_continued = True, whicch means that one or more
            # partitions need to be added to the DAG.
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
            # of A to False since B is complete so all of A's fanins/fanouts/faninNBs/collpases
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

        # Note:This is an old comment that explains the now commnted out 
        # if statement if first_previous_previous_group:
        # We took this code out of the above loop through the groups of
        # groups_of_previous_previous_partition so we do not need to 
        # limit this loop to one iteration (which would be for the fist group).
        ## For the previous_group (e.g., B), we need to reset a flag 
        ## for its previous groups, hence "previous_previous"
        ## but we only need to do this once. That is, the current
        ## group group_name (e.g., A) may have many previous_groups, and these
        ## previous groups may have many previous_groups. However two
        ## previous_groups of A, say, B1 and B2 have the same previous_groups, 
        ## which are the previous previous groups of A, so when we reset
        ## the previous groups of B1 we are also resetting the previous
        ## groups of B2. So do this resetting of the previous groups 
        ## of B1 and B2 (which are the previous previous groups of A) for
        ## only one of B1 or B2. In our case, we always choose the first group
        ## in the list of groups.
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
                
                # deallocate the Group_senders and Group_receivers for previous_previous_group
                if DAG_executor_constants.DEALLOCATE_BFS_SENDERS_AND_RECEIVERS and (num_nodes_in_graph > DAG_executor_constants.THRESHOLD_FOR_DEALLOCATING_ON_THE_FLY):
                    try:
                        del Group_senders[previous_previous_group]
                        logger.info("BFS_generate_DAG_info_incremental_groups: deallocate Group_senders: " + previous_previous_group)
                    except KeyError:
                        logger.info("BFS_generate_DAG_info_incremental_groups: deallocate: Group_senders has no key: " + previous_previous_group) 
                    try:
                        del Group_receivers[previous_previous_group]
                        logger.info("BFS_generate_DAG_info_incremental_groups: deallocate: Group_receivers: " + previous_previous_group)
                    except KeyError:
                        logger.info("BFS_generate_DAG_info_incremental_groups: deallocate: Group_receivers has no key: " + previous_previous_group)    
                
        """
        Note: We handle the shared state_info objects for all the groups
        at the end of the group loop below.
        """

        logger.trace("generate_DAG_info_incremental_groups: generate_DAG_info_incremental_groups for"
            + " group " + str(group_name))

#brc: bug_fix:
        # Generate the new DAG_info
        number_of_groups_of_previous_partition_that_cannot_be_executed = 0
        if to_be_continued:
            number_of_incomplete_tasks = len(groups_of_current_partition)
            duplicates_removed = list(set(previous_groups_with_TBC_faninsfanoutscollapses))
            number_of_groups_of_previous_partition_that_cannot_be_executed = len(duplicates_removed)
        else:
            number_of_incomplete_tasks = 0        
            number_of_groups_of_previous_partition_that_cannot_be_executed = 0       
        logger.info("generate_DAG_for_groups: number_of_groups_of_previous_partition_that_cannot_be_executed:"
            + str(number_of_groups_of_previous_partition_that_cannot_be_executed))
    
#brc: use of DAG_info: 
        # This is for current_partition_number >=2 for both the
        # senders == None case and the senders != None case.  Below we added a condition
        # for choosing full/partial DAG generation. Note that in the partition version 
        # we have this conditon in the senders == None branch and this condition is also in
        # the other bracnch. Here we have the condition after both of these branches (i.e,.
        # after the if-statement)

        #DAG_info = generate_full_DAG_for_groups(to_be_continued,number_of_incomplete_tasks,
        #    number_of_groups_of_previous_partition_that_cannot_be_executed)
        #if current_partition_number == 3:
        #    logging.shutdown()
        #    os._exit(0)


        # We publish the DAG if the graph is complete (not to be continued).
        # This is true whether the current partition is 1 or 2, or a later
        # partition. If the current partition is 1 and the graph is complete,
        # then we will save the DAG and start the DAG_executor_driver, which 
        # will read and execute the DAG. If the current partition is 2, we save 
        # the DAG whether it is complete or not and start the DAG_excutor_driver,
        # which will read and execute the DAG. (If partition 1 is not compplete
        # we do not save the DAG; DAG execution will start with a DAG that
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
            #The current_partition_number must be 2 - if it were 1 we would have 
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
            # executing it, so generate a full DAG.
            DAG_info = generate_full_DAG_for_groups(to_be_continued,number_of_incomplete_tasks,
                number_of_groups_of_previous_partition_that_cannot_be_executed)
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
                DAG_info = generate_full_DAG_for_groups(to_be_continued,number_of_incomplete_tasks,
                    number_of_groups_of_previous_partition_that_cannot_be_executed)                           
            else:
                DAG_info = generate_partial_DAG_for_groups(to_be_continued,number_of_incomplete_tasks,
                    number_of_groups_of_previous_partition_that_cannot_be_executed)


        # We are adding state_info objects for the groups of the current
        # partition to the DAG as incmplete (to_be_continued). They will 
        # be accessed (read) by the DAG_executor and we cannot modify them 
        # during execution. However, when we process
        # the next partition, these incomplete groups that we are adding
        # to the DAG now, will need to be modified as we will generate their
        # fanin/fanout/faninNB/collase sets. So here we do not 

        # modify these previous state_info objects; instead, we create
        # a deep copy of these state_info objects so that the DAG_executor
        # and the DAG we are incrementally generating access different 
        # (deep) copies. This means when we modify the copy in the ongoing
        # incremental DAG, we are not modifying the copy given to the 
        # DAG_executor.  Thus, the DAG_executor and
        # the DAG_generator do not share state_info objects so there
        # is no need to synchronize their access to state_info objects.
        # The other objects in DAG_info that are accessed by
        # DAG_executor and DAG_generator are immutable, so that when
        # the DAG_generator writes one of these objects it is generating
        # a new reference that is different from the reference in the ADG_info
        # that the DAG_executor references, e.g., for all Booleans. That means
        # these other ojects, which are only read by DAG_executor and are 
        # written be DAG_generator, are not really being shared. Funny.

#brc: use of DAG_info:
        # We only need to make these copies if we will be publishing/executing 
        # this DAG. Note: not DAG is to_be_continued ==> DAG is complete
        if current_partition_number == 2 or (
                not to_be_continued or (
                (num_incremental_DAGs_generated_since_base_DAG+1) % DAG_executor_constants.INCREMENTAL_DAG_DEPOSIT_INTERVAL == 0
                )):
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
                    if not USE_INCREMENTAL_DAG_GENERATION:
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

    """
    Deallocate DAG_info for workers:
    - workers request DAG version number i: Note: all workers request 
      the same next version. This is not true for Lambdas.
        - first DAG that workers get is DAG with complete 1 and incomplete 2.
          This is version 1, where 2 is the current partition number
          when the workers' first DAG was generated.
        - Note: The last partition L in an incremental DAG is incomplete. 
          The partition B before that is complete but it has fanouts/fanins 
          to the incomplete last partition. The partition BB before B is complete 
          and it does not have any fanins/fanouts to an 
          incomplete partition since all its fanin/fanouts are to 
          partition B and partition B is complete.
        - first actual requested version is 2, if the interval between publishing is 2, 
          then the next DAG published is 4 then 6 then 8 ..., etc.
        - Note: We may be generating version 10 of the DAG. However, if workers
          requested version, say, 4, then they have not executed the partitions
          that were added to the DAG to create versions 4, 5, 6, 7, 8, 9 or 10.
          That is, we have generated DAG with these partitions in them but we
          have not executed thes partitions yet, which means we cannot deallocat
          these unexcuted partitions. So deallocation is basd in what partitions
          have been executed (based on which version the workers are requsting)
          and not on what partitions have been generated, i.e., added to the DAG

    - if workers request version 2, and they get 6, they will then next request 7. 
    - When we see max requested version is 1, and we are building 6/7/8/etc
      we cannot dellocate anything since a request of 3 means they need
      1, 2, and 3. 
    - If workers request 3 and get 3, they will next request 4, and if we 
      see max request is 4 while we are, say building 8, we can delete 1. 
      Since workers need 2, 3, and 4 but not 1.
    - We should keep track of max_deallocate (inited to 0). Suppose
      they request 3 and get 4. We can't deallocate any. If they then request
      5 and get get 12, we can deallocate 1 and 2 (but not 3, 4, 5). max_deallocate
      is now 2. They will next request 13. If we see max request is 13 
      while we build 20, we can deallocate 1-10, but max_deallocate is 2  
      so we can start deallocating with max_deallocate+1 = 3, so 3-10, and set 
      max_delete to 10. In general we deallocate the range max_deallocate+1
      through max_request-3.
      - Note if we do this then we may also see max_request is 13 while we 
      build 21, 22, etc. Since we have already deallocated 3-10, we do not 
      want to deallocate them again. If we set max_deallocate to 10, we would 
      try to start deleting with max_deallocate+1 = 11 and the range of deallocations 
      would be 11 to 13-3=10 so we would not try to deallocate 2-10 again.
    """
    """
    Note: for lambdas, they have their own DAG so we would need to
    do the deallocations per lambda - we receive their request but we 
    would need to save they max_deallocation in the DAG_info and
    they could send that along with their request.
    Note: Lambdas do not wait for a new DAG, they terminate. When a 
    deposit occurs, we iterate through the lambdas that have requested
    a new version and restart them. We will need to prune their 
    DAG of old states before we restart them. if we have their
    max_deallocation then this is a range from max_deallocation+1
    to max_request-3. 
    Note: One idea is to sort the waiting lambdas based on 
    their requested version. If we process the lambdas in ascending
    order of requests, then we can make one pass through the DAG -
    prune for the first lambda and restart the lambda, for the 
    second lambda, keep the pruning that we did for the first lambda
    and possiby prune more for the second lambda, etc. 
    """
    
    # Note: To stop after DAG is completely generated, use the
    # sleep at the start of the DAG_executor_driver_Invoker_Thread 
    # so that DAG excution does not start before we get here and exit,
    #def DAG_executor_driver_Invoker_Thread():
    #  time.sleep(3)
    #  run()
    #
    # Here, shutdowwn when incremental DAG is complete
    #if DAG_info.get_DAG_info_is_complete():
    #    logging.shutdown()
    #    os._exit(0)

    return DAG_info

def deallocate_DAG_structures(current_partition_number,current_version_number_DAG_info,
        num_incremental_DAGs_generated_since_base_DAG):
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
    global deallocation_start_index_partitions
    deallocation_end_index_partitions = (2+((current_version_number_DAG_info-2)*DAG_executor_constants.INCREMENTAL_DAG_DEPOSIT_INTERVAL))-2

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

    logger.info("deallocate_DAG_structures: deallocation_start_index_partitions: " + str(deallocation_start_index_partitions)
        + " deallocation_end_index_partitions: " + str(deallocation_end_index_partitions))
    for i in range(deallocation_start_index_partitions, deallocation_end_index_partitions):
        logger.info("deallocate_DAG_structures: deallocate " + str(i))
#brc: ToDo: get the size of the groups of partition i and deallocate those n groups
        groups_of_partition_i = BFS.groups_of_partitions[i-1]
        # number of groups >= 1
        number_of_groups_of_partition_i = len(groups_of_partition_i)
        # end index is now 1 past the last group to be deallocated, e.g., 
        # if start is 1 and number of groups is 3 we want to deallocate 
        # 1, 2, and 3. The value 4 is fine for end since we will use a 
        # range(1,4) and 1 is inclusive but 4 is exclusive.
        # After adding number_of_groups_of_partition_i to deallocation_end_index_groups, 
        # deallocation_start_index_groups < deallocation_end_index_groups.
        global deallocation_start_index_groups
        deallocation_end_index_groups = deallocation_start_index_groups+number_of_groups_of_partition_i

        try:
            msg = "[Error]: deallocate_DAG_structures:" \
                + " deallocation_start_index_groups is not less than deallocation_end_index_groups after add." \
                + " deallocation_start_index_groups: " + str(deallocation_start_index_groups) \
                + " deallocation_end_index_groups: "  + str(deallocation_end_index_groups)
            assert deallocation_start_index_groups < deallocation_end_index_groups , msg
        except AssertionError:
            logger.exception("[Error]: assertion failed")
            if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
                logging.shutdown()
                os._exit(0)

        logger.info("deallocate_DAG_structures: "
            + " number_of_groups_of_partition_i " + str(number_of_groups_of_partition_i)
            + " deallocation_start_index_groups: " + str(deallocation_start_index_groups)
            + " deallocation_end_index_groups: "  + str(deallocation_end_index_groups))

        for j in range(deallocation_start_index_groups, deallocation_end_index_groups):
            # Note: This deallocation uses group_name = BFS.partition_names[j-1]
            # so deallocate_Group_DAG_structures deallocates group in position j-1
            deallocate_Group_DAG_structures(j)
        # reset start to end to prepare for the the next deallocations
        deallocation_start_index_groups = deallocation_end_index_groups
    
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
    if deallocation_start_index_partitions < deallocation_end_index_partitions:
        deallocation_start_index_partitions = deallocation_end_index_partitions

    # Example. Suppose the publishing interval is 4, i.e., after publishing the
    # base DAG (version 1) with partitions 1 and 2, we publish version 2 with 
    # 1 2 3 4 5 6 (base DAG plus 4 partitions). Before bfs() calls depost()
    # to publish this DAG it gets the last requsted DAG version which is initializd
    # to 1 and passes 1 to deallocate_DAG_structures.
    # For deallocation_end_index_partitions = (2+((current_version_number_DAG_info-2)
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
    # For deallocation_end_index_partitions = (2+((current_version_number_DAG_info-2)
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
    # For deallocation_end_index_partitions = (2+((current_version_number_DAG_info-2)
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

