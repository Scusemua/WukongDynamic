# Manually execute whietboard DAG:
"""
logger.trace("Input partitions/groups")
input_partitions()

task_name = "PR1_1"
payload = {}
payload['input'] = []
total_num_nodes = 20
results = []
for _ in range(total_num_nodes+1):
    results.append(-1)
PageRank_output_from_PR_1_1 = PageRank_Task(task_name,total_num_nodes,payload,results)
PR2_1_input_from_PR_1_1 = PageRank_output_from_PR_1_1["PR2_1"]
PR2_2_input_from_PR_1_1 = PageRank_output_from_PR_1_1["PR2_2"]
PR2_3_input_from_PR_1_1 = PageRank_output_from_PR_1_1["PR2_3"]
task_name = "PR2_1"
payload = {}
payload['input'] = PR2_1_input_from_PR_1_1
PageRank_output_from_PR_2_1 = PageRank_Task(task_name,total_num_nodes,payload,results)
PR2_2_input_from_PR_2_1 = PageRank_output_from_PR_2_1["PR2_2"]
task_name = "PR2_2L"
payload = {}
PR2_2_input = PR2_2_input_from_PR_1_1 + PR2_2_input_from_PR_2_1
payload['input'] = PR2_2_input
PageRank_output_from_PR_2_2 = PageRank_Task(task_name,total_num_nodes,payload,results)
PR3_1_input_from_PR_2_2 = PageRank_output_from_PR_2_2["PR3_1"]
PR3_2_input_from_PR_2_2 = PageRank_output_from_PR_2_2["PR3_2"]
task_name = "PR2_3"
payload = {}
PR2_3_input = PR2_3_input_from_PR_1_1
payload['input'] = PR2_3_input
PageRank_output_from_PR_2_3 = PageRank_Task(task_name,total_num_nodes,payload,results)
PR3_3_input_from_PR_2_3 = PageRank_output_from_PR_2_3["PR3_3"]
task_name = "PR3_1"
payload = {}
PR3_1_input = PR3_1_input_from_PR_2_2
payload['input'] = PR3_1_input
PageRank_output_from_PR_3_1 = PageRank_Task(task_name,total_num_nodes,payload,results)
PR3_2_input_from_PR_3_1 = PageRank_output_from_PR_3_1["PR3_2"]
task_name = "PR3_2"
payload = {}
PR3_2_input = PR3_2_input_from_PR_2_2 + PR3_2_input_from_PR_3_1
payload['input'] = PR3_2_input
PageRank_output_from_PR_3_2 = PageRank_Task(task_name,total_num_nodes,payload,results)
task_name = "PR3_3"
payload = {}
PR3_3_input = PR3_3_input_from_PR_2_3
payload['input'] = PR3_3_input
PageRank_output_from_PR_3_3 = PageRank_Task(task_name,total_num_nodes,payload,results)
logger.trace("Results:")
for i in range(len(results)):
    logger.trace ("ID:"+str(i) + " pagerank:" + str(results[i]))
"""


# This is the graph parameter in the old calls to BFS and dfs_parent.
# It was used in the starter BFS.
graph = {
  '5' : ['3','7'],
  '3' : ['2', '4'],
  '7' : ['8'],
  '2' : [],
  '4' : ['8'],
  '8' : []
}

# moved down from its top position right after class Partition_Node
"""
N5 = Node(5)
N3 = Node(3)
N7 = Node(7)
N2 = Node(2)
N4 = Node(4)
N8 = Node(8)
N9 = Node(9)

#N5.ID = 5
N5.children = [N3,N7]
N5.parents = []

#N3.ID = 3
N3.children = [N2,N4]
N3.parents = [N5,N9]

#N7.ID = 7
N7.children = [N8]
N7.parents = [N5]

#N2.ID = 2
N2.children = [N9]
N2.parents = [N3]

#N4.ID = 4
N4.children = [N8]
N4.parents = [N3]

#N8.ID = 8
N8.children = []
N8.parents = [N7,N4]

#N9.ID = 9
N9.children = [N3,N5]
N9.parents = [N2]
"""

#nodes = []

"""
N1 = Node(1)
N2= Node(2)
N3 = Node(3)
N4 = Node(4)
N5 = Node(5)
N6 = Node(6)
N7 = Node(7)
N8 = Node(8)
N9 = Node(9)
N10 = Node(10)
N11 = Node(11)
N12 = Node(12)
"""
#num_nodes = 0
#num_edges = 0
"""
num_nodes = 12
#put non-null elements in place
for x in range(num_nodes+1):
    nodes.append(Node(x))
"""

"""
# Assign above nodes
nodes[0] = Node(0)  # not used; num_nodes does not include nodes[0]
nodes[1] = N1
nodes[2] = N2
nodes[3] = N3
nodes[4] = N4
nodes[5] = N5
nodes[6] = N6
nodes[7] = N7
nodes[8] = N8
nodes[9] = N9
nodes[10] = N10
nodes[11] = N11
nodes[12] = N12
"""
"""
N1.children = [N3,N2]
N1.parents = []
N2.children = [N9,N8]
N2.parents = [N1]
N3.children = [N11,N10]
N3.parents = [N1,N4]
N4.children = [N3]
N4.parents = [N5,N6]
N5.children = [N4]
N5.parents = []
N6.children = [N7]
N6.parents = []
N7.children = []
N7.parents = [N6]
N8.children = []
N8.parents = [N2]
N9.children = []
N9.parents = [N2]
N10.children = []
N10.parents = [N3]
N11.children = []
N11.parents = [N3]
"""

"""
# Regular bfs.
def bfs(visited, graph, node): #function for BFS
  visited.append(node)
  queue.append(node)

  while queue:          # Creating loop to visit each node
    m = queue.pop(0) 
    logger.trace (m, end = " ") 

    for neighbor in graph[m]:
      if neighbor not in visited:
        visited.append(neighbor)
        queue.append(neighbor)
"""

# Consider: for Loop Groups, separate non-loop parents from parents so we 
# only do non-loop parents once at beginning).
#
# ToDo: Determine whether a node in current frontier is dependent or
# independent. Independent means it does not have an ancestor (parent
# or a parent of a parent, etc) that is a child of a node in the 
# previous frontier? (We can add multiple frontiers to a partition
# but some of the nodes in the first frontier added will be parents
# of nodes in the seconf frontier added, etc. If P is a parent of a 
# node N in this partition then N inherets the independant/dependant
# of P?  (This is an issue if we can put multiple frontiers in a 
# partition vs only one/)
#
# When pop 86, 86 was put in frontier because at that time (foo) 86 had
# 1 unvisited child 77 after parent traversal so put 86 in queue. But
# when we finished the dfs_parent, *all* nodes were in partition and
# 86 was on frontier, which means 86 had left the frontier and yes 
# all of 86's chldren, including 77, were visited. So 86 should not 
# be in fronter - if we cannot take it off we need to iterate
# through the frontier and adjust it - remove frontier nodes that have
# no unvisited children (so all children in partition) or that have
# singleton children. But still might want to split partition and
# want partition and its frontier?
# 
# So is it better to check all the parents of the children that become visited
# or adjust the queue and frontier after return from dfs_parent
# to bfs? Where we might want to do continue partitioning the result of
# bfs's dfs_parent. The constant child checking during dfs_parent is costly?
# as opposed to just doing it for only the nodes in the queue and the nodes
# in the frontier, i.e., just the nodes for which it is possble to prune.
# 
# And we perhaps want to identify singletons anyway in this same frontier
# reduction? Note that leaving the nodes with no unvisited chldren in the 
# queue does not hurt since bfs will ignore them anyway. Although
# the queue is inaccurate that might not affect the partitioning of
# bsf's dfs_parent result for large partitions returned to bfs.
#
# If not doing child stuff in dfs_parent, do we mark node visited before 
# or after parent_traversal? Consider multithreading version, which will
# stop visiting along a (parent) traversal when it see's a visted node.
# So node visited means: have visited node's parents then set node
# visited or marke visited and will visit node's parents. Does it matter?
#
# ToDo: Any reason to not short circuit 7, i.e., put 7 in queue, i.e., do
# not do the unvisited stuff for 7 when 6 calls dfs_p(7)?
#
# Now storing IDs in queue and frontier
#
# So, for now, check parents of child set to visited; if a parent no longer
# has any unvisited children, then remove parent from queue, if present,
# and frontier, if present. May not be in either, etc. If just popped X from
# queue then not in there but stil in frontier. If dfs_parent removes X
# from queue and frontier then BFS will find X is no longer in frontier
# when it finishes visiting all children of X and tries to remove X.

"""
# ToDo: This can be part of code to reduce size of frontier? Look for these
# sinlge-node stragglers. Recall that we can;t do this in dfs_parent easily
# since we should not put child in partition until its parents are in 
# partition and we haven't put all parents in partition until recursion is
# totally unwound. (Consider child 7 of 6). We try to maintain paent first
# order, so we can, e.g., remove nodes from end of partition and be sure
# parents of remaining nodes are also in partition.
# Or
# put this in dfs_parent after you put parent in the partition, since then 
# you can add the only child of a parent (where child has no other parents)
# as long as the parent is in the partition.
# Note: doing this during dfs_p makes a potentialy big sub-partition added
#  by dfs_p even bigger when you add singletons, but want to visit nodes
#  once, either by dfs or bss, not many times (but we may check if node N
#  has unvisited children and if so add N to queue then evenually its
# unvisited singleton child is added to queue). When we check N to 
# see if it has an (only) singleton child and if no then queue N then we
# visit N twice. So ...

    for ID in frontier:
        node = nodes[ID]
        if len(node.children == 1) and (
            len(node.children[0].children == 0)) and (
            len(node.children[0].parents == 1)):
        # Don't add this node to queue if it has one child, that child has no children, 
        # i.e. it is a sink, and only one parent, which is this node.  Adding node to visited
        # and queue but with this unvisited child would put node on the frontier 
        # but only child has no children so BFS search from node would stop
        # at this child and child has no other parents so all of the child's
        # parents (which is just this node) are in the current partition.  
        # So just add child and node to current partition. 
        logger.trace ("dfs_parent add node " + str(node.ID) + " and child " +
            node.children[0].ID + " to visited since this is the only child"
            + " and child has no children and only one parent which is node")
        visited.append(node.children[0].ID)
        current_partition.append(node.children[0].ID)
        # Do not add node.children[0].ID to frontier since it has no children

        # if this is for reducing partition then node is already visited and in 
        # partition, but node is in frontier and we can now remove it
        #visited.append(node.ID)
        frontier.remove(node.ID)
"""

"""
# old code: BFS origially called dfs_p() for nodes that were dequeued.
# dfs_p_new is essentially dfs_parent. So dequeued nodes are no longer
# treated specially, we call dfs_parent to start the recursive parent
# traversal.

def dfs_p_new(visited, graph, node):  #function for dfs 
    # e.g. dfs(3) where bfs is visiting 3 as a child of enqueued node
    # so 3 is not visited yet
    logger.trace ("dfs_p_new from node " + str(node.ID))

    #dfs_p_start_partition_size = len(current_partition)
    #global loop_nodes_added
    #loop_nodes_added_start = loop_nodes_added

    list_of_unvisited_children = []
    check_list_of_unvisited_chldren_after_visiting_parents = False

    logger.trace("in dfs_p_new start: list_of_unvisited_children:" + str(list_of_unvisited_children))

    # process children before parent traversal
    #list_of_unvisited_children, 
    check_list_of_unvisited_chldren_after_visiting_parents = dfs_parent_pre_parent_traversal(node,
        visited,list_of_unvisited_children)

    logger.trace("in dfs_p_new after pre: list_of_unvisited_children:" + str(list_of_unvisited_children))

    if not len(node.parents):
        logger.trace ("dfs_p node " + str(node.ID) + " has no parents")
    else:
        logger.trace ("dfs_p node " + str(node.ID) + " visit parents")

    # visit parents
    for neighbor_index in node.parents:
        neighbor = nodes[neighbor_index]
        if neighbor.ID not in visited:
            logger.trace ("dfs_p visit node " + str(neighbor.ID))
            dfs_parent(visited, graph, neighbor)
        else:
            logger.trace ("dfs_p neighbor.ID " + str(neighbor.ID) + " already visited")
            if neighbor.partition_number == -1:
                # Example: 1 5 6 7 3(Lp) 12(Lp) 11 11(Lc) 12 4 3 2 10 9 8
                # Here, 3 is a parent of 11 that 11 finds visited so when visiting
                # 11 in dfs_parent 11 will output 3(Lprnt_of_11). Same for when 
                # 11 finds parent 12 is visited 12(Lprnt_of_11) We use "3(Lprnt_of_11)
                # indicators to show a loop was detected when 11 visited parent 3
                # and to show 3 in the partition before 11, where 3 is the parent of 11.
                # We use "12(Lprnt_of_11)" to show a loop was detected when 11 visited 
                # parent 12. 11 is the parent of 12 and 11 was put in partition before 
                # 12 so we do not need "12(Lprnt_of_11)" before the 11 - it is just to 
                # indicates the loop detected when 11 saw it's parent 12 was visited.
                loop_indicator = str(neighbor.ID)+"(Lprnt_of_" + str(node.ID) + ")"
                current_partition.append(loop_indicator)
                logger.trace("[Info]: Possible parent loop detected, start and end with " + str(neighbor.ID)
                    + ", loop indicator: " + loop_indicator)
                loop_nodes_added += 1

    # process children after parent traversal
    dfs_parent_post_parent_traversal(node, visited,
    list_of_unvisited_children, check_list_of_unvisited_chldren_after_visiting_parents)

    #dfs_p_end_partition_size = len(current_partition)
    #loop_nodes_added_end = loop_nodes_added
    #dfs_p_change_in_partitiob_size = (dfs_p_end_partition_size - dfs_p_start_partition_size) - (
    #    loop_nodes_added_end - loop_nodes_added_start)
    #logger.trace("dfs_p_change_in_partition_size: " + str(dfs_p_change_in_partitiob_size))
    #dfs_p_changes_in_partiton_size.append(dfs_p_change_in_partitiob_size)

def dfs_p(visited, graph, node):
    logger.trace ("dfs_p from node " + str(node.ID))
    dfs_p_start_partition_size = len(current_partition)
    loop_nodes_added_start = loop_nodes_added

    # target child node c of dfs_p(c) in bfs was to to visited in bfs before call to dfs_p(c)
    if not len(node.parents):
        logger.trace ("dfs_p node " + str(node.ID) + " has no parents")
    else:
        logger.trace ("dfs_p node " + str(node.ID) + " visit parents")

    for neighbor_index in node.parents:
        neighbor = nodes[neighbor_index]
        if neighbor.ID not in visited:
            logger.trace ("dfs_p visit node " + str(neighbor.ID))
            dfs_parent(visited, graph, neighbor)
        else:
            logger.trace ("dfs_p neighbor.ID " + str(neighbor.ID) + " already visited")


    # make sure parent in partition before any if its children. We visit parents of node 
    # in dfs_parents and they are added to partition in dfs_parents after their parents 
    # are added in dfs_parents then here we add node to partition. node is the target 
    # of dfs_p(node)  
    if node.partition_number == -1:
        logger.trace ("dfs_p add " + str(node.ID) + " to partition")
        node.partition_number = current_partition_number
        current_partition.append(node.ID)
    else:
        logger.trace ("dfs_p do not add " + str(node.ID) + " to partition "
            + str(current_partition_number) + " since it is already in partition " 
            + str(node.partition_number))

    dfs_p_end_partition_size = len(current_partition)
    loop_nodes_added_end = loop_nodes_added
    dfs_p_change_in_partitiob_size = (dfs_p_end_partition_size - dfs_p_start_partition_size) - (
        loop_nodes_added_end - loop_nodes_added_start)
    logger.trace("dfs_p_change_in_partition_size: " + str(dfs_p_change_in_partitiob_size))
    dfs_p_changes_in_partiton_size.append(dfs_p_change_in_partitiob_size)
"""

"""
    def generate_DAG_info_OLD(graph_name, nodes):
    # from DFS_visit
    DAG_map = {} # map from state (per task) to the fanin/fanout/faninNB operations executed after the task is executed
    DAG_states = {} # map from String task_name to the state that task is executed (one state per task)
    DAG_leaf_task_start_states = []
    DAG_leaf_tasks = []
    DAG_leaf_task_inputs = []
    DAG_tasks = {} # map from task name to task, e.g., "add" to add()
    all_fanout_task_names = []	# list of all fanout task names in the DAG
    all_fanin_task_names = []
    all_faninNB_task_names = []
    all_collapse_task_names = []  # if task A is followed only by a fanout to task B: A --> B then we collapse B and A
    all_fanin_sizes = [] # all_fanin_sizes[i] is the size of all_fanin_task_names[i] 
    all_faninNB_sizes = []

    # graph_20
    DAG_map = {}
    DAG_states = {}
    DAG_leaf_tasks = ["PR1"]
    DAG_leaf_task_start_states = [1]# No inputs, inputs are parent prs not partition nodes
    DAG_leaf_task_inputs = [[5,17,1]]
    all_fanout_task_names = ["PR2_1", "PR2_3"]	# list of all fanout task names in the DAG
    all_fanin_task_names = []
    all_faninNB_task_names = ["PR2_2"]
    all_collapse_task_names = ["PR3_1", "PR3_2"]
    all_fanin_sizes = []
    all_faninNB_sizes = [2]
    key_list = ["PR1", "PR2_1", "PR2_2", "PR2_3", "PR3_1", "PR3_2"]
    DAG_tasks = dict.fromkeys(key_list,PageRank)

    # per state

    state = 1
    fanouts = ["PR2_1", "PR2_3"]	# list of task_names of fanout tasks of T --> fanout
    fanins = []	    # list of task_names of fanin tasks of T --> fanin, where there will be a become
    faninNBs = ["PR2_2"]   # list of task_names of fanin tasks of T --> fanin, where there will be no become (NB)
    collapse = []   # list of task_names of collapsed tasks of T --> collapse, where there will be one succ (pred) edge of T (collapse)
    fanin_sizes = [] # sizes of fanins by position in fanins
    faninNB_sizes = [1] # sizes of faninNBs by position in faninNBs  
    
#rhc: No, PageRank needs to generate its outputs by idntifying the dependents and
    #grouping them by fanout followed by fanins.

    fanout1 = [5]
    fanout2 = [1]
    faninNB1 = [17]
    fanout_dependents = [fanout1,fanout2]
    faninNB_dependents = [faninNB1]
    collapse_dependents = []
    DAG_map[state] = state_info("PR1", fanouts, fanins, faninNBs, collapse, fanin_sizes, faninNB_sizes, [5,17,1],
        fanout_dependents, faninNB_dependents,collapse_dependents)
    DAG_states["PR1"] = state

    state = 2
    fanouts = []	
    fanins = []	    
    faninNBs = ["PR2_2"]   
    collapse = []   
    fanin_sizes = [] 
    faninNB_sizes = [1]
    faninNB1 = [2]
    fanout_dependents = []
    faninNB_dependents = [faninNB1]
    collapse_dependents = []
    DAG_map[state] = state_info("PR2_1", fanouts, fanins, faninNBs, collapse, fanin_sizes, faninNB_sizes, ["PR1"],
        fanout_dependents, faninNB_dependents,collapse_dependents)
    DAG_states["PR2_1"] = state

    state = 3
    fanouts = []	
    fanins = []	    
    faninNBs = []   
    collapse = ["PR3_1"]   
    fanin_sizes = [] 
    faninNB_sizes = []
    collapse1 = [8,11]
    fanout_dependents = []
    faninNB_dependents = []
    collapse_dependents = [collapse1]
    DAG_map[state] = state_info("PR2_2", fanouts, fanins, faninNBs, collapse, fanin_sizes, faninNB_sizes, ["PR1","PR2_1"],
        fanout_dependents, faninNB_dependents,collapse_dependents)
    DAG_states["PR2_2"] = state

    state = 4
    fanouts = []	
    fanins = []	    
    faninNBs = []   
    collapse = ["PR3_2"]   
    fanin_sizes = [] 
    faninNB_sizes = []
    collapse1 = [8,11]
    fanout_dependents = []
    faninNB_dependents = []
    collapse_dependents = [collapse1]
    DAG_map[state] = state_info("PR2_3", fanouts, fanins, faninNBs, collapse, fanin_sizes, faninNB_sizes, ["PR1"],
        fanout_dependents, faninNB_dependents,collapse_dependents)
    DAG_states["PR2_3"] = state

    state = 5
    fanouts = []	
    fanins = []	    
    faninNBs = []   
    collapse = []   
    fanin_sizes = [] 
    faninNB_sizes = []
    fanout_dependents = []
    faninNB_dependents = []
    collapse_dependents = []
    DAG_map[state] = state_info("PR3_1", fanouts, fanins, faninNBs, collapse, fanin_sizes, faninNB_sizes, ["PR2_2"],
        fanout_dependents, faninNB_dependents,collapse_dependents)
    DAG_states["PR3_1"] = state

    state = 6
    fanouts = []	
    fanins = []	    
    faninNBs = []   
    collapse = []   
    fanin_sizes = [] 
    faninNB_sizes = []
    fanout_dependents = []
    faninNB_dependents = []
    collapse_dependents = []
    DAG_map[state] = state_info("PR3_2", fanouts, fanins, faninNBs, collapse, fanin_sizes, faninNB_sizes, ("PR2_3"),
        fanout_dependents, faninNB_dependents,collapse_dependents)
    DAG_states["PR3_2"] = state

    DAG_info = {}
    DAG_info["DAG_map"] = DAG_map
    DAG_info["DAG_states"] = DAG_states
    DAG_info["DAG_leaf_tasks"] = DAG_leaf_tasks
    DAG_info["DAG_leaf_task_start_states"] = DAG_leaf_task_start_states
    DAG_info["DAG_leaf_task_inputs"] = DAG_leaf_task_inputs
    DAG_info["all_fanout_task_names"] = all_fanout_task_names
    DAG_info["all_fanin_task_names"] = all_fanin_task_names
    DAG_info["all_faninNB_task_names"] = all_faninNB_task_names
    DAG_info["all_collapse_task_names"] = all_collapse_task_names
    DAG_info["all_fanin_sizes"] = all_fanin_sizes
    DAG_info["all_faninNB_sizes"] = all_faninNB_sizes
    DAG_info["DAG_tasks"] = DAG_tasks

    # For now, add graph nodes to DAG_info, where DAG_info is the DAG
    # for computing the pagerank of the nodes.
    # No, write the nodes and each partition to a file: (nodes,partition)
    # Seems like yuo need to write the dependents that will be inputs to the
    # faninNBs and fanouts. Example For "PR1", partition is [5,17,1] and
    # dependents for "P2_1" are [5] and for "P2_2" are [17] and "P2_3" are [1].
    # So iputs sent to fanouts and faninNBs are list of dependents, which is 
    # different for each fanout/faninNB.
    #DAG_info["PageRank_nodes"] = nodes

    file_name = "./"+graph_name+".pickle"
    with open(file_name, 'wb') as handle:
        cloudpickle.dump(DAG_info, handle) #, protocol=pickle.HIGHEST_PROTOCOL)  

    num_fanins = len(all_fanin_task_names)
    num_fanouts = len(all_fanout_task_names)
    num_faninNBs = len(all_faninNB_task_names)
    num_collapse = len(all_collapse_task_names)

    logger.trace("DAG_map:")
    for key, value in DAG_map.items():
        logger.trace(str(key) + ' : ' + str(value))
    logger.trace("")
    logger.trace("states:")         
    for key, value in DAG_states.items():
        logger.trace(str(key) + ' : ' + str(value))
    logger.trace("")
    logger.trace("num_fanins:" + str(num_fanins) + " num_fanouts:" + str(num_fanouts) + " num_faninNBs:" 
            + str(num_faninNBs) + " num_collapse:" + str(num_collapse))
    logger.trace("")  
    logger.trace("all_fanout_task_names")
    for name in all_fanout_task_names:
        logger.trace(name)
    logger.trace("")
    logger.trace("all_fanin_task_names")
    for name in all_fanin_task_names :
        logger.trace(name)
    logger.trace("")
    logger.trace("all_faninNB_task_names")
    for name in all_faninNB_task_names:
        logger.trace(name)
    logger.trace("")
    logger.trace("all_collapse_task_names")
    for name in all_collapse_task_names:
        logger.trace(name)
    logger.trace("")
    logger.trace("leaf task start states")
    for start_state in DAG_leaf_task_start_states:
        logger.trace(start_state)
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
"""

"""
generate_DAG_info("graph20_DAG", nodes)
"""