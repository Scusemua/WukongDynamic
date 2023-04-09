import networkx as nx
import matplotlib.pyplot as plt
import numpy as np

import logging 
import cloudpickle
import os

from collections import defaultdict
import copy

#from .DFS_visit import state_info
#from .DAG_info import DAG_Info
#from .DAG_executor_constants import run_all_tasks_locally, using_threads_not_processes

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
#logger.setLevel(logging.INFO)
formatter = logging.Formatter('[%(asctime)s] [%(threadName)s] %(levelname)s: %(message)s')
#formatter = logging.Formatter('%(levelname)s: %(message)s')
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
#ch.setLevel(logging.INFO)
ch.setFormatter(formatter)
logger.addHandler(ch)

USING_BFS = False

def input_DAG_info(file_name):
    with open(file_name, 'rb') as handle:
        DAG_info = cloudpickle.load(handle)
    return DAG_info

class DAG_Info(object):
    def __init__(self,file_name = './DAG_info.pickle'):
        self.file_name = file_name
        self.DAG_info = input_DAG_info(file_name)
    def get_DAG_map(self):
        return self.DAG_info["DAG_map"]
    def get_DAG_states(self):
        return self.DAG_info["DAG_states"]
    def get_all_fanin_task_names(self):
        return self.DAG_info["all_fanin_task_names"]
    def get_all_fanin_sizes(self):
        return self.DAG_info["all_fanin_sizes"]
    def get_all_faninNB_task_names(self):
        return self.DAG_info["all_faninNB_task_names"]
    def get_all_faninNB_sizes(self):
        return self.DAG_info["all_faninNB_sizes"]
    def get_all_fanout_task_names(self):
        return self.DAG_info["all_fanout_task_names"]
    def get_DAG_leaf_tasks(self):
        return self.DAG_info["DAG_leaf_tasks"]
    def get_DAG_leaf_task_start_states(self):
        return self.DAG_info["DAG_leaf_task_start_states"]
    def get_DAG_leaf_task_inputs(self):
        return self.DAG_info["DAG_leaf_task_inputs"]
    # After the driver gets the leaf task inputs it sets DAG_info["DAG_leaf_task_inputs"]
    # to None so that we are not passing all of these inputs to each Lambda executor.
    def set_DAG_leaf_task_inputs_to_None(self):
        self.DAG_info["DAG_leaf_task_inputs"] = None
    def get_DAG_tasks(self):
        return self.DAG_info["DAG_tasks"]

class state_info:
    def __init__(self, task_name, fanouts = None, fanins = None, faninNBs = None, collapse = None,
        fanin_sizes = None, faninNB_sizes = None, task_inputs = None, 
        dependents_per_fanout=None, dependents_per_faninNB=None, dependents_per_collapse=None):
        self.task_name = task_name
        self.fanouts = fanouts      # see comment below for examples
        self.fanins = fanins
        self.faninNBs = faninNBs
        self.fanin_sizes = fanin_sizes
        self.faninNB_sizes = faninNB_sizes
        self.collapse = collapse
        self.task_inputs = task_inputs
    def __str__(self):
        if self.fanouts != None:
            fanouts_string = str(self.fanouts)
        else:
            fanouts_string = "None"
        if self.fanins != None:
            fanins_string = str(self.fanins)
        else:
            fanins_string = "None"
        if self.faninNBs != None:
            faninNBs_string = str(self.faninNBs)
        else:
            faninNBs_string = "None"
        if self.collapse != None:
            collapse_string = str(self.collapse)
        else:
            collapse_string = "None"
        if self.fanin_sizes != None:
            fanin_sizes_string = str(self.fanin_sizes)
        else:
            fanin_sizes_string = "None"
        if self.faninNB_sizes != None:
            faninNB_sizes_string = str(self.faninNB_sizes)
        else:
            faninNB_sizes_string = "None"         
        if self.task_inputs != None:
            task_inputs_string = str(self.task_inputs)
        else:
            task_inputs_string = "None"  
        return (" task: " + self.task_name + ", fanouts:" + fanouts_string + ", fanins:" + fanins_string + ", faninsNB:" + faninNBs_string 
            + ", collapse:" + collapse_string + ", fanin_sizes:" + fanin_sizes_string
            + ", faninNB_sizes:" + faninNB_sizes_string + ", task_inputs: " + task_inputs_string
        )

"""
class PageRank_results:
    def __int__(self):
        self.results = []
        for _ in range(num_nodes+1):
            self.results.append(0.0)
    def setResult(self,i,pagerank):
        self.results[i]=pagerank
    def print_results(self):
        for i in range(1,21):
            print(str(i)) # +":"+str(self.results[i]),end=" ")
"""
class Graph:

    def __init__(self, vertices=0):
        # No. of vertices
        self.V = vertices
        self.num_edges = 0

        # default dictionary to store graph
        self.graph = defaultdict(list)

        self.Time = 0

        self.scc_NodeID_to_GraphID_map = {}
        self.scc_GraphID_to_NodeID_map = {}
        self.next_scc_ID = 0

    # We need nodes in range 0 .. num_vertices-1, so collapse node IDs.
    # node.ID mapped to next as you see the nodes, with another map to get 
    # back to original IDs, map(next,node.ID). Then the SCC is a set of ids 
    # x, y, ... where the actual node IDs are map(x) and map(y). Map back
    # before logger.infoing the scc's.
    def map_nodeID_to_GraphID(self,ID):
        if ID not in self.scc_NodeID_to_GraphID_map:
            Graph_ID = self.next_scc_ID
            self.scc_NodeID_to_GraphID_map[ID] = Graph_ID
            self.scc_GraphID_to_NodeID_map[self.next_scc_ID] = ID
            self.next_scc_ID += 1
            self.V += 1
            return Graph_ID
        else:
            return self.scc_NodeID_to_GraphID_map[ID]
    def get_GraphID(self,ID):
        return self.scc_NodeID_to_GraphID_map[ID]
    def get_nodeID_from_GraphID(self,ID):
            return self.scc_GraphID_to_NodeID_map[ID]

    def print_ID_map(self):
        logger.debug("scc_NodeID_to_GraphID_map:")
        for i in self.scc_NodeID_to_GraphID_map:
            logger.info (i, self.scc_NodeID_to_GraphID_map[i])
        logger.debug("scc_NodeID_to_GraphID_map:")
        for i in self.scc_GraphID_to_NodeID_map:
            logger.info (i, self.scc_GraphID_to_NodeID_map[i])

    # added to code
    def setV(self,V):
        self.V = V

	# function to add an edge to graph
    def addEdge(self, u, v):
        self.graph[u].append(v)
        self.num_edges += 1

    def printEdges(self):
        logger.info("graph scc_graph GraphIDs: num_vertices: " + str(self.V) 
            + ", num_edges: " + str(self.num_edges) + ": ")
        for k, v in self.graph.items():
            for item in v:
                logger.info(str(k) + "," + str(item))
        logger.info("graph scc_graph node IDs: num_vertices: " + str(self.V) 
            + ", num_edges: " + str(self.num_edges) + ": ")
        for k, v in self.graph.items():
            for item in v:
                logger.info(str(self.get_nodeID_from_GraphID(k)) + "," + str(self.get_nodeID_from_GraphID(item)))

    def clear(self):
        logger.info("clear scc_graph")
        self.graph = defaultdict(list)
        self.V = 0
        self.num_edges = 0
        self.Time = 0
        self.scc_NodeID_to_GraphID_map = {}
        self.scc_GraphID_to_NodeID_map = {}
        self.next_scc_ID = 0



visited = [] # List for visited nodes.
queue = []     #Initialize a queue
partitions = []
current_partition = []
current_partition_number = 1
dfs_parent_changes_in_partiton_size = []
dfs_parent_changes_in_frontier_size = []
# This is used in the pre/post dfs_parent code when adding L-nodes to
# partitions.
loop_nodes_added = 0
shadow_nodes_added_to_partitions = 0
shadow_nodes_added_to_groups = 0
total_loop_nodes_added = 0
frontier_costs = []
frontier_cost = []
frontiers = []
frontier = []
all_frontier_costs = []
frontier_groups_sum = 0
num_frontier_groups = 0
groups = []
current_group = []
patch_parent_mapping_for_partitions = []
patch_parent_mapping_for_groups = []
frontier_parent_partition_patch_tuple_list = []
frontier_parent_group_patch_tuple_list = []
sender_receiver_partition_patch_tuple_list = []
sender_receiver_group_patch_tuple_list = []
current_group_number = 1
partition_names = []
group_names = []
current_partition_isLoop = False
current_group_isLoop = False
#For DAG generation, map sending task to list of Reveiving tasks, and 
# map receiving task to list of Sending tasks.
Partition_senders = {}
Partition_receivers = {}
Group_senders = {}
Group_receivers = {}
# These are the names of the partitions that have a loop. In the 
# DAG, we will append an 'L' to the name.
Partition_loops = set()
# These are the names of the groups that have a loop. In the 
# DAG, we will append an 'L' to the name.
# Note: Not using this, which is used when generating the DAG to first
# modify non-loop names (in Group_loops) to loop-names (with an 'L').
# Now we use sender and reeiver loop names that have an 'L' so we don't
# have to modify them when we get to building the DAG. Left it in for
# debugging - so we can see which groups become loop groups.
Group_loops = set()
# map the index of a node in nodes to its index in its partition/group.
# node i in nodes is in position i. When we place a node in a partition/group, 
# this node is not assumed to be in postion i; nodes are added to the partition/group
# one by one using append. We map node i, whch we know is at position i in nodes,
# to its position in its partition/group. Example node 25 in nodes at position 25 is mapped 
# to position 4 in its partition/group.
# Note: we map shadow nodes to their positions too. We do not map shadow nodes 
# in the global map nodeIndex_to_partition_partitionIndex_group_groupIndex_map since
# a shadow node ID and a non-shadow node for ID would have the same key. We could 
# use string keys and use, e.g, "5" and "5s" for "shadow" so the keys would be unique.
nodeIndex_to_partitionIndex_map = {}
nodeIndex_to_groupIndex_map = {}
# collection of all nodes_to_group_map maps, one for each group
nodeIndex_to_partitionIndex_maps = []
nodeIndex_to_groupIndex_maps = []
# map a noe to its partition number, partition index, group number ans group index.
# A "global map"for nodes. May supercede nodeIndex_to_partitionIndex_map. We need
# a nodes position in its partition if we map partitions to functions and we need
# a nodes position in its group if we map groups to functions. This map supports
# both partition mapping and group mapping.
# Q: We can remove the nodes in Pi from this map after we have finished 
# computing Pi+1 since we will no longer need to know this info for 
# the nodes in Pi? We may want to remove these nodes to free the space.
# Note: If a node is in Pi+1 all of its parents are in Pi+1 or Pi,
# by definition, since Pi+1 contains all the children of Pi and
# all of the parents (actually, ancestor) of these Pi+1 nodes that 
# are not in Pi.
nodeIndex_to_partition_partitionIndex_group_groupIndex_map = {}

dfs_parent_start_partition_size = 0
loop_nodes_added_start = 0
dfs_parent_start_frontier_size = 0
dfs_parent_end_partition_size = 0
loop_nodes_added_end = 0
dfs_parent_end_frontier_size = 0

IDENTIFY_SINGLETONS = False
TRACK_PARTITION_LOOPS = False
CHECK_UNVISITED_CHILDREN = False
DEBUG_ON = True
PRINT_DETAILED_STATS = True
debug_pagerank = False

#scc_graph = Graph(0)
scc_num_vertices = 0

class Node:
    def __init__(self,ID):
        self.partition_number = -1
        self.group_number = -1
        self.ID = ID
        self.parents = []
        self.children = []
        self.num_children  = 0
#rhc: ToDo
        # this will be in Partition_Node not here
        self.pagerank = 0.00
        # Same for prev - we have these here so we can compute PageRank over all Nodes.
        self.prev = 0.00
        # a list of tuples (frontier, frontier_group) if this is a parent node
        # on the frontier (and so must be sent to its children's partitions).
        # We may send it to multiple chldren in differetn partitions or
        # multiple children in the same partition. For the latter we only 
        # send one copy to the one partition. 
        self.frontier_parents =  []
        # True if this is a shadow node, i.e., a place holder for the actual
        # parent node that will be sent (via  fanout/faninNB) to the partition
        # containing this node. Shadow nodes immediately precede their children
        # in the partition. The pagerank value of this node was computer by 
        # the previous parition and sent to this partition so the child in this
        # partition can use it for their pagerank computation.
#rhc: ToDo
        # this will be in Partition_Node not here
        self.isShadowNode = False

    def __eq__(self,other):
        return self.ID == other.ID

#rhc: ToDo
    # change this if move shadow node
    def __str__(self):
        shadow = ""
        if self.isShadowNode:
            shadow = "-s"
        return str(self.ID) + shadow

    def update_PageRank_of_PageRank_Function_loop(self, partition_or_group,damping_factor,
        one_minus_dumping_factor,random_jumping,total_num_nodes):
        parent_nodes = self.parents
        if not self.isShadowNode:
            my_ID = str(self.ID)
        else:
            my_ID = str(self.ID) + "-s"

        global debug_pagerank
        #logger.debug("debug_pagerank: "  + str(debug_pagerank))
        if (debug_pagerank):
            logger.debug("update_pagerank: node " + my_ID)
            logger.debug("update_pagerank: parent_nodes: " + str(parent_nodes))
            logger.debug("update_pagerank: num_children: " + str(self.num_children))
        
        #if self.ID == 16:
        #    parent1 = partition_or_group[1]
        #    parent2 = partition_or_group[2]
        #    if (debug_pagerank):
        #        logger.info("16 parent : " + str(parent1.ID) + " num_children: " + str(parent1.num_children))
        #       logger.info("16 parent : " + str(parent2.ID) + " num_children: " + str(parent2.num_children))
        
        #Note: a paent has at least one child so num_children is not 0
        pagerank_sum = sum((partition_or_group[node_index].prev / partition_or_group[node_index].num_children) for node_index in parent_nodes)
        if (debug_pagerank):
            logger.debug("update_pagerank: pagerank_sum: " + str(pagerank_sum))
        #random_jumping = damping_factor / total_num_nodes
        if (debug_pagerank):
            logger.debug("damping_factor:" + str(damping_factor) + " 1-damping_factor:" + str(1-damping_factor) + " num_nodes: " + str(total_num_nodes) + " random_jumping: " + str(random_jumping))
        #self.pagerank = random_jumping + ((1-damping_factor) * pagerank_sum)
        self.pagerank = random_jumping + (one_minus_dumping_factor * pagerank_sum)
        if (debug_pagerank):
            logger.debug ("update_pagerank: pagerank of node: " + str(self.ID) + ": " + str(self.pagerank))
            logger.debug("")


class Partition_Node:
    def __init__(self,ID):
        self.partition_number = -1
        self.group_number = -1
        self.ID = ID
        self.parents = []
        self.num_children = 0
        #self.children = []
        self.prev = 0.00
        self.pagerank = 0.00
        # a list of tuples (frontier, frontier_group) if this is a parent node
        # on the frontier (and so must be sent to its children's partitions).
        # We may send it to multiple chldren in different partitions or
        # multiple children in the same partition. For the latter we only 
        # send one copy to the one partition. 
        self.frontier_parents =  []
        # True if this is a shadow node, i.e., a place holder for the actual
        # parent node that will be sent (via  fanout/faninNB) to the partition
        # containing this node. Shadow nodes immediately precede their children
        # in the partition. The pagerank value of this node was computer by 
        # the previous parition and sent to this partition so the child in this
        # partition can use it for their pagerank computation.
        self.isShadowNode = False
        # Note: can't be a shadow node and have a non-empty frontier_parents

    def update_PageRank_main(self, damping_factor,total_num_nodes):
        parent_nodes = self.parents
        logger.info("update_pagerankM: node " + str(self.ID))
        logger.info("update_pagerankM: parent_nodes: " + str(parent_nodes))
        logger.info("update_pagerankM: num_children: " + str(self.num_children))
        #Note: a paent has at least one child so len(children) is not 0
        pagerank_sum = sum((nodes[node_index].pagerank / len(nodes[node_index].children)) for node_index in parent_nodes)
        logger.info("update_pagerankM: pagerank_sum: " + str(pagerank_sum))
        random_jumping = damping_factor / total_num_nodes
        logger.info("update_pagerankM: damping_factor:" + str(damping_factor) + " num_nodes: " + str(total_num_nodes) + " random_jumping: " + str(random_jumping))
        self.pagerank = random_jumping + (1-damping_factor) * pagerank_sum
        logger.info ("update_pagerankM: update_pagerank: pagerank of node: " + str(self.ID) + ": " + str(self.pagerank))
        logger.info("")

    def update_PageRank_of_PageRank_Function(self, partition_or_group,damping_factor,
        one_minus_dumping_factor,random_jumping,total_num_nodes):
        parent_nodes = self.parents
        if not self.isShadowNode:
            my_ID = str(self.ID)
        else:
            my_ID = str(self.ID) + "-s"

        global debug_pagerank
        #logger.debug("debug_pagerank: "  + str(debug_pagerank))
        if (debug_pagerank):
            logger.debug("update_pagerank: node " + my_ID)
            logger.debug("update_pagerank: parent_nodes: " + str(parent_nodes))
            logger.debug("update_pagerank: num_children: " + str(self.num_children))
        
        
        #if self.ID == 16:
        #    parent1 = partition_or_group[1]
        #    parent2 = partition_or_group[2]
        #    if (debug_pagerank):
        #        logger.info("16 parent : " + str(parent1.ID) + " num_children: " + str(parent1.num_children))
        #       logger.info("16 parent : " + str(parent2.ID) + " num_children: " + str(parent2.num_children))
        
        #Note: a paent has at least one child so num_children is not 0
        pagerank_sum = sum((partition_or_group[node_index].pagerank / partition_or_group[node_index].num_children) for node_index in parent_nodes)
        if (debug_pagerank):
            logger.debug("update_pagerank: pagerank_sum: " + str(pagerank_sum))
        #random_jumping = damping_factor / total_num_nodes
        if (debug_pagerank):
            logger.debug("damping_factor:" + str(damping_factor) + " 1-damping_factor:" + str(1-damping_factor) + " num_nodes: " + str(total_num_nodes) + " random_jumping: " + str(random_jumping))
        #self.pagerank = random_jumping + ((1-damping_factor) * pagerank_sum)
        self.pagerank = random_jumping + (one_minus_dumping_factor * pagerank_sum)
        if (debug_pagerank):
            logger.debug ("update_pagerank: pagerank of node: " + str(self.ID) + ": " + str(self.pagerank))
            logger.debug("")

    def update_PageRank_of_PageRank_Function_loop(self, partition_or_group,damping_factor,
        one_minus_dumping_factor,random_jumping,total_num_nodes):
        parent_nodes = self.parents
        if not self.isShadowNode:
            my_ID = str(self.ID)
        else:
            my_ID = str(self.ID) + "-s"

        global debug_pagerank
        #logger.debug("debug_pagerank: "  + str(debug_pagerank))
        if (debug_pagerank):
            logger.debug("update_pagerank: node " + my_ID)
            logger.debug("update_pagerank: parent_nodes: " + str(parent_nodes))
            logger.debug("update_pagerank: num_children: " + str(self.num_children))
        
        #if self.ID == 16:
        #    parent1 = partition_or_group[1]
        #    parent2 = partition_or_group[2]
        #    if (debug_pagerank):
        #        logger.info("16 parent : " + str(parent1.ID) + " num_children: " + str(parent1.num_children))
        #       logger.info("16 parent : " + str(parent2.ID) + " num_children: " + str(parent2.num_children))
        
        #Note: a paent has at least one child so num_children is not 0
        pagerank_sum = sum((partition_or_group[node_index].prev / partition_or_group[node_index].num_children) for node_index in parent_nodes)
        if (debug_pagerank):
            logger.debug("update_pagerank: pagerank_sum: " + str(pagerank_sum))
        #random_jumping = damping_factor / total_num_nodes
        if (debug_pagerank):
            logger.debug("damping_factor:" + str(damping_factor) + " 1-damping_factor:" + str(1-damping_factor) + " num_nodes: " + str(total_num_nodes) + " random_jumping: " + str(random_jumping))
        #self.pagerank = random_jumping + ((1-damping_factor) * pagerank_sum)
        self.pagerank = random_jumping + (one_minus_dumping_factor * pagerank_sum)
        if (debug_pagerank):
            logger.debug ("update_pagerank: pagerank of node: " + str(self.ID) + ": " + str(self.pagerank))
            logger.debug("")

    def __eq__(self,other):
        return self.ID == other.ID

    def __str__(self):
        shadow = ""
        if self.isShadowNode:
            shadow = "-s"
        return str(self.ID) + shadow

    def toString_PageRank(self):
        if not self.isShadowNode:
            my_ID = str(self.ID)
        else:
            my_ID = str(self.ID) + "-s"
        return("ID:" + my_ID + " pr:" + str(self.pagerank) + " num_children:"+str(self.num_children))

nodes = []

num_nodes = 0
num_edges = 0
"""
num_nodes = 12
#put non-null elements in place
for x in range(num_nodes+1):
    nodes.append(Node(x))
"""

# visual is a list which stores all the set of edges that constitutes a graph
visual = []
def visualize():
    fig = plt.figure()
    #fig.set_size_inches(width,height)
    fig.set_figheight(8)
    fig.set_figwidth(12)
    fig.show()
    G = nx.DiGraph()
    G.add_edges_from(visual)
    nx.draw_networkx(G)
    #plt.show()
    # comment 
    #nx.draw_planar(G,with_labels = True, alpha=0.8) #NEW FUNCTION
    fig.canvas.draw()

# process children before parent traversal
def dfs_parent_pre_parent_traversal(node,visited,list_of_unvisited_children):
    check_list_of_unvisited_chldren_after_visiting_parents = False
    # set child node to visited if possible before dfs_parent so that when the parent 
    # checks if this child is visited it will be visited. 
    logger.debug("dfs_parent_pre: at start: list_of_unvisited_children:" + str(list_of_unvisited_children))
    if len(node.children) == 0:
        # Can a child be in visited? If child was visited then parent must have been
        # already visited? No. 
        # This is true for partition - ad node afte add parent - 
        # but not for visited. We traverse parents in reverse order
        # of child arrows, and node N can have a child C that has no children 
        # and we may visit C during a parents traversal before we visit N (as C's
        # parent is N) and if C has no children we will mark C as visited and
        # then visit C's parent which is N which will see its child C as visited.
        # Example: children arrows. 3 --> 11 --> 12 --> 4. So parent traversal 
        # is 4, 12, 11, 3 so node 11 is parent of child C 12.
        check_list_of_unvisited_chldren_after_visiting_parents = False
        visited.append(node.ID)
        logger.debug ("dfs_parent_pre: add " + str(node.ID) + " to visited since no children")              
    else:
        # node has more than one child or it has one child that has one or more
        # children (it is not a sink) or more than one parent (so if
        # we put child in partition we have to put all parents (including node)
        # in the partition (first))
        has_unvisited_children = False
        for neighbor_index in node.children:
            child_node = nodes[neighbor_index]
            if child_node.ID not in visited:
                logger.debug ("dfs_parent_pre: child " + str(child_node.ID) + " not in visited")
                has_unvisited_children = True
                list_of_unvisited_children.append(child_node.ID)
                #break
        if not has_unvisited_children:
            logger.debug ("dfs_parent_pre mark " + str(node.ID) + " as visited since it has no unvisited children "
            + "but do not add it to bfs queue since no children need to be visited")
            check_list_of_unvisited_chldren_after_visiting_parents = False
            visited.append(node.ID)
        else:
            logger.debug ("dfs_parent_pre " + str(node.ID) + " has unvisted children so mark " 
                + str(node.ID) + " as visited and check children again after parent traversal")
            # this node can be marked as visited, but we will only add it to the queue
            # if these unvisited children are still unvisited when we return from 
            # visiting the parent nodes (all ancestors). If children nodes are 
            # also parents or ancestors (in general) then we may visit them on 
            # parent traversal and if we visit all of th node's children this way then
            # we need not add node to the queue. 
            visited.append(node.ID)
            check_list_of_unvisited_chldren_after_visiting_parents = True
            logger.debug("dfs_parent_pre: set check_list_of_unvisited_chldren True")
            logger.debug("dfs_parent_pre: list_of_unvisited_children:" + str(list_of_unvisited_children))
#rhc: un
            node.unvisited_children = list_of_unvisited_children

#rhc: un
    for neighbor_index in node.parents:
        parent_node = nodes[neighbor_index]
        if parent_node.ID in visited:
            parent_node.unvisited_children.remove(node.ID)
            if len(parent_node.unvisited_children) == 0:
                logger.debug("*******dfs_parent_pre_parent_traversal: " + str(parent_node.ID) + " after removing child " 
                    + str(node.ID) + " has no unvisited children, so remove "
                    + str(parent_node.ID) + " from queue and frontier.")
                try:
                    queue.remove(parent_node.ID)
                except ValueError:
                    logger.debug("*******dfs_parent_pre_parent_traversal: " + str(parent_node.ID)
                    + " not in queue.")
                try:
                    frontier.remove(parent_node.ID)
                except ValueError:
                    logger.debug("*******dfs_parent_pre_parent_traversal: " + str(parent_node.ID)
                    + " not in frontier.")
    
    return check_list_of_unvisited_chldren_after_visiting_parents

#def dfs_parent(visited, graph, node):  #function for dfs 
def dfs_parent(visited, node):  #function for dfs 
    # e.g. dfs(3) where bfs is visiting 3 as a child of enqueued node
    # so 3 is not visited yet
    logger.debug ("dfs_parent from node " + str(node.ID))

    list_of_unvisited_children = []
    check_list_of_unvisited_chldren_after_visiting_parents = False

    # Fill these in below, e.g., we have to remap the parents since 
    # parent IS is not in position ID in the partition/group.
    partition_node = Partition_Node(node.ID)
    partition_node.ID = node.ID
    group_node = Partition_Node(node.ID)
    group_node.ID = node.ID

    # Get unvisited children, which affects whether ndoe is addded to the queue
    # in the post traversal.
    # For example, assume 4's parent is 6 and 6 has no parents and only
    # one child 7 where 7 has no children and 7's only parent is 6.
    # With singleton checking, 4 will call dfs_parent(6), which will mark
    # 6 as visited and look at 6's children to see whether 6 should be queued.
    # dfs_parent(6) will see that 6 has an unvisited child 7, which remains
    # unvisited after dfs_parent(6) tries to traverse 6's parents (but
    # it has none). If checking for singletons, dfs_parent(6) will see that
    # 7 is a singleton and so mark 7 as visited, add 6 then 7 (parent first)
    # to the current partition, and not add 6 or 7 to the queue. If singleton
    # chcking is off, then 7 will not be marked visited (6 was already marked
    # visited) and 6 will be added to the queue. In this case, 6 is added to 
    # the frontier having a singleton child 7. When the current partition is
    # full, we can examine the frontier, and for 6 we can move its singleton
    # child 7 into the frontier, reducing the cost of the fronter by 1.
    # When 6 is dequeued, we call dfs_parent(7), whcih sees that 7 has no chldren
    # and marks 7 as visited. 7's parents (6) are already visited so after
    # the parent traversal 7 still has no unvisited children. Thus 7 is not 
    # added to the queue or the frontier (since it has no children) and 7 is
    # added to the curret partition.
    if CHECK_UNVISITED_CHILDREN:
        check_list_of_unvisited_chldren_after_visiting_parents = dfs_parent_pre_parent_traversal(node,visited,list_of_unvisited_children)
        logger.debug("after pre: list_of_unvisited_children: " + str(list_of_unvisited_children))
    else:
#rhc: If not doing child stuff do we mark node visited here or when we enqueue 
# node in dfs_parent path?
        visited.append(node.ID)

    #Note: dfs_parent_pre_parent_traversal will mark node as visitd

    
    # Note: BFS will not call dfs_parent(child) if chld has been visited. So
    # if child has been visited and thus has been added to global map, we will 
    # not be resetting the pg_tuple here of such a child.
    #
    # Put node in the global node map with -1 as the partition and group number.
    # replace the -1 when we eventually put the node in a partition and group.
    # until then, we'll get -1 to indicate that we haven't placed the node yet.
    partition_number = current_partition_number
    parent_partition_index = -1
    group_number = current_group_number
    parent_group_parent_index = -1
    index_in_groups_list = -1
    init_pg_tuple = (partition_number,parent_partition_index,group_number,parent_group_parent_index,index_in_groups_list)
    global nodeIndex_to_partition_partitionIndex_group_groupIndex_map
    nodeIndex_to_partition_partitionIndex_group_groupIndex_map[node.ID] = init_pg_tuple

    """
    # for debugging
    logger.info("nodeIndex_to_partition_partitionIndex_group_groupIndex_map, len: " + str(len(nodeIndex_to_partition_partitionIndex_group_groupIndex_map)) + ":")
    logger.info("shadow nodes not mapped and not shown")
    if PRINT_DETAILED_STATS:
        for k, v in nodeIndex_to_partition_partitionIndex_group_groupIndex_map.items():
            logger.info((k, v))
        logger.info("")
    else:
        logger.info("-- (" + str(len(nodeIndex_to_partition_partitionIndex_group_groupIndex_map)) + ")")
    logger.info("")
    """

    if not len(node.parents):
        logger.debug ("dfs_parent node " + str(node.ID) + " has no parents")
    else:
        logger.debug ("dfs_parent node " + str(node.ID) + " visit parents")

    # part of SCC computation
    #node_GraphID = scc_graph.map_nodeID_to_GraphID(node.ID)

    #parents_in_previous_partition = False
    # visit parents
    list_of_parents_in_previous_partition = []

    #parents_in_previous_group = False
    # visit parents
    list_of_parents_in_previous_group = []

    already_visited_parents = []
    index_of_parent = 0
    for parent_index in node.parents:
 
        parent_node = nodes[parent_index]
        logger.debug("parent_node: " + str(parent_node))

        """
        Note: This entire check of different partition/group was moved down to after 
        call to dfs_parent, when we determine that parent is in same partition,
        instead of here before the call.

        partition_group_tuple = nodeIndex_to_partition_partitionIndex_group_groupIndex_map.get(parent_node.ID)
        parent_partition_number = None
        parent_group_number = None
        if partition_group_tuple != None:
        """

        # declaration of pg_tuple moved here before the if statement since 
        # pg_tuple is used in both the then and else part
        pg_tuple = None

        if parent_node.ID not in visited:
            logger.debug ("dfs_parent visit node " + str(parent_node.ID))
            #dfs_parent(visited, graph, parent_node)
            dfs_parent(visited, parent_node)

#rhc: case: no shadow nodes since parent is in this partition/group as we have not 
# visited parent previously. Check if this is a loop and parent partition/group
# number is -1
            # get pg_tuple after dfs_parent returns so parent has been processed
            pg_tuple = nodeIndex_to_partition_partitionIndex_group_groupIndex_map[parent_index]
            # parent has been mapped but, in general, the partition and group indices
            # might be -1. Here, they should not be -1 since the parent was unvisited
            # and loops require a parent that has already been visited. (This already
            # visited parent visits its parent ,which visits its parent etc until we try
            # to revisit the already visited parent. Note: this applies also fro loops
            # within loops since in such a case we must still try to visit an already 
            # visited parent, From above, for documentation, a nodes's global map
            # info is initialized at start of dfs_parent as:
            #partition_number = current_partition_number
            #partition_index = -1
            #group_number = current_group_number
            #group_index = -1
            parent_partition_index = pg_tuple[1]
            parent_group_parent_index = pg_tuple[3]
            if (parent_partition_index == -1) or (parent_group_parent_index == -1):
                # assert group_index is also -1
                logger.debug("[Error]: Internal Error: dfs_parent call to unvisited"
                    + " parent resulted in parent/group partition index of -1, which means"
                    + " a loop was detected at an unvisited parent.")
            partition_node.parents.append(parent_partition_index)
            group_node.parents.append(parent_group_parent_index)

        else:
            # loop detected - mark this loop in partition (for debugging for now)
            logger.debug ("dfs_parent neighbor " + str(parent_node.ID) + " already visited")
            parent_node_visited_tuple = (parent_node,index_of_parent)
            already_visited_parents.append(parent_node_visited_tuple)
            partition_node.parents.append(-1)
            group_node.parents.append(-1)
    
            # parent node has been processed so get its info and determine whether
            # this indicates a loop
            pg_tuple = nodeIndex_to_partition_partitionIndex_group_groupIndex_map[parent_index]
            parent_partition_index = pg_tuple[1]

            #rhc: Note: Not clear whether we will be tracking loops here and if so what 
            # we want to do when we find a loop. For now, TRACK_PARTITION_LOOPS is False
            if TRACK_PARTITION_LOOPS:
                # this pg_tuple was moved up before this if since it is also used
                # after the if.
                #pg_tuple = nodeIndex_to_partition_partitionIndex_group_groupIndex_map[parent_index]

                # parent_partition_number = pg_tuple[0]
                # Changed to parent_partition_index since we set the the 
                #parent_partition_number to current_partition_number at the
                # beginning of dfs_parents().
                # parent_partition_index = pg_tuple[1]
                if parent_partition_index == -1:
                #if parent_partition_number == -1:
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
                    loop_indicator = str(parent_node.ID)+"(Lprnt_of_" + str(node.ID) + ")"
                    current_partition.append(loop_indicator)
                    logger.debug("[Info]: Possible parent loop detected, start and end with " + str(parent_node.ID)
                        + ", loop indicator: " + loop_indicator)
                    global loop_nodes_added
                    loop_nodes_added += 1

            # Detect a loop here instead of below when we check each parent_node_visited_tuple
            # since this allows us to detect a loop now and hence use a partition or group
            # name with an 'L' at the end, e.g., "PR2_2L" when we crate frontier tuples
            # and add names to the Senders and Receivers structures used for DAG creation.
            if parent_partition_index == -1:
                logger.debug("XXXXXXXXXXXXXXXXX dfs_parent: Loop Detected: "
                    + "PR" + str(current_partition_number) + "_" + str(num_frontier_groups))
                global current_partition_isLoop
                current_partition_isLoop = True
                # assert:
                if parent_group_parent_index != -1:
                    logger.error("[Error] Internal Error: parent_partition_index is -1"
                        + " indicating that current partition is a loop but "
                        + " parent_group_parent_index is not -1, when the group should also be a loop.") 
                global current_group_isLoop
                current_group_isLoop = True
            else:
                logger.debug("YYYYYYYYYYYYY dfs_parent: No Loop Detected: "
                    + "PR" + str(current_partition_number) + "_" + str(num_frontier_groups))


        index_of_parent += 1

    #N ote: If a loops is detecte current_partition_isLoop and current_group_isLoop are
    # both set to True. current_partition_isLoop remains True until the end 
    # of the partition is reached. current_group_isLoop is set to False when the 
    # end of the group is reached. So it is possible that current_partition_isLoop is
    # True and current_partition_isLoop is False.

    # The name of the current partition/group depends on whether it
    # has a loop. If so we add an 'L' to the end of the name.
    # Example: "PR2_2" becomes "PR2_2L".
    current_partition_name = "PR" + str(current_partition_number) + "_1"
    current_group_name = "PR" + str(current_partition_number) + "_" + str(num_frontier_groups)

    if current_partition_isLoop:
        current_partition_name += "L"

    if current_group_isLoop:
        current_group_name += "L"

    # can't add shadow nodes and associated node until all parents added via dfs_parent
    # Q: Can we do this as part of else and then finish the appends here?
    # I think that is what we are doing since all this is the appends of shadow nodes
    # and saving the frontier_node in the parent in different partition/group
    for parent_node_visited_tuple in already_visited_parents:
        visited_parent_node = parent_node_visited_tuple[0]
        index_of_parent = parent_node_visited_tuple[1]
        #where: parent_node_visited_tuple = (parent_node,index_of_parent) 
        partition_group_tuple = nodeIndex_to_partition_partitionIndex_group_groupIndex_map.get(visited_parent_node.ID)
        # this is also used in the else: part for debugging so declare here
        parent_partition_number = None
        parent_group_number = None
        if partition_group_tuple != None:
            parent_partition_number = partition_group_tuple[0]
            if parent_partition_number == -1 or parent_partition_number == current_partition_number:
                # parent is not in previous partition, i.e., node is not a child of
                # a parent node that was in previous partition. This means
                # parent is in this partition and it is either in the same 
                # group as node or it is in a different group, which was computed
                # previously.
                #
                # If parent is in the same group then we do not need shadow nodes;
                # otherwise, we need shadow_nodes just like the case in which the
                # parent is in a different partition, which is like saying that 
                # the parent is in a group of a different partition, but we presumably
                # are not tracking groups, just partitions.
                #
                # The parent is in a different group if: it has a different group
                # number and it's not -1. Either have to look in the global node to partition/group
                # map or have a group_number member of Node.

                logger.debug ("dfs_parent: parent in same partition: parent_partition_number: " 
                    + str(parent_partition_number) 
                    + ", current_partition_number:" + str(current_partition_number)
                    + ", parent ID: " + str(parent_index))

                # Check if this is a loop and parent partition/group number is -1. A loop is possible
                # since parent is in the same partition/group. If not a loop, then parent is in 
                # previous partition or group, and that is handled next.
                #pg_tuple = nodeIndex_to_partition_partitionIndex_group_groupIndex_map[parent_index]
                # parent has been mapped but the partition and group indices
                # might be -1. From above, for documentation, a node's
                # global map info is initialized at start of dfs_parent as:
                #partition_number = current_partition_number
                #partition_index = -1
                #group_number = current_group_number
                #group_index = -1
                parent_partition_index = partition_group_tuple[1]
                parent_group_parent_index = partition_group_tuple[3]

#rhc: case: visited parent before and it is is same partition so no shadow node.
# parent may be in a loop so check for loop and if parent indicates a loop
# then need to patch
                if parent_partition_index != -1:
                    # No need to patch the parent index. We will need a shadow node
                    # if the parent is in a different partition/group in whcih case
                    # we will make this partition_node / group_node's parent be
                    # the shadow node(s).
                    # assert group_index is also -1
                    partition_node.parents[index_of_parent] = parent_partition_index
                    group_node.parents[index_of_parent] = parent_group_parent_index
                else:
                    # need to patch the parent index
                    partition_node.parents[index_of_parent] = -1
                    group_node.parents[index_of_parent] = -1
                    # finish this partition_node and group_node parent ermapping 
                    # when the parent/group has finished and all parents hve been mapped.
                    patch_tuple = (parent_index,partition_node.parents,group_node.parents,index_of_parent,node.ID)
                    logger.debug("patch_tuple: " +str(patch_tuple))
                    patch_parent_mapping_for_partitions.append(patch_tuple)
                    patch_parent_mapping_for_groups.append(patch_tuple)

                    # Detected loop. When we compute pagerank for a group, the number
                    # of iterations for a loop-group is more than 1, while the number
                    # of iterations for a non-loop group is 1. The name for a group
                    # or partition with a loop ends with "L".
                    # Note: We now detect loops above when we generate the parent_node_visited_tuples
                    # so we can use the L-based partition/grou names for partitions/groups that
                    # have a loop. Here we just assert that the just detected loop should also
                    # have been detected earlier.

                    #logger.debug("dfs_parent set current_partition_isLoop to True.")
                    #global current_partition_isLoop
                    #current_partition_isLoop = True
                    # assert: we should have detected a loop above 
                    if current_partition_isLoop == False:
                        logger.error("[Error] Internal Error: detected partition loop when"
                            + " processing parent_node_visited_tuple that was not"
                            + " detected when generating parent_node_visited_tuple")

                """
                parent_partition_index = partition_group_tuple[1]
                partition_node.parents[index_of_parent] = parent_partition_index
                """

                #partition_group_tuple = nodeIndex_to_partition_partitionIndex_group_groupIndex_map.get(parent_node.ID)
                #parent_group_number = None
                # This must be true (leave this if for now)
                if partition_group_tuple != None:
                    # we have visited parent, but if visit was part of a loop then 
                    # the parent may not have been assigned to a partition or group
                    # yet. (the assignment is done when recursion bcks up to the parent.)
                    # If the group number is not the same as the current group
                    # then the parent is in a different group.
                    # Q: is there any other result possible? we've visited parent and it
                    # cant have been part of this group's parent traversal. Answer: For
                    # example, if this node and parent are parents of each other, then 
                    # the second parent to be visited will see the other as a parent 
                    # and that other will be in the global map with a group number of -1.
                    # In general, if this is part of a loop, we will cal dfs_parent on 
                    # a parent node P that was visited by dfs_parent(p) already and as
                    # part of traversing p's parents we will call dfs_parent(p) again. 
                    # P will be in the map and will have a partition_number and group_number
                    # of -1.
                    parent_group_number = partition_group_tuple[2]
                    if parent_group_number == -1 or parent_group_number == current_group_number:
                        logger.debug ("dfs_parent: parent in same group: parent_group_number: " 
                            + str(parent_group_number)
                            + ", current_group_number: " + str(current_group_number)
                            + ", parent ID: " + str(parent_index)) 

# rhc: case: visited parent before and it is is same group.  So no shadow node and 
# we already checked to see if parent indicates a loop.
                        # Note: The parent is in the same group and could indicate
                        # a loop; however, we already checked for this when we saw
                        # that the parent was in the same partition. (Note: parent in the 
                        # same group ==> parent in same partition. Also, if the parent 
                        # indicates there is a loop in the current group/partition, then
                        # this parent will also indicate there is a loop in the current
                        # partition/group.). If the checked showed a loop in the partition
                        # then we created a path tuple for the partition and group.

                        # Detected loop. When we compute pagerank for a group, the number
                        # of iterations for a loop-group is more than 1, while the number
                        # of iterations for a non-loop group is 1. The name for a group
                        # or partition with a loop ends with "L".

                        # Changed this to an assert. The loop should have also been
                        # detected above when we generated parent_node_visited_tuples.
                        # assert: already detectd loop
                        #global current_group_isLoop
                        #if parent_partition_index == -1:
                        #    current_group_isLoop = True
                        if current_group_isLoop == False:
                            logger.error("[Error] Internal Error: detected group loop when"
                                + " processing parent_node_visited_tuple that was not"
                                + " detected when generating parent_node_visited_tuple")
 
                        """
                        parent_group_parent_index = partition_group_tuple[3]
                        partition_node.parents[index_of_parent] = parent_group_parent_index
                        """

                    else:
                        logger.debug ("dfs_parent: parent in different group: parent_group_number: " 
                            + str(parent_group_number) 
                            + ", current_group_number: " + str(current_group_number)
                            + ", parent ID: " + str(parent_index))

                        # The name of the current partition/group depends on whether it
                        # has a loop. If so we add an 'L' to the end of the name.
                        # Example: "PR2_2" becomes "PR2_2L".
                        #current_group_name = "PR" + str(current_partition_number) + "_" + str(num_frontier_groups)
                        #if current_group_isLoop:
                        #    current_group_name += "L"

                        #parents_in_previous_group = True
                        list_of_parents_in_previous_group.append(visited_parent_node.ID) 

                        #logger.debug ("dfs_parent: found parent in previous group: " + str(parent_node.ID))
                        # index of child just added (we just visited it because it ws an 
                        # unvisited child) to partition
                        child_index_in_current_group = len(current_group)
                        # shadow node is a parent Node on frontier of previous partition
                        #shadow_node = Node(parent_node.ID)
                        shadow_node = Partition_Node(visited_parent_node.ID)
                        shadow_node.isShadowNode = True
                        shadow_node.num_children = len(visited_parent_node.children)
                        # this will possibly be overwritten; the parent may be a
                        # node after the end of the partiton with a pageran value
                        # that keeps he shadow_node's pagerank value constant.
                        shadow_node.parents.append(-1)
                        # insert shadow_node before child (so only shift one)
                        #current_partition.insert(child_index,shadow_node)
                        logger.debug("dfs_parent: add shadow node to group: " + str(visited_parent_node.ID) + "-s")

    #rhc: ToDo:
                        # only do part/group if using part/group or option to do both
                        # for debugging? No, if in different group but same partition 
                        # then no shadow node in partition as no need to send pr values
                        # to same partition,
                        #current_partition.append(shadow_node)
                        current_group.append(shadow_node)
# rhc: case: visited parent before and it is is same partition so set the parent
# at index index_of_parent to parent_partition_index = partition_group_tuple[0]
                        """
                        parent_group_parent_index = len(current_group)-1
                        partition_node.parents[index_of_parent] = parent_group_parent_index
                        """

    #rhc: ToDo:
                        # only do part/group if using part/group or option to do both
                        # for debugging? No, see above.
                        #nodeIndex_to_partitionIndex_map[shadow_node.ID] = len(current_partition)-1

                        nodeIndex_to_groupIndex_map[shadow_node.ID] = len(current_group)-1
                        # Note: We do not add shadow_node to the 
                        # X map. But shadow_node IDs should be mapped to their positions
                        # when we are computing the group since f the shadow node
                        # is a parent of node n then n.parents are remapped to their 
                        # position in the group and one of n's parents will be the shadow
                        # node so we need its position in the group.

                        #rhc: make group node's parent be this shadow node
                        group_node.parents[index_of_parent] = len(current_group)-1
                    
                        global shadow_nodes_added_to_groups
                        shadow_nodes_added_to_groups += 1

                        # remember where the frontier_parent node should be placed when the 
                        # partition the PageRank task sends it to receives it. 
                        logger.debug ("frontier_groups: " + str(num_frontier_groups) + ", child_index: " + str(child_index_in_current_group))

                        
                        #d1 = child_index-dfs_parent_start_partition_size
                        #logger.debug("ZZZZZZZZZZZZZZZZZZZZZZZZZ child_index: " + str(child_index) + " d1: " + str(d1))
                        #if child_index != d1:
                        #    logger.debug("ZZZZZZZZZZZZZZZZZZZZZZZZZ Difference: " 
                        #       + " child_index: " + str(child_index) + " d1: " + str(d1))
                        #else:
                        #   logger.debug("ZZZZZZZZZZZZZZZZZZZZZZZZZ No Difference: ") 
                        
                        #logger.debug("ZZZZZZZZZZZ")

                        # Note: Added a partition/group name field to the tuple since we need an 'L'
                        # in the name of the current partition/group if it is a loop. We probably won't
                        # need the current_partition_number/num_frontier_groups but it's available for now for debugging.
                        frontier_parent_tuple = (current_partition_number,num_frontier_groups,child_index_in_current_group,current_group_name)
                        logger.debug ("bfs frontier_parent_tuple: " + str(frontier_parent_tuple))

                        # mark this node as one that PageRank needs to send in its output to the 
                        # next partition (via fanout/faninNB).That is, the fact that list
                        # frontier_parent is not empty indicates it needs to be sent in the 
                        # PageRank output. The tuple indictes which frontier group it should 
                        # be sent to. PageRank may send frontier_parent nodes to mulltiple groups
                        # of multiple partitions
                        #
                        # need to use the current partition, not nodes as the current
                        # partition is what the functions will be using to compute pr
                        # nodes[parent_node.ID].frontier_parents.append(frontier_parent_tuple)
                        partition_group_tuple = nodeIndex_to_partition_partitionIndex_group_groupIndex_map[visited_parent_node.ID]
                        parent_group_number = partition_group_tuple[2]
                        parent_group_parent_index = partition_group_tuple[3]
                        index_in_groups_list = partition_group_tuple[4]
                        # parent_group_number is the number of the group in the 
                        # current partition. We are working on the current group,
                        # which is the next group to be added to groups. The 
                        # current group number is num_frontier_groups and it 
                        # will be added at position len(groups) in groups. Note that group 
                        # numbers in a partition start at 1 not 0. The parent group position
                        # in groups is before that, i.e., len(groups) - i. What is i?
                        # Note: we are working back from the end of the groups list 
                        # to find the parent position.
                        # The current group is num_frontier_groups. The group of the parent
                        # (in this partition) is parent_group_number, which is 
                        # less than num_frontier_groups. If current group = num_frontier_groups
                        # is 2, and parent group is 1, then we want the group at 
                        # len(groups) - (current_group-parent_group). Example, if len(groups)
                        # is 2, the 2 existing groups, groups (1 and 2) are in positions [0] 
                        # and [1]. The current group will be the third group and will be added 
                        # at position [2]. Since len(groups) is 2, and (current_group-parent_group) 
                        # is (2-1) = 1, then the parent group we want is at groups[2-1], which is groups[1].
                        parent_group_position = len(groups) - (num_frontier_groups-parent_group_number)
                        # asssert
                        if not index_in_groups_list == parent_group_position:
                            logger.error("[Error]: Internal Error: dfs_parent: for parent " + str(parent_index)
                                + " index_in_groups_list != parent_group_position"
                                + " index_in_groups_list: " + str(index_in_groups_list)
                                + " parent_group_position: " + str(parent_group_position))

                        parent_group = groups[index_in_groups_list]

                        logger.debug("groupOOOOOOOOOOOOOOO add tuple to parent group: ")
                        for n in parent_group:
                            logger.debug(str(n))
                        logger.debug("len(groups): " + str(len(groups)) + ", parent_group_number: " + str(parent_group_number)
                            + ", num_frontier_groups: " + str(num_frontier_groups) 
                            + ", index_in_groups_list: " + str(index_in_groups_list)
                            + ", parent_group_parent_index: " + str(parent_group_parent_index)
                            + ", frontier_parent_tuple: " + str(frontier_parent_tuple))
                        parent_group[parent_group_parent_index].frontier_parents.append(frontier_parent_tuple)
                        logger.debug("parent_group[parent_group_parent_index].ID: " + str(parent_group[parent_group_parent_index].ID))
                        logger.debug("frontier tuples:")
                        for t in  parent_group[parent_group_parent_index].frontier_parents:
                            logger.debug(str(t))
                        if not current_group_isLoop:
                            position_in_frontier_parents_group_list = len(parent_group[parent_group_parent_index].frontier_parents)-1
                            frontier_parent_group_patch_tuple = (index_in_groups_list,parent_group_parent_index,position_in_frontier_parents_group_list)
                            frontier_parent_group_patch_tuple_list.append(frontier_parent_group_patch_tuple)
       
                        # generate dependency in DAG
                        #sending_group = "PR"+str(parent_partition_number)+"_"+str(parent_group_number)
                        # index in groups list is the actual index, starting with index 0
                        sending_group = group_names[index_in_groups_list]
                        receiving_group = current_group_name
                        #sending_group = "PR"+str(parent_partition_number)+"_"+str(parent_group_number)
                        #receiving_group = "PR"+str(current_partition_number)+"_"+str(num_frontier_groups)
                        sender_set = Group_senders.get(sending_group)
                        if sender_set == None:
                            Group_senders[sending_group] = set()
                        Group_senders[sending_group].add(receiving_group)
                        receiver_set = Group_receivers.get(receiving_group)
                        if receiver_set == None:
                            Group_receivers[receiving_group] = set()
                        Group_receivers[receiving_group].add(sending_group)

                        if not current_group_isLoop:
                            sender_receiver_group_patch_tuple = (index_in_groups_list,receiving_group)
                            sender_receiver_group_patch_tuple_list.append(sender_receiver_group_patch_tuple)

                else:
                    logger.error("[Error] Internal Error. dfs_parent: partition_group_tuple " 
                        + "is None should be unreachable.")
                    # if there's no entry in the global map then we have not visited the
                    # parent yet so it's not in the same group.
                    # Note that this check is before the call to dfs_parent(parent_index).
                    # If it were after, then the partition_group_tuple could not be None
                    # since we add parent to the global map at the start of dfs_parent()
                    logger.debug ("dfs_parent: parent in same group: parent_group_number: " 
                        + str(parent_group_number)
                        + ", parent ID: " + str(parent_index))
                # we haven't seen parent parent_node yet so it is not in a previous group.
                # For example, root 1's parent is 17 so we call dfs_parent(17) and 17 will
                # be in the sme group. Here, we have not seen 17 yet so it is not in
                # nodeIndex_to_partition_partitionIndex_group_groupIndex_map. Noet that 1
                # will be aded to nodeIndex_to_partition_partitionIndex_group_groupIndex_map
                # at start of dfs_parent then 1 does this check on its parent 17
                # before calling dfs_parent(17).

                # part of SCC computation
                #parent_GraphID = scc_graph.map_nodeID_to_GraphID(parent_index)
                # add edge from parent to node
                #scc_graph.addEdge(parent_GraphID, node_GraphID)
                #logger.debug ("dfs_parent add (unmapped) edge: " + str(parent_index) + "," + str(node.ID))
                #logger.debug ("dfs_parent add (mapped) edge: " + str(parent_GraphID) + "," + str(node_GraphID))
                #logger.debug("dfs_parent: Graph after add edge:")
                #scc_graph.logger.infoEdges()
                #global scc_num_vertices
                #scc_num_vertices += 1

            else:
                #parent is in different/previous partition, (must be current_partition - 1)
                logger.debug ("dfs_parent: parent in different partition: parent_partition_number: " 
                    + str(parent_partition_number) 
                    + ", current_partition_number:" + str(current_partition_number)
                    + ", parent ID: " + str(parent_index))

                # The name of the current partition/group depends on whether it
                # has a loop. If so we add an 'L' to the end of the name.
                # Example: "PR2_2" becomes "PR2_2L".
                #current_partition_name = "PR" + str(current_partition_number) + "_1"
                #current_group_name = "PR" + str(current_partition_number) + "_" + str(num_frontier_groups)
                #if current_partition_isLoop:
                #    current_partition_name += "L"
                #if current_group_isLoop:
                #    current_group_name += "L"

                #parents_in_previous_partition = True
                list_of_parents_in_previous_partition.append(visited_parent_node.ID)

                # take care of this now
                # index of child just added (we just visited it because it ws an 
                # unvisited child) to partition
                child_index_in_current_partition = len(current_partition)
                # shadow node is a parent Node on frontier of previous partition
                #shadow_node = Node(parent_node.ID)
                shadow_node = Partition_Node(visited_parent_node.ID)
                shadow_node.isShadowNode = True
                shadow_node.num_children = len(visited_parent_node.children)
                # this will possibly be overwritten; the parent may be a
                # node after the end of the partiton with a pageran value
                # that keeps he shadow_node's pagerank value constant.
                shadow_node.parents.append(-1)
                # insert shadow_node before child (so only shift one)
                #current_partition.insert(child_index,shadow_node)

                current_partition.append(shadow_node)
# rhc: case: visited parent before and it is is same partition so set the parent
# at index index_of_parent to parent_partition_index = partition_group_tuple[0]
                """
                parent_partition_index = len(current_partition)-1
                partition_node.parents[index_of_parent] = parent_partition_index
                """

#rhc: ToDo:
                # only do part/group if using part/group or option to do both
                # for debugging? If in differet partition then if using parts then
                # add to part and if using group then add to group and if using 
                # both then add to both.  
                # wait: but add tuple to node in partition if using partitions 
                # and group if using groups. So do both for now? Does tuple
                # work for both partitions and groups? Just ignore group
                # number if using partitions? (when forming function names)
                # Or just use group number of 0 when using partitions?

                child_index_in_current_group = len(current_group)
                current_group.append(copy.deepcopy(shadow_node))
                logger.debug("dfs_parent: add shadow node to group: " + str(visited_parent_node.ID) + "-s")
# rhc: case: visited parent before and it is is same partition so set the parent
# at index index_of_parent to parent_partition_index = partition_group_tuple[0]
                """
                parent_group_parent_index = len(current_group)-1
                partition_node.parents[index_of_parent] = parent_group_parent_index
                """

                global nodeIndex_to_partitionIndex_map
                #global nodeIndex_to_groupIndex_map
                nodeIndex_to_partitionIndex_map[shadow_node.ID] = len(current_partition)-1
#rhc: ToDo:
                # only do part/group if using part/group or option to do both
                # for debugging?
                nodeIndex_to_groupIndex_map[shadow_node.ID] = len(current_group)-1
                # Note: We do not add shadow_node to the 
                # X map. But shadw_node IDs should be mapped to their positions
                # when we are computing the group since f the shadow node
                # is a parent of node n then n.parents are remapped to their 
                # position in the group and one of n's parents will be the shadow
                # node so we need its position in the group.

                #rhc: make group node's parent be this shadow node
                partition_node.parents[index_of_parent] = len(current_partition)-1
                group_node.parents[index_of_parent] = len(current_group)-1
            
                global shadow_nodes_added_to_partitions
                #global shadow_nodes_added_to_groups
                shadow_nodes_added_to_partitions += 1
                shadow_nodes_added_to_groups += 1

                # remember where the frontier_parent node should be placed when the 
                # partition the PageRank task sends it to receives it. 
                logger.debug ("num partitions: " + str(current_partition_number) + ", child_index_in_current_partition: " + str(child_index_in_current_partition))
                logger.debug ("num_frontier_groups: " + str(num_frontier_groups) + ", child_index_in_current_group: " + str(child_index_in_current_group))
# rhc: ToDo: if we are using partition then we just need partition number and index
# but we won't use group number? That is, the names aer PR1, PR2, etc, so we ignore'
# the group number when we form partition name for target funtion with shadow nodes?
# Rather: just use group node of 0 when using partitions, so PR1_0, PR2_0,...
                # Note : For partitions, the child_index is the index relatve to the 
                # start of the partition. child_index is len(current_partition).
                # The calculation for groups (below) is a bit difference.
                # Q: Use 0 instead of num_frontier_groups so we can just grab the 0.
                #
                # Note: Added a partition/group name field to the tuple since we need an 'L'
                # in the name of the current partition/group if it is a loop. We probably won't
                # need the current_partition_number/num_frontier_groups but it's available for now for debugging.
                frontier_parent_partition_tuple = (current_partition_number,1,child_index_in_current_partition,current_partition_name)
                frontier_parent_group_tuple = (current_partition_number,num_frontier_groups,child_index_in_current_group,current_group_name)
                logger.debug ("bfs frontier_parent_partition_tuple: " + str(frontier_parent_partition_tuple))
                logger.debug ("bfs frontier_parent_group_tuple: " + str(frontier_parent_group_tuple))
 
                # mark this node as one that PageRank needs to send in its output to the 
                # next partition (via fanout/faninNB).That is, the fact that list
                # frontier_parent is not empty indicates it needs to be sent in the 
                # PageRank output. The tuple indictes which frontier group it should 
                # be sent to. PageRank may send frontier_parent nodes to mulltiple groups
                # of multiple partitions
                #
                # need to use the current partition, not nodes as the current
                # partition is what the functions will be using to compute pr
                # nodes[parent_node.ID].frontier_parents.append(frontier_parent_tuple)
                logger.debug ("visited_parent_node.ID " + str(visited_parent_node.ID))
                partition_group_tuple = nodeIndex_to_partition_partitionIndex_group_groupIndex_map[visited_parent_node.ID]
                parent_partition_number = partition_group_tuple[0]
                parent_partition_parent_index = partition_group_tuple[1]
                parent_group_number = partition_group_tuple[2]
                parent_group_parent_index = partition_group_tuple[3]
                        

                # cannot use parent_group_number to index groups; parent_group_number
                # is a number within a partition, e.g., PR2_2 has a group index of 2,
                # but this is not necessarily the 2nd group overall.
                index_in_groups_list = partition_group_tuple[4]
                logger.debug ("partition_group_tuple " + str(partition_group_tuple))
                # partition numbers start at 1 not 0
                parent_partition = partitions[parent_partition_number-1]
                parent_partition[parent_partition_parent_index].frontier_parents.append(frontier_parent_partition_tuple)
                logger.debug ("partitionOOOOOOOOOOOOOOOOOOOOO add frontier tuple to parent group ")
                #parent_group = groups[parent_group_number-1]
                parent_group = groups[index_in_groups_list]
                parent_group[parent_group_parent_index].frontier_parents.append(frontier_parent_group_tuple)
                # It's possible that even though we have not seen a loop yet in this partition,
                # we will. At that point current_partition_isLoop will be set to true and the 
                # current_partition_name will become an L-name, i.e., it will have an 'L'
                # at the end. That means the frontier parent tuples created up to that point
                # were using the wrong name and need to be "patched", i.e., corrected. So we
                # save all the frontier tuples that are created with (not current_partition_name)
                # so that when the partition ends, if we find current_partition_name is True we
                # can iterate through this list and make the changes. If no loop is dected then 
                # no changes need to be made.
                if not current_partition_isLoop:
                    position_in_frontier_parents_partition_list = len(parent_partition[parent_partition_index].frontier_parents)-1
                    frontier_parent_partition_patch_tuple = (parent_partition_number,parent_partition_parent_index,position_in_frontier_parents_partition_list)
                    frontier_parent_partition_patch_tuple_list.append(frontier_parent_partition_patch_tuple)

                # Note: in white board group 2_2, when 20 sees 2 it detects no loop
                # and then it sees 19 and detects a loop, so 20 uses "PR2_2L" as
                # the name of its group.
                if not current_group_isLoop:
                    position_in_frontier_parents_group_list = len(parent_group[parent_group_parent_index].frontier_parents)-1
                    frontier_parent_group_patch_tuple = (index_in_groups_list,parent_group_parent_index,position_in_frontier_parents_group_list)
                    frontier_parent_group_patch_tuple_list.append(frontier_parent_group_patch_tuple)

                # generate dependency in DAG
                #
                # Need to use L-based names. The recever is the name of the 
                # current partition/group. The sender's name is the name
                # assigned when the dfs_parent() for that partition/group completed.
                #sending_partition = "PR"+str(parent_partition_number)+"_1"
                # parent_partition_numbers start with 1, e.g. the "PR1" in "PR1_1"
                # but the partition_names are a list with the fitrst name at position 0
                sending_partition = partition_names[parent_partition_number-1]
                #receiving_partition = "PR"+str(current_partition_number)+"_1"
                receiving_partition = current_partition_name
                sender_set = Partition_senders.get(sending_partition)
                if sender_set == None:
                    Partition_senders[sending_partition] = set()
                Partition_senders[sending_partition].add(receiving_partition)
                receiver_set = Partition_receivers.get(receiving_partition)
                if receiver_set == None:
                    Partition_receivers[receiving_partition] = set()
                Partition_receivers[receiving_partition].add(sending_partition)
                # It's possible that even though we have not seen a loop yet in this partition,
                # we will. At that point current_partition_isLoop will be set to true and the 
                # current_partition_name will become an L-name, i.e., it will have an 'L'
                # at the end. That means the sender/receiver names used up to that point
                # were using the wrong name and need to be "patched", i.e., corrected. So we
                # save information about the senders/receivers that were created with (not current_partition_name)
                # so that when the partition ends, if we find current_partition_name is True we
                # can iterate through this list and make the changes to the sender/receiver names.
                # If no loop is dected then no changes need to be made.
                if not current_partition_isLoop:
                    sender_receiver_partition_patch_tuple = (parent_partition_number,receiving_partition)
                    sender_receiver_partition_patch_tuple_list.append(sender_receiver_partition_patch_tuple)

                # generate dependency in DAG
                #sending_group = "PR"+str(parent_partition_number)+"_"+str(parent_group_number)
                # index in groups list is the actual index, sarting with index 0
                sending_group = group_names[index_in_groups_list]
                #receiving_group = "PR"+str(current_partition_number)+"_"+str(num_frontier_groups)
                receiving_group = current_group_name
                sender_set = Group_senders.get(sending_group)
                if sender_set == None:
                    Group_senders[sending_group] = set()
                Group_senders[sending_group].add(receiving_group)
                receiver_set = Group_receivers.get(receiving_group)
                if receiver_set == None:
                    Group_receivers[receiving_group] = set()
                Group_receivers[receiving_group].add(sending_group)

                if not current_group_isLoop:
                    sender_receiver_group_patch_tuple = (index_in_groups_list,receiving_group)
                    sender_receiver_group_patch_tuple_list.append(sender_receiver_group_patch_tuple)
 

        else:
            logger.error("[Error] Internal Error. dfs_parent: partition_group_tuple" 
                + " of parent is None should be unreachable since this is after calling "
                + " dfs_parent() on the parent.")
            # if there's no entry in the global map then we have not visited the
            # parent before so it's not in a different/previous partition.
            # Note that this check is before the call to dfs_parent(parent_index).
            # If it were after, then the partition_group_tuple could not be None
            # since we add parent to the global map at the start of dfs_parent()
            logger.debug ("dfs_parent: parent in same partition/group: parent_partition_number:" 
                + " parent_partition_number: " + str(parent_partition_number)
                + " parent_group_number: " + str(parent_group_number)
                + ", parent ID: " + str(parent_index))

    if CHECK_UNVISITED_CHILDREN:
        # process children after parent traversal
        dfs_parent_post_parent_traversal(node, visited,
            list_of_unvisited_children, check_list_of_unvisited_chldren_after_visiting_parents)
    else:
        queue.append(node.ID)
        #queue.append(-1)
        if DEBUG_ON:
            print_val = "queue after add " + str(node.ID) + ": "
            for x in queue:
                #logger.debug(x.ID, end=" ")
                print_val = print_val + str(x) + " "
            logger.debug(print_val)
            logger.debug("")
        #frontier.append(node)
        frontier.append(node.ID)
        if DEBUG_ON:
            print_val = "frontier after add " + str(node.ID) + ":"
            for x in frontier:
                #logger.debug(x.ID, end=" ")
                #logger.info(x, end=" ")
                print_val = print_val + str(x) + " "
            logger.debug(print_val)
            logger.debug("")
        # make sure parent in partition before any if its children. We visit parents of nodein dfs_parents 
        # and they are added to partition in dfs_parents after their parents are added 
        # in dfs_parents then here we add node to partition.  

#rhc: Can this be false? can we dfs_parent visit a node that has already been visited
# and that already has been put in a partition?

        if node.partition_number == -1:
            logger.debug ("dfs_parent add " + str(node.ID) + " to partition")
            node.partition_number = current_partition_number
            logger.debug("set " + str(node.ID) + " partition number to " + str(node.partition_number))
            #current_partition.append(node.ID)
            #rhc: append node 

            """
            ### moved this up inside parent loop.

            if parents_in_previous_partition:
                # Note: May be adding multiple shadow nodes before partition_node
                for parent_node in list_of_parents_in_previous_partition:
                    # index of child just added (we just visited it because it ws an 
                    # unvisited child) to partition
                    child_index = len(current_partition)
                    # shadow node is a parent Node on frontier of previous partition
                    #shadow_node = Node(parent_node.ID)
                    shadow_node = Partition_Node(parent_node.ID)
                    shadow_node.isShadowNode = True
                    # insert shadow_node before child (so only shift one)
                    #current_partition.insert(child_index,shadow_node)

                    current_partition.append(shadow_node)
#rhc: ToDo:
                    # only do part/group if using part/group or option to do both
                    # for debugging? If in differet partition then if using parts then
                    # add to part and if using group then add to group and if using 
                    # both then add to both.  
                    # wait: but add tuple to node in partition if using partitions 
                    # and group if using groups. So do both for now? Does tuple
                    # work for both partitions and groups? Just ignore group
                    # number if using partitions? (when forming function names)
                    # Or just use group number of 0 when using partitions?
                    current_group.append(copy.deepcopy(shadow_node))

                    global nodeIndex_to_partitionIndex_map
                    global nodeIndex_to_groupIndex_map
                    nodeIndex_to_partitionIndex_map[shadow_node.ID] = len(current_partition)-1
#rhc: ToDo:
                    # only do part/group if using part/group or option to do both
                    # for debugging?
                    nodeIndex_to_groupIndex_map[shadow_node.ID] = len(current_group)-1
                    # Note: We do not add shadow_node to the 
                    # X map. But shadw_node IDs should be mapped to their positions
                    # when we are computing the group since f the shadow node
                    # is a parent of node n then n.parents are remapped to their 
                    # position in the group and one of n's parents will be the shadow
                    # node so we need its position in the group.
                  
                    global shadow_nodes_added_to_partitions
                    global shadow_nodes_added_to_groups
                    shadow_nodes_added_to_partitions += 1
                    shadow_nodes_added_to_groups += 1

                    # remember where the frontier_parent node should be placed when the 
                    # partition the PageRank task sends it to receives it. 
                    logger.debug ("num_frontier_groups: " + str(num_frontier_groups) + ", child_index: " + str(child_index))
# rhc: ToDo: if we are using partition then we just need partition number and index
# but we won't use group number? That is, the names aer PR1, PR2, etc, so we ignore'
# the group number when we form partition name for target funtion with shadow nodes?
# Rather: just use group node of 0 when using partitions, so PR1_0, PR2_0,...
                    # Note : For partitions, the child_index is the index relatve to the 
                    # start of the partition. child_index is len(current_partition).
                    # The calculation for groups (below) is a bit difference.
                    # Q: Use 0 instead of num_frontier_groups so we can just grab the 0
                    frontier_parent_tuple = (current_partition_number,0,child_index)
                    logger.debug ("bfs frontier_parent_tuple: " + str(frontier_parent_tuple))
                    # mark this node as one that PageRank needs to send in its output to the 
                    # next partition (via fanout/faninNB).That is, the fact that list
                    # frontier_parent is not empty indicates it needs to be sent in the 
                    # PageRank output. The tuple indictes which frontier group it should 
                    # be sent to. PageRank may send frontier_parent nodes to mulltiple groups
                    # of multiple partitions
                    #
                    # need to use the current partition, not nodes as the current
                    # partition is what the functions will be using to compute pr
                    # nodes[parent_node.ID].frontier_parents.append(frontier_parent_tuple)
                    partition_group_tuple = nodeIndex_to_partition_partitionIndex_group_groupIndex_map[parent_node.ID]
                    parent_partition_number = partition_group_tuple[0]
                    parent_partition_index = partition_group_tuple[1]
                    # partition numbers start at 1 not 0
                    parent_partition = partitions[parent_partition_number-1]
                    parent_partition[parent_partition_index].frontier_parents.append(frontier_parent_tuple)
            """

#rhc: Set group tuples but not partition since not sending pr values if nodes
# are in different groups but the same partitions
            """
            ### moved this up inside parent loop.

            if parents_in_previous_group:
                for parent_node in list_of_parents_in_previous_group:
                    
                    ### moved this up inside parent loop.

                    logger.debug ("dfs_parent: found parent in previous group: " + str(parent_node.ID))
                    # index of child just added (we just visited it because it ws an 
                    # unvisited child) to partition
                    child_index = len(current_group)
                    # shadow node is a parent Node on frontier of previous partition
                    #shadow_node = Node(parent_node.ID)
                    shadow_node = Partition_Node(parent_node.ID)
                    shadow_node.isShadowNode = True
                    # insert shadow_node before child (so only shift one)
                    #current_partition.insert(child_index,shadow_node)

#rhc: ToDo:
                    # only do part/group if using part/group or option to do both
                    # for debugging? No, if in different group but same partition 
                    # then no shadow node in partition as no need to send pr values
                    # to same partition,
                    #current_partition.append(shadow_node)
                    current_group.append(shadow_node)

#rhc: ToDo:
                    # only do part/group if using part/group or option to do both
                    # for debugging? No, see above.
                    #nodeIndex_to_partitionIndex_map[shadow_node.ID] = len(current_partition)-1

                    nodeIndex_to_groupIndex_map[shadow_node.ID] = len(current_group)-1
                    # Note: We do not add shadow_node to the 
                    # X map. But shadw_node IDs should be mapped to their positions
                    # when we are computing the group since f the shadow node
                    # is a parent of node n then n.parents are remapped to their 
                    # position in the group and one of n's parents will be the shadow
                    # node so we need its position in the group.
                  
                    #global shadow_nodes_added_to_groups
                    shadow_nodes_added_to_groups += 1

                    # remember where the frontier_parent node should be placed when the 
                    # partition the PageRank task sends it to receives it. 
                    logger.debug ("frontier_groups: " + str(num_frontier_groups) + ", child_index: " + str(child_index))

                    
                    #d1 = child_index-dfs_parent_start_partition_size
                    #logger.debug("ZZZZZZZZZZZZZZZZZZZZZZZZZ child_index: " + str(child_index) + " d1: " + str(d1))
                    #if child_index != d1:
                    #    logger.debug("ZZZZZZZZZZZZZZZZZZZZZZZZZ Difference: " 
                    #       + " child_index: " + str(child_index) + " d1: " + str(d1))
                    #else:
                    #   logger.debug("ZZZZZZZZZZZZZZZZZZZZZZZZZ No Difference: ") 
                    
                    #logger.debug("ZZZZZZZZZZZ")
                    frontier_parent_tuple = (current_partition_number,num_frontier_groups,child_index)
                    logger.debug ("bfs frontier_parent_tuple: " + str(frontier_parent_tuple))
                    # mark this node as one that PageRank needs to send in its output to the 
                    # next partition (via fanout/faninNB).That is, the fact that list
                    # frontier_parent is not empty indicates it needs to be sent in the 
                    # PageRank output. The tuple indictes which frontier group it should 
                    # be sent to. PageRank may send frontier_parent nodes to mulltiple groups
                    # of multiple partitions
                    #
                    # need to use the current partition, not nodes as the current
                    # partition is what the functions will be using to compute pr
                    # nodes[parent_node.ID].frontier_parents.append(frontier_parent_tuple)
                    partition_group_tuple = nodeIndex_to_partition_partitionIndex_group_groupIndex_map[parent_node.ID]
                    parent_group_number = partition_group_tuple[2]
                    parent_group_parent_index = partition_group_tuple[3]
                    # parent_group_number is the number of the group in the 
                    # current partition. We are working on the current group,
                    # which is the next group to be added to groups. The 
                    # current group number is num_frontier_groups and it 
                    # will be added at position len(groups) in groups. Note that group 
                    # numbers in a partition start at 1 not 0. The parent group position
                    # in groups is before that, i.e., len(groups) - i. What is i?
                    # Note: we are working back from the end of the groups list 
                    # to find the parent position.
                    # The current group is num_frontier_groups. The group of the parent
                    # (in this partition) is parent_group_number, which is 
                    # less than num_frontier_groups. If current group = num_frontier_groups
                    # is 2, and parent group is 1, then we want the group at 
                    # len(groups) - (current_group-parent_group). Example, if len(groups)
                    # is 2, the 2 existing groups, groups (1 and 2) are in positions [0] 
                    # and [1]. The current group will be the third group and will be added 
                    # at position [2]. Since len(groups) is 2, and (current_group-parent_group) 
                    # is (2-1) = 1, then the parent group we want is at groups[2-1], which is groups[1].
                    parent_group_position = len(groups) - (num_frontier_groups-parent_group_number)
                    parent_group = groups[parent_group_position]
                    logger.debug("add tuple to parent group " 
                        + "len(groups): " + str(len(groups)) + ", parent_group_number: " + str(parent_group_number)
                        + ", num_frontier_groups: " + str(num_frontier_groups) 
                        + ", parent_group_position: " + str(parent_group_position)
                        + ", parent_group_parent_index: " + str(parent_group_parent_index)
                        + ", frontier_parent_tuple: " + str(frontier_parent_tuple))
                    parent_group[parent_group_parent_index].frontier_parents.append(frontier_parent_tuple)

            """
            #partition_node = Partition_Node(node.ID)
            #partition_node.ID = node.ID
            #group_node = Partition_Node(node.ID)
            #group_node.ID = node.ID


#rhc: Todo: Can we do this as part of for each parent loop? instead of looping again?
# Note: parent remaps to different index depending on partiton or group!!
#
# Cases for remappng parent:
# parent is in same partition or group: 
# - partition/group number is -1: then we need to patch so append -1
#   to parents[] and create and save path tuple. In this case, there
#   are no shadow nodes (plural) and the parent is part of our loop
#   in this partition/group so patch will be the index of parent.
# - partition/group number is not -1, then we do not need to patch,
#   but the parent may be in this partition/group or not.
#   - parent is in this partition/group: no shadow nodes so we
#     can set parents[] to parent's index
#   - parent is not in this partition/group: we will push
#     a shadow node in front of this partition_node so the parent 
#     index should be the shadow node's index ot the actul parent,
#     Note: this node may have multiple parents in a different
#     partition/group and we will push a shadow node for each of
#     these parents *before* we push the partition node. The 
#     partition node's parents should include all of these shadow
#     nodes.
#rhc: handle multiple shadow nodes? Note that we do not push the 
#     partition/group node until after we process all the parents
#     So?
#   
# so we can 
#   append partition/group index to parents[]

            """
            i = 0
            for parent_index in node.parents:
                # new_index = nodeIndex_to_partitionIndex_map.get(parent)
                pg_tuple = nodeIndex_to_partition_partitionIndex_group_groupIndex_map[parent_index]
                # parent has been mapped but the partition and group indices
                # might be -1. From above:
                #partition_number = current_partition_number
                #partition_index = -1
                #group_number = current_group_number
                #group_index = -1
                parent_partition_index = pg_tuple[1]
                parent_group_parent_index = pg_tuple[3]
                if parent_partition_index != -1:
                    # assert group_index is also -1
                    partition_node.parents.append(parent_partition_index)
                    group_node.parents.append(parent_group_parent_index)
                else:
                    partition_node.parents.append(-1)
                    group_node.parents.append(-1)
                    # finish this partition_node and group_node parent ermapping 
                    # when the parent/group has finished and all parents hve been mapped.
                    patch_tuple = (parent_index,partition_node.parents,group_node.parents,i,node.ID)
                    logger.debug("YYYYYYYYY patch_tuple: " +str(patch_tuple))
                    patch_parent_mapping_for_partitions.append(patch_tuple)
                    patch_parent_mapping_for_groups.append(patch_tuple)
                i += 1
            """

            #partition_node.parents = node.parents
            
            partition_node.num_children = len(node.children)
            # these are the default values so we do not need these assignments 
            partition_node.pagerank = 0.0
            partition_node.isShadowNode = False
            partition_node.frontier_parents = []

            group_node.num_children = len(node.children)
            # these are the default values so we do not need these assignments 
            group_node.pagerank = 0.0
            group_node.isShadowNode = False
            group_node.frontier_parents = []

            #current_partition.append(node)
            #current_group.append(node)
            current_partition.append(partition_node)
            #current_group.append(copy.deepcopy(partition_node))
            current_group.append(group_node)

            # partition_node.ID and group_node.ID are the same
            nodeIndex_to_partitionIndex_map[partition_node.ID] = len(current_partition)-1
            nodeIndex_to_groupIndex_map[partition_node.ID] = len(current_group)-1
            
            # Note: if node's parent is in different partition then we'll add a 
            # shadow_node to the partition and the group in a position right before 
            # node in partition and group. But if a node's parent is in a different 
            # group but same partition then we only add a shadow node to the group
            # in a position right before node in the group. 

            # information for this partition node
            # There are n partitions, this node is in partition partition_number
            partition_number = current_partition_number
            # In this partition, this node is at position partition_index
            partition_index = len(current_partition)-1
            # Likewise for groups
            group_number = current_group_number
            group_index = len(current_group)-1
#rhc ToDo: We need postion in groups (frontier_groups-1) when we add a frontier tuple
# to a group. That is, if say 13 is a child of 8, we need to add a frontier_tuple
# to 8.  group_number is the number of the group in a partition, e.g., 8 might be
# in group 2 in partition 2, but any partition can have a group 2, so we cannot 
# use the group number 2 to access this group 2 of 8 in the groups list. We need
# to know group 2's position in the groups list, which could be anything.
# So for 8 we will save it's partition number and its partition index, where
# partition number i is always in position i-1 of the partitions list. We also
# save 8's group number and group index, and 8's position in groups so we can 
# get 8's group from the groups list when we need it.
            index_in_groups_list = frontier_groups_sum-1
            pg_tuple = (partition_number,partition_index,group_number,group_index,index_in_groups_list)
            nodeIndex_to_partition_partitionIndex_group_groupIndex_map[partition_node.ID] = pg_tuple
            logger.debug("HHHHHHHHHHHHHHHH dfs_parent: pg_tuple generate for " + str(partition_node.ID)
                + str(pg_tuple))
        else:
            logger.debug ("dfs_parent do not add " + str(node.ID) + " to partition "
                + current_partition_number + " since it is already in partition " 
                + node.partition_number)

# process children after parent traversal
def dfs_parent_post_parent_traversal(node, visited, list_of_unvisited_children, check_list_of_unvisited_chldren_after_visiting_parents):
    # See if the unvisited children found above are still unvisited
    unvisited_children_after_parent_loop = []
    if check_list_of_unvisited_chldren_after_visiting_parents:
        for child_index in list_of_unvisited_children:
            logger.debug("check unvisited child " + str(child_index))
            child_node = nodes[child_index]
            if child_node.ID not in visited:
                logger.debug("unvisited child " + str(child_node.ID) + " not visited during parent traversal")
                # Did not visit this unvsited child when visiting parents
                unvisited_children_after_parent_loop.append(child_node.ID)
#rhc: un
        node.unvisited_children = unvisited_children_after_parent_loop
    logger.debug(str(len(unvisited_children_after_parent_loop)) + " children remain unvisited")

    # All or none of children in list_of_unvisited_children could remain 
    # unvisited. If they are all unvisited then no loop is detected but
    # if child C is now visited then a loop was detected. If loop
    # detected then output a loop indicator. Note: If there is a loop
    # of children 3 -> 11 -> 12 -> 4 -> 3 , there is no way that we can 
    # add each child to the partition only after we add all of that child's
    # parents. The partition might be: 11, 12, 4, 3. Here, 11's parent is 3, 
    # but 3 is not added to the partition before 11. Since we have a cycl,
    # we cannot always ensure a node is added to a partition only after all 
    # of it parents are added. We output a loop indicator to help us see this:
    #  3(Lp) 11, 12, 4, 3 indicates that 11's 'p'arent" 3, as in 3(L'p') was not 
    # added to partition before 11 due to a cycle/loop 'L' as in 'L'p

    loop_indicator = ""
    first = True
    logger.info_loop_indicator = False
    # unvisited_children_after_parent_loop: is the children that were unvisited
    # before the parent traversal and that aer still unvisited after the parent
    # traversal. unvisited_children_after_parent_loop may be a subset of 
    # list_of_unvisited_children. 
    # Need to know the children that remain unvisited. They are the children 
    # in list_of_unvisited_children tht are not in unvisited_children_after_parent_loop.
    if TRACK_PARTITION_LOOPS:
        for unvisited_child in list_of_unvisited_children:
            logger.debug("check whether node " + str(node.ID) + " unvisited child " + str(unvisited_child) + " is still unvisited"
                + " after parent traversal")
            if unvisited_child not in unvisited_children_after_parent_loop:
                # Unvisited_child is no longer unvisited after call to dfs_parent.
                # A child that was unvisited before parent loop traverasal but
                # that was visited durng this traversal is part of a loop.
                # This child is also a parent or ancestor.
                # output loop (L) indicators in partition, children are in no 
                # particular order.
                logger.debug("unvisited child " + str(unvisited_child) + " not still unvisited")
                if first:
                    first = False
                    loop_indicator = str(nodes[unvisited_child].ID)
                else:
                    loop_indicator += "/" + str(nodes[unvisited_child].ID)
                logger.debug_loop_indicator = True
            else:
                logger.debug("unvisited child " + str(unvisited_child) + " was not visited during parent traversal")
        if logger.info_loop_indicator:
            # a loop involving child 'c' as in (L'c')
            # Example: 1 5 6 7 3(Lp) 12(Lp) 11 11(Lc) 12 4 3 2 10 9 8
            # Here, 11 is a child of 12, and also 12 is a child of 11. When we visit 12
            # dring a dfs_parent parent traversl, 12 will have an unvisited child 11.
            # But 11 will become visited when 12 does dfs_parent(11). So after the return
            # of 12's cal to dfs_parent(11), 12 will se that 11 is now visited, which means
            # 12's child 11 was visited during the traversal dfs_parent(11) of 12's 
            # parents, which meas that a cycle has been detected. So 12 outputs
            # 11(Lchld_of_12) to indicate this cycle. It also puts 11 in the partition 
            # before 12, which, in general, we want since a parent node is supposed to 
            # be added to a partition before any of its children. This is not always
            # possible due to cycles, e.g., 3, 11, 12, 4, 3, so we use the loop indicators
            # to make this true. In this example, 11 appears before 12 in the partition so 
            # we did not need the loop indicator to make this true; still it shows that a
            # loop was detected FYI.
    
            loop_indicator += "(Lchild_of_" + str(node.ID) + ")"
            current_partition.append(loop_indicator)
            global loop_nodes_added
            loop_nodes_added += 1
            logger.debug("[Info]: possible loop detected, loop indicator: " + loop_indicator)

    if len(unvisited_children_after_parent_loop) > 0:
        # There remains some unvisited children
#rhc: ToDo: check for singleton
        if IDENTIFY_SINGLETONS and (
        len(unvisited_children_after_parent_loop)) == 1:
            # in fact, there is only one unvisited child
            logger.debug("1 unvisited child after parent loop.")
            #only_child_index = node.children[0]
            unvisited_child_index = unvisited_children_after_parent_loop[0]
            logger.debug("unvisited_child_index: " + str(unvisited_child_index))
            unvisited_child = nodes[unvisited_child_index]
#rhc: ToDo: node may have more than 1 child, but if there is only one 
#  unvisited child only_child and it has no children and node is only_child's
#  only parent then we could mark only_child as visited. 
#  Note that in this case only_child would not be a singleton.
#  Of course, we could do this for all of node's children but that's too much?
            if len(node.children) == 1 and (
                # and node actually has only one child (which must be unvisited)
                len(unvisited_child.children) == 0) and (
                # this unvisited only child has no children
                len(unvisited_child.parents) == 1):
                # and node is this unvisited child's only parent, then we can 
                # put the node (first) and its child in the partition, put
                # the child in visited (parent already marked visited) and
                # do not put parent in the queue since it has no unvisited
                # children. (This means child will never be enqueued either.)
                #
                # Make sure parent in partition before any if its children. We visit 
                # parents of node in dfs_parents and these parents are added to 
                # partition in dfs_parents after their parents are added to the 
                # partition in dfs_parents, now here we add node to partition
                # (after all nodes ancestors (unless there is a cycle).) 

                # node already added to partition above. We only decided here
                # whether to add node to queue, and whether node has a 
                # singleton child that can be marked visited and added to the 
                # partition along with node
                logger.debug("the 1 unvisited child after parent loop is a singleton"
                    + " mark it visited and add parent (first) and child to partition.")
                visited.append(unvisited_child.ID)
                # add node to partition before child 
                if node.partition_number == -1:
                    logger.debug ("dfs_parent add " + str(node.ID) + " to partition")
                    node.partition_number = current_partition_number

                    partition_node = Partition_Node(node.ID)
                    partition_node.ID = node.ID
                    # The parents of node must already be in the partition and thus
                    # in the nodeIndex_to_partitionIndex_map
                    """
                    global nodeIndex_to_partitionIndex_map
                    for parent in node.parents:
                        new_index = nodeIndex_to_partitionIndex_map.get(parent)
                        if new_index != None:
                            partition_node.parents.append(new_index)
                        else:
                            patch_parent_mapping.append(partition_node)
                    #partition_node.parents = node.parents
                    """
                    partition_node.num_children = len(node.children)
                    # these are the default values so we do not need these assignments 
                    partition_node.pagerank = 0.0
                    partition_node.isShadowNode = False
                    partition_node.frontier_parents = []

                    #current_partition.append(node.ID)
                    current_partition.append(partition_node)
                    current_group.append(partition_node)

                    global nodeIndex_to_partitionIndex_map
                    nodeIndex_to_partitionIndex_map[partition_node.ID] = len(current_partition)-1
                    global nodeIndex_to_groupIndex_map           
                    nodeIndex_to_groupIndex_map[partition_node.ID] = len(current_group)-1

                    global nodeIndex_to_partition_partitionIndex_group_groupIndex_map
                    partition_number = current_partition_number
                    partition_index = len(current_partition)-1
                    group_number = current_group_number
                    group_index = len(current_group)-1
                    index_in_groups_list = frontier_groups_sum-1
                    pg_tuple = (partition_number,partition_index,group_number,group_index,index_in_groups_list)
                    nodeIndex_to_partition_partitionIndex_group_groupIndex_map[partition_node.ID] = pg_tuple

                else:
                    logger.debug ("dfs_parent do not add " + str(node.ID) + " to partition "
                        + current_partition_number + " since it is already in partition " 
                        + node.partition_number)
                if unvisited_child.partition_number == -1:
                    logger.debug ("dfs_parent add " + str(unvisited_child.ID) + " to partition")
                    unvisited_child.partition_number = current_partition_number

                    partition_node = Partition_Node(unvisited_child.ID)
                    partition_node.ID = unvisited_child.ID
                    """
                    for parent in unvisited_child.parents:
                        partition_node.parents.append(nodeIndex_to_partitionIndex_map[parent])
                    #partition_node.parents = unvisited_child.parents
                    """
                    partition_node.num_children = len(unvisited_child.children)
                    # these are the default values so we do not need these assignments 
                    partition_node.pagerank = 0.0
                    partition_node.isShadowNode = False
                    partition_node.frontier_parents = []

                    #current_partition.append(unvisited_child.ID)
                    current_partition.append(partition_node)
                    current_group.append(partition_node)

                    nodeIndex_to_partitionIndex_map[partition_node.ID] = len(current_partition)-1         
                    nodeIndex_to_groupIndex_map[partition_node.ID] = len(current_group)-1

                    partition_number = current_partition_number
                    partition_index = len(current_partition)-1
                    group_number = current_group_number
                    group_index = len(current_group)-1
                    index_in_groups_list = frontier_groups_sum-1
                    pg_tuple = (partition_number,partition_index,group_number,group_index,index_in_groups_list)
                    nodeIndex_to_partition_partitionIndex_group_groupIndex_map[partition_node.ID] = pg_tuple

                else:
                    # assert: this is an Error
                    logger.debug ("dfs_parent do not add " + str(unvisited_child.ID) + " to partition "
                        + current_partition_number + " since it is already in partition " 
                        + unvisited_child.partition_number)

            else:
                #queue.append(node)
                queue.append(node.ID)
                if DEBUG_ON:
                    # logger.debug(, end=" ")
                    print_val = "queue after add " + str(node.ID) + ": "
                    for x in queue:
                        #logger.debug(x.ID, end=" ")
                        print_val = print_val + str(x) + " "
                    logger.debug(print_val)
                    logger.debug("")
                #frontier.append(node)
                frontier.append(node.ID)
                if DEBUG_ON:
                    print_val = "frontier after add " + str(node.ID) + ": "
                    for x in frontier:
                        #logger.debug(x.ID, end=" ")
                        print_val = print_val + str(x) + " "
                    logger.debug(print_val)
                    logger.debug("")
                # make sure parent in partition before any if its children. We visit parents of nodein dfs_parents 
                # and they are added to partition in dfs_parents after their parents are added 
                # in dfs_parents then here we add node to partition.  
                if node.partition_number == -1:
                    logger.debug ("dfs_parent add " + str(node.ID) + " to partition")
                    node.partition_number = current_partition_number

                    partition_node = Partition_Node(node.ID)
                    partition_node.ID = node.ID
                    """
                    for parent in node.parents:
                        partition_node.parents.append(nodeIndex_to_partitionIndex_map[parent])
                    #partition_node.parents = node.parents
                    """
                    partition_node.num_children = len(node.children)
                    # these are the default values so we do not need these assignments 
                    partition_node.pagerank = 0.0
                    partition_node.isShadowNode = False
                    partition_node.frontier_parents = []
                    
                    #current_partition.append(node.ID)
                    current_partition.append(partition_node)
                    current_group.append(partition_node)

                    nodeIndex_to_partitionIndex_map[partition_node.ID] = len(current_partition)-1         
                    nodeIndex_to_groupIndex_map[partition_node.ID] = len(current_group)-1

                    #information for this partition node
                    partition_number = current_partition_number
                    partition_index = len(current_partition)-1
                    group_number = current_group_number
                    group_index = len(current_group)-1
                    index_in_groups_list = frontier_groups_sum -1
                    pg_tuple = (partition_number,partition_index,group_number,group_index,index_in_groups_list)
                    nodeIndex_to_partition_partitionIndex_group_groupIndex_map[partition_node.ID] = pg_tuple

                else:
                    logger.debug ("dfs_parent do not add " + str(node.ID) + " to partition "
                        + current_partition_number + " since it is already in partition " 
                        + node.partition_number)
        else:
                #queue.append(node)
                queue.append(node.ID)
                if DEBUG_ON:
                    print_val = "queue after add " + str(node.ID) + ":"
                    for x in queue:
                        #logger.debug(x.ID, end=" ")
                        #logger.debug(x, end=" ")
                        print_val = print_val + str(x) + " "
                    logger.debug(print_val)
                    logger.debug("")
                #frontier.append(node)
                frontier.append(node.ID)
                if DEBUG_ON:
                    print_val = "frontier after add " + str(node.ID) + ":"
                    for x in frontier:
                        #logger.debug(x.ID, end=" ")
                        #logger.debug(x, end=" ")
                        print_val = print_val + str(x) + " "
                    logger.debug(print_val)
                    logger.debug("")
                # make sure parent in partition before any if its children. We visit parents of nodein dfs_parents 
                # and they are added to partition in dfs_parents after their parents are added 
                # in dfs_parents then here we add node to partition.  
                if node.partition_number == -1:
                    logger.debug ("dfs_parent add " + str(node.ID) + " to partition")
                    node.partition_number = current_partition_number

                    partition_node = Partition_Node(node.ID)
                    partition_node.ID = node.ID
                    """
                    for parent in node.parents:
                        partition_node.parents.append(nodeIndex_to_partitionIndex_map[parent])
                    #partition_node.parents = node.parents
                    """
                    partition_node.num_children = len(node.children)
                    # these are the default values so we do not need these assignments 
                    partition_node.pagerank = 0.0
                    partition_node.isShadowNode = False
                    partition_node.frontier_parents = []

                    #current_partition.append(node.ID)
                    current_partition.append(partition_node)
                    current_group.append(partition_node)

                    nodeIndex_to_partitionIndex_map[partition_node.ID] = len(current_partition)-1
                    nodeIndex_to_groupIndex_map[partition_node.ID] = len(current_group)-1

                    # information for this partition node
                    partition_number = current_partition_number
                    partition_index = len(current_partition)-1
                    group_number = current_group_number
                    group_index = len(current_group)-1
                    index_in_groups_list = frontier_groups_sum-1
                    pg_tuple = (partition_number,partition_index,group_number,group_index,index_in_groups_list)
                    nodeIndex_to_partition_partitionIndex_group_groupIndex_map[partition_node.ID] = pg_tuple

                else:
                    logger.debug ("dfs_parent do not add " + str(node.ID) + " to partition "
                        + current_partition_number + " since it is already in partition " 
                        + node.partition_number)
    else:
        logger.debug("node " + str(node.ID) + " has no unvisited children after parent traversal,"
            + " add it to partition but not queue")
        if node.partition_number == -1:
            logger.debug("dfs_parent add " + str(node.ID) + " to partition")
            node.partition_number = current_partition_number


            partition_node = Partition_Node(node.ID)
            partition_node.ID = node.ID
            """
            for parent in node.parents:
                partition_node.parents.append(nodeIndex_to_partitionIndex_map[parent])
            #partition_node.parents = node.parents
            """
            partition_node.num_children = len(node.children)
            # these are the default values so we do not need these assignments 
            partition_node.pagerank = 0.0
            partition_node.isShadowNode = False
            partition_node.frontier_parents = []

            #current_partition.append(node.ID)
            current_partition.append(partition_node)
            current_group.append(partition_node)

            nodeIndex_to_partitionIndex_map[partition_node.ID] = len(current_partition)-1
            nodeIndex_to_groupIndex_map[partition_node.ID] = len(current_group)-1

            # information for this partition node
            partition_number = current_partition_number
            partition_index = len(current_partition)-1
            group_number = current_group_number
            group_index = len(current_group)-1
            index_in_groups_list = frontier_groups_sum-1
            pg_tuple = (partition_number,partition_index,group_number,group_index,index_in_groups_list)
            nodeIndex_to_partition_partitionIndex_group_groupIndex_map[partition_node.ID] = pg_tuple
            logger.debug("HHHHHHHHHHHHHHHH dfs_parent: pg_tuple generate for " + str(partition_node.ID)
                + str(pg_tuple))

        else:
            logger.debug("dfs_parent do not add " + str(node.ID) + " to partition "
                + current_partition_number + " since it is already in partition " 
                + node.partition_number)

#def bfs(visited, graph, node): #function for BFS
def bfs(visited, node): #function for BFS
    logger.debug ("bfs mark " + str(node.ID) + " as visited and add to queue")
    #rhc: add to visited is done in dfs_parent
    #visited.append(node.ID)
    # dfs_parent will add node to partition (and its unvisited parent nodes)
    global current_partition


    global dfs_parent_start_partition_size
    global loop_nodes_added_start
    global dfs_parent_start_frontier_size
    global dfs_parent_end_partition_size
    global loop_nodes_added_end
    global dfs_parent_end_frontier_size

#rhc: q:
    # are not these lengths 0?
    dfs_parent_start_partition_size = len(current_partition)
    dfs_parent_start_frontier_size = len(frontier)
    global loop_nodes_added
    loop_nodes_added_start = loop_nodes_added

    #dfs_p(visited, graph, node)
    #dfs_p_new(visited, graph, node)

#rhc: 
    queue.append(-1)
    #global scc_num_vertices
    #scc_num_vertices += 1
    #dfs_parent(visited, graph, node)
    global num_frontier_groups
    num_frontier_groups = 1
    global frontier_groups_sum
    frontier_groups_sum = 1
    dfs_parent(visited, node)
    #logger.debug("BFS set V to " + str(scc_num_vertices))
    #scc_graph.setV(scc_num_vertices)
    #scc_graph.logger.infoEdges()
    #scc_graph.clear()

    global current_group
    global groups
    groups.append(current_group)
    current_group = []
    #global frontier_groups_sum
    # root group
    #frontier_groups_sum += 1

    # this first group ends here after first dfs_parent
    global nodeIndex_to_groupIndex_maps
    global nodeIndex_to_groupIndex_map
    nodeIndex_to_groupIndex_maps.append(nodeIndex_to_groupIndex_map)
    nodeIndex_to_groupIndex_map = {}

    global current_partition_number
    global current_group_number
    group_name = "PR" + str(current_partition_number) + "_" + str(current_group_number)
    # group_number_in_fronter stays at 1 since this is the only group in the frontier_list
    # partition and thus the first group in the next parttio is also group 1

    global current_group_isLoop
    if current_group_isLoop:
        # These are the names of the groups that have a loop. In the 
        # DAG, we will append an 'L' to the name.
        # These are the names of the groups that have a loop. In the 
        # DAG, we will append an 'L' to the name. Not used since we 
        # use loop names (with 'L") as we generate Sender and Recevers.
        # instead of modifying the names of senders/receievers before we 
        # generate the DAG.
        Group_loops.add(group_name)
        group_name = group_name + "L"
    current_group_isLoop = False
    # Note: not incrementing current_group_number. This root group is the 
    # only group in this partition. We consider it to be group 1, 
    # which is the initial value of current_group_number. We do not increment
    # it since we are done with the groups in the first partition, so 
    # current_group_number will be 1 wen we find the first group of the 
    # next partition.
    group_names.append(group_name)

    dfs_parent_end_partition_size = len(current_partition)
    dfs_parent_end_frontier_size = len(frontier)
    loop_nodes_added_end = loop_nodes_added
#rhc: Q: are not these sizes len(current_partition) and len(frontier)/
    dfs_parent_change_in_partition_size = (dfs_parent_end_partition_size - dfs_parent_start_partition_size) - (
        loop_nodes_added_end - loop_nodes_added_start)
    dfs_parent_change_in_frontier_size = (dfs_parent_end_frontier_size - dfs_parent_start_frontier_size) - (
        loop_nodes_added_end - loop_nodes_added_start)
    logger.debug("dfs_parent(root)_change_in_partition_size: " + str(dfs_parent_change_in_partition_size))
    logger.debug("dfs_parent(root)_change_in_frontier_size: " + str(dfs_parent_change_in_frontier_size))
    dfs_parent_changes_in_partiton_size.append(dfs_parent_change_in_partition_size)
    dfs_parent_changes_in_frontier_size.append(dfs_parent_change_in_frontier_size)

    # queue.append(node) and frontier.append(node) done optionally in dfs_parent
#rhc
    end_of_current_frontier = False
    while queue:          # Creating loop to visit each node
        #node = queue.pop(0) 
        ID = queue.pop(0) 
        logger.debug("bfs pop node " + str(ID) + " from queue") 
#rhc
        # issue: if we add queue.append(-1) in dfs_parent, we get smaller partitions
        # but the frontiers overlap. this is becuase in dfs_parent we get
        # a -1 b -1 c -1 then we rturn to bfs then it checks -1 at front, which is 
        # true, so it crates partition a b c sincne a b and c are in the partition
        # but haven't got thru aall the nodes on frontier, which is a b c so next 
        # frontier is b c ... So if we put a b c in partition before we see our
        # first -1 then we have to get through a b and c. Note there is a funny interaction
        # with when we remove node from frontier, i.e., after we isit all of its
        # children. So only can process -1's after processing all of a node's children.
        # Hmmm. 
        # We get 5, 17, 1 so we dfs_parent 5 and visit 5's parents then visit 5's children:
        """
        bfs node 5 visit children
        bfs visit child 16 mark it visited and dfs_parent(16)
        bfs dfs_parent(16)
        dfs_parent from node 16
        dfs_parent node 16 visit parents
        dfs_parent neighbor 5 already visited
        dfs_parent visit node 10
        dfs_parent from node 10
        dfs_parent node 10 visit parents
        dfs_parent visit node 2
        dfs_parent from node 2
        dfs_parent node 2 has no parents
        queue after add 2: 17 1 -1 2
        frontier after add 2: 5 17 1 2
        dfs_parent add 2 to partition
        queue after add 10: 17 1 -1 2 10
        frontier after add 10: 5 17 1 2 10 
        dfs_parent add 10 to partition
        queue after add 16: 17 1 -1 2 10 16
        frontier after add 16: 5 17 1 2 10 16
        dfs_parent add 16 to partition
        dfs_parent_change_in_partition_size: 3
        dfs_parent_change_in_frontier_size: 3
        bfs node 17 already visited
        frontier after remove 5: 17 1 2 10 16
        """
        # but no -1 after 5. So coul put -1 after 5 if we replaced 5 on queue
        # with all its parent cild stuff?

        if ID == -1:
            end_of_current_frontier = True

            if queue:
                ID = queue.pop(0)
                logger.debug("bfs after pop -1 pop node " + str(ID) + " from queue") 
                queue.append(-1)

                #scc_graph.logger.infoEdges()
                #scc_graph.clear()
            else:
                break

        node = nodes[ID]
        # so we can see the frontier costs that do not correspnd to when 
        # partitions were created, i.e., was there a better frontier for partition?
        all_frontier_costs.append("pop-"+str(node.ID) + ":" + str(len(frontier)))

        # Note: There are no singletons in the frontier. if N has a singleton child
        # C then the dfs_parent(N) will se that C is unvisited. IF singleton checking
        # is on then singleton C will be identified, C will be marked visited, and
        # neither N nor C will be enqueued but both N and C will be added to the 
        # partition but not the frontier. If singleton checking is off then N
        # will be enqueued. When N is popped off the queue, dfs_parent(C) will be
        # called and it will see that C has no children before or after C's parent 
        # traversal (N is already visited) so C will be marked visited and C will 
        # not be enqueued but will be added to the partition but not the frontier.
        # Note: If the partiton becmes full with N in the frontier with singleton 
        # chld C, we can reduce the frontier by pruning N - pop N from the queue,
        # mark N as visited, remove N fro the frontier, mark its singleton child C 
        # visited, and add C to the partition but not the queue.
        # Note: handling singletons here is more efficent since we don;t waste time 
        # checkng for singletons in dfs_parent when most nodes are not singletons.

#rhc: problem: we don't find partition loop until we dfs_parent(17) so the 
# frontier tuple for the 5 is in different partition than 16 is wring since
# we will use PR2_1 for partition. So don't do partition tuples until after
# finish partition, where we know whether partition has a loop or not?
# save frontier tuples with empty name and then patch the name before you
# process the tuple.

        if end_of_current_frontier:
            logger.debug("BFS: end_of_current_frontier")
            end_of_current_frontier = False
            if len(current_partition) > 0:
            #if len(current_partition) >= num_nodes/5:
                logger.debug("BFS: create sub-partition at end of current frontier")
                # does not require a deepcopy
                partitions.append(current_partition.copy())
                current_partition = []

                partition_name = "PR" + str(current_partition_number) + "_1"
                global current_partition_isLoop
                if current_partition_isLoop:
                    # These are the names of the partitions that have a loop. In the 
                    # DAG, we will append an 'L' to the name.
                    Partition_loops.add(partition_name)
                    partition_name = partition_name + "L"
                # Patch the partition name of the frontier_parent tuples. 
                if current_partition_isLoop:
                    # When the tuples in frontier_parent_partition_patch_tuple_list were created,
                    # no loop had been detectd in the partition so we used a partitiob name that 
                    # did not end in 'L'. At some point a loop was detected so we need to
                    # change the partition name in the tuple so that it ends with 'L'. If no loop
                    # is detectd, then current_partition_isLoop will be false and no changes
                    # need to be made.
                    logger.debug("XXXXXXXXXXX BFS: patch partition frontier_parent tuples: ")
                    # frontier_parent_partition_patch_tuple was created as:
                    #   (parent_partition_number,parent_partition_index,(current_partition_number,1,child_index_in_current_partition,current_partition_name))
                    for frontier_parent_partition_patch_tuple in frontier_parent_partition_patch_tuple_list:
                        # These values were used to create the tuples in dfs_parent()
                        parent_partition_number = frontier_parent_partition_patch_tuple[0]
                        parent_partition_index = frontier_parent_partition_patch_tuple[1]
                        position_in_frontier_parents_partition_list = frontier_parent_partition_patch_tuple[2]

                        # get the tuple that has the wrong name
                        parent_partition = partitions[parent_partition_number-1]
                        frontier_parents = parent_partition[parent_partition_index].frontier_parents
                        frontier_parent_partition_tuple_to_patch = frontier_parents[position_in_frontier_parents_partition_list]
                        logger.debug("XXXXXXX BFS: patching partition frontier_tuple name "
                            + frontier_parent_partition_tuple_to_patch[3] + " to " + partition_name)
                        # create a new tuple that reuses the first 3 fields and chnages the name in the last field
                        first_field = frontier_parent_partition_tuple_to_patch[0]
                        second_field = frontier_parent_partition_tuple_to_patch[1]
                        third_field = frontier_parent_partition_tuple_to_patch[2]
                        new_frontier_parent_partition_tuple = (first_field,second_field,third_field,partition_name)
                        # delete the old tuples
                        del frontier_parents[position_in_frontier_parents_partition_list]
                        # append the new tuple, order of tuples may change but order is not important
                        frontier_parents.append(new_frontier_parent_partition_tuple)
                        logger.debug("XXXXXXX BFS:  new frontier_parents: " + str(frontier_parents))

                frontier_parent_partition_patch_tuple_list.clear()

                if current_partition_isLoop:
                    # When the tuples in sender_receiver_partition_patch_tuple_list were created,
                    # no loop had been detectd in the partition so we used a partitiom name 
                    # for the receiver name that did not end in 'L'. At some point a loop was detected so we need to
                    # change the receiver name so that it ends with 'L'. If no loop
                    # is detectd, then current_partition_isLoop will be false and no changes
                    # need to be made.
                    logger.debug("XXXXXXXXXXX BFS: patch partition sender/receiver names: ")
                    for sender_receiver_partition_patch_tuple in sender_receiver_partition_patch_tuple_list:
                        # sender_receiver_partition_patch_tuple crated as:
                        #   sender_receiver_partition_patch_tuple = (parent_partition_number,receiving_partition)
                        parent_partition_number = sender_receiver_partition_patch_tuple[0]
                        receiving_partition = sender_receiver_partition_patch_tuple[1]

                        sending_partition = partition_names[parent_partition_number-1]
                        sender_name_set = Partition_senders[sending_partition]
                        logger.debug("XXXXXXX BFS: patching partition sender_set receiver name "
                            + receiving_partition + " to " + partition_name)
                        sender_name_set.remove(receiving_partition)
                        sender_name_set.add(partition_name)
                        logger.debug("XXXXXXX BFS:  new partition sender_Set: " + str(sender_name_set))

                        logger.debug("XXXXXXX BFS: patching Partition_receivers receiver name "
                            + receiving_partition + " to " + partition_name)
                        Partition_receivers[partition_name] = Partition_receivers[receiving_partition]
                        del Partition_receivers[receiving_partition]
                        logger.debug("XXXXXXX BFS:  new Partition_receivers[partition_name]: " + str(Partition_receivers[partition_name]))
                
                sender_receiver_partition_patch_tuple_list.clear()

                current_partition_isLoop = False
                partition_names.append(partition_name)

                global patch_parent_mapping_for_partitions
                logger.debug("BFS: partition_nodes to patch: ")
                for parent_tuple in patch_parent_mapping_for_partitions:
                    logger.debug("BFS: parent_tuple: " + str(parent_tuple) + "," )
                    # where: patch_tuple = (parent_index,partition_node.parents,
                    # group_node.parents,i,node.ID)
                    #
                    # For debugging, this is the node ID of the node whose parents 
                    # we are patching. There is a node with this ID in the current
                    # partition and in the current group. We also saved this node's
                    # parent list, both for the partition node and the group node
                    # in the tuple (see below).
                    node_ID = parent_tuple[4]
                    # ID of parent whose index was not known when we remapped
                    # parents of node node_ID in dfs_parent(); recall when we
                    # add a node to a partition/group the position of it's
                    # parents change (since node ID is no longer at position
                    # ID in a partition/group) so we need to remap the parent 
                    # positions of node node_ID.
                    parent_index = parent_tuple[0]
                    # list of parents - for the partition node and group node
                    # that had a parent whose remapped index was not yet knows,
                    # we save the node's parent list in the tuple; there is 
                    # one list for the ode in the partition and one list for 
                    # the node in the group.
                    list_of_parents_of_partition_node = parent_tuple[1]
                    #list_of_parents_of_group_node = parent_tuple[2]
                    # Since we did not know the new index of node node_IDs parent,
                    # we made this index -1. The poisition in the list of
                    # parents where the -1 is is i.
                    i = parent_tuple[3]

                    pg_tuple = nodeIndex_to_partition_partitionIndex_group_groupIndex_map[parent_index]
                    partition_index_of_parent = pg_tuple[1]
                    #group_index_of_parent = pg_tuple[3]
                    if partition_index_of_parent != -1:
                        # assert group_index is also -1
                        list_of_parents_of_partition_node[i] = partition_index_of_parent
                        #list_of_parents_of_group_node[i] = group_index_of_parent
                        logger.debug("BFS: end of frontier: remapping parent " + str(parent_index)
                            + " of " + str(node_ID) +  " to " + str(partition_index_of_parent) 
                            + " for partition node.")
                            #+ group_index_of_parent + " for group node")
                    else:
                        logger.error("BFS: global map index of " + parent_index + " is -1")

                # Q: where do this? After partition is done since we need to patch
                # partition then and that is after last group is done.
                # Q: do we need separate patch lists for partitions and groups?
                patch_parent_mapping_for_partitions = []

                # track partitions here; track groups after dfs_parent()
                global nodeIndex_to_partitionIndex_maps
                global nodeIndex_to_partitionIndex_map             
                nodeIndex_to_partitionIndex_maps.append(nodeIndex_to_partitionIndex_map)
                nodeIndex_to_partitionIndex_map = {}

                # Note: we cannot clear nodeIndex_to_partition_partitionIndex_group_groupIndex_map
                # but we do not need to have all nodes in this map. If we just finished
                # partition i, we will next work on partition i+1 and while doing that we
                # may need the nodes in partition i as a node in partition i+1 can have
                # a parent node in partition i but by definition not in any partition previous
                # to partition i. So from this point on we don't need the nodes in partition i-1,;
                # thus, we could remove them from the map, where partition i-1 is 
                # saved in partitions[] so we can get the nodes in partition i-1.

                global total_loop_nodes_added
                total_loop_nodes_added += loop_nodes_added
                loop_nodes_added = 0

                """ GLOBAL SCC GRAPH HERE ***
                global scc_graph
                scc_graph.logger.infoEdges()
                scc_graph.logger.info_ID_map()
                logger.debug("SCCs (node IDs):")
                list_of_sccs = scc_graph.SCC()
                logger.debug("len of list_of_sccs: " + str(len(list_of_sccs)))
                
                list_of_lambdas = []
                no_loop = []
                has_a_no_loop_function = False
                for list in list_of_sccs:
                    if len(list) == 1:
                        has_a_no_loop_function = True
                        no_loop = no_loop + list
                    else:
                        list_of_lambdas.append(list)
                if len(no_loop) > 0:
                    list_of_lambdas.append(no_loop) 
                i = 0
                logger.debug("Serverless Function Inputs:")
                for serverless_function in list_of_lambdas:
                    if has_a_no_loop_function and i == (len(list_of_lambdas)-1):
                        f_string = "   F" + str(current_partition_number) + "_" + str(i) + " (no-loop-function): "
                    else:
                        f_string = "   F" + str(current_partition_number) + "_" + str(i) + ": "
                    logger.info("DEBUG: " + f_string,end="")
                    for node_index in serverless_function:
                        logger.info(str(node_index),end=" ") 
                    logger.info("")
                    i = i+1
                #scc_graph.clear()
                """

                # using this to determine whether parent is in current partition
                current_partition_number += 1
                current_group_number = 1
                #global frontier_groups_sum
                #global num_frontier_groups
                logger.info("BFS: frontier groups: " + str(num_frontier_groups))

                # use this if to filter the very small numbers of groups
                #if frontier_groups > 10:
                # frontier_groups_sum += num_frontier_groups
                logger.info("BFS: frontier_groups_sum: " + str(frontier_groups_sum))
                num_frontier_groups = 0
                #scc_graph = Graph(0)
#rhc: Q: 
# - So some nodes are in current_partition. Some of these nodes that are in the 
# current_partition are in the frontier and some aer not in the frontier. For example, 
# a node N with lots of children may be added to the partition, but N will stay on
# the frontier until we pop N's last child. This means that N can stay on the
# frontier for many partionings after it is added to the current_partition.
# - Noet that we still satisfy the rule that a parent is aded to a/the partition
# before its children aer added toa/the partition. ==> Are the parent and its
# children expected to be in the same partition? No. 
# ==> If a parent is in partition i are all of the parent"s children exepcted to be 
# in partition i+1? Is this required? No? The only rule is: if node X is in 
# partition i then all of X's parent's are in partition n, n<=i? No! we only want
# partition i to have to send nodes to partition i+1. But ...
# Woops:)
# So 
# - we only add a node N to the frontier when we add N to the partition. 
# - we only add a node N to the partition when we dequeue it and thus begin
#   visiting its children?
# - No. When we deque a node N:
#   We don't add N to the partition P1. We have to first visit N's children
#   and follow the parents-before-children rule to see what's going into
#   the partition Part1. Let the first child of N be C1. We have to dfs_parent(C1)
#   to visit C1's parents. They have to go into the partition Part1 before C1.
#   If we add a parent P1 of C1 to the partition Part1 then we want all of P1's 
#   children to go into Part 2 to get a ring.
# - ETC!!!!!
# Alternately: we only take a partition when all of the nodes that were in the previous
# partition leave the frontier? So not balanced? But we can combine partitions?
# Ugh!!

                # does not require a deepcopy
                frontiers.append(frontier.copy())
                frontier_cost = "pop-"+str(node.ID) + ":" + str(len(frontier))
                frontier_costs.append(frontier_cost)

        if not len(node.children):
            logger.debug ("bfs node " + str(node.ID) + " has no children")
        else:
            logger.debug ("bfs node " + str(node.ID) + " visit children")
        for neighbor_index in node.children:
            neighbor = nodes[neighbor_index]
            if neighbor.ID not in visited:
                logger.debug ("bfs visit child " + str(neighbor.ID) + " mark it visited and "
                    + "dfs_parent(" + str(neighbor.ID) + ")")

                #visited.append(neighbor.ID)
                logger.debug ("bfs dfs_parent("+ str(neighbor.ID) + ")")

                dfs_parent_start_partition_size = len(current_partition)
                loop_nodes_added_start = loop_nodes_added
                dfs_parent_start_frontier_size = len(frontier)

                num_frontier_groups += 1
                frontier_groups_sum += 1
                #dfs_p_new(visited, graph, neighbor)
                #dfs_parent(visited, graph, neighbor) 
                dfs_parent(visited, neighbor)

                """
                # index of child just added (we just visited it because it ws an 
                # unvisited child) to partition
                child_index = len(current_partition) - 1
                # shadow node is a parent Node on frontier of previous partition
                shadow_node = Node(node.ID)
                shadow_node.isShadowNode = True
                # insert shadow_node before child (so only shift one)
                current_partition.insert(child_index,shadow_node)
                # remember where the frontier_parent node should be placed when the 
                # partition the PageRank task sends it to receives it. 
                frontier_parent_tuple = (current_partition_number,frontier_groups,child_index-dfs_parent_start_partition_size)
                logger.debug ("bfs frontier_parent_tuple: " + str(frontier_parent_tuple))
                # mark this node as one that PageRank needs to send in its output to the 
                # next partition (via fanout/faninNB).That is, the fact that list
                # frontier_parent is not empty indicates it needs to be sent in the 
                # PageRank output. The tuple indictes which frontier group it should 
                # be sent to. PageRank may send frontier_parent nodes to mulltiple groups
                # of multiple partitions
                nodes[node.ID].frontier_parents.append(frontier_parent_tuple)
                """

                # Note: append() uses a shallow copy.
                groups.append(current_group)
                # this is a list of partition_nodes in the current group
                current_group = []
                group_name = "PR" + str(current_partition_number) + "_" + str(current_group_number)
                if current_group_isLoop:
                    # These are the names of the groups that have a loop. In the 
                    # DAG, we will append an 'L' to the name. Not used since we 
                    # use loop names (with 'L") as we generate Sender and Recevers.
                    # instead of modifying the names of senders/receievers before we 
                    # generate the DAG.
                    Group_loops.add(group_name)
                    group_name = group_name + "L"
                    

#rhc:
# 1. clear instead of re-init?
# 2. Really need to patch groups? If no assert no patching. Note:
#       we find loops on backup nd we don't do atch stuff until after
#       we see all backups, so can we do patch stuff without knowing 
#       about loop that will be detected later?

                if current_group_isLoop:
                    # When the tuples in frontier_parent_partition_patch_tuple_list were created,
                    # no loop had been detectd in the partition so we used a partitiob name that 
                    # did not end in 'L'. At some point a loop was detected so we need to
                    # change the partition name in the tuple so that it ends with 'L'. If no loop
                    # is detectd, then current_partition_isLoop will be false and no changes
                    # need to be made.
                    logger.debug("XXXXXXXXXXX BFS: patch group frontier_parent tuples: ")
                    # frontier_parent_partition_patch_tuple was created as:
                    #   (parent_partition_number,parent_partition_index,(current_partition_number,1,child_index_in_current_partition,current_partition_name))
                    for frontier_parent_group_patch_tuple in frontier_parent_group_patch_tuple_list:
                        # These values were used to create the tuples in dfs_parent()
                        index_in_groups_list = frontier_parent_group_patch_tuple[0]
                        parent_group_parent_index = frontier_parent_group_patch_tuple[1]
                        position_in_frontier_parents_group_list = frontier_parent_group_patch_tuple[2]

                        # get the tuple that has the wrong name
                        parent_group = groups[index_in_groups_list]
                        frontier_parents = parent_group[parent_group_parent_index].frontier_parents
                        frontier_parent_group_tuple_to_patch = frontier_parents[position_in_frontier_parents_group_list]
                        logger.debug("XXXXXXX BFS: patching group frontier_tuple name "
                            + frontier_parent_group_tuple_to_patch[3] + " to " + group_name)
                        # create a new tuple that reuses the first 3 fields and chnages the name in the last field
                        first_field = frontier_parent_group_tuple_to_patch[0]
                        second_field = frontier_parent_group_tuple_to_patch[1]
                        third_field = frontier_parent_group_tuple_to_patch[2]
                        new_frontier_parent_group_tuple = (first_field,second_field,third_field,group_name)
                        # delete the old tuples
                        del frontier_parents[position_in_frontier_parents_group_list]
                        # append the new tuple, order of tuples may change but order is not important
                        frontier_parents.append(new_frontier_parent_group_tuple)
                        logger.debug("XXXXXXX BFS:  new frontier_parents: " + str(frontier_parents))

                frontier_parent_group_patch_tuple_list.clear()

                if current_group_isLoop:
                    # When the tuples in sender_receiver_partition_patch_tuple_list were created,
                    # no loop had been detectd in the partition so we used a partitiom name 
                    # for the receiver name that did not end in 'L'. At some point a loop was detected so we need to
                    # change the receiver name so that it ends with 'L'. If no loop
                    # is detectd, then current_partition_isLoop will be false and no changes
                    # need to be made.
                    logger.debug("XXXXXXXXXXX BFS: patch group sender/receiver names: ")
                    for sender_receiver_group_patch_tuple in sender_receiver_group_patch_tuple_list:
                        # sender_receiver_partition_patch_tuple crated as:
                        #   sender_receiver_partition_patch_tuple = (parent_partition_number,receiving_partition)
                        index_in_groups_list = sender_receiver_group_patch_tuple[0]
                        receiving_group = sender_receiver_group_patch_tuple[1]

                        sending_group = partition_names[index_in_groups_list]
                        sender_name_set = Group_senders[sending_group]
                        logger.debug("XXXXXXX BFS: patching group sender_set receiving_group "
                            + receiving_group + " to " + group_name)
                        sender_name_set.remove(receiving_group)
                        sender_name_set.add(group_name)
                        logger.debug("XXXXXXX BFS:  new group sender_Set: " + str(sender_name_set))

                        logger.debug("XXXXXXX BFS: patching Group_receivers receiver name "
                            + receiving_group + " to " + group_name)
                        Group_receivers[group_name] = Group_receivers[receiving_group]
                        del Group_receivers[receiving_group]
                        logger.debug("XXXXXXX BFS:  new Group_receivers[group_name]: " + str(Group_receivers[group_name]))
                
                sender_receiver_group_patch_tuple_list.clear()

                current_group_isLoop = False
                current_group_number += 1
                group_names.append(group_name)

                #global patch_parent_mapping_for_partitions
                global patch_parent_mapping_for_groups
                logger.debug("partition_nodes to patch: ")
                for parent_tuple in patch_parent_mapping_for_groups:
                    logger.debug("parent_tuple: " + str(parent_tuple) + "," )
                    # where: patch_tuple = (parent_index,partition_node.parents,
                    # group_node.parents,i,node.ID)
                    #
                    # For debugging, this is the node ID of the node whose parents 
                    # we are patching. There is a node with this ID in the current
                    # partition and in the current group. We also saved this node's
                    # parent list, both for the partition node and the group node
                    # in the tuple (see below).
                    node_ID = parent_tuple[4]
                    # ID of parent whose index was not known when we remapped
                    # parents of node node_ID in dfs_parent(); recall when we
                    # add a node to a partition/group the position of it's
                    # parents change (since node ID is no longer at position
                    # ID in a partition/group) so we need to remap the parent 
                    # positions of node node_ID.
                    parent_ID = parent_tuple[0]
                    # list of parents - for the partition node and group node
                    # that had a parent whose remapped index was not yet knows,
                    # we save the node's parent list in the tuple; there is 
                    # one list for the ode in the partition and one list for 
                    # the node in the group.
                    #list_of_parents_of_partition_node = parent_tuple[1]
                    list_of_parents_of_group_node = parent_tuple[2]
                    # Since we did not know the new index of node node_IDs parent,
                    # we made this index -1. The poisition in the list of
                    # parents where the -1 is is i.
                    i = parent_tuple[3]

                    pg_tuple = nodeIndex_to_partition_partitionIndex_group_groupIndex_map[parent_ID]
                    #partition_index_of_parent = pg_tuple[1]
                    group_index_of_parent = pg_tuple[3]
                    if group_index_of_parent != -1:
                        # assert group_index is also -1
                        #list_of_parents_of_partition_node[i] = partition_index_of_parent
                        list_of_parents_of_group_node[i] = group_index_of_parent
                        logger.debug("end of frontier: remapping parent " + str(parent_ID)
                            + " of " + str(node_ID) 
                            #+  " to " + partition_index_of_parent 
                            #+ " for partition node and "
                            + " to " + str(group_index_of_parent) + " for group node")
                    else:
                        logger.error("global map index of " + parent_index + " is -1")

                patch_parent_mapping_for_groups = []

                # track groups here; track partitions when frontier ends above
                nodeIndex_to_groupIndex_maps.append(nodeIndex_to_groupIndex_map)
                nodeIndex_to_groupIndex_map = {}

                logger.info("")
                if PRINT_DETAILED_STATS:
                    logger.info("KKKKKKKKKKKKKKKKKKKKK group nodes' frontier_parent_tuples:")
                    for x in groups:
                        if PRINT_DETAILED_STATS:
                            print_val = "-- (" + str(len(x)) + "): "
                            for node in x:
                                print_val += str(node.ID) + ": "
                                # logger.info(node.ID,end=": ")
                                for parent_tuple in node.frontier_parents:
                                    print_val += str(parent_tuple) + " "
                                    # print(str(parent_tuple), end=" ")
                            logger.info(print_val)
                            logger.info("")
                        else:
                            logger.info("-- (" + str(len(x)) + ")")
                else:
                    logger.info("-- (" + str(len(x)) + ")")
                logger.info("")

                dfs_parent_end_partition_size = len(current_partition)
                dfs_parent_end_frontier_size = len(frontier)
                loop_nodes_added_end = loop_nodes_added
                dfs_parent_change_in_partition_size = (dfs_parent_end_partition_size - dfs_parent_start_partition_size) - (
                    loop_nodes_added_end - loop_nodes_added_start)
                dfs_parent_change_in_frontier_size = (dfs_parent_end_frontier_size - dfs_parent_start_frontier_size) - (
                    loop_nodes_added_end - loop_nodes_added_start)
                logger.debug("dfs_parent("+str(node.ID) + ")_change_in_partition_size: " + str(dfs_parent_change_in_partition_size))
                logger.debug("dfs_parent("+str(node.ID) + ")_change_in_frontier_size: " + str(dfs_parent_change_in_frontier_size))
                dfs_parent_changes_in_partiton_size.append(dfs_parent_change_in_partition_size)
                dfs_parent_changes_in_frontier_size.append(dfs_parent_change_in_frontier_size)

                """
                # dfs_parent decides whether to queue the node to queue and frontier. 
                # If the neighbor has a child but this child has no children and neighbor
                # is the child's single parent, do not add neighbor to queue and 
                # mark the child as visited.  If neighbot has no children do not 
                # queue it or add t to the frontier. We used to have the followng
                if len(neighbor.children) > 0:
                    queue.append(neighbor)
                    frontier.append(neighbor)
                else:
                    logger.debug("child " + str(neighbor.ID) + " of node " + str(node.ID)
                        + " has no children, already marked it visited and added"
                        + " it to partition but do not queue it or add it to frontier.")
                """
            else:
                logger.debug ("bfs node " + str(neighbor.ID) + " already visited")
        #frontier.remove(node)
        #frontier.remove(node.ID)
        try:
            frontier.remove(node.ID)
        except ValueError:
            logger.debug("*******bfs: " + str(node.ID)
                + " not in frontier.")

        if DEBUG_ON:
            print_val = "frontier after remove " + str(node.ID) + ": "
            for x in frontier:
                #logger.debug(x.ID, end=" ")
                print_val = print_val + str(x) + " "
            logger.debug(print_val)
            logger.debug("")
    
    """
    if len(current_partition) >= 0:
        logger.debug("BFS: create final sub-partition")
        partitions.append(current_partition.copy())
        current_partition = []
        #global total_loop_nodes_added
        total_loop_nodes_added += loop_nodes_added
        loop_nodes_added = 0
        frontiers.append(frontier.copy())
        frontier_cost = "atEnd:" + str(len(frontier))
        frontier_costs.append(frontier_cost)
    """

def input_graph():
    """
    c FILE                  :graph1.gr.gr
    c No. of vertices       :20
    c No. of edges          :23
    c Max. weight           :1
    c Min. weight           :1
    c Min. edge             :1
    c Max. edge             :3
    p sp 20 23
    """
    #graph_file = open('100.gr', 'r')
    graph_file = open('graph_20.gr', 'r')
    #graph_file = open('graph_3000.gr', 'r')
    #graph_file = open('graph_30000.gr', 'r')
    count = 0
    file_name_line = graph_file.readline()
    count += 1
    logger.debug("file_name_line{}: {}".format(count, file_name_line.strip()))
    vertices_line = graph_file.readline()
    count += 1
    logger.debug("vertices_line{}: {}".format(count, vertices_line.strip()))
    edges_line = graph_file.readline()
    count += 1
    logger.debug("edges_line{}: {}".format(count, edges_line.strip()))
    
    max_weight_line_ignored = graph_file.readline()
    count += 1
    #logger.debug("max_weight_line{}: {}".format(count, max_weight_line_ignored.strip()))
    min_weight_line_ignored = graph_file.readline()
    count += 1
    #logger.debug("min_weight_line{}: {}".format(count,  min_weight_line_ignored.strip()))

    # need this for generated graphs; 100.gr is old format?
    
    min_edge_line_ignored = graph_file.readline()
    count += 1
    #logger.debug("min_edge_line{}: {}".format(count, min_edge_line_ignored.strip()))
    max_edge_line_ignored = graph_file.readline()
    count += 1
    #logger.debug("max_edge_line{}: {}".format(count, max_edge_line_ignored.strip()))
    
    vertices_edges_line = graph_file.readline()
    count += 1
    logger.debug("vertices_edges_line{}: {}".format(count, vertices_edges_line.strip()))

    words = vertices_edges_line.split(' ')
    logger.debug("nodes:" + words[2] + " edges:" + words[3])
    global num_nodes
    num_nodes = int(words[2])
    global num_edges
    num_edges = int(words[3])
    logger.info("input_file: read: num_nodes:" + str(num_nodes) + " num_edges:" + str(num_edges))

    # if num_nodes is 100, this fills nodes[0] ... nodes[100], length of nodes is 101
    # Note: nodes[0] is not used, 
    for x in range(num_nodes+1):
        nodes.append(Node(x))

    num_parent_appends = 0
    num_children_appends = 0
    num_self_loops = 0

    while True:
        count += 1
    
        # Get next line from file
        line = graph_file.readline()
        # if line is empty
        # end of file is reached
        if not line:
            break
        words = line.split(' ')
        source = int(words[1])
        target = int(words[2])
        if source == target:
            logger.debug("[Warning]: self loop: " + str(source) + " -->" + str(target))
            num_self_loops += 1
            continue
        #logger.debug("target:" + str(target))
        #if target == 101:
        #    logger.debug("target is 101")
        #rhc: 101 is a sink, i.e., it has no children so it will not appear as a source
        # in the file. Need to append a new node if target is out of range, actually 
        # append target - num_nodes. Is this just a coincidence that sink is node 100+1
        # where the gaph is supposed to have 100 nodes?

        # Example: num_nodes is 100 and target is 101, so 101 > 100.
        # But nodes is filled from nodes[0] ... nodes[100] so len(nodes) is 101
        #if (target == 101):
        #    logger.debug ("target is 101, num_nodes is " + str(num_nodes) + " len nodes is "
        #       + str(len(nodes)))
        if target > num_nodes:
            # If len(nodes) is 101 and num_nodes is 100 and we have a tatget of
            # 101, which is a sink, i.e., parents but no children, then there is 
            # no source 101. We use target+1, where 101 - num_nodes = 101 - 100 - 1
            # and target+1 = 101+1 = 102 - len(nodes) = 101 - 101 - 1, so we get 
            # the number_of_nodes_to_append to be 1, as needed.
            if len(nodes) < target+1:
                number_of_nodes_to_append = target - num_nodes
                logger.debug("number_of_nodes_to_append:" + str(number_of_nodes_to_append))
                # in our example, number_of_nodes_to_append = 1 so i starts
                # with 0 (default) and ends with number_of_nodes_to_append-1 = 0
                for i in range(number_of_nodes_to_append):
                    logger.debug("Node(" + str(num_nodes+i+1) + ")")
                    # new node ID for our example is 101 = num_nodes+i+1 = 100 + 0 + 1 = 101
                    nodes.append(Node((num_nodes+i+1)))
                num_nodes += number_of_nodes_to_append
        #logger.debug ("source:" + str(source) + " target:" + str(target))
        source_node = nodes[source]
        source_node.children.append(target)
        num_children_appends += 1
        target_node = nodes[target]
        target_node.parents.append(source)
        num_parent_appends +=  1

        # Only visualize small graphs
        #temp = [source,target]
        #visual.append(temp)
    
        #logger.debug("Line {}: {}".format(count, line.strip()))

    """
    source_node = nodes[1]
    logger.debug("Node1 children:")
    for child in source_node.children:
        logger.debug(child)
    logger.debug("Node1 parents:")
    for parent in source_node.parents:
        logger.debug(parent)

    source_node = nodes[7]
    logger.debug("Node7 children:")
    for child in source_node.children:
        logger.debug(child)
    logger.debug("Node7 parents:")
    for parent in source_node.parents:
        logger.debug(parent)
    """

    count_child_edges = 0
    i = 1
    while i <= num_nodes:
        node = nodes[i]

#rhc: Too: Note: nodes has num_children so we can use the same pagerank
# computation on a Node that we do on a partition_node. A Node does not 
# really need num_children.
        node.num_children = len(node.children)


        #logger.debug (str(i) + ": get children: " + str(len(node.children)))
        count_child_edges += len(node.children)
        i += 1
    logger.debug("num edges in graph: " + str(num_edges) + " = num child edges: " 
        + str(count_child_edges) + " + num_self_loops: " + str(num_self_loops))
    if not ((num_edges - num_self_loops) == count_child_edges):
        logger.error("[Error]: num child edges in graph is " + str(count_child_edges) + " but edges in file is "
            + str(num_edges))

    count_parent_edges = 0
    i = 1
    while i <= num_nodes:
        node = nodes[i]
        #logger.debug (str(i) + ": get parents: " + str(len(node.parents)))
        count_parent_edges += len(node.parents)
        i += 1

    logger.debug("num_edges in graph: " + str(num_edges) + " = num parent edges: " 
        + str(count_parent_edges) + " + num_self_loops: " + str(num_self_loops))
    if not ((num_edges - num_self_loops) == count_parent_edges):
        logger.error("[Error]: num parent edges in graph is " + str(count_parent_edges) + " but edges in file is "
        + str(num_edges))

    logger.debug("num_parent_appends:" + str(num_parent_appends))
    logger.debug("num_children_appends:" + str(num_children_appends))
    logger.debug("num_self_loops: " + str(num_self_loops))
    if num_self_loops > 0:
        save_num_edges = num_edges
        num_edges -= + num_self_loops
        logger.debug("old num_edges: " + str(save_num_edges) + " num_edges: " + str(num_edges))
    else:
        logger.debug("num_edges: " + str(num_edges))

    graph_file.close()

def output_partitions():
    for name, partition in zip(group_names, groups):
            with open('./'+name + '.pickle', 'wb') as handle:
                cloudpickle.dump(partition, handle) #, protocol=pickle.HIGHEST_PROTOCOL)  
  
def input_partitions():
    group_inputs = []
    for name in group_names:
        with open('./'+name+'.pickle', 'rb') as handle:
            group_inputs.append(cloudpickle.load(handle))
    logger.info("Group Nodes w/parents:")
    for group in groups:
        for node in group:
            #logger.info(node,end=":")
            print_val = str(node) + ":"
            for parent in node.parents:
                print_val += str(parent) + " "
                #logger.info(parent,end=" ")
            logger.info(print_val)
            logger.info("")
        logger.info("")
    logger.info("Group Nodes w/Frontier parent tuples:")
    for group in groups:
        for node in group:
            #logger.info(node,end=":")
            print_val = str(node) + ":"
            for tup in node.frontier_parents:
                print_val += str(tup) + " "
                # logger.info(tup,end=" ")
            logger.info(print_val)
            logger.info("")
        logger.info("")
  
# Driver Code

# if USING_BFS is true then when we logger.info SCC components we will 
# map the scc IDs back to Node IDs. Kluge for now.
USING_BFS = True

logger.debug("Following is the Breadth-First Search")
input_graph()
logger.debug("num_nodes after input graph: " + str(num_nodes))
#visualize()
#input('Press <ENTER> to continue')

"""
G = nx.DiGraph()
G.add_edges_from(visual)
logger.debug(nx.is_connected(G))
"""
def PageRank_Function_Main(nodes,total_num_nodes):
    if (debug_pagerank):
        logger.debug("PageRank_Function output partition_or_group (node:parents):")
        for node in nodes:
            #logger.debug(node,end=":")
            print_val = str(node) + ":"
            for parent in node.parents:
                print_val += str(parent) + " "
                #logger.debug(parent,end=" ")
            if len(node.parents) == 0:
                #logger.debug(",",end=" ")
                print_val += ", "
            else:
                #logger.debug(",",end=" ")
                print_val += ", "
            logger.debug(print_val)
        logger.debug("")
        logger.debug("PageRank_Function output partition_or_group (node:num_children):")
        print_val = ""
        for node in nodes:
            print_val += str(node)+":"+str(node.num_children) + ", "
            # logger.debug(str(node)+":"+str(node.num_children),end=", ")
        logger.debug(print_val)
        logger.debug("")
        logger.debug("")
        # node's children set when the partition/grup node created

    damping_factor=0.15
    random_jumping = damping_factor / total_num_nodes
    one_minus_dumping_factor = 1 - damping_factor

    iteration = int(1000)

    num_nodes_for_pagerank_computation = len(nodes)

    for index in range(num_nodes_for_pagerank_computation):
        nodes[index].prev = (1/total_num_nodes)

    for i in range(1,iteration+1): # if 10 iterations then i ranges from 1 to 10
        if (debug_pagerank):
            logger.debug("***** PageRank: iteration " + str(i))
            logger.debug("")

        for index in range(1,num_nodes_for_pagerank_computation):
            nodes[index].update_PageRank_of_PageRank_Function_loop(nodes, 
                damping_factor,one_minus_dumping_factor,random_jumping,total_num_nodes)
        for index in range(1,num_nodes_for_pagerank_computation):
            nodes[index].prev = nodes[index].pagerank
    
    print("PageRank result:")
    for i in range(num_nodes_for_pagerank_computation):
        print(str(nodes[i].ID) + ":" + str(nodes[i].pagerank))
    print()
    print()

PageRank_Function_Main(nodes,num_nodes)
# where if we input 20 nodes, nodes[] has Nodes in nodes[0] .. nodes[21]
# and nodes[] has a length of 21.
# The pagernk computation is the range:
# for index in range(1,num_nodes) so from Node 1 to Node 20, where num_nodes is 21.

#Note:
#Informs the logging system to perform an orderly shutdown by flushing 
#and closing all handlers. This should be called at application exit and no 
#further use of the logging system should be made after this call.
#logging.shutdown()
#time.sleep(3)   #not needed due to shutdwn
#os._exit(0)

#bfs(visited, graph, '5')    # function calling
# example: num_nodes = 100, so Nodes in nodes[1] to nodes[100]
# i start = 1 as nodes[0] not used, i end is (num_nodes+1) - 1  = 100
for i in range(1,num_nodes+1):
    if i not in visited:
        logger.debug("*************Driver call BFS " + str(i))
        #bfs(visited, graph, nodes[i])    # function calling
        bfs(visited, nodes[i])    # function calling

if len(current_partition) > 0:
    logger.debug("BFS: create final sub-partition")
    # does not require a deepcop
    partitions.append(current_partition.copy())
    current_partition = []

    groups.append(current_group)
    current_group = []

    nodeIndex_to_partitionIndex_maps.append(nodeIndex_to_partitionIndex_map)
    nodeIndex_to_partitionIndex_map = {}
    nodeIndex_to_groupIndex_maps.append(nodeIndex_to_groupIndex_map)
    nodeIndex_to_groupIndex_map = {}

    #global total_loop_nodes_added
    total_loop_nodes_added += loop_nodes_added
    loop_nodes_added = 0
    # does not require a deepcopy
    frontiers.append(frontier.copy())
    frontier_cost = "atEnd:" + str(len(frontier))
    frontier_costs.append(frontier_cost)
else:
    # always do this - below we assert final frontier is empty
    # does not require a deepcop
    frontiers.append(frontier.copy())

def generate_DAG_info():
    #Given Partition_senders, Partition_receivers, Group_senders, Group_receievers

#rhc: ToDo: Do we want to use collapse? fanin? If so, one task will input
# its partition/grup and then input the collapse/fanin group, etc. Need
# to clear the old partition/group before doing next?
# If we pre-load the partitions, thn we would want to do fanouts/faninNBs
# so we can use the pre-loaded partition?

    Partition_all_fanout_task_names = set()
    Partition_all_fanin_task_names = set()
    Partition_all_faninNB_task_names = set()
    Partition_all_collapse_task_names = set()
    Partition_all_fanin_sizes = []
    Partition_all_faninNB_sizes = []

    Partition_DAG_leaf_tasks = []
    Partition_DAG_leaf_task_start_states = []
    # no inputs for leaf tasks
    Partition_DAG_leaf_task_inputs = []
    Partition_DAG_map = {}
    Partition_DAG_states = {}
    Partition_DAG_tasks = {}

    """
    print()
    print("Partition_loops:" + str(Partition_loops))
    Partition_senders_Copy = Partition_senders.copy()
    for sender_name,receiver_name_set in Partition_senders_Copy.items():
        print("sender_name:" + sender_name + " receiver_name_set: " + str(receiver_name_set))
        receiver_name_set_new = set()
        for receiver_name in receiver_name_set:
            if receiver_name in Partition_loops:
                receiver_name_with_L_at_end = str(receiver_name) + "L"
                receiver_name_set_new.add(receiver_name_with_L_at_end)
            else:
                receiver_name_set_new.add(receiver_name)
        Partition_senders[sender_name] = receiver_name_set_new
        if sender_name in Partition_loops: 
            sender_name_with_L_at_end = str(sender_name) + "L"
            Partition_senders[sender_name_with_L_at_end] = Partition_senders[sender_name]
            del Partition_senders[sender_name]

    Partition_receivers_Copy = Partition_receivers.copy()
    for receiver_name,sender_name_set in Partition_receivers_Copy.items():
        print("receiver_name:" + receiver_name + " sender_name_set: " + str(sender_name_set))
        sender_name_set_new = set()
        for sender_name in sender_name_set:
            if sender_name in Partition_loops:
                sender_name_with_L_at_end = str(sender_name) + "L"
                sender_name_set_new.add(sender_name_with_L_at_end)
            else:
                sender_name_set_new.add(sender_name)
        Partition_receivers[receiver_name] = sender_name_set_new
        if receiver_name in Partition_loops: 
            receiver_name_with_L_at_end = str(receiver_name) + "L"
            Partition_receivers[receiver_name_with_L_at_end] = Partition_receivers[receiver_name]
            del Partition_receivers[receiver_name]
    """

    print("Partition_senders:")
    for sender_name,receiver_name_set in Partition_senders.items():
        print("sender:" + sender_name)
        print("receiver_name_set:" + str(receiver_name_set))

    print("Partition_receivers:")
    for receiver_name,sender_name_set in Partition_receivers.items():
        print("receiver:" + receiver_name)
        print("sender_name_set:" + str(sender_name_set))

    """
    print()
    print("Group_loops:" + str(Group_loops))
    Group_senders_Copy = Group_senders.copy()
    for sender_name,receiver_name_set in Group_senders_Copy.items():
        print("sender_name:" + sender_name + " receiver_name_set: " + str(receiver_name_set))
        receiver_name_set_new = set()
        for receiver_name in receiver_name_set:
            if receiver_name in Group_loops:
                receiver_name_with_L_at_end = str(receiver_name) + "L"
                receiver_name_set_new.add(receiver_name_with_L_at_end)
            else:
                receiver_name_set_new.add(receiver_name)
        Group_senders[sender_name] = receiver_name_set_new
        if sender_name in Group_loops: 
            sender_name_with_L_at_end = str(sender_name) + "L"
            Group_senders[sender_name_with_L_at_end] = Group_senders[sender_name]
            del Group_senders[sender_name]

    Group_receivers_Copy = Group_receivers.copy()
    for receiver_name,sender_name_set in Group_receivers_Copy.items():
        print("receiver_name:" + receiver_name + " sender_name_set: " + str(sender_name_set))
        sender_name_set_new = set()
        for sender_name in sender_name_set:
            if sender_name in Group_loops:
                sender_name_with_L_at_end = str(sender_name) + "L"
                sender_name_set_new.add(sender_name_with_L_at_end)
            else:
                sender_name_set_new.add(sender_name)
        Group_receivers[receiver_name] = sender_name_set_new
        if receiver_name in Group_loops: 
            receiver_name_with_L_at_end = str(receiver_name) + "L"
            Group_receivers[receiver_name_with_L_at_end] = Group_receivers[receiver_name]
            del Group_receivers[receiver_name]
    """
    print()
    print("Group_senders:")
    for sender_name,receiver_name_set in Group_senders.items():
        print("sender:" + sender_name)
        print("receiver_name_set:" + str(receiver_name_set))

    print("Group_receivers:")
    for receiver_name,sender_name_set in Group_receivers.items():
        print("receiver:" + receiver_name)
        print("sender_name_set:" + str(sender_name_set))


    #Note:
    #Informs the logging system to perform an orderly shutdown by flushing 
    #and closing all handlers. This should be called at application exit and no 
    #further use of the logging system should be made after this call.
    #logging.shutdown()
    #time.sleep(3)   #not needed due to shutdwn
    #os._exit(0)

    """
    for group_name in Group_loops:
        group_name_with_L_at_end += str(group_name) + "L"
        Group_senders[group_name_with_L_at_end] = Group_senders[group_name]
        del Group_senders[group_name]
    """

#rhc: ToDo: use the loop map to change the nambes in the 
# Partition/roup senders and receivers.
#dictionary[new_key] = dictionary[old_key]
#del dictionary[old_key]

    # sink nodes, i.e., nodes that do not send any inputs
    Partition_sink_set = set()
    logger.info("Partition DAG:")
    state = 1
    # partition i has a collapse to partition i+1
    # Task senderX sends inputs to one or more other tasks
    for senderX in Partition_senders:
        fanouts = []
        faninNBs = []
        fanins = []
        collapse = []
        fanin_sizes = []
        faninNB_sizes = []
        # tasks that receive inputs from senderX
        receiver_set_for_senderX = Partition_senders[senderX]
        # task receiverY receives inputs from other tasks (all tasks receive
        # inputs from other tasks except leaf tasks)
        for receiverY in receiver_set_for_senderX:
            sender_set_for_receiverY = Partition_senders.get(receiverY)
            if sender_set_for_receiverY == None:
                Partition_sink_set.add(receiverY)
            # tasks that send inputs to receiverY
            sender_set_for_receiverY = Partition_receivers[receiverY]
            length_of_sender_set_for_receiverY = len(sender_set_for_receiverY)
            length_of_receiver_set_for_senderX = len(receiver_set_for_senderX)
            if length_of_sender_set_for_receiverY == 1:
                # collapse or fanout as receiverY receives on input
                if length_of_receiver_set_for_senderX == 1:
                    # only one task sends input to receiverY and this sending 
                    # task only sends to one task, so collapse receiverY, i.e.,
                    # senderX becomes receiverY
                    logger.info("sender " + senderX + " --> " + receiverY + " : Collapse")
                    if not receiverY in Partition_all_collapse_task_names:
                        Partition_all_collapse_task_names.add(receiverY)
                    else:
                        pass # error only one task can collapse a given task
                    collapse.append(receiverY)
                else:
                    # only one task sends input to receiverY and this sending 
                    # task sends to other tasks too, so senderX does a fanout 
                    # to receiverY         
                    logger.info("sender " + senderX + " --> " + receiverY + " : Fanout")
                    if not receiverY in Partition_all_fanout_task_names:
                        Partition_all_fanout_task_names.add(receiverY)
                    fanouts.append(receiverY)
            else:
                # fanin or fannNB since receiverY receives inputs from multiple tasks
                isFaninNB = False
                 # senderZ sends an input to receiverY
                for senderZ in sender_set_for_receiverY:
                    # tasks to which senderX sends an input
                    receiver_set_for_senderZ = Partition_senders[senderZ]
                    # since senderX sends inputs to more than one task, receiverY
                    # is a faninNB task (as senderX cannot become receiverY)
                    if len(receiver_set_for_senderZ) > 1:
                        # if any task sends inputs to reciverY and any other task(s)
                        # then receiverY must be a faninNB task since some sender cannot 
                        # become receiverY.
                        isFaninNB = True
                        break
                if isFaninNB:
                    logger.info("sender " + senderX + " --> " + receiverY + " : FaninNB")
                    if not receiverY in Partition_all_faninNB_task_names:
                        Partition_all_faninNB_task_names.add(receiverY)
                        Partition_all_faninNB_sizes.append(length_of_sender_set_for_receiverY)
                    faninNBs.append(receiverY)
                    faninNB_sizes.append(length_of_sender_set_for_receiverY)
                else:
                    # senderX sends an input only to receiverY, same for any other
                    # tasks that sends inputs to receiverY so receiverY is a fanin task.
                    logger.info("sender " + senderX + " --> " + receiverY + " : Fanin")
                    if not receiverY in Partition_all_fanin_task_names:
                        Partition_all_fanin_task_names.add(receiverY)
                        Partition_all_fanin_sizes.append(length_of_sender_set_for_receiverY)
                    fanins.append(receiverY)
                    fanin_sizes.append(length_of_sender_set_for_receiverY)

        # get the tasks that send to senderX, i.e., provide inputs for senderX
        sender_set_for_senderX = Partition_receivers.get(senderX)
        if sender_set_for_senderX == None:
            # senderX is a leaf task since it is not a receiver
            Partition_DAG_leaf_tasks.append(senderX)
            Partition_DAG_leaf_task_start_states.append(state)
            task_inputs = ()
            Partition_DAG_leaf_task_inputs.append(task_inputs)
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
            for name in sender_set_for_senderX:
                qualified_name = str(name) + "-" + str(senderX)
                sender_set_for_senderX_with_qualified_names.add(qualified_name)
            # sender_set_for_senderX provides input for senderX
            task_inputs = tuple(sender_set_for_senderX_with_qualified_names)
        Partition_DAG_map[state] = state_info(senderX, fanouts, fanins, faninNBs, collapse, fanin_sizes, faninNB_sizes, task_inputs)
        Partition_DAG_states[senderX] = state

        state += 1

    # Finish by doing the receivers that are not senders (opposite of leaf tasks);
    # these are reeivers tht send no nputs to other tasks. They have no fanins/
    # faninBs, fanouts or collapses, but they do have task inputs.
    for receiverY in Partition_sink_set: # Partition_receivers:
        #if not receiverY in Partition_DAG_states:
        fanouts = []
        faninNBs = []
        fanins = []
        collapse = []
        fanin_sizes = []
        faninNB_sizes = []

        sender_set_for_receiverY = Partition_receivers[receiverY]
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
        # for each task senderX that sends input to receiverY, the 
        # qualified name of the sender is senderX+"-"+senderX
        for senderX in sender_set_for_receiverY:
            qualified_name = str(senderX) + "-" + str(receiverY)
            sender_set_for_receiverY_with_qualified_names.add(qualified_name)
        # sender_set_for_senderX provides input for senderX
        task_inputs = tuple(sender_set_for_receiverY_with_qualified_names)

        Partition_DAG_map[state] = state_info(receiverY, fanouts, fanins, faninNBs, collapse, fanin_sizes, faninNB_sizes, task_inputs)
        Partition_DAG_states[receiverY] = state
        state += 1

    for key in Partition_DAG_states:
        Partition_DAG_tasks[key] = PageRank_Function_Driver

    logger.info("")
    DAG_info = {}
    DAG_info["DAG_map"] = Partition_DAG_map
    DAG_info["DAG_states"] = Partition_DAG_states
    DAG_info["DAG_leaf_tasks"] = Partition_DAG_leaf_tasks
    DAG_info["DAG_leaf_task_start_states"] = Partition_DAG_leaf_task_start_states
    DAG_info["DAG_leaf_task_inputs"] = Partition_DAG_leaf_task_inputs
    DAG_info["all_fanout_task_names"] = Partition_all_fanout_task_names
    DAG_info["all_fanin_task_names"] = Partition_all_fanin_task_names
    DAG_info["all_faninNB_task_names"] = Partition_all_faninNB_task_names
    DAG_info["all_collapse_task_names"] = Partition_all_collapse_task_names
    DAG_info["all_fanin_sizes"] = Partition_all_fanin_sizes
    DAG_info["all_faninNB_sizes"] = Partition_all_faninNB_sizes
    DAG_info["DAG_tasks"] = Partition_DAG_tasks

    file_name = "./DAG_info_Partition.pickle"
    with open(file_name, 'wb') as handle:
        cloudpickle.dump(DAG_info, handle) #, protocol=pickle.HIGHEST_PROTOCOL)  

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
    logger.info("Partition_all_fanout_task_names:")
    for name in Partition_all_fanout_task_names:
        logger.info(name)
    logger.info
    logger.info("all_fanin_task_names:")
    for name in Partition_all_fanin_task_names :
        logger.info(name)
    logger.info("")
    logger.info("all_fanin_sizes:")
    for s in Partition_all_fanin_sizes :
        logger.info(s)
    logger.info("")
    logger.info("all_faninNB_task_names:")
    for name in Partition_all_faninNB_task_names:
        logger.info(name)
    logger.info("")
    logger.info("all_faninNB_sizes:")
    for s in Partition_all_faninNB_sizes:
        logger.info(s)
    logger.info("")
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

    DAG_info_partition_read = DAG_Info(file_name = "./DAG_info_Partition.pickle")
    
    DAG_map = DAG_info_partition_read.get_DAG_map()
    all_fanin_task_names = DAG_info_partition_read.get_all_fanin_task_names()
    all_fanin_sizes = DAG_info_partition_read.get_all_fanin_sizes()
    all_faninNB_task_names = DAG_info_partition_read.get_all_faninNB_task_names()
    all_faninNB_sizes = DAG_info_partition_read.get_all_faninNB_sizes()
    all_fanout_task_names = DAG_info_partition_read.get_all_fanout_task_names()
    # Note: all fanout_sizes is not needed since fanouts are fanins that have size 1
    DAG_states = DAG_info_partition_read.get_DAG_states()
    DAG_leaf_tasks = DAG_info_partition_read.get_DAG_leaf_tasks()
    DAG_leaf_task_start_states = DAG_info_partition_read.get_DAG_leaf_task_start_states()
    DAG_tasks = DAG_info_partition_read.get_DAG_tasks()

    DAG_leaf_task_inputs = DAG_info_partition_read.get_DAG_leaf_task_inputs()

    logger.info("")
    logger.info("DAG_info partition after read:")
    output_DAG = True
    # add-0bec4d19-bce6-4394-ad62-9b0eab3081a9
    if output_DAG:
        # FYI:
        logger.info("DAG_map:")
        for key, value in DAG_map.items():
            logger.info(key)
            logger.info(value)
        logger.info("  ")
        logger.info("DAG states:")         
        for key, value in DAG_states.items():
            logger.info(key)
            logger.info(value)
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
        #logger.info("") 
        logger.info("")

    Group_all_fanout_task_names = set()
    Group_all_fanin_task_names = set()
    Group_all_faninNB_task_names = set()
    Group_all_collapse_task_names = set()
    Group_all_fanin_sizes = []
    Group_all_faninNB_sizes = []

    Group_DAG_leaf_tasks = []
    Group_DAG_leaf_task_start_states = []
    # no inputs for leaf tasks
    Group_DAG_leaf_task_inputs = []
    Group_DAG_map = {}
    Group_DAG_states = {}
    Group_DAG_tasks = {}

#rhc: ToDo: use the loop map to change the nambes in the 
# Partition/roup senders and receivers.
#dictionary[new_key] = dictionary[old_key]
#del dictionary[old_key]

    # sink nodes, i.e., nodes that do not send any inputs
    Group_sink_set = set()
    logger.info("Group DAG:")
    state = 1
    for senderX in Group_senders:
        logger.info("senderX: " + senderX)
        fanouts = []
        fanins = []
        faninNBs = []
        collapse = []
        fanin_sizes = []
        faninNB_sizes = []
        receiver_set_for_senderX = Group_senders[senderX]
        for receiverY in receiver_set_for_senderX:
            sender_set_for_receiverY = Group_senders.get(receiverY)
            if sender_set_for_receiverY == None:
                Group_sink_set.add(receiverY)
            sender_set_for_receiverY = Group_receivers[receiverY]
            length_of_sender_set_for_receiverY = len(sender_set_for_receiverY)
            length_of_receiver_set_for_senderX = len(receiver_set_for_senderX)
            if length_of_sender_set_for_receiverY == 1:
                # collapse or fanout
                if length_of_receiver_set_for_senderX == 1:
                    # only one task sends input to receiverY and this sending 
                    # task only sends to one task, so collapse receiverY, i.e.,
                    # senderX becomes receiverY
                    logger.info("sender " + senderX + " --> " + receiverY + " : Collapse")
                    if not receiverY in Group_all_collapse_task_names:
                        Group_all_collapse_task_names.add(receiverY)
                    else:
                        pass # this is an error, only one task can collapse a given task
                    collapse.append(receiverY)
                else:
                    # only one task sends input to receiverY and this sending 
                    # task sends to other tasks too, so senderX does a fanout 
                    # to receiverY   
                    logger.info("sender " + senderX + " --> " + receiverY + " : Fanout")
                    if not receiverY in Group_all_fanout_task_names:
                        Group_all_fanout_task_names.add(receiverY)
                    fanouts.append(receiverY)
            else:
                # fanin or fannNB since receiverY receives inputs from multiple tasks
                isFaninNB = False
                 # senderZ sends an input to receiverY
                for senderZ in sender_set_for_receiverY:
                    # tasks to which senderX sends an input
                    receiver_set_for_senderZ = Group_senders[senderZ]
                    # since senderX sends inputs to more than one task, receiverY
                    # is a faninNB task (as senderX cannot become receiverY)
                    if len(receiver_set_for_senderZ) > 1:
                        # if any task sends inputs to reciverY and any other task(s)
                        # then receiverY must be a faninNB task since some sender cannot 
                        # become receiverY.
                        isFaninNB = True
                        break
                if isFaninNB:
                    logger.info("sender " + senderX + " --> " + receiverY + " : FaninNB")
                    if not receiverY in Group_all_faninNB_task_names:
                        Group_all_faninNB_task_names.add(receiverY)
                        Group_all_faninNB_sizes.append(length_of_sender_set_for_receiverY)
                    logger.info ("after Group_all_faninNBs_sizes append: " + str(Group_all_faninNB_sizes))
                    logger.info ("faninNBs append: " + receiverY)
                    faninNBs.append(receiverY)
                    faninNB_sizes.append(length_of_sender_set_for_receiverY)
                else:
                    # senderX sends an input only to receiverY, same for any other
                    # tasks that sends inputs to receiverY so receiverY is a fanin task.
                    logger.info("sender " + senderX + " --> " + receiverY + " : Fanin")
                    if not receiverY in Group_all_fanin_task_names:
                        Group_all_fanin_task_names.add(receiverY)
                        Group_all_fanin_sizes.append(length_of_sender_set_for_receiverY)
                    fanins.append(receiverY)
                    fanin_sizes.append(length_of_sender_set_for_receiverY)

        # get the tasks that send to senderX, i.e., provide inputs for senderX
        sender_set_for_senderX = Group_receivers.get(senderX)
        if sender_set_for_senderX == None:
            # senderX is a leaf task since it is not a receiver
            Group_DAG_leaf_tasks.append(senderX)
            Group_DAG_leaf_task_start_states.append(state)
            task_inputs = ()
            Group_DAG_leaf_task_inputs.append(task_inputs)
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
        Group_DAG_map[state] = state_info(senderX, fanouts, fanins, faninNBs, collapse, fanin_sizes, faninNB_sizes, task_inputs)
        Group_DAG_states[senderX] = state

        state += 1

    # Finish by doing the receivers that are not senders (opposite of leaf tasks);
    # these are reeivers tht send no nputs to other tasks. They have no fanins/
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

    for key in Group_DAG_states:
        Group_DAG_tasks[key] = PageRank_Function_Driver

    logger.info("")
    DAG_info = {}
    DAG_info["DAG_map"] = Group_DAG_map
    DAG_info["DAG_states"] = Group_DAG_states
    DAG_info["DAG_leaf_tasks"] = Group_DAG_leaf_tasks
    DAG_info["DAG_leaf_task_start_states"] = Group_DAG_leaf_task_start_states
    DAG_info["DAG_leaf_task_inputs"] = Group_DAG_leaf_task_inputs
    DAG_info["all_fanout_task_names"] = Group_all_fanout_task_names
    DAG_info["all_fanin_task_names"] = Group_all_fanin_task_names
    DAG_info["all_faninNB_task_names"] = Group_all_faninNB_task_names
    DAG_info["all_collapse_task_names"] = Group_all_collapse_task_names
    DAG_info["all_fanin_sizes"] = Group_all_fanin_sizes
    DAG_info["all_faninNB_sizes"] = Group_all_faninNB_sizes
    DAG_info["DAG_tasks"] = Group_DAG_tasks

    file_name = "./DAG_info_Group.pickle"
    with open(file_name, 'wb') as handle:
        cloudpickle.dump(DAG_info, handle) #, protocol=pickle.HIGHEST_PROTOCOL)  

    num_fanins = len(Group_all_fanin_task_names)
    num_fanouts = len(Group_all_fanout_task_names)
    num_faninNBs = len(Group_all_faninNB_task_names)
    num_collapse = len(Group_all_collapse_task_names)

    logger.info("GroupDAG_map:")
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
    logger.info("")
    logger.info("all_fanin_task_names:")
    for name in Group_all_fanin_task_names :
        logger.info(name)
    logger.info("")
    logger.info("all_fanin_sizes:")
    for s in Group_all_fanin_sizes :
        logger.info(s)
    logger.info("")
    logger.info("all_faninNB_task_names:")
    for name in Group_all_faninNB_task_names:
        logger.info(name)
    logger.info("")
    logger.info("all_faninNB_sizes:")
    for s in Group_all_faninNB_sizes :
        logger.info(s)
    logger.info("")
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

    DAG_info_partition_read = DAG_Info(file_name = "./DAG_info_Group.pickle")
    
    DAG_map = DAG_info_partition_read.get_DAG_map()
    all_fanin_task_names = DAG_info_partition_read.get_all_fanin_task_names()
    all_fanin_sizes = DAG_info_partition_read.get_all_fanin_sizes()
    all_faninNB_task_names = DAG_info_partition_read.get_all_faninNB_task_names()
    all_faninNB_sizes = DAG_info_partition_read.get_all_faninNB_sizes()
    all_fanout_task_names = DAG_info_partition_read.get_all_fanout_task_names()
    # Note: all fanout_sizes is not needed since fanouts are fanins that have size 1
    DAG_states = DAG_info_partition_read.get_DAG_states()
    DAG_leaf_tasks = DAG_info_partition_read.get_DAG_leaf_tasks()
    DAG_leaf_task_start_states = DAG_info_partition_read.get_DAG_leaf_task_start_states()
    DAG_tasks = DAG_info_partition_read.get_DAG_tasks()

    DAG_leaf_task_inputs = DAG_info_partition_read.get_DAG_leaf_task_inputs()

    logger.info("")
    logger.info("DAG_info group after read:")
    output_DAG = True
    # add-0bec4d19-bce6-4394-ad62-9b0eab3081a9
    if output_DAG:
        # FYI:
        logger.info("DAG_map:")
        for key, value in DAG_map.items():
            logger.info(key)
            logger.info(value)
        logger.info("  ")
        logger.info("DAG states:")         
        for key, value in DAG_states.items():
            logger.info(key)
            logger.info(value)
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
        #logger.info("") 
        logger.info("")



    """

    Perhaps:
        group_name_list = ["PR1_1", "PR2_1", "PR2_2", "PR2_3", "PR3_1", "PR3_2"]
        DAG_tasks = dict.fromkeys(key_list,PageRank)
    partition_name_list = ["PR1_1", "PR2_1", "PR3_1"]
    where:
        def func(value=i):
            logger.info value
        funcs.append(func)
    where:
    #new_func='def receiverY(task_name, set, input2):\n  return x+1'
    """

    """
    first = True
    comma = ""
    receiverY = "PR2_1"
    PageRank_func = "def " + receiverY + "(task_name, "
    #for receiverY in Receivers:
    sender_set_for_receiverY = Partition_receivers[receiverY]
    for senderZ in sender_set_for_receiverY:
        if first:
            pass
        else:
            comma = ","
            first = False
        PageRank_func += comma + str(senderZ)
        PageRank_func += "):\n  logger.info(1)"
        #where FOOO is a simple body for PageRank_task, which calls the actual task
    #or
    #for i, senderZ in enumerate(sender_set_for_receiverY):
    #if i: new_func += "," + str(senderZ)
    #else: new_func += str(senderZ)
    logger.info("PageRank_func: ")
    logger.info(PageRank_func)
    the_code=compile(PageRank_func,'<string>','exec')
    """ 

# We just reuses the DAG_executor and DAG_executor_processes with work loop.
# DAG_executor_workloop_pagerank(...): No. Executing DAG so no changes
# to the work loop
# - get nodes from payload: No, one payload per worker not per task
#
#   So PankRank should do this
# - partition_file_name = "./"+task_name+".pickle"
# - partition = input_PageRank_nodes_and_partition(partition_file_name)
#   For now, input both.
# - PageRank(nodes,partition)

# Called by DAG task to read its partition from storage.
# DAG with name task_name calls:
#    partition_file_name = "./"+task_name+".pickle"
#    partition = input_PageRank_partition(partition_file_name)
def input_PageRank_nodes_and_partition(partition_file_name):
    # Example file name: './PA1_partition.pickle'
    with open(partition_file_name, 'rb') as handle:
        nodes_and_partition = cloudpickle.load(handle)
    nodes = nodes_and_partition["nodes"]
    partition = nodes_and_partition["partition"]
    # Example partition, for "PR1" of graph_20: [5, 17, 1]
    return nodes, partition

def normalize_PageRank(nodes):
    pagerank_sum = sum(node.pagerank for node in nodes)
    for node in nodes:
        node.pagerank /= pagerank_sum

def PageRank_one_iter(target_nodes,partition,damping_factor):
    for target_node_index in target_nodes:
        nodes[target_node_index].update_PageRank_main(damping_factor, len(nodes))
        logger.info("PageRank: target_index isShadowNode: " 
            + str(nodes[target_node_index].isShadowNode))
    normalize_PageRank(nodes)


def PageRank_Function_one_iter(partition_or_group,damping_factor,
    one_minus_dumping_factor,random_jumping,total_num_nodes,num_nodes_for_pagerank_computation):
    #for index in range(len(partition_or_group)):
    for index in range(num_nodes_for_pagerank_computation):
        # Need number of non-shadow nodes'
#rhc: handle shadow nodes
        if partition_or_group[index].isShadowNode:
            #if (debug_pagerank):
            logger.debug("PageRank: before pagerank computation: node at position " 
            + str(index) + " isShadowNode: " 
            + str(partition_or_group[index].isShadowNode) 
            + ", pagerank: " + str(partition_or_group[index].pagerank)
            + ", parent: " + str(partition_or_group[index].parents[0])
            + ", (real) parent's num_children: " + str(partition_or_group[index].num_children)
            )
        #if (debug_pagerank):
        #    logger.debug("")

        #print(str(partition_or_group[index].ID) + " type of node: " + str(type(partition_or_group[index])))
        #if not partition_or_group[index].isShadowNode:
        partition_or_group[index].update_PageRank_of_PageRank_Function(partition_or_group, 
            damping_factor,one_minus_dumping_factor,random_jumping,total_num_nodes)

        if partition_or_group[index].isShadowNode:
            #if (debug_pagerank):
            logger.debug("PageRank:  after pagerank computation: node at position " 
            + str(index) + " isShadowNode: " 
            + str(partition_or_group[index].isShadowNode) 
            + ", pagerank: " + str(partition_or_group[index].pagerank)
            + ", parent: " + str(partition_or_group[index].parents[0])
            + ", (real) parent's num_children: " + str(partition_or_group[index].num_children)
            )
        #if (debug_pagerank):
        logger.debug("")
#rhc: ToDo: do this?
    #normalize_PageRank(nodes)

def PageRank_Function_Driver(task_file_name,total_num_nodes,results_dictionary):
    input_tuples = []
    for (_,v) in results_dictionary.items():
        # pagerank leaf tasks have no input. This results in a rresult_dictionary
        # in DAG_executor of "DAG_executor_driver_0" --> (), where
        # DAG_executor_driver_0 is used to mean that eh DAG_excutor_driver
        # provided an empty input tuple fpr the leaf task. Here, we just ignore
        # empty input tuples so that the input_tuples provided to the 
        # PageRank_Function will be an empty list.
        if not v ==  ():
            input_tuples += v
    output = PageRank_Function(task_file_name,total_num_nodes,input_tuples)
    return output


#def PageRank_Function(task_file_name,total_num_nodes,input_tuples,results):
def PageRank_Function(task_file_name,total_num_nodes,input_tuples):
        # task_file_name is, e.g., "PR1_1" not "PR1_1.pickle"
        # We check for task_file_name ending with "L" for loop below,
        # so we make this check esy by having 'L' at the end (endswith)
        # instead of having to parse ("PR1_1.pickle")
        complete_task_file_name = './'+task_file_name+'.pickle'
        with open(complete_task_file_name, 'rb') as handle:
            partition_or_group = (cloudpickle.load(handle))
        if (debug_pagerank):
            logger.debug("PageRank_Function output partition_or_group (node:parents):")
            for node in partition_or_group:
                #logger.debug(node,end=":")
                print_val = str(node) + ":"
                for parent in node.parents:
                    print_val += str(parent) + " "
                    #logger.debug(parent,end=" ")
                if len(node.parents) == 0:
                    #logger.debug(",",end=" ")
                    print_val += ", "
                else:
                    #logger.debug(",",end=" ")
                    print_val += ", "
                logger.debug(print_val)
            logger.debug("")
            logger.debug("PageRank_Function output partition_or_group (node:num_children):")
            print_val = ""
            for node in partition_or_group:
                print_val += str(node)+":"+str(node.num_children) + ", "
                # logger.debug(str(node)+":"+str(node.num_children),end=", ")
            logger.debug(print_val)
            logger.debug("")

            logger.debug("")
            # node's children set when the partition/grup node created

        num_shadow_nodes = 0
        for node in partition_or_group:
            if node.isShadowNode:
                num_shadow_nodes += 1

        #actual_num_nodes = len(partition_or_group)-num_shadow_nodes

        damping_factor=0.15
        random_jumping = damping_factor / total_num_nodes
        one_minus_dumping_factor = 1 - damping_factor

        iteration = -1
        if not task_file_name.endswith('L'):
            iteration = int(1)
        else:
            iteration = int(1000)

        num_nodes_for_pagerank_computation = len(partition_or_group)

        i=0
        for tup in input_tuples:
            logger.debug("PageRank_Function: input tuple:" + str(tup))
            shadow_node_index = tup[0]
            pagerank_value = tup[1]
            # assert
            if not partition_or_group[shadow_node_index].isShadowNode:
                logger.debug("[Error]: Internal Error: input tuple " + str(tup))
            # If shadow_node x is a shadow_node for node y (where the one or more
            # shadow nodes of y are immediatley preceeding y) then shadow_node x
            # represents a parent node of y that was in a different partition P or 
            # group G. P/G will send the pagerank value for parent to the partition
            # or group for x and y. We ser the pagerank for the shadow_node equal to this
            # received pagerank value. 
            # We will use the shadow_node's pagerank as the pagerank value for one of 
            # y's parents (there may be shadow_nodes for other parents of y and y may
            # have parents in its grup/partition). We have two choices: (1) do not compuet
            # the pagerank value of a shadow_node; this prevents the shadow_node's pagerank
            # value from changing but we need an if-statement to check whether a node ia 
            # a shadow_node. Choce (2) is to give the shadow_noe a parent node that is 
            # out of the pagerank computation's range and set the pagerank of the
            # shadow_node's parent such that when we compute the pagerank of the 
            # shadow_node we always get the same value. For this case, we avoid the
            # if-statement in the tight pagerank loop. So we avoid missed branch
            # predctions by the hardware. Noet that there is a limit to the number of
            # missed predictions allowed if out tight loop is to be considered by 
            # the loop-stream detector as a loop whose micro ops can be buffered
            # avoiding the re-decoding of the loop's machine instructions on ech 
            # iteration of the loop. (The frontend of the instruction cycle can be 
            # powered off also.)
            #
            # pagerank of shadow_node is the pagerank value (of a parent of the 
            # shadow_node received from the parents partition/group executor.
            partition_or_group[shadow_node_index].pagerank = pagerank_value
            # IDs: -1, -2, -3, etc
            shadow_node_ID = partition_or_group[shadow_node_index].ID
            parent_of_shadow_node_ID = str(shadow_node_ID) + "-s-p"
            parent_of_shadow_node = Partition_Node(parent_of_shadow_node_ID)
            # set the pagerank of the parent_of_shadow_node so that when we recompute
            # the pagerank of the shadow_node we alwas get the same value.
            parent_of_shadow_node.pagerank = (
                (partition_or_group[shadow_node_index].pagerank - random_jumping)  / one_minus_dumping_factor)
            # if (debug_pagerank):
            logger.debug(parent_of_shadow_node_ID + " pagerank set to: " + str(parent_of_shadow_node.pagerank))
            # num_children = 1 makes the computation easier; the computation assumed
            # num_children was set to 1
            parent_of_shadow_node.num_children = 1
            # appending new nodes at the end
            partition_or_group.append(parent_of_shadow_node)
            partition_or_group[shadow_node_index].parents[0] = num_nodes_for_pagerank_computation + i
            i += i+1
        if (debug_pagerank):
            logger.debug("")
            logger.debug("PageRank_Function output partition_or_group after add " + str(len(input_tuples)) + " SN parents (node:parents):")
            for node in partition_or_group:
                print_val = str(node) + ":"
                # print(node,end=":")
                for parent in node.parents:
                    #print(parent,end=" ")
                    print_val += str(parent) + " "
                if len(node.parents) == 0:
                    #print(" ,",end=" ")
                    print_val += " ,"
                else:
                    #print(",",end=" ")
                    print_val += ","
                logger.debug(print_val)
            logger.debug("")

        if task_file_name.endswith('L'):
            for index in range(num_nodes_for_pagerank_computation):
                partition_or_group[index].prev = (1/total_num_nodes)

        for i in range(1,iteration+1):
            if (debug_pagerank):
                logger.debug("***** PageRank: iteration " + str(i))
                logger.debug("")

            #PageRank_Function_one_iter(partition_or_group,damping_factor,one_minus_dumping_factor,random_jumping,total_num_nodes,num_nodes_for_pagerank_computation)
    
            for index in range(num_nodes_for_pagerank_computation):
                # Need number of non-shadow nodes'
        #rhc: handle shadow nodes
                if partition_or_group[index].isShadowNode:
                    #if (debug_pagerank):
                    logger.debug("PageRank: before pagerank computation: node at position " 
                    + str(index) + " isShadowNode: " 
                    + str(partition_or_group[index].isShadowNode) 
                    + ", pagerank: " + str(partition_or_group[index].pagerank)
                    + ", parent: " + str(partition_or_group[index].parents[0])
                    + ", (real) parent's num_children: " + str(partition_or_group[index].num_children)
                    )

                #if (debug_pagerank):
                #    logger.debug("")

                #print(str(partition_or_group[index].ID) + " type of node: " + str(type(partition_or_group[index])))
                #if not partition_or_group[index].isShadowNode:

                if not task_file_name.endswith('L'):
                    partition_or_group[index].update_PageRank_of_PageRank_Function(partition_or_group, 
                        damping_factor,one_minus_dumping_factor,random_jumping,total_num_nodes)
                else:
                    partition_or_group[index].update_PageRank_of_PageRank_Function_loop(partition_or_group, 
                        damping_factor,one_minus_dumping_factor,random_jumping,total_num_nodes)
    
                if partition_or_group[index].isShadowNode:
                    #if (debug_pagerank):
                    logger.debug("PageRank:  after pagerank computation: node at position " 
                    + str(index) + " isShadowNode: " 
                    + str(partition_or_group[index].isShadowNode) 
                    + ", pagerank: " + str(partition_or_group[index].pagerank)
                    + ", parent: " + str(partition_or_group[index].parents[0])
                    + ", (real) parent's num_children: " + str(partition_or_group[index].num_children)
                    )

                #if (debug_pagerank):
                #logger.debug("")

            if task_file_name.endswith('L'):
                for index in range(num_nodes_for_pagerank_computation):
                    partition_or_group[index].prev = partition_or_group[index].pagerank
        
        """
        if run_all_tasks_locally and using_threads_not_processes:
            logger.info("PageRanks: ")
            for i in range(num_nodes_for_pagerank_computation):
                if not partition_or_group[i].isShadowNode:
                    my_ID = str(partition_or_group[i].ID)
                    results[partition_or_group[i].ID] = partition_or_group[i].pagerank
                else:
                    my_ID = str(partition_or_group[i].ID) + "-s"
                logger.info(partition_or_group[i].toString_PageRank())
        """

        if (debug_pagerank):
            logger.debug("")
            logger.debug("Frontier Parents:")
            for i in range(len(partition_or_group)):
                if not partition_or_group[i].isShadowNode:
                    my_ID = str(partition_or_group[i].ID)
                else:
                    my_ID = str(partition_or_group[i].ID) + "-s"
                logger.debug("ID:" + my_ID + " frontier_parents: " + str(partition_or_group[i].frontier_parents))
            logger.debug("")
        """
        ID:5 frontier_parents: [(2, 1, 2)]
        ID:17 frontier_parents: [(2, 2, 5)]
        ID:1 frontier_parents: [(2, 3, 3)]
        """
        PageRank_output = {}
        for i in range(len(partition_or_group)):
            if len(partition_or_group[i].frontier_parents) > 0:
                for frontier_parent in partition_or_group[i].frontier_parents:
                    #partition_number = frontier_parent[0]
                    #group_number = frontier_parent[1]
                    parent_or_group_index = frontier_parent[2]
                    # Passing name in tuple so that name for loop partition/groups
                    # will have an "l" at the end
                    #partition_or_group_name = "PR"+str(partition_number)+"_"+str(group_number)
                    partition_or_group_name = frontier_parent[3]
                    output_list = PageRank_output.get(partition_or_group_name)
                    if output_list == None:
                        output_list = []
                    output_tuple = (parent_or_group_index,partition_or_group[i].pagerank)
                    output_list.append(output_tuple)
                    PageRank_output[partition_or_group_name] = output_list
        #if (debug_pagerank):
        print("PageRank output tuples for " + task_file_name + ":")
        print_val = ""
        for k, v in PageRank_output.items():
            #print_val += "(%s, %s) " % (k, v)
            print((k, v),end=" ")
        #print(print_val)
        print()
        print()

        print("PageRank result for " + task_file_name + ":", end=" ")
        for i in range(num_nodes_for_pagerank_computation):
            if not partition_or_group[i].isShadowNode:
                print(str(partition_or_group[i].ID) + ":" + str(partition_or_group[i].pagerank),end=" ")
        print()
        print()
        return PageRank_output

def PageRank_Task(task_file_name,total_num_nodes,payload,results):
    input_tuples = payload['input']
    # sort inut tuples so that they are in shadow_node order, left to right.
    # The first index of tuple is index of the shadow_node in the input_tuples
    # so sort will sort on these indices resulting in shadow_node order.
    # The shadow_node parents are appended to the end of the input tuple as
    # we process the input_tuple so the order f the shadow_nodes and the 
    # shadow_node parents will be the will be the same.
    #
    # This sort is not necessary; it just helps with the visual during debugging.
    input_tuples.sort()
    #if (debug_pagerank):
    logger.debug(task_file_name + " input tuples: ")
    for tup in input_tuples:
        print(tup,end=" ")
    logger.debug("")
    logger.debug("")
    #PageRank_output = PageRank_Function(task_file_name,total_num_nodes,input_tuples,results)
    PageRank_output = PageRank_Function(task_file_name,total_num_nodes,input_tuples)
    return PageRank_output

#rhc: the actual pagerank will be working on Nodes not node indices?
# So we need a new PageRank for the DAG execution.
# The first node will be in position 0? Normally node i is in position i
# but there is no node 0 so no Node in position 0.
def PageRank_main(target_nodes, partition):
    logger.info("PageRank:partition is:" + str(partition))
    damping_factor=0.15
    iteration=int(1)
    for i in range(iteration):
        logger.info("***** PageRank: iteration " + str(i))
        logger.info("")
        PageRank_one_iter(target_nodes,partition,damping_factor)
    logger.info("PageRank: partition is: " + str(partition))

"""
rhc: ToDo: If we add one or more shadow nodes before each dependent node in 
the partition (where a dependent node is a node whose parent is in the 
previous partition), we have to either (1) add an if-statement to the 
pagernk calculation so that we do not calculate the pagerank values for 
shadow nodes, since we do not want to change the pagerank values of 
shadow nodes, or (2) we do something to ensure that the value caclulated
the shadow node is always the same as the original value. For the latter
case, we would not need an if-statement in the pagerank caclulation which 
may speed it up. 

Note: This branch would be pretty random and hard to predict
by the branch_predictor? Also, we can implement the nodes as a strct of
arrays instead of an array of structs, which minimizes the cache misses
for nodes. The PageRank calculation instructions are small and should easily
fit in the i-cache. Pagerank is a loop so we might want to try to make sure
it is being detected by the Loop Stream Detector aad is small enough to be 
executd out of the decoded micro-op cache:
https://www.anandtech.com/show/3922/intels-sandy-bridge-architecture-exposed/2
or whatever happens in the latest archtectures.

The PageRank code is:
    parent_nodes = self.parents
    # for shadow nodes, there is only one node_index and its value is i (see below)
    pagerank_sum = sum((nodes[node_index].pagerank / len(nodes[node_index].children)) for node_index in in_nodes)
    random_jumping = damping_factor / num_nodes
    logger.info("damping_factor:" + str(damping_factor) + " num_nodes: " + str(num_nodes) + " random_jumping: " + str(random_jumping))
    self.pagerank = random_jumping + (1-damping_factor) * pagerank_sum
    where nodes is an array of Nodes and node i is strored at nodes[i]

Assume the shadow node index is i and its pagerank value is pr. 
Set the shadows node's only parent to be parent_index and only child to be itself.
  The parent_index will be the index of a node that is not in the partition, i.e.,
  if the partition is nodes 1..20 then the parent_index can be the node at position 21.
We want the value computed for self.pagerank to always be pr.
Let the value of random_jumping be j., and the value of (1-damping_factor) be nd.
Given the statement to compute the value of self.pagerank:
    self.pagerank = random_jumping + (1-damping_factor) * pagerank_sum,
this evaluates to 
    self.pagerank = j + nd * pagerank_sum.
We want self.pagerank to evaluate to pr. We can only control the value of 
pagerank_sum, so 
    pr = j + nd * pagerank_sum ==>  pagerank_sum = (pr - j) / nd.
Since pagerank_sum is the pagerank value of the shadow node's parent
divided by 1, we should set the shadow nodes's parent's pagerank value to be 
(pr - j) / nd. This guarantees that we will always compute a pagerank of pr for
the shadow node. 

As I mentioned, we can add a parent node for each shadow node at postions just
past the end of the partition. Since these parent nodes are not in the partition
their pagerank values will not be changed and thus the shadow node's pagerank
values will not change.

"""

# We are using the DAG_excutor routines to execure DAG, like normal.
# PageRank specific actions need to be done by the PageRank task.
# This will change as we incrementally update the functionality.
# Using input_PageRank_nodes_and_partition
def PageRank(dependents,task_name):
#rhc: not clear whether we can add task_name as an arg to the task in the 
# DAG_executor_work_loop. If so, we could also add nodes and partition
# as parms (for small sizes)?
# ToDo: Added the fanout/faninNB/collapse dependents to the state_info
# what's next?
    partition_file_name = "./" + task_name + "_nodes_and_partition.pickle"
    nodes, partition = input_PageRank_nodes_and_partition(partition_file_name)
    # overwite nodes[i] with delegate i
    logger.info("PageRank: partition is: " + str(partition))
    damping_factor=0.15
    iteration=int(10)
    for i in range(iteration):
        logger.info("***** PageRank: iteration " + str(i))
        logger.info("")
        PageRank_one_iter(nodes,partition,damping_factor)
    logger.info("PageRank: partition is: " + str(partition))

def get_PageRank_list(nodes):
    pagerank_list = np.asarray([node.pagerank for node in nodes], dtype='float32')
    return np.round(pagerank_list, 3)

#partitions.append(current_partition.copy())
#frontiers.append(frontier.copy())
#frontier_cost = "END" + ":" + str(len(frontier))
#frontier_costs.append(frontier_cost)
logger.info("")
logger.info("input_file: generated: num_nodes: " + str(num_nodes) + " num_edges: " + str(num_edges))
logger.info("")
logger.info("visited length: " + str(len(visited)))
if len(visited) != num_nodes:
    logger.error("[Error]: visited length is " + str(len(visited))
        + " but num_nodes is " + str(num_nodes))
for x in visited:
    print(x, end=" ")
logger.info("")
logger.info("")
logger.info("final current_partition length: " + str(len(current_partition)-loop_nodes_added))
sum_of_partition_lengths = 0
for x in partitions:
    sum_of_partition_lengths += len(x)
    logger.debug("length of partition: " + str(len(x)))
logger.debug("shadow_nodes_added: " + str(shadow_nodes_added_to_partitions))
sum_of_partition_lengths -= (total_loop_nodes_added + shadow_nodes_added_to_partitions)
#if (len(current_partition)-loop_nodes_added) != num_nodes
logger.info("sum_of_partition_lengths (not counting total_loop_nodes_added or shadow_nodes_added): " 
    + str(sum_of_partition_lengths))
if sum_of_partition_lengths != num_nodes:
    logger.error("[Error]: sum_of_partition_lengths is " + str(sum_of_partition_lengths)
        + " but num_nodes is " + str(num_nodes))
logger.info("")
sum_of_groups_lengths = 0
for x in groups:
    sum_of_groups_lengths += len(x)
    logger.debug("length of group: " + str(len(x)))
logger.debug("shadow_nodes_added: " + str(shadow_nodes_added_to_groups))
sum_of_groups_lengths -= (total_loop_nodes_added + shadow_nodes_added_to_groups)
#if (len(current_partition)-loop_nodes_added) != num_nodes
logger.info("sum_of_groups_lengths (not counting total_loop_nodes_added or shadow_nodes_added): " 
    + str(sum_of_groups_lengths))
if sum_of_groups_lengths != num_nodes:
    logger.error("[Error]: sum_of_groups_lengths is " + str(sum_of_groups_lengths)
        + " but num_nodes is " + str(num_nodes))

print_val = ""
for x in current_partition:
   print_val += str(x) + " "
   # logger.info(x, end=" ")
logger.info(print_val)
logger.info("")

# adjusting for loop_nodes_added in dfs_p
sum_of_changes = sum(dfs_parent_changes_in_partiton_size)-shadow_nodes_added_to_partitions
avg_change = sum_of_changes / len(dfs_parent_changes_in_partiton_size)
print_val = "dfs_parent_changes_in_partiton_size length, len: " + str(len(dfs_parent_changes_in_partiton_size)) + ", sum_of_changes: " + str(sum_of_changes)
print_val += ", average dfs_parent change: %.1f" % avg_change
logger.info(print_val)
if PRINT_DETAILED_STATS:
    if sum_of_changes != num_nodes:
        logger.error("[Error]: sum_of_changes is " + str(sum_of_changes)
            + " but num_nodes is " + str(num_nodes))
    print_val = ""
    for x in dfs_parent_changes_in_partiton_size:
        print_val += str(x) + " "
        # print(x, end=" ")
    logger.info(print_val)

logger.info("")
logger.info("")
if PRINT_DETAILED_STATS:
    # adjusting for loop_nodes_added in dfs_p
    sum_of_changes = sum(dfs_parent_changes_in_frontier_size)
    logger.info("dfs_parent_changes_in_frontier_size length, len: " + str(len(dfs_parent_changes_in_frontier_size))
        + ", sum_of_changes: " + str(sum_of_changes))
    if sum_of_changes != num_nodes:
        logger.error("[Error]: sum_of_changes is " + str(sum_of_changes)
            + " but num_nodes is " + str(num_nodes))
    for x in dfs_parent_changes_in_frontier_size:
        print(x, end=" ")
    logger.info("")
    logger.info("")
#logger.info("frontier length: " + str(len(frontier)))
#if len(frontier) != 0:
#    logger.error("[Error]: frontier length is " + str(len(frontier))
#       + " but num_nodes is " + str(num_nodes))
#for x in frontier:
#    logger.info(str(x.ID), end=" ")
#logger.info("")
#logger.info("frontier cost: " + str(len(frontier_cost)))
#for x in frontier_cost:
#    logger.info(str(x), end=" ")
#logger.info("")
# final frontier shoudl always be empty
# assert: 
logger.info("frontiers: (final fronter should be empty), len: " + str(len(frontiers))+":")
for frontier_list in frontiers:
    if PRINT_DETAILED_STATS:
        print_val = "-- (" + str(len(frontier_list)) + "): "
        for x in frontier_list:
            #logger.info(str(x.ID),end=" ")
            print_val += str(x) + " "
            #print(str(x),end=" ")
        logger.info(print_val)
        logger.info("")
    else:
        logger.info("-- (" + str(len(frontier_list)) + ")") 
frontiers_length = len(frontiers)
if len(frontiers[frontiers_length-1]) != 0:
    logger.info ("Error]: final frontier is not empty.")
logger.info("")
logger.info("partitions, len: " + str(len(partitions))+":")
for x in partitions:
    if PRINT_DETAILED_STATS:
        print("-- (" + str(len(x)) + "):", end=" ")
        for node in x:
            print(node,end=" ")
            #if not node.isShadowNode:
            #    logger.info(str(index),end=" ")
            #else:
            #   logger.info(str(index)+"-s",end=" ")
        logger.info("")
    else:
        logger.info("-- (" + str(len(x)) + ")")
logger.info("")
logger.info("partition names, len: " + str(len(partition_names))+":")
for name in partition_names:
    if PRINT_DETAILED_STATS:
        logger.info("-- " + name)
logger.info("")
logger.info("groups, len: " + str(len(groups))+":")
for g in groups:
    if PRINT_DETAILED_STATS:
        print("-- (" + str(len(g)) + "):", end=" ")
        for node in g:
            print(node,end=" ")
        logger.info("")
    else:
        logger.info("-- (" + str(len(g)) + ")")
logger.info("")
logger.info("group names, len: " + str(len(group_names))+":")
for name in group_names:
    if PRINT_DETAILED_STATS:
        logger.info("-- " + name)
logger.info("")
logger.info("nodes_to_partition_maps (incl. shadow nodes), len: " + str(len(nodeIndex_to_partitionIndex_maps))+":")
for m in nodeIndex_to_partitionIndex_maps:
    if PRINT_DETAILED_STATS:
        print("-- (" + str(len(m)) + "):", end=" ")
        for k, v in m.items():
            print((k, v),end=" ")
        logger.info("")
    else:
        logger.info("-- (" + str(len(m)) + ")")
logger.info("")
logger.info("nodes_to_group_maps, (incl. shadow nodes), len: " + str(len(nodeIndex_to_groupIndex_maps))+":")
for m in nodeIndex_to_groupIndex_maps:
    if PRINT_DETAILED_STATS:
        print("-- (" + str(len(m)) + "):", end=" ")
        for k, v in m.items():
            print((k, v),end=" ")
        logger.info("")
    else:
        logger.info("-- (" + str(len(m)) + ")")
logger.info("")
if PRINT_DETAILED_STATS:
    logger.info("frontier costs (cost=length of frontier), len: " + str(len(frontier_costs))+":")
    print_val = ""
    for x in frontier_costs:
        print_val += "-- str(x)"
        #logger.info("-- ",end="")
        #logger.info(str(x))
    logger.info(print_val)
    logger.info("")
sum_of_partition_costs = 0
for x in all_frontier_costs:
    words = x.split(':')
    cost = int(words[1])
    sum_of_partition_costs += cost
logger.info("all frontier costs, len: " + str(len(all_frontier_costs)) + ", sum: " 
    + str(sum_of_partition_costs))
if PRINT_DETAILED_STATS:
    i = 0
    costs_per_line = 13
    for x in all_frontier_costs:
        if (i < costs_per_line):
            print(str(x),end=" ")
        else:
            logger.info(str(x))
            i = 0
        i += 1
logger.info("")
"""
# Doing this for each node in each partition now (next)
logger.info("")
if PRINT_DETAILED_STATS:
    logger.info("Node frontier_parent_tuples:")
    for node in nodes:
        logger.info(str(node.ID) + ": frontier_parent_tuples: ", end = " ")
        for parent_tuple in node.frontier_parents:
            logger.info(str(parent_tuple), end=" ")
        logger.info("")
else:
    logger.info("-- (" + str(len(x)) + ")")
"""
logger.info("")
if PRINT_DETAILED_STATS:
    logger.info("partition nodes' frontier_parent_tuples:")
    for x in partitions:
        if PRINT_DETAILED_STATS:
            print("-- (" + str(len(x)) + "):", end=" ")
            print_val = ""
            for node in x:
                print_val += str(node.ID) + ": " 
                # logger.info(node.ID,end=": ")
                for parent_tuple in node.frontier_parents:
                    print_val += str(parent_tuple) + " "
                    # print(str(parent_tuple), end=" ")
            logger.info(print_val)
            logger.info("")
        else:
            logger.info("-- (" + str(len(x)) + ")")
else:
    logger.info("-- (" + str(len(x)) + ")")
logger.info("")
if PRINT_DETAILED_STATS:
    logger.info("group nodes' frontier_parent_tuples:")
    for x in groups:
        if PRINT_DETAILED_STATS:
            print_val = "-- (" + str(len(x)) + "): "
            for node in x:
                print_val += str(node.ID) + ": "
                # logger.info(node.ID,end=": ")
                for parent_tuple in node.frontier_parents:
                    print_val += str(parent_tuple) + " "
                    # print(str(parent_tuple), end=" ")
            logger.info(print_val)
            logger.info("")
        else:
            logger.info("-- (" + str(len(x)) + ")")
else:
    logger.info("-- (" + str(len(x)) + ")")
logger.info("")
logger.info("frontier_groups_sum: " + str(frontier_groups_sum) + ", len(frontiers)-1: " 
    +  str(len(frontiers)-1))
logger.info("Average number of frontier groups: " + (str(frontier_groups_sum / len(frontiers)-1)))
logger.info("")
logger.info("nodeIndex_to_partition_partitionIndex_group_groupIndex_map, len: " + str(len(nodeIndex_to_partition_partitionIndex_group_groupIndex_map)) + ":")
logger.info("shadow nodes not mapped and not shown")
if PRINT_DETAILED_STATS:
    for k, v in nodeIndex_to_partition_partitionIndex_group_groupIndex_map.items():
        logger.info((k, v))
    logger.info("")
else:
    logger.info("-- (" + str(len(nodeIndex_to_partition_partitionIndex_group_groupIndex_map)) + ")")
logger.info("")
logger.info("Partition Node parents (shad. node is a parent), len: " + str(len(partitions))+":")
for x in partitions:
    if PRINT_DETAILED_STATS:
        #logger.info("-- (" + str(len(x)) + "):", end=" ")
        for node in x:
            print(node,end=":")
            for parent in node.parents:
                print(parent,end=" ")
            logger.info("")
            #if not node.isShadowNode:
            #    logger.info(str(index),end=" ")
            #else:
            #   logger.info(str(index)+"-s",end=" ")
        logger.info("")
    else:
        logger.info("-- (" + str(len(x)) + ")")
logger.info("")
logger.info("Group Node parents (shad. node is a parent), len: " + str(len(partitions))+":")
for x in groups:
    if PRINT_DETAILED_STATS:
        #logger.info("-- (" + str(len(x)) + "):", end=" ")
        for node in x:
            print(node,end=":")
            for parent in node.parents:
                print(parent,end=" ")
            logger.info("")
            #if not node.isShadowNode:
            #    logger.info(str(index),end=" ")
            #else:
            #   logger.info(str(index)+"-s",end=" ")
        logger.info("")
    else:
        logger.info("-- (" + str(len(x)) + ")")
logger.info("")
logger.info("Group Node num_children, len: " + str(len(groups))+":")
for x in groups:
    if PRINT_DETAILED_STATS:
        #logger.info("-- (" + str(len(x)) + "):", end=" ")
        for node in x:
            print(str(node) + ":" + str(node.num_children),end=", ")
        logger.info("")
    else:
        logger.info("-- (" + str(len(x)) + ")")
logger.info("")
logger.info("Partition_senders, len: " + str(len(Partition_senders)) + ":")
if PRINT_DETAILED_STATS:
    for k, v in Partition_senders.items():
        logger.info((k, v))
    logger.info("")
else:
    logger.info("-- (" + str(len(Partition_senders)) + ")")
    logger.info("")
logger.info("Partition_receivers, len: " + str(len(Partition_receivers)) + ":")
if PRINT_DETAILED_STATS:
    for k, v in Partition_receivers.items():
        logger.info((k, v))
    logger.info("")
else:
    logger.info("-- (" + str(len(Partition_receivers)) + ")")
    logger.info("")
logger.info("Group_senders, len: " + str(len(Group_senders)) + ":")
if PRINT_DETAILED_STATS:
    for k, v in Group_senders.items():
        logger.info((k, v))
    logger.info("")
else:
    logger.info("-- (" + str(len(Group_senders)) + ")")
    logger.info("")
logger.info("Group_receivers, len: " + str(len(Group_receivers)) + ":")
if PRINT_DETAILED_STATS:
    for k, v in Group_receivers.items():
        logger.info((k, v))
    logger.info("")
else:
    logger.info("-- (" + str(len(Group_receivers)) + ")")
    logger.info("")
generate_DAG_info()
#visualize()
#input('Press <ENTER> to continue')


logger.debug("Ouput partitions/groups")
output_partitions()
"""
logger.debug("Input partitions/groups")
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
logger.info("Results:")
for i in range(len(results)):
    logger.info ("ID:"+str(i) + " pagerank:" + str(results[i]))
"""

"""
generate_DAG_info("graph20_DAG", nodes)
"""
"""
target_nodes = [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20]
total_num_nodes = 20
PageRank_main(target_nodes,target_nodes,total_num_nodes)
np_array = get_PageRank_list(nodes)
logger.info(str(np_array))
"""

# This was moved down to here, out of the way. 
"""
A recursive function that find finds and logger.infos strongly connected
components using DFS traversal
u --> The vertex to be visited next
disc[] --> Stores discovery times of visited vertices
low[] -- >> earliest visited vertex (the vertex with minimum
            discovery time) that can be reached from subtree
            rooted with current vertex
st -- >> To store all the connected ancestors (could be part
    of SCC)
stackMember[] --> bit/index array for faster check whether
            a node is in stack
"""

def SCCUtil(self, u, low, disc, stackMember, st, list_of_sccs):
    # added list_of_sccs

    # Initialize discovery time and low value
    disc[u] = self.Time
    low[u] = self.Time
    self.Time += 1
    stackMember[u] = True
    st.append(u)

    # Go through all vertices adjacent to this
    for v in self.graph[u]:
        # added for debug
        #logger.info("v: " + str(v))
        # If v is not visited yet, then recur for it
        if disc[v] == -1:

            self.SCCUtil(v, low, disc, stackMember, st, list_of_sccs)

            # Check if the subtree rooted with v has a connection to
            # one of the ancestors of u
            # Case 1 (per above discussion on Disc and Low value)
            low[u] = min(low[u], low[v])

        elif stackMember[v] == True:

            '''Update low value of 'u' only if 'v' is still in stack
            (i.e. it's a back edge, not cross edge).
            Case 2 (per above discussion on Disc and Low value) '''
            low[u] = min(low[u], disc[v])

    # head node found, pop the stack and logger.info an SCC

    w = -1 # To store stack extracted vertices
    if low[u] == disc[u]:
        one_scc = []
        while w != u:
            w = st.pop()
            # added: if this is a call from BFS then remap back to Node IDs
            global USING_BFS
            if USING_BFS:
                ID = self.get_nodeID_from_GraphID(w)
                print(ID, end=" ")
                one_scc.append(ID)
            else:
                print(w, end=" ")

            stackMember[w] = False
            

        logger.info("")
        if USING_BFS:
            list_of_sccs.append(one_scc)

# The function to do DFS traversal.
# It uses recursive SCCUtil()

def SCC(self):

    # Mark all the vertices as not visited
    # and Initialize parent and visited,
    # and ap(articulation point) arrays
    # added for debug
    #logger.info("SCC: V:" + str(self.V))
    disc = [-1] * (self.V)
    low = [-1] * (self.V)
    stackMember = [False] * (self.V)
    st = []
    #added
    list_of_sccs = []

    # Call the recursive helper function
    # to find articulation points
    # in DFS tree rooted with vertex 'i'
    for i in range(self.V):
        if disc[i] == -1:
            self.SCCUtil(i, low, disc, stackMember, st, list_of_sccs)

    return list_of_sccs

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
    logger.info (m, end = " ") 

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
        logger.info ("dfs_parent add node " + str(node.ID) + " and child " +
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
    logger.info ("dfs_p_new from node " + str(node.ID))

    #dfs_p_start_partition_size = len(current_partition)
    #global loop_nodes_added
    #loop_nodes_added_start = loop_nodes_added

    list_of_unvisited_children = []
    check_list_of_unvisited_chldren_after_visiting_parents = False

    logger.info("in dfs_p_new start: list_of_unvisited_children:" + str(list_of_unvisited_children))

    # process children before parent traversal
    #list_of_unvisited_children, 
    check_list_of_unvisited_chldren_after_visiting_parents = dfs_parent_pre_parent_traversal(node,
        visited,list_of_unvisited_children)

    logger.info("in dfs_p_new after pre: list_of_unvisited_children:" + str(list_of_unvisited_children))

    if not len(node.parents):
        logger.info ("dfs_p node " + str(node.ID) + " has no parents")
    else:
        logger.info ("dfs_p node " + str(node.ID) + " visit parents")

    # visit parents
    for neighbor_index in node.parents:
        neighbor = nodes[neighbor_index]
        if neighbor.ID not in visited:
            logger.info ("dfs_p visit node " + str(neighbor.ID))
            dfs_parent(visited, graph, neighbor)
        else:
            logger.info ("dfs_p neighbor.ID " + str(neighbor.ID) + " already visited")
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
                logger.info("[Info]: Possible parent loop detected, start and end with " + str(neighbor.ID)
                    + ", loop indicator: " + loop_indicator)
                loop_nodes_added += 1

    # process children after parent traversal
    dfs_parent_post_parent_traversal(node, visited,
    list_of_unvisited_children, check_list_of_unvisited_chldren_after_visiting_parents)

    #dfs_p_end_partition_size = len(current_partition)
    #loop_nodes_added_end = loop_nodes_added
    #dfs_p_change_in_partitiob_size = (dfs_p_end_partition_size - dfs_p_start_partition_size) - (
    #    loop_nodes_added_end - loop_nodes_added_start)
    #logger.info("dfs_p_change_in_partition_size: " + str(dfs_p_change_in_partitiob_size))
    #dfs_p_changes_in_partiton_size.append(dfs_p_change_in_partitiob_size)

def dfs_p(visited, graph, node):
    logger.info ("dfs_p from node " + str(node.ID))
    dfs_p_start_partition_size = len(current_partition)
    loop_nodes_added_start = loop_nodes_added

    # target child node c of dfs_p(c) in bfs was to to visited in bfs before call to dfs_p(c)
    if not len(node.parents):
        logger.info ("dfs_p node " + str(node.ID) + " has no parents")
    else:
        logger.info ("dfs_p node " + str(node.ID) + " visit parents")

    for neighbor_index in node.parents:
        neighbor = nodes[neighbor_index]
        if neighbor.ID not in visited:
            logger.info ("dfs_p visit node " + str(neighbor.ID))
            dfs_parent(visited, graph, neighbor)
        else:
            logger.info ("dfs_p neighbor.ID " + str(neighbor.ID) + " already visited")


    # make sure parent in partition before any if its children. We visit parents of node 
    # in dfs_parents and they are added to partition in dfs_parents after their parents 
    # are added in dfs_parents then here we add node to partition. node is the target 
    # of dfs_p(node)  
    if node.partition_number == -1:
        logger.info ("dfs_p add " + str(node.ID) + " to partition")
        node.partition_number = current_partition_number
        current_partition.append(node.ID)
    else:
        logger.info ("dfs_p do not add " + str(node.ID) + " to partition "
            + str(current_partition_number) + " since it is already in partition " 
            + str(node.partition_number))

    dfs_p_end_partition_size = len(current_partition)
    loop_nodes_added_end = loop_nodes_added
    dfs_p_change_in_partitiob_size = (dfs_p_end_partition_size - dfs_p_start_partition_size) - (
        loop_nodes_added_end - loop_nodes_added_start)
    logger.info("dfs_p_change_in_partition_size: " + str(dfs_p_change_in_partitiob_size))
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

    logger.info("DAG_map:")
    for key, value in DAG_map.items():
        logger.info(str(key) + ' : ' + str(value))
    logger.info("")
    logger.info("states:")         
    for key, value in DAG_states.items():
        logger.info(str(key) + ' : ' + str(value))
    logger.info("")
    logger.info("num_fanins:" + str(num_fanins) + " num_fanouts:" + str(num_fanouts) + " num_faninNBs:" 
            + str(num_faninNBs) + " num_collapse:" + str(num_collapse))
    logger.info("")  
    logger.info("all_fanout_task_names")
    for name in all_fanout_task_names:
        logger.info(name)
    logger.info("")
    logger.info("all_fanin_task_names")
    for name in all_fanin_task_names :
        logger.info(name)
    logger.info("")
    logger.info("all_faninNB_task_names")
    for name in all_faninNB_task_names:
        logger.info(name)
    logger.info("")
    logger.info("all_collapse_task_names")
    for name in all_collapse_task_names:
        logger.info(name)
    logger.info("")
    logger.info("leaf task start states")
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
"""
