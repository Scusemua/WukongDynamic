import logging
import numpy as np

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

def initialize(): 
    global shared_partition
    global shared_groups
    global shared_partition_map
    global shared_groups_map
    global shared_partition_frontier_parents_map
    global shared_groups_frontier_parents_map

    # an aray of all the partitions, used for worker threads or when threads
    # are used to simulate lambdas. All the threads can access this local
    # shared array of partitions or groups
    shared_partition = []
    shared_groups = []
    # maps partition "P" to its position/size in shared_partition/shared_groups
    shared_partition_map = {}
    # maps group "G" to its position/size in shared_partition/shared_groups
    shared_groups_map = {}
    # maps a partition "P" to its list of frontier tuples
    shared_partition_frontier_parents_map = {}
    # maps a group "G" to its list of frontier tuples
    shared_groups_frontier_parents_map = {}

def initialize_struct_of_arrays(number_of_nodes,number_of_parent_nodes):

    global pagerank
    global previous
    global number_of_children
    global number_of_parents
    global parent_index
    global starting_indices_of_parents
    global parents

    #pagerank = []
    pagerank = np.empty(number_of_nodes,dtype=np.double)
    # prev[i] is previous pagerank value of i
    previous = np.full(number_of_nodes,float((1/number_of_nodes)))
    # num_chldren[i] is number of child nodes of node i
    # rhc: Q: make these short or something shorter than int?
    number_of_children = np.empty(number_of_nodes,dtype=np.intc)
    # numParents[i] is number of parent nodes of node i
    number_of_parents = np.empty(number_of_nodes,dtype=np.intc)
    # parent_index[i] is the index in parents[] of the first of 
    # num_parents parents of node i
    starting_indices_of_parents = np.empty(number_of_nodes,dtype=np.intc)
    # parents - to get the parents of node i: num_parents = numParents[i];
    # parent_index = parent_index[i]; 
    # for j in (parent_index,num_parents) parent = parents[j]
    parents = np.empty(number_of_parent_nodes,dtype=np.intc)

def update_PageRank_of_PageRank_Function_loop_Shared(shared_nodes, position_size_tuple ,damping_factor,
    one_minus_dumping_factor,random_jumping,total_num_nodes):

    debug_pagerank = False

    """
    global debug_pagerank
    if (debug_pagerank):
        logger.debug("update_pagerank: node " + my_ID)
        logger.debug("update_pagerank: parent_nodes: " + str(parent_nodes))
        logger.debug("update_pagerank: num_children: " + str(self.num_children))
    """

    starting_position_in_partition_group = position_size_tuple[0]
    # FYI:
    #size_of_partition_group = position_size_tuple[1]
    num_nodes_for_pagerank_computation = 1 # size_of_partition_group - num_shadow_nodes

    for node_index in range (starting_position_in_partition_group,starting_position_in_partition_group+num_nodes_for_pagerank_computation):

        global starting_indices_of_parents 
        # starting index of parents in the parents array
        starting_index_of_parent = starting_indices_of_parents[node_index]

        #parent_nodes = self.parents
        global number_of_parents
        num_parents = number_of_parents[node_index]

        global previous
        global number_of_children
        global parents
        global pagerank
    
    #Note: a parent has at least one child so num_children is not 0
    #pagerank_sum = sum((shared_nodes[node_index+starting_position_in_partition_group].prev / shared_nodes[node_index+starting_position_in_partition_group].num_children) for node_index in parent_nodes)
    pagerank_sum = sum((previous[parent_index] / number_of_children[parent_index]) for parent_index in parents[starting_index_of_parent:num_parents])
    if (debug_pagerank):
        logger.debug("update_pagerank: pagerank_sum: " + str(pagerank_sum))
    #random_jumping = damping_factor / total_num_nodes
    if (debug_pagerank):
        logger.debug("damping_factor:" + str(damping_factor) + " 1-damping_factor:" + str(1-damping_factor) + " num_nodes: " + str(total_num_nodes) + " random_jumping: " + str(random_jumping))
    #self.pagerank = random_jumping + ((1-damping_factor) * pagerank_sum)
    pagerank[node_index] = random_jumping + (one_minus_dumping_factor * pagerank_sum)
    if (debug_pagerank):
        logger.debug ("update_pagerank: pagerank of node: " + str(node_index) + ": " + str(pagerank[node_index]))
        logger.debug("")

