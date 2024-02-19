import logging 

#from .BFS import nodes

logger = None
logger = logging.getLogger(__name__)
"""
logger.setLevel(logging.DEBUG)
#logger.setLevel(logging.INFO)
formatter = logging.Formatter('[%(asctime)s] [%(module)s] [%(processName)s] [%(threadName)s]: %(message)s')
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
#ch.setLevel(logging.INFO)
ch.setFormatter(formatter)
logger.addHandler(ch)
"""


debug_pagerank = True

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
        # on the frontier (and so its pagerank value must be sent to its children's 
        # partitions). We may send pagerank to multiple chldren in different partitions or
        # multiple children in the same partition. For the latter we only 
        # send one pagerank value to the one partition. 
        self.frontier_parents =  []
        # True if this is a shadow node, i.e., a place holder for the actual
        # parent node that will be sent (via  fanout/faninNB) to the partition
        # containing this node. Shadow nodes immediately precede their children
        # in the partition. The pagerank value of this node was computed by 
        # the previous parition/group and sent to this partition so the child in this
        # partition can use it for their pagerank computation.
        self.isShadowNode = False
        # Note: shadow nodes cannot have a non-empty frontier_parents

    # the calls in BFS_pagerank.py to these update functions look like:
    """
        if not task_file_name.endswith('L'):
            partition_or_group[index].update_PageRank_of_PageRank_Function(partition_or_group, 
                damping_factor,one_minus_dumping_factor,random_jumping,total_num_nodes)
        else:
            # multi-iteration
            partition_or_group[index].update_PageRank_of_PageRank_Function_loop(partition_or_group, 
                damping_factor,one_minus_dumping_factor,random_jumping,total_num_nodes)
    """
    # which is in a for-each-node loop which is nested in a for-each-iteration loop
    """
        # If there is no loop in the partition/group then we make 1 iteration else 
        # we make a user-specified number of iterations.
        # For each iteration
        for i in range(1,iterations+1):
            if (debug_pagerank):
                logger.trace("***** PageRank: iteration " + str(i))
                logger.trace("")

            # Compute pagerank for each node in the partition/group
            # For each Partition Node
            for index in range(num_nodes_for_pagerank_computation):
                # above if-else statement with calls to update
    """
    # setting these parameter values in the caller:
    """
            #total_num_nodes is the number of nodes in the graph
            damping_factor=0.15
            random_jumping = damping_factor / total_num_nodes
            one_minus_dumping_factor = 1 - damping_factor
    """
    # called on a Partition_Node by BFS_pagerank.py or BFS_Shared.py to compute its pagerank value.
    # Node is not in a loop.
    def update_PageRank_of_PageRank_Function(self, partition_or_group,damping_factor,
        one_minus_dumping_factor,random_jumping,total_num_nodes):
        parent_nodes = self.parents
        if not self.isShadowNode:
            my_ID = str(self.ID)
        else:
            my_ID = str(self.ID) + "-s"

        global debug_pagerank
        #logger.trace("debug_pagerank: "  + str(debug_pagerank))
        if (debug_pagerank):
            logger.trace("update_PageRank_of_PageRank_Function: update_pagerank: node " + my_ID)
            logger.trace("update_PageRank_of_PageRank_Function: update_pagerank: parent_nodes: " + str(parent_nodes))
            logger.trace("update_PageRank_of_PageRank_Function: update_pagerank: num_children: " + str(self.num_children))
        
        #Note: a parent has at least one child so num_children is not 0
        pagerank_sum = sum((partition_or_group[node_index].pagerank / partition_or_group[node_index].num_children) for node_index in parent_nodes)
        if (debug_pagerank):
            logger.trace("update_PageRank_of_PageRank_Function: update_pagerank: pagerank_sum: " + str(pagerank_sum))
        if (debug_pagerank):
            logger.trace("update_PageRank_of_PageRank_Function: damping_factor:" 
                + str(damping_factor) + " 1-damping_factor:" + str(1-damping_factor) + " num_nodes: " + str(total_num_nodes) + " random_jumping: " + str(random_jumping))
        self.pagerank = random_jumping + (one_minus_dumping_factor * pagerank_sum)
        if (debug_pagerank):
            logger.trace ("update_PageRank_of_PageRank_Function: update_pagerank: pagerank of node: " + str(self.ID) + ": " + str(self.pagerank))
            logger.trace("")


    # called on a Partition_Node by BFS_pagerank.py or BFS_Shared.py to compute its pagerank value.
    # Node is in a loop.
    def update_PageRank_of_PageRank_Function_loop(self, partition_or_group,damping_factor,
        one_minus_dumping_factor,random_jumping,total_num_nodes):
        parent_nodes = self.parents
        if not self.isShadowNode:
            my_ID = str(self.ID)
        else:
            my_ID = str(self.ID) + "-s"

        global debug_pagerank
        if (debug_pagerank and self.ID == 2 and not self.isShadowNode):
            logger.info("update_PageRank_of_PageRank_Function_loop: update_pagerank: of node " + my_ID)
            logger.info("update_PageRank_of_PageRank_Function_loop: update_pagerank: parent_nodes of " + my_ID + ": " + str(parent_nodes))
            logger.info("update_PageRank_of_PageRank_Function_loop: update_pagerank of node " + my_ID + ": num_children: " + str(self.num_children))

            for node_indexD in parent_nodes:
                logger.info("update_PageRank_of_PageRank_Function_loop: node_indexD of parent: " + str(node_indexD))
                logger.info("update_PageRank_of_PageRank_Function_loop: partition_or_group[node_indexD].prev: " + str(partition_or_group[node_indexD].prev))
                logger.info("update_PageRank_of_PageRank_Function_loop: partition_or_group[node_indexD].num_children: " + str(partition_or_group[node_indexD].num_children))
                
        #Note: a paent has at least one child so num_children is not 0
        pagerank_sum = sum((partition_or_group[node_index].prev / partition_or_group[node_index].num_children) for node_index in parent_nodes)
        if (debug_pagerank and self.ID == 2 and not self.isShadowNode):
            logger.info("update_PageRank_of_PageRank_Function_loop: update_pagerank: pagerank_sum: " + str(pagerank_sum))
        if (debug_pagerank and self.ID == 2 and not self.isShadowNode):
            logger.info("update_PageRank_of_PageRank_Function_loop: damping_factor:" + str(damping_factor) + " 1-damping_factor:" + str(1-damping_factor) + " num_nodes: " + str(total_num_nodes) + " random_jumping: " + str(random_jumping))
        self.pagerank = random_jumping + (one_minus_dumping_factor * pagerank_sum)
        if (debug_pagerank and self.ID == 2 and not self.isShadowNode):
            logger.info ("update_PageRank_of_PageRank_Function_loop: update_pagerank: pagerank of node: " + str(self.ID) + ": " + str(self.pagerank))
            logger.info("")

    
#brc: shared

    # called on a Partition_Node by BFS_pagerank.py or BFS_Shared.py to compute its pagerank value.
    # Node is not in a loop. Passing position_size_tuple so we can get the statr of the partition/
    # group in the shared array. Where:
    #    position_size_tuple = shared_map[task_file_name]
    #    starting_position_in_partition_group = position_size_tuple[0]
    #    size_of_partition_group = position_size_tuple[1]
    def update_PageRank_of_PageRank_Function_Shared(self, shared_nodes, position_size_tuple, damping_factor,
        one_minus_dumping_factor,random_jumping,total_num_nodes):

        starting_position_in_partition_group = position_size_tuple[0]
        # FYI: size_of_partition_group = position_size_tuple[1]

        parent_nodes = self.parents
        if not self.isShadowNode:
            my_ID = str(self.ID)
        else:
            my_ID = str(self.ID) + "-s"

        global debug_pagerank
        if (debug_pagerank):
            logger.trace("update_PageRank_of_PageRank_Function_Shared: update_pagerank: node " + my_ID)
            logger.trace("update_PageRank_of_PageRank_Function_Shared: update_pagerank: parent_nodes: " + str(parent_nodes))
            logger.trace("update_PageRank_of_PageRank_Function_Shared: update_pagerank: num_children: " + str(self.num_children))
        
        #Note: a parent has at least one child so num_children is not 0
        pagerank_sum = sum((shared_nodes[node_index+starting_position_in_partition_group].pagerank / shared_nodes[node_index+starting_position_in_partition_group].num_children) for node_index in parent_nodes)
        if (debug_pagerank):
            logger.trace("update_PageRank_of_PageRank_Function_Shared: update_pagerank: pagerank_sum: " + str(pagerank_sum))
        if (debug_pagerank):
            logger.trace("update_PageRank_of_PageRank_Function_Shared: damping_factor:" + str(damping_factor) + " 1-damping_factor:" + str(1-damping_factor) + " num_nodes: " + str(total_num_nodes) + " random_jumping: " + str(random_jumping))
        self.pagerank = random_jumping + (one_minus_dumping_factor * pagerank_sum)
        if (debug_pagerank):
            logger.trace ("update_PageRank_of_PageRank_Function_Shared: update_pagerank: pagerank of node: " + str(self.ID) + ": " + str(self.pagerank))
            logger.trace("")

    # called on a Partition_Node by BFS_pagerank.py or BFS_Shared.py to compute its pagerank value.
    # Node is in a loop. Passing position_size_tuple so we can get the statr of the partition/
    # group in the shared array. Where:
    #    position_size_tuple = shared_map[task_file_name]
    #    starting_position_in_partition_group = position_size_tuple[0]
    #    size_of_partition_group = position_size_tuple[1]
    def update_PageRank_of_PageRank_Function_loop_Shared(self, shared_nodes, position_size_tuple ,damping_factor,
        one_minus_dumping_factor,random_jumping,total_num_nodes):

        starting_position_in_partition_group = position_size_tuple[0]
        logger.trace("update_PageRank_of_PageRank_Function_loop_Shared: starting_position_in_partition_group: " + str(starting_position_in_partition_group))
        # FYI: size_of_partition_group = position_size_tuple[1]

        parent_nodes = self.parents
        if not self.isShadowNode:
            my_ID = str(self.ID)
        else:
            my_ID = str(self.ID) + "-s"

        global debug_pagerank
        if (debug_pagerank):
            logger.trace("update_PageRank_of_PageRank_Function_loop_Shared: update_pagerank: node " + my_ID)
            logger.trace("update_PageRank_of_PageRank_Function_loop_Shared: update_pagerank: " + str(my_ID) + " parent_nodes: " + str(parent_nodes))
            logger.trace("update_PageRank_of_PageRank_Function_loop_Shared: update_pagerank: " + str(my_ID) + " num_children: " + str(self.num_children))
            logger.trace("update_PageRank_of_PageRank_Function_loop_Shared: parfent information:")
            for parent_indexD in parent_nodes:
                logger.trace("update_PageRank_of_PageRank_Function_loop_Shared: parent_indexD: " + str(parent_indexD))
                logger.trace("update_PageRank_of_PageRank_Function_loop_Shared: shared_nodes[parent_indexD+starting_position_in_partition_group].prev : " + str(shared_nodes[parent_indexD+starting_position_in_partition_group].prev))
                logger.trace("update_PageRank_of_PageRank_Function_loop_Shared: shared_nodes[parent_indexD+starting_position_in_partition_group].num_children: " + str(shared_nodes[parent_indexD+starting_position_in_partition_group].num_children))

        #Note: a parent has at least one child so num_children is not 0
        pagerank_sum = sum((shared_nodes[parent_index+starting_position_in_partition_group].prev / shared_nodes[parent_index+starting_position_in_partition_group].num_children) for parent_index in parent_nodes)
        if (debug_pagerank):
            logger.trace("update_PageRank_of_PageRank_Function_loop_Shared: update_pagerank: pagerank_sum: " + str(pagerank_sum))
        if (debug_pagerank):
            logger.trace("update_PageRank_of_PageRank_Function_loop_Shared: damping_factor:" + str(damping_factor) + " 1-damping_factor:" + str(1-damping_factor) + " num_nodes: " + str(total_num_nodes) + " random_jumping: " + str(random_jumping))
        self.pagerank = random_jumping + (one_minus_dumping_factor * pagerank_sum)
        if (debug_pagerank):
            logger.trace ("update_PageRank_of_PageRank_Function_loop_Shared: update_pagerank: pagerank of node: " + str(self.ID) + ": " + str(self.pagerank))
            logger.trace("")

    def __eq__(self,other):
        return self.ID == other.ID

    def __str__(self):
        shadow = ""
        # if shadow node ID is "20", whcih is the ID of the parent the shadow node is proxying for,
        # the ID is orinted as "20-s".
        if self.isShadowNode:
            shadow = "-s"
        return str(self.ID) + shadow

    # special print of pagerank value
    def toString_PageRank(self):
        if not self.isShadowNode:
            my_ID = str(self.ID)
        else:
            my_ID = str(self.ID) + "-s"
        return("ID:" + my_ID + " pr:" + str(self.pagerank) + " num_children:"+str(self.num_children))
