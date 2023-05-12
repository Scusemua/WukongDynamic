import logging 

#from .BFS import nodes

logger = None
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

    
    """
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
    """

#rhc shared

#    position_size_tuple = shared_map[task_file_name]
#    starting_position_in_partition_group = position_size_tuple[0]
#    size_of_partition_group = position_size_tuple[1]
# parent indices re relative to partition_or_group, so we need to translate them 
# too? So need new shared version of thse update functions. Pass the starting 
# position so we can use [startng_position + index]. Same for loop.
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
        
        #Note: a parent has at least one child so num_children is not 0
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
            logger.debug("update_pagerank: of node " + my_ID)
            logger.debug("update_pagerank: parent_nodes of " + my_ID + ": " + str(parent_nodes))
            logger.debug("update_pagerank of node " + my_ID + ": num_children: " + str(self.num_children))

            for node_indexD in parent_nodes:
                logger.debug("node_indexD: " + str(node_indexD))
                logger.debug("partition_or_group[node_indexD].prev: " + str(partition_or_group[node_indexD].prev))
                logger.debug("partition_or_group[node_indexD].num_children: " + str(partition_or_group[node_indexD].num_children))
        
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

    
#rhc shared

#    position_size_tuple = shared_map[task_file_name]
#    starting_position_in_partition_group = position_size_tuple[0]
#    size_of_partition_group = position_size_tuple[1]
# parent indices re relative to partition_or_group, so we need to translate them 
# too? So need new shared version of thse update functions. Pass the starting 
# position so we can use [startng_position + index]. Same for loop.
    def update_PageRank_of_PageRank_Function_Shared(self, shared_nodes, position_size_tuple, damping_factor,
        one_minus_dumping_factor,random_jumping,total_num_nodes):

        starting_position_in_partition_group = position_size_tuple[0]
        # FYI:
        #size_of_partition_group = position_size_tuple[1]

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
        
        #Note: a parent has at least one child so num_children is not 0
        pagerank_sum = sum((shared_nodes[node_index+starting_position_in_partition_group].pagerank / shared_nodes[node_index+starting_position_in_partition_group].num_children) for node_index in parent_nodes)
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

    def update_PageRank_of_PageRank_Function_loop_Shared(self, shared_nodes, position_size_tuple ,damping_factor,
        one_minus_dumping_factor,random_jumping,total_num_nodes):

        starting_position_in_partition_group = position_size_tuple[0]
        # FYI:
        #size_of_partition_group = position_size_tuple[1]

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

            for node_indexD in parent_nodes:
                logger.debug("node_indexD: " + str(node_indexD))
                logger.debug("shared_nodes[node_index+starting_position_in_partition_group].prev : " + str(shared_nodes[node_indexD+starting_position_in_partition_group].prev))
                logger.debug("shared_nodes[node_index+starting_position_in_partition_group].num_children: " + str(shared_nodes[node_indexD+starting_position_in_partition_group].num_children))


        #Note: a parent has at least one child so num_children is not 0
        pagerank_sum = sum((shared_nodes[node_index+starting_position_in_partition_group].prev / shared_nodes[node_index+starting_position_in_partition_group].num_children) for node_index in parent_nodes)
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
