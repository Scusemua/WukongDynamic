import logging 

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

debug_pagerank = False

class Node:
    def __init__(self,ID):
        self.partition_number = -1
        self.group_number = -1
        self.ID = ID
        self.parents = []
        self.children = []
        self.num_children  = 0
#brc: ToDo
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
#brc: ToDo
        # this will be in Partition_Node not here
        self.isShadowNode = False

    def __eq__(self,other):
        return self.ID == other.ID

#brc: ToDo
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
        #logger.trace("debug_pagerank: "  + str(debug_pagerank))
        if (debug_pagerank):
            logger.trace("update_pagerank: node " + my_ID)
            logger.trace("update_pagerank: parent_nodes: " + str(parent_nodes))
            logger.trace("update_pagerank: num_children: " + str(self.num_children))
        
        #if self.ID == 16:
        #    parent1 = partition_or_group[1]
        #    parent2 = partition_or_group[2]
        #    if (debug_pagerank):
        #        logger.trace("16 parent : " + str(parent1.ID) + " num_children: " + str(parent1.num_children))
        #       logger.trace("16 parent : " + str(parent2.ID) + " num_children: " + str(parent2.num_children))
        
        #Note: a paent has at least one child so num_children is not 0
        pagerank_sum = sum((partition_or_group[node_index].prev / partition_or_group[node_index].num_children) for node_index in parent_nodes)
        if (debug_pagerank):
            logger.trace("update_pagerank: pagerank_sum: " + str(pagerank_sum))
        #random_jumping = damping_factor / total_num_nodes
        if (debug_pagerank):
            logger.trace("damping_factor:" + str(damping_factor) + " 1-damping_factor:" + str(1-damping_factor) + " num_nodes: " + str(total_num_nodes) + " random_jumping: " + str(random_jumping))
        #self.pagerank = random_jumping + ((1-damping_factor) * pagerank_sum)
        self.pagerank = random_jumping + (one_minus_dumping_factor * pagerank_sum)
        if (debug_pagerank):
            logger.trace ("update_pagerank: pagerank of node: " + str(self.ID) + ": " + str(self.pagerank))
            logger.trace("")