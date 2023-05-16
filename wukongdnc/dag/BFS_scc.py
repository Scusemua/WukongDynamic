import logging 
from collections import defaultdict

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
#logger.setLevel(logging.INFO)
formatter = logging.Formatter('[%(asctime)s] [%(module)s] [%(processName)s] [%(threadName)s]: %(message)s')
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
#ch.setLevel(logging.INFO)
ch.setFormatter(formatter)
logger.addHandler(ch)


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

#scc_graph = Graph(0)
scc_num_vertices = 0



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

# SCC 1
# part of SCC computation
#node_GraphID = scc_graph.map_nodeID_to_GraphID(node.ID)

# SCC 2
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

# SCC 3
    #global scc_num_vertices
    #scc_num_vertices += 1

# SCC 4
    #logger.debug("BFS set V to " + str(scc_num_vertices))
    #scc_graph.setV(scc_num_vertices)
    #scc_graph.logger.infoEdges()
    #scc_graph.clear()

# SCC 5
                #scc_graph.logger.infoEdges()
                #scc_graph.clear()

# SCC 6
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

# SCC 7
                #scc_graph = Graph(0)

# SCC 8
# if USING_BFS is true then when we logger.info SCC components we will 
# map the scc IDs back to Node IDs. Kluge for now.
USING_BFS = True