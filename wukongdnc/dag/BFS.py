graph = {
  '5' : ['3','7'],
  '3' : ['2', '4'],
  '7' : ['8'],
  '2' : [],
  '4' : ['8'],
  '8' : []
}

class Node:
    def __init__(self,ID):
        self.partition_number = -1
        self.ID = ID
        self.parents = []
        self.children = []
        self.partition = -1

    def __eq__(self,other):
        return self.ID == other.ID

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

nodes = []

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

num_nodes = 12
for x in range(num_nodes+1):
    nodes.append(Node(x))

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
N1.children = [3,2]
N1.parents = []
N2.children = [9,8]
N2.parents = [1]
N3.children = [11,10]
N3.parents = [1,4]
N4.children = [3]
N4.parents = [5,6,12]
N5.children = [4]
N5.parents = []
N6.children = [7]
N6.parents = []
N7.children = []
N7.parents = [6]
N8.children = []
N8.parents = [2]
N9.children = []
N9.parents = [2]
N10.children = []
N10.parents = [3]
N11.children = [12]
N11.parents = [3,12]
N12.children = [4,11]
N12.parents = [11]

visited = [] # List for visited nodes.
queue = []     #Initialize a queue
current_partition = []
current_partition_number = 1
dfs_p_changes_in_partiton_size = []
frontier = []
loop_nodes_added = 0

"""
def bfs(visited, graph, node): #function for BFS
  visited.append(node)
  queue.append(node)

  while queue:          # Creating loop to visit each node
    m = queue.pop(0) 
    print (m, end = " ") 

    for neighbor in graph[m]:
      if neighbor not in visited:
        visited.append(neighbor)
        queue.append(neighbor)
"""

def dfs_p(visited, graph, node):
    print ("dfs_p from node " + str(node.ID))
    dfs_p_start_partition_size = len(current_partition)
    loop_nodes_added_start = loop_nodes_added

    # target child node c of dfs_p(c) in bfs was to to visited in bfs before call to dfs_p(c)
    if not len(node.parents):
        print ("dfs_p node " + str(node.ID) + " has no parents")
    else:
        print ("dfs_p node " + str(node.ID) + " visit parents")
    for neighbor_index in node.parents:
        neighbor = nodes[neighbor_index]
        if neighbor.ID not in visited:
            print ("dfs_p visit node " + str(neighbor.ID))
            dfs_parent(visited, graph, neighbor)
        else:
            print ("dfs_p neighbor.ID " + str(neighbor.ID) + " already visited")


    # make sure parent in partition before any if its children. We visit parents of node 
    # in dfs_parents and they are added to partition in dfs_parents after their parents 
    # are added in dfs_parents then here we add node to partition. node is the target 
    # of dfs_p(node)  
    if node.partition_number == -1:
        print ("dfs_p add " + str(node.ID) + " to partition")
        node.partition_number = current_partition_number
        current_partition.append(node.ID)
    else:
        print ("dfs_p do not add " + str(node.ID) + " to partition "
            + str(current_partition_number) + " since it is already in partition " 
            + str(node.partition_number))

    dfs_p_end_partition_size = len(current_partition)
    loop_nodes_added_end = loop_nodes_added
    dfs_p_change_in_partitiob_size = (dfs_p_end_partition_size - dfs_p_start_partition_size) - (
        loop_nodes_added_end - loop_nodes_added_start)
    print("dfs_p_change_in_partition_size: " + str(dfs_p_change_in_partitiob_size))
    dfs_p_changes_in_partiton_size.append(dfs_p_change_in_partitiob_size)

def dfs_parent(visited, graph, node):  #function for dfs 
    # e.g. dfs(3) where bfs is visiting 3 as a child of enqueued node
    # so 3 is not visited yet
    print ("dfs_parent from node " + str(node.ID))

    # set child node to visited if possible before dfs_parent so that when the parent 
    # checks if this child is visited it will be visited. 
    if len(node.children) == 0:
        # Can a child be n visited? If it was visted then parent must have been
        # already visited? since can't visit/add to partition unless all parents 
        # are already in there?
        visited.append(node.ID)
        print ("dfs_parent add " + str(node.ID) + " to visited since no children")              
    else:
        # node has more than one child or it has one child that has one or more
        # children (it is not a sink) or more than one parent (so if
        # we put child in partition we have to put all parents (including node)
        # in the partition (first))
        has_unvisited_children = False
        list_of_unvisited_children = []
        check_list_of_unvisited_chldren = False
        for neighbor_index in node.children:
            neighbor = nodes[neighbor_index]
            if neighbor.ID not in visited:
                print ("dfs_parent child " + str(neighbor.ID) + " not in visited")
                has_unvisited_children = True
                list_of_unvisited_children.append(neighbor.ID)
                #break
        if not has_unvisited_children:
            print ("dfs_parent mark " + str(node.ID) + " as visited since it has no unvisited children "
            + "but do not add it to bfs queue since no children need to be visited")
            visited.append(node.ID)
        else:
            print ("dfs_parent " + str(node.ID) + " has unvisted children so mark " 
                + str(node.ID) + " as visited and check children after parent traversal")
            # this node can be arked as visited, but we will only add it to the queue
            # if these unvisited children are still unvisited when we return from 
            # visiting the parent nodes (all ancestors). If children nodes are 
            # also parents or ancestors (in general) then we may visit them on 
            # parent traversal and if we visit of of node's children this way then
            # we need not add node to the queue. 
            visited.append(node.ID)
            check_list_of_unvisited_chldren = True
            print("set check_list_of_unvisited_chldren True")
            """ Chck this after dfs_parent
            queue.append(node)
            print("queue after add " + str(node.ID) + ":", end=" ")
            for x in queue:
                print(x.ID, end=" ")
            print()
            frontier.append(node)
            print("frontier after add " + str(node.ID) + ":", end=" ")
            for x in frontier:
                print(x.ID, end=" ")
            print()
            """

    if not len(node.parents):
        print ("dfs_parent node " + str(node.ID) + " has no parents")
    else:
        print ("dfs_parent node " + str(node.ID) + " visit parents")
    for neighbor_index in node.parents:
        neighbor = nodes[neighbor_index]
        if neighbor.ID not in visited:
            print ("dfs_parent visit node " + str(neighbor.ID))
            dfs_parent(visited, graph, neighbor)
        else:
            print ("dfs_parent neighbor " + str(neighbor.ID) + " already visited")
            # ???
            if neighbor.partition_number == -1:
                loop_indicator = str(neighbor.ID)+"(Lp)"
                current_partition.append(loop_indicator)
                print("[Info]: Possible loop detected, start and end with " + str(neighbor.ID)
                    + ", loop indicator: " + loop_indicator)
                global loop_nodes_added
                loop_nodes_added += 1
    # See if the unvisited children found above are still unvisited
    unvisited_chldren_after_parent_loop = []
    if check_list_of_unvisited_chldren:
        for child_index in list_of_unvisited_children:
            child_node = nodes[child_index]
            if child_node.ID not in visited:
                print("unvisited child :" + str(child_node.ID) + " not visited during parent traversal")
                # Did not visit this unvsited child when visiting parents
                unvisited_chldren_after_parent_loop.append(child_node.ID)
    # There remains some unvisited children
    print(str(len(unvisited_chldren_after_parent_loop)) + " children remain unvisited")
    if len(unvisited_chldren_after_parent_loop) > 0:
        queue.append(node)
        print("queue after add " + str(node.ID) + ":", end=" ")
        for x in queue:
            print(x.ID, end=" ")
        print()
        frontier.append(node)
        print("frontier after add " + str(node.ID) + ":", end=" ")
        for x in frontier:
            print(x.ID, end=" ")
        print()

    loop_indicator = ""
    first = True
    for unvisited_child in list_of_unvisited_children:
        print("check unvisited child: " + str(unvisited_child) + " still unvisited"
            + " after parent traversal")
        if unvisited_child not in unvisited_chldren_after_parent_loop:
            # A child that was unvisited before parent loop traverasal but
            # that was visited durng this traversal is part of a loop.
            # This child is also a parent or ancestor.
            # output loop (L) indicators in partition, children are in no 
            # particular order.
            print("unvisited child " + str(unvisited_child) + " still unvisited")
            if first:
                first = False
                loop_indicator = str(nodes[unvisited_child].ID)
            else:
                loop_indicator += "/" + str(nodes[unvisited_child].ID)
        else:
            print("unvisited child " + str(unvisited_child) + " was visited during parent traversal")
        loop_indicator += "(Lc)"
        current_partition.append(loop_indicator)
        #global loop_nodes_added
        loop_nodes_added += 1
        print("[Info]: possible loop detected, loop indicator: " + loop_indicator)

    # make sure parent in partition before any if its children. We visit parents of nodein dfs_parents 
    # and they are added to partition in dfs_parents after their parents are added 
    # in dfs_parents then here we add node to partition.  
    if node.partition_number == -1:
        print ("dfs_parent add " + str(node.ID) + " to partition")
        node.partition_number = current_partition_number
        current_partition.append(node.ID)
    else:
        print ("dfs_parent do not add " + str(node.ID) + " to partition "
            + current_partition_number + " since it is already in partition " 
            + node.partition_number)

def bfs(visited, graph, node): #function for BFS
    print ("bfs mark " + str(node.ID) + " as visited and add to queue")
    visited.append(node.ID)
    # dfs_p will add node to partition (and its unvisited parent nodes)
    dfs_p(visited, graph, node)
    queue.append(node)
    print("queue after add " + str(node.ID) + ":", end=" ")
    for x in queue:
        print(x.ID, end=" ")
    print()
    frontier.append(node)
    print("frontier after add " + str(node.ID) + ":", end=" ")
    for x in frontier:
        print(x.ID, end=" ")
    print()
    print ("bfs add " + str(node.ID) + " to partition")
    #node.partition_number = current_partition_number
    #current_partition.append(node.ID)

    while queue:          # Creating loop to visit each node
        node = queue.pop(0) 
        #print (node.ID, end = " ") 
        print("bfs pop node " + str(node.ID) + " from queue") 

        if not len(node.children):
            print ("bfs node " + str(node.ID) + " has no children")
        else:
            print ("bfs node " + str(node.ID) + " visit children")
        for neighbor_index in node.children:
            neighbor = nodes[neighbor_index]
            if neighbor.ID not in visited:
                print ("bfs mark " + str(neighbor.ID) + " visited")
                visited.append(neighbor.ID)
                print ("bfs dfs_p("+ str(neighbor.ID) + ")")
                dfs_p(visited, graph, neighbor)
                print ("bfs after dfs_p, add " + str(neighbor.ID) + " to queue")
                queue.append(neighbor)
                print("queue after add " + str(neighbor.ID) + ":", end=" ")
                for x in queue:
                    print(x.ID, end=" ")
                print()
                frontier.append(neighbor)
                print("frontier after add " + str(neighbor.ID) + ":", end=" ")
                for x in frontier:
                    print(x.ID, end=" ")
                print()
            else:
                print ("bfs node " + str(neighbor.ID) + " already visited")
        frontier.remove(node)
        print("frontier after remove " + str(node.ID) + ":", end=" ")
        for x in frontier:
            print(x.ID, end=" ")
        print()

def input_graph():
    graph_file = open('100.gr', 'r')
    count = 0
    file_name_line = graph_file.readline()
    count += 1
    print("file_name_line{}: {}".format(count, file_name_line.strip()))
    vertices_line = graph_file.readline()
    count += 1
    print("vertices_line{}: {}".format(count, vertices_line.strip()))
    edges_line = graph_file.readline()
    count += 1
    print("edges_line{}: {}".format(count, edges_line.strip()))
    max_weight_line = graph_file.readline()
    count += 1
    print("max_weight_line{}: {}".format(count, max_weight_line.strip()))
    min_weight_line = graph_file.readline()
    count += 1
    print("min_weight_line{}: {}".format(count, min_weight_line.strip()))
    vertices_edges_line = graph_file.readline()
    count += 1
    print("vertices_edges_line{}: {}".format(count, vertices_edges_line.strip()))

    words = vertices_edges_line.split(' ')
    print("nodes:" + words[2] + " edges:" + words[3])
    num_nodes = int(words[2])
    num_edges = int(words[3])
    print ("num_nodes:" + str(num_nodes) + " num_edges:" + str(num_edges))

    for x in range(num_nodes+1):
        nodes.append(Node(x))

    num_parent_appends = 0
    num_children_appends = 0

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
            print("[Warning]: self loop: " + source + " -->" + target)
        #print("target:" + str(target))
        #if target == 101:
        #    print("target is 101")
        #rhc: 101 is a sink, i.e., it has no children so it will not appear as a source
        # in the file. Need to append a new node if target is out of range, actually 
        # append target - num_nodes. Is this just a coincidence that sink is node 100+1
        # where the gaph is supposed to have 100 nodes?
        if target > num_nodes:
            if len(nodes) < target+1:
                number_of_nodes_to_append = target - num_nodes
                print("number_of_nodes_to_append:" + str(number_of_nodes_to_append))
                for i in range(number_of_nodes_to_append):
                    print("Node(" + str(num_nodes+i+1) + ")")
                    nodes.append(Node((num_nodes+i+1)))
                num_nodes += number_of_nodes_to_append
        print ("source:" + str(source) + " target:" + str(target))
        source_node = nodes[source]
        source_node.children.append(target)
        num_children_appends += 1
        target_node = nodes[target]
        target_node.parents.append(source)
        num_parent_appends +=  1
    
        print("Line {}: {}".format(count, line.strip()))

    source_node = nodes[1]
    print("Node1 children:")
    for child in source_node.children:
        print(child)
    print("Node1 parents:")
    for parent in source_node.parents:
        print(parent)

    source_node = nodes[7]
    print("Node7 children:")
    for child in source_node.children:
        print(child)
    print("Node7 parents:")
    for parent in source_node.parents:
        print(parent)

    count_child_edges = 0
    i = 1
    while i <= num_nodes:
        node = nodes[i]
        #print (str(i) + ": get children: " + str(len(node.children)))
        count_child_edges += len(node.children)
        i += 1
    print("num edges in graph: " + str(num_edges))
    if not num_edges == count_child_edges:
        print("[Error]: num child edges in graph is " + str(count_child_edges) + " but edges in file is "
            + str(num_edges))

    count_parent_edges = 0
    i = 1
    while i <= num_nodes:
        node = nodes[i]
        #print (str(i) + ": get parents: " + str(len(node.parents)))
        count_parent_edges += len(node.parents)
        i += 1

    print("num_edges in graph: " + str(num_edges))
    if not num_edges == count_parent_edges:
        print("[Error]: num parent edges in graph is " + str(count_parent_edges) + " but edges in file is "
        + str(num_edges))

    print("num_parent_appends:" + str(num_parent_appends))
    print("num_children_appends:" + str(num_children_appends))

    graph_file.close()

# Driver Code

print("Following is the Breadth-First Search")
#bfs(visited, graph, '5')    # function calling
bfs(visited, graph, nodes[1])    # function calling

print("visited length: " + str(len(visited)))
if len(visited) != num_nodes:
    print("[Error]: visited length is " + len(visited) 
        + " but num_nodes is " + num_nodes)
for x in visited:
    print(x, end=" ")
print()
print("current_partition length: " + str(len(current_partition)-loop_nodes_added))
if (len(current_partition)-loop_nodes_added) != num_nodes:
    print("[Error]: current_partition length is " + str(len(current_partition))
        + " but num_nodes is " + str(num_nodes))
for x in current_partition:
    print(x, end=" ")
print()
# adjusting for loop_nodes_added in dfs_p
sum_of_changes = sum(dfs_p_changes_in_partiton_size)
print("dfs_p_changes_in_partiton_size length: " + str(len(dfs_p_changes_in_partiton_size))
    + " sum_of_changes: " + str(sum_of_changes))
if sum_of_changes != num_nodes:
    print("[Error]: sum_of_changes is " + str(sum_of_changes)
        + " but num_nodes is " + str(num_nodes))
for x in dfs_p_changes_in_partiton_size:
    print(x, end=" ")
print()
print("frontier length: " + str(len(frontier)))
#if len(frontier) != 0:
#    print("[Error]: frontier length is " + str(len(frontier))
 #       + " but num_nodes is " + str(num_nodes))
for x in frontier:
    print(x.ID, end=" ")
print()



# ToDo: check frontier code. keep current cost of frontier, which is number 
# of nodes in frontier? assuming no sinks in frontier.


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
        print ("dfs_parent add node " + str(node.ID) + " and child " +
            node.children[0].ID + " to visited since this is the only child"
            + " and child has no children and only one parent which is node")
        visited.append(node.children[0].ID)
        current_partition.append(node.children[0].ID)
        # Do not add node.children[0].ID to frontier since it has no children

        # if this is for reducing partition then node is already visited and in 
        # partition, but node is in frontier and we can now remove it
        #visited.append(node.ID)
        frontier.remove(node.ID)
# """

# problem: 12 11 loop. One will be in before its parent. (L) thing 
# isn;t working since 12 sees 11 as unvisted so can't do 11(L).
# 11 sees child 12 as visited while in dfs_parent(11) call from 12
# so is that a loop? but already have partition: 12 so too late for 
# (L).
# 1 5 6 3(L) 11 12 4 3 2 7 10 9 8
# so 12 not in partition until back from 11 and 11 sees child 12
# is visited so 11 does "12(L)" then "11" and 12 does 12 so
# 12(L) 11 12
# Also, 12 when return from dfs_paranrt can check if the child that 
# was unvisited is not visited (due to dfs_parent action) and thus
# no need to put 12 in queue. (12 check all its children again and
# use this second result.)
# Again, if 11 sees a visited child in dfs_parent(11) does it mean
# that there must be a loop? Can the visited child just be a non-loop
# descendent of 11? No? consider 5 --> 4, 4 will call dfs_parent(5)
# and 5 will see that child 4 is visited and there is no loop.
# !!!!!!! First
# Consider as above I thin, if 12 sees unviited child 11 then 
# comes back from dfs parent and sees 11 is now visited, then 11
# was on parent path and 11 is a child so loop? 11 12 11(L)

# but 6 add to q ad frontier
# 7 is child so 7 visited
# 7 add to queue and frontier
# 7 has no children
# remove 7 from frontier
# Should we add child to queue and frontier if
#   chid has no children and only one parent (this node)?

# perhaps 11 adds 13(L) then 13(L) 11 12 4 13 so 13 in partition 
# before 11 and 11 12 4 in partition before 13. Hah!
