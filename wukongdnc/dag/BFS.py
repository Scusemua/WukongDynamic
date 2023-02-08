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
#put non-null elements in place
for x in range(num_nodes+1):
    nodes.append(Node(x))

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
partitions = []
current_partition = []
current_partition_number = 1
dfs_p_changes_in_partiton_size = []
loop_nodes_added = 0
total_loop_nodes_added = 0
frontier_costs = []
frontier_cost = []
frontiers = []
frontier = []


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

def dfs_p_new(visited, graph, node):  #function for dfs 
    # e.g. dfs(3) where bfs is visiting 3 as a child of enqueued node
    # so 3 is not visited yet
    print ("dfs_p_new from node " + str(node.ID))

    #dfs_p_start_partition_size = len(current_partition)
    #global loop_nodes_added
    #loop_nodes_added_start = loop_nodes_added

    list_of_unvisited_children = []
    check_list_of_unvisited_chldren_after_visiting_parents = False

    print("in dfs_p_new start: list_of_unvisited_children:" + str(list_of_unvisited_children))

    # process children before parent traversal
    #list_of_unvisited_children, 
    check_list_of_unvisited_chldren_after_visiting_parents = dfs_parent_pre_parent_traversal(node,
        visited,list_of_unvisited_children)

    print("in dfs_p_new after pre: list_of_unvisited_children:" + str(list_of_unvisited_children))

    if not len(node.parents):
        print ("dfs_p node " + str(node.ID) + " has no parents")
    else:
        print ("dfs_p node " + str(node.ID) + " visit parents")

    # visit parents
    for neighbor_index in node.parents:
        neighbor = nodes[neighbor_index]
        if neighbor.ID not in visited:
            print ("dfs_p visit node " + str(neighbor.ID))
            dfs_parent(visited, graph, neighbor)
        else:
            print ("dfs_p neighbor.ID " + str(neighbor.ID) + " already visited")
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
                print("[Info]: Possible parent loop detected, start and end with " + str(neighbor.ID)
                    + ", loop indicator: " + loop_indicator)
                loop_nodes_added += 1

    # process children after parent traversal
    dfs_parent_post_parent_traversal(node, visited,
    list_of_unvisited_children, check_list_of_unvisited_chldren_after_visiting_parents)

    #dfs_p_end_partition_size = len(current_partition)
    #loop_nodes_added_end = loop_nodes_added
    #dfs_p_change_in_partitiob_size = (dfs_p_end_partition_size - dfs_p_start_partition_size) - (
    #    loop_nodes_added_end - loop_nodes_added_start)
    #print("dfs_p_change_in_partition_size: " + str(dfs_p_change_in_partitiob_size))
    #dfs_p_changes_in_partiton_size.append(dfs_p_change_in_partitiob_size)

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

# process children before parent traversal
def dfs_parent_pre_parent_traversal(node,visited,list_of_unvisited_children):
    check_list_of_unvisited_chldren_after_visiting_parents = False
    # set child node to visited if possible before dfs_parent so that when the parent 
    # checks if this child is visited it will be visited. 
    print("in pre start: list_of_unvisited_children:" + str(list_of_unvisited_children))
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
        visited.append(node.ID)
        print ("dfs_parent add " + str(node.ID) + " to visited since no children")              
    else:
        # node has more than one child or it has one child that has one or more
        # children (it is not a sink) or more than one parent (so if
        # we put child in partition we have to put all parents (including node)
        # in the partition (first))
        has_unvisited_children = False
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
                + str(node.ID) + " as visited and check children again after parent traversal")
            # this node can be marked as visited, but we will only add it to the queue
            # if these unvisited children are still unvisited when we return from 
            # visiting the parent nodes (all ancestors). If children nodes are 
            # also parents or ancestors (in general) then we may visit them on 
            # parent traversal and if we visit all of th node's children this way then
            # we need not add node to the queue. 
            visited.append(node.ID)
            check_list_of_unvisited_chldren_after_visiting_parents = True
            print("set check_list_of_unvisited_chldren True")
            print("in pre: list_of_unvisited_children:" + str(list_of_unvisited_children))

    return check_list_of_unvisited_chldren_after_visiting_parents

def dfs_parent(visited, graph, node):  #function for dfs 
    # e.g. dfs(3) where bfs is visiting 3 as a child of enqueued node
    # so 3 is not visited yet
    print ("dfs_parent from node " + str(node.ID))

    list_of_unvisited_children = []
    check_list_of_unvisited_chldren_after_visiting_parents = False

    # process children before parent traversal
    # list_of_unvisited_children, 
    check_list_of_unvisited_chldren_after_visiting_parents = dfs_parent_pre_parent_traversal(node,visited,list_of_unvisited_children)

    print("after pre: list_of_unvisited_children: " + str(list_of_unvisited_children))
    
    if not len(node.parents):
        print ("dfs_parent node " + str(node.ID) + " has no parents")
    else:
        print ("dfs_parent node " + str(node.ID) + " visit parents")

    # visit parents
    for neighbor_index in node.parents:
        neighbor = nodes[neighbor_index]
        if neighbor.ID not in visited:
            print ("dfs_parent visit node " + str(neighbor.ID))
            dfs_parent(visited, graph, neighbor)
        else:
            # loop detected - mark this loop in partition (for debugging for now)
            print ("dfs_parent neighbor " + str(neighbor.ID) + " already visited")
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
                print("[Info]: Possible parent loop detected, start and end with " + str(neighbor.ID)
                    + ", loop indicator: " + loop_indicator)
                global loop_nodes_added
                loop_nodes_added += 1

    # process children after parent traversal
    dfs_parent_post_parent_traversal(node, visited,
    list_of_unvisited_children, check_list_of_unvisited_chldren_after_visiting_parents)

# process children after parent traversal
def dfs_parent_post_parent_traversal(node, visited, list_of_unvisited_children, check_list_of_unvisited_chldren_after_visiting_parents):
    # See if the unvisited children found above are still unvisited
    unvisited_children_after_parent_loop = []
    if check_list_of_unvisited_chldren_after_visiting_parents:
        for child_index in list_of_unvisited_children:
            print("check unvisited child " + str(child_index))
            child_node = nodes[child_index]
            if child_node.ID not in visited:
                print("unvisited child " + str(child_node.ID) + " not visited during parent traversal")
                # Did not visit this unvsited child when visiting parents
                unvisited_children_after_parent_loop.append(child_node.ID)
    print(str(len(unvisited_children_after_parent_loop)) + " children remain unvisited")

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
    print_loop_indicator = False
    # unvisited_children_after_parent_loop: is the children that were unvisited
    # before the parent traversal and that aer still unvisited after the parent
    # traversal. unvisited_children_after_parent_loop may be a subset of 
    # list_of_unvisited_children. 
    # Need to know the children that remain unvisited. They are the children 
    # in list_of_unvisited_children tht are not in unvisited_children_after_parent_loop.

    for unvisited_child in list_of_unvisited_children:
        print("check whether node " + str(node.ID) + " unvisited child " + str(unvisited_child) + " is still unvisited"
            + " after parent traversal")
        if unvisited_child not in unvisited_children_after_parent_loop:
            # Unvisited_child is no longer unvisited after call to dfs_parent.
            # A child that was unvisited before parent loop traverasal but
            # that was visited durng this traversal is part of a loop.
            # This child is also a parent or ancestor.
            # output loop (L) indicators in partition, children are in no 
            # particular order.
            print("unvisited child " + str(unvisited_child) + " not still unvisited")
            if first:
                first = False
                loop_indicator = str(nodes[unvisited_child].ID)
            else:
                loop_indicator += "/" + str(nodes[unvisited_child].ID)
            print_loop_indicator = True
        else:
            print("unvisited child " + str(unvisited_child) + " was not visited during parent traversal")
    if print_loop_indicator:
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
        print("[Info]: possible loop detected, loop indicator: " + loop_indicator)

    if len(unvisited_children_after_parent_loop) > 0:
        # There remains some unvisited children
#rhc: ToDo: check for singleton
        if len(unvisited_children_after_parent_loop) == 1:
            # in fact, there is only one unvisited child
            print("1 unvisited child after parent loop.")
            only_child_index = node.children[0]
            only_child = nodes[only_child_index]
            if len(node.children) == 1 and (
                # and node actually has only one child (which must be unvisited)
                len(only_child.children) == 0) and (
                # this unvisited only child has no children
                len(only_child.parents) == 1):
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
                print("the 1 unvisited child after parent loop is a singleton"
                    + " mark it visited and add parent (first) and child to partition.")
                visited.append(only_child.ID)
                # add node to partition before child 
                if node.partition_number == -1:
                    print ("dfs_parent add " + str(node.ID) + " to partition")
                    node.partition_number = current_partition_number
                    current_partition.append(node.ID)
                else:
                    print ("dfs_parent do not add " + str(node.ID) + " to partition "
                        + current_partition_number + " since it is already in partition " 
                        + node.partition_number)
                if only_child.partition_number == -1:
                    print ("dfs_parent add " + str(only_child.ID) + " to partition")
                    only_child.partition_number = current_partition_number
                    current_partition.append(only_child.ID)
                else:
                    # assert: this is an Error
                    print ("dfs_parent do not add " + str(only_child.ID) + " to partition "
                        + current_partition_number + " since it is already in partition " 
                        + only_child.partition_number)

            else:
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
        else:
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
    else:
        print("node " + str(node.ID) + " has no unvisited children after parent traversal,"
            + " add it to partition but not queue")
        if node.partition_number == -1:
            print("dfs_parent add " + str(node.ID) + " to partition")
            node.partition_number = current_partition_number
            current_partition.append(node.ID)
        else:
            print("dfs_parent do not add " + str(node.ID) + " to partition "
                + current_partition_number + " since it is already in partition " 
                + node.partition_number)

def bfs(visited, graph, node): #function for BFS
    print ("bfs mark " + str(node.ID) + " as visited and add to queue")
#rhc: changed
    #visited.append(node.ID)
    # dfs_p will add node to partition (and its unvisited parent nodes)
#rhc: ToDO: use dfs_p_new(visited, graph, node)
# ToDo: Move the stats at beginning and end of dfs_p_new here; then just call 
#       dfs_parent instead of dfs_p
    global current_partition
    dfs_p_start_partition_size = len(current_partition)
    global loop_nodes_added
    loop_nodes_added_start = loop_nodes_added

    #dfs_p(visited, graph, node)
    dfs_parent(visited, graph, node)

#   ToDo:
    dfs_p_end_partition_size = len(current_partition)
    loop_nodes_added_end = loop_nodes_added
    dfs_p_change_in_partitiob_size = (dfs_p_end_partition_size - dfs_p_start_partition_size) - (
        loop_nodes_added_end - loop_nodes_added_start)
    print("dfs_p_change_in_partition_size: " + str(dfs_p_change_in_partitiob_size))
    dfs_p_changes_in_partiton_size.append(dfs_p_change_in_partitiob_size)

    """
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
    #print ("bfs add " + str(node.ID) + " to partition")
    #node.partition_number = current_partition_number
    #current_partition.append(node.ID)
    """

    while queue:          # Creating loop to visit each node

        node = queue.pop(0) 
        #frontier_cost.append("pop-"+str(node.ID) + ":" + str(len(frontier)))

        if len(current_partition) >= num_nodes/2:
            print("BFS: create sub-partition")
            partitions.append(current_partition.copy())
            current_partition = []
            global total_loop_nodes_added
            total_loop_nodes_added += loop_nodes_added
            loop_nodes_added = 0
            frontiers.append(frontier.copy())
            frontier_cost = "pop-"+str(node.ID) + ":" + str(len(frontier))
            frontier_costs.append(frontier_cost)
        #print (node.ID, end = " ") 
        print("bfs pop node " + str(node.ID) + " from queue") 

        if not len(node.children):
            print ("bfs node " + str(node.ID) + " has no children")
        else:
            print ("bfs node " + str(node.ID) + " visit children")
        for neighbor_index in node.children:
            neighbor = nodes[neighbor_index]
            if neighbor.ID not in visited:
                print ("bfs visit child " + str(neighbor.ID) + " mark it visited and "
                    + "dfs_p(" + str(neighbor.ID) + ")")
#rhc: Changed
                #visited.append(neighbor.ID)
                print ("bfs dfs_p("+ str(neighbor.ID) + ")")

# ToDo: Move the stats at beginning and end of dfs_p_new here; then just call 
#       dfs_parent instead of dfs_p
                dfs_p_start_partition_size = len(current_partition)
                loop_nodes_added_start = loop_nodes_added

                #dfs_p_new(visited, graph, neighbor)
                dfs_parent(visited, graph, neighbor)
#   ToDo:
                dfs_p_end_partition_size = len(current_partition)
                loop_nodes_added_end = loop_nodes_added
                dfs_p_change_in_partitiob_size = (dfs_p_end_partition_size - dfs_p_start_partition_size) - (
                loop_nodes_added_end - loop_nodes_added_start)
                print("dfs_p_change_in_partition_size: " + str(dfs_p_change_in_partitiob_size))
                dfs_p_changes_in_partiton_size.append(dfs_p_change_in_partitiob_size)

                """
                # dfs_p(neighbor) did parent traversal and added neighbor to partition. 
                # If neighbor has no children, do not add neighbor to queue. This puts 
                # neighbor on the frontier with a zero cost (since it has no children
                # that are not in the same (current) partition.)
                if len(neighbor.children) > 0:
                    print ("bfs after dfs_p, add child " + str(neighbor.ID) + " of node " 
                        + str(node.ID) + " to queue")
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
                    print("child " + str(neighbor.ID) + " of node " + str(node.ID)
                        + " has no children, already marked it visited and added"
                        + " it to partition but do not queue it or add it to frontier.")
                """
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

    # if num_nodes is 100, this fills nodes[0] ... nodes[100]
    # Note: nodes[0] is not used
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

        # Example: num_nodes is 100 and target is 101, so 101 > 100.
        # But nodes is filled from nodes[0] ... nodes[100] so len(nodes) is 101
        if target > num_nodes:
            # If len(nodes) is 101 and num_nodes is 100 and we have a tatget of
            # 101, which is a sink, i.e., parents but no children, then there is 
            # no source 101. We use target+1, where 101 - num_nodes = 101 - 100 - 1
            # and target+1 = 101+1 = 102 - len(nodes) = 101 - 101 - 1, so we get 
            # the number_of_nodes_to_append to be 1, as needed.
            if len(nodes) < target+1:
                number_of_nodes_to_append = target - num_nodes
                print("number_of_nodes_to_append:" + str(number_of_nodes_to_append))
                # in our example, number_of_nodes_to_append = 1 so i starts
                # with 0 (default) and ends with number_of_nodes_to_append-1 = 0
                for i in range(number_of_nodes_to_append):
                    print("Node(" + str(num_nodes+i+1) + ")")
                    # new node ID for our example is 101 = num_nodes+i+1 = 100 + 0 + 1 = 101
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
    print("num edges in graph: " + str(num_edges) + " num child edges: " 
        + str(count_child_edges))
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

    print("num_edges in graph: " + str(num_edges) + " num parent edges: " 
        + str(count_parent_edges))
    if not num_edges == count_parent_edges:
        print("[Error]: num parent edges in graph is " + str(count_parent_edges) + " but edges in file is "
        + str(num_edges))

    print("num_parent_appends:" + str(num_parent_appends))
    print("num_children_appends:" + str(num_children_appends))

    graph_file.close()

# Driver Code

print("Following is the Breadth-First Search")
#bfs(visited, graph, '5')    # function calling
# example: num_nodes = 100, so Nodes in nodes[1] to nodes[100]
# i start = 1 as nodes[0] not used, i end is (num_nodes+1) - 1  = 100
for i in range(1,num_nodes+1):
    if i not in visited:
        bfs(visited, graph, nodes[i])    # function calling

partitions.append(current_partition.copy())
frontiers.append(frontier.copy())
frontier_cost = "END" + ":" + str(len(frontier))
frontier_costs.append(frontier_cost)
print()
print("visited length: " + str(len(visited)))
if len(visited) != num_nodes:
    print("[Error]: visited length is " + str(len(visited))
        + " but num_nodes is " + str(num_nodes))
for x in visited:
    print(x, end=" ")
print()
print()
print("final current_partition length: " + str(len(current_partition)-loop_nodes_added))
sum_of_partition_lengths = 0
for x in partitions:
    sum_of_partition_lengths += len(x)
sum_of_partition_lengths -= total_loop_nodes_added
#if (len(current_partition)-loop_nodes_added) != num_nodes
print("sum_of_partition_lengths (not counting total_loop_nodes_added): " 
    + str(sum_of_partition_lengths))
if sum_of_partition_lengths != num_nodes:
    print("[Error]: sum_of_partition_lengths is " + str(sum_of_partition_lengths)
        + " but num_nodes is " + str(num_nodes))
#for x in current_partition:
#    print(x, end=" ")
print()
# adjusting for loop_nodes_added in dfs_p
sum_of_changes = sum(dfs_p_changes_in_partiton_size)
print("dfs_p_changes_in_partiton_size length: " + str(len(dfs_p_changes_in_partiton_size))
    + ", sum_of_changes: " + str(sum_of_changes))
if sum_of_changes != num_nodes:
    print("[Error]: sum_of_changes is " + str(sum_of_changes)
        + " but num_nodes is " + str(num_nodes))
for x in dfs_p_changes_in_partiton_size:
    print(x, end=" ")
print()
print()
#print("frontier length: " + str(len(frontier)))
#if len(frontier) != 0:
#    print("[Error]: frontier length is " + str(len(frontier))
#       + " but num_nodes is " + str(num_nodes))
#for x in frontier:
#    print(str(x.ID), end=" ")
#print()
#print("frontier cost: " + str(len(frontier_cost)))
#for x in frontier_cost:
#    print(str(x), end=" ")
#print()
print("partitions: " + str(len(partitions)))
for x in partitions:
    print("-- " + str(x))
print()
# final frontier shoudl always be empty
# assert: 
print("frontiers: (final fronter shoudl be empty) " + str(len(frontiers)))
for frontier_list in frontiers:
    print("-- ",end="")
    for x in frontier_list:
        print(str(x.ID),end="")
    print()
frontiers_length = len(frontiers)
if len(frontiers[frontiers_length-1]) != 0:
    print ("Error]: final frontoer is not empty.")
print()
print("frontier costs (cost=length of frontier) " + str(len(frontier_costs)))
for x in frontier_costs:
    print("-- ",end="")
    print(str(x))
print()



# ToDo: check frontier code. keep current cost of frontier, which is number 
# of nodes in frontier? assuming no singleton sinks in frontier?

# ToDo: get rid of visited parameter?

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
# 
"""
