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

"""
def dfs_parent_pre_parent_traversal(node,visited,list_of_unvisited_children):
    check_list_of_unvisited_chldren_after_visiting_parents = False
    # set child node to visited if possible before dfs_parent so that when the parent 
    # checks if this child is visited it will be visited. 
    logger.trace("dfs_parent_pre: at start: list_of_unvisited_children:" + str(list_of_unvisited_children))
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
        logger.trace ("dfs_parent_pre: add " + str(node.ID) + " to visited since no children")              
    else:
        # node has more than one child or it has one child that has one or more
        # children (it is not a sink) or more than one parent (so if
        # we put child in partition we have to put all parents (including node)
        # in the partition (first))
        has_unvisited_children = False
        for neighbor_index in node.children:
            child_node = nodes[neighbor_index]
            if child_node.ID not in visited:
                logger.trace ("dfs_parent_pre: child " + str(child_node.ID) + " not in visited")
                has_unvisited_children = True
                list_of_unvisited_children.append(child_node.ID)
                #break
        if not has_unvisited_children:
            logger.trace ("dfs_parent_pre mark " + str(node.ID) + " as visited since it has no unvisited children "
            + "but do not add it to bfs queue since no children need to be visited")
            check_list_of_unvisited_chldren_after_visiting_parents = False
            visited.append(node.ID)
        else:
            logger.trace ("dfs_parent_pre " + str(node.ID) + " has unvisted children so mark " 
                + str(node.ID) + " as visited and check children again after parent traversal")
            # this node can be marked as visited, but we will only add it to the queue
            # if these unvisited children are still unvisited when we return from 
            # visiting the parent nodes (all ancestors). If children nodes are 
            # also parents or ancestors (in general) then we may visit them on 
            # parent traversal and if we visit all of th node's children this way then
            # we need not add node to the queue. 
            visited.append(node.ID)
            check_list_of_unvisited_chldren_after_visiting_parents = True
            logger.trace("dfs_parent_pre: set check_list_of_unvisited_chldren True")
            logger.trace("dfs_parent_pre: list_of_unvisited_children:" + str(list_of_unvisited_children))
#rhc: un
            node.unvisited_children = list_of_unvisited_children

#rhc: un
    for neighbor_index in node.parents:
        parent_node = nodes[neighbor_index]
        if parent_node.ID in visited:
            parent_node.unvisited_children.remove(node.ID)
            if len(parent_node.unvisited_children) == 0:
                logger.trace("*******dfs_parent_pre_parent_traversal: " + str(parent_node.ID) + " after removing child " 
                    + str(node.ID) + " has no unvisited children, so remove "
                    + str(parent_node.ID) + " from queue and frontier.")
                try:
                    queue.remove(parent_node.ID)
                except ValueError:
                    logger.trace("*******dfs_parent_pre_parent_traversal: " + str(parent_node.ID)
                    + " not in queue.")
                try:
                    frontier.remove(parent_node.ID)
                except ValueError:
                    logger.trace("*******dfs_parent_pre_parent_traversal: " + str(parent_node.ID)
                    + " not in frontier.")
    
    return check_list_of_unvisited_chldren_after_visiting_parents
"""

"""
def dfs_parent_post_parent_traversal(node, visited, list_of_unvisited_children, check_list_of_unvisited_chldren_after_visiting_parents):
    # See if the unvisited children found above are still unvisited
    unvisited_children_after_parent_loop = []
    if check_list_of_unvisited_chldren_after_visiting_parents:
        for child_index in list_of_unvisited_children:
            logger.trace("check unvisited child " + str(child_index))
            child_node = nodes[child_index]
            if child_node.ID not in visited:
                logger.trace("unvisited child " + str(child_node.ID) + " not visited during parent traversal")
                # Did not visit this unvsited child when visiting parents
                unvisited_children_after_parent_loop.append(child_node.ID)
#rhc: un
        node.unvisited_children = unvisited_children_after_parent_loop
    logger.trace(str(len(unvisited_children_after_parent_loop)) + " children remain unvisited")

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
            logger.trace("check whether node " + str(node.ID) + " unvisited child " + str(unvisited_child) + " is still unvisited"
                + " after parent traversal")
            if unvisited_child not in unvisited_children_after_parent_loop:
                # Unvisited_child is no longer unvisited after call to dfs_parent.
                # A child that was unvisited before parent loop traverasal but
                # that was visited durng this traversal is part of a loop.
                # This child is also a parent or ancestor.
                # output loop (L) indicators in partition, children are in no 
                # particular order.
                logger.trace("unvisited child " + str(unvisited_child) + " not still unvisited")
                if first:
                    first = False
                    loop_indicator = str(nodes[unvisited_child].ID)
                else:
                    loop_indicator += "/" + str(nodes[unvisited_child].ID)
                logger.debug_loop_indicator = True
            else:
                logger.trace("unvisited child " + str(unvisited_child) + " was not visited during parent traversal")
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
            logger.trace("[Info]: possible loop detected, loop indicator: " + loop_indicator)

    if len(unvisited_children_after_parent_loop) > 0:
        # There remains some unvisited children
#rhc: ToDo: check for singleton
        if IDENTIFY_SINGLETONS and (
        len(unvisited_children_after_parent_loop)) == 1:
            # in fact, there is only one unvisited child
            logger.trace("1 unvisited child after parent loop.")
            #only_child_index = node.children[0]
            unvisited_child_index = unvisited_children_after_parent_loop[0]
            logger.trace("unvisited_child_index: " + str(unvisited_child_index))
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
                logger.trace("the 1 unvisited child after parent loop is a singleton"
                    + " mark it visited and add parent (first) and child to partition.")
                visited.append(unvisited_child.ID)
                # add node to partition before child 
                if node.partition_number == -1:
                    logger.trace ("dfs_parent add " + str(node.ID) + " to partition")
                    node.partition_number = current_partition_number

                    partition_node = Partition_Node(node.ID)
                    partition_node.ID = node.ID
                    # The parents of node must already be in the partition and thus
                    # in the nodeIndex_to_partitionIndex_map


                    #global nodeIndex_to_partitionIndex_map
                    #for parent in node.parents:
                    #    new_index = nodeIndex_to_partitionIndex_map.get(parent)
                    #    if new_index != None:
                    #        partition_node.parents.append(new_index)
                    #    else:
                    #        patch_parent_mapping.append(partition_node)
                    ##partition_node.parents = node.parents


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
                    logger.trace ("dfs_parent do not add " + str(node.ID) + " to partition "
                        + current_partition_number + " since it is already in partition " 
                        + node.partition_number)
                if unvisited_child.partition_number == -1:
                    logger.trace ("dfs_parent add " + str(unvisited_child.ID) + " to partition")
                    unvisited_child.partition_number = current_partition_number

                    partition_node = Partition_Node(unvisited_child.ID)
                    partition_node.ID = unvisited_child.ID

                    #for parent in unvisited_child.parents:
                    #    partition_node.parents.append(nodeIndex_to_partitionIndex_map[parent])
                    ##partition_node.parents = unvisited_child.parents

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
                    logger.error ("[Error]: dfs_parent do not add " + str(unvisited_child.ID) + " to partition "
                        + current_partition_number + " since it is already in partition " 
                        + unvisited_child.partition_number)

            else:
                #queue.append(node)
                queue.append(node.ID)
                if DEBUG_ON:
                    # logger.trace(, end=" ")
                    print_val = "queue after add " + str(node.ID) + ": "
                    for x in queue:
                        #logger.trace(x.ID, end=" ")
                        print_val = print_val + str(x) + " "
                    logger.trace(print_val)
                    logger.trace("")
                #frontier.append(node)
                frontier.append(node.ID)
                if DEBUG_ON:
                    print_val = "frontier after add " + str(node.ID) + ": "
                    for x in frontier:
                        #logger.trace(x.ID, end=" ")
                        print_val = print_val + str(x) + " "
                    logger.trace(print_val)
                    logger.trace("")
                # make sure parent in partition before any if its children. We visit parents of nodein dfs_parents 
                # and they are added to partition in dfs_parents after their parents are added 
                # in dfs_parents then here we add node to partition.  
                if node.partition_number == -1:
                    logger.trace ("dfs_parent add " + str(node.ID) + " to partition")
                    node.partition_number = current_partition_number

                    partition_node = Partition_Node(node.ID)
                    partition_node.ID = node.ID


                    #for parent in node.parents:
                    #    partition_node.parents.append(nodeIndex_to_partitionIndex_map[parent])
                    ##partition_node.parents = node.parents

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
                    logger.trace ("dfs_parent do not add " + str(node.ID) + " to partition "
                        + current_partition_number + " since it is already in partition " 
                        + node.partition_number)
        else:
                #queue.append(node)
                queue.append(node.ID)
                if DEBUG_ON:
                    print_val = "queue after add " + str(node.ID) + ":"
                    for x in queue:
                        #logger.trace(x.ID, end=" ")
                        #logger.trace(x, end=" ")
                        print_val = print_val + str(x) + " "
                    logger.trace(print_val)
                    logger.trace("")
                #frontier.append(node)
                frontier.append(node.ID)
                if DEBUG_ON:
                    print_val = "frontier after add " + str(node.ID) + ":"
                    for x in frontier:
                        #logger.trace(x.ID, end=" ")
                        #logger.trace(x, end=" ")
                        print_val = print_val + str(x) + " "
                    logger.trace(print_val)
                    logger.trace("")
                # make sure parent in partition before any if its children. We visit parents of nodein dfs_parents 
                # and they are added to partition in dfs_parents after their parents are added 
                # in dfs_parents then here we add node to partition.  
                if node.partition_number == -1:
                    logger.trace ("dfs_parent add " + str(node.ID) + " to partition")
                    node.partition_number = current_partition_number

                    partition_node = Partition_Node(node.ID)
                    partition_node.ID = node.ID

                    #for parent in node.parents:
                    #    partition_node.parents.append(nodeIndex_to_partitionIndex_map[parent])
                    ##partition_node.parents = node.parents

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
                    logger.trace ("dfs_parent do not add " + str(node.ID) + " to partition "
                        + current_partition_number + " since it is already in partition " 
                        + node.partition_number)
    else:
        logger.trace("node " + str(node.ID) + " has no unvisited children after parent traversal,"
            + " add it to partition but not queue")
        if node.partition_number == -1:
            logger.trace("dfs_parent add " + str(node.ID) + " to partition")
            node.partition_number = current_partition_number


            partition_node = Partition_Node(node.ID)
            partition_node.ID = node.ID

            #for parent in node.parents:
            #    partition_node.parents.append(nodeIndex_to_partitionIndex_map[parent])
            ##partition_node.parents = node.parents

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
            logger.trace("HHHHHHHHHHHHHHHH dfs_parent: pg_tuple generate for " + str(partition_node.ID)
                + str(pg_tuple))

        else:
            logger.trace("dfs_parent do not add " + str(node.ID) + " to partition "
                + current_partition_number + " since it is already in partition " 
                + node.partition_number)
"""