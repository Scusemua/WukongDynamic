
import queue
import numpy as np
import os

from . import BFS_Shared
from .DAG_executor_constants import use_shared_partitions_groups, use_page_rank_group_partitions
from .DAG_executor_constants import use_struct_of_arrays_for_pagerank
from .DAG_executor_constants import using_threads_not_processes
from .DAG_executor_constants import exit_program_on_exception
#from .DAG_executor_constants import use_multithreaded_multiprocessing

from .BFS_Partition_Node import Partition_Node


import logging 

logger = logging.getLogger(__name__)

"""
if not (not using_threads_not_processes or use_multithreaded_multiprocessing):
    logger.setLevel(logging.ERROR)
    formatter = logging.Formatter('[%(asctime)s] [%(module)s] [%(processName)s] [%(threadName)s]: %(message)s')
    ch = logging.StreamHandler()
    ch.setLevel(logging.ERROR)
    #ch.setLevel(logging.INFO)
    ch.setFormatter(formatter)
    logger.addHandler(ch)
"""

#When using a pagerank array that is shared amoung workers and that contains all
# the partitions/groups (instead of inputting each partition/group from a file)
# this generates the shared aray of partitions/groups. This includes the 
# struct of arrays.
#
# if not use_struct_of_arrays_for_pagerank:
#   #generating single array 
#   if not use_page_rank_group_partitions:
#       generate shared partitions
#   else:
#       generate shared groups
# else:
#   #generating struct of arrays
#   if not use_page_rank_group_partitions:
#       # generating partitions
#       if using_threads_not_processes:
#          threads access global shared partitions as struct of arrays
#       else:
#          processes access global partitions as struct of arrays in **multiprocessing shared memory**
#   else:
#       # generating groups
#       if using_threads_not_processes:
#          threads access global shared groups as struct of arrays
#       else:
#          processes access global groups as struct of arrays in **multiprocessing shared memory**

def generate_shared_partitions_groups(num_nodes,num_parent_appends,partitions,partition_names,
    partitions_num_shadow_nodes_list,num_shadow_nodes_added_to_partitions,
    groups, group_names,groups_num_shadow_nodes_list,num_shadow_nodes_added_to_groups):
    
    try:
        assert use_shared_partitions_groups , "[Error]: Internal Error: Called generate_shared_partitions_groups"
        + " but use_shared_partitions_groups is False."
    except AssertionError:
        logger.exception("[Error]: assertion failed")
        if exit_program_on_exception:
            logging.shutdown()
            os._exit(0)
    # assertOld
    #if not use_shared_partitions_groups:
    #    logger.trace("[Error]: Internal Error: Called generate_shared_partitions_groups"
    #        + " but use_shared_partitions_groups is False.")

    # Either the values needed for pagerank are stored in individual 
    # Partition_Node in a single shared array, or we have multiple
    # arrays, one for each of the needed values, e.g., array of 
    # num_children values, array of num_parents values, etc.
    if not use_struct_of_arrays_for_pagerank:
        #rhc shared
        if not use_page_rank_group_partitions:
            next = 0
            for name, partition, num_shadow_nodes in zip(partition_names, partitions, partitions_num_shadow_nodes_list):
                partition_position = next
                partition_size = len(partition)
                num_shadow_nodes_seen_so_far = 0
                queue_of_shadow_node_IDs = queue.Queue()
                for p_node in partition:
                    BFS_Shared.shared_partition.append(p_node)
                    # For shadow nodes, the value -1 was appended to its
                    # parents, so len(p_node.parents) is the correct value
                    # but need to change -1 to the actual position of shadow
                    # nodes parent, which will be partition_size plus the 
                    # number of shadow_nodes we have already processed. That is,
                    # we will add he parent nodes to the partition after we have
                    # processed all of the p_nodes. So the first shadow node will
                    # have a parent that is the first parent node added. This
                    # parent, if the size of the partition is initially partition_size 
                    # will be at position partition_size (i.e., if partition_size is 3 
                    # the positions of the p_nodes are 0, 1, and 2, so the next 
                    # position is 3 = partition_size + num_shadow_nodes = 3+0, where
                    # num_shadow_nodes is initially 0, and is incremented
                    # *after* we use it to get the position of the next parent.)
                    if p_node.isShadowNode:
                        # Note: we are not changing partition_size as we are not
                        # adding a parent node here. The parent nodes are added
                        # next and partition_size is incremented as we add parents.
                        p_node.parents[0] = partition_size + num_shadow_nodes_seen_so_far
                        queue_of_shadow_node_IDs.put(p_node.ID)
                        num_shadow_nodes_seen_so_far += 1
                    next += 1
                for _ in range(num_shadow_nodes):
                    my_ID = queue_of_shadow_node_IDs.get()
                    my_ID = str(my_ID) + "-s-p"
                    parent_node = Partition_Node(my_ID)
                    parent_node.num_children = 1
                    BFS_Shared.shared_partition.append(parent_node)
                    next += 1
                    partition_size += 1
                partition_triple = (partition_position,partition_size,num_shadow_nodes)
                BFS_Shared.shared_partition_map[name] = partition_triple
            logger.trace("Number of shadow nodes for partitions:")
            for num in partitions_num_shadow_nodes_list:
                logger.trace(num)
            logger.trace("shared_partition_map:")
            for (k,v) in BFS_Shared.shared_partition_map.items():
                logger.trace(str(k) + ", (" + str(v[0]) + "," + str(v[1]) + "," + str(v[2]) + ")")
            logger.trace("shared_partition (w/ : parents : num_children)")
            for p_node in BFS_Shared.shared_partition:
                #logger.trace(p_node)
                print_val = ""
                print_val += str(p_node) + ": "
                if len(p_node.parents) == 0:
                    print_val += "- "
                else:
                    for parent in p_node.parents:
                        print_val += str(parent) + " "
                print_val += ": " + str(p_node.num_children)
                logger.trace(print_val)
            logger.trace("")
        else:
            next = 0
            for name, group, num_shadow_nodes in zip(group_names, groups, groups_num_shadow_nodes_list):
                group_position = next
                group_size = len(group)
                num_shadow_nodes_seen_so_far = 0
                queue_of_shadow_node_IDs = queue.Queue()
                for p_node in group:
                    BFS_Shared.shared_groups.append(p_node)
                    # For shadow nodes, the value -1 was appended to its
                    # parents, so len(p_node.parents) is the correct value
                    # but need to change -1 to the actual position of shadow
                    # nodes parent, which will be partition_size plus the 
                    # number of shadow_nodes we have already processed. That is,
                    # we will add he parent nodes to the partition after we have
                    # processed all of the p_nodes. So the first shadow node will
                    # have a parent that is the first parent node added. This
                    # parent, if the size of the partition is initially partition_size 
                    # will be at position partition_size (i.e., if partition_size is 3 
                    # the positions of the p_nodes are 0, 1, and 2, so the next 
                    # position is 3 = partition_size + num_shadow_nodes = 3+0, where
                    # num_shadow_nodes is initially 0, and is incremented
                    # *after* we use it to get the position of the next parent.)
                    if p_node.isShadowNode:
                        # Note: we are not changing partition_size as we are not
                        # adding a parent node here. The parent nodes are added
                        # next and partition_size is incremented as we add parents.
                        p_node.parents[0] = group_size + num_shadow_nodes_seen_so_far
                        queue_of_shadow_node_IDs.put(p_node.ID)
                        num_shadow_nodes_seen_so_far += 1
                    next += 1
                for _ in range(num_shadow_nodes):
                    my_ID = queue_of_shadow_node_IDs.get()
                    my_ID = str(my_ID) + "-s-p"
                    parent_node = Partition_Node(my_ID)
                    parent_node.num_children = 1
                    BFS_Shared.shared_groups.append(parent_node)
                    next += 1
                    group_size += 1
                group_triple = (group_position,group_size,num_shadow_nodes)
                BFS_Shared.shared_groups_map[name] = group_triple
            logger.trace("Number of shadow nodes for groups:")
            for num in groups_num_shadow_nodes_list:
                logger.trace(num)
            logger.trace("shared_groups_map:")
            for (k,v) in BFS_Shared.shared_groups_map.items():
                logger.trace(str(k) + ", (" + str(v[0]) + "," + str(v[1]) + "," + str(v[2]) + ")")
            logger.trace("shared_groups (w/ : parents : num_children)")
            for p_node in BFS_Shared.shared_groups:
                #logger.trace(p_node)
                print_val = ""
                print_val += str(p_node) + ": "
                if len(p_node.parents) == 0:
                    print_val += "- "
                else:
                    for parent in p_node.parents:
                        print_val += str(parent) + " "     
                print_val += ": " + str(p_node.num_children)
                logger.trace(print_val)
            logger.trace("")
    else: 
        """ In BFS_Shared.py:
        global pagerank
        global previous
        global number_of_children
        global number_of_parents
        global starting_indices_of_parents
        global IDs
        global parents
        """
        #rhc shared
        #if not use_page_rank_group_partitions:
        next = 0
        next_parent_index = 0
        # 64 byte padding : w/ 32 bit ints
        # Note: For pagerank and previous we allocate for each an entire empty 
        # array that includes space for padding between partitions/groups.
        # The pagerank and previous values are computed during DAG execution
        # for positions in the array that correspn to nodes. Other positions
        # are implicitly padding and are never assigned a value -- their values
        # will be random.
        int_padding = np.array([-4,-4,-4,-4, -4,-4,-4,-4, -4,-4,-4,-4, -4,-4,-4,-4])
        logger.trace("int padding: " + str(int_padding))
        if not use_page_rank_group_partitions:
            # size of pagerank and previos is n floats, where n is the number of 
            # nodes in the input graph + the number of shadow nodes and their 
            # parent nodes (2*num_shadow_nodes_added_to_partitions) plus 
            # the padding, where there is 64 bytes of padding between partitions
            # /groups and we are padding with ints so 64*4=16 ints giving a
            # total of (len(partitions/groups)-1)*16 ints added for padding.
            np_arrays_size_for_shared_partition = num_nodes + (
                (2*num_shadow_nodes_added_to_partitions) + ((len(partitions)-1)*16)
            )
            logger.trace("num_nodes: " + str(num_nodes) 
                + " (2*num_shadow_nodes_added_to_partitions):" + str((2*num_shadow_nodes_added_to_partitions))
                + " ((len(partitions)-1)*16): " + str(((len(partitions)-1)*16)))
            logger.trace("np_arrays_size_for_shared_partition: " 
                + str(np_arrays_size_for_shared_partition))
            # the size of the parent array is the total number of parents for all the 
            # nodes, which is trackd as num_parent_appends in the input_graph() method
            # (incrementing whenever we append to a parents list) plus the padding
            # between partitions/groups which is 16 ints that are added between 
            # the partitions/groups so we pad len(partitions/groups)-1 times.
            np_arrays_size_for_shared_partition_parents = num_parent_appends + num_shadow_nodes_added_to_partitions + ((len(partitions)-1)*16)
            logger.trace("num_parent_appends: " + str(num_parent_appends) 
                + " num_shadow_nodes_added_to_partitions:" + str(num_shadow_nodes_added_to_partitions)
                + " ((len(partitions)-1)*16): " + str(((len(partitions)-1)*16)))
            logger.trace("np_arrays_size_for_shared_partition_parents: "
                + str(np_arrays_size_for_shared_partition_parents))
            # 1/num_nodes is used for the initial value of pagerank
            if using_threads_not_processes:
                BFS_Shared.initialize_struct_of_arrays(num_nodes, 
                    np_arrays_size_for_shared_partition,
                    np_arrays_size_for_shared_partition_parents)
            else:
                BFS_Shared.initialize_struct_of_arrays_shared_memory(num_nodes, 
                    np_arrays_size_for_shared_partition,
                    np_arrays_size_for_shared_partition_parents)
            """
            ToDo: For now we use full instead of empty for pagerank so we
            can fill it will a sentinal value that helps for debugging, i.e.,
            we see all -4's or something for pagerank values that have not
            yet been computed, instead of random values. Using empty will
            save time.
            BFS_Shared.pagerank = np.empty(np_arrays_size_for_shared_partition,dtype=np.double)
            # prev[i] is previous pagerank value of i
            BFS_Shared.previous = np.full(np_arrays_size_for_shared_partition,float((1/num_nodes)))
            # num_chldren[i] is number of child nodes of node i
            # rhc: Q: make these short or something shorter than int?
            BFS_Shared.number_of_children = np.empty(np_arrays_size_for_shared_partition,dtype=np.intc)
            # numParents[i] is number of parent nodes of node i
            BFS_Shared.number_of_parents = np.empty(np_arrays_size_for_shared_partition,dtype=np.intc)
            # parent_index[i] is the index in parents[] of the first of 
            # num_parents parents of node i
            BFS_Shared.starting_indices_of_parents = np.empty(np_arrays_size_for_shared_partition,dtype=np.intc)
            BFS_Shared.IDs = np.empty(np_arrays_size_for_shared_partition,dtype=np.intc)
            # parents - to get the parents of node i: num_parents = numParents[i];
            # parent_index = parent_index[i]; 
            # for j in (parent_index,num_parents) parent = parents[j]
            BFS_Shared.parents = np.empty(np_arrays_size_for_shared_partition_parents,dtype=np.intc)
            """
            if using_threads_not_processes:
                num_partitions_processed = 0
                for name, partition, num_shadow_nodes in zip(partition_names, partitions, partitions_num_shadow_nodes_list):
                    logger.trace("name: " + name)
                    partition_position = next
                    partition_size = len(partition)
                    # Note: in dfs_parent:
                    # shadow_node.num_children = len(visited_parent_node.children)
                    # shadow_node.parents.append(-1)
                    # This -1 will be overwritten; the parent is a node after
                    # the end of the partiton with a pagerank value
                    # that keeps the shadow_node's pagerank value constant.
                    num_shadow_nodes_seen_so_far = 0
                    queue_of_shadow_node_IDs = queue.Queue()
                    for p_node in partition:
                        #BFS_Shared.shared_partition.append(p_node)
                        # For shadow nodes:
                        # - num_children was set to num children of
                        # actual parent node in the different partition/group.
                        # - number of parents will be one since -1 was appended
                        # to the shadow node's parent list
                        BFS_Shared.number_of_children[next] = p_node.num_children
                        BFS_Shared.number_of_parents[next] = len(p_node.parents)
                        BFS_Shared.starting_indices_of_parents[next] = next_parent_index
                        BFS_Shared.IDs[next] = p_node.ID
                        # For shadow nodes, the value -1 was appended to its
                        # parents, so len(p_node.parents) is the correct value
                        # but need to change -1 to the actual position of shadow
                        # nodes parent, which will be partition_size plus the 
                        # number of shadow_nodes we have already processed. That is,
                        # we will add he parent nodes to the partition after we have
                        # processed all of the p_nodes. So the first shadow node will
                        # have a parent that is the first parent node added. This
                        # parent, if the size of the partition is initially partition_size 
                        # will be at position partition_size (i.e., if partition_size is 3 
                        # the positions of the p_nodes are 0, 1, and 2, so the next 
                        # position is 3 = partition_size + num_shadow_nodes = 3+0, where
                        # num_shadow_nodes is initially 0, and is incremented
                        # *after* we use it to get the position of the next parent.)
                        if p_node.isShadowNode:
                            # Note: we are not changing partition_size as we are not
                            # adding a parent node here. The parent nodes are added
                            # next and partition_size is incremented as we add parents.
                            #
                            # We will add the parent node at the end of the partition.
                            # This position depends on num_shadow_nodes_seen so far
                            p_node.parents[0] = partition_size + num_shadow_nodes_seen_so_far
                            queue_of_shadow_node_IDs.put(p_node.ID)
                            num_shadow_nodes_seen_so_far += 1
                        logger.trace("len(p_node.parents: " + str(len(p_node.parents)))
                        for parent in p_node.parents:
                            logger.trace("parent loop: next_parent_index:" + str(next_parent_index))

                            # Note: Shadow nodes have one parent, which is a parent node,
                            # and this parent index was just set to (partition_size + num_shadow_nodes)
                            BFS_Shared.parents[next_parent_index] = parent
                            next_parent_index += 1
                        next += 1

                    # After adding nodes, including shadow nodes, add parents of shadow nodes
                    for _ in range(num_shadow_nodes):
                        #BFS_Shared.shared_partition.append(Partition_Node(-2))
                        # Note: The pagerank value of the parent node will be 
                        # set when the shadow node's pagerank value is set, i.e.,
                        # the shadow node shadows a node in a different 
                        # partition/group PG, and when this partition/group PG is 
                        # executed, after PGs pagerank values are computed PG will
                        # will set all the associated shadow nodes and their
                        # parents with pagerank values computed by PG.
                        # 
                        # parent node IDs are the asme as the correspnding shadow_node
                        # ID, using the queue of shadow nodes (FIFO)
                        my_ID = queue_of_shadow_node_IDs.get()

                        BFS_Shared.number_of_children[next] = 1
                        # parent nodes have no parents. Also, we do not compute
                        # a pagerank value for parent nodes but we use the parent's
                        # pagerank value when we compute the pagerank for its
                        # shadow node child. The pagerank of the parent is set so that
                        # we always compute the same pagerank value for the shadow node.
                        # (We do compuet the pagerank for the shadow node like the 
                        # non-shadow nodes. Since we compute pagerank for all the nodes
                        # (except the parents of shadow nodes, which are not included
                        # in the loop over the partition/group), we do not need an if-statement
                        # which is a traeoff - extra pagerank computations (whcih are quick)
                        # for no mispredicted branchs.)
                        BFS_Shared.number_of_parents[next] = 0
                        # Note: No parents need be added to the parents array
                        BFS_Shared.starting_indices_of_parents[next] = -2
                        BFS_Shared.IDs[next] = my_ID
                        next += 1
                        partition_size += 1

                    if num_partitions_processed < len(partitions)-1:
                        for j in range(len(int_padding)):
                            logger.trace("padding loop j: " + str(j) + " next: " + str(next)
                                + " next_parent_index:" + str(next_parent_index))
                            BFS_Shared.number_of_children[next] = int_padding[j]
                            BFS_Shared.number_of_parents[next] = int_padding[j]
                            BFS_Shared.starting_indices_of_parents[next] = int_padding[j]
                            BFS_Shared.IDs[next] = int_padding[j]
                            # rhc: It is possible that a root node of BFS (there may be more than one)
                            # does not have any parents, whcih happens if it is the only node in its
                            # partition/group. Then we will still add padding between the zero parents
                            # of the root partition/group and the parents of the next partition/group.
                            # We do this since we assume there will be padding between all the partitions/groups
                            # when we calculate the size of the np array at the start. 
                            # ToDo: During BFS we can count the number of partitions/groups that have
                            # no parents and thus how many paddings we should drop when we compute
                            # the size of the np array of parents.
                            BFS_Shared.parents[next_parent_index] = int_padding[j]
                            next += 1
                            next_parent_index += 1
                        #partition_size += 16
                        #next_parent_index += 16
                    partition_triple = (partition_position,partition_size,num_shadow_nodes)
                    BFS_Shared.shared_partition_map[name] = partition_triple
                    num_partitions_processed += 1
                logger.trace("Number of shadow nodes for partitions:")
                for num in partitions_num_shadow_nodes_list:
                    logger.trace(num)
                logger.trace("shared_partition_map:")
                for (k,v) in BFS_Shared.shared_partition_map.items():
                    logger.trace(str(k) + ", (" + str(v[0]) + "," + str(v[1]) + "," + str(v[2]) + ")")
                logger.trace("Shared_Arrays")
                logger.trace("BFS_Shared.pagerank:")
                for element in BFS_Shared.pagerank:
                    logger.trace(str(element)+",")
                logger.trace("BFS_Shared.previous:")
                for element in BFS_Shared.previous:
                    logger.trace(str(element)+",")
                logger.trace("BFS_Shared.number_of_children:")
                for element in BFS_Shared.number_of_children:
                    logger.trace(str(element)+",")
                logger.trace("BFS_Shared.number_of_parents: ")
                for element in BFS_Shared.number_of_parents:
                    logger.trace(str(element)+",")
                logger.trace("BFS_Shared.starting_indices_of_parents:")
                for element in BFS_Shared.starting_indices_of_parents:
                    logger.trace(str(element)+",")
                logger.trace("BFS_Shared.parents:")
                for element in BFS_Shared.parents:
                    logger.trace(str(element)+",")
                logger.trace("")
            else:
                num_partitions_processed = 0
                for name, partition, num_shadow_nodes in zip(partition_names, partitions, partitions_num_shadow_nodes_list):
                    partition_position = next
                    partition_size = len(partition)
                    # Note: in dfs_parent:
                    # shadow_node.num_children = len(visited_parent_node.children)
                    # shadow_node.parents.append(-1)
                    # This -1 will be overwritten; the parent is a node after
                    # the end of the partiton with a pagerank value
                    # that keeps the shadow_node's pagerank value constant.
                    num_shadow_nodes_seen_so_far = 0
                    queue_of_shadow_node_IDs = queue.Queue()
                    for p_node in partition:
                        #BFS_Shared.shared_partition.append(p_node)
                        # For shadow nodes:
                        # - num_children was set to num children of
                        # actual parent node in the different partition/group.
                        # - number of parents will be one since -1 was appended
                        # to the shadow node's parent list
                        BFS_Shared.nonshared_number_of_children[next] = p_node.num_children
                        BFS_Shared.nonshared_number_of_parents[next] = len(p_node.parents)
                        BFS_Shared.nonshared_starting_indices_of_parents[next] = next_parent_index
                        BFS_Shared.nonshared_IDs[next] = p_node.ID
                        # For shadow nodes, the value -1 was appended to its
                        # parents, so len(p_node.parents) is the correct value
                        # but need to change -1 to the actual position of shadow
                        # nodes parent, which will be partition_size plus the 
                        # number of shadow_nodes we have already processed. That is,
                        # we will add he parent nodes to the partition after we have
                        # processed all of the p_nodes. So the first shadow node will
                        # have a parent that is the first parent node added. This
                        # parent, if the size of the partition is initially partition_size 
                        # will be at position partition_size (i.e., if partition_size is 3 
                        # the positions of the p_nodes are 0, 1, and 2, so the next 
                        # position is 3 = partition_size + num_shadow_nodes = 3+0, where
                        # num_shadow_nodes is initially 0, and is incremented
                        # *after* we use it to get the position of the next parent.)
                        if p_node.isShadowNode:
                            # Note: we are not changing partition_size as we are not
                            # adding a parent node here. The parent nodes are added
                            # next and partition_size is incremented as we add parents.
                            #
                            # We will add the parent node at the end of the partition.
                            # This position depends on num_shadow_nodes_seen so far
                            p_node.parents[0] = partition_size + num_shadow_nodes_seen_so_far
                            queue_of_shadow_node_IDs.put(p_node.ID)
                            num_shadow_nodes_seen_so_far += 1
                        for parent in p_node.parents:
                            # Note: Shadow nodes have one parent, which is a parent node,
                            # and this parent index was just set to (partition_size + num_shadow_nodes)
                            BFS_Shared.nonshared_parents[next_parent_index] = parent
                            next_parent_index += 1
                        next += 1

                    # After adding nodes, including shadow nodes, add parents of shadow nodes
                    for _ in range(num_shadow_nodes):
                        #BFS_Shared.nonshared_shared_partition.append(Partition_Node(-2))
                        # Note: The pagerank value of the parent node will be 
                        # set when the shadow node's pagerank value is set, i.e.,
                        # the shadow node shadows a node in a different 
                        # partition/group PG, and when this partition/group PG is 
                        # executed, after PGs pagerank values are computed PG will
                        # will set all the associated shadow nodes and their
                        # parents with pagerank values computed by PG.
                        # 
                        # parent node IDs are the asme as the correspnding shadow_node
                        # ID, using the queue of shadow nodes (FIFO)
                        my_ID = queue_of_shadow_node_IDs.get()

                        BFS_Shared.nonshared_number_of_children[next] = 1
                        # parent nodes have no parents. Also, we do not compute
                        # a pagerank value for parent nodes but we use the parent's
                        # pagerank value when we compute the pagerank for its
                        # shadow node child. The pagerank of the parent is set so that
                        # we always compute the same pagerank value for the shadow node.
                        # (We do compuet the pagerank for the shadow node like the 
                        # non-shadow nodes. Since we compute pagerank for all the nodes
                        # (except the parents of shadow nodes, which are not included
                        # in the loop over the partition/group), we do not need an if-statement
                        # which is a traeoff - extra pagerank computations (whcih are quick)
                        # for no mispredicted branchs.)
                        BFS_Shared.nonshared_number_of_parents[next] = 0
                        # Note: No parents need be added to the parents array
                        BFS_Shared.nonshared_starting_indices_of_parents[next] = -2
                        BFS_Shared.nonshared_IDs[next] = my_ID
                        next += 1
                        partition_size += 1

                    if num_partitions_processed < len(partitions)-1:
                        for j in range(len(int_padding)):
                            BFS_Shared.nonshared_number_of_children[next] = int_padding[j]
                            BFS_Shared.nonshared_starting_indices_of_parents[next] = int_padding[j]
                            BFS_Shared.nonshared_number_of_parents[next] = int_padding[j]
                            BFS_Shared.nonshared_IDs[next] = int_padding[j]
                            BFS_Shared.nonshared_parents[next_parent_index] = int_padding[j]
                            next += 1
                            next_parent_index += 1
                        #partition_size += 16
                        #next_parent_index += 16
                    partition_triple = (partition_position,partition_size,num_shadow_nodes)
                    BFS_Shared.shared_partition_map[name] = partition_triple
                    num_partitions_processed += 1
                logger.trace("Number of shadow nodes for partitions:")
                for num in partitions_num_shadow_nodes_list:
                    logger.trace(num)
                logger.trace("shared_partition_map:")
                for (k,v) in BFS_Shared.shared_partition_map.items():
                    logger.trace(str(k) + ", (" + str(v[0]) + "," + str(v[1]) + "," + str(v[2]) + ")")
                logger.trace("Shared_Arrays")
                logger.trace("BFS_Shared.nonshared_pagerank:")
                for element in BFS_Shared.nonshared_pagerank:
                    logger.trace(str(element)+",")
                logger.trace("BFS_Shared.nonshared_previous:")
                for element in BFS_Shared.nonshared_previous:
                    logger.trace(str(element)+",")
                logger.trace("BFS_Shared.nonshared_number_of_children:")
                for element in BFS_Shared.nonshared_number_of_children:
                    logger.trace(str(element)+",")
                logger.trace("BFS_Shared.nonshared_number_of_parents: ")
                for element in BFS_Shared.nonshared_number_of_parents:
                    logger.trace(str(element)+",")
                logger.trace("BFS_Shared.nonshared_starting_indices_of_parents:")
                for element in BFS_Shared.nonshared_starting_indices_of_parents:
                    logger.trace(str(element)+",")
                logger.trace("BFS_Shared.nonshared_parents:")
                for element in BFS_Shared.nonshared_parents:
                    logger.trace(str(element)+",")
                logger.trace("")
                logger.trace("BFS_Shared.generate_struct_of_arrays_shared_memory()")
                BFS_Shared.generate_struct_of_arrays_shared_memory()
        else:
            # See the comment above about these values
            np_arrays_size_for_shared_groups = num_nodes + (
                (2*num_shadow_nodes_added_to_groups) + ((len(groups)-1)*16)
            )
            np_arrays_size_for_shared_groups_parents = num_parent_appends + num_shadow_nodes_added_to_groups + ((len(groups)-1)*16)
            logger.trace("num_parent_appends: " + str(num_parent_appends)
                + " ((len(groups)-1)*16): " + str(((len(groups)-1)*16)))

            if using_threads_not_processes:
                BFS_Shared.initialize_struct_of_arrays(num_nodes, 
                    np_arrays_size_for_shared_groups,
                    np_arrays_size_for_shared_groups_parents)
            else:
                BFS_Shared.initialize_struct_of_arrays_shared_memory(num_nodes, 
                    np_arrays_size_for_shared_groups,
                    np_arrays_size_for_shared_groups_parents)

            """
            ToDo: For now we use full instead of empty for pagerank so we
            can fill it will a sentinal value that helps for debugging, i.e.,
            we see all -4's or something for pagerank values that have not
            yet been computed, instead of random values. Using empty will
            save time.
            BFS_Shared.pagerank = np.empty(np_arrays_size_for_shared_groups_pagerank_and_previous,dtype=np.double)
            # prev[i] is previous pagerank value of i
            BFS_Shared.previous = np.full(np_arrays_size_for_shared_groups_pagerank_and_previous,float((1/num_nodes)))
            # num_chldren[i] is number of child nodes of node i
            # rhc: Q: make these short or something shorter than int?
            BFS_Shared.number_of_children = np.empty(np_arrays_size_for_shared_groups,dtype=np.intc)
            # numParents[i] is number of parent nodes of node i
            BFS_Shared.number_of_parents = np.empty(np_arrays_size_for_shared_groups,dtype=np.intc)
            # parent_index[i] is the index in parents[] of the first of 
            # num_parents parents of node i
            BFS_Shared.starting_indices_of_parents = np.empty(np_arrays_size_for_shared_groups,dtype=np.intc)
            BFS_Shared.IDs = np.empty(np_arrays_size_for_shared_partition,dtype=np.intc)
            # parents - to get the parents of node i: num_parents = numParents[i];
            # parent_index = parent_index[i]; 
            # for j in (parent_index,num_parents) parent = parents[j]
            BFS_Shared.parents = np.empty(np_arrays_size_for_shared_groups,dtype=np.intc)
            """
            if using_threads_not_processes:
                next = 0
                next_parent_index = 0
                num_groups_processed = 0
                for name, group, num_shadow_nodes in zip(group_names, groups, groups_num_shadow_nodes_list):
                    group_position = next
                    group_size = len(group)
                    num_shadow_nodes_seen_so_far = 0
                    queue_of_shadow_node_IDs = queue.Queue()
                    for p_node in group:
                        #BFS_Shared.shared_groups.append(p_node)
                        BFS_Shared.number_of_children[next] = p_node.num_children
                        BFS_Shared.number_of_parents[next] = len(p_node.parents)
                        BFS_Shared.starting_indices_of_parents[next] = next_parent_index
                        BFS_Shared.IDs[next] = p_node.ID
                        # For shadow nodes, the value -1 was appended to its
                        # parents, so len(p_node.parents) is the correct value
                        # but need to change -1 to the actual position of shadow
                        # nodes parent, which will be partition_size plus the 
                        # number of shadow_nodes we have already processed. That is,
                        # we will add he parent nodes to the partition after we have
                        # processed all of the p_nodes. So the first shadow node will
                        # have a parent that is the first parent node added. This
                        # parent, if the size of the partition is initially partition_size 
                        # will be at position partition_size (i.e., if partition_size is 3 
                        # the positions of the p_nodes are 0, 1, and 2, so the next 
                        # position is 3 = partition_size + num_shadow_nodes = 3+0, where
                        # num_shadow_nodes is initially 0, and is incremented
                        # *after* we use it to get the position of the next parent.)
                        if p_node.isShadowNode:
                            # Note: we are not changing partition_size as we are not
                            # adding a parent node here. The parent nodes are added
                            # next and partition_size is incremented as we add parents.
                            p_node.parents[0] = group_size + num_shadow_nodes_seen_so_far
                            queue_of_shadow_node_IDs.put(p_node.ID)
                            num_shadow_nodes_seen_so_far += 1
                        for parent in p_node.parents:
                            BFS_Shared.parents[next_parent_index] = parent
                            logger.trace("for node: " + str(p_node) + " parent is: " 
                                + str(parent) + " at position: " + str(next_parent_index))
                            next_parent_index += 1
                        next += 1
                    for _ in range(num_shadow_nodes):
                        #BFS_Shared.shared_groups.append(Partition_Node(-2))
                        my_ID = queue_of_shadow_node_IDs.get()
                        BFS_Shared.IDs[next] = my_ID
                        BFS_Shared.number_of_children[next] = 1
                        BFS_Shared.number_of_parents[next] = 0
                        BFS_Shared.starting_indices_of_parents[next] = -2
                        next += 1
                        group_size += 1
                    if num_groups_processed < (len(groups)-1):
                        for j in range(len(int_padding)):
                            logger.trace("padding loop j: " + str(j) + " next: " + str(next)
                                + " next_parent_index:" + str(next_parent_index))
                            BFS_Shared.number_of_children[next] = int_padding[j]
                            BFS_Shared.number_of_parents[next] = int_padding[j]
                            BFS_Shared.starting_indices_of_parents[next] = int_padding[j]
                            BFS_Shared.IDs[next] = int_padding[j]
                            # rhc: It is possible that a root node of BFS (there may be more than one)
                            # does not have any parents, whcih happens if it is the only node in its
                            # partition/group. Then we will still add padding between the zero parents
                            # of the root partition/group and the parents of the next partition/group.
                            # We do this since we assume there will be padding between all the partitions/groups
                            # when we calculate the size of the np array at the start. 
                            # ToDo: During BFS we can count the number of partitions/groups that have
                            # no parents and thus how many paddings we should drop when we compute
                            # the size of the np array of parents.
                            BFS_Shared.parents[next_parent_index] = int_padding[j]
                            next += 1
                            next_parent_index += 1
                        #group_size += 16
                        logger.trace("after padding: next_parent_index: " + str(next_parent_index))
                    group_triple = (group_position,group_size,num_shadow_nodes)
                    BFS_Shared.shared_groups_map[name] = group_triple
                    num_groups_processed += 1
                logger.trace("Number of shadow nodes for groups:")
                for num in groups_num_shadow_nodes_list:
                    logger.trace(num)
                logger.trace("shared_groups_map:")
                for (k,v) in BFS_Shared.shared_groups_map.items():
                    logger.trace(str(k) + ", (" + str(v[0]) + "," + str(v[1]) + "," + str(v[2]) + ")")
                logger.trace("Shared_Arrays")
                logger.trace("BFS_Shared.pagerank:")
                for element in BFS_Shared.pagerank:
                    logger.trace(str(element)+",")
                logger.trace("BFS_Shared.previous:")
                for element in BFS_Shared.previous:
                    logger.trace(str(element)+",")
                logger.trace("BFS_Shared.number_of_children:")
                for element in BFS_Shared.number_of_children:
                    logger.trace(str(element)+",")
                logger.trace("BFS_Shared.number_of_parents: ")
                for element in BFS_Shared.number_of_parents:
                    logger.trace(str(element)+",")
                logger.trace("BFS_Shared.starting_indices_of_parents:")
                for element in BFS_Shared.starting_indices_of_parents:
                    logger.trace(str(element)+",")
                logger.trace("BFS_Shared.IDs:")
                for element in BFS_Shared.IDs:
                    logger.trace(str(element)+",")
                logger.trace("BFS_Shared.parents:")
                for element in BFS_Shared.parents:
                    logger.trace(str(element)+",")
                logger.trace("")
            else:
                next = 0
                next_parent_index = 0
                num_groups_processed = 0
                for name, group, num_shadow_nodes in zip(group_names, groups, groups_num_shadow_nodes_list):
                    group_position = next
                    group_size = len(group)
                    num_shadow_nodes_seen_so_far = 0
                    queue_of_shadow_node_IDs = queue.Queue()
                    for p_node in group:
                        #BFS_Shared.shared_groups.append(p_node)
                        BFS_Shared.nonshared_number_of_children[next] = p_node.num_children
                        BFS_Shared.nonshared_number_of_parents[next] = len(p_node.parents)
                        BFS_Shared.nonshared_starting_indices_of_parents[next] = next_parent_index
                        BFS_Shared.nonshared_IDs[next] = p_node.ID
                        # For shadow nodes, the value -1 was appended to its
                        # parents, so len(p_node.parents) is the correct value
                        # but need to change -1 to the actual position of shadow
                        # nodes parent, which will be partition_size plus the 
                        # number of shadow_nodes we have already processed. That is,
                        # we will add he parent nodes to the partition after we have
                        # processed all of the p_nodes. So the first shadow node will
                        # have a parent that is the first parent node added. This
                        # parent, if the size of the partition is initially partition_size 
                        # will be at position partition_size (i.e., if partition_size is 3 
                        # the positions of the p_nodes are 0, 1, and 2, so the next 
                        # position is 3 = partition_size + num_shadow_nodes = 3+0, where
                        # num_shadow_nodes is initially 0, and is incremented
                        # *after* we use it to get the position of the next parent.)
                        if p_node.isShadowNode:
                            # Note: we are not changing partition_size as we are not
                            # adding a parent node here. The parent nodes are added
                            # next and partition_size is incremented as we add parents.
                            p_node.parents[0] = group_size + num_shadow_nodes_seen_so_far
                            queue_of_shadow_node_IDs.put(p_node.ID)
                            num_shadow_nodes_seen_so_far += 1
                        for parent in p_node.parents:
                            BFS_Shared.nonshared_parents[next_parent_index] = parent
                            logger.trace("for node: " + str(p_node) + " parent is: " 
                                + str(parent) + " at position: " + str(next_parent_index))
                            next_parent_index += 1
                        next += 1
                    for _ in range(num_shadow_nodes):
                        #BFS_Shared.nonshared_shared_groups.append(Partition_Node(-2))
                        my_ID = queue_of_shadow_node_IDs.get()
                        BFS_Shared.nonshared_IDs[next] = my_ID
                        BFS_Shared.nonshared_number_of_children[next] = 1
                        BFS_Shared.nonshared_number_of_parents[next] = 0
                        BFS_Shared.nonshared_starting_indices_of_parents[next] = -2
                        next += 1
                        group_size += 1
                    if num_groups_processed < (len(groups)-1):
                        for j in range(len(int_padding)):
                            BFS_Shared.nonshared_number_of_children[next] = int_padding[j]
                            BFS_Shared.nonshared_number_of_parents[next] = int_padding[j]
                            BFS_Shared.nonshared_starting_indices_of_parents[next] = int_padding[j]
                            BFS_Shared.nonshared_IDs[next] = int_padding[j]
                            BFS_Shared.nonshared_parents[next_parent_index] = int_padding[j]
                            next += 1
                            next_parent_index += 1
                        #group_size += 16
                        logger.trace("after padding: next_parent_index: " + str(next_parent_index))
                    group_triple = (group_position,group_size,num_shadow_nodes)
                    BFS_Shared.shared_groups_map[name] = group_triple
                    num_groups_processed += 1
                logger.trace("Number of shadow nodes for groups:")
                for num in groups_num_shadow_nodes_list:
                    logger.trace(num)
                logger.trace("shared_groups_map:")
                for (k,v) in BFS_Shared.shared_groups_map.items():
                    logger.trace(str(k) + ", (" + str(v[0]) + "," + str(v[1]) + "," + str(v[2]) + ")")
                logger.trace("Shared_Arrays")
                logger.trace("BFS_Shared.nonshared_pagerank:")
                for element in BFS_Shared.nonshared_pagerank:
                    logger.trace(str(element)+",")
                logger.trace("BFS_Shared.nonshared_previous:")
                for element in BFS_Shared.nonshared_previous:
                    logger.trace(str(element)+",")
                logger.trace("BFS_Shared.nonshared_number_of_children:")
                for element in BFS_Shared.nonshared_number_of_children:
                    logger.trace(str(element)+",")
                logger.trace("BFS_Shared.nonshared_number_of_parents: ")
                for element in BFS_Shared.nonshared_number_of_parents:
                    logger.trace(str(element)+",")
                logger.trace("BFS_Shared.nonshared_starting_indices_of_parents:")
                for element in BFS_Shared.nonshared_starting_indices_of_parents:
                    logger.trace(str(element)+",")
                logger.trace("BFS_Shared.nonshared_IDs:")
                for element in BFS_Shared.nonshared_IDs:
                    logger.trace(str(element)+",")
                logger.trace("BFS_Shared.nonshared_parents:")
                for element in BFS_Shared.nonshared_parents:
                    logger.trace(str(element)+",")
                logger.trace("")

                logger.trace("BFS_Shared.generate_struct_of_arrays_shared_memory()")
                BFS_Shared.generate_struct_of_arrays_shared_memory()
