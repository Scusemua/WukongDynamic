"""
ToDO:
Document the debug flag stuff
Note: We run BFS which calls DFS and uses Shared for comm
Fix DAG_info thing!!
Document BFS all
Create a output tuples map so each PR task can set the result of its
  dependents, then no input tuples since a PR task sets the values 
  itself instead of sending them to its dependents. So the task
  will get its tuples from the Output Tuple Map and set them 
  in the Shared array instea of iterating through its partition/group
  and creating its output tuples and outputting them. So no input 
  tuples for R shared task and no output since no output tuples (which
  then become input tuples via fanouts/fanins). So no values sent to 
  fanouts/fanins - use empty dictionariesf for now.
"""

"""
BFS() builds the DAG for pagerank. It begins by inputting the graph input_graph()
and then it starts a BFS traversal:
    for i in range(1,num_nodes+1):
        if i not in visited:
            logger.trace("*************Driver call BFS for node[" + str(i) + "]")
            bfs(visited, nodes[i])    # function calling
where visited is a set of visited nodes and nodes[] are the nodes input by input_graph().

BFS() generates collctions of nodes that are either called partitions or groups. A group
is essentially a sub-partition, i.e., a partition can be partitioned into one or more groups.
When using partitions, we compute the pageranks of the nodes in the partitions in ascending
order of partitions Pi, i.e., P1, P2, ... Pn. Thus. there is no parallelism in processing the 
partitions. So the partitions form a DAG but there is a sngle directed edge from Pi to Pi+1. 

The DAG represented by the groups has more parallelism and the granlarity is finer. In fact the 
granularity may be too fine and it may speed up DAG execution to cluster groups. There is
a lot of flexibility in the granlarity and amount of parellelism when executing a DAG of groups.

BFS() uses DFS(), so the pagerank partitions/groups are generated using a combinaion of
BFS and DFS, as explained below.

For the whiteboard example, there 20 nodes and the DAG has three partitions:

P1: 5 17 1
P2: 2 10 16 20 8 11 3 19 4 6 14 12
P3: 13 7 15 9 18 

and 7 groups:

G1: 5 17 1
G2: 2 10 16
G3: 20 8 11 3 19
G4: 4 6 14 12
G5: 13
G6: 7 15
G7: 9 18

(See the DAG below.)

Partitions/groups are not required to be equal in size. The nodes in a partion/group
are in "descendent order", e.g., 5 is the parent of 17 is the parent of 1 in the 
input graph. (The nodes of a group are determined using a DFS. For example, for G1,
we start with node 1, and do a DFS search based on the parent-relationship - the
parent of 1 is 17, and the parent of 17 is 5, and 5 has no parents. so DFS(1) followed
by DFS(17) followed by DFS(5) followed by return, return, return. During DFS, 
we enqueue 5, 17, and 1 (in that order) and switch back to BFS when the DFS finished,
So BFS deques 5 and DFS searches the children of 5, then BFS deques 17 and DFS searches the 
children of 17, then BFS deques 1 and DFS searches the children of 1, enqueing the parents 
of these children during the DFS of the children, and so on. (For example, the children
of 5 are 17 and 16. Node 17 has already been searched so there is no FS of 17 for the
child 17 of 5. There is however a DFS of child 16 which visits nodes 10 and 2. Nodes
2 and 10 (in that order, which means when DFS recursion unwinds back to the node) ar
enqueed for BFS.

Note that partiton P2 has been partitioned into three groups G2, G3, and G4, and that 
the descendent order in P2 has been retained in G2, G3, and G4.

The "descendents order" within a partition/group is very important. The pagrank
for a node depends on the pagrank of that node's parents (and this indirectly
on the parents of that node's parents, etc) Note that for a node 
N in a group, many of N's parents are in the same group. For example, for node 1
above, its only parent 17 is in the same group, likewse, the only parent 5
of node 17 is in the same group as 17. Node 5 has no parents. Thus, all the nodes
needed to compute the pageranks of the nodes in partition/group P1/G1, are in P1/G1.
If P1/G1 is assigned to "compute node" CN1, then CN1 dos not need to communicate
with any other compute node in order to compute its assigned pagerank values.
The parent(s) of a node N may not be in the N's partition/group. For example, parent
5 or node 16 is in a different partiton/group. This means that
some communication of pagerank values between compute nodes
will be required to compute the pagerank value of such an N. This communicatin is restricted
since the parent nodes of N are either in the same partition/group as N or they 
are in the previous partition or a sub-partitiion (i.e., group) of the pervious
partition, i.e., the communication is restrictd by the fanin structure of the DAG.

A partition/group either has nodes that form a cycle in the graph, which we
refer to as a "loop partition" or a "loop group" or it has no such cycle of nodes.
Partiton P2 is a loop partition since there is a cycle:
  20->8->11->3->19  # 20 is parent of 8 is parent of 11 ...
  ^             |
  |--------------   # (backedge from 19 to 20)

  This means G3: 20 8 11 3 19 is a loop group. The fact that partitions/groups isolate 
  the loops is very important. When computing pagerank, there may be many iterations needed
  for the values to converge due to loops. So, say, 100 iterations may be needed to 
  compute the pagerank values of the nodes in a loop group. However, a non-loop group
  requires only a single iteration. Thus, when computing pagerank for
    P2: 2 10 16 20 8 11 3 19 4 6 14 12
  there will be 100 iterations were each iteration computes pagerank for all 12 nodes;
  thus, page rank will be computed 12 * 100 = 1200 times.
  Compare this to using 1 iteration for the nodes in G2, 100 iterations for the nodes
  in G3, and one iteration for the nodes in G4, for a total of 3 + 500 + 4 = 507
  pagrank computations. So DAGs of groups will have more parallelism and less pagerank
  computations. The DAG structure also decreases the amount of communication between
  compute nodes. A DAG also temporally partitions the pagerank computation. For example,
  in the DAG above there is not reason to allocate compute resouces for (the nodes in)
  G5, G6 and G7 until after the other groups have been processed. (Graphlab for 
  example distributes the nodes in G5, G6, and G7 (and other nodes) to servers and 
  must run these servers over the entire pagerank computation in case these node's
  pagerank values are needed. Serverless functions ned not be allocated for G5, G6, and
  G7 until the pagerank values for nodes in these groups are computed.  Consider again,
  group G2. The parent nodes of all the nodes in G2 are either in G2 or G1. Thus, the 
  pagerank values for the nodes in G2 can be computed in one iteration, starting with the 
  greatest ancestor(s), which is left-to-right in the pictures. These pagerank
  computations may use paret values that have been received from other compute 
  nodes (as part of a fanout or fanin).

  The group-based DAG for the whiteboard example is:

                G1
             /  |   \
            /   |    \
          v     v     v
        G2----> G3L   G4     # G3L indicates G3 is a loop group, likewise for P2L
             /   |    |
            /    |    |
          v      v    v
        G5----> G6    G7

 Note: This DAG has 20 nodes. There is also an extended version of this in which we 
 add two more connected components, each with two nodes. One
 is nodes 21-->22, which results in groups G8->G9 and the other is 
 23-->24, which results in groups G10-->G11. There is also an
 addtional fanin from G6 to G7 so that G7 is a fanin node.
 This file is "graph_24N_3CC_fanin.gr"

 graph_24N_3CC_fanin.gr 

This DAG is based on the parent-relationship between the nodes within a group.
e.g., for G3:
  20->8->11->3->19  # 20 is parent of 8 is parent of 11 ...
  ^             |
  |--------------   # (backedge from 19 to 20)
and the child relationships between the nodes in different groups (and likewise
on the parent relationship between the nodes in a parttion and the child relationship
between the nodes in different partitions)

In detail, the group-based DAG is:

               G1: 5       17               1
                 /          |                \
                /           |                 \
              v             |                  \ 
    G2: 2 10 16             |                   \
        |       G3L:        v    G4:             v
        |------>20 8  11 3 19      4       6 13 12
                  /    |            \     /
                /      |             \   /
               v       |              \ /   
           G5:13  G6:  v               \     # 18 is a child of 4
                     7 15             / \    # 9 is a child of 6
                                    v   v
                               G7: 9    18

Note that G1, which is a sub-partition of P1 has children in G2, G3L and G4, and
the latter are all sub-partitions of P2L. In general, as we mentioned above,
a group that belongs to Pi can only have children in Pi+1.

Also as we mentiond above, the nodes of G1 are generated during a DFS search 
based on the parent relationship - DFS(1), DFS(17), DFS (5), return, return, return.
Nodes 5, 17, and 1 are enqueued (in that order) for BFS. BFS(5) seaches the children
of 5, which is the single child 16. The DFS(16) based on the parent-relationship
gives DFS(1), DFS(10), DFS(2), return, return, return. Nodes 2, 10 and 16 are enqueued 
for BFS, etc. The only detail left is to determine the boundaries between partitions.
As a hint partition P2 is finished when the BFS/DFS for nodes 5, 17, and 1 is completed.
The nodes in G2 are the child 16 of 5 (child-realtionship shown vertically) and the 
ancestors (via DFS) 10 and 2 (shown horizonatally) of 16. The nodes of G4 are the child 12 
of node 1 and the ancestors 14, 6, and 4 of 16. Thus, all of the children of a node N
in partition Pi are in Pi+1, as BFS searches all the children of node N in Pi to
compute partition Pi+1.

The pagerank of node 19 in group G3L, is computed using the pagerank value of
19's parent node 17. This, if G1 and G3L are assigned to different compute
nodes, the some inter-node communication is required, i.e., after computing 
the pagerank of 17, the computed value must be communicated to the compute
node of 19. Note that if groups are assigned to threads on the same multicore server,
the communication cost is lower than using processes on the same server, 
which is lower than using threads/processes on multiple servers or serverless
functions. 

Note that G2, G3L and G4 are all dependent on G1. Thsi means that we 
compute the pagerank values for G1's nodes before we compute the pagerank
values for G2, G3L and G4. In this case, the pagerank values of G1, will 
be communicated to G2, G3L, and G4 before computation begins on G2,
G3L and G4. A sequential pagerank algorithm would iteraratively compute
the pagerank values for all the nodes, in the order that the nodes appear
in the array. The nodes will most likely not be in "parent first" order, e.g.,
the pagerank for node 12 might be computed before the pagerank values
for the parents 3 and 1 of node 12. Note that each node has a "prev" and "pagerank"
value such that on the current iteration we compute a "pagerank" value or node
12 using the "prev" values for 3 and 1. The "prev" values are initialized at the start
to special values so that they are non-zero on the first iteration. At the end of 
each iteration, all "pagerank" values of a node are assigned to the node's "prev" value.

On the first iteration, the pagerank for node, say, 12 would be computed without 
having computed a pagerank value for 12's parent nodes 3 and 1. This computation 
is "wasted" since the second iteration will recompute the pagerank for node 12 with 
the pagerank values of 3 and 1 that wer computed on the first iteration. Note that when
the pagerank values of 3 and 1 were computed on the first iteration, the computation 
was done without any computed pagerank values for the parent nodes of 1 or 3 or the 
grandparents of 1 or 3, etc. The use of "prev" and "pagerank" values means that 
it may take may iterations for information about the pagerank values of ancestor
nodes to propogate to their descendents. Also, nodes will reach their final values,
i.e., their values cannot change in future iterations, but the pagerank values
for these nodes will be recomputed in the remaining iterations even though changes 
are impossible.

The DAG stuctured execution saves time by (1) preventing useless iterations 
from being excuted for nodes that are not in a loop and (2), by computing pagerank values 
"parent first". Based on the "parent first" node ordering for non-loop partitions/groups,
there is no need for a "prev" variable, only "pagerank" is computed and only one iteration
is needed for non-loop nodes. For loop partitions or groups, we still use "prev" and "pagerank" 
and perform multiple iterations, but the computations for the loop partition or group do not 
start until the partitions/groups it depends on have been processed (in DAG order).

As an example, consider computing the pagerank values for G1: 5 17 1. By definition of 
parent-first ordering, 5 has no parents (G1 is not a loop group). So the pagerank value
computed for 5 is its final value. The pagerank value for 17 is computed using it's 
parent's pagerank values. By defintion, the pagerank values of its parents, in this case
node 5, were computed first. The computed pagerank value for node 17 is its final value.
The computation for node 1 is similar. Note that the pagerank of node 12 depends on that of
node 1, and node 1's pagerank is computed before node 12's pagerank. If group G1 and G4 are 
computed on different machines M1 and M4 respectively, then M1 will have to send the pagerank
value for node 1 to M4 and this value will be received by M4 before it computes the pagerank
value for node 12.


In a serverless execution of the DAG, while the pagerank values for group G1 are being computed 
by serverless function 1, no other serverless functions are wasting time computing pagerank values 
for other groups. Note that all of the other groups depend on the pagerank values for G1.
Compare this to a cluster of servers that are all running all the time. Nodes will be
partitioned among these servers randomly or by using a more cache-friendly method. But there 
is no guarantee that a node and any of its parents are on the same server. Computing pagerank
for nodes that would be considered very downstream in a DAG would be a waste of server resources
and would potentially require lots of communication between the servers. For example,
the only useful work that can be done at the start of the pagerank computation for the nodes in 
the whiteboard DAG is to compute the pagerank values of nodes 5, 17, and 1. The cluster, however,
would compute the pagerank for all the nodes, including 5, 17, and 1, and would compute the 
pageranks for 5, 17, and 1 on multiple iterations, with inter-server communication on each iteration.

Note that other parallel/distributed pagerank methods also use a node partitioning.
so while the BFS/DFS partitioning tasks time, so do the other partioning methods.
Also, the BFS/DFS partition can be done incrementally, i.e., we can start executing
the DAG before we finish the partition. It is not known yet whether partitioning the 
nodes (generating the DAG) takes more or les time than executing the DAG nodes.
The partitioning reduces communication between the compute nodes, which may or
may not reduce the total time (partitioning + execution). The DAG structure
may significantly reduce cost, whether or not it reduces the execution time.

The outline of BFS is as follows:

BFS(node):
    # -1 in the queue marks the end of a partition. For example, when the 
    # first partition P1 is processed, the queue becomes -1 5 17 1 and 
    # 5, 17, 1 will have all been added to P1. The -1 indicates tha thAT
    # p1 is complete. The queue will then become 5 17 1 -1, so that when 
    # -1 is encountered agin, P2 will be complete. Etc.
    BFS_queue.append(-1)

    # perform a dfs starting at node based on the parent realationship,
    # This will visit 1, 1's parent 17, and 17's parent 5. All three nodes
    # will be added to P1 and marked visited. they will also be enqueued
    # in the BFS queue, which becomes -1 5 17 1. Note that 5, 17, 1 is
    # the "current group" which is the first group G1.
    dfs_parent(visited,node)

    # save this first group G1. P1 is saved next in the "usual spot"
    # for saving the current partition. There is below the usual spot
    # for saving the current group. This first group is a special case.
    groups.append(current_group)
    current_group = []

    # Note the convention for Group names. The first group is "PR1_1" where
    # the "1" in "PR1" indicates this is a group of partition 1, and the 
    # "1" in "_1" indiates ths is group 1 of partition 1. So the names of 
    # the groups in partition 2 will be "PR2_1", "PR2_2L" and "PR2_3"
    group_name = "PR" + str(current_partition_number) + "_" + str(current_group_number)

    if current_group_isLoop:
        # Append an "L" to the name if it is a loop group. Note that the tasks
        # for loop and non-loop partitions/groups are different. A non-loop (loop)
        # task does 1 (many) iterations. The "L" tells us which task to do,
        group_name = group_name + "L"

    # save the group names in a list parellel to the list of groups (see above)
    group_names.append(group_name)

    # the first group collected is a leaf group of the DAG
    BFS_generate_DAG_info.leaf_tasks_of_groups.add(group_name)

    while BFS_queue:          # loop to visit each node
        ID = BFS_queue.pop(0) # queue of integer IDs, not nodes

        if ID == -1:
            end_of_current_partition = True

            if BFS_queue:
                # poppd a -1 so pop again to get a real node
                ID = BFS_queue.pop(0)
                # reappend the -1 at the end of the queue which is the end 
                # of the next partition 
                BFS_queue.append(-1)
            else:
                # just finished the last partition
                break

        # get the actual node using its ID
        node = nodes[ID]

        if end_of_current_partition:
            end_of_current_frontier = False

            if len(current_partition) > 0:
            # Note: If the partition is "too small" we can keep going, which is
            # essentially cluster this partition and the next (one)s
            #if len(current_partition) >= num_nodes/5:

                # save the current partition
                partitions.append(current_partition.copy())
                current_partition = []

                partition_name = "PR" + str(current_partition_number) + "_1"

                if current_partition_isLoop:
                    # Add a "L" to the name of loop partitions. 
                    partition_name = partition_name + "L"
                    Partition_loops.add(partition_name)

                # The first partition collected by any call to BFS() is a leaf node of the DAG.
                # There may be many calls to BFS(). We set is_leaf_node = True at thr
                # start of BFS.
                if is_leaf_node:
                    BFS_generate_DAG_info.leaf_tasks_of_partitions.add(partition_name)
                    BFS_generate_DAG_info.leaf_tasks_of_partitions_incremental.add(partition_name)
                    is_leaf_node = False

                ...  various other things ...

                # after "PR1" we do "PR2"
                current_partition_number += 1
                # reset group numbers so first group of partition is always group 1
                current_group_number = 1

            for neighbor_index in node.children:
                neighbor = nodes[neighbor_index]
                if neighbor.ID not in visited:
 
                    # number of groups in current partition/sum
                    current_group_number += 1
                    # total number of groups, if this is i then this group will
                    # be stored in groups[i]
                    total_num_of_groups += 1

                    # dfs search based on parents - enqueue all parents for BFS
                    # The parents (ancestors) are adde to the current group
                    dfs_parent(visited, neighbor)

                    # process the current group (of the current partition)

                    groups.append(current_group)
                    current_group = []

                    group_name = "PR" + str(current_partition_number) + "_" + str(current_group_number)
                    if current_group_isLoop:
                        group_name = group_name + "L"
                        Group_loops.add(group_name)
                    group_names.append(group_name)

                     if current_group_isLoop:
                        # various things need to be done if the current group is a loop group
                        # ...

                    current_group_number += 1
                else:
                    # already visited

# The main method performs a standard BFS (with calls to DFS wihin)
if __name__ == '__main__':

    logger.trace("Following is the Breadth-First Search")
    input_graph()
    logger.trace("num_nodes after input graph: " + str(num_nodes))

    for i in range(1,num_nodes+1):
        if i not in visited:
            bfs(visited, nodes[i])    # function calling

    # hande the last partition/group that was generated, same as above
    if len(current_partition) > 0:
        logger.trace("BFS: create final sub-partition")

        partitions.append(current_partition.copy())
        current_partition = []
        partition_name = "PR" + str(current_partition_number) + "_1"
        if current_partition_isLoop:
            partition_name = partition_name + "L"
            Partition_loops.add(partition_name)
        partition_names.append(partition_name)

        groups.append(current_group)
        current_group = []
        group_name = "PR" + str(current_partition_number) + "_" + str(current_group_number)
        if current_group_isLoop:
            group_name = group_name + "L"
            Group_loops.add(group_name)
        group_names.append(group_name)

        # various other simple things are not shown

def dfs_parent(visited, node)
# Do a dfs search of node and its parents and their parents, etc.
# The nodes collected are a group, if we are generating a DAG of groups 
# or just part of the current partition if we are collectng groups.
# (A partition consists of one or more groups.)
# A parent node may have already been visited, either because this is
# a loop-group or loop-partition, or because the parent node is part of a
# group or partition collected earlier. This type of parent-child relationship,
# i.e., from a node in on partition/group to a node in a previous partition/group
# forms an edge in the DAG. Thus, an edge in the DAG between a partitions/groups
# represents a parent node that is in a different partition/group then its
# child node. For a node N in partition/group PR_i/PRi_j, all of N's parents are 
# either in the same partition/group PR(i)PR(i)_(j) as N, or are in partition 
# (PR(i-1)) or a group PR(i-1)_k of PR(i-1). This means that all of the edges
# from partition Pi, if any, are to partition Pi+1.

    # rhc : ******* Partition
        partition_node = Partition_Node(node.ID)
        partition_node.ID = node.ID
        list_of_parents_in_previous_partition = []

    # rhc : ******* Group
        group_node = Partition_Node(node.ID)
        group_node.ID = node.ID
        list_of_parents_in_previous_group = []

        visited.append(node.ID)

        # parents that have been visited before we try to visit them. These parents
        # may be in a partition/group different from node's or we may be searching 
        # nodes (including node) that are in a loop/cycle and are in the same 
        # partition/group as node.
        already_visited_parents = []
        # we will be adding nodes to the current partition/group and we need to know
        # their position. This is the current position in the partition, starting with 0
        index_of_parent = 0
        for parent_index in node.parents:
            # nodes is an aray of integer IDs, so get the actual parent node
            parent_node = nodes[parent_index]
            logger.trace("parent_node: " + str(parent_node))

            #pg_tuple is a map that accumlates information about each node.
            pg_tuple = None

            if parent_node.ID not in visited:
                logger.trace ("dfs_parent visit parent node " + str(parent_node.ID))
                # recursively visit parent
                dfs_parent(visited, parent_node)

                # Get the stored information about the parent node we just visited
                # i.e., on the rcursive dfs_parent call that just returned
                pg_tuple = nodeIndex_to_partition_partitionIndex_group_groupIndex_map[parent_index]

                # the list of parents for node is a list of the parent's position
                # in this current partition/group. Get these positions from the pg_tuple.
                parent_partition_parent_index = pg_tuple[1]
                parent_group_parent_index = pg_tuple[3]

                # Partition_node and Group_node were created at the start of
                # dfs_parent. We will add them to the partition/group and fill
                # in information about them. A Partition_node is smaller than
                # the regular Nodes in a graph. e.g., Partition_nodes don't
                # have a list of children since we don't need this list when
                # we compute pagerank; we only need a list of the node's parents.
    # rhc : ******* Partition
                partition_node.parents.append(parent_partition_parent_index)
    # rhc : ******* Group
                group_node.parents.append(parent_group_parent_index)

            else:
                # This parent was already visited. Below we determine whether node
                # and this parent are part of a loop and thus in the same partition/group
                # or are in different partitiions/groups and thus we need to add an edge
                # to the DAG.

                logger.trace ("dfs_parent parent " + str(parent_node.ID) + " of " + str(node.ID) + " already visited"
                    + " append parent " + str(parent_node.ID) + " to already_visited_parents.")

                # save the parent and its position in a list of already_visited_parents
                # so we can check them later.
                parent_node_visited_tuple = (parent_node,index_of_parent)
                already_visited_parents.append(parent_node_visited_tuple)

                # If the parent node P is in a previous partition/group, then 
                # it is a parent of node N, and P's position should be in N's
                # parent list, but P will not be placed in this partition as it
                # already is in a preious partition. Instad, we will create a 
                # "shadow_node" SN that is a proxy for P and place SN in this
                # partition. After computing the pagerank for P, we will send
                # P's pagerank to the thread/process/function computing the
                # pagerank of N. That sent value will be assigned to the 
                # shadow node SN and used to compute the pagerank of N.
                # It's also possible that parent P is in a loop with N so we
                # don't yet know what to do with P.
    # rhc : ******* Partition
                partition_node.parents.append(-1)
    # rhc : ******* Group
                group_node.parents.append(-1)
        
                # parent node has been processed so get its info and determine whether
                # this indicates a loop
                pg_tuple = nodeIndex_to_partition_partitionIndex_group_groupIndex_map[parent_index]
                parent_partition_parent_index = pg_tuple[1]

                # At this point, we try to detect a loop. If the parent P's position
                # in it's partition is not -1, then P has been visited and already been added to a previous 
                # partition/group and we are not in a loop. 
                # 
                # Tricky: Suppose BFS dequeues a node N. BFS will set N to visited and call
                # dfs_parent(N). Note that we only add a node N to a partition/group *after* we have 
                # recursively called dfs_parent on all of N's parents. If we call dfs_parent(N) and make 
                # recursive calls dfs_parent ... dfs_parent ... on N's parents and grandparents etc
                # and N is part of a loop, we will try to visit N again. While N will
                # have been marked visited, it will not have a position in any partition 
                # assigned to it yet (we only add N to a partition after all of its calls
                # to dfs_parent have unwould and we are not there yet.) So N is part of a loop.
                # If N is not part of a loop and it is visited then we will have added N to
                # some previous partition/group when its recursion was completely unwound.
                
                # If we detect a loop here we can  use a partition or group
                # name with an 'L' at the end, e.g., "PR2_2L". This is the proper name for a 
                # loop. But not that N may be in a loop that we just have not detected yet,
                # i.e., N and its parent P1 are not in a loop but N and its parent P2 are
                # in a loop. When we detect a loop we set current_partition_isLoop and
                # current_group_isLoop to True. (If the current group is in a loop the 
                # current partition, which is the partition the current group is in, is 
                # also in a loop.)

                if parent_partition_parent_index == -1:
                    logger.trace("XXXXXXXXXXXXXXXXX dfs_parent: Loop Detected: "
                        + "PR" + str(current_partition_number) + "_" + str(num_frontier_groups))
    # rhc : ******* Partition
                    global current_partition_isLoop
                    current_partition_isLoop = True

    # rhc : ******* Group
                    global current_group_isLoop
                    current_group_isLoop = True
                else:
                    logger.trace("YYYYYYYYYYYYY dfs_parent: No Loop Detected (yet): "
                        + "PR" + str(current_partition_number) + "_" + str(num_frontier_groups))

            # next parent will be in next position in parent list
            index_of_parent += 1

# if we found a loop then add 'L' to the partition/group name
# Note: If a loop is detected current_partition_isLoop and current_group_isLoop are
# both set to True. current_partition_isLoop remains True until the end 
# of the partition is reached. current_group_isLoop is set to False when the 
# end of the group is reached. So it is possible that current_partition_isLoop is
# True and current_group_isLoop is False.

# rhc : ******* Partition
    current_partition_name = "PR" + str(current_partition_number) + "_1"
    if current_partition_isLoop:
        current_partition_name += "L"

# rhc : ******* Group
    current_group_name = "PR" + str(current_partition_number) + "_" + str(num_frontier_groups)
    if current_group_isLoop:
        current_group_name += "L"

    # During the dfs search of node's parents, already_visited_parents is
    # a list of parents that were already visited (and so were ot searched.)
    # These parents could be in the same partition/group as node (because of 
    # a loop) or in a different (previous) partition/group of node. Note: a 
    # different group from node's could still be in the same partition as node.

    for parent_node_visited_tuple in already_visited_parents:
        # parent_node_visited_tuple is a tuple containing thew parent
        # node and the index of the parent in node's parent list.
        visited_parent_node = parent_node_visited_tuple[0]
        index_of_parent = parent_node_visited_tuple[1]

        #where: parent_node_visited_tuple = (parent_node,index_of_parent) 
        # information about the parent node, its paritition, group, etc.
        partition_group_tuple = nodeIndex_to_partition_partitionIndex_group_groupIndex_map.get(visited_parent_node.ID)
        
        # this is also used in the else: part for debugging so declare here
        parent_partition_number = None
        parent_group_number = None

        # This must be True - the else prints an error message
        if partition_group_tuple != None:
            parent_partition_number = partition_group_tuple[0]
            # If parent is not in the previous partition, the parent may still
            # be in a previous group (same partition). So in this branch we will 
            # check below to see if the parent is in the same group.
            #
            # If the parent is in the previous partition, it is also in a previous 
            # group. So in the else branch we will have found a parent that is in the
            # previous partition PR and in a previous group (which is in partition PR.)
            #
            # if parent_partition_number == current_partition_number then the dfs 
            # through parent has backtracked from parent and so is finished and 
            # parent is in the same partition as node. (Example, in PR1
            # of the whiteboard example, node 17 will find it parent 5 is in the 
            # same partition since parent_partition_number for 5 will be the same
            # as the current_partition_number (which is the partition we are currently
            # processing containing 5, 17, and 1. In PR2_2L, node 20 will find that 
            # its parent 19 is in the same partition but we will check below to 
            # see whether 19 and 20 are in a loop, which they are.
            if parent_partition_number == current_partition_number:
                # Parent is not in previous partition, i.e., node is not a child of
                # a parent node that was in previous partition. This means that
                # parent is in this partition and it is either in the same 
                # group as node or it is in a different group, which was processed
                # previously.
                #
                # If parent is in the same group then we do not need shadow nodes;
                # otherwise, we need shadow_nodes just like the case in which the
                # parent is in a different partition, which is like saying that 
                # the parent is in a group of a different partition, but we presumably
                # are not tracking groups, just partitions.
                #
                # The parent is in a different group if: it has a different group
                # number.

                logger.trace ("dfs_parent: parent in same partition: parent_partition_number: " 
                    + str(parent_partition_number) 
                    + ", current_partition_number:" + str(current_partition_number)
                    + ", parent ID: " + str(parent_index))

                # Check if this is a loop. A loop is possible since parent is in the same 
                # partition/group. If not a loop, then parent is in previous partition or group, and that is handled next.
                #pg_tuple = nodeIndex_to_partition_partitionIndex_group_groupIndex_map[parent_index]
                # parent has been mapped but the partition and group indices
                # might be -1. From above, for documentation, a node's
                # global map info is initialized at start of dfs_parent as:
                #partition_number = current_partition_number
                #partition_index = -1
                #group_number = current_group_number
                #group_index = -1
                parent_partition_parent_index = partition_group_tuple[1]
                parent_group_parent_index = partition_group_tuple[3]

                # case: visited parent before and it is is same partition.
                # Parent and node may be in a loop so check for loop.

                # Exampe: In PR2_2L, node 20 will find that 
                # its parent 19 is in the same partition but we check to 
                # see whether 19 and 20 are in a loop, which they are. Parent 19 has 
                # parent_partition_parent_index -1 since 19's parent_partition_parent_index
                # is not set until the dfs that started at 19 unwinds back to 19
                # and that hasn't happened - this search from 19 eventually visits 20 and 
                # then tries to visit 19 but 19 is already visited and its parent_partition_parent_index
                # is not set until the search at 19 unwinds back to 19 so 20 will see 
                # parent_partition_parent_index for 19 as -1, indicating a loop.

                if parent_partition_parent_index != -1:
"""

import networkx as nx
import matplotlib.pyplot as plt
import socket
import cloudpickle
import threading
import os
import time
#from statistics import mean
import copy

# Note: When we run BFS, BFS will generate a DAG for pagerank and call
# DAG_excutor_driver.run(). We addLoggingLevel TRACE here before we
# import the run from DAG_executor_driver so that the TRACE level is 
# already defined when we process DAG_executor_driver. In DAG_executor_driver
# we do not addLoggingLevel if we are computing pagerank. 
# If we are not computing pagerank, we will run DAG_excutor_driver, and since
# we are not computing pagernk, DAG_executor_driver will addLoggingLevel TRACE.

import logging
from wukongdnc.dag.DAG_executor_constants import log_level
from .addLoggingLevel import addLoggingLevel
addLoggingLevel('TRACE', logging.DEBUG - 5)
logging.basicConfig(encoding='utf-8',level=log_level, format='[%(asctime)s][%(module)s][%(processName)s][%(threadName)s]: %(message)s')
# Added this to suppress the logging message:
#   credentials - MainProcess - MainThread: Found credentials in shared credentials file: ~/.aws/credentials
# But it appears that we could see other things liek this:
# https://stackoverflow.com/questions/1661275/disable-boto-logging-without-modifying-the-boto-files
logging.getLogger('botocore').setLevel(logging.CRITICAL)

""" How to use: https://stackoverflow.com/questions/2183233/how-to-add-a-custom-loglevel-to-pythons-logging-facility/35804945#35804945
    >>> addLoggingLevel('TRACE', logging.DEBUG - 5)
    >>> logging.getLogger(__name__).setLevel("TRACE")
    >>> logging.getLogger(__name__).trace('that worked')
    >>> logging.trace('so did this')
    >>> logging.TRACE
"""

from .DAG_executor_constants import use_shared_partitions_groups, use_page_rank_group_partitions
from .DAG_executor_constants import use_struct_of_arrays_for_pagerank, compute_pagerank
from .DAG_executor_constants import use_incremental_DAG_generation, using_workers
from .DAG_executor_constants import run_all_tasks_locally, using_threads_not_processes
from .DAG_executor_constants import incremental_DAG_deposit_interval
from .DAG_executor_constants import check_pagerank_output
from .DAG_executor_constants import using_threads_not_processes
from .DAG_executor_constants import use_multithreaded_BFS
from .DAG_executor_constants import enable_runtime_task_clustering
from .DAG_executor_constants import exit_program_on_exception
from .BFS_Node import Node
from .BFS_Partition_Node import Partition_Node
from . import BFS_generate_DAG_info_incremental_partitions
# Note: avoiding circular imports:
# https://stackoverflow.com/questions/744373/what-happens-when-using-mutual-or-circular-cyclic-imports
from . import BFS_generate_DAG_info_incremental_groups
#from .BFS_generate_DAG_info import generate_DAG_info
from . import BFS_generate_DAG_info
from . import BFS_generate_DAG_info_incremental_groups
from . import BFS_generate_DAG_info_incremental_partitions
#from .BFS_generate_DAG_info import Partition_senders, Partition_receivers, Group_senders, Group_receivers
#from .BFS_generate_DAG_info import leaf_tasks_of_partitions, leaf_tasks_of_partitions_incremental
#from .BFS_generate_DAG_info import leaf_tasks_of_groups, leaf_tasks_of_groups_incremental
from .BFS_generate_shared_partitions_groups import generate_shared_partitions_groups

# This will either be a DAG_infoBuffer_Monitor or a DAG_infoBuffer_Monitor_for_Lambdas
from .DAG_infoBuffer_Monitor_for_threads import DAG_infobuffer_monitor
from .BFS_DAG_Generator_Multithreaded import DAG_Generator_Multithreaded
#if not using_workers:
#    import wukongdnc.dag.DAG_infoBuffer_Monitor_for_lambdas_for_threads 
#    DAG_infobuffer_monitor = wukongdnc.dag.DAG_infoBuffer_Monitor_for_lambdas_for_threads .DAG_infobuffer_monitor
#else:
#    import wukongdnc.dag.DAG_infoBuffer_Monitor_for_threads 
#    DAG_infobuffer_monitor = wukongdnc.dag.DAG_infoBuffer_Monitor_for_threads .DAG_infobuffer_monitor

#rhc shared
#from .DAG_executor import shared_partition, shared_groups
#from .DAG_executor import shared_partition_map, shared_groups_map
#from .Shared import shared_partition, shared_groups, shared_partition_map,  shared_groups_map
from . import BFS_Shared
from .DAG_executor_driver import run
#from .DAG_boundedbuffer_work_queue import Work_Queue_Client
from .Remote_Client_for_DAG_infoBuffer_Monitor import Remote_Client_for_DAG_infoBuffer_Monitor
from .Remote_Client_for_DAG_infoBuffer_Monitor_for_Lambdas import Remote_Client_for_DAG_infoBuffer_Monitor_for_Lambdas
from .DAG_executor_output_checker import get_pagerank_outputs
from .DAG_executor_output_checker import verify_pagerank_outputs
from wukongdnc.constants import TCP_SERVER_IP

logger = logging.getLogger(__name__)
#if not (not using_threads_not_processes or use_multithreaded_multiprocessing):
    #logger.setLevel(logging.DEBUG)
    #logger.setLevel("TRACE")
    #logger.setLevel(log_level)

    #formatter = logging.Formatter('[%(asctime)s] [%(module)s] [%(processName)s] [%(threadName)s]: %(message)s')
    #ch = logging.StreamHandler()

    #ch.setLevel(logging.DEBUG)
    #ch.setLevel(logging.INFO)
    #ch.setLevel("TRACE")
    #ch.setFormatter(formatter)
    #logger.addHandler(ch)

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

visited = [] # List for visited nodes.
BFS_queue = []     #Initialize a queue
partitions = []
current_partition = []
current_partition_number = 1
dfs_parent_changes_in_partiton_size = []
dfs_parent_changes_in_frontier_size = []
# This is used in the pre/post dfs_parent code when adding L-nodes to
# partitions.
loop_nodes_added = 0
num_shadow_nodes_added_to_partitions = 0
num_shadow_nodes_added_to_groups = 0
start_num_shadow_nodes_for_partitions = 0
end_num_shadow_nodes_for_partitions = 0
start_num_shadow_nodes_for_groups = 0
end_num_shadow_nodes_for_groups = 0
partitions_num_shadow_nodes_list = []
groups_num_shadow_nodes_list = []
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
shared_frontier_parent_partition_patch_tuple_list = []
shared_frontier_parent_groups_patch_tuple_list = []
sender_receiver_partition_patch_tuple_list = []
sender_receiver_group_patch_tuple_list = []
current_group_number = 1
partition_names = []
group_names = []
current_partition_isLoop = False
current_group_isLoop = False

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
# That is, nodes is the list of nodes in the graph. Each graph node will
# be added to some partition/group. 
# node i in nodes is in position i. When we place a node in a partition/group, 
# this node is not necessarily in postion i; nodes are added to the partition/group
# one by one using append. We map node i, which we know is at position i in nodes,
# to its position in its partition/group. Example node 25 in nodes at position 25 is mapped 
# to position 4 in its partition/group.
# Note: we map shadow nodes to their positions too. We do not map shadow nodes 
# in the global map nodeIndex_to_partition_partitionIndex_group_groupIndex_map since
# a shadow node ID and a non-shadow node for ID would have the same key. We could 
# use string keys and use, e.g, "5" and "5s" for "shadow" so the keys would be unique.
# Note: A shadow node can be added more than once, in which case the index of the 
# shadow node will be its last index, e.g., if shadow node ID is in positions 0 and 2 its
# index will be 2. We do not use the index of shadow node's so we don't care.
#
# Note: we are using these maps only for debugging. We can turn them off.
# we ar now using nodeIndex_to_partition_partitionIndex_group_groupIndex_map
# which has the same information.
nodeIndex_to_partitionIndex_map = {}
nodeIndex_to_groupIndex_map = {}
# collection of all nodes_to_group_map maps, one for each group
nodeIndex_to_partitionIndex_maps = []
nodeIndex_to_groupIndex_maps = []

# map a node to its partition number, partition index, group number and group index.
# A "global map"for nodes. May supercede nodeIndex_to_partitionIndex_map. 
# We need a nodes position in its partition if we map partitions to functions and we need
# a nodes position in its group if we map groups to functions. This map supports
# both partition mapping and group mapping.
#
# Note: We do not map shadow nodes in this map.
#
# Consider: We can remove the nodes in Pi from this map after we have finished 
# computing Pi+1 since we will no longer need to know this info for 
# the nodes in Pi? We may want to remove these nodes to free the space.
# Note: If a node is in Pi+1 all of its parents are in Pi+1 or Pi,
# by definition, since Pi+1 contains all the children of Pi and
# all of the parents (actually, ancestor) of these Pi+1 nodes that 
# are not in Pi.
nodeIndex_to_partition_partitionIndex_group_groupIndex_map = {}

# reset dfs_parent counters
dfs_parent_start_partition_size = 0
dfs_parent_loop_nodes_added_start = 0
dfs_parent_start_frontier_size = 0
dfs_parent_end_partition_size = 0
dfs_parent_loop_nodes_added_end = 0
dfs_parent_end_frontier_size = 0

IDENTIFY_SINGLETONS = False
TRACK_PARTITION_LOOPS = False
CHECK_UNVISITED_CHILDREN = False
DEBUG_ON = True
PRINT_DETAILED_STATS = True
debug_pagerank = False
generate_networkx_file = False

# list of nodes in the inut graph
nodes = []

num_nodes = 0
num_edges = 0
# used to compute size of numPy parents array for pagerank calculation
num_parent_appends = 0 

#Shared.shared_partition = []
#Shared.shared_groups = []
# maps partition "P" to its position/size in shared_partition/shared_groups
#Shared.shared_partition_map = {}
#Shared.shared_group_map = {}

"""
# old test code for building a graph. Now we input a graph.
num_nodes = 12
#put non-null elements in place
for x in range(num_nodes+1):
    nodes.append(Node(x))
"""

# global object used by during incremental DAG generation to invoke the 
# DAG_excutor_driver. A thread is created to call
# DAG_executor_driver.run() while BFS continues with 
# incremental ADG generation. BFS joins this thread
# at the end of BFS; the join occurs when DAG execution is done.
invoker_thread_for_DAG_executor_driver = None

# used during incremental DAG generation by BFS to
# access the work_queue on the tcp_server when we are
# using worker processes.
websocket = None

# count of incremental DAG generated. Note: we generate 
# a DAG with the first partition, then we generate a DAG
# with the first two partitions and start the DAG_executor_driver.
# Then we use this counter to determine when to generate another
# DAG. If the incremental_interval is 2, we will generate the next DAG after
# adding partition 4. This will be the third DAG - one with P1,
# one with P1 and P2, and one with P!, P2, P3, and P4.
num_incremental_DAGs_generated = 0

#rhc incremental
    # total number of graph nodes that have been added to the 
    # partitions generated so far. When all nodes have been 
    # added to a partition, the partitions are complete.
    # Used for incremental DAG generation where we do not know
    # the number of partitions so we cannot stop based on the 
    # number of partitions we have seen.
num_nodes_in_partitions = 0

#rhc: incremental groups
# used for incremental DAG generation.
# groups in current partition
groups_of_current_partition = []
# list of groups of current partitions. When we process the 
# current partition we need to know the groups in it and in the 
# previous partition and in the previous previous partition.
groups_of_partitions = []

def DAG_executor_driver_Invoker_Thread():
    time.sleep(6)
    # call run() of the DAG_executor_driver, where: from .DAG_executor_driver import run
    run()

# visual is a list which stores all the set of edges that constitutes a graph
# we can visualize only small graphs
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
# Not used
def dfs_parent_pre_parent_traversal(node,visited,list_of_unvisited_children):
    return False

#def dfs_parent(visited, graph, node):  #function for dfs 
def dfs_parent(visited, node):  #function for dfs 
    # e.g. dfs(3) where bfs is visiting 3 as a child of enqueued node
    # so 3 is not visited yet
    logger.trace ("dfs_parent from node " + str(node.ID))

    list_of_unvisited_children = []
    check_list_of_unvisited_chldren_after_visiting_parents = False

    # These are the nodes to be added to the partition/group
    # Fill the parent info in later, e.g., we have to remap the positions of node's parents since 
    # while parent ID is in position ID in the nodes[] list, it is not in position ID in 
    # the partition/group. The position in the partition/group is where it happens to be added.

#rhc : ******* Partition
    partition_node = Partition_Node(node.ID)
    partition_node.ID = node.ID

#rhc : ******* Group
    group_node = Partition_Node(node.ID)
    group_node.ID = node.ID

    if CHECK_UNVISITED_CHILDREN:
        # Get unvisited children, which affects whether node is addded to the queue
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
        check_list_of_unvisited_chldren_after_visiting_parents = dfs_parent_pre_parent_traversal(node,visited,list_of_unvisited_children)
        logger.trace("after pre: list_of_unvisited_children: " + str(list_of_unvisited_children))
    else:
#rhc: If not doing child stuff do we mark node visited here or when we enqueue 
# node in dfs_parent path?
        visited.append(node.ID)

    #Note: dfs_parent_pre_parent_traversal will mark node as visitd

    
    # Note: BFS will not call dfs_parent(child) if child has been visited. So
    # if the child has been visited we will not be resetting the pg_tuple here of 
    # such a child.

# rhc : ******* Partition and Group
    # A global map that maps a node to its partition and group numbers and the positions

    # Note: we know the partition number and group number so we fill them in now.
    partition_number = current_partition_number
    parent_partition_parent_index = -1      # fill in later when this position becomes known
    group_number = current_group_number
    parent_group_parent_index = -1          # fill in later when this position becomes known
    parent_index_in_groups_list = -1
    init_pg_tuple = (partition_number,parent_partition_parent_index,group_number,parent_group_parent_index,parent_index_in_groups_list)
    global nodeIndex_to_partition_partitionIndex_group_groupIndex_map
    nodeIndex_to_partition_partitionIndex_group_groupIndex_map[node.ID] = init_pg_tuple

    """
    # for debugging
    logger.trace("nodeIndex_to_partition_partitionIndex_group_groupIndex_map, len: " + str(len(nodeIndex_to_partition_partitionIndex_group_groupIndex_map)) + ":")
    logger.trace("shadow nodes not mapped and not shown")
    if PRINT_DETAILED_STATS:
        for k, v in nodeIndex_to_partition_partitionIndex_group_groupIndex_map.items():
            logger.trace((k, v))
        logger.trace("")
    else:
        logger.trace("-- (" + str(len(nodeIndex_to_partition_partitionIndex_group_groupIndex_map)) + ")")
    logger.trace("")
    """

    if not len(node.parents):
        logger.trace ("dfs_parent node " + str(node.ID) + ": has no parents to visit.")
    else:
        logger.trace ("dfs_parent node " + str(node.ID) + ": visit parents.")

    # SCC 1

    # list of node's parents that are in the previos partition. A parent can be in the 
    # same partition as node or in the previous partition,
# rhc : ******* Partition
    #parents_in_previous_partition = False
    # visit parents
    list_of_parents_in_previous_partition = []

# rhc : ******* Group
    #parents_in_previous_group = False
    # visit parents
    list_of_parents_in_previous_group = []

    # As we visit the parents (and their ancestors) we track the parents of
    # node that have already been visited. We will process them after
    # the recursive calls to dfs_parent(visited, parent_node) have unwound.
    already_visited_parents = []
    # if we find a visited parent in node.parents we save its index
    # so we can process it later, where below:
    #   parent_node = nodes[parent_index]
    #   ...
    #   parent_node_visited_tuple = (parent_node,index_of_parent)
    #   already_visited_parents.append(parent_node_visited_tuple)

    # index in node's parent list where the next parent is added
    index_of_parent = 0
    # for each parent node of this node, recursive call dfs_parent()
    # to visit that parent (and its ancestors) if that parent is 
    # currently unvisited.
    for parent_index in node.parents:
 
        parent_node = nodes[parent_index]
        logger.trace("parent_node: " + str(parent_node))

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
            logger.trace ("dfs_parent visit parent node " + str(parent_node.ID))
            dfs_parent(visited, parent_node)

#rhc: case: since we have not visited parent previously, node and parent_node will both b in
# this same partition/group and thus no shadow node will be added as a proxy for
# parent_node. We only add a shadow node to this partition for parent_node if parent_node 
# is not in the same partition/group as node. To be in a different partition group 
# parent_node has to have been visited already. 
# (Note: if parent_node has been visited it does not mean that it
# is in a different partition/group, parent_node and node may be part of a loop in which 
# case node can find that parent_node has been visited.

            # Get pg_tuple after dfs_parent returns. parent_node has been processed
            # by some call to dfs_parent() and we know parent_node's partition and group
            # numbers have been recorded in nodeIndex_to_partition_partitionIndex_group_groupIndex_map. 
            # Since parent_node was not yet visited, it's parent and group indices (in the 
            # partition or group) will also have been recorded in the map. if parent_node
            # has already been visitd then its parent and group indices will not yet be recored
            # in the map if node and parent_node are part of a loop.
            # Note: dfs_parent saves the parent's partition and group numbers at the beginning
            # of dfs_parent, so if we know dfs_parent as been called on parent then we know that
            # the partition and group numbers for parent are known and available in the map.
            # dfs_parent saves the parent's partition and group indices at the end of dfs_parent,
            # which means the save happen adter the recursive calls to dfs_parent have unwound
            # back to the parent. Example: in the whiteboard example, dfs_parent(19)
            # calls dfs_parent(3) calls dfs_parent(11) etc. When this recurrsion 
            # unwinds back to dfs_parent(19), the index of 19 will be known and
            # saved in a tuple in nodeIndex_to_partition_partitionIndex_group_groupIndex_map.
            # Note also that dfs_parent(19) will need to know the index of its
            # parent 3 and this index will be available to dfs_parent(19) since
            # it will be saved at the end of dfs_parent(3) in map
            # nodeIndex_to_partition_partitionIndex_group_groupIndex_map. 
            # Note also that 3 is added to a partition/group in dfs_parent(3) so
            # its position is not known until it is added. This is usally straight
            # forward since the position of 3 is only needed by its children like 
            # 19 and the position of 3 is set in dfs_parent(3) before reursion
            # unwinds to dfs_parent(19). This is not so easy when there are cycles.
            # When dfs(20) checks visited for its parent 19, it sees that 19 has
            # alrady been visited, hence a cycle is detected. This also means
            # that the index of 19 is not yet available to dfs_parent(20) since this 
            # index is not known and saved until the recursion backs up to dfs_parent(19)
            # which hasn't happened yet. In this case, we set the parent index of 
            # 19 in dfs_parent(20) to -1 and then we "patch" in the actual index
            # later when the index is known. We keop a list of "patch tuples" to 
            # track all the patches that must be mad (due to cycles/loops.)
            #
            # get the tuple of parent and group numbers for parent parent_index
            pg_tuple = nodeIndex_to_partition_partitionIndex_group_groupIndex_map[parent_index]
            # parent's partition and group indices are in the tuple. 
            # From above, and here again for documentation, a nodes's global map
            # info is initialized at the start of dfs_parent as:
            #   partition_number = current_partition_number
            #   partition_index = -1
            #   group_number = current_group_number
            #   group_index = -1
            # The index values are set at the end of dfs_parent after the node will
            # have been added to a partition/group in dfs_parent

            parent_partition_parent_index = pg_tuple[1]
            parent_group_parent_index = pg_tuple[3]

            try:
                # Note: not checking the inner asssertion
                msg = "[Error]: dfs_parent call to unvisited" + " parent resulted in parent/group partition index of -1, which means" \
                    + " a loop was detected at an unvisited parent."
                assert not (parent_partition_parent_index == -1 or parent_group_parent_index == -1), msg
            except AssertionError:
                logger.exception("[Error]: assertion failed")
                if exit_program_on_exception:
                    logging.shutdown()
                    os._exit(0)
            # assertOld: The index values are not -1, which is the value of the 
            # index when there is a loop. Here, there is no loop since the parent was 
            # not visited based on the loop condition (if parent_node.ID not in visited).
            # (An already visited parent P visits its parent, which visits its parent etc 
            # until we try to revisit again the already visited parent P, e.g., like above 
            # example where dfs_parent(20) tries to visit its parent 19, which is already visited. 
            # Note: this applies also for loops within loops since in such a case we must 
            # still try to visit an already visited parent.
            #if (parent_partition_parent_index == -1) or (parent_group_parent_index == -1):
            #    # nested assertOld: parent group index and partition index are both -1: 
            #    if not (parent_partition_parent_index == -1 and parent_group_parent_index == -1):
            #        logger.error("[Error]: dfs_parent call to unvisited"
            #            + " parent resulted in only one of parent/group partition index having "
            #            + " the value -1 when both should be -1.")                
            #    logger.error("[Error]: dfs_parent call to unvisited"
            #       + " parent resulted in parent/group partition index of -1, which means"
            #        + " a loop was detected at an unvisited parent.")
            # Now we know the position of this parent, i.e., its parent index and we can
            # add it to partition_node's/group_nodes's list of parent indices.
            # This is a partition node not a graph node; graph nodes are input
            # with parent and child lists, partition nodes have their parent list
            # constructed by dfs_parent(), which will be called on each of the 
            # nodes parents based on the graph node's parent list that was input. We will do 
            # such an append for each parent when the call to dfs_parent(parent) returns.
            # Note: we may not know the value of the index in which case we append -1
            # and patch it with the actual value later.
# rhc : ******* Partition
            partition_node.parents.append(parent_partition_parent_index)
# rhc : ******* Group
            group_node.parents.append(parent_group_parent_index)
        else:
            # parent already visited. Node and parent may or may not be in a loop.
            # For example, in the whiteboard example, for partition 2, we will put
            # 2, 10 and 16 in partition 2, in that order, which is the reverse order
            # of the dfs_parent calls as dfs_parent(16) ads 16 to the partition 
            # after all of its calsl to dfs_parent(parent) have returned for all of
            # its parents (16's parent is 10, 10's parent is 2 and 2 has no parents
            # so 2 is added to partition 2 first, then 10, then 16).  We will then 
            # execute dfs_parent(19), which has parent 3, whcih has parent 11,....
            # which has parent 20. So dfs_parent(8) calls dfs_parent(20) which
            # ses that 20's parent 2 has already been visited. Noet that 20 and 
            # 2 are not in a loop together. dfs_parent(2) will also see that 20's
            # parent 19 is already visited. Note that 20 and 19 aer in a loop together.
            # Thus, we have determine for node's already visited parents whether node
            # and parent are in a cycle. This is done below after we have collected and 
            # saved a list of node's already visited parents.
            # For an already visited parent 19 of node 20, the index of 19 in its partition/group 
            # will not be known until recursion unwinds back to that parent node. For example,
            # dfs_parent(19) calls dfs_parent(3) calls .... calls dfs_parent(20) which sees
            # that 19 is alread visited. node 19 will not be added to the partition until 
            # the recursive calls unwind back to dfs_parent(19), so the index of 19 is not 
            # known when dfs_parent(20) tries to visit 19. 19's index will thus have the 
            # value -1, which indicates 20 and 19 are part of the same loop. dfs_parent(20)
            # will also sees that its parent 2 has already been visited. But dfs_parent(2)
            # called by dfs_parent(10) will add 2 to the partition and 2's index in the 
            # partition, which is 0 since it is the first node added, is not -1 and so 
            # 20 and 2 are not part of the same loop.
            # At this point dfs_parent(20) will add both 2 and 19 to the list of alrady 
            # visited parents as a tuple (parent_node,index_of_parent) and 
            # decide after the loop (for parent_index in node.parents:) we are currently in
            # whether 20 and/or 19 are part of a loop also containing 20.
            logger.trace ("dfs_parent parent " + str(parent_node.ID) + " of " + str(node.ID) + " already visited"
                + " append parent " + str(parent_node.ID) + " to already_visited_parents.")
            parent_node_visited_tuple = (parent_node,index_of_parent)
            already_visited_parents.append(parent_node_visited_tuple)
            # use -1 for the unknown parent index value. Later we will 
            # correct/patch this value.
# rhc : ******* Partition
            partition_node.parents.append(-1)
# rhc : ******* Group
            group_node.parents.append(-1)
    
            # This tracking of partition loops generates debug information 
            # about the detected loops. When we detect a loop, e.g., when 
            # dfs_parent(20) sees that it parent 19 is already visited and 
            # 20 and 19 aer part of the same loop, we output a message to 
            # the effect that a loop was detected when 20 tries to visit 19. 
            # We do not yet try to generate any
            # debug nmessages about the set of nodes that form the loop.
            #
            # The parent node has been visited so get its info and determine whether
            # this info indicates a loop
            pg_tuple = nodeIndex_to_partition_partitionIndex_group_groupIndex_map[parent_index]
            parent_partition_parent_index = pg_tuple[1]
# rhc : ******* Partition
            # global constant
            if TRACK_PARTITION_LOOPS:
                if parent_partition_parent_index == -1:
                    # Example of debug information:
                    # "1 5 6 7 3(Lprnt_of_11) 12(Lprnt_of_11) 11 11(Lc) 12 4 3 2 10 9 8"
                    # Here, 3 is a parent of 11 that 11 finds visited so 
                    # dfs_parent(11) will output 3(Lprnt_of_11). Same for when dfs_parent(11)
                    # finds parent 12 is visited 12(Lprnt_of_11) We use "3(Lprnt_of_11)
                    # indicators to show a loop was detected when df_parent(1)1 visited parent 3
                    # and to show 3 in the partition before 11, where 3 is the parent of 11.
                    # We use "12(Lprnt_of_11)" to show a loop was detected when dfs_parent(1)1 visited 
                    # parent 12. 11 is the parent of 12 and 11 was put in partition before 
                    # 12 so we do not need "12(Lprnt_of_11)" before the 11 - it is just to 
                    # indicates the loop detected when 11 saw it's parent 12 was visited.
                    loop_indicator = str(parent_node.ID)+"(Lprnt_of_" + str(node.ID) + ")"
                    current_partition.append(loop_indicator)
                    logger.trace("[Info]: Possible parent loop detected, start and end with " + str(parent_node.ID)
                        + ", loop indicator: " + loop_indicator)
                    global loop_nodes_added
                    loop_nodes_added += 1
# rhc : ******* end Partition - only track loops for partitions, for now

            # We try to deect a loop as early as possible so that we can use the
            # correct partition name, e.g., "PR2_1L" instead of "PR2_1" when we use the 
            # name in frontier tuples and add names to the Senders and Receivers structures 
            # used for DAG creation.
            #
            # Note: We will use the non-loop partition/group names, e.g., "PR2_1"
            # unless and until we detect a loop. This means we may change the
            # name to "PR2_1L" after we have used the name "PR2_1" one or more 
            # times. Thus, we will need to change/correct the name "PR2_1" in 
            # all the places it was used to "PR2_1L". This is called a "patch"
            # and we maintain several lists of patch tuples which we iterate
            # through at the end to make the necessary changes to the partition/
            # group names. If no loop is detected, the original name, e.g., "PR2_1"
            # is fine and no changes will be needed.

            if parent_partition_parent_index == -1:
                # Set a flag to indicate a loop was detected.
                logger.trace("XXXXXXXXXXXXXXXXX dfs_parent: Loop Detected: "
                    + "PR" + str(current_partition_number) + "_" + str(num_frontier_groups))
# rhc : ******* Partition
                # Set a flag indicating that a loop was detected in the
                # current partition.
                global current_partition_isLoop
                current_partition_isLoop = True

                try:
                    msg = "[Error]: parent_partition_parent_index is -1" + " indicating that current partition is a loop but " \
                        + " parent_group_parent_index is not -1, when the group should also be in a loop."
                    assert not (parent_group_parent_index != -1) , msg
                except AssertionError:
                    logger.exception("[Error]: assertion failed")
                    if exit_program_on_exception:
                        logging.shutdown()
                        os._exit(0)
                # assertOld:
                #if parent_group_parent_index != -1:
                #    logger.error("[Error]: parent_partition_parent_index is -1"
                #        + " indicating that current partition is a loop but "
                #        + " parent_group_parent_index is not -1, when the group should also be in a loop.") 
# rhc : ******* Group
                # Set a flag indicating that a loop was detected in the
                # current group.
                global current_group_isLoop
                current_group_isLoop = True
            else:
                logger.trace("YYYYYYYYYYYYY dfs_parent: No Loop Detected: "
                    + "PR" + str(current_partition_number) + "_" + str(num_frontier_groups))

        # We have added a parent index to the list of parent indices. This 
        # was the actual index or -1. So the index for the next parent is incremented.
        # index_of_parent is initialized to 0, the first index of a list.
        index_of_parent += 1

    # Note: If a loop is detected current_partition_isLoop and current_group_isLoop are
    # both set to True. current_partition_isLoop remains True until the end 
    # of the partition is reached. current_group_isLoop is set to False when the 
    # end of the group is reached. So it is possible that current_partition_isLoop is
    # True and current_group_isLoop is False since we can end a group in the
    # partition before we end the partition (since the partition can
    # have more groups.)

    # The name of the current partition/group depends on whether it
    # has a loop. If so we add an 'L' to the end of the name.
    # Example: "PR2_2" becomes "PR2_2L".

# rhc : ******* Partition
    current_partition_name = "PR" + str(current_partition_number) + "_1"
    if current_partition_isLoop:
        current_partition_name += "L"

# rhc : ******* Group
    current_group_name = "PR" + str(current_partition_number) + "_" + str(num_frontier_groups)
    if current_group_isLoop:
        current_group_name += "L"

    # Did we find already visited parents in the above dfs search loop?
    if len(already_visited_parents) > 0:
        logger.trace("process already_visited_parents of " + str(node.ID))
    # can't add shadow nodes for node until all parents have been added via dfs_parent
    # Q: Can we do this as part of else and then finish the appends here?
    # I think that is what we are doing since all this is the appends of shadow nodes
    # and saving the frontier_node in the parent in different partition/group
    #
    # For each already visited parent:
    # - 
    for parent_node_visited_tuple in already_visited_parents:
        # parent node
        visited_parent_node = parent_node_visited_tuple[0]
        # index of parent in this node's parent list (which is -1 for now)
        index_of_parent = parent_node_visited_tuple[1]
        #where the tuple we aer getting here was put into map as: (parent_node,index_of_parent) 
        partition_group_tuple = nodeIndex_to_partition_partitionIndex_group_groupIndex_map.get(visited_parent_node.ID)
        # this is also used in the else: part for debugging so declare here
        parent_partition_number = None
        parent_group_number = None

        # assserting the if statement is True
        try:
            msg = "[Error]: BFS: partition_group_tuple is None"
            assert partition_group_tuple != None , msg
        except AssertionError:
            logger.exception("[Error]: assertion failed")
            if exit_program_on_exception:
                logging.shutdown()
                os._exit(0)
        if partition_group_tuple != None:   # if this is false it's an error (see the else-part)
            parent_partition_number = partition_group_tuple[0]
            # If parent is not in the previous partition, the parent may still
            # be in a previous group (same partition). So in this then-branch we will 
            # check below to see if the parent is in the same group. 
            #
            # If the parent is in the previous partition, it is also in a previous 
            # group. So in the else branch we will have found parent that is in the
            # previous partition PR and a previous group (which is a group in PR.)
            #
            # In the whiteboard example:
            # 19 is a parent of 20; they are in the same partition and group and
            # are part of a loop. 2 is a parent of 20, they are in the same partition but
            # different groups. 17 is a parent of 19, they are in different partitions
            # and thus are also in different groups.

            try:
                msg = "[Error]: parent_partition_number is -1 but" + " it should have been set to current_partition_number at the beginning" + " of dfs_parent(parent)."
                assert not (parent_partition_number == -1) , msg
            except AssertionError:
                logger.exception("[Error]: assertion failed")
                if exit_program_on_exception:
                    logging.shutdown()
                    os._exit(0)
            # assertOld: parent_partition_number != -1. We set the parent's partition number
            # at the beginning of dfs_parent(parent) to current_partition so it should not be -1.
            # (We set the parent partition number to the actual partition number. However, the
            # index of the parent node in the partition was set to -1 at the start of 
            # dfs_parent and it is not set to the actual value of the index until the 
            # end of dfs_parent when the parent was added to the partition and its index thus became known.)
            # Either the parent partition number is the same as the current_partition number, 
            # i.e., the parent and node are in the same partition, or different. If node's
            # current_partition, which is the partition that node will be put in,
            # is n and node's parent is in a different partition then the partition number of 
            # node's parent is n-1; it cannot be -1. 
            # if the partition numbers of node and its parent are the 
            # same, node and parent can be in a loop; otherise, they cannot be in 
            # a loop (since if they were in a loop then node would have been 
            # visited as part of dfs_parent(parent) and node and parent would have been
            # both added to the parent's partition.)

            #if parent_partition_number == -1:
            #    logger.error("[Error]: parent_partition_number is -1 but"
            #        + " it should have been set to current_partition_number at the beginning"
            #        + " of dfs_parent(parent).")
            #    logging.shutdown()
            #    os._exit(0)

            # parent_partition_number != -1 was assserted.
            # We know the parent was already visited. dfs_parent(parent)
            # set and saved the parent's partition number. The index of the 
            # parent may or may not be known. If node and parent are in a loop then the index of
            # the parent is now known. Otherwise, the parent's index is known and the parent 
            # is either in the same partition as node or a different partition. In the example
            # above, node 20 and parent 2 are not part of a loop and they are in the same 
            # partition. While node 20 and parent 19 are part of a loop and are in the
            # same partition. Note that 19 and parent 17 are not in a loop that includes both
            # of them and they are in different parttions. 
            #
            # So:
            # If node and parent are in the same partition, we check whether they are part of 
            # the same loop. Also, we check to see if they are in the same group.
            # If node and parent are not in the same partition, they are not part of the same loop,
            # and they must be in different groups.
            # In the whiteboard example:
            # 19 is a parent of 20; they are in the same partition and group and
            # are part of the same loop. 2 is a parent of 20, they are in the same partition but
            # they are in different groups, and they are not in a loop. 17 is a parent of 19, 
            # they are in different partitions and thus must be in different groups.
            #if parent_partition_number == -1 or parent_partition_number == current_partition_number:
            if parent_partition_number == current_partition_number:
                # Recall that we either build a DAG of parititions or a DAG of groups. So we will
                # either be building partitions or building groups. If we are building partitions
                # then an executor/worker will compute pagerank values for the nodes in a partition
                # and if the partition has a node P that is a parent of some node C in a different
                # partition, then the executor/worker will communicate the computed pagerank value
                # for P to the executor/worker that will handle the partition containing C.
                #
                # parent is not in previous partition, i.e., node is not a child of
                # a parent node that was in previous partition. This means
                # parent is in the same partition as node and parent is either in the same 
                # group as node or it is in a different group. For example, 2 is parent
                # of 20 and 2 is in the same partition as 20 but a different group.
                # parent and node may be part of the same loop. Since parent and node
                # are in the same partition, we do not need a shadow node (procy) for the parent.
                #
                # If parent is in the same group then we do not need a shadow node for the parent.
                # otherwise, we need a shadow_node just like the case in which the
                # parent is in a different partition. A shadow node is a proxy for the parent.
                # The value receieved for the parent is assigned to the proxy and the proxy
                # value is used for the pagerank computation of node. (We will use the index of the 
                # shadow node as a parent index of node.)
                #
                # The parent and node are in different groups if they have different group numbers.

                logger.trace ("dfs_parent: parent in same partition: parent_partition_number: " 
                    + str(parent_partition_number) 
                    + ", current_partition_number:" + str(current_partition_number)
                    + ", parent ID: " + str(parent_index))

                # Check if we detected a loop. A loop is possible since parent is in the same 
                # partition/group as node. If not a loop, then parent is in previous partition or group, 
                # and that is handled next. Note that 
                # pg_tuple = nodeIndex_to_partition_partitionIndex_group_groupIndex_map[parent_index]
                # and the partition and group numbers are not -1 since they were assigned
                # a value at the start of the dfs search for the parent but the 
                # parent_partition_parent_index and parent_group_parent_index are not assigned
                # a value until the dfs search backs up to parent so if node and parent are
                # in a loop, parent will eventully visit node (as an ancestor of parent) which may 
                # try to visit parent if there is a loop and will see that 
                # parent_partition_parent_index or parent_group_parent_index is -1.
                #
                # From above, a node's
                # information in map pg_tuple is initialized at the start of dfs_parent as:
                #partition_number = current_partition_number
                #partition_index = -1
                #group_number = current_group_number
                #group_index = -1

                parent_partition_parent_index = partition_group_tuple[1]
                parent_group_parent_index = partition_group_tuple[3]

                # case: visited parent before and it is is same partition.
                # Parent and node may be in a loop so check for loop.

                # Example: In PR2_2L, node 20 will find that 
                # its parent 19 is in the same partition but we check to 
                # see whether 19 and 20 are in a loop, which they are. Parent 19 has 
                # parent_partition_parent_index -1 since 19's parent_partition_parent_index
                # is not set until the dfs that started at 19 unwinds back to 19
                # and that hasn't happened - this search from 19 eventually visits 20 and 
                # then tries to visit 19 but 19 is already visited and its parent_partition_parent_index
                # is not set until the search at 19 unwinds back to 19 so 20 will see 
                # parent_partition_parent_index for 19 as -1, indicating a loop.

                if parent_partition_parent_index != -1: # not a loop
                    # No loop detcted. The parent's partition and group indices are available
                    # in the map. So there will be no need to patch the index values, i.e., we
                    # will not use -1 and patch then later.  We will need a shadow node
                    # if the parent is in a different group, in which case
                    # we will make this partition_node / group_node's parent be
                    # the shadow node. 
                    #
                    # Suggested assert group_index is also -1
# rhc : ******* Partition
                    partition_node.parents[index_of_parent] = parent_partition_parent_index
# rhc : ******* Group
                    group_node.parents[index_of_parent] = parent_group_parent_index
                else:
                    # need to patch the parent index, i.e., since parent_partition_parent_index
                    # is -1 due to the loop, we will not know what the value of parent_partition_parent_index
                    # is until the dfs search unwns back to the parent. Here, we set the parent's
                    # index in the partition/group to -1. Later we will path this value with the 
                    # actual value assigned to parent_partition_parent_index.
# rhc : ******* Partition
                    partition_node.parents[index_of_parent] = -1
# rhc : ******* Group
                    group_node.parents[index_of_parent] = -1
                    # finish this partition_node and group_node parent mapping 
                    # when the parent/group has finished and all parents have been mapped.
                    # This tuple tell us which position in the parents list needs to be patched.
                    patch_tuple = (parent_index,partition_node.parents,group_node.parents,index_of_parent,node.ID)
                    logger.trace("patch_tuple: " +str(patch_tuple))
# rhc : ******* Partition
                    # save all the patch tuples for later porcessing when the 
                    # actual parent index positions are known.
                    patch_parent_mapping_for_partitions.append(patch_tuple)
# rhc : ******* Group
                    patch_parent_mapping_for_groups.append(patch_tuple)

                    # Detected loop. When we compute pagerank for a group, the number
                    # of iterations for a loop-group is more than 1, while the number
                    # of iterations for a non-loop group is 1. The name for a group
                    # or partition with a loop ends with "L".
                    #
                    # Note: We detect loops above when we generate the parent_node_visited_tuples
                    # in the (for parent_index in node.parents) loop so we can use the L-based 
                    # partition/group names for partitions/groups that
                    # have a loop. Here we just asssert that the just detected loop that we found 
                    # iterating through the parent_node_visited_tuples should also
                    # have been detected above in the (for parent_index in node.parents) loop
                    # that generated the parent_node_visited_tuples.

                    try:
                        msg = "[Error] detected partition loop when" + " processing parent_node_visited_tuple that was not" \
                            + " detected when generating parent_node_visited_tuple"
                        assert not (current_partition_isLoop == False) , msg
                    except AssertionError:
                        logger.exception("[Error]: assertion failed")
                        if exit_program_on_exception:
                            logging.shutdown()
                            os._exit(0)
                    # assertOld: we should have detected a loop above. 
                    #if current_partition_isLoop == False:
                    #    logger.error("[Error]: detected partition loop when"
                    #        + " processing parent_node_visited_tuple that was not"
                    #        + " detected when generating parent_node_visited_tuple")

                """
                parent_partition_parent_index = partition_group_tuple[1]
                partition_node.parents[index_of_parent] = parent_partition_parent_index
                """

# rhc : ******* Group
                #partition_group_tuple = nodeIndex_to_partition_partitionIndex_group_groupIndex_map.get(parent_node.ID)
                #parent_group_number = None
                # This must be true (leave this if for now)
                try:
                    msg = "[Error]: BFS: partition_group_tuple is None"
                    assert partition_group_tuple != None , msg
                except AssertionError:
                    logger.exception("[Error]: assertion failed")
                    if exit_program_on_exception:
                        logging.shutdown()
                        os._exit(0)
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

                    if parent_group_number == -1:
                        logger.error("[Error]: parent_group_number is -1 but"
                            + " it should have been set to current_group_number at the beginning"
                            + " of dfs_parent(parent).")
                        logging.shutdown()
                        os._exit(0)

                    parent_group_number = partition_group_tuple[2]
                    if parent_group_number == current_group_number:
                    #if parent_group_number == -1 or parent_group_number == current_group_number:
                        logger.trace ("dfs_parent: parent in same group: parent_group_number: " 
                            + str(parent_group_number)
                            + ", current_group_number: " + str(current_group_number)
                            + ", parent ID: " + str(parent_index)) 

# rhc: case: visited parent before and it is is same group.  So no shadow node and 
# we already checked to see if parent partition index indicates a loop has been detected.

                        # Note: The parent is in the same group and could indicate
                        # a loop; however, we already checked for this when we saw
                        # that the parent was in the same partition. (Note: parent in the 
                        # same group ==> parent in same partition. Also, if the parent 
                        # indicates there is a loop in the current partition, then
                        # this parent will also indicate there is a loop in the current
                        # group.). If the check showed a loop in the partition
                        # then we created a path tuple for the partition and group.
                        #
# rhc : ******* Partition/Group
                        # Note: if we change code so that we either do partitions or groups 
                        # but not both then make sure we still detect loops for groups, i.e.,
                        # if we disbale partition code and it is detecting group loops then
                        # we can miss a group loop.

                        # Detected loop. When we compute pagerank for a group, the number
                        # of iterations for a loop-group is more than 1, while the number
                        # of iterations for a non-loop group is 1. The name for a group
                        # or partition with a loop ends with "L".

                        # Changed this to an asssert. The loop should have also been
                        # detected above when we generated parent_node_visited_tuples.
                        # assert: already detectd loop
                        #global current_group_isLoop
                        #if parent_partition_parent_index == -1:
                        #    current_group_isLoop = True


                        try:
                            msg = "[Error]: detected group loop when" + " processing parent_node_visited_tuple that was not" \
                                + " detected when generating parent_node_visited_tuple"
                            assert not (current_group_isLoop == False) , msg
                        except AssertionError:
                            logger.exception("[Error]: assertion failed")
                            if exit_program_on_exception:
                                logging.shutdown()
                                os._exit(0)
                        # assertOld:
                        #if current_group_isLoop == False:
                        #    logger.error("[Error]: detected group loop when"
                        #        + " processing parent_node_visited_tuple that was not"
                        #        + " detected when generating parent_node_visited_tuple")
                        #    logging.shutdown()
                        #    os._exit(0)
 
                        """
                        parent_group_parent_index = partition_group_tuple[3]
                        partition_node.parents[index_of_parent] = parent_group_parent_index
                        """

                    else:
                        # parent and node are in different groups so we add a shadow node to this
                        # group as a proxy for the parent node.
                        logger.trace ("dfs_parent: parent in different group: parent_group_number: " 
                            + str(parent_group_number) 
                            + ", current_group_number: " + str(current_group_number)
                            + ", parent ID: " + str(parent_index))

                        # The name of the current partition/group depends on whether it
                        # has a loop. If so we add an 'L' to the end of the name.
                        # Example: "PR2_2" becomes "PR2_2L".
                        #current_group_name = "PR" + str(current_partition_number) + "_" + str(num_frontier_groups)
                        #if current_group_isLoop:
                        #    current_group_name += "L"

                        list_of_parents_in_previous_group.append(visited_parent_node.ID) 
                        child_index_in_current_group = len(current_group)
                        # shadow node is a proxy for parent Node on frontier of previous partition
                        #shadow_node = Node(parent_node.ID)
                        shadow_node = Partition_Node(visited_parent_node.ID)
                        shadow_node.isShadowNode = True
                        # shadow noe looks like parent node - same number of children, which 
                        # is used in pagerank calculation.
                        shadow_node.num_children = len(visited_parent_node.children)
                        # this will be overwritten; the parent of the shadow node will be a
                        # node after the offical end of the partiton with a pagerank value
                        # that keeps he shadow_node's pagerank value constant. That is,
                        # when we perform the pagerank calculations for the nodes in the 
                        # partition/group, we perform the pagerank calculation for the 
                        # shadow node too, even though we don't really need it. To do this
                        # calculation for the shadow node we need its number of children and 
                        # we need th pagerank value for the shadow node's parent. But recall
                        # that the shadow node is assigned the pagerank value for the parent
                        # node that th shadow node proxies for. We use this pagerank
                        # value as the pagerank value for a parent of node. (node has a shadow
                        # node for each if its parents.) So when we compuet the pagerank value for
                        # the shadow node, we do not want its pagerank value to change. thus we 
                        # assign to the node that we add after the end of the partition (that 
                        # represents the parent of the shadow node) a pagerank value that will 
                        # keep the shadow node's pagerank value the same when we compute
                        # the pagernk of the shadw node. (We reverse engineer this value and assign
                        # it to the shadow node's parent node.) Now we can compute the pagerank 
                        # values for all the nodes with no if-statements, which will remove slowdowns
                        # dut to speculatve branch failures. The idea is to tru to make the pagerank
                        # calculation loop have minimal cache misses and branch misses. (All the 
                        # nodes will be in shared arrays using the structure-of-arrays approach
                        # instead of the array of structures approach.)
                        #
                        # insert shadow_node in group before inserting node
                        shadow_node.parents.append(-1)
                        # The shadow node's ID is the same as the parent it proxies, with an "-s"
                        # to indicate it is a shadow node. For example, noed 20 has a parent of 2
                        # so the shadow node for node 20 has ID "2-s"
                        logger.trace("dfs_parent: add shadow node: " + str(visited_parent_node.ID) + "-s")
#rhc: ToDo:
                        # If parent and child nodes are in different group but same partition 
                        # then no shadow node is needed in he partition as no need to send 
                        # parent's pagerank value. Thus we only do the group append here.

                        current_group.append(shadow_node)

# rhc: case: visited parent before and it is is same partition so set the parent
# at index index_of_parent to parent_partition_parent_index = partition_group_tuple[0]
                        """
                        parent_group_parent_index = len(current_group)-1
                        partition_node.parents[index_of_parent] = parent_group_parent_index
                        """

                        # We appended shadow node to current_group so shadow node 
                        # position is len(current_group)-1
                        nodeIndex_to_groupIndex_map[shadow_node.ID] = len(current_group)-1
                        # Note: We do not add shadow_node to the global 
                        # X map. But shadow_node IDs should be mapped to their positions
                        # when we are computing the group since if the shadow node
                        # is a parent of node n then n.parents are remapped to their 
                        # position in the group and one of n's parents will be the shadow
                        # node so we need its position in the group.

                        #rhc: make one of group node's parents be this shadow node
                        group_node.parents[index_of_parent] = len(current_group)-1

                        global num_shadow_nodes_added_to_groups
                        num_shadow_nodes_added_to_groups += 1

                        # remember where the frontier_parent node's pagerank value should be placed when 
                        # this task receives it, i.e., put this received value in the 
                        # shadow node which is at position child_index_in_current_group, which is
                        # the position of the just appended node.
                        # Note: child_index_in_current_group = len(current_group) before
                        # we added shadow node so child_index_in_current_group was the 
                        # position in which we added shadow node, which is now len(current_group)-1
                        logger.trace ("dfs_parent: num_frontier_groups: " + str(num_frontier_groups) + ", shadow node added at index: " + str(child_index_in_current_group))

                        #d1 = child_index-dfs_parent_start_partition_size
                        #logger.trace("ZZZZZZZZZZZZZZZZZZZZZZZZZ child_index: " + str(child_index) + " d1: " + str(d1))
                        #if child_index != d1:
                        #    logger.trace("ZZZZZZZZZZZZZZZZZZZZZZZZZ Difference: " 
                        #       + " child_index: " + str(child_index) + " d1: " + str(d1))
                        #else:
                        #   logger.trace("ZZZZZZZZZZZZZZZZZZZZZZZZZ No Difference: ") 
                        
                        #logger.trace("ZZZZZZZZZZZ")

                        # Note: Added a partition/group name field to this tuple since we need an 'L'
                        # in the name of the current partition/group if it is a loop. That is, we cannot 
                        # just compuet the group name using the partiton number and group number, as, say,
                        # "PR2_2" is not the right name if we hve detected that this node is in a loop and
                        # thus we need to use "PR2_2L". It could be that node is in a loop but we haven't
                        # detected the loop yet so the group name could be "PR2_1" when it should be "PR2_1L".
                        # We will patch the name to "PR2_1L" later in that case.
                        # We probably won't need the current_partition_number/num_frontier_groups but it's 
                        # available for debugging.
                        frontier_parent_tuple = (current_partition_number,num_frontier_groups,child_index_in_current_group,current_group_name)
                        logger.trace ("bfs frontier_parent_tuple: " + str(frontier_parent_tuple))

                        # Mark this node as a child node that will need the pagerank value computed for its parent.
                        # We will add the frontier_parent_tuple to a list of such tuples that in the 
                        # actual parent node. That is, the fact that this list is not empty for the parent node
                        # means that the parent node pagerank value needs to be sent to the child. The tuple 
                        # indicates which group it should be sent to. PageRank may send pagerank parent values to 
                        # multiple groups of multiple partitions. Noet that these parent 
                        # values are the outputs sent at fanouts and fanins. That is the 
                        # output of a pagerank task is one or more parent values sent to one or
                        # more groups/partitons. These output values are received as inputs and 
                        # assigned to the corresponding shadow nodes. If we send a parent value
                        # P for child C, we assign P to a shadow node of C.
                        #
                        # Get the group information about the parent - we needto locate the parent
                        # node in the grooup it was added to.
                        partition_group_tuple = nodeIndex_to_partition_partitionIndex_group_groupIndex_map[visited_parent_node.ID]
                        # group numer of parent
                        parent_group_number = partition_group_tuple[2]
                        # parent's position/index in this group
                        parent_group_parent_index = partition_group_tuple[3]
                        # index into the list of groups (a group contains one or more nodes)
                        parent_index_in_groups_list = partition_group_tuple[4]
                        # parent_group_number is the number of the group in the 
                        # current partition, e.g., this is the 2nd group in the current partition.)
                        # We are working on the current group, which is the next group to be added 
                        # to list groups. The current group number is num_frontier_groups, i.e., 
                        # this is the total number of groups that have been identified so far,
                        # as opposed to the number of groups that have been identified in the 
                        # current partition, and the current group 
                        # will be appended at position len(groups) in list groups. Note that group 
                        # numbers in a partition start at 1 not 0 = "PR2_1", "PR2_2", etc
                        # The parent group position in groups is before the current group, 
                        # i.e., len(groups) - i. What is i?
                        # Note: we are working back from the end of the groups list to find 
                        # the parent position. Also, the current group is in the current
                        # partition not some previous partition.
                        # The current group number is num_frontier_groups. The parent group number
                        # (in this same partition) is parent_group_number, which is 
                        # less than num_frontier_groups. (If current group is, say, 3rd group identified
                        # so far then parent group must be group 2 or 1.) If current group = num_frontier_groups
                        # is 2, and parent group is 1, then we want the group in the groups list at position 
                        # len(groups) - (current group num-parent group num). Example, if len(groups)
                        # is 2, the 2 existing groups, groups (1 and 2) are in positions [0] 
                        # and [1]. The current group will be the third group and will be added 
                        # at position [2]. Since len(groups) is 2, and (current_group-parent_group) 
                        # is (2-1) = 1, then the parent group we want is at groups[2-1], which is groups[1].
                        parent_group_position = len(groups) - (num_frontier_groups-parent_group_number)
                        # assert: double check
                        try:
                            msg = "[Error]: dfs_parent: for parent " + str(parent_index) + " parent_index_in_groups_list != parent_group_position" \
                                + " parent_index_in_groups_list: " + str(parent_index_in_groups_list) + " parent_group_position: " + str(parent_group_position)
                            assert parent_index_in_groups_list == parent_group_position , msg
                        except AssertionError:
                            logger.exception("[Error]: assertion failed")
                            if exit_program_on_exception:
                                logging.shutdown()
                                os._exit(0)
                        #assertOld:
                        #if not parent_index_in_groups_list == parent_group_position:
                        #    logger.error("[Error]: dfs_parent: for parent " + str(parent_index)
                        #        + " parent_index_in_groups_list != parent_group_position"
                        #        + " parent_index_in_groups_list: " + str(parent_index_in_groups_list)
                        #        + " parent_group_position: " + str(parent_group_position))
                        #    logging.shutdown()
                        #    os._exit(0)

                        parent_group = groups[parent_index_in_groups_list]

                        logger.trace("dfs_parent: add frontier tuple to this parent group:")
                        for n in parent_group:
                            logger.trace(str(n))
                        logger.trace("len(groups): " + str(len(groups)) + ", parent_group_number: " + str(parent_group_number)
                            + ", num_frontier_groups: " + str(num_frontier_groups) 
                            + ", parent_index_in_groups_list: " + str(parent_index_in_groups_list)
                            + ", parent_group_parent_index: " + str(parent_group_parent_index)
                            + ", frontier_parent_tuple: " + str(frontier_parent_tuple))
                        parent_group[parent_group_parent_index].frontier_parents.append(frontier_parent_tuple)
                        logger.trace("parent_group[parent_group_parent_index].ID: " + str(parent_group[parent_group_parent_index].ID))
                        logger.trace("frontier tuples of parent node after:")
                        for t in  parent_group[parent_group_parent_index].frontier_parents:
                            logger.trace(str(t))
                        # The current partition/group name in [3] may not have an "L" but if we later 
                        # find a loop we'll need to append an "L" to this name in the frontier tuple
                        # so we need to patch this name (of this task, which is receiving task) but
                        # not the name of parent, which has already been determined (loop or not)
                        # as it is a different partition/group from this one and so already processed.
                        # If we do not find a loop in this current group, we will not need to do any patches.
                        if not current_group_isLoop:
                            position_in_frontier_parents_group_list = len(parent_group[parent_group_parent_index].frontier_parents)-1
                            frontier_parent_group_patch_tuple = (parent_index_in_groups_list,parent_group_parent_index,position_in_frontier_parents_group_list)
                            frontier_parent_group_patch_tuple_list.append(frontier_parent_group_patch_tuple)
                        """
                        Sample Output of above:
                        # for parent node 2, node 2 is in group 1 of partition 2. The current node is node 20 of group 2 of partition 2
                        DEBUG: dfs_parent: parent in different group: parent_group_number: 1, current_group_number: 2, parent ID: 2
                        # add shadow node to group 2 as a proxy for node 20's parent 2. shadow node name is "2-s"
                        # meaning the parent node it proxies is 2 and it is a shadow node "-s"
                        DEBUG: dfs_parent: add shadow node: 2-s
                        # we aew currently building group 2 and we will add the shadow node as
                        @ the first node in the group, which is at position 0
                        DEBUG: frontier_groups: 2, child_index: 0
                        # The shadow node is in partition 2, group 2, position 0 and group 2 is named "PR2_2L"
                        # as we have detected a loop (20's parent is 19 which was already visited(as a child of 17))
                        DEBUG: bfs frontier_parent_tuple: (2, 2, 0, 'PR2_2L')
                        # This is group 1 which contains node 2 in position 0
                        DEBUG: add tuple to this parent group:
                        DEBUG: 2
                        DEBUG: 10
                        DEBUG: 5-s
                        DEBUG: 16
                        # we have collected 2 groups so far, node 2 is in parent group 1 of 
                        # partition 2, node 20 is in group 2 (num_frontier_groups) of partition 2.
                        DEBUG: len(groups): 2, parent_group_number: 1, num_frontier_groups: 2, 
                               parent_index_in_groups_list: 1, parent_group_parent_index: 0, frontier_parent_tuple: (2, 2, 0, 'PR2_2L')
                        # We will add frontier tuple to node 2
                        DEBUG: parent_group[parent_group_parent_index].ID: 2
                        # after adding this tuple, parent node 2 has 1 tuple, the one just added.
                        DEBUG: frontier tuples of parent node after:
                        DEBUG: (2, 2, 0, 'PR2_2L')
                        """
       
                        # If we are using worker threads/processes and we put all of the 
                        # partition/groups, i.e., DAG tasks, in one shared array, then 
                        # we create a shared_frontier_parent_tuple for this case. When 
                        # a thread/process computes the pagerank for a group/partition
                        # it will still have to "output" the value of parent nodes
                        # that are needed by their children, but it can do this by copying
                        # the parent pagerank value from the parent's position in the 
                        # array to the child's position in the array. Thus, we added a field
                        # to the tuple to identify the position of the parent's pagerank
                        # value in the array. The pagerank function will grab this parent value
                        # at the end of its computation and copy it to the child's shadow node
                        # as a proxy for the pagerank value of parent. This means that the shadow
                        # node will appear in the list of parents for the child node. We use the 
                        # shadow node when computing the pagernk value of the child node.
                        # See BFS_Shared.py

                        # if True: 
                        if use_shared_partitions_groups:
                            # this is the key for the map
                            task_name_of_parent_group = group_names[parent_index_in_groups_list]
                            #task_name_of_parent = "PR" + str(parent_partition_number) + "_" + str(parent_group_number)
                            shared_frontier_parent_tuple = (current_partition_number,num_frontier_groups,child_index_in_current_group,current_group_name,parent_group_parent_index)
                            # get the tuples (value) for the task named task_name_of_parent_group (key).
                            # we will add this tuple. So when we computing pagerank for task_name_of_parent_group
                            # after computing the values we will get the values for the parent nodes identified
                            # in the tuples (at position parent_group_parent_index in the array)
                            # and copy then to the child positions identified in the tuples
                            # (child_index_in_current_group).
                            list_of_parent_frontier_tuples = BFS_Shared.shared_groups_frontier_parents_map.get(task_name_of_parent_group)
                            if list_of_parent_frontier_tuples is None:
                                list_of_parent_frontier_tuples = []
                            logger.trace("groupOGOGOGOGOGOGOGOG add shared tuple to parent group: ")
                            for n in parent_group:
                                logger.trace(str(n))
                            logger.trace("task_name_of_parent: " + task_name_of_parent_group
                                + ", frontier_parent_tuple: " + str(frontier_parent_tuple))
                            logger.trace("list_of_parent_frontier_tuples before appending tuple: " 
                                + str(list_of_parent_frontier_tuples))
                            # Add new tuple for task task_name_of_parent_group to list of tuples
                            # and map task_name_of_parent_group to this updated list
                            list_of_parent_frontier_tuples.append(shared_frontier_parent_tuple)
                            BFS_Shared.shared_groups_frontier_parents_map[task_name_of_parent_group] = list_of_parent_frontier_tuples
                            logger.trace("New BFS_Shared.shared_groups_frontier_parents_map:")
                            for (k,v) in BFS_Shared.shared_groups_frontier_parents_map.items():
                                logger.trace(str(k) + ": " + str(v))                            

                            """
                            where:  in bfs_pagerank, we grab the shared_frontier_parent_tuple
                            and its fields using:
                                    #FYI:
                                    # partition_number = frontier_parent[0]
                                    # group_number = frontier_parent[1]

                                    position_or_group_index_of_output_task = frontier_parent_tuple[2]
                                    partition_or_group_name_of_output_task = frontier_parent_tuple[3]
                                    # Note: We added this field [4] to the frontier tuple so that when
                                    # we are using a shared_nodes array or multithreading we can
                                    # copy values from shared_nodes[i] to shared_nodes[j] instead of 
                                    # having the tasks input/output these values, as they do when 
                                    # each task has its own partition and the parent values need to be sent
                                    # and received (possibly over tcp for fanouts/fanins) instead of copied.
                                    parent_or_group_index_of_this_task_to_be_output = frontier_parent_tuple[4]
                            """

                            # The current partition/group name in [3] may not have an "L" but if we later 
                            # find a loop we'll need to append an "L" to this name in the frontier tuple
                            # so we need to patch this name (of this task, which is receiving task) but
                            # not the sname of parent, which has already been determined (loop or not)
                            # as it is a different partition/group from this one so already processed.
                            if not current_group_isLoop:
                                position_in_list_of_parent_frontier_tuples = len(list_of_parent_frontier_tuples)-1
                                shared_frontier_parent_group_patch_tuple = (task_name_of_parent_group,position_in_list_of_parent_frontier_tuples)
                                shared_frontier_parent_groups_patch_tuple_list.append(shared_frontier_parent_group_patch_tuple)

                        # Generate dependency dges in DAG. If parent in group i has an edge
                        # to child in group j, i!=j, then add edge i-->j to dag. Do this
                        # by adding j to the receivers of i, and adding i to the receivers
                        # of j. Si group i is the "sender" and j is the "receiver"
                        # Note: If we want to speedup construction of the DAG, then we might want to 
                        # specifically add the edge i-->j, e.g., by depositing i-->j into a bounded
                        # buffer that a DAG-generator thread withdraws and uses for generating
                        # the incremental DAG_info object. As opposed to generating at the end
                        # all-at-once the DAG_info object for all the ependencies/edges identified..
                        #sending_group = "PR"+str(parent_partition_number)+"_"+str(parent_group_number)
                        # index in groups list is the actual index, starting with index 0
                        sending_group = group_names[parent_index_in_groups_list] # group with parent node
                        receiving_group = current_group_name
                        #sending_group = "PR"+str(parent_partition_number)+"_"+str(parent_group_number)
                        #receiving_group = "PR"+str(current_partition_number)+"_"+str(num_frontier_groups)
                        # get the set of receivers for sending_group
                        sender_set = BFS_generate_DAG_info.Group_senders.get(sending_group)
                        if sender_set is None:
                            BFS_generate_DAG_info.Group_senders[sending_group] = set()
                        # add receiving group as another group sending_group sends to 
                        BFS_generate_DAG_info.Group_senders[sending_group].add(receiving_group)
                        # these are the senders that send to receiver receiving_group
                        receiver_set = BFS_generate_DAG_info.Group_receivers.get(receiving_group)
                        if receiver_set is None:
                            BFS_generate_DAG_info.Group_receivers[receiving_group] = set()
                        # add sending_group as another sender that sends to receiving_group
                        BFS_generate_DAG_info.Group_receivers[receiving_group].add(sending_group)
                        # So now task receiving_group depends on sending_group

                        # We may nee to patch the name of the receiving group, i.e., we may yet
                        # find a loop and so need to change e.g., "PR2_2" to "PR2_2L", hhich means
                        # we would need to change the places we have already used the name "PR2_2"
                        # to "PR2_2L" by performing a "patch" using the "path tuples"
                        if not current_group_isLoop:
                            sender_receiver_group_patch_tuple = (parent_index_in_groups_list,receiving_group)
                            sender_receiver_group_patch_tuple_list.append(sender_receiver_group_patch_tuple)

                else:
                    #assertOld:
                    logger.error("[Error]: dfs_parent: partition_group_tuple " 
                        + "is None should be unreachable.")
                    logging.shutdown()
                    os._exit(0)
                # SCC 2
# rhc : ******* END Group

            else:
                ## Note: assertion above should have failed.

                # parent is in different/previous partition, (must be previous partition current_partition - 1)
                # This means that the parent is in a different group too, i.e., it is in a group that
                # is in the different partition. We can add a noed to the current partiton and a node
                # to the current group (depending on which we are collecting)
                #logger.error("[Error]: dfs_parent: parent_partition_number != current_partition_number "
                #    + " should be unreachable.")
                #logging.shutdown()
                #os._exit(0)

                logger.trace ("dfs_parent: parent in different partition/group: parent_partition_number: " 
                    + str(parent_partition_number) 
                    + ", current_partition_number:" + str(current_partition_number)
                    + ", parent ID: " + str(visited_parent_node.ID))

                # The name of the current partition/group depends on whether it
                # has a loop. If so we add an 'L' to the end of the name.
                # Example: "PR2_2" becomes "PR2_2L".

# rhc : ******* Partition
                #parents_in_previous_partition = True
                list_of_parents_in_previous_partition.append(visited_parent_node.ID)

                # parent is in different partition so add a shadow node as 
                # a proxy for the parent. This is the index of the shadow node in partition.
                child_index_in_current_partition = len(current_partition)
                shadow_node = Partition_Node(visited_parent_node.ID)
                shadow_node.isShadowNode = True
                shadow_node.num_children = len(visited_parent_node.children)
                # this will be overwritten; the parent node of the shadow node will 
                # be a node addedafter the official end of the partiton with a pagerank 
                # value that keeps he shadow_node's pagerank value constant.when we
                # perform the pagerank computation on the shadow node.
                shadow_node.parents.append(-1)
                # shadow nodes for child are in positiions immediatley preceeding shadow node
                # add shadow node to partition
                current_partition.append(shadow_node)

# rhc: case: visited parent before and it is is same partition so set the parent
# at index index_of_parent to parent_partition_parent_index = partition_group_tuple[0]
                """
                parent_partition_parent_index = len(current_partition)-1
                partition_node.parents[index_of_parent] = parent_partition_parent_index
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

# rhc : ******* Group
                # This is the index of the shadow node in group.
                child_index_in_current_group = len(current_group)
                # add shadow node to group.
                current_group.append(copy.deepcopy(shadow_node))
                logger.trace("dfs_parent: add shadow node to group: " + str(visited_parent_node.ID) + "-s")
# rhc: case: visited parent before and it is is same partition so set the parent
# at index index_of_parent to parent_partition_parent_index = partition_group_tuple[0]
                """
                parent_group_parent_index = len(current_group)-1
                partition_node.parents[index_of_parent] = parent_group_parent_index
                """

# rhc : ******* Partition
                global nodeIndex_to_partitionIndex_map
                #global nodeIndex_to_groupIndex_map
                nodeIndex_to_partitionIndex_map[shadow_node.ID] = len(current_partition)-1
#rhc: ToDo:
# rhc : ******* Group
                # only do part/group if using part/group or option to do both
                # for debugging?
                nodeIndex_to_groupIndex_map[shadow_node.ID] = len(current_group)-1
                # Note: We do not add shadow_node to the global
                # X map. But shadow_node IDs should be mapped to their positions
                # when we are computing the group since if the shadow node
                # is a parent of node n then n.parents are remapped to their 
                # position in the group and one of n's parents will be the shadow
                # node so we need its position in the group.

# rhc : ******* Partition
                #rhc: make node's parent be this shadow node as it is a proxy for parent
                partition_node.parents[index_of_parent] = len(current_partition)-1
# rhc : ******* Group
                group_node.parents[index_of_parent] = len(current_group)-1
            
# rhc : ******* Partition
                global num_shadow_nodes_added_to_partitions
                #global shadow_nodes_added_to_groups
# rhc : ******* Partition or Group
                num_shadow_nodes_added_to_partitions += 1
                num_shadow_nodes_added_to_groups += 1

# rhc : ******* Partition
                # remember where the frontier_parent node should be placed when the 
                # partition the PageRank task sends it to receives it. 
                logger.trace ("num partitions: " + str(current_partition_number) + ", child_index_in_current_partition: " + str(child_index_in_current_partition))
# rhc : ******* Group
                logger.trace ("num_frontier_groups: " + str(num_frontier_groups) + ", child_index_in_current_group: " + str(child_index_in_current_group))

# STOP

# rhc: ToDo: if we are using partition then we just need partition number and index
# but we won't use group number? That is, the names aer PR1, PR2, etc, so we ignore'
# the group number when we form partition name for target funtion with shadow nodes?
# Actually, we use the name of the partition instead of the partition and group numbers
# since the name might have an "L" in it so partition and group numbers are not enough 
# to generate the name - we need to know if there's an L so we use the compete name.
# We currently ignore the partition number and group number in the frontier tuple.
# They are here for debugging.

                # Note : For partitions, the child_index is the index relatve to the 
                # start of the partition. child_index is len(current_partition).
                # The calculation for groups (below) is a bit difference.
                #
                # Note: Added a partition/group name field to the tuple since we need an 'L'
                # in the name of the current partition/group if it is a loop. We probably won't
                # need the current_partition_number/num_frontier_groups but it's available for now for debugging.
# rhc : ******* Partition
                frontier_parent_partition_tuple = (current_partition_number,1,child_index_in_current_partition,current_partition_name)
                logger.trace ("bfs frontier_parent_partition_tuple (pnum,1,childindx,pname): " + str(frontier_parent_partition_tuple))
# rhc : ******* Group
                frontier_parent_group_tuple = (current_partition_number,num_frontier_groups,child_index_in_current_group,current_group_name)
                logger.trace ("bfs frontier_parent_group_tuple: (pnum,gnum,chldinx,gname) " + str(frontier_parent_group_tuple))
 
                # mark this node as one that PageRank needs to send in its output to the 
                # next partition (via fanout/faninNB).That is, the fact that list
                # frontier_parent is not empty indicates it needs to be sent in the 
                # PageRank output. The tuple indictes which frontier group it should 
                # be sent to. PageRank may send frontier_parent nodes to mulltiple groups
                # of multiple partitions
                #
                # need to use the current partition, not nodes as the current
                # partition is what the functions will be using to compute pagerank
                # nodes[parent_node.ID].frontier_parents.append(frontier_parent_tuple)

                partition_group_tuple = nodeIndex_to_partition_partitionIndex_group_groupIndex_map[visited_parent_node.ID]
                logger.trace ("visited_parent_node.ID " + str(visited_parent_node.ID)
                    + "partition_group_tupleof parent from global map:" + str(partition_group_tuple))
                parent_partition_number = partition_group_tuple[0]
                parent_partition_parent_index = partition_group_tuple[1]
                parent_group_number = partition_group_tuple[2]
                parent_group_parent_index = partition_group_tuple[3]
                # cannot use parent_group_number to index groups; parent_group_number
                # is a number within a partition, e.g., PR2_2 has a group index of 2,
                # but this is not necessarily the 2nd group identified overall.
                parent_index_in_groups_list = partition_group_tuple[4]

# rhc : ******* Partition
                logger.trace ("partition_group_tuple of parent from global map: " + str(visited_parent_node.ID) + " (pnum,pindx,gnum,gindx,posingroupslist): " + str(partition_group_tuple))
                # partition numbers start at 1 not 0
                logger.trace ("add frontier tuple to parent partition")
                # actual partition of nodes
                parent_partition = partitions[parent_partition_number-1]
                # add tuple to parent node in parent;s partition
                parent_partition[parent_partition_parent_index].frontier_parents.append(frontier_parent_partition_tuple)
# rhc : ******* Group
                logger.trace ("add frontier tuple to parent group")
                #parent_group = groups[parent_group_number-1]
                parent_group = groups[parent_index_in_groups_list]
                # add tuple to parent node if parent's group
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
# rhc : ******* Partition
                if not current_partition_isLoop:
                    position_in_frontier_parents_partition_list = len(parent_partition[parent_partition_parent_index].frontier_parents)-1
                    frontier_parent_partition_patch_tuple = (parent_partition_number,parent_partition_parent_index,position_in_frontier_parents_partition_list)
                    frontier_parent_partition_patch_tuple_list.append(frontier_parent_partition_patch_tuple)

# rhc : ******* Group
                # Note: in white board group 2_2, when 20 sees 2 it has detected no loop
                # so we will use "PR2_2" but then it sees 19 and we detect a loop, so 20 uses "PR2_2L" as
                # the name of its group. We'll need to patch the name "PR2_1" so we remember
                # this by creating a frontier_parent_group_patch_tuple for it nd saving this tuple
                # is a list of tupled to be processed later.
                if not current_group_isLoop:
                    position_in_frontier_parents_group_list = len(parent_group[parent_group_parent_index].frontier_parents)-1
                    frontier_parent_group_patch_tuple = (parent_index_in_groups_list,parent_group_parent_index,position_in_frontier_parents_group_list)
                    frontier_parent_group_patch_tuple_list.append(frontier_parent_group_patch_tuple)
            
                """
                Sample output of above:
                DEBUG: dfs_parent: parent in different partition: parent_partition_number: 1, current_partition_number:2, parent ID: 17
                dfs_parent: add shadow node to group: 17-s   # 17 is parent of 19
                num partitions: 2, child_index_in_current_partition: 8  # position of 19 is partition 2
                num_frontier_groups: 2, child_index_in_current_group: 5
                bfs frontier_parent_partition_tuple (pnum,1,childindx,pname): (2, 1, 8, 'PR2_1L')
                bfs frontier_parent_group_tuple of parent from global map: (pnum,gnum,chldinx,gname) (2, 2, 5, 'PR2_2L')
                # node 17 is in partition 1 and group 1 at position 1
                visited_parent_node.ID 17: partition_group_tuple of parent from global map:(1, 1, 1, 1, 0)
                partition_group_tuple of parent 17 (pnum,pindx,gnum,gindx,posingroupslist): (1, 1, 1, 1, 0)
                add frontier tuple to parent partition
                add frontier tuple to parent group
                """

# rhc : ******* Partition
                #if True: # 
                if use_shared_partitions_groups:
                    # shared partitions frontier code:
                    # this is the key for the map
                    task_name_of_parent_partition = partition_names[parent_partition_number-1]
                    #task_name_of_parent = "PR" + str(parent_partition_number) + "_" + "1"
                    shared_frontier_parent_tuple = (current_partition_number,1,child_index_in_current_partition,current_partition_name,parent_partition_parent_index)
                    # get the tuples (value) for the task named task_name_of_parent_group (key).
                    # we will add this tuple. So when we computing pagerank for task_name_of_parent_group
                    # after computing the values we will get the values for the parent nodes identified
                    # in the tuples (at position parent_group_parent_index in the array)
                    # and copy then to the child positions identified in the tuples
                    # (child_index_in_current_group).
                    list_of_parent_frontier_tuples = BFS_Shared.shared_partition_frontier_parents_map.get(task_name_of_parent_partition)
                    if list_of_parent_frontier_tuples is None:
                        list_of_parent_frontier_tuples = []
                    logger.trace("groupOPOPOPOPOPOP add shared tuple to parent group: ")
                    for n in parent_group:
                        logger.trace(str(n))
                    logger.trace("task_name_of_parent: " + task_name_of_parent_partition
                        + ", shared_frontier_parent_tuple: " + str(shared_frontier_parent_tuple))
                    logger.trace("list_of_parent_frontier_tuples before appending tuple: " 
                        + str(list_of_parent_frontier_tuples))
                    # Add new tuple for task task_name_of_parent_group to list of tuples
                    # and map task_name_of_parent_group to this updated list
                    list_of_parent_frontier_tuples.append(shared_frontier_parent_tuple)
                    BFS_Shared.shared_partition_frontier_parents_map[task_name_of_parent_partition] = list_of_parent_frontier_tuples
                    logger.trace("New BFS_Shared.shared_partition_frontier_parents_map:")
                    for (k,v) in BFS_Shared.shared_partition_frontier_parents_map.items():
                        logger.trace(str(k) + ": " + str(v))

                    """
                    where:  in bfs_pagerank, we grab the shared_frontier_parent_tuple
                    and its fields using:
                            #FYI:
                            # partition_number = frontier_parent[0]
                            # group_number = frontier_parent[1]

                            position_or_group_index_of_output_task = frontier_parent_tuple[2]
                            partition_or_group_name_of_output_task = frontier_parent_tuple[3]
                            # Note: We added this field [4] to the frontier tuple so that when
                            # we are using a shared_nodes array or multithreading we can
                            # copy values from shared_nodes[i] to shared_nodes[j] instead of 
                            # having the tasks input/output these values , as they do when 
                            # each task has its won partition and the alues need to be sent
                            # and received instead of copied.
                            parent_or_group_index_of_this_task_to_be_output = frontier_parent_tuple[4]
                    """
                    # The current partition/group name in [3] may not have an "L" but if we later 
                    # find a loop we'll need to append an "L" to this name in the frontier tuple
                    # so we need to patch this name (of this task, which is receiving task) but
                    # not the sname of parent, which has already been determined (loop or not)
                    # as it is a different partition/group from this one so already processed.
                    if not current_partition_isLoop:
                        position_in_list_of_parent_frontier_tuples = len(list_of_parent_frontier_tuples)-1
                        shared_frontier_parent_partition_patch_tuple = (task_name_of_parent_partition,position_in_list_of_parent_frontier_tuples)
                        shared_frontier_parent_partition_patch_tuple_list.append(shared_frontier_parent_partition_patch_tuple)

# rhc : ******* Group
                #if True: # 
                if use_shared_partitions_groups:
                    # See the comments immed. above for partition case.
                    # shared groups frontier code:
                    task_name_of_parent_group = group_names[parent_index_in_groups_list]
                    #task_name_of_parent = "PR" + str(parent_partition_number) + "_" + str(parent_group_number)
                    shared_frontier_parent_tuple = (current_partition_number,num_frontier_groups,child_index_in_current_group,current_group_name,parent_group_parent_index)
                    list_of_parent_frontier_tuples = BFS_Shared.shared_groups_frontier_parents_map.get(task_name_of_parent_group)
                    if list_of_parent_frontier_tuples is None:
                        list_of_parent_frontier_tuples = []
                    logger.trace("groupOGPOGPOGPOGPOGPOGPOGP add shared tuple to parent group: ")
                    for n in parent_group:
                        logger.trace(str(n))
                    logger.trace("task_name_of_parent_group: " + task_name_of_parent_group
                        + ", shared_frontier_parent_tuple: " + str(shared_frontier_parent_tuple))
                    logger.trace("list_of_parent_frontier_tuples before appending tuple: " 
                        + str(list_of_parent_frontier_tuples))
                    list_of_parent_frontier_tuples.append(shared_frontier_parent_tuple)
                    BFS_Shared.shared_groups_frontier_parents_map[task_name_of_parent_group] = list_of_parent_frontier_tuples
                    logger.trace("New BFS_Shared.shared_groups_frontier_parents_map:")
                    for (k,v) in BFS_Shared.shared_groups_frontier_parents_map.items():
                        logger.trace(str(k) + ": " + str(v))

                    # The current partition/group name in [3] may not have an "L" but if we later 
                    # find a loop we'll need to append an "L" to this name in the frontier tuple
                    # so we need to patch this name (of this task, which is receiving task) but
                    # not the sname of parent, which has already been determined (loop or not)
                    # as it is a different partition/group from this one so already processed.
                    if not current_group_isLoop:
                        position_in_list_of_parent_frontier_tuples = len(list_of_parent_frontier_tuples)-1
                        shared_frontier_parent_group_patch_tuple = (task_name_of_parent_group,position_in_list_of_parent_frontier_tuples)
                        shared_frontier_parent_groups_patch_tuple_list.append(shared_frontier_parent_group_patch_tuple)

                # generate dependency in DAG
                #
                # Need to use L-based names. The recever is the name of the 
                # current partition/group. The sender's name is the name
                # assigned when the dfs_parent() for that partition/group completed.
                #sending_partition = "PR"+str(parent_partition_number)+"_1"
                # parent_partition_numbers start with 1, e.g. the "PR1" in "PR1_1"
                # but the partition_names are a list with the first name at position 0.

                # Partition_senders[sending_partition] is a list of partitions
                # that sending_partition sends pagerank values to, i.e., these
                # are the partitions that receive from sending_partition.
                # We add a partition name N to Partition_senders[sending_partition]
                # when we find that the parent node of some
                # node C in N was in the previous partition P. Then P was
                # the sending partition and N is a partition that receives
                # from P. 
# rhc : ******* Partition
                
                sending_partition = partition_names[parent_partition_number-1]
                #receiving_partition = "PR"+str(current_partition_number)+"_1"
                receiving_partition = current_partition_name
                # sender set is the partitions that receive from the sender
                sender_set = BFS_generate_DAG_info.Partition_senders.get(sending_partition)
                if sender_set is None:
                    BFS_generate_DAG_info.Partition_senders[sending_partition] = set()
                # add the receiving_partition to the sender set of the sending partition
                BFS_generate_DAG_info.Partition_senders[sending_partition].add(receiving_partition)
                # receiver set is the partitions that send to the receiver
                receiver_set = BFS_generate_DAG_info.Partition_receivers.get(receiving_partition)
                if receiver_set is None:
                    BFS_generate_DAG_info.Partition_receivers[receiving_partition] = set()
                # add the sending_partition to the receievr set of the receiving_partition
                BFS_generate_DAG_info.Partition_receivers[receiving_partition].add(sending_partition)
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

# rhc : ******* Group
                # generate dependency in DAG
                #sending_group = "PR"+str(parent_partition_number)+"_"+str(parent_group_number)
                # index in groups list is the actual index, sarting with index 0
                sending_group = group_names[parent_index_in_groups_list]
                #receiving_group = "PR"+str(current_partition_number)+"_"+str(num_frontier_groups)
                receiving_group = current_group_name
                # sender set is the groups that receive from the sender
                sender_set = BFS_generate_DAG_info.Group_senders.get(sending_group)
                if sender_set is None:
                    BFS_generate_DAG_info.Group_senders[sending_group] = set()
                BFS_generate_DAG_info.Group_senders[sending_group].add(receiving_group)
                # receiver set is the groups that send to the receiver
                receiver_set = BFS_generate_DAG_info.Group_receivers.get(receiving_group)
                if receiver_set is None:
                    BFS_generate_DAG_info.Group_receivers[receiving_group] = set()
                # add the sending_partition to the receievr set of the receiving_partition
                BFS_generate_DAG_info.Group_receivers[receiving_group].add(sending_group)
                # see the comment above for partitions
                if not current_group_isLoop:
                    sender_receiver_group_patch_tuple = (parent_index_in_groups_list,receiving_group)
                    sender_receiver_group_patch_tuple_list.append(sender_receiver_group_patch_tuple)
 
        else:
            # Note: assertion abobe before if should have failed 
            logger.error("[Error]: dfs_parent: partition_group_tuple" 
                + " of parent is None should be unreachable since this is after calling "
                + " dfs_parent() on the parent and dfs_parent puts a tuple for parent in the map.")
            logging.shutdown()
            os._exit(0)

    if CHECK_UNVISITED_CHILDREN:
        # process children after parent traversal
        dfs_parent_post_parent_traversal(node, visited,
            list_of_unvisited_children, check_list_of_unvisited_chldren_after_visiting_parents)
    else:
        # add noed to the BFS_queue; we will pop it from the queue and visit its
        # unvisited children. For each child we call dfs_parent(child) to vist the 
        # unvisited ancestors of the child using dfs_parent(child). The child and 
        # its unvisted ancestors form a group of a partition.
        BFS_queue.append(node.ID)
        #queue.append(-1)
        if DEBUG_ON:
            print_val = "queue after add " + str(node.ID) + ": "
            for x in BFS_queue:
                #logger.trace(x.ID, end=" ")
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
        # Note: the parennt nodes of node were added to a partition (the previous partition
        # or the current partition) before node or any of node's children. We visit 
        # unvisited ancestors of node in dfs_parent(node) and they were added to the current 
        # partition before we added node to the current partition. (Already visited
        # ancestors of node are all in the previous partition.)

        # This should not be false, i.e., we cannot dfs_parent(node) when node
        # has already been visited and put in a partition.

        #assserting if condiion is True
        try:
            msg = "[Error]: dfs_parent: when adding node to partition" \
                + " its partition_number is not -1."
            assert node.partition_number == -1 , msg
        except AssertionError:
            logger.exception("[Error]: assertion failed")
            if exit_program_on_exception:
                logging.shutdown()
                os._exit(0)

        if node.partition_number == -1:
            logger.trace ("dfs_parent add " + str(node.ID) + " to partition")
            node.partition_number = current_partition_number
            logger.trace("set " + str(node.ID) + " partition number to " + str(node.partition_number))

#rhc: Todo: Can we do this as part of for each parent loop? instead of looping again?
# Note: parent remaps to different index depending on partiton or group!!
# Note: node can have multiple parents in previous partition or group therein or in
#   different group in this partition. So need to add all shadow nodes before
#   we add node to partition.
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

            #partition_node.parents = node.parents
# rhc : ******* Partition    
            partition_node.num_children = len(node.children)
            # these are the default values so we do not need these assignments 
            partition_node.pagerank = 0.0
            partition_node.isShadowNode = False
            partition_node.frontier_parents = []

# rhc : ******* Group
            group_node.num_children = len(node.children)
            # these are the default values so we do not need these assignments 
            group_node.pagerank = 0.0
            group_node.isShadowNode = False
            group_node.frontier_parents = []

# rhc : ******* Partition
            current_partition.append(partition_node)
# rhc : ******* Group
            current_group.append(group_node)

            # partition_node.ID and group_node.ID are the same
# rhc : ******* Partition
            nodeIndex_to_partitionIndex_map[partition_node.ID] = len(current_partition)-1
# rhc : ******* Group
            nodeIndex_to_groupIndex_map[partition_node.ID] = len(current_group)-1
            
            # Note: if node's parent is in different partition then we'll add a 
            # shadow_node to the partition and the group in a position right before 
            # node in partition and group (since if the parent is in a different 
            # partition it is laso in a different group.)
            # But if a node's parent is in a different group but same partition then 
            # we only add a shadow node to the group in a position right before node in the group. 

# rhc : ******* Partition
            # information for this partition node
            # There are n partitions, this node is in partition partition_number

            # We set partition_number in tuple at beginning of dfs_parent to current_partition_number
            # and likewise for the group number, no need to set it here, but can do 
            # assserts on these values.
            # Suggested assert
            partition_number = current_partition_number
            # In this partition, this node is at position partition_index,
            # where we just added the node to current_partition
            partition_index = len(current_partition)-1
# rhc : ******* Group
            # Likewise for groups
            group_number = current_group_number
            group_index = len(current_group)-1

            # Save node's partition and group information in the global map.
            # We need postion in groups (frontier_groups-1) when we add a frontier tuple
            # to a group. That is, if say 13 is a child of 8, we need to add a frontier_tuple
            # to 8.  group_number is the number of the group in a partition, e.g., 8 might be
            # in group 2 in partition 2, but any partition can have a group 2, so we cannot 
            # use the group number 2 to access this group 2 of 8 in the groups list. We need
            # to know group 2's position in the groups list, which could be anything.
            # So for 8 we will save it's partition number and its partition index, where
            # partition number i is always in position i-1 of the partitions list. We also
            # save 8's group number and group index, and 8's position in groups so we can 
            # get 8's group from the groups list when we need it.
            parent_index_in_groups_list = frontier_groups_sum-1
            pg_tuple = (partition_number,partition_index,group_number,group_index,parent_index_in_groups_list)
            nodeIndex_to_partition_partitionIndex_group_groupIndex_map[partition_node.ID] = pg_tuple
            logger.trace("HHHHHHHHHHHHHHHH dfs_parent: pg_tuple(pnum,pindx,gnum,gindx,posingroupslist) generate for " + str(partition_node.ID)
                + str(pg_tuple))
        else:
            # Note: assertion shoudl have failed
            #logger.trace ("dfs_parent do not add " + str(node.ID) + " to partition "
            #    + current_partition_number + " since it is already in partition " 
            #   + node.partition_number)
            logger.error("[Error]: dfs_parent: when adding node to partition"
                + " its partition_number is not -1.")
            logging.shutdown()
            os._exit(0)

# process children after parent traversal
def dfs_parent_post_parent_traversal(node, visited, list_of_unvisited_children, check_list_of_unvisited_chldren_after_visiting_parents):
    pass
# this is in a seperate file

# Called in BFS.py, as in:
    #for i in range(1,num_nodes+1):
    #    if i not in visited:
    #        logger.trace("*************BFS Driver call BFS for node[" + str(i) + "]")
    #    bfs(visited, nodes[i])    # function calling
# Builds a DAG using a combination of bfs, whcih is used to visit the children
# of a Node and dfs, which is used to visit the parents of a Node. For a given
# Node N in the graph, N's parents are eiher in te same partition as N or
# are in the previous partition that we collected.
def bfs(visited, node): 
    logger.trace ("bfs mark " + str(node.ID) + " as visited and add to bfs queue")
    #rhc: add to visited is done in dfs_parent
    #visited.append(node.ID)
    # dfs_parent will add a node to the current partition and also add 
    # its unvisited parent nodes. Some parent nodes may have been added to the 
    # previous partition and marked visited then.
    global current_partition
    
    #various counters
    global dfs_parent_start_partition_size
    global dfs_parent_loop_nodes_added_start
    global dfs_parent_start_frontier_size
    global dfs_parent_end_partition_size
    global dfs_parent_loop_nodes_added_end
    global dfs_parent_end_frontier_size
    #rhc shared
    global start_num_shadow_nodes_for_partitions
    global end_num_shadow_nodes_for_partitions
    global start_num_shadow_nodes_for_groups
    global end_num_shadow_nodes_for_groups
    global num_shadow_nodes_added_to_partitions
    global num_shadow_nodes_added_to_groups

    # this thread will be created during incemental DAG generation
    # to call the run() method of DAG_executor_driver to start 
    # DAG execution.
    global invoker_thread_for_DAG_executor_driver
    global num_incremental_DAGs_generated
    # constant: If this value is 2 we publish every second incremental DAG generated
    global incremental_DAG_deposit_interval
    # when this value equals the number of nodes in the graph, all nodes
    # are in a partition and the incremental DAG is complete, meaning 
    # incremental DAG generation stops
    global num_nodes_in_partitions

#rhc: incremental groups
    # for each partition, we create a list of the groups in the 
    # current partition and add this list to the list groups_of_partitions
    global groups_of_partitions
    global groups_of_current_partition

#rhc:
    # These are per dfs_parent() stats not per partition.
    # These lengths should be 0 at the start
    dfs_parent_start_partition_size = len(current_partition)
    dfs_parent_start_frontier_size = len(frontier)

    # Using these when we TRACK_PARTITION_LOOPS, i.e., generate information
    # about the loops we find. Only used this for debugging.
    global loop_nodes_added
    dfs_parent_loop_nodes_added_start = loop_nodes_added

#rhc shared
    if use_shared_partitions_groups or enable_runtime_task_clustering:
        # we keep a count of the total number of shadow nodes added to a group
        # or partition. Then we track the start and end values of this counter
        # at the start of dfs_parent() so we can identify the number end-start
        # of shadow nodes added by dfs_parent. There is one shadow node in 
        # a group or partition for each node whose parent nodes is in a different
        # group or partition. Shadow nodes get there paerank values from the 
        # paent node that they represent, i.e., a shadow node "shadows" the
        # parent node. After the computation of the parent node's pagerank
        # value (by some executor or worker) the pagerank value will be "sent"
        # and assigned to the shadow node. The shadow node value is used as a 
        # proxy for the parent's value for the child of the parent that is 
        # associated with the shadow node. If the child is in position i of the
        # partition/group, then the shadow node is in position i-1.
        #
        # save start value before root call to dfs_parents(). At end 
        # compute end - start.
        start_num_shadow_nodes_for_partitions = num_shadow_nodes_added_to_partitions
        start_num_shadow_nodes_for_groups = num_shadow_nodes_added_to_groups

    #dfs_p(visited, graph, node)
    #dfs_p_new(visited, graph, node)

#rhc: 
    # start with -1 in the bfs queue; after call to dfs_parent, which will 
    # collect a node and its unvisited ancestors (parents, parents of parents, etc),
    # we will pop the -1 fron the queue, marking the end the current partition.
    # So: we were passed node, a node in the graph that is the first node
    # in a connected component (which could also be the first node in the graph
    # to be processed). We put -1 in the bfs queue and collect the unvisited
    # ancestors of node. (some ancestors of node may have been collected/visted
    # on a previous call to dfs_paent() - all of these visited ancestors
    # are n the same partition). These collected unvisited ancestos of node
    # will be added to the bs queue, e.g., for the whiteboard DAG, the bfs
    # queue will have 5, 17 and 1 added so that after dfs_parent the bfs
    # queue contains -1 5 17 1. This means the current partition will 
    # contain 5, 17, and 1. We then pop the -1, which means we have colleccted
    # all of the nodes 5, 17, 1 in the current partition. We will then 
    # add -1 to the (end of the) bfs quueue getting 5, 17, 1 -1. Next
    # we call dfs_parent() on the chldren of these nodes to get the 
    # second partition. That is, the second partition contains all of the
    # child nodes of the first partition and all of the unvisited ancestor
    # nodes of these child nodes. The only child of 5 is 16.The only
    # child of 17 is 19. The only child of 1 is 12. So we will pop 5
    # from the bfs_queue and call dfs_parent(5) to collect a group of
    # nodes which is the child 16 of 5 and all the unvisited ancestor
    # nodes of 16 (16's parents, the parents of these parents, etc)
    # Likewise, we pop 17 and call dfs_parent(17) and pop 1 and call dfs_parent(1).
    # dfs_parent(5) will collect 2, 10, 16, where 16 is the only
    # child of 5. These nodes are group 1 of this second partition.
    # Group 2 nodes are 20, 8, 11, 3, and 19, where 19 is the only child
    # of 17. Thse nodes are in a loop (20 is parent of 8 is parent 
    # of 11, etc) and 20 is a child of 19. which is indicated by the 
    # name of this group "PR2_2L", as in partition 2 group 2 is a loop 'L'
    #  Group 3 is 4, 6, 14, and 12, where 12 is the only child of 1.
    # All of these nodes i partition will be added to the bfs_quueue,
    # which will contain -1 2 10 16 .... 4 6 14 12. The -1 indicates
    # the current_partition, which is partition 2, is complete. So
    # partition 3 wil lbe generated by using dfs_parent to explore
    # the children and their ancestors of the nodes in the bsf_queue.
    # The partitions aer getting bigger and will have more groups.
    #
    # The main idea is that a node will have all of its parent nodes
    # either in the same parttion (locally) are in a single different
    # partition (perhaps remotely). Recall that pagerank for node 
    # N requires the pagerank values for the parent nodes of N. This
    # bfs-dfs scheme tries to reduce the communication of parent 
    # values between compute nodes by keeping parents local to the
    # child or minimizing the communication between the compute 
    # nodes containing the parent values and the compute nodes 
    # containing the child nodes of these parents. So we try to
    # "cluster" a child with as many of its parents as possible.
    BFS_queue.append(-1)

    # SCC 3

    global num_frontier_groups
    # number of groups in the current partition, e.g., "PR2_2" 
    # where 2 is the paritition numbrr and num_frontier_groups
    # is 2 meaning the 2nd group in this partition. Reset to 0 
    # when we finish a partition.
    num_frontier_groups = 1
    global frontier_groups_sum
    # frontier_groups_sum inited to 0 so this makes it 1. Note that
    # if we call bfs() again then frontier_groups_sum is not reset
    # so it keeps growing
    frontier_groups_sum += 1
    dfs_parent(visited, node)

    # Note: No shadow_nodes can be added during first call to dfs_parent
    # as the generated group has no previously visited parents; it is the first group.
    # SCC 4

    # Note: -1 is at the front of the queue so we will pop the -1 indicating
    # the end of the current partition. The current partition contains
    # the chidren of the previous partition and the unvisited ancestors
    # of these parents.

    # current group being collectd
    global current_group
    # list of collected groups
    global groups
    
# rhc : ******* Group
    # in dfs_parent(), we add a node to either list partitions or list
    # groups. Note that the nodes of partition 1 ere also the nodes of
    # group 1. We collect the first group here. We collect the first
    # partition (same nodes) below.
    groups.append(current_group)

    # this first group ends here after first dfs_parent
    global nodeIndex_to_groupIndex_maps
    global nodeIndex_to_groupIndex_map
    nodeIndex_to_groupIndex_maps.append(nodeIndex_to_groupIndex_map)
    nodeIndex_to_groupIndex_map = {}
# rhc : ******* end Group

    # these start with 1 not 0. 
    global current_partition_number
    global current_group_number

# rhc : ******* Group
    # First partition/group is "PR1_1".
    # Next group is first group of partition 2: "PR2_1", etc.
    group_name = "PR" + str(current_partition_number) + "_" + str(current_group_number)

    # This is set in dfs_parent if (some or all of) the nodes in this 
    # group form a loop.
    global current_group_isLoop
    if current_group_isLoop:
        # These are the names of the groups that have a loop. In the 
        # DAG, we will append an 'L' to the name. Not used any more since we 
        # use partition/group names that end with 'L" so we can tell by the
        # name if the partition/group has a loop. Recal that pagernk for 
        # nodes in a loop requires many iterations, but for node not in 
        # a loop only one iteration.
        # (Note: we used to change the names of the partitions/groups at the
        # end to reflect tht we found a loop. We used Group_loops for this.
        # This collection of partitions/groups with loops might come in handy again.
        group_name = group_name + "L"
        Group_loops.add(group_name)

    current_group_isLoop = False
    # Note: not incrementing current_group_number. This root group is the 
    # only group in this partition. We consider it to be group 1, 
    # which is the initial value of current_group_number. We do not increment
    # it since we are done with the groups in the first partition, so 
    # current_group_number will be 1 when we find the first group of the 
    # next partition and use current_group_number+1=2 as the group number.
    group_names.append(group_name)

#rhc: incremental groups
    if compute_pagerank and (use_incremental_DAG_generation or use_multithreaded_BFS):
        # For incremental DAG generation, we track the groups in the current
        # partition. We will need to iterate through these groups.
        groups_of_current_partition.append(group_name)
        logger.info("BFS: add " + group_name + " to groups_of_current_partition: "
            + str(groups_of_current_partition))
    

    # The first group collected by call to bfs() is a leaf node of the DAG.
    # There may be many calls to bfs(). Below, we will collect the first
    # partition. Set is_leaf_node to True so we know it is the first partition
    # collected on this call to bfs(). In general, a leaf group/partition
    # is the first group/partition of a connected component.
    BFS_generate_DAG_info.leaf_tasks_of_groups.add(group_name)
    BFS_generate_DAG_info.leaf_tasks_of_groups_incremental.append(group_name)
    is_leaf_node = True

#rhc: clustering
    # Note: need the group name here.
    # Need to know the number of shadow nodes when clustering in order
    # to compute the number of non-shadow nodes (nodies-shadow_nodes = non-shadow nodes)
    #if use_shared_partitions_groups:
    if use_shared_partitions_groups or enable_runtime_task_clustering:
        # we are using worker processes/threads and we are putting all the 
        # groups in one shared array in an order that minimizes cache 
        # misses during the pagerank computation.
#rhc shared
        # compute number of shadow nodes added to first group
        end_num_shadow_nodes_for_groups = num_shadow_nodes_added_to_groups
        change_in_shadow_nodes_for_groups = end_num_shadow_nodes_for_groups - start_num_shadow_nodes_for_groups
        try:
            msg = "[Error]: bfs: first group/partition has shadow nodes (non-zero change)."
            assert change_in_shadow_nodes_for_groups == 0 , msg
        except AssertionError:
            logger.exception("[Error]: assertion failed")
            if exit_program_on_exception:
                logging.shutdown()
                os._exit(0)
        # assertOld:
        #if not change_in_shadow_nodes_for_groups == 0:
        #    logger.error("[Error]: bfs: first group/partition has shadow nodes (non-zero change).")
        #    logging.shutdown()
        #    os._exit(0)

        groups_num_shadow_nodes_list.append(change_in_shadow_nodes_for_groups)
        size_of_group = len(current_group) - change_in_shadow_nodes_for_groups
        BFS_generate_DAG_info.groups_num_shadow_nodes_map[group_name] = size_of_group
        # start it here before next call to dfs_parent but note that we 
        # may not call dfs_parent() since a node popped from bfs queue
        # may not have any (unvisited) children in which case we will generate a final partition/group
        # and we need to have called start here.
        # Note that num_shadow_nodes_added_to_groups is a running total 
        # that is not reset. We get start and end values so we can get the 
        # number of shadow nodes in some interval.
        start_num_shadow_nodes_for_groups = num_shadow_nodes_added_to_groups
        #
        # Note: This first group is also the first partition. We process the first partition 
        # below and we add the change in the number of shadow nodes to partitions_num_shadow_nodes_list.

    current_group = []
    
# rhc : ******* end Group

    # tracking the groups with loops that we found if TRACK_PARTITION_LOOPS
    dfs_parent_loop_nodes_added_end = loop_nodes_added

    # These are tracked per dfs_parent() call, so we compute them here and 
    # after the calls to dfs_parent() below.
    #
    # First group is also the first partition
# rhc : ******* Partition
    dfs_parent_end_partition_size = len(current_partition)
    dfs_parent_change_in_partition_size = (dfs_parent_end_partition_size - dfs_parent_start_partition_size) - (
        dfs_parent_loop_nodes_added_end - dfs_parent_loop_nodes_added_start)
    dfs_parent_changes_in_partiton_size.append(dfs_parent_change_in_partition_size)
    logger.trace("dfs_parent(root)_change_in_partition_size: " + str(dfs_parent_change_in_partition_size))
# rhc : ******* end Partition

    dfs_parent_end_frontier_size = len(frontier)
    dfs_parent_change_in_frontier_size = (dfs_parent_end_frontier_size - dfs_parent_start_frontier_size) - (
        dfs_parent_loop_nodes_added_end - dfs_parent_loop_nodes_added_start)
    dfs_parent_changes_in_frontier_size.append(dfs_parent_change_in_frontier_size)
    logger.trace("dfs_parent(root)_change_in_frontier_size: " + str(dfs_parent_change_in_frontier_size))

    # queue.append(node) and frontier.append(node) done optionally in dfs_parent
#rhc
    end_of_current_frontier = False
    while BFS_queue:          # Creating loop to visit each node in BFS_queue
        # queue of int IDs not Node objects. Node for int ID is in Nodes[ID]
        ID = BFS_queue.pop(0) 
        logger.trace("bfs pop node " + str(ID) + " from queue") 
        if ID == -1:
            # end of partition is end of frontier
            end_of_current_frontier = True
            # got a -1 so pop again to get an ID, if the bfs queue is not empty 
            if BFS_queue:
                ID = BFS_queue.pop(0)
                try:
                    msg = "[Error]: bfs: immediately after popping a -1" + " we pop a second -1."
                    assert not (ID == -1) , msg
                except AssertionError:
                    logger.exception("[Error]: assertion failed")
                    if exit_program_on_exception:
                        logging.shutdown()
                        os._exit(0)
                #assertOld: 
                #if ID == -1:
                #    logger.error("[Error]: bfs: immediately after popping a -1"
                #        + " we pop a second -1.")
                #    logging.shutdown()
                #    os._exit(0)

                logger.trace("bfs after pop -1; pop node " + str(ID) + " from queue") 
                # add -1 to the end of bfs_queue, which means that all the IDs in the 
                # queue are the Node IDs of the nodes in the next partition (and its groups)
                # Note: we add the -1 here and not in dfs_parent. So we add a -1
                # after *all* of the nodes in the bfs_queue of a collected partition.
                # For example, from the above comment, the first group/partition is
                # 5, 17, 1 so we get -1 5, 17, 1 on the bfs_queue, pop the -1, and 
                # push it again to get 5, 17, 1 -1.
                BFS_queue.append(-1)
                # SCC 5
            else:
                # no more nodes to process, break main bfs loop.
                break

        node = nodes[ID]
        # The frontier is nodes in current partition that can have children
        # in the next partition. Currently, the current_partition and the
        # current frontier are the same.
        # So we can see the frontier costs that do not correspnd to when 
        # partitions were created, i.e., was there a better frontier for partition?
        all_frontier_costs.append("pop-"+str(node.ID) + ":" + str(len(frontier)))

        # Consider the case where the first group/partition is 5, 17, 1 and
        # 5 has a single child 6, 6 has a single parent 5, and 6 has no
        # children: 5--child-->6. Then we call 6 a "singleton". One option 
        # when we visit 5 and add it to the first partition is to detect unvisited
        # singleton's like 6 and put the singleton in the same partition as
        # its parent 5. This eliminates the communication that would otherwise
        # occur when the partition with 5 had to communicate its parent value
        # to the partition with 6. Then 5 has a singleton child
        # 6 then the dfs_parent(5) will see that 6 is unvisited. If singleton checking
        # is on then singleton 6 will be identified, 6 will be marked visited, and
        # neither 5 nor 6 will be enqueued in dfs_queue since they have no 
        # unvisited children, and both 5 and 6 will be added to the partition but not 
        # the frontier. If singleton checking is off then 5
        # will be enqueued. When 5 is popped off the queue, dfs_parent(6) will be
        # called and it will see that 6 has no children before or after 6's parent 
        # traversal (5 is already visited) so 6 will be marked visited and 6 will 
        # not be enqueued but will be added to the next partition (but not the 
        # frontier.
        # Note: handling singletons here, instead of in dfs_parent, is more efficen
        # nodes are not singletons. So if the partiton has 5 in the frontier with singleton 
        # child 6, we can reduce the frontier by pruning 5 - pop 5 from the queue,
        # mark 5 as visited, remove 5 from the frontier, mark its singleton child 6 
        # visited, and add 6 to the partition but not the queue (6 has no children)

        if end_of_current_frontier:
            logger.trace("BFS: end_of_current_frontier")
            end_of_current_frontier = False

# rhc : ******* Partition
            # end of partition
            if len(current_partition) > 0:
            #if len(current_partition) >= num_nodes/5:
                logger.trace("BFS: end of current partition.")
                # save the current partition in list of partitions.
                # This does not require a deepcopy.
                partitions.append(current_partition.copy())
#rhc incremental:   
                # this includes regulat nodes and shadow nodes
                num_nodes_in_partitions += len(current_partition)
                # reset current_partition

                # For example: "PR2_1", "PR3_1" for partitions 2 and 3.
                # The 1 is the group number. When we are collcting partitions
                # instead of groups,the group number is always 1.
                partition_name = "PR" + str(current_partition_number) + "_1"

                global current_partition_isLoop
                # if the partition is a loop then add an "L" to its name
                if current_partition_isLoop:
                    # These are the names of the partitions that have a loop. In the 
                    # DAG, we will append an 'L' to the name. 
                    partition_name = partition_name + "L"
                    # Not using this anymore, but could be usefuk later.
                    Partition_loops.add(partition_name)

                # Note: partition_name is collected below

#rhc: clustering
                 # Note: need the partition name here.
                # Need to know the number of shadow nodes when clustering in order
                # to compute the number of non-shadow nodes (nodies-shadow_nodes = non-shadow nodes)
                #if use_shared_partitions_groups:
                if use_shared_partitions_groups or enable_runtime_task_clustering:
                    # tracking the number of shadow nodes added to current partition.
                    # num_shadow_nodes_added_to_partitions is grand total of shadow
                    # nodes added to the partitions. We got the start value of this 
                    # and now we get the end value of this so that end - start is the 
                    # number of shadow nodes added to the current partition.
                    # For clustering, we need to know the number of nodes in the 
                    # partition/group so we need to subtract the number of shadow 
                    # nodes from the number of nodes in the partition/group.
                    #
                    # For clustering, we need to know the number of nodes in the 
                    # partition/group so we need to subtract the number of shadow 
                    # nodes from the number of nodes in the partition/group.
#rhc shared
                    end_num_shadow_nodes_for_partitions = num_shadow_nodes_added_to_partitions
                    change_in_shadow_nodes_for_partitions = end_num_shadow_nodes_for_partitions - start_num_shadow_nodes_for_partitions
                    partitions_num_shadow_nodes_list.append(change_in_shadow_nodes_for_partitions)
                    size_of_partition = len(current_partition) - change_in_shadow_nodes_for_partitions
                    BFS_generate_DAG_info.partitions_num_shadow_nodes_map[partition_name] = size_of_partition
                    start_num_shadow_nodes_for_partitions = num_shadow_nodes_added_to_partitions

                current_partition = []

#rhc: incremental groups
                if compute_pagerank and (use_incremental_DAG_generation or use_multithreaded_BFS):
                    # For incremental DAG generation, we need to know the 
                    # groups that each partition contains. That is, when we process
                    # the groups of the current_partion, which is being added to the 
                    # end of the incremental DAG, we need to know the groups of the 
                    # previous partition an the groups of the previous previous partition.
                    groups_of_partitions.append(copy.copy(groups_of_current_partition))

                    logger.trace("BFS: for partition " + partition_name + " collect groups_of_current_partition: "
                        + str(groups_of_current_partition)
                        + ", groups_of_partitions: " + str(groups_of_partitions)) 

                # The first partition collected by any call to BFS() is a leaf node of the DAG.
                # There may be many calls to BFS(). We set is_leaf_node = True at the
                # start of bfs() above.
                if is_leaf_node:
                    BFS_generate_DAG_info.leaf_tasks_of_partitions.add(partition_name)
                    BFS_generate_DAG_info.leaf_tasks_of_partitions_incremental.append(partition_name)
                    is_leaf_node = False

                # Patch the partition name of the frontier_parent tuples. 
                if current_partition_isLoop:
                    # When the tuples in frontier_parent_partition_patch_tuple_list were created,
                    # it is possible that no loop had been detectd in the partition so we used a 
                    # partition name that did not end in 'L'. Then at some point a loop was detected 
                    # so we need to change the partition name in the tuple so that it ends with 'L'. 
                    # If no loop is detectd, then current_partition_isLoop will be false and no changes
                    # need to be made.
                    # For example, in the whiteboard example, 16's parent is 5
                    # and when we discover that 16 is considered to be in partition "PR2_1"
                    # but then we discver a loop when we see 17's child is 19 and we 
                    # do dfs_parent 19. Now the partition name will actually be "PR2_1L"
                    # but we've already used "PR2_1" for the partition of 16 when we 
                    # used "PR2_1" for the tuple in 5's partition "PR1_1"; this tuple
                    # tells the executor of "PR1_1" to send 5's pagerank value to 
                    # the executor of "PR2_1" since 5's child 6 needs this parent value.
                    # The target for this should actually be "PR2_1L"
                    logger.trace("XXXXXXXXXXX BFS: patch partition frontier_parent tuples: ")
                    # Note: frontier_parent_partition_patch_tuple was created as:
                    #   frontier_parent_partition_patch_tuple = 
                    #       (parent_partition_number,parent_partition_parent_index,position_in_frontier_parents_partition_list,
                    #           partition_name)
                    for frontier_parent_partition_patch_tuple in frontier_parent_partition_patch_tuple_list:
                        # These values were used to create the tuples in dfs_parent()
                        parent_partition_number = frontier_parent_partition_patch_tuple[0]
                        parent_partition_parent_index = frontier_parent_partition_patch_tuple[1]
                        position_in_frontier_parents_partition_list = frontier_parent_partition_patch_tuple[2]
                        # This information allows us to get the tuple to patch. Each partition 
                        # has a list of frontier parents, which are nodes tha have chidren
                        # in a different partition (e.g., node 5 in partiton "PR1_1" has a child
                        # node 6 in "PR2_1" so 5 is a frontier node, i.e., it is "on the 
                        # frontier between partitions." We get the tuples for the frontier parents and 
                        # then get the tuple that needs patched.

                        # get the tuple that has the wrong name
                        # frontier parent are in previous partition
                        parent_partition = partitions[parent_partition_number-1]
                        # get the tuples for a node, which is a parent node of one or more
                        # children in a different partition. For example, parent 5 in "PR1_1"
                        # has a child in "PR2_1L" but the name used in the tuple was "PR2_1"
                        # so we patch this by changing "PR2_1" to "PR2_1L"
                        frontier_parents = parent_partition[parent_partition_parent_index].frontier_parents
                        frontier_parent_partition_tuple_to_patch = frontier_parents[position_in_frontier_parents_partition_list]
                        logger.trace("BFS: patching partition frontier_tuple name "
                            + frontier_parent_partition_tuple_to_patch[3] + " to " + partition_name)
                        # create a new tuple that reuses the first 3 fields and changes the name in the 
                        # last field to the name of the current partition, e.g. "PR2_1" is patched to "PR2_1L"
                        first_field = frontier_parent_partition_tuple_to_patch[0]
                        second_field = frontier_parent_partition_tuple_to_patch[1]
                        third_field = frontier_parent_partition_tuple_to_patch[2]
                        new_frontier_parent_partition_tuple = (first_field,second_field,third_field,partition_name)
                        # delete the old tuple
                        del frontier_parents[position_in_frontier_parents_partition_list]
                        # append the new tuple, order of the tuples may change but order is not important
                        frontier_parents.append(new_frontier_parent_partition_tuple)
                        logger.trace("BFS:  new frontier_parents: " + str(frontier_parents))

                frontier_parent_partition_patch_tuple_list.clear()

                # Patch the partition name of the shared_frontier_parent_partition tuples. 
                # When we use shared partitions/groups, all of the partitions/groups
                # are put into one shared array.
                # The logic for patching is the same as above.
                if use_shared_partitions_groups:
                    # Given:
                    # shared_frontier_parent_partition_patch_tuple = (task_name_of_parent,position_in_list_of_parent_frontier_tuples)
                    if current_partition_isLoop:
                        logger.trace("X-X-X-X-X-X-X BFS: patch shared partition frontier_parent tuples: ")
                        for shared_frontier_parent_partition_patch_tuple in shared_frontier_parent_partition_patch_tuple_list:
                            # These values were used to create the tuples in dfs_parent()
                            task_name_of_parent = shared_frontier_parent_partition_patch_tuple[0]
                            position_of_tuple_in_list_of_parent_frontier_tuples = shared_frontier_parent_partition_patch_tuple[1]

                            list_of_parent_frontier_tuples = BFS_Shared.shared_partition_frontier_parents_map.get(task_name_of_parent)
                            frontier_parent_partition_tuple_to_patch = list_of_parent_frontier_tuples[position_of_tuple_in_list_of_parent_frontier_tuples]
                            logger.trace("BFS: patching shared partition frontier_tuple name "
                            + frontier_parent_partition_tuple_to_patch[3] + " to " + partition_name)
 
                            # Given the tuple was created using:
                            # shared_frontier_parent_tuple = 
                            #     (current_partition_number,num_frontier_groups,child_index_in_current_partition,current_partition_name,parent_partition_parent_index)
                            # we aer patching the partition name in fourth field
                            first_field = frontier_parent_partition_tuple_to_patch[0]
                            second_field = frontier_parent_partition_tuple_to_patch[1]
                            third_field = frontier_parent_partition_tuple_to_patch[2]
                            # fourth field will be the patched partition_name
                            # FYI: [3] is the partition name to be patched with the new name ending in "L"
                            # e.g., "PR2_1" --> "PR2_1L". Partition name was appended with "L" above
                            fifth_field = frontier_parent_partition_tuple_to_patch[4]
                            new_frontier_parent_partition_tuple = (first_field,second_field,third_field,partition_name,fifth_field)
                            del list_of_parent_frontier_tuples[position_of_tuple_in_list_of_parent_frontier_tuples]
                            # append the new tuple, order of tuples may change but order is not important
                            list_of_parent_frontier_tuples.append(new_frontier_parent_partition_tuple)
                            logger.trace("BFS:  new shared partition frontier_parent tuples for " + task_name_of_parent + " is " +  str(list_of_parent_frontier_tuples))

                    shared_frontier_parent_partition_patch_tuple_list.clear()

                # patch receiver name. When a partition S has a pagerank value for
                # a parent node that will be sent to a partition R that has the child node,
                # then S-->R is an edge in the DAG. We track These senderd and receievers
                # while we generate partitions and we then use the senders and receivers
                # to build the DAG - partitions/groups are DAG nodes and the edges
                # aer determined by senders and receivers.
                if current_partition_isLoop:
                    # When the tuples in sender_receiver_partition_patch_tuple_list were created,
                    # no loop had been detectd in the partition so we used a partitiom name 
                    # for the receiver name that did not end in 'L'. At some point a loop was detected so we need to
                    # change the receiver name so that it ends with 'L'. If no loop
                    # is detectd, then current_partition_isLoop will be false and no changes
                    # need to be made.
                    logger.trace("XXXXXXXXXXX BFS: patch partition sender/receiver names: ")
                    for sender_receiver_partition_patch_tuple in sender_receiver_partition_patch_tuple_list:
                        # sender_receiver_partition_patch_tuple crated as:
                        #   sender_receiver_partition_patch_tuple = (parent_partition_number,receiving_partition)
                        parent_partition_number = sender_receiver_partition_patch_tuple[0]
                        receiving_partition = sender_receiver_partition_patch_tuple[1]

                        sending_partition = partition_names[parent_partition_number-1]
                        # Partition_senders[sending_partition] is a list of partitions
                        # that sending_partition sends pagerank values to, i.e., these
                        # are the partitions that receive from sending_partition.
                        # When we added a partition name N to Partition_senders[sending_partition]
                        # we were creating partition N and we found that the parent node of some
                        # node C in N was in the previous partition P. Then P was
                        # the sending partition and N is a partition that receives
                        # from P. So Partition_senders[P] contains receiver N. We
                        # need to patch the name N since later we found a loop in 
                        # this partition so its name changed (we added an "L" to the 
                        # name N). We do not patch name P here. If when we processed
                        # P as the current partition and found a loop in P, we will 
                        # then have to patch the name P, but that would have been done
                        # after we complete partition P (as the current parttion)
                        # previously and patched P.
                        #
                        # Patch the receiving_partition name
                        # sender set is the partitions that receive from the sender
                        sender_name_set = BFS_generate_DAG_info.Partition_senders[sending_partition]
                        logger.trace("BFS: patching partition sender_set receiver name "
                            + receiving_partition + " to " + partition_name)
                        # remove old receiving_partition name, without the "L"
                        sender_name_set.remove(receiving_partition)
                        # add new name, which will have the "L"
                        sender_name_set.add(partition_name)
                        logger.trace("BFS: new partition sender_Set: " + str(sender_name_set))

                        logger.trace("BFS: patching Partition_receivers receiver name "
                            + receiving_partition + " to " + partition_name)
                        # Partition_receivers[N] is all the partitions that sent values 
                        # to N. But we have changed the name N by adding an "L", so we 
                        # need to use Partition_receivers[partition_name] instead of 
                        # Partition_receivers[N]. 
                        BFS_generate_DAG_info.Partition_receivers[partition_name] = BFS_generate_DAG_info.Partition_receivers[receiving_partition]
                        del BFS_generate_DAG_info.Partition_receivers[receiving_partition]
                        logger.trace("BFS: new Partition_receivers[partition_name]: " + str(BFS_generate_DAG_info.Partition_receivers[partition_name]))
                
                sender_receiver_partition_patch_tuple_list.clear()

                # reset for nect partition
                current_partition_isLoop = False
                # track all partition names
                partition_names.append(partition_name)


                global patch_parent_mapping_for_partitions
                logger.trace("BFS: partition_nodes to patch: ")
                for parent_tuple in patch_parent_mapping_for_partitions:
                    logger.trace("BFS: parent_tuple: " + str(parent_tuple) + "," )
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
                    # one list for the node in the partition and one list for 
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

                    # assserting if condition is True
                    try:
                        msg = "BFS: global map index of " + str(parent_index) + " is -1"
                        assert partition_index_of_parent != -1 , msg
                    except AssertionError:
                        logger.exception("[Error]: assertion failed")
                        if exit_program_on_exception:
                            logging.shutdown()
                            os._exit(0)
        
                    if partition_index_of_parent != -1:
                        # Suggsted assert group_index is also -1
                        list_of_parents_of_partition_node[i] = partition_index_of_parent
                        #list_of_parents_of_group_node[i] = group_index_of_parent
                        logger.trace("BFS: end of frontier: remapping parent " + str(parent_index)
                            + " of " + str(node_ID) +  " to " + str(partition_index_of_parent) 
                            + " for partition node.")
                            #+ group_index_of_parent + " for group node")
                    else:
                        #Note: assertion should have failed
                        logger.error("BFS: global map index of " + str(parent_index) + " is -1")
                        logging.shutdown()
                        os._exit(0)

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

# rhc : ******* end Partition Group

                global total_loop_nodes_added
                total_loop_nodes_added += loop_nodes_added
                loop_nodes_added = 0
           
                # SCC 6

                # SCC 7

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
                # Generate stats.
                frontier_cost = "pop-"+str(node.ID) + ":" + str(len(frontier))
                frontier_costs.append(frontier_cost)
                frontier.clear()

#rhc incremental     
                # outline: call generate_DAG_info_incremental_partitions  
                # or BFS_generate_DAG_info_incremental_groups to add another
                # partition or the groups in a partition to the incremental DAG,
                # respectively. When the next incremental DAG_infi is generated
                # we call method deposit() to deposit the DAG_info in a "buffer"
                # on which the DAG_executor calls withdraw() to get the next
                # incremental DAG. The DAG_executor and BFS are running concurrently
                # and using the shared buffer to communicate the generated DAG_info objects.
                # Note the partiton 1 and group 1 are the same since partition 1 only
                # has 1 group in it - it is the first partition/group identified
                # in the DAG. Each call to bfs() identifies the first partition/group
                # in a connected component of the DAG. For example, for a graph with 
                # 2 nodes N1 and N2 and no edges, the first call to bfs() identifies 
                # N1 as the first partition/group in the connected component with the 
                # single node N1, and the second call to bfs() identifies N2 as the 
                # first partition/group in the connected component with the single 
                # node N2. For incrmental DAG generation, the DAG_excution_driver
                # will start workers/lambdas that will execute leaf node N1. For 
                # leaf node N2, we will have to take the role of the DAG_executor_driver
                # and esnure the leaf tasks get started. Leaf tasks are not the target
                # of any fanin/fanout so no other task can fanout/fanin a leaf task.
                # Cases:
                # 1. current_partition_number == 1. Added partition/group 1 to the DAG.
                # if there are more partitins to come, then partition 1 is incomplete
                # and cannot be executed unti we get partition 2. In general, if we
                # add partition i (or the groups of partition ) then partition 1 
                # becomes complete and can be executed. Partition 1 is a leaf
                # partition/group; the DG_executor_drivr will ensure it is executed.
                # Other leaf task that are discovered later will not be started by the 
                # DAG_executor_driver; we will start them below as we get them. If
                # partition 1 is the only partition in the DAG then the ADG is now 
                # complete. In this case, we save partiton 1 to a file and write
                # DAG_info to a file. The DAG_executor_driver will read this DAG_info
                # file and a worker or a lambda will execute partition/group 1.
                # (The first partition is also the first group.) We call the run()
                # method of the DAG_executor_drivr and BFS is essentially done.
                # BFS will wait for the DAG_executr to finish. (We start a thread
                # to call run() and BFS joins this thread.)
                # 2. current_partition_number >=2: The previous partition or the 
                # groups therein are now complete so output the previous partition/groups.
                # If the DAG is complete, the the current partition is also compplete
                # so output the current partition/groups. If the following codition 
                # is true:
                #    # The DAG has partitions 1 and 2 so we can excute partition 1
                #    current_partition_number == 2   or
                #    # The DAG has all the partitions so we are done with incremental 
                #    # ADG execution and we can execute all the partitions that have not 
                #    # been executed yet
                #    DAG_info.get_DAG_info_is_complete() or 
                #    # we do not give every incremental DAG to the DAG_excutor, we only
                #    # give every th DAG. Check if this DAG should be made available
                #    # for ececution.
                #    num_incremental_DAGs_generated % incremental_DAG_deposit_interval == 0
                # First we take care of any leaf tasks that we found, If we are using workers
                # the leaf tasks will be added to the work queue. If we are using lambdas
                # then a lambda will be started (in method deposit()) to execute the leaf tasks.
                # We then call DAG_infobuffer_monitor.deposit() to make the new DAG_info object
                # available to the DAG_executor. The DAG_executor may be waiting in withdraw()
                # for it or it may still be executing the ccomplete tasks in the previous DAG_info.
                # If current_partition_number == 2 we write the DAG_info object to a file and 
                # statr the DAG_executor_driver (which will read the DAG_info object) and start
                # the workers (which will eecute the partition/group 1 task) or statr a lambda to 
                # execute partition/group 1.)
                if compute_pagerank and use_incremental_DAG_generation:
                    # partitioning is over when all graph nodes have been
                    # put in some partition
                    num_graph_nodes_in_partitions = num_nodes_in_partitions - num_shadow_nodes_added_to_partitions
                    # to_be_continued set to False when the DAG has been completely generated
                    to_be_continued = (num_graph_nodes_in_partitions < num_nodes)
                    logger.trace("BFS: calling gen DAG incremental"
                        + " num_nodes_in_partitions: " + str(num_nodes_in_partitions)
                        + " num_shadow_nodes_added_to_partitions: " + str(num_shadow_nodes_added_to_partitions)
                        + " num_graph_nodes_in_partitions: " + str(num_graph_nodes_in_partitions)
                        + " num_nodes: " + str(num_nodes) + " to_be_continued: "
                        + str(to_be_continued))

                    if using_workers or not using_workers:
                        if not use_page_rank_group_partitions:
                            logger.info("BFS: calling generate_DAG_info_incremental_partitions for"
                                + " partition " + str(partition_name) + " using workers.")
                            DAG_info = BFS_generate_DAG_info_incremental_partitions.generate_DAG_info_incremental_partitions(partition_name,current_partition_number,to_be_continued)
                        else:
#rhc increnetal groups
                            # avoiding circular import - above: from . import FS_generate_DAG_info_incremental_groups
                            # then use FS_generate_DAG_info_incremental_groups.generate_DAG_info_incremental_groups(...)
                            logger.info("BFS: calling generate_DAG_info_incremental_groups for"
                                + " partition " + str(partition_name) + " groups_of_current_partition: "
                                + str(groups_of_current_partition)
                                + " groups_of_partitions: " + str(groups_of_partitions))
                            #logger.info("BFS: BFS_queue empty: " + str(len(BFS_queue)))
                            #if len(BFS_queue)==1:
                            #    logger.info("BFS: BFS_queue[0]: " + str(BFS_queue[0]))
                            # Note: len of queue is 1 w/ contents -1 for both 4 and 5, and for 6 and 7
                            # Need to now number of children to know if it's end of component?
                            # We track node.num_children; we would need to check num_children==0
                            # for all of the nodes in all of the groups?
                            DAG_info = BFS_generate_DAG_info_incremental_groups.generate_DAG_info_incremental_groups(partition_name,current_partition_number,
                                groups_of_current_partition,groups_of_partitions,
                                to_be_continued)
                            # we are done with groups_of_current_partition so clear it so it is empty at start
                            # of next partition.
                            groups_of_current_partition.clear()
                            logger.trace("BFS: after calling generate_DAG_info_incremental_groups for"
                                + " partition " + str(partition_name) + " groups_of_current_partition: "
                                + str(groups_of_current_partition)
                                + ", groups_of_partitions: " + str(groups_of_partitions))
                        
                        # A DAG with a single partition, and hence a single group is a special case.
                        if current_partition_number == 1:
#rhc incremental groups
                            if not use_page_rank_group_partitions:
                                try:
                                    msg = "[Error]: BFS: partition " + partition_name + " is the first partition" \
                                        + " but it is not in leaf_tasks_of_partitions_incemental."
                                    assert partition_name in BFS_generate_DAG_info.leaf_tasks_of_partitions_incremental , msg
                                except AssertionError:
                                    logger.exception("[Error]: assertion failed")
                                    if exit_program_on_exception:
                                        logging.shutdown()
                                        os._exit(0)
                                #assertOld:
                                #if not partition_name in BFS_generate_DAG_info.leaf_tasks_of_partitions_incremental:
                                #    logger.error("partition " + partition_name + " is the first partition"
                                #        + " but it is not in leaf_tasks_of_partitions_incemental.")
                                #else:
                                # we have generated a state for leaf task partition_name. 
                                BFS_generate_DAG_info.leaf_tasks_of_partitions_incremental.remove(partition_name)
                            else:
                                try:
                                    msg = "[Error]: BFS: group " + group_name + " is the first group/partition" \
                                        + " but it is not in leaf_tasks_of_groups_incemental."
                                    assert group_name in BFS_generate_DAG_info.leaf_tasks_of_groups_incremental , msg
                                except AssertionError:
                                    logger.exception("[Error]: assertion failed")
                                    if exit_program_on_exception:
                                        logging.shutdown()
                                        os._exit(0)
                                #assertOld:
                                #if not group_name in BFS_generate_DAG_info.leaf_tasks_of_groups_incremental:
                                #    logger.error("group " + group_name + " is the first group/partition"
                                #        + " but it is not in leaf_tasks_of_groups_incemental.")
                                #else:
                                # we have generated a state for leaf task group_name. 
                                BFS_generate_DAG_info.leaf_tasks_of_groups_incremental.remove(group_name)


                            if DAG_info.get_DAG_info_is_complete():
                                # if there is only one partition in the DAG, save the partition and the DAG_info and 
                                # start the DAG_excutor_driver. Otherwise, we do all of this when we get partition 2,
                                # since when we get partition 2 partition 1 is complete and can be executed.
                                # Note: This means for incremental DAG generation we always start execution 
                                # after processing one partitio, if there is only 1 partition in the DAG,
                                # or 2 partitions otherwise. We may not want to start execution unti we have
                                # n partitions, since 2 partitions might be executed very quickly and the
                                # the DAG_executor woudld just wait for another incremental DAG. Hard to say
                                # what N should be.
                        
                                if not use_page_rank_group_partitions:
                                    # output partition 1, which is complete
                                    with open('./'+partition_name + '.pickle', 'wb') as handle:
                                        # partition indices in partitions[] start with 0, so current partition i
                                        # is in partitions[i-1] and previous partition is partitions[i-2]
                                        cloudpickle.dump(partitions[0], handle) #, protocol=pickle.HIGHEST_PROTOCOL)  

                                    logger.trace("BFS: deposit first DAG, which is complete, with num_incremental_DAGs_generated:"
                                        + str(num_incremental_DAGs_generated)
                                        + " current_partition_number: " + str(current_partition_number))
                                        # current partition number is 1
                                else:
                                    # output group 1, which is complete
                                    with open('./'+group_name + '.pickle', 'wb') as handle:
                                        # partition indices in partitions[] start with 0, so current partition i
                                        # is in partitions[i-1] and previous partition is partitions[i-2]
                                        cloudpickle.dump(groups[0], handle) #, protocol=pickle.HIGHEST_PROTOCOL)  

                                    logger.trace("BFS: deposit first DAG, which is complete, with num_incremental_DAGs_generated:"
                                        + str(num_incremental_DAGs_generated)
                                        + " current_group_number: " + str(1))
                                        # The only group in a complete DAG with one group is group 1

    #rhc: leaf tasks
                                # Deposit complete DAG_info for workers. Note that we have 
                                # not started the DAG_executor_driver yet, so this deposited
                                # DAG_info will not ever be withdrawn - the leaf task
                                # for partition/group 1 is covered by the DAG_executor_driver which either
                                # starts the workers and deposits the leaf task in the worker queue
                                # or starts a lambda to execute the leaf task, The DAG_executor_driver
                                # gets the leaf task from the  DAG_info it reads from a file 
                                # (which is output below).
                                # First partition/group is a leaf task but we do not want deposit() to 
                                # try to start it since the DAG_executor_driver always starts the 
                                # partition/group leaf task.
                                # Note: We probably do not need to do this deposit() at all. Test it.
                                new_leaf_tasks = []
                                DAG_info_is_complete = True # based on above if-condition being True
                                DAG_infobuffer_monitor.deposit(DAG_info,new_leaf_tasks,DAG_info_is_complete)

                                # We just processed the first and only partition; so we can output the 
                                # initial DAG_info and start the DAG_executor_driver. DAG_info
                                # will have a complete state for partition 1. the DAG_executor_driver
                                # will start workers or lambdas to execuet this leaf task.
                                #
                                # Before we start the DAG_executor_driver we need to have
                                # saved to a file PR1_1's nodes and saved the DAG_info object 
                                # to file DAG_info; we also do the DAG_infobuffer_monitor.deposit(DAG_info) 
                                # though it is not strictly required since the DAG_info will be 
                                # read by the workers or appear in the payload of the lambda started to execuet it,
                                #
                                # DAG_info will be read by the worker (threads/proceses)
                                # and the threads simulating lambdas
                                # or it will be read by the DAG_executor_driver and given
                                # to the real (leaf) lambdas as part of their payload.
                                
                                file_name = "./DAG_info.pickle"
                                #with open(file_name, 'wb') as handle:
                                #    cloudpickle.dump(DAG_info, handle) #, protocol=pickle.HIGHEST_PROTOCOL)  
                                DAG_info_dictionary = DAG_info.get_DAG_info_dictionary()
                                with open(file_name, 'wb') as handle:
                                    cloudpickle.dump(DAG_info_dictionary, handle) #, protocol=pickle.HIGHEST_PROTOCOL)  
                    
                                # Need to call run() but it has to be asynchronous as BFS needs to continue. So start a thread to do it.
                                thread_name = "DAG_executor_driver_Invoker"
                                logger.trace("BFS: Starting DAG_executor_driver_Invoker_Thread for incrmental DAG generation.")
                                # BFS joins this thread at the end of its execution. This ref is global.
                                invoker_thread_for_DAG_executor_driver = threading.Thread(target=DAG_executor_driver_Invoker_Thread, name=(thread_name), args=())
                                invoker_thread_for_DAG_executor_driver.start()
                                # Note: BFS calls DAG_executor_driver.run() to start DAG execution
                                # after it write the DAG_info to a file.
                                # If we are using tcp_server to store the fanin objects ermotely on the 
                                # server, then tcp_server needs the DAG_info. However, tcp_server
                                # cannot read the DAG_info from its file. This is because if we start 
                                # tcp_server before we starr BFS then the file will not have been written
                                # by BFS. Also, we want tcp_server o be running before DAG_executor 
                                # is started. So ... tcp_server does not read DAG_info from a file
                                # when we aer doing incremental DAG_generation. We pass the DAG_info
                                # to tcp_server on calls to fan-in etc.
                            else:
                                # there is more than one partition in the DAG so DAG is not complete
                                # and we continue with incremental DAG generation.
                                pass # empty
                        
                        else: # current_partition_number >=2:
                            # generate complete DAG_info for previous partition current_partition_number-1;
                            # the current partition may be complete or incomplete.
                            #
                            # Note: For groups, we may still key off partitions, i.e., when 
                            # we complete a partition, we generate the groups in this partition.
                            if not use_page_rank_group_partitions or use_page_rank_group_partitions:
                                #previous_partition_name = "PR"+str(current_partition_number-1)+"_1"
                            
                                # Previous partition is complete so save partition to a file
                                # which will be in cloud for real lambdas. (When we process 
                                # partition i, we compute the indices of the (parent) nodes in i-1 
                                # whose pageranks should be output by partition i-1 and input by partition i
                                # and save thse indices in the nodes of partition i-1; that makes partition i-1 
                                # "complete." These indices are of the nodes in partition i-1 that have chidren 
                                # in partition i.
                                # Note: Any children of nodes in partition i-1 that are in a different
                                # partition must be in partition, based on how partitions are generated.
                                # Note: Partition numbers start  at 1 not 0, so the name of partition i-1 is not in position 
                                # current_partition_number-1 it is in current_partition_number-2,
                                # e.g., name for the partition 2 that is previous to current partition
                                # 3 is position 1, which is 3-2.

                                if not use_page_rank_group_partitions:
                                    # always output the previous partition of nodes
                                    with open('./'+partition_names[current_partition_number-2] + '.pickle', 'wb') as handle:
                                        # partition indices in partitions[] start with 0, so current partition i
                                        # is in partitions[i-1] and previous partition is partitions[i-2]
                                        cloudpickle.dump(partitions[current_partition_number-2], handle) #, protocol=pickle.HIGHEST_PROTOCOL)  

                                    # the current partition might be the last partition in the DAG, if so
                                    # save the partition to a file. Below we will save the DAG_info.
                                    if DAG_info.get_DAG_info_is_complete():
                                        with open('./'+partition_name + '.pickle', 'wb') as handle:
                                            # partition indices in partitions[] start with 0, so current partition i
                                            # is in partitions[i-1] and previous partition is partitions[i-2]
                                            cloudpickle.dump(partitions[current_partition_number-1], handle) #, protocol=pickle.HIGHEST_PROTOCOL)  
                                else:
                                    previous_partition_number = current_partition_number - 1
                                    logger.trace("BFS: previous_partition_number: " + str(previous_partition_number))
                                    #frontier_groups_sum is the total number of groups, so last group was
                                    #   frontier_groups_sum
                                    logger.trace("BFS: frontier_groups_sum: " + str(frontier_groups_sum))
                                    groups_of_previous_partition = groups_of_partitions[previous_partition_number-1]
                                    logger.trace("BFS: groups_of_previous_partition: " + str(groups_of_previous_partition))

                                    # Using groups_of_current_partitionX instead of the global
                                    # variable groups_of_current_partition; The global was cleared
                                    # above so let's just keep this local.
                                    groups_of_current_partitionX = groups_of_partitions[current_partition_number-1]
                                    logger.trace("BFS: groups_of_current_partitionX: " + str(groups_of_current_partitionX))
                                    i = 0
                                    for previous_group in groups_of_previous_partition:
                                        index_in_groups_list_of_last_group_in_current_partition = frontier_groups_sum
                                        logger.trace("BFS: index_in_groups_list_of_last_group_in_current_partition: " + str(index_in_groups_list_of_last_group_in_current_partition))
                                        index_in_groups_list_of_first_group_of_current_partition = frontier_groups_sum - (len(groups_of_current_partitionX)-1)
                                        logger.trace("BFS: index_in_groups_list_of_first_group_of_current_partition: " + str(index_in_groups_list_of_first_group_of_current_partition))
                                        index_in_groups_list_of_first_group_of_previous_partition = index_in_groups_list_of_first_group_of_current_partition - len(groups_of_previous_partition)
                                        logger.trace("BFS: index_in_groups_list_of_first_group_of_previous_partition: " + str(index_in_groups_list_of_first_group_of_previous_partition))
                                        index_in_groups_list_of_previous_group = index_in_groups_list_of_first_group_of_previous_partition + i - 1
                                        logger.trace("BFS: for " + previous_group + " index_in_groups_list_of_previous_group: " + str(index_in_groups_list_of_previous_group))


# always output the previous partition of nodes
                                        with open('./'+previous_group + '.pickle', 'wb') as handle:
                                            # partition indices in partitions[] start with 0, so current partition i
                                            # is in partitions[i-1] and previous partition is partitions[i-2]
                                            cloudpickle.dump(groups[index_in_groups_list_of_previous_group], handle) #, protocol=pickle.HIGHEST_PROTOCOL)  
                                        i += 1

                                    # The current partition might be the last partition in the DAG, if so
                                    # save the partition's groups to a file. Below we will save the DAG_info
                                    # which is input by workers/lambdas for DAG execution.
                                    if DAG_info.get_DAG_info_is_complete():
                                        i = 0
                                        for current_group in groups_of_current_partitionX:
                                            index_in_groups_list_of_last_group_in_current_partition = frontier_groups_sum
                                            logger.trace("BFS: index_in_groups_list_of_last_group_in_current_partition: " + str(index_in_groups_list_of_last_group_in_current_partition))
                                            index_in_groups_list_of_first_group_of_current_partition = frontier_groups_sum - (len(groups_of_current_partitionX)-1)
                                            logger.trace("BFS: index_in_groups_list_of_first_group_of_current_partition: " + str(index_in_groups_list_of_first_group_of_current_partition))
                                            index_in_groups_list_of_current_group = index_in_groups_list_of_first_group_of_current_partition + i - 1
                                            logger.trace("BFS: for " + current_group + " index_in_groups_list_of_current_group: " + str(index_in_groups_list_of_current_group))

                                            with open('./'+current_group + '.pickle', 'wb') as handle:
                                                # partition indices in partitions[] start with 0, so current partition i
                                                # is in partitions[i-1] and previous partition is partitions[i-2]
                                                cloudpickle.dump(groups[index_in_groups_list_of_current_group], handle) #, protocol=pickle.HIGHEST_PROTOCOL)  
                                            
                                            i += 1

                                # Try to make sure workers are waiting for the DAG that is deposted below.
                                #logger.trace("BFS: sleeping before calling DAG_infobuffer_monitor.deposit(DAG_info).")
                                #time.sleep(1)

                                if current_partition_number > 2:
                                    # this is the number of DAGS generated by BFS but this
                                    # number may be greater than the number of DAGS deposited.
                                    num_incremental_DAGs_generated += 1

                                # Note: current_partition_number is not 1
                                # Note: Deposit DAG if current partition is 2 (which means this is the 
                                # first deposit. We only deposit the first DAG if that is the 
                                # only DAG that will be generated; or this is the last DAG that
                                # can be generated, i.e., the DAG is complete, or this is the 
                                # next DAG to generate based on the deposit interval)
                                # Note: if current_partition_number == 2 then we did not 
                                # increment num_incremental_DAGs_generated so it is still 0.
                                # This means that num_incremental_DAGs_generated % incremental_DAG_deposit_interval
                                # will be 0 so that num_incremental_DAGs_generated % incremental_DAG_deposit_interval == 0
                                # is True, where current_partition_number == 2 is also True.
                                # When current_partition_number is 1 we take a different branch
                                # above, i.e, we don't get here so it doesn't matter that
                                # num_incremental_DAGs_generated % incremental_DAG_deposit_interval is True,
                                # we won't publish the DAG when current_partition_number is unless
                                # the ADG is complete, i.e., has a total of 1 partitions.
                                # Note: We don't increment num_incremental_DAGs_generated until 
                                # current_partition_number is 3. So the first DAG published has
                                # a complete partition 1 and an incomplete partition 2. Then 
                                # we generate a DAG with complete partitions 1 and 2 and incomplete
                                # partition 3. At this point, num_incremental_DAGs_generated is 1.
                                # So this starts the count of generated DAGs since the last DAG
                                # was published. If incremental_DAG_deposit_interval is 1, we 
                                # would publish this DAG with complete partitions 1 and 2 and 
                                # incomplete partition 3. This we will be publishing each DAG
                                # that is generated. If incremental_DAG_deposit_interval is w, we 
                                # would not publish this DAG with complete partitions 1 and 2 and 
                                # incomplete partition 3, instead we would publish the next DAG
                                # generated, with complete partitions 1, 2, and 3, and incomplete
                                # partition 4. So every other generated DAG would be published.
                                if current_partition_number == 2 or (
                                    DAG_info.get_DAG_info_is_complete() or (
                                    num_incremental_DAGs_generated % incremental_DAG_deposit_interval == 0
                                    )):
                            
#rhc leaf tasks
                                    new_leaf_task_work_tuples = []           
#rhc incremental groups
                                    if not use_page_rank_group_partitions:
                                        if len(BFS_generate_DAG_info.leaf_tasks_of_partitions_incremental) > 0:
                                            # New leaf task partitions have been generated. Since no task
                                            # will fanout/fanin these leaf tasks, we must ensure they 
                                            # get started. if we are using workers, deposit these leaf 
                                            # tasks in the work queue. If we are using lambdas, start lambdas 
                                            # to execute these leaf tasks/partitions when they become complete.
                                            # (If the current partition is a leaf task, then it is incomplete
                                            # and cannot be executed until it becomes complete - it becomes complete
                                            # in the next DAG that is generated.)
                                            # For non-incremental DAG generation, the DAG_executor_driver starts 
                                            # all leaf tasks at the beginning of DAG excution - this includes the first leaf
                                            # task which is the first group/parititon generated by BFS and 
                                            # if the second group/partition is a leaf then the DAG_executor_driver
                                            # starts it too.
                                            # 
                                            # The leaf tasks are in DAG_info just returned. (They were added
                                            # on this last call to generate DAG_info or on a previous call. This
                                            # is the first call to DAG_infobuffer_monitor.deposit(DAG_info) since
                                            # these leaf tasks were added.)
                                            # We need the states of these leaf tasks so we can 
                                            # create the work that is added to the work_queue.

                                            logger.trace("BFS: new leaf tasks (some may be for partition/group 2):" + str(BFS_generate_DAG_info.leaf_tasks_of_partitions_incremental))
                                            DAG_states_incremental = DAG_info.get_DAG_states()
                                            # This is DAG_states of DAG_info
                                            logger.trace("BFS: DAG_states_incremental of new DAG_info: " + str(DAG_states_incremental))
                                            #DAG_leaf_task_start_states_incremental = DAG_info.get_DAG_leaf_task_start_states()
                                            DAG_map_incremental = DAG_info.get_DAG_map()

                                            if using_workers or not using_workers:
                                                # leaf task states (a task is identified by its state) are put in work_queue
                                                for name in BFS_generate_DAG_info.leaf_tasks_of_partitions_incremental:
                                                    state_incremental = DAG_states_incremental[name]
                                                    state_info_incremental = DAG_map_incremental[state_incremental]
                                                    logger.trace("BFS: state_info_incremental: " + str(state_info_incremental))
                                                    task_inputs = state_info_incremental.task_inputs
                                                    try:
                                                        msg = "[Error]: task_input for leaf" + " task/partition for incremental DAG generation is not empty."
                                                        assert not (len(task_inputs) != 0) , msg
                                                    except AssertionError:
                                                        logger.exception("[Error]: assertion failed")
                                                        if exit_program_on_exception:
                                                            logging.shutdown()
                                                            os._exit(0)
                                                    # assertOld:
                                                    #if len(task_inputs) != 0:
                                                    #    logger.error("[Error]: task_input for leaf"
                                                    #       + " task/partition for incremental DAG generation is not empty.")
                                                    
                                                    task_name = state_info_incremental.task_name
                                                    try:
                                                        msg = "[Error]: BFS: task name of leaf task is not" \
                                                            + " name in leaf_tasks_of_partitions_incremental."
                                                        assert task_name == name , msg
                                                    except AssertionError:
                                                        logger.exception("[Error]: assertion failed")
                                                        if exit_program_on_exception:
                                                            logging.shutdown()
                                                            os._exit(0)
                                                    #assertOld:
                                                    #if not task_name == name:
                                                    #    logger.error("[Error]: task name of leaf task is not"
                                                    #        + " name in leaf_tasks_of_partitions_incremental.")
                    
                                                    dict_of_results_incremental =  {}
                                                    dict_of_results_incremental[task_name] = task_inputs
                                                    logger.trace("BFS: add leaf task to new_leaf_task_work_tuples: " + task_name)
                                                    work_tuple = (state_incremental,dict_of_results_incremental)
    #rhc leaf tasks
                                                    #work_queue.put(work_tuple)
                                                    new_leaf_task_work_tuples.append(work_tuple)
                                            else:
                                                pass 
                                                # documents that we are using the same code for lambdas and workers

                                            BFS_generate_DAG_info.leaf_tasks_of_partitions_incremental.clear()
                                            #logger.trace("BFS: leaf tasks after clear: " + str(BFS_generate_DAG_info.leaf_tasks_of_partitions_incremental))
                                    else:
                                        if len(BFS_generate_DAG_info.leaf_tasks_of_groups_incremental) > 0:
                                            # New leaf task partitions have been generated. Since no task
                                            # will fanout/fanin these leaf tasks, we must ensure they 
                                            # get started; if we are using workers, deposit these leaf tasks
                                            # in the work queue. If we are using lambdas, start lambdas 
                                            # to execute these leaf tasks/partitions. For non-incremental
                                            # DAG generation, the DAG_executor_driver starts all leaf tasks
                                            # at the beginning of DAG excution. 
                                            # 
                                            # The leaf tasks are in DAG_info just returned. (They were added
                                            # on this last call to generate DAG_info or on a previous call. This
                                            # is the first call to DAG_infobuffer_monitor.deposit(DAG_info) since
                                            # these leaf tasks were added.)
                                            # We need the states of these leaf tasks so we can 
                                            # create the work that is added to the work_queue.
   
    # (Same comment applies in mirrored code above.)                                            
    #rhc: put them in work_queue after deposit? 
    # so we know that whoever gets the lead tasks as work has a DAG that contains the tasks?
    # No? since some workers may get this leaf work and then will not need to call withdraw()
    # to get a DAG?
    # So if a worker gets a leaf task and it's state is not in its DAG or it is but leaf task
    # state is not complete then we got the leaf task before we got a new
    # DAG that has theleaf task in it. so the worker should put leaf task in its continue_queue 
    # and do leaf task after it gets a new DAG which will have leaf task(s) in it. So worker does not 
    # execute the leaf task so when it gets the -1 ... it will call withdraw,
    # as expected. Ugh.
    #
    # issue is: put -1 in work_queue but then get 4 from work_queue so do 4 and inc num_tasks_executed
    # so this condition becomes false
    #    if num_tasks_executed == num_tasks_to_execute: 
    # thus we do not call work_qeuue.get, instead we try to process -1 as a state.
    #
    # an this get out of sync? So we look at 4, it's in the DAG as complete.
    # so we can execute it. But then can we get a -1? -1 means 

    # another Issue: first worker to see num_tasks_executed == num_tasks_to_execute
    # will put -1 in work_queue. Some worker (maybe the same worker) will get
    # this -1 and call withdraw, maybe after putting a second -1 in the 
    # work_queue (if there are multiple workers). The first worker can call
    # withdraw and get a DAG and set the new num_tasks_to_execute so that 
    # the second worker will not see num_tasks_executed == num_tasks_to_execute 
    # and so will not get the -1 tha the first worker put there?
    #

    #
    # So if they get the 4, they can, add it to their continue queue and
    # call get_work again?


                                            logger.trace("BFS: new leaf tasks (some may be for partition/group 2): " + str(BFS_generate_DAG_info.leaf_tasks_of_groups_incremental))
                                            DAG_states_incremental = DAG_info.get_DAG_states()
                                            logger.trace("BFS: DAG_states_incremental: " + str(DAG_states_incremental))
                                            #DAG_leaf_task_start_states_incremental = DAG_info.get_DAG_leaf_task_start_states()
                                            DAG_map_incremental = DAG_info.get_DAG_map()

#rhc: issue: don't do this for lambdas?
                                            if using_workers or not using_workers:
                                                # leaf task states (a task is identified by its state) are put in work_queue
                                                for name in BFS_generate_DAG_info.leaf_tasks_of_groups_incremental:
                                                    state_incremental = DAG_states_incremental[name]
                                                    state_info_incremental = DAG_map_incremental[state_incremental]
                                                    logger.trace("BFS: state_info_incremental: " + str(state_info_incremental))
                                                    task_inputs = state_info_incremental.task_inputs
                                                    try:
                                                        msg = "[Error]: task_input for leaf" + " task/partition for incremental DAG generation is not empty."
                                                        assert not (len(task_inputs) != 0) , msg
                                                    except AssertionError:
                                                        logger.exception("[Error]: assertion failed")
                                                        if exit_program_on_exception:
                                                            logging.shutdown()
                                                            os._exit(0)
                                                    # assertOld:
                                                    #if len(task_inputs) != 0:
                                                    #    logger.error("[Error]: task_input for leaf"
                                                    #        + " task/partition for incremental DAG generation is not empty.")
                                                    
                                                    task_name = state_info_incremental.task_name
                                                    try:
                                                        msg = "[Error]: BFS: task name of leaf task is not" \
                                                            + " name in leaf_tasks_of_groups_incremental."
                                                        assert task_name == name , msg
                                                    except AssertionError:
                                                        logger.exception("[Error]: assertion failed")
                                                        if exit_program_on_exception:
                                                            logging.shutdown()
                                                            os._exit(0)
                                                    #assertOld:
                                                    #if not task_name == name:
                                                    #    logger.error("[Error]: task name of leaf task is not"
                                                    #        + " name in leaf_tasks_of_groups_incremental.")
                                                            
                                                    dict_of_results_incremental =  {}
                                                    dict_of_results_incremental[task_name] = task_inputs
                                                    logger.trace("BFS: add leaf task to new_leaf_task_work_tuples: " + task_name)
                                                    work_tuple = (state_incremental,dict_of_results_incremental)
    #rhc leaf tasks
                                                    #work_queue.put(work_tuple)
                                                    new_leaf_task_work_tuples.append(work_tuple)
                                            else:
                                                pass # complete for lambdas
                                                # start a lambda with empty input payload (like DAG_executor_driver)

                                            BFS_generate_DAG_info.leaf_tasks_of_groups_incremental.clear()
                                            #logger.trace("BFS: leaf tasks after clear: " + str(BFS_generate_DAG_info.leaf_tasks_of_groups_incremental))

                                    # Deposit new incremental DAG. This may be the 
                                    # first DAG and since the workers and lambdas
                                    # will receive this DAG as a leaf task, they 
                                    # will not need to withdraw this DAG as a new
                                    # DAG, i.e., their first request for a new 
                                    # incremental DAG is for any newer DAG than the 
                                    # first DAG (i.e., any version later than version 1.)

                                    logger.info("BFS: deposit next DAG with num_incremental_DAGs_generated:"
                                        + str(num_incremental_DAGs_generated)
                                        + " current_partition_number: " + str(current_partition_number))
                                    # if not current_partition_number == 2:
                                    #     logging.shutdown()
                                    #     os._exit(0) 
#rhc leaf tasks
                                    DAG_info_is_complete = DAG_info.get_DAG_info_is_complete()
                                    # If current_partition_number is 2 this partition/group 2 may be the 
                                    # start of a new component, i.e., a leaf task. But if so then the 
                                    # DAG_executor_driver will start a lambda to execute this leaf task so 
                                    # deposit should not also start a lambda for this leaf task.
                                    # Prevent deposit() from doing this by clearing the list of leaf taaks.
                                    # (There should only be one leaf task in the list.)

                                    if (current_partition_number) == 2:
                                        new_leaf_task_work_tuples = []
                                    DAG_infobuffer_monitor.deposit(DAG_info,new_leaf_task_work_tuples,DAG_info_is_complete)
                                    # deposit starts a lambda with empty input payload (like DAG_executor_driver)
                                    # when the leaf task becomes complete (the leaf task on this
                                    # call to deposit has a lambda started for it on the next call to deposit.)
                                if (current_partition_number) == 2:
                                    # We just processed the second partition in a DAG that 
                                    # has more than one partition, so we can output the 
                                    # initial DAG_info and start the DAG_executor_driver. This DAG_info
                                    # will have a complete state for partition 1 and an incomplete
                                    # state for 2. Thus, we can start DAG_execution and compute
                                    # pagerank for P1. At that point, if P2 is incomplete, we will
                                    # add the state for P2 to the continue queue and P2 will not be
                                    # executed until new DAG_info is generated. (DAG_info may be 
                                    # complete in whixh case P2 is complete.)
                                    #
                                    # We can't generate the initial DAG for execuion until we have partitions 
                                    # 1 and 2, since we don't know partition 1's outputs until we have processed 
                                    # the nodes in partition 2; partition 2 is "to be continued".
                                    #
                                    # So before we start the DAG_executor_driver we need to have
                                    # saved to a file PR1_1's nodes and saved the initial DAG_info;
                                    # we also do the DAG_infobuffer_monitor.deposit(DAG_info) though
                                    # it is not strictly requried since the DAG_info file can be 
                                    # read by the workers/lambdas.
                                    #
                                    # DAG_info will be read by the worker (threads/proceses)
                                    # or it will be read by the DAG_executor_driver and given
                                    # to the real/simulated (leaf) lambdas as part of their payload.
                                    file_name = "./DAG_info.pickle"
#rhc: incremental
                                    #DAG_info_dictionary = DAG_info.DAG_info_dictionary
                                    DAG_info_dictionary = DAG_info.get_DAG_info_dictionary()
                                    with open(file_name, 'wb') as handle:
                                        cloudpickle.dump(DAG_info_dictionary, handle) #, protocol=pickle.HIGHEST_PROTOCOL)  
                                    
                                    # Need to call DAG_executor_driver.run() but it has to be invoked asynch
                                    thread_name = "DAG_executor_driver_Invoker"
                                    logger.trace("BFS: Starting DAG_executor_driver_Invoker_Thread for incrmental DAG generation.")
#rhc: incremental
                                    # Note: BFS joins this thread. This is a global.
                                    invoker_thread_for_DAG_executor_driver = threading.Thread(target=DAG_executor_driver_Invoker_Thread, name=(thread_name), args=())

                                    invoker_thread_for_DAG_executor_driver.start()
                                    time.sleep(10)
                                else:
                                    # For debugging:
                                    # This is not partition 1 and it is not partition 2 so we do not 
                                    # start the DAG_executor_driver.run(). However, we still need to 
                                    # check whether the current partition (which is not partition 2) 
                                    # is the last partition in the DAG; if so, save this complete
                                    # DAG_info to file. (If this is partition 2, we always
                                    # save the DAG_info since we will also start the DAG_executor_driver.)
                                    if DAG_info.get_DAG_info_is_complete():
                                        file_name = "./DAG_info_complete.pickle"
                                        with open(file_name, 'wb') as handle:
                                            cloudpickle.dump(DAG_info, handle) #, protocol=pickle.HIGHEST_PROTOCOL)  
                            
                            else:
                                pass 
                                # this helps document that we are using same code for partitions
                                # and groups so this else is never exwcuted (ee if-condition)
                        # end else #current_partition_number >=2
                    else:
                        pass # complete this code for lambdas
 
                elif compute_pagerank and use_multithreaded_BFS:
                    # partitioning is over when all graph nodes have been
                    # put in some partition
                    num_graph_nodes_in_partitions = num_nodes_in_partitions - num_shadow_nodes_added_to_partitions
                    # to_be_continued set to False when the DAG has been completely generated
                    to_be_continued = (num_graph_nodes_in_partitions < num_nodes)
                    logger.trace("BFS: calling gen DAG incremental"
                        + " num_nodes_in_partitions: " + str(num_nodes_in_partitions)
                        + " num_shadow_nodes_added_to_partitions: " + str(num_shadow_nodes_added_to_partitions)
                        + " num_graph_nodes_in_partitions: " + str(num_graph_nodes_in_partitions)
                        + " num_nodes: " + str(num_nodes) + " to_be_continued: "
                        + str(to_be_continued))
                    if using_workers or not using_workers:
                        if not use_page_rank_group_partitions:
                            #logger.trace("BFS: calling deposit for"
                            #    + " partition " + str(partition_name) + " using workers.")
                            # All of these parameters are immutable: string, int, boolean
                            global DAG_generator_for_multithreaded_DAG_generation
                            partition_tuple = (partition_name, current_partition_number,to_be_continued)
                            DAG_info = DAG_generator_for_multithreaded_DAG_generation.deposit(partition_tuple)
                        else:
#rhc increnetal groups
                            # avoiding circular import - above: from . import FS_generate_DAG_info_incremental_groups
                            # then use FS_generate_DAG_info_incremental_groups.generate_DAG_info_incremental_groups(...)
                            #logger.info("BFS: calling deposit for"
                            #    + " partition " + str(partition_name) + " groups_of_current_partition: "
                            #    + str(groups_of_current_partition)
                            #    + " groups_of_partitions: " + str(groups_of_partitions))
                            # these parameters are immutable: partition_name,current_partition_number, to_be_continued

                            #ToDo: # Copy? Deep, shallow?
                            copy_of_groups_of_current_partition = copy.copy(groups_of_current_partition)
                            copy_of_groups_of_partitions = copy.copy(groups_of_partitions)
                            group_tuple = (partition_name,current_partition_number,
                                copy_of_groups_of_current_partition,copy_of_groups_of_partitions, to_be_continued)
                            DAG_info = DAG_generator_for_multithreaded_DAG_generation.deposit(group_tuple)
                            # we are done with groups_of_current_partition so clear it so it is empty at start
                            # of next partition.
                            groups_of_current_partition.clear()
                            #logger.trace("BFS: after calling deposit for"
                            #    + " partition " + str(partition_name) + " groups_of_current_partition: "
                            #    + str(groups_of_current_partition)
                            #    + ", groups_of_partitions: " + str(groups_of_partitions))

                            pass # ToDo: if DAG_info is complete then ....

                #global frontier_groups_sum
                #global num_frontier_groups
                logger.trace("BFS: frontier groups: " + str(num_frontier_groups))

                # use this if to filter the very small numbers of groups
                #if frontier_groups > 10:
                # using this to determine whether parent is in current partition
                current_partition_number += 1
                current_group_number = 1
                # frontier_groups_sum += num_frontier_groups
                logger.trace("BFS: frontier_groups_sum: " + str(frontier_groups_sum))
                # this was incrementd in dfs_parent for each unvsited child of a 
                # parent, i.e., when a new group was generated.
                # Note: we increment this in the loop that follows before we
                # call dfs_parent so its value will be the same as current_group_number
                # which is set to 1 above and not incremented before the call to 
                # dfs_parent; instead, it is incremented after the call to dfs_parent.
                # So on the first call, both values will be 1, on the second call,
                # both will be 2, ...
                num_frontier_groups = 0
  
        if not len(node.children):
            logger.trace ("bfs node " + str(node.ID) + " has no children")
        else:
            logger.trace ("bfs node " + str(node.ID) + " visit children")
        for neighbor_index in node.children:
            neighbor = nodes[neighbor_index]
            if neighbor.ID not in visited:
                logger.trace ("bfs visit child " + str(neighbor.ID) + " mark it visited and "
                    + "dfs_parent(" + str(neighbor.ID) + ")")

                #visited.append(neighbor.ID)
                logger.trace ("bfs dfs_parent("+ str(neighbor.ID) + ")")

                dfs_parent_start_partition_size = len(current_partition)
                dfs_parent_loop_nodes_added_start = loop_nodes_added
                dfs_parent_start_frontier_size = len(frontier)
 
                # number of groups in current partition/sum
                num_frontier_groups += 1
                # total number of groups, if this is i then this group will
                # be stored in groups[i]
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
                logger.trace ("bfs frontier_parent_tuple: " + str(frontier_parent_tuple))
                # mark this node as one that PageRank needs to send in its output to the 
                # next partition (via fanout/faninNB).That is, the fact that list
                # frontier_parent is not empty indicates it needs to be sent in the 
                # PageRank output. The tuple indictes which frontier group it should 
                # be sent to. PageRank may send frontier_parent nodes to mulltiple groups
                # of multiple partitions
                nodes[node.ID].frontier_parents.append(frontier_parent_tuple)
                """

# rhc : ******* Group
                # Note: append() uses a shallow copy.
                groups.append(current_group)

                group_name = "PR" + str(current_partition_number) + "_" + str(current_group_number)
                if current_group_isLoop:
                    # These are the names of the groups that have a loop. In the 
                    # DAG, we will append an 'L' to the name. Not used since we 
                    # use loop names (with 'L") as we generate Sender and Recevers.
                    # instead of modifying the names of senders/receievers before we 
                    # generate the DAG.
                    group_name = group_name + "L"
                    Group_loops.add(group_name)

                # Note: group_name is collected below

#rhc: incremental groups
                if compute_pagerank and (use_incremental_DAG_generation or use_multithreaded_BFS):
                    groups_of_current_partition.append(group_name)
                    logger.trace("BFS: add " + group_name + "for partition number " 
                        + str(current_partition_number) 
                        + " to groups_of_current_partition: " + str(groups_of_current_partition))

#rhc: clustering
                # Note: need the group name here.
                # Need to know the number of shadow nodes when clustering in order
                # to compute the number of non-shadow nodes (nodies-shadow_nodes = non-shadow nodes)

                #if use_shared_partitions_groups:
                if use_shared_partitions_groups or enable_runtime_task_clustering:
                    #rhc shared
                    end_num_shadow_nodes_for_groups = num_shadow_nodes_added_to_groups
                    change_in_shadow_nodes_for_groups = end_num_shadow_nodes_for_groups - start_num_shadow_nodes_for_groups
                    groups_num_shadow_nodes_list.append(change_in_shadow_nodes_for_groups)
                    size_of_group = len(current_group) - change_in_shadow_nodes_for_groups
                    BFS_generate_DAG_info.groups_num_shadow_nodes_map[group_name] = size_of_group
                    # call start here before next call to dfs_parents(), if any, since we 
                    # may not call dfs_parents() again as node may not have any (unvisited) children.
                    # if no call to dfs_parent() we may still have a final partition/group and we
                    # need to have called start before then.
                    start_num_shadow_nodes_for_groups = num_shadow_nodes_added_to_groups

                # this is a list of partition_nodes in the current group
                current_group = []

#rhc:
# 1. clear instead of re-init?
# 2. Really need to patch groups? If no, Suggested assert no patching. Note:
#       we find loops on backup and we don't do patch stuff until after
#       we see all backups, so can we do patch stuff without knowing 
#       about loop that will be detected later?

                if current_group_isLoop:
                    # When the tuples in frontier_parent_partition_patch_tuple_list were created,
                    # no loop had been detectd in the partition so we used a partitiob name that 
                    # did not end in 'L'. At some point a loop was detected so we need to
                    # change the partition name in the tuple so that it ends with 'L'. If no loop
                    # is detectd, then current_partition_isLoop will be false and no changes
                    # need to be made.
                    logger.trace("XXXXXXXXXXX BFS: patch group frontier_parent tuples: ")
                    # frontier_parent_partition_patch_tuple was created as:
                    #   (parent_partition_number,parent_partition_parent_index,(current_partition_number,1,child_index_in_current_partition,current_partition_name))
                    for frontier_parent_group_patch_tuple in frontier_parent_group_patch_tuple_list:
                        # These values were used to create the tuples in dfs_parent()
                        index_in_groups_list_of_previous_group = frontier_parent_group_patch_tuple[0]
                        parent_group_parent_index = frontier_parent_group_patch_tuple[1]
                        position_in_frontier_parents_group_list = frontier_parent_group_patch_tuple[2]

                        # get the tuple that has the wrong name
                        parent_group = groups[index_in_groups_list_of_previous_group]
                        frontier_parents = parent_group[parent_group_parent_index].frontier_parents
                        frontier_parent_group_tuple_to_patch = frontier_parents[position_in_frontier_parents_group_list]
                        logger.trace("XXXXXXX BFS: patching group frontier_tuple name "
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
                        logger.trace("XXXXXXX BFS:  new frontier_parents: " + str(frontier_parents))

                frontier_parent_group_patch_tuple_list.clear()

                if use_shared_partitions_groups:
                    # Given:
                    # shared_frontier_parent_partition_patch_tuple = (task_name_of_parent,position_in_list_of_parent_frontier_tuples)
                    if current_group_isLoop:
                        logger.trace("X-X-X-X-X-X-X BFS: patch shared groups frontier_parent tuples: ")
                        for shared_frontier_parent_groups_patch_tuple in shared_frontier_parent_groups_patch_tuple_list:
                            # These values were used to create the tuples in dfs_parent()
                            task_name_of_parent = shared_frontier_parent_groups_patch_tuple[0]
                            position_of_tuple_in_list_of_parent_frontier_tuples = shared_frontier_parent_groups_patch_tuple[1]

                            list_of_parent_frontier_tuples = BFS_Shared.shared_partition_frontier_group_map.get(task_name_of_parent)
                            logger.trace("X-X-X-X-X-X-X BFS:  old shared groups frontier_parents for " + task_name_of_parent + " is " +  str(list_of_parent_frontier_tuples))
                            frontier_parent_group_tuple_to_patch = list_of_parent_frontier_tuples[position_of_tuple_in_list_of_parent_frontier_tuples]
                            # Given:
                            #shared_frontier_parent_tuple = (current_partition_number,num_frontier_groups,child_index_in_current_partition,current_partition_name,parent_partition_parent_index)
                            first_field = frontier_parent_partition_tuple_to_patch[0]
                            second_field = frontier_parent_partition_tuple_to_patch[1]
                            third_field = frontier_parent_partition_tuple_to_patch[2]
                            # FYI: [3] is the group name to be patched with the new name ending in "L"
                            # e.g., "PR2_2" --> "PR2_2L". Partition name was appended with "L" above
                            fifth_field = frontier_parent_partition_tuple_to_patch[4]
                            new_frontier_parent_partition_tuple = (first_field,second_field,third_field,group_name,fifth_field)
                            del list_of_parent_frontier_tuples[position_of_tuple_in_list_of_parent_frontier_tuples]
                            # append the new tuple, order of tuples may change but order is not important
                            list_of_parent_frontier_tuples.append(new_frontier_parent_partition_tuple)
                            logger.trace("X-X-X-X-X-X-X BFS:  new shared groups frontier_parents for " + task_name_of_parent + " is " +  str(list_of_parent_frontier_tuples))

                    shared_frontier_parent_groups_patch_tuple_list.clear()

                if current_group_isLoop:
                    # When the tuples in sender_receiver_partition_patch_tuple_list were created,
                    # no loop had been detectd in the partition so we used a partitiom name 
                    # for the receiver name that did not end in 'L'. At some point a loop was detected so we need to
                    # change the receiver name so that it ends with 'L'. If no loop
                    # is detectd, then current_partition_isLoop will be false and no changes
                    # need to be made.
                    logger.trace("XXXXXXXXXXX BFS: patch group sender/receiver names: ")
                    for sender_receiver_group_patch_tuple in sender_receiver_group_patch_tuple_list:
                        # sender_receiver_partition_patch_tuple crated as:
                        #   sender_receiver_partition_patch_tuple = (parent_partition_number,receiving_partition)
                        index_in_groups_list_of_previous_group = sender_receiver_group_patch_tuple[0]
                        receiving_group = sender_receiver_group_patch_tuple[1]

                        sending_group = partition_names[index_in_groups_list_of_previous_group]
                        sender_name_set = BFS_generate_DAG_info.Group_senders[sending_group]
                        logger.trace("XXXXXXX BFS: patching group sender_set receiving_group "
                            + receiving_group + " to " + group_name)
                        sender_name_set.remove(receiving_group)
                        sender_name_set.add(group_name)
                        logger.trace("XXXXXXX BFS:  new group sender_Set: " + str(sender_name_set))

                        logger.trace("XXXXXXX BFS: patching Group_receivers receiver name "
                            + receiving_group + " to " + group_name)
                        BFS_generate_DAG_info.Group_receivers[group_name] = BFS_generate_DAG_info.Group_receivers[receiving_group]
                        del BFS_generate_DAG_info.Group_receivers[receiving_group]
                        logger.trace("XXXXXXX BFS:  new Group_receivers[group_name]: " + str(BFS_generate_DAG_info.Group_receivers[group_name]))
                
                sender_receiver_group_patch_tuple_list.clear()

                current_group_isLoop = False
                current_group_number += 1
                group_names.append(group_name)

                #global patch_parent_mapping_for_partitions
                global patch_parent_mapping_for_groups
                logger.trace("partition_nodes to patch: ")
                for parent_tuple in patch_parent_mapping_for_groups:
                    logger.trace("parent_tuple: " + str(parent_tuple) + "," )
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

                    #assserting if condition is True
                    try:
                        msg = "[Error]: group_index_of_parent is -1"
                        assert group_index_of_parent != -1, msg
                    except AssertionError:
                        logger.exception("[Error]: assertion failed")
                        if exit_program_on_exception:
                            logging.shutdown()
                            os._exit(0)
                    #assertOld:
                    if group_index_of_parent != -1:
                        # Suggested assert group_index is also -1
                        #list_of_parents_of_partition_node[i] = partition_index_of_parent
                        list_of_parents_of_group_node[i] = group_index_of_parent
                        logger.trace("BFS: end of frontier: remapping parent " + str(parent_ID)
                            + " of " + str(node_ID) 
                            #+  " to " + partition_index_of_parent 
                            #+ " for partition node and "
                            + " to " + str(group_index_of_parent) + " for group node")
                    else:
                        # assertion above should have failed
                        logger.error("BFS: group_index_of_parent is -1")
                        logging.shutdown()
                        os._exit(0)

                patch_parent_mapping_for_groups = []

                # track groups here; track partitions when frontier ends above
                nodeIndex_to_groupIndex_maps.append(nodeIndex_to_groupIndex_map)
                nodeIndex_to_groupIndex_map = {}

                """
                logger.trace("")
                if PRINT_DETAILED_STATS:
                    logger.trace("KKKKKKKKKKKKKKKKKKKKK group nodes' frontier_parent_tuples:")
                    for x in groups:
                        if PRINT_DETAILED_STATS:
                            print_val = "-- (" + str(len(x)) + "): "
                            for node in x:
                                print_val += str(node.ID) + ": "
                                # logger.trace(node.ID,end=": ")
                                for parent_tuple in node.frontier_parents:
                                    print_val += str(parent_tuple) + " "
                                    # print(str(parent_tuple), end=" ")
                            logger.trace(print_val)
                            logger.trace("")
                        else:
                            logger.trace("-- (" + str(len(x)) + ")")
                else:
                    logger.trace("-- (" + str(len(x)) + ")")
                logger.trace("")
                """

# rhc : ******* end Group
                # Tracking changes to partition size and frontier size
                # for every call to dfs_parent. So these are after
                # dfs_parent() calls. They are not when we end a frontier
                # since the changes are tracked for dfs_parent() call.
                # Note: dfs_parent() genertes a group so they are in essence
                # per group also.

                dfs_parent_loop_nodes_added_end = loop_nodes_added

# rhc : ******* Partition
                dfs_parent_end_partition_size = len(current_partition)
                dfs_parent_change_in_partition_size = (dfs_parent_end_partition_size - dfs_parent_start_partition_size) - (
                    dfs_parent_loop_nodes_added_end - dfs_parent_loop_nodes_added_start)
                logger.trace("dfs_parent("+str(node.ID) + ")_change_in_partition_size: " + str(dfs_parent_change_in_partition_size))
                dfs_parent_changes_in_partiton_size.append(dfs_parent_change_in_partition_size)
              
                dfs_parent_end_frontier_size = len(frontier)
                dfs_parent_change_in_frontier_size = (dfs_parent_end_frontier_size - dfs_parent_start_frontier_size) - (
                    dfs_parent_loop_nodes_added_end - dfs_parent_loop_nodes_added_start)
                logger.trace("dfs_parent("+str(node.ID) + ")_change_in_frontier_size: " + str(dfs_parent_change_in_frontier_size))
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
                    logger.trace("child " + str(neighbor.ID) + " of node " + str(node.ID)
                        + " has no children, already marked it visited and added"
                        + " it to partition but do not queue it or add it to frontier.")
                """
            else:
                logger.trace ("bfs node " + str(neighbor.ID) + " already visited")

        """ This raises an exception. We ar not using the frontier concept anymore.
            Frontiers and partition are now the same.
        try:
            frontier.remove(node.ID)
        except ValueError:
            logger.exception("[Error]: PageRank_Function: ValueError:"
                + "*******bfs: " + str(node.ID)+ " not in frontier.")
            if exit_program_on_exception:
                logging.shutdown()
                os._exit(0)

        if DEBUG_ON:
            print_val = "frontier after remove " + str(node.ID) + ": "
            for x in frontier:
                #logger.trace(x.ID, end=" ")
                print_val = print_val + str(x) + " "
            logger.info(print_val)
            logger.info("")
        """

    """
    if len(current_partition) >= 0:
        logger.trace("BFS: create final sub-partition")
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
    # usd to convert the gaph to networkX format so we can run networkX 
    # algorithms on it, e.g., fnd_cycle, diameter.
    networkX_lines = []
    #fname = "graph_3000"
    # whiteboard graph
    # fname = "graph_WB"
    # These are whiteboard graphs with various extensions
    # that, e.g., add connected components (CC)
    #fname = "graph_22N_2CC"
    #fname = "graph_23N"
    #fname = "graph_24N_3CC"

    fname = "graph_24N_3CC_fanin"   # fanin at end

    #fname = "graph_2N_2CC"  # 2 nodes (CCs) no edges
    #fname = "graph_3N_3CC"  # 3 nodes (CCs) no edges
    #fname = "graph_2N"
    #fname = "graph_1N"
    #fname = "graph_3P"
    #fname = "graph_27_loops"
    #graph_file = open(fname, 'r')
    #graph_file = open(fname, 'r')
    graph_file = open(fname+".gr", 'r')
    #graph_file = open(fname, 'r')
    count = 0
    file_name_line = graph_file.readline()
    count += 1
    logger.trace("input_graph: file_name_line{}: {}".format(count, file_name_line.strip()))
    vertices_line = graph_file.readline()
    count += 1
    logger.trace("input_graph: vertices_line{}: {}".format(count, vertices_line.strip()))
    edges_line = graph_file.readline()
    count += 1
    logger.trace("input_graph: edges_line{}: {}".format(count, edges_line.strip()))
    

    _max_weight_line_ignored = graph_file.readline()
    count += 1
    #logger.trace("max_weight_line{}: {}".format(count, max_weight_line_ignored.strip()))
    _min_weight_line_ignored = graph_file.readline()
    count += 1
    #logger.trace("min_weight_line{}: {}".format(count,  min_weight_line_ignored.strip()))

    # need this for generated graphs; 100.gr is old format?
    
    _min_edge_line_ignored = graph_file.readline()
    count += 1
    #logger.trace("min_edge_line{}: {}".format(count, min_edge_line_ignored.strip()))
    _max_edge_line_ignored = graph_file.readline()
    count += 1
    #logger.trace("max_edge_line{}: {}".format(count, max_edge_line_ignored.strip()))
    
    vertices_edges_line = graph_file.readline()
    count += 1
    logger.trace("input_graph: vertices_edges_line {}: {}".format(count, vertices_edges_line.strip()))

    words = vertices_edges_line.split(' ')
    logger.trace("input_graph: nodes:" + words[2] + " edges:" + words[3])
    global num_nodes
    num_nodes = int(words[2])
    global num_edges

#rhc: num_nodes
    # save number of graph nodes in BFS_generate_DAG_info
    # which is where the DAG_info is built. num_nodes_in_graph
    # is now a field in DAG_info.
    if not use_incremental_DAG_generation:
        BFS_generate_DAG_info.num_nodes_in_graph = num_nodes
        logger.info("input_graph: set BFS_generate_DAG_info.num_nodes_in_graph to "
            + str(BFS_generate_DAG_info.num_nodes_in_graph))
    else:
        if use_page_rank_group_partitions:
            BFS_generate_DAG_info_incremental_groups.num_nodes_in_graph = num_nodes
            logger.info("input_graph: set BFS_generate_DAG_info_incemental_groups.num_nodes_in_graph to "
                + str(BFS_generate_DAG_info.num_nodes_in_graph))
        else:
            BFS_generate_DAG_info_incremental_partitions.num_nodes_in_graph = num_nodes
            logger.info("input_graph: set BFS_generate_DAG_info_incremental_partitions.num_nodes_in_graph to "
                + str(BFS_generate_DAG_info.num_nodes_in_graph))


    num_edges = int(words[3])
    logger.trace("input_file: read: num_nodes:" + str(num_nodes) + " num_edges:" + str(num_edges))

    # if num_nodes is 100, this fills nodes[0] ... nodes[100], length of nodes is 101
    # Note: nodes[0] is not used, 
    for x in range(num_nodes+1):
        nodes.append(Node(x))

    global num_parent_appends
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
            logger.trace("input_graph: [Warning]: self loop: " + str(source) + " -->" + str(target))
            num_self_loops += 1
            continue
        #logger.trace("target:" + str(target))
        #if target == 101:
        #    logger.trace("target is 101")
        #rhc: 101 is a sink, i.e., it has no children so it will not appear as a source
        # in the file. Need to append a new node if target is out of range, actually 
        # append target - num_nodes. Is this just a coincidence that sink is node 100+1
        # where the gaph is supposed to have 100 nodes?

        # Example: num_nodes is 100 and target is 101, so 101 > 100.
        # But nodes is filled from nodes[0] ... nodes[100] so len(nodes) is 101
        #if (target == 101):
        #    logger.trace ("target is 101, num_nodes is " + str(num_nodes) + " len nodes is "
        #       + str(len(nodes)))
        if target > num_nodes:
            # If len(nodes) is 101 and num_nodes is 100 and we have a tatget of
            # 101, which is a sink, i.e., parents but no children, then there is 
            # no source 101. We use target+1, where 101 - num_nodes = 101 - 100 - 1
            # and target+1 = 101+1 = 102 - len(nodes) = 101 - 101 - 1, so we get 
            # the number_of_nodes_to_append to be 1, as needed.
            if len(nodes) < target+1:
                number_of_nodes_to_append = target - num_nodes
                logger.trace("input_graph: number_of_nodes_to_append:" + str(number_of_nodes_to_append))
                # in our example, number_of_nodes_to_append = 1 so i starts
                # with 0 (default) and ends with number_of_nodes_to_append-1 = 0
                for i in range(number_of_nodes_to_append):
                    logger.trace("input_graph: Node(" + str(num_nodes+i+1) + ")")
                    # new node ID for our example is 101 = num_nodes+i+1 = 100 + 0 + 1 = 101
                    nodes.append(Node((num_nodes+i+1)))
                num_nodes += number_of_nodes_to_append
        #logger.trace ("source:" + str(source) + " target:" + str(target))
        source_node = nodes[source]
        source_node.children.append(target)
        num_children_appends += 1
        target_node = nodes[target]
        target_node.parents.append(source)
        num_parent_appends +=  1

        if generate_networkx_file:
            networkX_line = str(source) + " " + str(target) + '\n'
            networkX_lines.append(networkX_line)

        # Only visualize small graphs
        #temp = [source,target]
        #visual.append(temp)
    
        #logger.trace("Line {}: {}".format(count, line.strip()))

    """
    source_node = nodes[1]
    logger.trace("Node1 children:")
    for child in source_node.children:
        logger.trace(child)
    logger.trace("Node1 parents:")
    for parent in source_node.parents:
        logger.trace(parent)

    source_node = nodes[7]
    logger.trace("Node7 children:")
    for child in source_node.children:
        logger.trace(child)
    logger.trace("Node7 parents:")
    for parent in source_node.parents:
        logger.trace(parent)
    """

    count_child_edges = 0
    i = 1
    while i <= num_nodes:
        node = nodes[i]

#rhc: Too: Note: nodes has num_children so we can use the same pagerank
# computation on a Node that we do on a partition_node. A Node does not 
# really need num_children.
        node.num_children = len(node.children)


        #logger.trace (str(i) + ": get children: " + str(len(node.children)))
        count_child_edges += len(node.children)
        i += 1
    logger.trace("input_graph: num edges in graph: " + str(num_edges) + " = num child edges: " 
        + str(count_child_edges) + " + num_self_loops: " + str(num_self_loops))
    try:
        msg = "[Error]: input_graph: num child edges in graph is " + str(count_child_edges) + " but edges in file is " \
            + str(num_edges)
        assert (num_edges - num_self_loops) == count_child_edges , msg
    except AssertionError:
        logger.exception("[Error]: assertion failed")
        if exit_program_on_exception:
            logging.shutdown()
            os._exit(0)
    #assertOld:
    #if not ((num_edges - num_self_loops) == count_child_edges):
    #    logger.error("[Error]: input_graph: num child edges in graph is " + str(count_child_edges) + " but edges in file is "
    #        + str(num_edges))

    count_parent_edges = 0
    i = 1
    while i <= num_nodes:
        node = nodes[i]
        #logger.trace (str(i) + ": get parents: " + str(len(node.parents)))
        count_parent_edges += len(node.parents)
        i += 1

    logger.trace("input_graph: num_edges in graph: " + str(num_edges) + " = num parent edges: " 
        + str(count_parent_edges) + " + num_self_loops: " + str(num_self_loops))
    
    try:
        msg = "[Error]: input_graph: num parent edges in graph is " + str(count_parent_edges) + " but edges in file is " \
        + str(num_edges)
        assert (num_edges - num_self_loops) == count_parent_edges , msg
    except AssertionError:
        logger.exception("[Error]: assertion failed")
        if exit_program_on_exception:
            logging.shutdown()
            os._exit(0)
    #assertOld:
    #if not ((num_edges - num_self_loops) == count_parent_edges):
    #    logger.error("[Error]: input_graph: num parent edges in graph is " + str(count_parent_edges) + " but edges in file is "
    #    + str(num_edges))

    logger.trace("input_graph: num_parent_appends:" + str(num_parent_appends))
    logger.trace("input_graph: num_children_appends:" + str(num_children_appends))
    logger.trace("input_graph: num_self_loops: " + str(num_self_loops))
    if num_self_loops > 0:
        save_num_edges = num_edges
        num_edges -= + num_self_loops
        logger.trace("input_graph: old num_edges: " + str(save_num_edges) + " num_edges: " + str(num_edges))
    else:
        logger.trace("input_graph: num_edges: " + str(num_edges))

    graph_file.close()

    if generate_networkx_file:
        file = open(fname+"_networkX.txt",'w')
        file.writelines(networkX_lines)
        file.close()   

        """
        nx.write_edgelist(nx.path_graph(4), "test.edgelist")
        G = nx.read_edgelist("test.edgelist")
        or
        fh = open("test.edgelist", "rb")
        G = nx.read_edgelist(fh)
        fh.close()

        edges = [(0, 0), (0, 1), (0, 2), (1, 2), (2, 0), (2, 1), (2, 2)]
        G = nx.DiGraph(edges)
        sorted(nx.simple_cycles(G))
        [[0], [0, 1, 2], [0, 2], [1, 2], [2]]

        G = nx.Graph([(1, 2), (1, 3), (1, 4), (3, 4), (3, 5), (4, 5)])
        nx.diameter(G)
        3

        Algorithms for directed acyclic graphs (DAGs):
        https://networkx.org/documentation/stable/reference/algorithms/dag.html
        
        periphery(G, e=None, usebounds=False, weight=None)[source]
        Returns the periphery of the graph G. The periphery is the set of 
        nodes with eccentricity equal to the diameter.
        """ 

def output_partitions():
    if use_page_rank_group_partitions:
        for name, group in zip(group_names, groups):
            with open('./'+name + '.pickle', 'wb') as handle:
                cloudpickle.dump(group, handle) #, protocol=pickle.HIGHEST_PROTOCOL)  
    else:
        for name, partition in zip(partition_names, partitions):
            with open('./'+name + '.pickle', 'wb') as handle:
                cloudpickle.dump(partition, handle) #, protocol=pickle.HIGHEST_PROTOCOL)  

def input_partitions():
    if use_page_rank_group_partitions:
        group_inputs = []
        for name in group_names:
            with open('./'+name+'.pickle', 'rb') as handle:
                group_inputs.append(cloudpickle.load(handle))
        logger.trace("Group Nodes w/parents:")
        for group in groups:
            for node in group:
                #logger.trace(node,end=":")
                print_val = str(node) + ":"
                for parent in node.parents:
                    print_val += str(parent) + " "
                    #logger.trace(parent,end=" ")
                logger.trace(print_val)
                logger.trace("")
            logger.trace("")
        logger.trace("Group Nodes w/Frontier parent tuples:")
        for group in groups:
            for node in group:
                #logger.trace(node,end=":")
                print_val = str(node) + ":"
                for tup in node.frontier_parents:
                    print_val += str(tup) + " "
                    # logger.trace(tup,end=" ")
                logger.trace(print_val)
                logger.trace("")
            logger.trace("")
    else:
        partition_inputs = []
        for name in partition_names:
            with open('./'+name+'.pickle', 'rb') as handle:
                partition_inputs.append(cloudpickle.load(handle))
        logger.trace("Partition Nodes w/parents:")
        for partition in partitions:
            for node in partition:
                #logger.trace(node,end=":")
                print_val = str(node) + ":"
                for parent in node.parents:
                    print_val += str(parent) + " "
                    #logger.trace(parent,end=" ")
                logger.trace(print_val)
                logger.trace("")
            logger.trace("")
        logger.trace("Partition Nodes w/Frontier parent tuples:")
        for partition in partitions:
            for node in partition:
                #logger.trace(node,end=":")
                print_val = str(node) + ":"
                for tup in node.frontier_parents:
                    print_val += str(tup) + " "
                    # logger.trace(tup,end=" ")
                logger.trace(print_val)
                logger.trace("")
            logger.trace("")
  
# Driver Code

# SCC 8

"""
G = nx.DiGraph()
G.add_edges_from(visual)
logger.trace(nx.is_connected(G))
"""
def PageRank_Function_Main(nodes,total_num_nodes):
    if (debug_pagerank):
        logger.trace("PageRank_Function output partition_or_group (node:parents):")
        for node in nodes:
            #logger.trace(node,end=":")
            print_val = str(node) + ":"
            for parent in node.parents:
                print_val += str(parent) + " "
                #logger.trace(parent,end=" ")
            if len(node.parents) == 0:
                #logger.trace(",",end=" ")
                print_val += ", "
            else:
                #logger.trace(",",end=" ")
                print_val += ", "
            logger.trace(print_val)
        logger.trace("")
        logger.trace("PageRank_Function output partition_or_group (node:num_children):")
        print_val = ""
        for node in nodes:
            print_val += str(node)+":"+str(node.num_children) + ", "
            # logger.trace(str(node)+":"+str(node.num_children),end=", ")
        logger.trace(print_val)
        logger.trace("")
        logger.trace("")
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
            logger.trace("***** PageRank: iteration " + str(i))
            logger.trace("")

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

#PageRank_Function_Main(nodes,num_nodes)
# where if we input 20 nodes, nodes[] has Nodes in nodes[0] .. nodes[21]
# and nodes[] has a length of 21.
# The pagernk computation is the range:
# for index in range(1,num_nodes) so from Node 1 to Node 20, where num_nodes is 21.

#Note:
#Informs the logging system to perform an orderly shutdown by flushing 
#and closing all handlers. This should be called at application exit and no 
#further use of the logging system should be made after this call.
#logging.shutdown()
#os._exit(0)


#Note: 
#In case your main module imports another module in which global 
# variables or class member variables are defined and initialized 
# to (or using) some new objects, you may have to condition that 
# import in the same way:
# as in
"""
    if __name__ == '__main__':       
        import librosa
        import os
        import pandas as pd
        run_my_program()
https://stackoverflow.com/questions/18204782/runtimeerror-on-windows-trying-python-multiprocessing
"""

#Q: Should we guard the BFS.py imports in the above way?

def print_BFS_stats():
    logger.trace("BFS: print_BFS_stats: ")
    #partitions.append(current_partition.copy())
    #frontiers.append(frontier.copy())
    #frontier_cost = "END" + ":" + str(len(frontier))
    #frontier_costs.append(frontier_cost)
    logger.trace("")
    logger.trace("input_file: generated: num_nodes: " + str(num_nodes) + " num_edges: " + str(num_edges))
    logger.trace("")
    logger.trace("visited length: " + str(len(visited)))
    try:
        msg = "[Error]: print_BFS_stats: visited length is " + str(len(visited)) \
            + " but num_nodes is " + str(num_nodes)
        assert not (len(visited) != num_nodes) , msg
    except AssertionError:
        logger.exception("[Error]: assertion failed")
        if exit_program_on_exception:
            logging.shutdown()
            os._exit(0)
    #assertOld:
    #if len(visited) != num_nodes:
    #    logger.error("[Error]: print_BFS_stats: visited length is " + str(len(visited))
    #       + " but num_nodes is " + str(num_nodes))
            
    print_val = ""
    for x in visited:
        print_val += str(x) + " "
        #print(x, end=" ")
    logger.trace(print_val)
    logger.trace("")
    logger.trace("")
    logger.trace("final current_partition length: " + str(len(current_partition)-loop_nodes_added))
    sum_of_partition_lengths = 0
    i = 1
    for x in partitions:
        sum_of_partition_lengths += len(x)
        logger.trace(str(i) + ":length of partition: " + str(len(x)))
        i += 1
    logger.trace("shadow_nodes_added: " + str(num_shadow_nodes_added_to_partitions))
    if not use_shared_partitions_groups:
        sum_of_partition_lengths -= (total_loop_nodes_added + num_shadow_nodes_added_to_partitions)
        logger.trace("sum_of_partition_lengths (not counting total_loop_nodes_added or shadow_nodes and their parents added): " 
            + str(sum_of_partition_lengths))
        try:
            msg = "[Error]: print_BFS_stats: sum_of_partition_lengths is " + str(sum_of_partition_lengths) \
                + " but num_nodes is " + str(num_nodes)
            assert not (sum_of_partition_lengths != num_nodes) , msg
        except AssertionError:
            logger.exception("[Error]: assertion failed")
            if exit_program_on_exception:
                logging.shutdown()
                os._exit(0)
        #if sum_of_partition_lengths != num_nodes:
        #    logger.error("[Error]: print_BFS_stats: sum_of_partition_lengths is " + str(sum_of_partition_lengths)
        #        + " but num_nodes is " + str(num_nodes))
    else: # use_shared_partitions_groups so computing PageRank
        if not use_page_rank_group_partitions:
            if not use_struct_of_arrays_for_pagerank:
                shared_partition_length = len(BFS_Shared.shared_partition)
                # added shadow nodes and their parents
                shared_partition_length -= (total_loop_nodes_added + (2*num_shadow_nodes_added_to_partitions))
                logger.trace("shared_partition_length (not counting total_loop_nodes_added or shadow_nodes and their parents added): " 
                    + str(shared_partition_length))
                try:
                    msg = "[Error]: print_BFS_stats: shared_partition_length is " + str(shared_partition_length) \
                        + " but num_nodes is " + str(num_nodes)
                    + str(num_edges)
                    assert not (shared_partition_length != num_nodes) , msg
                except AssertionError:
                    logger.exception("[Error]: assertion failed")
                    if exit_program_on_exception:
                        logging.shutdown()
                        os._exit(0)
                #assertOld:
                #if shared_partition_length != num_nodes:
                #    logger.error("[Error]: print_BFS_stats: shared_partition_length is " + str(shared_partition_length)
                #        + " but num_nodes is " + str(num_nodes))
            else:
                pass
                # we are not assserting anything about the length of the arrays
                # in the struct_of_arrays. These arrays length were calculated
                # and we are not checking that calculation here.

    logger.trace("")
    sum_of_groups_lengths = 0
    i = 1
    for x in groups:
        sum_of_groups_lengths += len(x)
        logger.trace(str(i) + ": length of group: " + str(len(x)))
        i+=1
    logger.trace("num_shadow_nodes_added_to_groups: " + str(num_shadow_nodes_added_to_groups))
    if not use_shared_partitions_groups:
        logger.trace("total_loop_nodes_added : " + str(total_loop_nodes_added))
        sum_of_groups_lengths -= (total_loop_nodes_added + num_shadow_nodes_added_to_groups)
        logger.trace("sum_of_groups_lengths (not counting total_loop_nodes_added or shadow_nodes and their parents added): " 
            + str(sum_of_groups_lengths))
        try:
            msg = "[Error]: print_BFS_stats: sum_of_groups_lengths is " + str(sum_of_groups_lengths) \
                + " but num_nodes is " + str(num_nodes)
            assert not (sum_of_groups_lengths != num_nodes) , msg
        except AssertionError:
            logger.exception("[Error]: assertion failed")
            if exit_program_on_exception:
                logging.shutdown()
                os._exit(0)
        #if sum_of_groups_lengths != num_nodes:
        #    logger.error("[Error]: print_BFS_stats: sum_of_groups_lengths is " + str(sum_of_groups_lengths)
        #        + " but num_nodes is " + str(num_nodes))
    else: # use_shared_partitions_groups so computing PageRank
        if use_page_rank_group_partitions:
            if not use_struct_of_arrays_for_pagerank:
                shared_groups_length = len(BFS_Shared.shared_groups)
                logger.trace("shared_groups_length first value: " + str(shared_groups_length))
                # added shadow nodes and their parents
                logger.trace("total_loop_nodes_added : " + str(total_loop_nodes_added))
                logger.trace("(2*num_shadow_nodes_added_to_groups):" + str(2*num_shadow_nodes_added_to_groups))
                shared_groups_length -= (total_loop_nodes_added + (2*num_shadow_nodes_added_to_groups))
                logger.trace("shared_groups_length (not counting total_loop_nodes_added or shadow_nodes and their parents added): " 
                    + str(shared_groups_length))
                try:
                    msg = "[Error]: print_BFS_stats: sum_of_groups_lengths is " + str(sum_of_groups_lengths) \
                        + " but num_nodes is " + str(num_nodes)
                    assert not (shared_groups_length != num_nodes) , msg
                except AssertionError:
                    logger.exception("[Error]: assertion failed")
                    if exit_program_on_exception:
                        logging.shutdown()
                        os._exit(0)
                #if shared_groups_length != num_nodes:
                #    logger.error("[Error]: print_BFS_stats: shared_groups_length is " + str(shared_groups_length)
                #       + " but num_nodes is " + str(num_nodes))
            else:
                pass
                # we are not assserting anything about the length of the arrays
                # in the struct_of_arrays. These arrays length were calculated
                # and we are not checking that calculation here.

    #if (len(current_partition)-loop_nodes_added) != num_nodes

    print_val = ""
    for x in current_partition:
        print_val += str(x) + " "
        # logger.trace(x, end=" ")
        logger.trace(print_val)
        logger.trace("")

    # adjusting for loop_nodes_added in dfs_p
    sum_of_changes = sum(dfs_parent_changes_in_partiton_size)-num_shadow_nodes_added_to_partitions
    avg_change = sum_of_changes / len(dfs_parent_changes_in_partiton_size)
    print_val = "dfs_parent_changes_in_partiton_size length, len: " + str(len(dfs_parent_changes_in_partiton_size)) + ", sum_of_changes: " + str(sum_of_changes)
    print_val += ", average dfs_parent change: %.1f" % avg_change
    logger.trace(print_val)
    if PRINT_DETAILED_STATS:
        if sum_of_changes != num_nodes:
            logger.error("[Error]: print_BFS_stats: sum_of_changes is " + str(sum_of_changes)
                + " but num_nodes is " + str(num_nodes))
        print_val = ""
        for x in dfs_parent_changes_in_partiton_size:
            print_val += str(x) + " "
            # print(x, end=" ")
        logger.trace(print_val)

    logger.trace("")
    logger.trace("")
    if PRINT_DETAILED_STATS:
        # adjusting for loop_nodes_added in dfs_p
        sum_of_changes = sum(dfs_parent_changes_in_frontier_size)
        logger.trace("dfs_parent_changes_in_frontier_size length, len: " + str(len(dfs_parent_changes_in_frontier_size))
            + ", sum_of_changes: " + str(sum_of_changes))
        if sum_of_changes != num_nodes:
            logger.error("[Error]: print_BFS_stats: sum_of_changes is " + str(sum_of_changes)
                + " but num_nodes is " + str(num_nodes))
        for x in dfs_parent_changes_in_frontier_size:
            print_val = str(x) + " "
            #print(x, end=" ")
        logger.trace(print_val)
        logger.trace("")
        logger.trace("")
    #logger.trace("frontier length: " + str(len(frontier)))
    #if len(frontier) != 0:
    #    logger.error("[Error]: print_BFS_stats: frontier length is " + str(len(frontier))
    #       + " but num_nodes is " + str(num_nodes))
    #for x in frontier:
    #    logger.trace(str(x.ID), end=" ")
    #logger.trace("")
    #logger.trace("frontier cost: " + str(len(frontier_cost)))
    #for x in frontier_cost:
    #    logger.trace(str(x), end=" ")
    #logger.trace("")
    # final frontier should always be empty

    try:
        logger.trace("frontiers: (final fronter should be empty), number of frontiers: " + str(len(frontiers))+ " (length):")
        for frontier_list in frontiers:
            if PRINT_DETAILED_STATS:
                print_val = "-- (" + str(len(frontier_list)) + "): "
                for x in frontier_list:
                    #logger.trace(str(x.ID),end=" ")
                    print_val += str(x) + " "
                    #print(str(x),end=" ")
                logger.info(print_val)
                logger.info("")
            else:
                logger.info("-- (" + str(len(frontier_list)) + ")") 
        frontiers_length = len(frontiers)
        msg = "[Error]: print_BFS_stats: final frontier is not empty."
        assert not len(frontiers[frontiers_length-1]) != 0 , msg
    except AssertionError:
        logger.exception("[Error]: assertion failed")
        if exit_program_on_exception:
            logging.shutdown()
            os._exit(0)
    # assertOld: 
    #logger.info("frontiers: (final fronter should be empty), number of frontiers: " + str(len(frontiers))+ " (length):")
    #for frontier_list in frontiers:
    #    if PRINT_DETAILED_STATS:
    #        print_val = "-- (" + str(len(frontier_list)) + "): "
    #        for x in frontier_list:
    #            #logger.trace(str(x.ID),end=" ")
    #            print_val += str(x) + " "
    #            #print(str(x),end=" ")
    #        logger.info(print_val)
    #        logger.info("")
    #    else:
    #        logger.info("-- (" + str(len(frontier_list)) + ")") 
    #frontiers_length = len(frontiers)
    #if len(frontiers[frontiers_length-1]) != 0:
    #    logger.error ("[Error]: print_BFS_stats: final frontier is not empty.")

    logger.info("")
    logger.info("partitions, number of partitions: " + str(len(partitions))+" (length):")

    for x in partitions:
        if PRINT_DETAILED_STATS:
            #print("-- (" + str(len(x)) + "):", end=" ")
            print_val = ""
            print_val += "-- (" + str(len(x)) + "):" + " "
            for node in x:
                print_val += str(node) + " "
                #print(node,end=" ")
                #if not node.isShadowNode:
                #    logger.trace(str(index),end=" ")
                #else:
                #   logger.trace(str(index)+"-s",end=" ")
            logger.info(print_val)
            logger.info("")
        else:
            logger.info("-- (" + str(len(x)) + ")")
    logger.info("")
    if use_shared_partitions_groups:
        logger.trace("Number of shadow nodes (when use_shared_partitions_groups):")
        for num in partitions_num_shadow_nodes_list:
            logger.trace(num)
        logger.trace("")
    logger.trace("partition names, len: " + str(len(partition_names))+":")
    for name in partition_names:
        if PRINT_DETAILED_STATS:
            logger.trace("-- " + name)
    logger.info("")
    logger.info("groups, len: " + str(len(groups))+":")
    for g in groups:
        if PRINT_DETAILED_STATS:
            print_val = ""
            print_val += "-- (" + str(len(g)) + "):" + " "
            for node in g:
                print_val += str(node) + " "
                #print(node,end=" ")
            logger.info(print_val)
            logger.info("")
        else:
            logger.info("-- (" + str(len(g)) + ")")
    logger.info("")
    if use_shared_partitions_groups:
        logger.trace("Number of shadow nodes (when use_shared_partitions_groups):")
        for num in groups_num_shadow_nodes_list:
            logger.trace(num)
        logger.trace("")
    logger.trace("group names, len: " + str(len(group_names))+":")
    for name in group_names:
        if PRINT_DETAILED_STATS:
            logger.trace("-- " + name)
    logger.trace("")
    logger.trace("nodes_to_partition_maps (incl. shadow nodes but only last index), len: " + str(len(nodeIndex_to_partitionIndex_maps))+":")
    for m in nodeIndex_to_partitionIndex_maps:
        if PRINT_DETAILED_STATS:
            print_val = ""
            print_val += "-- (" + str(len(m)) + "):" + " "
            for k, v in m.items():
                print_val += str((k, v)) + " "
                #print((k, v),end=" ")
            logger.trace(print_val)
            logger.trace("")
        else:
            logger.trace("-- (" + str(len(m)) + ")")
    logger.trace("")
    logger.trace("nodes_to_group_maps, ( but only last index), len: " + str(len(nodeIndex_to_groupIndex_maps))+":")
    for m in nodeIndex_to_groupIndex_maps:
        if PRINT_DETAILED_STATS:
            #print("-- (" + str(len(m)) + "):", end=" ")
            print_val = ""
            print_val += "-- (" + str(len(m)) + "):" + " "
            for k, v in m.items():
                print_val += str((k, v)) + " "
                #print((k, v),end=" ")
            logger.trace(print_val)
            logger.trace("")
        else:
            logger.trace("-- (" + str(len(m)) + ")")
    logger.trace("")
    if PRINT_DETAILED_STATS:
        logger.trace("frontier costs (cost=length of frontier), len: " + str(len(frontier_costs))+":")
        print_val = ""
        for x in frontier_costs:
            print_val += "-- " + str(x)
            #logger.trace("-- ",end="")
            #logger.trace(str(x))
        logger.trace(print_val)
        logger.trace("")
    sum_of_partition_costs = 0
    for x in all_frontier_costs:
        words = x.split(':')
        cost = int(words[1])
        sum_of_partition_costs += cost
    logger.trace("all frontier costs, len: " + str(len(all_frontier_costs)) + ", sum: " 
        + str(sum_of_partition_costs))
    if PRINT_DETAILED_STATS:
        i = 0
        costs_per_line = 13
        print_val = ""
        for x in all_frontier_costs:
            if (i < costs_per_line):
                print_val = str(x) + " "
                #print(str(x),end=" ")
            else:
                logger.trace(str(x))
                i = 0
            i += 1
        logger.trace(print_val)
    logger.trace("")
    """
    # Doing this for each node in each partition now (next)
    logger.trace("")
    if PRINT_DETAILED_STATS:
        logger.trace("Node frontier_parent_tuples:")
        for node in nodes:
            logger.trace(str(node.ID) + ": frontier_parent_tuples: ", end = " ")
            for parent_tuple in node.frontier_parents:
                logger.trace(str(parent_tuple), end=" ")
            logger.trace("")
    else:
        logger.trace("-- (" + str(len(x)) + ")")
    """
    logger.info("")
    if PRINT_DETAILED_STATS:
        logger.info("partition nodes' frontier_parent_tuples:")
        for x in partitions:
            if PRINT_DETAILED_STATS:
                print_val = "-- (" + str(len(x)) + "):" + " "
                print_val = ""
                for node in x:
                    print_val += str(node.ID) + ": " 
                    # logger.trace(node.ID,end=": ")
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
                    # logger.trace(node.ID,end=": ")
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
    logger.trace("frontier_groups_sum: " + str(frontier_groups_sum) + ", len(frontiers)-1: " 
        +  str(len(frontiers)-1))
    logger.trace("Average number of frontier groups: " + (str(frontier_groups_sum / (len(frontiers)-1))))
    logger.trace("")
    i#f True: # 
    if use_shared_partitions_groups: 
        logger.trace("Shared partition map frontier_parent_tuples:")                 
        for (k,v) in BFS_Shared.shared_partition_frontier_parents_map.items():
            logger.trace(str(k) + ": " + str(v))
        logger.trace("")
    #if True: # 
    if use_shared_partitions_groups:  
        logger.trace("Shared groups map frontier_parent_tuples:")                  
        for (k,v) in BFS_Shared.shared_groups_frontier_parents_map.items():
            logger.trace(str(k) + ": " + str(v))
        logger.trace("")
    logger.trace("nodeIndex_to_partition_partitionIndex_group_groupIndex_map, len: " + str(len(nodeIndex_to_partition_partitionIndex_group_groupIndex_map)) + ":")
    logger.trace("shadow nodes not mapped and not shown")
    if PRINT_DETAILED_STATS:
        for k, v in nodeIndex_to_partition_partitionIndex_group_groupIndex_map.items():
            logger.trace((k, v))
        logger.trace("")
    else:
        logger.trace("-- (" + str(len(nodeIndex_to_partition_partitionIndex_group_groupIndex_map)) + ")")
    logger.info("")
    logger.info("Partition Node parents (shad. node is a parent), len: " + str(len(partitions))+":")
    for x in partitions:
        if PRINT_DETAILED_STATS:
            #logger.trace("-- (" + str(len(x)) + "):", end=" ")
            for node in x:
                print_val = ""
                print_val += str(node) + ": "
                #print(node,end=":")
                for parent in node.parents:
                    print_val += str(parent) + " "
                    #print(parent,end=" ")
                logger.info(print_val)
                logger.info("")
                #if not node.isShadowNode:
                #    logger.trace(str(index),end=" ")
                #else:
                #   logger.trace(str(index)+"-s",end=" ")
            logger.info("")
        else:
            logger.info("-- (" + str(len(x)) + ")")
    logger.info("")
    logger.info("Partition Node num_children, len: " + str(len(partitions))+":")
    for x in partitions:
        if PRINT_DETAILED_STATS:
            #logger.trace("-- (" + str(len(x)) + "):", end=" ")
            print_val = ""
            for node in x:
                print_val += str(node) + ":" + str(node.num_children) + ", "
                #print(str(node) + ":" + str(node.num_children),end=", ")
            logger.info(print_val)
            logger.info("")
        else:
            logger.info("-- (" + str(len(x)) + ")")
    logger.info("")
    logger.info("Group Node parents (shad. node is a parent), len: " + str(len(partitions))+":")
    for x in groups:
        if PRINT_DETAILED_STATS:
            #logger.trace("-- (" + str(len(x)) + "):", end=" ")
            for node in x:
                print_val = ""
                print_val += str(node) + ": "
                #print(node,end=":")
                for parent in node.parents:
                    print_val += str(parent) + " "
                    #print(parent,end=" ")
                logger.info(print_val)
                logger.info("")
                #if not node.isShadowNode:
                #    logger.trace(str(index),end=" ")
                #else:
                #   logger.trace(str(index)+"-s",end=" ")
            logger.info("")
        else:
            logger.info("-- (" + str(len(x)) + ")")
    logger.info("")
    logger.info("Group Node num_children, len: " + str(len(groups))+":")
    for x in groups:
        if PRINT_DETAILED_STATS:
            #logger.trace("-- (" + str(len(x)) + "):", end=" ")
            print_val = ""
            for node in x:
                print_val += str(node) + ":" + str(node.num_children) + ", "
                #print(str(node) + ":" + str(node.num_children),end=", ")
            logger.info(print_val)
            logger.info("")
        else:
            logger.info("-- (" + str(len(x)) + ")")
    logger.info("")
    logger.trace("Partition_senders, len: " + str(len(BFS_generate_DAG_info.Partition_senders)) + ":")
    if PRINT_DETAILED_STATS:
        for k, v in BFS_generate_DAG_info.Partition_senders.items():
            logger.trace((k, v))
        logger.trace("")
    else:
        logger.trace("-- (" + str(len(BFS_generate_DAG_info.Partition_senders)) + ")")
        logger.trace("")
    logger.trace("Partition_receivers, len: " + str(len(BFS_generate_DAG_info.Partition_receivers)) + ":")
    if PRINT_DETAILED_STATS:
        for k, v in BFS_generate_DAG_info.Partition_receivers.items():
            logger.trace((k, v))
        logger.trace("")
    else:
        logger.trace("-- (" + str(len(BFS_generate_DAG_info.Partition_receivers)) + ")")
        logger.trace("")
    logger.trace("Group_senders, len: " + str(len(BFS_generate_DAG_info.Group_senders)) + ":")
    if PRINT_DETAILED_STATS:
        for k, v in BFS_generate_DAG_info.Group_senders.items():
            logger.trace((k, v))
        logger.trace("")
    else:
        logger.trace("-- (" + str(len(BFS_generate_DAG_info.Group_senders)) + ")")
        logger.trace("")

    logger.trace("Group_receivers, len: " + str(len(BFS_generate_DAG_info.Group_receivers)) + ":")
    if PRINT_DETAILED_STATS:
        for k, v in BFS_generate_DAG_info.Group_receivers.items():
            logger.trace((k, v))
    else:
        logger.trace("-- (" + str(len(BFS_generate_DAG_info.Group_receivers)) + ")")
        logger.trace("")

def main():
    if use_page_rank_group_partitions:
        logger.info("BFS: using groups")
    else:
        logger.info("BFS: using partitions.")

#0
    global num_nodes

    logger.trace("BFS: Following is the Breadth-First Search")
    input_graph()
    logger.trace("BFS: num_nodes after input graph: " + str(num_nodes))
    #visualize()
    #input('Press <ENTER> to continue')

    #rhc: Int BFS_shared before starting BFS. If we add to struct of arrays
    # during dfs_parent, we will eventually need those inits earlier too.
    if use_shared_partitions_groups:
        BFS_Shared.initialize()

    if use_multithreaded_BFS:
        global DAG_generator_for_multithreaded_DAG_generation
        DAG_generator_for_multithreaded_DAG_generation = DAG_Generator_Multithreaded()
        DAG_generator_for_multithreaded_DAG_generation.start_thread()
    
    # we are only using incremental_DAG_generation when we
    # are computing pagerank, so far. Pagerank DAGS are the
    # only DAGS we generate ourselves, so far.
    if compute_pagerank and use_incremental_DAG_generation:
        # create the connections to tcp_server needed for  
        if (run_all_tasks_locally and using_workers and not using_threads_not_processes): 
            # Config: A5, A6
            # sent the create() for work_queue to the tcp server in the DAG_executor_driver
            websocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            websocket.connect(TCP_SERVER_IP)
            DAG_infobuffer_monitor = Remote_Client_for_DAG_infoBuffer_Monitor(websocket)
            DAG_infobuffer_monitor.create()
            logger.info("BFS: created Remote DAG_infobuffer_monitor.")
        elif not run_all_tasks_locally:
            websocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            websocket.connect(TCP_SERVER_IP)
            DAG_infobuffer_monitor = Remote_Client_for_DAG_infoBuffer_Monitor_for_Lambdas(websocket)
            DAG_infobuffer_monitor.create_Remote_Client()
            logger.trace("BFS: created Remote DAG_infobuffer_monitor_for_lambdas.")

#00
    global visited
    global nodes
    #bfs(visited, graph, '5')    # function calling
    # example: num_nodes = 100, so Nodes in nodes[1] to nodes[100]
    # i start = 1 as nodes[0] not used, i end is (num_nodes+1) - 1  = 100
    for i in range(1,num_nodes+1):
        if i not in visited:
            logger.trace("*************BFS Driver call BFS for node[" + str(i) + "]")
            #bfs(visited, graph, nodes[i])    # function calling
            bfs(visited, nodes[i])    # function calling

# rhc : ******* Partition

    # This should never happen? If it can, we need to add the 
    # partition/group to incremental DAG if we are generating
    # DAGs incrementally. 
    # When we pop a -1 from the BFSQueue
    # the currrent partition is complete - we collect the 
    # current partition and process it. If the BFSQueue is 
    # now empty, we break the while(BFS_queue) loop which
    # ends bfs() and brings us here. As part of procssing 
    # the current partition, we set current_partition to []
    # and do frontier.clear(). So len(current_partition) should
    # be 0 and likewise for frontier.
    #      
    # Do last partition/group if there is one.

#1
    global current_partition
    try:
        msg = "[Error]: bfs: len(current_partition) > 0" + " after last call to bfs."
        assert not (len(current_partition) > 0), msg
    except AssertionError:
        logger.exception("[Error]: assertion failed")
        if exit_program_on_exception:
            logging.shutdown()
            os._exit(0)

    # assertOld: This should not be possible, assserted above.
    # If the assertion fails determine why.
    if len(current_partition) > 0:
    #    logger.error("[Error]: bfs: len(current_partition) > 0"
    #        + " after last call to bfs.")
    #    logging.shutdown()
    #    os._exit(0)

        logger.trace("BFS: create final sub-partition")
        # does not require a deepcopy
#2
        global partitions
        global current_partition_number
        partitions.append(current_partition.copy())

        #rhc shared: added all the name stuff - should have been there
        partition_name = "PR" + str(current_partition_number) + "_1"
        #global current_partition_isLoop
#3
        global current_partition_isLoop
        if current_partition_isLoop:
            # These are the names of the partitions that have a loop. In the 
            # DAG, we will append an 'L' to the name. Not using this anymore.
            partition_name = partition_name + "L"
            Partition_loops.add(partition_name)

        current_partition_isLoop = False
#4
        global partition_names
        partition_names.append(partition_name)

#rhc: clustering
        # Note: need the partition name here.
        # Need to know the number of shadow nodes when clustering in order
        # to compute the number of non-shadow nodes (nodies-shadow_nodes = non-shadow nodes)
        #if use_shared_partitions_groups:
#5
        global partitions_num_shadow_nodes_list
        global end_num_shadow_nodes_for_partitions
        global start_num_shadow_nodes_for_partitions
        global num_shadow_nodes_added_to_partitions
        if use_shared_partitions_groups or enable_runtime_task_clustering:
            #rhc shared
            end_num_shadow_nodes_for_partitions = num_shadow_nodes_added_to_partitions
            change_in_shadow_nodes_for_partitions = end_num_shadow_nodes_for_partitions - start_num_shadow_nodes_for_partitions
            partitions_num_shadow_nodes_list.append(change_in_shadow_nodes_for_partitions)
            size_of_partition = len(current_partition) - change_in_shadow_nodes_for_partitions
            BFS_generate_DAG_info.partitions_num_shadow_nodes_map[partition_name] = size_of_partition
            # not needed here since we are done but kept to be consisent with use above
            start_num_shadow_nodes_for_partitions = num_shadow_nodes_added_to_partitions

        current_partition = []

# rhc : ******* Group
# ToDo: if len(current_group) > 0:
        
#6
        global groups
        global current_group
        global current_group_number
        global current_group_isLoop
        global Group_loops
        global group_names
        groups.append(current_group)

        group_name = "PR" + str(current_partition_number) + "_" + str(current_group_number)
        if current_group_isLoop:
            # These are the names of the groups that have a loop. In the 
            # DAG, we will append an 'L' to the name. Not used since we 
            # use loop names (with 'L") as we generate Sender and Recevers.
            # instead of modifying the names of senders/receievers before we 
            # generate the DAG.
            group_name = group_name + "L"
            Group_loops.add(group_name)

        current_group_isLoop = False
        group_names.append(group_name)

#7
        global end_num_shadow_nodes_for_groups
        global groups_num_shadow_nodes_list
        global start_num_shadow_nodes_for_groups
        global num_shadow_nodes_added_to_groups
#rhc: clustering
        # Note: need the group name here.
        #if use_shared_partitions_groups:
        if use_shared_partitions_groups or enable_runtime_task_clustering:
            #rhc shared
            end_num_shadow_nodes_for_groups = num_shadow_nodes_added_to_groups
            change_in_shadow_nodes_for_groups = end_num_shadow_nodes_for_groups - start_num_shadow_nodes_for_groups
            groups_num_shadow_nodes_list.append(change_in_shadow_nodes_for_groups)
            size_of_group = len(current_group) - change_in_shadow_nodes_for_groups
            BFS_generate_DAG_info.groups_num_shadow_nodes_map[group_name] = size_of_group
            # not needed here since we are done but kept to be consisent with use above
            start_num_shadow_nodes_for_groups = num_shadow_nodes_added_to_groups

#8
        global nodeIndex_to_partitionIndex_maps
        global nodeIndex_to_partitionIndex_map
        global nodeIndex_to_groupIndex_maps
        global nodeIndex_to_groupIndex_map
        current_group = []

        nodeIndex_to_partitionIndex_maps.append(nodeIndex_to_partitionIndex_map)
        nodeIndex_to_partitionIndex_map = {}
        nodeIndex_to_groupIndex_maps.append(nodeIndex_to_groupIndex_map)
        nodeIndex_to_groupIndex_map = {}

#9
        global total_loop_nodes_added
        global frontiers
        global frontier
        global frontier_costs
        #global total_loop_nodes_added
        # if we didn't call dfs_parent() can this be non-zero?
        total_loop_nodes_added += loop_nodes_added
        # use loop_nodes_added below when printing stats so do not reset it
        # and besides BFS is done.
        #loop_nodes_added = 0
        # does not require a deepcopy
        frontiers.append(frontier.copy())
        frontier_cost = "atEnd:" + str(len(frontier))
        frontier_costs.append(frontier_cost)
    else:
        try:
            msg = "[Error]: bfs: len(frontier) > 0" \
                + " after last call to bfs."
            assert not (len(frontier) > 0) , msg
        except AssertionError:
            logger.exception("[Error]: assertion failed")
            if exit_program_on_exception:
                logging.shutdown()
                os._exit(0)
        #assertOld:
        #if len(frontier) > 0:
        #    logger.error("[Error]: bfs: len(frontier) > 0"
        #        + " after last call to bfs.")
        #    logging.shutdown()
        #    os._exit(0)

        # Always do this - below we asssert final frontier is empty.
        # Does not require a deepcopy.
        # ToDo: We can stop doing this and remove the asssertion
        # in print_BFS_stats().
        frontiers.append(frontier.copy())

    # generate shared array of partitions/groups if using multithreaded workers
    # or threads to simulate lambdas

#10
    global num_parent_appends
    if use_shared_partitions_groups:
        generate_shared_partitions_groups(num_nodes,num_parent_appends,partitions,partition_names,
            partitions_num_shadow_nodes_list,num_shadow_nodes_added_to_partitions,
            groups, group_names,groups_num_shadow_nodes_list,num_shadow_nodes_added_to_groups)

#rhc incremental
    if not use_incremental_DAG_generation:

        if not use_multithreaded_BFS:
            print_BFS_stats()
            #logging.shutdown()
            #os._exit(0)
            BFS_generate_DAG_info.generate_DAG_info()
        else:
            # started the DAG_generator_for_multithreaded_DAG_generation
            # at the start of bfs() execution.
            DAG_generator_for_multithreaded_DAG_generation.join_thread()
            print_BFS_stats()

        # old: do this if not incremental dag geeration. New: 
        # if doing multithhreaded bfs then we aer not using 
        # generate_DAG_info() and we print BFS stats after
        # bfs() and the generator thread are both finished.
        # We are bfs() so we just need to join the generator 
        # thread.
        #print_BFS_stats()
        #BFS_generate_DAG_info.generate_DAG_info()

        #visualize()
        #input('Press <ENTER> to continue')

        logger.info("Output partitions/groups")
        output_partitions()

        # Calling the run() method of DAG_executor_driver. Assuming we have
        # already started tcp_server. The tcp_server needs DAG_info but
        # tcp_server does not read DAG_info when it starts since we can't
        # read DAG_info until after bfs() writes it. So tcp_server will
        # read DAG_info the first time it needs it.
        # Alternative: start tcp_server and have tcp_server call bfs()
        # then bfs() can signal tcp_server when bfs after bfs writes
        # DAG_info. tcp_server can read DAG_info and call DAG_executor
        # _driver's run() method. This is for non-incremental. For
        # incremental DAG generation, tcp_server would call bfs() and 
        # do nothing else since in that case bfs() is calling 
        # DAG_executor_driver after bfs write the first incremental DAG.

        run()

        if use_struct_of_arrays_for_pagerank and use_shared_partitions_groups and not using_threads_not_processes:
            logger.trace("\nBFS:Close and unlink shared memory.")
            try:
                BFS_Shared.close_shared_memory()
                BFS_Shared.unlink_shared_memory()
            except Exception:
                logger.exception("[Error]: PageRank_Function:"
                    + " BFS: Failed to close or unlink shared memory.")
                if exit_program_on_exception:
                    logging.shutdown()
                    os._exit(0)
    else:
#11
        global invoker_thread_for_DAG_executor_driver

        # bfs generates the DAG incrementally, i.e., the DAG_executor
        # and bfs() overlap their executions. bfs depsoits incremental
        # DAGs in a bounded buffer that DAG_executor withdraws.
        # bfs will call DAG_executor_driver's run() method after bfs 
        # writes the first incremental DAG.
        logger.trace("\nBFS:join invoker_thread_for_DAG_executor_driver.")
        invoker_thread_for_DAG_executor_driver.join()   # global
        # 1. perhaps invoker_thread.join() here when inc dag gen
        logger.trace("\nBFS:join after join, print BFS stats")
    
        print_BFS_stats()

    if check_pagerank_output:
        # True when: comuting pagerank and using thread workers/lambdas
        # compute_pagerank and run_all_tasks_locally and (using_workers or not using_workers) and using_threads_not_processes
        if use_page_rank_group_partitions:
            number_of_groups_or_partitions = len(groups)
        else:
            number_of_groups_or_partitions = len(partitions) 
        _verified = verify_pagerank_outputs(number_of_groups_or_partitions)
        
        #if not _verified:
        #   might do somethig with this

        logger.info("")
        logger.info("")
        logger.info("DAG_executor_outputs:")
        pr_outputs = get_pagerank_outputs()
        output_keys = list(pr_outputs.keys())
        output_keys.sort()
        sorted_pagerank_outputs = {i: pr_outputs[i] for i in output_keys}
        if use_page_rank_group_partitions:
            for (k,v) in sorted_pagerank_outputs.items():
                logger.info(str(k) + ":" + group_names[k-1] + ":" +str(v))
        else:
            for (k,v) in sorted_pagerank_outputs.items():
                logger.info(str(k) + ":" + partition_names[k-1] + ":" +str(v))



#rhc: ToDo: put this code in main() so TestAll can call BFS.main()
if __name__ == '__main__':
    main()

"""
logger.trace("Sorted simple cycles:")
G = nx.read_edgelist("graph_3000_networkX.txt", create_using=nx.DiGraph)
sorted(nx.simple_cycles(G))
# diameter will fail if the directed graph is not strongly connected.
#"You cannot compute diameter for either 1) a weakly-connected directed graph or 2) a disconnected graph"
# https://stackoverflow.com/questions/33114746/why-does-networkx-say-my-directed-graph-is-disconnected-when-finding-diameter
#nx.diameter(G)
#This finds the maximum distance of a list containing the shortest paths 
#between any two nodes in G (computed with Dijkstra's algorithm), regardless of 
#what component they may belong to. Technically, diameter is infinite for 
#disconnected graphs which is why NetworkX's built-in method does not work. 
#The method above will find the largest diameter amongst all components within 
#G, but is not the diameter of G itself.
diameter = max([max(j.values()) for (i,j) in nx.shortest_path_length(G)])
logger.trace("Diameter:" + str(diameter))
aspl = mean([max(j.values()) for (i,j) in nx.shortest_path_length(G)])
logger.trace("avg shortest path lengh:" + str(aspl))

# (node for node, in_degree in G.in_degree() if in_degree == 0)
"""