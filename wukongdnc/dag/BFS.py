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
            logger.debug("*************Driver call BFS for node[" + str(i) + "]")
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
    leaf_tasks_of_groups.add(group_name)

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
                    leaf_tasks_of_partitions.add(partition_name)
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

    logger.debug("Following is the Breadth-First Search")
    input_graph()
    logger.debug("num_nodes after input graph: " + str(num_nodes))

    for i in range(1,num_nodes+1):
        if i not in visited:
            bfs(visited, nodes[i])    # function calling

    # hande the last partition/group that was generated, same as above
    if len(current_partition) > 0:
        logger.debug("BFS: create final sub-partition")

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
            logger.debug("parent_node: " + str(parent_node))

            #pg_tuple is a map that accumlates information about each node.
            pg_tuple = None

            if parent_node.ID not in visited:
                logger.debug ("dfs_parent visit parent node " + str(parent_node.ID))
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

                logger.debug ("dfs_parent parent " + str(parent_node.ID) + " of " + str(node.ID) + " already visited"
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
                    logger.debug("XXXXXXXXXXXXXXXXX dfs_parent: Loop Detected: "
                        + "PR" + str(current_partition_number) + "_" + str(num_frontier_groups))
    # rhc : ******* Partition
                    global current_partition_isLoop
                    current_partition_isLoop = True

    # rhc : ******* Group
                    global current_group_isLoop
                    current_group_isLoop = True
                else:
                    logger.debug("YYYYYYYYYYYYY dfs_parent: No Loop Detected (yet): "
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

                logger.debug ("dfs_parent: parent in same partition: parent_partition_number: " 
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

import logging 
import cloudpickle
import threading
#import os
#import time
#from statistics import mean

import copy

from .BFS_Node import Node
from .BFS_Partition_Node import Partition_Node
from .BFS_generate_DAG_info import generate_DAG_info
from .BFS_generate_DAG_info_incremental_partitions import generate_DAG_info_incremental_partitions
from .BFS_generate_DAG_info import Partition_senders, Partition_receivers, Group_senders, Group_receivers
from .BFS_generate_DAG_info import leaf_tasks_of_partitions, leaf_tasks_of_groups
from .BFS_generate_shared_partitions_groups import generate_shared_partitions_groups
from .DAG_infoBuffer_Monitor_for_threads import DAG_infobuffer_monitor


#rhc shared
#from .DAG_executor import shared_partition, shared_groups
#from .DAG_executor import shared_partition_map, shared_groups_map
#from .Shared import shared_partition, shared_groups, shared_partition_map,  shared_groups_map
from . import BFS_Shared

from .DAG_executor_constants import use_shared_partitions_groups, use_page_rank_group_partitions
from .DAG_executor_constants import use_struct_of_arrays_for_pagerank, compute_pagerank
from .DAG_executor_constants import use_incremental_DAG_generation, using_workers
from .DAG_executor_driver import run

#from .DAG_executor_constants import run_all_tasks_locally, using_threads_not_processes

logger = logging.getLogger(__name__)

logger.setLevel(logging.DEBUG)
#logger.setLevel(logging.INFO)
formatter = logging.Formatter('[%(asctime)s] [%(module)s] [%(processName)s] [%(threadName)s]: %(message)s')
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
#ch.setLevel(logging.INFO)
ch.setFormatter(formatter)
logger.addHandler(ch)

USING_BFS = False

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
# node i in nodes is in position i. When we place a node in a partition/group, 
# this node is not assumed to be in postion i; nodes are added to the partition/group
# one by one using append. We map node i, whch we know is at position i in nodes,
# to its position in its partition/group. Example node 25 in nodes at position 25 is mapped 
# to position 4 in its partition/group.
# Note: we map shadow nodes to their positions too. We do not map shadow nodes 
# in the global map nodeIndex_to_partition_partitionIndex_group_groupIndex_map since
# a shadow node ID and a non-shadow node for ID would have the same key. We could 
# use string keys and use, e.g, "5" and "5s" for "shadow" so the keys would be unique.
# Note: A shadow node can be addedmore than once, in which case the index of the 
# shadow node will be its last index, e.g., if shadow node in positions 0 and 2 its
# index will be 2. We do not use the shadow node's index'
nodeIndex_to_partitionIndex_map = {}
nodeIndex_to_groupIndex_map = {}
# collection of all nodes_to_group_map maps, one for each group
nodeIndex_to_partitionIndex_maps = []
nodeIndex_to_groupIndex_maps = []
# map a node to its partition number, partition index, group number ans group index.
# A "global map"for nodes. May supercede nodeIndex_to_partitionIndex_map. We need
# a nodes position in its partition if we map partitions to functions and we need
# a nodes position in its group if we map groups to functions. This map supports
# both partition mapping and group mapping.
# Note: We do not map shadow nodes in this map.
# Q: We can remove the nodes in Pi from this map after we have finished 
# computing Pi+1 since we will no longer need to know this info for 
# the nodes in Pi? We may want to remove these nodes to free the space.
# Note: If a node is in Pi+1 all of its parents are in Pi+1 or Pi,
# by definition, since Pi+1 contains all the children of Pi and
# all of the parents (actually, ancestor) of these Pi+1 nodes that 
# are not in Pi.
nodeIndex_to_partition_partitionIndex_group_groupIndex_map = {}

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
num_nodes = 12
#put non-null elements in place
for x in range(num_nodes+1):
    nodes.append(Node(x))
"""

def DAG_executor_driver_Invoker_Thread():
    run()

# visual is a list which stores all the set of edges that constitutes a graph
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
def dfs_parent_pre_parent_traversal(node,visited,list_of_unvisited_children):
    return False


#def dfs_parent(visited, graph, node):  #function for dfs 
def dfs_parent(visited, node):  #function for dfs 
    # e.g. dfs(3) where bfs is visiting 3 as a child of enqueued node
    # so 3 is not visited yet
    logger.debug ("dfs_parent from node " + str(node.ID))

    list_of_unvisited_children = []
    check_list_of_unvisited_chldren_after_visiting_parents = False

    # Fill these in below, e.g., we have to remap the parents since 
    # parent IS is not in position ID in the partition/group.

#rhc : ******* Partition
    partition_node = Partition_Node(node.ID)
    partition_node.ID = node.ID

#rhc : ******* Group
    group_node = Partition_Node(node.ID)
    group_node.ID = node.ID

    # Get unvisited children, which affects whether ndoe is addded to the queue
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
    if CHECK_UNVISITED_CHILDREN:
        check_list_of_unvisited_chldren_after_visiting_parents = dfs_parent_pre_parent_traversal(node,visited,list_of_unvisited_children)
        logger.debug("after pre: list_of_unvisited_children: " + str(list_of_unvisited_children))
    else:
#rhc: If not doing child stuff do we mark node visited here or when we enqueue 
# node in dfs_parent path?
        visited.append(node.ID)

    #Note: dfs_parent_pre_parent_traversal will mark node as visitd

    
    # Note: BFS will not call dfs_parent(child) if chld has been visited. So
    # if child has been visited and thus has been added to global map, we will 
    # not be resetting the pg_tuple here of such a child.

# rhc : ******* Partition and Group
    partition_number = current_partition_number
    parent_partition_parent_index = -1
    group_number = current_group_number
    parent_group_parent_index = -1
    index_in_groups_list = -1
    init_pg_tuple = (partition_number,parent_partition_parent_index,group_number,parent_group_parent_index,index_in_groups_list)
    
    global nodeIndex_to_partition_partitionIndex_group_groupIndex_map
    nodeIndex_to_partition_partitionIndex_group_groupIndex_map[node.ID] = init_pg_tuple

    """
    # for debugging
    logger.info("nodeIndex_to_partition_partitionIndex_group_groupIndex_map, len: " + str(len(nodeIndex_to_partition_partitionIndex_group_groupIndex_map)) + ":")
    logger.info("shadow nodes not mapped and not shown")
    if PRINT_DETAILED_STATS:
        for k, v in nodeIndex_to_partition_partitionIndex_group_groupIndex_map.items():
            logger.info((k, v))
        logger.info("")
    else:
        logger.info("-- (" + str(len(nodeIndex_to_partition_partitionIndex_group_groupIndex_map)) + ")")
    logger.info("")
    """

    if not len(node.parents):
        logger.debug ("dfs_parent node " + str(node.ID) + " has no parents")
    else:
        logger.debug ("dfs_parent node " + str(node.ID) + " visit parents")

    # SCC 1

# rhc : ******* Partition
    #parents_in_previous_partition = False
    # visit parents
    list_of_parents_in_previous_partition = []

# rhc : ******* Group
    #parents_in_previous_group = False
    # visit parents
    list_of_parents_in_previous_group = []

    already_visited_parents = []
    index_of_parent = 0
    for parent_index in node.parents:
 
        parent_node = nodes[parent_index]
        logger.debug("parent_node: " + str(parent_node))

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
            logger.debug ("dfs_parent visit parent node " + str(parent_node.ID))
            #dfs_parent(visited, graph, parent_node)
            dfs_parent(visited, parent_node)

#rhc: case: no shadow nodes since parent is in this partition/group as we have not 
# visited parent previously. Check if this is a loop and parent partition/group
# number is -1
            # get pg_tuple after dfs_parent returns so parent has been processed
            pg_tuple = nodeIndex_to_partition_partitionIndex_group_groupIndex_map[parent_index]
            # parent has been mapped but, in general, the partition and group indices
            # might be -1. Here, they should not be -1 since the parent was unvisited
            # and loops require a parent that has already been visited. (This already
            # visited parent visits its parent ,which visits its parent etc until we try
            # to revisit the already visited parent. Note: this applies also fro loops
            # within loops since in such a case we must still try to visit an already 
            # visited parent, From above, for documentation, a nodes's global map
            # info is initialized at start of dfs_parent as:
            #partition_number = current_partition_number
            #partition_index = -1
            #group_number = current_group_number
            #group_index = -1
            parent_partition_parent_index = pg_tuple[1]
            parent_group_parent_index = pg_tuple[3]
            if (parent_partition_parent_index == -1) or (parent_group_parent_index == -1):
                # assert group_index is also -1
                logger.debug("[Error]: Internal Error: dfs_parent call to unvisited"
                    + " parent resulted in parent/group partition index of -1, which means"
                    + " a loop was detected at an unvisited parent.")
# rhc : ******* Partition
            partition_node.parents.append(parent_partition_parent_index)
# rhc : ******* Group
            group_node.parents.append(parent_group_parent_index)

        else:

            logger.debug ("dfs_parent parent " + str(parent_node.ID) + " of " + str(node.ID) + " already visited"
                + " append parent " + str(parent_node.ID) + " to already_visited_parents.")

            parent_node_visited_tuple = (parent_node,index_of_parent)
            already_visited_parents.append(parent_node_visited_tuple)

# rhc : ******* Partition
            partition_node.parents.append(-1)
# rhc : ******* Group
            group_node.parents.append(-1)
    
            # parent node has been visited so get its info and determine whether
            # this info indicates a loop
            pg_tuple = nodeIndex_to_partition_partitionIndex_group_groupIndex_map[parent_index]
            parent_partition_parent_index = pg_tuple[1]

# rhc : ******* Partition
            #rhc: Note: Not clear whether we will be tracking loops here and if so what 
            # we want to do when we find a loop. For now, TRACK_PARTITION_LOOPS is False
            if TRACK_PARTITION_LOOPS:
                # this pg_tuple was moved up before this if since it is also used
                # after the if.
                #pg_tuple = nodeIndex_to_partition_partitionIndex_group_groupIndex_map[parent_index]

                # parent_partition_number = pg_tuple[0]
                # Changed to parent_partition_parent_index since we set the the 
                #parent_partition_number to current_partition_number at the
                # beginning of dfs_parents().
                # parent_partition_parent_index = pg_tuple[1]
                if parent_partition_parent_index == -1:
                #if parent_partition_number == -1:
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
                    loop_indicator = str(parent_node.ID)+"(Lprnt_of_" + str(node.ID) + ")"
                    current_partition.append(loop_indicator)
                    logger.debug("[Info]: Possible parent loop detected, start and end with " + str(parent_node.ID)
                        + ", loop indicator: " + loop_indicator)
                    global loop_nodes_added
                    loop_nodes_added += 1
# rhc : ******* end Partition - only track loops for partitions, for now

            # Detect a loop here instead of below when we check each parent_node_visited_tuple
            # since this allows us to detect a loop now and hence use a partition or group
            # name with an 'L' at the end, e.g., "PR2_2L" when we crate frontier tuples
            # and add names to the Senders and Receivers structures used for DAG creation.

            if parent_partition_parent_index == -1:
                logger.debug("XXXXXXXXXXXXXXXXX dfs_parent: Loop Detected: "
                    + "PR" + str(current_partition_number) + "_" + str(num_frontier_groups))
# rhc : ******* Partition
                global current_partition_isLoop
                current_partition_isLoop = True

                # assert:
                if parent_group_parent_index != -1:
                    logger.error("[Error] Internal Error: parent_partition_parent_index is -1"
                        + " indicating that current partition is a loop but "
                        + " parent_group_parent_index is not -1, when the group should also be a loop.") 
# rhc : ******* Group
                global current_group_isLoop
                current_group_isLoop = True
            else:
                logger.debug("YYYYYYYYYYYYY dfs_parent: No Loop Detected: "
                    + "PR" + str(current_partition_number) + "_" + str(num_frontier_groups))

        index_of_parent += 1

    # Note: If a loops is detecte current_partition_isLoop and current_group_isLoop are
    # both set to True. current_partition_isLoop remains True until the end 
    # of the partition is reached. current_group_isLoop is set to False when the 
    # end of the group is reached. So it is possible that current_partition_isLoop is
    # True and current_group_isLoop is False.

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

    if len(already_visited_parents) > 0:
        logger.debug("process already_visited_parents of " + str(node.ID))
    # can't add shadow nodes and associated node until all parents added via dfs_parent
    # Q: Can we do this as part of else and then finish the appends here?
    # I think that is what we are doing since all this is the appends of shadow nodes
    # and saving the frontier_node in the parent in different partition/group
    for parent_node_visited_tuple in already_visited_parents:
        visited_parent_node = parent_node_visited_tuple[0]
        index_of_parent = parent_node_visited_tuple[1]
        #where: parent_node_visited_tuple = (parent_node,index_of_parent) 
        partition_group_tuple = nodeIndex_to_partition_partitionIndex_group_groupIndex_map.get(visited_parent_node.ID)
        # this is also used in the else: part for debugging so declare here
        parent_partition_number = None
        parent_group_number = None
        if partition_group_tuple != None:
            parent_partition_number = partition_group_tuple[0]
            # If parent is not in the previous partition, the parent may still
            # be in a previous group (same partition). So in this branch we will 
            # check below to see if the parent is in the same group.
            # If the parent is in the previous partition, it is also in a previous 
            # group. So in the else branch we will have found parent that is in the
            # previous partition PR and a previous group (which is a group in PR.)
#rhc: ToDo: assert parent_partition_number != -1. We set the partition numner at the 
# beginning of dfs_parent to current_partition to it cannot be -1. Either it is 
# the same as current partition number or different, but it cannot be -1. 
# if it is the same, noed and parent can be in a loop or not. Check parent_partition_parent_index
# for loop.
            if parent_partition_number == -1:
                logger.error("[Error]: Internal Error: parent_partition_number is -1 but"
                    + " it should have been set to current_partition_number at the beginning"
                    + " of dfs_parent.")

            if parent_partition_number == -1 or parent_partition_number == current_partition_number:
                # parent is not in previous partition, i.e., node is not a child of
                # a parent node that was in previous partition. This means
                # parent is in this partition and it is either in the same 
                # group as node or it is in a different group, which was computed
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

                logger.debug ("dfs_parent: parent in same partition: parent_partition_number: " 
                    + str(parent_partition_number) 
                    + ", current_partition_number:" + str(current_partition_number)
                    + ", parent ID: " + str(parent_index))

                # Check if this is a loop. A loop is possible since parent is in the same 
                # partition/group. If not a loop, then parent is in previous partition or group, 
                # and that is handled next. Note that 
                # pg_tuple = nodeIndex_to_partition_partitionIndex_group_groupIndex_map[parent_index]
                # and the partition and group numbers are not -1 since they were assigned
                # a value at the start of the dfs search for the parent but the 
                # parent_partition_parent_index and parent_group_parent_index are not assigned
                # a value until the dfs search backs up to parent so if node and parent are
                # in a loop, parent will eventully visit node which may try to visit parent
                # and will see that parent_partition_parent_index or parent_group_parent_index
                # is -1.
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
                    # No need to patch the parent index. We will need a shadow node
                    # if the parent is in a different partition/group in which case
                    # we will make this partition_node / group_node's parent be
                    # a shadow node. 
                    #
                    # assert group_index is also -1
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
                    logger.debug("patch_tuple: " +str(patch_tuple))
# rhc : ******* Partition
                    # save all the patch tuples for later porcessing when the 
                    # actual paent positions are known.
                    patch_parent_mapping_for_partitions.append(patch_tuple)
# rhc : ******* Group
                    patch_parent_mapping_for_groups.append(patch_tuple)

                    # Detected loop. When we compute pagerank for a group, the number
                    # of iterations for a loop-group is more than 1, while the number
                    # of iterations for a non-loop group is 1. The name for a group
                    # or partition with a loop ends with "L".
                    # Note: We detect loops above when we generate the parent_node_visited_tuples
                    # so we can use the L-based partition/group names for partitions/groups that
                    # have a loop. Here we just assert that the just detected loop should also
                    # have been detected above in the for parent_index in node.parents loop.

                    # assert: we should have detected a loop above 
                    if current_partition_isLoop == False:
                        logger.error("[Error] Internal Error: detected partition loop when"
                            + " processing parent_node_visited_tuple that was not"
                            + " detected when generating parent_node_visited_tuple")

                """
                parent_partition_parent_index = partition_group_tuple[1]
                partition_node.parents[index_of_parent] = parent_partition_parent_index
                """

# rhc : ******* Group
                #partition_group_tuple = nodeIndex_to_partition_partitionIndex_group_groupIndex_map.get(parent_node.ID)
                #parent_group_number = None
                # This must be true (leave this if for now)
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
                    parent_group_number = partition_group_tuple[2]
                    if parent_group_number == -1 or parent_group_number == current_group_number:
                        logger.debug ("dfs_parent: parent in same group: parent_group_number: " 
                            + str(parent_group_number)
                            + ", current_group_number: " + str(current_group_number)
                            + ", parent ID: " + str(parent_index)) 

# rhc: case: visited parent before and it is is same group.  So no shadow node and 
# we already checked to see if parent indicates a loop.
                        # Note: The parent is in the same group and could indicate
                        # a loop; however, we already checked for this when we saw
                        # that the parent was in the same partition. (Note: parent in the 
                        # same group ==> parent in same partition. Also, if the parent 
                        # indicates there is a loop in the current group/partition, then
                        # this parent will also indicate there is a loop in the current
                        # partition/group.). If the checked showed a loop in the partition
                        # then we created a path tuple for the partition and group.

                        # Detected loop. When we compute pagerank for a group, the number
                        # of iterations for a loop-group is more than 1, while the number
                        # of iterations for a non-loop group is 1. The name for a group
                        # or partition with a loop ends with "L".

                        # Changed this to an assert. The loop should have also been
                        # detected above when we generated parent_node_visited_tuples.
                        # assert: already detectd loop
                        #global current_group_isLoop
                        #if parent_partition_parent_index == -1:
                        #    current_group_isLoop = True
                        if current_group_isLoop == False:
                            logger.error("[Error] Internal Error: detected group loop when"
                                + " processing parent_node_visited_tuple that was not"
                                + " detected when generating parent_node_visited_tuple")
 
                        """
                        parent_group_parent_index = partition_group_tuple[3]
                        partition_node.parents[index_of_parent] = parent_group_parent_index
                        """

                    else:
                        logger.debug ("dfs_parent: parent in different group: parent_group_number: " 
                            + str(parent_group_number) 
                            + ", current_group_number: " + str(current_group_number)
                            + ", parent ID: " + str(parent_index))

                        # The name of the current partition/group depends on whether it
                        # has a loop. If so we add an 'L' to the end of the name.
                        # Example: "PR2_2" becomes "PR2_2L".
                        #current_group_name = "PR" + str(current_partition_number) + "_" + str(num_frontier_groups)
                        #if current_group_isLoop:
                        #    current_group_name += "L"

                        #parents_in_previous_group = True
                        list_of_parents_in_previous_group.append(visited_parent_node.ID) 

                        #logger.debug ("dfs_parent: found parent in previous group: " + str(parent_node.ID))
                        # index of child just added (we just visited it because it ws an 
                        # unvisited child) to partition
                        child_index_in_current_group = len(current_group)
                        # shadow node is a parent Node on frontier of previous partition
                        #shadow_node = Node(parent_node.ID)
                        shadow_node = Partition_Node(visited_parent_node.ID)
                        shadow_node.isShadowNode = True
                        shadow_node.num_children = len(visited_parent_node.children)
                        # this will possibly be overwritten; the parent may be a
                        # node after the end of the partiton with a pagerank value
                        # that keeps he shadow_node's pagerank value constant.
                        shadow_node.parents.append(-1)
                        # insert shadow_node before child (so only shift one)
                        #current_partition.insert(child_index,shadow_node)
                        logger.debug("dfs_parent: add shadow node to group: " + str(visited_parent_node.ID) + "-s")

    #rhc: ToDo:
                        # only do part/group if using part/group or option to do both
                        # for debugging? No, if in different group but same partition 
                        # then no shadow node in partition as no need to send pr values
                        # to same partition,
                        #current_partition.append(shadow_node)
                        current_group.append(shadow_node)
# rhc: case: visited parent before and it is is same partition so set the parent
# at index index_of_parent to parent_partition_parent_index = partition_group_tuple[0]
                        """
                        parent_group_parent_index = len(current_group)-1
                        partition_node.parents[index_of_parent] = parent_group_parent_index
                        """

    #rhc: ToDo:
                        # only do part/group if using part/group or option to do both
                        # for debugging? No, see above.
                        #nodeIndex_to_partitionIndex_map[shadow_node.ID] = len(current_partition)-1

                        nodeIndex_to_groupIndex_map[shadow_node.ID] = len(current_group)-1
                        # Note: We do not add shadow_node to the 
                        # X map. But shadow_node IDs should be mapped to their positions
                        # when we are computing the group since f the shadow node
                        # is a parent of node n then n.parents are remapped to their 
                        # position in the group and one of n's parents will be the shadow
                        # node so we need its position in the group.

                        #rhc: make group node's parent be this shadow node
                        group_node.parents[index_of_parent] = len(current_group)-1
                    
                        global num_shadow_nodes_added_to_groups
                        num_shadow_nodes_added_to_groups += 1

                        # remember where the frontier_parent node's pagerank value should be placed when 
                        # this task receives it, i.e., put this received value in the 
                        # shadow node which is at position child_index_in_current_group, which is
                        # the position of the just appended node.
                        logger.debug ("frontier_groups: " + str(num_frontier_groups) + ", child_index: " + str(child_index_in_current_group))

                        
                        #d1 = child_index-dfs_parent_start_partition_size
                        #logger.debug("ZZZZZZZZZZZZZZZZZZZZZZZZZ child_index: " + str(child_index) + " d1: " + str(d1))
                        #if child_index != d1:
                        #    logger.debug("ZZZZZZZZZZZZZZZZZZZZZZZZZ Difference: " 
                        #       + " child_index: " + str(child_index) + " d1: " + str(d1))
                        #else:
                        #   logger.debug("ZZZZZZZZZZZZZZZZZZZZZZZZZ No Difference: ") 
                        
                        #logger.debug("ZZZZZZZZZZZ")

                        # Note: Added a partition/group name field to the tuple since we need an 'L'
                        # in the name of the current partition/group if it is a loop. We probably won't
                        # need the current_partition_number/num_frontier_groups but it's available for now for debugging.
                        frontier_parent_tuple = (current_partition_number,num_frontier_groups,child_index_in_current_group,current_group_name)
                        logger.debug ("bfs frontier_parent_tuple: " + str(frontier_parent_tuple))

                        # mark this node as one that PageRank needs to send in its output to the 
                        # next partition (via fanout/faninNB).That is, the fact that list
                        # frontier_parent is not empty indicates it needs to be sent in the 
                        # PageRank output. The tuple indictes which frontier group it should 
                        # be sent to. PageRank may send frontier_parent nodes to mulltiple groups
                        # of multiple partitions
                        #
                        # need to use the current partition, not nodes as the current
                        # partition is what the functions will be using to compute pr
                        # nodes[parent_node.ID].frontier_parents.append(frontier_parent_tuple)
                        partition_group_tuple = nodeIndex_to_partition_partitionIndex_group_groupIndex_map[visited_parent_node.ID]
                        parent_group_number = partition_group_tuple[2]
                        parent_group_parent_index = partition_group_tuple[3]
                        index_in_groups_list = partition_group_tuple[4]
                        # parent_group_number is the number of the group in the 
                        # current partition. We are working on the current group,
                        # which is the next group to be added to groups. The 
                        # current group number is num_frontier_groups and it 
                        # will be added at position len(groups) in groups. Note that group 
                        # numbers in a partition start at 1 not 0. The parent group position
                        # in groups is before that, i.e., len(groups) - i. What is i?
                        # Note: we are working back from the end of the groups list 
                        # to find the parent position.
                        # The current group is num_frontier_groups. The group of the parent
                        # (in this partition) is parent_group_number, which is 
                        # less than num_frontier_groups. If current group = num_frontier_groups
                        # is 2, and parent group is 1, then we want the group at 
                        # len(groups) - (current_group-parent_group). Example, if len(groups)
                        # is 2, the 2 existing groups, groups (1 and 2) are in positions [0] 
                        # and [1]. The current group will be the third group and will be added 
                        # at position [2]. Since len(groups) is 2, and (current_group-parent_group) 
                        # is (2-1) = 1, then the parent group we want is at groups[2-1], which is groups[1].
                        parent_group_position = len(groups) - (num_frontier_groups-parent_group_number)
                        # asssert
                        if not index_in_groups_list == parent_group_position:
                            logger.error("[Error]: Internal Error: dfs_parent: for parent " + str(parent_index)
                                + " index_in_groups_list != parent_group_position"
                                + " index_in_groups_list: " + str(index_in_groups_list)
                                + " parent_group_position: " + str(parent_group_position))

                        parent_group = groups[index_in_groups_list]

                        logger.debug("groupOOOOOOOOOOOOOOO add tuple to parent group: ")
                        for n in parent_group:
                            logger.debug(str(n))
                        logger.debug("len(groups): " + str(len(groups)) + ", parent_group_number: " + str(parent_group_number)
                            + ", num_frontier_groups: " + str(num_frontier_groups) 
                            + ", index_in_groups_list: " + str(index_in_groups_list)
                            + ", parent_group_parent_index: " + str(parent_group_parent_index)
                            + ", frontier_parent_tuple: " + str(frontier_parent_tuple))
                        parent_group[parent_group_parent_index].frontier_parents.append(frontier_parent_tuple)
                        logger.debug("parent_group[parent_group_parent_index].ID: " + str(parent_group[parent_group_parent_index].ID))
                        logger.debug("frontier tuples:")
                        for t in  parent_group[parent_group_parent_index].frontier_parents:
                            logger.debug(str(t))
                        # The current partition/group name in [3] may not have an "L" but if we later 
                        # find a loop we'll need to append an "L" to this name in the frontier tuple
                        # so we need to patch this name (of this task, which is receiving task) but
                        # not the sname of parent, which has already been determined (loop or not)
                        # as it is a different partition/group from this one so already processed.
                        if not current_group_isLoop:
                            position_in_frontier_parents_group_list = len(parent_group[parent_group_parent_index].frontier_parents)-1
                            frontier_parent_group_patch_tuple = (index_in_groups_list,parent_group_parent_index,position_in_frontier_parents_group_list)
                            frontier_parent_group_patch_tuple_list.append(frontier_parent_group_patch_tuple)
                        """
                        Sample Output of above:
                        DEBUG: dfs_parent: parent in different group: parent_group_number: 1, current_group_number: 2, parent ID: 19
                        DEBUG: dfs_parent: add shadow node to group: 2-s
                        DEBUG: frontier_groups: 2, child_index: 0
                        DEBUG: bfs frontier_parent_tuple: (2, 2, 0, 'PR2_2L')
                        DEBUG: groupOOOOOOOOOOOOOOO add tuple to parent group:
                        DEBUG: 2
                        DEBUG: 10
                        DEBUG: 5-s
                        DEBUG: 16
                        DEBUG: len(groups): 2, parent_group_number: 1, num_frontier_groups: 2, 
                               index_in_groups_list: 1, parent_group_parent_index: 0, frontier_parent_tuple: (2, 2, 0, 'PR2_2L')
                        DEBUG: parent_group[parent_group_parent_index].ID: 2
                        DEBUG: frontier tuples:
                        DEBUG: (2, 2, 0, 'PR2_2L')
                        """
       
                        # if True: 
                        if use_shared_partitions_groups:
                            task_name_of_parent_group = group_names[index_in_groups_list]
                            #task_name_of_parent = "PR" + str(parent_partition_number) + "_" + str(parent_group_number)
                            shared_frontier_parent_tuple = (current_partition_number,num_frontier_groups,child_index_in_current_group,current_group_name,parent_group_parent_index)
                            list_of_parent_frontier_tuples = BFS_Shared.shared_groups_frontier_parents_map.get(task_name_of_parent_group)
                            if list_of_parent_frontier_tuples == None:
                                list_of_parent_frontier_tuples = []
                            logger.debug("groupOGOGOGOGOGOGOGOG add shared tuple to parent group: ")
                            for n in parent_group:
                                logger.debug(str(n))
                            logger.debug("task_name_of_parent: " + task_name_of_parent_group
                                + ", frontier_parent_tuple: " + str(frontier_parent_tuple))
                            logger.debug("list_of_parent_frontier_tuples before appending tuple: " 
                                + str(list_of_parent_frontier_tuples))
                            list_of_parent_frontier_tuples.append(shared_frontier_parent_tuple)
                            BFS_Shared.shared_groups_frontier_parents_map[task_name_of_parent_group] = list_of_parent_frontier_tuples
                            logger.debug("New BFS_Shared.shared_groups_frontier_parents_map:")
                            for (k,v) in BFS_Shared.shared_groups_frontier_parents_map.items():
                                logger.debug(str(k) + ": " + str(v))                            


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
                            if not current_group_isLoop:
                                position_in_list_of_parent_frontier_tuples = len(list_of_parent_frontier_tuples)-1
                                shared_frontier_parent_group_patch_tuple = (task_name_of_parent_group,position_in_list_of_parent_frontier_tuples)
                                shared_frontier_parent_groups_patch_tuple_list.append(shared_frontier_parent_group_patch_tuple)

                        # generate dependency in DAG. If parent in group i has an dge
                        # to child in group j, i!=j, then add edge i-->j to dag. Doing this
                        # by adding j to the receivers of i, and adding i to the receivers
                        # of j. 
                        # Note: If we want to construct DAG incrementally, thrn we might want to 
                        # specifically add the edhe i-->j, e.g., by depositing i-->j no a bounded
                        # buffer that a DAG-generator thread withdraws and uses for generating
                        # the incremental DAG_info object. As opposed to generating at some point
                        # all-at-once the DAG_info object for all the edges that have been generated so far.
                        # Note: incremental DAG generation must handle the leaf nodes at the beginning
                        # of the DAG, which will be part of the first DAG increment, and the terminal 
                        # nodes at the end of the DAG, which have no outputs, i.e.,
                        # fanouts/fanins. The end of the incremental DAG might also have
                        # such nodes, i.e., nodes with no outputs/fanouts/fanins since these
                        # are in the rest of the DAG.
                        #sending_group = "PR"+str(parent_partition_number)+"_"+str(parent_group_number)
                        # index in groups list is the actual index, starting with index 0
                        sending_group = group_names[index_in_groups_list]
                        receiving_group = current_group_name
                        #sending_group = "PR"+str(parent_partition_number)+"_"+str(parent_group_number)
                        #receiving_group = "PR"+str(current_partition_number)+"_"+str(num_frontier_groups)
                        sender_set = Group_senders.get(sending_group)
                        if sender_set == None:
                            Group_senders[sending_group] = set()
                        Group_senders[sending_group].add(receiving_group)
                        receiver_set = Group_receivers.get(receiving_group)
                        if receiver_set == None:
                            Group_receivers[receiving_group] = set()
                        Group_receivers[receiving_group].add(sending_group)

                        if not current_group_isLoop:
                            sender_receiver_group_patch_tuple = (index_in_groups_list,receiving_group)
                            sender_receiver_group_patch_tuple_list.append(sender_receiver_group_patch_tuple)

                else:
                    logger.error("[Error] Internal Error. dfs_parent: partition_group_tuple " 
                        + "is None should be unreachable.")
                    # if there's no entry in the global map then we have not visited the
                    # parent yet so it's not in the same group.
                    # Note that this check is before the call to dfs_parent(parent_index).
                    # If it were after, then the partition_group_tuple could not be None
                    # since we add parent to the global map at the start of dfs_parent()
                    logger.debug ("dfs_parent: parent in same group: parent_group_number: " 
                        + str(parent_group_number)
                        + ", parent ID: " + str(parent_index))
                # we haven't seen parent parent_node yet so it is not in a previous group.
                # For example, root 1's parent is 17 so we call dfs_parent(17) and 17 will
                # be in the sme group. Here, we have not seen 17 yet so it is not in
                # nodeIndex_to_partition_partitionIndex_group_groupIndex_map. Noet that 1
                # will be aded to nodeIndex_to_partition_partitionIndex_group_groupIndex_map
                # at start of dfs_parent then 1 does this check on its parent 17
                # before calling dfs_parent(17).

                # SCC 2
# rhc : ******* END Group

            else:
                #parent is in different/previous partition, (must be current_partition - 1)
                logger.debug ("dfs_parent: parent in different partition/group: parent_partition_number: " 
                    + str(parent_partition_number) 
                    + ", current_partition_number:" + str(current_partition_number)
                    + ", parent ID: " + str(visited_parent_node.ID))

                # The name of the current partition/group depends on whether it
                # has a loop. If so we add an 'L' to the end of the name.
                # Example: "PR2_2" becomes "PR2_2L".
                #current_partition_name = "PR" + str(current_partition_number) + "_1"
                #current_group_name = "PR" + str(current_partition_number) + "_" + str(num_frontier_groups)
                #if current_partition_isLoop:
                #    current_partition_name += "L"
                #if current_group_isLoop:
                #    current_group_name += "L"

# rhc : ******* Partition
                #parents_in_previous_partition = True
                list_of_parents_in_previous_partition.append(visited_parent_node.ID)

                # take care of this now
                # index of child just added (we just visited it because it ws an 
                # unvisited child) to partition
                child_index_in_current_partition = len(current_partition)
                # shadow node is a parent Node on frontier of previous partition
                #shadow_node = Node(parent_node.ID)
                shadow_node = Partition_Node(visited_parent_node.ID)
                shadow_node.isShadowNode = True
                shadow_node.num_children = len(visited_parent_node.children)
                # this will possibly be overwritten; the parent may be a
                # node after the end of the partiton with a pagerank value
                # that keeps he shadow_node's pagerank value constant.
                shadow_node.parents.append(-1)
                # insert shadow_node before child (so only shift one)
                #current_partition.insert(child_index,shadow_node)

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
                child_index_in_current_group = len(current_group)
                current_group.append(copy.deepcopy(shadow_node))
                logger.debug("dfs_parent: add shadow node to group: " + str(visited_parent_node.ID) + "-s")
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
                # Note: We do not add shadow_node to the 
                # X map. But shadw_node IDs should be mapped to their positions
                # when we are computing the group since f the shadow node
                # is a parent of node n then n.parents are remapped to their 
                # position in the group and one of n's parents will be the shadow
                # node so we need its position in the group.

# rhc : ******* Partition
                #rhc: make group node's parent be this shadow node
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
                logger.debug ("num partitions: " + str(current_partition_number) + ", child_index_in_current_partition: " + str(child_index_in_current_partition))
# rhc : ******* Group
                logger.debug ("num_frontier_groups: " + str(num_frontier_groups) + ", child_index_in_current_group: " + str(child_index_in_current_group))
# rhc: ToDo: if we are using partition then we just need partition number and index
# but we won't use group number? That is, the names aer PR1, PR2, etc, so we ignore'
# the group number when we form partition name for target funtion with shadow nodes?
# Rather: just use group node of 0 when using partitions, so PR1_0, PR2_0,...
                # Note : For partitions, the child_index is the index relatve to the 
                # start of the partition. child_index is len(current_partition).
                # The calculation for groups (below) is a bit difference.
                # Q: Use 0 instead of num_frontier_groups so we can just grab the 0.
                #
                # Note: Added a partition/group name field to the tuple since we need an 'L'
                # in the name of the current partition/group if it is a loop. We probably won't
                # need the current_partition_number/num_frontier_groups but it's available for now for debugging.
# rhc : ******* Partition
                frontier_parent_partition_tuple = (current_partition_number,1,child_index_in_current_partition,current_partition_name)
                logger.debug ("bfs frontier_parent_partition_tuple (pnum,1,childindx,pname): " + str(frontier_parent_partition_tuple))
# rhc : ******* Group
                frontier_parent_group_tuple = (current_partition_number,num_frontier_groups,child_index_in_current_group,current_group_name)
                logger.debug ("bfs frontier_parent_group_tuple: (pnum,gnum,chldinx,gname) " + str(frontier_parent_group_tuple))
 
                # mark this node as one that PageRank needs to send in its output to the 
                # next partition (via fanout/faninNB).That is, the fact that list
                # frontier_parent is not empty indicates it needs to be sent in the 
                # PageRank output. The tuple indictes which frontier group it should 
                # be sent to. PageRank may send frontier_parent nodes to mulltiple groups
                # of multiple partitions
                #
                # need to use the current partition, not nodes as the current
                # partition is what the functions will be using to compute pr
                # nodes[parent_node.ID].frontier_parents.append(frontier_parent_tuple)

                partition_group_tuple = nodeIndex_to_partition_partitionIndex_group_groupIndex_map[visited_parent_node.ID]
                logger.debug ("visited_parent_node.ID " + str(visited_parent_node.ID)
                    + "partition_group_tuple:" + str(partition_group_tuple))
                parent_partition_number = partition_group_tuple[0]
                parent_partition_parent_index = partition_group_tuple[1]
                parent_group_number = partition_group_tuple[2]
                parent_group_parent_index = partition_group_tuple[3]
                # cannot use parent_group_number to index groups; parent_group_number
                # is a number within a partition, e.g., PR2_2 has a group index of 2,
                # but this is not necessarily the 2nd group overall.
                index_in_groups_list = partition_group_tuple[4]

# rhc : ******* Partition
                logger.debug ("partition_group_tuple of parent " + str(visited_parent_node.ID) + " (pnum,pindx,gnum,gindx,posingroupslist): " + str(partition_group_tuple))
                # partition numbers start at 1 not 0
                logger.debug ("add frontier tuple to parent partition")
                parent_partition = partitions[parent_partition_number-1]
                parent_partition[parent_partition_parent_index].frontier_parents.append(frontier_parent_partition_tuple)
# rhc : ******* Group
                logger.debug ("add frontier tuple to parent group")
                #parent_group = groups[parent_group_number-1]
                parent_group = groups[index_in_groups_list]
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
                # Note: in white board group 2_2, when 20 sees 2 it detects no loop
                # and then it sees 19 and detects a loop, so 20 uses "PR2_2L" as
                # the name of its group.
                if not current_group_isLoop:
                    position_in_frontier_parents_group_list = len(parent_group[parent_group_parent_index].frontier_parents)-1
                    frontier_parent_group_patch_tuple = (index_in_groups_list,parent_group_parent_index,position_in_frontier_parents_group_list)
                    frontier_parent_group_patch_tuple_list.append(frontier_parent_group_patch_tuple)
            
                """
                Sample output of above:
                DEBUG: dfs_parent: parent in different partition: parent_partition_number: 1, current_partition_number:2, parent ID: 17
                dfs_parent: add shadow node to group: 17-s
                num partitions: 2, child_index_in_current_partition: 8
                num_frontier_groups: 2, child_index_in_current_group: 5
                bfs frontier_parent_partition_tuple (pnum,1,childindx,pname): (2, 1, 8, 'PR2_1L')
                bfs frontier_parent_group_tuple: (pnum,gnum,chldinx,gname) (2, 2, 5, 'PR2_2L')
                visited_parent_node.ID 17partition_group_tuple:(1, 1, 1, 1, 0)
                partition_group_tuple of parent 17 (pnum,pindx,gnum,gindx,posingroupslist): (1, 1, 1, 1, 0)
                add frontier tuple to parent partition
                add frontier tuple to parent group
                """

# rhc : ******* Partition
                #if True: # 
                if use_shared_partitions_groups:
                    # shared partitions frontier code:
                    task_name_of_parent_partition = partition_names[parent_partition_number-1]
                    #task_name_of_parent = "PR" + str(parent_partition_number) + "_" + "1"
                    shared_frontier_parent_tuple = (current_partition_number,1,child_index_in_current_partition,current_partition_name,parent_partition_parent_index)
                    list_of_parent_frontier_tuples = BFS_Shared.shared_partition_frontier_parents_map.get(task_name_of_parent_partition)
                    if list_of_parent_frontier_tuples == None:
                        list_of_parent_frontier_tuples = []
                    logger.debug("groupOPOPOPOPOPOP add shared tuple to parent group: ")
                    for n in parent_group:
                        logger.debug(str(n))
                    logger.debug("task_name_of_parent: " + task_name_of_parent_partition
                        + ", shared_frontier_parent_tuple: " + str(shared_frontier_parent_tuple))
                    logger.debug("list_of_parent_frontier_tuples before appending tuple: " 
                        + str(list_of_parent_frontier_tuples))
                    list_of_parent_frontier_tuples.append(shared_frontier_parent_tuple)
                    BFS_Shared.shared_partition_frontier_parents_map[task_name_of_parent_partition] = list_of_parent_frontier_tuples
                    logger.debug("New BFS_Shared.shared_partition_frontier_parents_map:")
                    for (k,v) in BFS_Shared.shared_partition_frontier_parents_map.items():
                        logger.debug(str(k) + ": " + str(v))

                    """
                    where:  in bfs_pagerank, we grab the shared_frontier_parent_tuple
                    and its fields using:
                            #FYI:
                            # partition_number = frontier_parent[0]
                            # group_number = frontier_parent[1]

                            position_or_group_index_of_output_task = frontier_parent_tuple[2]
                            partition_or_group_name_of_output_task = frontier_parent_tuple[3]
                            # Note: We added this field [4] to the frontier tuple so that when
                            # we ar using a shared_nodes array or multithreading we can
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
                    # shared groups frontier code:
                    task_name_of_parent_group = group_names[index_in_groups_list]
                    #task_name_of_parent = "PR" + str(parent_partition_number) + "_" + str(parent_group_number)
                    shared_frontier_parent_tuple = (current_partition_number,num_frontier_groups,child_index_in_current_group,current_group_name,parent_group_parent_index)
                    list_of_parent_frontier_tuples = BFS_Shared.shared_groups_frontier_parents_map.get(task_name_of_parent_group)
                    if list_of_parent_frontier_tuples == None:
                        list_of_parent_frontier_tuples = []
                    logger.debug("groupOGPOGPOGPOGPOGPOGPOGP add shared tuple to parent group: ")
                    for n in parent_group:
                        logger.debug(str(n))
                    logger.debug("task_name_of_parent_group: " + task_name_of_parent_group
                        + ", shared_frontier_parent_tuple: " + str(shared_frontier_parent_tuple))
                    logger.debug("list_of_parent_frontier_tuples before appending tuple: " 
                        + str(list_of_parent_frontier_tuples))
                    list_of_parent_frontier_tuples.append(shared_frontier_parent_tuple)
                    BFS_Shared.shared_groups_frontier_parents_map[task_name_of_parent_group] = list_of_parent_frontier_tuples
                    logger.debug("New BFS_Shared.shared_groups_frontier_parents_map:")
                    for (k,v) in BFS_Shared.shared_groups_frontier_parents_map.items():
                        logger.debug(str(k) + ": " + str(v))

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
# rhc : ******* Partition
                sending_partition = partition_names[parent_partition_number-1]
                #receiving_partition = "PR"+str(current_partition_number)+"_1"
                receiving_partition = current_partition_name
                sender_set = Partition_senders.get(sending_partition)
                if sender_set == None:
                    Partition_senders[sending_partition] = set()
                Partition_senders[sending_partition].add(receiving_partition)
                receiver_set = Partition_receivers.get(receiving_partition)
                if receiver_set == None:
                    Partition_receivers[receiving_partition] = set()
                Partition_receivers[receiving_partition].add(sending_partition)
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
                sending_group = group_names[index_in_groups_list]
                #receiving_group = "PR"+str(current_partition_number)+"_"+str(num_frontier_groups)
                receiving_group = current_group_name
                sender_set = Group_senders.get(sending_group)
                if sender_set == None:
                    Group_senders[sending_group] = set()
                Group_senders[sending_group].add(receiving_group)
                receiver_set = Group_receivers.get(receiving_group)
                if receiver_set == None:
                    Group_receivers[receiving_group] = set()
                Group_receivers[receiving_group].add(sending_group)

                if not current_group_isLoop:
                    sender_receiver_group_patch_tuple = (index_in_groups_list,receiving_group)
                    sender_receiver_group_patch_tuple_list.append(sender_receiver_group_patch_tuple)
 

        else:
            logger.error("[Error] Internal Error. dfs_parent: partition_group_tuple" 
                + " of parent is None should be unreachable since this is after calling "
                + " dfs_parent() on the parent.")
            # if there's no entry in the global map then we have not visited the
            # parent before so it's not in a different/previous partition.
            # Note that this check is before the call to dfs_parent(parent_index).
            # If it were after, then the partition_group_tuple could not be None
            # since we add parent to the global map at the start of dfs_parent()
            logger.debug ("dfs_parent: parent in same partition/group: parent_partition_number:" 
                + " parent_partition_number: " + str(parent_partition_number)
                + " parent_group_number: " + str(parent_group_number)
                + ", parent ID: " + str(parent_index))

    if CHECK_UNVISITED_CHILDREN:
        # process children after parent traversal
        dfs_parent_post_parent_traversal(node, visited,
            list_of_unvisited_children, check_list_of_unvisited_chldren_after_visiting_parents)
    else:
        BFS_queue.append(node.ID)
        #queue.append(-1)
        if DEBUG_ON:
            print_val = "queue after add " + str(node.ID) + ": "
            for x in BFS_queue:
                #logger.debug(x.ID, end=" ")
                print_val = print_val + str(x) + " "
            logger.debug(print_val)
            logger.debug("")
        #frontier.append(node)
        frontier.append(node.ID)
        if DEBUG_ON:
            print_val = "frontier after add " + str(node.ID) + ":"
            for x in frontier:
                #logger.debug(x.ID, end=" ")
                #logger.info(x, end=" ")
                print_val = print_val + str(x) + " "
            logger.debug(print_val)
            logger.debug("")
        # make sure parent in partition before any if its children. We visit parents of nodein dfs_parents 
        # and they are added to partition in dfs_parents after their parents are added 
        # in dfs_parents then here we add node to partition.  

#rhc: Can this be false? can we dfs_parent visit a node that has already been visited
# and that already has been put in a partition?

        if node.partition_number == -1:
            logger.debug ("dfs_parent add " + str(node.ID) + " to partition")
            node.partition_number = current_partition_number
            logger.debug("set " + str(node.ID) + " partition number to " + str(node.partition_number))
            #current_partition.append(node.ID)
            #rhc: append node 

#rhc: Todo: Can we do this as part of for each parent loop? instead of looping again?
# Note: parent remaps to different index depending on partiton or group!!
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
            #current_partition.append(node)
            #current_group.append(node)
            current_partition.append(partition_node)
# rhc : ******* Group
            #current_group.append(copy.deepcopy(partition_node))
            current_group.append(group_node)

            # partition_node.ID and group_node.ID are the same
# rhc : ******* Partition
            nodeIndex_to_partitionIndex_map[partition_node.ID] = len(current_partition)-1
# rhc : ******* Group
            nodeIndex_to_groupIndex_map[partition_node.ID] = len(current_group)-1
            
            # Note: if node's parent is in different partition then we'll add a 
            # shadow_node to the partition and the group in a position right before 
            # node in partition and group. But if a node's parent is in a different 
            # group but same partition then we only add a shadow node to the group
            # in a position right before node in the group. 

# rhc : ******* Partition
            # information for this partition node
            # There are n partitions, this node is in partition partition_number

#rhc: we set partition_number in tuple at beginning of dfs_parent to current_partition_numer
# and likewise for the group number, no need to set it here, but can do 
# asserts.
            partition_number = current_partition_number
            # In this partition, this node is at position partition_index,
            # where we just added the node to current_partition
            partition_index = len(current_partition)-1
# rhc : ******* Group
            # Likewise for groups
            group_number = current_group_number
            group_index = len(current_group)-1

#rhc We need postion in groups (frontier_groups-1) when we add a frontier tuple
# to a group. That is, if say 13 is a child of 8, we need to add a frontier_tuple
# to 8.  group_number is the number of the group in a partition, e.g., 8 might be
# in group 2 in partition 2, but any partition can have a group 2, so we cannot 
# use the group number 2 to access this group 2 of 8 in the groups list. We need
# to know group 2's position in the groups list, which could be anything.
# So for 8 we will save it's partition number and its partition index, where
# partition number i is always in position i-1 of the partitions list. We also
# save 8's group number and group index, and 8's position in groups so we can 
# get 8's group from the groups list when we need it.
            index_in_groups_list = frontier_groups_sum-1
            pg_tuple = (partition_number,partition_index,group_number,group_index,index_in_groups_list)
            nodeIndex_to_partition_partitionIndex_group_groupIndex_map[partition_node.ID] = pg_tuple
            logger.debug("HHHHHHHHHHHHHHHH dfs_parent: pg_tuple(pnum,pindx,gnum,gindx,posingroupslist) generate for " + str(partition_node.ID)
                + str(pg_tuple))
        else:
            logger.debug ("dfs_parent do not add " + str(node.ID) + " to partition "
                + current_partition_number + " since it is already in partition " 
                + node.partition_number)

# process children after parent traversal
def dfs_parent_post_parent_traversal(node, visited, list_of_unvisited_children, check_list_of_unvisited_chldren_after_visiting_parents):
    pass
# this is in a seperate file

#def bfs(visited, graph, node): #function for BFS
def bfs(visited, node): #function for BFS
    logger.debug ("bfs mark " + str(node.ID) + " as visited and add to queue")
    #rhc: add to visited is done in dfs_parent
    #visited.append(node.ID)
    # dfs_parent will add node to partition (and its unvisited parent nodes)
    global current_partition


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

#rhc incremental
    # total number of graph nodes that have been added to the 
    # partitions generated so far. When all nodes have been 
    # added to a partition, the partitions are complete.
    # Used for incremental DAG generation where we do not know
    # the number of partitions so we cannot stop based on the 
    # number of partitions we have seen.
    num_nodes_in_partitions = 0

#rhc: q:
    # are not these lengths 0?
    # These are per dfs_parent() stats not per partition
    dfs_parent_start_partition_size = len(current_partition)
    dfs_parent_start_frontier_size = len(frontier)
    global loop_nodes_added
    dfs_parent_loop_nodes_added_start = loop_nodes_added
    #rhc shared
    if use_shared_partitions_groups:
        start_num_shadow_nodes_for_partitions = num_shadow_nodes_added_to_partitions
        # start it here before root cal to dfs_parents()
        start_num_shadow_nodes_for_groups = num_shadow_nodes_added_to_groups

    #dfs_p(visited, graph, node)
    #dfs_p_new(visited, graph, node)

#rhc: 
    # start with -1 in the queue; after call to dfs_parent, which will 
    # collect node and its ancestors, we will pop the -1 fron the 
    # queue, which will end the current partition.
    BFS_queue.append(-1)

    # SCC 3

    #dfs_parent(visited, graph, node)
    global num_frontier_groups
    num_frontier_groups = 1
    global frontier_groups_sum
    # This is used as the index into groups for the current group.
    # frontier_groups_sum inited to 0 so this makes it 1. Note that
    # if we call bfs() again then this does not reset frontier_groups_sum
    frontier_groups_sum += 1
    dfs_parent(visited, node)

    # Note: No shadow_nodes can be added during first call to dfs_parent
    # as the generated group has no parents; it is the first group.
    # SCC 4

    # Note: -1 is a the front of the queue so we will pop the -1 which 
    # means this is the end of the current partition, which is node and its 
    # ancestors.

    global current_group
    global groups
# rhc : ******* Group
    groups.append(current_group)
    current_group = []
    #global frontier_groups_sum
    # root group
    #frontier_groups_sum += 1


    # this first group ends here after first dfs_parent
    global nodeIndex_to_groupIndex_maps
    global nodeIndex_to_groupIndex_map
    nodeIndex_to_groupIndex_maps.append(nodeIndex_to_groupIndex_map)
    nodeIndex_to_groupIndex_map = {}
# rhc : ******* end Group

    global current_partition_number
    global current_group_number

# rhc : ******* Group
    group_name = "PR" + str(current_partition_number) + "_" + str(current_group_number)
    # group_number_in_fronter stays at 1 since this is the only group in the frontier_list
    # partition and thus the first group in the next parttio is also group 1

    global current_group_isLoop
    if current_group_isLoop:
        # These are the names of the groups that have a loop. In the 
        # DAG, we will append an 'L' to the name. Not used since we 
        # use loop names (with 'L") as we generate Sender and Receivers.
        # instead of modifying the names of senders/receievers before we 
        # generate the DAG.
        group_name = group_name + "L"
        Group_loops.add(group_name)

    current_group_isLoop = False
    # Note: not incrementing current_group_number. This root group is the 
    # only group in this partition. We consider it to be group 1, 
    # which is the initial value of current_group_number. We do not increment
    # it since we are done with the groups in the first partition, so 
    # current_group_number will be 1 wen we find the first group of the 
    # next partition.
    group_names.append(group_name)

    # The first group collected by call to BFS() is a leaf node of the DAG.
    # There may be many calls to BFS(). Below, we will collect the first
    # partition. Set is_leaf_node to True so we know it is the first partition
    # collected on this call to BFS()
    leaf_tasks_of_groups.add(group_name)
    is_leaf_node = True
    
    if use_shared_partitions_groups:
        #rhc shared
        # assert: first partition/group has no shadow_nodes
        change_in_shadow_nodes_for_group = end_num_shadow_nodes_for_groups - start_num_shadow_nodes_for_groups
        groups_num_shadow_nodes_list.append(change_in_shadow_nodes_for_group)
        # start it here before next call to dfs_parent but note that we 
        # may not call dfs_parent() since the node may not have any (unvisited)
        # children in which case we will generate a final partition/group
        # and we need to have called start here.
        start_num_shadow_nodes_for_groups = num_shadow_nodes_added_to_groups
# rhc : ******* end Group

    dfs_parent_loop_nodes_added_end = loop_nodes_added

# rhc : ******* Partition
    dfs_parent_end_partition_size = len(current_partition)
    dfs_parent_change_in_partition_size = (dfs_parent_end_partition_size - dfs_parent_start_partition_size) - (
        dfs_parent_loop_nodes_added_end - dfs_parent_loop_nodes_added_start)
    dfs_parent_changes_in_partiton_size.append(dfs_parent_change_in_partition_size)
    logger.debug("dfs_parent(root)_change_in_partition_size: " + str(dfs_parent_change_in_partition_size))
# rhc : ******* end Partition

    # These are tracked per dfs_parent() call, so we compute them here and 
    # at after the calls to dfs_parent() below.
    dfs_parent_end_frontier_size = len(frontier)
    dfs_parent_change_in_frontier_size = (dfs_parent_end_frontier_size - dfs_parent_start_frontier_size) - (
        dfs_parent_loop_nodes_added_end - dfs_parent_loop_nodes_added_start)
    dfs_parent_changes_in_frontier_size.append(dfs_parent_change_in_frontier_size)
    logger.debug("dfs_parent(root)_change_in_frontier_size: " + str(dfs_parent_change_in_frontier_size))

    # queue.append(node) and frontier.append(node) done optionally in dfs_parent
#rhc
    end_of_current_frontier = False
    while BFS_queue:          # Creating loop to visit each node
        #node = queue.pop(0) 
        ID = BFS_queue.pop(0) 
        logger.debug("bfs pop node " + str(ID) + " from queue") 
#rhc
        # issue: if we add queue.append(-1) in dfs_parent, we get smaller partitions
        # but the frontiers overlap. this is becuase in dfs_parent we get
        # a -1 b -1 c -1 then we rturn to bfs then it checks -1 at front, which is 
        # true, so it crates partition a b c sincne a b and c are in the partition
        # but haven't got thru aall the nodes on frontier, which is a b c so next 
        # frontier is b c ... So if we put a b c in partition before we see our
        # first -1 then we have to get through a b and c. Note there is a funny interaction
        # with when we remove node from frontier, i.e., after we isit all of its
        # children. So only can process -1's after processing all of a node's children.
        # Hmmm. 
        # We get 5, 17, 1 so we dfs_parent 5 and visit 5's parents then visit 5's children:
        """
        bfs node 5 visit children
        bfs visit child 16 mark it visited and dfs_parent(16)
        bfs dfs_parent(16)
        dfs_parent from node 16
        dfs_parent node 16 visit parents
        dfs_parent neighbor 5 already visited
        dfs_parent visit node 10
        dfs_parent from node 10
        dfs_parent node 10 visit parents
        dfs_parent visit node 2
        dfs_parent from node 2
        dfs_parent node 2 has no parents
        queue after add 2: 17 1 -1 2
        frontier after add 2: 5 17 1 2
        dfs_parent add 2 to partition
        queue after add 10: 17 1 -1 2 10
        frontier after add 10: 5 17 1 2 10 
        dfs_parent add 10 to partition
        queue after add 16: 17 1 -1 2 10 16
        frontier after add 16: 5 17 1 2 10 16
        dfs_parent add 16 to partition
        dfs_parent_change_in_partition_size: 3
        dfs_parent_change_in_frontier_size: 3
        bfs node 17 already visited
        frontier after remove 5: 17 1 2 10 16
        """
        # but no -1 after 5. So could put -1 after 5 if we replaced 5 on queue
        # with all its parent cild stuff?

        if ID == -1:
            end_of_current_frontier = True

            if BFS_queue:
                ID = BFS_queue.pop(0)
                logger.debug("bfs after pop -1; pop node " + str(ID) + " from queue") 
                BFS_queue.append(-1)

                # SCC 5

            else:
                break

        node = nodes[ID]
        # so we can see the frontier costs that do not correspnd to when 
        # partitions were created, i.e., was there a better frontier for partition?
        all_frontier_costs.append("pop-"+str(node.ID) + ":" + str(len(frontier)))

        # Note: There are no singletons in the frontier. if N has a singleton child
        # C then the dfs_parent(N) will se that C is unvisited. IF singleton checking
        # is on then singleton C will be identified, C will be marked visited, and
        # neither N nor C will be enqueued but both N and C will be added to the 
        # partition but not the frontier. If singleton checking is off then N
        # will be enqueued. When N is popped off the queue, dfs_parent(C) will be
        # called and it will see that C has no children before or after C's parent 
        # traversal (N is already visited) so C will be marked visited and C will 
        # not be enqueued but will be added to the partition but not the frontier.
        # Note: If the partiton becmes full with N in the frontier with singleton 
        # chld C, we can reduce the frontier by pruning N - pop N from the queue,
        # mark N as visited, remove N fro the frontier, mark its singleton child C 
        # visited, and add C to the partition but not the queue.
        # Note: handling singletons here is more efficent since we don;t waste time 
        # checkng for singletons in dfs_parent when most nodes are not singletons.

#rhc: problem: we don't find partition loop until we dfs_parent(17) so the 
# frontier tuple for the 5 is in different partition than 16 is wring since
# we will use PR2_1 for partition. So don't do partition tuples until after
# finish partition, where we know whether partition has a loop or not?
# save frontier tuples with empty name and then patch the name before you
# process the tuple.

        if end_of_current_frontier:
            logger.debug("BFS: end_of_current_frontier")
            end_of_current_frontier = False

# rhc : ******* Partition
            if len(current_partition) > 0:
            #if len(current_partition) >= num_nodes/5:
                logger.debug("BFS: create sub-partition at end of current frontier")
                # does not require a deepcopy
                partitions.append(current_partition.copy())
#rhc incremental:
                num_nodes_in_partitions += len(current_partition)
                current_partition = []

                partition_name = "PR" + str(current_partition_number) + "_1"

                global current_partition_isLoop
                if current_partition_isLoop:
                    # These are the names of the partitions that have a loop. In the 
                    # DAG, we will append an 'L' to the name. Not using this anymore.
                    partition_name = partition_name + "L"
                    Partition_loops.add(partition_name)

                # The first partition collected by any call to BFS() is a leaf node of the DAG.
                # There may be many calls to BFS(). We set is_leaf_node = True at thr
                # start of BFS.
                if is_leaf_node:
                    leaf_tasks_of_partitions.add(partition_name)
                    is_leaf_node = False

                # Patch the partition name of the frontier_parent tuples. 
                if current_partition_isLoop:
                    # When the tuples in frontier_parent_partition_patch_tuple_list were created,
                    # no loop had been detectd in the partition so we used a partition name that 
                    # did not end in 'L'. At some point a loop was detected so we need to
                    # change the partition name in the tuple so that it ends with 'L'. If no loop
                    # is detectd, then current_partition_isLoop will be false and no changes
                    # need to be made.
                    logger.debug("XXXXXXXXXXX BFS: patch partition frontier_parent tuples: ")
                    # frontier_parent_partition_patch_tuple was created as:
                    #   frontier_parent_partition_patch_tuple = 
                    #       (parent_partition_number,parent_partition_parent_index,position_in_frontier_parents_partition_list)
                    for frontier_parent_partition_patch_tuple in frontier_parent_partition_patch_tuple_list:
                        # These values were used to create the tuples in dfs_parent()
                        parent_partition_number = frontier_parent_partition_patch_tuple[0]
                        parent_partition_parent_index = frontier_parent_partition_patch_tuple[1]
                        position_in_frontier_parents_partition_list = frontier_parent_partition_patch_tuple[2]

                        # get the tuple that has the wrong name
                        parent_partition = partitions[parent_partition_number-1]
                        frontier_parents = parent_partition[parent_partition_parent_index].frontier_parents
                        frontier_parent_partition_tuple_to_patch = frontier_parents[position_in_frontier_parents_partition_list]
                        logger.debug("XXXXXXX BFS: patching partition frontier_tuple name "
                            + frontier_parent_partition_tuple_to_patch[3] + " to " + partition_name)
                        # create a new tuple that reuses the first 3 fields and chnages the name in the last field
                        first_field = frontier_parent_partition_tuple_to_patch[0]
                        second_field = frontier_parent_partition_tuple_to_patch[1]
                        third_field = frontier_parent_partition_tuple_to_patch[2]
                        new_frontier_parent_partition_tuple = (first_field,second_field,third_field,partition_name)
                        # delete the old tuples
                        del frontier_parents[position_in_frontier_parents_partition_list]
                        # append the new tuple, order of tuples may change but order is not important
                        frontier_parents.append(new_frontier_parent_partition_tuple)
                        logger.debug("XXXXXXX BFS:  new frontier_parents: " + str(frontier_parents))

                frontier_parent_partition_patch_tuple_list.clear()

                # Patch the partition name of the shared_frontier_parent_partition tuples. 
                if use_shared_partitions_groups:
                    # Given:
                    # shared_frontier_parent_partition_patch_tuple = (task_name_of_parent,position_in_list_of_parent_frontier_tuples)
                    if current_partition_isLoop:
                        logger.debug("X-X-X-X-X-X-X BFS: patch shared partition frontier_parent tuples: ")
                        for shared_frontier_parent_partition_patch_tuple in shared_frontier_parent_partition_patch_tuple_list:
                            # These values were used to create the tuples in dfs_parent()
                            task_name_of_parent = shared_frontier_parent_partition_patch_tuple[0]
                            position_of_tuple_in_list_of_parent_frontier_tuples = shared_frontier_parent_partition_patch_tuple[1]

                            list_of_parent_frontier_tuples = BFS_Shared.shared_partition_frontier_parents_map.get(task_name_of_parent)
                            frontier_parent_partition_tuple_to_patch = list_of_parent_frontier_tuples[position_of_tuple_in_list_of_parent_frontier_tuples]
                            logger.debug("X-X-X-X-X-X-X BFS: patching shared partition frontier_tuple name "
                            + frontier_parent_partition_tuple_to_patch[3] + " to " + partition_name)
 
                            # Given:
                            #shared_frontier_parent_tuple = (current_partition_number,num_frontier_groups,child_index_in_current_partition,current_partition_name,parent_partition_parent_index)
                            first_field = frontier_parent_partition_tuple_to_patch[0]
                            second_field = frontier_parent_partition_tuple_to_patch[1]
                            third_field = frontier_parent_partition_tuple_to_patch[2]
                            # FYI: [3] is the partition name to be patched with the new name ending in "L"
                            # e.g., "PR2_1" --> "PR2_1L". Partition name was appended with "L" above
                            fifth_field = frontier_parent_partition_tuple_to_patch[4]
                            new_frontier_parent_partition_tuple = (first_field,second_field,third_field,partition_name,fifth_field)
                            del list_of_parent_frontier_tuples[position_of_tuple_in_list_of_parent_frontier_tuples]
                            # append the new tuple, order of tuples may change but order is not important
                            list_of_parent_frontier_tuples.append(new_frontier_parent_partition_tuple)
                            logger.debug("X-X-X-X-X-X-X BFS:  new shared partition frontier_parent tuples for " + task_name_of_parent + " is " +  str(list_of_parent_frontier_tuples))

                    shared_frontier_parent_partition_patch_tuple_list.clear()

                # patch receiver name
                if current_partition_isLoop:
                    # When the tuples in sender_receiver_partition_patch_tuple_list were created,
                    # no loop had been detectd in the partition so we used a partitiom name 
                    # for the receiver name that did not end in 'L'. At some point a loop was detected so we need to
                    # change the receiver name so that it ends with 'L'. If no loop
                    # is detectd, then current_partition_isLoop will be false and no changes
                    # need to be made.
                    logger.debug("XXXXXXXXXXX BFS: patch partition sender/receiver names: ")
                    for sender_receiver_partition_patch_tuple in sender_receiver_partition_patch_tuple_list:
                        # sender_receiver_partition_patch_tuple crated as:
                        #   sender_receiver_partition_patch_tuple = (parent_partition_number,receiving_partition)
                        parent_partition_number = sender_receiver_partition_patch_tuple[0]
                        receiving_partition = sender_receiver_partition_patch_tuple[1]

                        sending_partition = partition_names[parent_partition_number-1]
                        sender_name_set = Partition_senders[sending_partition]
                        logger.debug("XXXXXXX BFS: patching partition sender_set receiver name "
                            + receiving_partition + " to " + partition_name)
                        sender_name_set.remove(receiving_partition)
                        sender_name_set.add(partition_name)
                        logger.debug("XXXXXXX BFS:  new partition sender_Set: " + str(sender_name_set))

                        logger.debug("XXXXXXX BFS: patching Partition_receivers receiver name "
                            + receiving_partition + " to " + partition_name)
                        Partition_receivers[partition_name] = Partition_receivers[receiving_partition]
                        del Partition_receivers[receiving_partition]
                        logger.debug("XXXXXXX BFS:  new Partition_receivers[partition_name]: " + str(Partition_receivers[partition_name]))
                
                sender_receiver_partition_patch_tuple_list.clear()

                current_partition_isLoop = False
                partition_names.append(partition_name)

                if use_shared_partitions_groups:
                    #rhc shared
                    end_num_shadow_nodes_for_partitions = num_shadow_nodes_added_to_partitions
                    change_in_shadow_nodes_for_partitions = end_num_shadow_nodes_for_partitions - start_num_shadow_nodes_for_partitions
                    partitions_num_shadow_nodes_list.append(change_in_shadow_nodes_for_partitions)
                    start_num_shadow_nodes_for_partitions = num_shadow_nodes_added_to_partitions

                global patch_parent_mapping_for_partitions
                logger.debug("BFS: partition_nodes to patch: ")
                for parent_tuple in patch_parent_mapping_for_partitions:
                    logger.debug("BFS: parent_tuple: " + str(parent_tuple) + "," )
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
                    # one list for the ode in the partition and one list for 
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
                    if partition_index_of_parent != -1:
                        # assert group_index is also -1
                        list_of_parents_of_partition_node[i] = partition_index_of_parent
                        #list_of_parents_of_group_node[i] = group_index_of_parent
                        logger.debug("BFS: end of frontier: remapping parent " + str(parent_index)
                            + " of " + str(node_ID) +  " to " + str(partition_index_of_parent) 
                            + " for partition node.")
                            #+ group_index_of_parent + " for group node")
                    else:
                        logger.error("BFS: global map index of " + parent_index + " is -1")

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
                frontier_cost = "pop-"+str(node.ID) + ":" + str(len(frontier))
                frontier_costs.append(frontier_cost)
                frontier.clear()

#rhc incremental                
                if compute_pagerank and use_incremental_DAG_generation:
                    if using_workers:

                        to_be_continued = (num_nodes_in_partitions < num_nodes)
                    
                        DAG_info = generate_DAG_info_incremental_partitions(partition_name,current_partition_number,to_be_continued)
                        
                        # A DAG with a single partition, and hence a single group is a special case.
                        if current_partition == 1 and DAG_info.get_DAG_info_is_complete():
                                # output partition 1, which s complete
                                with open('./'+partition_name + '.pickle', 'wb') as handle:
                                    # partition indices in partitions[] start with 0, so current partition i
                                    # is in partitions[i-1] and previous partition is partitions[i-2]
                                    cloudpickle.dump(partitions[0], handle) #, protocol=pickle.HIGHEST_PROTOCOL)  
                        
                                # deposit complete DAG_info for workers
                                DAG_infobuffer_monitor.deposit(DAG_info)

                                # We just processed the first and only partition; so we can output the 
                                # initial DAG_info and start the DAG_executor_driver. DAG_info
                                # will have a complete state for partition 1.
                                #
                                # Before we start the DAG_executor_driver we need to have
                                # saved to a file PR1_1's nodes and saved to file DAG_info;
                                # we also do the DAG_infobuffer_monitor.deposit(DAG_info) though
                                # it is not strictly required since the DAG_info file can be 
                                # read by the workers/lambdas.
                                #
                                # DAG_info will be read by the worker (threads/proceses)
                                # and the threads simulating lambdas
                                # or it will be read by the DAG_executor_driver and given
                                # to the real (leaf) lambdas as part of their payload.
                                file_name = "./DAG_info.pickle"
                                with open(file_name, 'wb') as handle:
                                    cloudpickle.dump(DAG_info, handle) #, protocol=pickle.HIGHEST_PROTOCOL)  
                                
                                # Need to call run() but it has to be asynch
                                thread_name = "DAG_executor_driver_Invoker"
                                logger.debug("BFS: Starting DAG_executor_driver_Invoker_Thread for incrmental DAG generation.")
    #rhc: incremental
                                #Question: This thread completes normally?
                                # Perhaps BFS can join this thread instad of calling run() when inc dag gen?
                                #     Then invoker_thread is global?
                                invoker_thread = threading.Thread(target=DAG_executor_driver_Invoker_Thread, name=(thread_name), args=())
                                invoker_thread.start()

                        if current_partition >=2:
                            # generate complete DAG_info for partition current_partition_number-1 and
                            # incomplete DAG_info for partition current_partition_number
                            
                            # generating partitions, not groups.
                            # Note: For groups, we may still key off partitions, i.e., when 
                            # we complete a partition, we generate the groups in this partition.
                            if not use_page_rank_group_partitions:
                                #
                                # Note: "PR1_1" is the one and only leaf partition/group
                                previous_partition_name = "PR"+str(current_partition_number-1)+"_1"
                            
                                # previous partition is complete so save partition to a file
                                # which will be in cloud for real lambdas
                                with open('./'+previous_partition_name + '.pickle', 'wb') as handle:
                                    # partition indices in partitions[] start with 0, so current partition i
                                    # is in partitions[i-1] and previous partition is partitions[i-2]
                                    cloudpickle.dump(partitions[current_partition-2], handle) #, protocol=pickle.HIGHEST_PROTOCOL)  

    #rhc: ToDo: deposit every jth partition. Make sure this condition is True for 
    # initial DAG.
                                if True:
                                    # Deposit new incremental DAG. This may be the 
                                    # first DAG and since the workers and lambdas
                                    # will receive this DAG as a leaf task, they 
                                    # will not need to withdraw this DAG as a new
                                    # DAG, i.e., their first request for a new 
                                    # incremental DAG is for any newer DAG than the 
                                    # first DAG (i.e., any version later than version 1.)
                                    DAG_infobuffer_monitor.deposit(DAG_info)

        #rhc: Question: Do we want to dump for debugging all the generated 
        # DAG_info files? or we will be displaying the generatd DAG_info files
        # for debugging so no need to save all the DAG_infos to file?

                                if (current_partition_number) == 2:
                                    # We just processed the second partition; so we can output the 
                                    # initial DAG_info and start the DAG_executor_driver. DAG_info
                                    # will have a complete state for partition 1 and an incomplete
                                    # state for P2. Thus, we can start DAG_execution and compute
                                    # pagerank for P1. At that point, since P2 is incomplete, we will
                                    # add the state for P2 to the continue queue and P2 will not be
                                    # executed until new DAG_info is generated.
                                    #
                                    # We can't generate initial DAG for execuion until we have partitions 
                                    # 1 and 2, since we don't know partition 1's outputs until we have processed 
                                    # the nodes in partition 2; partition 2 is "to be continued".
                                    #
                                    # So before we start the DAG_executor_driver we need to have
                                    # saved to a file PR1_1's nodes and saved the initial DAG_info;
                                    # we also do the DAG_infobuffer_monitor.deposit(DAG_info) though
                                    # it is not strictly requried since the ADG_info file can be 
                                    # read by the workers/lambdas.
                                    #
                                    # DAG_info will be read by the worker (threads/proceses)
                                    # and the threads simulating lambdas
                                    # or it will be read by the DAG_executor_driver and given
                                    # to the real (leaf) lambdas as part of their payload.
                                    file_name = "./DAG_info.pickle"
                                    with open(file_name, 'wb') as handle:
                                        cloudpickle.dump(DAG_info, handle) #, protocol=pickle.HIGHEST_PROTOCOL)  
                                    
                                    # Need to call run() but it has to be asynch
                                    thread_name = "DAG_executor_driver_Invoker"
                                    logger.debug("BFS: Starting DAG_executor_driver_Invoker_Thread for incrmental DAG generation.")
        #rhc: incremental
                                    #Question: This thread completes normally?
                                    # Perhaps BFS can join this thread instad of calling run() when inc dag gen?
                                    #     Then invoker_thread is global?
                                    invoker_thread = threading.Thread(target=DAG_executor_driver_Invoker_Thread, name=(thread_name), args=())
                                    invoker_thread.start()
                    else:
                        pass #complete for lambdas

                #global frontier_groups_sum
                #global num_frontier_groups
                logger.info("BFS: frontier groups: " + str(num_frontier_groups))

                # use this if to filter the very small numbers of groups
                #if frontier_groups > 10:
                # using this to determine whether parent is in current partition
                current_partition_number += 1
                current_group_number = 1
                # frontier_groups_sum += num_frontier_groups
                logger.info("BFS: frontier_groups_sum: " + str(frontier_groups_sum))
                # this was incrementd in dfs_parent for each unvsited child of a 
                # parent, i.e., when a new group was generated.
                num_frontier_groups = 0
  
        if not len(node.children):
            logger.debug ("bfs node " + str(node.ID) + " has no children")
        else:
            logger.debug ("bfs node " + str(node.ID) + " visit children")
        for neighbor_index in node.children:
            neighbor = nodes[neighbor_index]
            if neighbor.ID not in visited:
                logger.debug ("bfs visit child " + str(neighbor.ID) + " mark it visited and "
                    + "dfs_parent(" + str(neighbor.ID) + ")")

                #visited.append(neighbor.ID)
                logger.debug ("bfs dfs_parent("+ str(neighbor.ID) + ")")

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
                logger.debug ("bfs frontier_parent_tuple: " + str(frontier_parent_tuple))
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
                # this is a list of partition_nodes in the current group
                current_group = []
                group_name = "PR" + str(current_partition_number) + "_" + str(current_group_number)
                if current_group_isLoop:
                    # These are the names of the groups that have a loop. In the 
                    # DAG, we will append an 'L' to the name. Not used since we 
                    # use loop names (with 'L") as we generate Sender and Recevers.
                    # instead of modifying the names of senders/receievers before we 
                    # generate the DAG.
                    group_name = group_name + "L"
                    Group_loops.add(group_name)

#rhc:
# 1. clear instead of re-init?
# 2. Really need to patch groups? If no assert no patching. Note:
#       we find loops on backup nd we don't do atch stuff until after
#       we see all backups, so can we do patch stuff without knowing 
#       about loop that will be detected later?

                if current_group_isLoop:
                    # When the tuples in frontier_parent_partition_patch_tuple_list were created,
                    # no loop had been detectd in the partition so we used a partitiob name that 
                    # did not end in 'L'. At some point a loop was detected so we need to
                    # change the partition name in the tuple so that it ends with 'L'. If no loop
                    # is detectd, then current_partition_isLoop will be false and no changes
                    # need to be made.
                    logger.debug("XXXXXXXXXXX BFS: patch group frontier_parent tuples: ")
                    # frontier_parent_partition_patch_tuple was created as:
                    #   (parent_partition_number,parent_partition_parent_index,(current_partition_number,1,child_index_in_current_partition,current_partition_name))
                    for frontier_parent_group_patch_tuple in frontier_parent_group_patch_tuple_list:
                        # These values were used to create the tuples in dfs_parent()
                        index_in_groups_list = frontier_parent_group_patch_tuple[0]
                        parent_group_parent_index = frontier_parent_group_patch_tuple[1]
                        position_in_frontier_parents_group_list = frontier_parent_group_patch_tuple[2]

                        # get the tuple that has the wrong name
                        parent_group = groups[index_in_groups_list]
                        frontier_parents = parent_group[parent_group_parent_index].frontier_parents
                        frontier_parent_group_tuple_to_patch = frontier_parents[position_in_frontier_parents_group_list]
                        logger.debug("XXXXXXX BFS: patching group frontier_tuple name "
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
                        logger.debug("XXXXXXX BFS:  new frontier_parents: " + str(frontier_parents))

                frontier_parent_group_patch_tuple_list.clear()

                if use_shared_partitions_groups:
                    # Given:
                    # shared_frontier_parent_partition_patch_tuple = (task_name_of_parent,position_in_list_of_parent_frontier_tuples)
                    if current_group_isLoop:
                        logger.debug("X-X-X-X-X-X-X BFS: patch shared groups frontier_parent tuples: ")
                        for shared_frontier_parent_groups_patch_tuple in shared_frontier_parent_groups_patch_tuple_list:
                            # These values were used to create the tuples in dfs_parent()
                            task_name_of_parent = shared_frontier_parent_groups_patch_tuple[0]
                            position_of_tuple_in_list_of_parent_frontier_tuples = shared_frontier_parent_groups_patch_tuple[1]

                            list_of_parent_frontier_tuples = BFS_Shared.shared_partition_frontier_group_map.get(task_name_of_parent)
                            logger.debug("X-X-X-X-X-X-X BFS:  old shared groups frontier_parents for " + task_name_of_parent + " is " +  str(list_of_parent_frontier_tuples))
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
                            logger.debug("X-X-X-X-X-X-X BFS:  new shared groups frontier_parents for " + task_name_of_parent + " is " +  str(list_of_parent_frontier_tuples))

                    shared_frontier_parent_groups_patch_tuple_list.clear()

                if current_group_isLoop:
                    # When the tuples in sender_receiver_partition_patch_tuple_list were created,
                    # no loop had been detectd in the partition so we used a partitiom name 
                    # for the receiver name that did not end in 'L'. At some point a loop was detected so we need to
                    # change the receiver name so that it ends with 'L'. If no loop
                    # is detectd, then current_partition_isLoop will be false and no changes
                    # need to be made.
                    logger.debug("XXXXXXXXXXX BFS: patch group sender/receiver names: ")
                    for sender_receiver_group_patch_tuple in sender_receiver_group_patch_tuple_list:
                        # sender_receiver_partition_patch_tuple crated as:
                        #   sender_receiver_partition_patch_tuple = (parent_partition_number,receiving_partition)
                        index_in_groups_list = sender_receiver_group_patch_tuple[0]
                        receiving_group = sender_receiver_group_patch_tuple[1]

                        sending_group = partition_names[index_in_groups_list]
                        sender_name_set = Group_senders[sending_group]
                        logger.debug("XXXXXXX BFS: patching group sender_set receiving_group "
                            + receiving_group + " to " + group_name)
                        sender_name_set.remove(receiving_group)
                        sender_name_set.add(group_name)
                        logger.debug("XXXXXXX BFS:  new group sender_Set: " + str(sender_name_set))

                        logger.debug("XXXXXXX BFS: patching Group_receivers receiver name "
                            + receiving_group + " to " + group_name)
                        Group_receivers[group_name] = Group_receivers[receiving_group]
                        del Group_receivers[receiving_group]
                        logger.debug("XXXXXXX BFS:  new Group_receivers[group_name]: " + str(Group_receivers[group_name]))
                
                sender_receiver_group_patch_tuple_list.clear()

                current_group_isLoop = False
                current_group_number += 1
                group_names.append(group_name)

                if use_shared_partitions_groups:
                    #rhc shared
                    end_num_shadow_nodes_for_groups = num_shadow_nodes_added_to_groups
                    change_in_shadow_nodes_for_groups = end_num_shadow_nodes_for_groups - start_num_shadow_nodes_for_groups
                    groups_num_shadow_nodes_list.append(change_in_shadow_nodes_for_groups)
                    # call start here before next call to dfs_parents(), if any, since we 
                    # may not call dfs_parents() again as node may not have any (unvisited) children.
                    # if no call to dfs_parent() we may still have a final partition/group and we
                    # need to have called start before then.
                    start_num_shadow_nodes_for_groups = num_shadow_nodes_added_to_groups

                #global patch_parent_mapping_for_partitions
                global patch_parent_mapping_for_groups
                logger.debug("partition_nodes to patch: ")
                for parent_tuple in patch_parent_mapping_for_groups:
                    logger.debug("parent_tuple: " + str(parent_tuple) + "," )
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
                    if group_index_of_parent != -1:
                        # assert group_index is also -1
                        #list_of_parents_of_partition_node[i] = partition_index_of_parent
                        list_of_parents_of_group_node[i] = group_index_of_parent
                        logger.debug("end of frontier: remapping parent " + str(parent_ID)
                            + " of " + str(node_ID) 
                            #+  " to " + partition_index_of_parent 
                            #+ " for partition node and "
                            + " to " + str(group_index_of_parent) + " for group node")
                    else:
                        logger.error("global map index of " + parent_index + " is -1")

                patch_parent_mapping_for_groups = []

                # track groups here; track partitions when frontier ends above
                nodeIndex_to_groupIndex_maps.append(nodeIndex_to_groupIndex_map)
                nodeIndex_to_groupIndex_map = {}

                """
                logger.info("")
                if PRINT_DETAILED_STATS:
                    logger.info("KKKKKKKKKKKKKKKKKKKKK group nodes' frontier_parent_tuples:")
                    for x in groups:
                        if PRINT_DETAILED_STATS:
                            print_val = "-- (" + str(len(x)) + "): "
                            for node in x:
                                print_val += str(node.ID) + ": "
                                # logger.info(node.ID,end=": ")
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
                logger.debug("dfs_parent("+str(node.ID) + ")_change_in_partition_size: " + str(dfs_parent_change_in_partition_size))
                dfs_parent_changes_in_partiton_size.append(dfs_parent_change_in_partition_size)
              
                dfs_parent_end_frontier_size = len(frontier)
                dfs_parent_change_in_frontier_size = (dfs_parent_end_frontier_size - dfs_parent_start_frontier_size) - (
                    dfs_parent_loop_nodes_added_end - dfs_parent_loop_nodes_added_start)
                logger.debug("dfs_parent("+str(node.ID) + ")_change_in_frontier_size: " + str(dfs_parent_change_in_frontier_size))
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
                    logger.debug("child " + str(neighbor.ID) + " of node " + str(node.ID)
                        + " has no children, already marked it visited and added"
                        + " it to partition but do not queue it or add it to frontier.")
                """
            else:
                logger.debug ("bfs node " + str(neighbor.ID) + " already visited")
        #frontier.remove(node)
        #frontier.remove(node.ID)
        try:
            frontier.remove(node.ID)
        except ValueError:
            logger.debug("*******bfs: " + str(node.ID)
                + " not in frontier.")

        if DEBUG_ON:
            print_val = "frontier after remove " + str(node.ID) + ": "
            for x in frontier:
                #logger.debug(x.ID, end=" ")
                print_val = print_val + str(x) + " "
            logger.debug(print_val)
            logger.debug("")
    
    """
    if len(current_partition) >= 0:
        logger.debug("BFS: create final sub-partition")
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
    fname = "graph_20"
    #fname = "graph_27"
    #graph_file = open(fname, 'r')
    #graph_file = open(fname, 'r')
    graph_file = open(fname+".gr", 'r')
    #graph_file = open(fname, 'r')
    count = 0
    file_name_line = graph_file.readline()
    count += 1
    logger.debug("file_name_line{}: {}".format(count, file_name_line.strip()))
    vertices_line = graph_file.readline()
    count += 1
    logger.debug("vertices_line{}: {}".format(count, vertices_line.strip()))
    edges_line = graph_file.readline()
    count += 1
    logger.debug("edges_line{}: {}".format(count, edges_line.strip()))
    

    _max_weight_line_ignored = graph_file.readline()
    count += 1
    #logger.debug("max_weight_line{}: {}".format(count, max_weight_line_ignored.strip()))
    _min_weight_line_ignored = graph_file.readline()
    count += 1
    #logger.debug("min_weight_line{}: {}".format(count,  min_weight_line_ignored.strip()))

    # need this for generated graphs; 100.gr is old format?
    
    _min_edge_line_ignored = graph_file.readline()
    count += 1
    #logger.debug("min_edge_line{}: {}".format(count, min_edge_line_ignored.strip()))
    _max_edge_line_ignored = graph_file.readline()
    count += 1
    #logger.debug("max_edge_line{}: {}".format(count, max_edge_line_ignored.strip()))
    
    vertices_edges_line = graph_file.readline()
    count += 1
    logger.debug("vertices_edges_line {}: {}".format(count, vertices_edges_line.strip()))

    words = vertices_edges_line.split(' ')
    logger.debug("nodes:" + words[2] + " edges:" + words[3])
    global num_nodes
    num_nodes = int(words[2])
    global num_edges
    num_edges = int(words[3])
    logger.info("input_file: read: num_nodes:" + str(num_nodes) + " num_edges:" + str(num_edges))

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
            logger.debug("[Warning]: self loop: " + str(source) + " -->" + str(target))
            num_self_loops += 1
            continue
        #logger.debug("target:" + str(target))
        #if target == 101:
        #    logger.debug("target is 101")
        #rhc: 101 is a sink, i.e., it has no children so it will not appear as a source
        # in the file. Need to append a new node if target is out of range, actually 
        # append target - num_nodes. Is this just a coincidence that sink is node 100+1
        # where the gaph is supposed to have 100 nodes?

        # Example: num_nodes is 100 and target is 101, so 101 > 100.
        # But nodes is filled from nodes[0] ... nodes[100] so len(nodes) is 101
        #if (target == 101):
        #    logger.debug ("target is 101, num_nodes is " + str(num_nodes) + " len nodes is "
        #       + str(len(nodes)))
        if target > num_nodes:
            # If len(nodes) is 101 and num_nodes is 100 and we have a tatget of
            # 101, which is a sink, i.e., parents but no children, then there is 
            # no source 101. We use target+1, where 101 - num_nodes = 101 - 100 - 1
            # and target+1 = 101+1 = 102 - len(nodes) = 101 - 101 - 1, so we get 
            # the number_of_nodes_to_append to be 1, as needed.
            if len(nodes) < target+1:
                number_of_nodes_to_append = target - num_nodes
                logger.debug("number_of_nodes_to_append:" + str(number_of_nodes_to_append))
                # in our example, number_of_nodes_to_append = 1 so i starts
                # with 0 (default) and ends with number_of_nodes_to_append-1 = 0
                for i in range(number_of_nodes_to_append):
                    logger.debug("Node(" + str(num_nodes+i+1) + ")")
                    # new node ID for our example is 101 = num_nodes+i+1 = 100 + 0 + 1 = 101
                    nodes.append(Node((num_nodes+i+1)))
                num_nodes += number_of_nodes_to_append
        #logger.debug ("source:" + str(source) + " target:" + str(target))
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
    
        #logger.debug("Line {}: {}".format(count, line.strip()))

    """
    source_node = nodes[1]
    logger.debug("Node1 children:")
    for child in source_node.children:
        logger.debug(child)
    logger.debug("Node1 parents:")
    for parent in source_node.parents:
        logger.debug(parent)

    source_node = nodes[7]
    logger.debug("Node7 children:")
    for child in source_node.children:
        logger.debug(child)
    logger.debug("Node7 parents:")
    for parent in source_node.parents:
        logger.debug(parent)
    """

    count_child_edges = 0
    i = 1
    while i <= num_nodes:
        node = nodes[i]

#rhc: Too: Note: nodes has num_children so we can use the same pagerank
# computation on a Node that we do on a partition_node. A Node does not 
# really need num_children.
        node.num_children = len(node.children)


        #logger.debug (str(i) + ": get children: " + str(len(node.children)))
        count_child_edges += len(node.children)
        i += 1
    logger.debug("num edges in graph: " + str(num_edges) + " = num child edges: " 
        + str(count_child_edges) + " + num_self_loops: " + str(num_self_loops))
    if not ((num_edges - num_self_loops) == count_child_edges):
        logger.error("[Error]: num child edges in graph is " + str(count_child_edges) + " but edges in file is "
            + str(num_edges))

    count_parent_edges = 0
    i = 1
    while i <= num_nodes:
        node = nodes[i]
        #logger.debug (str(i) + ": get parents: " + str(len(node.parents)))
        count_parent_edges += len(node.parents)
        i += 1

    logger.debug("num_edges in graph: " + str(num_edges) + " = num parent edges: " 
        + str(count_parent_edges) + " + num_self_loops: " + str(num_self_loops))
    if not ((num_edges - num_self_loops) == count_parent_edges):
        logger.error("[Error]: num parent edges in graph is " + str(count_parent_edges) + " but edges in file is "
        + str(num_edges))

    logger.debug("num_parent_appends:" + str(num_parent_appends))
    logger.debug("num_children_appends:" + str(num_children_appends))
    logger.debug("num_self_loops: " + str(num_self_loops))
    if num_self_loops > 0:
        save_num_edges = num_edges
        num_edges -= + num_self_loops
        logger.debug("old num_edges: " + str(save_num_edges) + " num_edges: " + str(num_edges))
    else:
        logger.debug("num_edges: " + str(num_edges))

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
        logger.info("Group Nodes w/parents:")
        for group in groups:
            for node in group:
                #logger.info(node,end=":")
                print_val = str(node) + ":"
                for parent in node.parents:
                    print_val += str(parent) + " "
                    #logger.info(parent,end=" ")
                logger.info(print_val)
                logger.info("")
            logger.info("")
        logger.info("Group Nodes w/Frontier parent tuples:")
        for group in groups:
            for node in group:
                #logger.info(node,end=":")
                print_val = str(node) + ":"
                for tup in node.frontier_parents:
                    print_val += str(tup) + " "
                    # logger.info(tup,end=" ")
                logger.info(print_val)
                logger.info("")
            logger.info("")
    else:
        partition_inputs = []
        for name in partition_names:
            with open('./'+name+'.pickle', 'rb') as handle:
                partition_inputs.append(cloudpickle.load(handle))
        logger.info("Partition Nodes w/parents:")
        for partition in partitions:
            for node in partition:
                #logger.info(node,end=":")
                print_val = str(node) + ":"
                for parent in node.parents:
                    print_val += str(parent) + " "
                    #logger.info(parent,end=" ")
                logger.info(print_val)
                logger.info("")
            logger.info("")
        logger.info("Partition Nodes w/Frontier parent tuples:")
        for partition in partitions:
            for node in partition:
                #logger.info(node,end=":")
                print_val = str(node) + ":"
                for tup in node.frontier_parents:
                    print_val += str(tup) + " "
                    # logger.info(tup,end=" ")
                logger.info(print_val)
                logger.info("")
            logger.info("")
  
# Driver Code

# SCC 8

"""
G = nx.DiGraph()
G.add_edges_from(visual)
logger.debug(nx.is_connected(G))
"""
def PageRank_Function_Main(nodes,total_num_nodes):
    if (debug_pagerank):
        logger.debug("PageRank_Function output partition_or_group (node:parents):")
        for node in nodes:
            #logger.debug(node,end=":")
            print_val = str(node) + ":"
            for parent in node.parents:
                print_val += str(parent) + " "
                #logger.debug(parent,end=" ")
            if len(node.parents) == 0:
                #logger.debug(",",end=" ")
                print_val += ", "
            else:
                #logger.debug(",",end=" ")
                print_val += ", "
            logger.debug(print_val)
        logger.debug("")
        logger.debug("PageRank_Function output partition_or_group (node:num_children):")
        print_val = ""
        for node in nodes:
            print_val += str(node)+":"+str(node.num_children) + ", "
            # logger.debug(str(node)+":"+str(node.num_children),end=", ")
        logger.debug(print_val)
        logger.debug("")
        logger.debug("")
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
            logger.debug("***** PageRank: iteration " + str(i))
            logger.debug("")

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
#time.sleep(3)   #not needed due to shutdwn
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

if __name__ == '__main__':

    logger.debug("Following is the Breadth-First Search")
    input_graph()
    logger.debug("num_nodes after input graph: " + str(num_nodes))
    #visualize()
    #input('Press <ENTER> to continue')

    #rhc: Int BFS_shared before starting BFS. If we add to struct of arrays
    # during dfs_parent, we will eventually need those inits earlier too.
    if use_shared_partitions_groups:
        BFS_Shared.initialize()

    #bfs(visited, graph, '5')    # function calling
    # example: num_nodes = 100, so Nodes in nodes[1] to nodes[100]
    # i start = 1 as nodes[0] not used, i end is (num_nodes+1) - 1  = 100
    for i in range(1,num_nodes+1):
        if i not in visited:
            logger.debug("*************Driver call BFS for node[" + str(i) + "]")
            #bfs(visited, graph, nodes[i])    # function calling
            bfs(visited, nodes[i])    # function calling

# rhc : ******* Partition

    # Do last partition/group if there is one
    if len(current_partition) > 0:
        logger.debug("BFS: create final sub-partition")
        # does not require a deepcop
        partitions.append(current_partition.copy())
        current_partition = []

        #rhc shared: added all the name stuff - should have been there
        partition_name = "PR" + str(current_partition_number) + "_1"
        #global current_partition_isLoop
        if current_partition_isLoop:
            # These are the names of the partitions that have a loop. In the 
            # DAG, we will append an 'L' to the name. Not using this anymore.
            partition_name = partition_name + "L"
            Partition_loops.add(partition_name)

        current_partition_isLoop = False
        partition_names.append(partition_name)

        if use_shared_partitions_groups:
            #rhc shared
            end_num_shadow_nodes_for_partitions = num_shadow_nodes_added_to_partitions
            change_in_shadow_nodes_for_partitions = end_num_shadow_nodes_for_partitions - start_num_shadow_nodes_for_partitions
            partitions_num_shadow_nodes_list.append(change_in_shadow_nodes_for_partitions)
            # not needed here since we are done but kept to be consisent with use above
            start_num_shadow_nodes_for_partitions = num_shadow_nodes_added_to_partitions

# rhc : ******* Group
# ToDo: if len(current_group) > 0:
        groups.append(current_group)
        current_group = []

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

        if use_shared_partitions_groups:
            #rhc shared
            end_num_shadow_nodes_for_groups = num_shadow_nodes_added_to_groups
            change_in_shadow_nodes_for_groups = end_num_shadow_nodes_for_groups - start_num_shadow_nodes_for_groups
            groups_num_shadow_nodes_list.append(change_in_shadow_nodes_for_groups)
            # not needed here since we are done but kept to be consisent with use above
            start_num_shadow_nodes_for_groups = num_shadow_nodes_added_to_groups

        nodeIndex_to_partitionIndex_maps.append(nodeIndex_to_partitionIndex_map)
        nodeIndex_to_partitionIndex_map = {}
        nodeIndex_to_groupIndex_maps.append(nodeIndex_to_groupIndex_map)
        nodeIndex_to_groupIndex_map = {}

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
        # always do this - below we assert final frontier is empty
        # does not require a deepcop
        frontiers.append(frontier.copy())

    # generate shared array of partitions/groups if using multithreaded workers
    # or threads to simulate lambdas

    if use_shared_partitions_groups:
        generate_shared_partitions_groups(num_nodes,num_parent_appends,partitions,partition_names,
            partitions_num_shadow_nodes_list,num_shadow_nodes_added_to_partitions,
            groups, group_names,groups_num_shadow_nodes_list,num_shadow_nodes_added_to_groups)

    #logging.shutdown()
    #os._exit(0)

    #partitions.append(current_partition.copy())
    #frontiers.append(frontier.copy())
    #frontier_cost = "END" + ":" + str(len(frontier))
    #frontier_costs.append(frontier_cost)
    logger.info("")
    logger.info("input_file: generated: num_nodes: " + str(num_nodes) + " num_edges: " + str(num_edges))
    logger.info("")
    logger.info("visited length: " + str(len(visited)))
    if len(visited) != num_nodes:
        logger.error("[Error]: visited length is " + str(len(visited))
            + " but num_nodes is " + str(num_nodes))
    print_val = ""
    for x in visited:
        print_val += str(x) + " "
        #print(x, end=" ")
    logger.info(print_val)
    logger.info("")
    logger.info("")
    logger.info("final current_partition length: " + str(len(current_partition)-loop_nodes_added))
    sum_of_partition_lengths = 0
    i = 1
    for x in partitions:
        sum_of_partition_lengths += len(x)
        logger.debug(str(i) + ":length of partition: " + str(len(x)))
        i += 1
    logger.debug("shadow_nodes_added: " + str(num_shadow_nodes_added_to_partitions))
    if not use_shared_partitions_groups:
        sum_of_partition_lengths -= (total_loop_nodes_added + num_shadow_nodes_added_to_partitions)
        logger.info("sum_of_partition_lengths (not counting total_loop_nodes_added or shadow_nodes and their parents added): " 
            + str(sum_of_partition_lengths))
        if sum_of_partition_lengths != num_nodes:
            logger.error("[Error]: sum_of_partition_lengths is " + str(sum_of_partition_lengths)
                + " but num_nodes is " + str(num_nodes))
    else: # use_shared_partitions_groups so computing PageRank
        if not use_page_rank_group_partitions:
            if not use_struct_of_arrays_for_pagerank:
                shared_partition_length = len(BFS_Shared.shared_partition)
                # added shadow nodes and their parents
                shared_partition_length -= (total_loop_nodes_added + (2*num_shadow_nodes_added_to_partitions))
                logger.info("shared_partition_length (not counting total_loop_nodes_added or shadow_nodes and their parents added): " 
                    + str(shared_partition_length))
                if shared_partition_length != num_nodes:
                    logger.error("[Error]: shared_partition_length is " + str(shared_partition_length)
                        + " but num_nodes is " + str(num_nodes))
            else:
                pass
                # we are not asserting anything about the length of the arrays
                # in the struct_of_arrays. These arrays length were calculated
                # and we are not checking that calculation here.

    logger.info("")
    sum_of_groups_lengths = 0
    i = 1
    for x in groups:
        sum_of_groups_lengths += len(x)
        logger.debug(str(i) + ": length of group: " + str(len(x)))
        i+=1
    logger.debug("num_shadow_nodes_added_to_groups: " + str(num_shadow_nodes_added_to_groups))
    if not use_shared_partitions_groups:
        logger.info("total_loop_nodes_added : " + str(total_loop_nodes_added))
        sum_of_groups_lengths -= (total_loop_nodes_added + num_shadow_nodes_added_to_groups)
        logger.info("sum_of_groups_lengths (not counting total_loop_nodes_added or shadow_nodes and their parents added): " 
            + str(sum_of_groups_lengths))
        if sum_of_groups_lengths != num_nodes:
            logger.error("[Error]: sum_of_groups_lengths is " + str(sum_of_groups_lengths)
                + " but num_nodes is " + str(num_nodes))
    else: # use_shared_partitions_groups so computing PageRank
        if use_page_rank_group_partitions:
            if not use_struct_of_arrays_for_pagerank:
                shared_groups_length = len(BFS_Shared.shared_groups)
                logger.info("shared_groups_length first value: " + str(shared_groups_length))
                # added shadow nodes and their parents
                logger.info("total_loop_nodes_added : " + str(total_loop_nodes_added))
                logger.info("(2*num_shadow_nodes_added_to_groups):" + str(2*num_shadow_nodes_added_to_groups))
                shared_groups_length -= (total_loop_nodes_added + (2*num_shadow_nodes_added_to_groups))
                logger.info("shared_groups_length (not counting total_loop_nodes_added or shadow_nodes and their parents added): " 
                    + str(shared_groups_length))
                if shared_groups_length != num_nodes:
                    logger.error("[Error]: shared_groups_length is " + str(shared_groups_length)
                        + " but num_nodes is " + str(num_nodes))
            else:
                pass
                # we are not asserting anything about the length of the arrays
                # in the struct_of_arrays. These arrays length were calculated
                # and we are not checking that calculation here.

    #if (len(current_partition)-loop_nodes_added) != num_nodes

    print_val = ""
    for x in current_partition:
        print_val += str(x) + " "
        # logger.info(x, end=" ")
        logger.info(print_val)
        logger.info("")

    # adjusting for loop_nodes_added in dfs_p
    sum_of_changes = sum(dfs_parent_changes_in_partiton_size)-num_shadow_nodes_added_to_partitions
    avg_change = sum_of_changes / len(dfs_parent_changes_in_partiton_size)
    print_val = "dfs_parent_changes_in_partiton_size length, len: " + str(len(dfs_parent_changes_in_partiton_size)) + ", sum_of_changes: " + str(sum_of_changes)
    print_val += ", average dfs_parent change: %.1f" % avg_change
    logger.info(print_val)
    if PRINT_DETAILED_STATS:
        if sum_of_changes != num_nodes:
            logger.error("[Error]: sum_of_changes is " + str(sum_of_changes)
                + " but num_nodes is " + str(num_nodes))
        print_val = ""
        for x in dfs_parent_changes_in_partiton_size:
            print_val += str(x) + " "
            # print(x, end=" ")
        logger.info(print_val)

    logger.info("")
    logger.info("")
    if PRINT_DETAILED_STATS:
        # adjusting for loop_nodes_added in dfs_p
        sum_of_changes = sum(dfs_parent_changes_in_frontier_size)
        logger.info("dfs_parent_changes_in_frontier_size length, len: " + str(len(dfs_parent_changes_in_frontier_size))
            + ", sum_of_changes: " + str(sum_of_changes))
        if sum_of_changes != num_nodes:
            logger.error("[Error]: sum_of_changes is " + str(sum_of_changes)
                + " but num_nodes is " + str(num_nodes))
        for x in dfs_parent_changes_in_frontier_size:
            print_val = str(x) + " "
            #print(x, end=" ")
        logger.info(print_val)
        logger.info("")
        logger.info("")
    #logger.info("frontier length: " + str(len(frontier)))
    #if len(frontier) != 0:
    #    logger.error("[Error]: frontier length is " + str(len(frontier))
    #       + " but num_nodes is " + str(num_nodes))
    #for x in frontier:
    #    logger.info(str(x.ID), end=" ")
    #logger.info("")
    #logger.info("frontier cost: " + str(len(frontier_cost)))
    #for x in frontier_cost:
    #    logger.info(str(x), end=" ")
    #logger.info("")
    # final frontier shoudl always be empty
    # assert: 
    logger.info("frontiers: (final fronter should be empty), number of frontiers: " + str(len(frontiers))+ " (length):")
    for frontier_list in frontiers:
        if PRINT_DETAILED_STATS:
            print_val = "-- (" + str(len(frontier_list)) + "): "
            for x in frontier_list:
                #logger.info(str(x.ID),end=" ")
                print_val += str(x) + " "
                #print(str(x),end=" ")
            logger.info(print_val)
            logger.info("")
        else:
            logger.info("-- (" + str(len(frontier_list)) + ")") 
    frontiers_length = len(frontiers)
    if len(frontiers[frontiers_length-1]) != 0:
        logger.info ("Error]: final frontier is not empty.")
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
                #    logger.info(str(index),end=" ")
                #else:
                #   logger.info(str(index)+"-s",end=" ")
            logger.info(print_val)
            logger.info("")
        else:
            logger.info("-- (" + str(len(x)) + ")")
    logger.info("")
    logger.debug("Number of shadow nodes:")
    for num in partitions_num_shadow_nodes_list:
        logger.debug(num)
    logger.info("")
    logger.info("partition names, len: " + str(len(partition_names))+":")
    for name in partition_names:
        if PRINT_DETAILED_STATS:
            logger.info("-- " + name)
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
    logger.debug("Number of shadow nodes:")
    for num in groups_num_shadow_nodes_list:
        logger.debug(num)
    logger.info("")
    logger.info("group names, len: " + str(len(group_names))+":")
    for name in group_names:
        if PRINT_DETAILED_STATS:
            logger.info("-- " + name)
    logger.info("")
    logger.info("nodes_to_partition_maps (incl. shadow nodes but only last index), len: " + str(len(nodeIndex_to_partitionIndex_maps))+":")
    for m in nodeIndex_to_partitionIndex_maps:
        if PRINT_DETAILED_STATS:
            print_val = ""
            print_val += "-- (" + str(len(m)) + "):" + " "
            for k, v in m.items():
                print_val += str((k, v)) + " "
                #print((k, v),end=" ")
            logger.info(print_val)
            logger.info("")
        else:
            logger.info("-- (" + str(len(m)) + ")")
    logger.info("")
    logger.info("nodes_to_group_maps, ( but only last index), len: " + str(len(nodeIndex_to_groupIndex_maps))+":")
    for m in nodeIndex_to_groupIndex_maps:
        if PRINT_DETAILED_STATS:
            #print("-- (" + str(len(m)) + "):", end=" ")
            print_val = ""
            print_val += "-- (" + str(len(m)) + "):" + " "
            for k, v in m.items():
                print_val += str((k, v)) + " "
                #print((k, v),end=" ")
            logger.info(print_val)
            logger.info("")
        else:
            logger.info("-- (" + str(len(m)) + ")")
    logger.info("")
    if PRINT_DETAILED_STATS:
        logger.info("frontier costs (cost=length of frontier), len: " + str(len(frontier_costs))+":")
        print_val = ""
        for x in frontier_costs:
            print_val += "-- " + str(x)
            #logger.info("-- ",end="")
            #logger.info(str(x))
        logger.info(print_val)
        logger.info("")
    sum_of_partition_costs = 0
    for x in all_frontier_costs:
        words = x.split(':')
        cost = int(words[1])
        sum_of_partition_costs += cost
    logger.info("all frontier costs, len: " + str(len(all_frontier_costs)) + ", sum: " 
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
                logger.info(str(x))
                i = 0
            i += 1
        logger.info(print_val)
    logger.info("")
    """
    # Doing this for each node in each partition now (next)
    logger.info("")
    if PRINT_DETAILED_STATS:
        logger.info("Node frontier_parent_tuples:")
        for node in nodes:
            logger.info(str(node.ID) + ": frontier_parent_tuples: ", end = " ")
            for parent_tuple in node.frontier_parents:
                logger.info(str(parent_tuple), end=" ")
            logger.info("")
    else:
        logger.info("-- (" + str(len(x)) + ")")
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
                    # logger.info(node.ID,end=": ")
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
                    # logger.info(node.ID,end=": ")
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
    logger.info("frontier_groups_sum: " + str(frontier_groups_sum) + ", len(frontiers)-1: " 
        +  str(len(frontiers)-1))
    logger.info("Average number of frontier groups: " + (str(frontier_groups_sum / (len(frontiers)-1))))
    logger.info("")
    i#f True: # 
    if use_shared_partitions_groups: 
        logger.info("Shared partition map frontier_parent_tuples:")                 
        for (k,v) in BFS_Shared.shared_partition_frontier_parents_map.items():
            logger.debug(str(k) + ": " + str(v))
        logger.info("")
    #if True: # 
    if use_shared_partitions_groups:  
        logger.info("Shared groups map frontier_parent_tuples:")                  
        for (k,v) in BFS_Shared.shared_groups_frontier_parents_map.items():
            logger.debug(str(k) + ": " + str(v))
        logger.info("")
    logger.info("nodeIndex_to_partition_partitionIndex_group_groupIndex_map, len: " + str(len(nodeIndex_to_partition_partitionIndex_group_groupIndex_map)) + ":")
    logger.info("shadow nodes not mapped and not shown")
    if PRINT_DETAILED_STATS:
        for k, v in nodeIndex_to_partition_partitionIndex_group_groupIndex_map.items():
            logger.info((k, v))
        logger.info("")
    else:
        logger.info("-- (" + str(len(nodeIndex_to_partition_partitionIndex_group_groupIndex_map)) + ")")
    logger.info("")
    logger.info("Partition Node parents (shad. node is a parent), len: " + str(len(partitions))+":")
    for x in partitions:
        if PRINT_DETAILED_STATS:
            #logger.info("-- (" + str(len(x)) + "):", end=" ")
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
                #    logger.info(str(index),end=" ")
                #else:
                #   logger.info(str(index)+"-s",end=" ")
            logger.info("")
        else:
            logger.info("-- (" + str(len(x)) + ")")
    logger.info("")
    logger.info("Group Node parents (shad. node is a parent), len: " + str(len(partitions))+":")
    for x in groups:
        if PRINT_DETAILED_STATS:
            #logger.info("-- (" + str(len(x)) + "):", end=" ")
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
                #    logger.info(str(index),end=" ")
                #else:
                #   logger.info(str(index)+"-s",end=" ")
            logger.info("")
        else:
            logger.info("-- (" + str(len(x)) + ")")
    logger.info("")
    logger.info("Group Node num_children, len: " + str(len(groups))+":")
    for x in groups:
        if PRINT_DETAILED_STATS:
            #logger.info("-- (" + str(len(x)) + "):", end=" ")
            print_val = ""
            for node in x:
                print_val += str(node) + ":" + str(node.num_children) + ", "
                #print(str(node) + ":" + str(node.num_children),end=", ")
            logger.info(print_val)
            logger.info("")
        else:
            logger.info("-- (" + str(len(x)) + ")")
    logger.info("")
    logger.info("Partition_senders, len: " + str(len(Partition_senders)) + ":")
    if PRINT_DETAILED_STATS:
        for k, v in Partition_senders.items():
            logger.info((k, v))
        logger.info("")
    else:
        logger.info("-- (" + str(len(Partition_senders)) + ")")
        logger.info("")
    logger.info("Partition_receivers, len: " + str(len(Partition_receivers)) + ":")
    if PRINT_DETAILED_STATS:
        for k, v in Partition_receivers.items():
            logger.info((k, v))
        logger.info("")
    else:
        logger.info("-- (" + str(len(Partition_receivers)) + ")")
        logger.info("")
    logger.info("Group_senders, len: " + str(len(Group_senders)) + ":")
    if PRINT_DETAILED_STATS:
        for k, v in Group_senders.items():
            logger.info((k, v))
        logger.info("")
    else:
        logger.info("-- (" + str(len(Group_senders)) + ")")
        logger.info("")

    logger.info("Group_receivers, len: " + str(len(Group_receivers)) + ":")
    if PRINT_DETAILED_STATS:
        for k, v in Group_receivers.items():
            logger.info((k, v))
    else:
        logger.info("-- (" + str(len(Group_receivers)) + ")")
        logger.info("")


    generate_DAG_info()
    #visualize()
    #input('Press <ENTER> to continue')

    logger.debug("Output partitions/groups")

    output_partitions()
#rhc:  incremental
# 1. perhaps invoker_thread.join() here when inc dag gen
# 2. No show DAG_info stats until after join() when inc dag gen?
#    we will how stats as we gen DAG incrementally?
    run()
    """
    if use_shared_partitions_groups and use_struct_of_arrays_for_pagerank:
        logger.debug("\nBFS:Close and unlink shared memory.")
        try:
            BFS_Shared.close_shared_memory()
            BFS_Shared.unlink_shared_memory()
        except Exception as ex:
            logger.debug("[ERROR] BFS: Failed to close or unlink shared memory.")
            logger.debug(ex)
    """

"""
logger.debug("Sorted simple cycles:")
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
logger.debug("Diameter:" + str(diameter))
aspl = mean([max(j.values()) for (i,j) in nx.shortest_path_length(G)])
logger.debug("avg shortest path lengh:" + str(aspl))

# (node for node, in_degree in G.in_degree() if in_degree == 0)
"""