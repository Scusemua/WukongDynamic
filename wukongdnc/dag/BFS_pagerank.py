import logging
import cloudpickle
#import numpy as np
from .BFS_Partition_Node import Partition_Node

logger = logging.getLogger(__name__)
"""
logger.setLevel(logging.DEBUG)
#logger.setLevel(logging.INFO)
formatter = logging.Formatter('[%(asctime)s] [%(threadName)s] %(levelname)s: %(message)s')
#formatter = logging.Formatter('%(levelname)s: %(message)s')
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
#ch.setLevel(logging.INFO)
ch.setFormatter(formatter)
logger.addHandler(ch)
"""

debug_pagerank = False

"""

    Perhaps:
        group_name_list = ["PR1_1", "PR2_1", "PR2_2", "PR2_3", "PR3_1", "PR3_2"]
        DAG_tasks = dict.fromkeys(key_list,PageRank)
    partition_name_list = ["PR1_1", "PR2_1", "PR3_1"]
    where:
        def func(value=i):
            logger.info value
        funcs.append(func)
    where:
    #new_func='def receiverY(task_name, set, input2):\n  return x+1'
"""

"""
    first = True
    comma = ""
    receiverY = "PR2_1"
    PageRank_func = "def " + receiverY + "(task_name, "
    #for receiverY in Receivers:
    sender_set_for_receiverY = Partition_receivers[receiverY]
    for senderZ in sender_set_for_receiverY:
        if first:
            pass
        else:
            comma = ","
            first = False
        PageRank_func += comma + str(senderZ)
        PageRank_func += "):\n  logger.info(1)"
        #where FOOO is a simple body for PageRank_task, which calls the actual task
    #or
    #for i, senderZ in enumerate(sender_set_for_receiverY):
    #if i: new_func += "," + str(senderZ)
    #else: new_func += str(senderZ)
    logger.info("PageRank_func: ")
    logger.info(PageRank_func)
    the_code=compile(PageRank_func,'<string>','exec')
""" 

# Called by DAG task to read its partition from storage.
# DAG with name task_name calls:
#    partition_file_name = "./"+task_name+".pickle"
#    partition = input_PageRank_partition(partition_file_name)
def input_PageRank_nodes_and_partition(partition_file_name):
    # Example file name: './PA1_partition.pickle'
    with open(partition_file_name, 'rb') as handle:
        nodes_and_partition = cloudpickle.load(handle)
    nodes = nodes_and_partition["nodes"]
    partition = nodes_and_partition["partition"]
    # Example partition, for "PR1" of graph_20: [5, 17, 1]
    return nodes, partition

def normalize_PageRank(nodes):
    pagerank_sum = sum(node.pagerank for node in nodes)
    for node in nodes:
        node.pagerank /= pagerank_sum

"""
def PageRank_one_iter(target_nodes,partition,damping_factor):
    for target_node_index in target_nodes:
        nodes[target_node_index].update_PageRank_main(damping_factor, len(nodes))
        logger.info("PageRank: target_index isShadowNode: " 
            + str(nodes[target_node_index].isShadowNode))
    normalize_PageRank(nodes)
"""

def PageRank_Function_one_iter(partition_or_group,damping_factor,
    one_minus_dumping_factor,random_jumping,total_num_nodes,num_nodes_for_pagerank_computation):
    #for index in range(len(partition_or_group)):
    for index in range(num_nodes_for_pagerank_computation):
        # Need number of non-shadow nodes'
#rhc: handle shadow nodes
        if partition_or_group[index].isShadowNode:
            #if (debug_pagerank):
            logger.debug("PageRank: before pagerank computation: node at position " 
            + str(index) + " isShadowNode: " 
            + str(partition_or_group[index].isShadowNode) 
            + ", pagerank: " + str(partition_or_group[index].pagerank)
            + ", parent: " + str(partition_or_group[index].parents[0])
            + ", (real) parent's num_children: " + str(partition_or_group[index].num_children)
            )
        #if (debug_pagerank):
        #    logger.debug("")

        #print(str(partition_or_group[index].ID) + " type of node: " + str(type(partition_or_group[index])))
        #if not partition_or_group[index].isShadowNode:
        partition_or_group[index].update_PageRank_of_PageRank_Function(partition_or_group, 
            damping_factor,one_minus_dumping_factor,random_jumping,total_num_nodes)

        if partition_or_group[index].isShadowNode:
            #if (debug_pagerank):
            logger.debug("PageRank:  after pagerank computation: node at position " 
            + str(index) + " isShadowNode: " 
            + str(partition_or_group[index].isShadowNode) 
            + ", pagerank: " + str(partition_or_group[index].pagerank)
            + ", parent: " + str(partition_or_group[index].parents[0])
            + ", (real) parent's num_children: " + str(partition_or_group[index].num_children)
            )
        #if (debug_pagerank):
        logger.debug("")
#rhc: ToDo: do this?
    #normalize_PageRank(nodes)

def PageRank_Function_Driver(task_file_name,total_num_nodes,results_dictionary):
    input_tuples = []
    for (_,v) in results_dictionary.items():
        # pagerank leaf tasks have no input. This results in a rresult_dictionary
        # in DAG_executor of "DAG_executor_driver_0" --> (), where
        # DAG_executor_driver_0 is used to mean that eh DAG_excutor_driver
        # provided an empty input tuple fpr the leaf task. Here, we just ignore
        # empty input tuples so that the input_tuples provided to the 
        # PageRank_Function will be an empty list.
        if not v ==  ():
            input_tuples += v
    output = PageRank_Function(task_file_name,total_num_nodes,input_tuples)
    return output


#def PageRank_Function(task_file_name,total_num_nodes,input_tuples,results):
def PageRank_Function(task_file_name,total_num_nodes,input_tuples):
        # task_file_name is, e.g., "PR1_1" not "PR1_1.pickle"
        # We check for task_file_name ending with "L" for loop below,
        # so we make this check esy by having 'L' at the end (endswith)
        # instead of having to parse ("PR1_1.pickle")
        complete_task_file_name = './'+task_file_name+'.pickle'
        with open(complete_task_file_name, 'rb') as handle:
            partition_or_group = (cloudpickle.load(handle))
        if (debug_pagerank):
            logger.debug("PageRank_Function output partition_or_group (node:parents):")
            for node in partition_or_group:
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
            for node in partition_or_group:
                print_val += str(node)+":"+str(node.num_children) + ", "
                # logger.debug(str(node)+":"+str(node.num_children),end=", ")
            logger.debug(print_val)
            logger.debug("")

            logger.debug("")
            # node's children set when the partition/grup node created

        num_shadow_nodes = 0
        for node in partition_or_group:
            if node.isShadowNode:
                num_shadow_nodes += 1

        #actual_num_nodes = len(partition_or_group)-num_shadow_nodes

        damping_factor=0.15
        random_jumping = damping_factor / total_num_nodes
        one_minus_dumping_factor = 1 - damping_factor

        iteration = -1
        if not task_file_name.endswith('L'):
            iteration = int(1)
        else:
            iteration = int(10)

        num_nodes_for_pagerank_computation = len(partition_or_group)

        i=0
        for tup in input_tuples:
            logger.debug("PageRank_Function: input tuple:" + str(tup))
            shadow_node_index = tup[0]
            pagerank_value = tup[1]
            # assert
            if not partition_or_group[shadow_node_index].isShadowNode:
                logger.debug("[Error]: Internal Error: input tuple " + str(tup))
            # If shadow_node x is a shadow_node for node y (where the one or more
            # shadow nodes of y are immediatley preceeding y) then shadow_node x
            # represents a parent node of y that was in a different partition P or 
            # group G. P/G will send the pagerank value for parent to the partition
            # or group for x and y. We ser the pagerank for the shadow_node equal to this
            # received pagerank value. 
            # We will use the shadow_node's pagerank as the pagerank value for one of 
            # y's parents (there may be shadow_nodes for other parents of y and y may
            # have parents in its grup/partition). We have two choices: (1) do not compuet
            # the pagerank value of a shadow_node; this prevents the shadow_node's pagerank
            # value from changing but we need an if-statement to check whether a node ia 
            # a shadow_node. Choce (2) is to give the shadow_noe a parent node that is 
            # out of the pagerank computation's range and set the pagerank of the
            # shadow_node's parent such that when we compute the pagerank of the 
            # shadow_node we always get the same value. For this case, we avoid the
            # if-statement in the tight pagerank loop. So we avoid missed branch
            # predctions by the hardware. Noet that there is a limit to the number of
            # missed predictions allowed if out tight loop is to be considered by 
            # the loop-stream detector as a loop whose micro ops can be buffered
            # avoiding the re-decoding of the loop's machine instructions on ech 
            # iteration of the loop. (The frontend of the instruction cycle can be 
            # powered off also.)
            #
            # pagerank of shadow_node is the pagerank value (of a parent of the 
            # shadow_node received from the parents partition/group executor.
            partition_or_group[shadow_node_index].pagerank = pagerank_value
            # IDs: -1, -2, -3, etc
            shadow_node_ID = partition_or_group[shadow_node_index].ID
            parent_of_shadow_node_ID = str(shadow_node_ID) + "-s-p"
            parent_of_shadow_node = Partition_Node(parent_of_shadow_node_ID)
            # set the pagerank of the parent_of_shadow_node so that when we recompute
            # the pagerank of the shadow_node we alwas get the same value.
            parent_of_shadow_node.pagerank = (
                (partition_or_group[shadow_node_index].pagerank - random_jumping)  / one_minus_dumping_factor)
            # if (debug_pagerank):
            logger.debug(parent_of_shadow_node_ID + " pagerank set to: " + str(parent_of_shadow_node.pagerank))
            # num_children = 1 makes the computation easier; the computation assumed
            # num_children was set to 1
            parent_of_shadow_node.num_children = 1
            # appending new nodes at the end
            partition_or_group.append(parent_of_shadow_node)
            partition_or_group[shadow_node_index].parents[0] = num_nodes_for_pagerank_computation + i
            i += i+1
        if (debug_pagerank):
            logger.debug("")
            logger.debug("PageRank_Function output partition_or_group after add " + str(len(input_tuples)) + " SN parents (node:parents):")
            for node in partition_or_group:
                print_val = str(node) + ":"
                # print(node,end=":")
                for parent in node.parents:
                    #print(parent,end=" ")
                    print_val += str(parent) + " "
                if len(node.parents) == 0:
                    #print(" ,",end=" ")
                    print_val += " ,"
                else:
                    #print(",",end=" ")
                    print_val += ","
                logger.debug(print_val)
            logger.debug("")

        if task_file_name.endswith('L'):
            for index in range(num_nodes_for_pagerank_computation):
                partition_or_group[index].prev = (1/total_num_nodes)

        for i in range(1,iteration+1):
            if (debug_pagerank):
                logger.debug("***** PageRank: iteration " + str(i))
                logger.debug("")

            #PageRank_Function_one_iter(partition_or_group,damping_factor,one_minus_dumping_factor,random_jumping,total_num_nodes,num_nodes_for_pagerank_computation)
    
            for index in range(num_nodes_for_pagerank_computation):
                # Need number of non-shadow nodes'
        #rhc: handle shadow nodes
                if partition_or_group[index].isShadowNode:
                    #if (debug_pagerank):
                    logger.debug("PageRank: before pagerank computation: node at position " 
                    + str(index) + " isShadowNode: " 
                    + str(partition_or_group[index].isShadowNode) 
                    + ", pagerank: " + str(partition_or_group[index].pagerank)
                    + ", parent: " + str(partition_or_group[index].parents[0])
                    + ", (real) parent's num_children: " + str(partition_or_group[index].num_children)
                    )

                #if (debug_pagerank):
                #    logger.debug("")

                #print(str(partition_or_group[index].ID) + " type of node: " + str(type(partition_or_group[index])))
                #if not partition_or_group[index].isShadowNode:

                if not task_file_name.endswith('L'):
                    partition_or_group[index].update_PageRank_of_PageRank_Function(partition_or_group, 
                        damping_factor,one_minus_dumping_factor,random_jumping,total_num_nodes)
                else:
                    partition_or_group[index].update_PageRank_of_PageRank_Function_loop(partition_or_group, 
                        damping_factor,one_minus_dumping_factor,random_jumping,total_num_nodes)
    
                if partition_or_group[index].isShadowNode:
                    #if (debug_pagerank):
                    logger.debug("PageRank:  after pagerank computation: node at position " 
                    + str(index) + " isShadowNode: " 
                    + str(partition_or_group[index].isShadowNode) 
                    + ", pagerank: " + str(partition_or_group[index].pagerank)
                    + ", parent: " + str(partition_or_group[index].parents[0])
                    + ", (real) parent's num_children: " + str(partition_or_group[index].num_children)
                    )

                #if (debug_pagerank):
                #logger.debug("")

            if task_file_name.endswith('L'):
                for index in range(num_nodes_for_pagerank_computation):
                    partition_or_group[index].prev = partition_or_group[index].pagerank
        
        """
        if run_all_tasks_locally and using_threads_not_processes:
            logger.info("PageRanks: ")
            for i in range(num_nodes_for_pagerank_computation):
                if not partition_or_group[i].isShadowNode:
                    my_ID = str(partition_or_group[i].ID)
                    results[partition_or_group[i].ID] = partition_or_group[i].pagerank
                else:
                    my_ID = str(partition_or_group[i].ID) + "-s"
                logger.info(partition_or_group[i].toString_PageRank())
        """

        if (debug_pagerank):
            logger.debug("")
            logger.debug("Frontier Parents:")
            for i in range(len(partition_or_group)):
                if not partition_or_group[i].isShadowNode:
                    my_ID = str(partition_or_group[i].ID)
                else:
                    my_ID = str(partition_or_group[i].ID) + "-s"
                logger.debug("ID:" + my_ID + " frontier_parents: " + str(partition_or_group[i].frontier_parents))
            logger.debug("")
        """
        ID:5 frontier_parents: [(2, 1, 2)]
        ID:17 frontier_parents: [(2, 2, 5)]
        ID:1 frontier_parents: [(2, 3, 3)]
        """
        PageRank_output = {}
        for i in range(len(partition_or_group)):
            if len(partition_or_group[i].frontier_parents) > 0:
                for frontier_parent in partition_or_group[i].frontier_parents:
                    #partition_number = frontier_parent[0]
                    #group_number = frontier_parent[1]
                    parent_or_group_index = frontier_parent[2]
                    # Passing name in tuple so that name for loop partition/groups
                    # will have an "l" at the end
                    #partition_or_group_name = "PR"+str(partition_number)+"_"+str(group_number)
                    partition_or_group_name = frontier_parent[3]
                    output_list = PageRank_output.get(partition_or_group_name)
                    if output_list == None:
                        output_list = []
                    output_tuple = (parent_or_group_index,partition_or_group[i].pagerank)
                    output_list.append(output_tuple)
                    PageRank_output[partition_or_group_name] = output_list
        #if (debug_pagerank):
        print("PageRank output tuples for " + task_file_name + ":")
        print_val = ""
        for k, v in PageRank_output.items():
            #print_val += "(%s, %s) " % (k, v)
            print((k, v),end=" ")
        #print(print_val)
        print()
        print()

        print("PageRank result for " + task_file_name + ":", end=" ")
        for i in range(num_nodes_for_pagerank_computation):
            if not partition_or_group[i].isShadowNode:
                print(str(partition_or_group[i].ID) + ":" + str(partition_or_group[i].pagerank),end=" ")
        print()
        print()
        """
        logger.debug("PageRank result for " + task_file_name + ":")
        for i in range(num_nodes_for_pagerank_computation):
            if not partition_or_group[i].isShadowNode:
                print(str(partition_or_group[i].ID) + ":" + str(partition_or_group[i].pagerank))
        logger.debug("")
        logger.debug("")
        """
        return PageRank_output

def PageRank_Task(task_file_name,total_num_nodes,payload,results):
    input_tuples = payload['input']
    # sort inut tuples so that they are in shadow_node order, left to right.
    # The first index of tuple is index of the shadow_node in the input_tuples
    # so sort will sort on these indices resulting in shadow_node order.
    # The shadow_node parents are appended to the end of the input tuple as
    # we process the input_tuple so the order f the shadow_nodes and the 
    # shadow_node parents will be the will be the same.
    #
    # This sort is not necessary; it just helps with the visual during debugging.
    input_tuples.sort()
    #if (debug_pagerank):
    logger.debug(task_file_name + " input tuples: ")
    for tup in input_tuples:
        print(tup,end=" ")
    logger.debug("")
    logger.debug("")
    #PageRank_output = PageRank_Function(task_file_name,total_num_nodes,input_tuples,results)
    PageRank_output = PageRank_Function(task_file_name,total_num_nodes,input_tuples)
    return PageRank_output

#rhc: the actual pagerank will be working on Nodes not node indices?
# So we need a new PageRank for the DAG execution.
# The first node will be in position 0? Normally node i is in position i
# but there is no node 0 so no Node in position 0.
"""
def PageRank_main(target_nodes, partition):
    logger.info("PageRank:partition is:" + str(partition))
    damping_factor=0.15
    iteration=int(1)
    for i in range(iteration):
        logger.info("***** PageRank: iteration " + str(i))
        logger.info("")
        PageRank_one_iter(target_nodes,partition,damping_factor)
    logger.info("PageRank: partition is: " + str(partition))
"""

"""
rhc: ToDo: If we add one or more shadow nodes before each dependent node in 
the partition (where a dependent node is a node whose parent is in the 
previous partition), we have to either (1) add an if-statement to the 
pagernk calculation so that we do not calculate the pagerank values for 
shadow nodes, since we do not want to change the pagerank values of 
shadow nodes, or (2) we do something to ensure that the value caclulated
the shadow node is always the same as the original value. For the latter
case, we would not need an if-statement in the pagerank caclulation which 
may speed it up. 

Note: This branch would be pretty random and hard to predict
by the branch_predictor? Also, we can implement the nodes as a strct of
arrays instead of an array of structs, which minimizes the cache misses
for nodes. The PageRank calculation instructions are small and should easily
fit in the i-cache. Pagerank is a loop so we might want to try to make sure
it is being detected by the Loop Stream Detector aad is small enough to be 
executd out of the decoded micro-op cache:
https://www.anandtech.com/show/3922/intels-sandy-bridge-architecture-exposed/2
or whatever happens in the latest archtectures.

The PageRank code is:
    parent_nodes = self.parents
    # for shadow nodes, there is only one node_index and its value is i (see below)
    pagerank_sum = sum((nodes[node_index].pagerank / len(nodes[node_index].children)) for node_index in in_nodes)
    random_jumping = damping_factor / num_nodes
    logger.info("damping_factor:" + str(damping_factor) + " num_nodes: " + str(num_nodes) + " random_jumping: " + str(random_jumping))
    self.pagerank = random_jumping + (1-damping_factor) * pagerank_sum
    where nodes is an array of Nodes and node i is strored at nodes[i]

Assume the shadow node index is i and its pagerank value is pr. 
Set the shadows node's only parent to be parent_index and only child to be itself.
  The parent_index will be the index of a node that is not in the partition, i.e.,
  if the partition is nodes 1..20 then the parent_index can be the node at position 21.
We want the value computed for self.pagerank to always be pr.
Let the value of random_jumping be j., and the value of (1-damping_factor) be nd.
Given the statement to compute the value of self.pagerank:
    self.pagerank = random_jumping + (1-damping_factor) * pagerank_sum,
this evaluates to 
    self.pagerank = j + nd * pagerank_sum.
We want self.pagerank to evaluate to pr. We can only control the value of 
pagerank_sum, so 
    pr = j + nd * pagerank_sum ==>  pagerank_sum = (pr - j) / nd.
Since pagerank_sum is the pagerank value of the shadow node's parent
divided by 1, we should set the shadow nodes's parent's pagerank value to be 
(pr - j) / nd. This guarantees that we will always compute a pagerank of pr for
the shadow node. 

As I mentioned, we can add a parent node for each shadow node at postions just
past the end of the partition. Since these parent nodes are not in the partition
their pagerank values will not be changed and thus the shadow node's pagerank
values will not change.

"""

# We are using the DAG_excutor routines to execure DAG, like normal.
# PageRank specific actions need to be done by the PageRank task.
# This will change as we incrementally update the functionality.
"""
# Using input_PageRank_nodes_and_partition
def PageRank(dependents,task_name):
#rhc: not clear whether we can add task_name as an arg to the task in the 
# DAG_executor_work_loop. If so, we could also add nodes and partition
# as parms (for small sizes)?
# ToDo: Added the fanout/faninNB/collapse dependents to the state_info
# what's next?
    partition_file_name = "./" + task_name + "_nodes_and_partition.pickle"
    nodes, partition = input_PageRank_nodes_and_partition(partition_file_name)
    # overwite nodes[i] with delegate i
    logger.info("PageRank: partition is: " + str(partition))
    damping_factor=0.15
    iteration=int(10)
    for i in range(iteration):
        logger.info("***** PageRank: iteration " + str(i))
        logger.info("")
        PageRank_one_iter(nodes,partition,damping_factor)
    logger.info("PageRank: partition is: " + str(partition))

def get_PageRank_list(nodes):
    pagerank_list = np.asarray([node.pagerank for node in nodes], dtype='float32')
    return np.round(pagerank_list, 3)
"""

"""
target_nodes = [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20]
total_num_nodes = 20
PageRank_main(target_nodes,target_nodes,total_num_nodes)
np_array = get_PageRank_list(nodes)
logger.info(str(np_array))
"""



"""
Note:
def apply(func, args, kwargs=None):
    #Apply a function given its positional and keyword arguments.

    Equivalent to ``func(*args, **kwargs)``
    Most Dask users will never need to use the ``apply`` function.
    It is typically only used by people who need to inject
    keyword argument values into a low level Dask task graph.

    Parameters
    ----------
    func : callable
        The function you want to apply.
    args : tuple
        A tuple containing all the positional arguments needed for ``func``
        (eg: ``(arg_1, arg_2, arg_3)``)
    kwargs : dict, optional
        A dictionary mapping the keyword arguments
        (eg: ``{"kwarg_1": value, "kwarg_2": value}``

    Examples
    --------
    >>> from dask.utils import apply
    >>> def add(number, second_number=5):
    ...     return number + second_number
    ...
    >>> apply(add, (10,), {"second_number": 2})  # equivalent to add(*args, **kwargs)
    12

    >>> task = apply(add, (10,), {"second_number": 2})
    >>> dsk = {'task-name': task}  # adds the task to a low level Dask task graph
    """
# Implementation:
"""
    if kwargs:
        return func(*args, **kwargs)
    else:
        return func(*args)
"""
#PR1_1_Task = apply(PR1_1, ("PR1_1", ), ????)
#   >>> task = apply(add, (10,), {"second_number": 2})
#    >>> dsk = {'task-name': task}  # adds the task to a low level Dask task graph

"""
An estimate of the number of iterations needed to converge to a tolerance τ is log10 τ / log10 α [1]. For τ = 10
-6 and α =0.85, it can take roughly 85 iterations to converge. For α = 0.95, and α = 0.75,
with the same tolerance τ = 10-6, it takes roughly 269 and 48 iterations
respectively. For τ = 10-9, and τ = 10-3, with the same damping factor α =2
0.85, it takes roughly 128 and 43 iterations respectively. Thus, adjusting the
damping factor or the tolerance parameters of the PageRank algorithm can
have a significant effect on the convergence rate.

Is this estimate based on the total number of nodes? Looks like it isn't. A 
loop group is just part of the nodes.

Adjustment of the damping factor α is a delicate balancing act. For smaller
values of α, the convergence is fast, but the link structure of the graph used
to determine ranks is less true. Slightly different values for α can produce 3
very different rank vectors. Moreover, as α → 1, convergence slows down
drastically, and sensitivity issues begin to surface [1].
"""