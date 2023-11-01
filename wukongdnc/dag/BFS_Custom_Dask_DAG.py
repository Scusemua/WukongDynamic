import logging
import cloudpickle
# May want to use numpy arrays eventually for better cache performance
#import numpy as np
from .BFS_Partition_Node import Partition_Node

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter('[%(asctime)s] [%(module)s] [%(processName)s] [%(threadName)s]: %(message)s')
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch.setFormatter(formatter)
logger.addHandler(ch)

# Note: The pagerank function needs to know the total number of nodes
# in the graph. This value could be assigned here when we generate 
# this program. Or Wukng could input it somehow.
total_num_nodes = 20

# Note: Turning off debug for everything except outputting the computed 
# pagerank values, which are output at the end of PageRank_Function.
debug_pagerank = False

def PageRank_Function(task_file_name,total_num_nodes,input_tuples):
        # task_file_name is, e.g., "PR1_1" not "PR1_1.pickle"
        # We check for task_file_name ending with "L" for loop below,
        # so we make this check esy by having 'L' at the end (endswith)
        # instead of having to parse ("PR1_1.pickle")

# ToDo: Adjust input scheme for Wukong.

        complete_task_file_name = './'+task_file_name+'.pickle'
        with open(complete_task_file_name, 'rb') as handle:
            partition_or_group = (cloudpickle.load(handle))
        if (debug_pagerank):
            logger.trace("PageRank_Function output partition_or_group (node:parents):")
            for node in partition_or_group:
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
            for node in partition_or_group:
                print_val += str(node)+":"+str(node.num_children) + ", "
                # logger.trace(str(node)+":"+str(node.num_children),end=", ")
            logger.trace(print_val)
            logger.trace("")

            logger.trace("")
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
            logger.trace("PageRank_Function: input tuple:" + str(tup))
            shadow_node_index = tup[0]
            pagerank_value = tup[1]
            # assert
            if not partition_or_group[shadow_node_index].isShadowNode:
                logger.trace("[Error]: Internal Error: input tuple " + str(tup))
            # If shadow_node x is a shadow_node for node y (where the one or more
            # shadow nodes of y are immediatley preceeding y) then shadow_node x
            # represents a parent node of y that was in a different partition P or 
            # group G. P/G will send the pagerank value for parent to the partition
            # or group for x and y. We set the pagerank for the shadow_node equal to this
            # received pagerank value. 
            #
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
            logger.trace(parent_of_shadow_node_ID + " pagerank set to: " + str(parent_of_shadow_node.pagerank))
            # num_children = 1 makes the computation easier; the computation assumed
            # num_children was set to 1
            parent_of_shadow_node.num_children = 1
            # appending new nodes at the end
            partition_or_group.append(parent_of_shadow_node)
            partition_or_group[shadow_node_index].parents[0] = num_nodes_for_pagerank_computation + i
            i += i+1
        if (debug_pagerank):
            logger.trace("")
            logger.trace("PageRank_Function output partition_or_group after add " + str(len(input_tuples)) + " SN parents (node:parents):")
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
                logger.trace(print_val)
            logger.trace("")

        if task_file_name.endswith('L'):
            for index in range(num_nodes_for_pagerank_computation):
                partition_or_group[index].prev = (1/total_num_nodes)

        for i in range(1,iteration+1):
            if (debug_pagerank):
                logger.trace("***** PageRank: iteration " + str(i))
                logger.trace("")

            #PageRank_Function_one_iter(partition_or_group,damping_factor,one_minus_dumping_factor,random_jumping,total_num_nodes,num_nodes_for_pagerank_computation)
    
            for index in range(num_nodes_for_pagerank_computation):
                # Need number of non-shadow nodes
                """
                #logger.trace(str(partition_or_group[index].ID) + " type of node: " + str(type(partition_or_group[index])))
                if partition_or_group[index].isShadowNode:
                    #if (debug_pagerank):
                    logger.trace("PageRank: before pagerank computation: node at position " 
                    + str(index) + " isShadowNode: " 
                    + str(partition_or_group[index].isShadowNode) 
                    + ", pagerank: " + str(partition_or_group[index].pagerank)
                    + ", parent: " + str(partition_or_group[index].parents[0])
                    + ", (real) parent's num_children: " + str(partition_or_group[index].num_children)
                    )
                """

                if (debug_pagerank):
                    logger.trace("")

                if not task_file_name.endswith('L'):
                    partition_or_group[index].update_PageRank_of_PageRank_Function(partition_or_group, 
                        damping_factor,one_minus_dumping_factor,random_jumping,total_num_nodes)
                else:
                    partition_or_group[index].update_PageRank_of_PageRank_Function_loop(partition_or_group, 
                        damping_factor,one_minus_dumping_factor,random_jumping,total_num_nodes)
    
                """
                if partition_or_group[index].isShadowNode:
                    #if (debug_pagerank):
                    logger.trace("PageRank:  after pagerank computation: node at position " 
                    + str(index) + " isShadowNode: " 
                    + str(partition_or_group[index].isShadowNode) 
                    + ", pagerank: " + str(partition_or_group[index].pagerank)
                    + ", parent: " + str(partition_or_group[index].parents[0])
                    + ", (real) parent's num_children: " + str(partition_or_group[index].num_children)
                    )
                """

                if (debug_pagerank):
                    logger.trace("")

            if task_file_name.endswith('L'):
                for index in range(num_nodes_for_pagerank_computation):
                    partition_or_group[index].prev = partition_or_group[index].pagerank
        
        """
        if run_all_tasks_locally and using_threads_not_processes:
            logger.trace("PageRanks: ")
            for i in range(num_nodes_for_pagerank_computation):
                if not partition_or_group[i].isShadowNode:
                    my_ID = str(partition_or_group[i].ID)
                    results[partition_or_group[i].ID] = partition_or_group[i].pagerank
                else:
                    my_ID = str(partition_or_group[i].ID) + "-s"
                logger.trace(partition_or_group[i].toString_PageRank())
        """

        if (debug_pagerank):
            logger.trace("")
            logger.trace("Frontier Parents:")
            for i in range(len(partition_or_group)):
                if not partition_or_group[i].isShadowNode:
                    my_ID = str(partition_or_group[i].ID)
                else:
                    my_ID = str(partition_or_group[i].ID) + "-s"
                logger.trace("ID:" + my_ID + " frontier_parents: " + str(partition_or_group[i].frontier_parents))
            logger.trace("")
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
        if (debug_pagerank):
            logger.trace("PageRank output tuples for " + task_file_name + ":")
            print_val = ""
            for k, v in PageRank_output.items():
                print_val += "(%s, %s) " % (k, v)
            logger.trace(print_val)
            logger.trace("")
            logger.trace("")

# ToDo: Adjust output scheme for Wukong.

        logger.trace("PageRank result for " + task_file_name + ":")
        for i in range(num_nodes_for_pagerank_computation):
            if not partition_or_group[i].isShadowNode:
                print(str(partition_or_group[i].ID) + ":" + str(partition_or_group[i].pagerank))
        logger.trace("")
        logger.trace("")

        return PageRank_output

def PageRank_Function_Driver(task_file_name,total_num_nodes,results_dictionary):
    input_tuples = []
    for (_,v) in results_dictionary.items():
        if not v == ():
            input_tuples += v
    output = PageRank_Function(task_file_name,total_num_nodes,input_tuples)
    return output

dsk = { 'PR1_1':  (PageRank_Function_Driver            ),
        'PR2_1':  (PageRank_Function_Driver,   'PR1_1' ),
        'PR2_2L': (PageRank_Function_Driver,  ['PR1_1'  , 'PR2_1']),
        'PR2_3':  (PageRank_Function_Driver,   'PR1_1' ),
        'PR3_1':  (PageRank_Function_Driver,   'PR2_2L'),
        'PR3_2':  (PageRank_Function_Driver,  ['PR2_2L' , 'PR3_1']),
        'PR3_3':  (PageRank_Function_Driver,   'PR2_3' ) 
    }

from dask.threaded import get
get(dsk, ['PR3_1','PR3_2', 'PR3_3'])  # executes in parallel

# Questions:
# 1. Leaf tasks like PR1_1 have no inputs; is this sepecified correctly
#    in dsk DAG?
# 2. The tasks need to input their partitions. I use the task name as the file
#    name for the partition. Wukong needs to get this file somehow. 
# 3. Every pagerank task has an output. Right now, I just print the pagerank
#    values. Wukong can store the values somewhere. Dask DAGS are tree-structured,
#    like D&C DAGs, with a bunch of fanouts followed by a bunch of fanins.
#    For these DAGs it is convenient to have a "result" fanin at the end.
#    Pagerank doesn't have this tree structure so a "result" fanin is not
#    as convenient. Wuong can store the pagerank values wherever, including 
#    calling a "result" synchronization object on the server, which would 
#    gather the results. Whatever Wukong does bear in mind that there can be 
#    a large number of pagerank values to collect from lots of pagerank 
#    serverless functions.

"""
Note: The other option is to have one function per DAG task, which results 
in a lot more Python functions. 
def PR1_1(task_file_name,total_num_nodes,results_dictionary):
    input_tuples = []
    for (_,v) in results_dictionary.items():
        if not v == ():
            input_tuples += v
    output = PageRank_Function(task_file_name,total_num_nodes,input_tuples)
    return output
def PR2_1(task_file_name,total_num_nodes,results_dictionary):
    input_tuples = []
    for (_,v) in results_dictionary.items():
        if not v == ():
            input_tuples += v
    output = PageRank_Function(task_file_name,total_num_nodes,input_tuples)
    return output
def PR2_2L(task_file_name,total_num_nodes,results_dictionary):
    input_tuples = []
    for (_,v) in results_dictionary.items():
        if not v == ():
            input_tuples += v
    output = PageRank_Function(task_file_name,total_num_nodes,input_tuples)
    return output
def PR2_3(task_file_name,total_num_nodes,results_dictionary):
    input_tuples = []
    for (_,v) in results_dictionary.items():
        if not v == ():
            input_tuples += v
    output = PageRank_Function(task_file_name,total_num_nodes,input_tuples)
    return output
def PR3_1(task_file_name,total_num_nodes,results_dictionary):
    input_tuples = []
    for (_,v) in results_dictionary.items():
        if not v == ():
            input_tuples += v
    output = PageRank_Function(task_file_name,total_num_nodes,input_tuples)
    return output
def PR3_2(task_file_name,total_num_nodes,results_dictionary):
    input_tuples = []
    for (_,v) in results_dictionary.items():
        if not v == ():
            input_tuples += v
    output = PageRank_Function(task_file_name,total_num_nodes,input_tuples)
    return output
def PR3_3(task_file_name,total_num_nodes,results_dictionary):
    input_tuples = []
    for (_,v) in results_dictionary.items():
        if not v == ():
            input_tuples += v
    output = PageRank_Function(task_file_name,total_num_nodes,input_tuples)
    return output
"""


  

