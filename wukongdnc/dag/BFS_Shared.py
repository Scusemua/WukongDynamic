import logging
import numpy as np
#import os
from multiprocessing import shared_memory

from .DAG_executor_constants import use_page_rank_group_partitions, using_threads_not_processes

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

#rhc: shm moved globals up
shm_pagerank = None
shm_previous = None
shm_number_of_children = None
shm_number_of_parents = None
shm_starting_indices_of_parents = None
shm_parents = None
shm_IDs = None

nonshared_pagerank = None
nonshared_previous = None
nonshared_number_of_children = None
nonshared_number_of_parents = None
nonshared_starting_indices_of_parents = None
nonshared_parents = None
nonshared_IDs = None

pagerank_sent_to_processes = None
previous_sent_to_processes = None
number_of_children_sent_to_processes = None
number_of_parents_sent_to_processes = None
starting_indices_of_parents_sent_to_processes = None
parents_sent_to_processes = None
IDs_sent_to_processes = None

#Note: For Linux, this looks like a better way to do the shared memory.
# Linux uses fork and windows uses spawn. For fork, the child inherits 
# a copy of the parent's address spade, which this code seems to rely on
# (see comment on lines 11-12).
# https://gist.github.com/rossant/7a46c18601a2577ac527f958dd4e452f

def initialize(): 
    global shared_partition
    global shared_groups
    global shared_partition_map
    global shared_groups_map
    global shared_partition_frontier_parents_map
    global shared_groups_frontier_parents_map

    # an aray of all the partitions, used for worker threads or when threads
    # are used to simulate lambdas. All the threads can access this local
    # shared array of partitions or groups
    shared_partition = []
    shared_groups = []
    # maps partition "P" to its position/size in shared_partition/shared_groups
    shared_partition_map = {}
    # maps group "G" to its position/size in shared_partition/shared_groups
    shared_groups_map = {}
    # maps a partition "P" to its list of frontier tuples
    shared_partition_frontier_parents_map = {}
    # maps a group "G" to its list of frontier tuples
    shared_groups_frontier_parents_map = {}

def initialize_struct_of_arrays(num_nodes, np_arrays_size_for_shared_partition,
        np_arrays_size_for_shared_partition_parents):

    global pagerank
    global previous
    global number_of_children
    global number_of_parents
    global starting_indices_of_parents
    global parents
    global IDs

#rhc: ToDo: we can use empty instead of full but full is easier to debug for now.
#rhc: Also, empty can init array elelent with "nan" and if this element is 
#     part of padding, when we print it we will see "nan"

    #pagerank = np.empty(np_arrays_size_for_shared_partition,dtype=np.double)
    pagerank = np.full(np_arrays_size_for_shared_partition,float(0.0)) # dtype=np.double)
    # prev[i] is previous pagerank value of i
    previous = np.full(np_arrays_size_for_shared_partition,float((1/num_nodes)))
    # num_chldren[i] is number of child nodes of node i
    # rhc: Q: make these short or something shorter than int?
    #number_of_children = np.empty(np_arrays_size_for_shared_partition,dtype=np.intc)
    number_of_children = np.full(np_arrays_size_for_shared_partition, -3,dtype=np.intc)
    # numParents[i] is number of parent nodes of node i
    number_of_parents = np.full(np_arrays_size_for_shared_partition, -3,dtype=np.intc)
    # parent_index[i] is the index in parents[] of the first of 
    # num_parents parents of node i
    starting_indices_of_parents = np.full(np_arrays_size_for_shared_partition, -3,dtype=np.intc)
    # node IDs
    IDs = np.full(np_arrays_size_for_shared_partition, -3, dtype=np.intc)
    # parents - to get the parents of node i: num_parents = numParents[i];
    # parent_index = parent_index[i]; 
    # for j in (parent_index,num_parents) parent = parents[j]
    parents = np.full(np_arrays_size_for_shared_partition_parents, -3, dtype=np.intc)

def initialize_struct_of_arrays_shared_memory(num_nodes, np_arrays_size_for_shared_partition,
        np_arrays_size_for_shared_partition_parents):

#rhc: ToDo: we can use empty instead of full but full is easier to debug for now.
#rhc: ToDo: we can use empty instead of full but full is easier to debug for now.
#rhc: Also, empty can init array elelent with "nan" and if this element is 
#     part of padding, when we print it we will see "nan"

    global nonshared_pagerank
    global nonshared_previous
    global nonshared_number_of_children
    global nonshared_number_of_parents
    global nonshared_starting_indices_of_parents
    global nonshared_parents
    global nonshared_IDs

    nonshared_pagerank = np.full(np_arrays_size_for_shared_partition,float(0.0)) # dtype=np.double)
    # prev[i] is previous pagerank value of i
    nonshared_previous = np.full(np_arrays_size_for_shared_partition,float((1/num_nodes)))
    # num_chldren[i] is number of child nodes of node i
    # rhc: Q: make these short or something shorter than int?
    #number_of_children = np.empty(np_arrays_size_for_shared_partition,dtype=np.intc)
    nonshared_number_of_children = np.full(np_arrays_size_for_shared_partition, -3,dtype=np.intc)
    # numParents[i] is number of parent nodes of node i
    nonshared_number_of_parents = np.full(np_arrays_size_for_shared_partition, -3,dtype=np.intc)
    # parent_index[i] is the index in parents[] of the first of 
    # num_parents parents of node i
    nonshared_starting_indices_of_parents = np.full(np_arrays_size_for_shared_partition, -3,dtype=np.intc)
    # node IDs
    nonshared_IDs = np.full(np_arrays_size_for_shared_partition, -3, dtype=np.intc)
    # parents - to get the parents of node i: num_parents = numParents[i];
    # parent_index = parent_index[i]; 
    # for j in (parent_index,num_parents) parent = parents[j]
    nonshared_parents = np.full(np_arrays_size_for_shared_partition_parents, -3, dtype=np.intc)

def generate_struct_of_arrays_shared_memory():

    """
    Scheme for defining SharedMemory is based on:
        https://stackoverflow.com/questions/14124588/shared-memory-in-multiprocessing
    # one dimension of the 2d array which is shared
    dim = 5000
    import numpy as np
    from multiprocessing import shared_memory, Process, Lock
    from multiprocessing import cpu_count, current_process
    import time
    lock = Lock()
    def add_one(shr_name):
        existing_shm = shared_memory.SharedMemory(name=shr_name)
        np_array = np.ndarray((dim, dim,), dtype=np.int64, buffer=existing_shm.buf)
        lock.acquire()
        np_array[:] = np_array[0] + 1
        lock.release()
        time.sleep(10) # pause, to see the memory usage in top
        print('added one')
        existing_shm.close()
    def create_shared_block():
        a = np.ones(shape=(dim, dim), dtype=np.int64)  # Start with an existing NumPy array
        shm = shared_memory.SharedMemory(create=True, size=a.nbytes)
        # # Now create a NumPy array backed by shared memory
        np_array = np.ndarray(a.shape, dtype=np.int64, buffer=shm.buf)
        np_array[:] = a[:]  # Copy the original data into shared memory
        return shm, np_array
    if current_process().name == "MainProcess":
        print("creating shared block")
        shr, np_array = create_shared_block()
        processes = []
        for i in range(cpu_count()):
            _process = Process(target=add_one, args=(shr.name,))
            processes.append(_process)
            _process.start()
        for _process in processes:
            _process.join()
        print("Final array")
        print(np_array[:10])
        print(np_array[10:])
        shr.close()
        shr.unlink()
    
    """

    global shm_pagerank
    global shm_previous
    global shm_number_of_children
    global shm_number_of_parents
    global shm_starting_indices_of_parents
    global shm_parents
    global shm_IDs

    global pagerank
    global previous
    global number_of_children
    global number_of_parents
    global starting_indices_of_parents
    global parents
    global IDs

    global pagerank_sent_to_processes
    global previous_sent_to_processes
    global number_of_children_sent_to_processes
    global number_of_parents_sent_to_processes
    global starting_indices_of_parents_sent_to_processes
    global parents_sent_to_processes
    global IDs_sent_to_processes
    
    shm_pagerank = shared_memory.SharedMemory(create=True, size=nonshared_pagerank.nbytes)
    shm_previous = shared_memory.SharedMemory(create=True, size=nonshared_previous.nbytes)
    shm_number_of_children = shared_memory.SharedMemory(create=True, size=nonshared_number_of_children.nbytes)
    shm_number_of_parents = shared_memory.SharedMemory(create=True, size=nonshared_number_of_parents.nbytes)
    shm_starting_indices_of_parents = shared_memory.SharedMemory(create=True, size=nonshared_starting_indices_of_parents.nbytes)
    shm_IDs = shared_memory.SharedMemory(create=True, size=nonshared_IDs.nbytes)
    shm_parents = shared_memory.SharedMemory(create=True, size=nonshared_parents.nbytes)

    pagerank_sent_to_processes = np.ndarray(nonshared_pagerank.shape, dtype=np.double, buffer=shm_pagerank.buf)
    previous_sent_to_processes = np.ndarray(nonshared_previous.shape, dtype=np.double, buffer=shm_previous.buf)
    number_of_children_sent_to_processes = np.ndarray(nonshared_number_of_children.shape, dtype=np.intc, buffer=shm_number_of_children.buf)
    number_of_parents_sent_to_processes = np.ndarray(nonshared_number_of_parents.shape, dtype=np.intc, buffer=shm_number_of_parents.buf)
    starting_indices_of_parents_sent_to_processes = np.ndarray(nonshared_starting_indices_of_parents.shape, dtype=np.intc, buffer=shm_starting_indices_of_parents.buf)
    IDs_sent_to_processes = np.ndarray(nonshared_IDs.shape, dtype=np.intc, buffer=shm_IDs.buf)
    parents_sent_to_processes = np.ndarray(nonshared_parents.shape, dtype=np.intc, buffer=shm_parents.buf)

    pagerank_sent_to_processes[:] = nonshared_pagerank[:]
    previous_sent_to_processes[:] = nonshared_previous[:]
    number_of_children_sent_to_processes[:] = nonshared_number_of_children[:]
    number_of_parents_sent_to_processes[:] = nonshared_number_of_parents[:]
    starting_indices_of_parents_sent_to_processes[:] = nonshared_starting_indices_of_parents[:]
    IDs_sent_to_processes[:] = nonshared_IDs[:]
    parents_sent_to_processes[:] = nonshared_parents[:]

    print(pagerank_sent_to_processes[:10])

    pagerank = None
    previous = None
    number_of_children = None
    number_of_parents = None
    starting_indices_of_parents = None
    IDs = None
    parents = None
    #where:in DAG_executor_driver:
    #_process = Process(target=Foo, args=(shm_pagerank.name,shm_previous.name, ...etc))

def close_shared_memory():
    global shm_pagerank
    global shm_previous
    global shm_number_of_children
    global shm_number_of_parents
    global shm_starting_indices_of_parents
    global shm_parents
    global shm_IDs
# Closes access to the shared memory from this instance. In order to ensure 
# proper cleanup of resources, all instances should call close() once the 
# instance is no longer needed. Note that calling close() 
# does not cause the shared memory block itself to be destroyed.
# https://docs.python.org/3/library/multiprocessing.shared_memory.html
    shm_pagerank.close()
    shm_previous.close()
    shm_number_of_children.close()
    shm_number_of_parents.close()
    shm_starting_indices_of_parents.close()
    shm_parents.close()
    shm_IDs.close()

def unlink_shared_memory():
    global shm_pagerank
    global shm_previous
    global shm_number_of_children
    global shm_number_of_parents
    global shm_starting_indices_of_parents
    global shm_parents
    global shm_IDs
# Requests that the underlying shared memory block be destroyed. In order to 
# ensure proper cleanup of resources, unlink() should be called once (and 
# only once) across all processes which have need for the shared memory block. 
# After requesting its destruction, a shared memory block may or may not be 
# immediately destroyed and this behavior may differ across platforms. Attempts 
# to access data inside the shared memory block after unlink() has been called 
# may result in memory access errors. Note: the last process relinquishing i
# ts hold on a shared memory block may call unlink() and close() in either order.
# https://docs.python.org/3/library/multiprocessing.shared_memory.html
    shm_pagerank.unlink()
    shm_previous.unlink()
    shm_number_of_children.unlink()
    shm_number_of_parents.unlink()
    shm_starting_indices_of_parents.unlink()
    shm_parents.unlink()
    shm_IDs.unlink()

# as in:
"""
    np.set_printoptions(threshold=np.inf)
    # https://numpy.org/doc/stable/reference/generated/numpy.set_printoptions.html

    processes = []
    for i in range(cpu_count()):
        _process = Process(target=Foo, args=(shm_pagerank.name,shm_previous.name, ...etc))
        processes.append(_process)
        _process.start()

    for _process in processes:
        _process.join()

    print(pagerank)

    shm_pagerank.close()
    shm_previous.unlink()
    ...
"""

debug_pagerank = True

def PageRank_Function_Driver_Shared_Fast(task_file_name,total_num_nodes,results_dictionary,shared_map,shared_nodes):

    input_tuples = []
    if (debug_pagerank):
        logger.debug("")
    for (_,v) in results_dictionary.items():
        if (debug_pagerank):
            logger.debug("PageRank_Function_Driver_Shared:" + str(v))
        # pagerank leaf tasks have no input. This results in a result_dictionary
        # in DAG_executor of "DAG_executor_driver_0" --> (), where
        # DAG_executor_driver_0 is used to mean that the DAG_excutor_driver
        # provided an empty input tuple for the leaf task. Here, we just ignore
        debug_pagerank# empty input tuples so that the input_tuples provided to the 
        # PageRank_Function will be an empty list.
        if not v == ():
            # v is a list of tuples so ths is concatenating two lists of tuples 
            # to get a list.
            input_tuples += v

    # This sort is not necessary. Sorting ensures that shadow nodes are processed
    # in ascending order of their IDs, i.e., 2 before 17, so that the parents of
    # the shadow nodes are placed in the partition in the order of their associated
    # shadow nodes. Helps visualize thing during debugging.
    if (debug_pagerank):
        input_tuples.sort()

    if (debug_pagerank):
        logger.debug("PageRank_Function_Driver_Shared: input_tuples: " + str(input_tuples))

    output = PageRank_Function_Shared_Fast(task_file_name,total_num_nodes,input_tuples,shared_map,shared_nodes)
    return output

"""
def PageRank_Function_Shared_FastX(shared_nodes, position_size_tuple ,damping_factor,
    one_minus_dumping_factor,random_jumping,total_num_nodes):

    
    #global debug_pagerank
    #if (debug_pagerank):
    #    logger.debug("update_pagerank: node " + my_ID)
    #    logger.debug("update_pagerank: parent_nodes: " + str(parent_nodes))
    #    logger.debug("update_pagerank: num_children: " + str(self.num_children))
    

    starting_position_in_partition_group = position_size_tuple[0]
    # FYI:
    #size_of_partition_group = position_size_tuple[1]
    num_nodes_for_pagerank_computation = 1 # size_of_partition_group - num_shadow_nodes

    for node_index in range (starting_position_in_partition_group,starting_position_in_partition_group+num_nodes_for_pagerank_computation):

        global starting_indices_of_parents 
        # starting index of parents in the parents array
        starting_index_of_parent = starting_indices_of_parents[node_index]

        #parent_nodes = self.parents
        global number_of_parents
        num_parents = number_of_parents[node_index]
    
    #Note: a parent has at least one child so num_children is not 0
    #pagerank_sum = sum((shared_nodes[node_index+starting_position_in_partition_group].prev / shared_nodes[node_index+starting_position_in_partition_group].num_children) for node_index in parent_nodes)
    pagerank_sum = sum((previous[parent_index] / number_of_children[parent_index]) for parent_index in parents[starting_index_of_parent:num_parents])
    if (debug_pagerank):
        logger.debug("update_pagerank: pagerank_sum: " + str(pagerank_sum))
    #random_jumping = damping_factor / total_num_nodes
    if (debug_pagerank):
        logger.debug("damping_factor:" + str(damping_factor) + " 1-damping_factor:" + str(1-damping_factor) + " num_nodes: " + str(total_num_nodes) + " random_jumping: " + str(random_jumping))
    #self.pagerank = random_jumping + ((1-damping_factor) * pagerank_sum)
    pagerank[node_index] = random_jumping + (one_minus_dumping_factor * pagerank_sum)
    if (debug_pagerank):
        logger.debug ("update_pagerank: pagerank of node: " + str(node_index) + ": " + str(pagerank[node_index]))
        logger.debug("")
"""

def PageRank_Function_Shared_Fast(task_file_name,total_num_nodes,input_tuples,shared_map,shared_nodes):


        debug_pagerank = True
        # rhc shared
        # We do not need the input tuples that supply the shadow node values since
        # we set the shadow nodes and their parents for the output partitions
        # at the end.
       
        #rhc shared
        ## task_file_name is, e.g., "PR1_1" not "PR1_1.pickle"
        ## We check for task_file_name ending with "L" for loop below,
        ## so we make this check esy by having 'L' at the end (endswith)
        ## instead of having to parse ("PR1_1.pickle")
        #complete_task_file_name = './'+task_file_name+'.pickle'
        #with open(complete_task_file_name, 'rb') as handle:
        #    partition_or_group = (cloudpickle.load(handle))
        #partition_or_group = shared_nodes
        position_size_triple = shared_map[task_file_name]
        starting_position_in_partition_group = position_size_triple[0]
        size_of_partition_group = position_size_triple[1]

        #rhc shared
        #num_shadow_nodes = len(input_tuples)
        num_shadow_nodes = position_size_triple[2]
        if debug_pagerank:
            logger.debug("starting_position_in_partition_group: " + str(starting_position_in_partition_group)
                +", size_of_partition_group: " + str(size_of_partition_group)
                +", num_shadow_nodes: " + str(num_shadow_nodes))



#rhc: ToDo: output the arrays
        """
        if (debug_pagerank):
            logger.debug("PageRank_Function output partition_or_group (node:parents):")

            for node_index in range (starting_position_in_partition_group,starting_position_in_partition_group+size_of_partition_group):
            #for node in partition_or_group:
                #rhc shared
                node = shared_nodes[node_index]
                #logger.debug(node,end=":")
                # str(node) will print the node ID with an "-s" appended if 
                # the node is a shadow node. Parent nodes of shadow nodes
                # have an ID of -2, which uis changed below.
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
            #rhc shared
            #logger.debug("PageRank_Function output partition_or_group (node:num_children):")
            logger.debug("PageRank_Function output shared nodes (node:num_children):")
            print_val = ""
            #rhc shared
            for node_index in range (starting_position_in_partition_group,starting_position_in_partition_group+size_of_partition_group):
            #for node in partition_or_group:
                node = shared_nodes[node_index]
                print_val += str(node)+":"+str(node.num_children) + ", "
                # logger.debug(str(node)+":"+str(node.num_children),end=", ")
            logger.debug(print_val)
            logger.debug("")

            logger.debug("")
            # node's children set when the partition/grup node created
        """

        damping_factor=0.15
        random_jumping = damping_factor / total_num_nodes
        one_minus_dumping_factor = 1 - damping_factor

#rhc: ToDo: Not used
        """
        iteration = -1
        if not task_file_name.endswith('L'):
            iteration = int(1)
        else:
            iteration = int(10)
        """
        
        #rhc shared
        # When using shared partition/groups, the partition/group has regular
        # partition_nodes, shadow_nodes, and for each shadow node, its parent,
        # where all parents are at the end. If there are n shadow_nodes there
        # are n parents. If the size of the partition/group is size_of_partition_group,
        # which is computed above, then we subtract the number of parent nodes, which 
        # is the same as the nmber of shadow nodes. If this value is m, then m is also
        # the starting position of the parent nodes.
        #num_nodes_for_pagerank_computation = size_of_partition_group
        #num_nodes_for_pagerank_computation = len(partition_or_group)
#rhc: ToDo: Use these?
        num_nodes_for_pagerank_computation = size_of_partition_group - num_shadow_nodes
        #starting_position_of_parents_of_shadow_nodes = num_nodes_for_pagerank_computation

#rhc: ToDo: There are no input tuples an this code dos not work for fast arrays

        """
        #rhc shared
        # used i as increment past the end of the partition/group for the next parent
        # to be appended. Now the parent is already in the partition/group so we use
        # j to track the next parent ndex in the partition/group.
        #i = 0
        j = starting_position_of_parents_of_shadow_nodes
        for tup in input_tuples:
            logger.debug("PageRank_Function: input tuple:" + str(tup))
            shadow_node_index = tup[0]
            pagerank_value = tup[1]
            # assert
            #rhc shared
            position_of_shadow_node = starting_position_in_partition_group + shadow_node_index
            #if not partition_or_group[shadow_node_index].isShadowNode:
            if not shared_nodes[position_of_shadow_node].isShadowNode:
                logger.error("[Error]: Internal Error: input tuple " + str(tup)
                    + " position " + str(position_of_shadow_node) + " is not a shadow node.")
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
            
            #rhc_shared 
        
        
        Changes:
        1. new PageRank Shared functions. These are used for executing tasks
            when using shared partitions/groups. This includes the update
            functions in Partition_Node. When we generate DAGs, we use these
            new functions as the task in the DAG. 
        2. At the end of BFS, if we are using shared, we copy the Partition_Nodes
            to shared partitions and groups. At this point we also append the parents
            of the shadow nodes at the end. Note: Eventually we can just add
            the Partition_Nodes directly to shared instead of adding them to the 
            individual partitions/groups and then copying the Partition_Nodes in the 
            collected partitions/groups to shared.
        3. We keep for each partition/group the numner of shadow nodes so we can append
            an equal number of parents. This means when we collect a new partition/group
            we collect in addition to the partition/group name the number of shadow nodes
            for the partition/group. so we have three parallel lists or names, 
            partitions/groups, and num shadow nodes. For collecting num shadow nodes
            we have start and end values for the global num_shadow_nodes so we can compute
            end - start when we collect a new partition/group and save the value in the
            list of num shadow nodes.
        4. When DAG_executor execute a task it calls the non-shared or shared version
            of the pagerank task. There is also an option to use the partitions or
            the groups. This has to be tied into the generate DAG code which will
            generate a DAG_info file that constains a DAG of partitions or a DAG 
            of groups.
        5. When printing stats at end, if we are using shared then we add shadow
            nodes and parents to the partitions/groups so we have to subtract 
            (2*num_shadow_nodes_added)from the total number of nodes in the 
            partitions/groups (and subtract the loop nodes added) to check that the 
            number of nodes in the grapk is equal to the number of nodes in the 
            paritions/groups.
        ToDO:
        Test
        test all w/ partitions instad of groups
        cut in partions vs groups option
        partial DAG generation
        run with real lambdas


# rhc: ToDo: Try with empty input tuples and output. this loop will not run since
# no input tuples. Need to turn off output loop so PageRank_output = {} is empty.
            #rhc shared
            if not shared_nodes[position_of_shadow_node].pagerank == pagerank_value:
                logger.error("[Error]: Internal Error: " 
                    + task_file_name + " Copied value is not equal to input value,"
                    + " shared_nodes[position_of_shadow_node].pagerank: " 
                    + str(shared_nodes[position_of_shadow_node].pagerank)
                    + " pagerank_value: " + str(pagerank_value))
            else:
                logger.error(task_file_name + ": Fooooooooooooooooooooo")
            #shared_nodes[position_of_shadow_node].pagerank = pagerank_value
            if not task_file_name.endswith('L'):
                shared_nodes[position_of_shadow_node].pagerank = pagerank_value
            else:
                shared_nodes[position_of_shadow_node].prev = pagerank_value
            #partition_or_group[shadow_node_index].pagerank = pagerank_value
            # IDs: -1, -2, -3, etc
            #rhc shared
            shadow_node_ID = shared_nodes[position_of_shadow_node].ID
            #shadow_node_ID = partition_or_group[shadow_node_index].ID
            # The shadow node ID is an integer, e.g. 1, which does not have
            # a "-s" at the end. The "-s" is appwnsws by the __str__ of the partition
            # node. The parent node ID of a shadow node with ID n is "n-s-p". This 
            # is the actual node ID, unlike shadow nodes which have an int ID and when 
            # the ID is printed by Partition_Node's __str__ function "-s" is appended.
            # Note: Partition_Nodes do not have a member like isShadowNode thatindicates
            # that the ndoe is the parent of a shadow node. So we just use "n-s-p" as the node ID.
            parent_of_shadow_node_ID = str(shadow_node_ID) + "-s-p"

            #rhc shared
            # The parent nodes are already in the partition/groups, we grab
            # these parent nodes one-by-one using index j
            parent_of_shadow_node = shared_nodes[j+starting_position_in_partition_group]
            #rhc shared
            parent_of_shadow_node.ID = parent_of_shadow_node_ID
            #parent_of_shadow_node = Partition_Node(parent_of_shadow_node_ID)

            pagerank_value_of_parent_node = ((shared_nodes[position_of_shadow_node].pagerank - random_jumping)  / one_minus_dumping_factor)
            #rhc shared
            if not parent_of_shadow_node.pagerank == pagerank_value_of_parent_node:
                logger.error("[Error]: Internal Error: " 
                    + task_file_name + " pagerank value to be set for parent of shadow node: "
                    + str(pagerank_value_of_parent_node)
                    + " is not the current pagerank value of the parent node: "
                    + str(parent_of_shadow_node.pagerank))
            else:
                logger.error(task_file_name + ": Foxoxoxoxoxoxoxoxoxoxoxox")

            # set the pagerank of the parent_of_shadow_node so that when we recompute
            # the pagerank of the shadow_node we alwas get the same value.
            #parent_of_shadow_node.pagerank = (
            #    #rhc shared
            #   (pagerank_value_of_parent_node)
            #)
            ##    #(partition_or_group[shadow_node_index].pagerank - random_jumping)  / one_minus_dumping_factor)
            #if (debug_pagerank):
            #    logger.debug(parent_of_shadow_node_ID + " pagerank set to: " + str(parent_of_shadow_node.pagerank))

           if not task_file_name.endswith('L'):
                parent_of_shadow_node.pagerank = (
                    (pagerank_value_of_parent_node)
                # if (debug_pagerank):
                logger.debug("parent " + parent_of_shadow_node_ID + " pagerank set to: " + str(parent_of_shadow_node.pagerank))
            else:
                parent_of_shadow_node.prev = (
                    (pagerank_value_of_parent_node)
                # if (debug_pagerank):
                logger.debug("parent " + parent_of_shadow_node_ID + " prev set to: " + str(parent_of_shadow_node.prev))
 
            # num_children = 1 makes the computation easier; the computation assumed
            # num_children was set to 1
            parent_of_shadow_node.num_children = 1
            #rhc shared
            # parent node is already in partition/group so no need to append
            # appending new nodes at the end
            #partition_or_group.append(parent_of_shadow_node)

            #rhc shared
            # the parent node is in the partition/group at position j
            #shared_nodes[position_of_shadow_node].parents[0] = num_nodes_for_pagerank_computation + i
            shared_nodes[position_of_shadow_node].parents[0] = j
            #partition_or_group[shadow_node_index].parents[0] = num_nodes_for_pagerank_computation + i
            # rhc shared
            #i += 1
            j += 1
        """

#rhc: ToDo: Not using this
        """
        if (debug_pagerank):
            logger.debug("")
            logger.debug("PageRank_Function output partition_or_group after adding " + len(num_shadow_nodes) + " shadow node parents (node:parents):")
            #rhc shared
            for node_index in range (starting_position_in_partition_group,starting_position_in_partition_group+size_of_partition_group):
            #for node in partition_or_group:
                #rhc shared
                node = shared_nodes[node_index]
                # str(node) for shadow nodes will append "-s" to the int ID. For parents
                # node of hadow nodes, __str__ will not append any value since the actual 
                # node ID of a parent node is "n-s-p" so "n-s-p" will be printed by __str__.
                # For shadow nodes, ID is an int, e.g., n and for parent modes, ID is "n-s-p",
                # so for shadow nodes yo get "n-s: n" and for parent nodes "n-s-p: n-s-p".
                print_val = str(node) + ": "
                print_val += str(node.ID) + ", pr:" + str(node.pagerank) + ", prev:" + str(node.prev) + "par ["
                # print(node,end=":")
                for parent in node.parents:
                    #print(parent,end=" ")
                    print_val += str(parent) + " "
                print_val += "] "
                if len(node.parents) == 0:
                    #print(" ,",end=" ")
                    print_val += " ,"
                else:
                    #print(",",end=" ")
                    print_val += ","
                logger.debug(print_val)
            logger.debug("")
        """

#rhc: ToDo: Already did this when initialized arrays
        """
        if task_file_name.endswith('L'):
            # init prev for loops
            #rhc shared
            for node_index in range (starting_position_in_partition_group,starting_position_in_partition_group+num_nodes_for_pagerank_computation):
            #for index in range(num_nodes_for_pagerank_computation):
                #rhc shared
                if not shared_nodes[node_index].isShadowNode:
                    shared_nodes[node_index].prev = (1/total_num_nodes)
                #partition_or_group[index].prev = (1/total_num_nodes)
        """

        if not task_file_name.endswith('L'):
            update_PageRank_of_PageRank_Function_Shared_Fast(task_file_name,
                position_size_triple, num_nodes_for_pagerank_computation,
                damping_factor,one_minus_dumping_factor,random_jumping,total_num_nodes)
        else:
            update_PageRank_of_PageRank_Function_loop_Shared_Fast(task_file_name,
                position_size_triple, num_nodes_for_pagerank_computation,
                damping_factor,one_minus_dumping_factor,random_jumping,total_num_nodes)

#rhc: ToDo: Not using this
        """
        if (debug_pagerank):
            logger.debug("")
            logger.debug("Frontier Parents:")
            #rhc shared
            for node_index in range (starting_position_in_partition_group,starting_position_in_partition_group+size_of_partition_group):
            #for i in range(len(partition_or_group)):
                #rhc shared
                #if not partition_or_group[i].isShadowNode:
                #    my_ID = str(partition_or_group[i].ID)
                #else:
                #   my_ID = str(partition_or_group[i].ID) + "-s"
                #logger.debug("ID:" + my_ID + " frontier_parents: " + str(partition_or_group[i].frontier_parents))
                if not shared_nodes[node_index].isShadowNode:
                    # for parent nodes, ID is e.g., "n-s-p", for non-parent "n" and
                    # for shadow nodes the else part gives "n-s"
                    my_ID = str(shared_nodes[node_index].ID)
                else:
                    my_ID = str(shared_nodes[node_index].ID) + "-s"
                logger.debug("ID:" + my_ID + " frontier_parents: " + str(shared_nodes[node_index].frontier_parents))
            logger.debug("")
        """
        """
        ID:5 frontier_parents: [(2, 1, 2,"PR2_1")]
        ID:17 frontier_parents: [(2, 2, 5,"PR2_1")]
        ID:1 frontier_parents: [(2, 3, 3,"PR2_1")]
        """
        """
        New: Instead of node with ID n having a frontier_parent tuple,
        the tuple is retrieved from the shared_frontier_map which has a list
        of tuples for this task. This tuple has the ID value as the last field. 
        So we will copy the pagerank value in position 5 of this task's 
        partition/group to partition 2 group 1 ("PR2_1") position 2. Note that
        "PR2_1" may be a partition or a group. If we are using partitions,
        they are named "PR1_1", "PR2_1" ... "PRN_1".
        frontier_parent: [(2, 1, 2, "PR2_1", 5)]
        frontier_parent: [(2, 2, 5, "PR2_2", 17)]
        frontier_parent: [(2, 3, 3, "PR2_3", 1)]
        """
#rhc: Note: Instead of looping, we could give each partition/group 
# output tuples that indited where the nodes with non-empty
# frontoer parents are. These loops may be ong for large 
# partitions/groups. For shared we can keep a global map like 
# shared_map or just add a tuple to shared_map?
        PageRank_output = {}

#rhc: ToDo: Not using this
        """
        #rhc shared
        # Note: this shows frontiers of all the nodes including shadow nodes
        # and parent nodes for debugging, where the frontier tuples of shadow
        # nodes and parent nodes is always empty. There is a frontier tuples for
        # each output of the task. If the task has a pagerank value in position p that 
        # needs to be sent to anoher partition/group then the tuple will indicate the
        # name of the destination partition/group and the position in this (sending) tasks'
        # partition/group of the pagerank value to be sent.  Example: 
        # ID:5 frontier_parents: [(2, 1, 2)] meaning send to partition number 2 group
        # 1 with name "PR2_1" a pagerank value that is assigned to position 2 of 
        # the receiving task (where there is a shadow node).
        for node_index in range (starting_position_in_partition_group,starting_position_in_partition_group+size_of_partition_group):
        #for i in range(len(partition_or_group)):
            #rhc shared
            if len(shared_nodes[node_index].frontier_parents) > 0:
            #if len(partition_or_group[i].frontier_parents) > 0:
                #rhc shared
                for frontier_parent_tuple in shared_nodes[node_index].frontier_parents:
                #for frontier_parent in partition_or_group[i].frontier_parents:
                    #partition_number = frontier_parent[0]
                    #group_number = frontier_parent[1]
                    parent_or_group_index = frontier_parent_tuple[2]
                    # Passing name in tuple so that name for loop partition/groups
                    # will have an "l" at the end
                    #partition_or_group_name = "PR"+str(partition_number)+"_"+str(group_number)
                    partition_or_group_name = frontier_parent_tuple[3]
                    output_list = PageRank_output.get(partition_or_group_name)
                    if output_list == None:
                        output_list = []
                    #rhc shared
                    output_tuple = (parent_or_group_index,shared_nodes[node_index].pagerank)
                    #output_tuple = (parent_or_group_index,partition_or_group[i].pagerank)
                    output_list.append(output_tuple)
                    PageRank_output[partition_or_group_name] = output_list
        """

        # NEW:
        if debug_pagerank:
            logger.debug("Copy frontier values:")

        if use_page_rank_group_partitions:
            shared_frontier_map = shared_groups_frontier_parents_map
        else:
            shared_frontier_map = shared_partition_frontier_parents_map
        
        # Get the postition in this task and the position in the receiving task
        # of the pagrank values to be copied from this task to the receiving task
        # (to a shadow node in the receiving task.)
        #list_of_frontier_tuples = shared_frontier_map[task_file_name]
        # if task task_file_name has no frontier tuples then list_of_frontier_tuples
        # will be None.
        list_of_frontier_tuples = shared_frontier_map.get(task_file_name)
        if list_of_frontier_tuples == None:
            list_of_frontier_tuples = []
        for frontier_parent_tuple in list_of_frontier_tuples:
            # Each frontier tuple represents a pagerank value of this task that should
            # be output to a dependent task. partition_or_group_name_of_output_task
            # is the task name of the task that is receiving a pagerank value from 
            # this task. parent_or_group_index_of_output_task is the position in the 
            # dependent task of a shadow node that will be assigned this output value. 
            # That is, this task is "outputting" a pagerank value to task 
            # partition_or_group_name_of_output_task by copying a computed pagerank value 
            # of this task to the position parent_or_group_index_of_output_task of a 
            # shadow node in the receiving task. The position of the pagerank value in 
            # this task to be copied is parent_or_group_index_of_this_task_to_be_output.

            #partition_number = frontier_parent[0]
            #group_number = frontier_parent[1]
            position_or_group_index_of_output_task = frontier_parent_tuple[2]
            partition_or_group_name_of_output_task = frontier_parent_tuple[3]
            # assert: partition_or_group_name_of_output_task == task_file_name

            # Note: We added this field to the frontier tuple so that when
            # we ar using a shared_nodes array or multithreading we can
            # copy vlaues from shared_nodes[i] to shared_nodes[j] instead of 
            # having the tasks input/output these values , as they do when 
            # each task has its won partition and the alues need to be sent
            # and received instead of copied.
            parent_or_group_index_of_this_task_to_be_output = frontier_parent_tuple[4]
            if debug_pagerank:
                logger.debug("frontier_parent: " + str(frontier_parent_tuple))
                logger.debug("starting_position_in_partition_group: " + str(starting_position_in_partition_group))
                logger.debug("parent_or_group_index_in_this_task_to_be_output: " + str(parent_or_group_index_of_this_task_to_be_output))
            # Note: At the top, the starting position of this task in shared_nodes is
            # starting_position_in_partition_group = position_size_tuple[0]

            # This tuple has the starting position and size of the receiving task's
            # partition/group in the shared array, and num shadow nodes, pulled from the shared_map as above.
            if debug_pagerank:
                logger.debug("partition_or_group_name_of_output_task: " + str(partition_or_group_name_of_output_task))
            position_size_triple_of_output_task = shared_map[partition_or_group_name_of_output_task]
            if debug_pagerank:
                logger.debug("position_size_tuple_of_output_task (srt pos, size, num shad nodes): " + str(position_size_triple_of_output_task))
            starting_position_in_partition_group_of_output_task = position_size_triple_of_output_task[0]
            fromPosition = starting_position_in_partition_group+parent_or_group_index_of_this_task_to_be_output
            if debug_pagerank:
                logger.debug("fromPosition: " + str(fromPosition))
            toPosition = starting_position_in_partition_group_of_output_task + position_or_group_index_of_output_task
            if debug_pagerank:
                logger.debug("toPosition (of shadow_node): " + str(toPosition))

            # FYI: position_size_tuple_of_output_task[1] is the size of the partition or group
            if debug_pagerank:
                logger.debug(task_file_name + " copy from position " + str(fromPosition)
                    + " to position " + str(toPosition) + " of the shadow node"
                    + " the value " + str(pagerank[fromPosition]))

            if not partition_or_group_name_of_output_task.endswith('L'):
                pagerank[toPosition] = pagerank[fromPosition]
            else:
                previous[toPosition] = pagerank[fromPosition] 

            """ From above:
            global pagerank
            global previous
            global number_of_children
            global number_of_parents
            global starting_indices_of_parents
            global parents
            global IDs
            """

            if debug_pagerank:
                logger.debug("number_of_parents[toPosition] should be one for shad node): " 
                    + str(number_of_parents[toPosition]))
            starting_index_of_shadow_node_parent_in_parents_array = starting_indices_of_parents[toPosition]
            if debug_pagerank:
                logger.debug("starting_index_of_shadow_node_parent_in_parents_array: " 
                    + str(starting_index_of_shadow_node_parent_in_parents_array))
            node_index_of_parent_of_shadow_node = parents[starting_index_of_shadow_node_parent_in_parents_array]
            if debug_pagerank:
                logger.debug("node_index_of_parent_of_shadow_node: " 
                    + str(node_index_of_parent_of_shadow_node))
            #parent_index_of_shadow_node = parents[node_index_of_parent_of_shadow_node]
            #logger.debug("parent_index_of_shadow_node: " 
            #    + str(parent_index_of_shadow_node))
            ID_of_parent_of_shadow_node = IDs[starting_position_in_partition_group_of_output_task+node_index_of_parent_of_shadow_node]
            if debug_pagerank:
                logger.debug("ID_of_parent_of_shadow_node: " 
                    + str(ID_of_parent_of_shadow_node))
            pagerank_of_shadow_node = pagerank[fromPosition]
            if debug_pagerank:
                logger.debug("pagerank_of_shadow_node: " 
                    + str(pagerank_of_shadow_node))

            if debug_pagerank:
                logger.debug("set shadow node's parent node " + str(ID_of_parent_of_shadow_node) + " pagerank to: " 
                    + str((pagerank_of_shadow_node - random_jumping)  / one_minus_dumping_factor))

            if not partition_or_group_name_of_output_task.endswith('L'):
                pagerank[starting_position_in_partition_group_of_output_task+node_index_of_parent_of_shadow_node] = (
                    (pagerank_of_shadow_node - random_jumping)  / one_minus_dumping_factor)
            else:
                previous[starting_position_in_partition_group_of_output_task+node_index_of_parent_of_shadow_node] = (
                    (pagerank_of_shadow_node - random_jumping)  / one_minus_dumping_factor)

            
            #parent_of_shadow_node.pagerank = (
            #    #rhc shared
            #    (pagerank_of_shadow_node - random_jumping)  / one_minus_dumping_factor)
            #    #(partition_or_group[shadow_node_index].pagerank - random_jumping)  / one_minus_dumping_factor)


            #if debug_pagerank:
            #    index = starting_position_in_partition_group_of_output_task+node_index_of_parent_of_shadow_node
            #    logger.debug(ID_of_parent_of_shadow_node + " pagerank set to: " 
            #        + str(pagerank[index]))

            #output_list = PageRank_output.get(partition_or_group_name_of_output_task)
            #if output_list == None:
            #    output_list = []
            #rhc shared
            #output_tuple = (parent_or_group_index,shared_nodes[node_index].pagerank)
            #output_tuple = (parent_or_group_index,partition_or_group[i].pagerank)
            #output_list.append(output_tuple)

            # There is no output so output_list is an empty list, which means a list
            # having no output tuples, which means task inputs will be empty lists
            # of input tuples which is effecively no inputs.
            output_list = []
            PageRank_output[partition_or_group_name_of_output_task] = output_list


#rhc: ToDo: 
# Not an issue for Python, but for others: memory barriers okay? any synch op will do? 
# blank input/output tuples
# New version of set output above for fast pagerank

        #if (debug_pagerank):
        print("PageRank output tuples for " + task_file_name + ":")
        # print_val = ""
        for k, v in PageRank_output.items():
            #print_val += "(%s, %s) " % (k, v)
            print((k, v),end=" ")
        #logger.debug(print_val)
        print("")
        print("")

        #if (debug_pagerank):
        if not using_threads_not_processes:
            logger.debug("PageRank output tuples for " + task_file_name + ": ")
            print_val = ""
            for k, v in PageRank_output.items():
                print_val += "(%s, %s) " % (k, v)
            logger.debug(print_val)
            logger.debug("")
            logger.debug("")

#rhc: ToDo: print array values
        print("XXPageRank result for " + task_file_name + ":", end=" ")
        #rhc shared
        for node_index in range (starting_position_in_partition_group,starting_position_in_partition_group+num_nodes_for_pagerank_computation):
        #for i in range(num_nodes_for_pagerank_computation):
            #rhc shared
            #if not partition_or_group[i].isShadowNode:
            #if not shared_nodes[node_index].isShadowNode:
            #rhc shared
            #print(str(partition_or_group[i].ID) + ":" + str(partition_or_group[i].pagerank),end=" ")
            print(str(IDs[node_index]) + ":" + str(pagerank[node_index]),end=" ")
        print()
        print()
        print("pagerank:")
        print(str(pagerank))

        if not using_threads_not_processes:
            #rhc shared
            print_val = "XXPageRank result for " + task_file_name + ": " # + "\n"
            for node_index in range (starting_position_in_partition_group,starting_position_in_partition_group+num_nodes_for_pagerank_computation):
            #for i in range(num_nodes_for_pagerank_computation):
                #rhc shared
                #if not partition_or_group[i].isShadowNode:
                #if not shared_nodes[node_index].isShadowNode:
                #rhc shared
                #print(str(partition_or_group[i].ID) + ":" + str(partition_or_group[i].pagerank),end=" ")
                print_val += str(IDs[node_index]) + ":" + str(pagerank[node_index])+" "
                #logger.debug(str(IDs[node_index]) + ":" + str(pagerank[node_index]),end=" ")
            logger.debug(print_val)
            logger.debug("")
            logger.debug("pagerank:")
            logger.debug("\n"+str(pagerank))

#rhc: ToDo: Not using, code will not work
        """
        logger.debug("XXPageRank result for " + task_file_name + ":")
        for i in range(num_nodes_for_pagerank_computation):
            if not partition_or_group[i].isShadowNode:
                print(str(partition_or_group[i].ID) + ":" + str(partition_or_group[i].pagerank))
        logger.debug("")
        logger.debug("")
        """

        #logging.shutdown()
        #os._exit(0)

        return PageRank_output


def update_PageRank_of_PageRank_Function_Shared_Fast(task_file_name,
    position_size_tuple, num_nodes_for_pagerank_computation,
    damping_factor,one_minus_dumping_factor,random_jumping,total_num_nodes):

    starting_position_in_partition_group = position_size_tuple[0]
    iterations = 1

    for i in range(1,iterations+1):
        if (debug_pagerank):
            logger.debug("***** PageRank: iteration " + str(i))
            logger.debug("")
            logger.debug("prev of 19: " + str(previous[46]))

        if debug_pagerank:
            logger.debug("starting_position_in_partition_group: " + str(starting_position_in_partition_group)
                + ", num_nodes_for_pagerank_computation: " + str(num_nodes_for_pagerank_computation))
 
        for node_index in range (starting_position_in_partition_group,starting_position_in_partition_group+num_nodes_for_pagerank_computation):

            # starting index of parents in the parents array
            starting_index_of_parent = starting_indices_of_parents[node_index]

            #parent_nodes = self.parents
            global number_of_parents
            num_parents = number_of_parents[node_index]
            if debug_pagerank:
                logger.debug("node_index: " + str(node_index))
                logger.debug("ID: " + str(IDs[node_index]))
                logger.debug("starting_index_of_parent: " + str(starting_index_of_parent))
                logger.debug("num_parents: " + str(num_parents))
                for parent_indexD in parents[starting_index_of_parent:(starting_index_of_parent+num_parents)]:
                    logger.debug("parent index: " + str(parent_indexD))
                    logger.debug("pagerank[starting_position_in_partition_group+parent_index]: " + str(pagerank[starting_position_in_partition_group+parent_indexD]))
                    logger.debug("number_of_children[starting_position_in_partition_group+parent_index]: " + str(number_of_children[starting_position_in_partition_group+parent_indexD]))

            #Note: a parent has at least one child so num_children is not 0
            #pagerank_sum = sum((shared_nodes[node_index+starting_position_in_partition_group].prev / shared_nodes[node_index+starting_position_in_partition_group].num_children) for node_index in parent_nodes)
            pagerank_sum = sum((pagerank[starting_position_in_partition_group+parent_index] / number_of_children[starting_position_in_partition_group+parent_index]) for parent_index in parents[starting_index_of_parent:(starting_index_of_parent+num_parents)])
            if (debug_pagerank):
                logger.debug("update_pagerank: pagerank_sum: " + str(pagerank_sum))
            #random_jumping = damping_factor / total_num_nodes
            if (debug_pagerank):
                logger.debug("damping_factor:" + str(damping_factor) + " 1-damping_factor:" + str(1-damping_factor) + " num_nodes: " + str(total_num_nodes) + " random_jumping: " + str(random_jumping))
            #self.pagerank = random_jumping + ((1-damping_factor) * pagerank_sum)
            pagerank[node_index] = random_jumping + (one_minus_dumping_factor * pagerank_sum)
            if (debug_pagerank):
                logger.debug ("update_pagerank: pagerank of node: " + str(node_index) + ": " + str(pagerank[node_index]))
                logger.debug("")

        """
        print("XXPageRank result for " + task_file_name + ":", end=" ")
        #rhc shared
        for node_index in range (starting_position_in_partition_group,starting_position_in_partition_group+num_nodes_for_pagerank_computation):
            #rhc shared
            print(str(IDs[node_index]) + ":" + str(pagerank[node_index]),end=" ")
        print()
        print()
        """

def update_PageRank_of_PageRank_Function_loop_Shared_Fast(task_file_name,
    position_size_tuple, num_nodes_for_pagerank_computation,
    damping_factor,one_minus_dumping_factor,random_jumping,total_num_nodes):

    starting_position_in_partition_group = position_size_tuple[0]
    iterations = 10


    #Note: Previous[] is initialized when we create

    for i in range(1,iterations+1):
        if (debug_pagerank):
            logger.debug("***** PageRank: iteration " + str(i))
            logger.debug("")

        for node_index in range (starting_position_in_partition_group,starting_position_in_partition_group+num_nodes_for_pagerank_computation):

            # starting index of parents in the parents array
            starting_index_of_parent = starting_indices_of_parents[node_index]

            #parent_nodes = self.parents
            global number_of_parents
            num_parents = number_of_parents[node_index]
            if debug_pagerank:
                logger.debug("node_index: " + str(node_index))
                logger.debug("ID: " + str(IDs[node_index]))
                logger.debug("starting_index_of_parent: " + str(starting_index_of_parent))
                logger.debug("num_parents: " + str(num_parents))
                for parent_indexD in parents[starting_index_of_parent:(starting_index_of_parent+num_parents)]:
                    logger.debug("parent index: " + str(parent_indexD))
                    logger.debug("IDs[starting_position_in_partition_group+parent_index]: " + str(IDs[starting_position_in_partition_group+parent_indexD]))
                    logger.debug("starting_position_in_partition_group+parent_indexD:" + str(starting_position_in_partition_group+parent_indexD))
                    logger.debug("previous[starting_position_in_partition_group+parent_index]: " + str(previous[starting_position_in_partition_group+parent_indexD]))
                    logger.debug("number_of_children[starting_position_in_partition_group+parent_index]: " + str(number_of_children[starting_position_in_partition_group+parent_indexD]))

            #Note: a parent has at least one child so num_children is not 0
            #pagerank_sum = sum((shared_nodes[node_index+starting_position_in_partition_group].prev / shared_nodes[node_index+starting_position_in_partition_group].num_children) for node_index in parent_nodes)
            pagerank_sum = sum((previous[starting_position_in_partition_group+parent_index] / number_of_children[starting_position_in_partition_group+parent_index]) for parent_index in parents[starting_index_of_parent:(starting_index_of_parent+num_parents)])
            if (debug_pagerank):
                logger.debug("update_pagerank: pagerank_sum: " + str(pagerank_sum))
            #random_jumping = damping_factor / total_num_nodes
            if (debug_pagerank):
                logger.debug("damping_factor:" + str(damping_factor) + " 1-damping_factor:" + str(1-damping_factor) + " num_nodes: " + str(total_num_nodes) + " random_jumping: " + str(random_jumping))
            #self.pagerank = random_jumping + ((1-damping_factor) * pagerank_sum)
            pagerank[node_index] = random_jumping + (one_minus_dumping_factor * pagerank_sum)
            if (debug_pagerank):
                logger.debug ("update_pagerank: pagerank of node: " + str(node_index) + ": " + str(pagerank[node_index]))
                logger.debug("")

        # save current pagerank in prev
        #rhc shared
        for node_index in range (starting_position_in_partition_group,starting_position_in_partition_group+num_nodes_for_pagerank_computation):
            previous[node_index] = pagerank[node_index]

    """
    print("XXPageRank result for " + task_file_name + ":", end=" ")
    #rhc shared
    for node_index in range (starting_position_in_partition_group,starting_position_in_partition_group+num_nodes_for_pagerank_computation):
        #rhc shared
        print(str(IDs[node_index]) + ":" + str(pagerank[node_index]),end=" ")
    print()
    print()
    """


"""
Shared memory in multiprocessing
https://stackoverflow.com/questions/14124588/shared-memory-in-multiprocessing
(and see: https://mingze-gao.com/posts/python-shared-memory-in-multiprocessing/)
(and see: https://docs.python.org/3/library/multiprocessing.shared_memory.html)
# one dimension of the 2d array which is shared
dim = 5000

import numpy as np
from multiprocessing import shared_memory, Process, Lock
from multiprocessing import cpu_count, current_process
import time

lock = Lock()

Note: he general rule is that if there is a write on thread A and read
 on thread B for the same location, A has to execute a release operation 
 as part of its write or as a subsequent memory barrier, and B has to 
 execute an acquire operation as part of its read or as a preceding 
 memory barrier, otherwise there is no guarantee that B will read the 
 value written by A

Note:
Maybe you need to use a memory barrier to force the data to be seen by another cpu?
Maybe use shm lock operation to sync both sides?
Googling I see people talking about using stdatomic.h for this.
But I am far from clear what you would need to do.

def add_one(shr_name):
    existing_shm = shared_memory.SharedMemory(name=shr_name)
    np_array = np.ndarray((dim, dim,), dtype=np.int64, buffer=existing_shm.buf)
    lock.acquire()
    np_array[:] = np_array[0] + 1
    lock.release()
    time.sleep(10) # pause, to see the memory usage in top
    print('added one')
    existing_shm.close()

def create_shared_block():

    a = np.ones(shape=(dim, dim), dtype=np.int64)  # Start with an existing NumPy array

    shm = shared_memory.SharedMemory(create=True, size=a.nbytes)
    # # Now create a NumPy array backed by shared memory
    np_array = np.ndarray(a.shape, dtype=np.int64, buffer=shm.buf)
    np_array[:] = a[:]  # Copy the original data into shared memory
    return shm, np_array

if current_process().name == "MainProcess":
    print("creating shared block")
    shr, np_array = create_shared_block()

    processes = []
    for i in range(cpu_count()):
        _process = Process(target=add_one, args=(shr.name,))
        processes.append(_process)
        _process.start()

    for _process in processes:
        _process.join()

    print("Final array")
    print(np_array[:10])
    print(np_array[10:])

    shr.close()
    shr.unlink()
Note that because of the 64 bit ints this code can take about 1gb of ram to run, so make sure that you won't freeze your system using it. ^_^
"""
"""
# In the first Python interactive shell
import numpy as np
a = np.array([1, 1, 2, 3, 5, 8])  # Start with an existing NumPy array
from multiprocessing import shared_memory
shm = shared_memory.SharedMemory(create=True, size=a.nbytes)
# Now create a NumPy array backed by shared memory
b = np.ndarray(a.shape, dtype=a.dtype, buffer=shm.buf)
b[:] = a[:]  # Copy the original data into shared memory
b
array([1, 1, 2, 3, 5, 8])
type(b)
<class 'numpy.ndarray'>
type(a)
<class 'numpy.ndarray'>
shm.name  # We did not specify a name so one was chosen for us
'psm_21467_46075'
"""