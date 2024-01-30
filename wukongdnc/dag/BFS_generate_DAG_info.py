import logging
import cloudpickle
import os

from .DAG_info import DAG_Info
from .DFS_visit import state_info
from .BFS_pagerank import PageRank_Function_Driver, PageRank_Function_Driver_Shared
from .BFS_Shared import PageRank_Function_Driver_Shared_Fast
from .DAG_executor_constants import use_shared_partitions_groups, use_page_rank_group_partitions
from .DAG_executor_constants import use_struct_of_arrays_for_pagerank
#from .DAG_executor_constants import using_threads_not_processes, use_multithreaded_multiprocessing
from .DAG_executor_constants import enable_runtime_task_clustering
from .DAG_executor_constants import exit_program_on_exception
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


#For DAG generation, map sending task to list of Reveiving tasks, and 
# map receiving task to list of Sending tasks.
Partition_senders = {}
Partition_receivers = {}
Group_senders = {}
Group_receivers = {}

leaf_tasks_of_partitions = set()
# use a list so leaf tasks aer in order they are added (detected)
leaf_tasks_of_partitions_incremental = []
leaf_tasks_of_groups = set()
leaf_tasks_of_groups_incremental = []

#rhc: clustering
groups_num_shadow_nodes_map = {}
partitions_num_shadow_nodes_map = {}

#rhc: num_nodes
# this is set BFS.input_graph when doing non-incremental DAG generation
num_nodes_in_graph = 0

"""
get 1: DAG_info is
state 1 ia PR1_1
state 2 is PR2_1 with to be continued = True
get 2: DAG_info is
state 1 ia PR1_1
state 2 is PR2_2
state 3 is PR2_3 with to be continued = True

==>
get i
Compute state i for PRi_1 and replace current state i with new state i
generate state i+1 with to be continued = True


"""

def generate_DAG_info():
    #Given Partition_senders, Partition_receivers, Group_senders, Group_receievers

#rhc: ToDo: Do we want to use collapse? fanin? If so, one task will input
# its partition/grup and then input the collapse/fanin group, etc. Need
# to clear the old partition/group before doing next?
# If we pre-load the partitions, thn we would want to do fanouts/faninNBs
# so we can use the pre-loaded partition?

#rhc: Problem: Need lists for faninNB and fanin names
    Partition_all_fanout_task_names = []
    Partition_all_fanin_task_names = []
    Partition_all_faninNB_task_names = []
    Partition_all_collapse_task_names = []

    Partition_all_fanin_sizes = []
    Partition_all_faninNB_sizes = []

    Partition_DAG_leaf_tasks = []
    Partition_DAG_leaf_task_start_states = []
    # no inputs for leaf tasks
    Partition_DAG_leaf_task_inputs = []
    Partition_DAG_map = {}
    Partition_DAG_states = {}
    Partition_DAG_tasks = {}
    Partition_DAG_number_of_tasks = 0

#rhc: num_nodes:
    # The value of num_nodes_in_graph is set by BFS_input_graph
    # at the beginning of execution, which is before we start
    # DAG generation. This value does not change.
    Partition_DAG_num_nodes_in_graph = num_nodes_in_graph

    """ Add L to end of names: No longer needed
    print()
    print("Partition_loops:" + str(Partition_loops))
    Partition_senders_Copy = Partition_senders.copy()
    for sender_name,receiver_name_set in Partition_senders_Copy.items():
        print("sender_name:" + sender_name + " receiver_name_set: " + str(receiver_name_set))
        receiver_name_set_new = set()
        for receiver_name in receiver_name_set:
            if receiver_name in Partition_loops:
                receiver_name_with_L_at_end = str(receiver_name) + "L"
                receiver_name_set_new.add(receiver_name_with_L_at_end)
            else:
                receiver_name_set_new.add(receiver_name)
        Partition_senders[sender_name] = receiver_name_set_new
        if sender_name in Partition_loops: 
            sender_name_with_L_at_end = str(sender_name) + "L"
            Partition_senders[sender_name_with_L_at_end] = Partition_senders[sender_name]
            del Partition_senders[sender_name]

    Partition_receivers_Copy = Partition_receivers.copy()
    for receiver_name,sender_name_set in Partition_receivers_Copy.items():
        print("receiver_name:" + receiver_name + " sender_name_set: " + str(sender_name_set))
        sender_name_set_new = set()
        for sender_name in sender_name_set:
            if sender_name in Partition_loops:
                sender_name_with_L_at_end = str(sender_name) + "L"
                sender_name_set_new.add(sender_name_with_L_at_end)
            else:
                sender_name_set_new.add(sender_name)
        Partition_receivers[receiver_name] = sender_name_set_new
        if receiver_name in Partition_loops: 
            receiver_name_with_L_at_end = str(receiver_name) + "L"
            Partition_receivers[receiver_name_with_L_at_end] = Partition_receivers[receiver_name]
            del Partition_receivers[receiver_name]
    """

    """
    print()
    print("Partition_senders:")
    for sender_name,receiver_name_set in Partition_senders.items():
        print("sender:" + sender_name)
        print("receiver_name_set:" + str(receiver_name_set))
    print()
    print()
    print("Partition_receivers:")
    for receiver_name,sender_name_set in Partition_receivers.items():
        print("receiver:" + receiver_name)
        print("sender_name_set:" + str(sender_name_set))
    print()
    print()
    print("Leaf nodes of partitions:")
    for name in leaf_tasks_of_partitions:
        print(name + " ")
    """

    """
    print()
    print("Group_loops:" + str(Group_loops))
    Group_senders_Copy = Group_senders.copy()
    for sender_name,receiver_name_set in Group_senders_Copy.items():
        print("sender_name:" + sender_name + " receiver_name_set: " + str(receiver_name_set))
        receiver_name_set_new = set()
        for receiver_name in receiver_name_set:
            if receiver_name in Group_loops:
                receiver_name_with_L_at_end = str(receiver_name) + "L"
                receiver_name_set_new.add(receiver_name_with_L_at_end)
            else:
                receiver_name_set_new.add(receiver_name)
        Group_senders[sender_name] = receiver_name_set_new
        if sender_name in Group_loops: 
            sender_name_with_L_at_end = str(sender_name) + "L"
            Group_senders[sender_name_with_L_at_end] = Group_senders[sender_name]
            del Group_senders[sender_name]

    Group_receivers_Copy = Group_receivers.copy()
    for receiver_name,sender_name_set in Group_receivers_Copy.items():
        print("receiver_name:" + receiver_name + " sender_name_set: " + str(sender_name_set))
        sender_name_set_new = set()
        for sender_name in sender_name_set:
            if sender_name in Group_loops:
                sender_name_with_L_at_end = str(sender_name) + "L"
                sender_name_set_new.add(sender_name_with_L_at_end)
            else:
                sender_name_set_new.add(sender_name)
        Group_receivers[receiver_name] = sender_name_set_new
        if receiver_name in Group_loops: 
            receiver_name_with_L_at_end = str(receiver_name) + "L"
            Group_receivers[receiver_name_with_L_at_end] = Group_receivers[receiver_name]
            del Group_receivers[receiver_name]
    """
    
    """
    print()
    print("Group_senders:")
    for sender_name,receiver_name_set in Group_senders.items():
        print("sender:" + sender_name)
        print("receiver_name_set:" + str(receiver_name_set))
    print()
    print()
    print("Group_receivers:")
    for receiver_name,sender_name_set in Group_receivers.items():
        print("receiver:" + receiver_name)
        print("sender_name_set:" + str(sender_name_set))
    print()
    print()
    print("Leaf nodes of groups:")
    for name in leaf_tasks_of_groups:
        print(name + " ")
    """

    #Note:
    #Informs the logging system to perform an orderly shutdown by flushing 
    #and closing all handlers. This should be called at application exit and no 
    #further use of the logging system should be made after this call.
    #logging.shutdown()
    #time.sleep(3)   #not needed due to shutdwn
    #os._exit(0)

    """
    for group_name in Group_loops:
        group_name_with_L_at_end += str(group_name) + "L"
        Group_senders[group_name_with_L_at_end] = Group_senders[group_name]
        del Group_senders[group_name]
    """

    # sink nodes, i.e., nodes that do not send any inputs
    Partition_sink_set = set()
    logger.info("Partition DAG:")
    state = 1
    # partition i has a collapse to partition i+1
    # Task senderX sends inputs to one or more other tasks
    for senderX in Partition_senders:
        fanouts = []
#rhc: clustering
        fanout_partition_group_sizes = []
        faninNBs = []
        fanins = []
        collapse = []
        fanin_sizes = []
        faninNB_sizes = []
        # tasks that receive inputs from senderX
        receiver_set_for_senderX = Partition_senders[senderX]
        # task receiverY may receive inputs from other tasks (all tasks receive
        # inputs from other tasks except leaf tasks)
        for receiverY in receiver_set_for_senderX:
            receiver_set_for_receiverY = Partition_senders.get(receiverY)
            if receiver_set_for_receiverY is None:
                # receiverY does not send any inputs so it is a sink
                Partition_sink_set.add(receiverY)
            # tasks that send inputs to receiverY
            sender_set_for_receiverY = Partition_receivers[receiverY]
            length_of_sender_set_for_receiverY = len(sender_set_for_receiverY)
            length_of_receiver_set_for_senderX = len(receiver_set_for_senderX)
            if length_of_sender_set_for_receiverY == 1:
                # collapse or fanout as receiverY receives on input
                if length_of_receiver_set_for_senderX == 1:
                    # only one task sends input to receiverY and this sending 
                    # task only sends to one task, so collapse receiverY, i.e.,
                    # senderX becomes receiverY
                    logger.trace("sender " + senderX + " --> " + receiverY + " : Collapse")
                    if not receiverY in Partition_all_collapse_task_names:
                        Partition_all_collapse_task_names.append(receiverY)
                    else:
                        pass # error only one task can collapse a given task
                    collapse.append(receiverY)
                else:
                    # only one task sends input to receiverY and this sending 
                    # task sends to other tasks too, so senderX does a fanout 
                    # to receiverY         
                    logger.trace("sender " + senderX + " --> " + receiverY + " : Fanout")
                    if not receiverY in Partition_all_fanout_task_names:
                        Partition_all_fanout_task_names.append(receiverY)
                    fanouts.append(receiverY)
#rhc: clustering
                    if enable_runtime_task_clustering:
                        num_shadow_nodes = partitions_num_shadow_nodes_map[receiverY]
                        fanout_partition_group_sizes.append(num_shadow_nodes)
            else:
                # fanin or fannNB since receiverY receives inputs from multiple tasks
                isFaninNB = False
                 # senderZ sends an input to receiverY
                for senderZ in sender_set_for_receiverY:
                    # tasks to which senderX sends an input
                    receiver_set_for_senderZ = Partition_senders[senderZ]
                    # since senderX sends inputs to more than one task, receiverY
                    # is a faninNB task (as senderX cannot become receiverY)
                    if len(receiver_set_for_senderZ) > 1:
                        # if any task sends inputs to reciverY and any other task(s)
                        # then receiverY must be a faninNB task since some sender cannot 
                        # become receiverY.
                        isFaninNB = True
                        break
                if isFaninNB:
                    logger.trace("sender " + senderX + " --> " + receiverY + " : FaninNB")
                    if not receiverY in Partition_all_faninNB_task_names:
                        Partition_all_faninNB_task_names.append(receiverY)
                        Partition_all_faninNB_sizes.append(length_of_sender_set_for_receiverY)
                    faninNBs.append(receiverY)
                    faninNB_sizes.append(length_of_sender_set_for_receiverY)
                else:
                    # senderX sends an input only to receiverY, same for any other
                    # tasks that sends inputs to receiverY so receiverY is a fanin task.
                    logger.trace("sender " + senderX + " --> " + receiverY + " : Fanin")
                    if not receiverY in Partition_all_fanin_task_names:
                        Partition_all_fanin_task_names.append(receiverY)
                        Partition_all_fanin_sizes.append(length_of_sender_set_for_receiverY)
                    fanins.append(receiverY)
                    fanin_sizes.append(length_of_sender_set_for_receiverY)

        # get the tasks that send to senderX, i.e., provide inputs for senderX
        sender_set_for_senderX = Partition_receivers.get(senderX)
        if sender_set_for_senderX is None:
            # senderX is a leaf task since it is not a receiver
            Partition_DAG_leaf_tasks.append(senderX)
            Partition_DAG_leaf_task_start_states.append(state)
            task_inputs = ()
            Partition_DAG_leaf_task_inputs.append(task_inputs)

            try:
                msg = "[Error]: BFS_generate_DAG_info: partition " + senderX + " receives no inputs" \
                    + " but it is not in leaf_tasks_of_partitions."
                assert senderX in leaf_tasks_of_partitions , msg
            except AssertionError:
                logger.exception("[Error]: assertion failed")
                if exit_program_on_exception:
                    logging.shutdown()
                    os._exit(0)
            #assertOld:
            #if not senderX in leaf_tasks_of_partitions:
            #    logger.error("partition " + senderX + " receives no inputs"
            #        + " but it is not in leaf_tasks_of_partitions.")
            #else:
            # we have generated a state for leaf task senderX. 
            leaf_tasks_of_partitions.remove(senderX)

        else:
            # create a new set from sender_set_for_senderX. For 
            # each name in sender_set_for_senderX, qualify name by
            # prexing it with "senderX-". Example: senderX is "PR1_1"
            # and name is "PR2_3" so the qualified name is "PR1_1-PR2_3".
            # We use qualified names since the fanouts/faninNBs for a 
            # task in a pagerank DAG may al have diffent values. This
            # is unlike Dask DAGs in which all fanouts/faninNBs of a task
            # receive the same value. We denote the different outputs
            # of a task A having, e.g., fanouts B and C as "A-B" and "A-C"
            sender_set_for_senderX_with_qualified_names = set()
            for name in sender_set_for_senderX:
                qualified_name = str(name) + "-" + str(senderX)
                sender_set_for_senderX_with_qualified_names.add(qualified_name)
            # sender_set_for_senderX provides input for senderX
            task_inputs = tuple(sender_set_for_senderX_with_qualified_names)
        Partition_DAG_map[state] = state_info(senderX, fanouts, fanins, faninNBs, collapse, fanin_sizes, faninNB_sizes, task_inputs,
#rhc: clustering
            False,  False, fanout_partition_group_sizes)
        Partition_DAG_states[senderX] = state

        state += 1

    if not len(leaf_tasks_of_partitions) == 0:
        # there is a partition that is a leaf task but it is not a sender and not 
        # a receiver so this leaf task is not connected to any other node in the 
        # DAG. Above we see leaf tasks when we iterate thru Senders but since this
        # leaf task is not a Sender and by definition it is not a Receiver, we will
        # not see it above so we take care of it here.
        # Note: leaf tasks are the first partition/group collected by any call to 
        # BFS(). There may be many cals to BFS(). 
        # Note: We could have more than one leaf partition/group that is 
        # disconnected. 
        logger.trace("generate_DAG_info: len(leaf_tasks_of_partitions)>0, add leaf tasks")
        fanouts = []
#rhc: clustering
        fanout_partition_group_sizes = []
        faninNBs = []
        fanins = []
        collapse = []
        fanin_sizes = []
        faninNB_sizes = []

        task_inputs = ()
        for name in leaf_tasks_of_partitions:
            logger.trace("generate_DAG_info: add leaf task for partition " + name)
            Partition_DAG_leaf_tasks.append(name)
            Partition_DAG_leaf_task_start_states.append(state)
            Partition_DAG_leaf_task_inputs.append(task_inputs)

            Partition_DAG_map[state] = state_info(name, fanouts, fanins, faninNBs, collapse, fanin_sizes, faninNB_sizes, task_inputs,
#rhc: clustering
                False,  False, fanout_partition_group_sizes)
            Partition_DAG_states[name] = state
            state += 1

    # Finish by doing the receivers that are not senders (opposite of leaf tasks);
    # these are reeivers that send no inputs to other tasks. They have no fanins/
    # faninBs, fanouts or collapses, but they do have task inputs.
    for receiverY in Partition_sink_set: # Partition_receivers:
        #if not receiverY in Partition_DAG_states:
        fanouts = []
#rhc: clustering
        fanout_partition_group_sizes = []
        faninNBs = []
        fanins = []
        collapse = []
        fanin_sizes = []
        faninNB_sizes = []

        sender_set_for_receiverY = Partition_receivers[receiverY]
        #task_inputs = tuple(sender_set_for_receiverY)

        # create a new set from sender_set_for_senderX. For 
        # each name in sender_set_for_senderX, qualify name by
        # prexing it with "senderX-". Example: senderX is "PR1_1"
        # and name is "PR2_3" so the qualified name is "PR1_1-PR2_3".
        # We use qualified names since the fanouts/faninNBs for a 
        # task in a pagerank DAG may al have diffent values. This
        # is unlike Dask DAGs in which all fanouts/faninNBs of a task
        # receive the same value. We denote the different outputs
        # of a task A having, e.g., fanouts B and C as "A-B" and "A-C"
        sender_set_for_receiverY_with_qualified_names = set()
        # for each task senderX that sends input to receiverY, the 
        # qualified name of the sender is senderX+"-"+senderX
        for senderX in sender_set_for_receiverY:
            qualified_name = str(senderX) + "-" + str(receiverY)
            sender_set_for_receiverY_with_qualified_names.add(qualified_name)
        # sender_set_for_senderX provides input for senderX
        task_inputs = tuple(sender_set_for_receiverY_with_qualified_names)

        Partition_DAG_map[state] = state_info(receiverY, fanouts, fanins, faninNBs, collapse, fanin_sizes, faninNB_sizes, task_inputs,
#rhc: clustering
            False,  False, fanout_partition_group_sizes)
        Partition_DAG_states[receiverY] = state
        state += 1

    # Note. We could decide the Function in DAG_executor when we are
    # about to execuet the task. The pagerank function is the same 
    # for all tasks, so there is no need to package the same function
    # in all the states of the DAG.
    if not use_shared_partitions_groups:
        for key in Partition_DAG_states:
            Partition_DAG_tasks[key] = PageRank_Function_Driver
    else:
        if not use_struct_of_arrays_for_pagerank:
            for key in Partition_DAG_states:
                Partition_DAG_tasks[key] = PageRank_Function_Driver_Shared 
        else:
            for key in Partition_DAG_states:
                Partition_DAG_tasks[key] = PageRank_Function_Driver_Shared_Fast  

    logger.trace("")
    DAG_info = {}
    DAG_info["DAG_map"] = Partition_DAG_map
    DAG_info["DAG_states"] = Partition_DAG_states
    DAG_info["DAG_leaf_tasks"] = Partition_DAG_leaf_tasks
    DAG_info["DAG_leaf_task_start_states"] = Partition_DAG_leaf_task_start_states
    DAG_info["DAG_leaf_task_inputs"] = Partition_DAG_leaf_task_inputs
    DAG_info["all_fanout_task_names"] = Partition_all_fanout_task_names
    DAG_info["all_fanin_task_names"] = Partition_all_fanin_task_names
    DAG_info["all_faninNB_task_names"] = Partition_all_faninNB_task_names
    DAG_info["all_collapse_task_names"] = Partition_all_collapse_task_names
    DAG_info["all_fanin_sizes"] = Partition_all_fanin_sizes
    DAG_info["all_faninNB_sizes"] = Partition_all_faninNB_sizes
    DAG_info["DAG_tasks"] = Partition_DAG_tasks

    # Defaults are 1 and True
    DAG_version_number = 1
    DAG_is_complete = True
    Partition_DAG_number_of_tasks = len(Partition_DAG_tasks)
    DAG_info["DAG_version_number"] = DAG_version_number
    DAG_info["DAG_is_complete"] = DAG_is_complete
    DAG_info['DAG_number_of_tasks'] = Partition_DAG_number_of_tasks

#rhc: num_nodes:
    DAG_info["DAG_num_nodes_in_graph"] = Partition_DAG_num_nodes_in_graph

    file_name = "./DAG_info_Partition.pickle"
    with open(file_name, 'wb') as handle:
        cloudpickle.dump(DAG_info, handle) #, protocol=pickle.HIGHEST_PROTOCOL)  

    if not use_page_rank_group_partitions:
        file_name = "./DAG_info.pickle"
        with open(file_name, 'wb') as handle:
            cloudpickle.dump(DAG_info, handle) #, protocol=pickle.HIGHEST_PROTOCOL)  

    num_fanins = len(Partition_all_fanin_task_names)
    num_fanouts = len(Partition_all_fanout_task_names)
    num_faninNBs = len(Partition_all_faninNB_task_names)
    num_collapse = len(Partition_all_collapse_task_names)

    logger.trace("DAG_map:")
    for key, value in Partition_DAG_map.items():
        logger.trace(str(key) + ' : ' + str(value))
    logger.trace("")
    logger.trace("states:")        
    for key, value in Partition_DAG_states.items():
        logger.trace(str(key) + ' : ' + str(value))
    logger.trace("")
    logger.trace("num_fanins:" + str(num_fanins) + " num_fanouts:" + str(num_fanouts) + " num_faninNBs:"
    + str(num_faninNBs) + " num_collapse:" + str(num_collapse))
    logger.trace("")  
    logger.trace("Note: partitions only have collapse sets.")
    logger.trace("Partition_all_fanout_task_names:")
    for name in Partition_all_fanout_task_names:
        logger.trace(name)
    logger.trace("all_fanin_task_names:")
    for name in Partition_all_fanin_task_names :
        logger.trace(name)
    logger.trace("all_fanin_sizes:")
    for s in Partition_all_fanin_sizes :
        logger.trace(s)
    logger.trace("all_faninNB_task_names:")
    for name in Partition_all_faninNB_task_names:
        logger.trace(name)
    logger.trace("all_faninNB_sizes:")
    for s in Partition_all_faninNB_sizes:
        logger.trace(s)
    logger.trace("Partition_all_collapse_task_names:")
    for name in Partition_all_collapse_task_names:
        logger.trace(name)
    logger.trace("")
    logger.trace("leaf task start states:")
    for start_state in Partition_DAG_leaf_task_start_states:
        logger.trace(start_state)
    logger.trace("")
    logger.trace("DAG_tasks:")
    for key, value in Partition_DAG_tasks.items():
        logger.trace(str(key) + ' : ' + str(value))
    logger.trace("")
    logger.trace("DAG_leaf_tasks:")
    for task_name in Partition_DAG_leaf_tasks:
        logger.trace(task_name)
    logger.trace("")
    logger.trace("DAG_leaf_task_inputs:")
    for inp in Partition_DAG_leaf_task_inputs:
        logger.trace(inp)
    logger.trace("")
    logger.trace("DAG_version_number:")
    logger.trace(DAG_version_number)
    logger.trace("")
    logger.trace("DAG_is_complete:")
    logger.trace(DAG_is_complete)
    logger.trace("")
    logger.trace("DAG_number_of_tasks:")
    logger.trace(Partition_DAG_number_of_tasks)
#rhc: num_nodes
    logger.trace("")
    logger.trace("DAG_num_nodes_in_graph:")
    logger.trace(Partition_DAG_num_nodes_in_graph)
    logger.trace("")
    

    if False:
        DAG_info_Partition_read = DAG_Info.DAG_info_fromfilename(file_name_parm = "./DAG_info_Partition.pickle")
        
        DAG_map = DAG_info_Partition_read.get_DAG_map()
        #all_fanin_task_names = DAG_info_partition_read.get_all_fanin_task_names()
        #all_faninNB_task_names = DAG_info_partition_read.get_all_faninNB_task_names()
        #all_faninNB_sizes = DAG_info_partition_read.get_all_faninNB_sizes()
        #all_fanout_task_names = DAG_info_partition_read.get_all_fanout_task_names()
        # Note: all fanout_sizes is not needed since fanouts are fanins that have size 1
        DAG_states = DAG_info_Partition_read.get_DAG_states()
        DAG_leaf_tasks = DAG_info_Partition_read.get_DAG_leaf_tasks()
        DAG_leaf_task_start_states = DAG_info_Partition_read.get_DAG_leaf_task_start_states()
        DAG_tasks = DAG_info_Partition_read.get_DAG_tasks()

        DAG_leaf_task_inputs = DAG_info_Partition_read.get_DAG_leaf_task_inputs()

        DAG_is_complete = DAG_info_Partition_read.get_DAG_info_is_complete()
        DAG_version_number = DAG_info_Partition_read.get_DAG_version_number()
        DAG_number_of_tasks = DAG_info_Partition_read.get_DAG_number_of_tasks()
        DAG_num_nodes_in_graph = DAG_info_Partition_read.get_DAG_num_nodes_in_graph()

        logger.trace("")
        logger.trace("DAG_info partition after read:")
        output_DAG = True
        # add-0bec4d19-bce6-4394-ad62-9b0eab3081a9
        if output_DAG:
            # FYI:
            logger.trace("DAG_map:")
            for key, value in DAG_map.items():
                logger.trace(str(key) + ' : ' + str(value))
                #logger.trace(key)
                #logger.trace(value)
            logger.trace("  ")
            logger.trace("DAG states:")      
            for key, value in DAG_states.items():
                logger.trace(str(key) + ' : ' + str(value))
            logger.trace("   ")
            logger.trace("DAG leaf task start states")
            for start_state in DAG_leaf_task_start_states:
                logger.trace(start_state)
            logger.trace("")
            logger.trace("DAG_tasks:")
            for key, value in DAG_tasks.items():
                logger.trace(str(key) + ' : ' + str(value))
            logger.trace("")
            logger.trace("DAG_leaf_tasks:")
            for task_name in DAG_leaf_tasks:
                logger.trace(task_name)
            logger.trace("") 
            logger.trace("DAG_leaf_task_inputs:")
            for inp in DAG_leaf_task_inputs:
                logger.trace(inp)
            logger.trace("")
            logger.trace("DAG_version_number:")
            logger.trace(DAG_version_number)
            logger.trace("")
            logger.trace("DAG_is_complete:")
            logger.trace(DAG_is_complete)
            logger.trace("")
            logger.trace("DAG_number_of_tasks:")
            logger.trace(DAG_number_of_tasks)
            logger.trace("")
#rhc: num_nodes
            logger.trace("DAG_num_nodes_in_graph:")
            logger.trace(DAG_num_nodes_in_graph)
            logger.trace("")

    Group_all_fanout_task_names = []
    Group_all_fanin_task_names = []
    Group_all_faninNB_task_names = []
    Group_all_collapse_task_names = []
    Group_all_fanin_sizes = []
    Group_all_faninNB_sizes = []

    Group_DAG_leaf_tasks = []
    Group_DAG_leaf_task_start_states = []
    # no inputs for leaf tasks
    Group_DAG_leaf_task_inputs = []
    Group_DAG_map = {}
    Group_DAG_states = {}
    Group_DAG_tasks = {}

    Group_DAG_number_of_tasks = 0

#rhc_ num_nodes
    Group_DAG_num_nodes_in_graph = num_nodes_in_graph

    # sink nodes, i.e., nodes that do not send any inputs
    Group_sink_set = set()
    logger.trace("Group DAG:")
    state = 1
    for senderX in Group_senders:
        logger.trace("senderX: " + senderX)
        fanouts = []
#rhc: clustering
        fanout_partition_group_sizes = []
        fanins = []
        faninNBs = []
        collapse = []
        fanin_sizes = []
        faninNB_sizes = []
        receiver_set_for_senderX = Group_senders[senderX]
        for receiverY in receiver_set_for_senderX:
            receiver_set_for_receiverY = Group_senders.get(receiverY)
            if receiver_set_for_receiverY is None:
                # receiverY does not send any inputs so it is a sink
                Group_sink_set.add(receiverY)
            sender_set_for_receiverY = Group_receivers[receiverY]
            length_of_sender_set_for_receiverY = len(sender_set_for_receiverY)
            length_of_receiver_set_for_senderX = len(receiver_set_for_senderX)
            if length_of_sender_set_for_receiverY == 1:
                # collapse or fanout
                if length_of_receiver_set_for_senderX == 1:
                    # only one task sends input to receiverY and this sending 
                    # task only sends to one task, so collapse receiverY, i.e.,
                    # senderX becomes receiverY
                    logger.trace("sender " + senderX + " --> " + receiverY + " : Collapse")
                    if not receiverY in Group_all_collapse_task_names:
                        Group_all_collapse_task_names.append(receiverY)
                    else:
                        pass # this is an error, only one task can collapse a given task
                    collapse.append(receiverY)
                else:
                    # only one task sends input to receiverY and this sending 
                    # task sends to other tasks too, so senderX does a fanout 
                    # to receiverY   
                    logger.info("sender " + senderX + " --> " + receiverY + " : Fanout")
                    if not receiverY in Group_all_fanout_task_names:
                        Group_all_fanout_task_names.append(receiverY)
                    fanouts.append(receiverY)
#rhc: clustering
                    if enable_runtime_task_clustering:
                        num_shadow_nodes = groups_num_shadow_nodes_map[receiverY]
                        #logger.trace("number of shadow nodes for " + receiverY + " is " + str(num_shadow_nodes)) 
                        fanout_partition_group_sizes.append(num_shadow_nodes)
                        #logger.trace("fanout_partition_group_sizes after append: " + str(fanout_partition_group_sizes))

            else:
                # fanin or fannNB since receiverY receives inputs from multiple tasks
                isFaninNB = False
                 # senderZ sends an input to receiverY
                for senderZ in sender_set_for_receiverY:
                    # tasks to which senderX sends an input
                    receiver_set_for_senderZ = Group_senders[senderZ]
                    # since senderX sends inputs to more than one task, receiverY
                    # is a faninNB task (as senderX cannot become receiverY)
                    if len(receiver_set_for_senderZ) > 1:
                        # if any task sends inputs to reciverY and any other task(s)
                        # then receiverY must be a faninNB task since some sender cannot 
                        # become receiverY.
                        isFaninNB = True
                        break
                if isFaninNB:
                    logger.trace("sender " + senderX + " --> " + receiverY + " : FaninNB")
                    if not receiverY in Group_all_faninNB_task_names:
                        Group_all_faninNB_task_names.append(receiverY)
                        Group_all_faninNB_sizes.append(length_of_sender_set_for_receiverY)
                    logger.trace ("after Group_all_faninNBs_sizes append: " + str(Group_all_faninNB_sizes))
                    logger.trace ("faninNBs append: " + receiverY)
                    faninNBs.append(receiverY)
                    faninNB_sizes.append(length_of_sender_set_for_receiverY)
                else:
                    # senderX sends an input only to receiverY, same for any other
                    # tasks that sends inputs to receiverY so receiverY is a fanin task.
                    logger.trace("sender " + senderX + " --> " + receiverY + " : Fanin")
                    if not receiverY in Group_all_fanin_task_names:
                        Group_all_fanin_task_names.append(receiverY)
                        Group_all_fanin_sizes.append(length_of_sender_set_for_receiverY)
                    fanins.append(receiverY)
                    fanin_sizes.append(length_of_sender_set_for_receiverY)

        # get the tasks that send to senderX, i.e., provide inputs for senderX
        sender_set_for_senderX = Group_receivers.get(senderX)
        if sender_set_for_senderX is None:
            # senderX is a leaf task since it is not a receiver
            Group_DAG_leaf_tasks.append(senderX)
            Group_DAG_leaf_task_start_states.append(state)
            task_inputs = ()
            Group_DAG_leaf_task_inputs.append(task_inputs)

            try:
                msg = "[Error]: BFS_generate_DAG_info: partition " + senderX + " receives no inputs" \
                    + " but it is not in leaf_tasks_of_groups."
                assert senderX in leaf_tasks_of_groups , msg
            except AssertionError:
                logger.exception("[Error]: assertion failed")
                if exit_program_on_exception:
                    logging.shutdown()
                    os._exit(0)
            #assertOld:
            #if not senderX in leaf_tasks_of_groups:
            #    logger.error("partition " + senderX + " receives no inputs"
            #        + " but it is not in leaf_tasks_of_groups.")
            #else:
            # we have generated a state for leaf task senderX. 
            leaf_tasks_of_groups.remove(senderX)
            
        else:
            # create a new set from sender_set_for_senderX. For 
            # each name in sender_set_for_senderX, qualify name by
            # prexing it with "senderX-". Example: senderX is "PR1_1"
            # and name is "PR2_3" so the qualified name is "PR1_1-PR2_3".
            # We use qualified names since the fanouts/faninNBs for a 
            # task in a pagerank DAG may al have diffent values. This
            # is unlike Dask DAGs in which all fanouts/faninNBs of a task
            # receive the same value. We denote the different outputs
            # of a task A having, e.g., fanouts B and C as "A-B" and "A-C"
            sender_set_for_senderX_with_qualified_names = set()
            # for each task name that sends input to senderX, the 
            # qualified name of the sender is name+"-"+senderX
            for name in sender_set_for_senderX:
                qualified_name = str(name) + "-" + str(senderX)
                sender_set_for_senderX_with_qualified_names.add(qualified_name)
            # sender_set_for_senderX provides input for senderX
            task_inputs = tuple(sender_set_for_senderX_with_qualified_names)
        
        logger.info("fanout_partition_group_sizes for state_info: " + str(fanout_partition_group_sizes))
        Group_DAG_map[state] = state_info(senderX, fanouts, fanins, faninNBs, collapse, fanin_sizes, faninNB_sizes, task_inputs,
#rhc: clustering
            False, False, fanout_partition_group_sizes)
        Group_DAG_states[senderX] = state

        state += 1

    # This is for leaf tasks that are not senders. We processed
    # senders and removed senders that were leaf tasks from the
    # leaf_tasks_of_groups but there may be leaf tasks that are
    # not senders so we still need to add them to DAG.
    if not len(leaf_tasks_of_groups) == 0:
        logger.trace("generate_DAG_info: len(leaf_tasks_of_groups)>0, add leaf tasks")
        fanouts = []
#rhc: clustering
        fanout_partition_group_sizes = []
        faninNBs = []
        fanins = []
        collapse = []
        fanin_sizes = []
        faninNB_sizes = []

        task_inputs = ()
        for name in leaf_tasks_of_groups:
            logger.trace("generate_DAG_info: add leaf task for group " + name)
            Group_DAG_leaf_tasks.append(name)
            Group_DAG_leaf_task_start_states.append(state)
            Group_DAG_leaf_task_inputs.append(task_inputs)

            Group_DAG_map[state] = state_info(name, fanouts, fanins, faninNBs, collapse, fanin_sizes, faninNB_sizes, task_inputs,
#rhc: clustering
                False,  False, fanout_partition_group_sizes)
            Group_DAG_states[name] = state
            state += 1

    # Finish by doing the receivers that are not senders (opposite of leaf tasks);
    # these are reeivers that send no inputs to other tasks. They have no fanins/
    # faninBs, fanouts or collapses, but they do have task inputs.
    for receiverY in Group_sink_set: # Partition_receivers:
        #if not receiverY in Partition_DAG_states:
            fanouts = []
#rhc: clustering
            fanout_partition_group_sizes = []
            faninNBs = []
            fanins = []
            collapse = []
            fanin_sizes = []
            faninNB_sizes = []

            sender_set_for_receiverY = Group_receivers[receiverY]
            #task_inputs = tuple(sender_set_for_receiverY)

            # create a new set from sender_set_for_senderX. For 
            # each name in sender_set_for_senderX, qualify name by
            # prexing it with "senderX-". Example: senderX is "PR1_1"
            # and name is "PR2_3" so the qualified name is "PR1_1-PR2_3".
            # We use qualified names since the fanouts/faninNBs for a 
            # task in a pagerank DAG may al have diffent values. This
            # is unlike Dask DAGs in which all fanouts/faninNBs of a task
            # receive the same value. We denote the different outputs
            # of a task A having, e.g., fanouts B and C as "A-B" and "A-C"
            sender_set_for_receiverY_with_qualified_names = set()
            for senderX in sender_set_for_receiverY:
                qualified_name = str(senderX) + "-" + str(receiverY)
                sender_set_for_receiverY_with_qualified_names.add(qualified_name)
            # sender_set_for_senderX provides input for senderX
            task_inputs = tuple(sender_set_for_receiverY_with_qualified_names)

            Group_DAG_map[state] = state_info(receiverY, fanouts, fanins, faninNBs, collapse, fanin_sizes, faninNB_sizes, task_inputs,
#rhc: clustering
                False,  False, fanout_partition_group_sizes)
            Group_DAG_states[receiverY] = state
            state += 1

    if not use_shared_partitions_groups:
        for key in Group_DAG_states:
            Group_DAG_tasks[key] = PageRank_Function_Driver
    else:
        if not use_struct_of_arrays_for_pagerank:
            for key in Group_DAG_states:
                Group_DAG_tasks[key] = PageRank_Function_Driver_Shared 
        else:
            for key in Group_DAG_states:
                Group_DAG_tasks[key] = PageRank_Function_Driver_Shared_Fast  

    logger.trace("")
    DAG_info = {}
    DAG_info["DAG_map"] = Group_DAG_map
    DAG_info["DAG_states"] = Group_DAG_states
    DAG_info["DAG_leaf_tasks"] = Group_DAG_leaf_tasks
    DAG_info["DAG_leaf_task_start_states"] = Group_DAG_leaf_task_start_states
    DAG_info["DAG_leaf_task_inputs"] = Group_DAG_leaf_task_inputs
    DAG_info["all_fanout_task_names"] = Group_all_fanout_task_names
    DAG_info["all_fanin_task_names"] = Group_all_fanin_task_names
    DAG_info["all_faninNB_task_names"] = Group_all_faninNB_task_names
    DAG_info["all_collapse_task_names"] = Group_all_collapse_task_names
    DAG_info["all_fanin_sizes"] = Group_all_fanin_sizes
    DAG_info["all_faninNB_sizes"] = Group_all_faninNB_sizes
    DAG_info["DAG_tasks"] = Group_DAG_tasks

    DAG_version_number = 1
    DAG_is_complete = True
    Group_DAG_number_of_tasks = len(Group_DAG_tasks)
    DAG_info["DAG_version_number"] = DAG_version_number
    DAG_info["DAG_is_complete"] = DAG_is_complete
    DAG_info["DAG_number_of_tasks"] = Group_DAG_number_of_tasks
    DAG_info["DAG_number_of_incomplete_tasks"] = 0
#rhc: num_nodes
    DAG_info["DAG_num_nodes_in_graph"] = Group_DAG_num_nodes_in_graph
    

    file_name = "./DAG_info_Group.pickle"
    with open(file_name, 'wb') as handle:
        cloudpickle.dump(DAG_info, handle) #, protocol=pickle.HIGHEST_PROTOCOL)  

    if use_page_rank_group_partitions:
        file_name = "./DAG_info.pickle"
        with open(file_name, 'wb') as handle:
            cloudpickle.dump(DAG_info, handle) #, protocol=pickle.HIGHEST_PROTOCOL)  

    num_fanins = len(Group_all_fanin_task_names)
    num_fanouts = len(Group_all_fanout_task_names)
    num_faninNBs = len(Group_all_faninNB_task_names)
    num_collapse = len(Group_all_collapse_task_names)

    logger.trace("GroupDAG_map:")
    for key, value in Group_DAG_map.items():
        logger.trace(str(key) + ' : ' + str(value))
    logger.trace("")
    logger.trace("states:")        
    for key, value in Group_DAG_states.items():
        logger.trace(str(key) + ' : ' + str(value))
    logger.trace("")
    logger.trace("num_fanins:" + str(num_fanins) + " num_fanouts:" + str(num_fanouts) + " num_faninNBs:"
    + str(num_faninNBs) + " num_collapse:" + str(num_collapse))
    logger.trace("")  
    logger.trace("all_fanout_task_names:")
    for name in Group_all_fanout_task_names:
        logger.trace(name)
    logger.trace("")
    logger.trace("all_fanin_task_names:")
    for name in Group_all_fanin_task_names :
        logger.trace(name)
    logger.trace("")
    logger.trace("all_fanin_sizes:")
    for s in Group_all_fanin_sizes :
        logger.trace(s)
    logger.trace("")
    logger.trace("all_faninNB_task_names:")
    for name in Group_all_faninNB_task_names:
        logger.trace(name)
    logger.trace("")
    logger.trace("all_faninNB_sizes:")
    for s in Group_all_faninNB_sizes :
        logger.trace(s)
    logger.trace("")
    logger.trace("all_collapse_task_names:")
    for name in Group_all_collapse_task_names:
        logger.trace(name)
    logger.trace("")
    logger.trace("leaf task start states:")
    for start_state in Group_DAG_leaf_task_start_states:
        logger.trace(start_state)
    logger.trace("")
    logger.trace("DAG_tasks:")
    for key, value in Group_DAG_tasks.items():
        logger.trace(str(key) + ' : ' + str(value))
    logger.trace("")
    logger.trace("DAG_leaf_tasks:")
    for task_name in Group_DAG_leaf_tasks:
        logger.trace(task_name)
    logger.trace("")
    logger.trace("DAG_leaf_task_inputs:")
    for inp in Group_DAG_leaf_task_inputs:
        logger.trace(inp)
    logger.trace("")
    logger.trace("DAG_version_number:")
    logger.trace(DAG_version_number)
    logger.trace("")
    logger.trace("DAG_is_complete:")
    logger.trace(DAG_is_complete)
    logger.trace("")
    logger.trace("DAG_number_of_tasks:")
    logger.trace(Group_DAG_number_of_tasks)
    logger.trace("")
#rhc: num_nodes
    logger.trace("DAG_num_nodes_in_graph:")
    logger.trace(Group_DAG_num_nodes_in_graph)
    logger.trace("")
    

    if (False):
        DAG_info_Partition_read = DAG_Info.DAG_info_fromfilename(file_name_parm = "./DAG_info_Group.pickle")
        
        DAG_map = DAG_info_Partition_read.get_DAG_map()
        #all_fanin_task_names = DAG_info_partition_read.get_all_fanin_task_names()
        #all_fanin_sizes = DAG_info_partition_read.get_all_fanin_sizes()
        #all_faninNB_task_names = DAG_info_partition_read.get_all_faninNB_task_names()
        #all_faninNB_sizes = DAG_info_partition_read.get_all_faninNB_sizes()
        #all_fanout_task_names = DAG_info_partition_read.get_all_fanout_task_names()
        # Note: all fanout_sizes is not needed since fanouts are fanins that have size 1
        DAG_states = DAG_info_Partition_read.get_DAG_states()
        DAG_leaf_tasks = DAG_info_Partition_read.get_DAG_leaf_tasks()
        DAG_leaf_task_start_states = DAG_info_Partition_read.get_DAG_leaf_task_start_states()
        DAG_tasks = DAG_info_Partition_read.get_DAG_tasks()

        DAG_leaf_task_inputs = DAG_info_Partition_read.get_DAG_leaf_task_inputs()

        DAG_is_complete = DAG_info_Partition_read.get_DAG_info_is_complete()
        DAG_version_number = DAG_info_Partition_read.get_DAG_version_number()
        DAG_number_of_tasks = DAG_info_Partition_read.get_DAG_number_of_tasks()

        logger.trace("")
        logger.trace("DAG_info group after read:")
        output_DAG = True
        # add-0bec4d19-bce6-4394-ad62-9b0eab3081a9
        if output_DAG:
            # FYI:
            logger.trace("DAG_map:")
            for key, value in DAG_map.items():
                print_str = ""
                print_str = str(key) + " " + str(value)
                logger.trace(print_str)
                #logger.trace(key)
                #logger.trace(value)
            logger.trace("  ")
            logger.trace("DAG states:")         
            for key, value in DAG_states.items():
                print_str = ""
                print_str = str(key) + " " + str(value)
                logger.trace(print_str)
                #logger.trace(key)
                #logger.trace(value)
            logger.trace("   ")
            logger.trace("DAG leaf task start states")
            for start_state in DAG_leaf_task_start_states:
                logger.trace(start_state)
            logger.trace("")
            logger.trace("DAG_tasks:")
            for key, value in DAG_tasks.items():
                logger.trace(str(key) + ' : ' + str(value))
            logger.trace("")
            logger.trace("DAG_leaf_tasks:")
            for task_name in DAG_leaf_tasks:
                logger.trace(task_name)
            logger.trace("") 
            logger.trace("DAG_leaf_task_inputs:")
            for inp in DAG_leaf_task_inputs:
                logger.trace(inp)
            logger.trace("")
            logger.trace("DAG_version_number:")
            logger.trace(DAG_version_number)
            logger.trace("")
            logger.trace("DAG_info_is_complete:")
            logger.trace(DAG_is_complete)
            logger.trace("")
            logger.trace("DAG_info_number_of_tasks:")
            logger.trace(DAG_number_of_tasks)
            logger.trace("")
#rhc: num_nodes
            logger.trace("Group_DAG_num_nodes_in_graph:")
            logger.trace(Group_DAG_num_nodes_in_graph)
            logger.trace("")

    """
    dsk = {
            'PR1_1': (PageRank_Function_Driver),
            'PR9_1': (PageRank_Function_Driver),
            'PR2_1': (PageRank_Function_Driver, 'PR1_1'),
            'PR3_1': (PageRank_Function_Driver, 'PR2_1'),
            'PR3_2': (PageRank_Function_Driver, 'PR2_1'),
            'PR3_3': (PageRank_Function_Driver, 'PR2_1'),
            'PR4_1': (PageRank_Function_Driver, 'PR3_1'),
            'PR4_2': (PageRank_Function_Driver, 'PR3_3'),
            'PR4_3': (PageRank_Function_Driver, 'PR3_3'),
            'PR5_1': (PageRank_Function_Driver, 'PR4_2'),
            'PR5_2': (PageRank_Function_Driver, 'PR4_2'),
            'PR5_3': (PageRank_Function_Driver, 'PR4_3'),
            'PR5_4': (PageRank_Function_Driver, ['PR5_2', 'PR4_3']),
            'PR6_1': (PageRank_Function_Driver, 'PR5_1'),
            'PR6_2': (PageRank_Function_Driver, ['PR5_3', 'PR5_1']),
            'PR6_3': (PageRank_Function_Driver, 'PR5_3'),
            'PR7_1': (PageRank_Function_Driver, 'PR6_3'),
            'PR7_2': (PageRank_Function_Driver, 'PR6_3'),
            'PR8_1': (PageRank_Function_Driver, 'PR7_1'),
            'PR8_2': (PageRank_Function_Driver, 'PR7_2'),
            'PR8_3': (PageRank_Function_Driver, 'PR7_2'),
            'PR10_1': (PageRank_Function_Driver, 'PR9_1'),
            'PR10_2': (PageRank_Function_Driver, 'PR9_1'),
            'PR10_3': (PageRank_Function_Driver, 'PR9_1'),
            'PR11_1': (PageRank_Function_Driver, ['PR10_2', 'PR10_3'])
    }
    """

    logger.trace("")
    
    if not use_shared_partitions_groups:
        driver = "PageRank_Function_Driver"
    else:
        driver = "PageRank_Function_Driver_Shared"
    dsk_lines = []
    header_line = "\t" + "dsk = {"
    dsk_lines.append(header_line)
    for leaf_task in Partition_DAG_leaf_tasks:
        leaf_line = "\t\t\t" + "\'" + str(leaf_task) + "\': (" + driver + "),"
        dsk_lines.append(leaf_line)

    for receiverX in Partition_receivers:
        logger.trace("receiverX:" + receiverX)
        sender_set_for_receiverX = Partition_receivers.get(receiverX)
        logger.trace("sender_set_for_receiverX:" + str(sender_set_for_receiverX))
        non_leaf_line_prefix = "\t\t\t" + "\'" + str(receiverX) + "\': (" + driver + ", "
        if len(sender_set_for_receiverX) > 1:
            non_leaf_line = "["
            first = True
            for sender_task in sender_set_for_receiverX:
                if first:
                    first = False
                    non_leaf_line += "\'" + str(sender_task) + "\'"
                else:
                    non_leaf_line += ", " + "\'" + str(sender_task) + "\'"
            non_leaf_line += "]),"
        else:
            sender_task = tuple(sender_set_for_receiverX)[0]
            non_leaf_line = "\'" + sender_task + "\'),"
        dsk_lines.append(non_leaf_line_prefix + non_leaf_line)

    dsk_lines[len(dsk_lines)-1] = dsk_lines[len(dsk_lines)-1][:-1]
    footer_line = "\t\t" + "}"
    dsk_lines.append(footer_line)

    logger.trace("dsk lines for Partitions:")
    for line in dsk_lines:
        logger.trace(line)

    logger.trace("")
    logger.trace("")
    dsk_lines = []
    dsk_lines.append(header_line)
    for leaf_task in Group_DAG_leaf_tasks:
        leaf_line = "\t\t\t" + "\'" + str(leaf_task) + "\': (" + driver + "),"
        dsk_lines.append(leaf_line)

    for receiverX in Group_receivers:
        logger.trace("receiverX:" + receiverX)
        sender_set_for_receiverX = Group_receivers.get(receiverX)
        logger.trace("sender_set_for_receiverX:" + str(sender_set_for_receiverX))
        non_leaf_line_prefix = "\t\t\t" + "\'" + str(receiverX) + "\': (" + driver + ", "
        if len(sender_set_for_receiverX) > 1:
            non_leaf_line = "["
            first = True
            for sender_task in sender_set_for_receiverX:
                if first:
                    first = False
                    non_leaf_line += "\'" + str(sender_task) + "\'"
                else:
                    non_leaf_line += ", " + "\'" + str(sender_task) + "\'"
            non_leaf_line += "]),"
        else:
            sender_task = tuple(sender_set_for_receiverX)[0]
            non_leaf_line = "\'" + sender_task + "\'),"
        dsk_lines.append(non_leaf_line_prefix + non_leaf_line)

    dsk_lines[len(dsk_lines)-1] = dsk_lines[len(dsk_lines)-1][:-1]
    footer_line = "\t\t" + "}"
    dsk_lines.append(footer_line)

    logger.trace("dsk lines for Groups:")
    for line in dsk_lines:
        logger.trace(line)



"""
DELETE THIS - note: it was not updated with changes, e.g., clustering
def OLD_generate_DAG_info_incremental_partitions(partition_name,current_partition_number,is_complete):
    Partition_all_fanout_task_names = []
    Partition_all_fanin_task_names = []
    Partition_all_faninNB_task_names = []
    Partition_all_collapse_task_names = []

    Partition_all_fanin_sizes = []
    Partition_all_faninNB_sizes = []

    Partition_DAG_leaf_tasks = []
    Partition_DAG_leaf_task_start_states = []
    # no inputs for leaf tasks
    Partition_DAG_leaf_task_inputs = []
    Partition_DAG_map = {}
    Partition_DAG_states = {}
    Partition_DAG_tasks = {}

    print()
    print("Partition_senders:")
    for sender_name,receiver_name_set in Partition_senders.items():
        print("sender:" + sender_name)
        print("receiver_name_set:" + str(receiver_name_set))
    print()
    print()
    print("Partition_receivers:")
    for receiver_name,sender_name_set in Partition_receivers.items():
        print("receiver:" + receiver_name)
        print("sender_name_set:" + str(sender_name_set))
    print()
    print()
    print("Leaf nodes of partitions:")
    for name in leaf_tasks_of_partitions:
        print(name + " ")

    logger.trace("Partition DAG:")

    state = 1
    # partition i has a collapse to partition i+1
    # Task senderX sends inputs to one or more other tasks

    # sink nodes, i.e., nodes that do not send any inputs
    Partition_sink_set = set()

    partition_number = 1

    for senderX in Partition_senders:
        fanouts = []
#rhc: clustering
        fanout_partition_group_sizes = []
        faninNBs = []
        fanins = []
        collapse = []
        fanin_sizes = []
        faninNB_sizes = []
        # tasks that receive inputs from senderX
        receiver_set_for_senderX = Partition_senders[senderX]
        # suggest assert len = 1
        receiverY = receiver_set_for_senderX[0]

        receiver_set_for_receiverY = Partition_senders.get(receiverY)
        if receiver_set_for_receiverY is None:
            # receiverY does not send any inputs so it is a sink.
            # This is the last partition
            Partition_sink_set.add(receiverY)

        sender_set_for_receiverY = Partition_receivers[receiverY]
        # suggest assert len = 0
        logger.trace("sender " + senderX + " --> " + receiverY + " : Collapse")
        Partition_all_collapse_task_names.append(receiverY)
        collapse.append(receiverY)
        sender_set_for_senderX = Partition_receivers.get(senderX)
        if partition_number == 1:
            # suggest assert len ssfsX = None
            Partition_DAG_leaf_tasks.append(senderX)
            Partition_DAG_leaf_task_start_states.append(state)
            task_inputs = ()
            Partition_DAG_leaf_task_inputs.append(task_inputs)
            if not senderX in leaf_tasks_of_partitions:
                logger.error("partition " + senderX + " receives no inputs"
                    + " but it is not in leaf_tasks_of_partitions.")
            else:
                # we have generated a state for leaf task senderX. 
                leaf_tasks_of_partitions.remove(senderX)
        else:
            sender_set_for_senderX_with_qualified_names = set()
            for name in sender_set_for_senderX:
                qualified_name = str(name) + "-" + str(senderX)
                sender_set_for_senderX_with_qualified_names.add(qualified_name)
            # sender_set_for_senderX provides input for senderX
            task_inputs = tuple(sender_set_for_senderX_with_qualified_names)
        Partition_DAG_map[state] = state_info(senderX, fanouts, fanins, faninNBs, collapse, fanin_sizes, faninNB_sizes, task_inputs)
        Partition_DAG_states[senderX] = state
        state += 1
        partition_number += 1

    if partition_number > 2:
        # suggest assert len Partition_sink_set == 1
        # get first and only element of set, which is last partition
        receiverY = next(iter(Partition_sink_set))
        #if not receiverY in Partition_DAG_states:
        fanouts = []
#rhc: clustering
        fanout_partition_group_sizes = []
        faninNBs = []
        fanins = []
        collapse = []
        fanin_sizes = []
        faninNB_sizes = []

        sender_set_for_receiverY = Partition_receivers[receiverY]
        #task_inputs = tuple(sender_set_for_receiverY)

        # create a new set from sender_set_for_senderX. For 
        # each name in sender_set_for_senderX, qualify name by
        # prexing it with "senderX-". Example: senderX is "PR1_1"
        # and name is "PR2_3" so the qualified name is "PR1_1-PR2_3".
        # We use qualified names since the fanouts/faninNBs for a 
        # task in a pagerank DAG may al have diffent values. This
        # is unlike Dask DAGs in which all fanouts/faninNBs of a task
        # receive the same value. We denote the different outputs
        # of a task A having, e.g., fanouts B and C as "A-B" and "A-C"
        sender_set_for_receiverY_with_qualified_names = set()
        # for each task senderX that sends input to receiverY, the 
        # qualified name of the sender is senderX+"-"+senderX
        for senderX in sender_set_for_receiverY:
            qualified_name = str(senderX) + "-" + str(receiverY)
            sender_set_for_receiverY_with_qualified_names.add(qualified_name)
        # sender_set_for_senderX provides input for senderX
        task_inputs = tuple(sender_set_for_receiverY_with_qualified_names)

        Partition_DAG_map[state] = state_info(receiverY, fanouts, fanins, faninNBs, collapse, fanin_sizes, faninNB_sizes, task_inputs)
        Partition_DAG_states[receiverY] = state
        state += 1
        partition_number += 1

    if not use_shared_partitions_groups:
        for key in Partition_DAG_states:
            Partition_DAG_tasks[key] = PageRank_Function_Driver
    else:
        if not use_struct_of_arrays_for_pagerank:
            for key in Partition_DAG_states:
                Partition_DAG_tasks[key] = PageRank_Function_Driver_Shared 
        else:
            for key in Partition_DAG_states:
                Partition_DAG_tasks[key] = PageRank_Function_Driver_Shared_Fast  


    logger.trace("")
    DAG_info = {}
    DAG_info["DAG_map"] = Partition_DAG_map
    DAG_info["DAG_states"] = Partition_DAG_states
    DAG_info["DAG_leaf_tasks"] = Partition_DAG_leaf_tasks
    DAG_info["DAG_leaf_task_start_states"] = Partition_DAG_leaf_task_start_states
    DAG_info["DAG_leaf_task_inputs"] = Partition_DAG_leaf_task_inputs
    DAG_info["all_fanout_task_names"] = Partition_all_fanout_task_names
    DAG_info["all_fanin_task_names"] = Partition_all_fanin_task_names
    DAG_info["all_faninNB_task_names"] = Partition_all_faninNB_task_names
    DAG_info["all_collapse_task_names"] = Partition_all_collapse_task_names
    DAG_info["all_fanin_sizes"] = Partition_all_fanin_sizes
    DAG_info["all_faninNB_sizes"] = Partition_all_faninNB_sizes
    DAG_info["DAG_tasks"] = Partition_DAG_tasks

    # Defaults are 1 and True
    DAG_info_version_number = 1
    DAG_info_is_complete = False
    DAG_info["version_number"] = DAG_info_version_number
    DAG_info["DAG_info_is_complete"] = DAG_info_is_complete

    file_name = "./DAG_info_Partition_incremental.pickle"
    with open(file_name, 'wb') as handle:
        cloudpickle.dump(DAG_info, handle) #, protocol=pickle.HIGHEST_PROTOCOL)  


#rhc: Do this? We only read at start.

    if not use_page_rank_group_partitions:
        file_name = "./DAG_info.pickle"
        with open(file_name, 'wb') as handle:
            cloudpickle.dump(DAG_info, handle) #, protocol=pickle.HIGHEST_PROTOCOL)  

    num_fanins = len(Partition_all_fanin_task_names)
    num_fanouts = len(Partition_all_fanout_task_names)
    num_faninNBs = len(Partition_all_faninNB_task_names)
    num_collapse = len(Partition_all_collapse_task_names)

    logger.trace("DAG_map:")
    for key, value in Partition_DAG_map.items():
        logger.trace(str(key) + ' : ' + str(value))
    logger.trace("")
    logger.trace("states:")        
    for key, value in Partition_DAG_states.items():
        logger.trace(str(key) + ' : ' + str(value))
    logger.trace("")
    logger.trace("num_fanins:" + str(num_fanins) + " num_fanouts:" + str(num_fanouts) + " num_faninNBs:"
    + str(num_faninNBs) + " num_collapse:" + str(num_collapse))
    logger.trace("")  
    logger.trace("Note: partitions only have collapse sets.")
    logger.trace("Partition_all_fanout_task_names:")
    for name in Partition_all_fanout_task_names:
        logger.trace(name)
    logger.trace("all_fanin_task_names:")
    for name in Partition_all_fanin_task_names :
        logger.trace(name)
    logger.trace("all_fanin_sizes:")
    for s in Partition_all_fanin_sizes :
        logger.trace(s)
    logger.trace("all_faninNB_task_names:")
    for name in Partition_all_faninNB_task_names:
        logger.trace(name)
    logger.trace("all_faninNB_sizes:")
    for s in Partition_all_faninNB_sizes:
        logger.trace(s)
    logger.trace("Partition_all_collapse_task_names:")
    for name in Partition_all_collapse_task_names:
        logger.trace(name)
    logger.trace("")
    logger.trace("leaf task start states:")
    for start_state in Partition_DAG_leaf_task_start_states:
        logger.trace(start_state)
    logger.trace("")
    logger.trace("DAG_tasks:")
    for key, value in Partition_DAG_tasks.items():
        logger.trace(str(key) + ' : ' + str(value))
    logger.trace("")
    logger.trace("DAG_leaf_tasks:")
    for task_name in Partition_DAG_leaf_tasks:
        logger.trace(task_name)
    logger.trace("")
    logger.trace("DAG_leaf_task_inputs:")
    for inp in Partition_DAG_leaf_task_inputs:
        logger.trace(inp)
    logger.trace("")
    logger.trace("DAG_version_number:")
    logger.trace(DAG_info_version_number)
    logger.trace("")
    logger.trace("DAG_info_is_complete:")
    logger.trace(DAG_info_is_complete)
    logger.trace("")

    DAG_info_partition_read = DAG_Info(file_name = "./DAG_info_Partition_incremental.pickle")
    
    DAG_map = DAG_info_partition_read.get_DAG_map()
    #all_fanin_task_names = DAG_info_partition_read.get_all_fanin_task_names()
    #all_faninNB_task_names = DAG_info_partition_read.get_all_faninNB_task_names()
    #all_faninNB_sizes = DAG_info_partition_read.get_all_faninNB_sizes()
    #all_fanout_task_names = DAG_info_partition_read.get_all_fanout_task_names()
    # Note: all fanout_sizes is not needed since fanouts are fanins that have size 1
    DAG_states = DAG_info_partition_read.get_DAG_states()
    DAG_leaf_tasks = DAG_info_partition_read.get_DAG_leaf_tasks()
    DAG_leaf_task_start_states = DAG_info_partition_read.get_DAG_leaf_task_start_states()
    DAG_tasks = DAG_info_partition_read.get_DAG_tasks()

    DAG_leaf_task_inputs = DAG_info_partition_read.get_DAG_leaf_task_inputs()

    DAG_info_is_complete = DAG_info_partition_read.get_DAG_info_is_complete()
    DAG_info_version_number = DAG_info_partition_read.get_DAG_version_number()

    logger.trace("")
    logger.trace("DAG_info partition after read:")
    output_DAG = True
    if output_DAG:
        # FYI:
        logger.trace("DAG_map:")
        for key, value in DAG_map.items():
            logger.trace(str(key) + ' : ' + str(value))
            #logger.trace(key)
            #logger.trace(value)
        logger.trace("  ")
        logger.trace("DAG states:")      
        for key, value in DAG_states.items():
            logger.trace(str(key) + ' : ' + str(value))
        logger.trace("   ")
        logger.trace("DAG leaf task start states")
        for start_state in DAG_leaf_task_start_states:
            logger.trace(start_state)
        logger.trace("")
        logger.trace("DAG_tasks:")
        for key, value in DAG_tasks.items():
            logger.trace(str(key) + ' : ' + str(value))
        logger.trace("")
        logger.trace("DAG_leaf_tasks:")
        for task_name in DAG_leaf_tasks:
            logger.trace(task_name)
        logger.trace("") 
        logger.trace("DAG_leaf_task_inputs:")
        for inp in DAG_leaf_task_inputs:
            logger.trace(inp)
        logger.trace("")
        logger.trace("DAG_version_number:")
        logger.trace(DAG_info_version_number)
        logger.trace("")
        logger.trace("DAG_info_is_complete:")
        logger.trace(DAG_info_is_complete)
        logger.trace("")

    return  DAG_info
"""
