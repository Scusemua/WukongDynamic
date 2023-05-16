import logging
import cloudpickle
from .DAG_info import DAG_Info
from .DFS_visit import state_info
from .BFS_pagerank import PageRank_Function_Driver, PageRank_Function_Driver_Shared
from .BFS_Shared import PageRank_Function_Driver_Shared_Fast
from .DAG_executor_constants import use_shared_partitions_groups, use_page_rank_group_partitions
from .DAG_executor_constants import use_struct_of_arrays_for_pagerank

logger = logging.getLogger(__name__)

logger.setLevel(logging.DEBUG)
#logger.setLevel(logging.INFO)
formatter = logging.Formatter('[%(asctime)s] [%(module)s] [%(processName)s] [%(threadName)s]: %(message)s')
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
#ch.setLevel(logging.INFO)
ch.setFormatter(formatter)
logger.addHandler(ch)


#For DAG generation, map sending task to list of Reveiving tasks, and 
# map receiving task to list of Sending tasks.
Partition_senders = {}
Partition_receivers = {}
Group_senders = {}
Group_receivers = {}

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

    """ Add L to end of names
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

#rhc: ToDo: use the loop map to change the nambes in the 
# Partition/roup senders and receivers.
#dictionary[new_key] = dictionary[old_key]
#del dictionary[old_key]

    # sink nodes, i.e., nodes that do not send any inputs
    Partition_sink_set = set()
    logger.info("Partition DAG:")
    state = 1
    # partition i has a collapse to partition i+1
    # Task senderX sends inputs to one or more other tasks
    for senderX in Partition_senders:
        fanouts = []
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
            if receiver_set_for_receiverY == None:
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
                    logger.info("sender " + senderX + " --> " + receiverY + " : Collapse")
                    if not receiverY in Partition_all_collapse_task_names:
                        Partition_all_collapse_task_names.append(receiverY)
                    else:
                        pass # error only one task can collapse a given task
                    collapse.append(receiverY)
                else:
                    # only one task sends input to receiverY and this sending 
                    # task sends to other tasks too, so senderX does a fanout 
                    # to receiverY         
                    logger.info("sender " + senderX + " --> " + receiverY + " : Fanout")
                    if not receiverY in Partition_all_fanout_task_names:
                        Partition_all_fanout_task_names.append(receiverY)
                    fanouts.append(receiverY)
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
                    logger.info("sender " + senderX + " --> " + receiverY + " : FaninNB")
                    if not receiverY in Partition_all_faninNB_task_names:
                        Partition_all_faninNB_task_names.append(receiverY)
                        Partition_all_faninNB_sizes.append(length_of_sender_set_for_receiverY)
                    faninNBs.append(receiverY)
                    faninNB_sizes.append(length_of_sender_set_for_receiverY)
                else:
                    # senderX sends an input only to receiverY, same for any other
                    # tasks that sends inputs to receiverY so receiverY is a fanin task.
                    logger.info("sender " + senderX + " --> " + receiverY + " : Fanin")
                    if not receiverY in Partition_all_fanin_task_names:
                        Partition_all_fanin_task_names.append(receiverY)
                        Partition_all_fanin_sizes.append(length_of_sender_set_for_receiverY)
                    fanins.append(receiverY)
                    fanin_sizes.append(length_of_sender_set_for_receiverY)

        # get the tasks that send to senderX, i.e., provide inputs for senderX
        sender_set_for_senderX = Partition_receivers.get(senderX)
        if sender_set_for_senderX == None:
            # senderX is a leaf task since it is not a receiver
            Partition_DAG_leaf_tasks.append(senderX)
            Partition_DAG_leaf_task_start_states.append(state)
            task_inputs = ()
            Partition_DAG_leaf_task_inputs.append(task_inputs)
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
        Partition_DAG_map[state] = state_info(senderX, fanouts, fanins, faninNBs, collapse, fanin_sizes, faninNB_sizes, task_inputs)
        Partition_DAG_states[senderX] = state

        state += 1

    # Finish by doing the receivers that are not senders (opposite of leaf tasks);
    # these are reeivers tht send no nputs to other tasks. They have no fanins/
    # faninBs, fanouts or collapses, but they do have task inputs.
    for receiverY in Partition_sink_set: # Partition_receivers:
        #if not receiverY in Partition_DAG_states:
        fanouts = []
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

    logger.info("")
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

    logger.info("DAG_map:")
    for key, value in Partition_DAG_map.items():
        logger.info(str(key) + ' : ' + str(value))
    logger.info("")
    logger.info("states:")        
    for key, value in Partition_DAG_states.items():
        logger.info(str(key) + ' : ' + str(value))
    logger.info("")
    logger.info("num_fanins:" + str(num_fanins) + " num_fanouts:" + str(num_fanouts) + " num_faninNBs:"
    + str(num_faninNBs) + " num_collapse:" + str(num_collapse))
    logger.info("")  
    logger.info("Partition_all_fanout_task_names:")
    for name in Partition_all_fanout_task_names:
        logger.info(name)
    logger.info
    logger.info("all_fanin_task_names:")
    for name in Partition_all_fanin_task_names :
        logger.info(name)
    logger.info("")
    logger.info("all_fanin_sizes:")
    for s in Partition_all_fanin_sizes :
        logger.info(s)
    logger.info("")
    logger.info("all_faninNB_task_names:")
    for name in Partition_all_faninNB_task_names:
        logger.info(name)
    logger.info("")
    logger.info("all_faninNB_sizes:")
    for s in Partition_all_faninNB_sizes:
        logger.info(s)
    logger.info("")
    logger.info("Partition_all_collapse_task_names:")
    for name in Partition_all_collapse_task_names:
        logger.info(name)
    logger.info("")
    logger.info("leaf task start states:")
    for start_state in Partition_DAG_leaf_task_start_states:
        logger.info(start_state)
    logger.info("")
    logger.info("DAG_tasks:")
    for key, value in Partition_DAG_tasks.items():
        logger.info(str(key) + ' : ' + str(value))
    logger.info("")
    logger.info("DAG_leaf_tasks:")
    for task_name in Partition_DAG_leaf_tasks:
        logger.info(task_name)
    logger.info("")
    logger.info("DAG_leaf_task_inputs:")
    for inp in Partition_DAG_leaf_task_inputs:
        logger.info(inp)
    logger.info("")

    DAG_info_partition_read = DAG_Info(file_name = "./DAG_info_Partition.pickle")
    
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

    logger.info("")
    logger.info("DAG_info partition after read:")
    output_DAG = True
    # add-0bec4d19-bce6-4394-ad62-9b0eab3081a9
    if output_DAG:
        # FYI:
        logger.info("DAG_map:")
        for key, value in DAG_map.items():
            logger.info(str(key) + ' : ' + str(value))
            #logger.info(key)
            #logger.info(value)
        logger.info("  ")
        logger.info("DAG states:")      
        for key, value in DAG_states.items():
            logger.info(str(key) + ' : ' + str(value))
        logger.info("   ")
        logger.info("DAG leaf task start states")
        for start_state in DAG_leaf_task_start_states:
            logger.info(start_state)
        logger.info("")
        logger.info("DAG_tasks:")
        for key, value in DAG_tasks.items():
            logger.info(str(key) + ' : ' + str(value))
        logger.info("")
        logger.info("DAG_leaf_tasks:")
        for task_name in DAG_leaf_tasks:
            logger.info(task_name)
        logger.info("") 
        logger.info("DAG_leaf_task_inputs:")
        for inp in DAG_leaf_task_inputs:
            logger.info(inp)
        #logger.info("") 
        logger.info("")

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

#rhc: ToDo: use the loop map to change the nambes in the 
# Partition/roup senders and receivers.
#dictionary[new_key] = dictionary[old_key]
#del dictionary[old_key]

    # sink nodes, i.e., nodes that do not send any inputs
    Group_sink_set = set()
    logger.info("Group DAG:")
    state = 1
    for senderX in Group_senders:
        logger.info("senderX: " + senderX)
        fanouts = []
        fanins = []
        faninNBs = []
        collapse = []
        fanin_sizes = []
        faninNB_sizes = []
        receiver_set_for_senderX = Group_senders[senderX]
        for receiverY in receiver_set_for_senderX:
            receiver_set_for_receiverY = Group_senders.get(receiverY)
            if receiver_set_for_receiverY == None:
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
                    logger.info("sender " + senderX + " --> " + receiverY + " : Collapse")
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
                    logger.info("sender " + senderX + " --> " + receiverY + " : FaninNB")
                    if not receiverY in Group_all_faninNB_task_names:
                        Group_all_faninNB_task_names.append(receiverY)
                        Group_all_faninNB_sizes.append(length_of_sender_set_for_receiverY)
                    logger.info ("after Group_all_faninNBs_sizes append: " + str(Group_all_faninNB_sizes))
                    logger.info ("faninNBs append: " + receiverY)
                    faninNBs.append(receiverY)
                    faninNB_sizes.append(length_of_sender_set_for_receiverY)
                else:
                    # senderX sends an input only to receiverY, same for any other
                    # tasks that sends inputs to receiverY so receiverY is a fanin task.
                    logger.info("sender " + senderX + " --> " + receiverY + " : Fanin")
                    if not receiverY in Group_all_fanin_task_names:
                        Group_all_fanin_task_names.append(receiverY)
                        Group_all_fanin_sizes.append(length_of_sender_set_for_receiverY)
                    fanins.append(receiverY)
                    fanin_sizes.append(length_of_sender_set_for_receiverY)

        # get the tasks that send to senderX, i.e., provide inputs for senderX
        sender_set_for_senderX = Group_receivers.get(senderX)
        if sender_set_for_senderX == None:
            # senderX is a leaf task since it is not a receiver
            Group_DAG_leaf_tasks.append(senderX)
            Group_DAG_leaf_task_start_states.append(state)
            task_inputs = ()
            Group_DAG_leaf_task_inputs.append(task_inputs)
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
        Group_DAG_map[state] = state_info(senderX, fanouts, fanins, faninNBs, collapse, fanin_sizes, faninNB_sizes, task_inputs)
        Group_DAG_states[senderX] = state

        state += 1

    # Finish by doing the receivers that are not senders (opposite of leaf tasks);
    # these are reeivers tht send no nputs to other tasks. They have no fanins/
    # faninBs, fanouts or collapses, but they do have task inputs.
    for receiverY in Group_sink_set: # Partition_receivers:
        #if not receiverY in Partition_DAG_states:
            fanouts = []
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

            Group_DAG_map[state] = state_info(receiverY, fanouts, fanins, faninNBs, collapse, fanin_sizes, faninNB_sizes, task_inputs)
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

    logger.info("")
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

    logger.info("GroupDAG_map:")
    for key, value in Group_DAG_map.items():
        logger.info(str(key) + ' : ' + str(value))
    logger.info("")
    logger.info("states:")        
    for key, value in Group_DAG_states.items():
        logger.info(str(key) + ' : ' + str(value))
    logger.info("")
    logger.info("num_fanins:" + str(num_fanins) + " num_fanouts:" + str(num_fanouts) + " num_faninNBs:"
    + str(num_faninNBs) + " num_collapse:" + str(num_collapse))
    logger.info("")  
    logger.info("all_fanout_task_names:")
    for name in Group_all_fanout_task_names:
        logger.info(name)
    logger.info("")
    logger.info("all_fanin_task_names:")
    for name in Group_all_fanin_task_names :
        logger.info(name)
    logger.info("")
    logger.info("all_fanin_sizes:")
    for s in Group_all_fanin_sizes :
        logger.info(s)
    logger.info("")
    logger.info("all_faninNB_task_names:")
    for name in Group_all_faninNB_task_names:
        logger.info(name)
    logger.info("")
    logger.info("all_faninNB_sizes:")
    for s in Group_all_faninNB_sizes :
        logger.info(s)
    logger.info("")
    logger.info("all_collapse_task_names:")
    for name in Group_all_collapse_task_names:
        logger.info(name)
    logger.info("")
    logger.info("leaf task start states:")
    for start_state in Group_DAG_leaf_task_start_states:
        logger.info(start_state)
    logger.info("")
    logger.info("DAG_tasks:")
    for key, value in Group_DAG_tasks.items():
        logger.info(str(key) + ' : ' + str(value))
    logger.info("")
    logger.info("DAG_leaf_tasks:")
    for task_name in Group_DAG_leaf_tasks:
        logger.info(task_name)
    logger.info("")
    logger.info("DAG_leaf_task_inputs:")
    for inp in Group_DAG_leaf_task_inputs:
        logger.info(inp)
    logger.info("")

    DAG_info_partition_read = DAG_Info(file_name = "./DAG_info_Group.pickle")
    
    DAG_map = DAG_info_partition_read.get_DAG_map()
    #all_fanin_task_names = DAG_info_partition_read.get_all_fanin_task_names()
    #all_fanin_sizes = DAG_info_partition_read.get_all_fanin_sizes()
    #all_faninNB_task_names = DAG_info_partition_read.get_all_faninNB_task_names()
    #all_faninNB_sizes = DAG_info_partition_read.get_all_faninNB_sizes()
    #all_fanout_task_names = DAG_info_partition_read.get_all_fanout_task_names()
    # Note: all fanout_sizes is not needed since fanouts are fanins that have size 1
    DAG_states = DAG_info_partition_read.get_DAG_states()
    DAG_leaf_tasks = DAG_info_partition_read.get_DAG_leaf_tasks()
    DAG_leaf_task_start_states = DAG_info_partition_read.get_DAG_leaf_task_start_states()
    DAG_tasks = DAG_info_partition_read.get_DAG_tasks()

    DAG_leaf_task_inputs = DAG_info_partition_read.get_DAG_leaf_task_inputs()

    logger.info("")
    logger.info("DAG_info group after read:")
    output_DAG = True
    # add-0bec4d19-bce6-4394-ad62-9b0eab3081a9
    if output_DAG:
        # FYI:
        logger.info("DAG_map:")
        for key, value in DAG_map.items():
            print_str = ""
            print_str = str(key) + " " + str(value)
            logger.info(print_str)
            #logger.info(key)
            #logger.info(value)
        logger.info("  ")
        logger.info("DAG states:")         
        for key, value in DAG_states.items():
            print_str = ""
            print_str = str(key) + " " + str(value)
            logger.info(print_str)
            #logger.info(key)
            #logger.info(value)
        logger.info("   ")
        logger.info("DAG leaf task start states")
        for start_state in DAG_leaf_task_start_states:
            logger.info(start_state)
        logger.info("")
        logger.info("DAG_tasks:")
        for key, value in DAG_tasks.items():
            logger.info(str(key) + ' : ' + str(value))
        logger.info("")
        logger.info("DAG_leaf_tasks:")
        for task_name in DAG_leaf_tasks:
            logger.info(task_name)
        logger.info("") 
        logger.info("DAG_leaf_task_inputs:")
        for inp in DAG_leaf_task_inputs:
            logger.info(inp)
        #logger.info("") 
        logger.info("")

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

    logger.debug("")
    
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
        logger.debug("receiverX:" + receiverX)
        sender_set_for_receiverX = Partition_receivers.get(receiverX)
        logger.debug("sender_set_for_receiverX:" + str(sender_set_for_receiverX))
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

    logger.debug("dsk lines for Partitions:")
    for line in dsk_lines:
        logger.debug(line)

    logger.debug("")
    logger.debug("")
    dsk_lines = []
    dsk_lines.append(header_line)
    for leaf_task in Group_DAG_leaf_tasks:
        leaf_line = "\t\t\t" + "\'" + str(leaf_task) + "\': (" + driver + "),"
        dsk_lines.append(leaf_line)

    for receiverX in Group_receivers:
        logger.debug("receiverX:" + receiverX)
        sender_set_for_receiverX = Group_receivers.get(receiverX)
        logger.debug("sender_set_for_receiverX:" + str(sender_set_for_receiverX))
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

    logger.debug("dsk lines for Groups:")
    for line in dsk_lines:
        logger.debug(line)
