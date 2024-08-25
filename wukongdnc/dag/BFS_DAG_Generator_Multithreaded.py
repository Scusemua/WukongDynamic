import logging
import cloudpickle
import threading
import queue

from . import BFS_generate_DAG_info_incremental_partitions
from . import BFS_generate_DAG_info_incremental_groups
#from .DAG_executor_constants import USE_PAGERANK_GROUPS_PARTITIONS
from . import DAG_executor_constants

logger = logging.getLogger(__name__)

"""
For *non-incremental* DAG generation, instead of generating the
DAG_info at the end of bfs(), bfs deposits the next partition
or group it collects in a buffer and a DAG generator thread 
withdraws the partition or group and (calls the usual methods to) adds it to DAG_info. 
So we are overlapping the execution of bfs(), which builds the 
partitions/groups, with DAG_info generation by the generator_thread
and incremental DAG generation methods.
Here we are essentially multithreading  non-incremental bfs() - one thread to 
identify the partitions/groups and one thread to build the 
DAG of partitions/groups. This is opposed to Incremental DAG 
generation, which overlaps the *execution* of the DAG by 
DAG_executor with the building of the DAG by bfs.
Consider: Combining this: incremental DAG generation, which
overlaps DAG execution by DAG_executor with DAG generation 
by bfs(), where bfs() is multithreaded: bfs indentifies the 
next partition/group and deposits it into buffer to be 
withdrawn by the DAG generator thread and used to incremeentally
extend the DAG. That is, the new version of the incremental DAG would
be made available for eecution as usual. (For non-incremental ADG generation,
the ADG is made available for execution only after the entire DAG has been generated.
Note: The DAG generator thread would have to 
deposit the incremental DAG into the buffer from which incremental 
DAGs are withdrawn by the DAG_excutor threads/processes or where
lambdas are started to execute the new tasks added to the incremental DAG.

For incremental and multithreaded DAG generation we will delete
some of the info that has been maintained for DAG generation 
while we go, i.e., as infomation is no longer needed since the related
partitions/groups have alrady been added to the DAG, has delete it.
If we generate the DAG at the end (for non-incremental DAG generation), 
then all of the maintained information is needed at the end, which might 
be a lot of information.
So incremental DAG generation allows DAG information to be deleted on-the-fly.
Also, the input of the graph whose pagerank we are calculating can be streamed
(on-the-fly) if the nodes/edges in the graph are stored in the graph representation
in DAG order, i.e., in the order that they aer needed by bfs for DAG generation.

"""

def generator_thread(DAG_generator_for_multithreaded_DAG_generation,buffer):
    # whwn the generated thread is created/started it receives the 
    # self reference of the DAG_Generator_Multithreaded that created it.
    # buffer is where it withdraws the partitions/groups deposited 
    # by bfs().

    thread_name = threading.current_thread().name # for debugging

    while(True):
        DAG_info = None
        # get the next partition/group from the FIFO buffer
        # During incremental DAG generation, bfs() calls
        # BFS_generate_DAG_info_incremental_groups.generate_DAG_info_incremental_groups
        # and passes 5 parameters. In our case, bfs() packs these 5 
        # parameters into a tuple and deposits the tuple into buffer
        # from which the generator_thread withdrws the tuple. The 
        # generator thread withdraws the tuple, unpacks it, and passes
        # the parameters to DAG_generator_for_multithreaded_DAG_generation.generate_DAG_info_multithreaded_groups
        # whcih simply passes the paramaters on to 
        # BFS_generate_DAG_info_incremental_groups.generate_DAG_info_incremental_groups
        # or the version for partitions.
        #
        # See the comment on deposit() below.
        next_partition_or_group = buffer.get()
        logger.info(thread_name + "generator_thread: called get."
            + " USE_PAGERANK_GROUPS_PARTITIONS: " + str(DAG_executor_constants.USE_PAGERANK_GROUPS_PARTITIONS))

        if DAG_executor_constants.USE_PAGERANK_GROUPS_PARTITIONS:
            # tuple was created as:
            # group_tuple = (partition_name,current_partition_number,
            # copy_of_groups_of_current_partition,copy_of_groups_of_partitions, to_be_continued)

            # passing the partition/group information in a tuple
            current_partition_name = next_partition_or_group[0]
            current_partition_number =  next_partition_or_group[1]
            groups_of_current_partition =  next_partition_or_group[2]
            groups_of_partitions = next_partition_or_group[3]
            to_be_continued = next_partition_or_group[4]
#brc: use of DAG_info:
            num_incremental_DAGs_generated_since_base_DAG = next_partition_or_group[5]
            #logger.info("generator_thread: calling generate.")

            # This will simply pass the parameters in the tuple to the 
            # same mBFS_generate_DAG_info_incremental_groups.generate_DAG_info_incremental_groupsethod 
            # that is used during incremental DAG generation. That is,
            # during incremental DAG generation, bfs() calls
            # BFS_generate_DAG_info_incremental_groups.generate_DAG_info_incremental_groups
            # to add the next partition/group to the DAG. The new
            # incremental DAG is returned to BFS and BFS deposits the 
            # DAG into a buffer where the DAG_executor can withdraw the
            # new incremental DAG. (bfs does not necessarily deposit every
            # incremental ADG it gets - it published every ith incremental
            # DAG where i is selected by the user.) So we are using
            # the same BFS_generate_DAG_info_incremental_groups.generate_DAG_info_incremental_groups
            # to add a partition/group to the DAG. We also get the new
            # DAG_info returned to us. If there are no more partitions/groups
            # to be added to the DAG then we have built the complete DAG so
            # we save the DAG_info to a file. When we run DAG_executor_driver
            # it will read DAG_info and sart DAG execution.
            DAG_info = DAG_generator_for_multithreaded_DAG_generation.generate_DAG_info_multithreaded_groups(current_partition_name,current_partition_number,
                groups_of_current_partition,groups_of_partitions,to_be_continued,
#brc: use of DAG_info:
                num_incremental_DAGs_generated_since_base_DAG)
        else:
            # tuple was created as:
            # partition_tuple = (partition_name, current_partition_number,to_be_continued)
            current_partition_name = next_partition_or_group[0]
            current_partition_number =  next_partition_or_group[1]
            to_be_continued = next_partition_or_group[2]
            num_incremental_DAGs_generated_since_base_DAG = next_partition_or_group[3]
            DAG_info = DAG_generator_for_multithreaded_DAG_generation.generate_DAG_info_multithreaded_partitions(current_partition_name,
                current_partition_number, to_be_continued,
#brc: use of DAG_info:
                num_incremental_DAGs_generated_since_base_DAG)

        if DAG_info.get_DAG_info_is_complete():
            # we are done with DAG generation
            break

    # save the DAG_info in a file to be read by DAG_execution_driver.
    file_name = "./DAG_info.pickle"                  
    DAG_info_dictionary = DAG_info.get_DAG_info_dictionary()
    with open(file_name, 'wb') as handle:
        cloudpickle.dump(DAG_info_dictionary, handle) #, protocol=pickle.HIGHEST_PROTOCOL)  

# manages multithreaded DAG generation
class DAG_Generator_Multithreaded:
    def __init__(self,num_nodes):
        # bfs deposits the net partitions/groups and the generator 
        # thread withdraws them
        self.buffer = queue.Queue()
        self.num_nodes = num_nodes # number of nodes in input graph
        # this thrad needs a reference to seld so it can access buffer
        self.dag_generator_thread = threading.Thread(target=generator_thread, name=("dag_generator_thread"), args=(self,self.buffer,))

    # passes the info about the next partition/group to the 
    # method that adds the partition/group to the DAG_info.
    # Called by the generator thread:
    #   DAG_info = DAG_generator_for_multithreaded_DAG_generation.generate_DAG_info_multithreaded_groups
    def generate_DAG_info_multithreaded_groups(self,current_partition_name,
        current_partition_number, groups_of_current_partition,
        groups_of_partitions,
        to_be_continued,
#brc: use of DAG_info:
        num_incremental_DAGs_generated_since_base_DAG):


        DAG_info = BFS_generate_DAG_info_incremental_groups.generate_DAG_info_incremental_groups(current_partition_name,
            current_partition_number, groups_of_current_partition,
            groups_of_partitions,
            to_be_continued,
#brc: use of DAG_info:
            num_incremental_DAGs_generated_since_base_DAG)
        
        logger.trace("generate_DAG_info_multithreaded_groups: returned DAG_info:")
        DAG_map = DAG_info.get_DAG_map()
        DAG_states = DAG_info.get_DAG_states()
        DAG_leaf_tasks = DAG_info.get_DAG_leaf_tasks()
        DAG_leaf_task_start_states = DAG_info.get_DAG_leaf_task_start_states()
        DAG_version_number = DAG_info.get_DAG_version_number()
        DAG_is_complete = DAG_info.get_DAG_info_is_complete()
        DAG_number_of_tasks = DAG_info.get_DAG_number_of_tasks()
        DAG_num_nodes_in_graph = DAG_info.get_DAG_num_nodes_in_graph()
        # FYI:
        # This is for non-incremental DAG generation. So we are not iterating
        # over the DAG_map while we are concurrently modifyng the map as we 
        # might do if we are using incremental DAG generation.
        logger.info("DAG_executor_driver: DAG_map:")
        for key, value in DAG_map.items():
            logger.info(str(key))
            logger.info(str(value))
        logger.info("  ")
        logger.info("DAG_executor_driver: DAG states:")         
        for key, value in DAG_states.items():
            logger.info(str(key))
            logger.info(str(value))
        logger.info("   ")
        logger.info("DAG_executor_driver: DAG leaf task start states")
        for start_state in DAG_leaf_task_start_states:
            logger.info(str(start_state))
        logger.info("")
        logger.info("DAG_executor_driver: DAG_leaf_tasks:")
        for task_name in DAG_leaf_tasks:
            logger.info(task_name)
        logger.info("") 
        logger.info("'")
        logger.info("DAG_version: " + str(DAG_version_number))
        logger.info("")
        logger.info("DAG_is_complete: " + str(DAG_is_complete))
        logger.info("")
        logger.info("DAG_number_of_tasks: " + str(DAG_number_of_tasks))
        logger.info("")
        logger.info("DAG_num_nodes_in_graph: " + str(DAG_num_nodes_in_graph))
        logger.info("")
        
        return DAG_info
    
    # see comment for group version above
    def generate_DAG_info_multithreaded_partitions(self,
            current_partition_name,current_partition_number,to_be_continued,
#brc: use of DAG_info:
        num_incremental_DAGs_generated_since_base_DAG):
        
        # Note: This is non-incremental testing so we are not deallocating DAG structures on the fly.
        DAG_info = BFS_generate_DAG_info_incremental_partitions.generate_DAG_info_incremental_partitions(current_partition_name,current_partition_number,to_be_continued,
#brc: use of DAG_info:
            num_incremental_DAGs_generated_since_base_DAG)
                                                                                                        
        return DAG_info

    # called by bfs() to make the nexe partition/group available
    # to the generator_thread.
    # In bfs: 
    #   global DAG_generator_for_multithreaded_DAG_generation
    #   partition_tuple = (partition_name, current_partition_number,to_be_continued)
    #   DAG_info = DAG_generator_for_multithreaded_DAG_generation.deposit(partition_tuple)                    
    def deposit(self,next_partition_or_group):
        self.buffer.put(next_partition_or_group)
    # In the BFS.py main method, we create a DAG_Generator_Multithreaded
    # object which creates a thread in the above __init__ method
    # and we call start_thread on this object to start the generator thread.
    # In bfs: 
    #    global DAG_generator_for_multithreaded_DAG_generation
    #    DAG_generator_for_multithreaded_DAG_generation = DAG_Generator_Multithreaded()
    #    DAG_generator_for_multithreaded_DAG_generation.start_thread()
    
    def start_thread(self):
        self.dag_generator_thread.start()
    # at the end of bfs(), it excutes join_thread on the DAG_Generator_Multithreaded
    # object to join the generator_thread. At that point the DAG_info
    # has been saved to file and the DAG_executor run() method can
    # be called to execute the DAG.
    # In bfs(): DAG_generator_for_multithreaded_DAG_generation.join_thread()
    def join_thread(self):
        self.dag_generator_thread.join()