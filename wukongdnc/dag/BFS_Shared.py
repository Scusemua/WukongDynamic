import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
#logger.setLevel(logging.INFO)
formatter = logging.Formatter('[%(asctime)s] [%(threadName)s] %(levelname)s: %(message)s')
#formatter = logging.Formatter('%(levelname)s: %(message)s')
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
#ch.setLevel(logging.INFO)
ch.setFormatter(formatter)
logger.addHandler(ch)

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

