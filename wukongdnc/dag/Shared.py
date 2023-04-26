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

    shared_partition = []
    shared_groups = []
    # maps partition "P" to its position/size in shared_partition/shared_groups
    shared_partition_map = {}
    shared_groups_map = {}

