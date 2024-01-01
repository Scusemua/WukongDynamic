import logging
logger = logging.getLogger(__name__)

"""
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(asctime)s] [%(module)s] [%(processName)s] [%(threadName)s]: %(message)s')
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)
logger.addHandler(ch)
"""

# Will be a class

pagerank_outputs = {}

def set_pagerank_output(state,output):
    multiple_output = pagerank_outputs.get(state)
    if not multiple_output == None:
        logger.error("[Error]: Internal Error: set_pagerank_output:"
            + " more than one pagerank output for state " + str(state))
    pagerank_outputs[state] = output

def get_pagerank_outputs():
    return pagerank_outputs

def verify_pagerank_outputs(number_of_groups_or_partitions):
    if len(pagerank_outputs) != number_of_groups_or_partitions:
        logger.error("[Error]: Internal Error: verify_pagerank_outputs:"
            + " missing pagerank output, number of outputs is " + str(len(pagerank_outputs))) 
        return False
    else:
        logger.info("")
        logger.info("")
        logger.info("BFS: verify_pagerank_outputs: outputs verified")
        return True
 
