import os
from wukongdnc.dag.DAG_executor_constants import exit_program_on_exception

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
    try:
        msg = "[Error]: set_pagerank_output:" \
            + " more than one pagerank output for state " + str(state)
        assert multiple_output is None , msg
    except AssertionError:
        logger.exception("[Error]: assertion failed")
        if exit_program_on_exception:
            logging.shutdown()
            os._exit(0)
    #assertOld:
    #if multiple_output is not None:
    #    logger.error("[Error]: set_pagerank_output:"
    #        + " more than one pagerank output for state " + str(state))

    pagerank_outputs[state] = output

def get_pagerank_outputs():
    return pagerank_outputs

def verify_pagerank_outputs(number_of_groups_or_partitions):
    if len(pagerank_outputs) != number_of_groups_or_partitions:
        logger.error("******************")
        logger.error("[Error]: DAG_executor_driver: AssertionError: verify_pagerank_outputs:"
            + " missing pagerank output, number of outputs is " + str(len(pagerank_outputs))) 
        logger.error("******************")
        return False
    else:
        logger.info("")
        logger.info("")
        logger.info("DAG_executor_driver: verify_pagerank_outputs all outputs generated: outputs verified")
        return True
 
