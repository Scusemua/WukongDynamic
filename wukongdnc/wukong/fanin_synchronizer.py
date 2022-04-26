import sys 

import threading 
import logging
from logging import handlers
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s')
ch = logging.StreamHandler(sys.stdout)
ch.setFormatter(formatter)

class FanInSychronizer(object):
    lock = threading.Lock()

    resultMap = dict()
    memoizationMap = dict()

    def __init__(self):
        pass 
 
    @staticmethod
    def debug_print_maps():
        with FanInSychronizer.lock:
            logger.debug("=-=-=-= RESULT MAP =-=-=-=")

            for k,v in FanInSychronizer.resultMap.items():
                logger.debug("\t%s: %s" % (str(k), str(v)))
            
            logger.debug("")
            logger.debug("=-=-=-= MEMOIZATION MAP =-=-=-=")
            
            for k,v in FanInSychronizer.memoizationMap.items():
                logger.debug("\t%s: %s" % (str(k), str(v)))

    @staticmethod
    def put(fanin_id : str, result):
        """
        If nothing already in the map, return None.
        If something already in the map, return whatever was already in the map.        

        Get and put atomically.

        Put memoized copy of result.
        """
        copy_of_result = result.copy()
        copy_of_result.problem_id = result.problem_id 

        copy_of_return = None 

        with FanInSychronizer.lock:
            # Attempt to grab the existing value.
            copy_of_return = None 

            if fanin_id in FanInSychronizer.memoizationMap:
                copy_of_return = FanInSychronizer.memoizationMap[fanin_id]

                # If the existing value is not none, copy it.
                copy_of_return = copy_of_return.copy() 

                # Update the problem_id field.
                copy_of_return.problem_id = FanInSychronizer.memoizationMap[fanin_id].problem_id                

            # Now we update the map itself.
            FanInSychronizer.memoizationMap[fanin_id] = result
        
        return copy_of_return

    @staticmethod
    def get(fanin_id : str):
        copy = None 
        
        with FanInSychronizer.lock:
            result = None 
            
            if fanin_id in FanInSychronizer.memoizationMap:
                result = FanInSychronizer.memoizationMap[fanin_id]

            if result is not None:
                copy = result.copy() 
                copy.problem_id = result.problem_id
        
        return copy 