import base64
import logging 
import threading 
import time 
import sys
import cloudpickle

from wukong.wukong_problem import WukongProblem, FanInSychronizer, WukongResult, UserProgram

# import wukong.memoization.memoization_controller as memoization_controller

import redis 
import logging
from .constants import REDIS_IP
from logging import handlers
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s')
ch = logging.StreamHandler(sys.stdout)
ch.setFormatter(formatter)
#logger.addHandler(ch)

redis_client = redis.Redis(host = REDIS_IP, port = 6379)

if logger.handlers:
   for handler in logger.handlers:
      handler.setFormatter(formatter)

root = logging.getLogger()
if root.handlers:
    for handler in root.handlers:
       handler.setFormatter(formatter)

debug_lock = threading.Lock() 

def ResetRedis():
    print("Flushing Redis DB now.")
    redis_client.flushdb()
    redis_client.flushall()

class ProblemType(WukongProblem):
	# The threshold at which we switch to a sequential algorithm.
    SEQUENTIAL_THRESHOLD = 2
	
	# Get input arrays when the level reaches the INPUT_THRESHOLD, e.g., don't grab the initial 256MB array,
	# wait until you reach level , say, 1, when there are two subproblems each half as big.
	# To input at the start of the problem (root), use 0; the stack has 0 elements on it for the root problem.
	# To input the first two subProblems, use 1. Etc.
    INPUT_THRESHOLD = 1

    def __init__(self, numbers = [], from_idx = -1, to_idx = -1, value = -1):
        self.numbers = numbers
        self.from_idx = from_idx
        self.to_idx = to_idx
        self.value = value     # just to keep Fibonacci happy.

    def __str__(self):
        return "ProblemType(from=" + str(self.from_idx) + ", to=" + str(self.to_idx) + ", numbers=" + str(self.numbers) + ")"
    
    @property
    def memoize(self):
        return False    

class ResultType(WukongResult):
    def __init__(self, numbers = [], from_idx = -1, to_idx = -1, value = -1):
        self.numbers = numbers
        self.from_idx = from_idx
        self.to_idx = to_idx
        self.value = value     # just to keep Fibonacci happy.

    def copy(self):
        return ResultType(
            value = self.value,
            from_idx = self.from_idx,
            to_idx = self.to_idx,
            numbers = self.numbers)

    def __str__(self):
        return "ResultType(from=" + str(self.from_idx) + ", to=" + str(self.to_idx) + ", numbers=" + str(self.numbers) + ")"

class MergesortProgram(UserProgram):
    pass

NullResult = ResultType(type = -1, value = -1)
StopResult = ResultType(type = 0, value = -1)

def decode_base64(original_data, altchars=b'+/'):
    """Decode base64, padding being optional.

    :param data: Base64 data as an ASCII byte string
    :returns: The decoded byte string.

    """
    # data = re.sub(rb'[^a-zA-Z0-9%s]+' % altchars, b'', original_data)  # normalize
    # missing_padding = len(data) % 4
    # logger.debug("Original data length: " + str(len(original_data)) + ", normalized data length: " + str(len(data)) + ", missing padding: " + str(missing_padding))
    # if missing_padding > 0:
    #     data += b'='* (4 - missing_padding)
    #     logger.debug("Length of data after adjustment: " + str(len(data)))
    # else:
    #     logger.debug("Length of (normalized) data is multiple of 4; no adjustment required.")
    original_data += b'==='
    return base64.b64decode(original_data, altchars)