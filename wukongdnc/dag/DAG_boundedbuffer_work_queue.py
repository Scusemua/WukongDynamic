from .DAG_executor_State import DAG_executor_State
from ..server.api import create, synchronize_async, synchronize_sync 
#from .DAG_executor_constants import PROCESS_WORK_QUEUE_TYPE
from . import DAG_executor_constants

#from .DAG_executor_constants import USING_THREADS_NOT_PROCESSES, USE_MULTITHREADED_MULTIPROCESSING

import logging 
logger = logging.getLogger(__name__)

"""
if not (not USING_THREADS_NOT_PROCESSES or USE_MULTITHREADED_MULTIPROCESSING):
    logger.setLevel(logging.ERROR)
    formatter = logging.Formatter('[%(asctime)s] [%(module)s] [%(processName)s] [%(threadName)s]: %(message)s')
    ch = logging.StreamHandler()
    ch.setLevel(logging.ERROR)
    ch.setFormatter(formatter)
    logger.addHandler(ch)
"""

#brc: ToDo: Should we close this at end?
class Work_Queue_Client:
    def __init__(self,websocket, n):
        self.websocket = websocket
        self.n = n

    def create(self):
        state =  DAG_executor_State(            
            keyword_arguments = {
                'n': self.n
            }
        )
        
        #create(self.websocket, "create", "BoundedBuffer", "process_work_queue", state)
        create(self.websocket, "create", DAG_executor_constants.PROCESS_WORK_QUEUE_TYPE, "process_work_queue", state)

    def get(self,block = True):
        # bounded buffer is blocking; using same interface as Manager.Queue
        dummy_state = DAG_executor_State()
        dummy_state = synchronize_sync(self.websocket,"synchronize_sync", "process_work_queue", "withdraw", dummy_state)
        return dummy_state.return_value

    def get_my_half(self,block = True):
        dummy_state = DAG_executor_State()
        dummy_state = synchronize_sync(self.websocket,"synchronize_sync", "process_work_queue", "withdraw_half", dummy_state)
        return dummy_state.return_value

    def put(self,value):
        dummy_state = DAG_executor_State()
        dummy_state.keyword_arguments['value'] = value
        # ToDo: call deposit_no_try() which is no restart?
        # Or deposit_no_restarts()
        synchronize_async(self.websocket,"synchronize_async", "process_work_queue", "deposit", dummy_state)

    def put_all(self,list_of_values):
        dummy_state = DAG_executor_State()
        dummy_state.keyword_arguments['list_of_values'] = list_of_values
        synchronize_async(self.websocket,"synchronize_async", "process_work_queue", "deposit_all", dummy_state)


