from .DAG_executor_State import DAG_executor_State
from ..server.api import create, synchronize_async, synchronize_sync 

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

# local wrapper for a DAG_infoBuffer_Monitor. there is also a 
# local wrapper: Local_Client_for_DAG_infoBuffer_Monitor
# This wrapper deals with the keyword argument parameters
# that need to be sent to the DAG_infoBuffer_Monitor
# and the restart value returned by the DAG_infoBuffer_Monitor,
# which is not used. It calls server.api methods to make remote
# calls to the DAG_infoBuffer_Monitor.
#
# rhc: ToDo: Should we close this at end?
class Remote_Client_for_DAG_infoBuffer_Monitor:
    def __init__(self,websocket):
        self.websocket = websocket

    def create(self):
        dummy_state =  DAG_executor_State()
        # name of objects is process_DAG_infoBuffer_Monitor, type is DAG_infoBuffer_Monitor
        create(self.websocket, "create", "DAG_infoBuffer_Monitor", "process_DAG_infoBuffer_Monitor", dummy_state)


    def deposit(self,DAG_info,new_leaf_task_work_tuples):
        # bounded buffer is blocking; using same interface as Manager.Queue
        dummy_state = DAG_executor_State()
        dummy_state.keyword_arguments['new_current_version_DAG_info'] = DAG_info
        dummy_state.keyword_arguments['new_current_version_new_leaf_tasks'] = new_leaf_task_work_tuples
        # name of object is process_DAG_infoBuffer_Monitor, type specified on create
        synchronize_async(self.websocket,"synchronize_async", "process_DAG_infoBuffer_Monitor", "deposit", dummy_state)
 
    def withdraw(self,requested_current_version_number):
        dummy_state = DAG_executor_State()
        dummy_state.keyword_arguments['requested_current_version_number'] = requested_current_version_number
        # name of object is process_DAG_infoBuffer_Monitor, type specified on create
        # This call returns a new DAG_info object that is being 
        # constructed incrementally.
        dummy_state = synchronize_sync(self.websocket,"synchronize_sync", "process_DAG_infoBuffer_Monitor", "withdraw", dummy_state)
        return dummy_state.return_value