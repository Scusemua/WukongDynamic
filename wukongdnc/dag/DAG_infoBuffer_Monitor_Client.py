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

class DAG_infoBuffer_Monitor_Client:
    def __init__(self,websocket):
        self.websocket = websocket

    def create(self):
        dummy_state =  DAG_executor_State()
        # name of objects is process_DAG_infoBuffer_Monitor, type is DAG_infoBuffer_Monitor
        create(self.websocket, "create", "DAG_infoBuffer_Monitor", "process_DAG_infoBuffer_Monitor", dummy_state)

    def deposit(self,DAG_info):
        # bounded buffer is blocking; using same interface as Manager.Queue
        dummy_state = DAG_executor_State()
        dummy_state.keyword_arguments['new_current_version_DAG_info'] = DAG_info
        # name of object is process_DAG_infoBuffer_Monitor, type specified on create
        synchronize_async(self.websocket,"synchronize_async", "process_DAG_infoBuffer_Monitor", "deposit", dummy_state)
 
    def withdraw(self,requested_current_version_number):
        dummy_state = DAG_executor_State()
        dummy_state.keyword_arguments['requested_current_version_number'] = requested_current_version_number
        # name of object is process_DAG_infoBuffer_Monitor, type specified on create
        dummy_state = synchronize_sync(self.websocket,"synchronize_sync", "process_DAG_infoBuffer_Monitor", "withdraw", dummy_state)
        return dummy_state.return_value