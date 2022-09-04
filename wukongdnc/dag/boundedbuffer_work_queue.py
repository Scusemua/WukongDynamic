import websocket
from .DAG_executor_State import DAG_executor_State
from ..server.api import create, synchronize_async, synchronize_sync 

class BoundedBuffer_Work_Queue:
    def __init__(self,websocket, n):
        self.websocket = websocket
        self.n = n

    def create(self):
        state =  DAG_executor_State(            
            keyword_arguments = {
                'n': self.n
            }
        )
        create(websocket, "create", "BoundedBuffer", "process_work_queue", state)

    def get(self,DAG_executor_state, block = True):
        # bounded buffer is blocking; using same interface as Manager.Queue
        DAG_executor_state = synchronize_sync(self.websocket,"synchronize_sync", "process_work_queue", "deposit", DAG_executor_state)
        return DAG_executor_state

    def put(self,DAG_executor_state, value):
        DAG_executor_state.keyword_arguments['value'] = value
        # ToDo: call deposit_no_try() which is no restart?
        # Or deposit_no_restarts()
        synchronize_async(self.websocket,"synchronize_async", "process_work_queue", "deposit", DAG_executor_state)

    def put_all(self,DAG_executor_state, list_of_values):
        DAG_executor_state.keyword_arguments['value'] = list_of_values
        synchronize_async(self.websocket,"synchronize_async", "process_work_queue", "deposit", DAG_executor_state)


