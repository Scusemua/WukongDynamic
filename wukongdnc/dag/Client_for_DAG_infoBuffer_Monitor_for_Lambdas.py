#brc continue 
# self.DAG_infobuffer_monitor is a DAG_infoBuffer_Monitor
#from ..server import DAG_infoBuffer_Monitor
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

# Local wrapper for a DAG_infoBuffer_Monitor. there is also a 
# remote wrapper: Remote_Client_for_DAG_infoBuffer_Monitor
# This wrapper deals with the keword argument parameters
# that need to be sent to the DAG_infoBuffer_Monitor
# and the restart value returned by the DAG_infoBuffer_Monitor,
# which is not used.
class Local_Client_for_DAG_infoBuffer_Monitor_for_Lambdas:
    def __init__(self,DAG_infobuffer_monitor_for_lambdas):
        # This Local_Client_for_DAG_infoBuffer_Monitor 
        # wraps a DAG_infobuffer_monitor
        self.wrapped_DAG_infobuffer_monitor_for_lambdas = DAG_infobuffer_monitor_for_lambdas

    # This Local_Client_for_DAG_infoBuffer_Monitor 
    # wraps a DAG_infobuffer_monitor
    def set_DAG_infoBuffer_Monitor(self,DAG_infobuffer_monitor_for_lambdas):
        self.wrapped_DAG_infobuffer_monitor_for_lambdas = DAG_infobuffer_monitor_for_lambdas

    def deposit(self,DAG_info, new_leaf_task_work_tuples,DAG_info_is_complete):
        # bounded buffer is blocking; using same interface as Manager.Queue
        keyword_arguments = {}
        keyword_arguments['new_current_version_DAG_info'] = DAG_info
        keyword_arguments['new_current_version_new_leaf_tasks'] = new_leaf_task_work_tuples
        keyword_arguments['DAG_info_is_complete'] = DAG_info_is_complete
        # name of object is process_DAG_infoBuffer_Monitor, type specified on create
        # _restart return value begins with "_" so PyLint does not report it
        _return_value_ignored, _restart_value_ignored = self.wrapped_DAG_infobuffer_monitor_for_lambdas.deposit(**keyword_arguments)
 
    def withdraw(self,requested_current_version_number):
        keyword_arguments = {}
        keyword_arguments['requested_current_version_number'] = requested_current_version_number
        # name of object is process_DAG_infoBuffer_Monitor, type specified on create
        # This call returns a new DAG_info object that is being 
        # constructed incrementally.
        #DAG_info, new_leaf_task_states, _restart_value_ignored = self.wrapped_DAG_infobuffer_monitor.withdraw(**keyword_arguments)
        DAG_info, _restart_value_ignored = self.wrapped_DAG_infobuffer_monitor_for_lambdas.withdraw(**keyword_arguments)
        #return DAG_info, new_leaf_task_states
        return DAG_info