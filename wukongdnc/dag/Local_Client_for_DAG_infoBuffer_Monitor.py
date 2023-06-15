#rhc continue 
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

# local wrapper for a DAG_infoBuffer_Monitor. there is also a 
# remote wrapper: Remote_Client_for_DAG_infoBuffer_Monitor
# This wrapper deals with the keword argument parameters
# that need to be sent to the DAG_infoBuffer_Monitor
# and the restart value returned by the DAG_infoBuffer_Monitor,
# which is not used.
class Local_Client_for_DAG_infoBuffer_Monitor:
    def __init__(self,DAG_infobuffer_monitor):
        # This Local_Client_for_DAG_infoBuffer_Monitor 
        # wraps a DAG_infobuffer_monitor
        self.DAG_infobuffer_monitor = DAG_infobuffer_monitor

    # This Local_Client_for_DAG_infoBuffer_Monitor 
    # wraps a DAG_infobuffer_monitor
    def set_DAG_infoBuffer_Monitor(self,DAG_infobuffer_monitor):
        self.DAG_infobuffer_monitor = DAG_infobuffer_monitor

    def deposit(self,DAG_info):
        # bounded buffer is blocking; using same interface as Manager.Queue
        keyword_arguments = {}
        keyword_arguments['new_current_version_DAG_info'] = DAG_info
        # name of object is process_DAG_infoBuffer_Monitor, type specified on create
        # _restart return value begins with "_" so PyLint does not report it
        _return_value_ignored, _restart_value_ignored = self.DAG_infobuffer_monitor.deposit(**keyword_arguments)
 
    def withdraw(self,requested_current_version_number):
        keyword_arguments = {}
        keyword_arguments['requested_current_version_number'] = requested_current_version_number
        # name of object is process_DAG_infoBuffer_Monitor, type specified on create
        # This call returns a new DAG_info object that is being 
        # constructed incrementally.
        DAG_info, _restart_value_ignored = self.DAG_infobuffer_monitor.withdraw(**keyword_arguments)
        return DAG_info