from .DAG_executor_constants import using_threads_not_processes, using_workers
from .DAG_executor_constants import use_incremental_DAG_generation
from .Local_Client_for_DAG_infoBuffer_Monitor import Local_Client_for_DAG_infoBuffer_Monitor
from wukongdnc.server.DAG_infoBuffer_Monitor import DAG_infoBuffer_Monitor

DAG_infobuffer_monitor = None
if using_workers and use_incremental_DAG_generation and using_threads_not_processes:
    # wrapping a DAG_infobuffer_monitor in a Local_Client_for_DAG_infoBuffer_Monitor.
    # This wrapper deals with the keword argument parameters
    # that need to be sent to the DAG_infoBuffer_Monitor
    # and the restart value returned by the DAG_infoBuffer_Monitor,
    # which is ignored.
    wrapped_DAG_infobuffer_monitor = DAG_infoBuffer_Monitor()
    # this wraper does not take a websocket for __init__ since the
    # DAG_infoBuffer_Monitor is local.
    DAG_infobuffer_monitor = Local_Client_for_DAG_infoBuffer_Monitor(wrapped_DAG_infobuffer_monitor)