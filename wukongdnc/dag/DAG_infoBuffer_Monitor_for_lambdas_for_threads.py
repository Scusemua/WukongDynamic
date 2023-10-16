from .DAG_executor_constants import using_threads_not_processes, using_workers, run_all_tasks_locally
from .DAG_executor_constants import use_incremental_DAG_generation
from .Local_Client_for_DAG_infoBuffer_Monitor_for_Lambdas import Local_Client_for_DAG_infoBuffer_Monitor_for_Lambdas
from wukongdnc.server.DAG_infoBuffer_Monitor_for_Lambdas import DAG_infoBuffer_Monitor_for_Lambdas

# The DAG generator (BFS.py) calls deposit() on 
# DAG_infobuffer_monitor to deposit a new DAG and clients
# call (blocking) withdraw to get a new DAG.
DAG_infobuffer_monitor = None
if run_all_tasks_locally and using_workers and use_incremental_DAG_generation and using_threads_not_processes:
    # Wrapping a DAG_infobuffer_monitor in a Local_Client_for_DAG_infoBuffer_Monitor.
    # This wrapper deals with the keword argument parameters
    # that need to be sent to the DAG_infoBuffer_Monitor
    # and the restart value returned by the DAG_infoBuffer_Monitor,
    # which is ignored.
    wrapped_DAG_infobuffer_monitor_for_Lambdas = DAG_infoBuffer_Monitor_for_Lambdas()
    # This wrapper does not take a websocket for __init__ since the
    # DAG_infoBuffer_Monitor is local. 
    DAG_infobuffer_monitor = Local_Client_for_DAG_infoBuffer_Monitor_for_Lambdas(wrapped_DAG_infobuffer_monitor_for_Lambdas)