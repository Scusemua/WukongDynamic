#import os
import logging

#from .DAG_executor_constants import USING_THREADS_NOT_PROCESSES, USING_WORKERS, RUN_ALL_TASKS_LOCALLY
#from .DAG_executor_constants import COMPUTE_PAGERANK, USE_INCREMENTAL_DAG_GENERATION
from . import DAG_executor_constants

from .Local_Client_for_DAG_infoBuffer_Monitor import Local_Client_for_DAG_infoBuffer_Monitor
import wukongdnc.server.DAG_infoBuffer_Monitor
import wukongdnc.server.DAG_infoBuffer_Monitor_for_Lambdas
from .Local_Client_for_DAG_infoBuffer_Monitor_for_Lambdas import Local_Client_for_DAG_infoBuffer_Monitor_for_Lambdas
logger = logging.getLogger(__name__)

# We use a remote DAG_infobuffer_monitor(_for_lambdas) for real lambdas and processes.
# We use a local DAG_infobuffer_monitor for worker threads and simulated lambdas.
# More: real lambda always use a remote client for a monitor for lambdas, 
# simulated lambdas uses local client for lambda 
# for a monitor for lambdas. Threads use a local client, processes use 
# a remote client that is different from the remote client used by lambdas,
# So we have a Local_Client_for_DAG_infoBuffer_Monitor_for_Lambdas.py
# Remote_Client_for_DAG_infoBuffer_Monitor_for_Lambdas.py, and 
# Remote_Client_for_DAG_infoBuffer_Monitor for processes.

#
# Note that the remote DAG_infobuffer_monitor(_for_lambdas) is creatws at the 
# start of bfs().
#
# The DAG generator (BFS.py) calls deposit() on 
# DAG_infobuffer_monitor to deposit a new DAG and clients
# call (blocking) withdraw to get a new DAG.
DAG_infobuffer_monitor = None
if DAG_executor_constants.RUN_ALL_TASKS_LOCALLY and DAG_executor_constants.USING_WORKERS \
    and DAG_executor_constants.COMPUTE_PAGERANK and DAG_executor_constants.USE_INCREMENTAL_DAG_GENERATION \
    and DAG_executor_constants.USING_THREADS_NOT_PROCESSES:
    # Wrapping a DAG_infobuffer_monitor in a Local_Client_for_DAG_infoBuffer_Monitor.
    # This wrapper deals with the keword argument parameters
    # that need to be sent to the DAG_infoBuffer_Monitor
    # and the restart value returned by the DAG_infoBuffer_Monitor,
    # which is ignored.
    wrapped_DAG_infobuffer_monitor = wukongdnc.server.DAG_infoBuffer_Monitor.DAG_infoBuffer_Monitor()
    # This wrapper does not take a websocket for __init__ since the
    # DAG_infoBuffer_Monitor is local. 
    DAG_infobuffer_monitor = Local_Client_for_DAG_infoBuffer_Monitor(wrapped_DAG_infobuffer_monitor)

    #os._exit(0)
elif DAG_executor_constants.RUN_ALL_TASKS_LOCALLY and not DAG_executor_constants.USING_WORKERS and DAG_executor_constants.COMPUTE_PAGERANK and DAG_executor_constants.USE_INCREMENTAL_DAG_GENERATION and DAG_executor_constants.USING_THREADS_NOT_PROCESSES:
    # Wrapping a DAG_infobuffer_monitor in a Local_Client_for_DAG_infoBuffer_Monitor.
    # This wrapper deals with the keword argument parameters
    # that need to be sent to the DAG_infoBuffer_Monitor
    # and the restart value returned by the DAG_infoBuffer_Monitor,
    # which is ignored.
    wrapped_DAG_infobuffer_monitor_for_Lambdas = wukongdnc.server.DAG_infoBuffer_Monitor_for_Lambdas.DAG_infoBuffer_Monitor_for_Lambdas()
    # This wrapper does not take a websocket for __init__ since the
    # DAG_infoBuffer_Monitor is local. 
    DAG_infobuffer_monitor = Local_Client_for_DAG_infoBuffer_Monitor_for_Lambdas(wrapped_DAG_infobuffer_monitor_for_Lambdas)
