
from .DAG_executor_constants import run_all_tasks_locally 
from .DAG_executor_constants import  using_workers
from .DAG_executor_constants import num_workers
from multiprocessing import Process #, Manager
from .DAG_executor_create_threads_for_multiT_multiP import create_and_run_threads_for_multiT_multiP

import logging 
logger = logging.getLogger(__name__)
"""
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(asctime)s] [%(threadName)s] %(levelname)s: %(message)s')
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)
logger.addHandler(ch)
"""

#def create_multithreaded_multiprocessing_processes(num_processes_created_for_multithreaded_multiprocessing,multithreaded_multiprocessing_process_list,counter,process_work_queue,data_dict,log_queue,worker_configurer):
def create_multithreaded_multiprocessing_processes(num_processes_created_for_multithreaded_multiprocessing,multithreaded_multiprocessing_process_list,counter,log_queue,worker_configurer):

    logger.debug("DAG_executor_driver: Starting multi processors for multhreaded multipocessing.")
    iteration = 1
    while True:
        logger.debug("create processes iteration: " + str(iteration))
        iteration += 1
         # asserts:
        if not run_all_tasks_locally:
            logger.error("[Error]: multithreaded multiprocessing loop but not run_all_tasks_locally")
        if not using_workers:
            logger.debug("[ERROR] DAG_executor_driver: Starting multi processes for multithreaded multiprocessing but using_workers is false.")

        try:
            payload = {
            }
            process_name = "proc"+str(num_processes_created_for_multithreaded_multiprocessing + 1)
            #proc = Process(target=create_and_run_threads_for_multiT_multiP, name=(process_name), args=(process_name,payload,counter,process_work_queue,data_dict,log_queue,worker_configurer,))
            proc = Process(target=create_and_run_threads_for_multiT_multiP, name=(process_name), args=(process_name,payload,counter,log_queue,worker_configurer,))
            #proc.start()
            multithreaded_multiprocessing_process_list.append(proc)
            num_processes_created_for_multithreaded_multiprocessing += 1                      

            logger.debug("num_processes_created_for_multithreaded_multiprocessing: " + str(num_processes_created_for_multithreaded_multiprocessing)
                + " num_workers: " + str(num_workers))
            if num_processes_created_for_multithreaded_multiprocessing == num_workers:
                logger.debug("process creation loop breaking")
                break 

        except Exception as ex:
            logger.debug("[ERROR] DAG_executor_driver: Failed to start worker process for multithreaded multiprocessing " + "Worker_process_multithreaded_multiproc_" + str(num_processes_created_for_multithreaded_multiprocessing + 1))
            logger.debug(ex)

    return num_processes_created_for_multithreaded_multiprocessing