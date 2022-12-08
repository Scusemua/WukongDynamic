
from .DAG_executor_constants import run_all_tasks_locally, num_threads_for_multithreaded_multiprocessing

from . import DAG_executor
import threading

import logging 
logger = logging.getLogger(__name__)

logger.setLevel(logging.ERROR)
formatter = logging.Formatter('[%(asctime)s] [%(threadName)s] %(levelname)s: %(message)s')
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)
logger.addHandler(ch)



def create_and_run_threads_for_multiT_multiP(process_name,payload,counter,log_queue,worker_configurer):
    # create, start, and join the threads in the thread pool for a multi process
#def create_and_run_threads_for_multiT_multiP(process_name,payload,counter,process_work_queue,data_dict,log_queue,worker_configurer):

    global logger
#rhc: logging
    #worker_configurer(log_queue)
    #logger = logging.getLogger("multiP")
    #logger.setLevel(logging.DEBUG)

    logger.debug(process_name + ": multiT_multiP")


    thread_list = []
    num_threads_created_for_multiP = 0
    if not run_all_tasks_locally:
        logger.error("[Error]: DAG_executor_driver: create_and_run_threads_for_multiT_multiP: multithreaded multiprocessing loop but not run_all_tasks_locally")
    logger.debug(process_name + ": DAG_executor_driver: create_and_run_threads_for_multiT_multiP: Starting threads for multhreaded multipocessing.")
    iteration = 1
    #while True:
    while num_threads_created_for_multiP < num_threads_for_multithreaded_multiprocessing:
        logger.debug(process_name + ": iterate: " + str(iteration))
        try:
            DAG_exec_state = None
            payload = {
                "DAG_executor_state": DAG_exec_state
            }
            thread_name = process_name+"_thread"+str(num_threads_created_for_multiP+1)
            #thread = threading.Thread(target=DAG_executor.DAG_executor_processes, name=(thread_name), args=(payload,counter,log_queue,worker_configurer,))
            thread = threading.Thread(target=DAG_executor.DAG_executor_processes, name=(thread_name), args=(payload,counter,logger,worker_configurer,))

            thread_list.append(thread)
            #thread.start()
            num_threads_created_for_multiP += 1 
            logger.debug(process_name + ": iteration: " + str(iteration) + ": num_threads_created_for_multiP: " + str(num_threads_created_for_multiP)
                + " num_threads_for_multithreaded_multiprocessing: " + str(num_threads_for_multithreaded_multiprocessing))
            #if num_threads_created_for_multiP == num_threads_for_multithreaded_multiprocessing:
            #    logger.debug(process_name + " breaking")
            #    break     
            iteration += 1
        except Exception as ex:
            logger.debug("[ERROR] DAG_executor_driver: create_and_run_threads_for_multiT_multiP: Failed to start tread for multithreaded multiprocessing " + "thread_multitheaded_multiproc_" + str(num_threads_created_for_multiP + 1))
            logger.debug(ex)

    logger.debug(process_name + ": DAG_executor_driver: create_and_run_threads_for_multiT_multiP: "
        + process_name + " created " + str(len(thread_list)) + " threads")


    for thread in thread_list:
        thread.start()

    logger.debug(process_name + ": DAG_executor_driver: create_and_run_threads_for_multiT_multiP: "
        + process_name + " started " + str(len(thread_list)) + " threads")

    logger.debug(process_name + ": DAG_executor_driver: create_and_run_threads_for_multiT_multiP: "
        + process_name + " joining threads.")

    for thread in thread_list:
        thread.join()	


    # return and join multithreaded_multiprocessing_processes