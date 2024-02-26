import threading
import os

#from .DAG_executor_constants import RUN_ALL_TASKS_LOCALLY, NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING
#from .DAG_executor_constants import EXIT_PROGRAM_ON_EXCEPTION
from . import DAG_executor_constants
from . import DAG_executor

import logging 
logger = logging.getLogger(__name__)

"""
if not (not USING_THREADS_NOT_PROCESSES or USE_MULTITHREADED_MULTIPROCESSING):
    logger.setLevel(logging.ERROR)
    formatter = logging.Formatter('[%(asctime)s] [%(module)s] [%(processName)s] [%(threadName)s]: %(message)s')
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    ch.setFormatter(formatter)
    logger.addHandler(ch)
"""


#def create_and_run_threads_for_multiT_multiP(process_name,payload,counter,log_queue,worker_configurer):
def create_and_run_threads_for_multiT_multiP(process_name,payload,completed_tasks_counter,completed_workers_counter,log_queue,worker_configurer,
    shared_nodes,shared_map,shared_frontier_map,
    pagerank_sent_to_processes,previous_sent_to_processes,number_of_children_sent_to_processes,number_of_parents_sent_to_processes,starting_indices_of_parents_sent_to_processes,parents_sent_to_processes,IDs_sent_to_processes,):
    # create, start, and join the threads in the thread pool for a multi process
#def create_and_run_threads_for_multiT_multiP(process_name,payload,counter,process_work_queue,data_dict,log_queue,worker_configurer):

    #global logger
#brc: logging
    #
    # This is the start of the process code - get logger for multiprocessing.
    # Note that this logger is passed to the threads for each process:
    #  args=(payload,counter,logger,worker_configurer,)
    # The thread bodies are: target=DAG_executor.DAG_executor_processes
    # where the parameters are:
    #   DAG_executor_processes(payload,counter,logger = log_queue_or_logger, worker_configurer)
    # Note: "logger = log_queue_or_logger" is a queue when we are not using multithreaded
    # mutiprocessing, i.e., we are using redular worker proccesses which
    # are single threaded. In that case, DAG_executor.DAG_executor_processes is
    # the start of the process code so DAG_executor.DAG_executor_processes 
    # executes the "worker_configurer(log_queue)" at the start of the process
    # code. When we are using multithreaded multiprocessing, we have worker
    # processes, as usual, but these processes have multiple threads and each
    # thread runs DAG_executor.DAG_executor_processes as its body. This means
    # the start of the process code is this  method
    # create_and_run_threads_for_multiT_multiP so this method eecutes
    # worker_configurer(log_queue); logger = logging.getLogger("multiP")
    # and passes the logger to each created thread as an argument, but this
    # arument matches parameter "log_queue" of DAG_executor.DAG_executor_processes
    # so the threads will execute logger = logger = log_queue_or_logger.
    worker_configurer(log_queue)
    logger = logging.getLogger("multiP")
    #logger.setLevel(logging.DEBUG)

    logger.trace(process_name + ": multiT_multiP")

    thread_list = []
    num_threads_created_for_multiP = 0
    try:
        msg = "[Error]: DAG_executor_driver: create_and_run_threads_for_multiT_multiP: multithreaded multiprocessing loop but not RUN_ALL_TASKS_LOCALLY"
        assert DAG_executor_constants.RUN_ALL_TASKS_LOCALLY , msg
    except AssertionError:
        logger.exception("[Error]: assertion failed")
        if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
            logging.shutdown()
            os._exit(0)
    #assertOld:
    #if not RUN_ALL_TASKS_LOCALLY:
    #    logger.error("[Error]: DAG_executor_driver: create_and_run_threads_for_multiT_multiP: multithreaded multiprocessing loop but not RUN_ALL_TASKS_LOCALLY")

    logger.trace(process_name + ": DAG_executor_driver: create_and_run_threads_for_multiT_multiP: Starting threads for multhreaded multipocessing.")
    iteration = 1
    #while True:
    
    #print("pagerank_sent_to_processes: " + str(pagerank_sent_to_processes[:10]))

    # NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING defined in DAG_executor_constants
    while num_threads_created_for_multiP < DAG_executor_constants.NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING:
        logger.trace(process_name + ": iterate: " + str(iteration))
        try:
            DAG_exec_state = None
            payload = {
                "DAG_executor_state": DAG_exec_state
            }
            thread_name = process_name+"_thread"+str(num_threads_created_for_multiP+1)
            #thread = threading.Thread(target=DAG_executor.DAG_executor_processes, name=(thread_name), args=(payload,counter,log_queue,worker_configurer,))
            #thread = threading.Thread(target=DAG_executor.DAG_executor_processes, name=(thread_name), args=(payload,counter,logger,worker_configurer,))
            thread = threading.Thread(target=DAG_executor.DAG_executor_processes, name=(thread_name), args=(payload,completed_tasks_counter,completed_workers_counter,logger,worker_configurer,
                shared_nodes,shared_map,shared_frontier_map,
                pagerank_sent_to_processes,previous_sent_to_processes,number_of_children_sent_to_processes,number_of_parents_sent_to_processes,starting_indices_of_parents_sent_to_processes,parents_sent_to_processes,IDs_sent_to_processes,))

            thread_list.append(thread)
            #thread.start()
            num_threads_created_for_multiP += 1 
            logger.trace(process_name + ": iteration: " + str(iteration) + ": num_threads_created_for_multiP: " + str(num_threads_created_for_multiP)
                + " NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING: " + str(DAG_executor_constants.NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING))
            #if num_threads_created_for_multiP == NUM_THREADS_FOR_MULTITHREADED_MULTIPROCESSING:
            #    logger.trace(process_name + " breaking")
            #    break     
            iteration += 1
        except Exception:
            logger.exception("[Error]:"
                + " DAG_executor_driver: create_and_run_threads_for_multiT_multiP: Failed to start tread for multithreaded multiprocessing " 
                + "thread_multitheaded_multiproc_" + str(num_threads_created_for_multiP + 1))
            if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
                logging.shutdown()
                os._exit(0)

    logger.trace(process_name + ": DAG_executor_driver: create_and_run_threads_for_multiT_multiP: "
        + process_name + " created " + str(len(thread_list)) + " threads")

    for thread in thread_list:
        thread.start()

    logger.trace(process_name + ": DAG_executor_driver: create_and_run_threads_for_multiT_multiP: "
        + process_name + " started " + str(len(thread_list)) + " threads")

    logger.trace(process_name + ": DAG_executor_driver: create_and_run_threads_for_multiT_multiP: "
        + process_name + " joining threads.")

    for thread in thread_list:
        thread.join()	

    # return and join multithreaded_multiprocessing_processes in DAG_excutor_driver