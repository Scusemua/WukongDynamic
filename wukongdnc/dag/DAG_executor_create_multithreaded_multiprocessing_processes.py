from multiprocessing import Process #, Manager

from .DAG_executor_constants import run_all_tasks_locally,  using_workers, num_workers 
from .DAG_executor_constants import compute_pagerank, use_shared_partitions_groups,use_page_rank_group_partitions
from .DAG_executor_constants import use_struct_of_arrays_for_pagerank
from .DAG_executor_constants import using_threads_not_processes, use_multithreaded_multiprocessing

from . import BFS_Shared
from .DAG_executor_create_threads_for_multiT_multiP import create_and_run_threads_for_multiT_multiP

import logging 
logger = logging.getLogger(__name__)

if not (not using_threads_not_processes or use_multithreaded_multiprocessing):
    logger.setLevel(logging.ERROR)
    formatter = logging.Formatter('[%(asctime)s] [%(module)s] [%(processName)s] [%(threadName)s]: %(message)s')
    ch = logging.StreamHandler()
    ch.setLevel(logging.ERROR)
    ch.setFormatter(formatter)
    logger.addHandler(ch)


#def create_multithreaded_multiprocessing_processes(num_processes_created_for_multithreaded_multiprocessing,multithreaded_multiprocessing_process_list,counter,process_work_queue,data_dict,log_queue,worker_configurer):
#def create_multithreaded_multiprocessing_processes(num_processes_created_for_multithreaded_multiprocessing,multithreaded_multiprocessing_process_list,counter,log_queue,worker_configurer):
def create_multithreaded_multiprocessing_processes(num_processes_created_for_multithreaded_multiprocessing,multithreaded_multiprocessing_process_list,completed_tasks_counter,completed_workers_counter,log_queue,worker_configurer):

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
            #proc = Process(target=create_and_run_threads_for_multiT_multiP, name=(process_name), args=(process_name,payload,counter,log_queue,worker_configurer,))

            if not (compute_pagerank and use_shared_partitions_groups):
                proc = Process(target=create_and_run_threads_for_multiT_multiP, name=(process_name), args=(process_name,payload,completed_tasks_counter,completed_workers_counter,log_queue,worker_configurer,
                    None,None,None,None,None,None,None,None,None,None))
            else:
                if use_page_rank_group_partitions:
                    shared_nodes = BFS_Shared.shared_groups
                    shared_map = BFS_Shared.shared_groups_map
                    shared_frontier_map = BFS_Shared.shared_groups_frontier_parents_map
                else:
                    shared_nodes = BFS_Shared.shared_partition
                    shared_map = BFS_Shared.shared_partition_map
                    shared_frontier_map = BFS_Shared.shared_partition_frontier_parents_map

                if use_struct_of_arrays_for_pagerank:
                    #proc = Process(target=create_and_run_threads_for_multiT_multiP, name=(process_name), args=(process_name,payload,counter,log_queue,worker_configurer,
                    proc = Process(target=create_and_run_threads_for_multiT_multiP, name=(process_name), args=(process_name,payload,completed_tasks_counter,completed_workers_counter,log_queue,worker_configurer,
                        shared_nodes,shared_map,shared_frontier_map,
                        BFS_Shared.pagerank_sent_to_processes,BFS_Shared.previous_sent_to_processes,BFS_Shared.number_of_children_sent_to_processes,
                        BFS_Shared.number_of_parents_sent_to_processes,BFS_Shared.starting_indices_of_parents_sent_to_processes,
                        BFS_Shared.parents_sent_to_processes,BFS_Shared.IDs_sent_to_processes,))
                else:
                    #proc = Process(target=create_and_run_threads_for_multiT_multiP, name=(process_name), args=(process_name,payload,counter,log_queue,worker_configurer,
                    proc = Process(target=create_and_run_threads_for_multiT_multiP, name=(process_name), args=(process_name,payload,completed_tasks_counter,completed_workers_counter,log_queue,worker_configurer,
                        shared_nodes,shared_map,shared_frontier_map,
                        None,None,None,None,None,None,None))


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