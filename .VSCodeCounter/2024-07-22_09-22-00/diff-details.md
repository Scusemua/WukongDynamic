# Diff Details

Date : 2024-07-22 09:22:00

Directory c:\\Users\\benrc\\Desktop\\Executor\\DAG\\WukongDynamic

Total : 108 files,  16166 codes, -1820 comments, 2185 blanks, all 16531 lines

[Summary](results.md) / [Details](details.md) / [Diff Summary](diff.md) / Diff Details

## Files
| filename | language | code | comment | blank | total |
| :--- | :--- | ---: | ---: | ---: | ---: |
| [README.md](/README.md) | Markdown | 41 | 0 | 14 | 55 |
| [fibonnaci_program.py](/fibonnaci_program.py) | Python | 425 | -485 | 60 | 0 |
| [handler.py](/handler.py) | Python | -1 | 0 | 0 | -1 |
| [handlerDAG.py](/handlerDAG.py) | Python | 66 | 18 | 15 | 99 |
| [handlerNew.py](/handlerNew.py) | Python | -1 | 0 | 0 | -1 |
| [mergesort_driver.py](/mergesort_driver.py) | Python | 18 | -23 | 5 | 0 |
| [mergesort_program.py](/mergesort_program.py) | Python | 88 | -97 | 9 | 0 |
| [treereduction_program.py](/treereduction_program.py) | Python | 81 | -89 | 8 | 0 |
| [wukongdnc/constants.py](/wukongdnc/constants.py) | Python | 1 | 0 | 0 | 1 |
| [wukongdnc/dag/BFS.py](/wukongdnc/dag/BFS.py) | Python | 1,738 | 720 | 277 | 2,735 |
| [wukongdnc/dag/BFS_Custom_Dask_DAG.py](/wukongdnc/dag/BFS_Custom_Dask_DAG.py) | Python | 98 | -86 | 1 | 13 |
| [wukongdnc/dag/BFS_DAG_Generator_Multithreaded.py](/wukongdnc/dag/BFS_DAG_Generator_Multithreaded.py) | Python | 140 | 79 | 25 | 244 |
| [wukongdnc/dag/BFS_Node.py](/wukongdnc/dag/BFS_Node.py) | Python | 7 | -8 | 1 | 0 |
| [wukongdnc/dag/BFS_Partition_Node.py](/wukongdnc/dag/BFS_Partition_Node.py) | Python | 36 | -31 | -7 | -2 |
| [wukongdnc/dag/BFS_Shared.py](/wukongdnc/dag/BFS_Shared.py) | Python | 296 | -460 | 25 | -139 |
| [wukongdnc/dag/BFS_dfs_parent_pre_post.py](/wukongdnc/dag/BFS_dfs_parent_pre_post.py) | Python | 264 | -311 | 48 | 1 |
| [wukongdnc/dag/BFS_generate_DAG_info.py](/wukongdnc/dag/BFS_generate_DAG_info.py) | Python | 726 | -174 | 98 | 650 |
| [wukongdnc/dag/BFS_generate_DAG_info_incremental_groups.py](/wukongdnc/dag/BFS_generate_DAG_info_incremental_groups.py) | Python | 442 | 240 | 56 | 738 |
| [wukongdnc/dag/BFS_generate_DAG_info_incremental_partitions.py](/wukongdnc/dag/BFS_generate_DAG_info_incremental_partitions.py) | Python | 523 | 486 | 145 | 1,154 |
| [wukongdnc/dag/BFS_generate_shared_partitions_groups.py](/wukongdnc/dag/BFS_generate_shared_partitions_groups.py) | Python | 43 | -30 | 2 | 15 |
| [wukongdnc/dag/BFS_pagerank.py](/wukongdnc/dag/BFS_pagerank.py) | Python | 443 | -167 | 52 | 328 |
| [wukongdnc/dag/BFS_scc.py](/wukongdnc/dag/BFS_scc.py) | Python | 46 | -45 | 0 | 1 |
| [wukongdnc/dag/BFS_stuff.py](/wukongdnc/dag/BFS_stuff.py) | Python | 394 | -437 | 43 | 0 |
| [wukongdnc/dag/Ben_Wukong_Task_Code.py](/wukongdnc/dag/Ben_Wukong_Task_Code.py) | Python | 64 | -66 | 4 | 2 |
| [wukongdnc/dag/Client_for_DAG_infoBuffer_Monitor_for_Lambdas.py](/wukongdnc/dag/Client_for_DAG_infoBuffer_Monitor_for_Lambdas.py) | Python | 26 | 21 | 4 | 51 |
| [wukongdnc/dag/DAG_Executor_lambda_function_simulator.py](/wukongdnc/dag/DAG_Executor_lambda_function_simulator.py) | Python | 330 | -362 | 44 | 12 |
| [wukongdnc/dag/DAG_boundedbuffer_work_queue.py](/wukongdnc/dag/DAG_boundedbuffer_work_queue.py) | Python | 1 | 2 | 0 | 3 |
| [wukongdnc/dag/DAG_data_dict_for_threads.py](/wukongdnc/dag/DAG_data_dict_for_threads.py) | Python | 0 | 1 | 0 | 1 |
| [wukongdnc/dag/DAG_executor.py](/wukongdnc/dag/DAG_executor.py) | Python | 966 | 688 | 122 | 1,776 |
| [wukongdnc/dag/DAG_executor_constants.py](/wukongdnc/dag/DAG_executor_constants.py) | Python | 4,191 | 894 | 431 | 5,516 |
| [wukongdnc/dag/DAG_executor_counter.py](/wukongdnc/dag/DAG_executor_counter.py) | Python | 3 | 1 | 0 | 4 |
| [wukongdnc/dag/DAG_executor_countermp.py](/wukongdnc/dag/DAG_executor_countermp.py) | Python | 3 | 0 | 1 | 4 |
| [wukongdnc/dag/DAG_executor_create_multithreaded_multiprocessing_processes.py](/wukongdnc/dag/DAG_executor_create_multithreaded_multiprocessing_processes.py) | Python | 17 | 9 | 3 | 29 |
| [wukongdnc/dag/DAG_executor_create_threads_for_multiT_multiP.py](/wukongdnc/dag/DAG_executor_create_threads_for_multiT_multiP.py) | Python | 12 | 4 | -1 | 15 |
| [wukongdnc/dag/DAG_executor_driver.py](/wukongdnc/dag/DAG_executor_driver.py) | Python | 1,263 | -471 | 190 | 982 |
| [wukongdnc/dag/DAG_executor_output_checker.py](/wukongdnc/dag/DAG_executor_output_checker.py) | Python | 15 | 6 | 1 | 22 |
| [wukongdnc/dag/DAG_executor_synchronizer.py](/wukongdnc/dag/DAG_executor_synchronizer.py) | Python | 50 | 7 | 0 | 57 |
| [wukongdnc/dag/DAG_executor_work_queue_for_threads.py](/wukongdnc/dag/DAG_executor_work_queue_for_threads.py) | Python | 0 | 1 | 1 | 2 |
| [wukongdnc/dag/DAG_info.py](/wukongdnc/dag/DAG_info.py) | Python | 27 | 7 | 2 | 36 |
| [wukongdnc/dag/DAG_infoBuffer_Monitor_for_lambdas_for_threads.py](/wukongdnc/dag/DAG_infoBuffer_Monitor_for_lambdas_for_threads.py) | Python | 12 | 13 | 2 | 27 |
| [wukongdnc/dag/DAG_infoBuffer_Monitor_for_threads.py](/wukongdnc/dag/DAG_infoBuffer_Monitor_for_threads.py) | Python | 6 | 11 | 4 | 21 |
| [wukongdnc/dag/DFS.py](/wukongdnc/dag/DFS.py) | Python | 28 | -33 | 5 | 0 |
| [wukongdnc/dag/DFS_visit.py](/wukongdnc/dag/DFS_visit.py) | Python | 328 | -321 | 69 | 76 |
| [wukongdnc/dag/Hunar_Vectorize.py](/wukongdnc/dag/Hunar_Vectorize.py) | Python | 40 | 8 | 11 | 59 |
| [wukongdnc/dag/Local_Client_for_DAG_infoBuffer_Monitor.py](/wukongdnc/dag/Local_Client_for_DAG_infoBuffer_Monitor.py) | Python | 9 | -7 | 0 | 2 |
| [wukongdnc/dag/Local_Client_for_DAG_infoBuffer_Monitor_for_Lambdas.py](/wukongdnc/dag/Local_Client_for_DAG_infoBuffer_Monitor_for_Lambdas.py) | Python | 27 | 21 | 4 | 52 |
| [wukongdnc/dag/QueuevsBB.py](/wukongdnc/dag/QueuevsBB.py) | Python | 17 | -18 | 1 | 0 |
| [wukongdnc/dag/Remote_Client_for_DAG_infoBuffer_Monitor.py](/wukongdnc/dag/Remote_Client_for_DAG_infoBuffer_Monitor.py) | Python | 9 | -7 | 0 | 2 |
| [wukongdnc/dag/Remote_Client_for_DAG_infoBuffer_Monitor_for_Lambdas.py](/wukongdnc/dag/Remote_Client_for_DAG_infoBuffer_Monitor_for_Lambdas.py) | Python | 37 | 15 | 6 | 58 |
| [wukongdnc/dag/SCC.py](/wukongdnc/dag/SCC.py) | Python | 31 | -33 | 2 | 0 |
| [wukongdnc/dag/TestAll.py](/wukongdnc/dag/TestAll.py) | Python | 96 | 70 | 24 | 190 |
| [wukongdnc/dag/addLoggingLevel.py](/wukongdnc/dag/addLoggingLevel.py) | Python | 39 | 4 | 10 | 53 |
| [wukongdnc/dag/counter_mp.py](/wukongdnc/dag/counter_mp.py) | Python | 1 | 1 | -1 | 1 |
| [wukongdnc/dag/dask_dag.py](/wukongdnc/dag/dask_dag.py) | Python | 252 | -228 | 33 | 57 |
| [wukongdnc/dag/multiprocessing_logging.py](/wukongdnc/dag/multiprocessing_logging.py) | Python | 26 | -24 | 8 | 10 |
| [wukongdnc/dag/pagerank.py](/wukongdnc/dag/pagerank.py) | Python | 16 | -16 | 0 | 0 |
| [wukongdnc/dag/pythonMultiP.py](/wukongdnc/dag/pythonMultiP.py) | Python | 38 | -40 | 14 | 12 |
| [wukongdnc/dag/util.py](/wukongdnc/dag/util.py) | Python | 17 | -19 | 2 | 0 |
| [wukongdnc/old_invoker.py](/wukongdnc/old_invoker.py) | Python | 9 | -11 | 2 | 0 |
| [wukongdnc/programs/DivideAndConquerTreeReduction.py](/wukongdnc/programs/DivideAndConquerTreeReduction.py) | Python | 110 | -129 | 19 | 0 |
| [wukongdnc/programs/DivideandConquerMergeSort.py](/wukongdnc/programs/DivideandConquerMergeSort.py) | Python | 110 | -129 | 19 | 0 |
| [wukongdnc/server/CountingSemaphore_Monitor.py](/wukongdnc/server/CountingSemaphore_Monitor.py) | Python | 2 | 0 | -1 | 1 |
| [wukongdnc/server/CountingSemaphore_MonitorOld.py](/wukongdnc/server/CountingSemaphore_MonitorOld.py) | Python | 2 | 0 | 0 | 2 |
| [wukongdnc/server/CountingSemaphore_Monitor_Select.py](/wukongdnc/server/CountingSemaphore_Monitor_Select.py) | Python | 2 | 0 | 0 | 2 |
| [wukongdnc/server/DAG_executor_FanIn.py](/wukongdnc/server/DAG_executor_FanIn.py) | Python | 2 | 0 | 0 | 2 |
| [wukongdnc/server/DAG_executor_FanInNB.py](/wukongdnc/server/DAG_executor_FanInNB.py) | Python | 51 | 70 | 9 | 130 |
| [wukongdnc/server/DAG_executor_FanInNB_select.py](/wukongdnc/server/DAG_executor_FanInNB_select.py) | Python | 57 | 62 | 3 | 122 |
| [wukongdnc/server/DAG_executor_FanIn_select.py](/wukongdnc/server/DAG_executor_FanIn_select.py) | Python | -3 | 2 | -3 | -4 |
| [wukongdnc/server/DAG_infoBuffer_Monitor.py](/wukongdnc/server/DAG_infoBuffer_Monitor.py) | Python | 53 | 173 | 6 | 232 |
| [wukongdnc/server/DAG_infoBuffer_Monitor_for_Lambdas.py](/wukongdnc/server/DAG_infoBuffer_Monitor_for_Lambdas.py) | Python | 108 | 89 | 7 | 204 |
| [wukongdnc/server/api.py](/wukongdnc/server/api.py) | Python | 188 | -235 | 47 | 0 |
| [wukongdnc/server/barrier.py](/wukongdnc/server/barrier.py) | Python | 2 | 0 | 0 | 2 |
| [wukongdnc/server/barrierOld.py](/wukongdnc/server/barrierOld.py) | Python | 2 | 0 | 0 | 2 |
| [wukongdnc/server/bounded_buffer.py](/wukongdnc/server/bounded_buffer.py) | Python | 9 | 4 | 3 | 16 |
| [wukongdnc/server/bounded_bufferOld.py](/wukongdnc/server/bounded_bufferOld.py) | Python | 11 | -9 | 0 | 2 |
| [wukongdnc/server/bounded_buffer_select.py](/wukongdnc/server/bounded_buffer_select.py) | Python | 2 | 0 | 0 | 2 |
| [wukongdnc/server/counting_semaphore.py](/wukongdnc/server/counting_semaphore.py) | Python | 38 | -40 | 4 | 2 |
| [wukongdnc/server/fanin.py](/wukongdnc/server/fanin.py) | Python | 9 | 6 | 2 | 17 |
| [wukongdnc/server/faninOld.py](/wukongdnc/server/faninOld.py) | Python | 9 | 6 | 2 | 17 |
| [wukongdnc/server/lambda_driver.py](/wukongdnc/server/lambda_driver.py) | Python | 3 | -3 | 0 | 0 |
| [wukongdnc/server/lambda_function.py](/wukongdnc/server/lambda_function.py) | Python | 38 | -51 | 13 | 0 |
| [wukongdnc/server/local_driver.py](/wukongdnc/server/local_driver.py) | Python | 31 | -44 | 12 | -1 |
| [wukongdnc/server/message_handler_lambda.py](/wukongdnc/server/message_handler_lambda.py) | Python | 155 | -137 | 16 | 34 |
| [wukongdnc/server/monitor_su.py](/wukongdnc/server/monitor_su.py) | Python | 0 | -1 | 0 | -1 |
| [wukongdnc/server/result_buffer.py](/wukongdnc/server/result_buffer.py) | Python | 2 | 0 | -2 | 0 |
| [wukongdnc/server/selectableEntry.py](/wukongdnc/server/selectableEntry.py) | Python | 2 | 0 | -2 | 0 |
| [wukongdnc/server/selectivewait.py](/wukongdnc/server/selectivewait.py) | Python | 2 | 0 | -2 | 0 |
| [wukongdnc/server/selector.py](/wukongdnc/server/selector.py) | Python | 7 | 17 | 0 | 24 |
| [wukongdnc/server/selector_lambda.py](/wukongdnc/server/selector_lambda.py) | Python | 22 | 11 | -2 | 31 |
| [wukongdnc/server/state.py](/wukongdnc/server/state.py) | Python | -1 | 1 | 0 | 0 |
| [wukongdnc/server/synchronizer.py](/wukongdnc/server/synchronizer.py) | Python | 178 | -161 | 18 | 35 |
| [wukongdnc/server/synchronizerThreadSelect.py](/wukongdnc/server/synchronizerThreadSelect.py) | Python | 5 | 3 | -2 | 6 |
| [wukongdnc/server/synchronizer_lambda.py](/wukongdnc/server/synchronizer_lambda.py) | Python | 65 | -24 | -1 | 40 |
| [wukongdnc/server/synchronizer_thread.py](/wukongdnc/server/synchronizer_thread.py) | Python | 2 | 0 | -2 | 0 |
| [wukongdnc/server/tcp_server.py](/wukongdnc/server/tcp_server.py) | Python | 274 | -21 | 37 | 290 |
| [wukongdnc/server/tcp_server_lambda.py](/wukongdnc/server/tcp_server_lambda.py) | Python | 317 | -240 | 42 | 119 |
| [wukongdnc/server/tcp_server_old.py](/wukongdnc/server/tcp_server_old.py) | Python | 8 | 1 | -1 | 8 |
| [wukongdnc/server/util.py](/wukongdnc/server/util.py) | Python | 24 | -26 | 1 | -1 |
| [wukongdnc/wukong/channel.py](/wukongdnc/wukong/channel.py) | Python | 44 | -50 | 6 | 0 |
| [wukongdnc/wukong/client_server.py](/wukongdnc/wukong/client_server.py) | Python | 3 | -3 | 0 | 0 |
| [wukongdnc/wukong/dc_executor.py](/wukongdnc/wukong/dc_executor.py) | Python | 6 | -6 | 0 | 0 |
| [wukongdnc/wukong/fanin_synchronizer.py](/wukongdnc/wukong/fanin_synchronizer.py) | Python | 5 | -7 | 2 | 0 |
| [wukongdnc/wukong/handlerDAG.py](/wukongdnc/wukong/handlerDAG.py) | Python | 33 | 18 | 10 | 61 |
| [wukongdnc/wukong/invoker.py](/wukongdnc/wukong/invoker.py) | Python | 90 | 2 | 20 | 112 |
| [wukongdnc/wukong/memoization/memoization_controller.py](/wukongdnc/wukong/memoization/memoization_controller.py) | Python | 61 | -72 | 11 | 0 |
| [wukongdnc/wukong/old_invoker.py](/wukongdnc/wukong/old_invoker.py) | Python | 19 | -23 | 4 | 0 |
| [wukongdnc/wukong/synchronization/counting_semaphore.py](/wukongdnc/wukong/synchronization/counting_semaphore.py) | Python | 35 | -37 | 2 | 0 |
| [wukongdnc/wukong/wukong_problem.py](/wukongdnc/wukong/wukong_problem.py) | Python | 52 | -65 | 13 | 0 |

[Summary](results.md) / [Details](details.md) / [Diff Summary](diff.md) / Diff Details