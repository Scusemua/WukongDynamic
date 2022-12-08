# Configuraion:
#
# True if we are not using Lambdas, i.e., executing tasks with threads or processes
# local, i.e., on one machine.
run_all_tasks_locally = True         # vs run tasks remotely (in Lambdas)
# True if synch objects are stored locally, i.e., in the memory of the single
# machine on which the threads are executing.  If we are using multiprocessing
# or Lambdas, this must be False. When False, the synch objects are stored
# on the tcp_server or in InfiniX lambdas.
store_fanins_faninNBs_locally = False    # vs remotely
# True when all FanIn and FanInNB objects are created locally or on the
# tcp_server or IniniX all at once at the start of the DAG execution. If
# False, synch objects are created on the fly, i.e, we execute create-and-fanin
# operations that create a synch object if it has not been created yet and then
# execute a Fan_in operaation on the created object.
create_all_fanins_faninNBs_on_start = True
# True if the DAG is executed by a "pool" of threads/processes. False, if we are
# using Lambdas or we are using threads to simulate the use of Lambdas. In the latter
# case, instead of, e.g., starting a Lambda at fan_out operations, we start a thread.
# This results in the creation of many threads and is only use to test the logic 
# of the Lambda code.
using_workers = True
# True when we ae not using Lambas and tasks are executed by threads instead of processes. 
# False when we are not using lambdas and are using multiprocesssing 
using_threads_not_processes = False
# When using_workers, this is how many threads or processes in the pool.
num_workers = 2
use_multithreaded_multiprocessing = True
num_threads_for_multithreaded_multiprocessing = 2

# if using lambdas to store synch objects, run tcp_server_lambda.
# if store in regular python functions instead of real Lambdas
# set using_Lambda_Function_Simulator = True
FanIn_Type = "DAG_executor_FanIn"
FanInNB_Type = "DAG_executor_FanInNB"
process_work_queue_Type = "BoundedBuffer"
# FanIn_Type = "DAG_executor_FanIn_Select"
# FanInNB_Type = "DAG_executor_FanInNB_Select"
# process_work_queue_Type = "BoundedBuffer_Select"

# if running real lambdas or storing synch objects in real lambdas:
#
# Set SERVERLESS_SYNC to True or False in wukongdnc constants !!!!!!!!!!!!!!
#

# Currently, this is for storing synch objects in simulated lambdas;
using_Lambda_Function_Simulators_to_Store_Objects = True
# use orchestrator to invoke functions (e.g., when all fanin/fanout results are available)
using_DAG_orchestrator = True
# use a single lambda function to store all of the synchroization objects
# to make an easy test case. This cannot be used when using the function 
# simulators or using the DAG_orchestrator
using_single_lambda_function = False

A1 = A1_Server = A1_FunctionSimulator = A1_SingleFunction = A1_Orchestrator = False
A2 = False
A3 = A3_S = A3_FunctionSimulator = A3_SingleFunction = A3_Orchestrator = False
A4_L = A4_R = False
A5 = A6 = False

# These are used to shorten the expressions in the configurations
not_using_lambda_options = not using_Lambda_Function_Simulators_to_Store_Objects and not using_DAG_orchestrator and not using_single_lambda_function
A1 = not run_all_tasks_locally and not using_workers and not store_fanins_faninNBs_locally
A3 = run_all_tasks_locally and not using_workers and not store_fanins_faninNBs_locally

# configurations
A1_Server = A1 and not_using_lambda_options
# FanIn_Type = "DAG_executor_FanIn"
# FanInNB_Type = "DAG_executor_FanInNB"
# run tcp_server
# Set SERVERLESS_SYNC to True in wukongdnc constants
A1_FunctionSimulator = A1 and using_Lambda_Function_Simulators_to_Store_Objects and not using_single_lambda_function
# FanIn_Type = "DAG_executor_FanIn_Select"
# FanInNB_Type = "DAG_executor_FanInNB_Select"
# run tcp_server_lambda
# Set SERVERLESS_SYNC to True in wukongdnc constants
A1_SingleFunction = A1 and using_single_lambda_function and not using_Lambda_Function_Simulators_to_Store_Objects
# FanIn_Type = "DAG_executor_FanIn_Select"
# FanInNB_Type = "DAG_executor_FanInNB_Select"
# run tcp_server_lambda
# Set SERVERLESS_SYNC to True in wukongdnc constants
A1_Orchestrator = A1 and using_Lambda_Function_Simulators_to_Store_Objects and using_DAG_orchestrator and not using_single_lambda_function
# FanIn_Type = "DAG_executor_FanIn_Select"
# FanInNB_Type = "DAG_executor_FanInNB_Select"
# run tcp_server_lambda
# Set SERVERLESS_SYNC to True in wukongdnc constants

A2 = run_all_tasks_locally and not using_workers and store_fanins_faninNBs_locally and not_using_lambda_options
# set FanIn_Type = "DAG_executor_FanIn_Select" or "DAG_executor_FanIn"
# set FanInNB_Type = "DAG_executor_FanInNB_Select" or "DAG_executor_FanInNB"
# Set SERVERLESS_SYNC to False in wukongdnc constants

# Note: Currently we are assuming sing_Lambda_Function_Simulators_to_Store_Objects
# to do the orchestrator. 
# ToDo:
# We store objects in lmabdas when we use tcp_server_lambda, so we don't
# have a store_sync_objects_in_servers config variable. We can create oen and
# tcp_server_lambda can make sure it is set?

A3_Server = A3 and not_using_lambda_options
# set FanIn_Type = = "DAG_executor_FanIn_Select" or "DAG_executor_FanIn"
# set FanInNB_Type = "DAG_executor_FanInNB_Select" or "DAG_executor_FanInNB"
# Set SERVERLESS_SYNC to False in wukongdnc constants
A3_FunctionSimulator = A3 and using_Lambda_Function_Simulators_to_Store_Objects and not using_single_lambda_function and not using_DAG_orchestrator
# FanIn_Type = "DAG_executor_FanIn_Select"
# FanInNB_Type = "DAG_executor_FanInNB_Select"
# run tcp_server_lambda
# Set SERVERLESS_SYNC to False in wukongdnc constants
A3_SingleFunction = A3 and using_single_lambda_function and not using_Lambda_Function_Simulators_to_Store_Objects
# FanIn_Type = "DAG_executor_FanIn_Select"
# FanInNB_Type = "DAG_executor_FanInNB_Select"
# run tcp_server_lambda
# Set SERVERLESS_SYNC to False in wukongdnc constants
A3_Orchestrator = A3_FunctionSimulator and using_DAG_orchestrator
# FanIn_Type = "DAG_executor_FanIn_Select"
# FanInNB_Type = "DAG_executor_FanInNB_Select"
# run tcp_server
# Set SERVERLESS_SYNC to False in wukongdnc constants

A4_Local = run_all_tasks_locally and using_workers and using_threads_not_processes and store_fanins_faninNBs_locally and not_using_lambda_options
# set num_workers
# no tcp_server since storing locally
# Set SERVERLESS_SYNC to False in wukongdnc constants
A4_Remote = run_all_tasks_locally and using_workers and using_threads_not_processes and not store_fanins_faninNBs_locally and not_using_lambda_options
# set num_workers
# set FanIn_Type = "DAG_executor_FanIn_Select" or "DAG_executor_FanIn"
# FanInNB_Type = "DAG_executor_FanInNB_Select" or "DAG_executor_FanInNB"
# set process_work_queue_Type = "BoundedBuffer_Select" or process_work_queue_Type = "BoundedBuffer"
# run tcp_server
# Set SERVERLESS_SYNC to False in wukongdnc constants

A5 = run_all_tasks_locally and using_workers and not using_threads_not_processes and not store_fanins_faninNBs_locally and not_using_lambda_options
# set num_workers
# set FanIn_Type = "DAG_executor_FanIn_Select" or "DAG_executor_FanIn"
# set FanInNB_Type = "DAG_executor_FanInNB_Select" or "DAG_executor_FanInNB"
# set process_work_queue_Type = "BoundedBuffer_Select" or process_work_queue_Type = "BoundedBuffer"
# run tcp_server
# Set SERVERLESS_SYNC to False in wukongdnc constants

A6 = run_all_tasks_locally and use_multithreaded_multiprocessing and using_workers and not using_threads_not_processes and not store_fanins_faninNBs_locally and not_using_lambda_options
# set num_threads_for_multithreaded_multiprocessing
# set FanIn_Type = "DAG_executor_FanIn_Select" or "DAG_executor_FanIn"
# set FanInNB_Type = "DAG_executor_FanInNB_Select" or "DAG_executor_FanInNB"
# run tcp_server

# For all configurations:
# set create_all_fanins_faninNBs_on_start

not_A1s = not A1_FunctionSimulator and not A1_SingleFunction and not A1_Orchestrator
not_A2 = not A2
not_A3s = not A3_Server and not A3_FunctionSimulator and not A3_SingleFunction and not A3_Orchestrator 
not_A4s = not A4_L and not A4_R
not_A5 = not A5
not_A6 = not A6
if not_A1s and not_A2 and not_A3s and not_A4s and not_A5 and not_A6:
    pass


