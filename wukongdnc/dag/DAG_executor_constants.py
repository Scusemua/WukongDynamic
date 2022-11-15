# Configuraion:
#
# True if we are not using Lambdas, i.e., executing tasks with threads or processes
# locall, i.e., on one machine.
run_all_tasks_locally = True         # vs remotely (in Lambdas)
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
using_workers = False
# True when we ae not using Lambas and tasks are executed by threads instead of processes. 
# False when we are not using lambdas and are using multiprocesssing 
using_threads_not_processes = True
# When using_workers, this is how many threads or processes in the pool.
using_lambdas = False   # == not run_all_tasks_locally
num_workers = 2
use_multithreaded_multiprocessing = False
num_threads_for_multithreaded_multiprocessing = 2

# if using lambdas to store synch objects, run tcp_server_lambda.
# if store in regular python functions instead of real Lambdas
# set using_Lambda_Function_Simulator = True
#FanIn_Type = "DAG_executor_FanIn"
#FanInNB_Type = "DAG_executor_FanInNB"
#process_work_queue_Type = "BoundedBuffer"
FanIn_Type = "DAG_executor_FanIn_Select"
FanInNB_Type = "DAG_executor_FanInNB_Select"
process_work_queue_Type = "BoundedBuffer_Select"
# Note: Currently, this is for storing synch objects in simulated lambdas;
# we are not running real lambdas or storing synch objects in real lambdas.
using_Lambda_Function_Simulator = True
using_function_invoker = True
use_single_lambda_function = False