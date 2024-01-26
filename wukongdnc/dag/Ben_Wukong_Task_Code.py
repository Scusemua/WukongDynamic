# When a lambda starts, if it is not a leaf task put the inputs
# in the data dictionary

if not is_leaf_task:
    # lambdas invoked with inputs. We do not add leaf task inputs to the data
    # dictionary, we use them directly when we execute the leaf task.

    dict_of_results = payload['input']
    for key, value in dict_of_results.items():
        data_dict[key] = value

else:
    # leaf task inputs are not in a dictionary.
    inp = payload['input']    


# This is the code involving task execution. The option
# "tasks_use_result_dictionary_parameter" is true when we
# are running pagerank tasks. 
# Also option "same_output_for_all_fanout_fanin" is true when 
# we are splitting a task;s output into separate outputs for
# each fanin/fanout.
          
# ****************
# Note: My task_input values are qualifed names, e.g,, "PR1_1-PR2_1"
# since I generate them not Dask. Dask will use normal task names 
# whch for this example is "PR2_1". You need to change the names in the 
# task_inputs tuple to be qualifed names by adding the current task
# name as a prefix.
# ****************

task_inputs = state_info.task_inputs   

# Qualify the names in task inputs:
# list_of_qualified_names = []
# for simple name in task_inputs:
#   qualified_name = state_info.task_name + "-" + simple_name
#   list_of_qualified_names.append(qualified_name)
# task_inputs = tuple(list_of_qualified_names)

is_leaf_task = state_info.task_name in DAG_info.get_DAG_leaf_tasks()
logger.info("is_leaf_task: " + str(is_leaf_task))
logger.info("task_inputs: " + str(task_inputs))

# Note: For DAG generation, for each state we execute a task and 
# for each task T we have to say what T;' task_inputs are - these are the 
# names of tasks that give inputs to T. When we have per-fanout output
# instead of having the same output for all fanouts, we specify the 
# task_inputs as "sending task - receiving task". So a sending task
# S might send outputs to fanouts A and B so we use "S-A" and "S-B"
# as the task_inputs, instead of just using "S", which is the Dask way.
result_dictionary =  {}
if not is_leaf_task:
    logger.info("Packing data. Task inputs: %s. Data dict (keys only): %s" % (str(task_inputs), str(data_dict.keys())))
    # task_inputs is a tuple of task_names
    args = pack_data(task_inputs, data_dict)
    logger.info(thread_name + " argsX: " + str(args))
    if tasks_use_result_dictionary_parameter:
        logger.info("Foo1a")
        # task_inputs = ('task1','task2'), args = (1,2) results in a result_dictionary
        # where result_dictionary['task1'] = 1 and result_dictionary['task2'] = 2.
        # We pass a result_dictionary of inputs instead of the *args tuple (1,2).

        #asssert
        # Should be true
        if len(task_inputs) == len(args):
            logger.info("Foo1b")
            result_dictionary = {task_inputs[i] : args[i] for i, _ in enumerate(args)}
            logger.info(thread_name + " result_dictionaryX: " + str(result_dictionary))
        #else:
            # Internal Error

else:
    args = task_inputs
    if tasks_use_result_dictionary_parameter:
        # Passing am emoty inut tuple to the PageRank task,
        # This results in a rresult_dictionary
        # of "DAG_executor_driver_0" --> (), where
        # DAG_executor_driver_0 is used to mean that the DAG_excutor_driver
        # provided an empty input tuple for the leaf task. In the 
        # PageRank_Function_Driver we just ignore empty input tuples so 
        # that the input_tuples provided to the PageRank_Function will be an empty list.
        result_dictionary['DAG_executor_driver_0'] = ()

logger.info("argsZ: " + str(args))
logger.info(thread_name + " result_dictionaryZ: " + str(result_dictionary))

# using the map DAG_tasks to map from from task_name to task
task = DAG_tasks[state_info.task_name]
if not tasks_use_result_dictionary_parameter:
    # we will call the task with tuple args and unfold args: task(*args)
    output = execute_task(task,args)
else:
    output = execute_task_with_result_dictionary(task,state_info.task_name,20,result_dictionary)
""" where:
    def execute_task(task,args):
        logger.info("input of execute_task is: " + str(args))
        output = task(*args)
        return output
        
    def execute_task_with_result_dictionary(task,task_name,resultDictionary):
        output = task(task_name,resultDictionary)
        return output
"""

"""
PageRank output is a dictionary mapping the task name to a list of tuples.
This is the PR1_1 output:
    output = {'PR2_1': [(2, 0.0075)], 'PR2_2': [(5, 0.010687499999999999)], 'PR2_3': [(3, 0.012042187499999999)]}
Normally, we put it in the data_dict as
    data_dict["PR1_1"] = output
but we then send this output to PR1_1's fanout tasks. For Dask tasks,
we fanout all non-become tasks with the same output. For pagerank we
fanout tasks with their individual outputs. Currently, for each fanouts
we send a dictionary of results to the fanout task (as part of the Lambda's
payload)
    dict_of_results =  {}
    dict_of_results[calling_task_name] = output
For example, for calling_task_name = "P1_1"  with PA1_1's output in "output":
    dict_of_results[calling_task_name] = output.
The fanout task will get all of PR1_1's output. 
But we want to split PR1_1's output instead. If variable name holds the 
name "PR2_1" of the task being fanned out by PR1_1:
    dict_of_results[str(calling_task_name+"-"+name)] = output[name]
Here the output of PR1_1 is a dictionary tha maps the name of a fanout
task to its part of PA1_1's output
The dict_of_results will map "PR1-1-PR2_1" to the value in dictionary
output whose key is "PR2_1".
`  """

logger.info(thread_name + " executed task " + state_info.task_name + "'s output: " + str(output))
if same_output_for_all_fanout_fanin:
    # do not split the output - each fanout/fanin gets all of the output
    data_dict[state_info.task_name] = output
else:
#   Example: task PR1_1 producs an output for fanouts PR2_1
#   and PR2_3 and faninNB PR2_2. Output is a dictionary mapping fanout task name
#   to list
#       output = {'PR2_1': [(2, 0.0075)], 'PR2_2': [(5, 0.010687499999999999)], 'PR2_3': [(3, 0.012042187499999999)]}
    for (k,v) in output.items():
        # example: state_info.task_name = "PR1_1" and 
        # k is "PR2_3" so data_dict_key is "PR1_1-PR2_3"
        data_dict_key = str(state_info.task_name+"-"+k)
        data_dict_value = v
        data_dict[data_dict_key] = data_dict_value

logger.info("data_dict: " + str(data_dict))  


"""
Sample Output for leaf task PR1_1:
[2023-04-13 09:28:15,948] [Thread_leaf_ss1] DEBUG: is_leaf_task: True
[2023-04-13 09:28:15,949] [Thread_leaf_ss1] DEBUG: task_inputs: ()
[2023-04-13 09:28:15,950] [Thread_leaf_ss1] DEBUG: argsZ: ()
[2023-04-13 09:28:15,950] [Thread_leaf_ss1] DEBUG: Thread_leaf_ss1 result_dictionaryZ: {'DAG_executor_driver_0': ()}
[2023-04-13 09:28:15,951] [Thread_leaf_ss1] DEBUG: Thread_leaf_ss1: execute_task_with_result_dictionary: input of execute_task is: {'DAG_executor_driver_0': ()}
PageRank output tuples for PR1_1:
('PR2_1', [(2, 0.0075)]) ('PR2_2L', [(5, 0.010687499999999999)]) ('PR2_3', [(3, 0.012042187499999999)])

PageRank result for PR1_1: 5:0.0075 17:0.010687499999999999 1:0.012042187499999999

[2023-04-13 09:28:15,956] [Thread_leaf_ss1] DEBUG: Thread_leaf_ss1 executed task PR1_1's output: {'PR2_1': [(2, 0.0075)], 'PR2_2L': [(5, 0.010687499999999999)], 'PR2_3': [(3, 0.012042187499999999)]}
[2023-04-13 09:28:15,957] [Thread_leaf_ss1] DEBUG: data_dict: {'PR1_1-PR2_1': [(2, 0.0075)], 'PR1_1-PR2_2L': [(5, 0.010687499999999999)], 'PR1_1-PR2_3': [(3, 0.012042187499999999)]}
[
"""

"""
Sample output for executing task PR2_1:
[2023-04-13 09:28:15,988] [Thread_leaf_ss1] DEBUG: Thread_leaf_ss1 execute task: PR2_1
[2023-04-13 09:28:15,989] [Thread_leaf_ss1] DEBUG: is_leaf_task: False
[2023-04-13 09:28:15,993] [Thread_leaf_ss1] DEBUG: task_inputs: ('PR1_1-PR2_1',)
[2023-04-13 09:28:15,996] [Thread_leaf_ss1] DEBUG: Packing data. Task inputs: ('PR1_1-PR2_1',). Data dict (keys only): dict_keys(['PR1_1-PR2_1', 'PR1_1-PR2_2L', 'PR1_1-PR2_3'])
[2023-04-13 09:28:15,998] [Thread_leaf_ss1] DEBUG: Thread_leaf_ss1 argsX: ([(2, 0.0075)],)
[2023-04-13 09:28:16,000] [Thread_leaf_ss1] DEBUG: Foo1a
[2023-04-13 09:28:16,001] [Thread_leaf_ss1] DEBUG: Foo1b
[2023-04-13 09:28:16,002] [Thread_leaf_ss1] DEBUG: Thread_leaf_ss1 result_dictionaryX: {'PR1_1-PR2_1': [(2, 0.0075)]}
PageRank output tuples for PR2_3:
[2023-04-13 09:28:16,003] [Thread_leaf_ss1] DEBUG: argsZ: ([(2, 0.0075)],)
('PR3_3', [(2, 0.0075), (0, 0.010687499999999999)])
[2023-04-13 09:28:16,005] [Thread_leaf_ss1] DEBUG: Thread_leaf_ss1 result_dictionaryZ: {'PR1_1-PR2_1': [(2, 0.0075)]}

[2023-04-13 09:28:16,010] [Thread_leaf_ss1] DEBUG: Thread_leaf_ss1: execute_task_with_result_dictionary: input of execute_task is: {'PR1_1-PR2_1': [(2, 0.0075)]}
PageRank result for PR2_3: 4:0.0075 6:0.010687499999999999 14:0.012042187499999999 12:0.02797171875        
"""

# When T does a fanout or fanin to a task V, I split T's output and only send V's part
# of T's output:

dict_of_results =  {}
if same_output_for_all_fanout_fanin:
    # each fanout of a task T gets the same output, i.e., 
    # the entire output of T. Here, calling_task_name would be T
    dict_of_results[calling_task_name] = output
else:
    # Each fanout of a task T gets its own output; use qualified names
    # e.g., "PR1_1-PR2_3" as the calling task instead of 
    # just "PR1_1". The output of T is a dictionary
    # where keys are fanout/faninNB names and the values are
    # the outputs for that fanout/faninNB.

    # In the above example, name is "V"
    qualfied_name = str(calling_task_name) + "-" + str(name)
    dict_of_results[qualfied_name] = output[name]