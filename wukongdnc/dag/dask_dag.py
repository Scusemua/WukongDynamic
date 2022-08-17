from distributed import LocalCluster, Client
import dask
from collections import defaultdict
from .DFS_visit import Node

def execute_task(task_obj: tuple, existing_results: dict = {}, task_id: str = None):
  """
  Execute the given task.
  
  The `task_obj` parameter is expected to be a tuple. 
    The first element of the tuple will be the function to be executed.
    The remaining elements are the inputs to that function.
  
  The `existing_results` is expected to be a dictionary.
    The keys are task IDs.
    The values are the outputs generated by those tasks.
  
  Returns the result of executing the task.
  
  Will store the output in the `existing_results` dictionary if the task key of the task to be executed is provided.
  """
  func_obj = task_obj[0]  # func object
  inputs = task_obj[1:]   # tuple of 0 or more values
  
  return execute_task(func_obj, inputs, existing_results = existing_results, task_id = task_id)

def execute_task(func_obj, inputs: tuple, existing_results: dict = {}, task_id: str = None):
  """
  Execute the given task.
  
  The `func_obj` parameter is expected to be a callable function object.
  
  The `inputs` parameter is a tuple of 0 or more inputs.
  
  The `existing_results` is expected to be a dictionary.
    The keys are task IDs.
    The values are the outputs generated by those tasks.
  
  Returns the result of executing the task.
  
  Will store the output in the `existing_results` dictionary if the task key of the task to be executed is provided.
  """  
  # If the inputs are strings, then they refer to the outputs of other tasks.
  # Replace the strings (which refer to Task IDs) with the result of the associated task.
  for i in range(0, len(inputs)):
    if type(inputs[i]) is str:
      input_task_id = inputs[i]
      
      # If we do not have any data for the input task, then raise an exception.
      if input_task_id not in existing_results:
        raise ValueError("We do not have any data for task %s" % input_task_id)
      
      # Replace the task ID with the output of the associated task.
      inputs[i] = existing_results[input_task_id]
  
  res = func_obj(*inputs)
  
  # If the ID of the task we just executed was provided, then store the result in the `existing_results` dictionary.
  if task_id is not None:
    existing_results[task_id] = res 
  
  return res

if __name__ == "__main__":
  lc = LocalCluster()
  c = Client(lc)
  s = lc.scheduler

  def add(x, y):
    return x + y

  def triple(x):
    return 3 * x

  def square(x):
    return x ** 2

  def multiply(x, y, z):
    return x * y * z

  def divide(x):
    quotient = x / 72
    print("quotient: " + str(quotient))
    return quotient

  def increment(x):
    return x + 1

  inc0 = dask.delayed(increment)(0)
  inc1 = dask.delayed(increment)(1)
  trip = dask.delayed(triple)(inc1)
  sq = dask.delayed(square)(inc1)
  ad = dask.delayed(add)(inc1, inc0)
  mult = dask.delayed(multiply)(trip, sq, ad)
  div = dask.delayed(divide)(mult)

  graph = div.__dask_graph__()
  graph.layers

  nodes = [] 
  nodes_map = {}
  dependencies = graph.dependencies
  dependents = defaultdict(list)

  # Compute the dependents (i.e., successors) of each node in the DAG.
  for task, deps in graph.dependencies.items():
    for dep in deps:
      dependents[dep].append(task)

  # For each of the DAG nodes, create a Node object.
  for task, layer in graph.layers.items():
    node = Node(pred = list(dependencies[task]), succ = dependents[task], 
      #rhc
      task_name = task, task = layer[task][0], task_inputs = layer[task][1:])
    nodes_map[task] = node 
    nodes.append(node)

  for node in nodes:
    for i in range(0, len(node.pred)):
      node.pred[i] = nodes_map[node.pred[i]]
    for i in range(0, len(node.succ)):
      node.succ[i] = nodes_map[node.succ[i]]

  # Then for each node in the list, do node.generate_ops().
  #for node in nodes:
  #  node.generate_ops()

  # Then at the end, call Node.save_DAG_info(), which saves a 
  # pickle() file of stuff that was accumulated during DFS search.
  # Node.save_DAG_info()

  # Uncomment this line to generate an image showing the DAG.
  # dag_image = div.visualize("dag.png")

  result = div.compute()

  print("Result of executing the workload:", result)

  # This prints a mapping from Task ID to the 'Task' object.
  # The 'Task' object is a n-tuple. 
  # The first element of the n-tuple is the function to be executed.
  # The remaining elements are the inputs.
  # For leaf tasks, the inputs will be the actual numerical values to be input to the function.
  # For non-leaf tasks, the inputs will be the IDs of other tasks (strings).

  task_objects_map = graph.layers         # Map from Task ID to the task objects.
  task_objects = []                       # List of all the task objects. 
  leaf_tasks_map = {}                     # Mapping from task ID to task object for leaf tasks.
  leaf_task_ids = []                      # List of task IDs of leaf tasks.
  leaf_tasks = []                         # List of task objects, where each task object is a leaf task.
  leaf_nodes = []

  # Compute the leaf tasks.
  for task_id, deps in graph.dependencies.items():
    if len(deps) == 0:
      leaf_task_ids.append(task_id)
      leaf_tasks.append(graph[task_id])
      leaf_tasks_map[task_id] = graph[task_id]
  
  # Grab the inputs for the leaf tasks and put them in the appropriate nodes.
  for leaf_task_id in leaf_task_ids:
    node = nodes_map[leaf_task_id]
    # rhc
    #node.set_task_inputs(task_objects_map[leaf_task_id][leaf_task_id][1:])
    leaf_nodes.append(node)

  print("")

  print("Leaf Task IDs:", str(leaf_task_ids))
  
  print("")
  
  print("Leaf Tasks:")
  for leaf_task_id, leaf_task in leaf_tasks_map.items():
    print("\t %s: %s" % (leaf_task_id, str(leaf_task)))
    
  print("")

  print("Mapping from Task ID to the 'Task' object")
  print("This shows task keys mapped to task objects.")
  print("The first element of the task object is the function to be executed.")
  print("The remaining elements are the input to the tasks.")
  print("The inputs may be task IDs. This specifies data dependencies between tasks.\n")
  for k,v in graph.layers.items():
    print("%s: %s" % (k,v[k]))
    task_objects.append(v[k])
  
  DFS_nodes = []

  def dfs(visited, node):  #function for dfs 
    task_name = node.get_task_name() 
    if task_name not in visited:
      print("DFS: visit task: " + task_name)
      DFS_nodes.append(node)
      visited.add(task_name)
      dependents = node.get_succ()
      for dependent_node in dependents:
        dependent_task_name = dependent_node.get_task_name() 
        print("neighbor_task_name: " + dependent_task_name)
        dfs(visited, nodes_map[dependent_task_name])

  visited = set() # Set to keep track of visited nodes of DAG.

  for leaf_node in leaf_nodes:
    dfs(visited, leaf_node)

  print()
  print("DFS_nodes:")
  for n in DFS_nodes:
    print(n.get_task_name())

  print()
  print("generate ops for DFS_nodes:")        
  for n in DFS_nodes:  
    print("generate_ops for: " + n.get_task_name())
    n.generate_ops()
      
  Node.save_DAG_info()  