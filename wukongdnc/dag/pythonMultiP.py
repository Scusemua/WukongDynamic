# Old program for testing hoe multiprocessing works

#import queue
from multiprocessing import Process, Manager, Queue, Value
import cloudpickle

import logging 
from .addLoggingLevel import addLoggingLevel
""" How to use: https://stackoverflow.com/questions/2183233/how-to-add-a-custom-loglevel-to-pythons-logging-facility/35804945#35804945
    >>> addLoggingLevel('TRACE', logging.DEBUG - 5)
    >>> logging.getLogger(__name__).setLevel("TRACE")
    >>> logging.getLogger(__name__).trace('that worked')
    >>> logging.trace('so did this')
    >>> logging.TRACE
"""
logger = logging.getLogger(__name__)
addLoggingLevel('TRACE', logging.DEBUG - 5)

# old: has been updated, but this is fine for this test
class DAG_Info(object):
    def __init__(self):
        self.DAG_info = input_DAG_info()
    def get_DAG_map(self):
        return self.DAG_info["DAG_map"]
    def get_DAG_states(self):
        return self.DAG_info["DAG_states"]
    def get_all_fanin_task_names(self):
        return self.DAG_info["all_fanin_task_names"]
    def get_all_fanin_sizes(self):
        return self.DAG_info["all_fanin_sizes"]
    def get_all_faninNB_task_names(self):
        return self.DAG_info["all_faninNB_task_names"]
    def get_all_faninNB_sizes(self):
        return self.DAG_info["all_faninNB_sizes"]
    def get_DAG_leaf_tasks(self):
        return self.DAG_info["DAG_leaf_tasks"]
    def get_DAG_leaf_task_start_states(self):
        return self.DAG_info["DAG_leaf_task_start_states"]
    def get_DAG_leaf_task_inputs(self):
        return self.DAG_info["DAG_leaf_task_inputs"]
    def get_DAG_tasks(self):
        return self.DAG_info["DAG_tasks"]

def input_DAG_info():
    with open('./DAG_info.pickle', 'rb') as handle:
        DAG_info = cloudpickle.load(handle)
    return DAG_info

class FanInNB(object):	
	def __init__(self, n, lock):	
		self.n = n
		self.val = Value('i', 0)
		self.lock = lock

	def fan_in(self):
		with self.lock:
			if self.val.value < (self.n - 1):
				self.val.value += 1
				return 0
			else:
				return 1

	def value(self):
		with self.lock:
			return self.val.value


synch_dict = {}

def multi_DAG_executor(q,i,data_dict):
	print("process " + str(i) + " running")
	#DAG_info = DAG_Info()
	state = q.get()
	q.put(state+1)
	print("process " + str(i) + " got state " + str(state))	
	data_dict[str(i)] = i
	#return_value = fanin.fan_in()
	#print("process " + str(i) + " synch_dict:" + str(synch_dict.items()))
	#fanin = synch_dict[i]
	#return_value = fanin.fan_in()
	#print("process " + str(i) + " fanin returned " + str(return_value))	
	return

def run():
	num_DAG_tasks = 10	# DAG_info.num_DAG_tasks

	q = Queue(maxsize = num_DAG_tasks)

	num_processes = 2	# multiprocessing.cpu_count() - 1

	manager = Manager()
	data_dict = manager.dict()
	#lock = manager.Lock()
	#lockA = multiprocessing.Lock()
	#lockB = multiprocessing.Lock()
	#faninA = FanInNB(2,lockA)
	#faninB = FanInNB(2,lockB)
	#synch_dict = manager.dict()
	#synch_dict[0]= faninA
	#synch_dict[1]= faninB
	#print("run: synch_dicts:" + str(synch_dict.items()))

	#Consider: dict of value

	q.put(1)
	
	# server stuff
	# create all syncch objects in a dictionary
	#>>> c = dict(zip(['one', 'two', 'three'], [1, 2, 3]))
	#>>> d = dict([('two', 2), ('one', 1), ('three', 3)])
	# lists/tuples names objects

	for i in range(num_processes):
		p = Process(target=multi_DAG_executor, args=(q,i,data_dict))
		p.start()

	for _ in range(2):
		p.join()

	print("data_dict.items: " + str(data_dict.items()))
	
	#Note: q has a 3 in it at the end
	
	#print("value of 0: " + str(data_dict['0']))
	#print("value of 1: " + str(data_dict['1']))
		
# where:
"""
	def multi_DAG_executor(q,DAG_info,ID,data_dict):

		while (True):
			logger.trace ("access DAG_map with state " + str(DAG_executor_State.state))
			#state_info = DAG_info.DAG_map[DAG_executor_State.state]
			DAG_map = DAG_info.get_DAG_map()
			state_info = DAG_map[DAG_executor_State.state]
			##logger.trace ("access DAG_map with state " + str(state))
			##state_info = DAG_info.DAG_map[state]

			logger.trace("state_info: " + str(state_info) + " execute task: " + state_info.task_name)

			# Example:
			# 
			# task = (func_obj, "task1", "task2", "task3")
			# func = task[0]
			# args = task[1:] # everything but the 0'th element, ("task_id1", "taskid2", "taskid3")
			#
			# # Intermediate data; from executing other tasks.
			# # task IDs and their outputs
			# data_dict = {
			#     "task1": 1, 
			#     "task2": 10,
			#     "task3": 3
			# }
			#
			# args2 = pack_data(args, data_dict) # (1, 10, 3)
			# func(*args2)

			# using map DAG_tasks from task_name to task
			DAG_tasks = DAG_info.get_DAG_tasks()
			task = DAG_tasks[state_info.task_name]
			task_inputs = state_info.task_inputs

			is_leaf_task = state_info.task_name in DAG_info.get_DAG_leaf_tasks()
			if not is_leaf_task:
				# task_inputs is a tuple of task_names
				args = pack_data(task_inputs, data_dict)
			else:
			# task_inputs is a tuple of input values, e.g., '1'
				args = task_inputs

			#output = execute_task(task,input)
			output = execute_task(task,args)
			#where:
			#	def execute_task(task,args):
			#		logger.trace("input of execute_task is: " + str(args))
			#		#output = task(input)
			#		output = task(*args)
			#		return output
			
			logger.trace("execute_task output: " + str(output))
			data_dict[state_info.task_name] = output

			if

			elif

			elif

        	else:
            	logger.trace("state " + str(DAG_executor_State.state) + " after executing task " +  state_info.task_name 
					+ " has no fanouts, fanins, or faninNBs; return")
            	##logger.trace("state " + str(state) + " after executing task " +  state_info.task_name 
					+ " has no fanouts, fanins, or faninNBs; return")
					
			# ToDo: q.put(-1)
            return

"""		
			
			
if __name__ == "__main__":
	run()
