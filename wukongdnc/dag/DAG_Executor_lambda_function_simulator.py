#import re 
#import socket
import time
#import json 
import cloudpickle
import uuid
import threading
from threading import Lock

from wukongdnc.server.message_handler_lambda import MessageHandler
from .DAG_executor_State import DAG_executor_State
#from .DAG_info import DAG_Info
from wukongdnc.server.util import make_json_serializable
from .DAG_executor_constants import store_fanins_faninNBs_locally 
from .DAG_executor_constants import FanIn_Type, FanInNB_Type
from .DAG_executor_constants import using_single_lambda_function

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

#logger = logging.getLogger(__name__)
#logger.setLevel(logging.DEBUG)
#formatter = logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s')

#SLEEP_INTERVAL = 0.120

# used in real lambda handler, using it here too
warm_resources = {
	'cold_start_time': time.time(),
	'invocation_count': 0,
	'message_handler' : None
}

class Lambda_Function_Simulator:
	#def __init__(self):

	# called in tcp_server_lambda in invoke_lambda_synchronously to invke a simulated lambda.
	# Does what real lambda handler does
	def lambda_handler(self, payload):
		#start_time = time.time()
		global warm_resources
		invocation_time = time.time()
		warm_resources['invocation_count'] = warm_resources['invocation_count'] + 1
		#logger.debug("Invocation received. event: " + str(event))

		logger.debug(f'Lambda_Function_Simulator: lambda_handler: Invocation count: {warm_resources["invocation_count"]}, Seconds since cold start: {round(invocation_time - warm_resources["cold_start_time"], 1)}')

		# Extract all of the data from the payload.
		#json_message = cloudpickle.loads(base64.b64decode(event["json_message"]))
		json_message = payload['json_message']
		logger.debug("Lambda_Function_Simulator: lambda_handler: JSON message: " + str(json_message))

		if not warm_resources['message_handler']:
			# Issue: Can we get and print the name of the Lambda function - "LambdaBoundedBuffer" or "LambdaSemaphore"
			logger.debug("Lambda_Function_Simulator: lambda_handler: **************** Lambda function cold start ******************")
			warm_resources['message_handler'] = MessageHandler()
			#Issue: what if we lost a Lambda? If we have backup we can recover but how do we determine whether we failed?
		else:
			logger.debug("Lambda_Function_Simulator: lambda_handler: *************** warm start ******************")

		return_value = warm_resources['message_handler'].handle(json_message)

		return return_value

class DAG_orchestrator:
	def __init__(self):
		self.object_name_to_trigger_map = {}

	# create fanin and faninNB messages for creating all fanin and faninNB synch objects
	#
	# Not using for now - this method is in tcp_server_lamba and it
	# invokes the mapped to lambda for a given object name with a 
	# "create" msg so the object is created in that lamba.
	def create_fanin_and_faninNB_messages(self,DAG_map,DAG_states,DAG_info,all_fanin_task_names,all_fanin_sizes,all_faninNB_task_names,all_faninNB_sizes):
	
		"""
		logger.debug("SQS: create_fanin_and_faninNB_messages: size of all_fanin_task_names: " + str(len(all_fanin_task_names))
			+ " size of all_faninNB_task_names: " + str(len(all_faninNB_task_names)))
		logger.debug("SQS: create_fanin_and_faninNB_messages: size of all_fanin_sizes: " + str(len(all_fanin_sizes))
			+ " size of all_faninNB_sizes: " + str(len(all_faninNB_sizes)))
		logger.debug("SQS: create_fanin_and_faninNB_messages: all_faninNB_task_names: " + str(all_faninNB_task_names))
		"""

		fanin_messages = []

		# create a list of "create" messages, one for each fanin
		for fanin_name, size in zip(all_fanin_task_names,all_fanin_sizes):
			#logger.debug("iterate fanin: fanin_name: " + fanin_name + " size: " + str(size))
			# rhc: DES
			dummy_state = DAG_executor_State(function_name = "DAG_executor", function_instance_ID = str(uuid.uuid4()))
			# we will create the fanin object and call fanin.init(**keyword_arguments)
			dummy_state.keyword_arguments['n'] = size
			msg_id = str(uuid.uuid4())	# for debugging

			message = {
				"op": "create",
				"type": FanIn_Type,
				"name": fanin_name,
				"state": make_json_serializable(dummy_state),	
				"id": msg_id
			}
			fanin_messages.append(message)

		faninNB_messages = []

		# create a list of "create" messages, one for each faninNB
		for fanin_nameNB, size in zip(all_faninNB_task_names,all_faninNB_sizes):
			#logger.debug("iterate faninNB: fanin_nameNB: " + fanin_nameNB + " size: " + str(size))
			# rhc: DES
			dummy_state = DAG_executor_State(function_name = "DAG_executor", function_instance_ID = str(uuid.uuid4()))
			# passing to the fninNB object:
			# it size
			dummy_state.keyword_arguments['n'] = size
			# when the faninNB completes, if we are runnning locally and we are not pooling,
			# we start a new thread to execute the fanin task. If we are thread pooling, we put the 
			# start state in the work_queue. If we are using lambdas, we invoke a lambda to
			# execute the fanin task. If we are process pooling, then the last process to 
			# call fanin will put the start state of the fanin task in the work_queue. (FaninNb
			# cannot do this since the faninNB will be on the tcp_server.)
			dummy_state.keyword_arguments['start_state_fanin_task'] = DAG_states[fanin_nameNB]
			dummy_state.keyword_arguments['store_fanins_faninNBs_locally'] = store_fanins_faninNBs_locally
			dummy_state.keyword_arguments['DAG_info'] = DAG_info
			msg_id = str(uuid.uuid4())

			message = {
				"op": "create",
				"type": FanInNB_Type,
				"name": fanin_nameNB,
				"state": make_json_serializable(dummy_state),	
				"id": msg_id
			}
			faninNB_messages.append(message)

		logger.debug("SQS:create_fanin_and_faninNB_messages: number of fanin messages: " + str(len(fanin_messages))
			+ " number of faninNB messages: " + str(len(faninNB_messages)))

		return fanin_messages, faninNB_messages

	# for each sync object, we maintain a list of fanin/fanout operations that have been performed
	# and the size of the fanin or fanout (fanout is a fanout object with size 1)
	def map_object_name_to_trigger(self,object_name,n):
		empty_list = []
		# a pair, which is a list and a value n that represents the size
		list_n_pair = (empty_list,n)
		self.object_name_to_trigger_map[object_name] = list_n_pair

	# Whenever a fanin/fanout op is receievd by tcp_server_lambda via process_faninNBs_batch it
	# invokes the enqueue method of InfiniD, which invokes this enqueue of the DAG_orchestrator.
	def enqueue(self,json_message,simulated_lambda_function, simulated_lambda_function_lock):
		thread_name = threading.current_thread().name
		sync_object_name = json_message.get("name", None)
		list_n_pair = self.object_name_to_trigger_map[sync_object_name]
		list = list_n_pair[0]
		n = list_n_pair[1]
		# add the fanin/fannout op to the list of operations
		list.append(json_message)
		# if alln operations have been performed, invoke the function that contains the object
		if len(list) == n:
			msg_id = str(uuid.uuid4())
			dummy_state = DAG_executor_State(function_name = "DAG_executor", function_instance_ID = str(uuid.uuid4()))
			logger.debug("DAG_Orchestrator: Triggered: Sending 'process_enqueued_fan_ins' message to lambda function for " + sync_object_name)
			logger.debug("SDAG_Orchestrator: length of enqueue's list: " + str(len(list)))
			# we set state.keyword_arguments before call to create()
			message = {
				"op": "process_enqueued_fan_ins",
				"type": "DAG_executor_fanin_or_faninNB",
				# We are passing a list of operations to the lambda function, which will execute
				# the mone by one in the order they were received.
				"name": list,
				"state": make_json_serializable(dummy_state),
				"id": msg_id
			}
			#msg = json.dumps(message).encode('utf-8')
			#ToDo: if we are triggering the fanin task to run in the same lambda then we need to make sure
			# the lambda has the payload it needs to run, i.e., for sync object the payload is the message 
			# it normally gets, but if the sync object triggers a task to run in the same lambda, the payload
			# must also contain the info needed to run the task. This would be the payload info sent to
			# real lambdas by DAG_executor_lambda(payload), which is from Driver:
			"""
								lambda_DAG_exec_state = DAG_executor_State(function_name = "DAG_executor.DAG_executor_lambda", function_instance_ID = str(uuid.uuid4()), state = start_state)
								logger.debug ("DAG_executor_driver: lambda payload is DAG_info + " + str(start_state) + "," + str(inp))
								lambda_DAG_exec_state.restart = False      # starting new DAG_executor in state start_state_fanin_task
								lambda_DAG_exec_state.return_value = None
								lambda_DAG_exec_state.blocking = False            
								logger.info("DAG_executor_driver: Starting Lambda function %s." % lambda_DAG_exec_state.function_name)
			#ToDo: lambdas: 
								# We use "inp" for leaf task input otherwise all leaf task lambda Executors will 
								# receive all leaf task inputs in the leaf_task_inputs and in the state_info.task_inputs
								# - both are nulled out at beginning of driver. when we are using lambdas.
								# If we use "inp" then we will pass only a given leaf task's input to that leaf task. 
								# For non-lambda, each thread/process reads the DAG_info from a file. This DAG-info has
								# all the leaf task inputs in it so every thread/process reads all these inputs. This 
								# can be optimized if necessary, e.g., separate files for leaf tasks and non-leaf tasks.

								payload = {
									"input": inp,
									"DAG_executor_state": lambda_DAG_exec_state,
									"DAG_info": DAG_info
								}
			"""
			# So we need DAG_info, which InfiniD has and can pass to DAG_Orchestrator.
			# See below about: invoke (simulaed) function which creates object on the fly
			# and calls init() which needs DAG_info ,and whatever. So need to pass DAG_info
			# to (simulated) function (which is what Driver does when invoking a real 
			# lambda function). So the (simulated) function needs payoad for fan_in and 
			# payload for invoking real lambda, though they may overlap. If not triggering 
			# tasks then only need sync object payload. So the deployent for lambda
			# functions that store objects and trigger tasks is different than the deployment
			# for lambdas that just run tasks like Wukong. Former has a payload that has sync
			# object and task info. The triggering lambdas: create sync object on fly, init()
			# it as usual (with DAG_info and whatever) then will execute the case for triggering
			# task, whch it does by calling DAG_executor.DAG_executor_lambda(payload) with the
			# correct payload, created from faninNB info (results, DAG_info, ...)

			payload = {"json_message": message}

			# Do not allow parallel invocations. It's possible that parallel invocatins can never
			# be attempted using the scheme under development.
#ToDo: Allow calling real lambda based on options
#ToDo: if not create_functions_on-the-fly:
			# created all objects at start - create objects one by one and call init() passing kwargs
			with simulated_lambda_function_lock:
				try:
					logger.debug("DAG_Orchestrator enqueue: calling simulated_lambda_function.lambda_handler(payload)")
					# This is essentially a synchronous call to a regular Python function
# ToDo: Do we want to allow both sync and async calls? The latter done by creating a thread that does
# the call. Faster when we are invoking a real lambda, which we will do with invoke_lambda_asynch
					return_value = simulated_lambda_function.lambda_handler(payload)
					logger.debug("DAG_Orchestrator: called simulated_lambda_function.lambda_handler(payload)")
				except Exception as ex:
					logger.error("[ERROR]: " + thread_name + ": invoke_lambda_synchronously: Failed to run lambda handler for synch object: " + sync_object_name)
					logger.error(ex)

#   	else: 
			"""
			simulated_lambda_function = Lambda_Function_Simulator()
ToDo: since this Lambda_Function_Simulator() will create a fanin and this fanin may trigger its
task, we need to make sure the  will get the payload it needs to run tasks. Note that after we 
create the object we call init() and pass in DAG_info in case it needs to run a lambda.
Note that it is the (simulated) Lambda that creates the faninNB so the simulated function 
needs DAG_info on call to init(), and whatevr else init() gets.
Scheme: Lambda_Function_Simulator() does:
- create_if to create() one fanoutfaninNB/fanin object. This is a "select" object
- call object.init(), passing in:
	if kwargs is None or len(kwargs) == 0:
		raise ValueError("FanIn requires a length. No length provided.")
	elif len(kwargs) > 9:
		raise ValueError("Error - FanIn init has too many args.")
	self._n = kwargs['n']
	self.start_state_fanin_task = kwargs['start_state_fanin_task']
	self.store_fanins_faninNBs_locally = kwargs['store_fanins_faninNBs_locally']
	self.DAG_info = kwargs['DAG_info'] 
- call fanin(), passing kword args for fanin (which can be normal synchronize_sync call):
	kwargs['result']
	kwargs['calling_task_name']
	kwargs['fanin_task_name']
	kwargs['start_state_fanin_task']
	kwargs['server']
- fanin() triggers fanin task with:
	try:
		logger.debug("DAG_executor_FanInNB_Select: triggering DAG_Executor_Lambda() for task " + fanin_task_name)
		lambda_DAG_exec_state = DAG_executor_State(function_name = "DAG_executor.DAG_executor_lambda", function_instance_ID = str(uuid.uuid4()), state = start_state_fanin_task)
		logger.debug ("DAG_executor_FanInNB_Select: lambda payload is DAG_info + " + str(start_state_fanin_task) + "," + str(self._results))
		lambda_DAG_exec_state.restart = False      # starting new DAG_executor in state start_state_fanin_task
		lambda_DAG_exec_state.return_value = None
		lambda_DAG_exec_state.blocking = False            
		logger.info("DAG_executor_FanInNB_Select: Starting Lambda function %s." % lambda_DAG_exec_state.function_name) 
		payload = {
			"input": self._results,
			"DAG_executor_state": lambda_DAG_exec_state,
			"DAG_info": self.DAG_info
		}
		DAG_executor.DAG_executor_lambda(payload)
	except Exception as ex:
		logger.error("[ERROR] DAG_executor_FanInNB_Select: Failed to start DAG_executor.DAG_executor_lambda"
			+ " for triggered task " + fanin_task_name)
		logger.error(ex) 

So the above info needs to be passed in payload. Is it covered?
1. The kwargs of fan_in() are the same and are part of the regular fan_in messages in the list of results. 
   Currently this list is put in the message:
   			message = {
				"op": "process_enqueued_fan_ins",
				"type": "DAG_executor_fanin_or_faninNB",
				# We are passing a list of operations to the lambda function, which will execute
				# the mone by one in the order they were received.
				"name": list,
				"state": make_json_serializable(dummy_state),   # NOTE: no info in state
				"id": msg_id
			}
	passed as the payload: payload = {"json_message": message} on the 
	call: return_value = simulated_lambda_function.lambda_handler(payload)
2. Normally, the driver calls make all objects and when we create a message to pass to the server
   we add DAG_info to the DAG_executor_State that gets passed:
		dummy_state = DAG_executor_State(function_name = "DAG_executor", function_instance_ID = str(uuid.uuid4()))
		# passing to the fninNB object:
		# it size
		dummy_state.keyword_arguments['n'] = size
		# when the faninNB completes, if we are runnning locally and we are not pooling,
		# we start a new thread to execute the fanin task. If we are thread pooling, we put the 
		# start state in the work_queue. If we are using lambdas, we invoke a lambda to
		# execute the fanin task. If we are process pooling, then the last process to 
		# call fanin will put the start state of the fanin task in the work_queue. (FaninNb
		# cannot do this since the faninNB will be on the tcp_server.)
		dummy_state.keyword_arguments['start_state_fanin_task'] = DAG_states[fanin_nameNB]
		dummy_state.keyword_arguments['store_fanins_faninNBs_locally'] = store_fanins_faninNBs_locally
		dummy_state.keyword_arguments['DAG_info'] = DAG_info
	which is fine since this is info that init() expects. But now the create is being initiated
	here, not in the driver, so we need to get this info and pass it to the 
	simulated_lambda_function (which is a Lambda_Function_Simulator()):
	- DAG_info we have
	- store_fanins_faninNBs_locally is false
	- n we can get:
		list_n_pair = self.object_name_to_trigger_map[sync_object_name]
		list = list_n_pair[0]
		n = list_n_pair[1]
	- start_state_fanin_task we can get: 
		from above: sync_object_name = json_message.get("name", None)
		start_state_fanin_task = DAG_states[sync_object_name]
3. The information for the triggered task payload () is: start_state_fanin_task, self._results
   and DAG-info, which we have.

So: How to package this info in the payload of triggered Lambdas? These Lambdas are different 
from the simpler lambdas that just execute tasks Wukong style.

			try:
				logger.debug("SQS enqueue: calling simulated_lambda_function.lambda_handler(payload)")
				# This is essentially a synchronous call to a regular Python function
# ToDo: Do we want to allow both sync and async calls? The latter done by creating a thread that does
# the call. Faster when we are invoking a real lambda, which we will do with invoke_lambda_asynch
				return_value = simulated_lambda_function.lambda_handler(payload)
				logger.debug("SQS enqueue: called simulated_lambda_function.lambda_handler(payload)")
			except Exception as ex:
				logger.error("[ERROR]: " + thread_name + ": invoke_lambda_synchronously: Failed to run lambda handler for synch object: " + sync_object_name)
				logger.error(ex)
			"""
			
			return return_value
		else:
			logger.debug("SQS enqueue: function call not Triggered")

#ToDo: this value is used/ignored now that we can trigger tasks?
		dummy_DAG_exec_state = DAG_executor_State(function_name = "DAG_executor", function_instance_ID = str(uuid.uuid4()))
		dummy_DAG_exec_state.return_value = 0
		dummy_DAG_exec_state.blocking = False
		# returning value to caller which is InfiniD enqueue()
		return cloudpickle.dumps(dummy_DAG_exec_state)

# collection of simuated functions
class InfiniD:
	def __init__(self, DAG_info):
		self.dag_orchestrator = DAG_orchestrator()
		self.DAG_info = DAG_info
		self.DAG_map = DAG_info.get_DAG_map()
		self.DAG_states = DAG_info.get_DAG_states()
		self.all_fanin_task_namesDAG_states = DAG_info.get_DAG_states()
		self.all_fanin_task_names = DAG_info.get_all_fanin_task_names()
		self.all_fanin_sizes = DAG_info.get_all_fanin_sizes()
		self.all_faninNB_task_names = DAG_info.get_all_faninNB_task_names()
		self.all_faninNB_sizes = DAG_info.get_all_faninNB_sizes()
		self.all_fanout_task_names = DAG_info.get_all_fanout_task_names()
		num_Lambda_Function_Simulators = len(self.all_fanin_task_names) + len(self.all_fanout_task_names) + len(self.all_faninNB_task_names)
		self.list_of_Lambda_Function_Simulators = []
		self.list_of_function_locks = []
		self.num_Lambda_Function_Simulators = num_Lambda_Function_Simulators
		self.function_map = {}

	# not currently used.
	def create_fanin_and_faninNB_messages(self):
		self.dag_orchestrator.create_fanin_and_faninNB_messages(self.DAG_map,self.DAG_states,self.DAG_info,self.all_fanin_task_names,self.all_fanin_sizes,self.all_faninNB_task_names,self.all_faninNB_sizes)

	# create simulated functions. Number of fucntions is based on number of fanin/fanout/faninNB
	# objects though we may store several objects in one function.
	def create_functions(self):
		# if use_single_lambda_function then we map all the names to a single
		# function, which is function 0. In this case we create a single 
		# function, which we do by breaking the creating loop after one
		# function has been created.
		# Create function and a lock used to ensure there are no concurrent invoctions
		# of the function.
		for _ in range(0,self.num_Lambda_Function_Simulators):
			self.list_of_Lambda_Function_Simulators.append(Lambda_Function_Simulator())	
			self.list_of_function_locks.append(Lock())
			# if using a single function to store all objects, break to loop. 
			if using_single_lambda_function:
				break

	# map an object using its function name to one of the functions via its function indez
	def map_synchronization_object(self, object_name, object_index):
			self.function_map[object_name] = object_index

	# get function the object name was mapped to
	def get_function(self, object_name):
			return self.list_of_Lambda_Function_Simulators[self.function_map[object_name]]
	def get_function_lock(self, object_name):
			return self.list_of_function_locks[self.function_map[object_name]]

	# map the fanins/fanouts/faninNBs to a function, currently one object per function
	# where order does not matter
	def map_object_names_to_functions(self):
		# for fanouts, the fanin object size is always 1
		# map function name to a pair (empty_list,n) where n is size of fanin/faninNB.
		# if use_single_lambda_function then we map all the names to a single
		# function, which is function 0. We do this by skippng the increment
		# of i so that i is always 0.
		i=0
		for object_name, n in zip(self.all_faninNB_task_names, self.all_faninNB_sizes):
			self.map_synchronization_object(object_name,i)
			if not using_single_lambda_function:
				i += 1
			# Each name is mapped to a pair, which is (empty_list,n). The 
			# list collects results for the fan_in and fanin/fanot size n is 
			# used to determine when all of the results have been collected.
			# For a fanout, we can use a faninNB object with a size of 1.
			self.dag_orchestrator.map_object_name_to_trigger(object_name,n)

		for object_name, n in zip(self.all_fanin_task_names, self.all_fanin_sizes):
			self.map_synchronization_object(object_name,i)
			if not using_single_lambda_function:
				i += 1
			self.dag_orchestrator.map_object_name_to_trigger(object_name,n)

		for object_name in self.all_fanout_task_names:
			self.map_synchronization_object(object_name,i)
			if not using_single_lambda_function:
				i += 1
			n = 1 # a fanout is a fanin of size 1
			self.dag_orchestrator.map_object_name_to_trigger(object_name,n)

	# call enqueue of DAG_orchestrator. The DAG_orchestrator will eithor store the jsn_message, which is 
	# for a fanin/fanout op in the list of operations for the associated object, or if all n operaations
	# have been performed, t will invoke the function storing that objct and pass the list of operations
	# to be performed on the object. The function call will be locked with the function's lock.
	def enqueue(self,json_message):
		sync_object_name = json_message.get("name", None)
#ToDo: if crate objects on fly then there are not functions/locks to get?
#      Don't need to lock since only ever one caller of the function.
#      So: if not create functions on-the-fly: get f/l else: set them to None
		simulated_lambda_function = self.get_function(sync_object_name)
		simulated_lambda_function_lock = self.get_function_lock(sync_object_name)
		logger.debug("XXXXXXXXXXXXXXXXXXXX InfiniD enqueue: calling self.sqs.enqueue")
		return_value = self.dag_orchestrator.enqueue(json_message, simulated_lambda_function, simulated_lambda_function_lock)
		logger.debug("XXXXXXXXXXXXXXXXXXXX InfiniD enqueue: called self.sqs.enqueue")

		return return_value