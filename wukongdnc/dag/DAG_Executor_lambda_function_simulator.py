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
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(asctime)s] [%(threadName)s] %(levelname)s: %(message)s')

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)

logger.addHandler(ch)

#logger = logging.getLogger(__name__)
#logger.setLevel(logging.DEBUG)
#formatter = logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s')

#SLEEP_INTERVAL = 0.120

warm_resources = {
	'cold_start_time': time.time(),
	'invocation_count': 0,
	'message_handler' : None
}

class Lambda_Function_Simulator:
	#def __init__(self):
# rhc: Q: where to put this? who calls SQS.enqueue? At fanins and fanouts?
# For fanin, it's SQS.enqueue(the message you pass to fan_in). So sqs.enqueue
# is collecting fan_in messages that it can deliver as a batch of fanins or 
# iteratively call fan_in via synchronous lambda invoke so no cncurrent calls.
#
# So sqs.enqueue() is called from tcp_server_lambda.invoke_lambda_synchronouly, i.e., 
# we call SQS instead of directly invoking the simulated lambada?
# This considers dag_executor to be stored in infiniX lambda too? Simple version
# first that uses non-infiniX lambdas for DAG_excutors?

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

class SQS:
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

	def map_object_name_to_trigger(self,object_name,n):
		empty_list = []
		list_n_pair = (empty_list,n)
		self.object_name_to_trigger_map[object_name] = list_n_pair

	def enqueue(self,json_message,simulated_lambda_function, simulated_lambda_function_lock):
		thread_name = threading.current_thread().name
		sync_object_name = json_message.get("name", None)
		list_n_pair = self.object_name_to_trigger_map[sync_object_name]
		list = list_n_pair[0]
		n = list_n_pair[1]
		list.append(json_message)
		if len(list) == n:
#ToDo: payload is the list of json_messages, so need a new action for executing all the 
# messages in the list:
# create a new message that contains the list of messages and make this message the payload

			msg_id = str(uuid.uuid4())
			dummy_state = DAG_executor_State(function_name = "DAG_executor", function_instance_ID = str(uuid.uuid4()))
			logger.debug("SQS enqueue: Triggered: Sending 'process_enqueued_fan_ins' message to lambda function for " + sync_object_name)
			logger.debug("SQS enqueue: length of enqueue's list: " + str(len(list)))
			# we set state.keyword_arguments before call to create()
			message = {
				"op": "process_enqueued_fan_ins",
				"type": "DAG_executor_fanin_or_faninNB",
				"name": list,
				"state": make_json_serializable(dummy_state),
				"id": msg_id
			}
			#msg = json.dumps(message).encode('utf-8')
			payload = {"json_message": message}
			with simulated_lambda_function_lock:
				try:
					logger.debug("SQS enqueue: calling simulated_lambda_function.lambda_handler(payload)")
					# This is essentially a synchronous call to a regular Python function
					return_value = simulated_lambda_function.lambda_handler(payload)
					logger.debug("SQS enqueue: called simulated_lambda_function.lambda_handler(payload)")
				except Exception as ex:
					logger.error("[ERROR]: " + thread_name + ": invoke_lambda_synchronously: Failed to run lambda handler for synch object: " + sync_object_name)
					logger.error(ex)
			return return_value
		else:
			logger.debug("SQS enqueue: function call not Triggered")

		dummy_DAG_exec_state = DAG_executor_State(function_name = "DAG_executor", function_instance_ID = str(uuid.uuid4()))
		dummy_DAG_exec_state.return_value = 0
		dummy_DAG_exec_state.blocking = False
		return cloudpickle.dumps(dummy_DAG_exec_state)

class InfiniD:
	def __init__(self, DAG_info):
		self.sqs = SQS()
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

	def create_fanin_and_faninNB_messages(self):
		self.sqs.create_fanin_and_faninNB_messages(self.DAG_map,self.DAG_states,self.DAG_info,self.all_fanin_task_names,self.all_fanin_sizes,self.all_faninNB_task_names,self.all_faninNB_sizes)

	def create_functions(self):
		# if use_single_lambda_function then we map all the names to a single
		# function, which is function 0. In this case we create a single 
		# function, which we do by breaking the creating loop after one
		# function has been created.
		for _ in range(0,self.num_Lambda_Function_Simulators):
			self.list_of_Lambda_Function_Simulators.append(Lambda_Function_Simulator())	
			self.list_of_function_locks.append(Lock())
			# if using a single function to store all objects, break to loop. 
			if using_single_lambda_function:
				break

	def map_synchronization_object(self, object_name, object_index):
			self.function_map[object_name] = object_index

	def get_function(self, object_name):
			return self.list_of_Lambda_Function_Simulators[self.function_map[object_name]]
	def get_function_lock(self, object_name):
			return self.list_of_function_locks[self.function_map[object_name]]

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
			self.sqs.map_object_name_to_trigger(object_name,n)

		for object_name, n in zip(self.all_fanin_task_names, self.all_fanin_sizes):
			self.map_synchronization_object(object_name,i)
			if not using_single_lambda_function:
				i += 1
			self.sqs.map_object_name_to_trigger(object_name,n)

		for object_name in self.all_fanout_task_names:
			self.map_synchronization_object(object_name,i)
			if not using_single_lambda_function:
				i += 1
			n = 1 # a fanout is a fanin of size 1
			self.sqs.map_object_name_to_trigger(object_name,n)

	def enqueue(self,json_message):
		
		sync_object_name = json_message.get("name", None)
		simulated_lambda_function = self.get_function(sync_object_name)
		simulated_lambda_function_lock = self.get_function_lock(sync_object_name)
		logger.debug("XXXXXXXXXXXXXXXXXXXX InfiniD enqueue: calling self.sqs.enqueue")
		return_value = self.sqs.enqueue(json_message, simulated_lambda_function, simulated_lambda_function_lock)
		logger.debug("XXXXXXXXXXXXXXXXXXXX InfiniD enqueue: called self.sqs.enqueue")

		return return_value