import logging 
#import re 
#import socket
import time 
#import redis 
import uuid

from wukongdnc.server.message_handler_lambda import MessageHandler
from .DAG_executor_State import DAG_executor_State
#from .DAG_info import DAG_Info
from wukongdnc.server.util import make_json_serializable
from .DAG_executor_constants import store_fanins_faninNBs_locally 
from .DAG_executor_constants import FanIn_Type, FanInNB_Type

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s')

#SLEEP_INTERVAL = 0.120

warm_resources = {
	'cold_start_time': time.time(),
	'invocation_count': 0,
	'message_handler' : None
}

class Lambda_Function_Simulator:
	def __init__(self):
# rhc: Q: where to put this? who calls SQS.enqueue? At fanins and fanouts?
# For fanout, it's SQS.enqueue(the message you pass to fan_in). So sqs.enqueue
# is collecting fan_in messages that it can deliver as a batch of fanins or 
# iteratively call fan_in via synchronous lambda invoke so no cncurrent calls.
#
# So sqs.enqueue() is called from tcp_server_lambda.invoke_lambda_synchronouly, i.e., 
# we call SQS instead of directly invoking the simulated lambada?
# This considers dag_executor to be stored in infiniX lambda too? Simple vesion
# first that uses non-infiniX lambdas for DAG_excutors?
		self.sqs = SQS()

	def lambda_handler(self, payload):
		#start_time = time.time()
		global warm_resources
		invocation_time = time.time()
		warm_resources['invocation_count'] = warm_resources['invocation_count'] + 1
		#logger.debug("Invocation received. event: " + str(event))

		logger.debug(f'Invocation count: {warm_resources["invocation_count"]}, Seconds since cold start: {round(invocation_time - warm_resources["cold_start_time"], 1)}')

		# Extract all of the data from the payload.
		#json_message = cloudpickle.loads(base64.b64decode(event["json_message"]))
		json_message = payload['json_message']
		logger.debug("JSON message: " + str(json_message))

		if not warm_resources['message_handler']:
			# Issue: Can we get and print the name of the Lambda function - "LambdaBoundedBuffer" or "LambdaSemaphore"
			logger.debug("**************** Lambda function cold start ******************")
			warm_resources['message_handler'] = MessageHandler()
			#Issue: what if we lost a Lambda? If we have backup we can recover but how do we determine whether we failed?
		else:
			logger.debug("**************** warm start ******************")

		return_value = warm_resources['message_handler'].handle(json_message)

		return return_value

class SQS:
	# create fanin and faninNB messages for creating all fanin and faninNB synch objects
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


class InfiniX:
	def __init__(self,num_Lambda_Function_Simulators = 1):
		self.list_of_Lambda_Function_Simulators = []
		self.num_Lambda_Function_Simulators = num_Lambda_Function_Simulators
		self.function_map = {}

	def create_functions(self):
		for _ in range(0,self.num_Lambda_Function_Simulators):
			self.list_of_Lambda_Function_Simulators.append(Lambda_Function_Simulator())	

	def map_synchronization_object(self, function_name,	function_index):
			self.function_map[function_name] = function_index

	def get_function(self, function_name):
			return self.list_of_Lambda_Function_Simulators[self.function_map[function_name]]
