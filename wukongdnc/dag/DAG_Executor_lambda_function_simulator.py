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
from .DAG_executor_constants import create_all_fanins_faninNBs_on_start

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

	# called in tcp_server_lambda in invoke_lambda_synchronously to invoke a simulated lambda.
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
			# Note the import above: from wukongdnc.server.message_handler_lambda import MessageHandler
			warm_resources['message_handler'] = MessageHandler()
			#Issue: what if we lost a Lambda? If we have backup we can recover but how do we determine whether we failed?
		else:
			logger.debug("Lambda_Function_Simulator: lambda_handler: *************** warm start ******************")

		return_value = warm_resources['message_handler'].handle(json_message)

		return return_value

class DAG_orchestrator:
	def __init__(self,DAG_info):
		self.object_name_to_trigger_map = {}
		self.DAG_info = DAG_info
		self.DAG_states = DAG_info.get_DAG_states()
		self.all_fanin_task_names = DAG_info.get_all_fanin_task_names()

	# create fanin and faninNB messages for creating all fanin and faninNB synch objects
	#
	# Not using for now - this method is in tcp_server_lamba and it
	# invokes the mapped to lambda for a given object name with a 
	# "create" msg so the object is created in that lamba.
	# Note: If we use this we will also need to create fanout and leaf task messages
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
		list_of_fan_in_ops = list_n_pair[0]
		n = list_n_pair[1]
		# add the fanin/fannout op to the list of operations
		list_of_fan_in_ops.append(json_message)
		# if alln operations have been performed, invoke the function that contains the object
		if len(list_of_fan_in_ops) == n:
			""" moved this down
			msg_id = str(uuid.uuid4())
			dummy_state = DAG_executor_State(function_name = "DAG_executor", function_instance_ID = str(uuid.uuid4()))
			logger.debug("DAG_Orchestrator: Triggered: Sending 'process_enqueued_fan_ins' message to lambda function for " + sync_object_name)
			logger.debug("SDAG_Orchestrator: length of enqueue's list: " + str(len(list)))
			# we set state.keyword_arguments before call to create()
			message = {
				"op": "process_enqueued_fan_ins",
				"type": "DAG_executor_fanin_or_faninNB",
				# We are passing a list of operations to the lambda function, which will execute
				# them one by one in the order they were received.
				"name": list,
				"state": make_json_serializable(dummy_state),
				"id": msg_id
			}
			#msg = json.dumps(message).encode('utf-8')
			"""
			#ToDo: if we are triggering the fanin task to run in the same lambda then we need to make sure
			# the lambda has the payload it needs to run, i.e., for sync object the payload is the message 
			# it normally gets, but if the sync object triggers a task to run in the same lambda, the payload
			# must also contain the info needed to run the task. This would be the payload info sent to
			# real lambdas for DAG_executor_lambda(payload), which is from dag_executor_driver or the fan_in op.
			"""
				lambda_DAG_exec_state = DAG_executor_State(function_name = "DAG_executor.DAG_executor_lambda", function_instance_ID = str(uuid.uuid4()), state = start_state)
				logger.debug ("DAG_executor_driver: lambda payload is DAG_info + " + str(start_state) + "," + str(inp))
				lambda_DAG_exec_state.restart = False      # starting new DAG_executor in state start_state_fanin_task
				lambda_DAG_exec_state.return_value = None
				lambda_DAG_exec_state.blocking = False            
				logger.info("DAG_executor_driver: Starting Lambda function %s." % lambda_DAG_exec_state.function_name)

				# We use "inp" for leaf task input otherwise all leaf task lambda Executors will 
				# receive all DAG_info leaf task inputs in the leaf_task_inputs and in the state_info.task_inputs
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
			# So we need DAG_info, which InfiniD inputs and can pass to DAG_Orchestrator.
			# See below about: invoke a (simulated) function which creates the object on the fly
			# and calls init(), which needs DAG_info, and whatever. So need to pass DAG_info
			# to (simulated) function (which is what Driver/fan_in does when invoking a real 
			# lambda function). So the (simulated) function needs a payload for calling fan_in a
			# and a payload for invoking a lambda, and these overlap since fan_in can invoke lambda.
			# If not triggering tasks then only need sync object payload. So the deployment for lambda
			# functions that store objects and triggers tasks is different than the deployment
			# for lambdas that just run tasks like Wukong. Former has a payload that has sync
			# object and triggered task info. The triggering lambdas: create sync object on fly, 
			# call init() as usual (with DAG_info and whatever) then call the fan_in operations on the 
			# sync oject. The sync object will trigger the fanin task by calling 
			# DAG_executor.DAG_executor_lambda(payload) with the
			# correct payload, created from the faninNB fan_in info (results, DAG_info, ...)

			msg_id = str(uuid.uuid4())
			dummy_state = DAG_executor_State(function_name = "DAG_executor", function_instance_ID = str(uuid.uuid4()))
			logger.debug("DAG_Orchestrator: Triggered: Sending 'process_enqueued_fan_ins' message to lambda function for " + sync_object_name)
			logger.debug("SDAG_Orchestrator: length of enqueue's list: " + str(len(list_of_fan_in_ops)))

			if not create_all_fanins_faninNBs_on_start:
				is_fanin = sync_object_name in self.all_fanin_task_names
				dummy_state.keyword_arguments['is_fanin'] = is_fanin	
				# need to send the fanin_name. this name is in the fan_in ops in the 
				# list_of_fan_in_ops but easier to send it here as part of message so we
				# won't have to extract the first op and get the name.
				dummy_state.keyword_arguments['fanin_name'] = sync_object_name
				if not is_fanin: # faninNB or fanout
					# passing to the faninNB object:
					# its size
					dummy_state.keyword_arguments['n'] = n
					# when the faninNB completes, if we are runnning locally and we are not pooling,
					# we start a new thread to execute the fanin task. If we are thread pooling, we put the 
					# start state in the work_queue. If we are using lambdas, we invoke a lambda to
					# execute the fanin task. If we are process pooling, then the last process to 
					# call fanin will put the start state of the fanin task in the work_queue. (FaninNb
					# cannot do this since the faninNB will be on the tcp_server.)
					dummy_state.keyword_arguments['start_state_fanin_task'] = self.DAG_states[sync_object_name]
					dummy_state.keyword_arguments['store_fanins_faninNBs_locally'] = store_fanins_faninNBs_locally
					dummy_state.keyword_arguments['DAG_info'] = self.DAG_info
				else: # fanin
					# passing to the faninNB object:
					# its size
					dummy_state.keyword_arguments['n'] = n	

			# we set state.keyword_arguments before call to create()
			message = {
				"op": "process_enqueued_fan_ins",
				"type": "DAG_executor_fanin_or_faninNB",
				# We are passing a list of operations to the lambda function, which will execute
				# them one by one in the order they were received.
				"name": list_of_fan_in_ops,
				"state": make_json_serializable(dummy_state),
				"id": msg_id
			}
			#msg = json.dumps(message).encode('utf-8')

			payload = {"json_message": message}	

			# Do not allow parallel invocations. It's possible that parallel invocatins can never
			# be attempted using the scheme under development.
			# created all objects at start - created objects one by one and called init() passing kwargs
			with simulated_lambda_function_lock:
				try:
					logger.debug("DAG_Orchestrator enqueue: calling simulated_lambda_function.lambda_handler(payload)")
					# This is essentially a synchronous call to a regular Python function
	#ToDo: Allow calling real lambdas insted of simulated lambdas based on options. like we do 
	# in the tcp_server_lambda functions.
	# ToDo: Do we want to allow both sync and async calls to simulated functions? The latter done by 
	# creating a thread that does the call. Async faster when we are invoking a real lambda, which we will do with invoke_lambda_asynch
					# The payload is a message with "op": "process_enqueued_fan_ins", so lambda
					# Will execute process_enqueued_fan_ins to call the fan_in ops on the fanin objects 
					# that were created at the beginning and returning the fanin results to the client
					# caller, e.g., a thread that called fan_in i.e., the thread that is simulating a lambda.
					# The returned result for a fanin is 0 or the collected fan_in results.
					# For faninNB, no results are returned so the return value is 0.
					return_value = simulated_lambda_function.lambda_handler(payload)
					logger.debug("DAG_Orchestrator: called simulated_lambda_function.lambda_handler(payload)")
				except Exception as ex:
					logger.error("[ERROR]: " + thread_name + ": invoke_lambda_synchronously: Failed to run lambda handler for synch object: " + sync_object_name)
					logger.error(ex)		
			"""
#rhc: ToDo: the payload needs to work for the above pre-created objects case too

			simulated_lambda_function = Lambda_Function_Simulator()

So: The current lamba_function_simulator lamba_handler does warm_resources['message_handler'].handle(json_message)
where the message_handler is message_handler_lambda. This works for non-triggered lambdas. The 
message handler will call process_enqueued_fan_ins and return 0 (always 0 for faninNBs) or the 
collected fan_in results.
When we have a triggered lambda, and we create objects on the fly, we need to create and init
the object and call the fan_in(s) and let the fan_in trigger the task by calling 
DAG_executor_lambda(payload). However ....
*****
ToDo: ... 
- if we use pre-created and mapped objects, like we currently have, we can simply 
call the fan_in(s) and allow the fan_in to call DAG_excutor_lamba(payload)?
- And if we create on the fly, can we just do "create_and_process_enqueued_fan_ins" which simply
creates the object before we do fan_ins? The object will be local and will never be
accessed again.
- But we need to pass a payload to simulated lambda function that has information for all of 
this - create object, do fanins, call DAG_excutor_lamba(payload).
*****

ToDo: Since this Lambda_Function_Simulator() may create a fanin and this fanin may trigger its
task, we need to make sure the Lambda_Function_Simulator() will get the payload it needs to run 
tasks. Note that after we create the object we call init() and pass in DAG_info in case it needs 
to run a lambda. Note that it is the (simulated) Lambda that creates the fanin so the simulated 
lambda needs DAG_info for its call to init(), and whatever else init() gets.

rhc: ToDo: need to premap leaf tasks.

Scheme: Lambda_Function_Simulator() does:
- If create on fly:
  create() one local fanout/faninNB/fanin object. This is a "select" object since it is in a 
  lambda? or since the lifetime of this object is only this one execution of this lambda then 
  it could be a non-select object?
  Note that we know this object does not exist on this first invocation of the lambda so we can 
  call create() instead of create_if() to create the foo object. (That is, the name of the 
  object does not have to be the actual name of the fanout/fanin. We can reuse this local
  foo object for succeeding operations? Can we do multiple fanins if we are the become
  task for each of them? Or just do fann processing as usual, i.e., remote calls to
  fanin object - have to do remote call anyway to get other results.
- call object.init(), passing in:
	FaninNB and FaninNB_select
	def init(self,kwargs): 
		if kwargs is None or len(kwargs) == 0:
			raise ValueError("FanIn requires a length. No length provided.")
		elif len(kwargs) > 9:
			raise ValueError("Error - FanIn init has too many args.")
		self._n = kwargs['n']
		self.start_state_fanin_task = kwargs['start_state_fanin_task']
		self.store_fanins_faninNBs_locally = kwargs['store_fanins_faninNBs_locally']
		self.DAG_info = kwargs['DAG_info'] 
	Fanin and Fanin_Select: only ref kwargs['n']
- call all fan_ins(), passing kwargs for fan_in (which can be normal synchronize_sync call). The 
  kwargs for the saved ops in the list created by DAG_executor are all the same:
	For faninNB and FaninNB_select:
    def fan_in(self, **kwargs)
		where fan_in for non-select references:
		kwargs['result']
		kwargs['calling_task_name']
		kwargs['fanin_task_name']
		kwargs['start_state_fanin_task']
		kwargs['server']
	For fanin and fanin_select: only refs result and calling_task_name.
	Note: fanins do not do anything after last fan_in other than return results
- fan_in() for faninNB triggers fanin task with:
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
				# them one by one in the order they were received.
				"name": list,
				"state": make_json_serializable(dummy_state),   # NOTE: no info in state
				"id": msg_id
			}
	passed as the payload: payload = {"json_message": message} on the 
	call: return_value = simulated_lambda_function.lambda_handler(payload)
	So the payload for calling the fan_ins is just part of the saved fan_in
	messages in the list in this current message, which is used for doing 
	fan_in ops on the sync_ops, which are not currently triggering tasks.
2. Normally, the driver makes all objects and when we create a message to pass to the server
   we add DAG_info to the DAG_executor_State that gets passed:
		dummy_state = DAG_executor_State(function_name = "DAG_executor", function_instance_ID = str(uuid.uuid4()))
		# passing to the faninNB object:
		# its size
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
	- DAG_info we have ***but we need to null out the leaf task stuff***
	- store_fanins_faninNBs_locally is false
	- n we can get:
		list_n_pair = self.object_name_to_trigger_map[sync_object_name]
		list = list_n_pair[0]
		n = list_n_pair[1]
	- start_state_fanin_task we can get: 
		from above: sync_object_name = json_message.get("name", None)
		start_state_fanin_task = DAG_states[sync_object_name]
	So this info can be packaged as a dummy_state 
3. The information for the triggered task payload () is: start_state_fanin_task, self._results
   and DAG-info, which we have since it is the fan_in can alreay create payloads
   for invoking lambdas by using the info from init() and the collected results
#rhc: Q: results have all the results? or missing become task's results?
Comment says:
	# Last thread does not append results. It will recieve list of results of other threads and append 
	# its result locally to the returned list.
So we will send back results with last caller's results missing.

# rhc: ToDo: Do we actually invoke lambda to do fan_in for FanIn? We have the results
on tcp_sever_lambda (saved). And for FanIn can create a FanIn on tcp_server_lambda
to reuse the FanIn logic, i.e., fan_in will return what we want?

So: How to package this info in the payload of triggered Lambdas? These Lambdas are different 
from the simpler lambdas that just execute tasks Wukong style since these lambdas must
create a fanout/fanin object and cal fan_in(s) on it so it will trigger its task.
A: Seems like we have the fan_in kwargs in the saved fan_in op messages. We just
need the init inf, whch we can package as a DAG_executor_State. The trigger info
is info that the fan_in has - the results and the init() info. So the payload to
the lmabda function is: the message for processing the list of fan_in ops and the 
DAG_excutor_tate fr the call to init()
Note: 
-If not triggering and using pre-created objects, then we just process the 
list of fanins. Noet that pre-created objects must be created somewhere and
we need to remember where so these pre-created ojects are pre-mapped too.
Also, for simulated functions the functions are not named, they are indexed
in an array (i.e., we map a function to an index i) so the implicit name of 
the function in Array[i] is Fi. In general, we will map sync objects to 
"deployment names".
-If not triggering and not using precreated objects, we need to create() and
init() the sync object before we process the list of fan_ins
-If triggering and not using precreatd objects, we need to create() and
init() the sync object before we process the list of fan_ins. 
The fanin will trigger the task by calling DAG_executor_lambda, (which
does not need to access the object again.)

rhc: Q: if we pre-create object O in a function F then we have to know how 
to access F. For simulated objects, we map O to the index i of the function
in the list of simulated functions. For real lambdas, we map O to the "name"
of the function deployment. So when we pre-create objects we must also 
map objects to functions?

The current DAG_handler.py is:
	def lambda_handler(event, context):
		invocation_time = time.time()
		warm_resources['invocation_count'] = warm_resources['invocation_count'] + 1
		logger.debug(f'Invocation count: {warm_resources["invocation_count"]}, Seconds since cold start: {round(invocation_time - warm_resources["cold_start_time"], 1)}')

		start_time = time.time()
		rc = redis.Redis(host = REDIS_IP_PRIVATE, port = 6379)

		logger.debug("Invocation received. Starting DAG_executor_lambda: event/payload is: " + str(event))
		DAG_executor_lambda(event)
					
		end_time = time.time()
		duration = end_time - start_time
		logger.debug("DAG_executor_lambda finished. Time elapsed: %f seconds." % duration)
		rc.lpush("durations", duration) 

where:

def DAG_executor_lambda(payload):
    logger.debug("Lambda: started.")
    DAG_exec_state = cloudpickle.loads(base64.b64decode(payload['DAG_executor_state']))

    logger.debug("payload DAG_exec_state.state:" + str(DAG_exec_state.state))
    DAG_info = cloudpickle.loads(base64.b64decode(payload['DAG_info']))
    DAG_map = DAG_info.get_DAG_map()
    state_info = DAG_map[DAG_exec_state.state]
    is_leaf_task = state_info.task_name in DAG_info.get_DAG_leaf_tasks()
    if not is_leaf_task:
        # lambdas invoked with inputs. We do not add leaf task inputs to the data
        # dictionary, we use them directly when we execute the leaf task.
        # Also, leaf task inputs are not in a dictionary.
        dict_of_results = cloudpickle.loads(base64.b64decode(payload['input']))
        for key, value in dict_of_results.items():
            data_dict[key] = value
    else:
        # Passing leaf task input as state_info.task_inputs in DAG_info; we don't
        # want to add a leaf task input parameter to DAG_executor_work_loop(); this 
        # parameter would only be used by the Lambdas and we ha ve a place already
        # in state_info.task_inputs. 
        # Note: We null out state_info.task_inputs for leaf tasks after we use the input.
        inp = cloudpickle.loads(base64.b64decode(payload['input']))
        state_info.task_inputs = inp

    # lambdas do not use work_queues, for now.
    work_queue = None

    # server and counter are None
    # logger is local lambda logger
    DAG_executor_work_loop(logger, server, counter, DAG_exec_state, DAG_info, work_queue )
    logger.debug("DAG_executor_processes: returning after work_loop.")
    return

We are not here calling DAG_executor_lambda(event) since we are now doing the fan_in
ops and fan_in will trigger the task by calling DAG_executor_lambda(event).
We can just pass the message to call process fan_ins and it can create object if necessary, 
and call fan_ins. Note that create() calls init().

We are executing fanout/faninNB operations. [Note that fanins are handled the usual
way, i.e., we return a dictionary of results like usual, which is what happens when we use the
DAG_orchestrator but we do not trigger tasks - fan_in returns a DAG_executor_State with a 
return value that holds the dictionary of results. Note: When we trigger tasks, either the 
DAG_orchestrator or the method that calls D_O will have to take DAG_orchestrator's saved
results create a dictionary of results (borrowing the FanIn.fan_in() code) and put them in 
a DAG_executor_State.] 
For Fanout/faninNB, we create the fanout/fanin, call the fan_in operation(s) with 
the DAG_executor's saved results and let the fanout/fanin trigger the task by calling 
DAG_executor_lambda(payload); this will be the first call to  DAG_executor_lambda(payload) 
in this lambda. DAG_executor_lambda is unchanged, it executes the work loop, which means it 
executes the trggered task and does the fanouts/faninNBs/fanins after that, possibly becoming
one of the fanouts and calling process_faninNBs_batch to procss the other fanouts/faninNBs or 
processing the fanin, as usual.

Note that for fanins, fan_in is done as usual, as we do not invoke a new lambda for the fan_in tasks (only for tasks of 
  fanouts and faninNBs)  (as usual means we get back a 0 or the non-zero fan_in results.)  
  tcp_server_lambda - get the results from the saved ops and make a dict of results to be 
  returned to the DAG_executor_lambda borowing the FanIn.fan_in() code.

So this is same as non-triggered case with check for object pre-created.

rhc: toDo: For this create, we need to know the type but type is not in 
the fan_in message. So we can look up name in fanins - if in fanins the
its a fanin else its a faninNB (for faninNBs and fanouts).

Lambda_Function_Simulator() payload is:
payload = {
	"json_message": message,			# message for process_enqueued_fan_ins
	"state for init": dummy_state
} 

No: We still pass the process_enqueued_fan_ins message in the payload; 
this message will cause process_enqueued_fan_ins to be called, which 
is what we want. But we also need to make sure we can get the message
for the possible create():

	creation_message = {
		"op": "create",
#rhc: ToDo: use correct type
		"type": FanIn_Type,
		"name": fanin_name,
		"state": make_json_serializable(dummy_state),	
		"id": msg_id
	}
	self.create_obj(creation_message)

So perhaps we can assemble this message and pass it to process_enqueued_fan_ins
as part of the dummy_state, which is now not used for anything in 
process_enqueued_fan_ins: 
	dummy_state = DAG_executor_State(function_name = "DAG_executor", function_instance_ID = str(uuid.uuid4()))
We need to compute the type of fanin object and we can get the set of
fan_ins, as InfniD could give DAG_info to the DAG_orchestrator so we 
can get the set of fanins. Note: This code here is in DAG_orchestrator.enqueue()
Note: We only need the creation message if not pre-created ojects.

			try:
				logger.debug("DAG_Orchestrator enqueue: calling simulated_lambda_function.lambda_handler(payload)")
				# This is essentially a synchronous call to a regular Python function
# ToDo: Do we want to allow both sync and async calls? The latter done by creating a thread that does
# the call. Faster when we are invoking a real lambda, which we will do with invoke_lambda_asynch
				return_value = simulated_lambda_function.lambda_handler(payload)
				logger.debug("v enqueue: called simulated_lambda_function.lambda_handler(payload)")
			except Exception as ex:
				logger.error("[ERROR]: " + thread_name + ": invoke_lambda_synchronously: Failed to run lambda handler for synch object: " + sync_object_name)
				logger.error(ex)
			"""
			
			return return_value
		else:
			logger.debug("DAG_Orchestator enqueue: function call not Triggered")

#ToDo: this value is used/ignored now that we can trigger tasks?
		dummy_DAG_exec_state = DAG_executor_State(function_name = "DAG_executor", function_instance_ID = str(uuid.uuid4()))
		dummy_DAG_exec_state.return_value = 0
		dummy_DAG_exec_state.blocking = False
		# returning value to caller which is InfiniD enqueue()
		return cloudpickle.dumps(dummy_DAG_exec_state)

# collection of simuated functions
class InfiniD:
	def __init__(self, DAG_info):
		self.dag_orchestrator = DAG_orchestrator(DAG_info)
		self.DAG_info = DAG_info
		self.DAG_map = DAG_info.get_DAG_map()
		self.DAG_states = DAG_info.get_DAG_states()
		self.all_fanin_task_names = DAG_info.get_all_fanin_task_names()
		self.all_fanin_sizes = DAG_info.get_all_fanin_sizes()
		self.all_faninNB_task_names = DAG_info.get_all_faninNB_task_names()
		self.all_faninNB_sizes = DAG_info.get_all_faninNB_sizes()
		self.all_fanout_task_names = DAG_info.get_all_fanout_task_names()
		self.DAG_leaf_tasks = DAG_info.get_DAG_leaf_tasks()
#rhc: leaf tasks
		self.num_Lambda_Function_Simulators = len(self.all_fanin_task_names) + (
			len(self.all_fanout_task_names)) + (
			len(self.all_faninNB_task_names)) + (
			len(self.DAG_leaf_tasks))
		self.list_of_Lambda_Function_Simulators = []
		self.list_of_function_locks = []
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

		for object_name in self.DAG_leaf_tasks:
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
#ToDo: if create objects on fly then there are no functions/locks to get?
#      Don't need to lock since only ever one caller of the function.
#      So: if not create functions on-the-fly: get func/lock 
# 		else: Create anonymous function/lock on the fly? Or if real lambdas 
#        use the single deployment.
		simulated_lambda_function = self.get_function(sync_object_name)
		simulated_lambda_function_lock = self.get_function_lock(sync_object_name)
		logger.debug("XXXXXXXXXXXXXXXXXXXX InfiniD enqueue: calling self.sqs.enqueue")
		return_value = self.dag_orchestrator.enqueue(json_message, simulated_lambda_function, simulated_lambda_function_lock)
		logger.debug("XXXXXXXXXXXXXXXXXXXX InfiniD enqueue: called self.sqs.enqueue")

		return return_value