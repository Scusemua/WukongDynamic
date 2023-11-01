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
from .DAG_executor_constants import create_all_fanins_faninNBs_on_start, map_objects_to_lambda_functions


import logging 
logger = logging.getLogger(__name__)

logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(asctime)s] [%(module)s] [%(processName)s] [%(threadName)s]: %(message)s')
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)
logger.addHandler(ch)


#logger = logging.getLogger(__name__)
#logger.setLevel(logging.DEBUG)
#formatter = logging.Formatter('[%(asctime)s] [%(module)s] [%(processName)s] [%(threadName)s]: %(message)s')

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
		#logger.trace("Invocation received. event: " + str(event))

		logger.trace(f'Lambda_Function_Simulator: lambda_handler: Invocation count: {warm_resources["invocation_count"]}, Seconds since cold start: {round(invocation_time - warm_resources["cold_start_time"], 1)}')

		# Extract all of the data from the payload.
		#json_message = cloudpickle.loads(base64.b64decode(event["json_message"]))
		json_message = payload['json_message']
		#logger.trace("Lambda_Function_Simulator: lambda_handler: JSON message: " + str(json_message))

		if not warm_resources['message_handler']:
			# Issue: Can we get and print the name of the Lambda function - "LambdaBoundedBuffer" or "LambdaSemaphore"
			logger.trace("Lambda_Function_Simulator: lambda_handler: **************** Lambda function cold start ******************")
			# Note the import above: from wukongdnc.server.message_handler_lambda import MessageHandler
			warm_resources['message_handler'] = MessageHandler()
			#Issue: what if we lost a Lambda? If we have backup we can recover but how do we determine whether we failed?
		else:
			logger.trace("Lambda_Function_Simulator: lambda_handler: *************** warm start ******************")

		# handle the message
		return_value = warm_resources['message_handler'].handle(json_message)

		return return_value

# Clients call the server, the DAG_orchestrator manages the lambda invocations (instead of calling the 
# lambda functions directly.)
class DAG_orchestrator:
	def __init__(self,DAG_info):
		# maps a fanin/fanout object name to a list of fanin/fanout operations that have been performed
		# and the size of the fanin or fanout (fanout is a fanout object with size 1). When, for example,
		# all of the fanin opertions have been received and are stored in the list, the fanin is
		# considered to be triggered.
		self.object_name_to_trigger_map = {}
		# DAG representation, input by server
		self.DAG_info = DAG_info
		# The states in the DAG - a state is a task to execte and the fanins/fanouts todo after that
		# So one state per DAG task
		self.DAG_states = DAG_info.get_DAG_states()
		# list of all fanin names (fanouts are treated as fanins of size 1 when the fanins/fanouts
		# are stored in lambas. 
		self.all_fanin_task_names = DAG_info.get_all_fanin_task_names()

	# create fanin and faninNB messages for creating all fanin and faninNB synch objects
	#
	# Not using for now - this method is in tcp_server_lamba and it
	# invokes the mapped-to lambda for a given object name with a 
	# "create" msg so the object is created in that lambda.
	# Note: If we use this we will also need to create fanout and leaf task messages
	def create_fanin_and_faninNB_messages(self,DAG_map,DAG_states,DAG_info,all_fanin_task_names,all_fanin_sizes,all_faninNB_task_names,all_faninNB_sizes):
	
		"""
		logger.trace("SQS: create_fanin_and_faninNB_messages: size of all_fanin_task_names: " + str(len(all_fanin_task_names))
			+ " size of all_faninNB_task_names: " + str(len(all_faninNB_task_names)))
		logger.trace("SQS: create_fanin_and_faninNB_messages: size of all_fanin_sizes: " + str(len(all_fanin_sizes))
			+ " size of all_faninNB_sizes: " + str(len(all_faninNB_sizes)))
		logger.trace("SQS: create_fanin_and_faninNB_messages: all_faninNB_task_names: " + str(all_faninNB_task_names))
		"""

		fanin_messages = []

		# create a list of "create" messages, one for each fanin
		for fanin_name, size in zip(all_fanin_task_names,all_fanin_sizes):
			#logger.trace("iterate fanin: fanin_name: " + fanin_name + " size: " + str(size))
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
			#logger.trace("iterate faninNB: fanin_nameNB: " + fanin_nameNB + " size: " + str(size))
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

		logger.trace("SQS:create_fanin_and_faninNB_messages: number of fanin messages: " + str(len(fanin_messages))
			+ " number of faninNB messages: " + str(len(faninNB_messages)))

		return fanin_messages, faninNB_messages

	# for each sync object, we maintain a list of fanin/fanout operations that have been performed
	# and the size of the fanin or fanout (fanout is a fanout object with size 1)
	def map_object_name_to_trigger(self,object_name,n):
		empty_list = []
		# a pair, which is a list and a value n that represents the size of the fanin
		list_n_pair = (empty_list,n)
		# store a received fanin message for fanin "object_name" in the mapped list in the list-n pair.
		# all the messages will be passed to the invoked lambda that stores the fanin object when
		# the fanin is "triggerred" i.e., all n messages have been received for a fanin of size n
		self.object_name_to_trigger_map[object_name] = list_n_pair

	# Whenever a fanin/fanout op is receieved by tcp_server_lambda via process_faninNBs_batch 
	# (calld by the client) it invokes the enqueue method of InfiniD, which invokes this enqueue 
	# of the DAG_orchestrator.
	def enqueue(self,json_message,simulated_lambda_function, simulated_lambda_function_lock):
		thread_name = threading.current_thread().name # for debugging
		sync_object_name = json_message.get("name", None)
		list_n_pair = self.object_name_to_trigger_map[sync_object_name]
		list_of_fan_in_ops = list_n_pair[0]
		n = list_n_pair[1]
		# add the fanin/fannout op to the list of operations in the list-n pair that the 
		# synch object name is mapped to.
		list_of_fan_in_ops.append(json_message)
		# if all n fanin operations have been performed, invoke the function that stores the object
		if len(list_of_fan_in_ops) == n:
			#Note: we are triggering the fanin task to run in the same lambda that stores he fanin object
			# Thus we need to ensure the lambda/task has the payload it needs to run, i.e., for fanin sync object 
			# the payload is the message it normally gets, but when the sync object gets all of its n messages it
			# triggers its fanin task to run (in the same lambda), so the payload must also contain the info needed 
			# to run the task. This amounts to the payload that gets sent to real lambdas, when not using the 
			# DAG_orchestrator. Lambdas execute DAG_executor_lambda(payload), which is from DAG_executor_driver. 
			# This is the payload we are referring to (which is the normal AWS lambda payload object).
			# Here is the code in DAG_executor_driver that is used to start real Lambdas; the payload is shown.
			"""
				lambda_DAG_exec_state = DAG_executor_State(function_name = "DAG_executor.DAG_executor_lambda", function_instance_ID = str(uuid.uuid4()), state = start_state)
				logger.trace ("DAG_executor_driver: lambda payload is DAG_info + " + str(start_state) + "," + str(inp))
				lambda_DAG_exec_state.restart = False      # starting new DAG_executor in state start_state_fanin_task
				lambda_DAG_exec_state.return_value = None
				lambda_DAG_exec_state.blocking = False            
				logger.trace("DAG_executor_driver: Starting Lambda function %s." % lambda_DAG_exec_state.function_name)

				# We use "inp" for th leaf task input otherwise all leaf task lambda Executors will 
				# receive all DAG_info leaf task inputs in the DAG_info.leaf_task_inputs and in the 
				# state_info.task_inputs (which has the inputs for all tasks, not just leaf tasks)
				# - both of these are nulled out at beginning of driver when we are using lambdas.
				# so that we don't keep storing the leaf task inputs in DAG_info after we have given
				# the inputs to the invoked leaf task lambdas.
				# If we use "inp" then we will pass only a given leaf task's input to that leaf task. 
				# For non-lambda, each thread/process reads the DAG_info from a file. This DAG-info has
				# all the leaf task inputs in it so every thread/process reads all these inputs. This 
				# can be optimized if necessary, e.g., separate ADG_info files for leaf tasks and 
				# non-leaf tasks. For processes and threads, we do not pass DAG_info around (as
				# when e invoke a Lambda and give the lambda a DAG_info object.)

				payload = {
					"input": inp,
					"DAG_executor_state": lambda_DAG_exec_state,
					"DAG_info": DAG_info
				}
			"""
			# So we need the DAG_info object. InfiniD inputs DAG_info and passes it to the DAG_Orchestrator
			# when it constructs the ADG_orchestrator.
			# When we invoke a (simulated) lambda function that creates the synch object it stores
			# on the fly, i.e., the object is created the first time a fanin op is called on it instead of 
			# creating all the synch object(s) at the start of ADG execution. When the synch object is 
			# created on the fly, init() is called, and this call to init must pass DAG_info, and the other
			# parameters. So need to pass DAG_info to (simulated) functions, which is what the 
			# DAG_executor_Driver does when invoking a real lambda function and also what fanin operations
			# do when the fanin objects are stored on the server and the fanin operations invoke a 
			# real lambda to execute the fanin task). So the (simulated) function needs a payload that contains
			# the data it needs for creating a fanin object on the fly and initing it, and the data being passsed
			# on a fan_in operation on the fanin object it stores (The DAG_executor invokes the mapped
			# to lambda, which executes a method that rceives the saved list of fanin operations and calls
			# fanin_object.fan_in(message) for each of these saved messages, one by one) and the data for 
			# executing the the fanin task, which gets executed after the last fan_in operatio 
			# occurs. The data needed for all of these things overlaps.
			# So the deployment for lambda functions that store objects and excute tasks in this same lambda
			# is different than the deployment for lambdas that just run tasks like Wukong. The former 
			# lambda deployment has a payload with data for creating and initing its sync
			# object and for executing the fanin task. The triggering lambdas: create sync object on fly, 
			# call init() as usual (with DAG_info and whatever initialization data) then call the fan_in
			# operations (recieved from DAG_orchstrator) on the sync oject. The sync object will execute 
			# the fanin task by calling DAG_executor.DAG_executor_lambda(payload) with the
			# correct payload, created from the fanin_object.fan_in() info, i.e, the fan_in results, 
			# and DAG_info etc from the fan_in operations. Of course, we pass the collected fan_in results
			# to the fanin taskalong with any other data the fanin task needs.)

			"""
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
			"""

			msg_id = str(uuid.uuid4())
			dummy_state = DAG_executor_State(function_name = "DAG_executor", function_instance_ID = str(uuid.uuid4()))
			logger.trace("DAG_Orchestrator: Triggered: Sending 'process_enqueued_fan_ins' message to lambda function for " + sync_object_name)
			logger.trace("SDAG_Orchestrator: length of enqueue's list: " + str(len(list_of_fan_in_ops)))
			
			if not create_all_fanins_faninNBs_on_start:
				# we will invoke a lambda that stores the fanin object but this object will be created
				# on the fly.
				# This object is either a true fanin object or it is a fanout object that we are treating
				# as a fanin object of size 1
				is_fanin = sync_object_name in self.all_fanin_task_names
				dummy_state.keyword_arguments['is_fanin'] = is_fanin	
				# need to send the fanin_name. this name is in the fan_in ops in the 
				# list_of_fan_in_ops but easier to send it here as part of message so we
				# won't have to extract the first op and get the name.
				dummy_state.keyword_arguments['fanin_name'] = sync_object_name
				if not is_fanin: 
					# faninNB or fanout object, so a lambda will be given the 
					# the faninNB results for executing the fanin task or the fanin is aly a 
					# fanout so the fanned out task will be executed.
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
					# passing to the fanin object:
					# its size
					dummy_state.keyword_arguments['n'] = n	
					# the fanin objct will return the collected fanin results to the (becomes) caller 
					# instead of executing the fanin task


			# we set state.keyword_arguments before call to create()
			message = {
				"op": "process_enqueued_fan_ins",
				"type": "DAG_executor_fanin_or_faninNB",
				# We are passing a list of fanin messages to the lambda function, which will call
				# fanin() on its fanin object for each message, one by one in the order they were received.
				# The fanin object treats them as normal fanin messages, i.e., the fanin object does
				# the same thing whether it is stored on the tcp_server and receiving messages
				# directly from clients, or stored in a lambda on the tcp_server_lambda, and the
				# tcp_server is receiving messages from the clients and invoking the lambda one time
				# for each message it receives, or the tcp_server_lambda passes the messages to the 
				# DAG_orchestrator, whcih collects them and invokes the lambda once after all the fan_in
				# messages have been received. (the lambda calls fan_in for each message, then the fanin 
				# task will be executed in the sme lambad or in a new lamba (liek Wukong))
				"name": list_of_fan_in_ops,
				"state": make_json_serializable(dummy_state),
				"id": msg_id
			}
			#msg = json.dumps(message).encode('utf-8')

			payload = {"json_message": message}	

			# Note: If we created all objects at start - we created objects one by one and called init() 
			# on each object, passing kwargs needed for object creation.

# ToDo: Do we want to allow both sync and async calls to simulated functions? The latter done by 
# creating a thread that does the call. Async faster when we are invoking a real lambda, which we 
# will do with invoke_lambda_asynch.

# ToDo: Allow calling real lambdas instead of simulated lambdas. Hve an option to call a real 
# lambda or a simulated lambda,  like we do in the tcp_server_lambda functions when we have an option 
# to invoke a lambda directly with the fanin message or to pass the fanin message to the DAG_orchestrator
# which will collect the messages and invoke the lambda when all the messages have been received.
# Note: Another option to be implemented is to allow the DAG_orchestrator to invoke a lambda
# with only one fanin message so that the lambad can input the message (then stop) while the other
# messages aer being computed. Thsi overlaps a fanin task inputting a (possibly large) input object
# with the other fanin inputs being created (and eventually given to the DAG_orchestrator)

			# This is essentially a synchronous call to a regular Python function (called a
			# simulated_lambda_function).
			# The payload is a message with "op": "process_enqueued_fan_ins", so lambda
			# will call process_enqueued_fan_ins(), which calls the fan_in ops on the fanin objects 
			# that were created at the beginning or created on the fly at the beginning of 
			# process_enqueued_fan_ins(). 
			# If the fanin objects are stored in lambdas and the fan_in operations are either
			# excuting the faninNB task in the same lambda or invoking other lambdas to execute
			# the faninNB task then there are no results to return to the client. If this is a
			# FanIN object then the last caller gets the FanIn results (i.e., becomes the executor
			# of the fanin task) 
			# 
			# We are currecly using simulated lambdas, which means the functions that store 
			# synch objects are real Python functions, and the lambda clients are just threads being 
			# used to simulate lambdas. Eventually we'll be using real lambdas to store synch
			# objects and real lambdas to execute fanin tasks. (These real lambdas can be the 
			# lambdas that store a fanin object and run that objects fanin task or real lambdas
			# that get invoked by the fanin object to execute the fanin task)
			# The returned result for a fanin is 0 (if the caller is not the last caller to call fanin)
			# or the collected fan_in results (for the caller that becomes the excutor of the fanin task)
			# For faninNB, no results are returned so the return value is 0.

			return_value = None
			
			if map_objects_to_lambda_functions:
				# Do not allow parallel invocations. We may invoke the function
				# more than once, e.g., two or more sync ojects mapped to the
				# same function, or the object is a semaphore and we make 
				# multiple P/V calls one by one. We have to wait for this call to finish before we can make 
				# the next call.
				with simulated_lambda_function_lock:
					try:
						logger.trace("DAG_Orchestrator enqueue: calling simulated_lambda_function.lambda_handler(payload)")
						"""
						So: The lamba_function_simulator lamba_handler does warm_resources['message_handler'].handle(json_message)
						where the message_handler is message_handler_lambda.py not message_handler.py. The message handler will call 
						process_enqueued_fan_ins(), which will receive the list of enqueued fan_in operations
						and invoke them one by one, and return 0 (always 0 for faninNBs) or return the 
						collected fan_in results.
						When we create objects on the fly, we need to create and init
						the object first and then call the fan_in(s). For a faninNB, the last fan_in can invoke
						a new lambda to execute the fanin task, or it trigger the task to run in the same lambda
						by calling DAG_executor_lambda(payload).
						"""
						return_value = simulated_lambda_function.lambda_handler(payload)
						logger.trace("DAG_Orchestrator: called simulated_lambda_function.lambda_handler(payload)")
					except Exception as ex:
						logger.error("[ERROR]: " + thread_name + ": invoke_lambda_synchronously: Failed to run lambda handler for synch object: " + sync_object_name)
						logger.error(ex)
			else:
				# we are not mapping objects to functions; instead, we are using
				# anonymous functions that can inherently only be invoked one
				# time (there is no way to refer to the function by name to call it again)
				# so there is no need to lock the invocation. Anonymous function are okay for
				# fanin/fanout since we only need to invoke a fanin/fanout function once to
				# do a single fanin/fanout operation on that object.
				# Note: A function could store multipe fanin/fanout objects. We would need fewer
				# functions. The idea would be to store fanin/fanout objects that are not called
				# concurrently, e.g., the fanins for Divide and Conquer Wukong can easily be 
				# partitioned into such groups.
				try:
					logger.trace("DAG_Orchestrator enqueue: calling simulated_lambda_function.lambda_handler(payload)")
					# See the above "So:" comment in the then-part
					# This is essentially a synchronous call to a regular Python function
# ToDo: Do we want to allow both sync and async calls? The latter done by creating a thread that does
# the call. Faster when we are invoking a real lambda, which we will do with invoke_lambda_asynch
					return_value = simulated_lambda_function.lambda_handler(payload)
					logger.trace("DAG_Orchestrator: called simulated_lambda_function.lambda_handler(payload)")
				except Exception as ex:
					logger.error("[ERROR]: " + thread_name + ": invoke_lambda_synchronously: Failed to run lambda handler for synch object: " + sync_object_name)
					logger.error(ex)					

			return return_value

		else:
			logger.trace("DAG_Orchestator enqueue: function call not Triggered")

			dummy_DAG_exec_state = DAG_executor_State(function_name = "DAG_executor", function_instance_ID = str(uuid.uuid4()))
			dummy_DAG_exec_state.return_value = 0
			dummy_DAG_exec_state.blocking = False
			# returning value to caller which is InfiniD enqueue()
			return cloudpickle.dumps(dummy_DAG_exec_state)

# manages a collection of simuated functions
class InfiniD:
	def __init__(self, DAG_info):
		# ech lambda invoked by the DAG_orchestrator gets a 
		# copy of the DAG_info
		self.dag_orchestrator = DAG_orchestrator(DAG_info)
		# DAG_info is a representtation of the DAG.
		self.DAG_info = DAG_info
		self.DAG_map = DAG_info.get_DAG_map()
		self.DAG_states = DAG_info.get_DAG_states()
		self.all_fanin_task_names = DAG_info.get_all_fanin_task_names()
		self.all_fanin_sizes = DAG_info.get_all_fanin_sizes()
		self.all_faninNB_task_names = DAG_info.get_all_faninNB_task_names()
		self.all_faninNB_sizes = DAG_info.get_all_faninNB_sizes()
		self.all_fanout_task_names = DAG_info.get_all_fanout_task_names()
		self.DAG_leaf_tasks = DAG_info.get_DAG_leaf_tasks()
		# Assuming each sync object will be stored in a separate function. We may eventualy
		# assign several objects to a function, e.g., input sets of objects that go in 
		# the same function in which case the number of functions will be the 
		# number of sets.
		self.num_Lambda_Function_Simulators = len(self.all_fanin_task_names) + (
			len(self.all_fanout_task_names)) + (
			len(self.all_faninNB_task_names)) + (
			len(self.DAG_leaf_tasks))
		self.list_of_Lambda_Function_Simulators = []
		# functions that will be invoked multiple times have each invocation locked
		# so that the current invocation must end befoer the next can start.
		self.list_of_function_locks = []
		# maps synch object name to a function index of list_of_Lambda_Function_Simulators
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
		# 
		# Create function and a lock used to ensure there are no concurrent invoctions
		# of the function.
		for _ in range(0,self.num_Lambda_Function_Simulators):
			self.list_of_Lambda_Function_Simulators.append(Lambda_Function_Simulator())	
			self.list_of_function_locks.append(Lock())
			# if using a single function to store all objects, break the loop at one funnction
			if using_single_lambda_function:
				break

	# map an object using its function name to one of the functions via the function's indez
	def map_synchronization_object(self, object_name, object_index):
			self.function_map[object_name] = object_index

	# get function, if mapping objects to functions, returns the function the object name was mapped to;
	# otherwise, it returns an anonyous function. Note: An anonyous function can only be invoked
	# once snce we have no way to refer to it by name (index or deploment name) to invoke it again.
	# Note: an anymous function could open a socket to the tcp_server_lambda and accept multple
	# messages instead of being invoked multiple times. The function could also save its
	# state and terminate so we could invoke it "again", i.e., invoke another anymous
	# function with the saved state.
	# Note: This is all for simulated functions, for now.
	def get_simulated_lambda_function(self, object_name):
		if map_objects_to_lambda_functions:
			return self.list_of_Lambda_Function_Simulators[self.function_map[object_name]]
		else:
			# anonymous fnction
			return Lambda_Function_Simulator()
	def get_function_lock(self, object_name):
		if map_objects_to_lambda_functions:
			return self.list_of_function_locks[self.function_map[object_name]]
		else:
			# anonyous functions called only once so no need to lock them
			return None
	
	def get_real_lambda_function(self, object_name):
		# returns the deployment name that object_name is mapped to. Here we 
		# assume that the function with index i is mapped to deployment "DAG_executor_i"
		if map_objects_to_lambda_functions:
			i = self.function_map[object_name]
			deployment_name = "DAG_executor_"+str(i)
			return deployment_name
		else:
			# This is the deployment for anonymous functions
			return "DAG_executor"

	# map the fanins/fanouts/faninNBs to a function, currently one object per function
	# where order does not matter
	def map_object_names_to_functions(self):
		# For fanouts, the fanin object size is always 1,
		# Map function name to a pair (empty_list,i) where i is an index, which
		# is either the list index of the function or i is used to generate
		# the deployment name of a real lambda, e.g., "DAG_executor_1" for i=1.
		# If use_single_lambda_function then we map all the names to a single
		# function, which is function 0. We do this by skippng the increment
		# of i so that i is always 0.
		# Note: This maps each name to a separate function (index). we may want to
		# map multiple names to the same function, in which case we can input
		# sets of names, so "a" and "b" mapped to 1, "c" and "d" mapped to 2, etc.
		# Note: We map_object_name_to_triggers in a separate method since
		# we want to do that mapping even if we do not map object names
		# to functions. That is, an anonymous lambda can still trigger its 
		# fanin task to run in the same lambda. This works fine for DAGs, i.e.,
		# no mapping of (sync object) names to functions is needed.
		i=0
		for object_name in self.all_faninNB_task_names:
			# Mapping name to an index i not a string and not a func(), so we do not need
			# to have created the functions yet. If using real lambdas, then, of course,
			# we do not "create a function"; we have the deployment name and we can
			# call a function using its deployment name. 
			# Note: we can use deployment names "DAG_executor_i" so we can still map to
			# an index i.
			self.map_synchronization_object(object_name,i)
			if not using_single_lambda_function:
				i += 1
			# Each name is mapped to a pair, which is (empty_list,n). The 
			# list collects results for the fan_in and fanin/fanout size n is 
			# used to determine when all of the results have been collected.
			# For a fanout, we use a faninNB object with a size of 1. That is
			# we pass the results to fanout to the faninNB of size 1 and it
			# immediately trigers its fanin (really fanout) task.

		for object_name in self.all_fanin_task_names:
			self.map_synchronization_object(object_name,i)
			if not using_single_lambda_function:
				i += 1

		for object_name in self.all_fanout_task_names:
			self.map_synchronization_object(object_name,i)
			if not using_single_lambda_function:
				i += 1

		for object_name in self.DAG_leaf_tasks:
			self.map_synchronization_object(object_name,i)
			if not using_single_lambda_function:
				i += 1

	# map the fanins/fanouts/faninNBs to a trigger, which is a pair pair (empty_list,n).
	# The list is used by the ADG_orchestrator to store the fan_in ops for a fanin object.
	def map_object_names_to_triggers(self):
		# for fanouts, the fanin object size is always 1
		# map function name to a pair (empty_list,n) where n is size of fanin/faninNB.
		# if use_single_lambda_function then we map all the names to a single
		# function, which is function 0. We do this by skippng the increment
		# of i so that i is always 0.
		# Note: This maps each name to a separate function (index). we may want to
		# map multiple names to the same function, in which case we can input
		# sets of names, so "a" and "b" mapped to 1, "c" and "d" mapped to 2, etc.

		for object_name, n in zip(self.all_faninNB_task_names, self.all_faninNB_sizes):
			# Each name is mapped to a pair, which is (empty_list,n). The 
			# list collects results for the fan_in and fanin/fanot size n is 
			# used to determine when all of the results have been collected.
			# For a fanout, we can use a faninNB object with a size of 1.
			self.dag_orchestrator.map_object_name_to_trigger(object_name,n)

		for object_name, n in zip(self.all_fanin_task_names, self.all_fanin_sizes):
			self.dag_orchestrator.map_object_name_to_trigger(object_name,n)

		for object_name in self.all_fanout_task_names:
			n = 1 # a fanout is a fanin of size 1
			self.dag_orchestrator.map_object_name_to_trigger(object_name,n)

		for object_name in self.DAG_leaf_tasks:
			n = 1 # a fanout is a fanin of size 1
			self.dag_orchestrator.map_object_name_to_trigger(object_name,n)

	# Call enqueue of DAG_orchestrator. The DAG_orchestrator will eithor store the jason_message, which is 
	# for a fanin/fanout op in the list of operations for the associated object, or if all n operaations
	# have been performed, it will invoke the function storing that object and pass the list of operations
	# to be performed on the object. If the synch objct names are mapped to functions,
	# the function call will be locked with the function's lock. This would allow multiple
	# non-overlapping invocations of the function (serialized by the lock). Since fnin/fanout
	# objects only require one invocation, they are not mapped, instead we use anonymous
	# functions wnd the invocation do not need to be locked.
	def enqueue(self,json_message):
		sync_object_name = json_message.get("name", None)
		simulated_lambda_function = self.get_simulated_lambda_function(sync_object_name)
		# If we are using anonymous functions (no mapping names to functions) then we
		# do not need function invocations to be locked. In this case,  get_function_lock()
		# returns None. Method enqueue will check whether we are using mapped or
		# anonymous functions and if we are using anonymous functins it will not
		# attempt to lock the lock.
		simulated_lambda_function_lock = self.get_function_lock(sync_object_name)
		logger.trace("XXXXXXXXXXXXXXXXXXXX InfiniD enqueue: calling self.dag_orchestrator.enqueue for sync_object " + sync_object_name)
		return_value = self.dag_orchestrator.enqueue(json_message, simulated_lambda_function, simulated_lambda_function_lock)
		logger.trace("XXXXXXXXXXXXXXXXXXXX InfiniD enqueue: called self.dag_orchestrator.enqueue for sync_object " + sync_object_name)

		return return_value


"""

Scheme: The Lambda_Function_Simulator(), which stores a sych object does:

- If create on fly: create() one local fanout/faninNB/fanin object. This is a currently 
  a "selective wait" object (not a monitor-based object) since selectve wait synch 
  objects were implemented so synch objects could be stored in a lambda. 

  Note: Since the lifetime of a fanin/fanout object is only one invocation of this 
  lambda then the synch object could be a non-select, i.e., monitor-based object.

- call object.init(), passing in:
	for FaninNB and FaninNB_select
		def init(self,kwargs): 
			if kwargs is None or len(kwargs) == 0:
				raise ValueError("FanIn requires a length. No length provided.")
			elif len(kwargs) > 9:
				raise ValueError("Error - FanIn init has too many args.")
			self._n = kwargs['n']
			self.start_state_fanin_task = kwargs['start_state_fanin_task']
			self.store_fanins_faninNBs_locally = kwargs['store_fanins_faninNBs_locally']
			self.DAG_info = kwargs['DAG_info'] 
	for Fanin and Fanin_Select: only ref kwargs['n']

- call all fan_in() ops in the list of ops received from the DAG_excutor. Passing the
  usual kwargs for fan_in (which is a normal synchronize_sync call). The 
  kwargs for the saved ops in the list created by DAG_orchestrator are all the same:
	For faninNB and FaninNB_select:
		def fan_in(self, **kwargs)
			where fan_in for non-select references:
			kwargs['result']
			kwargs['calling_task_name']
			kwargs['fanin_task_name']
			kwargs['start_state_fanin_task']
			kwargs['server']
	For fanin and fanin_select: there are only refs to result and calling_task_name.

	Note: FanIns (as oppsed to FanInNBs) do not do anything after last fan_in op  other 
	than return results. fan_in ops for faninNB triggers the fanin task with:
		try:
			logger.trace("DAG_executor_FanInNB_Select: triggering DAG_Executor_Lambda() for task " + fanin_task_name)
			lambda_DAG_exec_state = DAG_executor_State(function_name = "DAG_executor.DAG_executor_lambda", function_instance_ID = str(uuid.uuid4()), state = start_state_fanin_task)
			logger.trace ("DAG_executor_FanInNB_Select: lambda payload is DAG_info + " + str(start_state_fanin_task) + "," + str(self._results))
			lambda_DAG_exec_state.restart = False      # starting new DAG_executor in state start_state_fanin_task
			lambda_DAG_exec_state.return_value = None
			lambda_DAG_exec_state.blocking = False            
			logger.trace("DAG_executor_FanInNB_Select: Starting Lambda function %s." % lambda_DAG_exec_state.function_name) 
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

So the data for the above (possibly creating fanin objects and calling fanin ops and 
invoking or triggering fanin tasks), which is performed by the invoked lambda function
that stores a lambda object, needs to be passed in the payload to the lambda function.
Only some of this data may actually be used, e.g., if we don't create objects on the fly
we don't need creation data. And the data needed for these various things overlaps.

This data is:

1. The kwargs of the fan_in() ops. Each fan_in op has parameters. If the DAG_executor collects
fan_in messages, each message has its own kwarg values, so the kwarg data is part of the 
list of messages collected by the DAG_executor. Currently this list is put in the 
control message that is passed to the lambda.
   			message = {
				"op": "process_enqueued_fan_ins",
				"type": "DAG_executor_fanin_or_faninNB",
				# We are passing a list of operations to the lambda function, which will execute
				# them one by one in the order they were received.
				"name": list,
				"state": make_json_serializable(dummy_state),   # NOTE: no info in state
				"id": msg_id
			}
	as part of the lambda's payload: payload = {"json_message": message} on the 
	call: return_value = simulated_lambda_function.lambda_handler(payload)
	So the data needed for calling the fan_in operatons is just part of the saved 
	fan_in messages in the list in this control message, which is used for doing 
	fan_in ops on the sync objects. Thus, this data is covered.
2. Normally, the DAG_executor_driver creates all objects by sending creation messages
   to the server. These messages contain the DAG_info, which is part of the 
   DAG_executor_State that gets passed in the creation message:
		dummy_state = DAG_executor_State(function_name = "DAG_executor", function_instance_ID = str(uuid.uuid4()))
		# passing to the faninNB object:
		# its size
		dummy_state.keyword_arguments['n'] = size
		# when the faninNB completes, if we are executing tasks locally and we are not pooling,
		# we start a new thread to execute the fanin task. If we are thread pooling, we put the 
		# start state in the local work_queue. If we are using lambdas (i.e., running remotely) we 
		# invoke a lambda the execute the fanin task. If we are process pooling, then we call
		# the tcp_server and pass the fan_in operation to be performed on the faninNB object. 
		# The server will get the result of calling fan_in and if this is the last fan_in 
		# (i.e., the becomes task) this result will be non-zero and the server will put the 
		# start state of the fanin task in the work_queue, which is also on the server.
		dummy_state.keyword_arguments['start_state_fanin_task'] = DAG_states[fanin_nameNB]
		dummy_state.keyword_arguments['store_fanins_faninNBs_locally'] = store_fanins_faninNBs_locally
		dummy_state.keyword_arguments['DAG_info'] = DAG_info
	This is the info that must be passed to init() when an object is created. But now 
	the create is being initiated on-the-fly, when the object is first needed, not by 
	the DAG_executor_driver, at the start of execution. Thus, we need to obtain this 
	information and pass it to the simulated_lambda_function (which is a Lambda_Function_Simulator()):
	The information above, and where we can get it is:
	- DAG_info, which is input by the tcp_server_lambda (and tcp_server) so we don't
	  have to input it on the clients or the driver and pass it to the tcp_server(lambda).
	- store_fanins_faninNBs_locally is false, by definition, i.e., we are storing objects
	  in lambdas which means "remotely". (Storing objects on the tc_server is also "remotely.")
	- n we can get from the pair list_n_pair, which is used by the DAG_orchestrator. (The 
	  lits is the list of fanin messages, and n is the size of the fanin.)
		list_n_pair = self.object_name_to_trigger_map[sync_object_name]
		list = list_n_pair[0]
		n = list_n_pair[1]
	- start_state_fanin_task we can get from the DAG_info. We get DAG_states from
	  DAG_info and start_state_fanin_task from DAG_states.
		from above: sync_object_name = json_message.get("name", None)
		start_state_fanin_task = DAG_states[sync_object_name]
	So all of this info can be obtained and packaged in a dummy_state that is passed
	to the lambda as part of the lambdas payload.
3. The data for the triggered task payload () is: start_state_fanin_task, 
   self._results (the collected results of te fan_in ops), and DAG-info. We have this 
   data since it is part of the fan_in() methods local data. The fan_in() op already has
   the capability to create payloads for invoking lambdas to execute fanin tasks.
   It gets this data from the call to init() when the fanin object was crated and 
   the results it collects as fanin ops are performed.
   Note: For FanIn (not FanInNB) objects, the collected results are sent back to the 
   become task, i.e., the last client to call fan_in(). The last caller does not 
   append its result to the collected result since the client, of course, has its 
   own result and this makes the list of results sent back to the client smaller. 
   The clientwill append its result locally to the returned list.

How to package this data to the Lambdas. These lambdas store objects, possibly
create their objects on the fly, and either invoke a lambda to execute the 
fanin task or triger the fanin task to run in the same lambda are different 
from the simpler lambdas that just execute tasks Wukong style. To summarize the
above, the fan_in() op kwargs are part of the fan_in op messages saved by the
DAG_orchestrator.  The data needed for excuting the fanin task is data that the 
fan_in() method will have - the collected fanin results and the init() data used to 
init a created object. We just need the init() data, whch is the data used for 
on the fly object creation. So the payload to
the lmabda function is: the message for processing the list of fan_in ops and a 
DAG_excutor_state that contains the init() data.

Note: 
- If fan_in operations are not creating objects on the fly, then the 
lamba will just process the list of fanins. Objects created at the start of excution, 
instead of on the fly, are created in a lambda and we need to know in which lambda 
the object was created, so we need to map the object names to their functions
Also, for simulated functions the functions are not named, they are indexed
in an array (i.e., we map a function to an index i) so the implicit name of 
the function in Array[i] is DAG_executor_i. In general, we will map sync objects to 
"deployment names".
-If we are triggering and we are creating objects on the fly, we need to create() 
and init() the sync object before we process the list of fan_ins. The last fan_in 
op will trigger the task by calling method DAG_executor_lambda(payload), (which
does not need to access the object again.)

Note: when the fan_in op invokes a lambda to excute the fanin task, we use a
handler as usual. The current DAG_handler.py is:
	def lambda_handler(event, context):
		invocation_time = time.time()
		warm_resources['invocation_count'] = warm_resources['invocation_count'] + 1
		logger.trace(f'Invocation count: {warm_resources["invocation_count"]}, Seconds since cold start: {round(invocation_time - warm_resources["cold_start_time"], 1)}')

		start_time = time.time()
		rc = redis.Redis(host = REDIS_IP_PRIVATE, port = 6379)

		logger.trace("Invocation received. Starting DAG_executor_lambda: event/payload is: " + str(event))
		DAG_executor_lambda(event)
					
		end_time = time.time()
		duration = end_time - start_time
		logger.trace("DAG_executor_lambda finished. Time elapsed: %f seconds." % duration)
		rc.lpush("durations", duration) 

where:

def DAG_executor_lambda(payload):
    logger.trace("Lambda: started.")
    DAG_exec_state = cloudpickle.loads(base64.b64decode(payload['DAG_executor_state']))

    logger.trace("payload DAG_exec_state.state:" + str(DAG_exec_state.state))
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
    logger.trace("DAG_executor_processes: returning after work_loop.")
    return

When the fan_in op instead execute the fanin task in the same lambda, it does not 
actually invoke a lambda, i.e., it does not go thru a hanlder that calls 
DAG_executor_lambda(event); instead, the fan_in will trigger the task by calling 
DAG_executor_lambda(payload), where payload is defined as above and DAG_executor_lambda
is a method in DAG_excutor. (So DAG_executor_lambda is either invoked by the handler (as
pat of a lambda invocation) or it is called directly by the fan_in method.

We are executing fanout/faninNB operations. Note that fanins are handled the usual
way, i.e., we return a dictionary of results like usual, which is what happens when we use the
DAG_orchestrator. Actually, the fan_in() operation on a FanIn object, not a FanInNB object,
returns a DAG_executor_State with a return value that holds the dictionary of results. 

For Fanouts/FaninNBs that create their objects on the fly and execute the 
fanin task in the same lambda, we create the fanout/fanin, call the fan_in operation(s) with 
the DAG_orchestrator's saved results and let the fanout/fanin trigger the task by calling 
DAG_executor_lambda(payload); this will be the first call to DAG_executor_lambda(payload) 
in this lambda. DAG_executor_lambda executes the work loop, which means it 
executes the trggered task and does the fanouts/faninNBs/fanins after that, either: 
(1)  possibly becoming one of the fanouts and calling process_faninNBs_batch to procss the 
other fanouts/faninNBs or (2) processing the fanin op, as usual. (Note, after the task is executed, 
we can either do fanout and faninNBs, or we can do a fanin; these are mutually exclusive.

Note that for fanins, fan_in is done as usual, i.e., using "becomes". So we do not invoke a 
new lambda for the fan_in tasks of a FanIn object (only for tasks of fanouts and faninNBs)  
(As usual means we get back a 0 for non-becomes or the non-zero fan_in results for becomes.)  
Also, FanIn objects can be creeated on the fly, like FanInNB objects (where FanOut objects
are FanInNB objects of size 1))

Note: The DAG_info object has a list of fanin/fanout/faninNB names, sowe can determine
the type of an object rom its name - look for that name in these lists.

So:
In the DAG_orchestrator enqueue message, we pack all the data that the lambda function 
will need to create its lambda function on the fly (calling init()), and possibly
invoking a lambda to do the fanin task (for faninNB/fanout, but not FanIn) 
into a message. 

	msg_id = str(uuid.uuid4())
	dummy_state = DAG_executor_State(function_name = "DAG_executor", function_instance_ID = str(uuid.uuid4()))
	logger.trace("DAG_Orchestrator: Triggered: Sending 'process_enqueued_fan_ins' message to lambda function for " + sync_object_name)
	logger.trace("SDAG_Orchestrator: length of enqueue's list: " + str(len(list_of_fan_in_ops)))
	
	if not create_all_fanins_faninNBs_on_start:
		# we will invoke a lambda that stores the fanin object but this object will be created
		# on the fly.
		# This object is either a true fanin object or it is a fanout object that we are treating
		# as a fanin object of size 1
		is_fanin = sync_object_name in self.all_fanin_task_names
		dummy_state.keyword_arguments['is_fanin'] = is_fanin	
		# need to send the fanin_name. this name is in the fan_in ops in the 
		# list_of_fan_in_ops but easier to send it here as part of message so we
		# won't have to extract the first op and get the name.
		dummy_state.keyword_arguments['fanin_name'] = sync_object_name
		if not is_fanin: 
			# faninNB or fanout object, so a lambda will be given the 
			# the faninNB results for executing the fanin task or the fanin is aly a 
			# fanout so the fanned out task will be executed.
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
			# passing to the fanin object:
			# its size
			dummy_state.keyword_arguments['n'] = n	
			# the fanin objct will return the collected fanin results to the (becomes) caller 
			# instead of executing the fanin task


	# we set state.keyword_arguments before call to create()
	message = {
		"op": "process_enqueued_fan_ins",
		"type": "DAG_executor_fanin_or_faninNB",
		# We are passing a list of fanin messages to the lambda function, which will call
		# fanin() on its fanin object for each message, one by one in the order they were received.
		# The fanin object treats them as normal fanin messages, i.e., the fanin object does
		# the same thing whether it is stored on the tcp_server and receiving messages
		# directly from clients, or stored in a lambda on the tcp_server_lambda, and the
		# tcp_server is receiving messages from the clients and invoking the lambda one time
		# for each message it receives, or the tcp_server_lambda passes the messages to the 
		# DAG_orchestrator, whcih collects them and invokes the lambda once after all the fan_in
		# messages have been received. (the lambda calls fan_in for each message, then the fanin 
		# task will be executed in the sme lambad or in a new lamba (liek Wukong))
		"name": list_of_fan_in_ops,
		"state": make_json_serializable(dummy_state),
		"id": msg_id
	}
	#msg = json.dumps(message).encode('utf-8')

	payload = {"json_message": message}	

	...

	return_value = simulated_lambda_function.lambda_handler(payload)

The invoked lambda's handler will route the message to process_enqueued_fan_ins
whihc will create the object, if necessart, and call the fan_in ops in the list of ops
saved by DAG_executor.

def process_enqueued_fan_ins(self,message=None):
	....
	list_of_messages = message['name']

	if not create_all_fanins_faninNBs_on_start:
		dummy_state_for_creation_message = decode_and_deserialize(message["state"])
		fanin_name = dummy_state_for_creation_message.keyword_arguments['fanin_name']
		is_fanin = dummy_state_for_creation_message.keyword_arguments['is_fanin']
		if is_fanin:
			fanin_type = FanIn_Type
		else:
			fanin_type = FanInNB_Type

		msg_id = str(uuid.uuid4())	# for debugging
		 = {
			"op": "create",
			"type": fanin_type,
			"name": fanin_name,
			"state": make_json_serializable(dummy_state_for_creation_message),	
			"id": msg_id
		}

		#logger.trace("message_handler_lambda: process_enqueued_fan_ins: "
		#   + "create sync object " + fanin_name + "on the fly")

		#self.create_obj(creation_message)

		dummy_state_for_control_message = DAG_executor_State(function_name = "DAG_executor.DAG_executor_lambda", function_instance_ID = str(uuid.uuid4()))
		control_message = {
			"op": "createif_and_synchronize_sync",
			"type": "DAG_executor_fanin_or_faninNB_or_fanout",
			"name": None,   # filled in below with tuple of messages for each fanin op
			"state": make_json_serializable(dummy_state_for_control_message),	
			"id": msg_id
		}

		for msg in list_of_messages: ...

Note about storing FanIns in lambdas: We can stre FanIn objcts in a lambda. The 
DAG_orchestrator will invoke the lambda which will pocess the list of fan_in ops. 
The last fan_in willreturn the collected results through the returning lambda to 
the DAG_orchestrator on tcp_server_lambda, which will send the results to the client.
Invoking a real lambda to execute the fan_ins takes adds some overhead. That is, 
we could just store the FanIn object on the server and acess it like usual, without
callng a lambda. Furthermore, the DAG_orchestraor is acting like the FanIn object
since it is collecting the fan_in ops, which contain their fan_in results. So the 
DAG_orchestraor could just grab the results fron the fan_in ops and return
them to the Client. This would be faster.
"""		


