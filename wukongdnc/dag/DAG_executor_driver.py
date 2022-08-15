"""
ToDo:
1. change all state to DAG_executor_State.state, and return value for fan_in for faninNB is DAG_executor_State.erturn_value
"""

import threading
import _thread
import time
import json
import cloudpickle

import base64 
from .DFS_visit import Node
from .DFS_visit import state_info
#from DAG_executor_FanInNB import DAG_executor_FanInNB
from . import DAG_executor
from . import DAG_executor_FanInNB
from . import DAG_executor_FanIn
from .DAG_executor_State import DAG_executor_State
import uuid
import pickle

import logging 
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(asctime)s] [%(threadName)s] %(levelname)s: %(message)s')

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)

logger.addHandler(ch)

################## SET THIS #################
run_fanout_task_on_server = True
run_faninNB_task_on_server = True
#############################################

def add(inp):
    logger.debug("add: " + "input: " + str(input))
    num1 = inp['inc0']
    num2 = inp['inc1']
    sum = num1 + num2
    output = {'add': sum}
    logger.debug("add output: " + str(sum))
    return output
def multiply(inp):
    logger.debug("multiply")
    num1 = inp['add']
    num2 = inp['square']
    num3 = inp['triple']
    product = num1 * num2 * num3
    output = {'multiply': product}
    logger.debug("multiply output: " + str(product))
    return output
def divide(inp):
    logger.debug("divide")
    num1 = inp['multiply']
    quotient = num1 / 72
    output = {'quotient': quotient}
    logger.debug("quotient output: " + str(quotient))
    return output
def triple(inp):
    logger.debug("triple")
    value = inp['inc1']
    value *= 3
    output = {'triple': value}
    logger.debug("triple output: " + str(output))
    return output
def square(inp):
    logger.debug("square")
    value = inp['inc1']
    value *= value
    output = {'square': value}
    logger.debug("square output: " + str(output))
    return output
def inc0(inp):
    logger.debug("inc0")
    #value = inp['input']
    input_tuple = inp['input']
    value = input_tuple[0]
    value += 1
    output = {'inc0': value}
    logger.debug("inc0 output: " + str(output))
    return output
def inc1(inp):
    logger.debug("inc1")
    #value = inp['input']
    input_tuple = inp['input']
    value = input_tuple[0]
    value += 1
    output = {'inc1': value}
    logger.debug("inc1 output: " + str(output))
    return output

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
        

def get_leaf_task_input(name):
    if name == "inc0":
        input_tuple = (0,)
        #return int(0)	# inc0
        return input_tuple
    else:
        input_tuple = (1,)
        #return int(1)	# inc1
        return input_tuple

def get_DAG_leaf_task_inputs(DAG_leaf_tasks):
	leaf_task_inputs = []
	for name in DAG_leaf_tasks:
		input = get_leaf_task_input(name)
		leaf_task_inputs.append(input)
	return leaf_task_inputs
"""
import pickle

a = {'hello': 'world'}

with open('filename.pickle', 'wb') as handle:
    pickle.dump(a, handle, protocol=pickle.HIGHEST_PROTOCOL)

with open('filename.pickle', 'rb') as handle:
    b = pickle.load(handle)

print(a == b)
"""

def input_DAG_info():
    with open('./DAG_info.pickle', 'rb') as handle:
        DAG_info = cloudpickle.load(handle)
    return DAG_info
    
def input_DAG_map():
    DAG_map = Node.DAG_map
    #with open('DAG_map', 'rb') as handle:
    #DAG_map = pickle.load(handle)
    return DAG_map

def input_DAG_states():
    DAG_states = Node.DAG_states
    #with open('DAG_states', 'rb') as handle:
    #DAG_states = pickle.load(handle)
    return DAG_states
    
def input_all_fanin_task_names():
    all_fanin_task_names = Node.all_fanin_task_names
    #with open('all_fanin_task_names', 'rb') as handle:
    #DAG_states = pickle.load(handle)
    return all_fanin_task_names
    
def input_all_fanin_sizes():
    all_fanin_sizes = Node.all_fanin_sizes
    #with open('input_all_fanin_sizes', 'rb') as handle:
    #DAG_states = pickle.load(handle)
    return all_fanin_sizes
    
def input_all_faninNB_task_names():
    all_faninNB_task_names = Node.all_faninNB_task_names
    #with open('all_faninNB_task_names', 'rb') as handle:
    #DAG_states = pickle.load(handle)
    return all_faninNB_task_names
    
    
def input_all_faninNB_sizes():
    all_faninNB_sizes = Node.all_faninNB_sizes
    #with open('all_faninNB_sizes', 'rb') as handle:
    #DAG_states = pickle.load(handle)
    return all_faninNB_sizes
    
def input_DAG_leaf_tasks():
    DAG_leaf_tasks = ["inc0","inc1"]
    #with open('DAG_leaf_tasks.pickle', 'rb') as handle:
    #DAG_leaf_tasks = pickle.load(handle)
    return DAG_leaf_tasks

def input_DAG_leaf_task_start_states():
    DAG_leaf_tasks_start_states = [1,3]
    #with open('DAG_leaf_task_start_states.pickle', 'rb') as handle:
    #DAG_leaf_task_start_states = pickle.load(handle)
    return DAG_leaf_tasks_start_states

def run():
	# generate DAG_map using DFS_visit
    # n1 = Node(None,None,"inc0",inc0)
    # n3 = Node(None,None,"triple",triple)
    # n4 = Node(None,None,"inc1",inc1)
    # n5 = Node(Node,Node,"square",square)
    # n2 = Node(Node,Node,"add",add) 
    # n6 = Node(Node,Node,"multiply",multiply) 
    # n7 = Node(Node,Node,"divide",divide)
	
    # n1.set_succ([n2])
    # n1.set_pred([])
    # n2.set_succ([n6])	
    # n2.set_pred([n1,n4])
    # n3.set_succ([n6])
    # n3.set_pred([n4])
    # n4.set_succ([n2,n5,n3])
    # n4.set_pred([])
    # n5.set_succ([n6])
    # n5.set_pred([n4])
    # n6.set_succ([n7])
    # n6.set_pred([n2,n3,n5])
    # n7.set_succ([])
    # n7.set_pred([n6])
	
    # n1.generate_ops()
    # n4.generate_ops()
    # n2.generate_ops()
    # n3.generate_ops()
    # n5.generate_ops()
    # n6.generate_ops()
    # n7.generate_ops()
    # #Node.save_DAG_info()
	
    # logger.debug("DAG_map:")
    # for key, value in Node.DAG_map.items():
    #     logger.debug(key)
    #     logger.debug(value)
    # logger.debug("  ")
	
    # logger.debug("states:")         
    # for key, value in Node.DAG_states.items():
    #     logger.debug(key)
    #     logger.debug(value)
    # logger.debug("   ")
	
    # logger.debug("num_fanins:" + str(Node.num_fanins) + " num_fanouts:" + str(Node.num_fanouts) + " num_faninNBs:" 
    #     + " num_collapse:" + str(Node.num_collapse))
    # logger.debug("   ")
	
    # logger.debug("all_fanout_task_names")
    # for name in Node.all_fanout_task_names:
    #     logger.debug(name)
    #     logger.debug("   ")
    # logger.debug("   ")
	
    # logger.debug("all_fanin_task_names")
    # for name in Node.all_fanin_task_names :
    #     logger.debug(name)
    #     logger.debug("   ")
    # logger.debug("   ")
		  
    # logger.debug("all_faninNB_task_names")
    # for name in Node.all_faninNB_task_names:
    #     logger.debug(name)
    #     logger.debug("   ")
    # logger.debug("   ")
		  
    # logger.debug("all_collapse_task_names")
    # for name in Node.all_collapse_task_names:
    #     logger.debug(name)
    #     logger.debug("   ")
    # logger.debug("   ")
	
    # DAG_map = Node.DAG_map
    
    # logger.debug("DAG_map after assignment:")
    # for key, value in Node.DAG_map.items():
    #     logger.debug(key)
    #     logger.debug(value)
    # logger.debug("   ")
    # states = Node.DAG_states
    
    #all_fanout_task_names = Node.all_fanout_task_names
    #all_fanin_task_names = Node.all_fanin_task_names
    #all_faninNB_task_names = Node.all_faninNB_task_names
    #all_collapse_task_names = Node.all_collapse_task_names
      
    DAG_info = DAG_Info()
    
    """
    DAG_map = input_DAG_map()
    all_fanin_task_names = input_all_fanin_task_names()
    all_fanin_sizes = input_all_fanin_sizes()
    all_faninNB_task_names = input_all_faninNB_task_names()
    all_faninNB_sizes = input_all_faninNB_sizes()
    DAG_states = input_DAG_states()
    DAG_leaf_tasks = input_DAG_leaf_tasks()
    DAG_leaf_task_start_states = input_DAG_leaf_task_start_states()
	"""
    DAG_map = DAG_info.get_DAG_map()
    all_fanin_task_names = DAG_info.get_all_fanin_task_names()
    all_fanin_sizes = DAG_info.get_all_fanin_sizes()
    all_faninNB_task_names = DAG_info.get_all_faninNB_task_names()
    all_faninNB_sizes = DAG_info.get_all_faninNB_sizes()
    DAG_states = DAG_info.get_DAG_states()
    DAG_leaf_tasks = DAG_info.get_DAG_leaf_tasks()
    DAG_leaf_task_start_states = DAG_info.get_DAG_leaf_task_start_states()
    DAG_tasks = DAG_info.get_DAG_tasks()
    #DAG_leaf_task_inputs = DAG_info.get_DAG_leaf_task_inputs()
    
    #ResetRedis()
    
    start_time = time.time()
	
	#with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as websocket:
		#logger.debug("Connecting to TCP Server at %s." % str(TCP_SERVER_IP))
		#websocket.connect(TCP_SERVER_IP)
 		#create_all_fanins_and_faninNBs(websocket,DAG_map,DAG_states, input_all_fanin_task_names, input_all_faninNB_task_names)

    server = DAG_executor.DAG_executor_Synchronizer()
    
    server.create_all_fanins_and_faninNBs(DAG_map,DAG_states, all_fanin_task_names, all_fanin_sizes, all_faninNB_task_names, all_faninNB_sizes)

    DAG_leaf_task_inputs = get_DAG_leaf_task_inputs(DAG_leaf_tasks)
    
    print("DAG_leaf_tasks: " + str(DAG_leaf_tasks))
    print("DAG_leaf_task_start_states: " + str(DAG_leaf_task_start_states))
    print("DAG_leaf_task_inputs: " + str(DAG_leaf_task_inputs))

    for start_state, inp, task_name in zip(DAG_leaf_task_start_states, DAG_leaf_task_inputs, DAG_leaf_tasks):
        print("iterate")
        DAG_exec_state = DAG_executor_State(function_name = "DAG_executor", function_instance_ID = str(uuid.uuid4()), state = start_state)
        logger.debug("Starting DAG_executor for task " + task_name)
        """
        payload = {
            #"start_state": start_state,
            "input": input,
			"DAG_executor_State": DAG_exec_state,
            #"server": server
        }
												
        #invoke_lambda(payload = payload, is_first_invocation = True, n = 1, initial_permits = 0, function_name = "ComposerServerlessSync")
        invoke_lambda(payload = payload, function_name = "DAG_executor")
        """
        if run_fanout_task_on_server:
            try:
                DAG_exec_state = DAG_executor_State(function_name = "DAG_executor", function_instance_ID = str(uuid.uuid4()), state = start_state)
                logger.debug("Starting DAG_executor thread for leaf task " + task_name)
                payload = {
##rhc
                    #"state": int(start_state),
                    # DAG_executor does input = payload['input'] so input is ['input': inp]; this is passed to the executed task using:
                    #    def execute_task(task_name,input): output = Node.DAG_tasks[task_name](input)
                    # So the executed task gets ['input': inp], just like a non-leaf task gets ['output': X]. For leaf tasks, we use "input"
                    # as the label for the value.
                    "input": {'input': inp},
                    "DAG_executor_State": DAG_exec_state,
                    "DAG_info": DAG_info,
                    "server": server
                }
                _thread.start_new_thread(DAG_executor.DAG_executor_task, (payload,))
            except Exception as ex:
                logger.debug("[ERROR] Failed to start DAG_executor thread for state 1")
                logger.debug(ex)
        else:
            try:
                logger.debug("Starting DAG_executor thread for leaf task " + task_name)
                lambda_DAG_executor_State = DAG_executor_State(function_name = "DAG_executor", function_instance_ID = str(uuid.uuid4()), state = start_state)
                logger.debug ("payload is " + str(start_state) + "," + str(inp))
                lambda_DAG_executor_State.restart = False      # starting new DAG_executor in state start_state_fanin_task
                lambda_DAG_executor_State.return_value = None
                lambda_DAG_executor_State.blocking = False            
                logger.info("Starting Lambda function %s." % lambda_DAG_executor_State.function_name)
                #logger.debug("lambda_DAG_executor_State: " + str(lambda_DAG_executor_State))
                payload = {
##rhc
                    #"state": int(start_state),
                    "input": {'input': inp},
                    "DAG_executor_State": lambda_DAG_executor_State,
                    "DAG_info": DAG_info
                    #"server": server   # used to mock server during testing
                }
                ###### DAG_executor_State.function_name has not changed
                invoke_lambda(payload = payload, function_name = "DAG_executor")
            except Exception as ex:
                logger.debug("FanInNB:[ERROR] Failed to start DAG_executor Lambda.")
                logger.debug(ex)     
    """ verify results: this is synch, but no synch yet for synch objects stored in Lambdas - so comment out for lambda version
	
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as websocket:
        print("Connecting to " + str(TCP_SERVER_IP))
        websocket.connect(TCP_SERVER_IP)
        default_state = State("Composer", function_instance_ID = str(uuid.uuid4()), list_of_functions = ["FuncA", "FuncB"])

        sleep_length_seconds = 20.0
        logger.debug("Sleeping for " + str(sleep_length_seconds) + " seconds before calling synchronize_sync()")
        time.sleep(sleep_length_seconds)
        logger.debug("Finished sleeping. Calling synchronize_sync() now...")

		# Note: no-try op
        state = synchronize_sync(websocket, "synchronize_sync", "final_result", "withdraw", default_state)
        answer = state.return_value 

        end_time = time.time()
        
        error_occurred = False
        if type(answer) is str:
            logger.error("Unexpected solution recovered from Redis: %s\n\n" % answer)
            error_occurred = True
        else:
            logger.debug("Solution: " + str(answer) + "\n\n")
            expected_answer = int(72)
            if expected_answer != answer:
                logger.error("Error in answer: " + str(answer) + " expected_answer: " + str(expected_answer))
                error_occurred = True 

        if not error_occurred:
            logger.debug("Verified.")
			
		# rest is performance stuff, close websocket and return
		
		..
		
		# then main() stuff
	if __name__ == "__main__":

	"""	

    logger.debug("Sleeping")
    time.sleep(5)	
			
			
#####
def invoke_lambda(
    function_name: str = "DAG_executor",
    payload: dict = None
):
    """
    Invoke an AWS Lambda function.

    Arguments:
    ----------
        function_name (str):
            Name of the AWS Lambda function to invoke.
        
        payload (dict):
            Dictionary to be serialized and sent via the AWS Lambda invocation payload.
            This is typically expected to contain a "state" entry with a state object.
            The only time it wouldn't is at the very beginning of the program, in which
            case we automatically create the first State object
    """
    logger.debug("Creating AWS Lambda invocation payload for function '%s'" % function_name)
    logger.debug("Provided payload: " + str(payload))
												
    s = time.time()

	# The `_payload` variable is the one I actually pass to AWS Lambda.
	# The `payload` variable is passed by the user to `invoke_lambda`.
	# For each key-value pair in `payload`, we create a corresponding 
	# entry in `_payload`. The key is the same. But we first pickle]
	# the value via cloudpickle.dumps(). This returns a `bytes` object.
	# AWS Lambda uses JSON encoding to pass invocation payloads to Lambda
	# functions, and JSON doesn't support bytes. So, we convert the bytes 
	# to a string by encoding the bytes in base64 via base64.b64encode().
	# There is ONE more step, however. base64.b64encode() returns a UTF-8-encoded
	# string, which is also bytes. So, we call .decode('utf-8') to convert it
	# to a regular python string, which is stored as the value in `_payload[k]`, where
	# k is the key.
    _payload = {}
    for k,v in payload.items():
        _payload[k] = base64.b64encode(cloudpickle.dumps(v)).decode('utf-8')
											
    payload_json = json.dumps(_payload)
    logger.debug("Finished creating AWS Lambda invocation payload in %f ms." % ((time.time() - s) * 1000.0))

    logger.info("Invoking AWS Lambda function '" + function_name + "' with payload containing " + str(len(payload)) + " key(s).")
    s = time.time()
    
    # This is the call to the AWS API that actually invokes the Lambda.
    status_code = lambda_client.invoke(
        FunctionName = function_name, 
        InvocationType = 'Event',
        Payload = payload_json) 											
    logger.info("Invoked AWS Lambda function '%s' in %f ms. Status: %s." % (function_name, (time.time() - s) * 1000.0, str(status_code)))
									
												
def create_all_fanins_and_faninNBs(websocket,DAG_map,DAG_states,all_fanin_task_names,all_fanin_sizes,all_faninNB_task_names,all_faninNB_sizes):										
    """
    all_fanins = []
    all_fanin_sizes = []
    for key in DAG_map:
        state_info = DAG_map[key]
        all_fanins = all_fanins + state_info.fanins
        all_fanin_sizes = all_fanin_sizes + state_info.fanin_sizes
												
    all_faninNBs = []
    all_faninNB_sizes = []
    for key in DAG_map:
        state_info = DAG_map[key]
        all_faninNBs = all_faninNBs + state_info.faninNBs
        all_faninNB_sizes = all_faninNB_sizes + state_info.faninNB_sizes
	"""
    
    fanin_messages = []
    #for fanin_name, size in [(fanin_name,size) for fanin_name in all_fanins for size in all_fanin_sizes]:
    for fanin_name, size in zip(all_fanins,all_fanin_sizes):
        dummy_state = DAG_executor_State()
        dummy_state.keyword_arguments['n'] = size
        dummy_state.keyword_arguments['start_state_fanin_task'] = DAG_states[fanin_name]      
        msg_id = str(uuid.uuid4())	# for debugging
        message = {
            "op": "create",
            "type": "DAG_executor_FanIn",
            "name": fanin_name,
            "state": make_json_serializable(dummy_state),	
            "id": msg_id
        }
        fanin_messages.append(message)

    faninNB_messages = []
    #for fanin_nameNB, size in [(fanin_nameNB,size) for fanin_nameNB in all_faninNBs for size in all_faninNB_sizes]:
    for fanin_nameNB, size in zip(all_faninNBs,all_faninNB_sizes):
        dummy_state = DAG_executor_State()
        dummy_state.keyword_arguments['n'] = size
        dummy_state.keyword_arguments['start_state_fanin_task'] = DAG_states[fanin_name]
        ####################################################################
        dummy_state.keyword_arguments['run_faninNB_task_on_server'] = run_faninNB_task_on_server
        ####################################################################
        msg_id = str(uuid.uuid4())
        message = {
            "op": "create",
            "type": "DAG_executor_FanInNB",
            "name": faninNB_name,
            "state": make_json_serializable(dummy_state),	
            "id": msg_id
        }
        faninNB_messages.append(message)

    # msg_id for debugging
    msg_id = str(uuid.uuid4())
    logger.debug("Sending 'create_all' message to server")
    messages = (fanin_messages,faninNB_messages)
    # we set state.keyword_arguments before call to create()
    message = {
        "op": "create_all_fanins_and_faninNBs",
        "type": "DAG_executor_fanin_or_faninNB",
        "name": messages,						# Q: Fix this? usually it's a synch object name (string)
        "state": make_json_serializable(state),
        "id": msg_id
    }
												
    msg = json.dumps(message).encode('utf-8')
    send_object(msg, websocket)
    logger.debug("Sent 'create_all' message to server")

    # Receive data. This should just be an ACK, as the TCP server will 'ACK' our create_all() call.
    ack = recv_object(websocket)

def lambda_handler(event, context):
    start_time = time.time()
    rc = redis.Redis(host = REDIS_IP_PRIVATE, port = 6379)
    logger.debug("Invocation received. event/payload: " + str(event))
    logger.debug("Starting DAG_executor: payload is: " + str(event))

    DAG_executor.DAG_executor(event)
				 
    end_time = time.time()
    duration = end_time - start_time
    logger.debug("DAG_executor finished. Time elapsed: %f seconds." % duration)
    rc.lpush("durations", duration)    
		

if __name__ == "__main__":
    run()