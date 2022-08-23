import threading
import _thread
import time

from .DFS_visit import Node
from .DFS_visit import state_info
#from DAG_executor_FanInNB import DAG_executor_FanInNB
from . import  DAG_executor_FanInNB
from . import  DAG_executor_FanIn
from . import DAG_executor_driver
from .DAG_executor_State import DAG_executor_State
import uuid

from .util import pack_data

from threading import RLock
import queue

import logging 
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(asctime)s] [%(threadName)s] %(levelname)s: %(message)s')

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)

logger.addHandler(ch)

################## SET THIS #################
run_fanout_task_on_server = DAG_executor_driver.run_fanout_task_on_server
run_faninNB_task_on_server = DAG_executor_driver.run_faninNB_task_on_server
using_workers = DAG_executor_driver.using_workers
num_workers = DAG_executor_driver.num_workers
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
    value = inp['input']
    value += 1
    output = {'inc0': value}
    logger.debug("inc0 output: " + str(output))
    return output
def inc1(inp):
    logger.debug("inc1")
    value = inp['input']
    value += 1
    output = {'inc1': value}
    logger.debug("inc1 output: " + str(output))
    return output

class Counter(object):
    def __init__(self,initial_value=0):
        self.value = initial_value
        self._lock = threading.Lock()
        
    def increment_and_get(self):
        with self._lock:
            self.value += 1
            return self.value
  
# This is taking role of server. 
class DAG_executor_Synchronizer(object):
    synchronizers =  {} 
    mutex =  RLock()
    def __init__(self):
        pass
    
    # This is for fanin, which can be a try_fanin.
    # faninNB is asynch and w/always terminate
    # ToDo: If we create all fanins/faninNBs at beginning then we can just call the usual fan_in method 
    #       and don't need a lock.
    def create_and_fanin(self,keyword_arguments):
        # create new fanin with specified name if it hasn't been created 

        # create new faninNB with specified name if it hasn't been created 
		# Here are the keyword arguments:
        fanin_task_name = keyword_arguments['fanin_task_name']
        n = keyword_arguments['n']	# size
		# used by FanInNB:
        #start_state_fanin_task = keyword_arguments['start_state_fanin_task']
        # where: keyword_arguments['start_state_fanin_task'] = DAG_states[name]
        output = keyword_arguments['result']
        calling_task_name = keyword_arguments['calling_task_name']
        DAG_executor_State = keyword_arguments['DAG_executor_State']
        server = keyword_arguments['server']
		# used by FanInNB:
        # run_faninNB_task_on_server = keyword_arguments['run_faninNB_task_on_server']  # option set in DAG_executor
  
        # create_and_fan_in op must be atomic (as will be on server, with many client callers)
        server.mutex.acquire()

        inmap = fanin_task_name in server.synchronizers
        logger.debug ("calling_task_name: " + calling_task_name + " fanin_task_name: " + fanin_task_name
			+ " inmap: " + str(inmap))
        #if not fanin_task_name in DAG_executor_Synchronizer.synchronizers:
        if not inmap: 	# fanin_task_name in server.synchronizers:
            FanIn = DAG_executor_FanIn.DAG_executor_FanIn(n, fanin_task_name) # initial_n = 0, monitor_name = None
            FanIn.init(**keyword_arguments)
            logger.debug("calling_task_name: " + calling_task_name + " fanin_task_name: " + fanin_task_name
				+ " DAG_executor_Synchronizer: create_and_fanin: create caching new fanin with name '%s'" % (fanin_task_name))
            logger.debug(" DAG_executor_Synchronize: create_and_fanin: create caching new fanin with name '%s'" % (fanin_task_name))
            server.synchronizers[fanin_task_name] = FanIn # Store Synchronizer object.
        
        inmap = fanin_task_name in server.synchronizers  
        logger.debug ("calling_task_name: " + calling_task_name + " fanin_task_name: " + fanin_task_name + " inmap: " + str(inmap))
        FanIn = server.synchronizers[fanin_task_name]  
        
        # call fanin
        try_return_value = FanIn.try_fan_in(**keyword_arguments)
        #logger.debug("calling_task_name: " + calling_task_name + " FanIn: try_return_value: " + str(try_return_value))
        logger.debug("fanin_task_name: " + fanin_task_name + " try_return_value: " + str(try_return_value))
        if try_return_value:   # synchronize op will execute wait so tell client to terminate
            DAG_executor_State.blocking = True 
            DAG_executor_State.return_value = None 
			
			#FanInNB gets DAG_executor_State in kwargs when it starts a new executor to execute the fanin task.
			#FanIn does not access the DAG_executor_State in kwargs
			
			# return is: self._results, restart, where restart is always 0 and results is 0 or a map
            return_value, restart = FanIn.fan_in(**keyword_arguments)
        else:
            return_value, restart = FanIn.fan_in(**keyword_arguments)

            logger.debug("fanin become task output:" + str(output))
            logger.debug("calling_task_name:" + calling_task_name)
            # Add our result to the results (instead of sending it to the fanin on the server and server sending it back
            return_value[calling_task_name] = output
            DAG_executor_State.return_value = return_value
            DAG_executor_State.blocking = False 
			
        server.mutex.release()
		# This is returned to process_fanin which returns it to DAG_executor; DAG_executtor will look at blocking
		# and if not blocking the return_value to get the input as if not blocking then becomes fanin task.
        return DAG_executor_State
        
    # faninNB is asynch w/client always terminate
    def create_and_faninNB(self,**keyword_arguments): 
        # keyword_arguments['fanin_task_name'] = name
        # keyword_arguments['n'] = n
        # keyword_arguments['start_state_fanin_task'] = DAG_states[name]
        
        # create new faninNB with specified name if it hasn't been created 
        fanin_task_name = keyword_arguments['fanin_task_name']
        n = keyword_arguments['n']
        #start_state_fanin_task = keyword_arguments['start_state_fanin_task']
        #output = keyword_arguments['result']
        calling_task_name = keyword_arguments['calling_task_name']
        DAG_executor_State = keyword_arguments['DAG_executor_State']
        server = keyword_arguments['server']
        #run_faninNB_task_on_server = keyword_arguments['run_faninNB_task_on_server']  # option set in DAG_executor
        #DAG_info = keyword_arguments['DAG_info']
        
        # create and fan_in must be atomic
        server.mutex.acquire()
        inmap = fanin_task_name in server.synchronizers
        logger.debug ("calling_task_name: " + calling_task_name + " fanin_task_name: " + fanin_task_name + " inmap: " + str(inmap))
        if not inmap: 	# fanin_task_name in server.synchronizers:
            FanInNB = DAG_executor_FanInNB.DAG_executor_FanInNB(n, fanin_task_name) # initial_n = 0, monitor_name = None
            FanInNB.init(**keyword_arguments)
            logger.debug("calling_task_name: " + calling_task_name + " fanin_task_name: " + fanin_task_name
                + " DAG_executor_Synchronizer: create_and_faninNB: create caching new fanin with name '%s'" % (fanin_task_name))
            logger.debug(" DAG_executor_Synchronizer: create_and_faninNB: create caching new fanin with name '%s'" % (fanin_task_name))

            server.synchronizers[fanin_task_name] = FanInNB # Store Synchronizer object.
 
        inmap = fanin_task_name in server.synchronizers  
        logger.debug ("calling_task_name: " + calling_task_name + " fanin_task_name: " + fanin_task_name + " inmap: " + str(inmap))
        FanInNB = server.synchronizers[fanin_task_name]
        
		# Note: in real code, we would return here so caller can quit, letting server do the op.
		# Here, we can just wait for op to finish, then return. Caller has nothing to do but 
		# quit since nothing to do after a fanin.

        # return is: None, restart, where restart is always 0 and return_value is None; and makes no change to DAG_executor_State	
        return_value, restart = FanInNB.fan_in(**keyword_arguments)

#ToDo: if we always return a state:
        DAG_executor_State = keyword_arguments['DAG_executor_State']
        DAG_executor_State.blocking = True 
		# for faninNB there is never a result, even for last caller since No Becomes (NB)
		# Note: We could have fan_in for FanInNB return the results for debugging
        DAG_executor_State.return_value = None 

        server.mutex.release()
        
# ToDo: may return DAG_executor_State to be consistent - it can be ignored.
# No: this is not being returned to user, this goes to DAG_executor, which will just process fanins next.
        return 0
        
    def create_all_fanins_and_faninNBs(self,DAG_map,DAG_states,DAG_info,all_fanin_task_names, all_fanin_sizes, all_faninNB_task_names, all_faninNB_sizes):
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
        #for fanin_name, size in [(fanin_name,size) for fanin_name in all_fanin_task_names for size in all_fanin_sizes]:
        for fanin_name, size in zip(all_fanin_task_names,all_fanin_sizes):
            dummy_state = DAG_executor_State()
			# keywword_argumments used in init()
            dummy_state.keyword_arguments['n'] = size
            #dummy_state.keyword_arguments['fanin_task_name']  = fanin_name
            msg_id = str(uuid.uuid4())	# for debugging
            message = {
                "op": "create",
                "type": "DAG_executor_FanIn",
                "name": fanin_name,
                "state": dummy_state,	
                "id": msg_id
            }
            fanin_messages.append(message)

        faninNB_messages = []
        #for fanin_nameNB, size in [(fanin_nameNB,size) for fanin_nameNB in all_faninNB_task_names for size in all_faninNB_sizes]:
        for fanin_nameNB, size in zip(all_faninNB_task_names,all_faninNB_sizes):
            dummy_state = DAG_executor_State()
			# keywword_argumments used in init()
            dummy_state.keyword_arguments['n'] = size
            dummy_state.keyword_arguments['start_state_fanin_task'] = DAG_states[fanin_nameNB]
            dummy_state.keyword_arguments['run_faninNB_task_on_server'] = True
            dummy_state.keyword_arguments['DAG_info'] = DAG_info
            msg_id = str(uuid.uuid4())
            message = {
                "op": "create",
                "type": "DAG_executor_FanInNB",
                "name": fanin_nameNB,
                "state": dummy_state,	
                "id": msg_id
            }
            faninNB_messages.append(message)

        # create new faninNB with specified name if it hasn't been created 
        #fanin_task_name = keyword_arguments['fanin_task_name']
        #n = keyword_arguments['n']
        #start_state_fanin_task = keyword_arguments['start_state_fanin_task']
        # where: keyword_arguments['start_state_fanin_task'] = DAG_states[name]
        #output = keyword_arguments['result']
        #calling_task_name = keyword_arguments['calling_task_name']
        #DAG_executor_State = keyword_arguments['DAG_executor_State']
        #server = keyword_arguments['server']
            
        # create_and_fan_in op must be atomic (as will bee on server, with many client callers)
        #server.mutex.acquire()

        print(str(fanin_messages))
        print(str(faninNB_messages))
        for msg in fanin_messages:
            #logger.debug("calling_task_name: " + calling_task_name + " fanin_task_name: " + fanin_task_name)
            fanin_task_name = msg["name"]
            DAG_exec_state = msg['state']
            n = DAG_exec_state.keyword_arguments['n']
            inmap = fanin_task_name in self.synchronizers
            logger.debug (" inmap before: " + str(inmap))
            #if not fanin_task_name in DAG_executor_Synchronizer.synchronizers:
            if not inmap: 	# fanin_task_name in self.synchronizers:
                FanIn = DAG_executor_FanIn.DAG_executor_FanIn(n, fanin_task_name) # initial_n = 0, monitor_name = None
                FanIn.init(**(msg['state'].keyword_arguments))
                logger.debug(" create_and_fanin: create caching new fanin with name '%s'" % (fanin_task_name))
                self.synchronizers[fanin_task_name] = FanIn # Store Synchronizer object.
            inmap = fanin_task_name in self.synchronizers  
            logger.debug (" inmap after: " + str(inmap))
            FanIn = self.synchronizers[fanin_task_name] 
            
        for msg in faninNB_messages:
            faninNB_task_name = msg["name"]
            DAG_exec_state = msg['state']
            n = DAG_exec_state.keyword_arguments['n']
            #start_state_fanin_task = DAG_exec_state.keyword_arguments['start_state_fanin_task']
            inmap = faninNB_task_name in self.synchronizers
            logger.debug (" inmap before: " + str(inmap))
            #if not fanin_task_name in DAG_executor_Synchronizer.synchronizers:
            if not inmap: 	# faninNB_task_name in self.synchronizers:
                FanInNB = DAG_executor_FanInNB.DAG_executor_FanInNB(n, faninNB_task_name) # initial_n = 0, monitor_name = None
                FanInNB.init(**(msg['state'].keyword_arguments))
                logger.debug(" create_and_fanin: create caching new fanin with name '%s'" % (faninNB_task_name))
                self.synchronizers[faninNB_task_name] = FanInNB # Store Synchronizer object.
            inmap = faninNB_task_name in self.synchronizers  
            logger.debug (" inmap after: " + str(inmap))
            FanInNB = self.synchronizers[faninNB_task_name]  

		# ToDo: may return DAG_executor_State to be consistent - it can be ignored.
        return 0
     
#DES = DAG_executor_Synchronizer()
				 
def create_and_faninNB_task(kwargs):
    logger.debug("DAG_executor_task: call DAG_excutor")
    server = kwargs['server']
    # Not using return_value from faninNB since faninNB starts the fanin task, i.e., there is No Become
    return_value = server.create_and_faninNB(**kwargs)

# used to execute a task; need to give the task its "input" map
#name_to_function_map = {'inc0': inc0, 'inc1': inc1, 'add': add, 'multiply': multiply, 'triple': triple, 'square': square, 'divide':divide}


# execute task from name_to_function_map with key task_name
def execute_task(task, args):
    logger.debug("input of execute_task is: " + str(args))
    #output = task(input)
    #for i in range(0, len(args)):
    #    print("Type of argument #%d: %s" % (i, type(args[i])))
    #    print("Argument #%d: %s" % (i, str(args[i])))
    output = task(*args)
    return output
				 			 
def process_faninNBs(faninNBs, faninNB_sizes, calling_task_name, DAG_states, DAG_exec_State, output, DAG_info, server):
    logger.debug("process_faninNBs")
	# There may be multiple faninNBs; we cannot become one, by definition.
	# Note: This thread cannot become since it may need to become a fanout.
	# Or: faninNB is asynch wo/terminate, so create a thread that does the 
	# stuff in DES.create_and_fanin(), and this thread keeps going
	# Note: For real version, DAG_executor task is a Lambda? or thread of a Lambda?
	#       Do we want faninNB to run as a thread of this current Lambda? with other
	#       faninNB threads and fanout become threads? Or always start a new Lambda?
    #       Or make that decision based on this Lammba's load?
  
    #ToDo: not passing State to FanIn, we set it after return?
    # No changes to State - this is asynch we get no State back, but we do pass it for asynch ops
    # in general since they can be asynch w/terminate and restart
    # for name, n in [(name,n) for name in faninNBs for n in faninNB_sizes]:
    for name, n in zip(faninNBs,faninNB_sizes):

        # create new faninNB with specified name if it hasn't been created  
        start_state_fanin_task  = DAG_states[name]
        # The FanINNB fan_in will change the (start) state of the DAG_executor_State. (We are making the exact same change here
        # so these assignments to state are redunadant. If this is part of a local test (any (remote) lambda created will have its own DAG_executor_State)
        # then we need a new DAG_executor_State for the FanInNB; otherwise, the DAG_executor_State of the become task of the fanouts and the 
        # DAG_executor_State of the new thread that will execute the faninNB task (i.e., after the fan_in completes) will share the same 
        # DAG_executor_State object, and changing the state for the fanout and for the faninNB task is a race condition. Do create a new
        # DAG_executor_State object for the faninNB to eliminate the possibility of sharing.
        new_DAG_exec_State = DAG_executor_State(state = start_state_fanin_task)   
       	# keyword_arguments['state'] = 
        keyword_arguments = {}
        keyword_arguments['fanin_task_name'] = name
        keyword_arguments['n'] = n
        keyword_arguments['start_state_fanin_task'] = start_state_fanin_task
        keyword_arguments['result'] = output
        keyword_arguments['calling_task_name'] = calling_task_name
        keyword_arguments['DAG_executor_State'] = new_DAG_exec_State # given to the thread/lambda that executes the fanin task.
        keyword_arguments['server'] = server
        keyword_arguments['run_faninNB_task_on_server'] = run_faninNB_task_on_server
        keyword_arguments['DAG_info'] = DAG_info
				 
		#Q: kwargs put in DAG_executor_State keywords and on server it gets keywords from state and passes to create and fanin
				
        try:
            logger.debug("Starting create_and_fanin for faninNB " + name)
            NBthread = threading.Thread(target=create_and_faninNB_task, name=("create_and_faninNB_task_"+name), args=(keyword_arguments,))
            NBthread.start()
            #_thread.start_new_thread(create_and_faninNB_task, (keyword_arguments,))
        except Exception as ex:
            logger.error("[ERROR] Failed to start create_and_faninNB_task thread.")
            logger.debug(ex)
			
    # return value not used; will process any fanouts next; no change to DAG_executor_State
    return 0
				 
#Todo: Global fanin, which determines whether last caller or not, and delegates
#      collection of results to local fanins in infinistore executors.

def process_fanouts(fanouts, calling_task_name, DAG_states, DAG_exec_State, output, DAG_info, server):
    logger.debug("process_fanouts, length is " + str(len(fanouts)))
    
    #process become task 
    become_task = fanouts[0]
    logger.debug("fanout for " + calling_task_name + "become_task is " + become_task)
    # Note:  We will keep using DAG_exec_State for the become task. If we are running everything on a single machine, i.e., 
    # no server or Lambdas, then faninNBs should use a new DAG_exec_State. Fanins and fanouts/faninNBs are mutually exclusive
    # so, e.g., the become fanout and a fanin cannot use the same DAG_exec_Stat. Same for a faninNb and a fanin, at least 
    # as long as we do not generate faninNBs and a fanin for the same state/task. We could optimize so that we can have
    # a fanin (with a become task and one or more faninNBs.
    become_start_state = DAG_states[become_task]  
    # change state for this thread so that this thread will become the new task, i.e., execute another iteration with the new state
    DAG_exec_State.state = DAG_states[become_task]

    logger.debug ("fanout for " + calling_task_name + " become_task state is " + str(become_start_state))  
    fanouts.remove(become_task)
    logger.debug("new fanouts after remove:" + str(fanouts))
    
    # process rest of fanins
    logger.debug("run_fanout_task_on_server:" + str(run_fanout_task_on_server))
    for name in fanouts:
         #rhc queue
        work_queue.put(DAG_states[name])
        if run_fanout_task_on_server:
            try:
                logger.debug("Starting fanout DAG_executor thread for " + name)
                fanout_task_start_state = DAG_states[name]
                task_DAG_executor_State = DAG_executor_State(state = fanout_task_start_state)

                #rhc task_inputs
                #output_tuple = (calling_task_name,)
                #output_dict[calling_task_name] = output
                logger.debug ("fanout payload for " + name + " is " + str(fanout_task_start_state) + "," + str(output))
                payload = {
##rhc
                    #"state": fanout_task_start_state, 
                    #rhc task_inputs
                    "input": output,
                    "DAG_executor_State": task_DAG_executor_State,
                    "DAG_info": DAG_info,
                    "server": server
                }
                _thread.start_new_thread(DAG_executor_task, (payload,))
            except Exception as ex:
                logger.error("[ERROR] Failed to start DAG_executor thread for " + name)
                logger.debug(ex)
        else:
            try:
                logger.debug("Starting fanout DAG_executor Lambda for " + name)
                fanout_task_start_state = DAG_states[name]
                # create a new DAG_executor_State object so no DAG_executor_State object is shared by fanout/faninNB threads in a local test.
                lambda_DAG_executor_State = DAG_executor_State(state = fanout_task_start_state)
                logger.debug ("payload is " + str(fanout_task_start_state) + "," + str(output))
                lambda_DAG_executor_State.restart = False      # starting new DAG_executor in state start_state_fanin_task
                lambda_DAG_executor_State.return_value = None
                lambda_DAG_executor_State.blocking = False            
                logger.info("Starting Lambda function %s." % lambda_DAG_executor_State.function_name)
                #logger.debug("lambda_DAG_executor_State: " + str(lambda_DAG_executor_State))
                payload = {
##rhc
                    #"state": int(fanout_task_start_state),
                    "input": output,
                    "DAG_executor_State": lambda_DAG_executor_State,
                    "DAG_info": DAG_info
                    #"server": server   # used to mock server during testing
                }
                ###### DAG_executor_State.function_name has not changed
                DAG_executor_driver.invoke_lambda_DAG_executor(payload = payload, function_name = "DAG_executor")
            except Exception as ex:
                logger.error("FanInNB:[ERROR] Failed to start DAG_executor Lambda.")
                logger.debug(ex)       
  
    return become_start_state

def process_fanins(fanins, faninNB_sizes, calling_task_name, DAG_states, DAG_executor_State, output, server):
    logger.debug("process_fanins")
    # assert len(fanins) == len(faninNB_sizes) ==  1

    # call synch-op try-op on the fanin with name f, passing output and start_state.
    # The return value will have [state,input] tuple? Or we know state when we call
    # the fanin, we just don't know whether we become task. If not, we return. If
    # become, then we set state = states[f] before calling fanin.
    # Note: Only one fanin object so only one state for become. We pass our State and get it back
    # with the fanin results for the other tasks that fanin. We will add our results in create_and_fanin
    # using return_value[calling_task_name] = output[calling_task_name], which is add our results from output
    # to those returned by the fanin.
    
    keyword_arguments = {}
    logger.debug(str(fanins))
    keyword_arguments['fanin_task_name'] = fanins[0]
    keyword_arguments['n'] = faninNB_sizes[0]
    #keyword_arguments['start_state_fanin_task'] = DAG_states[fanins[0]]
    keyword_arguments['result'] = output
    keyword_arguments['calling_task_name'] = calling_task_name
    keyword_arguments['DAG_executor_State'] = DAG_executor_State
    keyword_arguments['server'] = server
	     
    DAG_executor_State = server.create_and_fanin(keyword_arguments)
    return DAG_executor_State
	
# Driver will not have any payload args unless it will invoke the leaf nodes with their inputs
# Example: payload = {"list of functions:" ..., 'starting_input': 0}
# Example: invoke_lambda(payload = payload, is_first_invocation = True, n = 1, initial_permits = 0, function_name = "ComposerServerlessSync")
# where invoker is: if is_first_invocation:
"""
        state = State(
            function_name = "Composer",  # this is name of Lambda function
            function_instance_ID = str(uuid.uuid4()),
            restart = False,
            pc = 0,
            return_value = None,
            blocking = False,
			# Note: could use a "starting_input" from the driver, instead of state.return_value, used for results from fanin/faninNB
			state = 0
            keyword_arguments = {
                'n': n,
                'initial_permits': initial_permits
            }
        )
        _payload["state"] = base64.b64encode(cloudpickle.dumps(state)).decode('utf-8')
		...
		payload_json = json.dumps(_payload)
		...
   		status_code = lambda_client.invoke( FunctionName = function_name, InvocationType = 'Event', Payload = payload_json) 
"""
# where handler does: 
#	state = cloudpickle.loads(base64.b64decode(event["state"]))
"""
    if target == "Composer":
        composer = Composer(state = state)
        composer.execute()
		
	Note: We can call DAG_execute(state)
"""

work_queue = queue.Queue()
data_dict = {}

counter = None
if using_workers:
    counter = Counter(0)
				 
def DAG_executor(payload):		 
    # Note: could instead use a "state" parameter. Then we have state.starting_input and state.return_value so would need
    # to know which to acccess, as in: if first_invocation, where first_invocation is in state. Or always use
    # state.return_value so we don't need to check and save first_invocation:
    #   input = state.return_value
    # Note: for blocking try_fanin, it's not really a restart, it means we will be started in a specified fanin/faninNB
    #     task state - there is no other state to save/restore. So DAG_executor can get input and give to execute_task.
    #     try_fanin: with state.blocking = 0 and state.return_value = results map. Then we continue in state 
    #     states[fanin_task_name], which we can set before call. try_fanin w/state.blocking = 1 and state.return_value = 0, 
    #     in which case we terminate/return and no restart on fanins.
    #	  So: not really saving state and restoring on restart - fanin-block=>term w/ no restart; fanin-noblock means
    #         we transition to next state which is DAG_states[fanin_task_name]; faninNB => asynch continue on to fanouts.
    #	  But fanin has state.return_value and we pass keyword args to fanin/faninNB in real version.
    # Q:  Do we allow synchronous fanin? Yes if server, no if Lambda (for now)?
    #     faninNB: is like an async call with always terminate, so never restarted. Always term as in
    #      if self.state.blocking:
    #         self.state.blocking = False
    #         return
    #      where synchronize_async_terminate() doesn't wait for return value from server and sets self.state.blocking to True.
    # Note: may need to change dask leaf node inputs so, e.g., withdraw input from BB - then need a first_invocation
    # to control this?
    # Note: invoking DAG_executor() is different from executing task. The DAG_executor needs to get input and present it
    # to execute_task in the proper form.
		
	# use DAG_executor_state.state
    DAG_executor_State = payload['DAG_executor_State']
##rhc
    ##state = payload['state'] # refers to state var, not the usual State of DAG_executor 
    ##logger.debug("state:" + str(state))
    ##DAG_executor_State.state = payload['state']
    logger.debug("payload state:" + str(DAG_executor_State.state))
    # For leaf task, we get  ['input': inp]; this is passed to the executed task using:
    #    def execute_task(task_name,input): output = DAG_info.DAG_tasks[task_name](input)
    # So the executed task gets ['input': inp], just like a non-leaf task gets ['output': X]. For leaf tasks, we use "input"
    # as the label for the value.
    #rhc task_inputs
    task_payload_inputs = payload['input']
    logger.debug("DAG_executor starting payload input:" +str(task_payload_inputs) + " payload state: " + str(DAG_executor_State.state) )
  
    DAG_info = payload['DAG_info']
    DAG_map = DAG_info.get_DAG_map()
    DAG_tasks = DAG_info.get_DAG_tasks()
    num_tasks_to_execute = len(DAG_tasks)
    logger.debug("DAG_executor: num_tasks_to_execute: " + str(num_tasks_to_execute))
    server = payload['server']
    
    #ToDo:
	#if input == None:
		#pass  # withdraw input from payload.synchronizer_name
    
    worker_needs_input = using_workers and True

    while (True):

#ToDo: multP's don't always get work from the queue. i.e., no get when no put.
# - collapse: no get; fanout: become so not get; fanin: - become no get + not-become do gets
# - faninB: no becomes - last caller does put, all callers need to do gets
# ==> set get_flag if process does faninNB or it does fanin and is not last caller; then 
# ==> if get_flag, do a get and reset get_flag. (Need to check return from process_fanin and 
# ==> set get_flag accordingly.) Also, don;t do put if you don't do get.

        if using_workers:
            if worker_needs_input:
                DAG_executor_State.state = work_queue.get(block=True)
                if DAG_executor_State.state == -1:
                    work_queue.put(-1)
                    return
                worker_needs_input = False # default
                logger.debug("DAG_executor: Worker access work_queue: process state: " + str(DAG_executor_State.state))
            else:
                 logger.debug("DAG_executor: Worker don't access work_queue: process state: " + str(DAG_executor_State.state))
            num_tasks_executed = counter.increment_and_get()
            if num_tasks_executed == num_tasks_to_execute:
                work_queue.put(-1)

##rhc
        logger.debug ("access DAG_map with state " + str(DAG_executor_State.state))
        state_info = DAG_map[DAG_executor_State.state]
        ##logger.debug ("access DAG_map with state " + str(state))
        ##state_info = DAG_info.DAG_map[state]
        logger.debug("state_info: " + str(state_info) + " execute task: " + state_info.task_name)
 
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

#ToDo: Lambdas use payload inputs; 
        # using map DAG_tasks from task_name to task
        task = DAG_tasks[state_info.task_name]
        #rhc task_inputs
        # a tuple f input task names, not actual inputs. The inputs retrieved from data_dict,
        # Lambdas need to put payload inputs in data_dict then get them from data_dict
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
        """ where:
            def execute_task(task,args):
                logger.debug("input of execute_task is: " + str(args))
                #output = task(input)
                output = task(*args)
                return output
        """
        logger.debug("execute_task output: " + str(output))
        data_dict[state_info.task_name] = output

        logger.debug("data_dict: " + str(data_dict))

        if len(state_info.collapse) > 0:
            if len(state_info.fanins) + len(state_info.fanouts) + len(state_info.faninNBs) > 0:
                logger.error("Error1")
            # execute collapsed task next - transition to new state and iterate loop
##rhc
            DAG_executor_State.state = DAG_info.get_DAG_states()[state_info.collapse[0]]
            ##state = DAG_info.get_DAG_states()[state_info.collapse[0]]
            # output of just executed task is input of next (collapsed) task
            #input = output

            #rhc task_inputs
            #task_inputs = (state_info.task_name,)
            # We get new state_info and then state_info.task_inputs when we iterate

#ToDo: Don't add to work_queue just do it
            # rhc queue
            #work_queue.put(DAG_executor_State.state)
            worker_needs_input = False

        elif len(state_info.faninNBs) > 0 or len(state_info.fanouts) > 0:
            # assert len(collapse) + len(fanin) == 0
            # If len(state_info.collapse) > 0 then there are no fanins, fanouts, or faninNBs and we will not excute this elif or the else
            if len(state_info.faninNBs) > 0:
				# asynch + terminate + start DAG_executor in start state
                process_faninNBs(state_info.faninNBs, state_info.faninNB_sizes, 
					state_info.task_name, DAG_info.get_DAG_states(), DAG_executor_State, output, DAG_info, server)
                # there can be faninNBs and fanouts.

                # Note: If we only process faninNBs in this state then we will need more work.
                # However, we check fanouts next and set worker_needs_input to true (false)
                # if there are (are not) any fanouts. So this is only for documentation.
                if using_workers:
                    worker_needs_input = True
            if len(state_info.fanouts) > 0:
				# start DAG_executor in start state w/ pass output or deposit/withdraw it
				# if deposit in synchronizer need to pass synchronizer name in payload. If synchronizer stored
				# in Lambda, then fanout task executed in that Lambda.
##rhc
                DAG_executor_State.state = process_fanouts(state_info.fanouts, state_info.task_name, DAG_info.get_DAG_states(), DAG_executor_State, output, DAG_info, server)
                logger.debug("become state:" + str(DAG_executor_State.state))
                ##state = process_fanouts(state_info.fanouts, DAG_info.get_DAG_states(), DAG_executor_State, output, server)
                ##logger.debug("become state:" + str(state))
                #input = output
                #rhc task_inputs
                #task_inputs = (state_info.task_name,)
                # We get new state_info and then state_info.task_inputs when we iterate
                if using_workers:   # we are become task so we have more work
                    worker_needs_input = False
                #Don't add to work_queue just do it = False
                #rhc: queue
                #work_queue.put(DAG_executor_State.state)

            else:   # If there were fanouts then continue with become task, else this thread is done.
                if using_workers:
                    worker_needs_input = True
                else:   # workers don't stop until there's no more work to be done
                    return
        elif len(state_info.fanins) > 0:
            # assert len(state_info.faninNBs)  + len(state_info.fanouts) + len(collapse) == 0
            # if faninNBs or fanouts then can be no fanins. length of faninNBs and fanouts must be 0 
##rhc
            DAG_executor_State.state = DAG_info.get_DAG_states()[state_info.fanins[0]]
			#state = DAG_info.get_DAG_states()[state_info.fanins[0]]
			#if len(state_info.fanins) > 0:
			#ToDo: Set next state before process_fanins, returned state just has return_value, which has input.
			# single fanin, try-op w/ returned_state.return_value or restart with return_value or deposit/withdraw it

            returned_state = process_fanins(state_info.fanins, state_info.fanin_sizes, state_info.task_name, DAG_info.get_DAG_states(),  DAG_executor_State, output, server)
            logger.debug("After process_fanin: " + str(state_info.fanins[0]) + " returned_state.blocking: " + str(returned_state.blocking) + ", returned_state.return_value: "
##rhc
                + str(returned_state.return_value) + ", state: " + str(DAG_executor_State.state))
			##+ str(returned_state.return_value) + ", state: " + str(state))
            if returned_state.blocking:
                if using_workers:
                    worker_needs_input = True
                else:
                    return
            else:
                if using_workers:
                    worker_needs_input = False
            #ToDo: Don't add to work_queue just do it
            # rhc queue
            #work_queue.put(DAG_executor_State.state)

            #rhc task_inputs
            #else:
            #    input = returned_state.return_value
            # We get new state_info and then state_info.task_inputs when we iterate. For local exection,
            # the fanin task will get its inputs from the data dictionary -they were placed there after
            # tasks executed. For non-local, we will need to add them to the local ata dictionary.

        else:
##rhc
            logger.debug("state " + str(DAG_executor_State.state) + " after executing task " +  state_info.task_name + " has no fanouts, fanins, or faninNBs; return")
            ##logger.debug("state " + str(state) + " after executing task " +  state_info.task_name + " has no fanouts, fanins, or faninNBs; return")
            if using_workers:
                worker_needs_input = True
            else:
                return
					
#Local tests
def DAG_executor_task(payload):
    DAG_executor_State = payload['DAG_executor_State']
    logger.debug("DAG_executor_task: call DAG_excutor, state is " + str(DAG_executor_State.state))
    DAG_executor(payload)
    
	
def main():
	# generate DAG_map using DFS_visit
    n1 = Node(None,None,"inc0",inc0)
    n3 = Node(None,None,"triple",triple)
    n4 = Node(None,None,"inc1",inc1)
    n5 = Node(Node,Node,"square",square)
    n2 = Node(Node,Node,"add",add) 
    n6 = Node(Node,Node,"multiply",multiply) 
    n7 = Node(Node,Node,"divide",divide)
	
    n1.set_succ([n2])
    n1.set_pred([])
    n2.set_succ([n6])	
    n2.set_pred([n1,n4])
    n3.set_succ([n6])
    n3.set_pred([n4])
    n4.set_succ([n2,n5,n3])
    n4.set_pred([])
    n5.set_succ([n6])
    n5.set_pred([n4])
    n6.set_succ([n7])
    n6.set_pred([n2,n3,n5])
    n7.set_succ([])
    n7.set_pred([n6])
	
    n1.generate_ops()
    n4.generate_ops()
    n2.generate_ops()
    n3.generate_ops()
    n5.generate_ops()
    n6.generate_ops()
    n7.generate_ops()
	
    logger.debug("DAG_map:")
    for key, value in Node.DAG_map.items():
        logger.debug(key)
        logger.debug(value)
    logger.debug("  ")
	
    logger.debug("states:")         
    for key, value in Node.DAG_states.items():
        logger.debug(key)
        logger.debug(value)
    logger.debug("   ")
    
    logger.debug("DAG_tasks:")         
    for key, value in Node.DAG_tasks.items():
        logger.debug(key)
        logger.debug(value)
    logger.debug("   ")
	
    logger.debug("num_fanins:" + str(Node.num_fanins) + " num_fanouts:" + str(Node.num_fanouts) + " num_faninNBs:" 
        + " num_collapse:" + str(Node.num_collapse))
    logger.debug("   ")
	
    logger.debug("all_fanout_task_names")
    for name in Node.all_fanout_task_names:
        logger.debug(name)
        logger.debug("   ")
    logger.debug("   ")
	
    logger.debug("all_fanin_task_names")
    for name in Node.all_fanin_task_names :
        logger.debug(name)
        logger.debug("   ")
    logger.debug("   ")
		  
    logger.debug("all_faninNB_task_names")
    for name in Node.all_faninNB_task_names:
        logger.debug(name)
        logger.debug("   ")
    logger.debug("   ")
		  
    logger.debug("all_collapse_task_names")
    for name in Node.all_collapse_task_names:
        logger.debug(name)
        logger.debug("   ")
    logger.debug("   ")
	
    DAG_map = Node.DAG_map
    task_name_to_function_map =  Node.DAG_tasks
    
    logger.debug("DAG_map after assignment:")
    for key, value in DAG_map.items():
        logger.debug(key)
        logger.debug(value)
    logger.debug("   ")
    logger.debug("task_name_to_function_map after assignment:")
    for key, value in task_name_to_function_map.items():
        logger.debug(key)
        logger.debug(value)
    logger.debug("   ")
    
    #states = Node.DAG_states
    #all_fanout_task_names = Node.all_fanout_task_names
    #all_fanin_task_names = Node.all_fanin_task_names
    #all_faninNB_task_names = Node.all_faninNB_task_names
    #all_collapse_task_names = Node.all_collapse_task_names

	
	# ToDo: logger.debug Node.DAG_map
	#		logger.debug Node.states (after making states global)
	# 		logger.debug Node.all_fanin_task_names (for synch objects)
	#		logger.debug Node.all_faninNB_task_names (for synch objects)

	# ToDo: get this from DAG map:
	# start states are x and y, where:
	# state 1: input 0, ("inc0": add 1), output 1 to faninNB "add",
	# state 2: input 1, ("inc1": add 1), output 2 to faninNB "add", output 2 to fanout "square"
	# state 3: input 2, ("square": square 2), output 4 to fanin "mult"
	# state 4: input 1 from "inc0" and 2 from "inc1", ("add": 1+2), output 3 to fanin "mult"
	# state 5: input 4 from "square" and 3 from "add", ("mult": 4*3), output 12 to ?
	
    server = DAG_executor_Synchronizer()

    #ToD0: loop thru DAG_info.DAG_start_states - but need their inputs to do that
    try:
        DAG_executor_State1 = DAG_executor_State(state = int(1))
        logger.debug("Starting DAG_executor thread for state 1")
        payload = {
##rhc
            #"state": int(1),
            "input": {'input': int(0)},
			"DAG_executor_State": DAG_executor_State1,
            "server": server
        }
        _thread.start_new_thread(DAG_executor_task, (payload,))
    except Exception as ex:
        logger.error("[ERROR] Failed to start DAG_executor thread for state 1")
        logger.debug(ex)
        
    try:
        DAG_executor_State3 = DAG_executor_State(state = int(3))
        logger.debug("Starting DAG_executor thread for state 3")
        payload = {
##rhc
            #"state": int(3),
            "input": {'input': int(1)},
			"DAG_executor_State": DAG_executor_State3,
            "server": server
        }
        _thread.start_new_thread(DAG_executor_task, (payload,))
    except Exception as ex:
        logger.error("[ERROR] Failed to start DAG_executor thread for state 3")
        logger.debug(ex)
        
    logger.debug("Sleeping")
    time.sleep(5)
	
    
if __name__=="__main__":
    main()
