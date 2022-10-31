from threading import RLock
from .DAG_executor_constants import store_fanins_faninNBs_locally, FanIn_Type, FanInNB_Type
from ..server import DAG_executor_FanInNB
from ..server import DAG_executor_FanIn
from .DAG_executor_State import DAG_executor_State
import uuid

import logging 
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(asctime)s] [%(threadName)s] %(levelname)s: %(message)s')

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)

logger.addHandler(ch)

# This is taking the role of the tcp_server when synch objects are stored locally. 
# It is a global singleton.
class DAG_executor_Synchronizer(object):
    def __init__(self):
        self.synchronizers =  {}
        self.mutex =  RLock() 
    
    # This is for fanin, which can be a try_fanin.
    # faninNB is asynch and w/always terminate
    # ToDo: If we create all fanins/faninNBs at beginning then we can just call the usual fan_in method 
    #       and don't need a lock.
    def create_and_fanin_locally(self,DAG_exec_state,keyword_arguments):
        # create new fanin with specified name if it hasn't been created 

        # create new faninNB with specified name if it hasn't been created 
		# Here are the keyword arguments:
        fanin_task_name = keyword_arguments['fanin_task_name']
        #n = keyword_arguments['n']	# size
		# used by FanInNB:
        #start_state_fanin_task = keyword_arguments['start_state_fanin_task']
        # where: keyword_arguments['start_state_fanin_task'] = DAG_states[name]
        output = keyword_arguments['result']
        calling_task_name = keyword_arguments['calling_task_name']
        #DAG_executor_state = keyword_arguments['DAG_executor_State']
        #server = keyword_arguments['server']
		# used by FanInNB:
        # run_faninNB_task_on_server = keyword_arguments['run_faninNB_task_on_server']  # option set in DAG_executor
  
        # check_for_object_and_create_i_not_there must be atomic (as will be on server, with many client callers)
        self.mutex.acquire()

        inmap = fanin_task_name in self.synchronizers
        logger.debug ("calling_task_name: " + calling_task_name + " fanin_task_name: " + fanin_task_name
			+ " inmap: " + str(inmap))
        #if not fanin_task_name in DAG_executor_Synchronizer.synchronizers:
        if not inmap: 	# fanin_task_name in self.synchronizers:
            # Note: When we create FanIn objects locally, we are always using DAG_executor_FanIn.DAG_executor_FanIn.
            # We never use the "select" version.
            FanIn = DAG_executor_FanIn.DAG_executor_FanIn(fanin_task_name) # initial_n = 0, monitor_name = None
            FanIn.init(**keyword_arguments)
            logger.debug("calling_task_name: " + calling_task_name + " fanin_task_name: " + fanin_task_name
				+ " DAG_executor_Synchronizer: create_and_fanin: create caching new fanin with name '%s'" % (fanin_task_name))
            logger.debug(" DAG_executor_Synchronize: create_and_fanin: create caching new fanin with name '%s'" % (fanin_task_name))
            self.synchronizers[fanin_task_name] = FanIn # Store Synchronizer object.

        # check and possibly create the object is atomic; the call to fan_in the follows does not need
        # to be atomic with the create. If two callers attempt the first create for FanIn F, one caller
        # may create F and the other caller may execute its fan_in operation first.
        self.mutex.release()

        inmap = fanin_task_name in self.synchronizers  
        logger.debug ("calling_task_name: " + calling_task_name + " fanin_task_name: " + fanin_task_name + " inmap: " + str(inmap))
        FanIn = self.synchronizers[fanin_task_name]  
        
        # call fanin
        try_return_value = FanIn.try_fan_in(**keyword_arguments)
        #logger.debug("calling_task_name: " + calling_task_name + " FanIn: try_return_value: " + str(try_return_value))
        logger.debug("fanin_task_name: " + fanin_task_name + " try_return_value: " + str(try_return_value))
        if try_return_value:   # synchronize op will execute wait so tell client to terminate
            DAG_exec_state.blocking = True 
            DAG_exec_state.return_value = None 
			
			#FanInNB gets DAG_executor_State in kwargs when it starts a new executor to execute the fanin task.
			#FanIn does not access the DAG_executor_State in kwargs
			
			# return is: self._results, restart, where restart is always 0 and results is 0 or a map
            return_value, restart_value_ignored = FanIn.fan_in(**keyword_arguments)
        else:
            return_value, restart_value_ignored = FanIn.fan_in(**keyword_arguments)

            logger.debug("fanin become task output:" + str(output))
            logger.debug("calling_task_name:" + calling_task_name)
            # Add our result to the results (instead of sending it to the fanin on the server and server sending it back
            return_value[calling_task_name] = output
            DAG_exec_state.return_value = return_value
            DAG_exec_state.blocking = False 
			
		# This is returned to process_fanin which returns it to DAG_executor; DAG_executtor will look at blocking
		# and if not blocking the return_value to get the input as if not blocking then becomes fanin task.
        return DAG_exec_state

    # This is for fanin, which can be a try_fanin. Note, however, that even though try_fan_in will
    # return True when the caller is not the last, i.e., the become task, the call to fan_in will
    # not block, fan_n will return 0 to the caller. For DAG_executor, thus, there is no need to
    # call try_fan_in, we can simply call fan_in and check the return_value. If we are not using a 
    # thread/process pool the DAG_executor thread/process can terminate. If a pool is being used, the
    # thread/process can get more work from the work_queue.

    # faninNB is asynch and w/always terminate
    # ToDo: If we create all fanins/faninNBs at beginning then we can just call the usual fan_in method 
    #       and don't need a lock.
    def fanin_locally(self,DAG_exec_state,keyword_arguments):
        # create new fanin with specified name if it hasn't been created 

        # create new faninNB with specified name if it hasn't been created 
		# Here are the keyword arguments:
        # fanin_task_name = keyword_arguments['fanin_task_name']
        #n = keyword_arguments['n']	# size
		# used by FanInNB:
        #start_state_fanin_task = keyword_arguments['start_state_fanin_task']
        # where: keyword_arguments['start_state_fanin_task'] = DAG_states[name]
        #output = keyword_arguments['result']
        #calling_task_name = keyword_arguments['calling_task_name']
        #DAG_executor_state = keyword_arguments['DAG_executor_State']
        #server = keyword_arguments['server']
		# used by FanInNB:
        # store_fanins_faninNBs_locally = keyword_arguments['store_fanins_faninNBs_locally']  # option set in DAG_executor

        logger.debug ("calling_task_name: " + keyword_arguments['calling_task_name'] + " calling fanin for " + " fanin_task_name: " + keyword_arguments['fanin_task_name'])
        FanIn = self.synchronizers[keyword_arguments['fanin_task_name']]  

        # Note: fan_in does not block - if the caller is not the last to fan_in, it does not block, 0 is
        # returned to the caller who can terminate or, f a thread/process pool is being used, withdraw
        # more work from the work_queue. (Of course, try_fan_in does not block either.)

        # call try_fan_in and fan_in
        # Note: try_fan_in and fan_in are coordnated in monitorSU so that they are atomic.
        # If we use the "select" version of FanInNB, we need to lock the FanINNB. See that code.
        try_return_value = FanIn.try_fan_in(**keyword_arguments)
        #logger.debug("calling_task_name: " + calling_task_name + " FanIn: try_return_value: " + str(try_return_value))
        logger.debug("fanin_task_name: " + keyword_arguments['fanin_task_name'] + " try_return_value: " + str(try_return_value))
        if try_return_value:   # synchronize op will execute wait so tell client to terminate
            DAG_exec_state.blocking = True 
            DAG_exec_state.return_value = 0 

			#FanInNB gets DAG_executor_State in kwargs when it starts a new executor to execute the fanin task.
			#FanIn does not access the DAG_executor_State in kwargs

			# return is: self._results, restart, where restart is always 0 and results is 0 or a map
            return_value, restart_value_ignored = FanIn.fan_in(**keyword_arguments)
        else:
            return_value, restart_value_ignored = FanIn.fan_in(**keyword_arguments)

            logger.debug("fanin become task output:" + str(keyword_arguments['result']))
            logger.debug("calling_task_name:" + keyword_arguments['calling_task_name'])
            # Add our result to the results (instead of sending it to the fanin on the server and server sending it back
            return_value[keyword_arguments['calling_task_name']] = keyword_arguments['result']
            DAG_exec_state.return_value = return_value
            DAG_exec_state.blocking = False 

		# This is returned to process_fanin which returns it to DAG_executor; DAG_executtor will look at blocking
		# and if not blocking the return_value to get the input as if not blocking then becomes fanin task.
        return DAG_exec_state
        
    # faninNB is asynch w/client always terminate
    def create_and_faninNB_locally(self,DAG_exec_state,**keyword_arguments): 
        # keyword_arguments['fanin_task_name'] = name
        # keyword_arguments['n'] = n
        # keyword_arguments['start_state_fanin_task'] = DAG_states[name]

 #ToDo: Don't need the results for local faninNB when running locally since we get them from 
 # the data_dict, so in these cases we don;t need to send the output => can set output to None.       
        # create new faninNB with specified name if it hasn't been created 
        fanin_task_name = keyword_arguments['fanin_task_name']
        #n = keyword_arguments['n']
        #start_state_fanin_task = keyword_arguments['start_state_fanin_task']
        #output = keyword_arguments['result']
        calling_task_name = keyword_arguments['calling_task_name']
        #DAG_executor_State = keyword_arguments['DAG_executor_State']
        #server = keyword_arguments['server']
        #run_faninNB_task_on_server = keyword_arguments['run_faninNB_task_on_server']  # option set in DAG_executor
        #DAG_info = keyword_arguments['DAG_info']
        
        # create and fan_in must be atomic
        self.mutex.acquire()
        inmap = fanin_task_name in self.synchronizers
        logger.debug ("calling_task_name: " + calling_task_name + " fanin_task_name: " + fanin_task_name + " inmap: " + str(inmap))
        if not inmap: 	# fanin_task_name in self.synchronizers:
            # Note: When we create FanIn objects locally, we are always using DAG_executor_FanInNB.DAG_executor_FanInNB.
            # We never use the "select" version.
            FanInNB = DAG_executor_FanInNB.DAG_executor_FanInNB(fanin_task_name) # initial_n = 0, monitor_name = None
            FanInNB.init(**keyword_arguments)
            logger.debug("calling_task_name: " + calling_task_name + " fanin_task_name: " + fanin_task_name
                + " DAG_executor_Synchronizer: create_and_faninNB: create caching new fanin with name '%s'" % (fanin_task_name))
            logger.debug(" DAG_executor_Synchronizer: create_and_faninNB: create caching new fanin with name '%s'" % (fanin_task_name))

            self.synchronizers[fanin_task_name] = FanInNB # Store Synchronizer object.
 
        inmap = fanin_task_name in self.synchronizers  
        logger.debug ("calling_task_name: " + calling_task_name + " fanin_task_name: " + fanin_task_name + " inmap: " + str(inmap))
        FanInNB = self.synchronizers[fanin_task_name]
        
		# Note: in real code, we would return here so caller can quit, letting server do the op.
		# Here, we can just wait for op to finish, then return. Caller has nothing to do but 
		# quit since nothing to do after a fanin.

        # return is: None, restart, where restart is always 0 and return_value is None; and makes no change to DAG_executor_State	
        return_value_ignored, restart_value_ignored = FanInNB.fan_in(**keyword_arguments)

#ToDo: if we always return a state:
        DAG_exec_state = keyword_arguments['DAG_executor_State']
        DAG_exec_state.blocking = True 
		# for faninNB there is never a result, even for last caller since No Becomes (NB)
		# Note: We could have fan_in for FanInNB return the results for debugging
        DAG_exec_state.return_value = None 

        self.mutex.release()
        
# ToDo: may return DAG_executor_State to be consistent - it can be ignored.
# No: this is not being returned to user, this goes to DAG_executor, which will just process fanins next.
        return 0

    # faninNB is asynch w/client always terminate
    def faninNB_locally(self,**keyword_arguments): 
        # keyword_arguments['fanin_task_name'] = name
        # keyword_arguments['n'] = n
        # keyword_arguments['start_state_fanin_task'] = DAG_states[name]
        
        # create new faninNB with specified name if it hasn't been created 
        #fanin_task_name = keyword_arguments['fanin_task_name']
        #n = keyword_arguments['n']
        #start_state_fanin_task = keyword_arguments['start_state_fanin_task']
        #output = keyword_arguments['result']
        #calling_task_name = keyword_arguments['calling_task_name']
        #DAG_executor_state = keyword_arguments['DAG_executor_State']
        #server = keyword_arguments['server']
        #store_fanins_faninNBs_locally = keyword_arguments['store_fanins_faninNBs_locally']  # option set in DAG_executor
        #DAG_info = keyword_arguments['DAG_info']
        
        logger.debug ("calling_task_name: " + keyword_arguments['calling_task_name'] + "calling faninNB with fanin_task_name: " + keyword_arguments['fanin_task_name'])

        FanInNB = self.synchronizers[keyword_arguments['fanin_task_name']]
        
		# Note: in real code, we would return here so caller can quit, letting server do the op.
		# Here, we can just wait for op to finish, then return. Caller has nothing to do but 
		# quit since nothing to do after a fanin.

#ToDo: Like Fanin: can call try_fan_in, dont need to since fan_in does not realy block, i.e., it 
#      returns instead of blocking. More consistent to call try_fan_in?

        # return is: None, restart, where restart is always 0 and return_value is None; and makes no change to DAG_executor_State	
        # Not using "asynch" here as no way to implement "asynch" locally.
        return_value_ignored, restart_value_ignored = FanInNB.fan_in(**keyword_arguments)

#ToDo: if we always return a state:
        # rhc: DES
        DAG_exec_state = DAG_executor_State(function_name = "DAG_executor", function_instance_ID = str(uuid.uuid4()))
        #DAG_exec_state = keyword_arguments['DAG_executor_State']
        DAG_exec_state.blocking = True 
		# for faninNB there is never a result, even for last caller since No Becomes (NB)
		# Note: We could have fan_in for FanInNB return the results for debugging
        DAG_exec_state.return_value = None 
        
# ToDo: may return DAG_executor_State to be consistent - it can be ignored.
# No: this is not being returned to user, this goes to DAG_executor, which will just process fanins next.
        return 0
        
    def create_all_fanins_and_faninNBs_locally(self,DAG_map,DAG_states,DAG_info,all_fanin_task_names, all_fanin_sizes, all_faninNB_task_names, all_faninNB_sizes):
                                                            
        fanin_messages = []
        for fanin_name, size in zip(all_fanin_task_names,all_fanin_sizes):
            # rhc: DES
            dummy_state = DAG_executor_State(function_name = "DAG_executor", function_instance_ID = str(uuid.uuid4()))
			# keywword_argumments used in init()
            dummy_state.keyword_arguments['n'] = size
            #dummy_state.keyword_arguments['fanin_task_name']  = fanin_name
            msg_id = str(uuid.uuid4())	# for debugging
            message = {
                "op": "create",
                "type": FanIn_Type,
                "name": fanin_name,
                "state": dummy_state,	
                "id": msg_id
            }
            fanin_messages.append(message)

        faninNB_messages = []
        for fanin_nameNB, size in zip(all_faninNB_task_names,all_faninNB_sizes):
            # rhc: DES
            dummy_state = DAG_executor_State(function_name = "DAG_executor", function_instance_ID = str(uuid.uuid4()))
			# keywword_argumments used in init()
            dummy_state.keyword_arguments['n'] = size
            dummy_state.keyword_arguments['start_state_fanin_task'] = DAG_states[fanin_nameNB]
            dummy_state.keyword_arguments['store_fanins_faninNBs_locally'] = True
            dummy_state.keyword_arguments['DAG_info'] = DAG_info
            msg_id = str(uuid.uuid4())
            message = {
                "op": "create",
                "type": FanInNB_Type,
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
        #self.mutex.acquire()

        print(str(fanin_messages))
        print(str(faninNB_messages))
        for msg in fanin_messages:
            #logger.debug("calling_task_name: " + calling_task_name + " fanin_task_name: " + fanin_task_name)
            fanin_task_name = msg["name"]
            #DAG_exec_state = msg['state']
            #n = DAG_exec_state.keyword_arguments['n']
            inmap = fanin_task_name in self.synchronizers
            logger.debug (" inmap before: " + str(inmap))
            #if not fanin_task_name in DAG_executor_Synchronizer.synchronizers:
            if not inmap: 	# fanin_task_name in self.synchronizers:
                FanIn = DAG_executor_FanIn.DAG_executor_FanIn(fanin_task_name) # initial_n = 0, monitor_name = None
                FanIn.init(**(msg['state'].keyword_arguments))
                logger.debug(" create_and_fanin: create caching new fanin with name '%s'" % (fanin_task_name))
                self.synchronizers[fanin_task_name] = FanIn # Store Synchronizer object.
            inmap = fanin_task_name in self.synchronizers  
            logger.debug (" inmap after: " + str(inmap))
            FanIn = self.synchronizers[fanin_task_name] 
            
        for msg in faninNB_messages:
            faninNB_task_name = msg["name"]
            #DAG_exec_state = msg['state']
            #n = DAG_exec_state.keyword_arguments['n']
            #start_state_fanin_task = DAG_exec_state.keyword_arguments['start_state_fanin_task']
            inmap = faninNB_task_name in self.synchronizers
            logger.debug (" inmap before: " + str(inmap))
            #if not fanin_task_name in DAG_executor_Synchronizer.synchronizers:
            if not inmap: 	# faninNB_task_name in self.synchronizers:
                FanInNB = DAG_executor_FanInNB.DAG_executor_FanInNB(faninNB_task_name) # initial_n = 0, monitor_name = None
                FanInNB.init(**(msg['state'].keyword_arguments))
                logger.debug(" create_and_fanin: create caching new fanin with name '%s'" % (faninNB_task_name))
                self.synchronizers[faninNB_task_name] = FanInNB # Store Synchronizer object.
            inmap = faninNB_task_name in self.synchronizers  
            logger.debug (" inmap after: " + str(inmap))
            FanInNB = self.synchronizers[faninNB_task_name]  

		# ToDo: may return DAG_executor_State to be consistent - it can be ignored.
        return 0

server = None
if store_fanins_faninNBs_locally:
    server = DAG_executor_Synchronizer()