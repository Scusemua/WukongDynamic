import logging 
import base64
import re 
import socket
import time 
import redis 
import uuid

import cloudpickle
from wukongdnc.wukong.invoker import invoke_lambda
from wukongdnc.server.state import State 
from wukongdnc.server.api import synchronize_sync, synchronize_sync
from wukongdnc.constants import REDIS_IP_PRIVATE, TCP_SERVER_IP

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s')

SLEEP_INTERVAL = 0.120

if logger.handlers:
    for handler in logger.handlers:
        handler.setFormatter(formatter)

root = logging.getLogger()
if root.handlers:
    for handler in root.handlers:
        handler.setFormatter(formatter)

class FuncA(object):
    def __init__(self, state = None):
        self.state = state
        self.state.ID = "FuncA"
    
    def execute(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as websocket:
            logger.debug("Connecting to TCP Server at %s." % str(TCP_SERVER_IP))
            websocket.connect(TCP_SERVER_IP)
            logger.debug("Successfully connected to TCP Server at %s. Calling executor.create() now...")
            while True:
                if self.state.pc == 0:
                    self.state.pc = 1 # if restart PC will be 1
                    # this is essentially: value = result.withdraw()
                    self.state = synchronize_sync(websocket, "synchronize_sync", "result", "try_withdraw", self.state)
                    if self.state.blocking:
                        self.state.blocking = False
                        return
                    # else:
                    #     # self.state.blocking is False
                    #     # withdrawn value returned in self.state.return_value
                    #     self.state.pc = 1 # transition to state PC=1

                elif self.state.pc == 1:
                    value = self.state.return_value
                    self.state.return_value = None
                    logger.debug("FuncA (pc=1) value pre-increment: " + str(value))
                    value += 1
                    time.sleep(SLEEP_INTERVAL) # Sleep for a bit.
                    logger.debug("FuncA (pc=1) value post-increment: " + str(value))
                    if self.state.keyword_arguments is None:
                        self.state.keyword_arguments = {}
                    self.state.function_name = "ComposerServerlessSync"
                    self.state.keyword_arguments["value"] = value
                    synchronize_sync(websocket, "synchronize_sync", "result", "try_deposit", self.state)  
                    synchronize_sync(websocket, "synchronize_sync", "finish", "try_V", self.state)
                    break
                else: 
                    logger.error("Invalid PC value: " + str(self.state.pc))

            # TODO: Deposit answer in another bounded buffer.
            logger.debug(str(self.state.ID) + " is done. Result: " + str(self.state.return_value))

class FuncB(object): # same as FuncA with different ID
    def __init__(self, state = None):
        self.state = state
        self.state.ID = "FuncB"
    
    def execute(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as websocket:
            logger.debug("Connecting to TCP Server at %s." % str(TCP_SERVER_IP))
            websocket.connect(TCP_SERVER_IP)
            logger.debug("Successfully connected to TCP Server at %s. Calling executor.create() now...")
            while True:
                if self.state.pc == 0:
                    self.state.pc = 1 # if restart PC will be 1
                    # this is essentially: value = result.withdraw()
                    self.state = synchronize_sync(websocket, "synchronize_sync", "result", "try_withdraw", self.state)
                    if self.state.blocking:
                        self.state.blocking = False
                        return
                    # else:
                    #     # self.state.blocking is False
                    #     # withdrawn value returned in self.state.return_value
                    #     self.state.pc = 1 # transition to state PC=1

                elif self.state.pc == 1:
                    print("FuncB (START of pc=1) self.state.blocking = " + str(self.state.blocking))
                    value = self.state.return_value
                    self.state.return_value = None
                    logger.debug("FuncB (pc=1) value pre-increment: " + str(value))
                    value += 1
                    time.sleep(SLEEP_INTERVAL) # Sleep for a bit. 
                    logger.debug("FuncB (pc=1) value post-increment: " + str(value))
                    if self.state.keyword_arguments is None:
                        self.state.keyword_arguments = {}
                    self.state.function_name = "ComposerServerlessSync"
                    self.state.keyword_arguments["value"] = value
                    logger.debug("FuncB (pc=1) calling result.deposit() now.")
                    self.state.blocking = False
                    synchronize_sync(websocket, "synchronize_sync", "result", "try_deposit", self.state)  
                    logger.debug("FuncB (pc=1) calling finish.V() now.")
                    self.state.blocking = False
                    synchronize_sync(websocket, "synchronize_sync", "finish", "try_V", self.state)
                    break
                else: 
                    logger.error("Invalid PC value: " + str(self.state.pc))

            # TODO: Deposit answer in another bounded buffer.
            logger.debug(str(self.state.ID) + " is done. Result: " + str(self.state.return_value))

class Composer(object):
    def __init__(self, state = None):
        self.state = state
        self.state.ID = "ComposerServerlessSync"
        # self.List_of_Lambdas = ["FuncA", "FuncB"]

    def execute(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as websocket:
            logger.debug("Connecting to TCP Server at %s." % str(TCP_SERVER_IP))
            websocket.connect(TCP_SERVER_IP)
            logger.debug("Successfully connected to TCP Server") 
            
            if not self.state.initialized:
                self.state.initialized = True
                logger.debug("Calling executor.create() now...")
                if self.state.keyword_arguments is None:
                    self.state.keyword_arguments = {}
                self.state.keyword_arguments["value"] = self.state.starting_input
                self.state.return_value = None
                synchronize_sync(websocket, "synchronize_sync", "result", "try_deposit", self.state)
            
            print("Composer -- self.state.list_of_functions = " + str(self.state.list_of_functions))
            print("Composer -- INITIALLY self.state.i = " + str(self.state.i))

            # while is same indentation level as the above if                 
            while self.state.i < len(self.state.list_of_functions):
                print("Composer -- self.state.i = " + str(self.state.i))
                Func = self.state.list_of_functions[self.state.i]  # invoke function Func
                print("Composer -- Func = " + str(Func))
                payload = {
                    "state": State(function_name = Func, restart = False, function_instance_ID = str(uuid.uuid4()))
                }
                invoke_lambda(payload = payload)
  
                self.state.i += 1
                print("Composer -- POST-INCREMENT -- self.state.i = " + str(self.state.i))
                self.state = synchronize_sync(websocket, "synchronize_sync", "finish", "try_P", self.state)
                if self.state.blocking:
                    # all of the if self.state.blocking have two statements: set blocking to False and return
                    self.state.blocking = False
                    return           
                    # restart when finish.P completes or non-blocking finish.P;
                    # either way we have finished P (and P doesn't return anything)
                    # If blocking was false, the synchronous_synch returned a state with the return_value. If blocking
                    # was true, then the Lambda was restarted with a state that contained the return_value. So 
                    # self.state.return_value is the return value of the synchronous_synch.                                  

            # if is same indentation level as above while and the if before it
            if self.state.pc == 0:
                # restart when finish.P completes or non-blocking finish.P;
                # either way we have finished P (and P doesn't return anything)
                # If blocking was false, the synchronous_synch returned a state with the return_value. If blocking
                # was true, then the Lambda was restarted with a state that contained the return_value. So 
                # self.state.return_value is the return value of the synchronous_synch.
                self.state.return_value = None
                self.state.pc = 1
                self.state = synchronize_sync(websocket,"synchronize_sync", "result", "try_withdraw", self.state)
                print("Composer -- withdrawn state: " + str(self.state))                
                if self.state.blocking:
                    self.state.blocking = False
                    return #transition to state 1 on restart
                    
            #same indentation level as if pc == 0    
            # restart when withdraw completes or non-blocking withdraw;
            # either way we have return_value of withdraw
            value = self.state.return_value
            logger.debug("Composer -- state.return_value at end = " + str(value))
            self.state.return_value = None
            self.state.keyword_arguments["value"] = value
            self.state.return_value = None
            synchronize_sync(websocket, "synchronize_sync", "final_result", "try_deposit", self.state)

def lambda_handler(event, context):
    start_time = time.time()
    rc = redis.Redis(host = REDIS_IP_PRIVATE, port = 6379)
    logger.debug("Invocation received. event: " + str(event))

    # Extract all of the data from the payload.
    # first_executor = event["first_executor"]
    state = cloudpickle.loads(base64.b64decode(event["state"]))

    if "list_of_functions" in event:
        state.list_of_functions = cloudpickle.loads(base64.b64decode(event["list_of_functions"])) # event["list_of_functions"]
    else:
        logger.debug("No entry for 'list_of_functions' in invocation payload. Payload: " + str(event) + ", state: " + str(state))
    
    if "starting_input" in event:
        state.starting_input = cloudpickle.loads(base64.b64decode(event["starting_input"])) # event["starting_input"]

    target = state.function_name 
    logger.debug("Starting *****%s*****." % target)
    # target = event['target']
    if target == "ComposerServerlessSync":
        composer = Composer(state = state)
        composer.execute()
    elif target == "FuncA":
        A = FuncA(state = state)
        A.execute()
    elif target == "FuncB":
        B = FuncB(state = state)
        B.execute()
    else:
        raise ValueError("Invalid target specified: " + str(target))
    end_time = time.time()
    duration = end_time - start_time
    logger.debug("Executor finished. Time elapsed: %f seconds." % duration)
    rc.lpush("durations", duration)    