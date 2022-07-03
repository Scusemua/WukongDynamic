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
from wukongdnc.server.api import synchronize_sync, synchronize_sync, synchronize_async_terminate
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

###### These Lambdda functions are for synchronize_async_terminate - lambda terminates after every synchronize async
###### call and then restarts. So we use 2*10 Lambdas (Lambdas are started annd make 10 calls that are restarted.

###### Composer invokes Lambda "ComposerServerlessSync" with state.function_name = "FuncA" - don't change it.
# ###### Server will also restart "ComposerServerlessSync" and will not change state.function_name. 
class FuncA(object):
    def __init__(self, state = None):
        self.state = state
        self.state.ID = "FuncA"
    
    def execute(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as websocket:
            logger.debug("Connecting to TCP Server at %s." % str(TCP_SERVER_IP))
            websocket.connect(TCP_SERVER_IP)
            logger.debug("Successfully connected to TCP Server.")
            while True:
                if self.state.pc == 0:
                    print("FuncA (START of pc=0)")
                    self.state.pc = 1 # if restart PC will be 1; otherwise will iterate again in state.pc == 1
                    # this is essentially: value = result.withdraw()
                    # Since we have one function that internally chooses to execute FuncA, FuncB, or Composer.
                    #self.state.function_name = "ComposerServerlessSync"
                    self.state.keyword_arguments = {}
                    self.state.return_value = None
                    #self.state = synchronize_sync(websocket, "synchronize_sync", "result", "try_withdraw", self.state)
                    self.state = synchronize_async_terminate(websocket, "synchronize_async", "result", "withdraw", self.state)
                    if self.state.blocking:
                        self.state.blocking = False
                        return # if we return, we will restart, enter the while-True loop and transition to state PC == 1
                    # Note: if we reach here (no return), we will iterate the while-True loop and transition to state pc == 1
                elif self.state.pc == 1:
                    print("FuncA (START of pc=1)")    
                    value = self.state.return_value    # get self.state.return_value whether we restarted or not
                    self.state.return_value = None
                    logger.debug("FuncA (pc=1) value pre-increment: " + str(value))
                    value += 1
                    time.sleep(SLEEP_INTERVAL) # Sleep for a bit.
                    logger.debug("FuncA (pc=1) value post-increment: " + str(value))
                    self.state.pc = 2 # if we get restarted pc will be 2
                    #self.state.function_name = "ComposerServerlessSync"
                    self.state.keyword_arguments = {}
                    self.state.return_value = None
                    self.state.keyword_arguments["value"] = value
                    # this is essentially: result.deposit(value)
                    #self.state = synchronize_sync(websocket, "synchronize_sync", "result", "try_deposit", self.state)
                    self.state = synchronize_async_terminate(websocket, "synchronize_async", "result", "deposit", self.state)
                    self.state.keyword_arguments = {}
                    if self.state.blocking:
                        self.state.blocking = False
                        return
                elif self.state.pc == 2:
                    print("FuncA (START of pc=2)")
                    self.state.pc = 3 # if we get restarted pc will be 3
                   # self.state.function_name = "ComposerServerlessSync"
                    self.state.keyword_arguments = {}
                    self.state.return_value = None
                    # this is essentially: finish.V()
                    #self.state =  synchronize_sync(websocket, "synchronize_sync", "finish", "try_V", self.state)
                    self.state =  synchronize_async_terminate(websocket, "synchronize_async", "finish", "V", self.state)
                    if self.state.blocking:
                        self.state.blocking = False
                        return
                elif self.state.pc == 3:
                    print("FuncA (START of pc=3)")
                    print("FuncA (Stopping)")
                    break
                else: 
                    logger.error("Invalid PC value: " + str(self.state.pc))

            logger.debug(str(self.state.ID) + " completing. Result was: " + str(value))

###### Composer invokes Lambda "ComposerServerlessSync" with state.function_name = "FuncA" - don't change it. 
###### Server will also restart "ComposerServerlessSync" and will not change state.function_name.
class FuncB(object):
    def __init__(self, state = None):
        self.state = state
        self.state.ID = "FuncB"
    
    def execute(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as websocket:
            logger.debug("Connecting to TCP Server at %s." % str(TCP_SERVER_IP))
            websocket.connect(TCP_SERVER_IP)
            logger.debug("Successfully connected to TCP Server.")
            while True:
                if self.state.pc == 0:
                    print("FuncB (START of pc=0)")
                    self.state.pc = 1 # if get restarted pc will be 1
                    # this is essentially: value = result.withdraw()
                    #self.state.function_name = "ComposerServerlessSync"
                    self.state.keyword_arguments = {}
                    self.state.return_value = None
                    #self.state = synchronize_sync(websocket, "synchronize_sync", "result", "try_withdraw", self.state)
                    self.state = synchronize_async_terminate(websocket, "synchronize_async", "result", "withdraw", self.state)
                    if self.state.blocking:
                        self.state.blocking = False
                        return # will restart in (saved) state pc == 1
                    # Note: if we reach here (no return), we will iterate the while-True loop and transition to next state pc == 1
                elif self.state.pc == 1:
                    print("FuncB (START of pc=1)")            
                    value = self.state.return_value
                    self.state.return_value = None
                    logger.debug("FuncB (pc=1) value pre-increment: " + str(value))
                    value += 1
                    time.sleep(SLEEP_INTERVAL) # Sleep for a bit.
                    logger.debug("FuncB (pc=1) value post-increment: " + str(value))
                    self.state.pc = 2 # if get restarted PC will be 2
                    #self.state.function_name = "ComposerServerlessSync"
                    self.state.keyword_arguments = {}
                    self.state.return_value = None
                    self.state.keyword_arguments["value"] = value
                    # this is essentially: result.deposit(value)
                    #self.state = synchronize_sync(websocket, "synchronize_sync", "result", "try_deposit", self.state)
                    self.state = synchronize_async_terminate(websocket, "synchronize_async", "result", "deposit", self.state)
                    self.state.keyword_arguments = {}
                    if self.state.blocking:
                        self.state.blocking = False
                        return #will restart in (saved) state pc == 2
                elif self.state.pc == 2:
                    print("FuncB (START of pc=2)")
                    self.state.pc = 3 # if restart PC will be 3
                    #self.state.function_name = "ComposerServerlessSync"
                    self.state.keyword_arguments = {}
                    self.state.return_value = None
                    # this is essentially: finish.V()
                    #self.state = synchronize_sync(websocket, "synchronize_sync", "finish", "try_V", self.state)
                    self.state = synchronize_async_terminate(websocket, "synchronize_async", "finish", "V", self.state)
                    if self.state.blocking:
                        self.state.blocking = False
                        return #will restart in (saved) state pc == 3
                elif self.state.pc == 3:
                    print("FuncB (START of pc=3)")
                    print("FuncB (Stopping")
                    break
                else: 
                    logger.error("Invalid PC value: " + str(self.state.pc))

            logger.debug(str(self.state.ID) + " completing. Result was: " + str(value))


class Composer(object):
    def __init__(self, state = None):
        self.state = state
        self.state.ID = "Composer"

    def execute(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as websocket:
            logger.debug("Connecting to TCP Server at %s." % str(TCP_SERVER_IP))
            websocket.connect(TCP_SERVER_IP)
            logger.debug("Successfully connected to TCP Server") 
            while True:
                if self.state.pc == 0:
                    print("Composer (START of pc=0)")
                    if not self.state.initialized:
                        self.state.initialized = True
                        if self.state.keyword_arguments is None:
                            self.state.keyword_arguments = {}
                    self.state.pc = 1 # if get restarted PC will be 1
                    # this is essentially: value = result.deposit(value)
                    # self.state.function_name = "ComposerServerlessSync"
                    self.state.keyword_arguments = {}
                    self.state.return_value = None
                    self.state.keyword_arguments["value"] = self.state.starting_input
                    #synchronize_sync(websocket, "synchronize_sync", "result", "try_deposit", self.state)
                    synchronize_async_terminate(websocket, "synchronize_async", "result", "deposit", self.state)
                    if self.state.blocking:
                        self.state.blocking = False
                        return #will restart in (saved) state pc == 1
                    # if we reach here (no return), no restart and we will iterate the while-True loop and transition to next state
                    # Note: In this version, we always restart, so we will always return and 
                elif self.state.pc == 1:
                    print("Composer -- self.state.list_of_functions = " + str(self.state.list_of_functions))
                    print("Composer -- INITIALLY self.state.i = " + str(self.state.i))
                    # while is same indentation level as the prints
                    # Note: we do not initialize i to 0 here, this is done in State inits. If a restart is triggered in the 
                    # loop, we want i to have the same value here that it did before the restart, i.e., 1 or 2. If we reset i
                    # to 0 here, that will not be true.
                    while self.state.i < len(self.state.list_of_functions):
                        print("Composer Loop -- self.state.i = " + str(self.state.i))
                        Func = self.state.list_of_functions[self.state.i]  # invoke function Func
                        print("Composer -- invoke Func = " + str(Func))
                        payload = {
                            "state": State(function_name = Func, restart = False, function_instance_ID = str(uuid.uuid4()))
                        }
                        invoke_lambda(payload = payload)

                        self.state.i += 1
                        # self.state.function_name = "ComposerServerlessSync"
                        self.state.keyword_arguments = {}
                        self.state.return_value = None
                        print("Composer -- POST-INCREMENT i -- self.state.i = " + str(self.state.i))
                        #self.state = synchronize_sync(websocket, "synchronize_sync", "finish", "try_P", self.state)
                        self.state = synchronize_async_terminate(websocket, "synchronize_async", "finish", "P", self.state)
                        if self.state.blocking:
                            self.state.blocking = False
                            return # Note: on restart state.pc == 1, we will reenter while-i loop or not depending on i   
                        # Note: if we reach here, we will iterate the while-i loop and make no state transition so pc == 1 & i<=len(..                                  
                    # Note: when we reach here, we have terminated the loop in state.pc == 1; continue executing after the loop.
                    # This is true if we restarted after P with i==len(..), in which case we skipped while-ii loop, or we did not restart
                    # after the P with i==len().
                    # This is same indentation level as above while
                    ##if self.state.pc == 1:    # Note: we exit loop with state.pc == 1, and i = lenggth of function list
                    print("Composer (After while-i loop with pc=1)")
                    self.state.pc = 2
                    # self.state.function_name = "ComposerServerlessSync"
                    self.state.keyword_arguments = {}
                    self.state.return_value = None
                    #self.state = synchronize_sync(websocket,"synchronize_sync", "result", "try_withdraw", self.state)
                    self.state = synchronize_async_terminate(websocket,"synchronize_async", "result", "withdraw", self.state)
                    print("Composer -- withdrawn state: " + str(self.state))                
                    if self.state.blocking:
                        self.state.blocking = False
                        return #will restart in (saved) state pc == 2, which skips the loop processing in state pc == 1
                    # Note: if we reach here (no return), we will iterate the while-True loop and transition to next state pc == 2
                # this is same indentation level as elif self.state.pv == 1
                elif self.state.pc == 2: 
                    print("Composer (START of pc=2)")
                    #same indentation level as if pc == 0    
                    # restart when withdraw completes or non-blocking withdraw;
                    # either way we have return_value of withdraw
                    self.state.pc = 3
                    value = self.state.return_value
                    self.state.return_value = None
                    logger.debug("Composer -- final state.return_value at end = " + str(value))
                    # self.state.function_name = "ComposerServerlessSync"
                    self.state.keyword_arguments = {}
                    self.state.return_value = None
                    self.state.keyword_arguments["value"] = value
                    #synchronize_sync(websocket, "synchronize_sync", "final_result", "try_deposit", self.state)
                    self.state = synchronize_async_terminate(websocket, "synchronize_async", "final_result", "deposit", self.state)
                    if self.state.blocking:
                        self.state.blocking = False
                        return #will restart in (saved) state pc == 3
                    # Note: if we reach here (no return), we will iterate the while-True loop and transition to next state
                elif self.state.pc == 3:
                    print("Composer (START of pc=3)")
                    print("Composer (Stopping")
                    break
                
            logger.debug(str(self.state.ID) + " completing. Result was: " + str(value))

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

    # Using state.function_name to select method to run. When "ComposerServerlessSync" invoke Composer else 
    # "FuncA"/"FuncB" invoke FuncA/FuncB. When Compose invokes, e.g., FuncA(), it does so by invoking Lambda
    # named "ComposerServerlessSync" with sttw.function_name = "FuncA".
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