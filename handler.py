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
from wukongdnc.wukong.dc_executor import DivideAndConquerExecutor
from wukongdnc.constants import REDIS_IP_PRIVATE

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s')

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
        self.state.ID = "functA"
    
    def execute(self):
        while True:
            if self.state.pc == 0:
                self.state.pc = 1 # if restart PC will be 1
                # this is essentially: value = result.withdraw()
                self.state = self.synchronize_sync(websocket, "synchronize_sync", "result", "try_withdraw", self.state)
                if self.state.blocking:
                    self.state.blocking = False
                    return
                else:
                    # self.state.blocking is False
                    # withdrawn value returned in self.state.return_value
                    value = self.state.return_value
                    self.state.return_value = None
                    self.state.pc = 1 # transition to state PC=1

            elif self.state.pc == 1:
                value = self.state.return_value
                value += 1
                if self.state.keyword_arguments is None:
                    self.state.keyword_arguments = {}
                self.state.keyword_arguments["value"] = value
                self.state.return_value = None
                self.synchronize_async(websocket, "synchronize_async", "result", "deposit", self.state)  
                self.synchronize_async(websocket, "synchronize_async", "finished", "V", self.state)
            else: 
                logger.error("Invalid PC value: " + str(self.state.pc))

        # TODO: Deposit answer in another bounded buffer.
        logger.debug(str(self.state.ID) + " is done. Result: " + str(self.state.result))

class FuncB(object): # same as FuncA with different ID
    def __init__(self, state = None):
        self.state = state
        self.state.ID = "functB"
    
    def execute(self):
        while True:
            if self.state.pc == 0:
                self.state.pc = 1 # if restart PC will be 1
                # this is essentially: value = result.withdraw()
                self.state = self.synchronize_sync(websocket, "synchronize_sync", "result", "try_withdraw", self.state)
                if self.state.blocking:
                    self.state.blocking = False
                    return
                else:
                    # self.state.blocking is False
                    # withdrawn value returned in self.state.return_value
                    value = self.state.return_value
                    self.state.return_value = None
                    self.state.pc = 1 # transition to state PC=1

            elif self.state.pc == 1:
                value = self.state.return_value
                value += 1
                if self.state.keyword_arguments is None:
                    self.state.keyword_arguments = {}
                self.state.keyword_arguments["value"] = value
                self.state.return_value = None
                self.synchronize_async(websocket, "synchronize_async", "result", "deposit", self.state)  
                self.synchronize_async(websocket, "synchronize_async", "finished", "V", self.state)
                break 
            else: 
                logger.error("Invalid PC value: " + str(self.state.pc))

        # TODO: Deposit answer in another bounded buffer.
        logger.debug(str(self.state.ID) + " is done. Result: " + str(self.state.result))

class Composer(object):
    def __init__(self, state = None):
        self.state = state
        self.state.ID = "Composer"
        if not self.state.restart:
            self.List_of_Lambdas = ["FuncA", "FuncB"]

    def run(self):
        while True:
            if pc == 0:
                if self.state.keyword_arguments is None:
                    self.state.keyword_arguments = {}
                    self.state.keyword_arguments["value"] = 0
                    self.state.return_value = None
                    self.synchronize_async(websocket, "synchronize_async", "result", "deposit", self.state)
                    FuncA = self.List_of_Lambdas[self.state.i]  # invoke Lambda A
                    payload = {
                    "state": State(function_name = FuncA, restart = False, function_instance_ID = str(uuid.uuid4()))
                    }
                    invoke_lambda(payload = payload)
                    pc = 1
                    self.state = self.synchronize_sync(websocket, 
                    "synchronize_sync", "finish", "try_P", self.state)
                    if self.state.blocking:
                        self.state.blocking = False
                        return # transition to state 1 on restart

            elif pc==1: 
                # restart when finish.P completes or non-blocking finish.P;
                # either way we have finished P (w/ no return_value)
                self.state.return_value = None
                self.state.i += 1

                FuncB = self.List_of_Lambdas[self.state.i]  # invoke Lambda A
                payload = {
                "state": State(function_name = FuncB, restart = False, function_instance_ID = str(uuid.uuid4()))
                }
                invoke_lambda(payload = payload)

                pc = 2
                self.state = self.synchronize_sync(websocket, 
                "synchronize_sync", "finish", "try_P", self.state)
                if self.state.blocking:
                    self.state.blocking = False
                    return    #transition to state 2 on restart

            elif pc==2:
                # restart when finish.P completes or non-blocking finish.P;
                # either way we have finished P (w/ no return_value
                pc = 3
                self.state = self.synchronize_sync(websocket, 
                "synchronize_sync", "result", "try_withdraw", self.state)
                if self.state.blocking:
                    self.state.blocking = False
                    return #transition to state 2 on restart

            elif pc==3:
                # restart when withdraw completes or non-blocking withdraw;
                # either way we have return_value of withdraw
                value = self.state.return_value
                self.state.return_value = None
                break

        logger.debug("value = " + str(value))

def lambda_handler(event, context):
    start_time = time.time()
    rc = redis.Redis(host = REDIS_IP_PRIVATE, port = 6379)
    logger.debug("Invocation received.")

    # Extract all of the data from the payload.
    # first_executor = event["first_executor"]
    state = cloudpickle.loads(base64.b64decode(event["state"]))
    target = event['target']
    if target == "Composer":
        composer = Composer(
            state = state
        )
        composer.execute()
    elif target == "funcA":
        A = funcA(state = state)
        A.execute()
    elif target == "funcB":
        B = funcB(state = state)
        B.execute()
    else:
        raise ValueError("Invalid target specified: " + str(target))

    logger.debug("Starting *****TARGET***.")
    end_time = time.time()
    duration = end_time - start_time
    logger.debug("Executor finished. Time elapsed: %f seconds." % duration)
    rc.lpush("durations", duration)    