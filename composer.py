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
                synchronize_async(websocket, "synchronize_async", "result", "deposit", self.state)
            
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
                self.state = synchronize_sync(websocket, "synchronize_sync", "finish", "P", self.state)
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
                self.state = synchronize_sync(websocket,"synchronize_sync", "result", "withdraw", self.state)
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
            synchronize_async(websocket, "synchronize_async", "final_result", "deposit", self.state)