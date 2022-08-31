import uuid

# Class State represents the state of a running serverless function. State objects 
# are pickeled and sent to the server. The server can then restart a serverless
# function by invoking the function and passing the function its saved state.
#
# The DAG_executor has only one application specific member, which is "state."
# This is the state of the DAG task to be excuted.

class DAG_executor_State(object):
    def __init__(
        self, 
        function_name : str = None, 
        function_instance_ID : str = None,
        restart : bool = False, 
        pc : int = int(0), 
        keyword_arguments : dict = None, 
        return_value = None, 
        blocking : bool = False,
        state : int = int(0)
    ):
        # All state objects have these members, which are used by the framework, not the application.
        self.function_name = function_name                  # This is the variable used for the serverless function name.
        self.function_instance_ID = function_instance_ID    # Like a program ID.
        self.restart = restart                              # Indicates whether we're trying to restart a warm Lambda or if we're knowingly invoking a cold Lambda (value is False in this case).
        self.pc = pc
        self.keyword_arguments = keyword_arguments or {}    # These are the keyword arguments passed from AWS Lambda function to TCP server.
        self.return_value = return_value                    # The value being returned by the TCP server to the AWS Lambda function.
        self.blocking = blocking                            # Indicates whether the Lambda executor is making a blocking or non-blocking call to the TCP server.
        #These members are unique for the application
        self.state = state

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return "Dag_executor_State(FuncName='%s', FuncInstID=%s, Restart=%s, PC=%s, KeywordArgs=%s, ReturnVal=%s, Blocking=%s, state=%s)" % (self.function_name, self.function_instance_ID, str(self.restart), str(self.pc), str(self.keyword_arguments), str(self.return_value), str(self.blocking), str(self.state))