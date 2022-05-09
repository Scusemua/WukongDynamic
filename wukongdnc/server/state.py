import uuid

# Rename this to State when running the composer.
class State(object):
    def __init__(
        self, 
        function_name : str = None, 
        function_instance_ID : str = None,
        restart : bool = False, 
        pc : int = int(0), 
        keyword_arguments : dict = None, 
        return_value = None, 
        blocking : bool = False,
        i : int = int(0),
        ID: str = None,
        list_of_functions = None,
        starting_input = int(0),
        initialized = False
    ):
        self.function_name = function_name                  # This is the variable used for the serverless function name.
        self.function_instance_ID = function_instance_ID    # Like a program ID.
        self.restart = restart                              # Indicates whether we're trying to restart a warm Lambda or if we're knowingly invoking a cold Lambda (value is False in this case).
        self.keyword_arguments = keyword_arguments or {}    # These are the keyword arguments passed from AWS Lambda function to TCP server.
        self.return_value = return_value                    # The value being returned by the TCP server to the AWS Lambda function.
        self.blocking = blocking                            # Indicates whether the Lambda executor is making a blocking or non-blocking call to the TCP server.
        self.ID = ID
        self.pc = pc
        self.i = i
        self.list_of_functions = list_of_functions 
        self.starting_input = starting_input
        self.initialized = initialized

    def __str__(self):
        return "State(FuncName='%s', FuncInstID=%s, Restart=%s, PC=%s, ReturnVal=%s, Blocking=%s, KeywordArgs=%s, i=%s)" % (self.function_name, self.function_instance_ID, str(self.restart), str(self.pc), str(self.return_value), str(self.blocking), str(self.keyword_arguments), str(self.i))
        #return "State(FuncName='%s', FuncInstID=%s, Restart=%s, PC=%s, ReturnVal=%s, Blocking=%s,Problem=%s,Result=%s)" % (self.function_name, self.function_instance_ID, str(self.restart), str(self.pc), str(self.return_value), str(self.blocking), str(type(self.problem)), str(type(self.result)))