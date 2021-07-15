class MemoizationMessage(object):
    def __init__(
        self,
        message_type = None,
        sender_id = None,
        problem_or_result_id = None,
        memoization_label = None,
        result = None,
        fan_in_stack = None,
        become_executor = None,
        did_input = None 
    ):
        self.message_type = message_type 
        self.sender_id = sender_id 
        self.problem_or_result_id = problem_or_result_id 
        self.memoization_label = memoization_label
        self.result = result 
        self.fan_in_stack = fan_in_stack 
        self.become_executor = become_executor 
        self.did_input = did_input         

class MemoizationRecord(object):
    def __init__(self):
        pass 

class MemoizationController(object):
    def __init__(self):
        pass 