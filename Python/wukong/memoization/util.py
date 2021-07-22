from enum import Enum

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
    def __init__(
        self,
        type = None,
        result_id = None,
        memoization_label = None,
        result = None,
        promised_results = None,
        promised_results_temp = None
    ):
        self.type = type,
        self.result_id = result_id,
        self.memoization_label = memoization_label,
        self.result = result,
        self.promised_results = promised_results,
        self.promised_results_temp = promised_results_temp
    
    def __str__(self):
        return "type: " + str(type)

class MemoizationRecordType(Enum):
    PROMISEDVALUE = 0
    DELIVEREDVALUE = 1

class MemoizationMessageType(Enum):
    ADDPAIRINGNAME = 0
    REMOVEPAIRINGNAME = 1 
    PROMISEVALUE = 2
    DELIVEREDVALUE = 3
    PAIR = 4

class PromisedResult(object):
    def __init__(
        self, 
        problem_result_or_id = None, 
        fan_in_stack = None, 
        become_executor = False, 
        did_input = False
    ):
        self.problem_result_or_id = problem_result_or_id
        self.fan_in_stack = fan_in_stack
        self.become_executor = become_executor
        self.did_input = did_input