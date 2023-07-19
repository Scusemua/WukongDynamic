import cloudpickle
import os

# called below in the ADG_info __init__ method
def input_DAG_info(file_name):
    with open(file_name, 'rb') as handle:
        DAG_info = cloudpickle.load(handle)
    return DAG_info

class DAG_Info(object):
    # Using an __int__ that is same as DAG_info_fromfilename
    # since this init is what we started with and is what we use
    # most of the time, i.e., we generate a DAG_info dictionary 
    # and save it to a file that is read by __init__ and saved in
    # self.DAG_info. The DAG_execition_Driver calls __init_ to
    # get a DAG_info object that it adds to the payload for lambdas. 
    # The worker threads and processes read DAG_info objects at the 
    # start of their executions.
    # Having now added incremental DAG_generation, we need to generate
    # a DAG_info object and give it to th workers/lambdas without
    # writing a dictionary to file.  For this we 
    # can use DAG_info_fromdictionary.

    #def __init__(self,file_name = './DAG_info.pickle'):
    #    self.file_name = file_name
    #    self.DAG_info = input_DAG_info(file_name)

    def __init__(self,DAG_info,file_name = './DAG_info.pickle'):
        self.file_name = file_name
        self.DAG_info_dictionary = DAG_info

    @classmethod
    def DAG_info_fromfilename(cls, file_name = './DAG_info.pickle'):
        file_name = file_name
        DAG_info = input_DAG_info(file_name)
        return cls(DAG_info,file_name)

    @classmethod
    def DAG_info_fromdictionary(cls, DAG_info_dictionary):
        DAG_info = DAG_info_dictionary
        return cls(DAG_info)

    def get_DAG_map(self):
        return self.DAG_info_dictionary["DAG_map"]
    
    def get_DAG_states(self):
        return self.DAG_info_dictionary["DAG_states"]
    def get_all_fanin_task_names(self):
        return self.DAG_info_dictionary["all_fanin_task_names"]
    def get_all_fanin_sizes(self):
        return self.DAG_info_dictionary["all_fanin_sizes"]
    def get_all_faninNB_task_names(self):
        return self.DAG_info_dictionary["all_faninNB_task_names"]
    def get_all_faninNB_sizes(self):
        return self.DAG_info_dictionary["all_faninNB_sizes"]
    def get_all_fanout_task_names(self):
        return self.DAG_info_dictionary["all_fanout_task_names"]
    def get_DAG_leaf_tasks(self):
        return self.DAG_info_dictionary["DAG_leaf_tasks"]
    def get_DAG_leaf_task_start_states(self):
        return self.DAG_info_dictionary["DAG_leaf_task_start_states"]
    def get_DAG_leaf_task_inputs(self):
        return self.DAG_info_dictionary["DAG_leaf_task_inputs"]
    # After the driver gets the leaf task inputs it sets DAG_info["DAG_leaf_task_inputs"]
    # to None so that we are not passing all of these inputs to each Lambda executor.
    def set_DAG_leaf_task_inputs_to_None(self):
        self.DAG_info_dictionary["DAG_leaf_task_inputs"] = None
    def get_DAG_tasks(self):
        return self.DAG_info_dictionary["DAG_tasks"]
#rhc: continue
# Assuming we set version_number and DAG_info_is_complete when we 
# generate DAG. Default is version_number=1. DAG_info_is_complete=True
    def get_DAG_version_number(self):
        return self.DAG_info_dictionary["version_number"]
    def get_DAG_info_is_complete(self):
        return self.DAG_info_dictionary["DAG_info_is_complete"]