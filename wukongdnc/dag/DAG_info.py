import cloudpickle
#import os
import copy
#from .DAG_executor_constants import USE_INCREMENTAL_DAG_GENERATION
from . import DAG_executor_constants

import logging
logger = logging.getLogger(__name__)
# called below in the DAG_info __init__ method
def input_DAG_info(file_name = './DAG_info.pickle'):
    logger.info("Reading DAG info from file: \"%s\"" % file_name)
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
    # a DAG_info object and give it to the workers/lambdas without
    # writing a dictionary to file.  For this we 
    # can use DAG_info_fromdictionary.

    #def __init__(self,file_name = './DAG_info.pickle'):
    #    self.file_name = file_name
    #    self.DAG_info = input_DAG_info(file_name)

    def __init__(self,DAG_info_dictionary,file_name = './DAG_info.pickle'):
        self.file_name = file_name

#brc: incremental
# Q: Get rid of this? Or useful for getting latest info?
        #self.DAG_info_dictionary = DAG_info_dictionary

#brc: incremental
        # if we are using incremental DAG generation, the 
        # DAG_info_dictionary DAG_map can be modified by
        # the DAG generator while we are executing the DAG.
        # The writes are to the DAG_map, so here we make a copy
        # of the DAG map so there are two DAG_maps. After consructing
        # this DAG_info object, the generator will create a new reference
        # to  one of the states in the DAG_map so there will be separate references
        # for this state in the two DAG_maps. This is the state that can be 
        # modified while the DAG is executing.

        if not DAG_executor_constants.USE_INCREMENTAL_DAG_GENERATION:
            self.DAG_map = DAG_info_dictionary["DAG_map"]
        else:
            # Q: this is the same as DAG_info_dictionary["DAG_map"].copy()?
            self.DAG_map = copy.copy(DAG_info_dictionary["DAG_map"])
        #where:
        """
        old_Dict = {'name': 'Bob', 'age': 25}
        new_Dict = old_Dict.copy()
        new_Dict['name'] = 'xx'
        print(old_Dict)
        # Prints {'age': 25, 'name': 'Bob'}
        print(new_Dict)
        # Prints {'age': 25, 'name': 'xx'}
        """
        self.DAG_states = DAG_info_dictionary["DAG_states"]
        self.all_fanin_task_names = DAG_info_dictionary["all_fanin_task_names"]
        self.all_fanin_sizes = DAG_info_dictionary["all_fanin_sizes"]
        self.all_faninNB_task_names = DAG_info_dictionary["all_faninNB_task_names"]
        self.all_faninNB_sizes = DAG_info_dictionary["all_faninNB_sizes"]
        self.all_fanout_task_names = DAG_info_dictionary["all_fanout_task_names"]
        self.all_collapse_task_names = DAG_info_dictionary["all_collapse_task_names"]
        self.DAG_leaf_tasks = DAG_info_dictionary["DAG_leaf_tasks"]
        self.DAG_leaf_task_start_states = DAG_info_dictionary["DAG_leaf_task_start_states"]
        self.DAG_leaf_task_inputs = DAG_info_dictionary["DAG_leaf_task_inputs"]
        self.DAG_tasks = DAG_info_dictionary["DAG_tasks"]
        self.DAG_version_number = DAG_info_dictionary["DAG_version_number"]
        self.DAG_is_complete = DAG_info_dictionary["DAG_is_complete"]
        self.DAG_number_of_tasks = DAG_info_dictionary["DAG_number_of_tasks"]
        # This is used during incremental DAG generation. A DAG may have n tasks/groups/partitions
        # but if it has m incomplete groups/partitions, then at most n-m of then
        # can be excuted before a new incremental DAG must be requested. (Also,
        # see the note below about XXXXXXXXXXXXX.) This is used at the statr of the work
        # loop in DAG_executor to detemine how many tasks can be executed before a new
        # DAG should be requested. Likewise, it us used after a new incremental DAG is 
        # obtained to detemrine how many tasks in the new DAG can be executed before a 
        # another new DAG should be requested. 
        if not DAG_executor_constants.USE_INCREMENTAL_DAG_GENERATION:
            self.DAG_number_of_incomplete_tasks = 0
        else:
            self.DAG_number_of_incomplete_tasks = DAG_info_dictionary["DAG_number_of_incomplete_tasks"]
#brc: bug fix:
        if not DAG_executor_constants.USE_INCREMENTAL_DAG_GENERATION:
            self.DAG_number_of_groups_of_previous_partition_that_cannot_be_executed = 0
        else:
            self.DAG_number_of_groups_of_previous_partition_that_cannot_be_executed = DAG_info_dictionary["DAG_number_of_groups_of_previous_partition_that_cannot_be_executed"]

#brc: num_nodes
        self.DAG_num_nodes_in_graph = DAG_info_dictionary["DAG_num_nodes_in_graph"]

    def get_DAG_info_dictionary(self):
        DAG_info_dictionary = {}
        DAG_info_dictionary["DAG_map"] = self.DAG_map
        DAG_info_dictionary["DAG_states"] = self.DAG_states
        DAG_info_dictionary["all_fanin_task_names"] = self.all_fanin_task_names
        DAG_info_dictionary["all_fanin_sizes"] = self.all_fanin_sizes
        DAG_info_dictionary["all_faninNB_task_names"] = self.all_faninNB_task_names
        DAG_info_dictionary["all_faninNB_sizes"] = self.all_faninNB_sizes
        DAG_info_dictionary["all_fanout_task_names"] = self.all_fanout_task_names
        DAG_info_dictionary["all_collapse_task_names"] = self.all_collapse_task_names
        DAG_info_dictionary["DAG_leaf_tasks"] = self.DAG_leaf_tasks
        DAG_info_dictionary["DAG_leaf_task_start_states"] = self.DAG_leaf_task_start_states
        DAG_info_dictionary["DAG_leaf_task_inputs"] = self.DAG_leaf_task_inputs
        DAG_info_dictionary["DAG_tasks"] = self.DAG_tasks
        DAG_info_dictionary["DAG_version_number"] = self.DAG_version_number
        DAG_info_dictionary["DAG_is_complete"] = self.DAG_is_complete
        DAG_info_dictionary["DAG_number_of_tasks"] = self.DAG_number_of_tasks
        DAG_info_dictionary["DAG_number_of_incomplete_tasks"] = self.DAG_number_of_incomplete_tasks
#brc: bug fix:
        DAG_info_dictionary["DAG_number_of_groups_of_previous_partition_that_cannot_be_executed"] = self.DAG_number_of_groups_of_previous_partition_that_cannot_be_executed
#brc: num_nodes
        DAG_info_dictionary["DAG_num_nodes_in_graph"] = self.DAG_num_nodes_in_graph
        return DAG_info_dictionary

    def get_DAG_map(self):
        return self.DAG_map
    def get_DAG_states(self):
        return self.DAG_states
    def get_all_fanin_task_names(self):
        return self.all_fanin_task_names
    def get_all_fanin_sizes(self):
        return self.all_fanin_sizes
    def get_all_faninNB_task_names(self):
        return self.all_faninNB_task_names
    def get_all_faninNB_sizes(self):
        return self.all_faninNB_sizes
    def get_all_fanout_task_names(self):
        return self.all_fanout_task_names
    def get_all_collapse_task_names(self):
        return self.all_collapse_task_names
    def get_DAG_leaf_tasks(self):
        return self.DAG_leaf_tasks
    def get_DAG_leaf_task_start_states(self):
        return self.DAG_leaf_task_start_states
    def get_DAG_leaf_task_inputs(self):
        return self.DAG_leaf_task_inputs
    def get_DAG_tasks(self):
        return self.DAG_tasks
#brc: continue
# Assuming we set version_number and DAG_info_is_complete when we 
# generate DAG. Default is version_number=1. DAG_info_is_complete=True
    def get_DAG_version_number(self):
        return self.DAG_version_number
    def get_DAG_info_is_complete(self):
        return self.DAG_is_complete
    def get_DAG_number_of_tasks(self):
        return self.DAG_number_of_tasks
    def get_DAG_number_of_incomplete_tasks(self):
        return self.DAG_number_of_incomplete_tasks
    def get_DAG_number_of_groups_of_previous_partition_that_cannot_be_executed(self):
        return self.DAG_number_of_groups_of_previous_partition_that_cannot_be_executed
#brc: num_nodes
    def get_DAG_num_nodes_in_graph(self):
        return self.DAG_num_nodes_in_graph
    
    # After the driver gets the leaf task inputs it sets DAG_info["DAG_leaf_task_inputs"]
    # to None so that we are not passing all of these inputs to each Lambda executor.
    def set_DAG_leaf_task_inputs_to_None(self):
        self.DAG_leaf_task_inputs = None

    @classmethod
    def DAG_info_fromfilename(cls, file_name_parm = './DAG_info.pickle'):
        file_name = file_name_parm
        DAG_info_dictionary = input_DAG_info(file_name)
        return cls(DAG_info_dictionary,file_name)

    @classmethod
    def DAG_info_fromdictionary(cls, DAG_info_dict):
        DAG_info_dictionary = DAG_info_dict
        return cls(DAG_info_dictionary)
    

