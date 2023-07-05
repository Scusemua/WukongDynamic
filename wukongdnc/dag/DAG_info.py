import cloudpickle

def input_DAG_info(file_name):
    with open(file_name, 'rb') as handle:
        DAG_info = cloudpickle.load(handle)
    return DAG_info

class DAG_Info(object):
    def __init__(self,file_name = './DAG_info.pickle'):
        self.file_name = file_name
        self.DAG_info = input_DAG_info(file_name)

    def get_DAG_map(self):
        return self.DAG_info["DAG_map"]
    def get_DAG_states(self):
        return self.DAG_info["DAG_states"]
    def get_all_fanin_task_names(self):
        return self.DAG_info["all_fanin_task_names"]
    def get_all_fanin_sizes(self):
        return self.DAG_info["all_fanin_sizes"]
    def get_all_faninNB_task_names(self):
        return self.DAG_info["all_faninNB_task_names"]
    def get_all_faninNB_sizes(self):
        return self.DAG_info["all_faninNB_sizes"]
    def get_all_fanout_task_names(self):
        return self.DAG_info["all_fanout_task_names"]
    def get_DAG_leaf_tasks(self):
        return self.DAG_info["DAG_leaf_tasks"]
    def get_DAG_leaf_task_start_states(self):
        return self.DAG_info["DAG_leaf_task_start_states"]
    def get_DAG_leaf_task_inputs(self):
        return self.DAG_info["DAG_leaf_task_inputs"]
    # After the driver gets the leaf task inputs it sets DAG_info["DAG_leaf_task_inputs"]
    # to None so that we are not passing all of these inputs to each Lambda executor.
    def set_DAG_leaf_task_inputs_to_None(self):
        self.DAG_info["DAG_leaf_task_inputs"] = None
    def get_DAG_tasks(self):
        return self.DAG_info["DAG_tasks"]
#rhc: continue
# Assuming we set version_number and DAG_info_is_complete when we 
# generate DAG. Default is version_number=1. DAG_info_is_complete=True
    def get_DAG_version_number(self):
        return self.DAG_info["version_number"]
    def get_DAG_info_is_complete(self):
        return self.DAG_info["DAG_info_is_complete"]