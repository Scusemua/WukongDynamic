
#Algorithm: 
# During DFS create and compute a Node for each Dask_node in the Dask DAG.
# Comptute a Node simply by setting succ, pred, and task_name for the Node.
# Also, we need a map DAG_tasks from each task_name to task, ["add", add], where def add(...):
# Save all of the Nodes as they are computed in a list Node_List.
# After the DFS completes, iterate through the Node_list and call generate_ops() on each Node:
#   for node in Nodes_List:
#       node.generate_ops()
# Then call Node.save_DAG_info() to save the DAG information. DAG_info is a map from String to map/list, e.g., from "fanouts" 
# to a List of String (i.e., List of fanout task names)
# This creates file DAG_info.pickle, which is used by the DAG_executor.

#import pickle
import cloudpickle
import copy
import logging 

logger = logging.getLogger(__name__)

logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(asctime)s] [%(module)s] [%(processName)s] [%(threadName)s]: %(message)s')
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)

logger.addHandler(ch)


class state_info:
    def __init__(self, task_name, fanouts = None, fanins = None, faninNBs = None, collapse = None,
        fanin_sizes = None, faninNB_sizes = None, task_inputs = None,
#rhc continue
        ToBeContinued = False,
        fanout_fanin_faninNB_collapse_groups=False):

        self.task_name = task_name
        self.fanouts = fanouts      # see comment below for examples
        self.fanins = fanins
        self.faninNBs = faninNBs
        self.fanin_sizes = fanin_sizes
        self.faninNB_sizes = faninNB_sizes
        self.collapse = collapse
        self.task_inputs = task_inputs
#rhc continue
        # True if we are using incremental DAG_info generation, which is 
        # currently only for pagerank.  For Dask Dags, we use DFS_visit.py
        # to convert DASK Dags to our DAGs. For pagerank, we have a seperate
        # DAG generator.
        self.ToBeContinued = ToBeContinued
        # True if any of the fanout_fanin_faninNB_collapse_groups are incomplete
        self.fanout_fanin_faninNB_collapse_groups_are_ToBeContinued = fanout_fanin_faninNB_collapse_groups

    @classmethod
    def state_info_fromstate_info(cls, state_info_object):
        state_info_cls = state_info_object
        return cls(state_info_cls.task_name, state_info_cls.fanouts, state_info_cls.fanins, state_info_cls.faninNBs, state_info_cls.collapse,
            state_info_cls.fanin_sizes, state_info_cls.faninNB_sizes, state_info_cls.task_inputs,
#rhc continue
            state_info_cls.ToBeContinued,
            state_info_cls.fanout_fanin_faninNB_collapse_groups)

    def __str__(self):
        if self.fanouts != None:
            fanouts_string = str(self.fanouts)
        else:
            fanouts_string = "None"
        if self.fanins != None:
            fanins_string = str(self.fanins)
        else:
            fanins_string = "None"
        if self.faninNBs != None:
            faninNBs_string = str(self.faninNBs)
        else:
            faninNBs_string = "None"
        if self.collapse != None:
            collapse_string = str(self.collapse)
        else:
            collapse_string = "None"
        if self.fanin_sizes != None:
            fanin_sizes_string = str(self.fanin_sizes)
        else:
            fanin_sizes_string = "None"
        if self.faninNB_sizes != None:
            faninNB_sizes_string = str(self.faninNB_sizes)
        else:
            faninNB_sizes_string = "None"         
        if self.task_inputs != None:
            task_inputs_string = str(self.task_inputs)
        else:
            task_inputs_string = "None"

        ToBeContinued_string = str(self.ToBeContinued)
        fanout_fanin_faninNB_collapse_groups_string = str(self.fanout_fanin_faninNB_collapse_groups_are_ToBeContinued)

        return (" task: " + self.task_name + ", fanouts:" + fanouts_string + ", fanins:" + fanins_string + ", faninsNB:" + faninNBs_string
            + ", collapse:" + collapse_string + ", fanin_sizes:" + fanin_sizes_string
            + ", faninNB_sizes:" + faninNB_sizes_string + ", task_inputs:" + task_inputs_string
#rhc continue
            + ", ToBeContinued:" + ToBeContinued_string
            + ", fanout_fanin_faninNB_collapse_groups_are_ToBeContinued:" + fanout_fanin_faninNB_collapse_groups_string)

    def __deepcopy__(self, memodict={}):
        new_instance = state_info(self.task_name,
            self.fanouts, self.fanins, self.faninNBs, self.fanin_sizes,
            self.faninNB_sizes, self.collapse, self.task_inputs, self.ToBeContinued)
        new_instance.__dict__.update(self.__dict__)
        new_instance.task_name = copy.deepcopy(self.task_name, memodict)
        new_instance.fanouts = copy.deepcopy(self.fanouts, memodict)
        new_instance.fanins = copy.deepcopy(self.fanins, memodict)
        new_instance.faninNBs = copy.deepcopy(self.faninNBs, memodict)
        new_instance.collapse = copy.deepcopy(self.collapse, memodict)
        new_instance.fanin_sizes = copy.deepcopy(self.fanin_sizes, memodict)
        new_instance.faninNB_sizes = copy.deepcopy(self.faninNB_sizes, memodict)
        new_instance.task_inputs = copy.deepcopy(self.task_inputs, memodict)
        new_instance.ToBeContinued = copy.deepcopy(self.ToBeContinued, memodict)
        new_instance.fanout_fanin_faninNB_collapse_groups_are_ToBeContinued = copy.deepcopy(self.fanout_fanin_faninNB_collapse_groups_are_ToBeContinued, memodict)
        
        return new_instance

""" Examples of fanouts, fanins, and faninNBs (No Becomes)
  n2   n3
    t  t
     n1 
n1's fanout List is [n2,n3]

   n2
   |
   |
   n1 
n1's collapse list is [n2]

Note: When we get to the state in which we execute n1, we will fanout n2 and fanout n3 and n1 will
become one of these. Fanout n2/n3 means that we will start the DAG execuution program in the state 
in which n2/n3 is excuted. That is the fanout task is excuted by starting the DAG execution
program in the state that excutes the fanout task. So each fanout is asociated with a state, and
we pass this state in the payload for the Lamda that executes the DAG; in this state, the fanout
task will be excuted (using the information from the DAG_map obtained by using state as the key).
     
  n2   n3
   t  t  t
    n1   n4
n1's fanouts List is [n2] - N1 will become n2
n1's faninNBs list is n3 - neither n1 nor n4 will becone fanin task n3. 
n4's faninNBs list is n3

Note: perhaps you can refine the rules for fanins and faninNBs so there are more becomes. For example,

  n2   n3
 t t  t  t
n0  n1   n4

Instead of n2 and n3 are both faninNBs, n2 can be a fanin and n3 a faninNB.
     
     n3
    t  t
   n1   n4
n1's fanins List is n3
n4's fanins list is n3
"""

class Node:
	# global to visit()
    num_fanins = 0
    num_fanouts = 0
    num_faninNBs = 0
    num_collapse = 0
    next_state = 1
    
    # DAG information collected:
    DAG_map = {} # map from state (per task) to the fanin/fanout/faninNB operations executed after the task is executed
    DAG_states = {} # map from String task_name to the state that task is executed (one state per task)
    DAG_leaf_task_start_states = []
    DAG_leaf_tasks = []
    DAG_leaf_task_inputs = []
    DAG_tasks = {} # map from task name to task, e.g., "add" to add()
    all_fanout_task_names = []	# list of all fanout task names in the DAG
    all_fanin_task_names = []
    all_faninNB_task_names = []
    all_collapse_task_names = []  # if task A is followed only by a fanout to task B: A --> B then we collapse B and A
    all_fanin_sizes = [] # all_fanin_sizes[i] is the size of all_fanin_task_names[i] 
    all_faninNB_sizes = []
    # needed for incremental DAG generation
    # for non-incremental DAG generation, version is 1 and complete is True
    DAG_version_number = 1
    DAG_is_complete = True
    # set after the DAG has been constructed
    DAG_number_of_tasks = 0
    DAG_number_of_incomplete_tasks = 0
    
    # map of all the (above) DAG information, which is a map from String to list/map. The lists/maps in DAG_info
    # are essentially the class members above. This DAG_info will be input by the DAG_executor to execute the DAG.
    DAG_info = {} 
    
    @classmethod
    def save_DAG_info(cls):
        with open('./DAG_info.pickle', 'wb') as handle:
            cloudpickle.dump(Node.DAG_info, handle) #, protocol=pickle.HIGHEST_PROTOCOL)  
    
    def __init__(self,succ=None, pred=None, task_name = None, task = None, task_inputs = None):
        self.succ = succ # names of dependent (successr) tasks
        self.pred = pred # names of tasks that have this noed as a dependent (successor). They are our "enablers"
        self.task_name = task_name # task_name  for this node
        self.task = task
        self.task_inputs = task_inputs
    def get_task_inputs(self):
        return self.task_inputs 
    def set_task_inputs(self, task_inputs):
        self.task_inputs = task_inputs            
    def get_pred(self):     
        return self.pred
    def set_pred(self,lst):
        self.pred = lst
    def get_succ(self):     
        return self.succ
    def set_succ(self,succ):
        self.succ = succ
    def get_task_name(self):
        return self.task_name
    def set_task_name(self,task_name):
        self.task_name = task_name
    def get_task(self):
        return self.task
    def set_task(self,task):
        self.task = task
        
    def generate_ops(self):  
        # generate the task name, and the fanin, fanout, faninNB, and collapse operations for this Node. For example, in state 1, the task executed 
        # is "inc0" and the ops executed after this task are:
        # 1  :  inc0, fanouts:[],fanins:[],faninsNB:['add'],collapse:[]fanin_sizes:[],faninNB_sizes:[2]
        # After executing task "in0", execute one faninNB for faninNB task "add" that has 2 inputs, one of which will be from "add".
        fanouts = []	# list of task_names of fanout tasks of T --> fanout
        fanins = []	    # list of task_names of fanin tasks of T --> fanin, where there will be a become
        faninNBs = []   # list of task_names of fanin tasks of T --> fanin, where there will be no become (NB)
        collapse = []   # list of task_names of collapsed tasks of T --> collapse, where there will be one succ (pred) edge of T (collapse)
        fanin_sizes = [] # sizes of fanins by position in fanins
        faninNB_sizes = [] # sizes of faninNBs by position in faninNBs  
        
        Node.DAG_tasks[self.task_name] = self.task
        dependents_of_T = self.get_succ() # s is a dependant/successor of T if T --> s
        print("process dependents_of_T")
        for s in dependents_of_T or []:
            print("process dependent " + s.get_task_name())
            enablers = s.get_pred()	# e is an enabler of s if e --> s. Note: List of enablers of s includes T   
            if len(enablers) == 1:
                # assert: enablers[0] = T
                if len(dependents_of_T) == 1:
                    collapse.append(s.get_task_name())   				# append task_name of Node s as a collapsed task of T in List collapse
                    if not s.get_task_name() in Node.all_collapse_task_names:
                        Node.all_collapse_task_names.append(s.get_task_name())	# global list
                    Node.num_collapse += 1
                else:
                    fanouts.append(s.get_task_name())   				# append task_name of Node s as a fanout task of T in List fanouts
                    if not s.get_task_name() in Node.all_fanout_task_names:
                        Node.all_fanout_task_names.append(s.get_task_name())		# global list
                    Node.num_fanouts += 1
            else:
                NB = False  # set to True when we determine that the fanin has no become 
                for e in enablers or []:
                    dependents_of_e = e.get_succ()  # List
                    if len(dependents_of_e) > 1:
                        # if a fanin s has an enabler e that has more than one dependent then s is a faninNB
                        # ToDo: Optimize fanins: if no fanouts, many fanins/faninNBs, can still have a fanin (a pred of f1 is also a pred of f2)
                        faninNBs.append(s.get_task_name())	# per state list
                        faninNB_sizes.append(len(s.get_pred()))	# per state list
                        if not (s.get_task_name() in Node.all_faninNB_task_names):
                            Node.all_faninNB_task_names.append(s.get_task_name())	# global list
                            Node.all_faninNB_sizes.append(len(s.get_pred()))	# global list
                        Node.num_faninNBs += 1
                        NB = True
                        break
					
                if not NB:
                    fanins.append(s.get_task_name())
                    #print("fanin" + s.get_task_name() + " size is " + str(len(s.get_pred())))
                    fanin_sizes.append(len(s.get_pred()))	# per state list
                    if not s.get_task_name() in Node.all_fanin_task_names:
                        Node.all_fanin_task_names.append(s.get_task_name())	# global list
                        Node.all_fanin_sizes.append(len(s.get_pred()))	# global list
                    Node.num_fanins += 1

        # asserts
        if dependents_of_T == None:
            number_dependents_of_T = 0
        else:
            number_dependents_of_T = len(dependents_of_T)
		# all dependents must be accounted for
        if len(fanouts) + len(fanins) + len(faninNBs) + len(collapse) != number_dependents_of_T:
            print("Error 1") 
        if len(fanins) > 0:	# can only be one fanin and no fanin or faninNB
            if not len(fanins) == 1 and len(fanouts) == 0 and len(faninNBs) == 0:
                print("Error 2") 
        if len(faninNBs) > 0: # cannot be any fanins
            if len(fanins) > 0:
                print("Error 3")
		# if there is a collapse then there can be no fanins, fanouts, or fanin
        if len(collapse) > 0:	# can only be one fanin and no fanin or faninNB
            if not len(fanins) == 0 and len(fanouts) == 0 and len(faninNBs) == 0:
                print("Error 4") 				
		# end asserts
        
        state = self.generate_state(self.get_task_name())
		
        # if T previously encountered as a dependent, now we add T's information for executing T
        Node.DAG_map[state] = state_info(self.get_task_name(), fanouts, fanins, faninNBs, collapse, fanin_sizes, faninNB_sizes, self.get_task_inputs(),
#rhc continue : For DASK DAGS we do not generate incremental DAGs - a DAG contains
            # all the tasks.
            False)
        Node.DAG_states[self.get_task_name()] = state
        #ToDo: Don't need this if task inputs are in state_info
        if len(self.get_pred()) == 0:
            Node.DAG_leaf_task_start_states.append(state)
            Node.DAG_leaf_tasks.append(self.get_task_name()) 
            Node.DAG_leaf_task_inputs.append(self.get_task_inputs())
        
        # Generate fanin/fanouts. Order is important; we will execute faninNBs followed by fanouts (where
        # fanout has a become) or fanins, where fanins and faninNBs/fanouts are mutually exlusive. i.e., 
        # we cannot do a faninNB/fanout if we can do a fanin, and vice versa.
        for n in faninNBs:
            state = self.generate_state(n)
            if state not in Node.DAG_map:
                # assign state to faninNB tasks; fill in rest when visit task
                Node.DAG_map[state] = state_info(n)
                Node.DAG_states[n] = state	
			
        for n in collapse:
            state = self.generate_state(n)
            if state not in Node.DAG_map:
                # assign state to collapse task; fill in rest when visit task
                Node.DAG_map[state] = state_info(n)
                Node.DAG_states[n] = state	
        
        for n in fanouts:
            state = self.generate_state(n)
            if state not in Node.DAG_map:
                # assign state to fanout tasks; fill in rest when visit task
                Node.DAG_map[state] = state_info(n)
                Node.DAG_states[n] = state	
            
        for n in fanins:
            state = self.generate_state(n)
            if state not in Node.DAG_map:            
                # assign state to fanin tasks; fill in rest when visit task
                Node.DAG_map[state] = state_info(n)
                Node.DAG_states[n] = state	

        # needed for increemental DAG generation
        Node.DAG_number_of_tasks = len(Node.DAG_tasks)

		#assert: Need size_of_DAG from Ben
			# if (size_of_DAG != self.next_state-1) or (size_of_DAG  != len(DAG_map)) 
			# 	or (size_of_DAG  != len(self.states) or (size_of_DAG != len(DAG_tasks)
			#		print("Error 5")
		# end assert
            
        print("DAG_map:")
        for key, value in Node.DAG_map.items():
            print(key, ' : ', value)
        print()
        print("states:")         
        for key, value in Node.DAG_states.items():
            print(key, ' : ', value)
        print()
        print("num_fanins:" + str(Node.num_fanins) + " num_fanouts:" + str(Node.num_fanouts) + " num_faninNBs:" 
			  + str(Node.num_faninNBs) + " num_collapse:" + str(Node.num_collapse))
        print()  
        print("all_fanout_task_names")
        for name in Node.all_fanout_task_names:
            print(name)
        print()
        print("all_fanin_task_names")
        for name in Node.all_fanin_task_names :
            print(name)
        print()
        print("all_faninNB_task_names")
        for name in Node.all_faninNB_task_names:
            print(name)
        print()
        print("all_collapse_task_names")
        for name in Node.all_collapse_task_names:
            print(name)
        print()
        print("leaf task start states")
        for start_state in Node.DAG_leaf_task_start_states:
            print(start_state)
        print()
        print("DAG_tasks:")
        for key, value in Node.DAG_tasks.items():
            print(key, ' : ', value)
        print()
        print("DAG_leaf_tasks:")
        for task_name in Node.DAG_leaf_tasks:
            print(task_name)
        print() 
        print("DAG_leaf_task_inputs:")
        for inp in Node.DAG_leaf_task_inputs:
            print(inp)
        print() 
        print("DAG_version_number:")
        print(Node.DAG_version_number)
        print()
        print("DAG_is_complete:")
        print(Node.DAG_is_complete)
        print()
        print("DAG_number_of_tasks:")
        print(Node.DAG_number_of_tasks)       
		
        Node.DAG_info["DAG_map"] = Node.DAG_map
        Node.DAG_info["DAG_states"] = Node.DAG_states
        Node.DAG_info["DAG_leaf_tasks"] = Node.DAG_leaf_tasks
        Node.DAG_info["DAG_leaf_task_start_states"] = Node.DAG_leaf_task_start_states
        Node.DAG_info["DAG_leaf_task_inputs"] = Node.DAG_leaf_task_inputs
        Node.DAG_info["all_fanout_task_names"] = Node.all_fanout_task_names
        Node.DAG_info["all_fanin_task_names"] = Node.all_fanin_task_names
        Node.DAG_info["all_faninNB_task_names"] = Node.all_faninNB_task_names
        Node.DAG_info["all_collapse_task_names"] = Node.all_collapse_task_names
        Node.DAG_info["all_fanin_sizes"] = Node.all_fanin_sizes
        Node.DAG_info["all_faninNB_sizes"] = Node.all_faninNB_sizes
        Node.DAG_info["DAG_tasks"] = Node.DAG_tasks
        # needed for incremental DAG generation
        Node.DAG_info["DAG_version_number"]  = Node.DAG_version_number
        Node.DAG_info["DAG_is_complete"]  = Node.DAG_is_complete
        Node.DAG_info["DAG_number_of_tasks"]  = Node.DAG_number_of_tasks
        Node.DAG_info["DAG_number_of_incomplete_tasks"]  = Node.DAG_number_of_incomplete_tasks

        #with open('DAG_info.pickle', 'wb') as handle:
        #    pickle.dump(DAG_info, handle, protocol=pickle.HIGHEST_PROTOCOL)
        
        # Need to save DAG map as it is used to execute the DAG.
            
    def generate_state(self,task_name):
        # if already assigned a state to the task, use that state. When we generate the fanin/fanout/fanoutNB events
        # for a task that is executed in a given state, we will see the dependents of this task. We assign dependents
        # a state but we will not generate fanin/fanout/fanoutNB events for the dependents until we visit them.
        # Note: We need to know the state for the dependent tasks, e.g., the state for a target tsk of a fanout because
        # we will enter that state and execute that task when the fanout event occurs.
        if task_name in Node.DAG_states:
            assigned_state = Node.DAG_states[task_name]
        else:
            assigned_state = None
        print("generate state: task_name: " + task_name + " assigned state:" + str(assigned_state))
        if assigned_state != None:
            state = assigned_state
            # already encountered n as a dependent, here n is also a dependent so no need to add the same 
            # partial information about n. Will add complete information when n is visited.
        else:
            # assigned state is next state.
            state = Node.next_state
            Node.next_state += 1
            Node.DAG_states[task_name] = state	# Note: we won't be using T's state again

        return int(state)			

# Used as tasks in the test case
def add(inp):
    logger.debug("add: " + "input: " + str(input))
    num1 = inp['inc0']
    num2 = inp['inc1']
    sum = num1 + num2
    output = {'add': sum}
    logger.debug("add output: " + str(sum))
    return output
def multiply(inp):
    logger.debug("multiply")
    num1 = inp['add']
    num2 = inp['square']
    num3 = inp['triple']
    product = num1 * num2 * num3
    output = {'multiply': product}
    logger.debug("multiply output: " + str(product))
    return output
def divide(inp):
    logger.debug("divide")
    num1 = inp['multiply']
    quotient = num1 / 72
    output = {'quotient': quotient}
    logger.debug("quotient output: " + str(quotient))
    return output
def triple(inp):
    logger.debug("triple")
    value = inp['inc1']
    value *= 3
    output = {'triple': value}
    logger.debug("triple output: " + str(output))
    return output
def square(inp):
    logger.debug("square")
    value = inp['inc1']
    value *= value
    output = {'square': value}
    logger.debug("square output: " + str(output))
    return output
def inc0(inp):
    logger.debug("inc0")
    value = inp['input']
    value += 1
    output = {'inc0': value}
    logger.debug("inc0 output: " + str(output))
    return output
def inc1(inp):
    logger.debug("inc1")
    value = inp['input']
    value += 1
    output = {'inc1': value}
    logger.debug("inc1 output: " + str(output))
    return output

def main():
    #n2 = Node(None,None,"n2")
    #n3 = Node(None,None,"n3")
    #n1 = Node(Node,Node,"n1")
    #n1.set_succ([n2,n3])
    #n2.set_pred([n1])
    #n3.set_pred([n1])

    #n1.visit(n1)
    
    #n2 = Node(None,None,"n2")
    #n3 = Node(None,None,"n3")
    #n1 = Node(Node,Node,"n1")    
    #n1.set_succ([n3])
    #n2.set_succ([n3])
    #n3.set_pred([n1,n2])
    #n1.visit(n1)
    
    """
    n2 = Node(None,None,"n2")
    n3 = Node(None,None,"n3")
    n1 = Node(Node,Node,"n1")
    n4 = Node(Node,Node,"n4") 
    n5 = Node(Node,Node,"n5")  
	# n1 has fanouts to n4 and n5
	# n1 has a faninNB to n3, where n3 not fanin since n3 predecessor n1 has more than one dependent
	# n2 has a faninNB to n3, where n3 not fanin since n3 predecessor n1 has more than one dependent
    n1.set_succ([n3,n4,n5])
    n2.set_succ([n3])	# n2 has one dependent n3 but n1 has dependents n3 and n4 and n5, so n3 is a faninNB
    n3.set_pred([n1,n2])
    n3.set_succ([])
    n4.set_pred([n1])
    n4.set_succ([])
    n5.set_pred([n1])
    n5.set_succ([])
    n1.visit(n1)
    n2.visit(n2)
    n3.visit(n3)
    n4.visit(n4)
    n5.visit(n5)
    """
    
    n1 = Node(None,None,"inc0",inc0)
    n3 = Node(None,None,"triple",triple)
    n4 = Node(None,None,"inc1",inc1)
    n5 = Node(Node,Node,"square",square)
    n2 = Node(Node,Node,"add",add) 
    n6 = Node(Node,Node,"multiply",multiply) 
    n7 = Node(Node,Node,"divide",divide)
	
    n1.set_succ([n2])
    n1.set_pred([])
    n2.set_succ([n6])	
    n2.set_pred([n1,n4])
    n3.set_succ([n6])
    n3.set_pred([n4])
    n4.set_succ([n2,n5,n3])
    n4.set_pred([])
    n5.set_succ([n6])
    n5.set_pred([n4])
    n6.set_succ([n7])
    n6.set_pred([n2,n3,n5])
    n7.set_succ([])
    n7.set_pred([n6])

    task_name_TO_node = {}
    task_name_TO_node['inc0'] = n1
    task_name_TO_node['add'] = n2
    task_name_TO_node['triple'] = n3
    task_name_TO_node['inc1'] = n4
    task_name_TO_node['square'] = n5
    task_name_TO_node['multiply'] = n6
    task_name_TO_node['divide'] = n7
	
    leaf_nodes = []
    leaf_nodes.append(n1)
    leaf_nodes.append(n4)
    
    DFS_nodes = []

    def dfs(visited, node):  #function for dfs 
        task_name = node.get_task_name() 
        if task_name not in visited:
            print("DFS: visit task: " + task_name)
            DFS_nodes.append(node)
            visited.add(task_name)
            dependents = node.get_succ()
            for dependent_node in dependents:
                dependent_task_name = dependent_node.get_task_name() 
                print("neighbor_task_name: " + dependent_task_name)
                dfs(visited, task_name_TO_node[dependent_task_name])

    visited = set() # Set to keep track of visited nodes of DAG.
	
    for leaf_node in leaf_nodes:
        dfs(visited,leaf_node)

    print()
    print("DFS_nodes:")
    for n in DFS_nodes:
        print(n.get_task_name())

    print()
    print("generate ops for DFS_nodes:")        
    for n in DFS_nodes:  
        print("generate_ops for: " + n.get_task_name())
        n.generate_ops()
        
    #assert:
    if len(Node.DAG_leaf_tasks) == 0:
        print("Error 6")
    
        
    Node.save_DAG_info()
	
    #n1.generate_ops()
    #n4.generate_ops()
    #n2.generate_ops()
    #n3.generate_ops()
    #n5.generate_ops()
    #n6.generate_ops()
    #n7.generate_ops()
    #Node.save_DAG_info()

if __name__=="__main__":
    main()
	
	
""" Output:
generate states: task_name: inc0 assigned state:None
generate states: task_name: add assigned state:None
DAG_map:
1  :   task: inc0, fanouts:[],fanins:[],faninsNB:['add'],collapse:[]fanin_sizes:[],faninNB_sizes:[2]
2  :   task: add, fanouts:None,fanins:None,faninsNB:None,collapse:Nonefanin_sizes:None,faninNB_sizes:None

states:
inc0  :  1
add  :  2

num_fanins:0 num_fanouts:0 num_faninNBs: num_collapse:0

all_fanout_task_names

all_fanin_task_names

all_faninNB_task_names
add

all_collapse_task_names

leaf task start states
1

DAG_tasks:
inc0  :  <function inc0 at 0x000002215FDFDD30>

generate states: task_name: inc1 assigned state:None
generate states: task_name: add assigned state:2
generate states: task_name: square assigned state:None
generate states: task_name: triple assigned state:None
DAG_map:
1  :   task: inc0, fanouts:[],fanins:[],faninsNB:['add'],collapse:[]fanin_sizes:[],faninNB_sizes:[2]
2  :   task: add, fanouts:None,fanins:None,faninsNB:None,collapse:Nonefanin_sizes:None,faninNB_sizes:None
3  :   task: inc1, fanouts:['square', 'triple'],fanins:[],faninsNB:['add'],collapse:[]fanin_sizes:[],faninNB_sizes:[2]
4  :   task: square, fanouts:None,fanins:None,faninsNB:None,collapse:Nonefanin_sizes:None,faninNB_sizes:None
5  :   task: triple, fanouts:None,fanins:None,faninsNB:None,collapse:Nonefanin_sizes:None,faninNB_sizes:None

states:
inc0  :  1
add  :  2
inc1  :  3
square  :  4
triple  :  5

num_fanins:0 num_fanouts:2 num_faninNBs: num_collapse:0

all_fanout_task_names
square
triple

all_fanin_task_names

all_faninNB_task_names
add

all_collapse_task_names

leaf task start states
1
3

DAG_tasks:
inc0  :  <function inc0 at 0x000002215FDFDD30>
inc1  :  <function inc1 at 0x000002215FDFDDC0>

generate states: task_name: add assigned state:2
generate states: task_name: multiply assigned state:None
DAG_map:
1  :   task: inc0, fanouts:[],fanins:[],faninsNB:['add'],collapse:[]fanin_sizes:[],faninNB_sizes:[2]
2  :   task: add, fanouts:[],fanins:['multiply'],faninsNB:[],collapse:[]fanin_sizes:[3],faninNB_sizes:[]
3  :   task: inc1, fanouts:['square', 'triple'],fanins:[],faninsNB:['add'],collapse:[]fanin_sizes:[],faninNB_sizes:[2]
4  :   task: square, fanouts:None,fanins:None,faninsNB:None,collapse:Nonefanin_sizes:None,faninNB_sizes:None
5  :   task: triple, fanouts:None,fanins:None,faninsNB:None,collapse:Nonefanin_sizes:None,faninNB_sizes:None
6  :   task: multiply, fanouts:None,fanins:None,faninsNB:None,collapse:Nonefanin_sizes:None,faninNB_sizes:None

states:
inc0  :  1
add  :  2
inc1  :  3
square  :  4
triple  :  5
multiply  :  6

num_fanins:1 num_fanouts:2 num_faninNBs: num_collapse:0

all_fanout_task_names
square
triple

all_fanin_task_names
multiply

all_faninNB_task_names
add

all_collapse_task_names

leaf task start states
1
3

DAG_tasks:
inc0  :  <function inc0 at 0x000002215FDFDD30>
inc1  :  <function inc1 at 0x000002215FDFDDC0>
add  :  <function add at 0x000002215FDBB430>

generate states: task_name: triple assigned state:5
generate states: task_name: multiply assigned state:6
DAG_map:
1  :   task: inc0, fanouts:[],fanins:[],faninsNB:['add'],collapse:[]fanin_sizes:[],faninNB_sizes:[2]
2  :   task: add, fanouts:[],fanins:['multiply'],faninsNB:[],collapse:[]fanin_sizes:[3],faninNB_sizes:[]
3  :   task: inc1, fanouts:['square', 'triple'],fanins:[],faninsNB:['add'],collapse:[]fanin_sizes:[],faninNB_sizes:[2]
4  :   task: square, fanouts:None,fanins:None,faninsNB:None,collapse:Nonefanin_sizes:None,faninNB_sizes:None
5  :   task: triple, fanouts:[],fanins:['multiply'],faninsNB:[],collapse:[]fanin_sizes:[3],faninNB_sizes:[]
6  :   task: multiply, fanouts:None,fanins:None,faninsNB:None,collapse:Nonefanin_sizes:None,faninNB_sizes:None

states:
inc0  :  1
add  :  2
inc1  :  3
square  :  4
triple  :  5
multiply  :  6

num_fanins:2 num_fanouts:2 num_faninNBs: num_collapse:0

all_fanout_task_names
square
triple

all_fanin_task_names
multiply

all_faninNB_task_names
add

all_collapse_task_names

leaf task start states
1
3

DAG_tasks:
inc0  :  <function inc0 at 0x000002215FDFDD30>
inc1  :  <function inc1 at 0x000002215FDFDDC0>
add  :  <function add at 0x000002215FDBB430>
triple  :  <function triple at 0x000002215FDFDC10>

generate states: task_name: square assigned state:4
generate states: task_name: multiply assigned state:6
DAG_map:
1  :   task: inc0, fanouts:[],fanins:[],faninsNB:['add'],collapse:[]fanin_sizes:[],faninNB_sizes:[2]
2  :   task: add, fanouts:[],fanins:['multiply'],faninsNB:[],collapse:[]fanin_sizes:[3],faninNB_sizes:[]
3  :   task: inc1, fanouts:['square', 'triple'],fanins:[],faninsNB:['add'],collapse:[]fanin_sizes:[],faninNB_sizes:[2]
4  :   task: square, fanouts:[],fanins:['multiply'],faninsNB:[],collapse:[]fanin_sizes:[3],faninNB_sizes:[]
5  :   task: triple, fanouts:[],fanins:['multiply'],faninsNB:[],collapse:[]fanin_sizes:[3],faninNB_sizes:[]
6  :   task: multiply, fanouts:None,fanins:None,faninsNB:None,collapse:Nonefanin_sizes:None,faninNB_sizes:None

states:
inc0  :  1
add  :  2
inc1  :  3
square  :  4
triple  :  5
multiply  :  6

num_fanins:3 num_fanouts:2 num_faninNBs: num_collapse:0

all_fanout_task_names
square
triple

all_fanin_task_names
multiply

all_faninNB_task_names
add

all_collapse_task_names

leaf task start states
1
3

DAG_tasks:
inc0  :  <function inc0 at 0x000002215FDFDD30>
inc1  :  <function inc1 at 0x000002215FDFDDC0>
add  :  <function add at 0x000002215FDBB430>
triple  :  <function triple at 0x000002215FDFDC10>
square  :  <function square at 0x000002215FDFDCA0>

generate states: task_name: multiply assigned state:6
generate states: task_name: divide assigned state:None
DAG_map:
1  :   task: inc0, fanouts:[],fanins:[],faninsNB:['add'],collapse:[]fanin_sizes:[],faninNB_sizes:[2]
2  :   task: add, fanouts:[],fanins:['multiply'],faninsNB:[],collapse:[]fanin_sizes:[3],faninNB_sizes:[]
3  :   task: inc1, fanouts:['square', 'triple'],fanins:[],faninsNB:['add'],collapse:[]fanin_sizes:[],faninNB_sizes:[2]
4  :   task: square, fanouts:[],fanins:['multiply'],faninsNB:[],collapse:[]fanin_sizes:[3],faninNB_sizes:[]
5  :   task: triple, fanouts:[],fanins:['multiply'],faninsNB:[],collapse:[]fanin_sizes:[3],faninNB_sizes:[]
6  :   task: multiply, fanouts:[],fanins:[],faninsNB:[],collapse:['divide']fanin_sizes:[],faninNB_sizes:[]
7  :   task: divide, fanouts:None,fanins:None,faninsNB:None,collapse:Nonefanin_sizes:None,faninNB_sizes:None

states:
inc0  :  1
add  :  2
inc1  :  3
square  :  4
triple  :  5
multiply  :  6
divide  :  7

num_fanins:3 num_fanouts:2 num_faninNBs: num_collapse:1

all_fanout_task_names
square
triple

all_fanin_task_names
multiply

all_faninNB_task_names
add

all_collapse_task_names
divide

leaf task start states
1
3

DAG_tasks:
inc0  :  <function inc0 at 0x000002215FDFDD30>
inc1  :  <function inc1 at 0x000002215FDFDDC0>
add  :  <function add at 0x000002215FDBB430>
triple  :  <function triple at 0x000002215FDFDC10>
square  :  <function square at 0x000002215FDFDCA0>
multiply  :  <function multiply at 0x000002215FDFDAF0>

generate states: task_name: divide assigned state:7
DAG_map:
1  :   task: inc0, fanouts:[],fanins:[],faninsNB:['add'],collapse:[]fanin_sizes:[],faninNB_sizes:[2]
2  :   task: add, fanouts:[],fanins:['multiply'],faninsNB:[],collapse:[]fanin_sizes:[3],faninNB_sizes:[]
3  :   task: inc1, fanouts:['square', 'triple'],fanins:[],faninsNB:['add'],collapse:[]fanin_sizes:[],faninNB_sizes:[2]
4  :   task: square, fanouts:[],fanins:['multiply'],faninsNB:[],collapse:[]fanin_sizes:[3],faninNB_sizes:[]
5  :   task: triple, fanouts:[],fanins:['multiply'],faninsNB:[],collapse:[]fanin_sizes:[3],faninNB_sizes:[]
6  :   task: multiply, fanouts:[],fanins:[],faninsNB:[],collapse:['divide']fanin_sizes:[],faninNB_sizes:[]
7  :   task: divide, fanouts:[],fanins:[],faninsNB:[],collapse:[]fanin_sizes:[],faninNB_sizes:[]

states:
inc0  :  1
add  :  2
inc1  :  3
square  :  4
triple  :  5
multiply  :  6
divide  :  7

num_fanins:3 num_fanouts:2 num_faninNBs: num_collapse:1

all_fanout_task_names
square
triple

all_fanin_task_names
multiply

all_faninNB_task_names
add

all_collapse_task_names
divide

leaf task start states
1
3

DAG_tasks:
inc0  :  <function inc0 at 0x000002215FDFDD30>
inc1  :  <function inc1 at 0x000002215FDFDDC0>
add  :  <function add at 0x000002215FDBB430>
triple  :  <function triple at 0x000002215FDFDC10>
square  :  <function square at 0x000002215FDFDCA0>
multiply  :  <function multiply at 0x000002215FDFDAF0>
divide  :  <function divide at 0x000002215FDFDB80>

"""





