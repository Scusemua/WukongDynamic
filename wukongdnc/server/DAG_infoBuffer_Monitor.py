from .monitor_su import MonitorSU
#from monitor_su import MonitorSU
import _thread
import time
import copy
import os

from ..dag import DAG_executor_constants
import logging 

logger = logging.getLogger(__name__)
"""
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(asctime)s] [%(module)s] [%(processName)s] [%(threadName)s]: %(message)s')
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)
logger.addHandler(ch)
"""

# Controls workers when using incremental DAG generation
class DAG_infoBuffer_Monitor(MonitorSU):
    def __init__(self, monitor_name = "DAG_infoBuffer_Monitor"):
        super(DAG_infoBuffer_Monitor, self).__init__(monitor_name=monitor_name)
        # For testing, if we havn't called init() then version number will be 1
        self.current_version_DAG_info = None
        self.current_version_number_DAG_info = 1
#brc leaf tasks
        # The initial DAG has the initial leaf task(s) in it. As later we find
        # more leaf tasks (tht start new connected components), we supply them 
        # with the DAG so the leaf tasks can be aded to the work_queue and
        # and executed by workers (when we are using workers).
        self.current_version_new_leaf_tasks = []
        self._next_version=super().get_condition_variable(condition_name="_next_version")
#brc: same version
        # set to the is_0complete mmber of the current DAG
        self.current_version_DAG_info_is_complete = False
        # workers request new DAGs in "rounds". In each round the 
        # workers request a new DAG and none can receive a new
        # DAG until all workers have made a request. Getting
        # new DAGs ends the round.
        self.num_waiting_workers = 0
        # We use this to assert the the version numbers requested by the workers
        # in a round are the same.
        self.requested_version_number_in_this_round = -1
#brc: deallocate DAG
        self.max_deallocation_number = 0

    #def init(self, **kwargs):
    def init(self,**kwargs):
        pass
        # initialize with a DAG_info object. This will be version 1 of the DAG
        #self.current_version_DAG_info = kwargs['current_version_DAG_info']
        #self.current_version_number_DAG_info = self.current_version_DAG_info.get_version_number()
#brc leaf tasks
        # The initial DAG has the initial leaf task(s) in it. As later we find
        # more leaf tasks (tht start new connected components), we supply them 
        # with the DAG so the leaf tasks can be aded to the work_queue and
        # and executed by workers (when we aer using workers).
        #self.current_version_new_leaf_tasks = []

        # logger.trace(kwargs)

    def print_DAG_info(self,DAG_info):
        DAG_map = DAG_info.get_DAG_map()
        #all_fanin_task_names = DAG_info.get_all_fanin_task_names()
        #all_fanin_sizes = DAG_info.get_all_fanin_sizes()
        #all_faninNB_task_names = DAG_info.get_all_faninNB_task_names()
        #all_faninNB_sizes = DAG_info.get_all_faninNB_sizes()
        #all_fanout_task_names = DAG_info.get_all_fanout_task_names()
        #all_collapse_task_nams = DAG_info.get_all_collapse_task_names()
        # Note: all fanout_sizes is not needed since fanouts are fanins that have size 1
        DAG_states = DAG_info.get_DAG_states()
        DAG_leaf_tasks = DAG_info.get_DAG_leaf_tasks()
        DAG_leaf_task_start_states = DAG_info.get_DAG_leaf_task_start_states()
        DAG_tasks = DAG_info.get_DAG_tasks()
        DAG_leaf_task_inputs = DAG_info.get_DAG_leaf_task_inputs()

        print("DAG_infoBuffer_Monitor: DAG_map:")
        for key, value in DAG_map.items():
            print(key)
            print(value)
        print("  ")
        print("DAG_infoBuffer_Monitor: DAG states:")         
        for key, value in DAG_states.items():
            print(key)
            print(value)
        print("   ")
        print("DAG_infoBuffer_Monitor: DAG leaf task start states")
        for start_state in DAG_leaf_task_start_states:
            print(start_state)
        print()
        print("DAG_infoBuffer_Monitor: DAG_tasks:")
        for key, value in DAG_tasks.items():
            print(key, ' : ', value)
        print()
        print("DAG_infoBuffer_Monitor: DAG_leaf_tasks:")
        for task_name in DAG_leaf_tasks:
            print(task_name)
        print() 
        print("DAG_infoBuffer_Monitor: DAG_leaf_task_inputs:")
        for inp in DAG_leaf_task_inputs:
            print(inp)
        #print() 
        print()
        print("DAG_infoBuffer_Monitor: DAG_version_number:")
        print(DAG_info.get_DAG_version_number())
        print("DAG_infoBuffer_Monitor: DAG_info_is_complete:")
        print(DAG_info.get_DAG_info_is_complete())
        print()

    def get_current_version_number_DAG_info(self):
        try:
            super(DAG_infoBuffer_Monitor, self).enter_monitor(method_name="get_current_version_number_DAG_info")
        except Exception as ex:
            logger.exception("[ERROR]: DAG_infoBuffer_Monitor:  Failed super(DAG_infoBuffer_Monitor, self)")
            logger.exception("[ERROR] self: " + str(self.__class__.__name__))
            logger.trace(ex)
            return 0
        
        logger.trace("DAG_infoBuffer_Monitor: get_current_version_number_DAG_info() entered monitor, len(self._new_version) ="+str(len(self._next_version)))

        restart = False
        current_DAG_info = self.current_version_DAG_info
        super().exit_monitor()
        return current_DAG_info, restart

    def deposit(self,**kwargs):
        # deposit a new DAG_info object. It's version number will be one more
        # than the current DAG_info object.
        # Workers that finish their current version i of the DAG will wait for the next 
        # version i+1. Note: a worker may be finishing an earlier version of 
        # the DAG. When they request a new version, it may be the current
        # version (being deposited) or an older version that they are requesting, We will give
        # them the current version, which may be newer than the version they
        # requsted. This is fine. We assume tha the DAG grows incrementally
        # and we add new states but do not delete old states from the DAG.
        # Note: all the synchronization is to ensure that all workers are
        # using the same version of the DAG - no worker can get the next
        # version of the DAG until all workers have requested a new version,
        # except when the DAG is complete in which case we do not require all 
        # the workers to have requested a new version (using withdraw) since
        # they are guaranted to all get the last version (as there aer no more DAGs 
        # deposited after that.)
        logger.info("DAG_infoBuffer_Monitor: deposit() try to enter monitor, len(self._new_version) ="+str(len(self._next_version)))
        try:
            super(DAG_infoBuffer_Monitor, self).enter_monitor(method_name="deposit")
        except Exception as ex:
            logger.exception("[ERROR]: DAG_infoBuffer_Monitor: Failed super(DAG_infoBuffer_Monitor, self)")
            logger.exception("[ERROR] self: " + str(self.__class__.__name__))
            logger.trace(ex)
            return 0

        logger.info("DAG_infoBuffer_Monitor: deposit() entered monitor, len(self._new_version) ="+str(len(self._next_version)))
        self.current_version_DAG_info = kwargs['new_current_version_DAG_info']
        self.current_version_number_DAG_info = self.current_version_DAG_info.get_DAG_version_number()
#brc: same version
        self.current_version_DAG_info_is_complete = self.current_version_DAG_info.get_DAG_info_is_complete()

#brc leaf tasks
        new_leaf_tasks = kwargs['new_current_version_new_leaf_tasks']
        self.current_version_new_leaf_tasks += new_leaf_tasks
        logger.info("DAG_infoBuffer_Monitor: DAG_info deposited: ")
        self.print_DAG_info(self.current_version_DAG_info)

#brc leaf tasks
        logger.trace("DAG_infoBuffer_Monitor: new leaf task states deposited: ")
        for work_tuple in new_leaf_tasks:
            leaf_task_state = work_tuple[0]
            logger.trace(str(leaf_task_state))
        logger.trace("DAG_infoBuffer_Monitor: cumulative leaf task states deposited: ")
        for work_tuple in self.current_version_new_leaf_tasks:
            leaf_task_state = work_tuple[0]
            logger.trace(str(leaf_task_state))

        restart = False
#brc: same version
        # if all workers are waiting or this is the last DAG then wake them up.
        # Note: If not all workers are waiting then there is no signal below to workers. 
        # The last worker may enter withdraw in which case this workers starts
        # a cascaded wakeup of the NUM_WORKERS-1 other workers.
        # Note: If this is the last DAG then we will wakeup as many workers
        # as are waiting; if not all of the workers are waiting, then the 
        # waiting workers will wakeup and receive the last DAG and workers
        # that call withdraw() later will get this same last DAG - so all workers
        # will get the same (last) DAG.
        if self.num_waiting_workers == DAG_executor_constants.NUM_WORKERS:
            # Note: If the last version of the DAG deposited is a complete DAG,
            # then we might want to add "or self.current_version_DAG_info_is_complete"
            # to the if-condition, since we know that all the workers will get this
            # last DAG next, as by definition of "last DAG" there are no other
            # DAGs they can get. This would allow workers to return "early" with 
            # the last DAG, and start excuting it before other workers have 
            # called to get the last DAG. Allowing workers to work on the penultimate
            # DAG while workers start early on the last DAG might cause problems
            # though since some tasks in the last DAG will not be in the penultimte
            # (next to last) DAG.
            # Note: No worker can get reenter the monitor for the next round until all
            # the waiting workers in this round have left the monitor.
            # Reset for the next round. Note that if this is the last DAG
            # there is no next round.
            self.requested_version_number_in_this_round = -1
            logger.info("DAG_infoBuffer_Monitor: deposit() signal waiting writers:"
                + " self.requested_version_number_in_this_round: " 
                + str(self.requested_version_number_in_this_round))
            self._next_version.signal_c_and_exit_monitor()
        else:
            super().exit_monitor()
        return 0, restart

    def withdraw(self, **kwargs):
        # request a new version of the DAG. A worker that finishes version 
        # i will request i+1. Noet that i+1 may <= current_version. If so
        # return the current version. If not, then the worker is requesting
        # the next version of the DAG, which hasn't been generated yet.
        super().enter_monitor(method_name = "withdraw")
        requested_current_version_number = kwargs['requested_current_version_number']
        logger.info("DAG_infoBuffer_Monitor: withdraw() entered monitor, requested_current_version_number = "
            + str(requested_current_version_number) + " len(self._next_version) = " + str(len(self._next_version))
            + " self.requested_version_number_in_this_round:" 
            + str(self.requested_version_number_in_this_round))
        # asssert that the worker is requesting the same version number as the 
        # other workers, in this round.
        if self.requested_version_number_in_this_round == -1:
            self.requested_version_number_in_this_round = requested_current_version_number
        else:
            try:
                msg = "[Error]: DAG_infoBuffer_Monitor.withdraw:" \
                    + " requested_current_version_number is not the same as the" \
                    + " previously requested version number(s) " \
                    + " requested_current_version_number: " \
                    + str(requested_current_version_number) \
                    + " self.requested_version_number_in_this_round: " \
                    + str(self.requested_version_number_in_this_round)
                assert self.requested_version_number_in_this_round == requested_current_version_number , msg
            except AssertionError:
                logger.exception("[Error]: assertion failed")
                if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
                    logging.shutdown()
                    os._exit(0)   
        DAG_info = None
        restart = False
#brc: same version
            # Return with the new DAG if (the requested version number is less than 
            # the current version number and this is the last worker to call withdraw
            # for this round) or the DAG is complete. Note: if the DAG is complete then 
            # requested_current_version_number <= self.current_version_number_DAG_info is true
            # since we must have deposited a new DAG for the DAG to become complete - workers
            # will not request a new version of the DAG if their current version is complete.
            # There is no need to block a worker if the DAG is complete since this is the 
            # last DAG to be generated so all workers will get this last DAG on this 
            # current and last round.
            # Note: all workers should request the same version number; they may receive a 
            # newer version than they requested.
        #if requested_current_version_number <= self.current_version_number_DAG_info:
        if (requested_current_version_number <= self.current_version_number_DAG_info \
            and self.num_waiting_workers == DAG_executor_constants.NUM_WORKERS - 1):
            # see the note in deposit() about making this condition "if (...) or self.current_version_DAG_info_is_complete"
            try:
                msg = "[Error]: DAG_infoBuffer_Monitor: withdraw:" \
                    + " DAG is complete but request version number is not <= current version number." \
                    + " requested_current_version_number: " + str(requested_current_version_number) \
                    + " self.current_version_number_DAG_info: " + str(self.current_version_number_DAG_info)
                assert not (self.current_version_DAG_info_is_complete and requested_current_version_number > self.current_version_number_DAG_info) , msg
            except AssertionError:
                logger.exception("[Error]: assertion failed")
                if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
                    logging.shutdown()
                    os._exit(0)

            # Round is over so reset self.requested_version_number_in_this_round
            # Note: self.requested_version_number_in_this_round will have been reset
            # before the next round, if any, starts.
            # Note: No worker can reenter the monitor for the next round until all
            # the waiting workers in this round have left the monitor.
            self.requested_version_number_in_this_round = -1

            DAG_info = self.current_version_DAG_info
#brc leaf tasks
            new_leaf_task_states = copy.copy(self.current_version_new_leaf_tasks)
#brc: Issue: Add if. Also, above works if there is only 1 worker?
            if self.num_waiting_workers == 0:
                self.current_version_new_leaf_tasks.clear()

            logger.trace("DAG_infoBuffer_Monitor: withdraw: got DAG_info with version number " 
                + str(DAG_info.get_DAG_version_number()))
             
            # Note: This print_DAG_info() is disabled so that we do not in print_DAG_info try 
            # to iterate over the dictionaries in current_version_DAG_info while
            # the DAG generator is changing these dictionaries after
            # having deposited current_version_DAG_info. If we iterate
            # while the dictionary is being changed we can get a 
            # RUNTIME error saying the sixe of the dictionary changed
            # during iteration. We do print the version number of 
            # current_version_DAG_info so we can match the 
            # current_version_DAG_info with the versions printed by
            # deposit.
            #logger.trace("DAG_infoBuffer_Monitor: DAG_info withdrawn: ")
            #self.print_DAG_info(self.current_version_DAG_info)

#brc leaf tasks
            logger.trace("DAG_infoBuffer_Monitor: withdraw: new leaf task states to be returned: ")
            for work_tuple in new_leaf_task_states:
                leaf_task_state = work_tuple[0]
                logger.trace(str(leaf_task_state))
#brc same version
            # wake up the other (if any) waiting writers - we may be using 
            # only one writer so there may not be any other waiting writers
            # in which case the signal has no effect.
            # Note: this is a cascaded wakeup - the first worker wakes up the 
            # second, wakes up the third etc, and a new deposit cannot be made 
            # until all waiting workers have been signaled and left the monitor.
            # Note: self.requested_version_number_in_this_round was reset above
            self._next_version.signal_c_and_exit_monitor()
            #super().exit_monitor()
#brc leaf tasks
            DAG_info_and_new_leaf_task_states_tuple = (DAG_info,new_leaf_task_states)
            #return DAG_info, new_leaf_task_states, restart
            return DAG_info_and_new_leaf_task_states_tuple, restart
        else:
#brc: same version
            self.num_waiting_workers += 1
            logger.info("DAG_infoBuffer_Monitor: withdraw waiting for version " + str(requested_current_version_number)
                + " with " + str(self.num_waiting_workers) + " workers now waiting.")
            self._next_version.wait_c()
#brc: same version
            self.num_waiting_workers -= 1
            DAG_info = self.current_version_DAG_info
#brc leaf tasks
            new_leaf_task_states = copy.copy(self.current_version_new_leaf_tasks)
#brc: Issue: Add if. Also, above works if there is only 1 worker?
            if self.num_waiting_workers == 0:
                self.current_version_new_leaf_tasks.clear()
            # cascaded wakeup, i.e., if there are more than one worker waiting,
            # the deposit() will wakeup the first worker with its
            # signal_c_and_exit_monitor(). The first waitng worker will wakeup
            # the second worker here with signal_c_and_exit_monitor(); the 
            # second worker will wakeup the third worker with signal_c_and_exit_monitor()
            # etc. Note that all calls to signal are using signal_c_and_exit_monitor()
            # So no worker that signals another worker will wait to reenter the monitor;
            # instead, the signalling worker will just return. The last waiting worker will 
            # call signal_c_and_exit_monitor() and since no workers are waiting on 
            # the condition or to reenter the monitor, this will have no effect 
            # other than to release mutual exclusion. Note: Any calls to deposit()
            # for verson i+1 after a previous call to deposit() for version i
            # will have to wait for all the workers who were waiting for version i 
            # when the first deposit() occurred to return with version i, before the 
            # call to deposit with version i+1 can start. These workers will just
            # make a later call to withdraw to get version i+1. (Newer versions may be 
            # deposited before the workers try to get version i+1 - workers get the 
            # latest version.)
            #
            # Note: We could use an SC monitor in which case the second deposit
            # might be allowed to enter the monitor before waiting workers, in 
            # which case the workers would get verson i+1, which is not bad.
            # Note: However: The current design requires all workers to be using
            # the same version number.

            logger.trace("DAG_infoBuffer_Monitor: withdraw: got DAG_info with version number " 
                + str(DAG_info.get_DAG_version_number()))
            #logger.trace("DAG_infoBuffer_Monitor: DAG_info withdrawn: ")
            # Disabled to avoid concurent access (see comment above)
            #self.print_DAG_info(self.current_version_DAG_info)
#brc leaf tasks
            logger.trace("DAG_infoBuffer_Monitor: withdraw: new leaf task states to return: ")
            for work_tuple in new_leaf_task_states:
                leaf_task_state = work_tuple[0]
                logger.trace(str(leaf_task_state))

#brc: same version
            self._next_version.signal_c_and_exit_monitor()
#brc leaf tasks
            DAG_info_and_new_leaf_task_states_tuple = (DAG_info,new_leaf_task_states)
            #return DAG_info, new_leaf_task_states, restart
            logger.trace("DAG_infoBuffer_Monitor: return.")
            return DAG_info_and_new_leaf_task_states_tuple, restart
        


class Dummy_DAG_info:
    def __init__(self,value,version_number):
        self.value = value
        self.version_number = version_number

    def get_value(self):
        return self.value
    def get_version_number(self):
        return self.version_number

#Local tests
def taskD(b : DAG_infoBuffer_Monitor):
    time.sleep(3)
    DAG_info = Dummy_DAG_info("DAG_info2",2)
    keyword_arguments = {}
    keyword_arguments['new_current_version_DAG_info'] = DAG_info
    logger.trace("taskD Calling withdraw")
    b.deposit(**keyword_arguments)
    logger.trace("Successfully called deposit version 2")

def taskW1(b : DAG_infoBuffer_Monitor):
    logger.trace("taskW1 Calling withdraw")
    keyword_arguments = {}
    keyword_arguments['requested_current_version_number'] = 1
    DAG_info, restart = b.withdraw(**keyword_arguments)
    logger.trace("Successfully called withdraw, ret is " 
        + str(DAG_info.get_value()) + "," + str(DAG_info.get_version_number())
        + " restart " + str(restart))

def taskW2(b : DAG_infoBuffer_Monitor):
    logger.trace("taskW2 Calling withdraw")
    keyword_arguments = {}
    keyword_arguments['requested_current_version_number'] = 2
    DAG_info, restart = b.withdraw(**keyword_arguments)
    logger.trace("Successfully called withdraw, ret is " 
        + str(DAG_info.get_value()) + "," + str(DAG_info.get_version_number())
        + " restart " + str(restart))

def main(): 
    b = DAG_infoBuffer_Monitor(monitor_name="DAG_infoBuffer_Monitor")
    DAG_info = Dummy_DAG_info("DAG_info1",1)
    keyword_arguments = {}
    keyword_arguments['current_version_DAG_info'] = DAG_info
    b.init(**keyword_arguments)
    try:
        logger.trace("Starting D thread")
        _thread.start_new_thread(taskD, (b,))
    except Exception as ex:
        logger.exception("[ERROR] Failed to start first thread.")
        logger.exception(ex)

    try:
        logger.trace("Starting taskW1 thread")
        _thread.start_new_thread(taskW1, (b,))
    except Exception as ex:
        logger.exception("[ERROR] Failed to start taskW1 thread.")
        logger.exception(ex)

    try:
        logger.trace("Starting first taskW2 thread")
        _thread.start_new_thread(taskW2, (b,))
    except Exception as ex:
        logger.exception("[ERROR] Failed to start first taskW2 thread.")
        logger.exception(ex)

    try:
        logger.trace("Starting second taskW2 thread")
        _thread.start_new_thread(taskW2, (b,))
    except Exception as ex:
        logger.exception("[ERROR] Failed to start second taskW2 thread.")
        logger.exception(ex)

    time.sleep(4)
    logger.trace("Done sleeping")

if __name__=="__main__":
     main()
