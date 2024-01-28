import os
#from monitor_su import MonitorSU
from .selectivewait import selectiveWait
from .counting_semaphore import CountingSemaphore
from ..dag.DAG_executor_constants import exit_program_on_exception
from ..wukong.invoker import invoke_lambda

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
class Selector():
    def __init__(self, selector_name = None):
        #super(Selector, self).__init__(monitor_name=monitor_name)
        self._select = selectiveWait()
        self._entry_map = {}  # map entry_name to entry object
        # initialized by user calling appropriate set_restart_on_block/unblock/noblock method
        # If SQS then always restart
        self._restart_on_block = None 
        self._restart_on_unblock = None
        self._restart_on_noblock = None
        if selector_name == None:
            selector_name == ""
        self._mutex = CountingSemaphore(initial_permits = 1, semaphore_name = "Selector-" + str(selector_name) + "-mutex-CountingSemaphore") 

    def is_blocking(self,condition):
        # called by try_foo()
        return condition
        
    def lock(self):
        self._mutex.P()
        
    def unlock(self):
        self._mutex.V()

    def add_entry(self,entry):
        self._select.add_entry(entry) 
        # can get the name from entry and add [name,entry] to entry_map, if needed, or vice versa
        self._entry_map[entry._entry_name] = entry
        
    def get_entry(self,i):
        entry = self._select._entry_list[i]
        return entry
    
    def get_num_entries(self):
        return self._select.get_number_entries()

    def set_guards(self):
        raise ValueError("set_guards in parent class Selector should not be called.")

    # Could call [try-op; addArrival (no lock since it's private); execute]?
    # Q: Don't want to pass anything to choose(), or why not since user is not messing with
    # execute/choose in parent class.
          
#1: need state, and pass flag for try block = true or false
    def execute(self, entry_name, synchronizer, synchronizer_method, result_buffer, state, send_result, **kwargs):
        # state will be None if running the non-Lambda version, as state need not be saved in Arrival.
        # Using try_op/no-try-op protocol for acquiring/releasing lock

        #super().enter_monitor("execute")
            
        # guards could be using count (of arrivals) atribute so add arrrival first
        # index_of_entry_name, called_entry = index_of(entry_name,self._select)
        # Save arrival information that we need to process the entry call. The
        # sent value is in kwargs, the entry_name may be useful for debugging.
        # the result_buffer is used to return the return_value of the entry,
        # but we need the result_buffer after entry is processed. We also 
        # need to call the entry which means we need the synchronizer and
        # synchronizer_method. We have these things here, but in the while-loop
        # below we'll need to get the saved values from the arrival information,
        # i.e., if we choose() entry 1, get first arrival of this entry and 
        # grab alll the info we need. Don't remove entry until we are done
        # with it.
        # Arrivals are timestamped with global static
        called_entry = self._entry_map[entry_name]
#2: add state, so change entry add_arrival and Arrival
        called_entry.add_arrival(entry_name, synchronizer, synchronizer_method, result_buffer, state, **kwargs)
          
        num_entries = self.get_num_entries()
        # Debugging:
        for i in range(0, (num_entries-1)):
            entry = self.get_entry(i)
            logger.trace("execute: choosing among entries: entry " + str(i) + " is " + entry.get_entry_name() + ", number of arrivals: " + str(entry.get_num_arrivals()))

        #entry0 = self.get_entry(0)
        #logger.trace("after add: entry " + entry0.get_entry_name() + ": " + str(entry0.get_num_arrivals()))
        #entry1 = self.get_entry(1)
        #logger.trace("after add: entry " + entry1.get_entry_name() + ": " + str(entry1.get_num_arrivals()))

        self.set_guards()
            
#3: ckeck the send_result = true or false
        # number of arrivals is at least 1 since we just added one. Is this the only arrival?
        if called_entry.get_num_arrivals() > 1 or called_entry.testGuard() == False:
            try:
                msg = "[Error]: execute: send_result is True but entry will not be accepted."
                assert not send_result , msg
            except AssertionError:
                logger.exception("[Error]: assertion failed")
                if exit_program_on_exception:
                    logging.shutdown()
                    os._exit(0)
            # assertOld:
            #if send_result:
            #    # Note asynch calls always have send_result = False, whether they block or not.
            #    # try-op calls have send_result True if they are not blocking
            #    logger.error("[Error]: execute: send_result is True but entry will not be accepted.") 
            try:
                msg = "[Error]: execute: called_entry.testGuard() is True but this is not the first arrival." \
                    + " A previous arrival thus had a True guard and should have been selected earlier."
                assert not called_entry.get_num_arrivals() > 1 and called_entry.testGuard() , msg
            except AssertionError:
                logger.exception("[Error]: assertion failed")
                if exit_program_on_exception:
                    logging.shutdown()
                    os._exit(0)
            #assertOld:
            #if (called_entry.get_num_arrivals() > 1) and called_entry.testGuard():
            #    logger.error("[Error]: execute: called_entry.testGuard() is True but this is not the first arrival."
            #        + " A previous arrival thus had a True guard and should have been selected earlier.")
                
            # This is not the first wating arrival; guard cannot be true or the 
            # earlier arrivals would have been chosen on previous call to
            # execute and then processed.
            
            # ToDo: this is effectively wait(), so nothing to return now.
            # If this is fan-in then no need to wait since done with caller - they
            # did try-wait_b assume and got "Block" so they terminated and are done.
            # if not, then they did a no-try synch with no restart so we will be
            # sending back a value to indicate they were not last (False,Foo)
            
            #super().exit_monitor()
            
            # But to be like wait() we need to keep the serverthread blocked assuming
            # it's doing a synch and waiting for wait_b to complete. But need to exit
            # execute and exit_monitor so other callers can enter execute? So don't
            # do result_buffer.deposit and this will keep serverthread blocked. And 
            # since serverthread can do synchronizerThreadSelect join or not it is just 
            # eventually blocked on withdraw, and SynchronnizerThreadSelect can just die 
            # silently since we don't need it anymore.
            
            # return what? Part of delay alternative processing?
            logger.trace("execute returning: called_entry.get_num_arrivals(): " + str(called_entry.get_num_arrivals())
                + " called_entry.testGuard() == False: " + str(called_entry.testGuard() == False))
                             
##111: blocking call: blocking try-op: return nothing 0 which is ignored; asynch: must be client terminate: return nothing (0) 
##  which is ignored. (For async restart on block and restart on no-block are both true when client terminates.)
            # not setting any values in state; upon return to synchronize_lambda does state.blocking = True; state.return_value = None
            # as part of blocking try-op
            return 0
            
        else:
            return_value = self.domethodcall(entry_name, synchronizer, synchronizer_method, **kwargs)

            # We are sending state on restarts so need to set state for restart; 
            # for non-lambda we set state for restarts on return since that's where we were doing restarts.
            # for get_restart_on_noblock == False, we set state back at synchronize_sync. No set state
            # for async since never resturn value for async (unless it is an async after which lambda client
            # always terminates and then is restarted with return value.
                             
            state.restart = False                         
                        
#5: restart is always done by executor - so always return false to synchronizeLambda, but may or may not 
# do restart here, i.e., even if call did not block, need to do restart if restart_on_noblock and 
# return restart = false
            restart = called_entry.get_restart_on_noblock()
#222: For restart_on_noblock == True, must be async with client terminate. return_value is actual but
#  since async wait_for_result is False so result will be ignored. (That is, No result_buffer.withdraw 
# will be executed by synchronizeLAmbda since wait_for_result is always False for async calls.
            if restart:
#7: This is only the case when client always terminates, even when no block, and thus client needs restarting.
#   This case must be an asynch call, since if it were a try-op it would indicate no blocking and we would send 
#   the result to a client who must receive the result but would instead have terminated. 
                # Doing state changes here for restarts. For non-restart, we do state changes when we get back 
                # to synchronize_sync which called synchronizeLambda.
                state.restart = True 
                state.return_value = return_value
                state.blocking = False     
                self.doRestart(state, restart, return_value)
            # executor does all restarts and must deposit tuple with restart false. This deposited restart 
            # is not used by synchronizee_lambda, which never does a restart.
                             
            # For non-blocking try-op, wait_for_result is True so result_buffer.withdraw will be done. 
            # For non-blocking try-op, send_result is True so result_buffer.deposit
            # for asynch, wait_for_result == send_result == False - either client did not terminate assuming
            # non blocking call and meaningless result, or the client terminated and knows call will not block and 
            # has meaningless result, or the call may block and call may have meaningful result and cllient will get 
            # the result on restart.
            if send_result: # must be a non-blocking try-op (as asynch calls never wait_for_result)
                try:
                    msg = "selector_lambda: execute: send_result is True and restart is True, but restart can only " \
                        + "be True for aynch calls while send_result is always False for aynch calls."
                    assert not restart , msg
                except AssertionError:
                    logger.exception("[Error]: assertion failed")
                    if exit_program_on_exception:
                        logging.shutdown()
                        os._exit(0)
                #assertOld:
                #if restart:
                #    # Note asynch calls always have send_result = False, whether they block or not.
                #    # Also, only async calls can be restarted above, i.e., can have get_restart_on_noblock() is True.
                #    # So if restart is true then send_result must be False
                #    logger.error("execute: send_result is True and restart is True, but restart can only "
                #        + "be True for aynch calls while send_result is always False for aynch calls.")             
                restart = False
                return_tuple = (return_value, restart)
                # will use these values in synchronize_sync to make changes to state
                result_buffer.deposit(return_tuple)
                #called_entry.remove_first_arrival()
                logger.trace("execute called " + entry_name + ". returning " + str(return_value))
        
            # Note: not removing first arrival above in then-part when the call is not accepted; remove that arrival when call is accepted
            called_entry.remove_first_arrival()
                             
        logger.trace("execute: choosing")
        
        num_entries = self.get_num_entries()
        # Debugging:
        for i in range(0, (num_entries-1)):
            entry = self.get_entry(i)
            logger.trace("execute: choosing among entries: entry " + str(i) + " is " + entry.get_entry_name() + ", number of arrivals: " + str(entry.get_num_arrivals()))

        #entry0 = self.get_entry(0)
        #logger.trace("choosing: entry " + entry0.get_entry_name() + ": " + str(entry0.get_num_arrivals()))
        #entry1 = self.get_entry(1)
        #logger.trace("choosing: entry " + entry1.get_entry_name() + ": " + str(entry1.get_num_arrivals()))      
        
        while(True):
           # just added an Arrival for an entry so update guards (which may consider the number of arrivals or an entry)
           self.set_guards()
           choice = self._select.choose()
           if choice >= 1 and choice <= self._select.get_number_entries():
                logger.trace("Execute: choice is " + str(choice) + " so use list entry in position" + str(choice-1))
                called_entry = self.get_entry(choice-1)
                arrival = called_entry.getOldestArrival()
                synchronizer = arrival._synchronizer
                state = arrival._state
                synchronizer_method = arrival._synchronizer_method
                kwargs = arrival._kwargs
                result_buffer = arrival._result_buffer
                return_value = self.domethodcall(entry_name, synchronizer, synchronizer_method, **kwargs)
                logger.trace("Execute: called chosen method " + arrival._entry_name)
                             
                state.restart = False
                restart = called_entry.get_restart_on_unblock()
#8: 
#333: For restart_on_unblock == True, can be try-op that blocked or async with client terminate that blocked. return_value is actual but
# if async it should be ignored. If try-op that blocked, then send_result was false and "if wait_for_result: result_buffer.withdraw"
# means no result_buffer.withdraw will be executed. If asynch that blocked, the client must have terminated unconditionally. 
# We always do restart in this case, so restarting blocking try-op client and blocking async with cliennt termination. Now
# the former has send_result = False and the latter has send_result = False, so neither will do result_buffer.withdraw.

                if restart: 
                    state.restart = True
                    state.return_value = return_value
                    state.blocking = False 
                    self.doRestart(state,restart,return_value)
#9:
                else: #Error lambda clients must be restarted for this version in which sync objects are stored in client
                    logger.trace("execute: Blocking Lambdas must be restarted.")
#10: ToConsider: We could allow clients to wait - Lambda will send return value (via TCP) to tcp_server when the call
#    completes. Need to match return values to callers/operations.
                             
#10: Note: cannot check send_result here since here we are working on an entry call that was
#    made for a previous call to execute(). The call blocked and we checked send_result then.
#     And since the try determined the call would block, send_result was False. All async calls 
#    use send_result = false, so no client is waiting for a return and neither is the MessageHandler. 

# ToConsider: we could save the send_result value in the Arrival and check/assert here? send_result should be False.

                # Note: synchronizeSelect works for this case. That is, for asynch always terminate. If we 
                # allow terminate and set restart_on_no_block = True, and restart if restart_on_no_block it
                # works; this is because we can have a thread wait on result_buffer.withdraw for non-lambda objects
                # and always do deposit since deposited result will be ignored by asynch.

                called_entry.remove_first_arrival()
                break  # while-loop
           elif choice > self._select.get_number_entries()+1:
                # error
                logger.trace("Illegal choice in selective wait: " + choice + " number of entries" + self._select.get_number_entries())
                break # while-loop
           elif choice == -1: # else or delay
            # ToDo: on timeout we are just calling "delay" method so no choice of delay/else
                # if hasElse:
                    
                    # else part
                # if hasDelay:
                   
                    # delay part
                # no one called so no return to caller but may set timeout for delay
                break # while-loop
           elif choice == 0:
                #something like throw select_exception(...)
                break # while-loop
                
        #super().exit_monitor()
        logger.trace("execute: return 0")
        return 0
        
    # For example, call withdraw and return the value withdrawn.
    def domethodcall(self, entry_name, synchronizer, synchronizer_method, **kwargs):
        try:
           return_value = synchronizer_method(synchronizer, **kwargs)
        except Exception as x:
            logger.exception("Caught MonitorSelect domethodcall Error >>> %s" % x)
            raise ValueError("Error calling method " + entry_name + " in domethodcall of MonitorSelect")  
        return return_value
    
    def doRestart(self,state, restart, return_value):           
        logger.trace("synchronizer_lambda: synchronizeLamba: Restarting Lambda function %s." % state.function_name)
        payload = {"state": state}
#9: cannot hardcode name; use state.function_name, in general
        invoke_lambda(payload = payload, is_first_invocation = False, function_name = "ComposerServerlessSync")
       
    def set_restart_on_block(self,T_or_F):
        self._restart_on_block = T_or_F
       
    def get_restart_on_block(self):
        return self._restart_on_block
            
    def set_restart_on_unblock(self,T_or_F):
        self._restart_on_unblock = T_or_F
       
    def get_restart_on_unblock(self):
        return self._restart_on_unblock
            
    def set_restart_on_noblock(self,T_or_F):
        self._restart_on_noblock = T_or_F
   
    def get_restart_on_noblock(self):
        return self._restart_on_noblock     