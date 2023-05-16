import sys

import logging 
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(asctime)s] [%(module)s] [%(processName)s] [%(threadName)s]: %(message)s')

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)

logger.addHandler(ch)

class selectiveWait:

    def __init__(self):
        self._entry_list = []
        self._delay = None
        self._else = None
        self._hasElse = False
        self._hasDelay = False

    class delayAlternative:

        def __init__(self,msecDelay):
            self._msecDelay = msecDelay
            self._guard = True

        def getMsecDelay(self):
            return self._msecDelay

        def guard(self,g):
            self._guard = g

        def testGuard(self):
            return self._guard

        def accept(self):
            pass
    

    class elseAlternative:
        def accept(self):
            pass

    def add_entry(self,entry):
        self._entry_list.append(entry)
    
    def get_entry(self,i):
        return self._entry_list[i]
		
    def get_number_entries(self):
        return len(self._entry_list)

    def add_delay(self,delay):
        self._delay = delay
        self._hasDelay = True
       
        if self.hasDelay:
            print("Warning: A selectiveWait cannot have more than one delay alternative.")
            print("Warning: Only one of the delay alternatives can be selected.")
            
        if self.hasElse: 
            print("Warning: A selectiveWait cannot have an else alternative and a delay alternative.")
            print("Warning: Only one of the else/delay alternatives can be selected.")
    
    def add_else(self,elsealt):
        self._else_ = elsealt
        self._hasElse = True
       
        if self.hasElse:
            print("Warning: A selectiveWait cannot have more than one else alternative.")
            print("Warning: Only one of the else alternatives can be selected.")
            
        if self.hasDelay:
            print("Warning: A selectiveWait cannot have a delay alternative and an else alternative.")
            print("Warning: Only one of the else/delay alternatives can be selected.")

    def remove_else(self):
        self._hasElse = False
        self._else = None

    def remove_delay(self):
        self._hasDelay = False
        self._delay = None

    def clear_all_open(self):
        for entry in self._entry_list:
            entry.clear_open()

    def tryOpenAllWithTrueGuard(self):
        for entry in self._entry_list:
            if (entry.testGuard()):
                entry.setOpen()

    def test_all(self):
        oldest_timestamp = sys.maxsize    # oldest_timestamp is the smallest value since timestamps are in ascending order of arrival
        i = 0
        j = 1
        AtLeastOneTrueGuard = False
        for entry in self._entry_list:
            AtLeastOneTrueGuard = AtLeastOneTrueGuard or entry.testGuard()
            if entry.testReady() and entry.testGuard():
                oldestArrival = entry.getOldestArrival()
                if oldestArrival._timestamp < oldest_timestamp:
                    i = j
                    oldest_timestamp = oldestArrival._timestamp
            j += 1
        if i == 0 and not AtLeastOneTrueGuard:
            # you could have open guard(s) but none ready so AtLeastOneTrueGuard and i = 0
            # you could have no open guard(s) in which case !AtLeastOneTrueGuard and i = 0
            return -1 # all guards are False
        else:
            # return i if it has open guard and an arrival and it is next in FCFS order
            return i 

    def choose(self):
        ready_index = 0
        ready_index=self.test_all()
        if (ready_index<=0): # not ready
            if self._hasElse:
                return len(self._entry_list)+1
        self.tryOpenAllWithTrueGuard()  # set the guards earlier, no set open if guard is True same for delay's guard
        if self._hasDelay and self._delay.testGuard():
            XXX = True
            #t.start()?
            # no waits, though have timeouts on  sem.acquire()
         # # long startTime = System.currentTimeMillis()
          # waitTime = delay.getMsecDelay()
         # wait(waitTime) # this was synched
            # ready_index=testAll()
         # if ready_index <= 0 # must be timeout here, since no unrelated notifications.
            #     ready_index = len(entry_list)+1
                
        else: # no else or open delay and readyIndex <=0
            if ready_index == -1:          # all accept alternatives have False guards and the delay
                return -1                        # alternative is closed or no else so throw
                # No waiting now
		# print("selective wait starts to wait().") # waiting for an arrival (no delay/else and some guards for entries True
		# wait()
		# print("selective wait awakens.")
		# ready_index=testAll() ## see what happene
        self.clear_all_open()  # set all open to False will set guards next time and set open if True guard
        return ready_index
  
  
# def callDelay():
#   race here: could set flag when start timer and see if timer or other thread gets into execute first.
#   if other thread then don't do timer. count number of execute since startinggc timer. If timer thread
#   finds count>00 then no timeout reset flag and count. Also cancel timer at start of execute.
#   super().enter_monitor("callDelay")
#   call "delay"
#   super().exit_monitor()

# t=threading.Timer(delay.getMsecDelay(),callDelay)
# where we do the delay logic: 
#   t.start()
#  Then t.cancell the timer if we get a call, i.e., at start of execute()
#  where cancel has no effect if timer not waiting
