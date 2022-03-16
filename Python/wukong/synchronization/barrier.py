from re import L
from monitor_su import MonitorSU, ConditionVariable

class Barrier(MonitorSU):
    def __init__(self, initial_n = 0, monitor_name = None):
        MonitorSU.__init__(self, monitor_name = monitor_name)
        self._n = initial_n
        self.convar = ConditionVariable(self, condition_name = "go")
    
    @property
    def n(self):
        return self._n 

    @n.setter
    def n(self, value):
        print("Setting value of n to " + str(value))
        self._n = value 

    def wait_b(self):
        self.enter_monitor(method_name = "wait_b")

        if len(self.convar) < self.n - 1:
            self.convar.wait_c()
        
        self.convar.signal_c_and_exit_monitor()