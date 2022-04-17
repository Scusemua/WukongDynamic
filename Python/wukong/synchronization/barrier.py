from re import L
from monitor_su import MonitorSU, ConditionVariable

class Barrier(MonitorSU):
    def __init__(self, initial_n = 0, monitor_name = None):
        super(Barrier, self).__init__(monitor_name = monitor_name)
        self._n = initial_n
        self.convar = ConditionVariable(monitor = self, condition_name = "go")
    
    @property
    def n(self):
        return self._n 

    @n.setter
    def n(self, value):
        print("Setting value of n to " + str(value))
        self._n = value 

    def wait_b(self):
        print("Barrier.wait_b() being called. len(self.convar) = " + str(len(self.convar)) + ", self.n=" + str(self.n))
        super().enter_monitor(method_name = "wait_b")
        print("Entered monitor in wait_b()")

        if len(self.convar) < (self.n - 1):
        #if self.convar.__num_waiting_threads < (self.n - 1):
            print("Calling ConditionVariable.wait_c() from Barrier")
            self.convar.wait_c()
        
        self.convar.signal_c_and_exit_monitor()

class CyclicBarrier(object):
    def __init__(self):
        self.barrier = Barrier(monitor_name = "CyclicBarrierDefaultMonitor")
        self._n = 0

    def wait_b(self):
        self.barrier.wait_b()
    
    @property 
    def n(self):
        return self._n 
    
    @n.setter 
    def n(self, value):
        self._n = value 
        self.barrier.n = value 