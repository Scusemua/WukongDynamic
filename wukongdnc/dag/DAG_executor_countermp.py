import multiprocessing

# When using multiprocessing, the DAG_executor_driver
# creates a CounterMP object that is shared by the processes and is used to 
# count the number of tasks tha have been excuted.

class CounterMP(object):
    def __init__(self):
        self.val = multiprocessing.Value('i', 0)

    def increment_and_get(self, n=1):
        with self.val.get_lock():
            self.val.value += n
            return self.value

    def decrement_and_get(self, n=1):
        with self.val.get_lock():
            self.val.value -= n
            return self.value
        
    def get(self):
        with self.val.get_lock():
            return self.value

    @property
    def value(self):
        return self.val.value
