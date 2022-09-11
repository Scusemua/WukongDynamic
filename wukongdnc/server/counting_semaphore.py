#from imp import release_lock
from threading import Semaphore, RLock

import logging 
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(asctime)s] [%(threadName)s] %(levelname)s: %(message)s')

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)

logger.addHandler(ch)

#Used in implementation of monitor class.
class CountingSemaphore(object):
    def __init__(self, initial_permits = 0, id = -1, semaphore_name = "DEFAULT_NAME"):
        self.waiting_p = []
        self._name = semaphore_name
        self._id = id 
        self._permits = initial_permits
        self._mutex = RLock() # May not need to be recursive.

        if initial_permits < 0:
            raise ValueError("Initial value of CountingSemaphore must be >= 0.")
    
    """
    def __generate_id(self):
        
        #This might eventually be used. For now, we pass the ID to the constructor.
        
        pass 
    """

    def get_name(self) -> str:
        return self._name 
    
    def get_id(self) -> int:
        return self._id 

    def P(self):
        # Each thread blocks on its own conditionVar object.
        # Need somewhere to wait, here we use Java's ability to wait() on any object.
        # Assuming Python does not do this, you will need a Python semaphore or whatever to wait on. 
        # See semaphore waitHere below
        queue_object = QueueObject() # queue_object is called 'o' in the Java code.

        #logger.debug("Trying to lock counting semaphore now...")

        # Need a lock per each Semaphore, i.e., a "Lock thisLock" member of CountingSemaphore.
        # So thisLock.lock() instead of synchonized(this)
        self._mutex.acquire() # Lock semaphore

        #logger.debug("Counting semaphore " + str(self._name) + " has been locked.")

        #try:
        self._permits -= 1
    
        #logger.debug("Counting semaphore: self._permits after decrement: " + str(self._permits))
        if (self._permits >= 0): # then no need to block thread
            #logger.debug("release and return")
            self._mutex.release()
            return 
        #except Exception as ex:
            #logger.debug("[ERROR] Unexpected error occurred during `P()`: " + str(ex))
        #finally:
        self._mutex.release()

        # End synchronized (this) to avoid conditionVar.wait() while holding lock on this.

        wait_here = Semaphore(0)
        queue_object.wait_here = wait_here  # queue_object is called 'o' in the Java code.
        self.waiting_p.append(queue_object) # otherwise append blocked thread  

        try:
            wait_here.acquire()
        except Exception as ex:
            logger.debug("[ERROR] Exception encountered while acquiring Semaphore: " + str(ex))

    def acquire(self):
        """
        Alias for the `P` function.
        """        
        self.P() 
    
    def down(self):
        """
        Alias for the `P` function.
        """
        self.P()
    
    def decrement(self):
        """
        Alias for the `P` function.
        """
        self.P()    

    def wait_S(self):
        """
        Alias for the `P` function.
        """
        self.P()
    
    def up(self):
        """
        Alias for the `V` function.
        """
        self.V()

    def increment(self):
        """
        Alias for the `V` function.
        """
        self.V()

    def signalS(self):
        """
        Alias for the `V` function.
        """
        self.V()                

    def release(self):
        """
        Alias for the `V` function.
        """
        self.V()

    def V(self):
		# Need a lock per each Semaphore, i.e., a "Lock thisLock" member of CountingSemaphore.
		# So thisLock.lock() instead of synchonized(this)

        self._mutex.acquire()

        try:
            self._permits += 1

            if (len(self.waiting_p) > 0): # this should always be true since P()'s have blocked
                oldest_queue_object = self.waiting_p[0]
                
                # Remove the first element.
                if (len(self.waiting_p) > 1):
                    self.waiting_p = self.waiting_p[1:]
                else:
                    self.waiting_p = [] # TODO: Probably don't need this, right? 
                
                # this is now oldest.waitHere.V();
                oldest_queue_object.release()
        except Exception as ex:
            logger.debug("[ERROR] Unexpected error occurred during `V()`: " + str(ex))
        finally:
            self._mutex.release()
    
    def __do_p(self) -> bool:
        # called by VP() operation, decrements permits and returns
        # true if P() should block; false otherwise.
        self._permits -= 1

        if (self._permits >= 0):
            return False # no need to block thread

        return True 

    def __acquire_lock(self):
        """
        'Private' function to acquire the class' lock. 

        This exists because we don't have Synchronized in Python.
        """
        self._mutex.acquire()
    
    def __release_lock(self):
        """
        'Private' function to release the class' lock. 

        This exists because we don't have Synchronized in Python.
        """        
        self._mutex.release()
    
    def VP(self, v_semaphore):
        # execute {vSem.V(); this.P();} without any intervening P() or V() operations on this or vSem.	

        first = self         # Type is CountingSemaphore, refers to 'this' instance.
        second = v_semaphore # Type is CountingSemaphore
        
        # queue_object is called 'o' in the Java code.
        queue_object = QueueObject() # each thread blocks on its own conditionVar object

        first.__acquire_lock()
        try:
            second.__acquire_lock()
            try:

				# perform vSem.V()
                v_semaphore.V() # it's okay that we already hold vSem's lock

                # perform this.P()
                blocking_P = self.__do_p()

                if not blocking_P:
                    return 
                
                # Note: The call to P() in this case should always block.
                wait_here = Semaphore(0)
                queue_object.wait_here = wait_here # queue_object is called 'o' in the Java code.

                # queue_object is called 'o' in the Java code.
                self.waiting_p.append(queue_object) # append blocked thread
            finally:
                second.__release_lock()
        finally:
            first.__release_lock()
        
        try:
            queue_object.wait_here.acquire()
        except Exception as ex:
            logger.debug("[ERROR] Exception encountered while acquiring queue_object.wait_here: " + str(ex))

class QueueObject(object):
    def __init__(self):
        self.wait_here = Semaphore(0) 
    
    def release(self):
        self.wait_here.release()
    
    def acquire(self):
        self.wait_here.acquire()
