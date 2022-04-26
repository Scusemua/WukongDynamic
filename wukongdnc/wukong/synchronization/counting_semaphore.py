from imp import release_lock
from multiprocessing import Semaphore, RLock
import queue

class CountingSemaphore(object):
    def __init__(self, initial_permits = 0, id = -1, semaphore_name = "DEFAULT_NAME"):
        self.waiting_p = []
        self.__name = semaphore_name
        self.__id = id 
        self.__permits = initial_permits
        self.__mutex = RLock() # May not need to be recursive.

        if initial_permits < 0:
            raise ValueError("Initial value of CountingSemaphore must be >= 0.")
    
    def __generate_id(self):
        """
        This might eventually be used. For now, we pass the ID to the constructor.
        """
        pass 

    def get_name(self) -> str:
        return self.__name 
    
    def get_id(self) -> int:
        return self.__id 

    def P(self):
        # Each thread blocks on its own conditionVar object.
        # Need somewhere to wait, here we use Java's ability to wait() on any object.
        # Assuming Python does not do this, you will need a Python semaphore or whatever to wait on. 
        # See semaphore waitHere below
        queue_object = QueueObject() # queue_object is called 'o' in the Java code.

        print("Trying to lock counting semaphore now...")

        # Need a lock per each Semaphore, i.e., a "Lock thisLock" member of CountingSemaphore.
        # So thisLock.lock() instead of synchonized(this)
        self.__mutex.acquire() # Lock semaphore

        print("Counting semaphore " + str(self.__name) + " has been locked.")

        try:
            self.__permits -= 1
    
            if (self.__permits >= 0): # then no need to block thread
                return 
        except Exception as ex:
            print("[ERROR] Unexpected error occurred during `P()`: " + str(ex))
        finally:
            self.__mutex.release()

        # End synchronized (this) to avoid conditionVar.wait() while holding lock on this.

        wait_here = Semaphore(0)
        queue_object.wait_here = wait_here  # queue_object is called 'o' in the Java code.
        self.waiting_p.append(queue_object) # otherwise append blocked thread  

        try:
            print("Calling acquire() on wait_here")
            wait_here.acquire()
            print("Done acquiring 'wait_here' Semaphore")
        except Exception as ex:
            print("[ERROR] Exception encountered while acquiring Semaphore: " + str(ex))

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

        self.__mutex.acquire()

        try:
            self.__permits += 1

            if (len(self.waiting_p) > 0): # this should always be true since P()'s have blocked
                oldest_queue_object = self.waiting_p[0]
                
                # Remove the first element.
                if (len(self.waiting_p) > 1):
                    self.waiting_p = self.waiting_p[1:]
                else:
                    self.waiting_p = []
                
                # this is now oldest.waitHere.V();
                oldest_queue_object.release()
        except Exception as ex:
            print("[ERROR] Unexpected error occurred during `V()`: " + str(ex))
        finally:
            self.__mutex.release()
    
    def __do_p(self) -> bool:
        # called by VP() operation, decrements permits and returns
        # true if P() should block; false otherwise.
        self.__permits -= 1

        if (self.__permits >= 0):
            return False # no need to block thread

        return True 

    def __acquire_lock(self):
        """
        'Private' function to acquire the class' lock. 

        This exists because we don't have Synchronized in Python.
        """
        self.__mutex.acquire()
    
    def __release_lock(self):
        """
        'Private' function to release the class' lock. 

        This exists because we don't have Synchronized in Python.
        """        
        self.__mutex.release()
    
    def VP(self, v_semaphore):
        # execute {vSem.V(); this.P();} without any intervening P() or V() operations on this or vSem.	
        # lock semaphores in ascending order of IDs to prevent circular deadlock (i.e. T1 holds
        # this's lock and waits for vSem's lock while T2 holds vSem's lock and waits for this's lock.)

        first = self         # Type is CountingSemaphore, refers to 'this' instance.
        second = v_semaphore # Type is CountingSemaphore

		# Need somewhere to wait, here we use Java's ability to wait() on any object.
		# Assuming Python does not do this, you will need a Python semaphore or whatever to wait on. 
		# See semaphore waitHere below
        
        # queue_object is called 'o' in the Java code.
        queue_object = QueueObject() # each thread blocks on its own conditionVar object

        first.__acquire_lock()
        try:
            second.__acquire_lock()
            try:
				# this is a CountingSemaphore so assume no block
				# vSem.V() must not block
				# if (vSem instanceof binarySemaphore && vSem.permits == 1)
				#   throw new IllegalArgumentException("V() part of VP() operation will block. The V() part of VP() must not block.");

				# perform vSem.V()
				# Make sure this is a Lock, not a semaphore
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
            print("[ERROR] Exception encountered while acqu")

class QueueObject(object):
    def __init__(self):
        self.wait_here = Semaphore(0) 
    
    def release(self):
        self.wait_here.release()
    
    def acquire(self):
        self.wait_here.acquire()