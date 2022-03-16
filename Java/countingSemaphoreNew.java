import java.util.*;


/*
 * An elementsof the queue of blocked threads in a CountingSemaphore
 */
public final class queueObject {
   PythonSemaphore waitHere
}

public final class countingSemaphore {
// a FIFO counting semaphore

	private ArrayList waitingP = new ArrayList(); // queue of threads blocked on P
	
	// ID and Name are for debugging only
	private String semaphoreName = null;
	private int ID;
	
	public int getID() {return ID;}
	public String getName() {}
	private int generateID() {
		// this could be a locked static method that incements a static counter
	
	}
	
	public countingSemaphore(int initialPermits) {
		if (initialPermits < 0)
			throw new IllegalArgumentException("initial value of countingSemaphore must be >= 0");
		this.ID = generateID();
	}
	
	public countingSemaphore(int initialPermits, String semaphoreName) {
		if (initialPermits < 0)
			throw new IllegalArgumentException("initial value of countingSemaphore must be >= 0");
		this.semaphoreName = semaphoreName;
		this.ID = generateID();
	}
	
	// yuo can do acquire/release alises instead of up/down
	public void acquire() {P();} 	// acquire - relese
	public void down() {P();} 		// up - down
	public void decrement() {P();}	// increment - decrement
	public void waitS() {P();} 		// wait() is final in class Object and cannot be overridden
	public void P() 
	{
		queueObject o = new queueObject(); // each thread blocks on its own conditionVar object
		// Need somewhere to wait, here we use Java's ability to wait() on any object.
		// Assuming Python does not do this, you will need a Python semaphore or whatever to wait on. 
		// See semaphore waitHere below
		
		//The synchronized(o) block is no longer needed. - it was required for o.wait(), now we use waitHere.acquire()
		synchronized (o) {
			// Need a lock per each Semaphore, i.e., a "Lock thisLock" member of CountingSemaphore.
			// So thisLock.lock() instead of synchonized(this)
			synchronized (this) { // lock semaphore
				permits--;
				if (permits>=0)  { // then no need to block thread
					PythonSemapore waitHere = new PythonSemaphore(0);
					o.semaphore = waitHere;
					waitingP.add(o); // otherwise append blocked thread
			// This becomes thisLock.unlock()
			} // end synchronized (this) to avoid conditionVar.wait() while holding lock on this
			
			// using waitHere.acquire() instead of o.wait()
			try {o.wait();} catch (InterruptedException ex) {
		// remove
		} // end synchronized (o)
	} // end P

	public void release() {V();}	// acquire - release
	public void up() {V();}			// up - down
	public void increment() {V();}	// increment - decrement
	public void signalS() {V();}	// waitS - signalS
	public void V() {
		// Need a lock per each Semaphore, i.e., a "Lock thisLock" member of CountingSemaphore.
		// So thisLock.lock() instead of synchonized(this)
		synchronized (this) { // lock semaphore
			permits++;
			if (waitingP.size() > 0) { // this should always be true since P()'s have blocked
				queueObject oldest = (queueObject) waitingP.get(0);
				waitingP.remove(0);
				// this is now oldest.waitHere.V();
				synchronized(oldest) {
					oldest.notify();
				}
			// So thisLock.unlock()
			}  // end synchronized this
		}	
	} // end
	
	private boolean doP() {
	// called by VP() operation, decrements permits and returns
   // true if P() should block; false otherwise.
		permits--;
		if (permits >= 0)
			return false;	// no need to block thread
		else
			return true;
	}

	public int getSemaphoreID() { return semaphoreID;}

	public void VP(semaphore vSem) {
	// execute {vSem.V(); this.P();} without any intervening P() or V() operations on this or vSem.	

		// lock semaphores in ascending order of IDs to prevent circular deadlock (i.e. T1 holds
		// this's lock and waits for vSem's lock while T2 holds vSem's lock and waits for this's lock.)
		semaphore first = this;
 		semaphore second = vSem;
		//if (this.getSemaphoreID() > vSem.getSemaphoreID()) { 
		//	first = vSem;
		//	second = this;
		//}
		queueObject o = new queueObject(); // each thread blocks on its own conditionVar object
		// Need somewhere to wait, here we use Java's ability to wait() on any object.
		// Assuming Python does not do this, you will need a Python semaphore or whatever to wait on. 
		// See semaphore waitHere below

		//The synchronized(o) block is no longer needed. - it was required for o.wait(), now we use waitHere.acquire()
		synchronized(o) {
			// Now first.thisLock.lock()
			synchronized(first) { 
				// Now second.thisLock.lock()
				synchronized(second) { 
				
					// this is a CountingSemaphore so assume no block
					////vSem.V() must not block
					//if (vSem instanceof binarySemaphore && vSem.permits == 1)
					//	throw new IllegalArgumentException("V() part of VP() operation will block. The V() part of VP() must not block.");

					// perform vSem.V()
					// Make sure this is a Lock, not a semaphore
					vSem.V(); 		// it's okay that we already hold vSem's lock
					// perform this.P()
					boolean blockingP = doP();
					if (!blockingP)
						return;
					// Note: The call to P() in this case should always block.
					PythonSemapore waitHere = new PythonSemaphore(0);
					o.semaphore = waitHere;
					//System.out.println("blocked thread is"+o.blockedThread);
					waitingP.add(o); // append blocked thread
				// This becomes second.thisLock.unlock()
				} // end synchronized(second)
			// This becomes first.thisLock.unlock()
			} // end synchronized(first)
			
			// using waitHere.acquire() instead of o.wait()
			try {o.wait();} catch (InterruptedException ex) {}
		// remove
		} // end synchronized (o)
	} // end VP()

}// end  countingSemaphore

