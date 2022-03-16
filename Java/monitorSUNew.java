
public class monitorSU implements propertyParameters, monitorEventTypeParameters, eventTypeParameters {
	// Monitor Toolbox for tracing and replay

	private countingSemaphore mutex = new binarySemaphore(1, "mutex");
	private countingSemaphore reentry = new binarySemaphore(0, "reentry");
	private int reentryCount = 0;
	private String monitorName = null;

	private static final Object classLock = monitorTracingAndReplay.class;
	
	public int getName() {
		return monitorName;
	}

	public monitorSU(String monitorName) {
		this.monitorName = monitorName;
	}

	public final class conditionVariable {
		private countingSemaphore = null;
		private int numWaitingThreads = 0;
		private String conditionName = null;

		public conditionVariable(String conditionName) {
			this.conditionName = conditionName;
			threadQueue = new countingSemaphore(0, conditionName+ ":threadQueue");
		}

		public conditionVariable() {

		}

		public final void signalC() {
			if (numWaitingThreads > 0) {
				++reentryCount;
				reentry.VP(threadQueue);
				--reentryCount;
			}
		}

		public final void signalC_and_exitMonitor() {
			if (numWaitingThreads > 0) {
				threadQueue.V();
			} else if (reentryCount > 0)
				reentry.V();
			else
				mutex.V();
		}

		public final void waitC() {
			numWaitingThreads++;
			if (reentryCount > 0)
				threadQueue.VP(reentry);
			else
				threadQueue.VP(mutex);
			--numWaitingThreads;
		}

		public final boolean empty() {
			return (numWaitingThreads == 0);
		}

		public final int length() {
			return numWaitingThreads;
		}
	}

	public final void enterMonitor(String methodName) {
		mutex.P();
	}

	public final void enterMonitor() {
		mutex.P();
	}

	public final void exitMonitor() {
		if (reentryCount > 0)
			reentry.V();
		else
			mutex.V();
	}

}
