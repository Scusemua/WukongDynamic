import java.util.Arrays;
import java.util.concurrent.RecursiveTask;

import java.util.concurrent.ForkJoinPool; 
import java.util.concurrent.ConcurrentHashMap;
import java.util.Stack;
import java.util.ArrayList;

import java.lang.reflect.*;
import java.lang.reflect.Field;

 
 
//class ProblemType provided by User.
class ProblemType extends WukongProblem {
	// Fibonacci has no non-parallel algorithm, i.e., switch to sequential algorithm for speedup. Base case is values <= 1.
	static final int SEQUENTIAL_THRESHOLD = 1; // must be 1
	// Get input arrays when the size of the arrays reaches the INPUT_THRESHOLD, e.g., don't grab the initial 256MB array,
	// wait until you have 256gMB/1MB arrays of size 1MB. 
	
	// Note: For quicksort, we need the numbers for the root problem so we can partition them. If we want to start 
	// at a lower level, we need to input the subProblems with all heir information, stack, etc at the beginning,
	// which we could do: run QuickSort on the server, collect subProblems at level n, give them to the Executors
	// when they ask for them. Same could be done for MergeSort - which is a special case having easy to generate 
	// subProblems that we can generate locally by ignoring the array of numbers when we divide().
	//static final int INPUT_THRESHOLD = 1;
	
	//When unwinding recursion, output subProblme results when subProblem sizes reaches OUTPUT_THRESHOLD
	//static final int OUTPUT_THRESHOLD = 1;
	
	//Note: INPUT_THRESHOLD and OUTPUT_THRESHOLD are also declared in class WukongProblem, which this class inherits from.
	// If the user provides these constants here in ProblemType, we will use the values that the user provides; otherwise
	// we use INPUT_THRESHOLD = 0, i.e., input at start at root; and OUTPUT_THRESHOLD = Integer.MAX_VALUE;
	
	// memoize the problem results or not.
	static final Boolean memoize = false;
	
	int value; // only value for fibonacci
	
	// Just to keep mergesort and quicksort happy, i.e., so they compile 
	int[] numbers;
	int from;
	int to;

	public String toString() {
		StringBuffer b = new StringBuffer();
		b.append("(ID:"+problemID + "/");
		b.append("value:"+value+")");
		//b.append(value);
		return b.toString();
	}
}

//class ResultType provided by User, often same as ProblemType.
class ResultType extends WukongResult {
	int value; // same as problemType
	
	// Make copy of Problem. Shallow copy works here.
	// This is needed only for the non-Wukong, case, i.e., with no network,
	// since Java is passing local references around among the threads.
	// For Wukong, problems and results will be copied/serialized and sent over the network, 
	// so no local copies are needed.	
	ResultType copy() {
		ResultType copy = new ResultType();
		copy.value = value;
		return copy;
	}
	
	// Just to keep mergesort and quicksort happy
	int[] numbers;
	int from;
	int to;
	
	public String toString() {
		StringBuffer b = new StringBuffer();
		String s = super.toString();
		b.append(s);
		b.append(": ");
		b.append("(ID:"+problemID + "/");
		b.append("value:"+value+")");
		return b.toString();
	}
}

//class User provided by User.
class User {
	
	// No non-parallel algorithm for Fibonacci, i.e., no switch to sequential algorithm for speedup. 
	// SEQUENTIAL_THRESHOLD must be 1. 
	// This returns true/false, it does not do the base case. Base case done by sequential()
	static boolean baseCase(ProblemType problem) {
		if (ProblemType.SEQUENTIAL_THRESHOLD != 1) {
			// Should be dead code.
			System.out.println("Internal Error: baseCase: Fibonacci base case must be value <= 1.");
			System.out.println("Internal Error: baseCase: ProblemType.SEQUENTIAL_THRESHOLD set to: " + ProblemType.SEQUENTIAL_THRESHOLD);
			System.exit(1);
		}
		int value = problem.value;
		return value <= ProblemType.SEQUENTIAL_THRESHOLD;
	}
	// As in:
	// int fib(int n) {
	//	 if(n<=1)
	//		return n; // base case
	//	 return fib(n-1) + fib(n-2);
	//  }
	//
	
	// Some problems may use a pre-processing step before anything else is done.
	// For example, could be used for memoization to get result of previously saved problem.
	static void preprocess(ProblemType problem) {

	}
	
	// A problem P is by default passed to the executors that will be executing the child subproblems of P. One of these
	// executors will execute method combine() for these subproblems. In some cases, it is easier to write combine() when 
	// the subproblem's parent problem data is available; however, this parent data will also be sent and retrieved from 
	// storage if the parent data is part of the ProblemType data. (That is, problem P's parent data will be sent 
	// as part of problem P since the parent data will be on the stack of subProblems (representing the call stack) for P.) 
	// So if some of the parent data is not needed for combine() it should be deleted from the parent problem.
	// 
	// The field of a problem that will definitely be used is the problemID. Do not trim the problemID.  
	// The FanInStack (in parent class WukongProblem) is not needed by the DivideandConquer framework and 
	// can/should be trimmed. 
	// One option is to create a trimProblem() method in class WukongProblem and always call this method (in method
	// Fanout) in addition to calling User.trimProblem(), where User.tribProblem may be empty..
	static void trimProblem(ProblemType problem) {
		// No big value to trim for Fibonacci

		// We are cloning a parent's stack when we create a child's stack, and we do not need the parent's stack anymore.
		problem.FanInStack = null; // defined in class WukongProblem
	}

	// User must specify how subproblems are labeled. The problem label is used as a key into Wukong Storage,
	// where the value in the key-value pair is (eventually) the problem's result. Also used for Serverless
	// Networking pairing names.
	//
	// Note: we could have used level-order IDs like:
	//        1            2**0 children starting with child 2**0 (at level 0)
	//     2     3         2**1 children starting with child 2**! (at level 1)
	//  4    5  6   7      2**2 children starting with child 2**2 (at level 2)
	// This simplifies things since it is easy to compute the parent ID from the child's ID. With this ID scheme
	// we would not need to stack the problem IDs as we recurse; however, the stack makes it easy to get the level
	// since the level is just the size of the stack. 
	// We'll have to see if this scheme would work in general,
	// i.e., for different numbers of children, when the number of children may vary for parent nodes, etc. There has to be a 
	// way to recover the parent ID from a child, either by using a formula like above, or by stacking the IDs as we recurse.
	static String problemLabeler(ProblemType subProblem, int childID, ProblemType parentProblem, 
				ArrayList<ProblemType> subProblems) {
		// Note: supplying the subProblem to be labeled, it's parent Problem, and an ArrayList of all the subProblems,
		//       along with the index childID of the subProblem in the list of subProblems.
		// Called by WukongProblem.FanOut when an executor is forked for the subProblem.
		
		// For Fibonacci n, label is: path to n, e.g., For Fibonacci(5), the path (on stack) to Fibonacci(1) could be "5-4-2-1"
		// Note: the label for a subproblem must be unique. For example, for Fib(5), we create Fib(4) and Fib(3) and both
		// create Fib(2). These Fib(2)'s will be involved in separate fan-outs/fan-ins, and we need them to save their results in
		// different locations, etc. So we base their label on their *paths from the root*, which are unique, e.g. 5-4-2
		// and 5-3-2. Fan-out will call this and appen the value we return here to the parent's label, e.g., append "-2" to "5-4"
		StringBuffer b = new StringBuffer();
		
		synchronized(FanInSychronizer.getPrintLock()) {
		// subProblem.problemID assigned after divide() in Fan-out
			System.out.println(parentProblem.problemID + ": label subProblem ID (assigned in Fan-out): " + subProblem.problemID + " parent ID: " + parentProblem.problemID);
			//System.out.println(" subProblem stack: ");
			//for (int i=0;i< subProblem.FanInStack.size(); i++) {
			//	System.out.print(subProblem.FanInStack.get(i) + " ");
			//}
			//System.out.println();
			System.out.print(parentProblem.problemID + ":  labler: parent stack: ");
			for (int i=0; i< parentProblem.FanInStack.size(); i++) {
				System.out.print(parentProblem.FanInStack.get(i) + " ");
			}
			System.out.println();
			//System.out.println("labeler: subProblem stack: ");
			//for (int i=0; i< subProblem.FanInStack.size(); i++) {
			//	System.out.print(subProblem.FanInStack.get(i) + " ");
			//}
			//System.out.println();
		}

 		// label of subproblem is its stringified value
		b.append(String.valueOf(subProblem.value));
		synchronized(FanInSychronizer.getPrintLock()) {
			System.out.println(parentProblem.problemID + ": labler: generated subProblem Label: " + b.toString());
		}
		//}
		
		return b.toString();
	}
	
	//
	// Used for getting the memoized result of (sub)problem (for get(memoizedLabel, result)).
	// For example, When computing Fibonacci(4), Fibonacci 3's label will be "4-3" so memoized label is "3", i.e.,
	// we will store the result for Fibonacci(3) under the key "3". We will store the result for
	// Fibonacci(4) under the key "4".
	//
	static String memoizeIDLabeler(ProblemType problem) {
		String memoizedID = null;
		String label = problem.problemID;
		// String memoizedID = (label).substring(label.length()-1,label.length());
		
		// grab last token in: token1-token-2-....-tokenLast
		int lastIndex = label.lastIndexOf('-');
		if (lastIndex == -1) { 
			// no '-', e.g., for Fibonacci(4), problem label is "4", which has no '-' so memoize label is also "4"
			memoizedID = label;
		}
		else {
			// e.g., for computing Fibonacci(4), problem label for Fibonacci(3) will be "4-3", which has '-' so memoize label is "3"
			// which is the problem label for Fibonacci(3).
			memoizedID = label.substring(lastIndex+1, label.length());
		}
		return memoizedID;
	}
	
	// Divide the problem into (a list of) subproblems.
	static void divide(ProblemType problem, ArrayList<ProblemType> subProblems) {

		synchronized(FanInSychronizer.getPrintLock()) {
			System.out.println(problem.problemID+ ": Divide: fibonacci run: value: " + problem.value);
			System.out.println(problem.problemID+ ": Divide: problemID: " + problem.problemID);
			System.out.print(problem.problemID+ ": Divide: FanInStack: ");
			for (int i=0; i<problem.FanInStack.size(); i++)
				System.out.print(problem.FanInStack.get(i) + " ");
			System.out.println();
			System.out.flush();
		}

		
		// As in:
		// int fib(int n) {
		//	 if(n<=1)
		//		return n;
		//		return fib(n-1) + fib(n-2);
		//  }
		//
		
		ProblemType minus_1 = new ProblemType();
		minus_1.value = problem.value-1;
		ProblemType minus_2 = new ProblemType();
		minus_2.value = problem.value-2;
		
		synchronized(FanInSychronizer.getPrintLock()) {
			System.out.println(problem.problemID+ ": Divide: minus_1: " + minus_1);
			System.out.println(problem.problemID+ ": Divide: minus_2: " + minus_2);
		}
		
		// add right then left; order is up to user, become executore will be for last subprolem, which here is left
		subProblems.add(minus_2); // right
		subProblems.add(minus_1); // left

	} // divide

	// Combine the subproblem results. 
	static void combine(ArrayList<ResultType> subproblemResults, ResultType combination, String problem_problemID) {
		// add the two results, problem.problemID for debugging
		
		ResultType firstResult = subproblemResults.get(0);  // left
		ResultType secondResult = subproblemResults.get(1); // right

		int firstValue = firstResult.value;
		int secondValue = secondResult.value;
		
		combination.value = firstValue + secondValue;   // return value (parameter)
		synchronized(FanInSychronizer.getPrintLock()) {
			System.out.println(problem_problemID + ": Combine: firstValue: " + firstValue + " secondValue: " + secondValue 
				+ " combination.value: " + combination.value);
		}
	}

	// The problem data must be obtained from Wukong storage. Here, we are getting a specific subsegment of the input array,
	// which is the segment from-to.
	static void inputProblem(ProblemType problem) {
		// get problem's input from storage
		
		// Not using for Fibonacci since there is no input.
	}

	// User provides method to generate subproblem values, e.g., sub-array to be sorted, from problems.
	// The Problem Labels identify a (sub)problem, but we still need a way to generate the subproblem
	// data values. For example, if the parent array has values for 0-14, the left array has values for 0-7 and the 
	// right array has values for 8-14. The right array will be passed (as an Lambda invocation argument or 
	// written to Wukong storage) to a new executor for this subproblem.
	static void computeInputsOfSubproblems(ProblemType problem, ArrayList<ProblemType> subProblems) {

		// Not using for Fibonacci since there is no input.

	} // computeInput

	// User provides method to sequentially solve a problem
	// This is the base case computation.
	// As in:
	//if (User.baseCase(problem)) {
	//    User.sequential(problem,result);
	static void sequential(ProblemType problem, ResultType result) { //  int from, int to) {
		result.value = problem.value;
		synchronized(FanInSychronizer.getPrintLock()) {
			System.out.println(problem.problemID + ": Sequential: " + problem.problemID + " result.value: " + result.value);
		}
	}

	// User provides method to output the problem result.
	// We only call this for the final result, and this method verifies the final result.
	static void outputResult(String problem_problemID) {
		// problem.problemID passed for debug
		// Note: Used to be a result parameter but that was result at topof template, which is no longer
		// the final result since we create a new result object after every combine. The final result 
		// is the value in "root".
		//System.out.println();
		//System.out.print("Fibonacci("+DivideandConquerFibonacci.n+") = " + result.value);
		//System.out.println();
		
		ResultType result = FanInSychronizer.resultMap.get("root");
		
		MemoizationController.getInstance().stopThread();
		
		System.out.println();
		System.out.print(problem_problemID + ": Fibonacci("+DivideandConquerFibonacci.n+") = " + result.value);
		System.out.println();
		System.out.print(problem_problemID + ": Verifying ....... ");
		// Let other executors finish printing their output before we print result
		try {Thread.sleep(2000);} catch (InterruptedException e) {} 
		boolean error = false;
		if (result.value != DivideandConquerFibonacci.expectedValue)
			error = true;
		if (!error) {
			System.out.println("Verified.");
		}
		else {
			System.out.println("Error.");
		}
	}
}

// run as java DivideandConquerFibonacci
public class DivideandConquerFibonacci {

	// 0, 1, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144,
	static int n = 4;
	static int expectedValue = 3;
	static String rootProblemID = "root";


	public static void main(String[] args) {
		System.out.println("main: Running DivideandConquerFibonacci.");
		System.out.println("main: INPUT_THRESHOLD is: " + WukongProblem.INPUT_THRESHOLD);
		System.out.println("main: OUTPUT_THRESHOLD is: " + WukongProblem.OUTPUT_THRESHOLD);
		System.out.println("main: SEQUENTIAL_THRESHOLD (base_case) is: " + ProblemType.SEQUENTIAL_THRESHOLD);
		System.out.println("main: memoize is: " + ProblemType.memoize);
		
		// Assert:
		Field seq = null;
		try {
			seq = ProblemType.class.getDeclaredField("SEQUENTIAL_THRESHOLD");
		} catch (NoSuchFieldException nsfe) {
			// intentionally ignored
		}

		if (seq == null) {
			System.out.println("main: Error: ProblemType.SEQUENTIAL_THRESHOLD must be defined.");
			System.exit(1);
		} 
		
		// Assert: Sequential threshold based on size and input threshold based on level, so cannot compare them.
		//if (ProblemType.SEQUENTIAL_THRESHOLD > WukongProblem.INPUT_THRESHOLD) {
		//	System.out.println("Error: SORT_THRESHOLD>INPUT_THRESHOLD, as we cannot sort the numbers until we input them.");
		//	System.exit(1);
		//}
		
		// Assert:
		//if (WukongProblem.OUTPUT_THRESHOLD < ProblemType.SEQUENTIAL_THRESHOLD) {
		//	System.out.println("Internal Error: OUTPUT_THRESHOLD < SEQUENTIAL_THRESHOLD, where SEQUENTIAL_THRESHOLD is the ");
		//	System.out.println("                base case size, so output wil occur at the SEQUENTIAL_THRESHOLD, which is ");
		//	System.out.println("                larger than OUTPUT_THRESHOLD.");
		//	System.exit(1);
		//}

		System.out.println("main: n: " + n);
		System.out.println();

		String rootID = String.valueOf(n);
		Stack<ProblemType> FanInStack = new Stack<ProblemType>();
		//ProblemType rootProblem = new ProblemType(null, 0, values.length-1);
		ProblemType rootProblem = new ProblemType();
		rootProblem.value = n;
		rootProblem.problemID = rootProblemID;
		
		System.out.println("main: " + rootProblem);
		
		rootProblem.FanInStack = FanInStack;
		rootProblem.problemID = rootProblemID; // "root"
		DivideAndConquerExecutor root = new DivideAndConquerExecutor(rootProblem);
		root.run();
		
		/* No need to start root and join it; we will just wait for all DivideAndConquerExecutor threads to 
		 * terminate before this main thread finishes.
		 * "Java: As long as the main-method thread or any other *user* thread remains alive, the application will continue to execute."
		*/
		//root.start();
		//try {
		//	root.join();
		//} catch(Exception e) {
		//		e.printStackTrace();
		//		System.exit(1);
		//}
	}
}

/*
main: Running DivideandConquerFibonacci.
main: INPUT_THRESHOLD is: 0
main: OUTPUT_THRESHOLD is: 2147483647
main: SEQUENTIAL_THRESHOLD (base_case) is: 1
main: memoize is: true
main: n: 4

//        root           where root=Fibonacci(4)
//     3         2
//  2     1   1     0
//1   0

// Note: This does not reflect how the Fan-ins were processed - only the values returned by the Fan-ins
// root-2-0 returns 0
// root-3-1 returns 1
// root-2-1 returns 1, a memoized result from 3-1
// root-2 returns 1+0=1
// root-3-2 returns memoized result from root-2, which was 1 + 0 = 1
//  so root-3-2-1 and root-3-2-0 never called/computed
// root-3 returns 1+1=2
// root returns 2 + 1 = 3

// Note: Fan-in task is always Left child

// Fan-in processing:
// root-3-1 delivered 1, but was not Fan-in task for root-3 since root-3-1 is a Right child of root-3
// root-3-2 got stop since root-2 promised to compute result for problem "2" before root-3-2 promised; then
//  upon restart root-3-2 got memoized result 1 delivered by root-2; then was (the Left) fan-in task for root 3
//  with result 1+1=2; then was (the Left) fan-in task for root with result 2+1=3. 
// root-2-0 delivered 0, but was not fan-in task for root-2 since root-2-0 is a Right child of root-2
// root-2-1 got stop and upon restart got 1 from memoized 3-1; then was (Left) fan-in task for root-2 with result = 1 + 0 = 1,
//  but was not fan-in task for root since root-2 it is Right child of root


main: (ID:root/value:4)
root: Executor: root call pair on MemoizationController
root: channelMap keySet:root,
root: Executor: memoized send1: PROMISEVALUE: problem.problemID root memoizedLabel: root
root: Executor: memoized rcv1: problem.problemID root receiving ack.
root: Executor: memoized rcv1: problem.problemID root received ack.
root: Executor: memoized rcv1: problem.problemID root ack was null_result.
root: Executor: memoized rcv1: problem.problemID root memoizedLabel: root memoized result: null
root: Divide: fibonacci run: value: 4
root: Divide: problemID: root
root: Divide: FanInStack: 
root: Divide: minus_1: (ID:null/value:3)
root: Divide: minus_2: (ID:null/value:2)
root: Fanout: get subProblemID for non-become task.
root: label subProblem ID (assigned in Fan-out): null parent ID: root
root:  labeler: parent stack: 
root: labler: generated subProblem Label: 2
root: Fanout: push on childFanInStack: (poarent) problem: (ID:root/value:4)
root: Fanout: parent stack: 
root: Fanout: subProblem stack: (ID:root/value:4) 
root: Fanout: send ADDPAIRINGNAME message.
root: Fanout: ID: root invoking new right executor: root-2
root: Fanout: get subProblemID for become task.
root: label subProblem ID (assigned in Fan-out): null parent ID: root
root:  labler: parent stack:
root: labler: generated subProblem Label: 3
root: Fanout: ID: root becoming left executor: root-3

root-2: Executor: root-2 call pair on MemoizationController
root-2: channelMap keySet:root,root-2,
root-2: Executor: memoized send1: PROMISEVALUE: problem.problemID root-2 memoizedLabel: 2
root-2: Executor: memoized rcv1: problem.problemID root-2 receiving ack.
root-2: Executor: memoized rcv1: problem.problemID root-2 received ack.
root-2: Executor: memoized rcv1: problem.problemID root-2 ack was null_result.
root-2: Executor: memoized rcv1: problem.problemID root-2 memoizedLabel: 2 memoized result: null   // Q"correct place for this?
root-2: Divide: fibonacci run: value: 2
root-2: Divide: problemID: root-2
root-2: Divide: FanInStack: (ID:root/value:4) 
root-2: Divide: minus_1: (ID:null/value:1)
root-2: Divide: minus_2: (ID:null/value:0)
root-2: Fanout: get subProblemID for non-become task.
root-2: label subProblem ID (assigned in Fan-out): null parent ID: root-2
root-2:  labler: parent stack: (ID:root/value:4) 
root-2: labler: generated subProblem Label: 0
root-2: Fanout: push on childFanInStack: (poarent) problem: (ID:root-2/value:2)
root-2: Fanout: parent stack: (ID:root/value:4) 
root-2: Fanout: subProblem stack: (ID:root/value:4) (ID:root-2/value:2) 
root-2: Fanout: send ADDPAIRINGNAME message.
root-2: Fanout: ID: root-2 invoking new right executor: root-2-0
root-2: Fanout: get subProblemID for become task.
root-2: label subProblem ID (assigned in Fan-out): null parent ID: root-2
root-2:  labler: parent stack: (ID:root/value:4) 
root-2: labler: generated subProblem Label: 1
root-2: Fanout: ID: root-2 becoming left executor: root-2-1

root-3: Executor: root-3 call pair on MemoizationController
root-3: channelMap keySet:root-3,root,root-2,
root-3: Executor: memoized send1: PROMISEVALUE: problem.problemID root-3 memoizedLabel: 3
root-3: Executor: memoized rcv1: problem.problemID root-3 receiving ack.
root-3: Executor: memoized rcv1: problem.problemID root-3 received ack.
root-3: Executor: memoized rcv1: problem.problemID root-3 ack was null_result.
root-3: Executor: memoized rcv1: problem.problemID root-3 memoizedLabel: 3 memoized result: null
root-3: Divide: fibonacci run: value: 3
root-3: Divide: problemID: root-3
root-3: Divide: FanInStack: (ID:root/value:4) 
root-3: Divide: minus_1: (ID:null/value:2)
root-3: Divide: minus_2: (ID:null/value:1)
root-3: Fanout: get subProblemID for non-become task.
root-3: label subProblem ID (assigned in Fan-out): null parent ID: root-3
root-3:  labler: parent stack: (ID:root/value:4) 
root-3: labler: generated subProblem Label: 1
root-3: Fanout: push on childFanInStack: (poarent) problem: (ID:root-3/value:3)
root-3: Fanout: parent stack: (ID:root/value:4) 
root-3: Fanout: subProblem stack: (ID:root/value:4) (ID:root-3/value:3) 
root-3: Fanout: send ADDPAIRINGNAME message.
root-3: Fanout: ID: root-3 invoking new right executor: root-3-1
root-3: Fanout: get subProblemID for become task.
root-3: label subProblem ID (assigned in Fan-out): null parent ID: root-3
root-3:  labler: parent stack: (ID:root/value:4) 
root-3: labler: generated subProblem Label: 2
root-3: Fanout: ID: root-3 becoming left executor: root-3-2

root-2-0: Executor: root-2-0 call pair on MemoizationController
root-2-0: channelMap keySet:root-3,root-2-0,root,root-2,
root-2-0: Executor: memoized send1: PROMISEVALUE: problem.problemID root-2-0 memoizedLabel: 0
root-2-0: Executor: memoized rcv1: problem.problemID root-2-0 receiving ack.
root-2-0: Executor: memoized rcv1: problem.problemID root-2-0 received ack.
root-2-0: Executor: memoized rcv1: problem.problemID root-2-0 ack was null_result.
root-2-0: Sequential: root-2-0 result.value: 0
root-2-0: Executor: base case: result before ProcessBaseCase(): (ID:null: (ID:null/value:0)
root-2-0: Executor: ProcessBaseCase result: (ID:root-2-0: (ID:root-2-0/value:0)
root-2-0: **********************Start Fanin operation:
root-2-0: Fan-in: ID: root-2-0
root-2-0: Fan-in: becomeExecutor: false
root-2-0: Fan-in: FanInStack: (ID:root/value:4) (ID:root-2/value:2) 
root-2-0: Deliver starting Executors for promised Results:
root-2-0: Deliver end promised Results:
root-2-0: Fan-in: ID: root-2-0 parentProblem ID: root-2
root-2-0: Fan-in: ID: root-2-0 problem.becomeExecutor: false parentProblem.becomeExecutor: false
root-2-0: Fan-In: ID: root-2-0: FanInID: root-2 was not FanInExecutor:  result sent:(ID:root-2-0: (ID:root-2-0/value:0)
root-2-0: Fan-In: ID: root-2-0: FanInID: root-2: is not become Executor  and its value was: (ID:root-2-0: (ID:root-2-0/value:0) and after put is null



// Note: Does not reflect interleaving of calls to MC, e.g., 3-2's pair below occurred before this 2-1 pair
root-2-1: Executor: root-2-1 call pair on MemoizationController
root-2-1: channelMap keySet:root-3,root-2-1,root-2-0,root,root-3-1,root-2,
root-2-1: Executor: memoized send1: PROMISEVALUE: problem.problemID root-2-1 memoizedLabel: 1
root-2-1: Executor: memoized rcv1: problem.problemID root-2-1 receiving ack.
root-2-1: Executor: memoized rcv1: problem.problemID root-2-1 ack was stop.

// Note: Calling pair() again due to previous "Stop" on PROMISEVALUE
root-2-1: Executor: root-2-1 call pair on MemoizationController
root-2-1: channelMap keySet:root-3,root-2-1,root-2-0,root-3-2,root,root-3-1,root-2,
root-2-1: Executor: memoized send1: PROMISEVALUE: problem.problemID root-2-1 memoizedLabel: 1
root-2-1: Executor: memoized rcv1: problem.problemID root-2-1 receiving ack.
root-2-1: Executor: memoized rcv1: problem.problemID root-2-1 received ack.
root-2-1: Executor: memoized rcv1: problem.problemID root-2-1 memoizedLabel: 1 memoized result: (ID:root-2-1: (ID:root-2-1/value:1)
root-2-1: Executor: else in template: For Problem: (ID:root-2-1/value:0); Memoized result: (ID:root-2-1: (ID:root-2-1/value:1)

root-2-1: **********************Start Fanin operation:
root-2-1: Fan-in: ID: root-2-1
root-2-1: Fan-in: becomeExecutor: true
root-2-1: Fan-in: FanInStack: (ID:root/value:4) (ID:root-2/value:2) 
root-2-1: Fan-in: ID: root-2-1 parentProblem ID: root-2
root-2-1: Fan-in: ID: root-2-1 problem.becomeExecutor: true parentProblem.becomeExecutor: false
root-2-1: Fan-in: root-3-2,root-2-1: ID: root-2-1: FanIn: root-2 was FanInExecutor: result received:(ID:root-2-0: (ID:root-2-0/value:0)
root-2-1: FanIn: ID: root-2-1: FanInID: root-2: : Returned from put: executor isLastFanInExecutor 
root-2-1: (ID:root-2-0: (ID:root-2-0/value:0)
root-2-1: ID: root-2-1: call combine ***************
root-2-1: Combine: firstValue: 0 secondValue: 1 combination.value: 1

root-2-1: Exector: result.problemID: root-2-1 put memoizedLabel: 2 result: ID:root-2-1: (ID:root-2-1/value:1)
root-2-1: Deliver starting Executors for promised Results:
root-2-1: Deliver starting Executor for: root-3-2 problem.becomeExecutor: true problem.didInput: true
root-2-1: Deliver end promised Results:
root-2-1: Fan-in: ID: root-2-1 parentProblem ID: root
root-2-1: Fan-in: ID: root-2-1 problem.becomeExecutor: true parentProblem.becomeExecutor: false
root-2-1: Fan-In: ID: root-2-1: FanInID: root was not FanInExecutor:  result sent:(ID:root-2-1: (ID:root-2-1/value:1)
root-2-1: Fan-In: ID: root-2-1: FanInID: root: is not become Executor  and its value was: (ID:root-2-1: (ID:root-2-1/value:1) and after put is null

root-3-1: Executor: root-3-1 call pair on MemoizationController
root-3-1: channelMap keySet:root-3,root-2-1,root-2-0,root,root-3-1,root-2,
root-3-1: Executor: memoized send1: PROMISEVALUE: problem.problemID root-3-1 memoizedLabel: 1
root-3-1: Executor: memoized rcv1: problem.problemID root-3-1 receiving ack.
root-3-1: Executor: memoized rcv1: problem.problemID root-3-1 received ack.
root-3-1: Executor: memoized rcv1: problem.problemID root-3-1 ack was null_result.
root-3-1: Executor: memoized rcv1: problem.problemID root-3-1 memoizedLabel: 1 memoized result: null

root-3-1: Sequential: root-3-1 result.value: 1
root-3-1: Executor: base case: result before ProcessBaseCase(): (ID:null: (ID:null/value:1)
root-3-1: Executor: ProcessBaseCase result: ID:root-3-1: (ID:root-3-1/value:1)
root-3-1: Deliver starting Executors for promised Results:
root-3-1: Deliver starting Executor for: root-2-1 problem.becomeExecutor: true problem.didInput: true
root-3-1: Deliver end promised Results:
root-3-1: **********************Start Fanin operation:
root-3-1: Fan-in: ID: root-3-1
root-3-1: Fan-in: becomeExecutor: false
root-3-1: Fan-in: FanInStack: (ID:root/value:4) (ID:root-3/value:3) 
root-3-1: Fan-in: ID: root-3-1 parentProblem ID: root-3
root-3-1: Fan-in: ID: root-3-1 problem.becomeExecutor: false parentProblem.becomeExecutor: true
root-3-1: Fan-In: ID: root-3-1: FanInID: root-3 was not FanInExecutor:  result sent:(ID:root-3-1: (ID:root-3-1/value:1)
root-3-1: Fan-In: ID: root-3-1: FanInID: root-3: is not become Executor  and its value was: (ID:root-3-1: (ID:root-3-1/value:1) and after put is null

root-3-2: Executor: root-3-2 call pair on MemoizationController
root-3-2: channelMap keySet:root-3,root-2-1,root-2-0,root-3-2,root,root-3-1,root-2,
root-3-2: Executor: memoized send1: PROMISEVALUE: problem.problemID root-3-2 memoizedLabel: 2
root-3-2: Executor: memoized rcv1: problem.problemID root-3-2 receiving ack.
root-3-2: Executor: memoized rcv1: problem.problemID root-3-2 received ack.
root-3-2: Executor: memoized rcv1: problem.problemID root-3-2 ack was stop.
root-3-2: Executor: root-3-2 call pair on MemoizationController
root-3-2: channelMap keySet:root-3,root-2-1,root-2-0,root-3-2,root,root-3-1,root-2,
root-3-2: Executor: memoized send1: PROMISEVALUE: problem.problemID root-3-2 memoizedLabel: 2
root-3-2: Executor: memoized rcv1: problem.problemID root-3-2 receiving ack.
root-3-2: Executor: memoized rcv1: problem.problemID root-3-2 received ack.
root-3-2: Executor: memoized rcv1: problem.problemID root-3-2 memoizedLabel: 2 memoized result: (ID:root-3-2: (ID:root-3-2/value:1)
root-3-2: Executor: else in template: For Problem: (ID:root-3-2/value:0); Memoized result: (ID:root-3-2: (ID:root-3-2/value:1)
root-3-2: **********************Start Fanin operation:
root-3-2: Fan-in: ID: root-3-2
root-3-2: Fan-in: becomeExecutor: true
root-3-2: Fan-in: FanInStack: (ID:root/value:4) (ID:root-3/value:3) 
root-3-2: Fan-in: ID: root-3-2 parentProblem ID: root-3
root-3-2: Fan-in: ID: root-3-2 problem.becomeExecutor: true parentProblem.becomeExecutor: true
root-3-2: ID: root-3-2: FanIn: root-3 was FanInExecutor: starting receive.
root-3-2: ID: root-3-2: FanIn: root-3 was FanInExecutor: result received:(ID:root-3-1: (ID:root-3-1/value:1)
root-3-2: FanIn: ID: root-3-2: FanInID: root-3: : Returned from put: executor isLastFanInExecutor 
root-3-2: (ID:root-3-1: (ID:root-3-1/value:1)
root-3-2: ID: root-3-2: call combine ***************
root-3-2: Combine: firstValue: 1 secondValue: 1 combination.value: 2
root-3-2: Exector: result.problemID: root-3-2 put memoizedLabel: 3 result: ID:root-3-2: (ID:root-3-2/value:2)
// Note: May be that no Exeutors are waiting for the results
root-3-2: Deliver starting Executors for promised Results:
root-3-2: Deliver end promised Results:
root-3-2: Fan-in: ID: root-3-2 parentProblem ID: root
root-3-2: Fan-in: ID: root-3-2 problem.becomeExecutor: true parentProblem.becomeExecutor: false
root-3-2: ID: root-3-2: FanIn: root was FanInExecutor: starting receive.
root-3-2: ID: root-3-2: FanIn: root was FanInExecutor: result received:(ID:root-2-1: (ID:root-2-1/value:1)
root-3-2: FanIn: ID: root-3-2: FanInID: root: : Returned from put: executor isLastFanInExecutor 
root-3-2: (ID:root-2-1: (ID:root-2-1/value:1)
root-3-2: ID: root-3-2: call combine ***************
root-3-2: Combine: firstValue: 1 secondValue: 2 combination.value: 3
root-3-2: Exector: result.problemID: root-3-2 put memoizedLabel: root result: (ID:root-3-2: (ID:root-3-2/value:3)
root-3-2: Deliver starting Executors for promised Results:
root-3-2: Deliver end promised Results:
root-3-2: Executor: Writing the final value to root: (ID:root-3-2: (ID:root-3-2/value:3)

MemoizationThread: Interrupted: returning.

root-3-2: Fibonacci(4) = 3
root-3-2: Verifying ....... 

Verified.


***************** MemoizationThread Trace:******************
root: MemoizationThread: pair: pairingName: root
root: MemoizationThread: promise by: root

root: MemoizationThread: add pairing name: root-2
root: MemoizationThread: ADDPAIRINGNAME: pairing names after add: root,root-2,

root: MemoizationThread: add pairing name: root-3
root: MemoizationThread: ADDPAIRINGNAME: pairing names after add: root-3,root,root-2,

root: MemoizationThread: remove pairing name: root pairingNames.size: 3
root: MemoizationThread: pairing names after remove rootroot-3,root-2,root-2: 


root-2: MemoizationThread: pair: pairingName: root-2
root-2: MemoizationThread: promise by: root-2
root-2: MemoizationThread: add pairing name: root-2-0
root-2: MemoizationThread: ADDPAIRINGNAME: pairing names after add: root-3,root-2-0,root-2,
root-2: MemoizationThread: add pairing name: root-2-1
root-2: MemoizationThread: ADDPAIRINGNAME: pairing names after add: root-3,root-2-1,root-2-0,root-2,
root-2: MemoizationThread: remove pairing name: root-2 pairingNames.size: 5
root-2: MemoizationThread: pairing names after remove root-2root-3,root-2-1,root-2-0,root-3-1,root-2-0: Executor: memoized rcv1: problem.problemID root-2-0 memoizedLabel: 0 memoized result: null

root-3: MemoizationThread: pair: pairingName: root-3
root-3: MemoizationThread: promise by: root-3
root-3: MemoizationThread: add pairing name: root-3-1
root-3: MemoizationThread: ADDPAIRINGNAME: pairing names after add: root-3,root-2-1,root-2-0,root-3-1,root-2,
root-3: MemoizationThread: add pairing name: root-3-2
root-3: MemoizationThread: ADDPAIRINGNAME: pairing names after add: root-3,root-2-1,root-2-0,root-3-2,root-3-1,
root-3: MemoizationThread: pairing names after remove root-3root-2-1,root-2-0,root-3-2,root-3-1,root-2-1: Executor: memoized rcv1: problem.problemID root-2-1 received ack.

root-2-0: MemoizationThread: pair: pairingName: root-2-0
root-2-0: MemoizationThread: promise by: root-2-0
root-2-0: MemoizationThread: DELIVEREDVALUE: r2: type: PROMISEDVALUE
root-2-0: MemoizationThread: DELIVEREDVALUE: info: sender: root-2-0 problem/result ID root-2-0 memoizationLabel: 0 delivered result: (ID:root-2-0: (ID:root-2-0/value:0)
root-2-0: MemoizationThread: remove pairing name: root-2-0 pairingNames.size: 4
root-2-0: MemoizationThread: pairing names after remove root-2-0root-2-1,root-3-2,root-3-1,root-3-2: MemoizationThread: duplicate promise by: root-3-2

root-2-1: MemoizationThread: pair: pairingName: root-2-1
root-2-1: MemoizationThread: duplicate promise by: root-2-1
// repairing after restart
root-2-1: MemoizationThread: pair: pairingName: root-2-1  
root-2-1: MemoizationThread: promised and already delivered so deliver on promise to: root-2-1
root-2-1: MemoizationThread: DELIVEREDVALUE: r2: type: PROMISEDVALUE
root-2-1: MemoizationThread: DELIVEREDVALUE: info: sender: root-2-1 problem/result ID root-2-1 memoizationLabel: 2 delivered result: (ID:root-2-1: (ID:root-2-1/value:1)
root-2-1: MemoizationThread: remove pairing name: root-2-1 pairingNames.size: 2
root-2-1: MemoizationThread: pairing names after remove root-2-1root-3-2,root-3-2: MemoizationThread: promised and already delivered so deliver on promise to: root-3-2

root-3: MemoizationThread: remove pairing name: root-3 pairingNames.size: 5

root-3-1: MemoizationThread: pair: pairingName: root-3-1
root-3-1: MemoizationThread: promise by: root-3-1
root-3-1: MemoizationThread: remove pairing name: root-3-1 pairingNames.size: 3
root-3-1: MemoizationThread: pairing names after remove root-3-1root-2-1: ID: root-2-1: FanIn: root-2 was FanInExecutor: starting receive.

root-3-2: MemoizationThread: pair: pairingName: root-3-2
root-3-2: MemoizationThread: pair: pairingName: root-3-2
root-3-2: MemoizationThread: DELIVEREDVALUE: r2: type: PROMISEDVALUE
root-3-2: MemoizationThread: DELIVEREDVALUE: info: sender: root-3-2 problem/result ID root-3-2 memoizationLabel: 3 delivered result: (ID:root-3-2: (ID:root-3-2/value:2)
root-3-2: MemoizationThread: DELIVEREDVALUE: r2: type: PROMISEDVALUE
root-3-2: MemoizationThread: DELIVEREDVALUE: info: sender: root-3-2 problem/result ID root-3-2 memoizationLabel: root delivered result: (ID:root-3-2: (ID:root-3-2/value:3)
root-3-2: MemoizationThread: remove pairing name: root-3-2 pairingNames.size: 1
root-3-2: MemoizationThread: pairing names after remove root-3-2


*/
