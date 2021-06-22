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
	// Fibonacci has no non-parallel algorithm. Base case is values <= 1.
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
	static final Boolean memoize = true;
	
	int value;
	
	// Just to keep mergesort and quicksort happy
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
	int value;
	
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
	
	// No non-parallel algorithm for Fibonacci. SEQUENTIAL_THRESHOL must be 1. 
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
	//		return n;
	//		return fib(n-1) + fib(n-2);
	//  }
	//
	
	// Some problems may use a preprocessing step before anything else is done.
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
	// One option is to create a trimProblem() method in call WukongProblem and always call this method (in method
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
		//Note: the label for a subproblem must be unique. For example, for Fib(5), we create Fib(4) and Fib(3) and both
		// create Fib(2). These Fib(2)'s will be involved in separate fan-outs/fan-ins, and we need them to save their results in
		// different locations, etc. So we base their label on their *paths from the root*, which are unique, e.g. 5-4-2
		// and 5-3-2.
		StringBuffer b = new StringBuffer();
		
		synchronized(FanInSychronizer.getPrintLock()) {
		System.out.println("labeler: subProblem ID: " + subProblem.problemID + " parent ID: " + parentProblem.problemID);
		//System.out.println(" subProblem stack: ");
		//for (int i=0;i< subProblem.FanInStack.size(); i++) {
		//	System.out.print(subProblem.FanInStack.get(i) + " ");
		//}
		System.out.println();
		System.out.println(" parent stack: ");
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

 		// label of subproblem is its value
		b.append(String.valueOf(subProblem.value));
		synchronized(FanInSychronizer.getPrintLock()) {
			System.out.println("Label: " + b.toString());
		}
		//}
		
		return b.toString();
	}
	
	//
	// Used for getting the memoized result of (sub)problem (for get(memoizedLabel, result)).
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
			// e.g., for Fibonacci(4), problem label for Fibonacci(3) is "4-3", which has '-' so memoize label is "3"
			// which is the problem label for Fibonacci(3).
			memoizedID = label.substring(lastIndex+1, label.length());
		}
		return memoizedID;
	}
	
	// Divide the problem into (a list of) subproblems.
	static void divide(ProblemType problem, ArrayList<ProblemType> subProblems) {

		synchronized(FanInSychronizer.getPrintLock()) {
			System.out.println("Divide: fibonacci run: value: " + problem.value);
			System.out.println("Divide: problemID: " + problem.problemID);
			System.out.print("Divide: FanInStack: ");
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
		System.out.println("divide: minus_1: " + minus_1);
		System.out.println("divide: minus_2: " + minus_2);
		}
		
		subProblems.add(minus_2);
		subProblems.add(minus_1);

	} // divide

	// Combine the subproblem results. 
	static void combine(ArrayList<ResultType> subproblemResults, ResultType combination) {
		// Ignores from/to values for the subproblems, as it always starts merging from position 0
		// and finishes in the last positions of the arrays,
		// The from/to values of a subproblem are w.r.t the original input array.
		//
		// Simple merge: place left array values before right array values., but we don't know
		// which subproblem is left and which is right, so check their first values.

		ResultType firstResult = subproblemResults.get(0);
		ResultType secondResult = subproblemResults.get(1);

		int firstValue = firstResult.value;
		int secondValue = secondResult.value;
		
		combination.value = firstValue + secondValue;
		synchronized(FanInSychronizer.getPrintLock()) {
			System.out.println("combine: firstValue: " + firstValue + " secondValue: " + secondValue 
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
	static void sequential(ProblemType problem, ResultType result) { //  int from, int to) {
		result.value = problem.value;
		synchronized(FanInSychronizer.getPrintLock()) {
			System.out.println("sequential: " + problem.problemID + " result.value: " + result.value);
		}
	}

	// User provides method to output the problem result.
	// We only call this for the final result, and this method verifies the final result.
	static void outputResult() {
		// Note: Used to be a result parameter but that was result at topof template, which is no longer
		// the final result since we create a new result object after every combine. The final result 
		// is the value in "root".
		//System.out.println();
		//System.out.print("Fibonacci("+DivideandConquerFibonacci.n+") = " + result.value);
		//System.out.println();
		
		ResultType result = FanInSychronizer.resultMap.get("root");
		
		MemoizationController.getInstance().stopThread();
		
		System.out.println();
		System.out.print("Fibonacci("+DivideandConquerFibonacci.n+") = " + result.value);
		System.out.println();
		System.out.print("Verifying ....... ");
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

public class DivideandConquerFibonacci {

	// 0, 1, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144,
	static int n = 4;
	static int expectedValue = 3;
	static String rootProblemID = "root";


	public static void main(String[] args) {
		System.out.println("Running DivideandConquerFibonacci.");
		System.out.println("INPUT_THRESHOLD is: " + WukongProblem.INPUT_THRESHOLD);
		System.out.println("OUTPUT_THRESHOLD is: " + WukongProblem.OUTPUT_THRESHOLD);
		System.out.println("SEQUENTIAL_THRESHOLD is: " + ProblemType.SEQUENTIAL_THRESHOLD);
		System.out.println("memoize is: " + ProblemType.memoize);
		
		// Assert:
		Field seq = null;
		try {
			seq = ProblemType.class.getDeclaredField("SEQUENTIAL_THRESHOLD");
		} catch (NoSuchFieldException nsfe) {
			// intentionally ignored
		}

		if (seq == null) {
			System.out.println("Error: ProblemType.SEQUENTIAL_THRESHOLD must be defined.");
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

		System.out.println("n: " + n);
		System.out.println();

		String rootID = String.valueOf(n);
		Stack<ProblemType> FanInStack = new Stack<ProblemType>();
		//ProblemType rootProblem = new ProblemType(null, 0, values.length-1);
		ProblemType rootProblem = new ProblemType();
		rootProblem.value = n;
		
		System.out.println(rootProblem);
		
		rootProblem.FanInStack = FanInStack;
		rootProblem.problemID = rootProblemID;
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
