import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Stack;


//class ProblemType provided by User.
class ProblemType extends WukongProblem {
	// The threshold at which we switch to a sequential algorithm.
	static final int SEQUENTIAL_THRESHOLD = 2;
	
	// Get input arrays when the level reaches the INPUT_THRESHOLD, e.g., don't grab the initial 256MB array,
	// wait until you reach level , say, 1, when there are two subproblems each half as big.
	// To input at the start of the problem (root), use 0; the stack has 0 elements on it for the root problem.
	// To input the first two subProblems, use 1. Etc.
	static final int INPUT_THRESHOLD = 1;

	// When unwinding recursion, output subProblem results when subProblem level reaches OUTPUT_THRESHOLD
	//static final int OUTPUT_THRESHOLD = 1;
	
	//Note: INPUT_THRESHOLD and OUTPUT_THRESHOLD are also declared in class WukongProblem, which this class inherits from.
	// If the user provides these constants here in ProblemType, we will use the values that the user provides; otherwise
	// we use INPUT_THRESHOLD = 0, i.e., input at start at root; and OUTPUT_THRESHOLD = Integer.MAX_VALUE - since we 
	// should never be at this level, we will just output the final result.
	
	// memoize the problem results or not.
	static Boolean memoize = false;
	
	int[] numbers;
	int from;
	int to;
	
	// just to keep Fibonacci happy.
	int value;

	// Note: Default constructor() is required
	 
	// you can have other destructors but then you jave to defibe the no-arg default constructor.
	//ProblemType(int[] numbers, int from, int to) {
	//	this.numbers = numbers;
	//	this.from = from;
	//	this.to = to;
	//}

	public String toString() {
		StringBuffer b = new StringBuffer();
		b.append("(from: " + from + ", to: " + to + ")");
		if (numbers != null) {
			for (int i=0; i<numbers.length; i++)
				b.append(numbers[i] + " ");
			}
		System.out.flush();
		return b.toString();
	}
}

//class ResultType provided by User, often same as ProblemType.
class ResultType extends WukongProblem {
	int[] numbers;
	int from;
	int to;
	
	// Make copy of Problem. Shallow copy works here.
	// This is needed only for the non-Wukong, case, i.e., with no network,
	// since Java is passing local references around among the threads.
	// For Wukong, problems and results will be copied/serialized and sent over the network, 
	// so no local copies are needed.
	ResultType copy() {
		ResultType copy = new ResultType();
		copy.numbers = numbers;
		copy.from = from;
		copy.to = to;
		return copy;
	}
	
	public String toString() {
		StringBuffer b = new StringBuffer();
		b.append("(from: " + from + ", to: " + to + ") ");
		if (numbers != null) {
			for (int i=0; i<numbers.length; i++)
				b.append(numbers[i] + " ");
			}
		System.out.flush();
		return b.toString();
	}
}

//class User provided by User.
class User {
	
	// The baseCase is always a sequential sort (though it could be on an array of 
	// length 1, if that is the sequential threshold.)
	static boolean baseCase(ProblemType problem) {
		int size = problem.to - problem.from + 1;
		return size <= ProblemType.SEQUENTIAL_THRESHOLD;
	}
	
	// Some problems may use a pre-processing step before anything else is done.
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
	// Fanout) in addition to calling User.trimProblem(), where User.tribProblem may be empty.
	static void trimProblem(ProblemType problem) {
		// No need to keep the array of numbers to be sorted after we make a copy of each child's half of the numbers.
		problem.numbers = null;
		// We are including a parent's stack when we create a child's stack, and we do not need the parent's stack anymore.
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
	// we would not need to stack the problem IDs as we recurse. We'll have to see if this scheme would work in general,
	// i.e., for different numbers of children, when the number of children may vary for parent nodes, etc. There has to be a 
	// way to recover the parent ID from a child, either by using a formula liek above, or by stacking the IDs as we recurse.

	static String problemLabeler(ProblemType subProblem, int childID, ProblemType parentProblem, 
			ArrayList<ProblemType> subProblems) {
	
		// We label the problem with from = f and to = to as "from-to".
		// problemLabeler() is called once for each child problem identified
		// by method divide(). A childID is provided to assist in 
		// ID generation. For example the IDs for the left subproblem and 
		// the right subproblem are generated using different values.
		// For MergeSort, the childID for right is 0 and left is 1,
		// and if the parent ID is 0-14, the left child ID is 0-7 (from - mid)
		// and the right child ID is 8-14 (mid+1 - to)
		
		int mid = parentProblem.from + Math.floorDiv(parentProblem.to - parentProblem.from, 2);
		String ID = null;
		if (childID == 0) {
			if (mid+1 < parentProblem.to) {
				ID = String.valueOf((mid+1)) + "x" + String.valueOf(parentProblem.to);
			}
			else {
				ID = String.valueOf(mid+1);  
			}
			return ID;
		}
		else {
			if (parentProblem.from < mid) {
				ID = String.valueOf(parentProblem.from) + "x" + String.valueOf(mid);
			}
			else {
				ID = String.valueOf(parentProblem.from);
			}
			return ID;
		}
	}

	//
	// Used for getting the memoized result of (sub)problem (for get(memoizedLabel, result)).
	// MergeSort is not memoized
	static String memoizeIDLabeler(ProblemType problem) {
		return null;
	}
	
	// Divide the problem into (a list of) subproblems.
	static void divide(ProblemType problem, ArrayList<ProblemType> subProblems) {

		synchronized(FanInSychronizer.getPrintLock()) {
			System.out.println("Divide: mergeSort run: from: " + problem.from + " to: " + problem.to);
			System.out.println("Divide: problemID: " + problem.problemID);
			System.out.print("Divide: FanInStack: ");
			for (int i=0; i<problem.FanInStack.size(); i++)
				System.out.print(problem.FanInStack.get(i) + " ");
			System.out.println();
			System.out.flush();
		}

		int size = problem.to - problem.from + 1;

		int mid = problem.from + Math.floorDiv(problem.to - problem.from, 2);

		synchronized(FanInSychronizer.getPrintLock()) {
			System.out.println("Divide: ID: " + problem.problemID + ", mid: " + mid + ", mid+1: " + (mid+1) + ", to: " + (problem.to));
		}

		ProblemType right = new ProblemType();
		right.numbers = problem.numbers;
		right.from = mid+1;
		right.to = problem.to;
		ProblemType left = new ProblemType();
		left.numbers = problem.numbers;
		left.from = problem.from;
		left.to = mid;

		subProblems.add(right);
		subProblems.add(left);

	} // divide

	// Combine the subproblem results. 
	static void combine(ArrayList<ResultType> subproblemResults, ResultType problemResult) {
		// This is merge, which ignores from/to values for the subproblems, as it always starts merging from position 0
		// and finishes in the last positions of the arrays,
		// The from/to values of a subproblem are w.r.t the original input array.

		ResultType firstResult = subproblemResults.get(0);
		ResultType secondResult = subproblemResults.get(1);
		// For MergeSort this is merge.
		int[] firstArray = firstResult.numbers;
		int[] secondArray = secondResult.numbers;

		synchronized(FanInSychronizer.getPrintLock()) {
			int[] values = new int[firstArray.length+secondArray.length];
			int from = 0; 

			System.out.println("combine: values.length for merged arrays: " + values.length);

			System.out.print("first: ");
			for (int i=0; i<firstArray.length; i++)
				System.out.print(firstArray[i] + " ");

			System.out.print("second: ");
			for (int i=0; i<secondArray.length; i++)
				System.out.print(secondArray[i] + " ");
			System.out.println();

			int li = 0, ri = 0;
			while (li < firstArray.length && ri < secondArray.length) {
				System.out.println("li: " + li + " firstArray.length: " + firstArray.length + " ri: " + ri + " secondArray.length: " + secondArray.length);
				if (firstArray[li] < secondArray[ri]) {
					values[from++] = firstArray[li++];
				} else {
					values[from++] = secondArray[ri++];
				}
			}

			while (li < firstArray.length) {
				values[from++] = firstArray[li++];
			}

			while (ri < secondArray.length) {
				values[from++] = secondArray[ri++];
			}

			System.out.println("combine result: values.length: " + values.length + ", values: ");
			for (int i=0; i<values.length; i++)
				System.out.print(values[i] + " ");
			System.out.println();
			System.out.flush();
			problemResult.numbers = values;
			if (firstResult.from < secondResult.from) {
				problemResult.from = firstResult.from;
				problemResult.to = secondResult.to;
			}
			else {
				problemResult.from = secondResult.from; 
				problemResult.to = firstResult.to; 
			}
		}
	}

	// The problem data must be obtained from Wukong storage. Here, we are getting a specific subsegment of the input array,
	// which is the segment from-to.
	static void inputProblem(ProblemType problem) {
		// get problem's input from storage
		synchronized(FanInSychronizer.getPrintLock()) {
			System.out.println("inputNumbers");
		}

		// hard-coded the input array.
		int[] numbers = DivideandConquerMergeSort.values;

		// If the INPUT_THRESHOLD equals the size of the entire input array, then there is no need to 
		// copy the entire input array as we already initialized values to the input array.
		int size = problem.to - problem.from +1; 
		if (size != DivideandConquerMergeSort.values.length)
			numbers = Arrays.copyOfRange(numbers, problem.from, problem.to+1);

		problem.numbers = numbers;

		// where:
		//  public static boolean[] copyOfRange(boolean[] original,int from, int to)
		//  Parameters:
		//original - the array from which a range is to be copied
		//from - the initial index of the range to be copied, inclusive
		//to - the final index of the range to be copied, exclusive. (This index may lie outside the array.)
		//Returns: a new array containing the specified range from the original array, truncated or padded with false elements to obtain the required length
	}

	// User provides method to generate subproblem values, e.g., sub-array to be sorted, from problems.
	// The Problem Labels identify a (sub)problem, but we still need a way to generate the subproblem
	// data values. For example, if the parent array has values for 0-14, the left array has values for 0-7 and the 
	// right array has values for 8-14. The right array will be passed (as an Lambda invocation argument or 
	// written to Wukong storage) to a new executor for this subproblem.
	static void computeInputsOfSubproblems(ProblemType problem, ArrayList<ProblemType> subProblems) {
		int size = problem.to - problem.from + 1;
		int midArray =  0 + Math.floorDiv((problem.numbers.length-1) - 0, 2);

		synchronized(FanInSychronizer.getPrintLock()) {
			System.out.println("computeInputsOfSubproblems (" + ((problem.FanInStack.size() >= WukongProblem.INPUT_THRESHOLD)?">=":"<") + "INPUT_THRESHOLD) : ID: " + problem.problemID 
				+ ", midArray: " + midArray + ", to: " + (problem.to));
		}

		int[] leftArray = null;
		int[] rightArray = null;
		try {
			synchronized(FanInSychronizer.getPrintLock()) {
				System.out.println("computeInputsOfSubproblems: problem.numbers: " + problem.numbers);
				System.out.print("computeInputsOfSubproblems: ID: " + problem.problemID + ", numbers.length: " + problem.numbers.length + ", numbers: ");
				for (int i=0; i<problem.numbers.length; i++)
					System.out.print(problem.numbers[i] + " ");
				System.out.println();
				// Assuming that inputNumbers returns the problem's actual from-to subsegment of the complete input.

				// Copies are made from the parent problem's sub-segment of the input array, are a prefix of parent's copy, 
				// and start with 0
				System.out.println("computeInputsOfSubproblems: ID: " + problem.problemID + " size < threshold, make left copy: from: " + 0 + " midArray+1 " + (midArray+1));
				leftArray = Arrays.copyOfRange(problem.numbers, 0, midArray+1);
				System.out.println("computeInputsOfSubproblems: ID: " + problem.problemID + " size < threshold, make right copy: midArray+1: " + (midArray+1) + " to+1 " + (problem.numbers.length));
				rightArray = Arrays.copyOfRange(problem.numbers, midArray+1, problem.numbers.length);
				// Assert:
				if (size != problem.numbers.length) {
					System.out.println("Internal Error: computeInput: size != numbers.length-1");
					System.out.println("computeInputsOfSubproblems: size: " + size + " problem.numbers.length-1: " + (problem.numbers.length-1));
					System.exit(1); 
				}
				System.out.flush();
			}
		} catch (Exception e) {
			synchronized(FanInSychronizer.getPrintLock()) {
				e.printStackTrace();
				System.exit(1);
			}
		}

		// The Fan-out task will assign these child sub-arrays to the corresponding child problem.
		subProblems.get(0).numbers = rightArray;
		subProblems.get(1).numbers = leftArray;
	} // computeInput

	// User provides method to sequentially solve a problem
	// Insertion Sort
	static void sequential(ProblemType problem, ResultType result) { //  int from, int to) {
		int[] numbers = problem.numbers;
		int from = 0; 
		int to = numbers.length-1;
		synchronized(FanInSychronizer.getPrintLock()) {
			System.out.println("sequential sort: " + from + " " + to);
		}
		for (int i = from+1; i <= to; ++i) {
			int current = numbers[i];
			int j = i-1;
			while (from <= j && current < numbers[j]) {
				numbers[j+1] = numbers[j--];
			}
			numbers[j+1] = current;
		}
		result.numbers = numbers;
		result.from = problem.from;
		result.to = problem.to;
	}
	
	
	// User provides method to output the problem result.
	// We only call this for the final result, and this method verifies the final result.
	static void outputResult() {
		// Note: Used to be a result parameter but that was result at top of template, which is no longer
		// the final result since we create a new result object after every combine. The final result 
		// is the value in "root".
		//System.out.println();
		//System.out.print("Fibonacci("+DivideandConquerFibonacci.n+") = " + result.value);
		//System.out.println();
		
		ResultType result = FanInSychronizer.resultMap.get(DivideandConquerMergeSort.rootProblemID);
		
		MemoizationController.getInstance().stopThread();
		
		System.out.println();
		System.out.print("Unsorted " + "    : ");
		for (int i=0; i<DivideandConquerMergeSort.values.length; i++)
			System.out.print(DivideandConquerMergeSort.values[i] + " ");
		System.out.println();

		// The executor that does the last merge Fan-In has the final result
		System.out.print("Sorted (" + result.from + "-" + result.to + "): ");
		for (int i=0; i<result.numbers.length; i++)
			System.out.print(result.numbers[i] + " ");
		System.out.println();
		
		System.out.print("Expected     : ");
		for (int i=0; i<DivideandConquerMergeSort.expectedOrder.length; i++)
			System.out.print(DivideandConquerMergeSort.expectedOrder[i] + " ");
		System.out.flush();
		System.out.println();

		// Let other executors finish printing their output before we print result
		try {Thread.sleep(1000);} catch (InterruptedException e) {} 
		
		System.out.print("Verifying .......");
		boolean error = false;
		for (int i=0; i<DivideandConquerMergeSort.values.length; i++) {
			if (result.numbers[i] != DivideandConquerMergeSort.expectedOrder[i]) {
				System.out.println();
				System.out.println("Error in expected value: result.numbers[" + i + "] " 
					+ result.numbers[i] + " != expectedOrder[" + i + "]" + DivideandConquerMergeSort.expectedOrder[i]);
				error = true;
			}
		}
		if (!error) {
			System.out.println("Verified.");
		}
	}
}


public class DivideandConquerMergeSort {

	static int[] values = new int[] {9, -3, 5, 0, 1, 2, -1, 4, 11, 10, 13, 12, 15, 14, 17, 16}; // size 16
	static int[] expectedOrder = new int[] {-3, -1, 0, 1, 2, 4, 5, 9, 10, 11, 12, 13, 14, 15, 16, 17};
	//static int[] values = new int[] {9, -3};
	//static int[] expectedOrder = new int[] { -3, 9};
	//static String rootProblemID = "0x" + (values.length-1);
	static String rootProblemID = "root";

	public static void main(String[] args) {
		System.out.println("Running DivideandConquerMergeSort.");
		System.out.println("INPUT_THRESHOLD is: " + WukongProblem.INPUT_THRESHOLD);
		System.out.println("OUTPUT_THRESHOLD is: " + WukongProblem.OUTPUT_THRESHOLD);
		System.out.println("SEQUENTIAL_THRESHOLD is: " + ProblemType.SEQUENTIAL_THRESHOLD);
		
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
		if (values.length < ProblemType.SEQUENTIAL_THRESHOLD) {
			System.out.println("Internal Error: unsorted array must have length at least SEQUENTIAL_THRESHOLD, which is the base case.");
			System.exit(1);
		}
		
		// Assert:
		//if (WukongProblem.OUTPUT_THRESHOLD < ProblemType.SEQUENTIAL_THRESHOLD) {
		//	System.out.println("Internal Error: OUTPUT_THRESHOLD < SEQUENTIAL_THRESHOLD, where SEQUENTIAL_THRESHOLD is the ");
		//	System.out.println("                base case size, so output will occur at the SEQUENTIAL_THRESHOLD, which is ");
		//	System.out.println("                larger than OUTPUT_THRESHOLD.");
		//	System.exit(1);
		//}
		
		//Option: More Asserts on values >=1, etc.
		

		System.out.println("Numbers: ");
		for (int i=0; i<values.length; i++)
			System.out.print(values[i] + " ");
		System.out.println();

		String rootID = String.valueOf(0) + "x" + String.valueOf(values.length-1);  
		Stack<ProblemType> FanInStack = new Stack<ProblemType>();

		ProblemType rootProblem = new ProblemType();
		rootProblem.numbers = null;
		rootProblem.from = 0;
		rootProblem.to = values.length-1;
		
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

/* Part of trace:
 * 
mergeSort run: from: 0 to: 15
threadID: 0-15
FanInStack 
ID: 0-15 mid: 7 mid+1: 8 to: 15
ID: 0-15 Pushing FanInID :0-15
ID: 0-15 invoking new right thread: 8-15
ID: 0-15 becoming left thread: 0-7
mergeSort run: from: 0 to: 7
threadID: 0-7
FanInStack 0-15 

mergeSort run: from: 8 to: 15
threadID: 8-15
FanInStack 0-15 

ID: 8-15 mid: 11 mid+1: 12 to: 15
ID: 8-15 Pushing FanInID :8-15
ID: 8-15 invoking new right thread: 12-15
ID: 8-15 becoming left thread: 8-11

ID: 0-7 mid: 3 mid+1: 4 to: 7
ID: 0-7 Pushing FanInID :0-7
ID: 0-7 invoking new right thread: 4-7
ID: 0-7 becoming left thread: 0-3

FanInStack 0-15 8-15 

FanInStack 0-15 0-7

mergeSort run: from: 4 to: 7
threadID: 4-7
FanInStack 0-15 0-7 
inputNumbers

mergeSort run: from: 0 to: 3
threadID: 0-3
FanInStack 0-15 0-7 
inputNumbers

ID: 0-3 size == threshold, make right copy: mid+1: 2 to+1 4

ID: 0-3 invoking new right thread: 2-3
ID: 0-3 becoming left thread: 0-1

mergeSort run: from: 0 to: 1
threadID: 0-1
FanInStack 0-15 0-7 0-3 
ID: 0-1 pushing FanInID: 0-1
ID: 0-1 numbers.length: 2 numbers: 
9 -3 
ID: 0-1 size < threshold, make left copy: from: 0 midX+1 1
ID: 0-1 size < threshold, make right copy: midX+1: 1 to+1 2

ID: 0-1 invoking new right thread: 1

mergeSort run: from: 1 to: 1
threadID: 1
FanInStack 0-15 0-7 0-3 0-1 
FanInID: 1
**********************Start Fanin operation: ID : 1
ID: 1
ID: 1 FanInStack: 0-15 0-7 0-3 0-1 
ID: 1: FanInID: 0-1
ID: 1: previous value is null.

ID: 0-1 becoming left thread: 0
mergeSort run: from: 0 to: 0
threadID: 0
FanInStack 0-15 0-7 0-3 0-1 
FanInID: 0
**********************Start Fanin operation: ID : 0
ID: 0
ID: 0 FanInStack: 0-15 0-7 0-3 0-1 
ID: 0: FanInID: 0-1
ID: 0: Last Lambda
ID: 0: Returned from put: previousValue: -3 ID: 0: call MergeX ***************
numbers: 9 
previousValue: -3 
MergeX: values.length for merged arrays: 2
left: 9 right: -3 
li: 0 left.length: 1 ri: 0 right.length: 1
MergeX result: values.length: 2, values: 
-3 9 
ID: 0: FanInID: 0-3
ID: 0: previous value is null.

mergeSort run: from: 3 to: 3
threadID: 3
FanInStack 0-15 0-7 0-3 2-3 
FanInID: 3
**********************Start Fanin operation: ID : 3
ID: 3
ID: 3 FanInStack: 0-15 0-7 0-3 2-3 
ID: 3: FanInID: 2-3
ID: 3: Last Lambda
ID: 3: Returned from put: previousValue: 5 ID: 3: call MergeX ***************
numbers: 0 
previousValue: 5 
MergeX: values.length for merged arrays: 2
left: 0 right: 5 
li: 0 left.length: 1 ri: 0 right.length: 1
MergeX result: values.length: 2, values: 
0 5 
ID: 3: FanInID: 0-3
ID: 3: Last Lambda
ID: 3: Returned from put: previousValue: -3 9 ID: 3: call MergeX ***************
numbers: 0 5 
previousValue: -3 9 
MergeX: values.length for merged arrays: 4
left: 0 5 right: -3 9 
li: 0 left.length: 2 ri: 0 right.length: 2
li: 0 left.length: 2 ri: 1 right.length: 2
li: 1 left.length: 2 ri: 1 right.length: 2
MergeX result: values.length: 4, values: 
-3 0 5 9 

mergeSort run: from: 4 to: 4
threadID: 4
FanInStack 0-15 0-7 4-7 4-5 
FanInID: 4
ID: 4: FanInID: 4-5
ID: 4: Last Lambda
ID: 4: Returned from put: previousValue: 2 ID: 4: call MergeX ***************
numbers: 1 
previousValue: 2 
MergeX: values.length for merged arrays: 2
left: 1 right: 2 
li: 0 left.length: 1 ri: 0 right.length: 1
MergeX result: values.length: 2, values: 
1 2 
ID: 4: FanInID: 4-7
ID: 4: Last Lambda
ID: 4: Returned from put: previousValue: -1 4 ID: 4: call MergeX ***************
numbers: 1 2 
previousValue: -1 4 
MergeX: values.length for merged arrays: 4
left: 1 2 right: -1 4 
li: 0 left.length: 2 ri: 0 right.length: 2
li: 0 left.length: 2 ri: 1 right.length: 2
li: 1 left.length: 2 ri: 1 right.length: 2
MergeX result: values.length: 4, values: 
-1 1 2 4 
ID: 4: FanInID: 0-7
ID: 4: Last Lambda
ID: 4: Returned from put: previousValue: -3 0 5 9 ID: 4: call MergeX ***************
numbers: -1 1 2 4 
previousValue: -3 0 5 9 
MergeX: values.length for merged arrays: 8
left: -1 1 2 4 right: -3 0 5 9 
li: 0 left.length: 4 ri: 0 right.length: 4
li: 0 left.length: 4 ri: 1 right.length: 4
li: 1 left.length: 4 ri: 1 right.length: 4
li: 1 left.length: 4 ri: 2 right.length: 4
li: 2 left.length: 4 ri: 2 right.length: 4
li: 3 left.length: 4 ri: 2 right.length: 4
MergeX result: values.length: 8, values: 
-3 -1 0 1 2 4 5 9 
ID: 4: FanInID: 0-15
ID: 4: previous value is null.

ID: 10: FanInID: 0-15
ID: 10: Last Lambda
ID: 10: Returned from put: previousValue: -3 -1 0 1 2 4 5 9 ID: 10: call MergeX ***************
numbers: 10 11 12 13 14 15 16 17 
previousValue: -3 -1 0 1 2 4 5 9 
MergeX: values.length for merged arrays: 16
left: 10 11 12 13 14 15 16 17 right: -3 -1 0 1 2 4 5 9 
li: 0 left.length: 8 ri: 0 right.length: 8
li: 0 left.length: 8 ri: 1 right.length: 8
li: 0 left.length: 8 ri: 2 right.length: 8
li: 0 left.length: 8 ri: 3 right.length: 8
li: 0 left.length: 8 ri: 4 right.length: 8
li: 0 left.length: 8 ri: 5 right.length: 8
li: 0 left.length: 8 ri: 6 right.length: 8
li: 0 left.length: 8 ri: 7 right.length: 8
MergeX result: values.length: 16, values: 
-3 -1 0 1 2 4 5 9 10 11 12 13 14 15 16 17 

Unsorted: 9 -3 5 0 1 2 -1 4 11 10 13 12 15 14 17 16 
Sorted  : -3 -1 0 1 2 4 5 9 10 11 12 13 14 15 16 17 
Expected: -3 -1 0 1 2 4 5 9 10 11 12 13 14 15 16 17 
Verifying .......Verified.
*/
