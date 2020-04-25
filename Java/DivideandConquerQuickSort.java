import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Stack;

/*
//class ProblemType provided by User.
class ProblemType extends WukongProblem {
	// The threshold at which we switch to a sequential algorithm.
	static final int SEQUENTIAL_THRESHOLD = 2;
	// Get input arrays when the size of the arrays reaches the INPUT_THRESHOLD, e.g., don't grab the initial 256MB array,
	// wait until you have 256gMB/1MB arrays of size 1MB. 
	
	// Note: For quicksort, we need the numbers for the root problem so we can partition them. If we want to start 
	// at a lower level, we need to input the subProblems with all heir information, stack, etc at the beginning,
	// which we could do: run QuickSort on the server, collect subProblems at level n, give them to the Executors
	// when they ask for them. Same could be done for MergeSort - which is a special case having easy to generate 
	// subProblems that we can generate locally by ignoring the array of numbers when we divide().
	//static final int INPUT_THRESHOLD = 1;
	
	//When unwinding recursion, output subProblem results when level reaches OUTPUT_THRESHOLD
	//static final int OUTPUT_THRESHOLD = 2;
	
	//Note: INPUT_THRESHOLD and OUTPUT_THRESHOLD are also declared in class WukongProblem, which this class inherits from.
	// If the user provides these constants here in ProblemType, we will use the values that the user provides; otherwise
	// we use INPUT_THRESHOLD = 0, i.e., input at start at root; and OUTPUT_THRESHOLD = Integer.MAX_VALUE.
	
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
		b.append("(from: " + from + ", to: " + to + ") ");
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
	// As in:
	//    int arr[] = {3,2,1,4,7,6,5};
	//	quickSort(arr, 0, arr.length-1);
	//	if(p<r)
	//	{
	//		int q = partition(arr, p, r);
	//		quickSort(arr, p, q-1);
	//		quickSort(arr, q+1, r);
	//	}
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
	// Fanout) in addition to calling User.trimProblem(), where User.tribProblem may be empty.
	static void trimProblem(ProblemType problem) {
		// No need to keep the array of numbers to be sorted after we make a copy of each child's half of the numbers.
		problem.numbers = null;
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
		
		// We label the mergesort subproblem with from = 5 and to = 10 as "5x10".
		// problemLabeler() is called once for each child problem identified
		// by method divide(). A childID is provided to assist in 
		// ID generation. For example the IDs for the left subproblem and 
		// the right subproblem have indexes 0 and 1, respectively. You could use the 
		// index to determine which one you are labeling, left or right. 
		//
		
		// For quicksort, label is: fromxto. We don't need the index values, we just use from and to
		String ID = String.valueOf((subProblem.from)) + "x" + String.valueOf(subProblem.to);

		return ID;
	}

	// partition problem. The vlaue of the pivot is relative to the initial problem array. We are 
	// using the pivot to label the subproblems, e.g., problem 1x4 is partitioned into 1x1 and 2x4. 
	private static int partition(int numbers[], int pivot, int r) {
		// pivot is problem.from, r is problem.to. But the values of pivot and r are relative to 
		// the initial array, which is fine if you always have a reference to this array. But we
		// are making copies of subsections of this array and passing them across the network
		// to executors, so our arrays always start at 0 and end at array.length-1. We need to 
		// work with these actual arrays here.
		System.out.println("partition: numbers.length: " + numbers.length + " index r: " + r);

		// Suppose we have initially in the start of the array:  -3 -1 1 2 0, which is problem 0x4.
		// We then get subProblems left = 0x0 and right = 1x4, and make copies of their corresponding sections of the arrays:
		//left copy: 
		//-3 
		//right copy: 
		//-1 1 2 0 
		//We will sequential sort the 0x0 and then
		//Fanout: ID: 0-4 invoking new right executor: 1x4
		//Fanout: ID: 0-4 becoming left executor: 0-0
		//Divide: quickSort run: from: 1 to: 4
		//Divide: problemID: 1x4
		//Divide: FanInStack: (from: 0 to: 15) 9 -3 5 0 1 2 -1 4 11 10 13 12 15 14 16 17  (from: 0, to: 13) 9 -3 5 0 1 2 -1 4 11 10 13 12 14 15  (from: 0, to: 11) 9 -3 5 0 1 2 -1 4 11 10 12 13  (from: 0, to: 9) 9 -3 5 0 1 2 -1 4 10 11  (from: 0, to: 7) -3 0 1 2 -1 4 5 9  (from: 0, to: 4) -3 -1 1 2 0  
		//numbers.length: 4 index r: 4
		//
		// But there is no position r = 4 in the array for 1x4, so we need to adjust r so that it is the end of the 
		// array numbers.length-1;
		//

		int to = r;  // saved r
		int from = pivot; // saved pivot
		
		// so do not return return i+1, but actually i+1+pivot in values relative to the initial problem array
		// r only used as end of array marker, so no need to adjust, i.e., not returned
		r = numbers.length-1;
		pivot = 0; // weare computing pivot
		
		// use value in last position as pivot value
		int x = numbers[r];
		
		// note: returning pivot = i+1, so since argument pivot initially is "from" value then starting with 
		// the computed pivot i = from-1.
		int i = pivot - 1;
		for(int j = pivot; j <= r - 1; j++)	{
			if(numbers[j] <= x)	{
				i++;
				int swap=numbers[i];
				numbers[i]=numbers[j];
				numbers[j]=swap;
			}
		}
		//put pivot value (numbers[r]), which is still at the end, in its final pivot position (i+1) via swap
		int swap = numbers[i+1];
		numbers[i+1] = numbers[r];
		numbers[r] = swap;
		
		// Note: The computed pivot is i+1. The argument pivot is the from value of the subProblem we are partitioning.
		// Example: for 0x4, we partitioned into 0x0 and 1x4. Here we are partioning (1,4), so pivot = 1 and r = 4.
		// We return i+1 + pivot (which is computed pivot (i+1) + argument pivot, where argument pivot is the from value for 
		// this subproblem. 
		// Example: partition 1-4, say computed pivot is 1 but this is relative to the starting position of our subProblems array, 
		// which is always 0, not the starting position 1 of the subProblem, which is in terms of the initial problem array 
		// However, we can't use a computed pivot of 1, as this makes the right subproblem 1-4, which the same as the parent problem.
		// The correct value, relative to the initial problem array, is 1 + 1 = argument pivot + computed pivot = 2, (since in terms of 
		// the initial problem array, we start 1x4 at 1, not 0, so 2-4.
		// For the left subProblem, we will use start x computed pivot - 1, but when partitioning (1,4) start is 1 so if 
		// we use computed pivot - 1 = 1 - 1 = 0, then startx0 is 1x0, which cannot be correct.  Again, we need to add
		// argument pivot (which is 1) to the computed pivot, which means the computed pivot will be 1 + 1 = 2.
		// Now left is start x (computed pivot - 1) = 1x(2-1) = 1-1. So left = 1-1 and right = 2-4.
		//
		// ==> computed pivot = i+1+argument pivot = i+1+from; left is from x (pivot-1); right is pivot x to. 
		// Note: for singler processor sorting, the left and right subproblems do not include the pivot, e.g.,
		// [left] pivot [right] as pivot is in its final position. We include pivot as the first element of 
		// right: [left] [pivot right], so left is from ... pivot -1 and right is pivot ... to. 

		// Note: For problem 1x4, which is a label relative to the initial problem array, we will get 1x1 and 2x4,
		// as labels, but we will also have to grab the corresponding sub-arrays of these subProblems. That is 
		// 1x4 may be -1 1 2 0, We will get left = 1x1 with array -1 in positions 0..0 and and right = 2x4 with 
		// array 1 2 0 in positions 1 .. 3 of parent 1x4 with array -1 1 2 0.
		// How do we get the subarrays for left and right given 1-1 and 2-4. Note, based on pivot value 2, 
		// where pivot is always the from value of right, we know that actual pivot is 2-1 = right.from - left.from, 
		// i.e, adjustment was i+1+pivot which is i+1 + argument pivot = i+1+left.from.
		// So get input for subproblem uses this reverse adjustment. left's array is 0 ... left.to - left.from 
		// = 0 ...left.size, and right's array is (right.from - left.from) ... end of array, which is 
		// (right.from - left.from) ... left.right.length-1

		int newPivot = i+1+from;

		return newPivot;
	}
	
	//
	// Used for getting the memoized result of (sub)problem (in the call: get(memoizedLabel, result)).
	// Quicksort is not memoized.
	static String memoizeIDLabeler(ProblemType problem) {
		return null;
	}
	
	// Divide the problem into (a list of) subproblems.
	static void divide(ProblemType problem, ArrayList<ProblemType> subProblems) {

		synchronized(FanInSychronizer.getPrintLock()) {
			System.out.println("Divide: quickSort run: from: " + problem.from + " to: " + problem.to);
			System.out.println("Divide: problemID: " + problem.problemID);
			System.out.print("Divide: FanInStack: ");
			for (int i=0; i<problem.FanInStack.size(); i++)
				System.out.print(problem.FanInStack.get(i) + " ");
			System.out.println();
			System.out.flush();
		}
		
		// quicksort pivot.
		int pivot = partition(problem.numbers, problem.from, problem.to);

		synchronized(FanInSychronizer.getPrintLock()) {
			System.out.println("Divide (> INPUT_THRESHOLD) : ID: " + problem.problemID 
				+ ", pivot: " + pivot + ", pivot-1: " + (pivot-1) + ", pivot+1: " + (pivot+1)
				+ ", to: " + (problem.to));
		}
		
		//	As in:
		// 	int q = partition(arr, p, r);
		//	quickSort(arr, p, q-1);
		//	quickSort(arr, q+1, r);
		// but we must include the pivot in one of the subproblems, which for us is right, so
		// 	int q = partition(arr, p, r);
		//	quickSort(arr, p, q-1);
		//	quickSort(arr, q, r);  // pivot is in first position

		//Note: pivot is w.r.t the original problem (e.g., 0x15), where pivot is value that partition(0,15) returns.
		//Note: we must return pivot value in one of the arrays, so instead of using 
		//left = from - (pivot-1) and right = (pivot+1) - to, we use left = from - pivot-1
		//and right = (pivot) - to. Need to adjust when we get the inputs for subproblems.
		
		ProblemType left = new ProblemType();
		left.numbers = problem.numbers;
		left.from = problem.from;
		left.to = pivot-1;
		
		ProblemType right = new ProblemType();
		right.numbers = problem.numbers;
		right.from = pivot;
		right.to = problem.to;
		
		System.out.println("divide: left: " + left);
		System.out.println("divide: right: " + right);
		
		subProblems.add(right);
		subProblems.add(left);

	} // divide

	// Combine the subproblem results. 
	static void combine(ArrayList<ResultType> subproblemResults, ResultType problemResult) {
		// Ignores from/to values for the subproblems, as it always starts merging from position 0
		// and finishes in the last positions of the arrays,
		// The from/to values of a subproblem are w.r.t the original input array.
		//
		// Simple merge: place left array values before right array values, but we don't know
		// which subproblem is left and which is right, so check their "first" values.

		ResultType secondResult = subproblemResults.get(0);
		ResultType firstResult = subproblemResults.get(1);

		int[] firstArray = firstResult.numbers;
		int[] secondArray = secondResult.numbers;
		
		// first array values must be less than or equal to all second array values.
		// Note: if first values of arrays are the same 111111 122222, then looking only at 
		// first values could be an error. Make sure first of second array is >= last of first array.
		if (firstArray[firstArray.length-1] > secondArray[0]) {
			int[] temp = firstArray;
			firstArray = secondArray;
			secondArray=temp;
		}
		
		// combine arrays [first] [second] into array [first second] 

		synchronized(FanInSychronizer.getPrintLock()) {
			int[] values = new int[firstArray.length+secondArray.length];
			int from = 0; 

			System.out.println("combine: values.length for combined arrays: " + values.length);

			System.out.print("first: ");
			for (int i=0; i<firstArray.length; i++)
				System.out.print(firstArray[i] + " ");
			
			System.out.print("second: ");
			for (int i=0; i<secondArray.length; i++)
				System.out.print(secondArray[i] + " ");
			System.out.println();
			
			int i = 0;
			while(i<firstArray.length) {
				values[i] = firstArray[i];
				i++;
			}
			while(i<firstArray.length+secondArray.length) {
				values[i] = secondArray[i-firstArray.length];
				i++;
			}

			System.out.println("combine result: values.length: " + values.length + ", values: ");
			for (int j=0; j<values.length; j++)
				System.out.print(values[j] + " ");
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
	// which is the segment fromxto.
	static void inputProblem(ProblemType problem) {
		// get problem's input from storage
		synchronized(FanInSychronizer.getPrintLock()) {
			System.out.println("inputNumbers");
		}

		// hard-coded the input array.
		int[] numbers = DivideandConquerQuickSort.values;

		//If the INPUT_THRESHOLD equals the size of the entire input array, then there is no need to 
		// copy the entire input array as we already initialized values to the input array.
		int size = problem.to - problem.from +1; 
		if (size != DivideandConquerQuickSort.values.length)
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

		synchronized(FanInSychronizer.getPrintLock()) {
			System.out.println("computeInputsOfSubproblems (" + ((size == WukongProblem.INPUT_THRESHOLD)?"==":"<") + "INPUT_THRESHOLD) : ID: " + problem.problemID 
				+ ", from: " + problem.from 
				+ ", to: " + (problem.to));
		}
		
		ProblemType right = subProblems.get(0);
		ProblemType left = subProblems.get(1);
		
		synchronized(FanInSychronizer.getPrintLock()) {
			System.out.println("computeInputsOfSubproblems (" + ((size == WukongProblem.INPUT_THRESHOLD)?"==":"<") + "INPUT_THRESHOLD) : " 
			+ " left subproblem: " + left + " right subproblem: " + right);
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


				// When we get a pivot for fromxto, we use left = from - pivot-1 and right = (pivot) - to.
				// e.g., 1-4 partitioned into 1-1 and 2-4 with pivot = right.from. So problem array was
				// 0 - (right.to - left.from); subarray for left is 0 to (left.to - left.from) + 1 = (number of elements-1)  + 1, where 
				// (number of elements -1) is because if there are n elements then the position of last element in 
				// array (where first element is at 0) is n-1 and +1 is because of copyOfRange needed +1 past position of 
				// last element. So subaray for right starts at (right.from - left.from) = number of elements in left, 
				// which works since first of these elements is at position 0 and last of these is at position 
				// number of elements -1, so first element of right is in position number of elements.
				// Subarray for right ends at (right.to - left.from) + 1 = (total number of elements in left and right - 1) + 1, where
				// (number of elements -1) is because if there are n elements then the position of last element in 
				// array (where first element is at 0) is n-1 and +1 is because of copyOfRange needed +1 past position 
				// of last element.

				// Copies are made from the the sub problems and start with 0
				System.out.println("computeInputsOfSubproblems: ID: " + problem.problemID + " size < threshold, make left copy: from: " + 0 + " left.to+1 " + (left.to+1));
				leftArray = Arrays.copyOfRange(problem.numbers, 0, (left.to-left.from)+1);
				
				System.out.println("computeInputsOfSubproblems: ID: " + problem.problemID + " size < threshold, make right copy: from right.from " + (right.from) + " problem.numbers.length " + (problem.numbers.length));
				rightArray = Arrays.copyOfRange(problem.numbers, right.from - left.from, (right.to - left.from) + 1);
				// Assert:
				if (size != problem.numbers.length) {
					System.out.println("Internal Error: computeInput: size != numbers.length-1");
					System.out.println("computeInputsOfSubproblems: size: " + size + " problem.numbers.length-1: " + (problem.numbers.length-1));
					System.exit(1); 
				}
				
				System.out.println("left copy: ");
				for (int i=0; i<leftArray.length; i++)
					System.out.print(leftArray[i] + " ");
				System.out.println();
				System.out.println("right copy: ");
				for (int i=0; i<rightArray.length; i++)
					System.out.print(rightArray[i] + " ");
				System.out.println();
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
	// Insertion Sort,
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
		// is the value at the root.
		
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
*/
public class DivideandConquerQuickSort {

	static int[] values = new int[] {9, -3, 5, 0, 1, 2, -1, 4, 11, 10, 13, 12, 15, 14, 17, 16}; // size 16
	static int[] expectedOrder = new int[] {-3, -1, 0, 1, 2, 4, 5, 9, 10, 11, 12, 13, 14, 15, 16, 17};
	//static int[] values = new int[] {9, -3};
	//static int[] expectedOrder = new int[] { -3, 9};
	//static String rootProblemID = "0x" + (values.length-1);
	static String rootProblemID = "root";

	public static void main(String[] args) {
		System.out.println("Running DivideandConquerQuickSort.");
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
		//	System.out.println("Internal Error: OUTPUT_THRESHOLD < SEQUENTIAL_THRESHOLD, where ");
		//	System.out.println("                SEQUENTIAL_THRESHOLD is the base case size, so output ");
		//	System.out.println("                will occur at the SEQUENTIAL_THRESHOLD, which is larger ");
		//	System.out.println("                than OUTPUT_THRESHOLD.");
		//	System.exit(1);
		//}

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