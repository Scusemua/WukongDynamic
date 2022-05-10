import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.RecursiveTask;
import java.util.concurrent.Semaphore;

import javax.swing.text.html.HTMLDocument.Iterator;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Stack;
import java.util.ArrayList;

import java.lang.reflect.*;

class DivideAndConquerExecutor extends Thread {

	ProblemType problem;

	DivideAndConquerExecutor(ProblemType problem) {
		this.problem = problem;
	}

	public void run() {
		
		// start Fan-Out task
		
		ServerlessNetworkingClientServer ServerlessNetworkingMemoizer = null;
		if (WukongProblem.memoize && WukongProblem.USESERVERLESSNETWORKING) {
			ServerlessNetworkingMemoizer = MemoizationController.getInstance().pair(problem.problemID);
			ResultType ack = ServerlessNetworkingMemoizer.rcv1();
		}
		
		// pre-process problem, if required
		User.preprocess(problem);	

		// We do not necessarily input the entire initial problem at once. We may input several sub-problems instead.

		// Note: The level of Problems is FanInStack.size(). Root problem has empty stack, 2 children of root have
		// root Problem on their stacks, so level 1, etc.
		// System.out.println("Problem: " + problem + " at level: " + (problem.FanInStack.size()));
		//
		// Note: Probably easier to use the level of problem, which is easy to compute based on the problem's FanInStack
		// i.e., level of problems is FanInStack.size(), instead of size of problem. Then two sibling problems would have the 
		// same level, while they might not have the same sizes. You could then run the problem for n levels on a server, capture the subProblems, and 
		// send them to the executors who will ask for them (supplying their problem ID, which is a "task" ID)) when they 
		// get to level n.
		// Levels might also be better to use with WukongProblem.OUTPUT_THRESHOLD since size doesn't work well when sibling
		// subproblems have unequal sizes, like for Quicksort.
		
		ResultType result = null; //  = new ResultType(); //  = new ResultType(); // first and only set result is by sequentialSort when the baseCase is reached.
		boolean memoizedResult = false;
		
		if (WukongProblem.memoize) {
			// Here, we want to get the value previously computed for this subproblem
			// as opposed to below where we put a computed value for a subProblem
			// Note that the problem ID may be "4-3-2" or "4-2" but the memoized
			// result is 2 in both cases. So in addition to problemLabeler(),
			// we have memoizedLabeler().

			MemoizationMessage promiseMsg = new MemoizationMessage();
			promiseMsg.messageType = MemoizationController.MemoizationMessageType.PROMISEVALUE;
			promiseMsg.senderID = problem.problemID;
			promiseMsg.problemOrResultID = problem.problemID;
			promiseMsg.becomeExecutor = problem.becomeExecutor;
			promiseMsg.didInput = problem.didInput;
			
			// e.g., if problem.problemID is "4-3", memoizedLabel is "3"
			String memoizedLabel = User.memoizeIDLabeler(problem); 
			promiseMsg.memoizationLabel = memoizedLabel;
			promiseMsg.result = null;	
			promiseMsg.FanInStack = problem.FanInStack;
			
			synchronized(FanInSychronizer.getPrintLock()) {
				System.out.println("memoized send1: problem.problemID " + problem.problemID + " memoizedLabel: " + memoizedLabel);
			}
			
			ServerlessNetworkingMemoizer.send1(promiseMsg);
			
			synchronized(FanInSychronizer.getPrintLock()) {
				System.out.println("memoized get: problem.problemID " + problem.problemID + " getting ack.");
			}
			
			result = ServerlessNetworkingMemoizer.rcv1();
			
			synchronized(FanInSychronizer.getPrintLock()) {
				System.out.println("memoized get: problem.problemID " + problem.problemID + " got ack.");
			}
			
			if (result == MemoizationController.nullResult) {
				// no memoized result
				System.out.println("memoized get: problem.problemID " + problem.problemID + " ack was null result.");
				result = null;
			}
			else if (result == MemoizationController.stopResult) {
				// end executor, to be "restarted" later when subproblem result becomes available
				System.out.println("memoized get: problem.problemID " + problem.problemID + " ack was stop.");
				return; 
			}
			else {
				// got a memoized result for problem, but the result's ID is the ID of the problem whose result 
				// was memoized, which is not the problem we are working on here. So set ID to proper ID.
				result.problemID = problem.problemID;	
			}
			synchronized(FanInSychronizer.getPrintLock()) {
				System.out.println("memoized get: problem.problemID " + problem.problemID + " memoizedLabel: " + memoizedLabel + " memoized result: " + result);
			}
		}
		
		if (!WukongProblem.memoize || (WukongProblem.memoize && result==null)) {
			result = new ResultType();
			
//Can we do this if also doing Memoization? I think so.
			
			if (problem.FanInStack.size() == WukongProblem.INPUT_THRESHOLD && problem.didInput == false) {
				User.inputProblem(problem); 
				problem.didInput = true;
				/* Debug output is for Merge/Quick Sort only.
				synchronized(FanInSychronizer.getPrintLock()) {
					int size = problem.to - problem.from + 1;
					System.out.println("inputProblemNew: problem.from: " + problem.from + " problem.to: " + problem.to 
						+ " problem.FanInStack.size(): " + problem.FanInStack.size() + " size: " + size);
					for (int i=0; i<problem.numbers.length; i++)
						System.out.print(problem.numbers[i] + " ");
					System.out.println();
				}
				*/
			}
		
			/* If using size instead of level:
			// Using <= and not just == since for odd sized arrays to be sorted, the sizes of subproblems
			// are not always the same. Since using <=, must also check whether we have already input numbers,
			// i.e., once < it will stay less than but we cannot keep inputing numbers.
			if (size <= WukongProblem.INPUT_THRESHOLD && problem.didInput == false) {
				User.inputProblem(problem); 
				synchronized(FanInSychronizer.getPrintLock()) {
					System.out.println("inputProblemNew: problem.from: " + problem.from + " problem.to: " + problem.to 
						+ " problem.FanInStack.size(): " + problem.FanInStack.size() + " size: " + size);
					for (int i=0; i<problem.numbers.length; i++)
						System.out.print(problem.numbers[i] + " ");
					System.out.println();
				}
			}
			*/
		
			// Base case is a sequential algorithm though possibly on a problem of size 1
			if (User.baseCase(problem)) {
				if (!problem.didInput) {
					System.out.println("Error: SEQUENTIAL_THRESHOLD reached before INPUT_THRESHOLD, but we cannot sort the numbers until we input them.");
					System.out.println("problem.SEQUENTIAL_THRESHOLD: " + problem.SEQUENTIAL_THRESHOLD + " problem.INPUT_THRESHOLD: " + problem.INPUT_THRESHOLD);
					System.exit(1);
				}
				User.sequential(problem,result);

				System.out.println("base case: result before ProcessBaseCase(): " + result);
				WukongProblem.ProcessBaseCase(problem,result,ServerlessNetworkingMemoizer);

				// : At this point, the recursion stops and we begin the Fan-In operations for this leaf node executor.
			} else { // not baseCase
// start Fan-Out task
				ArrayList<ProblemType> subProblems = new ArrayList<ProblemType>();
				User.divide(problem, subProblems);

// end Fan-Out task

// start Fan-Out operation
				// Makes recursive call to run() for one subproblem and a new executor for the other(s).
				// Calls User.computeInputsOfSubproblems(problem,subProblems) when level == ProblemType.INPUT_THRESHOLD
				// and then divides the input of parent into the two inputs of the children. 
				WukongProblem.Fanout(problem, subProblems, ServerlessNetworkingMemoizer);
// end Fan-Out operation

				// After the executor is the first task of a Fan-In, or computes the final result, its recursion unwinds
				// and there is nothing to do on the unwind.
				return;
			} // !baseCase
		}
		else {
			synchronized(FanInSychronizer.getPrintLock()) {
				System.out.println("else in template: For Problem: " + problem + "; Memoized result: " + result);
			}
			memoizedResult = true;
		}
		// All of the executor that were invoked eventually become base case leaf nodes
		// as the recursive calls stop, unless they get a memoized result in which case they
		// start their unwind. At this point, the executor uses the Fan-In stack to perform Fan-In operations
		// as the recursion unwinds (logically).
		// When the final result is obtained, or an executor does not become the fan-in task, the executor
		// can just terminate. (But our Java threads unwind the recursion so their run() terminates.)
		//
		// A leaf node executor that does not get a memoized result does a base case, which is a 
		// call to sequential().For a base case X, no merge() is performed on the children of X, since no 
		// Divide is performed on X; we simply do a Fan-In operation involving base 
		// case X and base case Y = sibling(X) (in the two sibling case).
		//
		// If we are the first executor of a Fan-In, we can unwind our recursion
		// or simply terminate. That is, we have made  0, 1, or more recursive calls to 
		// Divide; when we reach a base case, we do not unwind the recursion; instead, 
		// we let the leaf node executors perform Fan-In operations with the Fan-in stack. At each Fan-in,
		// one executor will stop and one will continue, leaving one executor to compute the final 
		// merge/result.

//: start Fan-In operation and possibly  perform Fan-In task.      

		boolean finalRemainingExecutor = WukongProblem.FanInOperationandTask(problem,result,memoizedResult,
				ServerlessNetworkingMemoizer);
//: end Fan-In operation and Fan-In task.

		// The executor that is the last fan-in task of the final fan-in outputs the result. the
		// result will have been saved in the map with key "root."

		// Need to output the value having key "root"
		if (finalRemainingExecutor) {
			User.outputResult();
		}
		
		return;
	}
}

class WukongProblem {
	// Get input arrays when the level reaches the INPUT_THRESHOLD, e.g., don't grab the initial 256MB array,
	// wait until you reach level , say, 1, when there are two subproblems each half as big.
	// To input at the start of the problem (root), use 0; the stack has 0 elements on it for the root problem.
	// To input the first two subProblems, use 1. Etc.
	//
	// This value should not be changed. If the user provides ProblemType.INPUT_THRESHOLD, we will use that value;
	// otherwise, we use this default value, which causes the root problem to be input.
	// This is the default level for inputting problem. Default is at the root, i.e., level 0
	static int INPUT_THRESHOLD = 0;
	boolean didInput = false;
	// When unwinding recursion, output subProblme results when subProblem sizes reaches OUTPUT_THRESHOLD.
	// However, you must be sure that the entire result will be captured, i.e, that all the results are included
	// in the subproblems that reach threshold. This works for mergeSort, but not for QuickSort. In the latter,
	// you can have one subprogram that is combining itself with single element subproblems so only one subproblem
	// can reach the output threshold; if you stop then, the elements in the other (smaller) subproblems will
	// never be output as these other subproblems are only being combined with the big subproblem.
	// Note: in this case, the large subproblem is a big object so task collapsing should work well in this case.
	//
	// Using MAX_VALUE so the default will be greater than SEQUENTIAL_THRESHOLD. We should never reach level Integer.MAX_VALUE
	// when making recursive calls!
	static int OUTPUT_THRESHOLD = Integer.MAX_VALUE;
	
	static Boolean memoize = false;
	
	// For fan-in use serverless networking where invoked executor sends result to become executor,
	// or use redis-like shared storage to write and read results.
	static final boolean USESERVERLESSNETWORKING = true;
	boolean becomeExecutor = false;
	
	// Identifies the SubProblem; used as a key for Wukong Storage, where the value is a sorted subsegment of the array
	String problemID;
	// Stack of storage keys from root to leaf node (where there is a leaf node for each base case).
	Stack<ProblemType> FanInStack;
	
	// If users supply constants, use them, else use default values.
	static {
		Field IT = null;
		try {
			IT = ProblemType.class.getDeclaredField("INPUT_THRESHOLD");
		} catch (NoSuchFieldException nsfe) {
			// intentionally ignored
		}

		if (IT != null) {
			try {
				INPUT_THRESHOLD =  ProblemType.INPUT_THRESHOLD; //  IT; // .getInt(INPUT_THRESHOLD);
			} catch (Exception e) {
				e.printStackTrace();
			}
		} 
		
		Field OT = null;
		try {
			OT = ProblemType.class.getDeclaredField("OUTPUT_THRESHOLD");
		} catch (NoSuchFieldException nsfe) {
			// intentionally ignored
		}

		if (OT != null) {
			try {
				OUTPUT_THRESHOLD =  ProblemType.OUTPUT_THRESHOLD; //  IT; // .getInt(INPUT_THRESHOLD);
			} catch (Exception e) {
				e.printStackTrace();
			}
		} 
		
		Field m = null;
		try {
			m = ProblemType.class.getDeclaredField("memoize");
		} catch (NoSuchFieldException nsfe) {
			// intentionally ignored
		}

		if (m != null) {
			try {
				memoize =  ProblemType.memoize; //  IT; // .getInt(INPUT_THRESHOLD);
			} catch (Exception e) {
				e.printStackTrace();
			}
		} 

	} // end static

	static void ProcessBaseCase(ProblemType problem, ResultType result, ServerlessNetworkingClientServer ServerlessNetworkingMemoizer) {
		// memoizedResult true means that we got a memoized result (either at the base case or for a non-base case)
		// and we don't want to memoize this result, which would be redundant.

		// Each started executor eventually executes a base case (sequential sort) and then the executor competes
		// with its sibling at a Fan-In to do the Fan0-In task, which is a merge.
		
		// problem is a base case problem that uses sequential(); result is given the same ID
		// Or result was memoized, i.e., some other problem, and we need to update the problemID of result from 
		// the other problem's ID to this problem's ID.
		
		// Need to do this here as the base case sequential() is not required to set resultlabel = problemLabel
		result.problemID = problem.problemID;	

 		// Note: not sending a REMOVEPAIRINGNAME message since the executor is now unwinding and may be sending
 		// messages all the way up.


		// : no need to check !memoizedResult since if memoizedResult we stopped, and even if we did not stop
		// we only compute base case if we don't get memoized result for the base case. (If we get memoized
		// result, we unwind by fan-in to parent of current problem.
		if (WukongProblem.USESERVERLESSNETWORKING && /*!memoizedResult &&*/ WukongProblem.memoize) {
			MemoizationMessage deliverResultMsg = new MemoizationMessage();
			deliverResultMsg.messageType = MemoizationController.MemoizationMessageType.DELIVEREDVALUE;
			deliverResultMsg.senderID = problem.problemID;
			deliverResultMsg.problemOrResultID = result.problemID;
			System.out.println("result: " + result);
			String memoizedLabel = User.memoizeIDLabeler(problem);
			//String memoizedLabel = User.memoizeIDLabeler(result);
			deliverResultMsg.memoizationLabel = memoizedLabel;
			deliverResultMsg.result = result;
			deliverResultMsg.FanInStack = null;
			ServerlessNetworkingMemoizer.send1(deliverResultMsg);

			ResultType ack = ServerlessNetworkingMemoizer.rcv1();
		}
	}

	// Replace with actual Wukong fan-out code
	static void Fanout(ProblemType problem, ArrayList<ProblemType> subProblems, ServerlessNetworkingClientServer ServerlessNetworkingMemoizer) {
		// Retrieve subsegment of array when the threshold is reached. Fanout is only called while 
		// recursive calls are made and the stack is thus growing in size. Fanout is not called 
		// while the recursion is unwinding and the stack is shrinking. Thus, this condition will 
		// not be true twice, i.e., it will be true as the stack is growing but not while the stack is shrinking.
		//
		// Note: If using the size of the subproblem instead of the level, use this if-statement:
		//   if ((problem.to - problem.from + 1) <= WukongProblem.INPUT_THRESHOLD)
		//
		if (problem.FanInStack.size() >= WukongProblem.INPUT_THRESHOLD) 
			User.computeInputsOfSubproblems(problem, subProblems);
		
		// In General:
		// ProblemType becomeSubproblem = chooseBecome(subProblems);
		// where we choose the become subProblem based on the criterion
		// specified by the user. For example:
		// default: choose the last subProlem in list subProblems, which means
		// the user can control the choice by adding to list subProblems in
		// the best order, e.g., the left subProblem is always last
		// size: choose the largest subProblem, e.g., for Mergesort/Fibonaci the 
		// subProblems have equal size, so it does not matter which one is chosen,
		// but for Quickort one subProblem can be much larger than the other, and
		// for all Mergesort/QuickSort/Fibonaci the size of the task's output equals 
		// the size of the task's input.
		//
		// Here, I am just forking the subProblems and choosing the last as the 
		// become subProblem, which is always the left subProblem (not right).
		//
		// Note, for the become subProblem ewe do fork/invoke a new thread, and
		// call become.start, we call become.run().

		//For a fan-out of N, invoke N-1 executors and become 1
		for (int i=0; i< subProblems.size()-1; i++) {
			ProblemType invokedSubproblem = subProblems.get(i);

			// Generate the executor/storage label for the subproblem
			// Supply all: problem, subProblems, subProblem, i in case they are needed.
			String ID = User.problemLabeler(invokedSubproblem,i, problem, 
				subProblems);

			invokedSubproblem.problemID = problem.problemID + "-" + ID;
			//invokedSubproblem.problemID = ID;
			
			invokedSubproblem.becomeExecutor = false;
			
			@SuppressWarnings("unchecked")
			// Add label to stack of labels from root to subproblem
			Stack<ProblemType> childFanInStack = (Stack<ProblemType>) problem.FanInStack.clone(); 
			synchronized(FanInSychronizer.getPrintLock()) {
				System.out.println("fanout: push on childFanInStack: problem: " + problem);
				childFanInStack.push(problem);
				//childFanInStack.push(problem.value);
			}
			invokedSubproblem.FanInStack = childFanInStack;
			
			synchronized(FanInSychronizer.getPrintLock()) {
			System.out.print("fanout: parent stack: ");
			for (int j=0; j< problem.FanInStack.size(); j++) {
				System.out.print(problem.FanInStack.get(j) + " ");
			}
			System.out.println();
			System.out.print("fanout: subProblem stack: ");
			for (int j=0; j< invokedSubproblem.FanInStack.size(); j++) {
				System.out.print(invokedSubproblem.FanInStack.get(j) + " ");
			}
			System.out.println();
			}
			
			// If parent input was done then we will grab part of the parent's values and the child's input can be considered done too.
			invokedSubproblem.didInput = problem.didInput;
			
			if (WukongProblem.memoize && WukongProblem.USESERVERLESSNETWORKING) {
				MemoizationMessage addPairingNameMsgForInvoke = new MemoizationMessage();
				addPairingNameMsgForInvoke.messageType = MemoizationController.MemoizationMessageType.ADDPAIRINGNAME;
				addPairingNameMsgForInvoke.senderID = problem.problemID;
				addPairingNameMsgForInvoke.problemOrResultID = invokedSubproblem.problemID;
				addPairingNameMsgForInvoke.memoizationLabel = null;
				addPairingNameMsgForInvoke.result = null;
				addPairingNameMsgForInvoke.FanInStack = null;
				ServerlessNetworkingMemoizer.send1(addPairingNameMsgForInvoke);
				
				ResultType ack = ServerlessNetworkingMemoizer.rcv1();

			}
			
			// New subproblem
			DivideAndConquerExecutor newExecutor = 
				new DivideAndConquerExecutor(invokedSubproblem);
			synchronized(FanInSychronizer.getPrintLock()) {
				System.out.println("Fanout: ID: " + problem.problemID + " invoking new right executor: "  + invokedSubproblem.problemID);
			}
			// start the executor
			
			// where:
			//enum MemoizationMessageType {ADDPAIRINGNAME, REMOVEPAIRINGNAME, PROMISEDVALUE, DELIVEREDVALUE}
	
			//class MemoizationMessage {
			//	MemoizationController.MemoizationMessageType messageType;
			//	String senderID;
			//	String problemOrResultID;
			//	String memoizationLabel;
			//	ResultType result;
			//	Stack<ProblemType> FanInStack;
			//	boolean becomeExecutor;
			//	boolean didInput;
			//}
			
			newExecutor.start();
			// Note: no joins for the executor threads - when they become leaf node executors, they will perform all of the 
			// combine() operations and then return, which unwinds the recursion with nothing else to do.
		}
		// Do the same for the become executor
		ProblemType becomeSubproblem = subProblems.get(subProblems.size()-1);
		
		// Generate the executor/storage label for the subproblem
		// Supply all: problem, subProblems, subProblem, i in case they are needed.
		String ID = User.problemLabeler(becomeSubproblem,subProblems.size()-1, problem, 
			subProblems);
		
		// If two different subProblems in the divide and conquer tree can have the same label, then there would
		// be a problem since these labels are also used to label fan-in tasks and we cannot have two fan-in tasks
		// with the same label. We make sure labels are unique by using the path of labels from the root to this 
		// subproblem, e.g., instead of "3" and "2" we use "4-3" and "4-2" where "4" is the label of the 
		// root and subProblem's "#" and "4" are children of the root (as in Fibonacci).
		becomeSubproblem.problemID = problem.problemID + "-" + ID;

		becomeSubproblem.becomeExecutor = true;
		
		// No need to clone the problem stack for the become subproblem as we clones the stacks for the other subProblems  
		//@SuppressWarnings("unchecked")
		//Stack<ProblemType> childFanInStack = (Stack<ProblemType>) problem.FanInStack.clone();
		Stack<ProblemType> childFanInStack = (Stack<ProblemType>) problem.FanInStack;  
		childFanInStack.push(problem);
		becomeSubproblem.FanInStack = childFanInStack;
		
		// If parent input was done then we will grab part of the parent's values and the child's input can be considered done too.
		becomeSubproblem.didInput = problem.didInput;
		DivideAndConquerExecutor become = 
			new DivideAndConquerExecutor(becomeSubproblem);
		synchronized(FanInSychronizer.getPrintLock()) {
			System.out.println("Fanout: ID: " + problem.problemID  + " becoming left executor: "  + becomeSubproblem.problemID);
		}
		
		if (WukongProblem.memoize && WukongProblem.USESERVERLESSNETWORKING) {

			MemoizationMessage addPairingNameMsgForBecomes = new MemoizationMessage();
			addPairingNameMsgForBecomes.messageType = MemoizationController.MemoizationMessageType.ADDPAIRINGNAME;
			addPairingNameMsgForBecomes.senderID = problem.problemID;
			addPairingNameMsgForBecomes.problemOrResultID = becomeSubproblem.problemID;
			addPairingNameMsgForBecomes.memoizationLabel = null;
			addPairingNameMsgForBecomes.result = null;
			addPairingNameMsgForBecomes.FanInStack = null;
			ServerlessNetworkingMemoizer.send1(addPairingNameMsgForBecomes);
			
			ResultType ack1 = ServerlessNetworkingMemoizer.rcv1();

			MemoizationMessage removePairingNameMsgForParent = new MemoizationMessage();
			removePairingNameMsgForParent.messageType = MemoizationController.MemoizationMessageType.REMOVEPAIRINGNAME;
			removePairingNameMsgForParent.senderID = problem.problemID;
			removePairingNameMsgForParent.problemOrResultID = problem.problemID;
			removePairingNameMsgForParent.memoizationLabel = null;
			removePairingNameMsgForParent.result = null;
			removePairingNameMsgForParent.FanInStack = null;
			ServerlessNetworkingMemoizer.send1(removePairingNameMsgForParent);
			
			ResultType ack2 = ServerlessNetworkingMemoizer.rcv1();
		}

		
		// No need to start another Executor, current executor can recurse until the base case. At that point, the executor
		// will compete at a Fan-In point to do a merge with its "sibling" executor.
		//       1-2
		//      /   \
		//     1     2    // Executors 1 and 2 have a Fan-In at 1-2. One of executors 1 or 2 will merge input 1 and input 2 
		// and the other will stop by executing return, which will unwind its recursion. 
		become.run();  
		
		// Problem was pushed on subProblem stacks and stacks will be passed to invoked Executor. Here, user has a chance to 
		// trim any data in problem that is no longer needed (e.g., input array of problem) so less data is passed to 
		// Executor.
		
		// Option: create method WukongProblem.trimProblem() that sets WukongProblem.FanInStack.null and call 
		// WukongProblem.trimProblem() here before calling User.trimProblem(problem); I believe that problem's
		// FanInStack is no longer needed and can always be trimmed, though it might be helpful for debugging.
		User.trimProblem(problem);
		//: end Fan-Out operation

		// Recursion unwinds - nothing happens along the way.
		return;
	}
	
	// The last executor to fan-in will become the Fan-In task executor. The actual executor needs to save its Merge output 
	// and increment/set counter/boolean. We handle this  here by using map.put(key,value), which returns the previous 
	// value the key is mapped to, if any, and null if not. So if put() returns null we are not the last executor to Fan-in. 
	// This is a special case for MergeSort, which always has only two Fan-In executors.
	static private boolean isLastFanInExecutor(ProblemType parentProblem, ResultType result, ArrayList<ResultType> subproblemResults)  {
		// store result and check if we are the last executor to do this.
		// Add all of the other subproblem results to subproblemResults. We will add our sibling result later (before we call combine().)
		String FanInID = parentProblem.problemID;

		ResultType copyOfResult = result.copy();
		copyOfResult.problemID = result.problemID;
		
		synchronized(FanInSychronizer.getPrintLock()) {
			System.out.println("isLastFanInExecutor: Writing to " + FanInID + " the value " + copyOfResult); // result);
		}
		ResultType siblingResult = FanInSychronizer.resultMap.put(FanInID,result); 
		
		// firstFanInResult may be null
		if (siblingResult == null) {
			return false;
		} else {	
			ResultType copyOfSiblingResult = siblingResult.copy();
			copyOfSiblingResult.problemID = siblingResult.problemID;
			
			subproblemResults.add(copyOfSiblingResult);
			return true;
		}
	}

	// Perform Fan-in and possibly the Fan-in task 
	static boolean FanInOperationandTask(ProblemType problem, ResultType result, boolean memoizedResult,
			ServerlessNetworkingClientServer ServerlessNetworkingMemoizer) {
		// memoizedResult true means that we got a memoized result (either at the base case or for a non-base case)
		// and we don't want to memoize this result, which would be redundant.
		
//: start Fan-In operation

		synchronized(FanInSychronizer.getPrintLock()) {
			System.out.println("**********************Start Fanin operation:");
			System.out.println("Fan-in: ID: " + problem.problemID);
			System.out.println("Fan-in: becomeExecutor: " + problem.becomeExecutor);
			System.out.print("Fan-in: FanInStack: ");
			for (int i=0; i<problem.FanInStack.size(); i++)
				System.out.print(problem.FanInStack.get(i) + " ");
			System.out.println();
			System.out.flush();
		}
		// Each started executor eventually executes a base case (sequential sort) or gets a memoized
		// result for a duplicate subProblem, and then the executor competes with its sibling(s) at a Fan-In 
		// to do the Fan0-In task, which is a combine().
		
		// true if this is "become" rather than "invoke" problem. Set below.
		boolean FanInExecutor = false;	
		
		// For USESERVERLESSNETWORKING, whether a subProblem is become or invoked is decided when the problem is created.
		// If we instead determine at runtime which problem is the last to fan-in, then the last to fan-in is 
		// the become, and the others are invoke.
		if (WukongProblem.USESERVERLESSNETWORKING)
			FanInExecutor = problem.becomeExecutor;
 
		while (problem.FanInStack.size() != 0) {
			
			// Stop combining results when the results reach a certain size, and the communication delay for passing
			// the results is much larger than the time to combine them. The remaining combines can be done on one
			// processor. This is task collapsing of the remaining combine() tasks into one task.
			// However, we can only do this if we are sure all of the subproblems will reach the threshold. For example,
			// this works for mergeSort as sibling subproblems are all growing in size, more or less equally. But this
			// is not true for quicksort, which may have only one large subproblem that reaches the threshold as the 
			// other subproblems are small ones that get merged with the large one.

			//if (size >= WukongProblem.OUTPUT_THRESHOLD) { 
			if (problem.FanInStack.size() == WukongProblem.OUTPUT_THRESHOLD) { 
				synchronized(FanInSychronizer.getPrintLock()) {
					System.out.println("Exector: " + problem.problemID + " Reached OUTPUT_THRESHOLD for result: " + result.problemID 
						+ " with problem.FanInStack.size(): " + problem.FanInStack.size()
						+ " and WukongProblem.OUTPUT_THRESHOLD: " + WukongProblem.OUTPUT_THRESHOLD);
					System.out.println(result); 
					// return false so we are not considered to be the final Executor that displays final results. There will
					// be many executors that reach the OUPUT_THRESHOLD and stop.
					return false;
				}
			}

			ProblemType parentProblem = problem.FanInStack.pop();
			
			synchronized(FanInSychronizer.getPrintLock()) {
				System.out.println("Fan-in: ID: " + problem.problemID + " parentProblem ID: " + parentProblem.problemID);
				System.out.println("Fan-in: ID: " + problem.problemID + " problem.becomeExecutor: " + problem.becomeExecutor + " parentProblem.becomeExecutor: " + parentProblem.becomeExecutor);
				System.out.println();
				System.out.flush();
			}
			// The last task to fan-in will become the Fan-In task executor. The actual executor needs to save its Merge output 
			// and increment/set counter/boolean. We handle this in our prototype here by using map.put(key,value), which 
			// returns the previous value the key is mapped to, if there is one, and null if not. So if put() returns null 
			// we are not the last executor to Fan-in.This is a special case for MergeSort, which always has only two 
			// Fan-In tasks.
			//
			// Both siblings write result to storage using key FanInID, The second sibling to write will get the 
			// first sibling's value in previousValue. Now the second sibling has both values.
			
			ArrayList<ResultType> subproblemResults = new ArrayList<ResultType>();

			if (WukongProblem.USESERVERLESSNETWORKING) {
				String FanIn = parentProblem.problemID;
				if (FanInExecutor) {
					System.out.println("ID: " + problem.problemID + ": FanIn: " + parentProblem.problemID + " was FanInExecutor: starting receive.");
					ServerLessNetworkingUniReceiverHelper h = new ServerLessNetworkingUniReceiverHelper(FanIn);
					h.start();
					try {
						h.join();
					} catch (Exception e) {
						e.printStackTrace();
					}
					// r is a copy of sent result
					ResultType copyOfSentResult = h.result;
					subproblemResults.add(copyOfSentResult);
					synchronized(FanInSychronizer.getPrintLock()) {
						System.out.println("ID: " + problem.problemID + ": FanIn: " + parentProblem.problemID + " was FanInExecutor: result received:" + copyOfSentResult);
					}
				} else {
					// pair and send message
					ServerLessNetworkingUniSenderHelper h = new ServerLessNetworkingUniSenderHelper(FanIn,result);
					h.start();
					try {
						h.join(); // not really necessary
					} catch (Exception e) {
					}
					synchronized(FanInSychronizer.getPrintLock()) {
						System.out.println("Fan-In: ID: " + problem.problemID + ": FanInID: " + parentProblem.problemID + " was not FanInExecutor:  result sent:" + result);
					}
				}
			}
			else  {
				// When return we either have our result and sibling result or or our result and null. For latter, we were first
				// Executor to fan-in so we stop.
				FanInExecutor = isLastFanInExecutor(parentProblem, result, subproblemResults);
			}
			
			// If we are not the last task to Fan-In then unwind recursion and we are done
		
			if (!FanInExecutor) {
				
				synchronized(FanInSychronizer.getPrintLock()) {
					System.out.println("Fan-In: ID: " + problem.problemID + ": FanInID: " + parentProblem.problemID + ": is not become Executor "
						+ " and its value was: " + result + " and after put is " + (FanInSychronizer.resultMap.get(parentProblem.problemID)));
					System.out.flush();
				}
				
				//if (problem.FanInStack.size() < WukongProblem.OUTPUT_THRESHOLD) {
				//	synchronized(FanInSychronizer.getPrintLock()) {
				//		System.out.println("Exector: !lastFanInExecutor: " + problem.problemID + " Reached OUTPUT_THRESHOLD for result: " + result.problemID 
				//			+ " with problem.FanInStack.size(): " + problem.FanInStack.size()
				//			+ " and WukongProblem.OUTPUT_THRESHOLD: " + WukongProblem.OUTPUT_THRESHOLD);						System.out.println(result); 
				//		// return false so we are not considered to be the final Executor that displays final results. There will
				//		// be many executors that reach the OUPUT_THRESHOLD and stop.
				//		return false;
				//	}
				//}
				
				//if (size >= WukongProblem.OUTPUT_THRESHOLD) { 
				if (problem.FanInStack.size() == WukongProblem.OUTPUT_THRESHOLD) { 
					synchronized(FanInSychronizer.getPrintLock()) {
						System.out.println("Exector: " + problem.problemID + " Reached OUTPUT_THRESHOLD for result: " + result.problemID 
							+ " with problem.FanInStack.size(): " + problem.FanInStack.size()
							+ " and WukongProblem.OUTPUT_THRESHOLD: " + WukongProblem.OUTPUT_THRESHOLD);
						System.out.println(result); 
						// return false so we are not considered to be the final Executor that displays final results. There will
						// be many executors that reach the OUPUT_THRESHOLD and stop.
						return false;
					}
				}
				
				if (WukongProblem.memoize && WukongProblem.USESERVERLESSNETWORKING) {
					MemoizationMessage removePairingNameMsgForParent = new MemoizationMessage();
					removePairingNameMsgForParent.messageType = MemoizationController.MemoizationMessageType.REMOVEPAIRINGNAME;
					removePairingNameMsgForParent.senderID = problem.problemID;
					removePairingNameMsgForParent.problemOrResultID = problem.problemID;
					removePairingNameMsgForParent.memoizationLabel = null;
					removePairingNameMsgForParent.result = null;
					removePairingNameMsgForParent.FanInStack = null;
					ServerlessNetworkingMemoizer.send1(removePairingNameMsgForParent);
					ResultType ack = ServerlessNetworkingMemoizer.rcv1();
				}
				
				// Unwind recursion but real executor could simply terminate instead.
				return false;
			}
			else { // we are last Executor and first executor's result is in previousValue.

				synchronized(FanInSychronizer.getPrintLock()) {
					System.out.println("FanIn: ID: " + problem.problemID + ": FanInID: " + parentProblem.problemID + ": " 
						+ ": Returned from put: executor isLastFanInExecutor ");
					System.out.println(subproblemResults.get(0));
					System.out.flush();
               
					System.out.println("ID: " + problem.problemID + ": call combine ***************");
					System.out.flush();
				}
					// combine takes the result for this executor and the results for the sibling subproblems obtained by
					// the sibling executors to produce the result for this problem.
					
					// Add the result for this executor to the subproblems. 
					subproblemResults.add(result);
					
					// When not using ServerLessNetworking:
					// Above, we checked if we are last Executor by putting result in map, which, if we are last executor
					// left result in map and returns the sibling result, which was first. So it is result that is sitting
					// in the map. Now combine adds this result and the sibling's subProblem result, and 
					// stores the result of add as
//: end Fan-In operation
					
// : start Fan-In task 
					User.combine(subproblemResults, result);
					
					// Note: It's possible that we got a memoized value, e.g., 1 and we added 1+0 to get 1 and we are now
					// memoizing 1, which we do not need to do. 
					// Option: Track locally the memoized values we get and put so we don't put duplicates, since get/put is expensive
					// when memoized storage is remote.
										
					if (WukongProblem.memoize) {
						String memoizedLabel = User.memoizeIDLabeler(parentProblem);
						// put will memoize a copy of result
						// : store result with subProblem
						ResultType memoizationResult = FanInSychronizer.put(memoizedLabel,result);
						synchronized(FanInSychronizer.getPrintLock()) {
							System.out.println("Exector: result.problemID: " + result.problemID + " put memoizedLabel: " + memoizedLabel
								+ " result: " + result);
						}
					}
					
					if (WukongProblem.USESERVERLESSNETWORKING && WukongProblem.memoize) {
						MemoizationMessage deliverResultMsg = new MemoizationMessage();
						deliverResultMsg.messageType = MemoizationController.MemoizationMessageType.DELIVEREDVALUE;
						deliverResultMsg.senderID = problem.problemID;
						deliverResultMsg.problemOrResultID = result.problemID;
						String memoizedLabel = User.memoizeIDLabeler(parentProblem);
						deliverResultMsg.memoizationLabel = memoizedLabel;
						deliverResultMsg.result = result;
						deliverResultMsg.FanInStack = null;
						ServerlessNetworkingMemoizer.send1(deliverResultMsg);
						
						ResultType ack = ServerlessNetworkingMemoizer.rcv1();
					}
					
					if (WukongProblem.USESERVERLESSNETWORKING) {
						String FanInID = parentProblem.problemID;
						if (FanInID.equals("root")) {
							synchronized(FanInSychronizer.getPrintLock()) {
								System.out.println("Executor: Writing the final value to root: " + result); // result);
							}
							ResultType siblingResult = FanInSychronizer.resultMap.put("root",result); 
						}

						FanInExecutor = parentProblem.becomeExecutor;
					}
					// This executor continues to do Fan-In operations with the new problem result.
				
			} // end we are second executor
// : end Fan-In task 

			// Instead of doing all of the work for sorting as we unwind the recursion and call merge(),
			// we let the executors "unwind" the recursion using the explicit FanIn stack.

		} // end while (stack not empty)
		
		// Assuming that we are done with all problems and so done talking to Memoization Controller
		if (WukongProblem.memoize && WukongProblem.USESERVERLESSNETWORKING) {
			MemoizationMessage removePairingNameMsgForParent = new MemoizationMessage();
			removePairingNameMsgForParent.messageType = MemoizationController.MemoizationMessageType.REMOVEPAIRINGNAME;
			removePairingNameMsgForParent.senderID = problem.problemID;
			removePairingNameMsgForParent.problemOrResultID = problem.problemID;
			removePairingNameMsgForParent.memoizationLabel = null;
			removePairingNameMsgForParent.result = null;
			removePairingNameMsgForParent.FanInStack = null;
			ServerlessNetworkingMemoizer.send1(removePairingNameMsgForParent);
			ResultType ack = ServerlessNetworkingMemoizer.rcv1();
		}
		
		// Only the final executor, i.e., the last executor to execute the final Fan-in task, makes it to here.
		return true;
	}
}

class WukongResult {
	// Identifies the SubProblem; used as a key for Wukong Storage, where the value is a sorted subsegment of the array
	String problemID;
}

class FanInSychronizer {
	// Used to serialize debug print statements
	public static Object printLock = new Object();
	// WuKong Storage
	public static ConcurrentHashMap<String,ResultType> resultMap = new ConcurrentHashMap<String, ResultType>();
	public static ConcurrentHashMap<String,ResultType> memoizationMap = new ConcurrentHashMap<String, ResultType>();
	
	/*
	 * Put key-value in result map for determining which Executor is last to fan-in and get the result of first Executor to fan-in
	*/
	public static ResultType put(String FanInID, ResultType result) {
		// put memoized copy of result
		
		ResultType copyOfResult = result.copy();
		copyOfResult.problemID = result.problemID;
		
		// returns previous memoized value mapped to key, if any; otherwise null.
		ResultType returnValue = memoizationMap.put(FanInID,copyOfResult); 
		
		// return copy of returnValue, if any
		ResultType copyOfReturn = null;
		if (returnValue != null) {
			copyOfReturn = result.copy();
			copyOfReturn.problemID = result.problemID;
			
		}
		return copyOfReturn;
	}
	
	/*
	 * Get value for key from memoization map. Simulates getting the result for any problem that was stored earlier.
	 * Gets a copy of memoized result.
	*/
	public static ResultType get(String FanInID) {
		ResultType result = memoizationMap.get(FanInID);
		ResultType copy = null;
		if (result != null) {
			copy = result.copy();
			copy.problemID = result.problemID;
		}
		return copy;
	}
	
	synchronized public static Object getPrintLock()  
	  { 
	    if (printLock == null)  
	    { 
	      // if instance is null, initialize 
	    	printLock = new Object(); 
	    } 
	    return printLock; 
	  } 
}

/* 
 * ServerLessNetworkingReceiverHelper SNHelper = new ServerLessNetworkingReceiverHelper(pairingName);
 * SNHelper.start();
 * SNHelper.join();
 * ResultType result = SNHelper.getResult();
 * 
 * ServerLessNetworkingSenderHelper SNHelper = new ServerLessNetworkingSenderHelper(pairingName,result);
 * SNHelper.start();
 */
class ServerLessNetworkingUniReceiverHelper extends Thread {
	String pairingName;
	ResultType result;
	ServerLessNetworkingUniReceiverHelper(String pairingName) {
		this.pairingName = pairingName;
	}
	ResultType getResult() {
		return result;
	}
	public void run() {
		UniChannel queue = ServerlessNetwork.pair_rcv(pairingName);
		try {
			result = queue.rcv();
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
}

class ServerLessNetworkingUniSenderHelper extends Thread {
	String pairingName;
	ResultType result;
	ServerLessNetworkingUniSenderHelper(String pairingName, ResultType result) {
		this.pairingName = pairingName;
		this.result = result;
	}
	public void run() {
		UniChannel queue = ServerlessNetwork.pair_send(pairingName);
		try {
			queue.send(result);
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
}

/*
 *  Unidirectional Channel 
 *    send ---------------> rcv
 *    Note: Working directly with references to sent message. If this is not desired, someone should make\
 *    a copy of the message, either the sender or the receiver or the ultimate destination of the message,
 *    in our case, the memoization map.
 */
class UniChannel {
	String ID;
	LinkedBlockingQueue<ResultType> queue = new LinkedBlockingQueue<ResultType>(1);
	UniChannel(String ID) {
		this.ID = ID;
	}
	void send(ResultType result) {
		queue.add(result);
	}
	
	ResultType rcv() {
		ResultType result = null;
		try {
			result = queue.take();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
		return result;
	}
}

/*
 *  Bidirectional Channel - one side does send1 and receive1 and the other side does send2 and receive2
 *    send1 ---------------> rcv2
 *     rcv1 <--------------- send2
 *  
 *  Uses two LinkedBlockingQueues:
 *  An optionally-bounded blocking queue based on linked nodes. This queue orders elements FIFO 
 *  (first-in-first-out). The head of the queue is that element that has been on the queue the longest time. 
 *  The tail of the queue is that element that has been on the queue the shortest time. New elements are inserted 
 *  at the tail of the queue, and the queue retrieval operations obtain elements at the head of the queue. Linked 
 *  queues typically have higher throughput than array-based queues but less predictable performance in most concurrent 
 *  applications.
 *  
 *  Note: Working directly with references to sent message. If this is not desired, someone should make\
 *  a copy of the message, either the sender or the receiver or the ultimate destination of the message,
 *  in our case, the memoization map.
 */
class BiChannel<MsgType> {
	String ID;
	// Creates a LinkedBlockingQueue with a capacity of Integer.MAX_VALUE.
	LinkedBlockingQueue<MsgType> queue1 = new LinkedBlockingQueue<MsgType>();
	LinkedBlockingQueue<MsgType> queue2 = new LinkedBlockingQueue<MsgType>();
	BiChannel(String ID) {
		this.ID = ID;
	}
	void send1(MsgType result) {
		queue1.add(result);
	}
	
	MsgType rcv1() {
		MsgType result = null;
		try {
			result = queue2.take();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
		return result;
	}
	
	void send2(MsgType result) {
		queue2.add(result);
	}
	
	MsgType rcv2() throws InterruptedException {
		MsgType result = null;
		try {
			result = queue1.take();
		} 
		catch (InterruptedException e) {
			//e.printStackTrace();
			throw new InterruptedException();
		}
		catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
		return result;
	}
}

/* In general, use UDTEpoll; here we assume Divide and Conquer with divide into two subProblems.
 * 
*/
class ServerlessNetwork {
	public static ConcurrentHashMap<String,Semaphore> pairMap = new ConcurrentHashMap<String, Semaphore>();
	public static ConcurrentHashMap<String,UniChannel> channelMap = new ConcurrentHashMap<String, UniChannel>();

	static UniChannel pair_rcv(String pairingName) {
		Semaphore receiverSemaphore = new Semaphore(0);
		
		// See who does put() first - sender or receiver. If senderSemaphore is null then receiver did put() first.
		// Execute release on the semaphore that was put() first.
		Semaphore senderSemaphore = pairMap.putIfAbsent(pairingName, receiverSemaphore);
		
		// queue will be used by the sender-receiver pair to send-receive one message
		UniChannel queue = null;
		
		try {
			if (senderSemaphore == null) {
				// Sender does put() after receiver; acquire receiver semaphore (which completes after sender releases it)
				receiverSemaphore.acquire();
			}
			else {
				// receiver put() before sender, acquire sender semaphore (which completes after sender releases it)
				senderSemaphore.acquire();
			}
			// Sender created queue before it released the semaphore
			queue = channelMap.get(pairingName);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return queue;
	}
	
	static UniChannel pair_send(String pairingName) {
		// Semaphore used to make receiver block until sender creates message queue available for send/receive
		Semaphore senderSemaphore = new Semaphore(0);
		
		// sender and receiver will share this queue for one send-receive. 
		UniChannel queue = new UniChannel(pairingName);
		channelMap.put(pairingName, queue);
		
		// queue is ready, notify receiver
		// See who does put() first - sender or receiver. If receiverSemaphore is not null then receiver did put() first.
		// Execute release on the semaphore that was put() first.
		Semaphore receiverSemaphore = pairMap.putIfAbsent(pairingName, senderSemaphore);
		
		try {
			if (receiverSemaphore == null) {
				// Sender does put() before receiver, so release sender semaphore as queue is ready to be used by receiver
				senderSemaphore.release();
			}
			else {
				// Receiver did put before sender, so release receiver semaphore as queue is ready to be used by receiver
				receiverSemaphore.release();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return queue;
	}
	
	class testSender extends Thread {
		public void run() {
			ResultType r = new ResultType();
			
//Note: this only works for Fibonacci, when r.value is defined.	
			//r.value = 1;
			//r.problemID = "testSender";
			
			// pair and send message
			ServerLessNetworkingUniSenderHelper h = new ServerLessNetworkingUniSenderHelper("foo",r);
			h.start();
		}
		
	}
	
	class testReceiver extends Thread {
		public void run() {
			// pair and wait for message to be sent
			ServerLessNetworkingUniReceiverHelper h = new ServerLessNetworkingUniReceiverHelper("foo");
			h.start();
			try {
				h.join();
			} catch (Exception e) {
				e.printStackTrace();
			}
			ResultType r = h.result;
			System.out.println("testReceiver:  result:" + r);
		}
	}
	
	public static void main(String[] args) {
		//Test ServerlessNetwork
		ServerlessNetwork N = new ServerlessNetwork();
		testSender sender = N.new testSender();
		testReceiver receiver = N.new testReceiver();
		receiver.start();
		sender.start();
		try {
			sender.join();
			receiver.join();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}

// Many clients can send messages to one server using Bidirectional Channel, the server can reply
// to clients using one UniChannel per client.
class ServerlessNetworkingClientServer {
	//One of these for all clients - clients end and Memoization Controller receives
	BiChannel<MemoizationMessage> connections;
	// one of these for each client to receive replies from the Memoization Controller
	UniChannel clientChannel;
	
	ServerlessNetworkingClientServer(BiChannel<MemoizationMessage> connections, UniChannel clientChannel) {
		this.connections = connections;
		this.clientChannel = clientChannel;
	}
	
	void send1(MemoizationMessage msg) {
		// pass msg on to BiChannel send
		connections.send1(msg);
	}
	
	ResultType rcv1() {
		// rcv from client's UniChannel
		return clientChannel.rcv();
	}
	
	// Not used - messages sent to client using their personal UniChannel
	//void send2(MemoizationMessage msg) {
	//	connections.send2(msg);
	//}
	
	// Not used - Memoization Controller receives directly from BiChannel 
	//MemoizationMessage rcv2() throws InterruptedException {
	//	MemoizationMessage msg = connections.rcv2();
	//	return msg;
	//}
}

class MemoizationMessage {
	MemoizationController.MemoizationMessageType messageType;
	String senderID;
	String problemOrResultID;
	String memoizationLabel;
	ResultType result;
	Stack<ProblemType> FanInStack;
	boolean becomeExecutor;
	boolean didInput;
}

class MemoizationRecord {
	MemoizationController.MemoizationRecordType type; // enum MemoizationRecordType {PROMISEDVALUE,DELIVEREDVALUE}
	String resultID; // e.g., "4-2-1"
	String memoizationLabel; // e.g., "1" as in subProblem Fib(1)
	ResultType result; // memoized subproblem result
	ArrayList<promisedResult> promisedResults; // stack used to unwind recursion after getting memoized result
	ArrayList<String> promisedResultsTemp; // stack used to unwind recursion after getting memoized result
	public String toString() {
		StringBuffer b = new StringBuffer();
		b.append("type: " + type);
		return b.toString();
	}
}

class promisedResult {
	String problemOrResultID;
	boolean becomeExecutor;
	boolean didInput;
	Stack<ProblemType> FanInStack;
	promisedResult(String problemOrResultID, Stack<ProblemType> FanInStack, boolean becomeExecutor,
		boolean didInput) {
		this.FanInStack = FanInStack;
		this.problemOrResultID = problemOrResultID;
		this.becomeExecutor = becomeExecutor;
		this.didInput = didInput;
	}
}

class MemoizationController {
	private static final Object classLock = MemoizationController.class;
	// Singleton
	private static MemoizationController instance = null;
	enum MemoizationRecordType {PROMISEDVALUE,DELIVEREDVALUE}
  
	HashSet<String> pairingNames = new HashSet<String>();
	String initialPairingName = new String(DivideandConquerFibonacci.rootProblemID);
	// connections will be used by the sender-receiver pairs to send-receive messages.
	BiChannel<MemoizationMessage> BiChannelForMemoization = new BiChannel<MemoizationMessage>("MemoizationController");
	
	//BiChannel<MemoizationMessage> connections = new BiChannel<MemoizationMessage>("MemoizationController");
	HashMap<String,MemoizationRecord> memoizationRecords = new HashMap<String,MemoizationRecord>();
	public ConcurrentHashMap<String,UniChannel> channelMap = new ConcurrentHashMap<String,UniChannel>();
	
	boolean alive = true; // stop when final result is obtaine dand alive set to false;
	MemoizationThread myThread; // Memoization Controller runs, like a Lambda
	
	public static ResultType nullResult = new ResultType(); // serves as an ack
	public static ResultType stopResult = new ResultType(); // serves as an ack
	
	MemoizationController() {
		pairingNames.add(initialPairingName);
		myThread = new MemoizationThread();
		myThread.start();
	}
	
	ServerlessNetworkingClientServer pair(String pairingName) {
		// Add pairing name and send clients a ServerlessNetworkingClientServer for communication with Memoization Controller
		// Assert:
		if (!pairingNames.contains(pairingName)) {
			System.out.println("Internal Error: MemoizationController: Sender: pairing but receiver does not have pairingName " + pairingName);
			System.exit(1);
		}
		UniChannel clientChannel = new UniChannel("pairingName");
		channelMap.put(pairingName, clientChannel);
		synchronized(FanInSychronizer.getPrintLock()) {
			System.out.println("MemoizationController: pair: " + pairingName);
		}
		synchronized(FanInSychronizer.getPrintLock()) {
			System.out.println("channelMap keySet:");
			for (String name : channelMap.keySet())  {
				System.out.println(name);
			}
		}
		clientChannel.send(nullResult);
		ServerlessNetworkingClientServer connections = new ServerlessNetworkingClientServer(BiChannelForMemoization, clientChannel);
		return connections;
	}

	/*
	// Executors must call pair before they can send/receive messages on their BiChannel. 
	ResultType getReturnValue(String problemOrResultID) {
		synchronized(FanInSychronizer.getPrintLock()) {
			System.out.println("getReturnValue: problemOrResultID: " + problemOrResultID);
			System.out.println("keySet");
			for (String name : channelMap.keySet())  {
				System.out.println(name);
			}
		}
		LinkedBlockingQueue<ResultType> returnValueChannel = channelMap.get(problemOrResultID);
		if (returnValueChannel == null) {
			synchronized(FanInSychronizer.getPrintLock()) {
			System.out.println("getReturnValue: returnValueChannel is null.");
			}
		}
		ResultType result = null;
		try {
			result = returnValueChannel.take();
		} catch (Exception e) {

			e.printStackTrace();
			try {Thread.sleep(1000);} catch (InterruptedException e1) {}
			System.exit(1);
		}
		synchronized(FanInSychronizer.getPrintLock()) {
		System.out.println("MemoizationController: getReturnValue: return  " + result + " to " + problemOrResultID);
		//System.out.println("MemoizationController: remove from channelMap: " + problemOrResultID);

		}
		// Only one return message.
// 1:
		//channelMap.remove(problemOrResultID);
		return result;
	}
	*/
	
	void stopThread() { // called when final result is obtained for Divide and Conquer
		alive = false;
		myThread.interrupt(); // thread may be waiting on a blocking receive
	}
	
	void deliver(ArrayList<promisedResult> promisedResults) {
		// create a new Executor to handle the promised result.
		synchronized(FanInSychronizer.getPrintLock()) {
		System.out.println("Deliver starting Executors for promised Results:");
		
		for (int i=0; i<promisedResults.size(); i++) {
			promisedResult Executor = promisedResults.get(i);
			UniChannel queueDeliver= channelMap.get(Executor.problemOrResultID);
			ProblemType problem = new ProblemType();
			problem.problemID = Executor.problemOrResultID;
			problem.FanInStack = Executor.FanInStack;
			problem.becomeExecutor = Executor.becomeExecutor;
			problem.didInput = Executor.didInput;
			DivideAndConquerExecutor newExecutor = 	new DivideAndConquerExecutor(problem);
			
			synchronized(FanInSychronizer.getPrintLock()) {
				System.out.println("Deliver starting Executor for: " + problem.problemID
					+ " problem.becomeExecutor: " + problem.becomeExecutor 
					+ " problem.didInput: " + problem.didInput);
			}

//: 4 - comment start
			newExecutor.start();

		}
		synchronized(FanInSychronizer.getPrintLock()) {
			System.out.println("Deliver end promised Results:");
		}
		promisedResults.clear();
	}
}
	
	void deliverTemp(ArrayList<String> promisedResults) {
		// create a new Executor to handle the promised result.
		for (int i=0; i<promisedResults.size(); i++) {
			String waitingExecutor = promisedResults.get(i);
			UniChannel queueDeliver= channelMap.get(waitingExecutor);
			synchronized(FanInSychronizer.getPrintLock()) {
				System.out.println("DeliverTemp sending ack to: " + waitingExecutor);
			}
			queueDeliver.send(nullResult);
		}
		promisedResults.clear();
		
		// Need to send pairing names for new executors.
		// When executor stops, need to lean up its pairing names, close connection, etc.
		//
		// Note that leaf node executors never promise anything they just deliver.
		// The last promise is for the base case; this could be a duplicate promise, which means the lowest
		// that an executor can start is at the parent of a base case; otherwise, start is even higher
		// But Fan in begins with doing something with the results of the base case then enter parent loop.
		// Do we need to start with parent loop, i.e., put base case stuff before the 
	}
	
	public static MemoizationController getInstance( ) {
		synchronized(classLock) {
			if (instance == null) {
        		instance = new MemoizationController();    		
        	}
		}
		return instance;
   }
	
	enum MemoizationMessageType {ADDPAIRINGNAME, REMOVEPAIRINGNAME, PROMISEVALUE, DELIVEREDVALUE, PAIR}

	class MemoizationThread extends Thread {
		public void run() {
			while (alive) {
				MemoizationMessage msg = null;
				try {
					msg = BiChannelForMemoization.rcv2();
				} catch (InterruptedException e) {
					synchronized(FanInSychronizer.getPrintLock()) {
						System.out.println("MemoizationThread: Interrupted: returning.");
						System.out.println();
						System.out.println();
					}
					// return when interrupted
					return;
				}
				switch (msg.messageType) {
				// Note: this is no "PAIR" message. a parent sends ADDPAIRINGNAME to the MEMOIZATION 
				// Controller for each of its children and then REMOVEPAIRINGNAME for itself.
				// The children than call pair() method, and PROMISEVALUE and DELIVERVALUE as appropriate,
				// and ADDPAIRINGNAME for their children, and so on. The Executors that unwind call
				// REMOVEPAIRINGMESSAGE as they stop at a fan-in (becuase they are not the become exector.)
				// 
				// REMOVEPAIRINGMESSAGE is like ServerlessNetworking close().
				case PAIR:
					// Assert:
					if (!pairingNames.contains(msg.problemOrResultID)) {
						System.out.println("Internal Error: MemoizationController: Sender: pairing but receiver does not have pairingName " + msg.problemOrResultID);
						System.exit(1);
					}
					synchronized(FanInSychronizer.getPrintLock()) {
						System.out.println("MemoizationController: pair: " + msg.problemOrResultID);
					}
					UniChannel queuePair = channelMap.get(msg.problemOrResultID);
					queuePair.send(nullResult);
					break;
				
				case ADDPAIRINGNAME: 
					// Assert: 
					if (pairingNames.contains(msg.problemOrResultID)) {
						System.out.println("Internal Error: MemoizationThread: Adding a pairing name that already exists.");
						System.exit(1);
					}
					pairingNames.add(msg.problemOrResultID);
					synchronized(FanInSychronizer.getPrintLock()) {
						System.out.println("pairing names after add");
						for (String name : pairingNames)  {
							System.out.println(name);
						}
					}
					synchronized(FanInSychronizer.getPrintLock()) {
						System.out.println("MemoizationController: add pairing name: " + msg.problemOrResultID);
					}
					UniChannel queueAdd = channelMap.get(msg.senderID);
					queueAdd.send(nullResult);
					break;
				case REMOVEPAIRINGNAME: 
					// Assert: 
					if (!pairingNames.contains(msg.problemOrResultID)) {
						System.out.println("Internal Error: MemoizationThread: Removing a pairing name that does not exist: " + msg.problemOrResultID);
						System.exit(1);
					}
					// parent removes its name after adding the names of its children
					pairingNames.remove(msg.problemOrResultID);
					synchronized(FanInSychronizer.getPrintLock()) {
						System.out.println("pairing names after remove");
						for (String name : pairingNames)  {
							System.out.println(name);
						}
					}
					synchronized(FanInSychronizer.getPrintLock()) {
						System.out.println("MemoizationController: remove pairing name: " + msg.problemOrResultID + " pairingNames.size: " + pairingNames.size());
					}
					UniChannel queueRemove = channelMap.get(msg.problemOrResultID);
					queueRemove.send(nullResult);
					// where:
					//removePairingNameMsgForParent.messageType = MemoizationController.MemoizationMessageType.REMOVEPAIRINGNAME;
					//removePairingNameMsgForParent.senderID = problem.problemID;
					//removePairingNameMsgForParent.problemOrResultID = problem.problemID;
					break;
				case PROMISEVALUE: 
					MemoizationRecord r1 = memoizationRecords.get(msg.memoizationLabel);
					if (r1 == null) {
						// first promise formsg.memoizationLabel
						MemoizationRecord promise = new MemoizationRecord();
						promise.resultID = msg.problemOrResultID;
						promise.type = MemoizationRecordType.PROMISEDVALUE;
						promise.promisedResults = new ArrayList<promisedResult>();
						promise.promisedResultsTemp = new ArrayList<String>();
						memoizationRecords.put(msg.memoizationLabel,promise);
						UniChannel queuePromise = channelMap.get(msg.problemOrResultID);
						synchronized(FanInSychronizer.getPrintLock()) {
							System.out.println("MemoizationThread: promise by: " + msg.problemOrResultID);
						}
						queuePromise.send(nullResult);
					}
					else { 
						// this is not first promise for msg.memoizationLabel, executor will stop and when 
						// the result is delivered a new one will be created to get result and continue fan-ins    
						if (r1.type == MemoizationRecordType.PROMISEDVALUE) {

							// the first promised result has not been delivered yet
							synchronized(FanInSychronizer.getPrintLock()) {
								System.out.println("MemoizationThread: duplicate promise by: " + msg.problemOrResultID);
							}
							promisedResult promise = new promisedResult(msg.problemOrResultID,msg.FanInStack,msg.becomeExecutor,
								msg.didInput);
	
							r1.promisedResults.add(promise);
							r1.promisedResultsTemp.add(msg.problemOrResultID);
							UniChannel queuePromise = channelMap.get(msg.problemOrResultID);
// 2:  - nullResult
							queuePromise.send(/*nullResult*/ stopResult);
						}
						if (r1.type == MemoizationRecordType.DELIVEREDVALUE) {
							// the first promised result has been delivered, so grab the delivered result
							UniChannel queuePromise = channelMap.get(msg.problemOrResultID);
							
							ResultType copyOfReturn = r1.result.copy();
							copyOfReturn.problemID = r1.result.problemID;
									
							synchronized(FanInSychronizer.getPrintLock()) {
								System.out.println("MemoizationThread: promised and delivered so deliver to: " + msg.problemOrResultID);
							}
							queuePromise.send(copyOfReturn);
						}
					}
					break;
					
				case DELIVEREDVALUE: 
					MemoizationRecord r2 = memoizationRecords.get(msg.memoizationLabel);
					synchronized(FanInSychronizer.getPrintLock()) {
						System.out.println("MemoizationThread: r2: " + r2);
					}
					if (r2 == null) {
						// this is the first delivery of this result - there should be a promise record
						System.out.println("Internal Error: MemoizationThread Delivered: Result Delivered but no previous promise.");
						System.exit(1);
					}
					else {
						// Assert:
						if (r2.type == MemoizationRecordType.DELIVEREDVALUE) {
							synchronized(FanInSychronizer.getPrintLock()) {
								System.out.println("Internal Error: MemoizationThread: sender: " + msg.senderID + " problem/result ID " + msg.problemOrResultID + " memoizationLabel: " 
									+ msg.memoizationLabel + " delivered result: " + msg.result + " delivered twice.");
							}
//: 1: back to working version - comment out exit
							System.exit(1);
						}
						// must be a PROMISEDVALUE, so deliver it ...
						// Q: Do we need to create the problem that will be given to executor(s), i.e.,
						// create the problem that is getting the memoized result and give this problem the 
						// stack, so passing to executor as usual a ProblemType?
						// If so, need to pass the problem as part of the message to get the memoized result?
						// But lots of data? but passed result when we delivered result .. No: don't need 
						// value since we are not computing result we are grabbing result, so we can 
						// trim the problem and push it on the stack.
						// So don't need data to be passed to get possible memoized result, so pass copy of problem
						// without its data? but user need to trim? but we have a trim
						// Push problem on stack? then pop it before calling executor to unwind stack?
						
						// This may not be the first duplicate promise for msg.memoizationLabel, i.e., other executors 
						// may have been promised the result for this subproblem; so 0, 1, or more executors need to 
						// be created to deliver this result All of these executors are in r2.promisedResults. 	

						r2.resultID = msg.problemOrResultID;
						r2.type = MemoizationRecordType.DELIVEREDVALUE;
						
						//: Note: do not create new lists of promisedResults, as we are here going to deliver 
						// the current promisedResults.
						
						ResultType copyOfReturn = msg.result.copy();
						copyOfReturn.problemID = msg.result.problemID;
						
						r2.result = copyOfReturn;
						memoizationRecords.put(msg.memoizationLabel,r2);
						synchronized(FanInSychronizer.getPrintLock()) {
							System.out.println("MemoizationThread: sender: " + msg.senderID + " problem/result ID " + msg.problemOrResultID + " memoizationLabel: " + msg.memoizationLabel + " delivered result: " + msg.result);
						}
					
						UniChannel queueDeliver= channelMap.get(msg.problemOrResultID);
						queueDeliver.send(nullResult);
						deliver(r2.promisedResults);
//: 3 - take comment off
						//deliverTemp(r2.promisedResultsTemp);						
					}
					break;
				}
			}
			System.out.println("MemoizationThread: !alive.");
			System.out.println();
		} // while alive
	}
}

class testBiChannel  {
	public static void main(String[] args) {
		BiChannel<ResultType> b = new BiChannel<ResultType>("Testing");
		ResultType result1 = new ResultType();
		
//Note: this only works for Fibonacci, when r.value is defined.
		//result1.value = 1;
		//result1.problemID = "testBiChannel";
		
		b.send1(result1);
		ResultType result2 = null;
		try {
			result2 = b.rcv2();
		} catch (Exception e) {
			
		}
		
		ResultType copy = result2.copy();
		copy.problemID = result2.problemID;
		
//Note: this only works for Fibonacci, when r.value is defined.
		//copy.value = 2;
		
		b.send2(copy);
		ResultType result3 = b.rcv1();
		System.out.println("Result1: " + result1);
		System.out.println("Result3: " + result3);
	}
	
}