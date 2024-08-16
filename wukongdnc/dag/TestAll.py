import logging
import sys
import getopt
import os

import traceback

# if running real lambdas or storing synch objects in real lambdas:
#   Set SERVERLESS_SYNC to True or False in wukongdnc constants !!!!!!!!!!!!!!

# Runs a single test with the command:
# python -m wukongdnc.dag.TestAll -t test#, e.g., TestAll -t 1

# Note: worker (multi)processes log to file mptestNew.txt in 
# directory WukongDynamic.

# https://stackoverflow.com/questions/6234405/logging-uncaught-exceptions-in-python/16993115#16993115
def custom_excepthook(exc_type, exc_value, exc_traceback):
    # Do not print exception when user cancels the program
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return

    # Here, we can output a label that the test script can look for.

    logging.error("An uncaught exception occurred:")
    logging.error("Type: %s", exc_type)
    logging.error("Value: %s", exc_value)

    if exc_traceback:
        format_exception = traceback.format_tb(exc_traceback)
        for line in format_exception:
            logging.error(repr(line))

sys.excepthook = custom_excepthook # handle_exception

def main(argv):

    # Note: When we run a multiprocessing workers test, we start worker
    # processes and (from link below) "On Windows the subprocesses will 
    # import (i.e. execute) the main module at start." So we put all of 
    # these imports and logging commands here so they are only done 
    # when we run TestAll from the command line.
    # The main thing we are trying to avoid is duplicate console debugging
    # messages. The multiprocesses write their output to a log file
    # mptestNew in directory WukongDynamic. Putting these imports and 
    # log commands here seems to help!
    from . import DAG_executor_constants
    from .addLoggingLevel import addLoggingLevel
    addLoggingLevel('TRACE', logging.DEBUG - 5)
    logging.basicConfig(encoding='utf-8',level=DAG_executor_constants.LOG_LEVEL, format='[%(asctime)s][%(module)s][%(processName)s][%(threadName)s]: %(message)s')
    # Added this to suppress the logging message:
    #   credentials - MainProcess - MainThread: Found credentials in shared credentials file: ~/.aws/credentials
    # But it appears that we could see other things liek this:
    # https://stackoverflow.com/questions/1661275/disable-boto-logging-without-modifying-the-boto-files
    logging.getLogger('botocore').setLevel(logging.CRITICAL)
    logger = logging.getLogger(__name__)

    # When we run a pagerank test we call BFS.main(). For non-pagerank
    # tests we call DAG_executor_driver.run().
    tests_start = 1
    pagerank_tests_start = 35
    pagerank_tests_end = 61
    tests_end = 61
    # Nane of file in which we write the test number passed by user
    test_number_file_name = "./test_number.txt"
    # If file exists, delete it. We delete this file at the end 
    # of TestAll so it shouldn't exist unless TestAll gets stopped
    # before it deletes the file. In this case we would just 
    # overwrite the old tes number file.
    
    # Several different processes will run - one for TestAll and 
    # processes for the workers.
    #print("TestAll loaded in PID: " + str(os.getpid()))
    try:
        if os.path.isfile(test_number_file_name):
            os.remove(test_number_file_name)
            logger.info("TestAll: removed test number file.")
        #else:
        #    logger.info("TestAll: test number file not found at start.")
    except Exception:
        logger.exception("[ERROR]: TestAll: Failed to remove test_number file.")
        if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
            logging.shutdown()
            os._exit(0)

    test_number_string = ''
    test_number = -1

    # Example: python -m wukongdnc.dag.TestAll -t 1
    try:
        opts, _args = getopt.getopt(argv, "ht:",["test="])
    except Exception:
        print("Usage: python -m wukongdnc.dag.TestAll -t <test_number>")
        sys.exit()

    if len(opts) != 1:
        print("Usage: specify a single option, e.g.: python -m wukongdnc.dag.TestAll -t <test_number>")
        sys.exit()
    
    for opt, arg in opts:
        if opt == '-h':
            print ('TestAll.py -t <test number>')
            sys.exit()
        elif opt in ("-t", "--test"):
            test_number_string = arg
            test_number = int(test_number_string)

            if test_number < tests_start or test_number > tests_end:
                print("Usage: specify a test number between " + str(tests_start)
                    + " and " + str(tests_end))
                sys.exit()
            # Above, we imported DAG_executor_constants. Here we
            # modify the constants in DAG_executor_constants so
            # that we can test a particular configuration. 
            # There is a constant test_number = 0 that 
            # set_test_number_and_run_test sets to the given user value 
            # and then set_test_number_and_run_test calls method
            # testi, where i is the test_number specified by the user,
            # whih sets the configuration contants for the test,
            # e.g., use worker threads, with n workers, or use 
            # real lambdas, compute pagerank, etc.
            logger.info("TestAll: set_test_number: " + str(test_number))
            DAG_executor_constants.set_test_number_and_run_test(test_number)

            # If we are testing worker processes, each worker process
            # will also need to call set_test_number_and_run_test(test_number)
            # to set the configuration constants in the worker process' 
            # own version of DAG_executor_constants. We communicate
            # the test number by writing ot to a file. This value is
            # read by the worker process when it starts and the worker
            # process (in DAG_executor.py) calls set_test_number_and_run_test.
            # We only need to do this when we are testing the worker process
            # configuration, e.e., test20()
            if DAG_executor_constants.RUN_ALL_TASKS_LOCALLY and DAG_executor_constants.USING_WORKERS \
                and not DAG_executor_constants.USING_THREADS_NOT_PROCESSES:
                try:
                    with open(test_number_file_name, 'w') as test_number_file:
                        test_number_file.write('%d' % test_number)
                    logger.info("TestAll: wrote " + str(test_number) + " to test_number_file")
                except Exception:
                    logger.exception("[ERROR]: TestAll: Failed to write test_number file.")
                    if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
                        logging.shutdown()
                        os._exit(0)

            try:
                if not (test_number >= pagerank_tests_start and test_number <= pagerank_tests_end):
                    # non-pagerank tests call DAG_executor_driver.run()
                    from . import DAG_executor_driver
                    DAG_executor_driver.run()
                else:
                    # pagerank tests call BFS to generate the DAG and BFS 
                    # calls DAG_executor_driver.run().
                    from . import BFS
                    BFS.main()
            except Exception:
                logger.exception("[ERROR]: TestAll: Failed to complete test.")
            finally:
                # remove test_number_file
                if DAG_executor_constants.RUN_ALL_TASKS_LOCALLY and DAG_executor_constants.USING_WORKERS \
                    and not DAG_executor_constants.USING_THREADS_NOT_PROCESSES:
                    if os.path.isfile(test_number_file_name):
                        os.remove(test_number_file_name)
                    else:
                        logger.error("[ERROR]: TestAll: Failed to remove test_number_file at end.")
                        if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
                            logging.shutdown()
                            os._exit(0)

# Note: TestAll will be the main module when we run a test for multprocessing:
#"Make sure that the main module can be safely imported by a new Python interpreter without causing unintended side effects (such a starting a new process)."
#... by using if __name__ == '__main__'
# https://stackoverflow.com/questions/18204782/runtimeerror-on-windows-trying-python-multiprocessing
                            
if __name__ == "__main__":
    main(sys.argv[1:]) # skip argv[0] which is name of script


"""
where: in DAG_executor_constants:

test_number = 0
# called by TestAll.py to run testX
# the test number is verified by TestAll to be within range.
def set_test_number_and_run_test(number):
    global test_number
    test_number = number
    #print("number: " + str(number))
    #print("test_number: " + str(test_number))

    # Run Tests

    # Set the configuration constants one time for the tests
    # that do not involve pagerank.
    non_pagerank_non_store_objects_in_lambda_base()

    # call method test<test_number>() to configure test, e.g., test20()
    test_method_name = "test"+str(test_number)

    # can also use getattr() - how to specify this DAG_executor_constants module?
    # https://stackoverflow.com/questions/3061/calling-a-function-of-a-module-by-using-its-name-a-string
    try:
        globals()[test_method_name]()
    except Exception:
        print("[Error]: test " + test_method_name + "() not found.")
        logging.shutdown()
        os._exit(0)

    # Check assserts after setting the configuration constants
    check_asserts()

"""
