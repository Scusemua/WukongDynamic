import logging
import sys
import getopt
import os

#from .addLoggingLevel import addLoggingLevel
#import wukongdnc.dag.DAG_executor_constants

#from wukongdnc.dag.DAG_executor_constants import LOG_LEVEL
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

# if running real lambdas or storing synch objects in real lambdas:
#   Set SERVERLESS_SYNC to True or False in wukongdnc constants !!!!!!!!!!!!!!

# Runs a single test with the command:
# python -m wukongdnc.dag.TestAll test#, e.g., TestAll 1

def main(argv):
    pagerank_tests_start = 35
    pagerank_tests_end = 61
    test_number_file_name = "./test_number.txt"
    # If file exists, delete it.
    try:
        if os.path.isfile(test_number_file_name):
            os.remove(test_number_file_name)
            logger.info("TestAll: removed test number file.")
        else:
            logger.info("TestAll: test number file not found at start.")
    except Exception:
        logger.exception("[ERROR]: TestAll: Failed to remove test_number file.")
        if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
            logging.shutdown()
            os._exit(0)

    test_number_string = ''
    test_number = -1
    opts, _args = getopt.getopt(argv, "ht:",["test="])
    for opt, arg in opts:
        if opt == '-h':
            print ('TestAll.py -t <test number>')
            sys.exit()
        elif opt in ("-t", "--test"):
            test_number_string = arg
            test_number = int(test_number_string)

            from . import DAG_executor_constants
            logger.info("TestAll: set_test_number: " + str(test_number))
            DAG_executor_constants.set_test_number(test_number)

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
    #print("TestAll loaded in PID: " + str(os.getpid()))
    
#brc: 
# ToDo: 
    if not (test_number >= pagerank_tests_start and test_number <= pagerank_tests_end):
        from . import DAG_executor_driver
        DAG_executor_driver.run()
    else:
        from . import BFS
        BFS.main()

    if DAG_executor_constants.RUN_ALL_TASKS_LOCALLY and DAG_executor_constants.USING_WORKERS \
        and not DAG_executor_constants.USING_THREADS_NOT_PROCESSES:
        if os.path.isfile(test_number_file_name):
            os.remove(test_number_file_name)
        else:
            logger.error("[ERROR]: TestAll: Failed to remove test_number_file at end.")
            if DAG_executor_constants.EXIT_PROGRAM_ON_EXCEPTION:
                logging.shutdown()
                os._exit(0)

# ToDo: put top-level constants in noTest()

if __name__ == "__main__":
   main(sys.argv[1:])

"""
where: in DAG_executor_constants:

test_number = 0

# called by TestAll.py to run testX
def set_test_number(number):
    global test_number
    test_number = number

    if not test_number == 0:
        non_real_lambda_base()

    if test_number == 1:
        test1()
    elif test_number == 2:
        test2()
    elif test_number == 3:
        test3()
    elif test_number == 4:
        test4()

    # Check assserts after setting the configuration constants
    if not test_number == 0:
        check_asserts()
"""
