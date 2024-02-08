import logging
import sys
import getopt

#from .addLoggingLevel import addLoggingLevel
#import wukongdnc.dag.DAG_executor_constants

from wukongdnc.dag.DAG_executor_constants import log_level
from .addLoggingLevel import addLoggingLevel
addLoggingLevel('TRACE', logging.DEBUG - 5)
logging.basicConfig(encoding='utf-8',level=log_level, format='[%(asctime)s][%(module)s][%(processName)s][%(threadName)s]: %(message)s')
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

pagerank_tests_start = 35
pagerank_tests_end = 47
def main(argv):
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
        DAG_executor_constants.set_test_number(test_number)

    
#rhc: 
# ToDo: 
    if not (test_number >= pagerank_tests_start and test_number <= pagerank_tests_end):
        from . import DAG_executor_driver
        DAG_executor_driver.run()
    else:
        from . import BFS
        BFS.main()

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
