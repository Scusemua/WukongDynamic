import logging
import sys
import getopt

logger = logging.getLogger(__name__)

# if running real lambdas or storing synch objects in real lambdas:
#   Set SERVERLESS_SYNC to True or False in wukongdnc constants !!!!!!!!!!!!!!

# Runs a single test given by ...

def main(argv):
    test_number = ''
    opts, _args = getopt.getopt(argv, "ht:",["test="])
    for opt, arg in opts:
        if opt == '-h':
            print ('TestAll.py -t <test number>')
            sys.exit()
        elif opt in ("-t", "--test"):
            test_number = arg

    from . import DAG_executor_constants
    DAG_executor_constants.set_test_number(test_number)

    from . import DAG_executor_driver
    DAG_executor_driver.run()

if __name__ == "__main__":
   main(sys.argv[1:])
