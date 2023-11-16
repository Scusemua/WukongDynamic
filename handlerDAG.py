import time 
import redis 
import os

# Might be useful:
# https://unbiased-coder.com/detect-aws-env-python-nodejs/
def is_aws_env():
    # there are other environ vars we can use
    lambda_function_name = os.environ.get('AWS_LAMBDA_FUNCTION_NAME')
    aws_execution_env = os.environ.get('AWS_EXECUTION_ENV')
    #return os.environ.get('AWS_LAMBDA_FUNCTION_NAME') or os.environ.get('AWS_EXECUTION_ENV')
    return not (lambda_function_name == None) or not (aws_execution_env == None)

from wukongdnc.constants import REDIS_IP_PRIVATE
from wukongdnc.dag.DAG_executor import DAG_executor_lambda

import logging 
from wukongdnc.dag.addLoggingLevel import addLoggingLevel
""" How to use: https://stackoverflow.com/questions/2183233/how-to-add-a-custom-loglevel-to-pythons-logging-facility/35804945#35804945
    >>> addLoggingLevel('TRACE', logging.DEBUG - 5)
    >>> logging.getLogger(__name__).setLevel("TRACE")
    >>> logging.getLogger(__name__).trace('that worked')
    >>> logging.trace('so did this')
    >>> logging.TRACE
"""
# We are starting a new real Lambda so add logging level TRACE
addLoggingLevel('TRACE', logging.DEBUG - 5)

#Note: The extra logging comes from AWS Lamba's stdout 
# capturing code. Anything written to stdout is 
# going to be logged at INFO level. 
#https://stackoverflow.com/questions/50909824/getting-logs-twice-in-aws-lambda-function
# So we use logger.propagate = False, which causes all messages
# to stdout to be logged at the log level, but we put the levelname
# in our messages so the actual level name shows up in the message
# e.g., we see:
# [INFO]-__init__.py:880,main.test_function,2018-06-18 12:22:53,099, test function log statement
# where:
# the AWS stdout captured message format is
# [INFO]-__init__.py:880  [[Message]]
# and our [[Message]] will start with levelname "[D]" for debug
# [D] [2023-11-04 08:44:54,244] [invoker] [MainProcess] [MainThread]: ... Some message ...


logger = logging.getLogger(__name__)
# I believe INFO is the deault level for Lambda
logger.setLevel(logging.INFO)
    # the "-.1s" is explained in https://stackoverflow.com/questions/27453056/change-levelname-format-in-logrecord
formatter = logging.Formatter('[%(levelname)-.1s] [%(asctime)s] [%(module)s] [%(processName)s] [%(threadName)s]: %(message)s')
# Perhaps hekps with duplicate log entries?
logger.propagate = False
"""
https://stackoverflow.com/questions/50909824/getting-logs-twice-in-aws-lambda-function

AWS Lambda also sets up a handler, on the root logger, 
and anything written to stdout is captured and logged as 
level INFO. Your log message is thus captured twice:

By the AWS Lambda handler on the root logger (as log 
messages propagate from nested child loggers to the root), 
and this logger has its own format configured.
By the AWS Lambda stdout-to-INFO logger.
This is why the messages all start with 
(asctime) [(levelname)]-(module):(lineno), information; 
the root logger is configured to output messages with 
that format and the information written to stdout is 
just another %(message) part in that output.

ust don't set a handler when you are in the AWS environment, 
or, disable propagation of the output to the root handler 
and *** live with all your messages being recorded as INFO 
messages by AWS ***; in the latter case your own formatter 
could include the levelname level information in the output.

You can disable log propagation with logger.propagate = False, 
at which point your message is only going to be passed to 
your handler, not to to the root handler as well.

And:

I'm not sure whether this is the cause of your problem, 
but by default, Python's loggers propagate their messages 
up to logging hierarchy. As you probably know, Python 
loggers are organized in a tree, with the root logger at 
the top and other loggers below it. In logger names, a 
dot (.) introduces a new hierarchy level. So if you do

logger = logging.getLogger('some_module.some_function`)
then you actually have 3 loggers:

The root logger (`logging.getLogger()`)
    A logger at module level (`logging.getLogger('some_module'))
        A logger at function level (`logging.getLogger('some_module.some_function'))
If you emit a log message on a logger and it is not discarded 
based on the loggers minimum level, then the message is 
passed on to the logger's handlers and to its parent logger. 
See this flowchart for more information.

If that parent logger (or any logger higher up in the 
hierarchy) also has handlers, then they are called, too.

I suspect that in your case, either the root logger or the 
main logger somehow ends up with some handlers attached, 
which leads to the duplicate messages. To avoid that, you 
can set propagate in your logger to False or only attach your 
handlers to the root logger.

with:
The extra logging comes from AWS Lamba's stdout capturing 
code. Anything written to stdout is going to be logged at 
INFO level. - Martijn Pieters
"""

SLEEP_INTERVAL = 0.120

if logger.handlers:
    for handler in logger.handlers:
        handler.setFormatter(formatter)

root = logging.getLogger()
if root.handlers:
    for handler in root.handlers:
        handler.setFormatter(formatter)

warm_resources = {
	'cold_start_time': time.time(),
	'invocation_count': 0,
}

# implcitly called by AWS Lambda aftee we call lambda_client.invoke().
# Note: When we test real Lambda logic we bridge the call to
# lambda_client.invoke() by not calling lambda_client.invoke() and 
# calling a local lambda_handler() defined in wukongdnc\wukong\invoker.py
def lambda_handler(event, context):
    invocation_time = time.time()
    warm_resources['invocation_count'] = warm_resources['invocation_count'] + 1
    
    start_time = time.time()
    rc = redis.Redis(host = REDIS_IP_PRIVATE, port = 6379)

    logger.info("lambda_handler: is_aws_env(): " + str(is_aws_env()))
    logger.info("lambda_handler: Lambda invocation received. Calling method DAG_executor_lambda(): event/payload is: " + str(event))
    logger.info(f'Invocation count: {warm_resources["invocation_count"]}, Seconds since cold start: {round(invocation_time - warm_resources["cold_start_time"], 1)}')
    DAG_executor_lambda(event)
				 
    end_time = time.time()
    duration = end_time - start_time
    logger.info("lambda_handler: DAG_executor_lambda() finished. Time elapsed: %f seconds." % duration)
    rc.lpush("durations", duration)    