# Lambda handler when we store sync objects in Lambdas.

import logging 
import base64
#import re 
#import socket
import time 
#import redis 
#import uuid
import cloudpickle 

from wukongdnc.server.message_handler_lambda import MessageHandler

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s')

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
	'message_handler' : None
}

def lambda_handler(event, context):
	#start_time = time.time()
	global warm_resources
	invocation_time = time.time()
	warm_resources['invocation_count'] = warm_resources['invocation_count'] + 1
	logger.debug("Invocation received. event: " + str(event))

	logger.debug(f'Invocation count: {warm_resources["invocation_count"]}, Seconds since cold start: {round(invocation_time - warm_resources["cold_start_time"], 1)}')

	# Extract all of the data from the payload.
	json_message = cloudpickle.loads(base64.b64decode(event["json_message"]))
	logger.debug("JSON message: " + str(json_message))

	if not warm_resources['message_handler']:
		# Issue: Can we get and print the name of the Lambda function - "LambdaBoundedBuffer" or "LambdaSemaphore"
		logger.debug("**************** Lambda function cold start ******************")
		warm_resources['message_handler'] = MessageHandler()
		#Issue: what if we lost a Lambda? If we have backup we can recover but how do we determine whether we failed?
	else:
		logger.debug("**************** warm start ******************")
	
	# Discussion (Sat July 9, 2022)

	# The first thing this lambda would do, rather than handling the message, is open up a socket to the TCP server.
	# The TCP server would then send the commands via the socket. 
	# We will eventually have an object/class where we call a method that opens a socket to a server.
	# And then it's going to sit there doing receive operations while the server sends it these 'json message' things.
	
	# Question: is that a single thread doing a receive?
	# Answer: yes, there's typically a single thread listening.
	# We could have the one thread just place the messages in a queue, and other threads grab the message out of the queue to process them.
	# (Of course, only one thread will be running at a time because this is Python.)
	
	# When the Lambda starts, it does whatever, opens a socket, and then sit there and start listening for messages.
	# The thread receives a message, which contains a specific operation.
	# Call the handler, everything proceeds as normal. Handler gets back a return value.
	# This can be sent back to the TCP server via a send. Trivial, not difficult. 
	
	# When thread doing receive gets message, puts in buffer, someone grabs message from buffer, executes operation, can the thread executing the op perform the send?
	# Maybe? I'm not sure. We can definitely do sends and receives at the same time, though. I'm just not sure if we can share the socket between threads safely. 
	warm_resources['message_handler'].handle(json_message)