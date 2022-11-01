import logging 
#import re 
#import socket
import time 
#import redis 
#import uuid

from wukongdnc.server.message_handler_lambda import MessageHandler

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s')

#SLEEP_INTERVAL = 0.120

warm_resources = {
	'cold_start_time': time.time(),
	'invocation_count': 0,
	'message_handler' : None
}

class Lambda_Function_Simulator:
	def lambda_handler(self, payload):
		#start_time = time.time()
		global warm_resources
		invocation_time = time.time()
		warm_resources['invocation_count'] = warm_resources['invocation_count'] + 1
		#logger.debug("Invocation received. event: " + str(event))

		logger.debug(f'Invocation count: {warm_resources["invocation_count"]}, Seconds since cold start: {round(invocation_time - warm_resources["cold_start_time"], 1)}')

		# Extract all of the data from the payload.
		#json_message = cloudpickle.loads(base64.b64decode(event["json_message"]))
		json_message = payload['json_message']
		logger.debug("JSON message: " + str(json_message))

		if not warm_resources['message_handler']:
			# Issue: Can we get and print the name of the Lambda function - "LambdaBoundedBuffer" or "LambdaSemaphore"
			logger.debug("**************** Lambda function cold start ******************")
			warm_resources['message_handler'] = MessageHandler()
			#Issue: what if we lost a Lambda? If we have backup we can recover but how do we determine whether we failed?
		else:
			logger.debug("**************** warm start ******************")

		return_value = warm_resources['message_handler'].handle(json_message)

		return return_value