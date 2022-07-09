#Uses:
#from time import time
#import json

warm_resources = {
    'cold_start_time': time(),
    'invocation_count': 0,
	'message_handler' : None
}

def lambda_handler(event, context):
    #start_time = time.time()
	invocation_time = time()
	warm_resources['invocation_count'] = warm_resources['invocation_count'] + 1
    logger.debug("Invocation received. event: " + str(event))

    logger.debug(f'Invocation count: {warm_resources["invocation_count"]}, Seconds since cold start: {round(invocation_time - warm_resources["cold_start_time"], 1)}')

    # Extract all of the data from the payload.
    json_message = cloudpickle.loads(base64.b64decode(event["json_message"]))
	
	global warm_resources

    if not warm_resources['message_handler']:
		# Issue: Can we get and print the name of the Lambda function - "LambdaBoundedBuffer" or "LambdaSemaphore"
		logger.debug("**************** Lambda function cold start ******************")
        warm_resources['message_handler'] = MessageHandler(json_message)
		#Issue: what if we lost a Lambda? If we have backup we can recover but how do we determine whether we failed?
	else:
		logger.debug("**************** warm start ******************")

	message_handler.handle(json_message)
