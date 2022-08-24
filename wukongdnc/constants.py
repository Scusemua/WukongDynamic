REDIS_IP_PUBLIC = "34.239.113.201"
REDIS_IP_PRIVATE = "10.0.97.68"
TCP_SERVER_IP = ("127.0.0.1", 25565)

# If true, we are expecting to store sync objects in AWS Lambda functions.
# This causes BoundedBufferSelect and CountingSemaphoreMonitorSelect to import selector_lambda instead of selector.
SERVERLESS_SYNC = False