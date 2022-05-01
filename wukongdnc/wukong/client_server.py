from .channel import UniChannel, BiChannel
import sys

import logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s')
ch = logging.StreamHandler(sys.stdout)
ch.setFormatter(formatter)
#logger.addHandler(ch)

# fh = handlers.RotatingFileHandler("divide_and_conquer.log", maxBytes=(1048576*5), backupCount=7)
# fh.setFormatter(formatter)
# logger.addHandler(fh)

logger.propagate = False

if logger.handlers:
   for handler in logger.handlers:
      handler.setFormatter(formatter)

root = logging.getLogger()
if root.handlers:
    for handler in root.handlers:
       handler.setFormatter(formatter)

class ServerlessNetworkingClientServer(object):
    def __init__(self, connections : BiChannel, client_channel : UniChannel):
        self.connections = connections 
        self.client_channel = client_channel       
    
    def send1(self, msg):
        self.connections.send1(msg)
        
    def rcv1(self):
        """
        This will return an object of type `ResultType`. This will be a user-defined/user-supplied object.
        """
        res = self.client_channel.rcv()

        #logger.debug(">> ServerlessNetworkingClientServer.recv1(): type(res): " + str(type(res)) + ", value of res object: " + str(res))

        return res 